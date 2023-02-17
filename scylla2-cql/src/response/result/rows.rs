use std::{io, iter::FusedIterator, marker::PhantomData, sync::Arc};

use bytes::Bytes;
use enumflags2::{bitflags, BitFlags};

use crate::{
    cql::ReadCql,
    error::{BoxedError, ParseError, TypeError},
    extensions::ProtocolExtensions,
    response::result::column_spec::{deserialize_column_specs, ColumnSpec},
    utils::{invalid_data, tuples},
    value::{convert::FromValue, ReadValueExt},
    ProtocolVersion,
};

#[derive(Debug)]
pub struct Rows {
    pub envelope: Bytes,
    pub metadata: Metadata,
    pub rows_count: usize,
    pub rows_offset: usize,
}

impl Rows {
    pub fn deserialize(
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        envelope: Bytes,
        offset: usize,
    ) -> io::Result<Self> {
        let buf = &mut &envelope[offset..];
        let metadata = Metadata::deserialize(version, extensions, buf)?;
        let rows_count = u32::read_cql(buf)? as usize;
        let rows_offset = envelope.len() - buf.len();
        Ok(Rows {
            envelope,
            metadata,
            rows_count,
            rows_offset,
        })
    }

    pub fn rows_slice(&self) -> &[u8] {
        &self.envelope[self.rows_offset..]
    }

    pub fn parse<'a, P>(
        &'a self,
        column_specs: Option<&'a [ColumnSpec]>,
    ) -> Option<Result<RowIterator<'a, P>, TypeError>>
    where
        P: RowParser<'a>,
    {
        let column_specs = column_specs.or(self.metadata.column_specs.as_deref())?;
        if let Err(err) = P::check_column_specs(column_specs) {
            return Some(Err(err));
        }
        Some(Ok(RowIterator {
            envelope: &self.envelope,
            column_specs,
            rows_count: self.rows_count,
            bytes: self.rows_slice(),
            _phantom: PhantomData,
        }))
    }
}

// PagingState is allocated to avoid keeping a reference on previous payload,
// because it would be kept in memory during the next request otherwise
#[derive(Debug, Clone)]
pub struct PagingState(Arc<[u8]>);

impl AsRef<[u8]> for PagingState {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[bitflags]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
#[non_exhaustive]
pub enum RowsFlag {
    GlobalTableSpec = 0x0001,
    HasMorePage = 0x0002,
    NoMetadata = 0x0004,
    MetadataChanged = 0x0008,
}

#[derive(Debug)]
pub struct Metadata {
    pub columns_count: u32,
    pub paging_state: Option<PagingState>,
    pub new_metadata_id: Option<Arc<[u8]>>,
    pub column_specs: Option<Box<[ColumnSpec]>>,
}

impl Metadata {
    pub fn deserialize(
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        buf: &mut &[u8],
    ) -> io::Result<Self> {
        let flags = BitFlags::read_cql(buf)?;
        let columns_count = u32::read_cql(buf)?;
        let paging_state = if flags.contains(RowsFlag::HasMorePage) {
            Some(PagingState(<&[u8]>::read_cql(buf)?.into()))
        } else {
            None
        };
        let new_metadata_id = if flags.contains(RowsFlag::MetadataChanged) {
            Some(<&[u8]>::read_cql(buf)?.into())
        } else {
            None
        };
        let column_specs = if flags.contains(RowsFlag::NoMetadata) {
            None
        } else {
            let global_table_spec = if flags.contains(RowsFlag::GlobalTableSpec) {
                let keyspace = <&str>::read_cql(buf)?;
                let table = <&str>::read_cql(buf)?;
                Some((keyspace, table))
            } else {
                None
            };
            Some(deserialize_column_specs(columns_count, global_table_spec, buf)?.into())
        };
        Ok(Self {
            columns_count,
            paging_state,
            new_metadata_id,
            column_specs,
        })
    }
}

pub trait RowParser<'a>: Sized {
    fn check_column_specs(column_specs: &[ColumnSpec]) -> Result<(), TypeError>;
    fn parse_row(
        col_specs: &[ColumnSpec],
        envelope: &'a Bytes,
        bytes: &mut &'a [u8],
    ) -> Result<Self, ParseError>;
}

macro_rules! tuple_row {
    ($($tp:ident/$_:ident/$idx:tt),*;$len:literal) => {
        impl<'a, $($tp,)*> RowParser<'a> for ($($tp,)*)
        where
            $($tp: FromValue<'a>,)*
        {
            fn check_column_specs(column_specs: &[ColumnSpec]) -> Result<(), TypeError> {
                if column_specs.len() != $len {
                    return Err(TypeError);
                }
                $($tp::check_type(&column_specs[$idx].r#type)?;)*
                Ok(())
            }

            #[allow(unused_variables)]
            fn parse_row(
                _column_specs: &[ColumnSpec],
                envelope: &'a Bytes,
                bytes: &mut &'a [u8],
            ) -> Result<Self, ParseError> {
                Ok(($($tp::from_value($tp::Value::read_value_with_size(bytes, envelope)?)?,)*))
            }
        }
    };
}

tuples!(tuple_row);

#[derive(Debug, Clone)]
pub struct RowIterator<'a, P> {
    pub envelope: &'a Bytes,
    pub column_specs: &'a [ColumnSpec],
    pub rows_count: usize,
    pub bytes: &'a [u8],
    pub _phantom: PhantomData<P>,
}

impl<'a, P> Iterator for RowIterator<'a, P>
where
    P: RowParser<'a>,
{
    type Item = Result<P, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rows_count == 0 {
            if !self.bytes.is_empty() {
                return Some(Err(invalid_data("Unexpected remaining bytes").into()));
            }
            return None;
        }
        self.rows_count -= 1;
        Some(P::parse_row(
            self.column_specs,
            self.envelope,
            &mut self.bytes,
        ))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.rows_count, Some(self.rows_count))
    }
}

impl<'a, P> ExactSizeIterator for RowIterator<'a, P> where P: RowParser<'a> {}

impl<'a, P> FusedIterator for RowIterator<'a, P> where P: RowParser<'a> {}

pub trait FromRow<'a>: Sized {
    type Row: RowParser<'a>;
    fn check_column_specs(_column_specs: &[ColumnSpec]) -> Result<(), TypeError> {
        Ok(())
    }
    fn from_row(value: Self::Row) -> Result<Self, BoxedError>;
}

impl<'a, T> RowParser<'a> for T
where
    T: FromRow<'a>,
{
    fn check_column_specs(column_specs: &[ColumnSpec]) -> Result<(), TypeError> {
        T::Row::check_column_specs(column_specs)?;
        <T as FromRow>::check_column_specs(column_specs)
    }

    fn parse_row(
        col_specs: &[ColumnSpec],
        envelope: &'a Bytes,
        bytes: &mut &'a [u8],
    ) -> Result<Self, ParseError> {
        Ok(T::from_row(T::Row::parse_row(col_specs, envelope, bytes)?)?)
    }
}
