use std::{borrow::Cow, str};

use crate::{
    cql::{Consistency, CqlWrite, LongString},
    error::ValueTooBig,
    extensions::Extensions,
    frame::envelope::OpCode,
    request::statement::{query_values::QueryValues, Statement, StatementParameters},
    ProtocolVersion,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
pub enum QueryParametersFlag {
    Values = 0x01,
    SkipMetadata = 0x02,
    PageSize = 0x04,
    WithPagingState = 0x08,
    WithSerialConsistency = 0x10,
    WithDefaultTimestamp = 0x20,
    WithNamesForValues = 0x40,
}

pub(crate) fn query_parameters_size(
    version: ProtocolVersion,
    params: impl StatementParameters,
    values: Option<&impl QueryValues>, // None for batch
) -> Result<usize, ValueTooBig> {
    fn opt_size(opt: Option<impl CqlWrite>) -> Result<usize, ValueTooBig> {
        opt.as_ref().map(CqlWrite::cql_size).unwrap_or(Ok(0))
    }
    let flags_size = match version {
        ProtocolVersion::V4 => 0u8.cql_size()?,
        ProtocolVersion::V5 => 0u32.cql_size()?,
    };
    Ok(Consistency::default().cql_size()?
        + flags_size
        + 0i16.cql_size()?
        + opt_size(values.map(QueryValues::count).filter(|c| *c > 0))?
        + values.map(QueryValues::serialized_size).unwrap_or(Ok(0))?
        + opt_size(params.page_size())?
        + opt_size(params.paging_state().map(AsRef::as_ref))?
        + opt_size(params.serial_consistency())?
        + opt_size(params.timestamp())?)
}

pub(crate) fn write_query_parameters(
    version: ProtocolVersion,
    buf: &mut &mut [u8],
    skip_metadata: bool,
    params: impl StatementParameters,
    values: Option<&impl QueryValues>, // None for batch
) {
    params.consistency().write_cql(buf);
    let mut flags = 0u32;
    let mut add_flag = |flag, condition| {
        if condition {
            flags |= flag as u32
        }
    };
    add_flag(
        QueryParametersFlag::Values,
        values.map(|v| v.count() > 0).unwrap_or(false),
    );
    add_flag(QueryParametersFlag::SkipMetadata, skip_metadata);
    add_flag(QueryParametersFlag::PageSize, params.page_size().is_some());
    add_flag(
        QueryParametersFlag::WithPagingState,
        params.paging_state().is_some(),
    );
    add_flag(
        QueryParametersFlag::WithSerialConsistency,
        params.serial_consistency().is_some(),
    );
    add_flag(
        QueryParametersFlag::WithDefaultTimestamp,
        params.timestamp().is_some(),
    );
    add_flag(
        QueryParametersFlag::WithNamesForValues,
        values.map(QueryValues::named).unwrap_or(false),
    );
    match version {
        ProtocolVersion::V4 => (flags as u8).write_cql(buf),
        ProtocolVersion::V5 => flags.write_cql(buf),
    };
    if let Some(values) = values.filter(|v| v.count() > 0) {
        values.count().write_cql(buf);
        values.serialize(buf);
    }
    fn opt_write(opt: Option<impl CqlWrite>, buf: &mut &mut [u8]) {
        if let Some(v) = opt {
            v.write_cql(buf)
        }
    }
    opt_write(params.page_size(), buf);
    opt_write(params.paging_state().map(AsRef::as_ref), buf);
    opt_write(params.serial_consistency(), buf);
    opt_write(params.timestamp(), buf);
}

impl<V> Statement<V> for str
where
    V: QueryValues,
{
    const OPCODE: OpCode = OpCode::Query;

    fn serialized_size(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&Extensions>,
        params: impl StatementParameters,
        values: &V,
    ) -> Result<usize, ValueTooBig> {
        Ok(LongString(self).cql_size()? + query_parameters_size(version, params, Some(values))?)
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&Extensions>,
        mut slice: &mut [u8],
        params: impl StatementParameters,
        values: &V,
    ) {
        LongString(self).write_cql(&mut slice);
        write_query_parameters(version, &mut slice, false, params, Some(values))
    }
}

// impl<'a, C, V, P> IntoStatement<C, V, P> for &'a str
// where
//     V: QueryValues,
// {
//     type Statement = &'a str;
//
//     fn into_statement(self) -> Self::Statement {
//         self
//     }
//
//     fn config(self) -> Option<C> {
//         None
//     }
//
//     fn partition(self, _values: &V) -> Option<P> {
//         None
//     }
// }
//
// impl<'a, C, V, P> IntoStatement<C, V, P> for &'a String
// where
//     V: QueryValues,
// {
//     type Statement = &'a str;
//
//     fn into_statement(self) -> Self::Statement {
//         self.as_ref()
//     }
//
//     fn config(self) -> Option<C> {
//         None
//     }
//
//     fn partition(self, _values: &V) -> Option<P> {
//         None
//     }
// }
//
// impl<'a, C, V, P> IntoStatement<C, V, P> for &'a Cow<'a, str>
// where
//     V: QueryValues,
// {
//     type Statement = &'a str;
//
//     fn into_statement(&'a self) -> Self::Statement {
//         self.as_ref()
//     }
//
//     fn config(self) -> Option<C> {
//         None
//     }
//
//     fn partition(self, _values: &V) -> Option<P> {
//         None
//     }
// }
