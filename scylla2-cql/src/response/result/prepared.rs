use std::{io, sync::Arc};

use enumflags2::{bitflags, BitFlags};

use crate::{
    cql::{ReadCql, ShortBytes},
    extensions::ProtocolExtensions,
    response::result::{
        column_spec::{deserialize_column_specs, ColumnSpec},
        rows::Metadata,
    },
    utils::invalid_data,
    ProtocolVersion,
};

#[bitflags]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
#[non_exhaustive]
pub enum PreparedFlag {
    GlobalTableSpec = 0x0001,
}

#[derive(Debug, Clone)]
pub struct Prepared {
    pub id: Arc<[u8]>,
    pub result_metadata_id: Option<Arc<[u8]>>,
    pub pk_indexes: Arc<[u16]>,
    pub column_specs: Arc<[ColumnSpec]>,
    pub result_specs: Option<Arc<[ColumnSpec]>>,
    pub is_lwt: bool,
}

impl Prepared {
    pub fn deserialize(
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        mut slice: &[u8],
    ) -> io::Result<Self> {
        let buf = &mut slice;
        let id = ShortBytes::read_cql(buf)?.0.into();
        let result_metadata_id = match version {
            ProtocolVersion::V5 => Some(ShortBytes::read_cql(buf)?.0.into()),
            ProtocolVersion::V4 => None,
        };
        let flags = BitFlags::read_cql(buf)?;
        let columns_count = u32::read_cql(buf)?;
        let pk_count = i32::read_cql(buf)?;
        let pk_indexes = (0..pk_count)
            .map(|_| i16::read_cql(buf))
            .map(|pki| pki?.try_into().map_err(invalid_data))
            .collect::<Result<_, io::Error>>()?;
        let global_table_spec = if flags.contains(PreparedFlag::GlobalTableSpec) {
            let keyspace = <&str>::read_cql(buf)?;
            let table = <&str>::read_cql(buf)?;
            Some((keyspace, table))
        } else {
            None
        };
        let column_specs = deserialize_column_specs(columns_count, global_table_spec, buf)?.into();
        let result_specs = Metadata::deserialize(version, extensions, buf)?
            .column_specs
            .map(Into::into);
        let is_lwt = extensions
            .and_then(|ext| ext.scylla_lwt_add_metadata_mark)
            .map_or(false, |flag| flags.bits() | flag == flag);
        Ok(Self {
            id,
            result_metadata_id,
            pk_indexes,
            column_specs,
            result_specs,
            is_lwt,
        })
    }
}
