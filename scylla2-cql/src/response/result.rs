use std::io;

use bytes::Bytes;
use rows::Rows;
use set_keyspace::SetKeyspace;

use crate::{
    cql::ReadCql, event::SchemaChangeEvent, extensions::ProtocolExtensions,
    response::result::prepared::Prepared, utils::invalid_data, ProtocolVersion,
};

pub mod column_spec;
pub mod lwt;
pub mod prepared;
pub mod rows;
pub mod set_keyspace;

#[derive(Debug)]
pub enum Result {
    Void,
    Rows(Rows),
    SetKeyspace(SetKeyspace),
    Prepared(Prepared),
    SchemaChange(SchemaChangeEvent),
}

pub type CqlResult = Result;

impl Result {
    pub fn deserialize(
        version: ProtocolVersion,
        extensions: ProtocolExtensions,
        envelope: Bytes,
        offset: usize,
    ) -> io::Result<Self> {
        let buf = &mut &envelope[offset..];
        Ok(match u32::read_cql(buf)? {
            0x0001 => Self::Void,
            0x0002 => Self::Rows(Rows::deserialize(
                version,
                extensions,
                envelope,
                offset + 4,
            )?),
            0x0003 => Self::SetKeyspace(SetKeyspace::deserialize(version, extensions, buf)?),
            0x0004 => Self::Prepared(Prepared::deserialize(version, extensions, buf)?),
            0x0005 => Self::SchemaChange(SchemaChangeEvent::deserialize(version, extensions, buf)?),
            n => return Err(format!("Invalid result kind {n}")).map_err(invalid_data),
        })
    }
}
