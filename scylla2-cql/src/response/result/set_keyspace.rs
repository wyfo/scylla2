use std::io;

use crate::{cql::ReadCql, extensions::ProtocolExtensions, ProtocolVersion};

#[derive(Debug)]
pub struct SetKeyspace {
    pub keyspace_name: String,
}

impl SetKeyspace {
    pub fn deserialize(
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        mut slice: &[u8],
    ) -> io::Result<Self> {
        Ok(Self {
            keyspace_name: String::read_cql(&mut slice)?,
        })
    }
}
