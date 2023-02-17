use std::str;

use enumflags2::bitflags;

use crate::{
    cql::{LongString, WriteCql},
    error::ValueTooBig,
    extensions::ProtocolExtensions,
    frame::envelope::OpCode,
    request::Request,
    ProtocolVersion,
};

#[bitflags]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
pub enum PrepareFlag {
    WithKeyspace = 0x01,
}

#[derive(Debug, Clone)]
pub struct Prepare<S, K> {
    pub statement: S,
    pub keyspace: Option<K>,
}

impl<S> From<S> for Prepare<S, &'static str> {
    fn from(statement: S) -> Self {
        Self {
            statement,
            keyspace: None,
        }
    }
}

impl<S, K> Request for Prepare<S, K>
where
    S: AsRef<str>,
    K: AsRef<str>,
{
    fn opcode(&self) -> OpCode {
        OpCode::Prepare
    }

    fn serialized_size(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig> {
        let mut size = LongString(self.statement.as_ref()).cql_size()?;
        if version >= ProtocolVersion::V5 {
            size += 0u32.cql_size()?;
            if let Some(ref ks) = self.keyspace {
                size += ks.as_ref().cql_size()?
            }
        }
        Ok(size)
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        mut slice: &mut [u8],
    ) {
        LongString(self.statement.as_ref()).write_cql(&mut slice);
        if version >= ProtocolVersion::V5 {
            if let Some(ref ks) = self.keyspace {
                (PrepareFlag::WithKeyspace as u32).write_cql(&mut slice);
                ks.as_ref().write_cql(&mut slice);
            } else {
                0u32.write_cql(&mut slice);
            }
        }
    }
}
