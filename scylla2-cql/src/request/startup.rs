use std::collections::HashMap;

use crate::{
    cql::WriteCql, error::ValueTooBig, extensions::ProtocolExtensions, frame::envelope::OpCode,
    request::Request, ProtocolVersion,
};

#[derive(Debug)]
pub struct Startup<'a> {
    pub options: &'a HashMap<String, String>,
}

impl Request for Startup<'_> {
    fn opcode(&self) -> OpCode {
        OpCode::Startup
    }
    fn serialized_size(
        &self,
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig> {
        self.options.cql_size()
    }
    fn serialize(
        &self,
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        mut slice: &mut [u8],
    ) {
        self.options.write_cql(&mut slice);
    }
}
