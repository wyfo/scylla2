use crate::{
    cql::WriteCql, error::ValueTooBig, event::EventType, extensions::ProtocolExtensions,
    frame::envelope::OpCode, request::Request, ProtocolVersion,
};

#[derive(Debug)]
pub struct Register<'a> {
    pub event_types: &'a [EventType],
}

impl Request for Register<'_> {
    fn opcode(&self) -> OpCode {
        OpCode::Register
    }
    fn serialized_size(
        &self,
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig> {
        self.event_types
            .iter()
            .map(EventType::to_string)
            .collect::<Vec<_>>()
            .cql_size()
    }
    fn serialize(
        &self,
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        mut slice: &mut [u8],
    ) {
        self.event_types
            .iter()
            .map(EventType::to_string)
            .collect::<Vec<_>>()
            .write_cql(&mut slice);
    }
}
