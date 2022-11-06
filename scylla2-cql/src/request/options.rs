use crate::{
    error::ValueTooBig, extensions::ProtocolExtensions, frame::envelope::OpCode, request::Request,
    ProtocolVersion,
};

#[derive(Debug)]
pub struct Options;

impl Request for Options {
    fn opcode(&self) -> OpCode {
        OpCode::Options
    }

    fn serialized_size(
        &self,
        _version: ProtocolVersion,
        _extensions: ProtocolExtensions,
    ) -> Result<usize, ValueTooBig> {
        Ok(0)
    }

    fn serialize(
        &self,
        _version: ProtocolVersion,
        _extensions: ProtocolExtensions,
        _buf: &mut [u8],
    ) {
    }
}
