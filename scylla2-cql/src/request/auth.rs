use crate::{
    error::ValueTooBig, extensions::ProtocolExtensions, frame::envelope::OpCode, request::Request,
    value::WriteValue, ProtocolVersion,
};

#[derive(Debug)]
pub struct AuthResponse<R> {
    pub token: R,
}

impl<R> Request for AuthResponse<R>
where
    R: AsRef<[u8]>,
{
    fn opcode(&self) -> OpCode {
        OpCode::AuthResponse
    }
    fn serialized_size(
        &self,
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig> {
        self.token.as_ref().value_size()
    }
    fn serialize(
        &self,
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        mut slice: &mut [u8],
    ) {
        self.token.as_ref().write_value(&mut slice)
    }
}
