use crate::{
    cql::{LongString, WriteCql},
    error::ValueTooBig,
    extensions::ProtocolExtensions,
    frame::envelope::OpCode,
    request::{
        query::{parameters::QueryParameters, values::QueryValues},
        Request,
    },
    ProtocolVersion,
};

pub mod parameters;
pub mod values;

#[derive(Debug)]
pub struct Query<'a, V> {
    pub query: &'a str,
    pub parameters: QueryParameters<'a, V>,
}

impl<V> Request for Query<'_, V>
where
    V: QueryValues,
{
    fn opcode(&self) -> OpCode {
        OpCode::Query
    }

    fn serialized_size(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig> {
        Ok(LongString(self.query).cql_size()?
            + self.parameters.serialized_size(version, extensions)?)
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        mut slice: &mut [u8],
    ) {
        LongString(self.query).write_cql(&mut slice);
        self.parameters.serialize(version, extensions, slice);
    }
}
