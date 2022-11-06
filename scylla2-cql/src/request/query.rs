use std::marker::PhantomData;

use crate::{
    cql::{LongString, WriteCql},
    error::ValueTooBig,
    extensions::ProtocolExtensions,
    frame::envelope::OpCode,
    request::{
        query::{
            parameters::{QueryParameters, QueryParametersExt},
            values::QueryValues,
        },
        Request,
    },
    ProtocolVersion,
};

pub mod parameters;
pub mod values;

#[derive(Debug)]
pub struct Query<'a, P, V>
where
    P: QueryParameters<V>,
{
    pub query: &'a str,
    pub parameters: P,
    pub _phantom: PhantomData<V>,
}

impl<P, V> Request for Query<'_, P, V>
where
    P: QueryParameters<V>,
    V: QueryValues,
{
    fn opcode(&self) -> OpCode {
        OpCode::Query
    }

    fn serialized_size(
        &self,
        version: ProtocolVersion,
        extensions: ProtocolExtensions,
    ) -> Result<usize, ValueTooBig> {
        Ok(LongString(self.query).cql_size()?
            + self.parameters.serialized_size(version, extensions)?)
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        extensions: ProtocolExtensions,
        mut slice: &mut [u8],
    ) {
        LongString(self.query).write_cql(&mut slice);
        self.parameters.serialize(version, extensions, slice)
    }
}
