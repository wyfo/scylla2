use std::marker::PhantomData;

use crate::{
    cql::{ShortBytes, WriteCql},
    error::{InvalidRequest, ValueTooBig},
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

#[derive(Debug)]
pub struct Execute<'a, P, V>
where
    P: QueryParameters<V>,
{
    pub id: &'a [u8],
    pub result_metadata_id: Option<&'a [u8]>,
    pub parameters: P,
    pub _phantom: PhantomData<V>,
}

impl<P, V> Request for Execute<'_, P, V>
where
    P: QueryParameters<V>,
    V: QueryValues,
{
    fn opcode(&self) -> OpCode {
        OpCode::Execute
    }

    fn check(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
    ) -> Result<(), InvalidRequest> {
        match version {
            ProtocolVersion::V5 if self.result_metadata_id.is_none() => {
                Err(InvalidRequest::MissingResultMetadataId)
            }
            _ => Ok(()),
        }
    }

    fn serialized_size(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig> {
        let result_metadata_id_size = match version {
            ProtocolVersion::V5 => ShortBytes(self.result_metadata_id.unwrap()).cql_size()?,
            _ => 0,
        };
        Ok(ShortBytes(self.id).cql_size()?
            + result_metadata_id_size
            + self.parameters.serialized_size(version, extensions)?)
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        mut slice: &mut [u8],
    ) {
        ShortBytes(self.id).write_cql(&mut slice);
        if version >= ProtocolVersion::V5 {
            ShortBytes(self.result_metadata_id.unwrap()).write_cql(&mut slice);
        }
        self.parameters.serialize(version, extensions, slice)
    }
}
