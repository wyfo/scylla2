use std::{collections::HashMap, io, result::Result as StdResult};

use bytes::Bytes;
use uuid::Uuid;

use crate::{
    cql::ReadCql,
    error::DatabaseError,
    event::Event,
    extensions::ProtocolExtensions,
    frame::{
        compression::Compression,
        envelope::{Envelope, EnvelopeHeaderFlag, OpCode},
    },
    response::{
        auth::{AuthChallenge, AuthSuccess, Authenticate},
        error::Error,
        result::Result,
        supported::Supported,
    },
    utils::invalid_data,
    ProtocolVersion,
};

pub mod auth;
pub mod error;
pub mod result;
pub mod supported;

#[derive(Debug)]
pub struct Response {
    pub tracing_id: Option<Uuid>,
    pub custom_payload: HashMap<String, Vec<u8>>,
    pub warnings: Vec<String>,
    pub body: ResponseBody,
}

impl Response {
    pub fn deserialize(
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        envelope: Envelope,
        compression: Option<Compression>,
    ) -> io::Result<Self> {
        let bytes = if envelope.flags.contains(EnvelopeHeaderFlag::Compression) {
            if let Some(compression) = compression {
                compression
                    .decompress_with_embedded_size(&envelope.body)?
                    .into()
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "No decompression algorithm provided",
                ));
            }
        } else {
            envelope.body
        };
        let buf = &mut &bytes[..];
        let tracing_id = envelope
            .flags
            .contains(EnvelopeHeaderFlag::Tracing)
            .then(|| ReadCql::read_cql(buf))
            .transpose()?;
        let warnings = envelope
            .flags
            .contains(EnvelopeHeaderFlag::Warnings)
            .then(|| ReadCql::read_cql(buf))
            .transpose()?
            .unwrap_or_default();
        let custom_payload = envelope
            .flags
            .contains(EnvelopeHeaderFlag::CustomPayload)
            .then(|| ReadCql::read_cql(buf))
            .transpose()?
            .unwrap_or_default();
        let offset = bytes.len() - buf.len();
        let body = ResponseBody::deserialize(version, extensions, envelope.opcode, bytes, offset)?;
        Ok(Response {
            tracing_id,
            custom_payload,
            warnings,
            body,
        })
    }
    pub fn ok(self) -> StdResult<Self, Box<DatabaseError>> {
        match self.body {
            ResponseBody::Error(error) => Err(Box::new(DatabaseError {
                error,
                tracing_id: self.tracing_id,
                custom_payload: self.custom_payload,
                warnings: self.warnings,
            })),
            body => Ok(Self { body, ..self }),
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum ResponseBody {
    Error(Error),
    Ready,
    Result(Result),
    Authenticate(Authenticate),
    AuthSuccess(AuthSuccess),
    AuthChallenge(AuthChallenge),
    Supported(Supported),
    Event(Event),
}

impl ResponseBody {
    pub fn deserialize(
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        opcode: OpCode,
        envelope: Bytes,
        offset: usize,
    ) -> io::Result<ResponseBody> {
        let slice = &envelope[offset..];
        Ok(match opcode {
            OpCode::Error => ResponseBody::Error(Error::deserialize(version, extensions, slice)?),
            OpCode::Ready => ResponseBody::Ready,
            OpCode::Result => {
                ResponseBody::Result(Result::deserialize(version, extensions, envelope, offset)?)
            }
            OpCode::Authenticate => {
                ResponseBody::Authenticate(Authenticate::deserialize(version, extensions, slice)?)
            }
            OpCode::AuthSuccess => {
                ResponseBody::AuthSuccess(AuthSuccess::deserialize(version, extensions, slice)?)
            }
            OpCode::AuthChallenge => {
                ResponseBody::AuthChallenge(AuthChallenge::deserialize(version, extensions, slice)?)
            }
            OpCode::Supported => ResponseBody::Supported(Supported::deserialize(slice)?),
            OpCode::Event => ResponseBody::Event(Event::deserialize(version, extensions, slice)?),
            opcode => return Err(format!("Invalid opcode {opcode:?}")).map_err(invalid_data),
        })
    }
}
