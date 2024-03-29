use std::{collections::HashMap, sync::Arc};

use bytes::BufMut;
use enumflags2::BitFlags;

use crate::{
    cql::{LongString, ReadCql, ShortBytes, WriteCql},
    error::{InvalidRequest, ValueTooBig},
    extensions::ProtocolExtensions,
    frame::{
        compression::Compression,
        envelope::{
            custom_payload_size, write_custom_payload, EnvelopeHeader, EnvelopeHeaderFlag, OpCode,
            ENVELOPE_HEADER_SIZE,
        },
    },
    request::batch::BatchFlags,
    utils::flags,
    Consistency, ProtocolVersion,
};

pub mod auth;
pub mod batch;
pub mod execute;
pub mod options;
pub mod prepare;
pub mod query;
pub mod register;
pub mod startup;

pub trait Request {
    fn opcode(&self) -> OpCode;
    fn check(
        &self,
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
    ) -> Result<(), InvalidRequest> {
        Ok(())
    }
    fn serialized_size(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig>;
    fn serialize(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        slice: &mut [u8],
    );
}

impl<T> Request for &T
where
    T: ?Sized + Request,
{
    fn opcode(&self) -> OpCode {
        T::opcode(self)
    }

    fn check(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
    ) -> Result<(), InvalidRequest> {
        T::check(self, version, extensions)
    }

    fn serialized_size(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig> {
        T::serialized_size(self, version, extensions)
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        slice: &mut [u8],
    ) {
        T::serialize(self, version, extensions, slice);
    }
}

pub trait RequestExt: Request {
    fn serialized_envelope_size(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        custom_payload: Option<&HashMap<String, Vec<u8>>>,
    ) -> Result<usize, ValueTooBig> {
        Ok(ENVELOPE_HEADER_SIZE
            + custom_payload.map_or(Ok(0), custom_payload_size)?
            + self.serialized_size(version, extensions)?)
    }

    fn serialize_envelope(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        tracing: bool,
        custom_payload: Option<&HashMap<String, Vec<u8>>>,
        stream: i16,
        mut slice: &mut [u8],
    ) {
        let header = EnvelopeHeader {
            version: version.client(),
            flags: flags!(
                EnvelopeHeaderFlag::Tracing: tracing,
                EnvelopeHeaderFlag::CustomPayload: custom_payload.is_some()
            ),
            stream,
            opcode: self.opcode(),
            length: (slice.len() - ENVELOPE_HEADER_SIZE) as u32,
        };
        slice.put_slice(&header.serialize());
        if let Some(custom_payload) = custom_payload {
            write_custom_payload(custom_payload, &mut slice);
        }
        self.serialize(version, extensions, slice);
    }

    fn serialize_envelope_owned(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        tracing: bool,
        custom_payload: Option<&HashMap<String, Vec<u8>>>,
        stream: i16,
    ) -> Result<Vec<u8>, ValueTooBig> {
        let size = self.serialized_envelope_size(version, extensions, custom_payload)?;
        let mut vec = vec![0; size];
        self.serialize_envelope(
            version,
            extensions,
            tracing,
            custom_payload,
            stream,
            &mut vec,
        );
        Ok(vec)
    }

    #[allow(clippy::too_many_arguments)]
    fn compress_envelope(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        compression: Compression,
        tracing: bool,
        custom_payload: Option<&HashMap<String, Vec<u8>>>,
        stream: i16,
        slice: &mut [u8],
    ) -> Vec<u8> {
        self.serialize_envelope(version, extensions, tracing, custom_payload, stream, slice);
        let (header, payload) = slice.split_at(ENVELOPE_HEADER_SIZE);
        let mut compressed = compression.compress(payload, ENVELOPE_HEADER_SIZE);
        compressed[..ENVELOPE_HEADER_SIZE].copy_from_slice(header);
        compressed[1] |= EnvelopeHeaderFlag::Compression as u8;
        compressed
    }

    fn serialized(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
    ) -> Result<SerializedRequest, InvalidRequest> {
        self.check(version, extensions)?;
        let mut bytes = vec![0; self.serialized_size(version, extensions)?];
        self.serialize(version, extensions, &mut bytes[..]);
        Ok(SerializedRequest {
            opcode: self.opcode(),
            version,
            bytes: bytes.into(),
        })
    }

    fn with_consistency(self, consistency: Consistency) -> WithConsistency<Self>
    where
        Self: Sized,
    {
        WithConsistency {
            request: self,
            consistency,
        }
    }
}

impl<T> RequestExt for T where T: Request {}

#[derive(Debug, Clone)]
pub struct SerializedRequest {
    opcode: OpCode,
    version: ProtocolVersion,
    bytes: Arc<[u8]>,
}

impl Request for SerializedRequest {
    fn opcode(&self) -> OpCode {
        self.opcode
    }

    fn serialized_size(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig> {
        assert_eq!(version, self.version);
        Ok(self.bytes.len())
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        slice: &mut [u8],
    ) {
        assert_eq!(version, self.version);
        slice.copy_from_slice(&self.bytes);
    }
}

#[derive(Debug)]
pub struct WithConsistency<R> {
    request: R,
    consistency: Consistency,
}

impl<R> Request for WithConsistency<R>
where
    R: Request,
{
    fn opcode(&self) -> OpCode {
        self.request.opcode()
    }

    fn check(
        &self,
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
    ) -> Result<(), InvalidRequest> {
        if matches!(
            self.opcode(),
            OpCode::Query | OpCode::Execute | OpCode::Batch
        ) {
            Ok(())
        } else {
            Err(InvalidRequest::NoConsistency)
        }
    }

    fn serialized_size(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig> {
        self.request.serialized_size(version, extensions)
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        slice: &mut [u8],
    ) {
        self.request.serialize(version, extensions, slice);
        let offset = &mut &*slice;
        match self.request.opcode() {
            OpCode::Query => {
                <LongString>::read_cql(offset).unwrap();
            }
            OpCode::Execute => {
                ShortBytes::read_cql(offset).unwrap();
                if version >= ProtocolVersion::V5 {
                    ShortBytes::read_cql(offset).unwrap();
                }
            }
            OpCode::Batch => {
                i8::read_cql(offset).unwrap();
                let flags = BitFlags::<BatchFlags>::read_cql(offset).unwrap();
                for _ in 0..i32::read_cql(offset).unwrap() {
                    match i8::read_cql(offset).unwrap() {
                        0 => LongString::read_cql(offset).map(|_| ()).unwrap(),
                        1 => ShortBytes::read_cql(offset).map(|_| ()).unwrap(),
                        _ => unreachable!(),
                    }
                    for _ in 0..i16::read_cql(offset).unwrap() {
                        if flags.contains(BatchFlags::WithNamesForValues) {
                            <&str>::read_cql(offset).unwrap();
                        }
                        <&[u8]>::read_cql(offset).unwrap();
                    }
                }
            }
            _ => unreachable!(),
        }
        let offset = slice.len() - offset.len();
        self.consistency.write_cql(&mut &mut slice[offset..]);
    }
}
