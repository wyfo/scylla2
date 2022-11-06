use std::{collections::HashMap, io};

use bytes::Bytes;
use enumflags2::{bitflags, BitFlags};

use crate::{
    cql::{ReadCql, WriteCql},
    error::ValueTooBig,
    utils::invalid_data,
    VersionByte,
};

pub const ENVELOPE_HEADER_SIZE: usize = 9;
pub const ENVELOPE_MAX_LENGTH: usize = (1 << 28) - 1; // only for V4

#[derive(Debug, Copy, Clone, PartialEq, Eq, strum::FromRepr)]
#[repr(u8)]
#[non_exhaustive]
pub enum OpCode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
}

#[bitflags]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum EnvelopeHeaderFlag {
    Compression = 0x01,
    Tracing = 0x02,
    CustomPayload = 0x04,
    Warnings = 0x08,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct EnvelopeHeader {
    pub version: VersionByte,
    pub flags: BitFlags<EnvelopeHeaderFlag>,
    pub stream: i16,
    pub opcode: OpCode,
    pub length: u32,
}

impl EnvelopeHeader {
    pub fn serialize(self) -> [u8; ENVELOPE_HEADER_SIZE] {
        let mut buffer = [0; ENVELOPE_HEADER_SIZE];
        let buf = &mut &mut buffer[..];
        self.version.0.write_cql(buf);
        self.flags.write_cql(buf);
        self.stream.write_cql(buf);
        (self.opcode as u8).write_cql(buf);
        self.length.write_cql(buf);
        buffer
    }

    pub fn deserialize(buf: [u8; ENVELOPE_HEADER_SIZE]) -> io::Result<Self> {
        let buf = &mut &buf[..];
        let version = VersionByte(u8::read_cql(buf)?);
        let flags = BitFlags::read_cql(buf)?;
        let stream = i16::read_cql(buf)?;
        let opcode = OpCode::from_repr(u8::read_cql(buf)?)
            .ok_or("Invalid opcode")
            .map_err(invalid_data)?;
        let length = u32::read_cql(buf)?;
        Ok(Self {
            version,
            flags,
            stream,
            opcode,
            length,
        })
    }
}

pub fn custom_payload_size(
    custom_payload: &HashMap<String, Vec<u8>>,
) -> Result<usize, ValueTooBig> {
    custom_payload.cql_size()
}

pub fn write_custom_payload(custom_payload: &HashMap<String, Vec<u8>>, buf: &mut &mut [u8]) {
    custom_payload.write_cql(buf);
}

#[derive(Debug)]
pub struct Envelope {
    pub flags: BitFlags<EnvelopeHeaderFlag>,
    pub stream: i16,
    pub opcode: OpCode,
    pub body: Bytes,
}

impl Envelope {
    pub fn from_parts(header: EnvelopeHeader, body: Bytes) -> Self {
        assert_eq!(body.len(), header.length as usize);
        Self {
            flags: header.flags,
            stream: header.stream,
            opcode: header.opcode,
            body,
        }
    }
}
