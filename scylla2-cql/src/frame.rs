use std::{cmp, io};

use bytes::Bytes;

use crate::{
    frame::{
        compression::Compression,
        envelope::{Envelope, EnvelopeHeader, ENVELOPE_HEADER_SIZE},
    },
    utils::invalid_data,
};

pub mod compression;
pub mod envelope;

pub const FRAME_UNCOMPRESSED_HEADER_SIZE: usize = 6;
pub const FRAME_COMPRESSED_HEADER_SIZE: usize = 8;
pub const FRAME_TRAILER_SIZE: usize = 4;
pub const FRAME_MAX_LENGTH: usize = (1 << 17) - 1;

const HEADER_CRC_MISMATCH: &str = "Header CRC mismatch";

#[derive(Debug, Clone, Copy)]
pub struct UncompressedFrameHeader {
    pub payload_length: usize,
    pub self_contained: bool,
}

impl UncompressedFrameHeader {
    pub fn serialize(self) -> [u8; FRAME_UNCOMPRESSED_HEADER_SIZE] {
        let mut buf = [0; FRAME_UNCOMPRESSED_HEADER_SIZE];
        assert!(self.payload_length < 1 << 17);
        let mut header = self.payload_length as u32;
        if self.self_contained {
            header |= 1 << 17;
        }
        buf[..3].copy_from_slice(&header.to_le_bytes()[..3]);
        let crc = crc24(&buf[..3]);
        buf[3..6].copy_from_slice(&crc);
        buf
    }

    pub fn deserialize(buf: [u8; FRAME_UNCOMPRESSED_HEADER_SIZE]) -> io::Result<Self> {
        if crc24(&buf[..3]) != buf[3..] {
            return Err(HEADER_CRC_MISMATCH).map_err(invalid_data);
        }
        let header = u32::from_le_bytes(buf[..4].try_into().unwrap());
        Ok(Self {
            payload_length: (header & 0x1ffff) as usize,
            self_contained: (header & 1 << 17) != 0,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CompressedFrameHeader {
    pub compressed_length: usize,
    pub uncompressed_length: usize,
    pub self_contained: bool,
}

impl CompressedFrameHeader {
    pub fn serialize(self) -> [u8; FRAME_COMPRESSED_HEADER_SIZE] {
        let mut buf = [0; FRAME_COMPRESSED_HEADER_SIZE];
        assert!(self.compressed_length < 1 << 17);
        assert!(self.uncompressed_length < 1 << 17);
        let mut header = self.compressed_length as u64;
        header |= (self.uncompressed_length as u64) << 17;
        if self.self_contained {
            header |= 1 << 34;
        }
        buf[..5].copy_from_slice(&header.to_le_bytes()[..5]);
        let crc = crc24(&buf[..5]);
        buf[5..8].copy_from_slice(&crc);
        buf
    }

    pub fn deserialize(buf: [u8; FRAME_COMPRESSED_HEADER_SIZE]) -> io::Result<Self> {
        if crc24(&buf[..5]) != buf[5..] {
            return Err(HEADER_CRC_MISMATCH).map_err(invalid_data);
        }
        let header = u64::from_le_bytes(buf[..8].try_into().unwrap());
        Ok(Self {
            compressed_length: (header & 0x1ffff) as usize,
            uncompressed_length: (header >> 17 & 0x1ffff) as usize,
            self_contained: (header & 1 << 34) != 0,
        })
    }
}

pub fn decompress_frame(
    bytes: Bytes,
    compression: Compression,
    uncompressed_length: usize,
) -> io::Result<Bytes> {
    if uncompressed_length == 0 {
        return Ok(bytes);
    }
    let uncompressed = compression.decompress(&bytes, uncompressed_length)?;
    if uncompressed.len() != uncompressed_length {
        return Err("Payload uncompressed length doesn't match").map_err(invalid_data);
    }
    Ok(bytes)
}

#[derive(Debug)]
pub struct EnvelopeBuilder {
    pub header: EnvelopeHeader,
    pub bytes: Vec<u8>,
}

impl EnvelopeBuilder {
    pub fn new(bytes: Vec<u8>) -> io::Result<Self> {
        if bytes.len() < ENVELOPE_HEADER_SIZE {
            return Err("Envelope header missing").map_err(invalid_data);
        }
        let header =
            EnvelopeHeader::deserialize(bytes[..ENVELOPE_HEADER_SIZE].try_into().unwrap())?;
        Ok(Self { header, bytes })
    }

    pub fn add_part(&mut self, part: &[u8]) -> io::Result<bool> {
        self.bytes.extend_from_slice(part);
        let expected_len = self.header.length as usize + ENVELOPE_HEADER_SIZE;
        match self.bytes.len().cmp(&expected_len) {
            cmp::Ordering::Greater => {
                Err("Envelope parts are bigger than expected").map_err(invalid_data)
            }
            cmp::Ordering::Equal => Ok(true),
            cmp::Ordering::Less => Ok(false),
        }
    }
}

impl From<EnvelopeBuilder> for Envelope {
    fn from(builder: EnvelopeBuilder) -> Self {
        Envelope::from_parts(
            builder.header,
            Bytes::from(builder.bytes).split_off(ENVELOPE_HEADER_SIZE),
        )
    }
}

// https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/net/Crc.java
pub fn crc24(buf: &[u8]) -> [u8; 3] {
    let mut crc = 0x875060;
    for &byte in buf {
        crc ^= (byte as u32) << 16;
        for _ in 0..8 {
            crc <<= 1;
            if crc & 0x1000000 != 0 {
                crc ^= 0x1974F0B
            }
        }
    }
    crc.to_le_bytes()[..3].try_into().unwrap()
}

// Wrap crc32fast::Hasher just to return le_bytes
#[derive(Debug, Default, Clone)]
pub struct Crc32Hasher(crc32fast::Hasher);

impl Crc32Hasher {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes)
    }

    pub fn finish(&self) -> [u8; 4] {
        self.0.clone().finalize().to_le_bytes()
    }
}

pub fn crc32(bytes: &[u8]) -> [u8; 4] {
    let mut hasher = Crc32Hasher::new();
    hasher.write(bytes);
    hasher.finish()
}
