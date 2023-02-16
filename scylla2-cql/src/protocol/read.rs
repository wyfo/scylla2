use std::io;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    error::ReadLoopError,
    frame::{
        compression::Compression,
        crc32,
        envelope::{Envelope, EnvelopeHeader, OpCode, ENVELOPE_HEADER_SIZE},
        CompressedFrameHeader, Crc32Hasher, EnvelopeBuilder, UncompressedFrameHeader,
        FRAME_COMPRESSED_HEADER_SIZE, FRAME_UNCOMPRESSED_HEADER_SIZE,
    },
    utils::invalid_data,
    ProtocolVersion,
};

#[inline]
async fn read_envelope_v4(
    mut reader: impl AsyncRead + Unpin,
    crc: Option<&mut Crc32Hasher>,
) -> io::Result<Envelope> {
    let mut header_buf = [0u8; ENVELOPE_HEADER_SIZE];
    reader.read_exact(&mut header_buf).await?;
    let header = EnvelopeHeader::deserialize(header_buf)?;
    pub const VOID: [u8; 4] = 0x0001i32.to_be_bytes();
    let body = if header.opcode == OpCode::Result && header.length as usize == VOID.len() {
        let mut buffer = [0; VOID.len()];
        reader.read_exact(&mut buffer).await?;
        if let Some(crc) = crc {
            crc.write(&header_buf);
            crc.write(&buffer);
        }
        if buffer == VOID {
            Bytes::from_static(&VOID)
        } else {
            Bytes::copy_from_slice(&buffer)
        }
    } else {
        let mut buffer = vec![0u8; header.length as usize];
        reader.read_exact(&mut buffer).await?;
        if let Some(crc) = crc {
            crc.write(&header_buf);
            crc.write(&buffer);
        }
        buffer.into()
    };
    Ok(Envelope::from_parts(header, body))
}

pub async fn read_envelope_loop<E>(
    version: ProtocolVersion,
    compression: Option<Compression>,
    mut reader: impl AsyncRead + Unpin,
    mut callback: impl FnMut(Envelope) -> Result<(), E>,
) -> Result<(), ReadLoopError<E>> {
    let mut envelope_builder: Option<EnvelopeBuilder> = None;
    let mut envelopes = Vec::new();
    loop {
        match version {
            ProtocolVersion::V4 => callback(read_envelope_v4(&mut reader, None).await?)
                .map_err(ReadLoopError::Callback)?,
            ProtocolVersion::V5 => {
                let header = read_frame_v5_header(compression.is_some(), &mut reader).await?;
                if !header.self_contained {
                    let payload = read_frame_v5_payload(
                        header.compressed_length,
                        header.uncompressed_length,
                        compression,
                        &mut reader,
                    )
                    .await?;
                    if let Some(mut builder) = envelope_builder.take() {
                        if builder.add_part(&payload)? {
                            callback(builder.into()).map_err(ReadLoopError::Callback)?
                        } else {
                            envelope_builder = Some(builder)
                        }
                    } else {
                        envelope_builder = Some(EnvelopeBuilder::new(payload)?)
                    }
                    continue;
                }
                if envelope_builder.is_some() {
                    return Err(
                        "Self-contained frame followed incomplete not-self-contained frame(s)",
                    )
                    .map_err(invalid_data)
                    .map_err(Into::into);
                }
                if header.uncompressed_length > 0 {
                    let payload = read_frame_v5_payload(
                        header.compressed_length,
                        header.uncompressed_length,
                        compression,
                        &mut reader,
                    )
                    .await?;
                    let mut buf = &payload[..];
                    while !buf.is_empty() {
                        envelopes.push(read_envelope_v4(&mut buf, None).await?)
                    }
                } else {
                    let mut remain = header.compressed_length;
                    let mut crc = Crc32Hasher::new();
                    while remain > 0 {
                        let envelope = read_envelope_v4(&mut reader, Some(&mut crc)).await?;
                        if remain < envelope.body.len() + ENVELOPE_HEADER_SIZE {
                            return Err(invalid_data(
                                "Frame envelopes doesn't match payload length",
                            )
                            .into());
                        }
                        remain -= envelope.body.len() + ENVELOPE_HEADER_SIZE;
                        envelopes.push(envelope);
                    }
                    check_frame_v5_crc32(crc.finish(), &mut reader).await?;
                }
                envelopes
                    .drain(..)
                    .try_for_each(&mut callback)
                    .map_err(ReadLoopError::Callback)?
            }
        }
    }
}

// use CompressedFrameHeader because it's kind of generic
// (with uncompressed_length == 0 for uncompressed)
async fn read_frame_v5_header(
    compression: bool,
    mut reader: impl AsyncRead + Unpin,
) -> io::Result<CompressedFrameHeader> {
    Ok(if compression {
        let mut header_buf = [0u8; FRAME_COMPRESSED_HEADER_SIZE];
        reader.read_exact(&mut header_buf).await?;
        CompressedFrameHeader::deserialize(header_buf)?
    } else {
        let mut header_buf = [0u8; FRAME_UNCOMPRESSED_HEADER_SIZE];
        reader.read_exact(&mut header_buf).await?;
        let header = UncompressedFrameHeader::deserialize(header_buf)?;
        CompressedFrameHeader {
            compressed_length: header.payload_length,
            uncompressed_length: 0,
            self_contained: header.self_contained,
        }
    })
}

async fn read_frame_v5_payload(
    payload_len: usize,
    uncompressed_len: usize,
    compression: Option<Compression>,
    mut reader: impl AsyncRead + Unpin,
) -> io::Result<Vec<u8>> {
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload).await?;
    check_frame_v5_crc32(crc32(&payload), reader).await?;
    #[allow(unreachable_code)] // because `compression.unwrap()` without compression features
    if uncompressed_len > 0 {
        payload = compression
            .unwrap()
            .decompress(&payload, uncompressed_len)?;
    }
    Ok(payload)
}

async fn check_frame_v5_crc32(
    expected_crc: [u8; 4],
    mut reader: impl AsyncRead + Unpin,
) -> io::Result<()> {
    let mut crc = [0u8; 4];
    reader.read_exact(&mut crc).await?;
    if crc != expected_crc {
        return Err("Payload crc mismatch").map_err(invalid_data);
    }
    Ok(())
}

pub async fn read_envelope(
    version: ProtocolVersion,
    compression: Option<Compression>,
    mut reader: impl AsyncRead + Unpin,
) -> io::Result<Envelope> {
    match version {
        ProtocolVersion::V4 => read_envelope_v4(reader, None).await,
        ProtocolVersion::V5 => {
            let header = read_frame_v5_header(compression.is_some(), &mut reader).await?;
            let payload = read_frame_v5_payload(
                header.compressed_length,
                header.uncompressed_length,
                compression,
                &mut reader,
            )
            .await?;
            let envelope = read_envelope_v4(&payload[..], None).await?;
            if envelope.body.len() + ENVELOPE_HEADER_SIZE < payload.len() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Expected a single envelope",
                ));
            }
            Ok(envelope)
        }
    }
}
