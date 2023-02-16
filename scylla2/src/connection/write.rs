use std::{io, io::IoSlice, ops::RangeBounds};

use scylla2_cql::{
    frame::{
        compression::Compression, crc32, CompressedFrameHeader, Crc32Hasher,
        UncompressedFrameHeader, FRAME_COMPRESSED_HEADER_SIZE, FRAME_MAX_LENGTH,
        FRAME_TRAILER_SIZE, FRAME_UNCOMPRESSED_HEADER_SIZE,
    },
    ProtocolVersion,
};
use swap_buffer_queue::{write::BytesSlice, write_vectored::VectoredSlice};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{connection::Connection, utils::AsyncWriteAllVectored};

impl Connection {
    pub(super) async fn write_task(&self, mut writer: impl AsyncWrite + Unpin) -> io::Result<()> {
        // TODO test if queue is closed and release the streams
        loop {
            tokio::select! {
                biased;
                Ok(mut slice) = self.slice_queue.dequeue() => {
                    self.write_slice(&mut writer, &mut slice).await?;
                    if let Ok(mut vectored) = self.vectored_queue.try_dequeue() {
                        self.write_vectored(&mut writer, &mut vectored).await?;
                    }
                }
                Ok(mut vectored) = self.vectored_queue.dequeue() => {
                    self.write_vectored(&mut writer, &mut vectored).await?;
                }
                else => {
                    writer.shutdown().await.ok();
                return Ok(());
                },
            }
        }
    }

    async fn write_slice(
        &self,
        mut writer: impl AsyncWrite + Unpin,
        slice: &mut BytesSlice<'_, FRAME_COMPRESSED_HEADER_SIZE, FRAME_TRAILER_SIZE>,
    ) -> io::Result<()> {
        match self.version {
            ProtocolVersion::V4 => writer.write_all(slice).await,
            ProtocolVersion::V5 => {
                let offset = if self.compression.is_some() {
                    let header = CompressedFrameHeader {
                        compressed_length: slice.len(),
                        uncompressed_length: 0,
                        self_contained: true,
                    };
                    slice.header().copy_from_slice(&header.serialize());
                    0
                } else {
                    let header = UncompressedFrameHeader {
                        payload_length: slice.len(),
                        self_contained: true,
                    };
                    let offset = FRAME_COMPRESSED_HEADER_SIZE - FRAME_UNCOMPRESSED_HEADER_SIZE;
                    slice.header()[offset..].copy_from_slice(&header.serialize());
                    offset
                };
                let crc = crc32(slice);
                slice.trailer().copy_from_slice(&crc);
                writer.write_all(&slice.frame()[offset..]).await
            }
            _ => panic!("unsupported"),
        }
    }

    async fn write_vectored(
        &self,
        writer: &mut (impl AsyncWrite + Unpin),
        slice: &mut VectoredSlice<'_>,
    ) -> io::Result<()> {
        match self.version {
            ProtocolVersion::V4 => writer.write_all_vectored(slice).await,
            ProtocolVersion::V5 => {
                if slice.total_size() < self.compression_min_size
                    && slice.total_size() < FRAME_MAX_LENGTH
                {
                    return self
                        .write_vectored_v5_uncompressed(writer, slice, .., slice.total_size())
                        .await;
                }
                let mut range = 0..0;
                let mut total_size = 0;
                for i in 0..slice.len() {
                    let io_slice = slice[i];
                    if total_size + io_slice.len() < FRAME_MAX_LENGTH
                        && (io_slice.len() < self.compression_min_size)
                    {
                        range.end = i + 1;
                        total_size += io_slice.len();
                        continue;
                    }
                    if !range.is_empty() {
                        self.write_vectored_v5_uncompressed(
                            writer,
                            slice,
                            range.clone(),
                            total_size,
                        )
                        .await?;
                    }
                    if io_slice.len() > FRAME_MAX_LENGTH {
                        for bytes in io_slice.chunks(FRAME_MAX_LENGTH) {
                            if let Some(compression) = self
                                .compression
                                .filter(|_| bytes.len() >= self.compression_min_size)
                            {
                                self.write_v5_compressed(writer, bytes, false, compression)
                                    .await?;
                            } else {
                                self.write_v5_uncompressed(writer, bytes, false).await?;
                            }
                        }
                        range = i + 1..i + 1;
                        total_size = 0;
                    } else if let Some(compression) = self
                        .compression
                        .filter(|_| io_slice.len() >= self.compression_min_size)
                    {
                        self.write_v5_compressed(writer, &io_slice, true, compression)
                            .await?;
                        range = i + 1..i + 1;
                        total_size = 0;
                    } else {
                        range = i..i + 1;
                        total_size = io_slice.len();
                    }
                }
                if !range.is_empty() {
                    self.write_vectored_v5_uncompressed(writer, slice, range.clone(), total_size)
                        .await?;
                }
                Ok(())
            }
            _ => panic!("unsupported"),
        }
    }

    async fn write_vectored_v5_uncompressed(
        &self,
        writer: &mut (impl AsyncWrite + Unpin),
        slice: &mut VectoredSlice<'_>,
        range: impl RangeBounds<usize>,
        payload_length: usize,
    ) -> io::Result<()> {
        let compressed_header_buf;
        let uncompressed_header_buf;
        let header_slice: &[u8] = if self.compression.is_some() {
            let header = CompressedFrameHeader {
                self_contained: true,
                compressed_length: payload_length,
                uncompressed_length: 0,
            };
            compressed_header_buf = header.serialize();
            &compressed_header_buf
        } else {
            let header = UncompressedFrameHeader {
                self_contained: true,
                payload_length,
            };
            uncompressed_header_buf = header.serialize();
            &uncompressed_header_buf
        };
        let mut hasher = Crc32Hasher::new();
        for bytes in slice.iter() {
            hasher.write(bytes);
        }
        let crc = hasher.finish();
        let mut frame = slice.frame(
            range,
            Some(IoSlice::new(header_slice)),
            Some(IoSlice::new(&crc)),
        );
        writer.write_all_vectored(&mut frame).await?;
        Ok(())
    }

    async fn write_v5_compressed(
        &self,
        writer: &mut (impl AsyncWrite + Unpin),
        bytes: &[u8],
        self_contained: bool,
        compression: Compression,
    ) -> io::Result<()> {
        let mut compressed = compression.compress(bytes, FRAME_COMPRESSED_HEADER_SIZE);
        let header = CompressedFrameHeader {
            self_contained,
            compressed_length: compressed.len(),
            uncompressed_length: bytes.len(),
        };
        compressed[..FRAME_COMPRESSED_HEADER_SIZE].copy_from_slice(&header.serialize());
        let crc = crc32(&compressed);
        compressed.extend_from_slice(&crc);
        writer.write_all(&compressed).await
    }

    async fn write_v5_uncompressed(
        &self,
        writer: &mut (impl AsyncWrite + Unpin),
        bytes: &[u8],
        self_contained: bool,
    ) -> io::Result<()> {
        let header = UncompressedFrameHeader {
            self_contained,
            payload_length: bytes.len(),
        };
        let header_buf = header.serialize();
        let crc = crc32(bytes);
        writer
            .write_all_vectored(&mut [
                IoSlice::new(&header_buf),
                IoSlice::new(bytes),
                IoSlice::new(&crc),
            ])
            .await
    }
}
