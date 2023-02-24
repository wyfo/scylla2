use std::{
    fmt,
    future::Future,
    io,
    io::IoSlice,
    pin::Pin,
    task::{ready, Context, Poll},
};

use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    frame::{crc32, CompressedFrameHeader, UncompressedFrameHeader},
    ProtocolVersion,
};

pub async fn write_envelope(
    version: ProtocolVersion,
    compression: bool,
    envelope: &[u8],
    mut writer: impl AsyncWrite + Unpin,
) -> io::Result<()> {
    match (version, compression) {
        (ProtocolVersion::V4, _) => writer.write_all(envelope).await,
        (ProtocolVersion::V5, true) => {
            let header = CompressedFrameHeader {
                compressed_length: envelope.len(),
                uncompressed_length: 0,
                self_contained: true,
            };
            writer
                .write_all_vectored(
                    &mut [&header.serialize(), envelope, &crc32(envelope)].map(IoSlice::new),
                )
                .await
        }

        (ProtocolVersion::V5, false) => {
            let header = UncompressedFrameHeader {
                payload_length: envelope.len(),
                self_contained: true,
            };
            writer
                .write_all_vectored(
                    &mut [&header.serialize(), envelope, &crc32(envelope)].map(IoSlice::new),
                )
                .await
        }
    }
}

// Temporary waiting for write_all_vectored stabilization
#[cfg(feature = "write-all-vectored")]
trait IoSliceExt<'a> {
    fn advance_slices2(bufs: &mut &mut [IoSlice<'a>], n: usize);
}
#[cfg(feature = "write-all-vectored")]
impl<'a> IoSliceExt<'a> for IoSlice<'a> {
    fn advance_slices2(bufs: &mut &mut [IoSlice<'a>], n: usize) {
        // Number of buffers to remove.
        let mut remove = 0;
        // Total length of all the to be removed buffers.
        let mut accumulated_len = 0;
        for buf in bufs.iter() {
            if accumulated_len + buf.len() > n {
                break;
            } else {
                accumulated_len += buf.len();
                remove += 1;
            }
        }

        *bufs = &mut std::mem::take(bufs)[remove..];
        if bufs.is_empty() {
            assert_eq!(
                n, accumulated_len,
                "advancing io slices beyond their length"
            );
        } else {
            bufs[0] = IoSlice::new(unsafe { &*(&bufs[0][n - accumulated_len..] as *const [u8]) });
        }
    }
}

pub trait AsyncWriteAllVectored: AsyncWriteExt {
    fn write_all_vectored<'a, 'b>(
        &'a mut self,
        bufs: &'a mut [IoSlice<'b>],
    ) -> WriteAllVectored<'a, 'b, Self>
    where
        Self: Unpin,
    {
        WriteAllVectored::new(self, bufs)
    }
}

impl<T> AsyncWriteAllVectored for T where T: AsyncWriteExt {}

#[cfg(feature = "write-all-vectored")]
pub struct WriteAllVectored<'a, 'b, W: ?Sized + Unpin> {
    writer: &'a mut W,
    bufs: &'a mut [IoSlice<'b>],
}

#[cfg(feature = "write-all-vectored")]
impl<'a, 'b, W: AsyncWrite + ?Sized + Unpin> WriteAllVectored<'a, 'b, W> {
    pub(super) fn new(writer: &'a mut W, mut bufs: &'a mut [IoSlice<'b>]) -> Self {
        IoSlice::advance_slices2(&mut bufs, 0);
        Self { writer, bufs }
    }
}

#[cfg(feature = "write-all-vectored")]
impl<W: AsyncWrite + ?Sized + Unpin> Future for WriteAllVectored<'_, '_, W> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        while !this.bufs.is_empty() {
            let n = ready!(Pin::new(&mut this.writer).poll_write_vectored(cx, this.bufs))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            } else {
                IoSlice::advance_slices2(&mut this.bufs, n);
            }
        }

        Poll::Ready(Ok(()))
    }
}

#[cfg(not(feature = "write-all-vectored"))]
pub struct WriteAllVectored<'a, 'b, W: ?Sized + Unpin> {
    writer: &'a mut W,
    bufs: &'a mut [IoSlice<'b>],
    index: usize,
    pos: usize,
}

#[cfg(not(feature = "write-all-vectored"))]
impl<'a, 'b, W: AsyncWrite + ?Sized + Unpin> WriteAllVectored<'a, 'b, W> {
    pub(super) fn new(writer: &'a mut W, bufs: &'a mut [IoSlice<'b>]) -> Self {
        Self {
            writer,
            bufs,
            index: 0,
            pos: 0,
        }
    }
}

#[cfg(not(feature = "write-all-vectored"))]
impl<W: AsyncWrite + ?Sized + Unpin> Future for WriteAllVectored<'_, '_, W> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        while this.index != this.bufs.len() {
            let n = ready!(
                Pin::new(&mut this.writer).poll_write(cx, &this.bufs[this.index][this.pos..])
            )?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            } else if n == this.bufs[this.index][this.pos..].len() {
                this.index += 1;
                this.pos = 0;
            } else {
                this.pos += n;
            }
        }
        Poll::Ready(Ok(()))
    }
}

impl<W: ?Sized + Unpin> Unpin for WriteAllVectored<'_, '_, W> {}

impl<W: ?Sized + Unpin> fmt::Debug for WriteAllVectored<'_, '_, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteAllVectored").finish()
    }
}
