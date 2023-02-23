use std::{
    future::Future,
    io,
    io::IoSlice,
    iter::Fuse,
    net::SocketAddr,
    pin::Pin,
    slice,
    task::{ready, Context, Poll},
};

use rand::seq::SliceRandom;
use scylla2_cql::response::ResponseBody;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::error::BoxedError;

pub(crate) fn invalid_response(response: ResponseBody) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("Invalid response: {response:?}"),
    )
}

pub(crate) fn other_error(error: impl Into<BoxedError>) -> io::Error {
    io::Error::new(io::ErrorKind::Other, error)
}

// Temporary waiting for write_all_vectored stabilization
trait IoSliceExt<'a> {
    fn advance_slices2(bufs: &mut &mut [IoSlice<'a>], n: usize);
}
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

pub(crate) trait AsyncWriteAllVectored: AsyncWriteExt {
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

pub(crate) struct WriteAllVectored<'a, 'b, W: ?Sized + Unpin> {
    writer: &'a mut W,
    bufs: &'a mut [IoSlice<'b>],
}

impl<W: ?Sized + Unpin> Unpin for WriteAllVectored<'_, '_, W> {}

impl<'a, 'b, W: AsyncWrite + ?Sized + Unpin> WriteAllVectored<'a, 'b, W> {
    pub(super) fn new(writer: &'a mut W, mut bufs: &'a mut [IoSlice<'b>]) -> Self {
        IoSlice::advance_slices2(&mut bufs, 0);
        Self { writer, bufs }
    }
}

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

// Resolve the given hostname using a DNS lookup if necessary.
// The resolution may return multiple IPs and the function returns one of them.
// It prefers to return IPv4s first, and only if there are none, IPv6s.
pub(crate) async fn resolve_hostname(hostname: &str) -> io::Result<SocketAddr> {
    let addrs: Result<Vec<_>, _> = match tokio::net::lookup_host(hostname).await {
        Err(err) => match tokio::net::lookup_host((hostname, 9042)).await {
            Ok(addrs) => Ok(addrs.collect()),
            Err(_) => Err(err),
        },
        Ok(addrs) => Ok(addrs.collect()),
    };
    #[cfg(feature = "tracing")]
    if let Err(ref error) = addrs {
        tracing::warn!(hostname, %error, "failed hostname lookup");
    }
    let addrs = addrs?;
    addrs
        .iter()
        .cloned()
        .find(|addr| addr.is_ipv4())
        .or_else(|| addrs.into_iter().next())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                format!("Could not resolve {hostname}"),
            )
        })
}

// match ($($stmt:ident/$values:ident/$idx:tt),*; $len:literal)
macro_rules! tuples {
    ($macro:ident) => {
        $macro!(; 1);
        $macro!(S1/V1/1; 2);
        $macro!(S1/V1/1, S2/V2/2; 3);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3; 4);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4; 5);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4, S5/V5/5; 6);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4, S5/V5/5, S6/V6/6; 7);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4, S5/V5/5, S6/V6/6, S7/V7/7; 8);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4, S5/V5/5, S6/V6/6, S7/V7/7, S8/V8/8; 9);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4, S5/V5/5, S6/V6/6, S7/V7/7, S8/V8/8, S9/V9/9; 10);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4, S5/V5/5, S6/V6/6, S7/V7/7, S8/V8/8, S9/V9/9, S10/V10/10; 11);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4, S5/V5/5, S6/V6/6, S7/V7/7, S8/V8/8, S9/V9/9, S10/V10/10, S11/V11/11; 12);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4, S5/V5/5, S6/V6/6, S7/V7/7, S8/V8/8, S9/V9/9, S10/V10/10, S11/V11/11, S12/V12/12; 13);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4, S5/V5/5, S6/V6/6, S7/V7/7, S8/V8/8, S9/V9/9, S10/V10/10, S11/V11/11, S12/V12/12, S13/V13/13; 14);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4, S5/V5/5, S6/V6/6, S7/V7/7, S8/V8/8, S9/V9/9, S10/V10/10, S11/V11/11, S12/V12/12, S13/V13/13, S14/V14/14; 15);
        $macro!(S1/V1/1, S2/V2/2, S3/V3/3, S4/V4/4, S5/V5/5, S6/V6/6, S7/V7/7, S8/V8/8, S9/V9/9, S10/V10/10, S11/V11/11, S12/V12/12, S13/V13/13, S14/V14/14, S15/V15/15; 16);
    };
}

pub(crate) use tuples;

pub(crate) struct RepeatLast<I, T> {
    iter: Fuse<I>,
    last: Option<T>,
}

impl<I, T> RepeatLast<I, T>
where
    I: Iterator<Item = T>,
{
    pub(crate) fn new(iter: I) -> Self {
        Self {
            iter: iter.fuse(),
            last: None,
        }
    }
}

impl<I, T> Iterator for RepeatLast<I, T>
where
    I: Iterator<Item = T>,
    T: Clone,
{
    type Item = T;
    fn next(&mut self) -> Option<T> {
        if let Some(next) = self.iter.next() {
            self.last = Some(next.clone());
            Some(next)
        } else if self.last.is_some() {
            self.last.clone()
        } else {
            panic!("Empty iterator");
        }
    }
}

pub(crate) enum RandomSliceIter<'a, T> {
    Single(slice::Iter<'a, T>),
    Random(rand::seq::SliceChooseIter<'a, [T], T>),
}
impl<'a, T> RandomSliceIter<'a, T> {
    pub(crate) fn new(slice: &'a [T]) -> Self {
        match slice.len() {
            1 => Self::Single(slice.iter()),
            len => Self::Random(slice.choose_multiple(&mut rand::thread_rng(), len)),
        }
    }
}
impl<'a, T> Iterator for RandomSliceIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(iter) => iter.next(),
            Self::Random(iter) => iter.next(),
        }
    }
}
