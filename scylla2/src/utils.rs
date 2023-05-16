use std::{io, iter::Fuse, net::SocketAddr, sync::Mutex};

use scylla2_cql::response::ResponseBody;

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

pub(crate) struct SharedIterator<I>(pub(crate) Mutex<I>);

impl<I> Iterator for &SharedIterator<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.lock().unwrap().next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.lock().unwrap().size_hint()
    }
}

impl<I> ExactSizeIterator for &SharedIterator<I>
where
    I: ExactSizeIterator,
{
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}
