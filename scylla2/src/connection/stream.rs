use std::{
    cmp, future,
    future::poll_fn,
    io, iter, mem,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicIsize, AtomicU32, Ordering},
        Mutex,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::task::{noop_waker, AtomicWaker};
use once_cell::sync::OnceCell;
use scylla2_cql::frame::envelope::Envelope;

use crate::{connection::CONNECTION_STREAM_UPPER_BOUND, error::RequestError};

#[derive(Debug, thiserror::Error)]
#[error("Invalid stream received")]
pub(super) struct InvalidStream;

#[derive(Debug, Default)]
pub(super) enum StreamState {
    #[default]
    Null,
    Waiting(Waker),
    Ready(Result<Envelope, RequestError>),
    Aborted,
}

// Abortion/response receiving/connection closing can happen at the same time, hence the mutex
// There should be no contention in normal case
#[derive(Debug, Default)]
struct StreamNode(Mutex<StreamState>);

impl StreamNode {
    fn acquire(&self) {
        let mut state = self.0.lock().unwrap();
        debug_assert!(matches!(state.deref(), StreamState::Null));
        *state = StreamState::Waiting(noop_waker());
    }

    fn abort(&self) -> bool {
        let mut state = self.0.lock().unwrap();
        debug_assert!(!matches!(
            state.deref(),
            StreamState::Aborted | StreamState::Null
        ));
        if let StreamState::Ready(_) = mem::replace(state.deref_mut(), StreamState::Aborted) {
            *state = StreamState::Null;
            return true;
        }
        false
    }

    fn set_response(
        &self,
        response: Result<Envelope, RequestError>,
    ) -> Result<bool, InvalidStream> {
        let mut state = self.0.lock().unwrap();
        let is_err = response.is_err();
        match mem::replace(state.deref_mut(), StreamState::Ready(response)) {
            StreamState::Waiting(waker) => waker.wake(),
            StreamState::Aborted => {
                *state = StreamState::Null;
                return Ok(true);
            }
            // set_error can be called before poll
            StreamState::Ready(res) if is_err => *state = StreamState::Ready(res),
            // set_error can be called after poll, buf before releasing the stream
            StreamState::Null if is_err => *state = StreamState::Null,
            _ => return Err(InvalidStream),
        }
        Ok(false)
    }

    fn poll(&self, cx: &mut Context) -> Poll<Result<Envelope, RequestError>> {
        let mut state = self.0.lock().unwrap();
        debug_assert!(!matches!(state.deref(), StreamState::Aborted));
        if let StreamState::Ready(res) = mem::take(state.deref_mut()) {
            return Poll::Ready(res);
        }
        *state = StreamState::Waiting(cx.waker().clone());
        Poll::Pending
    }
}

pub(super) struct Stream<'a> {
    value: i16,
    pool: &'a StreamPool,
    node: &'a StreamNode,
}

impl Stream<'_> {
    pub(super) fn get(&self) -> i16 {
        self.value
    }

    pub(super) async fn wait_response(self) -> Result<Envelope, RequestError> {
        let result = future::poll_fn(|cx| self.node.poll(cx)).await;
        self.pool.release(self.value);
        mem::forget(self);
        result
    }
}

impl Drop for Stream<'_> {
    fn drop(&mut self) {
        if self.node.abort() {
            let (map, index) = self.pool.get_map_index(self.value);
            map.release(index);
        } else if self.pool.orphans_limit.fetch_sub(1, Ordering::Relaxed) == 0 {
            self.pool.too_many_orphans.wake();
        }
    }
}

#[derive(Debug)]
struct StreamMap {
    bitmap: Box<[AtomicU32]>,
    streams: Box<[StreamNode]>,
}

impl StreamMap {
    fn new(size: usize) -> Self {
        assert!(size.is_power_of_two() && (1 << 5..=1 << 14).contains(&size));
        Self {
            bitmap: (0..size / 32).map(|_| AtomicU32::new(0)).collect(),
            streams: (0..size).map(|_| StreamNode::default()).collect(),
        }
    }

    fn try_allocate(&self) -> Option<(usize, &StreamNode)> {
        for (i, atomic_bits) in self.bitmap.iter().enumerate() {
            let mut stream = atomic_bits.load(Ordering::Relaxed).trailing_ones();
            while stream != 32 {
                let stream_bit = 1 << stream;
                let bits = atomic_bits.fetch_or(stream_bit, Ordering::Relaxed);
                if bits & stream_bit == 0 {
                    let map_index = stream as usize + i * 32;
                    let node = &self.streams[map_index];
                    node.acquire();
                    return Some((map_index, node));
                }
                stream = bits.trailing_ones();
            }
        }
        None
    }

    fn release(&self, index: usize) {
        self.bitmap[index / 32].fetch_and(!(1 << (index % 32)), Ordering::Relaxed);
    }

    fn reset(&self) {
        let error = || io::Error::new(io::ErrorKind::BrokenPipe, "Connection has been closed");
        for (i, atomic_bits) in self.bitmap.iter().enumerate() {
            let bits = atomic_bits.load(Ordering::Relaxed);
            for b in 0..32 {
                if bits & b != 0 {
                    let index = i * 32 + b as usize;
                    if let Ok(true) = self.streams[index].set_response(Err(error().into())) {
                        self.release(index);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct StreamPool {
    map: StreamMap,
    lazy_maps: [OnceCell<StreamMap>; 10],
    orphans_limit: AtomicIsize,
    too_many_orphans: AtomicWaker,
}

impl StreamPool {
    pub(super) fn new(orphan_count_threshold: usize) -> Self {
        Self {
            map: StreamMap::new(32),
            lazy_maps: Default::default(),
            orphans_limit: AtomicIsize::new(cmp::min(
                orphan_count_threshold,
                CONNECTION_STREAM_UPPER_BOUND,
            ) as isize),
            too_many_orphans: AtomicWaker::new(),
        }
    }

    pub(super) async fn wait_until_too_many_orphan_streams(
        &self,
        orphan_count_threshold_delay: Duration,
    ) {
        loop {
            poll_fn(|cx| {
                self.too_many_orphans.register(cx.waker());
                if self.orphans_limit.load(Ordering::Relaxed) >= 0 {
                    return Poll::Pending;
                }
                Poll::Ready(())
            })
            .await;
            tokio::time::sleep(orphan_count_threshold_delay).await;
            if self.orphans_limit.load(Ordering::Relaxed) < 0 {
                return;
            }
        }
    }

    pub(super) fn allocate(&self) -> Option<Stream> {
        let stream = |value, node| Stream {
            value,
            pool: self,
            node,
        };
        if let Some((index, node)) = self.map.try_allocate() {
            return Some(stream(index as i16, node));
        }
        for (i, map) in self.lazy_maps.iter().enumerate() {
            if let Some((index, node)) = map
                .get_or_init(|| StreamMap::new(1 << (i + 5)))
                .try_allocate()
            {
                return Some(stream(index as i16 + (1 << (i + 5)), node));
            }
        }
        None
    }

    fn get_map_index(&self, stream: i16) -> (&StreamMap, usize) {
        if stream < 32 {
            return (&self.map, stream as usize);
        }
        let map_index = 10 - stream.leading_zeros() as usize;
        let map = self.lazy_maps[map_index].get().unwrap();
        (map, stream as usize - (1 << (map_index + 5)))
    }

    pub(super) fn set_response(
        &self,
        stream: i16,
        response: Result<Envelope, RequestError>,
    ) -> Result<(), InvalidStream> {
        let (map, index) = self.get_map_index(stream);
        if map.streams[index].set_response(response)? {
            self.orphans_limit.fetch_add(1, Ordering::Relaxed);
            map.release(index);
        }
        Ok(())
    }

    fn release(&self, stream: i16) {
        let (map, index) = self.get_map_index(stream);
        map.release(index);
    }

    pub(super) fn reset(&self) {
        for map in iter::once(&self.map).chain(self.lazy_maps.iter().filter_map(OnceCell::get)) {
            map.reset();
        }
        self.orphans_limit.store(0, Ordering::Relaxed);
    }
}
