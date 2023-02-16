use std::{
    collections::HashMap, convert::identity, fmt, io, mem, ops::Deref, sync::Arc, time::Duration,
};

use scylla2_cql::{
    error::{FrameTooBig, InvalidRequest, ReadLoopError},
    extensions::ProtocolExtensions,
    frame::{
        compression::Compression,
        envelope::{Envelope, EnvelopeHeader, ENVELOPE_HEADER_SIZE, ENVELOPE_MAX_LENGTH},
        FRAME_COMPRESSED_HEADER_SIZE, FRAME_TRAILER_SIZE,
    },
    protocol::read::read_envelope_loop,
    request::{Request, RequestExt},
    response::Response,
    ProtocolVersion,
};
use swap_buffer_queue::{
    error::EnqueueError, write::WriteVecBuffer, write_vectored::WriteVectoredVecBuffer,
    AsyncSBQueue, SBQueue,
};
use tokio::{io::ReadHalf, sync::oneshot};

use crate::{
    connection::{stream::StreamPool, tcp::TcpConnection},
    error::ConnectionExecutionError,
    topology::node::Node,
    utils::other_error,
};

pub mod config;
mod stream;
pub(crate) mod tcp;
mod write;

pub struct Connection {
    version: ProtocolVersion,
    extensions: ProtocolExtensions,
    compression: Option<Compression>,
    compression_min_size: usize,
    slice_queue: AsyncSBQueue<WriteVecBuffer<FRAME_COMPRESSED_HEADER_SIZE, FRAME_TRAILER_SIZE>>,
    vectored_queue: AsyncSBQueue<WriteVectoredVecBuffer<Vec<u8>>>,
    stream_pool: StreamPool,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Connection")
            .field("version", &self.version)
            .field("extensions", &self.extensions)
            .field("compression", &self.compression)
            .field("closed", &self.is_closed())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct OwnedConnection {
    node: Arc<Node>,
    index: usize,
}

impl Deref for OwnedConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.node.connections().unwrap()[self.index]
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionRef {
    node: Arc<Node>,
    index: usize,
}

impl ConnectionRef {
    pub(crate) fn new(node: Arc<Node>, index: usize) -> Self {
        Self { node, index }
    }

    pub(crate) fn get(&self) -> &Connection {
        &self.node.connections().unwrap()[self.index]
    }
}

impl Connection {
    pub(crate) fn new(
        version: ProtocolVersion,
        extensions: ProtocolExtensions,
        compression: Option<Compression>,
        compression_min_size: usize,
        buffer_size: usize,
        orphan_count_threshold: usize,
    ) -> Self {
        Self {
            version,
            extensions,
            compression,
            compression_min_size,
            slice_queue: SBQueue::with_capacity(buffer_size),
            vectored_queue: SBQueue::with_capacity(100),
            stream_pool: StreamPool::new(orphan_count_threshold),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.slice_queue.is_closed()
    }

    pub fn as_owned(&self, node: Arc<Node>) -> OwnedConnection {
        let connections = node.connections().expect("node has no connection");
        assert!(
            connections.as_ptr_range().contains(&(self as *const _)),
            "connection doesn't belong to the node"
        );
        let index =
            (self as *const _ as usize - connections.as_ptr() as usize) / mem::size_of::<Self>();
        OwnedConnection { node, index }
    }

    pub fn protocol_version(&self) -> ProtocolVersion {
        self.version
    }

    pub fn protocol_extensions(&self) -> ProtocolExtensions {
        self.extensions
    }

    pub async fn execute(
        &self,
        request: impl Request,
        tracing: bool,
        custom_payload: Option<&HashMap<String, Vec<u8>>>,
    ) -> Result<Response, ConnectionExecutionError> {
        request.check(self.version, self.extensions)?;
        let size = request
            .serialized_envelope_size(self.version, self.extensions, custom_payload)
            .map_err(InvalidRequest::from)?;
        let stream = self
            .stream_pool
            .allocate()
            .ok_or(ConnectionExecutionError::NoStreamAvailable)?;
        let check_size = |size| {
            if self.version == ProtocolVersion::V4 && size > ENVELOPE_MAX_LENGTH {
                // TODO why Err type is needed here
                return Err::<_, ConnectionExecutionError>(
                    InvalidRequest::FrameTooBig(FrameTooBig(size)).into(),
                );
            }
            Ok(())
        };
        if let Some(compression) = self
            .compression
            .filter(|_| self.version == ProtocolVersion::V4)
            .filter(|_| size >= self.compression_min_size)
        {
            let bytes = request.compress_envelope(
                self.version,
                self.extensions,
                compression,
                tracing,
                custom_payload,
                stream.get(),
                &mut vec![0; size],
            );
            check_size(bytes.len())?;
            match self.vectored_queue.enqueue(bytes).await {
                Ok(_) => {}
                Err(EnqueueError::Closed(_)) => {
                    return Err(ConnectionExecutionError::ConnectionClosed)
                }
                Err(EnqueueError::InsufficientCapacity(_)) => unreachable!(),
            }
        } else {
            check_size(size)?;
            let write = |slice: &mut [u8]| {
                request.serialize_envelope(
                    self.version,
                    self.extensions,
                    tracing,
                    custom_payload,
                    stream.get(),
                    slice,
                )
            };
            match self.slice_queue.enqueue((size, write)).await {
                Ok(_) => {}
                Err(EnqueueError::Closed(_)) => {
                    return Err(ConnectionExecutionError::ConnectionClosed)
                }
                Err(EnqueueError::InsufficientCapacity(_)) => {
                    let mut vec = vec![0; size];
                    write(&mut vec);
                    match self.vectored_queue.enqueue(vec).await {
                        Ok(_) => {}
                        Err(EnqueueError::Closed(_)) => {
                            return Err(ConnectionExecutionError::ConnectionClosed)
                        }
                        Err(EnqueueError::InsufficientCapacity(_)) => unreachable!(),
                    }
                }
            }
        };
        Ok(Response::deserialize(
            self.version,
            self.extensions,
            stream.wait_response().await?,
            self.compression,
        )?)
    }

    pub fn close(&self) {
        self.slice_queue.close();
        self.vectored_queue.close();
    }

    pub(crate) async fn task(
        &self,
        conn_ref: ConnectionRef,
        connection: TcpConnection,
        orphan_count_threshold_delay: Duration,
        stop: oneshot::Receiver<()>,
    ) -> Option<io::Error> {
        self.slice_queue.reopen();
        self.vectored_queue.reopen();
        let (reader, writer) = tokio::io::split(connection);
        let write_ref = conn_ref.clone();
        let write_task = tokio::spawn(async move { write_ref.get().write_task(writer).await });
        let read_ref = conn_ref.clone();
        let read_task = tokio::spawn(async move { read_ref.get().read_task(reader, stop).await });
        let orphan_ref = conn_ref.clone();
        let orphan_task = tokio::spawn(async move {
            orphan_ref
                .get()
                .orphan_task(orphan_count_threshold_delay)
                .await
        });
        let write_error = write_task
            .await
            .map_err(other_error)
            .and_then(identity)
            .err();
        let read_error = read_task
            .await
            .map_err(other_error)
            .and_then(identity)
            .err();
        orphan_task.abort();
        orphan_task.await.ok();
        self.stream_pool.reset();
        write_error.or(read_error)
    }

    async fn read_task(
        &self,
        mut reader: ReadHalf<TcpConnection>,
        stop: oneshot::Receiver<()>,
    ) -> io::Result<()> {
        let callback = |env: Envelope| {
            self.stream_pool
                .set_response(env.stream, Ok(env))
                .map_err(other_error)
        };
        let result = tokio::select! {
            res = read_envelope_loop(self.version, self.compression, &mut reader, callback) => res,
            _ = stop => Ok(())
        };
        self.close();
        result.map_err(ReadLoopError::into_inner)
    }

    async fn orphan_task(&self, orphan_count_threshold_delay: Duration) {
        self.stream_pool
            .wait_until_too_many_orphan_streams(orphan_count_threshold_delay)
            .await;
        self.close();
    }

    // TODO use it in writer_task
    #[allow(dead_code)]
    fn mark_streams_as_closed(&self, mut envelopes: &[u8]) {
        while !envelopes.is_empty() {
            let (header, remain) = envelopes.split_at(ENVELOPE_HEADER_SIZE);
            let header = EnvelopeHeader::deserialize(header.try_into().unwrap()).unwrap();
            self.stream_pool
                .set_response(
                    header.stream,
                    Err(ConnectionExecutionError::ConnectionClosed),
                )
                .ok();
            envelopes = &remain[header.length as usize..];
        }
    }
}
