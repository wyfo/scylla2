use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    convert::identity,
    mem,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::DerefMut,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use scylla2_cql::{
    error::{ConnectionError, DatabaseError, DatabaseErrorKind, ReadLoopError},
    event::{Event, EventType},
    frame::envelope::Envelope,
    protocol::{
        read::{read_envelope, read_envelope_loop},
        startup,
        write::write_envelope,
    },
    request::{query::values::QueryValues, register::Register, Request, RequestExt},
    response::Response,
    ProtocolVersion,
};
use tokio::{
    io,
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, oneshot},
};
use uuid::Uuid;

use crate::{
    connection::tcp::TcpConnection,
    error::ExecutionError,
    execution::{maybe_cql_row, peers_and_local},
    statement::query::cql_query,
    topology::{node::NodeConfig, peer::Peer},
    utils::other_error,
    SessionEvent,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ControlError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Database error: {0}")]
    Database(#[from] Box<DatabaseError>), // Boxed because larger than 128 bytes
}

impl From<ControlError> for ConnectionError {
    fn from(value: ControlError) -> Self {
        match value {
            ControlError::Io(err) => Self::Io(err),
            ControlError::Database(err) => Self::Database(err),
        }
    }
}

impl From<ControlError> for ExecutionError {
    fn from(value: ControlError) -> Self {
        match value {
            ControlError::Io(err) => Self::Io(err),
            ControlError::Database(err) => Self::Database(err),
        }
    }
}

pub(crate) struct ControlConnection {
    rpc_address: IpAddr,
    version: ProtocolVersion,
    stream_generator: AtomicUsize,
    streams: Arc<Mutex<HashMap<i16, oneshot::Sender<io::Result<Envelope>>>>>,
    writer: tokio::sync::Mutex<Box<dyn AsyncWrite + Unpin + Send>>,
    session_events: mpsc::UnboundedSender<SessionEvent>,
    stop_tx: Mutex<Option<oneshot::Sender<io::Result<()>>>>,
}

pub(crate) enum ControlAddr {
    Config(SocketAddr),
    Peer(IpAddr),
}

impl ControlConnection {
    pub(crate) async fn open(
        address: ControlAddr,
        config: &NodeConfig,
        register_for_schema_event: bool,
        database_events: mpsc::UnboundedSender<Event>,
        session_events: mpsc::UnboundedSender<SessionEvent>,
    ) -> Result<Self, ConnectionError> {
        let address = match address {
            ControlAddr::Peer(peer) => match config.address_translator.translate(peer).await {
                Ok((addr, _)) => addr,
                Err(error) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(address = %peer, error, "Address translation failed");
                    let error_str = format!("Address translation failed: {error}");
                    let event = SessionEvent::AddressTranslationFailed {
                        rpc_address: peer,
                        error: Arc::new(error),
                    };
                    session_events.send(event).ok();
                    return Err(other_error(error_str).into());
                }
            },
            ControlAddr::Config(addr) => addr,
        };
        let (mut conn, version, _) = TcpConnection::open_with_minimal_version(
            address,
            None,
            config.init_socket.as_ref(),
            #[cfg(feature = "ssl")]
            config.ssl_context.as_ref(),
            config.connect_timeout,
            config.minimal_protocol_version,
        )
        .await?;
        startup(
            &mut conn,
            address,
            version,
            &config.startup_options,
            config.authentication_protocol.as_deref(),
        )
        .await?;
        let rpc_addr_envelope = cql_query(
            "SELECT rpc_address FROM system.local WHERE key = 'local'",
            (),
        )
        .serialize_envelope_owned(version, Default::default(), false, None, 0)
        .map_err(other_error)?;
        write_envelope(version, false, &rpc_addr_envelope, &mut conn).await?;
        let rpc_addr_response = Response::deserialize(
            version,
            Default::default(),
            read_envelope(version, None, &mut conn).await?,
            None,
        )?;
        let rpc_address = maybe_cql_row::<(_,)>(rpc_addr_response)?
            .map(|(ip,)| ip)
            .ok_or_else(|| other_error("Cannot request rpc_address"))?;
        // TODO handle "0.0.0.0" bug
        // (see https://github.com/scylladb/scylla-rust-driver/issues/640)
        // The issue with the official driver solution is that it prevent the already translated
        // to be translated again (for serverless for example)
        // I would prefer open a connection to another node and compare the peers obtained to
        // deduce the real rpc_address
        let (reader, writer) = tokio::io::split(conn);
        let (stop_tx, stop_rx) = oneshot::channel();
        let streams: Arc<Mutex<HashMap<i16, oneshot::Sender<io::Result<Envelope>>>>> =
            Default::default();
        tokio::spawn(read_task(
            rpc_address,
            version,
            reader,
            database_events,
            session_events.clone(),
            streams.clone(),
            stop_rx,
        ));
        let connection = Self {
            rpc_address,
            version,
            stream_generator: AtomicUsize::new(0),
            streams,
            writer: tokio::sync::Mutex::new(Box::new(writer)),
            session_events,
            stop_tx: Mutex::new(Some(stop_tx)),
        };
        let event_types = [
            EventType::TopologyChange,
            EventType::StatusChange,
            EventType::SchemaChange,
        ];
        let event_types = if register_for_schema_event {
            &event_types[..]
        } else {
            &event_types[..2]
        };
        connection.request(Register { event_types }).await?;
        let event = SessionEvent::ControlConnectionOpened { rpc_address };
        connection.session_events.send(event).ok();
        Ok(connection)
    }

    async fn request(&self, request: impl Request) -> Result<Response, ControlError> {
        let closed = || io::Error::new(io::ErrorKind::BrokenPipe, "Connection closed");
        let (stream, rx) = loop {
            let stream = (self.stream_generator.fetch_add(1, Ordering::Relaxed) % (1 << 15)) as i16;
            let mut streams = self.streams.lock().unwrap();
            if let Entry::Vacant(e) = streams.entry(stream) {
                let (tx, rx) = oneshot::channel();
                e.insert(tx);
                break (stream, rx);
            }
        };
        let bytes = request
            .serialize_envelope_owned(self.version, Default::default(), false, None, stream)
            .map_err(other_error)?;
        write_envelope(
            self.version,
            false,
            &bytes,
            self.writer.lock().await.deref_mut(),
        )
        .await?;
        let envelope = rx.await.map_err(|_| closed())??;
        let response = Response::deserialize(self.version, Default::default(), envelope, None)?;
        Ok(response.ok()?)
    }

    pub async fn query(
        &self,
        query: &str,
        values: impl QueryValues,
    ) -> Result<Response, ControlError> {
        let res = self.request(cql_query(query, values)).await;
        if let Err(ControlError::Io(ref error)) = res {
            if let Some(stop_tx) = self.stop_tx.lock().unwrap().take() {
                stop_tx
                    .send(Err(io::Error::new(error.kind(), error.to_string())))
                    .ok();
            }
        }
        res
    }

    pub(crate) async fn send_heartbeat(&self) -> Result<(), ControlError> {
        self.query("SELECT key FROM system.local where key = 'local'", ())
            .await?;
        Ok(())
    }

    pub(crate) async fn get_peers(&self) -> Result<Vec<Peer>, ControlError> {
        let (local, peers) = tokio::try_join!(
            self.query(
                "SELECT rpc_address, data_center, rack, tokens FROM system.local WHERE key = 'local'", ()
            ),
            self.query("SELECT rpc_address, data_center, rack, tokens FROM system.peers", ()),
        )?;
        // TODO handle "0.0.0.0" bug here too
        Ok(peers_and_local(peers, local, identity)?)
    }

    pub(crate) async fn check_schema_agreement(&self) -> Result<Option<Uuid>, ControlError> {
        let (local, peers) = tokio::try_join!(
            self.query(
                "SELECT schema_version FROM system.local WHERE key = 'local'",
                ()
            ),
            self.query("SELECT schema_version FROM system.peers", ()),
        )?;
        let schema_versions: HashSet<Uuid> = peers_and_local(peers, local, |(uuid,)| uuid)?;
        Ok(if schema_versions.len() == 1 {
            let schema_version = schema_versions.into_iter().next().unwrap();
            let event = SessionEvent::SchemaAgreement {
                schema_version,
                rpc_address: self.rpc_address,
            };
            self.session_events.send(event).ok();
            Some(schema_version)
        } else {
            None
        })
    }

    pub(crate) async fn get_partitioner(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<Option<String>, ControlError> {
        match self.query("SELECT partitioner FROM system_schema.scylla_tables WHERE keyspace = ? AND table = ?", (keyspace, table)).await {
            Ok(res) => {
                Ok(maybe_cql_row(res)?.map(|(p, )|p))
            }
            Err(ControlError::Database(err)) if matches!(err.kind, DatabaseErrorKind::Invalid) => Ok(None),
            Err(err) => Err(err)
        }
    }

    pub(crate) async fn get_replication(
        &self,
        keyspace: &str,
    ) -> Result<Option<HashMap<String, String>>, ControlError> {
        let query = "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?";
        let replication = self.query(query, (keyspace,)).await?;
        Ok(maybe_cql_row(replication)?.map(|(r,)| r))
    }
}

impl Drop for ControlConnection {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.lock().unwrap().take() {
            stop_tx.send(Ok(())).ok();
        }
    }
}

async fn read_task(
    rpc_address: IpAddr,
    version: ProtocolVersion,
    reader: impl AsyncRead + Unpin + Send + 'static,
    events: mpsc::UnboundedSender<Event>,
    session_events: mpsc::UnboundedSender<SessionEvent>,
    streams: Arc<Mutex<HashMap<i16, oneshot::Sender<io::Result<Envelope>>>>>,
    stop_rx: oneshot::Receiver<io::Result<()>>,
) {
    let callback = |envelope: Envelope| -> io::Result<()> {
        if envelope.stream == -1 {
            events
                .send(Event::deserialize(
                    version,
                    Default::default(),
                    &envelope.body,
                )?)
                .ok();
        } else {
            let mut streams = streams.lock().unwrap();
            if let Some(tx) = streams.remove(&envelope.stream) {
                tx.send(Ok(envelope)).ok();
            } else {
                return Err(io::Error::new(io::ErrorKind::Other, "Unexpected stream"));
            }
        };
        Ok(())
    };
    let result: Result<(), ReadLoopError<io::Error>> = tokio::select! {
        biased;
        res = stop_rx => res.unwrap_or(Ok(())).map_err(Into::into),
        res = read_envelope_loop(version, None, reader, callback) => res,
    };
    if let Err(error) = result {
        let error = error.into_inner();
        let mut streams = streams.lock().unwrap();
        for tx in mem::take(streams.deref_mut()).into_values() {
            tx.send(Err(io::Error::new(error.kind(), error.to_string())))
                .ok();
        }
        let event = SessionEvent::ControlConnectionClosed {
            rpc_address,
            error: Some(Arc::new(io::Error::new(error.kind(), error.to_string()))),
        };
        session_events.send(event).ok();
    }
}
