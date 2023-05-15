use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    convert::identity,
    net::{IpAddr, SocketAddr},
    ops::DerefMut,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use scylla2_cql::{
    error::{ConnectionError, DatabaseErrorKind, ReadLoopError},
    event::{Event, EventType},
    frame::envelope::Envelope,
    protocol::{execute, read::read_envelope_loop, startup, write::write_envelope},
    request::{query::values::QueryValues, register::Register, Request, RequestExt},
    response::Response,
    ProtocolVersion,
};
use tokio::{
    io,
    io::{AsyncWriteExt, ReadHalf, WriteHalf},
    sync::{mpsc, oneshot},
};
use uuid::Uuid;

use crate::{
    connection::tcp::TcpConnection,
    error::ExecutionError,
    event::SessionEventHandler,
    execution::utils::{cql_query, maybe_cql_row, peers_and_local},
    topology::{node::NodeConfig, peer::Peer},
    utils::other_error,
};

struct Connection {
    rpc_address: IpAddr,
    version: ProtocolVersion,
    writer: WriteHalf<TcpConnection>,
}

type StreamMap = HashMap<i16, oneshot::Sender<io::Result<Envelope>>>;

pub(crate) struct ControlConnection {
    connection: tokio::sync::Mutex<Option<Connection>>,
    stream_generator: AtomicUsize,
    streams: Arc<Mutex<Option<StreamMap>>>,
    session_event_handler: Option<Arc<dyn SessionEventHandler>>,
}

impl ControlConnection {
    pub(crate) fn new(session_event_handler: Option<Arc<dyn SessionEventHandler>>) -> Self {
        Self {
            connection: Default::default(),
            stream_generator: Default::default(),
            streams: Arc::new(Mutex::new(Some(StreamMap::default()))),
            session_event_handler,
        }
    }

    // Failure doesn't trigger `SessionEventHandler::control_connection_failed` because
    // because it's also used at session initialization where there is no such event
    pub(crate) async fn open(
        &self,
        rpc_address: Option<IpAddr>,
        address: SocketAddr,
        config: &NodeConfig,
        register_for_schema_event: bool,
        events: mpsc::UnboundedSender<Event>,
    ) -> Result<(), ConnectionError> {
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
        let rpc_address = if let Some(addr) = rpc_address {
            addr
        } else {
            let rpc_addr_response = execute(
                &mut conn,
                version,
                None,
                cql_query(
                    "SELECT rpc_address FROM system.local WHERE key = 'local'",
                    (),
                ),
            )
            .await?;
            maybe_cql_row::<(_,)>(rpc_addr_response)?
                .ok_or_else(|| other_error("Cannot request rpc_address"))?
                .0
        };
        // (see https://github.com/scylladb/scylla-rust-driver/issues/640)
        assert!(
            !rpc_address.is_unspecified(),
            "Node version is bugged and outdated, consider upgrading it"
        );
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
        execute(&mut conn, version, None, Register { event_types }).await?;
        let (reader, writer) = tokio::io::split(conn);
        *self.connection.lock().await = Some(Connection {
            rpc_address,
            version,
            writer,
        });
        let event_handler = self.session_event_handler.clone();
        tokio::spawn(read_task(
            version,
            self.streams.clone(),
            reader,
            events,
            move |error| event_handler.control_connection_closed(rpc_address, address, Some(error)),
        ));
        self.session_event_handler
            .control_connection_opened(rpc_address, address);
        Ok(())
    }

    async fn connection(&self) -> impl DerefMut<Target = Connection> + '_ {
        tokio::sync::MutexGuard::map(self.connection.lock().await, |guard| {
            guard.as_mut().unwrap()
        })
    }

    async fn request(&self, request: impl Request) -> Result<Response, ExecutionError> {
        let (stream, rx) = loop {
            let stream = (self.stream_generator.fetch_add(1, Ordering::Relaxed) % (1 << 15)) as i16;
            let mut guard = self.streams.lock().unwrap();
            let streams = guard
                .as_mut()
                .ok_or_else(|| other_error("Control connection closed"))?;
            if let Entry::Vacant(e) = streams.entry(stream) {
                let (tx, rx) = oneshot::channel();
                e.insert(tx);
                break (stream, rx);
            }
        };
        let mut connection = self.connection().await;
        let version = connection.version;
        let bytes = request
            .serialize_envelope_owned(version, Default::default(), false, None, stream)
            .map_err(other_error)?;
        write_envelope(version, false, &bytes, &mut connection.writer).await?;
        // drop connection to release the mutex
        drop(connection);
        let envelope = rx
            .await
            .map_err(|_| other_error("Control connection closed"))??;
        let response = Response::deserialize(version, Default::default(), envelope, None)?;
        Ok(response.ok()?)
    }

    pub async fn query(
        &self,
        query: &str,
        values: impl QueryValues,
    ) -> Result<Response, ExecutionError> {
        let result = self.request(cql_query(query, values)).await;
        if matches!(result, Err(ExecutionError::Io(_))) {
            self.connection().await.writer.shutdown().await.ok();
            self.streams.lock().unwrap().take();
        }
        result
    }

    pub(crate) async fn send_heartbeat(&self) -> Result<(), ExecutionError> {
        self.query("SELECT key FROM system.local where key = 'local'", ())
            .await?;
        Ok(())
    }

    pub(crate) async fn get_peers(&self) -> Result<Vec<Peer>, ExecutionError> {
        let (local, peers) = tokio::try_join!(
            self.query(
                "SELECT rpc_address, data_center, rack, tokens FROM system.local WHERE key = 'local'", ()
            ),
            self.query("SELECT rpc_address, data_center, rack, tokens FROM system.peers", ()),
        )?;
        Ok(peers_and_local(peers, local, identity)?)
    }

    pub(crate) async fn check_schema_agreement(&self) -> Result<Option<Uuid>, ExecutionError> {
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
            self.session_event_handler
                .schema_agreement(schema_version, self.connection().await.rpc_address);
            Some(schema_version)
        } else {
            None
        })
    }

    pub(crate) async fn get_partitioner(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<Option<String>, ExecutionError> {
        match self.query("SELECT partitioner FROM system_schema.scylla_tables WHERE keyspace_name = ? AND table_name = ?", (keyspace, table)).await {
            Ok(res) => {
                Ok(maybe_cql_row(res)?.map(|(p, )|p))
            }
            // Cassandra doesn't have `scylla_tables` table (obviously)
            Err(ExecutionError::Database(err)) if matches!(err.kind, DatabaseErrorKind::Invalid) => Ok(None),
            Err(err) => Err(err)
        }
    }

    pub(crate) async fn get_replication(
        &self,
        keyspace: &str,
    ) -> Result<Option<HashMap<String, String>>, ExecutionError> {
        let query = "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?";
        let replication = self.query(query, (keyspace,)).await?;
        Ok(maybe_cql_row(replication)?.map(|(r,)| r))
    }
}

impl Drop for ControlConnection {
    fn drop(&mut self) {
        if let Some(mut conn) = self.connection.get_mut().take() {
            tokio::spawn(async move { conn.writer.shutdown().await.ok() });
        }
    }
}

async fn read_task(
    version: ProtocolVersion,
    streams: Arc<Mutex<Option<StreamMap>>>,
    reader: ReadHalf<TcpConnection>,
    events: mpsc::UnboundedSender<Event>,
    closed_event: impl Fn(io::Error),
) {
    let callback = |envelope: Envelope| -> io::Result<()> {
        if envelope.stream == -1 {
            let event = Event::deserialize(version, Default::default(), &envelope.body)?;
            events.send(event).ok();
        } else {
            let mut guard = streams.lock().unwrap();
            let streams = guard
                .as_mut()
                .ok_or_else(|| other_error("Control connection closed"))?;
            if let Some(tx) = streams.remove(&envelope.stream) {
                tx.send(Ok(envelope)).ok();
            } else {
                return Err(other_error("Unexpected control stream"));
            }
        };
        Ok(())
    };
    if let Err(ReadLoopError::Io(err) | ReadLoopError::Callback(err)) =
        read_envelope_loop(version, None, reader, callback).await
    {
        streams.lock().unwrap().take();
        closed_event(err);
    }
}
