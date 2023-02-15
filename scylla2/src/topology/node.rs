use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    num::NonZeroU16,
    str::FromStr,
    sync::{
        atomic::{AtomicIsize, Ordering},
        Arc,
    },
    time::Duration,
};

use once_cell::sync::OnceCell;
use rand::{seq::SliceRandom, thread_rng};
use scylla2_cql::{
    extensions::ProtocolExtensions, frame::compression::Compression,
    protocol::auth::AuthenticationProtocol, ProtocolVersion,
};
use tokio::sync::{mpsc, Notify};
use uuid::Uuid;

use crate::{
    connection::{
        config::{ConnectionConfig, InitSocket, ReconnectionPolicy},
        Connection, OwnedConnection,
    },
    error::{ConnectionExecutionError, Disconnected, ExecutionError},
    execution::peers_and_local,
    session::event::SessionEvent,
    statement::query::cql_query,
    topology::{
        partitioner::Token,
        peer::{NodeDistance, Peer, ShardAwarePort},
        sharding::Sharder,
    },
};

#[derive(Debug, Copy, Clone)]
pub enum PoolSize {
    PerHost(NonZeroU16),
    PerShard(NonZeroU16),
}

impl Default for PoolSize {
    fn default() -> Self {
        Self::PerShard(1.try_into().unwrap())
    }
}

pub(crate) struct NodeConfig {
    pub(crate) authentication_protocol: Option<Arc<dyn AuthenticationProtocol>>,
    pub(crate) buffer_size: usize,
    pub(crate) compression_min_size: usize,
    pub(crate) connect_timeout: Duration,
    pub(crate) init_socket: Box<dyn InitSocket>,
    pub(crate) pool_size: PoolSize,
    pub(crate) minimal_protocol_version: Option<ProtocolVersion>,
    pub(crate) orphan_count_threshold: usize,
    pub(crate) orphan_count_threshold_delay: Duration,
    pub(crate) reconnection_policy: Box<dyn ReconnectionPolicy>,
    #[cfg(feature = "ssl")]
    pub(crate) ssl_context: Option<openssl::ssl::SslContext>,
    pub(crate) startup_options: Arc<HashMap<String, String>>,
}

impl NodeConfig {
    fn compression(&self) -> Option<Compression> {
        self.startup_options
            .get("COMPRESSION")
            .and_then(|c| Compression::from_str(c).ok())
    }
}

#[derive(Debug)]
struct ConnectionPool {
    connections: Box<[Connection]>,
    sharder: Option<Sharder>,
}

impl ConnectionPool {
    fn new(
        pool_size: PoolSize,
        sharder: Option<Sharder>,
        connection: impl Fn() -> Connection,
    ) -> Self {
        let connection_count = match (pool_size, &sharder) {
            (PoolSize::PerHost(count), _) | (PoolSize::PerShard(count), None) => count.get(),
            (PoolSize::PerShard(count), Some(sharder)) => count.get() * sharder.nr_shards().get(),
        };
        Self {
            connections: (0..connection_count).map(|_| connection()).collect(),
            sharder,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NodeStatus {
    Up,
    Down,
    Disconnected(NodeDisconnectionReason),
}

const REMOVED: isize = -1;
const PEER_CHANGED: isize = -2;
const TRANSLATED_ADDRESS_CHANGED: isize = -3;
const DISTANCE_CHANGED: isize = -4;
const SHARDING_CHANGED: isize = -5;
const SESSION_CLOSED: isize = -6;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NodeDisconnectionReason {
    Removed,
    PeerChanged,
    TranslatedAddressChanged,
    DistanceChanged,
    ShardingChanged,
    SessionClosed,
}

impl From<NodeDisconnectionReason> for NodeStatus {
    fn from(value: NodeDisconnectionReason) -> Self {
        NodeStatus::Disconnected(value)
    }
}

#[derive(Debug)]
pub struct Node {
    peer: Peer,
    address: SocketAddr,
    shard_aware_port: Option<ShardAwarePort>,
    distance: NodeDistance,
    active_connection_count: AtomicIsize, // negative means disconnected
    connection_pool: OnceCell<ConnectionPool>,
    status_notify: Notify,
    session_events: mpsc::UnboundedSender<SessionEvent>,
    connection_tx: mpsc::UnboundedSender<Option<NodeDisconnectionReason>>,
    schema_agreement_interval: Duration,
}

impl Node {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        peer: Peer,
        address: SocketAddr,
        shard_aware_port: Option<ShardAwarePort>,
        distance: NodeDistance,
        config: Option<Arc<NodeConfig>>,
        used_keyspace: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
        session_events: mpsc::UnboundedSender<SessionEvent>,
        schema_agreement_interval: Duration,
    ) -> Arc<Self> {
        let (connection_tx, connection_rx) = mpsc::unbounded_channel();
        let node = Arc::new(Self {
            peer,
            address,
            shard_aware_port,
            distance,
            active_connection_count: AtomicIsize::new(0),
            connection_pool: OnceCell::new(),
            status_notify: Notify::new(),
            session_events,
            connection_tx,
            schema_agreement_interval,
        });
        assert_eq!(matches!(distance, NodeDistance::Ignored), config.is_none());
        if let Some(config) = config {
            let worker = node.clone().worker(config, used_keyspace, connection_rx);
            tokio::spawn(worker);
        }
        node
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn shard_aware_port(&self) -> Option<ShardAwarePort> {
        self.shard_aware_port
    }

    pub fn peer(&self) -> &Peer {
        &self.peer
    }

    pub fn distance(&self) -> NodeDistance {
        self.distance
    }

    pub fn protocol_version(&self) -> Option<ProtocolVersion> {
        Some(self.connections()?[0].protocol_version())
    }

    pub fn protocol_extensions(&self) -> Option<ProtocolExtensions> {
        Some(self.connections()?[0].protocol_extensions())
    }

    pub fn status(&self) -> NodeStatus {
        match self.active_connection_count.load(Ordering::Relaxed) {
            n if n > 0 => NodeStatus::Up,
            0 => NodeStatus::Down,
            REMOVED => NodeDisconnectionReason::Removed.into(),
            PEER_CHANGED => NodeDisconnectionReason::PeerChanged.into(),
            TRANSLATED_ADDRESS_CHANGED => NodeDisconnectionReason::TranslatedAddressChanged.into(),
            DISTANCE_CHANGED => NodeDisconnectionReason::DistanceChanged.into(),
            SHARDING_CHANGED => NodeDisconnectionReason::ShardingChanged.into(),
            SESSION_CLOSED => NodeDisconnectionReason::SessionClosed.into(),
            _ => unreachable!(),
        }
    }

    pub fn is_up(&self) -> bool {
        matches!(self.status(), NodeStatus::Up)
    }

    pub async fn wait_status(&self) {
        self.status_notify.notified().await
    }

    pub fn sharder(&self) -> Option<&Sharder> {
        self.connection_pool.get()?.sharder.as_ref()
    }

    pub fn connections(&self) -> Option<&[Connection]> {
        Some(&self.connection_pool.get()?.connections)
    }

    pub fn get_random_connection(&self) -> Option<&Connection> {
        self.connections()?.choose(&mut thread_rng())
    }

    pub fn get_random_owned_connection(self: &Arc<Self>) -> Option<OwnedConnection> {
        Some(self.get_random_connection()?.as_owned(self.clone()))
    }

    pub fn get_sharded_connections(&self, token: Option<Token>) -> Option<&[Connection]> {
        let pool = self.connection_pool.get()?;
        let (Some(sharder), Some(token)) = (&pool.sharder, token) else {
          return Some(&pool.connections)
        };
        let shard = sharder.compute_shard(token);
        let nb_conn_per_shard = pool.connections.len() / sharder.nr_shards().get() as usize;
        let offset = shard as usize * nb_conn_per_shard;
        Some(&pool.connections[offset..offset + nb_conn_per_shard])
    }

    pub fn is_disconnected(&self) -> bool {
        self.active_connection_count.load(Ordering::Relaxed) < 0
    }

    pub fn try_reconnect(&self) -> Result<(), Disconnected> {
        if !self.is_up() {
            self.connection_tx.send(None).map_err(|_| Disconnected)?;
        }
        Ok(())
    }

    pub async fn check_schema_agreement(&self) -> Result<Option<Uuid>, ExecutionError> {
        let Some(conn )= self.get_random_connection() else {
            return Ok(None)
        };
        let local_query = cql_query(
            "SELECT schema_version FROM system.local WHERE key = 'local'",
            (),
        );
        let peers_query = cql_query("SELECT schema_version FROM system.peers", ());
        let (local, peers) = match tokio::try_join!(
            conn.execute(local_query, false, None),
            conn.execute(peers_query, false, None),
        ) {
            Ok(res) => res,
            Err(
                ConnectionExecutionError::ConnectionClosed
                | ConnectionExecutionError::NoStreamAvailable,
            ) => return Ok(None),
            Err(ConnectionExecutionError::InvalidRequest(invalid)) => return Err(invalid.into()),
            Err(ConnectionExecutionError::Io(err)) => return Err(err.into()),
        };
        let schema_versions: HashSet<Uuid> =
            peers_and_local(peers.ok()?, local.ok()?, |(uuid,)| uuid)?;
        Ok(if schema_versions.len() == 1 {
            let schema_version = schema_versions.into_iter().next().unwrap();
            let event = SessionEvent::SchemaAgreement {
                schema_version,
                address: self.address,
            };
            self.session_events.send(event).ok();
            Some(schema_version)
        } else {
            None
        })
    }

    pub async fn wait_schema_agreement(&self) -> Uuid {
        loop {
            if let Ok(Some(schema_version)) = self.check_schema_agreement().await {
                return schema_version;
            }
            tokio::time::sleep(self.schema_agreement_interval).await;
        }
    }

    pub(crate) fn disconnect(&self, reason: NodeDisconnectionReason) {
        self.connection_tx.send(Some(reason)).ok();
    }

    pub(crate) async fn worker(
        self: Arc<Self>,
        config: Arc<NodeConfig>,
        used_keyspace: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
        connection_rx: mpsc::UnboundedReceiver<Option<NodeDisconnectionReason>>,
    ) {
        todo!()
    }
}

impl Node {
    // pub(crate) async fn connect(
    //     self: Arc<Self>,
    //     config: Arc<NodeConfig>,
    //     used_keyspace: Arc<tokio::sync::RwLock<Option<String>>>,
    //     session_events: broadcast::Sender<(SessionEvent, Instant)>,
    //     connection_rx: mpsc::Receiver<bool>,
    //     reconnect: mpsc::Sender<Arc<Node>>,
    // ) {
    //     let mut reconnection_delays =
    //         ReconnectionDelays::new(config.reconnection_policy.reconnection_delays());
    //     let (version, supported) = loop {
    //         match self
    //             .get_supported(
    //                 config.connect_timeout,
    //                 config.minimal_protocol_version,
    //                 #[cfg(feature = "ssl")]
    //                 config.ssl_context.as_ref(),
    //             )
    //             .await
    //         {
    //             Ok(supported) => break supported,
    //             Err(error) => {
    //                 #[cfg(feature = "tracing")]
    //                 tracing::warn!(
    //                     address = self.address,
    //                     error,
    //                     "Initial node connection failed"
    //                 );
    //             }
    //         }
    //         tokio::select! {
    //             Ok(true) = connection_rx.recv() => {}
    //             _ = tokio::time::sleep(reconnection_delays.next_delay()) => {}
    //             else => return,
    //         }
    //     };
    //     let extensions = ProtocolExtensions::from_supported(&supported);
    //     let sharder = Sharder::from_supported(&supported);
    //     #[allow(unused_mut)]
    //     let mut shard_aware_port: Option<u16> = supported.get("SCYLLA_SHARD_AWARE_PORT");
    //     #[cfg(feature = "ssl")]
    //     if config.ssl_context.is_some() {
    //         shard_aware_port = supported.get("SCYLLA_SHARD_AWARE_PORT")
    //     }
    //     let pool = ConnectionPool::new(config.pool_size, sharder, || {
    //         Connection::new(
    //             version,
    //             extensions,
    //             config.compression(),
    //             config.compression_min_size,
    //             config.buffer_size,
    //             config.orphan_count_threshold,
    //         )
    //     });
    //     let pool = self.connection_pool.try_insert(pool).ok();
    // }
}
