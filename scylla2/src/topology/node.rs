use std::{
    collections::{HashMap, HashSet},
    fmt,
    num::NonZeroUsize,
    str::FromStr,
    sync::{
        atomic::{AtomicIsize, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use once_cell::sync::OnceCell;
use rand::seq::SliceRandom;
use scylla2_cql::{
    extensions::ProtocolExtensions, frame::compression::Compression,
    protocol::auth::AuthenticationProtocol, ProtocolVersion,
};
use tokio::sync::{mpsc, Notify};
use uuid::Uuid;

use crate::{
    connection::{
        config::{InitSocket, ReconnectionPolicy},
        tcp::TcpConnection,
        Connection, OwnedConnection,
    },
    error::{ConnectionExecutionError, Disconnected, ExecutionError},
    execution::peers_and_local,
    session::event::SessionEvent,
    statement::query::cql_query,
    topology::{
        node::worker::NodeWorker,
        partitioner::Token,
        peer::{AddressTranslator, AddressTranslatorExt, NodeDistance, Peer},
        sharding::Sharder,
    },
    utils::RepeatLast,
};

mod worker;

#[derive(Debug, Copy, Clone)]
pub enum PoolSize {
    PerHost(NonZeroUsize),
    PerShard(NonZeroUsize),
}

impl Default for PoolSize {
    fn default() -> Self {
        Self::PerShard(1.try_into().unwrap())
    }
}

pub(crate) struct NodeConfig {
    pub(crate) address_translator: Arc<dyn AddressTranslator>,
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
        version: ProtocolVersion,
        extensions: ProtocolExtensions,
        config: &NodeConfig,
    ) -> Self {
        let extensions = Arc::new(extensions);
        let connection_count = match (pool_size, &sharder) {
            (PoolSize::PerHost(count), _) | (PoolSize::PerShard(count), None) => count.get(),
            (PoolSize::PerShard(count), Some(sharder)) => {
                count.get() * sharder.nr_shards().get() as usize
            }
        };
        let connections = (0..connection_count)
            .map(|_| {
                Connection::new(
                    version,
                    extensions.clone(),
                    config.compression(),
                    config.compression_min_size,
                    config.buffer_size,
                    config.orphan_count_threshold,
                )
            })
            .collect();
        Self {
            connections,
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
const DISTANCE_CHANGED: isize = -3;
const SHARDING_CHANGED: isize = -4;
const EXTENSIONS_CHANGED: isize = -5;
const SESSION_CLOSED: isize = -6;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NodeDisconnectionReason {
    Removed,
    PeerChanged,
    DistanceChanged,
    ShardingChanged,
    ExtensionsChanged,
    SessionClosed,
}

impl From<NodeDisconnectionReason> for NodeStatus {
    fn from(value: NodeDisconnectionReason) -> Self {
        NodeStatus::Disconnected(value)
    }
}

pub struct Node {
    peer: Peer,
    distance: NodeDistance,
    active_connection_count: AtomicIsize, // negative means disconnected
    connection_pool: OnceCell<ConnectionPool>,
    status_notify: Notify,
    session_events: mpsc::UnboundedSender<SessionEvent>,
    connection_tx: mpsc::UnboundedSender<Option<NodeDisconnectionReason>>,
    schema_agreement_interval: Duration,
    round_robin: AtomicUsize,
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Node")
            .field("peer", &self.peer)
            .field("distance", &self.distance)
            .field("status", &self.status())
            .field("connections", &self.connections())
            .field("sharder", &self.sharder())
            .field("round_robin", &self.round_robin)
            .finish()
    }
}

impl Node {
    pub(crate) fn new(
        peer: Peer,
        distance: NodeDistance,
        config: Option<Arc<NodeConfig>>,
        used_keyspace: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
        session_events: mpsc::UnboundedSender<SessionEvent>,
        schema_agreement_interval: Duration,
    ) -> Arc<Self> {
        let (connection_tx, connection_rx) = mpsc::unbounded_channel();
        let node = Arc::new(Self {
            peer,
            distance,
            active_connection_count: AtomicIsize::new(0),
            connection_pool: OnceCell::new(),
            status_notify: Notify::new(),
            session_events,
            connection_tx,
            schema_agreement_interval,
            round_robin: AtomicUsize::new(0),
        });
        assert_eq!(matches!(distance, NodeDistance::Ignored), config.is_none());
        if let Some(config) = config {
            let worker = node.clone().worker(config, used_keyspace, connection_rx);
            tokio::spawn(worker);
        }
        node
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

    pub fn protocol_extensions(&self) -> Option<&ProtocolExtensions> {
        Some(self.connections()?[0].protocol_extensions())
    }

    pub fn status(&self) -> NodeStatus {
        match self.active_connection_count.load(Ordering::Relaxed) {
            n if n > 0 => NodeStatus::Up,
            0 => NodeStatus::Down,
            REMOVED => NodeDisconnectionReason::Removed.into(),
            PEER_CHANGED => NodeDisconnectionReason::PeerChanged.into(),
            DISTANCE_CHANGED => NodeDisconnectionReason::DistanceChanged.into(),
            SHARDING_CHANGED => NodeDisconnectionReason::ShardingChanged.into(),
            EXTENSIONS_CHANGED => NodeDisconnectionReason::ExtensionsChanged.into(),
            SESSION_CLOSED => NodeDisconnectionReason::SessionClosed.into(),
            _ => unreachable!(),
        }
    }

    pub fn is_up(&self) -> bool {
        matches!(self.status(), NodeStatus::Up)
    }

    pub async fn wait_status(&self) {
        self.status_notify.notified().await;
    }

    pub fn sharder(&self) -> Option<&Sharder> {
        self.connection_pool.get()?.sharder.as_ref()
    }

    pub fn connections(&self) -> Option<&[Connection]> {
        Some(&self.connection_pool.get()?.connections)
    }

    pub fn get_random_connection(&self) -> Option<&Connection> {
        self.connections()?.choose(&mut rand::thread_rng())
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
        let conn_per_shard = pool.connections.len() / sharder.nr_shards().get() as usize;
        let offset = shard as usize * conn_per_shard;
        Some(&pool.connections[offset..offset + conn_per_shard])
    }

    pub fn round_robin(&self) -> &AtomicUsize {
        &self.round_robin
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
                rpc_address: self.peer.rpc_address,
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

    fn acknowledge_disconnection(self: Arc<Self>, reason: NodeDisconnectionReason) {
        let disconnected = match reason {
            NodeDisconnectionReason::Removed => REMOVED,
            NodeDisconnectionReason::PeerChanged => PEER_CHANGED,
            NodeDisconnectionReason::DistanceChanged => DISTANCE_CHANGED,
            NodeDisconnectionReason::ShardingChanged => SHARDING_CHANGED,
            NodeDisconnectionReason::ExtensionsChanged => EXTENSIONS_CHANGED,
            NodeDisconnectionReason::SessionClosed => SESSION_CLOSED,
        };
        self.active_connection_count
            .store(disconnected, Ordering::Relaxed);
        let event = SessionEvent::NodeStatusUpdate {
            node: self.clone(),
            status: NodeStatus::Disconnected(reason),
        };
        self.session_events.send(event).ok();
    }

    fn update_active_connection_count(&self, update: isize) {
        assert!(update == 1 || update == -1);
        let mut conn_count = self.active_connection_count.load(Ordering::Relaxed);
        while (update == 1 && conn_count >= 0) || (update == -1 && conn_count > 1) {
            match self.active_connection_count.compare_exchange_weak(
                conn_count,
                conn_count + update,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(c) => conn_count = c,
            }
        }
    }

    pub(crate) async fn worker(
        self: Arc<Self>,
        config: Arc<NodeConfig>,
        used_keyspace: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
        mut connection_rx: mpsc::UnboundedReceiver<Option<NodeDisconnectionReason>>,
    ) {
        for delay in RepeatLast::new(config.reconnection_policy.reconnection_delays()) {
            let translated = config
                .address_translator
                .translate_or_warn(self.peer.rpc_address, &self.session_events)
                .await;
            if let Ok((address, shard_aware_port)) = translated {
                match TcpConnection::open_with_minimal_version(
                    address,
                    None,
                    config.init_socket.as_ref(),
                    #[cfg(feature = "ssl")]
                    config.ssl_context.as_ref(),
                    config.connect_timeout,
                    config.minimal_protocol_version,
                )
                .await
                {
                    Ok((conn, version, supported)) => {
                        let sharder = Sharder::from_supported(&supported);
                        let extensions = ProtocolExtensions::from_supported(&supported);
                        self.connection_pool
                            .set(ConnectionPool::new(
                                config.pool_size,
                                sharder,
                                version,
                                extensions,
                                &config,
                            ))
                            .unwrap();
                        let worker = NodeWorker::new(self, config, used_keyspace, connection_rx);
                        worker
                            .run(conn, supported, address, shard_aware_port)
                            .await
                            .ok();
                        return;
                    }
                    Err(err) => {
                        let event = SessionEvent::ConnectionFailed {
                            node: self.clone(),
                            error: Arc::new(err),
                        };
                        self.session_events.send(event).ok();
                    }
                }
            }
            tokio::select! {
                Some(Some(reason)) = connection_rx.recv() => {
                    self.acknowledge_disconnection(reason);
                    return
                }
                _= tokio::time::sleep(delay) => {}
                else => {}
            }
        }
    }
}
