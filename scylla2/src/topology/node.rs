use std::{
    collections::{HashMap, HashSet},
    fmt,
    net::SocketAddr,
    num::{NonZeroU16, NonZeroUsize},
    str::FromStr,
    sync::{
        atomic::{AtomicIsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use once_cell::sync::OnceCell;
use rand::seq::SliceRandom;
use scylla2_cql::{
    error::{ConnectionError, DatabaseErrorKind},
    extensions::ProtocolExtensions,
    frame::compression::Compression,
    protocol::auth::AuthenticationProtocol,
    response::supported::Supported,
    ProtocolVersion,
};
use tokio::sync::{mpsc, Notify};
use uuid::Uuid;

use crate::{
    connection::{
        config::{InitSocket, ReconnectionPolicy},
        tcp::TcpConnection,
        Connection,
    },
    error::{Disconnected, ExecutionError, RequestError},
    event::SessionEventHandler,
    execution::utils::{cql_query, peers_and_local},
    topology::{
        node::worker::NodeWorker,
        partitioner::Token,
        peer::{AddressTranslator, AddressTranslatorExt, NodeDistance, Peer, ShardAwarePort},
        sharding::{ShardInfo, Sharder},
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
    pub(crate) compression_min_size: usize,
    pub(crate) connect_timeout: Duration,
    pub(crate) init_socket: Box<dyn InitSocket>,
    pub(crate) pool_size: PoolSize,
    pub(crate) minimal_protocol_version: Option<ProtocolVersion>,
    pub(crate) orphan_count_threshold: usize,
    pub(crate) orphan_count_threshold_delay: Duration,
    pub(crate) read_buffer_size: usize,
    pub(crate) reconnection_policy: Box<dyn ReconnectionPolicy>,
    #[cfg(feature = "ssl")]
    pub(crate) ssl_context: Option<openssl::ssl::SslContext>,
    pub(crate) startup_options: Arc<HashMap<String, String>>,
    pub(crate) write_buffer_size: usize,
}

impl NodeConfig {
    fn compression(&self) -> Option<Compression> {
        self.startup_options
            .get("COMPRESSION")
            .and_then(|c| Compression::from_str(c).ok())
    }
}

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
                    config.write_buffer_size,
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
    Up(NonZeroUsize),
    Down,
    Connecting,
    Disconnected(NodeDisconnectionReason),
}

impl NodeStatus {
    fn up(conn_count: isize) -> Self {
        Self::Up(NonZeroUsize::new(conn_count as usize).unwrap())
    }
}

const IGNORED: isize = -1;
const REMOVED: isize = -2;
const PEER_CHANGED: isize = -3;
const DISTANCE_CHANGED: isize = -4;
const SHARDING_CHANGED: isize = -5;
const PROTOCOL_VERSION_CHANGED: isize = -6;
const EXTENSIONS_CHANGED: isize = -7;
const SESSION_CLOSED: isize = -8;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum NodeDisconnectionReason {
    Ignored,
    Removed,
    PeerChanged,
    DistanceChanged,
    ShardingChanged,
    ProtocolVersionChanged,
    ExtensionsChanged,
    SessionClosed,
}

impl From<NodeDisconnectionReason> for NodeStatus {
    fn from(value: NodeDisconnectionReason) -> Self {
        NodeStatus::Disconnected(value)
    }
}

enum NodeEvent {
    Reconnect,
    Disconnect(NodeDisconnectionReason),
}

pub struct Node {
    address: Mutex<Option<(SocketAddr, Option<ShardAwarePort>)>>,
    peer: Peer,
    distance: NodeDistance,
    active_connection_count: AtomicIsize, // negative means disconnected
    pool_size: Option<PoolSize>,
    connection_pool: OnceCell<ConnectionPool>,
    status_notify: Notify,
    disconnection_channel: mpsc::UnboundedSender<NodeDisconnectionReason>,
    session_event_handler: Option<Arc<dyn SessionEventHandler>>,
    node_events: mpsc::UnboundedSender<NodeEvent>,
    schema_agreement_interval: Duration,
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Node")
            .field("address", &self.address())
            .field("rpc_address", &self.peer.rpc_address)
            .field("datacenter", &self.peer.datacenter)
            .field("rack", &self.peer.rack)
            .field("distance", &self.distance)
            .field("pool_size", &self.pool_size)
            .field("nr_shards", &self.sharder().map(|s| s.nr_shards()))
            .field("status", &self.status())
            .field("protocol_version", &self.protocol_version())
            .field("protocol_extensions", &self.protocol_extensions())
            .finish()
    }
}

impl Node {
    pub(crate) fn new(
        peer: Peer,
        distance: NodeDistance,
        config: Option<Arc<NodeConfig>>,
        used_keyspace: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
        disconnection_channel: mpsc::UnboundedSender<NodeDisconnectionReason>,
        session_event_handler: Option<Arc<dyn SessionEventHandler>>,
        schema_agreement_interval: Duration,
    ) -> Arc<Self> {
        let (node_events_tx, node_events_rx) = mpsc::unbounded_channel();
        let node = Arc::new(Self {
            address: Mutex::new(None),
            peer,
            distance,
            active_connection_count: AtomicIsize::new(isize::MIN),
            pool_size: config.as_ref().map(|c| c.pool_size),
            connection_pool: OnceCell::new(),
            status_notify: Notify::new(),
            disconnection_channel,
            session_event_handler,
            node_events: node_events_tx,
            schema_agreement_interval,
        });
        assert_eq!(matches!(distance, NodeDistance::Ignored), config.is_none());
        if let Some(config) = config {
            let worker = node.clone().worker(config, used_keyspace, node_events_rx);
            tokio::spawn(worker);
        } else {
            node.acknowledge_disconnection(NodeDisconnectionReason::Ignored);
        }
        node
    }

    pub fn address(&self) -> Option<SocketAddr> {
        self.address.lock().unwrap().map(|addr| addr.0)
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
            n if n > 0 => NodeStatus::up(n),
            0 => NodeStatus::Down,
            IGNORED => NodeDisconnectionReason::Ignored.into(),
            REMOVED => NodeDisconnectionReason::Removed.into(),
            PEER_CHANGED => NodeDisconnectionReason::PeerChanged.into(),
            DISTANCE_CHANGED => NodeDisconnectionReason::DistanceChanged.into(),
            SHARDING_CHANGED => NodeDisconnectionReason::ShardingChanged.into(),
            PROTOCOL_VERSION_CHANGED => NodeDisconnectionReason::ProtocolVersionChanged.into(),
            EXTENSIONS_CHANGED => NodeDisconnectionReason::ExtensionsChanged.into(),
            SESSION_CLOSED => NodeDisconnectionReason::SessionClosed.into(),
            isize::MIN => NodeStatus::Connecting,
            _ => unreachable!(),
        }
    }

    pub fn is_up(&self) -> bool {
        matches!(self.status(), NodeStatus::Up(_))
    }

    pub async fn wait_for_status(&self, predicate: impl Fn(NodeStatus) -> bool) -> NodeStatus {
        loop {
            let notified = self.status_notify.notified();
            let status = self.status();
            if predicate(status) {
                return status;
            }
            notified.await;
        }
    }

    fn sharder(&self) -> Option<&Sharder> {
        self.connection_pool.get()?.sharder.as_ref()
    }

    pub fn nr_shards(&self) -> Option<NonZeroU16> {
        Some(self.sharder()?.nr_shards())
    }

    pub fn shard_of(&self, token: Token) -> Option<u16> {
        Some(self.sharder()?.compute_shard(token))
    }

    pub fn connections(&self) -> Option<&[Connection]> {
        Some(&self.connection_pool.get()?.connections)
    }

    pub fn get_random_connection(&self) -> Option<&Connection> {
        while self.is_up() {
            let conn = self.connections()?.choose(&mut rand::thread_rng()).unwrap();
            if !conn.is_closed() {
                return Some(conn);
            }
        }
        None
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

    pub fn get_connection(&self, token: Option<Token>) -> Option<&Connection> {
        let connections = self.get_sharded_connections(token)?;
        if connections.len() == 1 {
            connections.first().filter(|conn| !conn.is_closed())
        } else {
            connections
                .choose_multiple(&mut rand::thread_rng(), connections.len())
                .find(|conn| !conn.is_closed())
        }
        .or_else(|| self.get_random_connection())
    }

    pub fn try_reconnect(&self) -> Result<(), Disconnected> {
        if !self.is_up() {
            self.node_events
                .send(NodeEvent::Reconnect)
                .map_err(|_| Disconnected)?;
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
            conn.send(local_query, false, None),
            conn.send(peers_query, false, None),
        ) {
            Ok(res) => res,
            Err(RequestError::ConnectionClosed | RequestError::NoStreamAvailable) => {
                return Ok(None)
            }
            Err(RequestError::InvalidRequest(invalid)) => return Err(invalid.into()),
            Err(RequestError::Io(err)) => return Err(err.into()),
        };
        let schema_versions: HashSet<Uuid> =
            peers_and_local(peers.ok()?, local.ok()?, |(uuid,)| uuid)?;
        Ok(if schema_versions.len() == 1 {
            let schema_version = schema_versions.into_iter().next().unwrap();
            self.session_event_handler
                .schema_agreement(schema_version, self.peer.rpc_address);
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
        self.node_events.send(NodeEvent::Disconnect(reason)).ok();
    }

    fn update_status(self: &Arc<Self>, status: NodeStatus) {
        self.session_event_handler.node_status_update(self, status);
        self.status_notify.notify_waiters();
    }

    fn acknowledge_disconnection(self: &Arc<Self>, reason: NodeDisconnectionReason) {
        let disconnected = match reason {
            NodeDisconnectionReason::Ignored => IGNORED,
            NodeDisconnectionReason::Removed => REMOVED,
            NodeDisconnectionReason::PeerChanged => PEER_CHANGED,
            NodeDisconnectionReason::DistanceChanged => DISTANCE_CHANGED,
            NodeDisconnectionReason::ShardingChanged => SHARDING_CHANGED,
            NodeDisconnectionReason::ProtocolVersionChanged => PROTOCOL_VERSION_CHANGED,
            NodeDisconnectionReason::ExtensionsChanged => EXTENSIONS_CHANGED,
            NodeDisconnectionReason::SessionClosed => SESSION_CLOSED,
        };
        self.active_connection_count
            .store(disconnected, Ordering::Relaxed);
        self.update_status(NodeStatus::Disconnected(reason));
        self.disconnection_channel.send(reason).ok();
    }

    fn update_active_connection_count(self: &Arc<Self>, update: isize) {
        assert!(update == 1 || update == -1);
        let mut conn_count = self.active_connection_count.load(Ordering::Relaxed);
        while conn_count >= 0 || conn_count == isize::MIN {
            assert!(update == 1 || conn_count >= 1);
            let next_count = if conn_count == isize::MIN {
                1
            } else {
                conn_count + update
            };
            match self.active_connection_count.compare_exchange_weak(
                conn_count,
                next_count,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if next_count > 0 {
                        self.update_status(NodeStatus::up(next_count));
                    } else {
                        self.update_status(NodeStatus::Down);
                    }
                    return;
                }
                Err(c) => conn_count = c,
            }
        }
    }

    async fn update_address(self: &Arc<Node>, address_translator: &dyn AddressTranslator) {
        let Some(address) = address_translator.translate_or_warn(&self.peer, &self.session_event_handler).await.ok() else {
            return
        };
        let prev_addr = self.address.lock().unwrap().replace(address);
        if prev_addr.map_or(false, |prev| prev == address) {
            self.session_event_handler.node_address_update(self);
        }
    }

    async fn worker(
        self: Arc<Self>,
        config: Arc<NodeConfig>,
        used_keyspace: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
        mut node_events: mpsc::UnboundedReceiver<NodeEvent>,
    ) {
        let mut first_connection = true;
        for delay in RepeatLast::new(config.reconnection_policy.reconnection_delays()) {
            self.update_address(config.address_translator.as_ref())
                .await;
            let translated = *self.address.lock().unwrap();
            if let Some((address, _)) = translated {
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
                        let pool = ConnectionPool::new(
                            config.pool_size,
                            sharder,
                            version,
                            extensions,
                            &config,
                        );
                        self.connection_pool.set(pool).ok().unwrap();
                        let worker = NodeWorker::new(self, config, used_keyspace, node_events);
                        worker.run(conn, supported).await.ok();
                        return;
                    }
                    Err(err) => {
                        if first_connection {
                            first_connection = false;
                            self.update_status(NodeStatus::Down);
                        }
                        self.session_event_handler.connection_failed(&self, err);
                    }
                }
            }
            tokio::select! {
                Some(NodeEvent::Disconnect(reason)) = node_events.recv() => {
                    self.acknowledge_disconnection(reason);
                    return
                }
                _= tokio::time::sleep(delay) => {}
                else => {}
            }
        }
    }

    async fn open_connection(
        self: &Arc<Node>,
        config: &NodeConfig,
        shard: Option<u16>,
        supported_shard_aware_port: Option<u16>,
    ) -> ConnectionResult {
        let (address, shard_aware_port) = self.address.lock().unwrap().unwrap();
        let mut shard_info = match (
            shard,
            self.sharder(),
            (shard_aware_port, supported_shard_aware_port),
        ) {
            (
                Some(shard),
                Some(sharder),
                (Some(ShardAwarePort::Port(port)), _) | (None, Some(port)),
            ) => Some(ShardInfo {
                shard_aware_port: port,
                nr_shards: sharder.nr_shards(),
                shard,
            }),
            _ => None,
        };
        // loop because no async recursion
        loop {
            return match TcpConnection::open(
                address,
                shard_info,
                config.init_socket.as_ref(),
                #[cfg(feature = "ssl")]
                config.ssl_context.as_ref(),
                config.connect_timeout,
                self.protocol_version().unwrap(),
            )
            .await
            {
                Ok((conn, supported)) => ConnectionResult::Connected(conn, supported),
                Err(ConnectionError::Database(err))
                    if err.kind == DatabaseErrorKind::ProtocolError =>
                {
                    ConnectionResult::ProtocolVersionChanged
                }
                Err(err) => {
                    if shard_info.is_some() {
                        shard_info = None;
                        continue;
                    }
                    self.session_event_handler.connection_failed(self, err);
                    ConnectionResult::Error
                }
            };
        }
    }
}

enum ConnectionResult {
    Connected(TcpConnection, Supported),
    ProtocolVersionChanged,
    Error,
}
