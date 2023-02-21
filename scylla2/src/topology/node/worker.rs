use std::{
    net::SocketAddr,
    ops::Deref,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use scylla2_cql::{
    extensions::ProtocolExtensions,
    protocol::{execute, startup},
    response::supported::Supported,
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    connection::{tcp::TcpConnection, ConnectionRef},
    error::Disconnected,
    statement::query::cql_query,
    topology::{
        node::{Node, NodeConfig, NodeDisconnectionReason, NodeStatus},
        peer::{AddressTranslatorExt, ShardAwarePort},
        sharding::{ShardInfo, Sharder},
    },
    utils::RepeatLast,
    SessionEvent,
};

const EXCESS_CONNECTION_BOUND_PER_SHARD_MULTIPLIER: usize = 10;

pub(super) struct NodeWorker {
    node: Arc<Node>,
    opened_connection_count: usize,
    opened_connections: Vec<Vec<TcpConnection>>,
    used_connections: Vec<Option<oneshot::Sender<()>>>,
    config: Arc<NodeConfig>,
    used_keyspace: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
    connection_rx: mpsc::UnboundedReceiver<Option<NodeDisconnectionReason>>,
    connection_request_tx: mpsc::Sender<usize>,
    connection_request_rx: mpsc::Receiver<usize>,
}

impl NodeWorker {
    pub(super) fn new(
        node: Arc<Node>,
        config: Arc<NodeConfig>,
        used_keyspace: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
        connection_rx: mpsc::UnboundedReceiver<Option<NodeDisconnectionReason>>,
    ) -> Self {
        let pool = node.connection_pool.get().unwrap();
        let nr_shards = pool.sharder.as_ref().map_or(1, |s| s.nr_shards().get());
        let connection_count = pool.connections.len();
        let (connection_request_tx, connection_request_rx) = mpsc::channel(connection_count);
        Self {
            node,
            opened_connection_count: 0,
            opened_connections: (0..nr_shards).map(|_| Default::default()).collect(),
            used_connections: (0..connection_count).map(|_| Default::default()).collect(),
            config,
            used_keyspace,
            connection_rx,
            connection_request_tx,
            connection_request_rx,
        }
    }

    fn parse_supported(
        &self,
        supported: Supported,
        shard_aware_port: &mut Option<u16>,
    ) -> Result<usize, Disconnected> {
        if Sharder::from_supported(&supported).as_ref() != self.node.sharder() {
            self.node
                .clone()
                .acknowledge_disconnection(NodeDisconnectionReason::ShardingChanged);
            return Err(Disconnected);
        }
        if &ProtocolExtensions::from_supported(&supported)
            != self.node.protocol_extensions().unwrap()
        {
            self.node
                .clone()
                .acknowledge_disconnection(NodeDisconnectionReason::ExtensionsChanged);
            return Err(Disconnected);
        }
        *shard_aware_port = supported.get("SCYLLA_SHARD_AWARE_PORT");
        #[cfg(feature = "ssl")]
        if self.config.ssl_context.is_some() {
            *shard_aware_port = supported.get("SCYLLA_SHARD_AWARE_PORT_SSL");
        }
        Ok(self
            .node
            .sharder()
            .and_then(|_| supported.get("SCYLLA_SHARD"))
            .unwrap_or(0))
    }

    pub(super) async fn run(
        mut self,
        init_conn: TcpConnection,
        supported: Supported,
        mut address: SocketAddr,
        mut translated_shard_aware_port: Option<ShardAwarePort>,
    ) -> Result<(), Disconnected> {
        let mut supported_shard_aware_port = None;
        let init_shard = self.parse_supported(supported, &mut supported_shard_aware_port)?;
        self.push(init_shard, init_conn);
        for i in 0..self.used_connections.len() {
            self.connection_request_tx.send(i).await.unwrap();
        }
        'request: loop {
            let conn_index = loop {
                tokio::select! {
                    biased;
                    Some(Some(reason)) = self.connection_rx.recv() => {
                        self.node.acknowledge_disconnection(reason);
                        return Err(Disconnected);
                    }
                    Some(conn_index) = self.connection_request_rx.recv() => break conn_index,
                    else => {} // else can only be Some(None) = connection_rx.recv()
                }
            };
            if self.try_start(conn_index).await {
                continue;
            }
            for delay in RepeatLast::new(self.config.reconnection_policy.reconnection_delays()) {
                let (addr_with_port, shard_info) = match (
                    self.node.sharder(),
                    translated_shard_aware_port,
                    supported_shard_aware_port,
                ) {
                    (Some(sharder), None, Some(port))
                    | (Some(sharder), Some(ShardAwarePort::Port(port)), _) => (
                        (address.ip(), port).into(),
                        Some(ShardInfo {
                            nr_shards: sharder.nr_shards(),
                            shard: self.shard(conn_index) as u16,
                        }),
                    ),
                    _ => (address, None),
                };
                match TcpConnection::open(
                    addr_with_port,
                    shard_info,
                    self.config.init_socket.as_ref(),
                    #[cfg(feature = "ssl")]
                    self.config.ssl_context.as_ref(),
                    self.config.connect_timeout,
                    self.node.protocol_version().unwrap(),
                )
                .await
                {
                    Ok((conn, supported)) => {
                        let shard =
                            self.parse_supported(supported, &mut supported_shard_aware_port)?;
                        self.push(shard, conn);
                        if self.try_start(conn_index).await {
                            continue 'request;
                        }
                    }
                    Err(err) => {
                        let event = SessionEvent::ConnectionFailed {
                            node: self.node.clone(),
                            error: Arc::new(err),
                        };
                        self.node.session_events.send(event).ok();
                    }
                }
                tokio::select! {
                    Some(Some(reason)) = self.connection_rx.recv() => {
                        self.node.acknowledge_disconnection(reason);
                        return Err(Disconnected)
                    }
                    _= tokio::time::sleep(delay) => {}
                    else => {}  // else can only be Some(None) = connection_rx.recv()
                }
                let translated = self
                    .config
                    .address_translator
                    .translate_or_warn(self.node.peer.rpc_address, &self.node.session_events)
                    .await;
                if let Ok((addr, shard_aware_port)) = translated {
                    if addr != address {
                        let event = SessionEvent::NodeAddressUpdate {
                            node: self.node.clone(),
                            address,
                        };
                        self.node.session_events.send(event).ok();
                    }
                    address = addr;
                    translated_shard_aware_port = shard_aware_port;
                }
            }
        }
    }

    fn shard(&self, conn_index: usize) -> usize {
        let conn_per_shard = self.used_connections.len() / self.opened_connections.len();
        conn_index / conn_per_shard
    }

    fn push(&mut self, shard: usize, conn: TcpConnection) {
        if shard > self.opened_connections.len() {
            #[cfg(feature = "tracing")]
            tracing::error!(
                shard,
                nr_shard = self.opened_connections.len(),
                "Invalid shard returned by Scylla"
            );
            return;
        }
        if self.opened_connection_count
            > self.opened_connections.len() * EXCESS_CONNECTION_BOUND_PER_SHARD_MULTIPLIER
        {
            self.opened_connections.iter_mut().for_each(Vec::clear);
            self.opened_connection_count = 0;
        }
        self.opened_connection_count += 1;
        self.opened_connections[shard].push(conn);
    }

    async fn try_start(&mut self, conn_index: usize) -> bool {
        let shard = self.shard(conn_index);
        while let Some(mut conn) = self.opened_connections[shard].pop() {
            let Ok(address) = conn.peer_addr() else {
                continue
            };
            if startup(
                &mut conn,
                address,
                self.node.protocol_version().unwrap(),
                &self.config.startup_options,
                self.config.authentication_protocol.as_deref(),
            )
            .await
            .is_err()
            {
                continue;
            }
            let query = if let Some(keyspace) = self.used_keyspace.read().await.deref().as_deref() {
                format!("USE \"{keyspace}\"")
            } else {
                "SELECT key FROM system.local where key = 'local'".into()
            };
            if execute(
                &mut conn,
                self.node.protocol_version().unwrap(),
                self.node.protocol_extensions(),
                cql_query(&query, ()),
            )
            .await
            .is_err()
            {
                continue;
            }
            let (tx, rx) = oneshot::channel();
            self.used_connections[conn_index] = Some(tx);
            self.opened_connection_count -= 1;
            tokio::spawn(start_connection(
                self.node.clone(),
                conn_index,
                shard as u16,
                conn,
                self.config.read_buffer_size,
                self.config.orphan_count_threshold_delay,
                rx,
                self.node.session_events.clone(),
                self.connection_request_tx.clone(),
            ));
            return true;
        }
        false
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_connection(
    node: Arc<Node>,
    index: usize,
    shard: u16,
    connection: TcpConnection,
    read_buffer_size: usize,
    orphan_count_threshold_delay: Duration,
    stop: oneshot::Receiver<()>,
    session_events: mpsc::UnboundedSender<SessionEvent>,
    connection_request_tx: mpsc::Sender<usize>,
) {
    let mut conn_count = node.active_connection_count.load(Ordering::Relaxed);
    loop {
        if conn_count < 0 {
            return;
        }
        match node.active_connection_count.compare_exchange_weak(
            conn_count,
            conn_count + 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(c) => conn_count = c,
        }
    }
    let event = SessionEvent::ConnectionOpened {
        node: node.clone(),
        shard,
        index,
    };
    session_events.send(event).ok();
    if conn_count == 0 {
        let event = SessionEvent::NodeStatusUpdate {
            node: node.clone(),
            status: NodeStatus::Up,
        };
        session_events.send(event).ok();
    }
    node.update_active_connection_count(1);
    let conn_ref = ConnectionRef::new(node.clone(), index);
    let error = conn_ref
        .clone()
        .get()
        .task(
            conn_ref,
            connection,
            read_buffer_size,
            orphan_count_threshold_delay,
            stop,
        )
        .await;
    loop {
        if conn_count < 0 {
            return;
        }
        match node.active_connection_count.compare_exchange_weak(
            conn_count,
            conn_count - 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(c) => conn_count = c,
        }
    }
    let event = SessionEvent::ConnectionClosed {
        node: node.clone(),
        shard,
        index,
        error: error.map(Arc::new),
    };
    if conn_count == 1 {
        let event = SessionEvent::NodeStatusUpdate {
            node: node.clone(),
            status: NodeStatus::Down,
        };
        session_events.send(event).ok();
    }

    session_events.send(event).ok();
    connection_request_tx.send(index).await.ok();
}
