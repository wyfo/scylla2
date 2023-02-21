use std::{
    ops::Deref,
    sync::{atomic::Ordering, Arc},
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
        peer::ShardAwarePort,
        sharding::{ShardInfo, Sharder},
    },
    utils::RepeatLast,
    SessionEvent,
};

const EXCESS_CONNECTION_BOUND_PER_SHARD_MULTIPLIER: usize = 10;

pub(super) struct NodeWorker {
    node: Arc<Node>,
    shard_aware_port: Option<u16>,
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
            shard_aware_port: None,
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

    pub(super) async fn run(
        mut self,
        conn: TcpConnection,
        supported: Supported,
    ) -> Result<(), Disconnected> {
        self.add_connection(conn, supported)?;
        for i in 0..self.used_connections.len() {
            self.connection_request_tx.send(i).await.unwrap();
        }
        loop {
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
            loop {
                let (conn, supported) = self.open_connection(self.shard(conn_index) as u16).await?;
                self.add_connection(conn, supported)?;
                if self.try_start(conn_index).await {
                    break;
                }
            }
        }
    }

    async fn open_connection(
        &mut self,
        shard: u16,
    ) -> Result<(TcpConnection, Supported), Disconnected> {
        for delay in RepeatLast::new(self.config.reconnection_policy.reconnection_delays()) {
            let (address, shard_aware_port) = self.node.address.lock().unwrap().unwrap();
            let shard_info = match (self.node.sharder(), shard_aware_port, self.shard_aware_port) {
                (Some(sharder), Some(ShardAwarePort::Port(port)), _)
                | (Some(sharder), None, Some(port)) => Some(ShardInfo {
                    shard_aware_port: port,
                    nr_shards: sharder.nr_shards(),
                    shard,
                }),
                _ => None,
            };
            match TcpConnection::open(
                address,
                shard_info,
                self.config.init_socket.as_ref(),
                #[cfg(feature = "ssl")]
                self.config.ssl_context.as_ref(),
                self.config.connect_timeout,
                self.node.protocol_version().unwrap(),
            )
            .await
            {
                Ok(conn) => return Ok(conn),
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
            self.node
                .update_address(self.config.address_translator.as_ref())
                .await;
        }
        unreachable!()
    }

    fn add_connection(
        &mut self,
        conn: TcpConnection,
        supported: Supported,
    ) -> Result<(), Disconnected> {
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
        self.shard_aware_port = supported.get("SCYLLA_SHARD_AWARE_PORT");
        #[cfg(feature = "ssl")]
        if self.config.ssl_context.is_some() {
            self.shard_aware_port = supported.get("SCYLLA_SHARD_AWARE_PORT_SSL");
        }
        let shard = self
            .node
            .sharder()
            .and_then(|_| supported.get("SCYLLA_SHARD"))
            .unwrap_or(0);
        if shard > self.opened_connections.len() {
            #[cfg(feature = "tracing")]
            tracing::error!(
                shard,
                nr_shard = self.opened_connections.len(),
                "Invalid shard returned by Scylla"
            );
            return Ok(());
        }
        if self.opened_connection_count
            > self.opened_connections.len() * EXCESS_CONNECTION_BOUND_PER_SHARD_MULTIPLIER
        {
            self.opened_connections.iter_mut().for_each(Vec::clear);
            self.opened_connection_count = 0;
        }
        self.opened_connection_count += 1;
        self.opened_connections[shard].push(conn);
        Ok(())
    }

    fn shard(&self, conn_index: usize) -> usize {
        let conn_per_shard = self.used_connections.len() / self.opened_connections.len();
        conn_index / conn_per_shard
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
                self.config.clone(),
                rx,
                self.connection_request_tx.clone(),
            ));
            return true;
        }
        false
    }
}

async fn start_connection(
    node: Arc<Node>,
    index: usize,
    shard: u16,
    connection: TcpConnection,
    config: Arc<NodeConfig>,
    stop: oneshot::Receiver<()>,
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
    node.session_events.send(event).ok();
    if conn_count == 0 {
        let event = SessionEvent::NodeStatusUpdate {
            node: node.clone(),
            status: NodeStatus::Up,
        };
        node.session_events.send(event).ok();
    }
    node.update_active_connection_count(1);
    let conn_ref = ConnectionRef::new(node.clone(), index);
    let error = conn_ref
        .clone()
        .get()
        .task(
            conn_ref,
            connection,
            config.read_buffer_size,
            config.orphan_count_threshold_delay,
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
        node.session_events.send(event).ok();
    }

    node.session_events.send(event).ok();
    connection_request_tx.send(index).await.ok();
}
