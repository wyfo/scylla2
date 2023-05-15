use std::{ops::Deref, sync::Arc};

use scylla2_cql::{
    error::ConnectionError,
    extensions::ProtocolExtensions,
    protocol::{execute, startup},
    response::supported::Supported,
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    connection::{tcp::TcpConnection, Connection, ConnectionRef},
    error::Disconnected,
    event::SessionEventHandler,
    execution::utils::cql_query,
    topology::{
        node::{Node, NodeConfig, NodeDisconnectionReason, NodeEvent},
        sharding::Sharder,
    },
    utils::RepeatLast,
};

const EXCESS_CONNECTION_BOUND_PER_SHARD_MULTIPLIER: usize = 10;

enum ConnectionEvent {
    Opened(TcpConnection, Supported),
    Failed,
    Closed(usize, bool),
}

pub(super) struct NodeWorker {
    node: Arc<Node>,
    config: Arc<NodeConfig>,
    keyspace_used: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
    node_events: mpsc::UnboundedReceiver<NodeEvent>,
    supported_shard_aware_port: Option<u16>,
    connections_to_open: Vec<Vec<usize>>,
    opened_connections: Vec<TcpConnection>,
    started_connections: Vec<Option<oneshot::Sender<()>>>,
    conn_events_tx: mpsc::UnboundedSender<ConnectionEvent>,
    conn_events_rx: mpsc::UnboundedReceiver<ConnectionEvent>,
}

impl NodeWorker {
    pub(super) fn new(
        node: Arc<Node>,
        config: Arc<NodeConfig>,
        used_keyspace: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
        node_events: mpsc::UnboundedReceiver<NodeEvent>,
    ) -> Self {
        let pool = node.connection_pool.get().unwrap();
        let nr_shards = pool.sharder.as_ref().map_or(1, |s| s.nr_shards().get());
        let connection_count = pool.connections.len();
        let conn_per_shard = connection_count / nr_shards as usize;
        let (conn_events_tx, conn_events_rx) = mpsc::unbounded_channel();
        Self {
            node,
            config,
            keyspace_used: used_keyspace,
            node_events,
            supported_shard_aware_port: None,
            connections_to_open: (0..nr_shards)
                .map(|s| (s as usize * conn_per_shard..(s as usize + 1) * conn_per_shard).collect())
                .collect(),
            opened_connections: Vec::new(),
            started_connections: (0..connection_count).map(|_| Default::default()).collect(),
            conn_events_tx,
            conn_events_rx,
        }
    }

    pub(super) async fn run(
        mut self,
        conn: TcpConnection,
        supported: Supported,
    ) -> Result<(), Disconnected> {
        self.try_start(conn, supported)?;
        loop {
            let has_error = self.open_connections().await?;
            if has_error {
                for delay in RepeatLast::new(self.config.reconnection_policy.reconnection_delays())
                {
                    self.node
                        .update_address(self.config.address_translator.as_ref())
                        .await;
                    if let Some((conn, supported)) =
                        self.node.open_connection(&self.config, None, None).await
                    {
                        self.try_start(conn, supported)?;
                        break;
                    }
                    tokio::select! {
                        Some(NodeEvent::Disconnect(reason)) = self.node_events.recv() => {
                            self.node.acknowledge_disconnection(reason);
                            return Err(Disconnected)
                        }
                        _= tokio::time::sleep(delay) => {}
                        else => {}  // else can only be Some(None) = connection_rx.recv()
                    }
                }
            }
        }
    }

    fn try_start(&mut self, conn: TcpConnection, supported: Supported) -> Result<(), Disconnected> {
        if Sharder::from_supported(&supported).as_ref() != self.node.sharder() {
            self.node
                .acknowledge_disconnection(NodeDisconnectionReason::ShardingChanged);
            return Err(Disconnected);
        }
        if &ProtocolExtensions::from_supported(&supported)
            != self.node.protocol_extensions().unwrap()
        {
            self.node
                .acknowledge_disconnection(NodeDisconnectionReason::ExtensionsChanged);
            return Err(Disconnected);
        }
        self.supported_shard_aware_port = supported.get("SCYLLA_SHARD_AWARE_PORT");
        #[cfg(feature = "ssl")]
        if self.config.ssl_context.is_some() {
            self.supported_shard_aware_port = supported.get("SCYLLA_SHARD_AWARE_PORT_SSL");
        }
        let shard = self
            .node
            .sharder()
            .and_then(|_| supported.get("SCYLLA_SHARD"))
            .unwrap_or(0);
        if shard > self.connections_to_open.len() {
            #[cfg(feature = "tracing")]
            tracing::error!(
                shard,
                nr_shard = self.connections_to_open.len(),
                "Invalid shard returned by Scylla"
            );
            return Ok(());
        }
        if let Some(conn_index) = self.connections_to_open[shard].pop() {
            let (stop_tx, stop_rx) = oneshot::channel();
            self.started_connections[conn_index] = Some(stop_tx);
            tokio::spawn(start_connection(
                self.node.clone(),
                conn_index,
                shard as u16,
                conn,
                self.config.clone(),
                self.keyspace_used.clone(),
                stop_rx,
                self.conn_events_tx.clone(),
            ));
        } else {
            self.opened_connections.push(conn);
            if self.opened_connections.len()
                > self.connections_to_open.len() * EXCESS_CONNECTION_BOUND_PER_SHARD_MULTIPLIER
            {
                self.opened_connections.clear();
            }
        }
        Ok(())
    }

    async fn open_connections(&mut self) -> Result<bool, Disconnected> {
        self.connections_to_open
            .iter()
            .enumerate()
            .flat_map(|(shard, conn_indexes)| conn_indexes.iter().map(move |_| (shard, ())))
            .for_each(|(shard, _)| {
                tokio::spawn(open_connection(
                    self.node.clone(),
                    self.config.clone(),
                    shard as u16,
                    self.supported_shard_aware_port,
                    self.conn_events_tx.clone(),
                ));
            });
        let mut opening_count: usize = self.connections_to_open.iter().map(Vec::len).sum();
        if opening_count == 0 {
            self.opened_connections.clear();
        }
        let mut has_error = false;
        loop {
            let event = tokio::select! {
                Some(NodeEvent::Disconnect(reason)) = self.node_events.recv() => {
                    self.node.acknowledge_disconnection(reason);
                    return Err(Disconnected)
                }
                event = self.conn_events_rx.recv() => event.unwrap(),
                else => continue,
            };
            match event {
                ConnectionEvent::Opened(conn, supported) => {
                    opening_count -= 1;
                    self.try_start(conn, supported)?;
                }
                ConnectionEvent::Failed => {
                    opening_count -= 1;
                }
                ConnectionEvent::Closed(conn_index, error) => {
                    has_error |= error;
                    let conn_per_shard =
                        self.connections_to_open.len() / self.opened_connections.len();
                    let shard = conn_index / conn_per_shard;
                    self.connections_to_open[shard].push(conn_index);
                }
            }
            if opening_count == 0 {
                return Ok(has_error);
            }
        }
    }
}

async fn open_connection(
    node: Arc<Node>,
    config: Arc<NodeConfig>,
    shard: u16,
    supported_shard_aware_port: Option<u16>,
    conn_events: mpsc::UnboundedSender<ConnectionEvent>,
) {
    let event = match node
        .open_connection(&config, Some(shard), supported_shard_aware_port)
        .await
    {
        Some((conn, supported)) => ConnectionEvent::Opened(conn, supported),
        None => ConnectionEvent::Failed,
    };
    conn_events.send(event).ok();
}

#[allow(clippy::too_many_arguments)]
async fn start_connection(
    node: Arc<Node>,
    conn_index: usize,
    shard: u16,
    mut tcp_conn: TcpConnection,
    config: Arc<NodeConfig>,
    keyspace_used: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
    stop: oneshot::Receiver<()>,
    conn_events: mpsc::UnboundedSender<ConnectionEvent>,
) {
    let conn_ref = ConnectionRef::new(node.clone(), conn_index);
    let conn = conn_ref.get();
    if let Err(err) = startup_and_use(&node, &mut tcp_conn, conn, &config, &keyspace_used).await {
        node.session_event_handler.connection_failed(&node, err);
        let event = ConnectionEvent::Closed(conn_index, true);
        conn_events.send(event).ok();
        return;
    }
    node.session_event_handler
        .connection_opened(&node, shard, conn_index);
    node.update_active_connection_count(1);
    let task_res = conn
        .task(
            conn_ref.clone(),
            tcp_conn,
            config.read_buffer_size,
            config.orphan_count_threshold_delay,
            stop,
        )
        .await;
    let too_many_orphan_streams = *task_res.as_ref().unwrap_or(&false);
    node.update_active_connection_count(-1);
    node.session_event_handler.connection_closed(
        &node,
        shard,
        conn_index,
        too_many_orphan_streams,
        task_res.err(),
    );
    let event = ConnectionEvent::Closed(conn_index, !too_many_orphan_streams);
    conn_events.send(event).ok();
}

async fn startup_and_use(
    node: &Node,
    tcp_conn: &mut TcpConnection,
    conn: &Connection,
    config: &NodeConfig,
    keyspace_used: &tokio::sync::RwLock<Option<Arc<str>>>,
) -> Result<(), ConnectionError> {
    let version = node.protocol_version().unwrap();
    let address = node.address.lock().unwrap().unwrap().0;
    startup(
        &mut *tcp_conn,
        address,
        version,
        &config.startup_options,
        config.authentication_protocol.as_deref(),
    )
    .await?;
    let keyspace_guard = keyspace_used.read().await;
    if let Some(keyspace) = keyspace_guard.deref() {
        match execute(
            tcp_conn,
            version,
            None,
            cql_query(&format!("USE \"{keyspace}\""), ()),
        )
        .await
        {
            Ok(_) => {}
            #[allow(unused_variables)]
            Err(ConnectionError::Database(error)) => {
                #[cfg(feature = "tracing")]
                tracing::error!(?error, "USE query failed on connection opening");
            }
            Err(err) => return Err(err),
        }
    }
    conn.reopen();
    drop(keyspace_guard);
    Ok(())
}
