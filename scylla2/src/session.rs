use std::{
    collections::{HashMap, HashSet},
    fmt,
    fmt::Formatter,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use futures::{stream::FuturesUnordered, StreamExt};
use rand::seq::SliceRandom;
use scylla2_cql::{
    error::{ConnectionError, DatabaseError},
    event::Event,
    request::prepare::Prepare,
    response::{result::CqlResult, Response, ResponseBody},
};
use tokio::sync::{broadcast, mpsc, oneshot, Notify};
use uuid::Uuid;

use crate::{
    connection::config::ConnectionConfig,
    error::{
        ConnectionExecutionError, ExecutionError, InvalidKeyspace, OngoingUseKeyspace,
        SessionError, UseKeyspaceError,
    },
    execution::ExecutionResult,
    session::{
        config::{NodeAddress, SessionConfig},
        control::{ControlAddr, ControlConnection, ControlError},
        event::{SessionEvent, SessionEventType},
        worker::{
            database_event_worker, heartbeat_worker, refresh_topology_worker, session_event_worker,
        },
    },
    statement::{
        config::{StatementConfig, StatementOptions},
        prepared::PreparedStatement,
        query::cql_query,
        values::IntoValues,
        Statement,
    },
    topology::{
        node::{Node, NodeConfig, NodeDisconnectionReason},
        partitioner::Partitioner,
        peer::{NodeDistance, NodeLocalizer, Peer},
        ring::{Partition, ReplicationStrategy, Ring},
        Topology,
    },
    utils::{invalid_response, other_error, resolve_hostname},
    DatabaseEventType,
};

pub mod config;
pub(crate) mod control;
pub mod event;
mod worker;

pub(crate) struct SessionInner {
    auto_await_schema_agreement_timeout: Option<Duration>,
    control_connection: ArcSwap<ControlConnection>,
    control_notify: Notify,
    database_events: broadcast::Sender<Event>,
    database_event_internal: mpsc::UnboundedSender<Event>,
    database_event_filter: Option<HashSet<DatabaseEventType>>,
    keyspace_used: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
    node_config_local: Arc<NodeConfig>,
    node_config_remote: Arc<NodeConfig>,
    node_localizer: Arc<dyn NodeLocalizer>,
    partitioner_cache: tokio::sync::RwLock<HashMap<String, HashMap<String, Partitioner>>>,
    register_for_schema_event: bool,
    ring_cache: RwLock<HashMap<ReplicationStrategy, Arc<ArcSwap<Ring>>>>,
    schema_agreement_interval: Duration,
    session_events: broadcast::Sender<SessionEvent>,
    session_event_filter: Option<HashSet<SessionEventType>>,
    session_events_internal: mpsc::UnboundedSender<SessionEvent>,
    strategy_cache: tokio::sync::RwLock<HashMap<String, ReplicationStrategy>>,
    statement_config: Option<StatementConfig>,
    topology: ArcSwap<Topology>,
    topology_lock: tokio::sync::Mutex<()>,
}

#[derive(Clone)]
pub struct Session(Arc<SessionInner>);

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let keyspace_used = self
            .keyspace_used()
            .unwrap_or_else(|err| Some(err.to_string().into()));
        f.debug_struct("Session")
            .field("topology", &self.topology())
            .field("keyspace_used", &keyspace_used)
            .field("statement_config", &self.statement_config())
            .finish()
    }
}

impl Session {
    pub async fn new(config: SessionConfig) -> Result<Self, SessionError> {
        let (session_events_internal_tx, session_events_internal_rx) = mpsc::unbounded_channel();
        let (database_events_internal_tx, database_events_internal_rx) = mpsc::unbounded_channel();
        let authentication_protocol = config.authentication_protocol;
        let startup_options = Arc::new(config.startup_options);
        let local_heartbeat_interval = config.connection_local.heartbeat_interval;
        let remote_heartbeat_interval = config.connection_remote.heartbeat_interval;
        let node_config = |conn_cfg: ConnectionConfig| {
            Arc::new(NodeConfig {
                address_translator: config.address_translator.clone(),
                authentication_protocol: authentication_protocol.clone(),
                buffer_size: conn_cfg.buffer_size,
                compression_min_size: config.compression_minimal_size,
                connect_timeout: conn_cfg.connect_timeout,
                init_socket: conn_cfg.init_socket,
                pool_size: conn_cfg.pool_size,
                minimal_protocol_version: config.minimal_protocol_version,
                orphan_count_threshold: config.orphan_count_threshold,
                orphan_count_threshold_delay: config.orphan_count_threshold_delay,
                reconnection_policy: conn_cfg.reconnection_policy,
                #[cfg(feature = "ssl")]
                ssl_context: config.ssl_context.clone(),
                startup_options: startup_options.clone(),
            })
        };
        let node_config_local = node_config(config.connection_local);
        let node_config_remote = node_config(config.connection_remote);

        let mut control_conn = Err(SessionError::NoNodes);
        for node in config.nodes {
            let addr = match node {
                NodeAddress::Address(addr) => addr,
                NodeAddress::Hostname(hostname) => match resolve_hostname(&hostname).await {
                    Ok(addr) => addr,
                    Err(err) => {
                        control_conn = Err(err.into());
                        continue;
                    }
                },
            };
            control_conn = ControlConnection::open(
                ControlAddr::Config(addr),
                &node_config_local,
                config.register_for_schema_event,
                database_events_internal_tx.clone(),
                session_events_internal_tx.clone(),
            )
            .await
            .map_err(Into::into);
            if control_conn.is_ok() {
                break;
            }
        }
        let inner = Arc::new(SessionInner {
            auto_await_schema_agreement_timeout: config.auto_await_schema_agreement_timeout,
            control_connection: ArcSwap::from_pointee(control_conn?),
            control_notify: Notify::new(),
            database_events: config.database_event_channel,
            database_event_internal: database_events_internal_tx,
            database_event_filter: config.database_event_filter,
            keyspace_used: Arc::new(tokio::sync::RwLock::new(None)),
            node_config_local,
            node_config_remote,
            node_localizer: config.node_localizer,
            partitioner_cache: Default::default(),
            register_for_schema_event: config.register_for_schema_event,
            ring_cache: Default::default(),
            schema_agreement_interval: Default::default(),
            session_events: config.session_event_channel,
            session_event_filter: config.session_event_filter,
            session_events_internal: session_events_internal_tx,
            strategy_cache: Default::default(),
            statement_config: config.statement_config,
            topology: Default::default(),
            topology_lock: Default::default(),
        });
        let session = Self(inner);
        let (started_tx, started_rx) = oneshot::channel();
        let weak = Arc::downgrade(&session.0);
        tokio::spawn(session_event_worker(
            weak.clone(),
            session_events_internal_rx,
            started_tx,
        ));
        tokio::spawn(database_event_worker(
            weak.clone(),
            database_events_internal_rx,
        ));
        if let Some(interval) = local_heartbeat_interval {
            tokio::spawn(heartbeat_worker(
                weak.clone(),
                interval,
                NodeDistance::Local,
            ));
        }
        if let Some(interval) = remote_heartbeat_interval {
            tokio::spawn(heartbeat_worker(
                weak.clone(),
                interval,
                NodeDistance::Remote,
            ));
        }
        if let Some(interval) = config.refresh_topology_interval {
            tokio::spawn(refresh_topology_worker(weak.clone(), interval));
        }
        if let Some(keyspace) = config.use_keyspace {
            session.use_keyspace(keyspace).await?;
        }
        match session.refresh_topology().await {
            Ok(_) => Ok::<_, ConnectionError>(()),
            Err(ExecutionError::Io(error)) => Err(error.into()),
            Err(ExecutionError::Database(error)) => Err(error.into()),
            _ => unreachable!(),
        }?;
        started_rx.await.ok();
        Ok(session)
    }

    async fn reopen_control_connection(&self) {
        for delay in self
            .0
            .node_config_local
            .reconnection_policy
            .reconnection_delays()
        {
            let topology = self.topology();
            let local = topology
                .local_nodes()
                .choose_multiple(&mut rand::thread_rng(), topology.local_nodes().len())
                .map(|node| (node, &self.0.node_config_local));
            let remote = topology
                .remote_nodes()
                .choose_multiple(&mut rand::thread_rng(), topology.remote_nodes().len())
                .map(|node| (node, &self.0.node_config_remote));
            for (node, node_config) in local.chain(remote) {
                match ControlConnection::open(
                    ControlAddr::Peer(node.peer().rpc_address),
                    node_config,
                    self.0.register_for_schema_event,
                    self.0.database_event_internal.clone(),
                    self.0.session_events_internal.clone(),
                )
                .await
                {
                    Ok(conn) => {
                        self.0.control_connection.store(Arc::new(conn));
                        self.0.control_notify.notify_waiters();
                        return;
                    }
                    Err(error) => {
                        let event = SessionEvent::ControlConnectionFailed {
                            rpc_address: node.peer().rpc_address,
                            error: Arc::new(error),
                        };
                        self.0.session_events_internal.send(event).ok();
                    }
                }
            }
            tokio::time::sleep(delay).await;
        }
    }

    pub async fn execute<S, V>(
        &self,
        statement: S,
        values: V,
    ) -> Result<ExecutionResult, ExecutionError>
    where
        V: IntoValues,
        S: Statement<V::Values>,
    {
        self.execute_with(statement, values, StatementOptions::default())
            .await
    }

    pub async fn execute_with<S, V>(
        &self,
        statement: S,
        values: V,
        options: impl Into<StatementOptions>,
    ) -> Result<ExecutionResult, ExecutionError>
    where
        V: IntoValues,
        S: Statement<V::Values>,
    {
        let options = options.into();
        let values = values.into_values();
        let partition = statement.partition(&values)?;
        let default_config;
        let config = match (statement.config(), self.statement_config()) {
            (Some(cfg), Some(default)) => {
                default_config = cfg.merge(default);
                &default_config
            }
            (Some(cfg), None) => cfg,
            (None, Some(default)) => default,
            (None, None) => {
                default_config = Default::default();
                &default_config
            }
        };
        let request = statement.as_request(config, &options, values);
        let topology;
        let (local_nodes, remote_nodes) = match partition {
            Some(ref p) => (p.local_nodes(), p.remote_nodes()),
            None => {
                topology = self.topology();
                (topology.local_nodes(), topology.remote_nodes())
            }
        };
        static NODE_ROUND_ROBIN: AtomicUsize = AtomicUsize::new(0);
        let round_robin = NODE_ROUND_ROBIN.fetch_add(1, Ordering::Relaxed);
        let safe_modulo = |len| if len == 0 { 0 } else { round_robin % len };
        let local = safe_modulo(local_nodes.len());
        let remote = safe_modulo(remote_nodes.len());
        let token = partition.as_ref().map(Partition::token);
        for node in local_nodes[local..]
            .iter()
            .chain(&local_nodes[..local])
            .chain(&remote_nodes[remote..])
            .chain(&remote_nodes[..remote])
        {
            match node.get_sharded_connections(token) {
                Some(conns) => {
                    let index = if conns.len() == 1 {
                        0
                    } else {
                        node.round_robin().fetch_add(1, Ordering::Relaxed) % conns.len()
                    };
                    let connection = &conns[index];
                    match connection
                        .execute_queued(&request, config.tracing.unwrap_or(false), None)
                        .await
                    {
                        Ok(response) => {
                            match (&response.body, self.0.auto_await_schema_agreement_timeout) {
                                (
                                    ResponseBody::Result(CqlResult::SchemaChange(event)),
                                    Some(timeout),
                                ) => {
                                    tokio::time::timeout(timeout, node.wait_schema_agreement())
                                        .await
                                        .map_err(|_| {
                                            ExecutionError::SchemaAgreementTimeout(event.clone())
                                        })?;
                                }
                                (ResponseBody::Result(CqlResult::Rows(rows)), _)
                                    if rows.metadata.new_metadata_id.is_some() =>
                                {
                                    // TODO maybe a add a config flag to decide if an error has to be
                                    // raised here
                                    // TODO some statements (e.g. ArcSwap<Arc<PreparedStatement>>)
                                    // should provide a way to update their metadata in Statement trait
                                    #[cfg(feature = "tracing")]
                                    tracing::warn!("Outdated result metadata");
                                }
                                _ => {}
                            }
                            if let (
                                ResponseBody::Result(CqlResult::SchemaChange(event)),
                                Some(timeout),
                            ) = (&response.body, self.0.auto_await_schema_agreement_timeout)
                            {
                                tokio::time::timeout(timeout, node.wait_schema_agreement())
                                    .await
                                    .map_err(|_| {
                                        ExecutionError::SchemaAgreementTimeout(event.clone())
                                    })?;
                            }
                            return Ok(ExecutionResult::from_response(
                                response,
                                statement.result_specs(),
                            )?);
                        }
                        Err(
                            ConnectionExecutionError::ConnectionClosed
                            | ConnectionExecutionError::NoStreamAvailable,
                        ) => {}
                        Err(ConnectionExecutionError::InvalidRequest(invalid)) => {
                            return Err(invalid.into())
                        }
                        Err(ConnectionExecutionError::Io(err)) => return Err(err.into()),
                    }
                }
                None => continue,
            }
        }
        Err(ExecutionError::NoConnection)
    }

    pub async fn prepare<S, K>(
        &self,
        statement: impl Into<Prepare<S, K>>,
    ) -> Result<PreparedStatement, ExecutionError>
    where
        S: AsRef<str>,
        K: AsRef<str>,
    {
        let prepare = statement.into();
        let topology = self.topology();
        let mut executions: FuturesUnordered<_> = topology
            .nodes()
            .iter()
            .filter_map(|node| node.get_random_connection())
            .map(|conn| conn.execute(&prepare, false, None))
            .collect();
        while let Some(res) = executions.next().await {
            match res {
                Ok(Response {
                    body: ResponseBody::Result(CqlResult::Prepared(prepared)),
                    ..
                }) => {
                    let partitioning = if let Some(col_spec) = prepared.column_specs.first() {
                        let (partitioner, ring) = tokio::try_join!(
                            self.get_partitioner(&col_spec.keyspace, &col_spec.table),
                            self.get_ring(&col_spec.keyspace)
                        )?;
                        Some((partitioner, ring))
                    } else {
                        None
                    };
                    return Ok(PreparedStatement {
                        statement: prepare.statement.as_ref().into(),
                        prepared,
                        partitioning,
                        config: Default::default(),
                    });
                }
                Ok(response) => return Err(invalid_response(response.ok()?.body).into()),
                Err(ConnectionExecutionError::InvalidRequest(err)) => return Err(err.into()),
                Err(_) => {}
            }
        }
        Err(ExecutionError::NoConnection)
    }

    pub fn topology(&self) -> Arc<Topology> {
        self.0.topology.load_full()
    }

    pub fn database_events(&self) -> broadcast::Receiver<Event> {
        self.0.database_events.subscribe()
    }

    pub fn session_events(&self) -> broadcast::Receiver<SessionEvent> {
        self.0.session_events.subscribe()
    }

    fn control(&self) -> Arc<ControlConnection> {
        self.0.control_connection.load_full()
    }

    async fn with_control<T, F>(
        &self,
        f: impl Fn(Arc<ControlConnection>) -> F,
    ) -> Result<T, Box<DatabaseError>>
    where
        F: Future<Output = Result<T, ControlError>>,
    {
        loop {
            let notified = self.0.control_notify.notified();
            match f(self.control()).await {
                Ok(res) => return Ok(res),
                Err(ControlError::Database(err)) => return Err(err),
                Err(ControlError::Closed | ControlError::Io(_)) => notified.await,
            }
        }
    }

    pub async fn get_partitioner(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<Partitioner, ExecutionError> {
        let cache = self.0.partitioner_cache.read().await;
        if let Some(partitioner) = cache.get(keyspace).and_then(|tables| tables.get(table)) {
            return Ok(partitioner.clone());
        }
        drop(cache);
        let cache = self.0.partitioner_cache.write().await;
        if let Some(partitioner) = cache.get(keyspace).and_then(|tables| tables.get(table)) {
            return Ok(partitioner.clone());
        }
        Ok(self
            .with_control(|ctrl| async move { ctrl.get_partitioner(keyspace, table).await })
            .await?
            .as_deref()
            .and_then(|partitioner| partitioner.parse().ok())
            .unwrap_or_default())
    }

    pub async fn get_ring(&self, keyspace: &str) -> Result<Arc<ArcSwap<Ring>>, ExecutionError> {
        let cache = self.0.strategy_cache.read().await;
        if let Some(strategy) = cache.get(keyspace) {
            return Ok(self.get_or_compute_ring(strategy));
        }
        drop(cache);
        let cache = self.0.strategy_cache.write().await;
        if let Some(strategy) = cache.get(keyspace) {
            return Ok(self.get_or_compute_ring(strategy));
        }
        let strategy = self
            .with_control(|ctrl| async move { ctrl.get_replication(keyspace).await })
            .await?
            .map(ReplicationStrategy::parse)
            .transpose()
            .map_err(other_error)?
            .unwrap_or_default();
        Ok(self.get_or_compute_ring(&strategy))
    }

    fn get_or_compute_ring(&self, strategy: &ReplicationStrategy) -> Arc<ArcSwap<Ring>> {
        let cache = self.0.ring_cache.read().unwrap();
        if let Some(ring) = cache.get(strategy) {
            return ring.clone();
        }
        drop(cache);
        let ring = Ring::new(self.0.topology.load().nodes(), strategy);
        let ring = Arc::new(ArcSwap::from_pointee(ring));
        let mut cache = self.0.ring_cache.write().unwrap();
        cache.insert(strategy.clone(), ring.clone());
        ring
    }

    pub fn send_heartbeat(&self, distance: Option<NodeDistance>) {
        const HEARTBEAT_QUERY: &str = "SELECT key FROM system.local where key = 'local'";
        let control_conn = self.control();
        tokio::spawn(async move { control_conn.send_heartbeat().await });
        let topology = self.topology();
        let nodes = match distance {
            Some(NodeDistance::Local) => topology.local_nodes(),
            Some(NodeDistance::Remote) => topology.remote_nodes(),
            Some(NodeDistance::Ignored) => return,
            None => topology.nodes(),
        };
        nodes
            .iter()
            .flat_map(|node| {
                node.connections()
                    .unwrap_or(&[])
                    .iter()
                    .map(|conn| conn.as_owned(node.clone()))
            })
            .map(|conn| async move {
                conn.execute(cql_query(HEARTBEAT_QUERY, ()), false, None)
                    .await
            })
            .map(tokio::spawn)
            .for_each(drop);
    }

    pub async fn refresh_topology(&self) -> Result<(), ExecutionError> {
        let instant = Instant::now();
        let _lock = self.0.topology_lock.lock().await;
        let topology = self.topology();
        if topology.latest_refresh() > instant {
            return Ok(());
        }
        let peers = self.control().get_peers().await?;
        let node_stream: FuturesUnordered<_> = peers
            .into_iter()
            .map(|peer| self.topology_node(&topology, peer))
            .collect();
        let mut has_new_node = false;
        let nodes: Vec<_> = node_stream
            .filter_map(|res| async move { res })
            .inspect(|(_, new_node)| has_new_node |= new_node)
            .map(|(node, _)| node)
            .collect()
            .await;
        if nodes.len() != topology.nodes().len() || has_new_node {
            let new_topology = Arc::new(Topology::new(nodes));
            self.0.topology.store(new_topology.clone());
            for (strategy, ring) in self.0.ring_cache.read().unwrap().iter() {
                ring.store(Arc::new(Ring::new(new_topology.nodes(), strategy)));
            }
            for node in topology.nodes() {
                if !new_topology
                    .nodes_by_rpc_address()
                    .contains_key(&node.peer().rpc_address)
                {
                    node.disconnect(NodeDisconnectionReason::Removed);
                }
            }
            let event = SessionEvent::TopologyUpdate {
                topology: new_topology,
            };
            self.0.session_events_internal.send(event).ok();
        }
        Ok(())
    }

    async fn topology_node(&self, topology: &Topology, peer: Peer) -> Option<(Arc<Node>, bool)> {
        let distance = self.0.node_localizer.distance(&peer).await;
        if let Some(node) = topology.nodes_by_rpc_address().get(&peer.rpc_address) {
            if node.peer() != &peer {
                #[cfg(feature = "tracing")]
                tracing::info!(
                    rpc_address = ?peer.rpc_address,
                    datacenter = peer.datacenter,
                    rack = peer.rack,
                    tokens = ?peer.tokens,
                    "Node peer has changed"
                );
                node.disconnect(NodeDisconnectionReason::PeerChanged);
            } else if node.distance() != distance {
                #[cfg(feature = "tracing")]
                tracing::info!(
                    rpc_address = ?peer.rpc_address,
                    %distance,
                    "Node distance has changed"
                );
                node.disconnect(NodeDisconnectionReason::DistanceChanged);
            } else {
                return Some((node.clone(), false));
            }
        }
        let node_config = match distance {
            NodeDistance::Local => Some(self.0.node_config_local.clone()),
            NodeDistance::Remote => Some(self.0.node_config_remote.clone()),
            NodeDistance::Ignored => None,
        };
        let node = Node::new(
            peer,
            distance,
            node_config,
            self.0.keyspace_used.clone(),
            self.0.session_events_internal.clone(),
            self.0.schema_agreement_interval,
        );
        Some((node, true))
    }

    pub async fn check_schema_agreement(&self) -> Result<Option<Uuid>, ExecutionError> {
        let schema_version = self.control().check_schema_agreement().await?;
        Ok(schema_version)
    }

    pub async fn wait_schema_agreement(&self) -> Uuid {
        loop {
            if let Ok(Some(schema_version)) = self.check_schema_agreement().await {
                return schema_version;
            }
            tokio::time::sleep(self.0.schema_agreement_interval).await;
        }
    }

    pub fn statement_config(&self) -> Option<&StatementConfig> {
        self.0.statement_config.as_ref()
    }

    pub fn keyspace_used(&self) -> Result<Option<Arc<str>>, OngoingUseKeyspace> {
        Ok(self
            .0
            .keyspace_used
            .try_read()
            .map_err(|_| OngoingUseKeyspace)?
            .clone())
    }

    pub async fn use_keyspace(
        &self,
        keyspace: impl Into<Arc<str>>,
    ) -> Result<(), UseKeyspaceError> {
        let keyspace = keyspace.into();
        if keyspace.is_empty()
            || keyspace.len() > 48
            || !keyspace
                .chars()
                .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '_' | '0'..='9'))
        {
            return Err(InvalidKeyspace.into());
        }
        let query = format!("USE \"{keyspace}\"");
        let mut keyspace_used = self.0.keyspace_used.write().await;
        keyspace_used.replace(keyspace);
        let topology = self.topology();
        let mut executions: FuturesUnordered<_> = topology
            .nodes()
            .iter()
            .flat_map(|node| node.connections())
            .flatten()
            .map(|conn| conn.execute_queued(cql_query(&query, ()), false, None))
            .collect();
        while let Some(res) = executions.next().await {
            if let Ok(response) = res {
                response.ok()?;
            }
        }
        Ok(())
    }
}

impl Drop for SessionInner {
    fn drop(&mut self) {
        for node in self.topology.load().nodes() {
            node.disconnect(NodeDisconnectionReason::SessionClosed);
        }
    }
}
