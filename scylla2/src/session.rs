use std::{
    collections::HashMap,
    fmt,
    fmt::Formatter,
    future::Future,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use futures::{stream, stream::FuturesUnordered, Stream, StreamExt};
use rand::seq::SliceRandom;
use scylla2_cql::{
    error::{ConnectionError, InvalidRequest},
    request::{
        prepare::Prepare,
        query::values::{QueryValues, QueryValuesExt, SerializedQueryValues},
    },
    response::{result::CqlResult, Response, ResponseBody},
};
use tokio::sync::{mpsc, Notify};
use uuid::Uuid;

use crate::{
    connection::config::ConnectionConfig,
    error::{
        ExecutionError, InvalidKeyspace, OngoingUseKeyspace, RequestError, SessionError,
        UseKeyspaceError,
    },
    event::{DatabaseEventHandler, SessionEventHandler},
    execution::{
        load_balancing::LoadBalancingPolicy, utils::cql_query, Execution, ExecutionProfile,
        ExecutionResult,
    },
    session::{
        config::{NodeAddress, SessionConfig},
        control::ControlConnection,
        worker::{event_worker, loop_worker, node_worker},
    },
    statement::{options::StatementOptions, prepared::PreparedStatement, Statement},
    topology::{
        node::{Node, NodeConfig, NodeDisconnectionReason, NodeStatus},
        partitioner::Partitioner,
        peer::{NodeDistance, NodeLocalizer, Peer},
        ring::{Partition, ReplicationStrategy, Ring},
        Topology,
    },
    utils::{invalid_response, other_error, resolve_hostname},
};

pub mod config;
pub(crate) mod control;
pub mod worker;

pub(crate) struct SessionInner {
    auto_await_schema_agreement_timeout: Option<Duration>,
    control_connection: ControlConnection,
    control_notify: Notify,
    database_event_handler: Arc<dyn DatabaseEventHandler>,
    execution_profile: Arc<ExecutionProfile>,
    keyspace_used: Arc<tokio::sync::RwLock<Option<Arc<str>>>>,
    node_config_local: Arc<NodeConfig>,
    node_config_remote: Arc<NodeConfig>,
    node_disconnections: mpsc::UnboundedSender<NodeDisconnectionReason>,
    node_localizer: Arc<dyn NodeLocalizer>,
    partitioner_cache: tokio::sync::RwLock<HashMap<String, HashMap<String, Partitioner>>>,
    register_for_schema_event: bool,
    ring_cache: RwLock<HashMap<ReplicationStrategy, Ring>>,
    schema_agreement_interval: Duration,
    session_event_handler: Arc<dyn SessionEventHandler>,
    strategy_cache: tokio::sync::RwLock<HashMap<String, ReplicationStrategy>>,
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
            .field("execution_profile", &self.execution_profile())
            .finish()
    }
}

impl Session {
    pub async fn new(config: SessionConfig) -> Result<Self, SessionError> {
        let authentication_protocol = config.authentication_protocol;
        let startup_options = Arc::new(config.startup_options);
        let local_heartbeat_interval = config.connection_local.heartbeat_interval;
        let remote_heartbeat_interval = config.connection_remote.heartbeat_interval;
        let node_config = |conn_cfg: ConnectionConfig| {
            Arc::new(NodeConfig {
                address_translator: config.address_translator.clone(),
                authentication_protocol: authentication_protocol.clone(),
                compression_min_size: config.compression_minimal_size,
                connect_timeout: conn_cfg.connect_timeout,
                init_socket: conn_cfg.init_socket,
                pool_size: conn_cfg.pool_size,
                minimal_protocol_version: config.minimal_protocol_version,
                orphan_count_threshold: config.orphan_count_threshold,
                orphan_count_threshold_delay: config.orphan_count_threshold_delay,
                read_buffer_size: conn_cfg.read_buffer_size,
                reconnection_policy: conn_cfg.reconnection_policy,
                #[cfg(feature = "ssl")]
                ssl_context: config.ssl_context.clone(),
                startup_options: startup_options.clone(),
                write_buffer_size: conn_cfg.write_buffer_size,
            })
        };
        let node_config_local = node_config(config.connection_local);
        let node_config_remote = node_config(config.connection_remote);

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let control_connection = ControlConnection::new(config.session_event_handler.clone());
        let mut control_result = Err(SessionError::NoNodes);
        for node in config.nodes {
            let address = match node {
                NodeAddress::Address(addr) => addr,
                NodeAddress::Hostname(hostname) => match resolve_hostname(&hostname).await {
                    Ok(addr) => addr,
                    Err(err) => {
                        control_result = Err(ConnectionError::Io(err).into());
                        continue;
                    }
                },
            };
            control_result = control_connection
                .open(
                    None,
                    address,
                    &node_config_local,
                    config.register_for_schema_event,
                    event_tx.clone(),
                )
                .await
                .map_err(Into::into);
            if control_result.is_ok() {
                break;
            }
        }
        control_result?;
        let (node_disconnection_tx, node_disconnection_rx) = mpsc::unbounded_channel();
        let inner = Arc::new(SessionInner {
            auto_await_schema_agreement_timeout: config.auto_await_schema_agreement_timeout,
            control_connection,
            control_notify: Notify::new(),
            database_event_handler: config.database_event_handler,
            execution_profile: config.execution_profile,
            keyspace_used: Arc::new(tokio::sync::RwLock::new(None)),
            node_config_local,
            node_config_remote,
            node_disconnections: node_disconnection_tx,
            node_localizer: config.node_localizer,
            partitioner_cache: Default::default(),
            register_for_schema_event: config.register_for_schema_event,
            ring_cache: Default::default(),
            schema_agreement_interval: Default::default(),
            session_event_handler: config.session_event_handler,
            strategy_cache: Default::default(),
            topology: Default::default(),
            topology_lock: Default::default(),
        });
        let session = Self(inner);
        let weak = Arc::downgrade(&session.0);
        tokio::spawn(event_worker(weak.clone(), event_rx));
        tokio::spawn(node_worker(weak.clone(), node_disconnection_rx));
        if let Some(interval) = local_heartbeat_interval {
            tokio::spawn(loop_worker(weak.clone(), interval, |session| async move {
                session.send_heartbeat(Some(NodeDistance::Local));
            }));
        }
        if let Some(interval) = remote_heartbeat_interval {
            tokio::spawn(loop_worker(weak.clone(), interval, |session| async move {
                session.send_heartbeat(Some(NodeDistance::Remote));
            }));
        }
        if let Some(interval) = config.refresh_topology_interval {
            tokio::spawn(loop_worker(weak.clone(), interval, |session| async move {
                session.refresh_topology().await
            }));
        }
        if let Some(keyspace) = config.use_keyspace {
            session.use_keyspace(keyspace).await?;
        }
        let topology = match session.refresh_topology().await {
            Ok(topology) => topology,
            Err(ExecutionError::Io(error)) => return Err(ConnectionError::Io(error).into()),
            Err(ExecutionError::Database(error)) => {
                return Err(ConnectionError::Database(error).into())
            }
            _ => unreachable!(),
        };
        topology
            .wait_nodes(|_, status| !matches!(status, NodeStatus::Connecting))
            .await;
        Ok(session)
    }

    pub async fn wait_for_all_connections(&self) {
        self.topology()
            .wait_nodes(|node, status| match status {
                NodeStatus::Connecting => false,
                NodeStatus::Up(conn_count) => {
                    conn_count.get() == node.connections().map_or(0, <[_]>::len)
                }
                NodeStatus::Down | NodeStatus::Disconnected(_) => true,
            })
            .await;
    }

    pub async fn execute<S, V>(
        &self,
        statement: S,
        values: V,
    ) -> Result<ExecutionResult, ExecutionError>
    where
        S: Statement<V>,
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
        S: Statement<V>,
    {
        let options = options.into();
        let partition = statement.partition(&values, options.token)?;
        let profile = options
            .execution_profile
            .as_ref()
            .unwrap_or(&self.0.execution_profile);
        let serial_consistency = profile
            .serial_consistency
            .filter(|_| statement.is_lwt().unwrap_or(true));
        let request =
            statement.as_request(profile.consistency, serial_consistency, &options, values);
        let execution = Execution::new(
            &statement,
            &request,
            options.keyspace.as_deref(),
            options.custom_payload.as_ref(),
            profile,
        );
        let result = match (
            &profile.load_balancing_policy,
            partition,
            statement.is_lwt(),
        ) {
            (LoadBalancingPolicy::TokenAware, Some(partition), Some(true)) => {
                execution
                    .run(partition.replicas().iter(), Some(partition.token()))
                    .await?
            }
            (LoadBalancingPolicy::TokenAware, Some(partition), _) => {
                execution
                    .run(
                        partition.local_then_remote_replicas(),
                        Some(partition.token()),
                    )
                    .await?
            }
            (LoadBalancingPolicy::TokenAware, None, _) => {
                let topology = self.topology();
                let nodes = topology.nodes();
                execution
                    .run(
                        nodes.choose_multiple(&mut rand::thread_rng(), nodes.len()),
                        None,
                    )
                    .await?
            }
            (LoadBalancingPolicy::Dynamic(policy), partition, is_lwt) => {
                execution
                    .run(
                        policy.query_plan(self, partition.as_ref(), is_lwt.unwrap_or(false)),
                        partition.as_ref().map(Partition::token),
                    )
                    .await?
            }
        };

        match (
            result.as_result(),
            self.0.auto_await_schema_agreement_timeout,
        ) {
            (CqlResult::SchemaChange(event), Some(timeout)) => {
                tokio::time::timeout(timeout, result.node().wait_schema_agreement())
                    .await
                    .map_err(|_| ExecutionError::SchemaAgreementTimeout(event.clone()))?;
            }
            (CqlResult::SetKeyspace(_), _) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    "Raw USE KEYSPACE query executed instead of using `Session::use_keyspace`"
                );
            }
            (CqlResult::Rows(rows), _) if rows.metadata.new_metadata_id.is_some() => {
                // TODO maybe a add a config flag to decide if an error has to be
                // raised here
                // TODO some statements (e.g. ArcSwap<Arc<PreparedStatement>>)
                // should provide a way to update their metadata in Statement trait
                #[cfg(feature = "tracing")]
                tracing::warn!("Outdated result metadata");
            }
            _ => {}
        }
        Ok(result)
    }

    pub fn execute_paged<S, V>(
        &self,
        statement: S,
        values: V,
        page_size: i32,
    ) -> impl Stream<Item = Result<ExecutionResult, ExecutionError>>
    where
        S: Statement<V> + for<'a> Statement<&'a SerializedQueryValues>,
        V: QueryValues,
    {
        self.execute_paged_with(statement, values, page_size, StatementOptions::default())
    }

    pub fn execute_paged_with<S, V>(
        &self,
        statement: S,
        values: V,
        page_size: i32,
        options: impl Into<StatementOptions>,
    ) -> impl Stream<Item = Result<ExecutionResult, ExecutionError>>
    where
        S: Statement<V> + for<'a> Statement<&'a SerializedQueryValues>,
        V: QueryValues,
    {
        let mut options = options.into();
        options.page_size = Some(page_size);
        stream::try_unfold(
            (self.clone(), statement, values, options, None),
            |(session, statement, values, mut options, serialized_values)| async {
                if options.page_size.is_none() {
                    return Ok(None);
                }
                if options.token.is_none() {
                    let partition = statement.partition(&values, options.token)?;
                    options.token = partition.map(|part| part.token());
                }
                let serialized_values = serialized_values
                    .map_or_else(|| values.serialize(), Ok)
                    .map_err(InvalidRequest::from)?;
                let result = session
                    .execute_with(&statement, &serialized_values, options.clone())
                    .await?;
                if result.paging_state().is_some() {
                    options.paging_state = result.paging_state().cloned();
                } else {
                    options.page_size = None;
                }
                let next = (session, statement, values, options, Some(serialized_values));
                Ok(Some((result, next)))
            },
        )
    }

    pub async fn prepare<S, K>(
        &self,
        statement: impl Into<Prepare<S, K>>,
        idempotent: bool,
        is_lwt: impl Into<Option<bool>>,
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
            .map(|conn| conn.send_queued(&prepare, false, None))
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
                    let is_lwt = prepared.is_lwt.or_else(|| is_lwt.into());
                    return Ok(PreparedStatement {
                        statement: prepare.statement.as_ref().into(),
                        prepared,
                        partitioning,
                        idempotent,
                        is_lwt,
                    });
                }
                Ok(response) => return Err(invalid_response(response.ok()?.body).into()),
                Err(RequestError::InvalidRequest(err)) => return Err(err.into()),
                Err(_) => {}
            }
        }
        Err(ExecutionError::NoConnection)
    }

    pub fn topology(&self) -> Arc<Topology> {
        self.0.topology.load_full()
    }

    async fn with_control<'a, T, F>(
        &'a self,
        f: impl Fn(&'a ControlConnection) -> F,
    ) -> Result<T, ExecutionError>
    where
        F: Future<Output = Result<T, ExecutionError>> + 'a,
    {
        loop {
            let notified = self.0.control_notify.notified();
            match f(&self.0.control_connection).await {
                Err(ExecutionError::Io(_)) => notified.await,
                res => return res,
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
            .with_control(|ctrl| ctrl.get_partitioner(keyspace, table))
            .await?
            .as_deref()
            .and_then(|partitioner| partitioner.parse().ok())
            .unwrap_or_default())
    }

    pub async fn get_ring(&self, keyspace: &str) -> Result<Ring, ExecutionError> {
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
            .with_control(|ctrl| ctrl.get_replication(keyspace))
            .await?
            .map(ReplicationStrategy::parse)
            .transpose()
            .map_err(other_error)?
            .unwrap_or_default();
        Ok(self.get_or_compute_ring(&strategy))
    }

    fn get_or_compute_ring(&self, strategy: &ReplicationStrategy) -> Ring {
        let cache = self.0.ring_cache.read().unwrap();
        if let Some(ring) = cache.get(strategy) {
            return ring.clone();
        }
        drop(cache);
        let mut cache = self.0.ring_cache.write().unwrap();
        if let Some(ring) = cache.get(strategy) {
            return ring.clone();
        }
        let ring = Ring::new(self.0.topology.load().nodes(), strategy.clone());
        cache.insert(strategy.clone(), ring.clone());
        ring
    }

    pub fn send_heartbeat(&self, distance: Option<NodeDistance>) {
        const HEARTBEAT_QUERY: &str = "SELECT key FROM system.local where key = 'local'";
        let inner = self.0.clone();
        tokio::spawn(async move { inner.control_connection.send_heartbeat().await });
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
            .map(|conn| async move { conn.send(cql_query(HEARTBEAT_QUERY, ()), false, None).await })
            .map(tokio::spawn)
            .for_each(drop);
    }

    pub async fn refresh_topology(&self) -> Result<Arc<Topology>, ExecutionError> {
        let instant = Instant::now();
        let _lock = self.0.topology_lock.lock().await;
        let topology = self.topology();
        if topology.latest_refresh() > instant {
            return Ok(topology);
        }
        let peers = self.0.control_connection.get_peers().await?;
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
                ring.update(new_topology.nodes(), strategy.clone());
            }
            for node in topology.nodes() {
                if !new_topology
                    .nodes_by_rpc_address()
                    .contains_key(&node.peer().rpc_address)
                {
                    node.disconnect(NodeDisconnectionReason::Removed);
                }
            }
            self.0.session_event_handler.topology_update(&new_topology);
            return Ok(new_topology);
        }
        Ok(topology)
    }

    async fn topology_node(&self, topology: &Topology, peer: Peer) -> Option<(Arc<Node>, bool)> {
        assert!(
            !peer.rpc_address.is_unspecified(),
            "Node version is bugged and outdated, consider upgrading it"
        );
        let distance = self.0.node_localizer.distance(&peer);
        if let Some(node) = topology
            .nodes_by_rpc_address()
            .get(&peer.rpc_address)
            .filter(|node| !matches!(node.status(), NodeStatus::Disconnected(_)))
        {
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
            self.0.node_disconnections.clone(),
            self.0.session_event_handler.clone(),
            self.0.schema_agreement_interval,
        );
        Some((node, true))
    }

    pub async fn check_schema_agreement(&self) -> Result<Option<Uuid>, ExecutionError> {
        self.0.control_connection.check_schema_agreement().await
    }

    pub async fn wait_schema_agreement(&self) -> Uuid {
        loop {
            if let Ok(Some(schema_version)) = self.check_schema_agreement().await {
                return schema_version;
            }
            tokio::time::sleep(self.0.schema_agreement_interval).await;
        }
    }

    pub fn execution_profile(&self) -> Arc<ExecutionProfile> {
        self.0.execution_profile.clone()
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
            .map(|conn| conn.send_queued(cql_query(&query, ()), false, None))
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
