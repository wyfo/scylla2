use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use scylla2_cql::{
    frame::compression::Compression, protocol::auth::AuthenticationProtocol, Consistency,
    ProtocolVersion, SerialConsistency,
};
use tokio::sync::broadcast;

use crate::{
    auth::UserPassword,
    connection::config::{ConnectionConfig, InitSocket, ReconnectionPolicy},
    error::{BoxedError, SessionError},
    session::{
        event::{SessionEvent, SessionEventType},
        Session,
    },
    statement::config::StatementConfig,
    topology::{
        node::PoolSize,
        peer::{AddressTranslator, NodeDistance, NodeLocalizer, Peer, ShardAwarePort},
    },
    DatabaseEvent, DatabaseEventType,
};

#[derive(derivative::Derivative)]
#[derivative(Debug)]
#[non_exhaustive]
pub struct SessionConfig {
    #[derivative(Debug = "ignore")]
    pub address_translator: Arc<dyn AddressTranslator>,
    #[derivative(Debug = "ignore")]
    pub authentication_protocol: Option<Arc<dyn AuthenticationProtocol>>,
    pub auto_await_schema_agreement_timeout: Option<Duration>,
    pub compression_minimal_size: usize,
    pub connection_local: ConnectionConfig,
    pub connection_remote: ConnectionConfig,
    pub database_event_channel: broadcast::Sender<DatabaseEvent>,
    pub database_event_filter: Option<HashSet<DatabaseEventType>>,
    pub minimal_protocol_version: Option<ProtocolVersion>,
    #[derivative(Debug = "ignore")]
    pub node_localizer: Arc<dyn NodeLocalizer>,
    pub nodes: Vec<NodeAddress>,
    pub orphan_count_threshold: usize,
    pub orphan_count_threshold_delay: Duration,
    pub refresh_topology_interval: Option<Duration>,
    pub register_for_schema_event: bool,
    pub schema_agreement_interval: Duration,
    pub session_event_channel: broadcast::Sender<SessionEvent>,
    pub session_event_filter: Option<HashSet<SessionEventType>>,
    #[cfg(feature = "ssl")]
    pub ssl_context: Option<openssl::ssl::SslContext>,
    pub startup_options: HashMap<String, String>,
    pub statement_config: Option<StatementConfig>,
    pub use_keyspace: Option<Arc<str>>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        let mut startup_options = HashMap::new();
        if let Some(name) = option_env!("CARGO_PKG_NAME") {
            startup_options.insert("DRIVER_NAME".into(), name.into());
        }
        if let Some(version) = option_env!("CARGO_PKG_VERSION") {
            startup_options.insert("DRIVER_VERSION".into(), version.into());
        }
        Self {
            address_translator: Arc::new(
                |ip: IpAddr| -> Result<(SocketAddr, Option<ShardAwarePort>), BoxedError> {
                    Ok(((ip, 9042).into(), None))
                },
            ),
            authentication_protocol: None,
            auto_await_schema_agreement_timeout: Some(Duration::from_secs(60)),
            compression_minimal_size: 1 << 17,
            connection_local: ConnectionConfig::default(),
            connection_remote: ConnectionConfig::default(),
            database_event_channel: broadcast::channel(10).0,
            database_event_filter: None,
            minimal_protocol_version: None,
            node_localizer: Arc::new(|_: &Peer| NodeDistance::Remote),
            nodes: Vec::default(),
            orphan_count_threshold: usize::MAX,
            orphan_count_threshold_delay: Duration::from_secs(1),
            refresh_topology_interval: None,
            register_for_schema_event: false,
            schema_agreement_interval: Duration::from_millis(200),
            session_event_channel: broadcast::channel(10).0,
            session_event_filter: None,
            #[cfg(feature = "ssl")]
            ssl_context: None,
            startup_options,
            statement_config: None,
            use_keyspace: None,
        }
    }
}

impl SessionConfig {
    pub fn new() -> Self {
        Default::default()
    }

    pub async fn connect(self) -> Result<Session, SessionError> {
        Session::new(self).await
    }

    pub fn address_translator(mut self, translator: impl AddressTranslator + 'static) -> Self {
        self.address_translator = Arc::new(translator);
        self
    }

    pub fn authentication_protocol(
        mut self,
        protocol: impl AuthenticationProtocol + 'static,
    ) -> Self {
        self.authentication_protocol = Some(Arc::new(protocol));
        self
    }

    pub fn auto_await_schema_agreement_timeout(mut self, timeout: Duration) -> Self {
        self.auto_await_schema_agreement_timeout = Some(timeout);
        self
    }

    pub fn no_auto_await_schema_agreement(mut self) -> Self {
        self.auto_await_schema_agreement_timeout = None;
        self
    }

    pub fn compression(self, algo: Compression) -> Self {
        self.add_startup_options("COMPRESSION", algo.to_string())
    }

    pub fn compression_minimal_size(mut self, min_size: usize) -> Self {
        self.compression_minimal_size = min_size;
        self
    }

    pub fn connection_local(mut self, local_config: ConnectionConfig) -> Self {
        self.connection_local = local_config;
        self
    }

    pub fn connection_remote(mut self, remote_config: ConnectionConfig) -> Self {
        self.connection_remote = remote_config;
        self
    }

    pub fn connection_buffer_size(mut self, buffer_size: usize) -> Self {
        self.connection_local = self.connection_local.buffer_size(buffer_size);
        self.connection_remote = self.connection_remote.buffer_size(buffer_size);
        self
    }

    pub fn connection_init_socket(
        mut self,
        socket_config: impl InitSocket + Clone + 'static,
    ) -> Self {
        self.connection_local = self.connection_local.init_socket(socket_config.clone());
        self.connection_remote = self.connection_remote.init_socket(socket_config);
        self
    }

    pub fn connection_pool_size(mut self, pool_size: PoolSize) -> Self {
        self.connection_local = self.connection_local.pool_size(pool_size);
        self.connection_remote = self.connection_remote.pool_size(pool_size);
        self
    }

    pub fn connection_reconnection_policy(
        mut self,
        reconnection_policy: impl ReconnectionPolicy + Clone + 'static,
    ) -> Self {
        self.connection_local = self
            .connection_local
            .reconnection_policy(reconnection_policy.clone());
        self.connection_remote = self
            .connection_remote
            .reconnection_policy(reconnection_policy);
        self
    }

    pub fn connection_retry_exponential(
        mut self,
        initial_delay: Duration,
        max_delay: Duration,
        mult: u32,
    ) -> Self {
        self.connection_local =
            self.connection_local
                .retry_exponential(initial_delay, max_delay, mult);
        self.connection_remote =
            self.connection_remote
                .retry_exponential(initial_delay, max_delay, mult);
        self
    }

    pub fn connection_retry_interval(mut self, retry_interval: Duration) -> Self {
        self.connection_local = self.connection_local.retry_interval(retry_interval);
        self.connection_remote = self.connection_remote.retry_interval(retry_interval);
        self
    }

    pub fn database_event_channel(mut self, channel: broadcast::Sender<DatabaseEvent>) -> Self {
        self.database_event_channel = channel;
        self
    }

    pub fn database_event_filter(
        mut self,
        event_types: impl IntoIterator<Item = DatabaseEventType>,
    ) -> Self {
        self.database_event_filter = Some(event_types.into_iter().collect());
        self
    }

    pub fn cql_version(self, cql_version: impl Into<String>) -> Self {
        self.add_startup_options("CQL_VERSION", cql_version)
    }

    pub fn credentials(self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.authentication_protocol(UserPassword {
            username: username.into(),
            password: password.into(),
        })
    }

    pub fn datacenter(self, datacenter: impl Into<String>) -> Self {
        let datacenter = datacenter.into();
        self.node_localizer(move |peer: &Peer| {
            peer.datacenter
                .as_ref()
                .filter(|dc| *dc == &datacenter)
                .map_or(NodeDistance::Remote, |_| NodeDistance::Local)
        })
    }

    pub fn driver_name(self, name: impl Into<String>) -> Self {
        self.add_startup_options("DRIVER_NAME", name)
    }

    pub fn driver_version(self, version: impl Into<String>) -> Self {
        self.add_startup_options("DRIVER_VERSION", version)
    }

    pub fn minimal_protocol_version(mut self, protocol_version: ProtocolVersion) -> Self {
        self.minimal_protocol_version = Some(protocol_version);
        self
    }

    pub fn node_localizer(mut self, localizer: impl NodeLocalizer + 'static) -> Self {
        self.node_localizer = Arc::new(localizer);
        self
    }

    pub fn nodes<T>(mut self, nodes: impl IntoIterator<Item = T>) -> Self
    where
        T: Into<NodeAddress>,
    {
        self.nodes = nodes.into_iter().map(Into::into).collect();
        self
    }

    pub fn orphan_count_threshold(mut self, threshold: usize) -> Self {
        self.orphan_count_threshold = threshold;
        self
    }

    pub fn orphan_count_threshold_delay(mut self, delay: Duration) -> Self {
        self.orphan_count_threshold_delay = delay;
        self
    }

    pub fn refresh_topology_interval(mut self, interval: Duration) -> Self {
        self.refresh_topology_interval = Some(interval);
        self
    }

    pub fn register_for_schema_event(mut self) -> Self {
        self.register_for_schema_event = true;
        self
    }

    pub fn schema_agreement_interval(mut self, interval: Duration) -> Self {
        self.schema_agreement_interval = interval;
        self
    }

    pub fn session_event_channel(mut self, channel: broadcast::Sender<SessionEvent>) -> Self {
        self.session_event_channel = channel;
        self
    }

    pub fn session_event_filter(
        mut self,
        event_types: impl IntoIterator<Item = SessionEventType>,
    ) -> Self {
        self.session_event_filter = Some(event_types.into_iter().collect());
        self
    }

    #[cfg(feature = "ssl")]
    pub fn ssl_context(mut self, ssl_context: openssl::ssl::SslContext) -> Self {
        self.ssl_context = Some(ssl_context);
        self
    }

    pub fn startup_options(mut self, options: HashMap<String, String>) -> Self {
        self.startup_options = options;
        self
    }

    pub fn add_startup_options(
        mut self,
        option: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.startup_options.insert(option.into(), value.into());
        self
    }

    pub fn statement_config(mut self, params: StatementConfig) -> Self {
        self.statement_config = Some(params);
        self
    }

    pub fn statement_consistency(mut self, consistency: Consistency) -> Self {
        if let Some(ref mut cfg) = self.statement_config {
            cfg.consistency = Some(consistency);
        }
        self
    }

    pub fn statement_keyspace(mut self, keyspace: impl Into<Arc<str>>) -> Self {
        if let Some(ref mut cfg) = self.statement_config {
            cfg.keyspace = Some(keyspace.into());
        }
        self
    }

    pub fn statement_page_size(mut self, page_size: i32) -> Self {
        if let Some(ref mut cfg) = self.statement_config {
            cfg.page_size = Some(page_size);
        }
        self
    }

    pub fn statement_serial_consistency(mut self, serial_consistency: SerialConsistency) -> Self {
        if let Some(ref mut cfg) = self.statement_config {
            cfg.serial_consistency = Some(serial_consistency);
        }
        self
    }

    pub fn statement_tracing(mut self, tracing: bool) -> Self {
        if let Some(ref mut cfg) = self.statement_config {
            cfg.tracing = Some(tracing);
        }
        self
    }

    pub fn throw_on_overload(self) -> Self {
        self.add_startup_options("THROW_ON_OVERLOAD", "1")
    }

    pub fn use_keyspace(mut self, keyspace: impl Into<Arc<str>>) -> Self {
        self.use_keyspace = Some(keyspace.into());
        self
    }
}

#[derive(Debug)]
pub enum NodeAddress {
    Address(SocketAddr),
    Hostname(String),
}

impl NodeAddress {
    fn parse(s: &str) -> Result<Self, std::net::AddrParseError> {
        s.parse()
            .or_else(|_| Ok(SocketAddr::new(s.parse()?, 9042)))
            .map(Into::into)
    }
}

impl From<SocketAddr> for NodeAddress {
    fn from(value: SocketAddr) -> Self {
        Self::Address(value)
    }
}

impl From<IpAddr> for NodeAddress {
    fn from(value: IpAddr) -> Self {
        Self::Address((value, 9042).into())
    }
}

impl From<String> for NodeAddress {
    fn from(value: String) -> Self {
        NodeAddress::parse(&value).unwrap_or(Self::Hostname(value))
    }
}

impl From<&str> for NodeAddress {
    fn from(value: &str) -> Self {
        NodeAddress::parse(value).unwrap_or_else(|_| Self::Hostname(value.into()))
    }
}

impl From<&String> for NodeAddress {
    fn from(value: &String) -> Self {
        value.as_str().into()
    }
}
