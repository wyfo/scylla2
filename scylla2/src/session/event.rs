use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use scylla2_cql::error::BoxedError;
use uuid::Uuid;

use crate::{
    error::ConnectionError,
    topology::{
        node::{Node, NodeStatus},
        Topology,
    },
};

#[derive(Debug, Clone, strum::EnumDiscriminants)]
#[strum_discriminants(name(SessionEventType))]
#[strum_discriminants(derive(Hash))]
#[non_exhaustive]
pub enum SessionEvent {
    AddressTranslationFailed {
        rpc_address: IpAddr,
        error: Arc<BoxedError>,
    },
    ConnectionOpened {
        node: Arc<Node>,
        shard: u16,
        index: usize,
    },
    ConnectionFailed {
        node: Arc<Node>,
        error: Arc<ConnectionError>,
    },
    ConnectionClosed {
        node: Arc<Node>,
        shard: u16,
        index: usize,
        error: Option<Arc<io::Error>>,
    },
    ControlConnectionOpened {
        rpc_address: IpAddr,
    },
    ControlConnectionFailed {
        rpc_address: IpAddr,
        error: Arc<ConnectionError>,
    },
    ControlConnectionClosed {
        rpc_address: IpAddr,
        error: Option<Arc<io::Error>>,
    },
    NodeAddressUpdate {
        node: Arc<Node>,
        address: SocketAddr,
    },
    NodeStatusUpdate {
        node: Arc<Node>,
        status: NodeStatus,
    },
    SchemaAgreement {
        schema_version: Uuid,
        rpc_address: IpAddr,
    },
    TopologyUpdate {
        topology: Arc<Topology>,
    },
    UseKeyspace {
        keyspace: Arc<str>,
    },
}
