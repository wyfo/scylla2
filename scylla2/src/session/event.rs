use std::{io, net::SocketAddr, sync::Arc};

use scylla2_cql::error::BoxedError;
use uuid::Uuid;

use crate::{
    error::ConnectionError,
    topology::{
        node::{Node, NodeStatus},
        peer::Peer,
        Topology,
    },
};

#[derive(Debug, Clone, strum::EnumDiscriminants)]
#[strum_discriminants(name(SessionEventType))]
#[strum_discriminants(derive(Hash))]
pub enum SessionEvent {
    AddressTranslationFailed {
        peer: Peer,
        error: Arc<BoxedError>,
    },
    ConnectionOpened {
        node: Arc<Node>,
        shard: Option<u16>,
        index: usize,
    },
    ConnectionFailed {
        node: Option<Arc<Node>>,
        error: Arc<ConnectionError>,
    },
    ConnectionClosed {
        node: Option<Arc<Node>>,
        shard: Option<u16>,
        index: usize,
        error: Option<Arc<io::Error>>,
    },
    ControlConnectionOpened {
        address: SocketAddr,
    },
    ControlConnectionFailed {
        address: SocketAddr,
        error: Arc<ConnectionError>,
    },
    ControlConnectionClosed {
        address: SocketAddr,
        error: Option<Arc<io::Error>>,
    },
    NodeStatusUpdate {
        node: Arc<Node>,
        status: NodeStatus,
    },
    SchemaAgreement {
        schema_version: Uuid,
    },
    TopologyUpdate {
        topology: Arc<Topology>,
    },
    UseKeyspace {
        keyspace: Arc<str>,
    },
}
