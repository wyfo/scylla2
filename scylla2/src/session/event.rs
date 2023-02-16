use std::{io, net::IpAddr, sync::Arc};

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
pub enum SessionEvent {
    AddressTranslationFailed {
        rpc_address: IpAddr,
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
