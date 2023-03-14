use std::{
    fmt, io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use scylla2_cql::{
    error::ConnectionError,
    event::{SchemaChangeEvent, StatusChangeEvent, TopologyChangeEvent},
};
use uuid::Uuid;

use crate::{
    error::BoxedError,
    topology::{
        node::{Node, NodeStatus},
        peer::Peer,
        Topology,
    },
};

#[allow(unused_variables)]
pub trait DatabaseEventHandler: fmt::Debug + Send + Sync {
    fn topology_change(&self, event: TopologyChangeEvent) {}
    fn status_change(&self, event: StatusChangeEvent) {}
    fn schema_change(&self, event: SchemaChangeEvent) {}
}

impl DatabaseEventHandler for Option<Arc<dyn DatabaseEventHandler>> {}

#[allow(unused_variables)]
pub trait SessionEventHandler: fmt::Debug + Send + Sync {
    fn address_translation_failed(&self, peer: &Peer, error: BoxedError) {}
    fn connection_opened(&self, node: &Arc<Node>, shard: u16, index: usize) {}
    fn connection_failed(&self, node: &Arc<Node>, error: ConnectionError) {}
    fn connection_closed(
        &self,
        node: &Arc<Node>,
        shard: u16,
        index: usize,
        too_many_orphan_streams: bool,
        error: Option<io::Error>,
    ) {
    }
    fn control_connection_opened(&self, rpc_address: IpAddr, address: SocketAddr) {}
    fn control_connection_failed(&self, address: SocketAddr, error: ConnectionError) {}
    fn control_connection_closed(
        &self,
        rpc_address: IpAddr,
        address: SocketAddr,
        error: Option<io::Error>,
    ) {
    }
    fn node_address_update(&self, node: &Arc<Node>) {}
    fn node_status_update(&self, node: &Arc<Node>, status: NodeStatus) {}
    fn schema_agreement(&self, schema_version: Uuid, rpc_address: IpAddr) {}
    fn topology_update(&self, topology: &Arc<Topology>) {}
    fn keyspace_used(&self, keyspace: &str) {}
}

impl SessionEventHandler for Option<Arc<dyn SessionEventHandler>> {}
