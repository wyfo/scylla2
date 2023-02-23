use std::{collections::HashMap, fmt, net::IpAddr, sync::Arc, time::Instant};

use futures::{stream::FuturesUnordered, StreamExt};

use crate::topology::{
    node::{Node, NodeStatus},
    peer::NodeDistance,
};

pub mod node;
pub mod partitioner;
pub mod peer;
pub mod ring;
pub mod sharding;

pub struct Topology {
    nodes: Vec<Arc<Node>>,
    nodes_by_rpc_address: HashMap<IpAddr, Arc<Node>>,
    local_nodes: Box<[Arc<Node>]>,
    remote_nodes: Box<[Arc<Node>]>,
    latest_refresh: Instant,
}

impl fmt::Debug for Topology {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Topology")
            .field("nodes", &self.nodes)
            .finish()
    }
}

impl Default for Topology {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl Topology {
    pub(crate) fn new(nodes: Vec<Arc<Node>>) -> Self {
        Self {
            nodes_by_rpc_address: nodes
                .iter()
                .map(|node| (node.peer().rpc_address, node.clone()))
                .collect(),
            local_nodes: nodes
                .iter()
                .filter(|node| node.distance() == NodeDistance::Local)
                .cloned()
                .collect(),
            remote_nodes: nodes
                .iter()
                .filter(|node| node.distance() == NodeDistance::Remote)
                .cloned()
                .collect(),
            latest_refresh: Instant::now(),
            nodes,
        }
    }

    pub fn nodes(&self) -> &[Arc<Node>] {
        &self.nodes
    }

    pub fn local_nodes(&self) -> &[Arc<Node>] {
        &self.local_nodes
    }

    pub fn remote_nodes(&self) -> &[Arc<Node>] {
        &self.remote_nodes
    }

    pub fn nodes_by_rpc_address(&self) -> &HashMap<IpAddr, Arc<Node>> {
        &self.nodes_by_rpc_address
    }

    pub fn latest_refresh(&self) -> Instant {
        self.latest_refresh
    }

    pub(crate) async fn wait_nodes(&self, predicate: impl Fn(&Node, NodeStatus) -> bool) {
        let predicate = &predicate;
        let wait_nodes: FuturesUnordered<_> = self
            .nodes()
            .iter()
            .map(|node| node.wait_for_status(move |status| predicate(node, status)))
            .collect();
        wait_nodes.for_each(|_| async {}).await;
    }
}
