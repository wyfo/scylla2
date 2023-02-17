use std::{collections::HashMap, net::IpAddr, sync::Arc, time::Instant};

use crate::topology::{node::Node, peer::NodeDistance};

pub mod node;
pub mod partitioner;
pub mod peer;
pub mod ring;
pub mod sharding;

#[derive(Debug)]
pub struct Topology {
    nodes: Vec<Arc<Node>>,
    nodes_by_rpc_address: HashMap<IpAddr, Arc<Node>>,
    local_nodes: Box<[Arc<Node>]>,
    remote_nodes: Box<[Arc<Node>]>,
    latest_refresh: Instant,
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
}
