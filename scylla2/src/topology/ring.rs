use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use arc_swap::ArcSwap;
use once_cell::sync::OnceCell;

use crate::{
    error::BoxedError,
    topology::{node::Node, partitioner::Token, peer::NodeDistance},
};

#[derive(Clone, Eq, PartialEq, Hash)]
pub(crate) enum ReplicationStrategy {
    Local,
    Simple {
        replication_factor: usize,
    },
    NetworkTopology {
        datacenters: BTreeMap<String, usize>,
    },
    Other {
        class: String,
        data: BTreeMap<String, String>,
    },
}

impl Default for ReplicationStrategy {
    fn default() -> Self {
        Self::Simple {
            replication_factor: 1,
        }
    }
}

impl ReplicationStrategy {
    pub fn parse(mut replication: HashMap<String, String>) -> Result<Self, BoxedError> {
        let class = replication
            .remove("class")
            .ok_or("Missing replication class")?;
        if class.ends_with("NetworkTopologyStrategy") {
            let datacenters = replication
                .into_iter()
                .map(|(dc, rf)| Ok((dc, rf.parse()?)))
                .collect::<Result<_, BoxedError>>()?;
            Ok(ReplicationStrategy::NetworkTopology { datacenters })
        } else if class.ends_with("SimpleStrategy") {
            let replication_factor = replication
                .get("replication_factor")
                .ok_or("Missing replication_factor")?
                .parse()?;
            Ok(ReplicationStrategy::Simple { replication_factor })
        } else if class.ends_with("LocalStrategy") {
            Ok(ReplicationStrategy::Local)
        } else {
            let data = replication.into_iter().collect();
            Ok(ReplicationStrategy::Other { class, data })
        }
    }
}

#[derive(Clone)]
pub struct Ring(Arc<ArcSwap<RingInner>>);

impl fmt::Debug for Ring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Ring")
            .field(&self.0.load().token_map)
            .finish()
    }
}

impl Ring {
    pub(crate) fn new(nodes: &[Arc<Node>], strategy: ReplicationStrategy) -> Self {
        let inner = RingInner::new(nodes, strategy);
        Ring(Arc::new(ArcSwap::from_pointee(inner)))
    }

    pub(crate) fn update(&self, nodes: &[Arc<Node>], strategy: ReplicationStrategy) {
        self.0.store(Arc::new(RingInner::new(nodes, strategy)));
    }

    pub fn get_partition(&self, token: Token) -> Partition {
        let ring = self.0.load_full();
        Partition { token, ring }
    }
}

struct RingInner {
    strategy: ReplicationStrategy,
    token_map: BTreeMap<Token, Arc<Node>>,
    acceptable_repeats: HashMap<String, usize>,
    replicas: BTreeMap<Token, OnceCell<Replicas>>,
}

impl RingInner {
    pub(crate) fn new(nodes: &[Arc<Node>], strategy: ReplicationStrategy) -> Self {
        let token_map: BTreeMap<_, _> = nodes
            .iter()
            .flat_map(|node| node.peer().tokens.iter().map(move |tk| (*tk, node.clone())))
            .collect();
        let all_racks = nodes
            .iter()
            .filter_map(|n| Some((n.peer().datacenter.as_deref()?, n.peer().rack.as_deref()?)))
            .fold(
                HashMap::<String, HashSet<String>>::new(),
                |mut map, (dc, rack)| {
                    map.entry(dc.into()).or_default().insert(rack.into());
                    map
                },
            );
        let acceptable_repeats: HashMap<_, _> = match &strategy {
            ReplicationStrategy::NetworkTopology { datacenters } => datacenters
                .iter()
                .map(|(dc, rf)| (dc.clone(), rf.saturating_sub(all_racks[dc.as_str()].len())))
                .collect(),
            _ => Default::default(),
        };
        let replicas = token_map
            .keys()
            .map(|tk| (*tk, Default::default()))
            .collect();
        Self {
            strategy,
            token_map,
            acceptable_repeats,
            replicas,
        }
    }

    fn compute_replicas(&self, token: Token) -> Replicas {
        let all_replicas = match &self.strategy {
            ReplicationStrategy::Simple { replication_factor } => {
                simple_replicas(token, &self.token_map, *replication_factor)
            }
            ReplicationStrategy::NetworkTopology { datacenters } => network_topology_replicas(
                token,
                &self.token_map,
                datacenters,
                &self.acceptable_repeats,
            ),
            _ => vec![self.token_map[&token].clone()],
        };
        Replicas {
            local: all_replicas
                .iter()
                .filter(|node| node.distance() == NodeDistance::Local)
                .cloned()
                .collect(),
            remote: all_replicas
                .iter()
                .filter(|node| node.distance() == NodeDistance::Remote)
                .cloned()
                .collect(),
            all: all_replicas,
        }
    }
}

struct Replicas {
    local: Vec<Arc<Node>>,
    remote: Vec<Arc<Node>>,
    all: Vec<Arc<Node>>,
}

#[derive(Clone)]
pub struct Partition {
    token: Token,
    ring: Arc<RingInner>,
}

impl fmt::Debug for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Partition")
            .field("token", &self.token)
            .field("replicas", &self.replicas())
            .finish()
    }
}

impl Partition {
    pub fn token(&self) -> Token {
        self.token
    }

    fn get_replicas(&self) -> &Replicas {
        let (token, replicas) = self
            .ring
            .replicas
            .range(self.token..)
            .next()
            .or_else(|| self.ring.replicas.iter().next())
            .unwrap();
        replicas.get_or_init(|| self.ring.compute_replicas(*token))
    }

    pub fn replicas(&self) -> &[Arc<Node>] {
        &self.get_replicas().all
    }

    pub fn local_replicas(&self) -> &[Arc<Node>] {
        &self.get_replicas().local
    }

    pub fn remote_replicas(&self) -> &[Arc<Node>] {
        &self.get_replicas().remote
    }

    pub fn local_then_remote_replicas(&self) -> impl Iterator<Item = &Arc<Node>> {
        let replicas = self.get_replicas();
        replicas.local.iter().chain(&replicas.remote)
    }
}

#[derive(Debug)]
struct HashableNode<'a>(&'a Arc<Node>);

impl PartialEq for HashableNode<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.peer().rpc_address.eq(&other.0.peer().rpc_address)
    }
}

impl Eq for HashableNode<'_> {}

impl Hash for HashableNode<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.peer().rpc_address.hash(state);
    }
}

fn simple_replicas(
    token: Token,
    token_map: &BTreeMap<Token, Arc<Node>>,
    replication_factor: usize,
) -> Vec<Arc<Node>> {
    let mut replicas = Vec::new();
    let mut set = HashSet::new();
    for (_, node) in token_map.range(token..).chain(token_map.range(..token)) {
        if set.insert(HashableNode(node)) {
            replicas.push(node.clone());
        }
        if set.len() == replication_factor {
            return replicas;
        }
    }
    replicas
}

fn network_topology_replicas(
    token: Token,
    token_map: &BTreeMap<Token, Arc<Node>>,
    datacenters: &BTreeMap<String, usize>,
    acceptable_repeats: &HashMap<String, usize>,
) -> Vec<Arc<Node>> {
    let expected_replicas: usize = datacenters.values().sum();
    let mut replicas = Vec::new();
    let mut acceptable_repeats = acceptable_repeats.clone();
    let mut by_datacenter: BTreeMap<&str, (HashSet<HashableNode>, HashSet<Option<&str>>)> =
        BTreeMap::new();
    for (_, node) in token_map.range(token..).chain(token_map.range(..token)) {
        let Some(dc) = node.peer().datacenter.as_deref() else {continue};
        let Some(&rf) = datacenters.get(dc) else {continue};
        let (dc_nodes, dc_racks) = by_datacenter.entry(dc).or_default();
        if dc_nodes.len() == rf {
            continue;
        }
        let rack = node.peer().rack.as_deref();
        if !dc_racks.contains(&rack) {
            dc_racks.insert(rack);
            dc_nodes.insert(HashableNode(node));
            replicas.push(node.clone());
        } else if acceptable_repeats[dc] > 0 {
            *acceptable_repeats.get_mut(dc).unwrap() -= 1;
            if dc_nodes.insert(HashableNode(node)) {
                replicas.push(node.clone());
            }
        }
        if replicas.len() == expected_replicas {
            return replicas;
        }
    }
    replicas
}
