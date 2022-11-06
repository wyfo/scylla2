use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::{Hash, Hasher},
    net::SocketAddr,
    ops::Range,
    sync::Arc,
};

use crate::{
    error::BoxedError,
    topology::{node::Node, partitioner::Token, peer::NodeDistance, NodeByDistance},
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum ReplicationStrategy {
    SimpleStrategy {
        replication_factor: usize,
    },
    NetworkTopologyStrategy {
        datacenters: BTreeMap<String, usize>,
    },
}

impl Default for ReplicationStrategy {
    fn default() -> Self {
        Self::SimpleStrategy {
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
            Ok(ReplicationStrategy::NetworkTopologyStrategy { datacenters })
        } else if class.ends_with("SimpleStrategy") {
            let replication_factor = replication
                .get("replication_factor")
                .ok_or("Missing replication_factor")?
                .parse()?;
            Ok(ReplicationStrategy::SimpleStrategy { replication_factor })
        } else {
            Err("Invalid replication class".into())
        }
    }
}

#[derive(Debug, Default, Clone)]
struct PartitionOffsets {
    local: Range<u16>,
    remote: Range<u16>,
}

#[derive(Debug)]
pub struct Ring {
    partitions: BTreeMap<Token, PartitionOffsets>,
    local_combinations: Box<[Arc<Node>]>,
    remote_combinations: Box<[Arc<Node>]>,
}

impl Ring {
    pub fn get_partition(self: Arc<Self>, token: Token) -> Partition {
        let offsets = self
            .partitions
            .range(token..)
            .next()
            .map(|(_, c)| c)
            .or_else(|| self.partitions.values().next())
            .unwrap()
            .clone();
        Partition {
            token,
            ring: self,
            offsets,
        }
    }
}

struct HashableNode<'a>(&'a Arc<Node>);

impl PartialEq for HashableNode<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.address().eq(&other.0.address())
    }
}

impl Eq for HashableNode<'_> {}

impl PartialOrd for HashableNode<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.address().partial_cmp(&other.0.address())
    }
}

impl Ord for HashableNode<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.address().cmp(&other.0.address())
    }
}

impl Hash for HashableNode<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.address().hash(state)
    }
}

#[derive(Debug, Clone)]
pub struct Partition {
    token: Token,
    ring: Arc<Ring>,
    offsets: PartitionOffsets,
}

impl Partition {
    pub fn token(&self) -> Token {
        self.token
    }
}

impl NodeByDistance for Partition {
    fn local_nodes(&self) -> &[Arc<Node>] {
        let range = self.offsets.local.start as usize..self.offsets.local.end as usize;
        &self.ring.local_combinations[range]
    }

    fn remote_nodes(&self) -> &[Arc<Node>] {
        let range = self.offsets.remote.start as usize..self.offsets.remote.end as usize;
        &self.ring.remote_combinations[range]
    }
}

impl Ring {
    pub(crate) fn new(nodes: &[Arc<Node>], strategy: &ReplicationStrategy) -> Self {
        let token_ring: BTreeMap<_, _> = nodes
            .iter()
            .flat_map(|node| node.peer().tokens.iter().map(move |tk| (*tk, node)))
            .collect();
        let mut combinations: HashMap<BTreeSet<HashableNode>, Vec<Token>> = HashMap::new();
        match strategy {
            ReplicationStrategy::SimpleStrategy { replication_factor } => {
                for token in token_ring.keys().cloned() {
                    let mut combination = BTreeSet::new();
                    for (_, node) in token_ring.range(token..).chain(token_ring.range(..token)) {
                        combination.insert(HashableNode(node));
                        if combination.len() == *replication_factor {
                            break;
                        }
                    }
                    combinations.entry(combination).or_default().push(token);
                }
            }
            ReplicationStrategy::NetworkTopologyStrategy { datacenters } => {
                let all_racks: HashMap<&str, HashSet<&str>> = nodes
                    .iter()
                    .filter_map(|n| {
                        Some((n.peer().datacenter.as_deref()?, n.peer().rack.as_deref()?))
                    })
                    .fold(HashMap::new(), |mut map, (dc, rack)| {
                        map.entry(dc).or_default().insert(rack);
                        map
                    });
                let acceptable_repeats: HashMap<_, _> = datacenters
                    .iter()
                    .map(|(dc, rf)| (dc.as_str(), rf.saturating_sub(all_racks[dc.as_str()].len())))
                    .collect();
                for token in token_ring.keys().cloned() {
                    let mut acceptable_repeats = acceptable_repeats.clone();
                    let mut racks: HashSet<Option<&str>> = HashSet::new();
                    let mut by_datacenter: BTreeMap<&str, BTreeSet<_>> = BTreeMap::new();
                    for (_, node) in token_ring.range(token..).chain(token_ring.range(..token)) {
                        let Some(dc) = node.peer().datacenter.as_deref() else {continue};
                        let Some(&rf) = datacenters.get(dc) else {continue};
                        let dc_nodes = by_datacenter.entry(dc).or_default();
                        if dc_nodes.len() == rf {
                            continue;
                        }
                        let rack = node.peer().rack.as_deref();
                        if racks.contains(&rack) {
                            racks.insert(rack);
                            dc_nodes.insert(HashableNode(node));
                        } else if acceptable_repeats[dc] > 0 {
                            *acceptable_repeats.get_mut(dc).unwrap() -= 1;
                            dc_nodes.insert(HashableNode(node));
                        }
                    }
                    let combination = by_datacenter.into_values().flatten().collect();
                    combinations.entry(combination).or_default().push(token);
                }
            }
        }
        let mut partitions = BTreeMap::new();
        let mut local_combinations = Vec::new();
        let mut remote_combinations = Vec::new();
        let mut local_ranges = HashMap::new();
        let mut remote_ranges = HashMap::new();
        for (nodes, tokens) in combinations {
            let range_by_distance =
                |dist,
                 combinations: &mut Vec<Arc<Node>>,
                 ranges: &mut HashMap<Vec<SocketAddr>, Range<u16>>| {
                    let combinations_len = combinations.len();
                    combinations.extend(
                        nodes
                            .iter()
                            .map(|n| n.0.clone())
                            .filter(|n| n.distance() == dist),
                    );
                    let addrs: Vec<_> = combinations[combinations_len..]
                        .iter()
                        .map(|n| n.address())
                        .collect();
                    if addrs.is_empty() {
                        return 0..0;
                    } else if let Some(range) = ranges.get(&addrs) {
                        combinations.truncate(combinations_len);
                        return range.clone();
                    }
                    let range = combinations_len as u16..combinations.len() as u16;
                    ranges.insert(addrs, range.clone());
                    range
                };
            let local = range_by_distance(
                NodeDistance::Local,
                &mut local_combinations,
                &mut local_ranges,
            );
            let remote = range_by_distance(
                NodeDistance::Remote,
                &mut remote_combinations,
                &mut remote_ranges,
            );
            for token in tokens {
                partitions.insert(
                    token,
                    PartitionOffsets {
                        local: local.clone(),
                        remote: remote.clone(),
                    },
                );
            }
        }
        Ring {
            partitions,
            local_combinations: local_combinations.into(),
            remote_combinations: remote_combinations.into(),
        }
    }
}
