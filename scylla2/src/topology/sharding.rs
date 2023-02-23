use std::{
    num::NonZeroU16,
    sync::atomic::{AtomicU16, Ordering},
};

use scylla2_cql::response::supported::Supported;

use crate::topology::partitioner::Token;

#[derive(Copy, Clone, Eq, PartialEq)]
pub(crate) struct ShardInfo {
    pub(crate) shard_aware_port: u16,
    pub(crate) nr_shards: NonZeroU16,
    pub(crate) shard: u16,
}

impl ShardInfo {
    pub(crate) fn shard_aware_source_port(self) -> impl Iterator<Item = u16> {
        static PORT_OFFSET: AtomicU16 = AtomicU16::new(0);
        let nr_shards = self.nr_shards.get();
        let start = (49152 + nr_shards) / nr_shards * nr_shards;
        let offset = PORT_OFFSET.fetch_add(1, Ordering::Relaxed) % ((65535 - 49152) / nr_shards);
        let first_port = start + offset * nr_shards;
        Iterator::chain(
            (first_port..65535).step_by(nr_shards as usize),
            (start..first_port).step_by(nr_shards as usize),
        )
        .map(move |port| port + self.shard)
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum ShardingAlgorithm {
    BiasedTokenRoundRobin { ignore_msb: u8 },
}

impl ShardingAlgorithm {
    fn compute_shard(&self, token: Token, nr_shards: u16) -> u16 {
        match self {
            Self::BiasedTokenRoundRobin { ignore_msb } => {
                let mut biased_token = (token.0 as u64).wrapping_add(1u64 << 63);
                biased_token <<= *ignore_msb;
                (((biased_token as u128) * (nr_shards as u128)) >> 64) as u16
            }
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub(crate) struct Sharder {
    nr_shards: NonZeroU16,
    algorithm: ShardingAlgorithm,
}

impl Sharder {
    pub(crate) fn from_supported(supported: &Supported) -> Option<Self> {
        Some(Self {
            nr_shards: NonZeroU16::new(supported.get("SCYLLA_NR_SHARDS")?)?,
            algorithm: match supported
                .get::<String>("SCYLLA_SHARDING_ALGORITHM")?
                .as_str()
            {
                "biased-token-round-robin" => ShardingAlgorithm::BiasedTokenRoundRobin {
                    ignore_msb: supported.get("SCYLLA_SHARDING_IGNORE_MSB")?,
                },
                _ => return None,
            },
        })
    }

    pub fn nr_shards(&self) -> NonZeroU16 {
        self.nr_shards
    }

    pub fn compute_shard(&self, token: Token) -> u16 {
        self.algorithm.compute_shard(token, self.nr_shards.get())
    }
}
