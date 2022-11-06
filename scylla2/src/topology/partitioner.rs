use std::{num::Wrapping, sync::Arc};

use arc_swap::ArcSwap;
use bytes::BufMut;
use scylla2_cql::{
    error::ValueTooBig,
    request::query::values::QueryValues,
    value::{convert::AsValue, WriteValue},
};

use crate::{
    error::PartitionKeyError,
    topology::ring::{Partition, Ring},
};

#[derive(Copy, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Hash)]
pub struct Token(pub i64);

impl Token {
    pub fn parse(s: &str) -> Self {
        const EXPECT_I64: &str = "For performance reason, only i64 tokens are supported";
        Self(s.parse().expect(EXPECT_I64))
    }
}

// An implementation of MurmurHash3 ported from Scylla. Please note that this
// is not a "correct" implementation of MurmurHash3 - it replicates the same
// bugs made in the original Cassandra implementation in order to be compatible.
pub fn hash3_x64_128(mut data: &[u8]) -> i128 {
    let length = data.len();

    let c1: Wrapping<i64> = Wrapping(0x87c3_7b91_1142_53d5_u64 as i64);
    let c2: Wrapping<i64> = Wrapping(0x4cf5_ad43_2745_937f_u64 as i64);

    let mut h1 = Wrapping(0_i64);
    let mut h2 = Wrapping(0_i64);

    while data.len() >= 16 {
        let mut k1 = Wrapping(i64::from_le_bytes(data[0..8].try_into().unwrap()));
        let mut k2 = Wrapping(i64::from_le_bytes(data[8..16].try_into().unwrap()));
        data = &data[16..];

        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;

        h1 = rotl64(h1, 27);
        h1 += h2;
        h1 = h1 * Wrapping(5) + Wrapping(0x52dce729);

        k2 *= c2;
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;

        h2 = rotl64(h2, 31);
        h2 += h1;
        h2 = h2 * Wrapping(5) + Wrapping(0x38495ab5);
    }

    let mut k1 = Wrapping(0_i64);
    let mut k2 = Wrapping(0_i64);

    debug_assert!(data.len() < 16);

    if data.len() > 8 {
        for i in (8..data.len()).rev() {
            k2 ^= Wrapping(data[i] as i8 as i64) << ((i - 8) * 8);
        }

        k2 *= c2;
        k2 = rotl64(k2, 33);
        k2 *= c1;
        h2 ^= k2;
    }

    if !data.is_empty() {
        for i in (0..std::cmp::min(8, data.len())).rev() {
            k1 ^= Wrapping(data[i] as i8 as i64) << (i * 8);
        }

        k1 *= c1;
        k1 = rotl64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    }

    h1 ^= Wrapping(length as i64);
    h2 ^= Wrapping(length as i64);

    h1 += h2;
    h2 += h1;

    h1 = fmix(h1);
    h2 = fmix(h2);

    h1 += h2;
    h2 += h1;

    ((h2.0 as i128) << 64) | h1.0 as i128
}

#[inline]
fn rotl64(v: Wrapping<i64>, n: u32) -> Wrapping<i64> {
    Wrapping((v.0 << n) | (v.0 as u64 >> (64 - n)) as i64)
}

#[inline]
fn fmix(mut k: Wrapping<i64>) -> Wrapping<i64> {
    k ^= Wrapping((k.0 as u64 >> 33) as i64);
    k *= Wrapping(0xff51afd7ed558ccd_u64 as i64);
    k ^= Wrapping((k.0 as u64 >> 33) as i64);
    k *= Wrapping(0xc4ceb9fe1a85ec53_u64 as i64);
    k ^= Wrapping((k.0 as u64 >> 33) as i64);

    k
}

#[derive(Debug, Default, Clone)]
pub enum Partitioner {
    #[default]
    Murmur3,
    Cdc,
}

impl Partitioner {
    pub fn from_str(name: &str) -> Option<Self> {
        if name.ends_with("Murmur3Partitioner") {
            Some(Partitioner::Murmur3)
        } else if name.ends_with("CDCPartitioner") {
            Some(Partitioner::Cdc)
        } else {
            None
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Murmur3 => "Murmur3Partitioner",
            Self::Cdc => "CDCPartitioner",
        }
    }

    pub fn hash(&self, data: &[u8]) -> i64 {
        match self {
            Self::Murmur3 => hash3_x64_128(data) as i64,
            Self::Cdc => {
                if data.len() < 8 {
                    i64::MIN
                } else {
                    i64::from_be_bytes(data[..8].try_into().unwrap())
                }
            }
        }
    }

    pub fn token<I>(
        &self,
        values: &impl QueryValues,
        pk_indexes: I,
    ) -> Result<Token, PartitionKeyError>
    where
        I: IntoIterator<Item = u16> + Clone,
        I::IntoIter: ExactSizeIterator,
    {
        let size = serialized_partition_key_size(values, pk_indexes.clone().into_iter())?;
        // TODO is this value appropriate?
        const TOKEN_STACK_BUFFER_SIZE: usize = 256;
        let token = if size > TOKEN_STACK_BUFFER_SIZE {
            let mut buffer = vec![0; size];
            serialize_partition_key(values, pk_indexes.into_iter(), &mut buffer[..]);
            self.hash(&buffer)
        } else {
            let mut buffer = [0; TOKEN_STACK_BUFFER_SIZE];
            serialize_partition_key(values, pk_indexes.into_iter(), &mut buffer[..]);
            self.hash(&buffer)
        };
        Ok(Token(token))
    }
}

trait PartitioningValues {
    fn token(
        &self,
        pk_indexes: &[u16],
        partitioner: &Partitioner,
    ) -> Option<Result<Token, PartitionKeyError>>;
}

fn check_size(pki: u16, value: impl AsValue) -> Result<usize, PartitionKeyError> {
    match value.as_value().value_size() {
        Ok(0) => Err(PartitionKeyError::Null { value: pki }),
        Ok(size) if size <= i16::MAX as usize => Ok(size),
        Ok(size) | Err(ValueTooBig(size)) => {
            Err(PartitionKeyError::ValueTooBig { value: pki, size })
        }
    }
}

#[derive(Debug)]
pub struct Partitioning {
    pub(crate) partitioner: Partitioner,
    pub(crate) ring: Arc<ArcSwap<Ring>>,
}

impl Partitioning {
    pub fn partitioner(&self) -> &Partitioner {
        &self.partitioner
    }

    pub fn ring(&self) -> Arc<Ring> {
        self.ring.load_full()
    }

    pub fn get_partition<I>(
        &self,
        values: &impl QueryValues,
        pk_indexes: I,
    ) -> Result<Partition, PartitionKeyError>
    where
        I: IntoIterator<Item = u16> + Clone,
        I::IntoIter: ExactSizeIterator,
    {
        let token = self.partitioner.token(values, pk_indexes);
        token.map(|tk| self.ring().get_partition(tk))
    }
}

fn serialized_partition_key_size(
    values: impl QueryValues,
    mut pk_indexes: impl Iterator<Item = u16> + ExactSizeIterator,
) -> Result<usize, PartitionKeyError> {
    let check_size = |pki| {
        let check = |value: &dyn WriteValue| match value.value_size() {
            Ok(0) => Err(PartitionKeyError::Null { value: pki }),
            Ok(size) if size <= i16::MAX as usize => Ok(size),
            Ok(size) | Err(ValueTooBig(size)) => {
                Err(PartitionKeyError::ValueTooBig { value: pki, size })
            }
        };
        values
            .with_value(pki, check)
            .ok_or(PartitionKeyError::Missing { value: pki })?
    };
    match pk_indexes.len() {
        0 => panic!("no partition key indexes"),
        1 => check_size(pk_indexes.next().unwrap()),
        _ => pk_indexes.map(|pki| Ok(2 + check_size(pki)? + 1)).sum(),
    }
}

fn serialize_partition_key(
    values: impl QueryValues,
    mut pk_indexes: impl Iterator<Item = u16> + ExactSizeIterator,
    mut slice: &mut [u8],
) {
    match pk_indexes.len() {
        0 => panic!("no partition key indexes"),
        1 => values
            .with_value(pk_indexes.next().unwrap(), |v| v.write_value(&mut slice))
            .unwrap(),
        _ => {
            for pki in pk_indexes {
                values
                    .with_value(pki, |value: &dyn WriteValue| {
                        let size = value.value_size().unwrap();
                        slice.put_i16(size as i16);
                        value.write_value(&mut slice);
                        slice.put_u8(0);
                    })
                    .unwrap();
            }
        }
    }
}
