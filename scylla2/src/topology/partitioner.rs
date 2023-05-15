use std::{num::Wrapping, str::FromStr};

use bytes::BufMut;
use scylla2_cql::{
    error::ValueTooBig,
    request::query::values::{NamedQueryValues, SerializedQueryValues},
    value::WriteValue,
    AsValue,
};

use crate::{
    error::{PartitionKeyError, UnknownPartitioner},
    utils::tuples,
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
#[non_exhaustive]
pub enum Partitioner {
    #[default]
    Murmur3,
    Cdc,
}

impl FromStr for Partitioner {
    type Err = UnknownPartitioner;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        if name.ends_with("Murmur3Partitioner") {
            Ok(Partitioner::Murmur3)
        } else if name.ends_with("CDCPartitioner") {
            Ok(Partitioner::Cdc)
        } else {
            Err(UnknownPartitioner)
        }
    }
}

impl Partitioner {
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

    pub fn token(
        &self,
        values: &impl SerializePartitionKey,
        pk_indexes: &[u16],
    ) -> Result<Token, PartitionKeyError> {
        let size = values.partition_key_size(pk_indexes)?;
        // TODO is this value appropriate?
        const TOKEN_STACK_BUFFER_SIZE: usize = 256;
        let token = if size > TOKEN_STACK_BUFFER_SIZE {
            let mut buffer = vec![0; size];
            values.serialize_partition_key(pk_indexes, &mut buffer);
            self.hash(&buffer)
        } else {
            let mut buffer = [0; TOKEN_STACK_BUFFER_SIZE];
            values.serialize_partition_key(pk_indexes, &mut buffer);
            self.hash(&buffer)
        };
        Ok(Token(token))
    }
}

pub trait SerializePartitionKey {
    fn partition_key_size(&self, pk_indexes: &[u16]) -> Result<usize, PartitionKeyError>;
    fn serialize_partition_key(&self, pk_indexes: &[u16], slice: &mut [u8]);
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

impl<V> SerializePartitionKey for &[V]
where
    V: AsValue,
{
    fn partition_key_size(&self, pk_indexes: &[u16]) -> Result<usize, PartitionKeyError> {
        let value = |pki| {
            self.get(pki as usize)
                .ok_or(PartitionKeyError::Missing { value: pki })
        };
        match pk_indexes.len() {
            0 => Err(PartitionKeyError::NoPartitionKeyIndexes),
            1 => {
                let pki = pk_indexes[0];
                check_size(pki, value(pki)?)
            }
            _ => pk_indexes
                .iter()
                .cloned()
                .map(|pki| Ok(2 + check_size(pki, value(pki)?)? + 1))
                .sum(),
        }
    }

    fn serialize_partition_key(&self, pk_indexes: &[u16], mut slice: &mut [u8]) {
        match pk_indexes.len() {
            0 => panic!("no partition key indexes"),
            1 => self[pk_indexes[0] as usize]
                .as_value()
                .write_value(&mut slice),
            _ => {
                for &pki in pk_indexes {
                    let value = self[pki as usize].as_value();
                    slice.put_i16(value.value_size().unwrap() as i16);
                    value.write_value(&mut slice);
                    slice.put_u8(0);
                }
            }
        }
    }
}

impl<V> SerializePartitionKey for NamedQueryValues<V>
where
    V: AsValue,
{
    fn partition_key_size(&self, _pk_indexes: &[u16]) -> Result<usize, PartitionKeyError> {
        Err(PartitionKeyError::NoValues)
    }
    fn serialize_partition_key(&self, _pk_indexes: &[u16], _slice: &mut [u8]) {
        panic!("unsupported")
    }
}

impl SerializePartitionKey for () {
    fn partition_key_size(&self, pk_indexes: &[u16]) -> Result<usize, PartitionKeyError> {
        Err(match pk_indexes.first() {
            Some(&value) => PartitionKeyError::Missing { value },
            None => PartitionKeyError::NoPartitionKeyIndexes,
        })
    }
    fn serialize_partition_key(&self, _pk_indexes: &[u16], _slice: &mut [u8]) {
        panic!("unsupported")
    }
}

macro_rules! tuple_values {
    ($($_:ident/$value:ident/$idx:tt),*; $len:literal) => {
        impl<V0, $($value),*> SerializePartitionKey for (V0, $($value),*)
        where
            V0: AsValue,
            $($value: AsValue,)*
        {
            fn partition_key_size(&self, pk_indexes: &[u16]) -> Result<usize, PartitionKeyError> {
                match pk_indexes.len() {
                    0 => Err(PartitionKeyError::NoPartitionKeyIndexes),
                    1 => {
                        match pk_indexes[0] {
                            0 => check_size(0, &self.0),
                            $($idx => check_size($idx, &self.$idx),)*
                            value => Err(PartitionKeyError::Missing { value })
                        }
                    }
                    _ => pk_indexes
                        .iter()
                        .cloned()
                        .map(|pki| {
                            match pki {
                                0 => Ok(2 + check_size(0, &self.0)? + 1),
                                $($idx => Ok(2 + check_size($idx, &self.$idx)? + 1),)*
                                value => Err(PartitionKeyError::Missing { value })
                            }
                        })
                        .sum(),
                }
            }

            fn serialize_partition_key(&self, pk_indexes: &[u16], mut slice: &mut [u8]) {
                match pk_indexes.len() {
                    0 => panic!("no partition key indexes"),
                    1 => {
                        match pk_indexes[0] {
                            0 => self.0.as_value().write_value(&mut slice),
                            $($idx => self.$idx.as_value().write_value(&mut slice),)*
                            _ => panic!("out of bound")
                        }
                    }
                    _ => {
                        for &pki in pk_indexes {
                            match pki {
                                0 => {
                                    let value = &self.0.as_value();
                                    slice.put_i16(value.value_size().unwrap() as i16);
                                    value.write_value(&mut slice);
                                }
                                $($idx => {
                                    let value = &self.$idx.as_value();
                                    slice.put_i16(value.value_size().unwrap() as i16);
                                    value.write_value(&mut slice);
                                })*
                                _ => panic!("out of bound"),
                            }
                            slice.put_u8(0);
                        }
                    }
                }
            }
        }
    };
}

tuples!(tuple_values);

impl SerializePartitionKey for SerializedQueryValues {
    fn partition_key_size(&self, _pk_indexes: &[u16]) -> Result<usize, PartitionKeyError> {
        Err(PartitionKeyError::NoValues)
    }

    fn serialize_partition_key(&self, _pk_indexes: &[u16], _slice: &mut [u8]) {
        panic!("Unsupported")
    }
}
