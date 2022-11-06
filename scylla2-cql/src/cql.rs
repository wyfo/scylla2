use std::{
    collections::HashMap,
    fmt, io, mem,
    net::{IpAddr, SocketAddr},
    str,
};

use bytes::BufMut;
use enumflags2::{BitFlag, BitFlags};
use uuid::Uuid;

use crate::{error::ValueTooBig, utils::invalid_data};

fn check_size<I>(size: impl TryInto<I> + Into<usize> + Copy) -> Result<I, ValueTooBig> {
    size.try_into().map_err(|_| ValueTooBig(size.into()))
}

// There is no Buf method for this as Buf handle discontinuous buffers
#[inline]
fn read_and_advance<'a, S>(buf: &mut &'a [u8], len: S) -> io::Result<&'a [u8]>
where
    S: Into<u32>,
{
    let size = len.into() as usize;
    if size > buf.len() {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    let (a, b) = buf.split_at(size);
    *buf = b;
    Ok(a)
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, strum::FromRepr)]
#[repr(u16)]
#[non_exhaustive]
pub enum Consistency {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    #[default]
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    LocalOne = 0x000A,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, strum::FromRepr)]
#[repr(u16)]
#[non_exhaustive]
pub enum SerialConsistency {
    Serial = 0x0008,
    LocalSerial = 0x0009,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum LegacyConsistency {
    Regular(Consistency),
    Serial(SerialConsistency),
}

impl From<LegacyConsistency> for u16 {
    fn from(c: LegacyConsistency) -> u16 {
        match c {
            LegacyConsistency::Regular(c) => c as u16,
            LegacyConsistency::Serial(c) => c as u16,
        }
    }
}

impl fmt::Display for LegacyConsistency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Regular(c) => write!(f, "{c:?}"),
            Self::Serial(c) => write!(f, "{c:?}"),
        }
    }
}

pub(crate) trait WriteCql: Sized {
    fn cql_size(&self) -> Result<usize, ValueTooBig>;
    fn write_cql(&self, buf: &mut &mut [u8]);
}
pub(crate) trait ReadCql<'a>: Sized {
    fn read_cql(buf: &mut &'a [u8]) -> io::Result<Self>;
}

impl WriteCql for &[u8] {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        let len: i32 = check_size(self.len())?;
        Ok(len.cql_size()? + len as usize)
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (self.len() as i32).write_cql(buf);
        buf.put_slice(self);
    }
}
impl<'a> ReadCql<'a> for &'a [u8] {
    fn read_cql(buf: &mut &'a [u8]) -> io::Result<Self> {
        let len = u32::try_from(i32::read_cql(buf)?).map_err(invalid_data)?;
        read_and_advance(buf, len)
    }
}

pub(crate) struct ShortBytes<'a>(pub &'a [u8]);

impl WriteCql for ShortBytes<'_> {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        // TODO ValuesTooBig is not the right error, because it's i16 and not i32,
        // but it should never happen
        debug_assert!(self.0.len() < i16::MAX as usize);
        let len: i16 = check_size(self.0.len())?;
        Ok(len.cql_size()? + len as usize)
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (self.0.len() as i16).write_cql(buf);
        buf.put_slice(self.0);
    }
}
impl<'a> ReadCql<'a> for ShortBytes<'a> {
    fn read_cql(buf: &mut &'a [u8]) -> io::Result<Self> {
        let len = u16::read_cql(buf)?;
        read_and_advance(buf, len).map(ShortBytes)
    }
}

impl WriteCql for &str {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        debug_assert!(self.len() < i16::MAX as usize);
        let len: i16 = check_size(self.len())?;
        Ok(len.cql_size()? + len as usize)
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (self.len() as i16).write_cql(buf);
        buf.put_slice(self.as_bytes());
    }
}
impl<'a> ReadCql<'a> for &'a str {
    fn read_cql(buf: &mut &'a [u8]) -> io::Result<Self> {
        let len = u16::read_cql(buf)?;
        str::from_utf8(read_and_advance(buf, len)?).map_err(invalid_data)
    }
}

pub(crate) struct LongString<'a>(pub &'a str);

impl WriteCql for LongString<'_> {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        let len: i32 = check_size(self.0.len())?;
        Ok(len.cql_size()? + len as usize)
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (self.0.len() as i32).write_cql(buf);
        buf.put_slice(self.0.as_bytes());
    }
}
impl<'a> ReadCql<'a> for LongString<'a> {
    fn read_cql(buf: &mut &'a [u8]) -> io::Result<Self> {
        let len = u32::try_from(i32::read_cql(buf)?).map_err(invalid_data)?;
        str::from_utf8(read_and_advance(buf, len)?)
            .map_err(invalid_data)
            .map(LongString)
    }
}

impl WriteCql for Consistency {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        (*self as u16).cql_size()
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (*self as u16).write_cql(buf);
    }
}

impl WriteCql for SerialConsistency {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        (*self as u16).cql_size()
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (*self as u16).write_cql(buf);
    }
}

impl WriteCql for LegacyConsistency {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        u16::from(*self).cql_size()
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        u16::from(*self).write_cql(buf)
    }
}

impl ReadCql<'_> for LegacyConsistency {
    fn read_cql(buf: &mut &'_ [u8]) -> io::Result<Self> {
        let c = u16::read_cql(buf)?;
        Consistency::from_repr(c)
            .map(Self::Regular)
            .or_else(|| SerialConsistency::from_repr(c).map(Self::Serial))
            .ok_or("Invalid consistency")
            .map_err(invalid_data)
    }
}

impl<const N: usize> WriteCql for [u8; N] {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        Ok(N)
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        buf.put_slice(self);
    }
}
impl<const N: usize> ReadCql<'_> for [u8; N] {
    fn read_cql(buf: &mut &'_ [u8]) -> io::Result<Self> {
        Ok(read_and_advance(buf, N as u32)?.try_into().unwrap())
    }
}

impl WriteCql for Uuid {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        self.as_bytes().cql_size()
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        self.into_bytes().write_cql(buf)
    }
}
impl ReadCql<'_> for Uuid {
    fn read_cql(buf: &mut &'_ [u8]) -> io::Result<Self> {
        ReadCql::read_cql(buf).map(Uuid::from_bytes)
    }
}

impl ReadCql<'_> for IpAddr {
    fn read_cql(buf: &mut &'_ [u8]) -> io::Result<Self> {
        Ok(match i8::read_cql(buf)? {
            4 => IpAddr::V4(<[u8; 4]>::read_cql(buf)?.into()),
            16 => IpAddr::V6(<[u8; 16]>::read_cql(buf)?.into()),
            _ => return Err(invalid_data("Invalid IP length")),
        })
    }
}

impl WriteCql for SocketAddr {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        let ip_size = match self {
            Self::V4(_) => 4,
            Self::V6(_) => 16,
        };
        Ok(0i8.cql_size()? + ip_size + 0i32.cql_size()?)
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        match self {
            Self::V4(addr) => {
                4i8.write_cql(buf);
                addr.ip().octets().write_cql(buf);
            }
            Self::V6(addr) => {
                16i8.write_cql(buf);
                addr.ip().octets().write_cql(buf);
            }
        }
        (self.port() as i32).write_cql(buf);
    }
}
impl ReadCql<'_> for SocketAddr {
    fn read_cql(buf: &mut &'_ [u8]) -> io::Result<Self> {
        let ip = match i8::read_cql(buf)? {
            4 => IpAddr::V4(<[u8; 4]>::read_cql(buf)?.into()),
            16 => IpAddr::V6(<[u8; 16]>::read_cql(buf)?.into()),
            _ => return Err(invalid_data("Invalid IP length")),
        };
        Ok(SocketAddr::new(ip, i32::read_cql(buf)? as u16))
    }
}

macro_rules! int_cql {
    ($($int:ty),*) => {
        $(
        impl WriteCql for $int {
            fn cql_size(&self) -> Result<usize, ValueTooBig> {
                Ok(mem::size_of::<$int>())
            }

            fn write_cql(&self, buf: &mut &mut [u8]) {
                buf.put_slice(&self.to_be_bytes());
            }
        }

        impl ReadCql<'_> for $int {
            fn read_cql(buf: &mut &'_ [u8]) -> io::Result<Self> {
                Ok(<$int>::from_be_bytes(
                    read_and_advance(buf, mem::size_of::<$int>() as u32)?
                        .try_into()
                        .unwrap(),
                ))
            }
        }
        )*
    };
}

int_cql!(u8, i8, u16, i16, u32, i32, u64, i64);

impl WriteCql for bool {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        (*self as u8).cql_size()
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (*self as u8).write_cql(buf)
    }
}
impl ReadCql<'_> for bool {
    fn read_cql(buf: &mut &'_ [u8]) -> io::Result<Self> {
        match u8::read_cql(buf)? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("Invalid boolean value").map_err(invalid_data),
        }
    }
}

impl ReadCql<'_> for String {
    fn read_cql(buf: &mut &'_ [u8]) -> io::Result<Self> {
        Ok(<&str>::read_cql(buf)?.into())
    }
}

impl<'a> ReadCql<'a> for Option<&'a [u8]> {
    fn read_cql(buf: &mut &'a [u8]) -> io::Result<Self> {
        let len = i32::read_cql(buf)?;
        Ok(if len < 0 {
            None
        } else {
            Some(read_and_advance(buf, len as u32)?)
        })
    }
}

impl WriteCql for Vec<String> {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        let len: i16 = check_size(self.len())?;
        Ok(len.cql_size()?
            + self
                .iter()
                .map(|s| s.as_str().cql_size())
                .sum::<Result<usize, _>>()?)
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (self.len() as i16).write_cql(buf);
        self.iter().for_each(|s| s.as_str().write_cql(buf));
    }
}

impl<'a> ReadCql<'a> for Vec<String> {
    fn read_cql(buf: &mut &'a [u8]) -> io::Result<Self> {
        let capa = i16::read_cql(buf)? as usize;
        let mut vec = Vec::with_capacity(capa);
        // /!\ DO NOT USE `vec.capacity()` (with_capacity can allocate more than its parameter)
        for _ in 0..capa {
            vec.push(String::read_cql(buf)?);
        }
        Ok(vec)
    }
}

impl WriteCql for HashMap<String, String> {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        let len: i16 = check_size(self.len())?;
        Ok(len.cql_size()?
            + self
                .iter()
                .map(|(k, v)| Ok(k.as_str().cql_size()? + v.as_str().cql_size()?))
                .sum::<Result<usize, _>>()?)
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (self.len() as i16).write_cql(buf);
        for (k, v) in self.iter() {
            k.as_str().write_cql(buf);
            v.as_str().write_cql(buf);
        }
    }
}
impl<'a> ReadCql<'a> for HashMap<String, String> {
    fn read_cql(buf: &mut &'a [u8]) -> io::Result<Self> {
        let capa = i16::read_cql(buf)? as usize;
        let mut map = HashMap::with_capacity(capa);
        // /!\ DO NOT USE `map.capacity()` (with_capacity can allocate more than its parameter)
        for _ in 0..capa {
            map.insert(String::read_cql(buf)?, String::read_cql(buf)?);
        }
        Ok(map)
    }
}

impl WriteCql for HashMap<String, Vec<String>> {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        let len: i16 = check_size(self.len())?;
        Ok(len.cql_size()?
            + self
                .iter()
                .map(|(k, v)| Ok(k.as_str().cql_size()? + v.cql_size()?))
                .sum::<Result<usize, _>>()?)
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (self.len() as i16).write_cql(buf);
        for (k, v) in self.iter() {
            k.as_str().write_cql(buf);
            v.write_cql(buf);
        }
    }
}
impl<'a> ReadCql<'a> for HashMap<String, Vec<String>> {
    fn read_cql(buf: &mut &'a [u8]) -> io::Result<Self> {
        let capa = i16::read_cql(buf)? as usize;
        let mut map = HashMap::with_capacity(capa);
        // /!\ DO NOT USE `map.capacity()` (with_capacity can allocate more than its parameter)
        for _ in 0..capa {
            map.insert(String::read_cql(buf)?, <Vec<String>>::read_cql(buf)?);
        }
        Ok(map)
    }
}

impl WriteCql for HashMap<String, Vec<u8>> {
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        let len: i16 = check_size(self.len())?;
        Ok(len.cql_size()?
            + self
                .iter()
                .map(|(k, v)| Ok(k.as_str().cql_size()? + v.as_slice().cql_size()?))
                .sum::<Result<usize, _>>()?)
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (self.len() as i16).write_cql(buf);
        for (k, v) in self.iter() {
            k.as_str().write_cql(buf);
            v.as_slice().write_cql(buf);
        }
    }
}
impl<'a> ReadCql<'a> for HashMap<String, Vec<u8>> {
    fn read_cql(buf: &mut &'a [u8]) -> io::Result<Self> {
        let capa = i16::read_cql(buf)? as usize;
        let mut map = HashMap::with_capacity(capa);
        for _ in 0..capa {
            map.insert(String::read_cql(buf)?, <&[u8]>::read_cql(buf)?.into());
        }
        Ok(map)
    }
}

impl<T> WriteCql for BitFlags<T>
where
    T: BitFlag,
    T::Numeric: WriteCql,
{
    fn cql_size(&self) -> Result<usize, ValueTooBig> {
        (*self).bits().cql_size()
    }

    fn write_cql(&self, buf: &mut &mut [u8]) {
        (*self).bits().write_cql(buf)
    }
}

impl<'a, T> ReadCql<'a> for BitFlags<T>
where
    T: BitFlag + Send + Sync + fmt::Debug,
    T::Numeric: Send + Sync + ReadCql<'a>,
{
    fn read_cql(buf: &mut &'a [u8]) -> io::Result<Self> {
        BitFlags::from_bits(T::Numeric::read_cql(buf)?).map_err(invalid_data)
    }
}
