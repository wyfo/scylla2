#![warn(missing_debug_implementations)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::map_unwrap_or)]
#![deny(clippy::semicolon_if_nothing_returned)]

extern crate core;

use std::fmt;

pub mod cql;
pub mod cql_type;
pub mod error;
pub mod event;
pub mod extensions;
pub mod frame;
#[cfg(feature = "protocol")]
pub mod protocol;
pub mod request;
pub mod response;
mod utils;
pub mod value;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, strum::EnumIter)]
#[non_exhaustive]
pub enum ProtocolVersion {
    V4,
    V5,
}
pub const CLIENT_V4: u8 = 0x04;
pub const CLIENT_V5: u8 = 0x05;
pub const SERVER_V4: u8 = 0x84;
pub const SERVER_V5: u8 = 0x85;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct VersionByte(pub u8);

impl ProtocolVersion {
    pub fn client(self) -> VersionByte {
        VersionByte(match self {
            Self::V4 => 0x04,
            Self::V5 => 0x05,
        })
    }

    pub fn server(self) -> VersionByte {
        VersionByte(match self {
            Self::V4 => 0x84,
            Self::V5 => 0x85,
        })
    }
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

impl fmt::Display for LegacyConsistency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Regular(c) => write!(f, "{c:?}"),
            Self::Serial(c) => write!(f, "{c:?}"),
        }
    }
}

impl From<LegacyConsistency> for u16 {
    fn from(c: LegacyConsistency) -> u16 {
        match c {
            LegacyConsistency::Regular(c) => c as u16,
            LegacyConsistency::Serial(c) => c as u16,
        }
    }
}
