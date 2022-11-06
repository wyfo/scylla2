#![warn(missing_debug_implementations)]

extern crate core;

pub mod cql;
pub mod cql_type;
pub mod error;
pub mod event;
pub mod extensions;
pub mod frame;
pub mod options;
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
