use std::io;

use crate::{cql::ReadCql, extensions::ProtocolExtensions, ProtocolVersion};

#[derive(Debug)]
#[non_exhaustive]
pub struct Authenticate {
    pub authenticator: String,
}

impl Authenticate {
    pub fn deserialize(
        _version: ProtocolVersion,
        _extensions: ProtocolExtensions,
        mut slice: &[u8],
    ) -> io::Result<Self> {
        let authenticator = String::read_cql(&mut slice)?;
        Ok(Self { authenticator })
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct AuthChallenge {
    pub token: Option<Box<[u8]>>,
}

impl AuthChallenge {
    pub fn deserialize(
        _version: ProtocolVersion,
        _extensions: ProtocolExtensions,
        mut slice: &[u8],
    ) -> io::Result<Self> {
        let token = Option::<&[u8]>::read_cql(&mut slice)?.map(Into::into);
        Ok(Self { token })
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub struct AuthSuccess {
    pub token: Option<Vec<u8>>,
}

impl AuthSuccess {
    pub fn deserialize(
        _version: ProtocolVersion,
        _extensions: ProtocolExtensions,
        mut slice: &[u8],
    ) -> io::Result<Self> {
        let token = Option::<&[u8]>::read_cql(&mut slice)?.map(Into::into);
        Ok(Self { token })
    }
}
