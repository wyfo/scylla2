use std::net::{IpAddr, SocketAddr};

use scylla2_cql::{error::BoxedError, response::result::rows::FromRow};

use crate::topology::partitioner::Token;

#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Peer {
    pub rpc_address: IpAddr,
    pub datacenter: Option<String>,
    pub rack: Option<String>,
    pub tokens: Vec<Token>,
}

impl FromRow<'_> for Peer {
    type Row = (IpAddr, Option<String>, Option<String>, Vec<String>);

    fn from_row(value: Self::Row) -> Result<Self, BoxedError> {
        let tokens = value.3.iter().map(|tk| Token::parse(tk)).collect();
        Ok(Peer {
            rpc_address: value.0,
            datacenter: value.1,
            rack: value.2,
            tokens,
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ShardAwarePort {
    NoShardAwarePort,
    ShardAwarePort(u16),
}

#[async_trait::async_trait]
pub trait AddressTranslator: Send + Sync {
    async fn translate(
        &self,
        peer: &Peer,
    ) -> Result<(SocketAddr, Option<ShardAwarePort>), BoxedError>;
}

#[async_trait::async_trait]
impl<F> AddressTranslator for F
where
    F: Send + Sync + Fn(&Peer) -> Result<(SocketAddr, Option<ShardAwarePort>), BoxedError>,
{
    async fn translate(
        &self,
        peer: &Peer,
    ) -> Result<(SocketAddr, Option<ShardAwarePort>), BoxedError> {
        self(peer)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, strum::Display)]
pub enum NodeDistance {
    Local,
    Remote,
    Ignored,
}

#[async_trait::async_trait]
pub trait NodeLocalizer: Send + Sync {
    async fn distance(&self, peer: &Peer, address: SocketAddr) -> NodeDistance;
}

#[async_trait::async_trait]
impl<F> NodeLocalizer for F
where
    F: Send + Sync + Fn(&Peer, SocketAddr) -> NodeDistance,
{
    async fn distance(&self, peer: &Peer, address: SocketAddr) -> NodeDistance {
        self(peer, address)
    }
}
