use std::{
    fmt, io,
    net::{IpAddr, SocketAddr},
};

use scylla2_cql::{error::BoxedError, response::result::rows::FromRow};

use crate::{
    debug::Closure, event::SessionEventHandler, topology::partitioner::Token, utils::other_error,
};

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
    NoPort,
    Port(u16),
}

// Is there an interest for AddressTranslator to take &Peer instead of IpAddr?

#[async_trait::async_trait]
pub trait AddressTranslator: fmt::Debug + Send + Sync {
    async fn translate(
        &self,
        address: IpAddr,
    ) -> Result<(SocketAddr, Option<ShardAwarePort>), BoxedError>;
}

#[derive(Debug)]
pub struct ConnectionPort(pub u16);

#[async_trait::async_trait]
impl AddressTranslator for ConnectionPort {
    async fn translate(
        &self,
        address: IpAddr,
    ) -> Result<(SocketAddr, Option<ShardAwarePort>), BoxedError> {
        Ok(((address, self.0).into(), None))
    }
}

#[async_trait::async_trait]
pub(crate) trait AddressTranslatorExt: AddressTranslator {
    async fn translate_or_warn(
        &self,
        address: IpAddr,
        event_handler: &impl SessionEventHandler,
    ) -> io::Result<(SocketAddr, Option<ShardAwarePort>)> {
        match self.translate(address).await {
            Ok(ok) => Ok(ok),
            Err(error) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(%address, error, "Address translation failed");
                let error_str = format!("Address translation failed: {error}");
                event_handler.address_translation_failed(address, error);
                Err(other_error(error_str))
            }
        }
    }
}

impl<T> AddressTranslatorExt for T where T: ?Sized + AddressTranslator {}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, strum::Display)]
pub enum NodeDistance {
    Local,
    Remote,
    Ignored,
}

pub trait NodeLocalizer: fmt::Debug + Send + Sync {
    fn distance(&self, peer: &Peer) -> NodeDistance;
}

impl<F> NodeLocalizer for Closure<F>
where
    F: Send + Sync + Fn(&Peer) -> NodeDistance,
{
    fn distance(&self, peer: &Peer) -> NodeDistance {
        self.0(peer)
    }
}

#[derive(Debug)]
pub(crate) struct AllRemote;

impl NodeLocalizer for AllRemote {
    fn distance(&self, _peer: &Peer) -> NodeDistance {
        NodeDistance::Remote
    }
}

#[derive(Debug)]
pub(crate) struct LocalDatacenter(pub(crate) String);

impl NodeLocalizer for LocalDatacenter {
    fn distance(&self, peer: &Peer) -> NodeDistance {
        peer.datacenter
            .as_ref()
            .filter(|dc| *dc == &self.0)
            .map_or(NodeDistance::Remote, |_| NodeDistance::Local)
    }
}
