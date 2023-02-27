use std::{fmt, net::SocketAddr};

use crate::error::AuthenticationError;

#[async_trait::async_trait]
pub trait AuthenticationProtocol: fmt::Debug + Send + Sync {
    async fn authenticate(
        &self,
        authenticator: &str,
        addr: SocketAddr,
    ) -> Result<(Vec<u8>, Option<Box<dyn AuthenticationSession>>), AuthenticationError>;
}

#[async_trait::async_trait]
pub trait AuthenticationSession: fmt::Debug + Send + Sync {
    async fn challenge(&mut self, bytes: Option<Box<[u8]>>)
        -> Result<Vec<u8>, AuthenticationError>;
}
