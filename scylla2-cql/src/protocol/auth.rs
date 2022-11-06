use std::{future::Future, net::SocketAddr};

use crate::error::AuthenticationError;

#[async_trait::async_trait]
pub trait AuthenticationProtocol: Send + Sync {
    async fn authenticate(
        &self,
        authenticator: &str,
        addr: SocketAddr,
    ) -> Result<(Vec<u8>, Option<Box<dyn AuthenticationSession>>), AuthenticationError>;
}

#[async_trait::async_trait]
pub trait AuthenticationSession: Send + Sync {
    async fn challenge(&mut self, bytes: Option<Box<[u8]>>)
        -> Result<Vec<u8>, AuthenticationError>;
}

#[async_trait::async_trait]
impl<F, Fut> AuthenticationSession for F
where
    F: Send + Sync + FnMut(Option<Box<[u8]>>) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, AuthenticationError>> + Send,
{
    async fn challenge(
        &mut self,
        bytes: Option<Box<[u8]>>,
    ) -> Result<Vec<u8>, AuthenticationError> {
        self(bytes).await
    }
}
