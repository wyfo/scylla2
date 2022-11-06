use std::net::SocketAddr;

use scylla2_cql::{
    error::AuthenticationError,
    protocol::auth::{AuthenticationProtocol, AuthenticationSession},
};

#[derive(Debug)]
pub struct UserPassword {
    pub username: String,
    pub password: String,
}

#[async_trait::async_trait]
impl AuthenticationProtocol for UserPassword {
    async fn authenticate(
        &self,
        authenticator: &str,
        _addr: SocketAddr,
    ) -> Result<(Vec<u8>, Option<Box<dyn AuthenticationSession>>), AuthenticationError> {
        if !matches!(
            authenticator,
            "PasswordAuthenticator"
                | "org.apache.cassandra.auth.PasswordAuthenticator"
                | "com.scylladb.auth.TransitionalAuthenticator"
        ) {
            return Err(AuthenticationError::UnexpectedAuthenticator(
                authenticator.into(),
            ));
        }
        let mut creds = Vec::new();
        creds.push(0);
        creds.extend(self.username.as_bytes());
        creds.push(0);
        creds.extend(self.password.as_bytes());
        Ok((creds, None))
    }
}
