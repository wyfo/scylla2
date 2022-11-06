use std::{collections::HashMap, io, ops::Deref};

use uuid::Uuid;

use crate::response::error::Error;
pub use crate::response::error::{ErrorCode as DatabaseErrorCode, ErrorKind as DatabaseErrorKind};

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
#[error("Value too big to be serialized ({0} bytes) - maximum is 2GiB")]
pub struct ValueTooBig(pub usize);

#[derive(Debug, thiserror::Error)]
#[error("Frame too big to be sent ({0} bytes) - maximum is 256MiB")]
pub struct FrameTooBig(pub usize);

#[derive(Debug, thiserror::Error)]
#[error("Unexpected type")]
pub struct TypeError;

#[derive(Debug, thiserror::Error)]
#[error("Unexpected null value")]
pub struct NullError;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    NullError(#[from] NullError),
    #[error(transparent)]
    Other(#[from] BoxedError),
}

#[derive(Debug, thiserror::Error)]
pub enum InvalidRequest {
    #[error(transparent)]
    ValueTooBig(#[from] ValueTooBig),
    #[error(transparent)]
    FrameTooBig(#[from] FrameTooBig),
    #[error(
        "Missing result metadata id in EXECUTE, required with v5; statement may have been prepared with v4"
    )]
    MissingResultMetadataId,
    #[error(
        "Batch statements count {statements_count} doesn't match with values count {values_count}"
    )]
    BatchStatementsAndValuesCountNotMatching {
        statements_count: usize,
        values_count: usize,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct DatabaseError {
    pub tracing_id: Option<Uuid>,
    pub custom_payload: HashMap<String, Vec<u8>>,
    pub warnings: Vec<String>,
    pub error: Error,
}

impl Deref for DatabaseError {
    type Target = Error;
    fn deref(&self) -> &Self::Target {
        &self.error
    }
}

#[cfg(feature = "protocol")]
#[derive(Debug, thiserror::Error)]
pub enum AuthenticationError {
    #[error("Authentication is required with authenticator {0}")]
    AuthenticationRequired(String),
    #[error("Unexpected authenticator {0}")]
    UnexpectedAuthenticator(String),
    #[error("Authentication challenge requested with authenticator {0}")]
    ChallengeRequested(String),
    #[error(transparent)]
    Other(#[from] BoxedError),
}

#[cfg(feature = "protocol")]
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error(transparent)]
    AuthenticationError(#[from] AuthenticationError),
    #[error(transparent)]
    InvalidRequest(#[from] InvalidRequest),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Database error: {0}")]
    Database(#[from] Box<DatabaseError>),
}

#[cfg(feature = "protocol")]
impl From<tokio::time::error::Elapsed> for ConnectionError {
    fn from(value: tokio::time::error::Elapsed) -> Self {
        Self::Io(value.into())
    }
}

#[cfg(feature = "protocol")]
#[derive(Debug, thiserror::Error)]
pub enum ReadLoopError<E> {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Callback error: {0:?}")]
    Callback(E),
}
#[cfg(feature = "protocol")]
impl ReadLoopError<io::Error> {
    pub fn into_inner(self) -> io::Error {
        match self {
            Self::Io(error) | Self::Callback(error) => error,
        }
    }
}
