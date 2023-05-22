use std::io;

use scylla2_cql::event::SchemaChangeEvent;
use tokio::time::error::Elapsed;

use crate::execution::retry::RetryableError;

#[rustfmt::skip]
pub use scylla2_cql::error::*;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
#[error("Keyspace must match `[a-zA-Z_0-9]{{1, 48}}`")]
pub struct InvalidKeyspace;

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("No nodes have been provided")]
    NoNodes,
    #[error(transparent)]
    InvalidKeyspace(#[from] InvalidKeyspace),
    #[error(transparent)]
    Connection(#[from] ConnectionError),
}

impl From<UseKeyspaceError> for SessionError {
    fn from(value: UseKeyspaceError) -> Self {
        match value {
            UseKeyspaceError::InvalidKeyspace(error) => error.into(),
            UseKeyspaceError::Database(error) => Self::Connection(error.into()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UseKeyspaceError {
    #[error(transparent)]
    InvalidKeyspace(#[from] InvalidKeyspace),
    #[error("Database error: {0}")]
    Database(#[from] Box<DatabaseError>),
}

#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    #[error(transparent)]
    InvalidRequest(#[from] InvalidRequest),
    #[error("No stream is available, request has not been sent")]
    NoStreamAvailable,
    #[error("Connection is closed, request has not been sent")]
    ConnectionClosed,
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error(transparent)]
    PartitionKey(#[from] PartitionKeyError),
    #[error(transparent)]
    InvalidRequest(#[from] InvalidRequest),
    #[error("No connection available")]
    NoConnection,
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Database error: {0}")]
    Database(#[from] Box<DatabaseError>),
    #[error("Schema agreement timeout {0:?}")]
    SchemaAgreementTimeout(SchemaChangeEvent),
}

impl ExecutionError {
    pub fn as_database_error_kind(&self) -> Result<&DatabaseErrorKind, &ExecutionError> {
        match self {
            Self::Database(error) => Ok(&error.kind),
            other => Err(other),
        }
    }

    pub fn retryable(&self) -> Option<RetryableError<'_>> {
        match self {
            Self::Io(err) => Some(RetryableError::Io(err)),
            Self::Database(err) => Some(RetryableError::Database(err)),
            _ => None,
        }
    }
}

impl From<Elapsed> for ExecutionError {
    fn from(value: Elapsed) -> Self {
        Self::Io(value.into())
    }
}

impl From<RequestError> for ExecutionError {
    fn from(value: RequestError) -> Self {
        match value {
            RequestError::ConnectionClosed | RequestError::NoStreamAvailable => Self::NoConnection,
            RequestError::Io(io) => io.into(),
            RequestError::InvalidRequest(invalid) => invalid.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Ongoing USE KEYSPACE query")]
pub struct OngoingUseKeyspace;

#[derive(Debug, thiserror::Error)]
pub enum PartitionKeyError {
    #[error("No values to match partition key indexes. If you provided named query values, use positional instead (you may need to map your name on their position, using prepared statement metadata). Pre-serialized query values are also not supported")]
    NoValues,
    #[error("No partition key indexes")]
    NoPartitionKeyIndexes,
    #[error("Missing value {value}")]
    Missing { value: u16 },
    #[error("Partition key value {value} cannot be null")]
    Null { value: u16 },
    #[error("Partition key value {value} serialized size cannot exceed 64KiB, found {size}")]
    ValueTooBig { value: u16, size: usize },
}

#[derive(Debug, thiserror::Error)]
#[error("Node is disconnected")]
pub struct Disconnected;

#[derive(Debug, thiserror::Error)]
pub enum RowsError {
    #[error(transparent)]
    TypeError(BoxedError),
    #[error("No rows")]
    NoRows,
    #[error("No metadata")]
    NoMetadata,
}

#[derive(Debug, thiserror::Error)]
pub enum LwtAppliedError {
    #[error(transparent)]
    Rows(#[from] RowsError),
    #[error(transparent)]
    Parse(#[from] ParseError),
}

#[derive(Debug, thiserror::Error)]
#[error("Unknown partitioner")]
pub struct UnknownPartitioner;
