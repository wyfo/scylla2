use std::io;

use scylla2_cql::{
    error::{DatabaseError, InvalidRequest, TypeError},
    event::SchemaChangeEvent,
};
#[rustfmt::skip]
pub use scylla2_cql::error::ConnectionError;
use scylla2_cql::error::DatabaseErrorKind;

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
    ConnectionError(#[from] ConnectionError),
}

impl From<io::Error> for SessionError {
    fn from(value: io::Error) -> Self {
        Self::ConnectionError(value.into())
    }
}

impl From<UseKeyspaceError> for SessionError {
    fn from(value: UseKeyspaceError) -> Self {
        match value {
            UseKeyspaceError::InvalidKeyspace(error) => error.into(),
            UseKeyspaceError::Database(error) => Self::ConnectionError(error.into()),
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
pub enum ConnectionExecutionError {
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
    PartitionKeyError(#[from] PartitionKeyError),
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
}

#[derive(Debug, thiserror::Error)]
#[error("Ongoing USE KEYSPACE query")]
pub struct OngoingUseKeyspace;

#[derive(Debug, thiserror::Error)]
pub enum PartitionKeyError {
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
    TypeError(#[from] TypeError),
    #[error("No rows")]
    NoRows,
    #[error("No metadata")]
    NoMetadata,
}

#[derive(Debug, thiserror::Error)]
#[error("Unknown partitioner")]
pub struct UnknownPartitioner;
