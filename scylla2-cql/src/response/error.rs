use std::{collections::HashMap, io, net::IpAddr, str::FromStr};

use crate::{
    cql::{ReadCql, ShortBytes},
    extensions::ProtocolExtensions,
    utils::invalid_data,
    LegacyConsistency, ProtocolVersion,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq, strum::FromRepr)]
#[repr(u32)]
#[non_exhaustive]
pub enum ErrorCode {
    ServerError = 0x0000,
    ProtocolError = 0x000A,
    AuthenticationError = 0x0100,
    Unavailable = 0x1000,
    Overloaded = 0x1001,
    IsBootstrapping = 0x1002,
    TruncateError = 0x1003,
    WriteTimeout = 0x1100,
    ReadTimeout = 0x1200,
    ReadFailure = 0x1300,
    FunctionFailure = 0x1400,
    WriteFailure = 0x1500,
    CdcWriteFailure = 0x1600,
    CasWriteUnknown = 0x1700,
    SyntaxError = 0x2000,
    Unauthorized = 0x2100,
    Invalid = 0x2200,
    ConfigError = 0x2300,
    AlreadyExists = 0x2400,
    Unprepared = 0x2500,
}

#[derive(Debug, thiserror::Error)]
#[error("{kind}\nmessage: {message}")]
pub struct Error {
    pub code: Result<ErrorCode, u32>,
    pub message: String,
    pub kind: ErrorKind,
}

impl Error {
    pub fn deserialize(
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        mut slice: &[u8],
    ) -> io::Result<Self> {
        let error_code = u32::read_cql(&mut slice)?;
        let code = ErrorCode::from_repr(error_code).ok_or(error_code);
        let message = String::read_cql(&mut slice)?;
        let kind = ErrorKind::deserialize(version, extensions, code, slice)?;
        Ok(Self {
            code,
            message,
            kind,
        })
    }
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The submitted query has a syntax error
    #[error("The submitted query has a syntax error")]
    SyntaxError,

    /// The query is syntactically correct but invalid
    #[error("The query is syntactically correct but invalid")]
    Invalid,

    /// Attempted to create a keyspace or a table that was already existing
    #[error(
        "Attempted to create a keyspace or a table that was already existing \
        (keyspace: {keyspace}, table: {table})"
    )]
    AlreadyExists {
        /// Created keyspace name or name of the keyspace in which table was created
        keyspace: String,
        /// Name of the table created, in case of keyspace creation it's an empty string
        table: String,
    },

    /// User defined function failed during execution
    #[error(
        "User defined function failed during execution \
        (keyspace: {keyspace}, function: {function}, arg_types: {arg_types:?})"
    )]
    FunctionFailure {
        /// Keyspace of the failed function
        keyspace: String,
        /// Name of the failed function
        function: String,
        /// Types of arguments passed to the function
        arg_types: Vec<String>,
    },

    /// Authentication failed - bad credentials
    #[error("Authentication failed - bad credentials")]
    AuthenticationError,

    /// The logged user doesn't have the right to perform the query
    #[error("The logged user doesn't have the right to perform the query")]
    Unauthorized,

    /// The query is invalid because of some configuration issue
    #[error("The query is invalid because of some configuration issue")]
    ConfigError,

    /// Not enough nodes are alive to satisfy required consistency level
    #[error(
        "Not enough nodes are alive to satisfy required consistency level \
        (consistency: {consistency}, required: {required}, alive: {alive})"
    )]
    Unavailable {
        /// Consistency level of the query
        consistency: LegacyConsistency,
        /// Number of nodes required to be alive to satisfy required consistency level
        required: u32,
        /// Found number of active nodes
        alive: u32,
    },

    /// The request cannot be processed because the coordinator node is overloaded
    #[error("The request cannot be processed because the coordinator node is overloaded")]
    Overloaded,

    /// The coordinator node is still bootstrapping
    #[error("The coordinator node is still bootstrapping")]
    IsBootstrapping,

    /// Error during truncate operation
    #[error("Error during truncate operation")]
    TruncateError,

    /// Not enough nodes responded to the read request in time to satisfy required consistency level
    #[error("Not enough nodes responded to the read request in time to satisfy required consistency level \
            (consistency: {consistency}, received: {received}, blockfor: {blockfor}, data_present: {data_present})")]
    ReadTimeout {
        /// Consistency level of the query
        consistency: LegacyConsistency,
        /// Number of nodes that responded to the read request
        received: u32,
        /// Number of nodes required to respond to satisfy required consistency level
        blockfor: u32,
        /// Replica that was asked for data has responded
        data_present: bool,
    },

    /// Not enough nodes responded to the write request in time to satisfy required consistency level
    #[error("Not enough nodes responded to the write request in time to satisfy required consistency level \
            (consistency: {consistency}, received: {received}, blockfor: {blockfor}, write_type: {write_type:?})")]
    WriteTimeout {
        /// Consistency level of the query
        consistency: LegacyConsistency,
        /// Number of nodes that responded to the write request
        received: u32,
        /// Number of nodes required to respond to satisfy required consistency level
        blockfor: u32,
        /// Type of write operation requested
        write_type: WriteType,
    },

    /// A non-timeout error during a read request
    #[error(
        "A non-timeout error during a read request \
        (consistency: {consistency}, received: {received}, blockfor: {blockfor}, \
        failures: {failures:?}, data_present: {data_present})"
    )]
    ReadFailure {
        /// Consistency level of the query
        consistency: LegacyConsistency,
        /// Number of nodes that responded to the read request
        received: u32,
        /// Number of nodes required to respond to satisfy required consistency level
        blockfor: u32,
        /// Number of nodes that experience a failure while executing the request
        failures: Failures,
        /// Replica that was asked for data has responded
        data_present: bool,
    },

    /// A non-timeout error during a write request
    #[error(
        "A non-timeout error during a write request \
        (consistency: {consistency}, received: {received}, blockfor: {blockfor}, \
        failures: {failures:?}, write_type: {write_type:?}"
    )]
    WriteFailure {
        /// Consistency level of the query
        consistency: LegacyConsistency,
        /// Number of nodes that responded to the read request
        received: u32,
        /// Number of nodes required to respond to satisfy required consistency level
        blockfor: u32,
        /// Number of nodes that experience a failure while executing the request
        failures: Failures,
        /// Type of write operation requested
        write_type: WriteType,
    },

    /// Undocumented
    #[error("Undocumented")]
    CdcWriteFailure,

    ///  An exception occurred due to contended Compare And Set write/update.
    /// The CAS operation was only partially completed and the operation may or may not
    /// get completed by the contending CAS write or SERIAL/LOCAL_SERIAL read.
    #[error("An exception occurred due to contended Compare And Set write/update (consistency: {consistency}, received: {received}, blockfor: {blockfor})")]
    CasWriteUnknown {
        consistency: LegacyConsistency,
        /// Number of nodes that responded to the read request
        received: u32,
        /// Number of nodes required to respond to satisfy required consistency level
        blockfor: u32,
    },

    /// Tried to execute a prepared statement that is not prepared. Driver should prepare it again
    #[error(
        "Tried to execute a prepared statement that is not prepared. Driver should prepare it again"
    )]
    Unprepared {
        /// Statement id of the requested prepared query
        statement_id: Box<[u8]>,
    },

    /// Internal server error. This indicates a server-side bug
    #[error("Internal server error. This indicates a server-side bug")]
    ServerError,

    /// Invalid protocol message received from the driver
    #[error("Invalid protocol message received from the driver")]
    ProtocolError,

    /// Rate limit was exceeded for a partition affected by the request.
    #[error("Rate limit was exceeded for a partition affected by the request (op_type: {op_type:?}, rejected_by_coordinator: {rejected_by_coordinator})")]
    ScyllaRateLimitReached {
        /// Type of the operation rejected by rate limiting.
        op_type: OperationType,
        /// Whether the operation was rate limited on the coordinator or not.
        /// Writes rejected on the coordinator are guaranteed not to be applied
        /// on any replica.
        rejected_by_coordinator: bool,
    },

    /// Other error code not specified in the specification
    #[error("Other error not specified in the specification. Error code: {0}")]
    Other(u32),
}

impl ErrorKind {
    pub fn deserialize(
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        code: Result<ErrorCode, u32>,
        mut slice: &[u8],
    ) -> io::Result<Self> {
        let buf = &mut slice;
        Ok(match code {
            Ok(ErrorCode::ServerError) => Self::ServerError,
            Ok(ErrorCode::ProtocolError) => Self::ProtocolError,
            Ok(ErrorCode::AuthenticationError) => Self::AuthenticationError,
            Ok(ErrorCode::Unavailable) => ErrorKind::Unavailable {
                consistency: LegacyConsistency::read_cql(buf)?,
                required: u32::read_cql(buf)?,
                alive: u32::read_cql(buf)?,
            },
            Ok(ErrorCode::Overloaded) => ErrorKind::Overloaded,
            Ok(ErrorCode::IsBootstrapping) => ErrorKind::IsBootstrapping,
            Ok(ErrorCode::TruncateError) => ErrorKind::TruncateError,
            Ok(ErrorCode::WriteTimeout) => ErrorKind::WriteTimeout {
                consistency: LegacyConsistency::read_cql(buf)?,
                received: u32::read_cql(buf)?,
                blockfor: u32::read_cql(buf)?,
                write_type: WriteType::from_str(<&str>::read_cql(buf)?).unwrap(),
            },
            Ok(ErrorCode::ReadTimeout) => ErrorKind::ReadTimeout {
                consistency: LegacyConsistency::read_cql(buf)?,
                received: u32::read_cql(buf)?,
                blockfor: u32::read_cql(buf)?,
                data_present: bool::read_cql(buf)?,
            },
            Ok(ErrorCode::ReadFailure) => ErrorKind::ReadFailure {
                consistency: LegacyConsistency::read_cql(buf)?,
                received: u32::read_cql(buf)?,
                blockfor: u32::read_cql(buf)?,
                failures: Failures::deserialize(version, buf)?,
                data_present: bool::read_cql(buf)?,
            },
            Ok(ErrorCode::FunctionFailure) => ErrorKind::FunctionFailure {
                keyspace: String::read_cql(buf)?,
                function: String::read_cql(buf)?,
                arg_types: <Vec<String>>::read_cql(buf)?,
            },
            Ok(ErrorCode::WriteFailure) => ErrorKind::WriteFailure {
                consistency: LegacyConsistency::read_cql(buf)?,
                received: u32::read_cql(buf)?,
                blockfor: u32::read_cql(buf)?,
                failures: Failures::deserialize(version, buf)?,
                write_type: WriteType::from_str(<&str>::read_cql(buf)?).unwrap(),
            },
            Ok(ErrorCode::CdcWriteFailure) => ErrorKind::CdcWriteFailure,
            Ok(ErrorCode::CasWriteUnknown) => ErrorKind::CasWriteUnknown {
                consistency: LegacyConsistency::read_cql(buf)?,
                received: u32::read_cql(buf)?,
                blockfor: u32::read_cql(buf)?,
            },
            Ok(ErrorCode::SyntaxError) => ErrorKind::SyntaxError,
            Ok(ErrorCode::Unauthorized) => ErrorKind::Unauthorized,
            Ok(ErrorCode::Invalid) => ErrorKind::Invalid,
            Ok(ErrorCode::ConfigError) => ErrorKind::ConfigError,
            Ok(ErrorCode::AlreadyExists) => ErrorKind::AlreadyExists {
                keyspace: String::read_cql(buf)?,
                table: String::read_cql(buf)?,
            },
            Ok(ErrorCode::Unprepared) => ErrorKind::Unprepared {
                statement_id: ShortBytes::read_cql(buf)?.0.into(),
            },
            Err(code) if Some(code) == extensions.and_then(|ext| ext.rate_limit_error_code) => {
                ErrorKind::ScyllaRateLimitReached {
                    op_type: OperationType::from_repr(u8::read_cql(buf)?)
                        .ok_or("Invalid op_type")
                        .map_err(invalid_data)?,
                    rejected_by_coordinator: bool::read_cql(buf)?,
                }
            }
            Err(code) => ErrorKind::Other(code),
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum Failures {
    ReasonMap(HashMap<IpAddr, u16>),
    NumFailures(u32),
}

impl Failures {
    pub fn deserialize(version: ProtocolVersion, buf: &mut &[u8]) -> io::Result<Self> {
        Ok(match version {
            ProtocolVersion::V4 => Self::NumFailures(u32::read_cql(buf)?),
            ProtocolVersion::V5 => {
                let mut reason_map = HashMap::new();
                for _ in 0..u32::read_cql(buf)? {
                    reason_map.insert(IpAddr::read_cql(buf)?, u16::read_cql(buf)?);
                }
                Self::ReasonMap(reason_map)
            }
        })
    }
}

/// Type of the operation rejected by rate limiting
#[derive(Debug, Clone, PartialEq, Eq, strum::FromRepr)]
#[repr(u8)]
pub enum OperationType {
    Read = 0,
    Write = 1,
}

/// Type of write operation requested
#[derive(Debug, Clone, PartialEq, Eq, strum::EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum WriteType {
    /// Non-batched non-counter write
    Simple,
    /// Logged batch write. If this type is received, it means the batch log has been successfully written
    /// (otherwise BatchLog type would be present)
    Batch,
    /// Unlogged batch. No batch log write has been attempted.
    UnloggedBatch,
    /// Counter write (batched or not)
    Counter,
    /// Timeout occurred during the write to the batch log when a logged batch was requested
    BatchLog,
    /// Timeout occurred during Compare And Set write/update
    Cas,
    /// Write involves VIEW update and failure to acquire local view(MV) lock for key within timeout
    View,
    /// Timeout occurred  when a cdc_total_space_in_mb is exceeded when doing a write to data tracked by cdc
    Cdc,
    /// Other type not specified in the specification
    #[strum(default)]
    Other(String),
}
