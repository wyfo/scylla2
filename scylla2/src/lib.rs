#![warn(missing_debug_implementations)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::map_unwrap_or)]
#![deny(clippy::semicolon_if_nothing_returned)]
#![forbid(unsafe_code)]

pub mod auth;
pub mod connection;
pub mod debug;
pub mod error;
pub mod event;
pub mod execution;
pub mod session;
pub mod statement;
pub mod topology;
mod utils;

pub use scylla2_cql as cql;

pub use crate::{
    execution::{ExecutionProfile, ExecutionResult},
    session::{config::SessionConfig, Session},
    statement::{
        batch::{Batch, BatchN},
        options::StatementOptions,
        prepared::PreparedStatement,
        query::Query,
    },
};
