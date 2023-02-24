#![warn(missing_debug_implementations)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::map_unwrap_or)]
#![deny(clippy::semicolon_if_nothing_returned)]
#![forbid(unsafe_code)]

pub mod auth;
pub mod connection;
pub mod error;
pub mod execution;
pub mod session;
pub mod statement;
pub mod topology;
mod utils;

pub use scylla2_cql as cql;
pub use scylla2_cql::event::{Event as DatabaseEvent, EventType as DatabaseEventType};

pub use crate::{
    session::{config::SessionConfig, event::SessionEvent, Session},
    statement::{
        batch::{Batch, BatchN},
        config::StatementConfig,
        prepared::PreparedStatement,
        query::Query,
    },
};
