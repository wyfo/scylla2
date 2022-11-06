use std::{sync::Arc, time::Duration};

use scylla2_cql::{
    cql::{Consistency, SerialConsistency},
    response::result::rows::PagingState,
};

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct StatementConfig {
    pub consistency: Option<Consistency>,
    pub idempotency: Option<bool>,
    pub keyspace: Option<Arc<str>>,
    pub page_size: Option<i32>,
    pub serial_consistency: Option<SerialConsistency>,
    pub tracing: Option<bool>,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct StatementOptions {
    pub now_in_seconds: Option<i32>,
    pub paging_state: Option<PagingState>,
    pub timestamp: Option<i64>,
}

impl From<PagingState> for StatementOptions {
    fn from(paging_state: PagingState) -> Self {
        StatementOptions {
            paging_state: Some(paging_state),
            ..Default::default()
        }
    }
}
