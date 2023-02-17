use std::sync::Arc;

use scylla2_cql::{response::result::rows::PagingState, Consistency, SerialConsistency};

// TODO add idemptotency/timeout/retry_policy/speculative_execution_policy/etc.

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct StatementConfig {
    pub consistency: Option<Consistency>,
    pub keyspace: Option<Arc<str>>,
    pub page_size: Option<i32>,
    pub serial_consistency: Option<SerialConsistency>,
    pub tracing: Option<bool>,
}

impl StatementConfig {
    pub fn merge(&self, other: &StatementConfig) -> Self {
        Self {
            consistency: self.consistency.or(other.consistency),
            keyspace: self.keyspace.clone().or_else(|| other.keyspace.clone()),
            page_size: self.page_size.or(other.page_size),
            serial_consistency: self.serial_consistency.or(other.serial_consistency),
            tracing: self.tracing.or(other.tracing),
        }
    }
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
