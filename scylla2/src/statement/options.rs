use std::{collections::HashMap, sync::Arc};

use scylla2_cql::{
    request::{
        batch::{Batch as CqlBatch, BatchType},
        query::parameters::QueryParameters,
    },
    response::result::rows::PagingState,
    Consistency, SerialConsistency,
};

use crate::{execution::ExecutionProfile, topology::partitioner::Token};

#[derive(Debug, Clone, Default)]
pub struct StatementOptions {
    pub custom_payload: Option<HashMap<String, Vec<u8>>>,
    pub execution_profile: Option<Arc<ExecutionProfile>>,
    pub keyspace: Option<Arc<str>>,
    pub now_in_seconds: Option<i32>,
    pub page_size: Option<i32>,
    pub paging_state: Option<PagingState>,
    pub timestamp: Option<i64>,
    pub token: Option<Token>,
}

impl From<Arc<ExecutionProfile>> for StatementOptions {
    fn from(value: Arc<ExecutionProfile>) -> Self {
        Self {
            execution_profile: Some(value),
            ..Default::default()
        }
    }
}

impl StatementOptions {
    pub(crate) fn to_query_parameters<V>(
        &self,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        skip_metadata: bool,
        values: V,
    ) -> QueryParameters<V> {
        QueryParameters {
            consistency,
            keyspace: self.keyspace.as_deref(),
            now_in_seconds: self.now_in_seconds,
            page_size: self.page_size,
            paging_state: self.paging_state.as_ref().map(AsRef::as_ref),
            serial_consistency,
            skip_metadata,
            timestamp: self.timestamp,
            values,
        }
    }

    pub(crate) fn to_batch<S, V>(
        &self,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        r#type: BatchType,
        statements: S,
        values: V,
    ) -> CqlBatch<S, V> {
        CqlBatch {
            r#type,
            statements,
            values,
            consistency,
            keyspace: self.keyspace.as_deref(),
            now_in_seconds: self.now_in_seconds,
            serial_consistency,
            timestamp: self.timestamp,
        }
    }
}
