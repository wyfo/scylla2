use std::{marker::PhantomData, ops::Deref, sync::Arc};

use arc_swap::ArcSwap;
use scylla2_cql::{
    request::{execute::Execute, query::values::QueryValues},
    response::result::{column_spec::ColumnSpec, prepared::Prepared},
};

use crate::{
    error::PartitionKeyError,
    statement::{
        config::{StatementConfig, StatementOptions},
        query::QueryParameters,
        Statement,
    },
    topology::{
        partitioner::{Partitioner, SerializePartitionKey},
        ring::{Partition, Ring},
    },
};

#[derive(Debug)]
pub struct PreparedStatement {
    pub(crate) statement: String,
    pub(crate) prepared: Prepared,
    pub(crate) partitioning: Option<(Partitioner, Arc<ArcSwap<Ring>>)>,
    pub(crate) config: StatementConfig,
}

impl Deref for PreparedStatement {
    type Target = Prepared;
    fn deref(&self) -> &Self::Target {
        &self.prepared
    }
}

impl PreparedStatement {
    pub fn statement(&self) -> &str {
        &self.statement
    }

    pub fn partitioner(&self) -> Option<&Partitioner> {
        Some(&self.partitioning.as_ref()?.0)
    }

    pub fn ring(&self) -> Option<Arc<Ring>> {
        Some(self.partitioning.as_ref()?.1.load_full())
    }
}

impl<V> Statement<V> for PreparedStatement
where
    V: QueryValues + SerializePartitionKey,
{
    type Request<'a> = Execute<'a, QueryParameters<'a, V>, V>;

    fn as_request<'a>(
        &'a self,
        config: &'a StatementConfig,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        Execute {
            id: &self.id,
            result_metadata_id: self.result_metadata_id.as_deref(),
            parameters: QueryParameters {
                config,
                options,
                skip_metadata: self.result_specs.is_some(),
                values,
            },
            _phantom: PhantomData,
        }
    }

    fn config(&self) -> Option<&StatementConfig> {
        Some(&self.config)
    }

    fn partition(&self, values: &V) -> Result<Option<Partition>, PartitionKeyError> {
        if let Some((partitioner, ring)) = self.partitioning.as_ref() {
            let token = partitioner.token(values, &self.pk_indexes)?;
            return Ok(Some(ring.load_full().get_partition(token)));
        }
        Ok(None)
    }

    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        self.result_specs.clone()
    }

    fn is_lwt(&self) -> bool {
        self.is_lwt
    }
}
