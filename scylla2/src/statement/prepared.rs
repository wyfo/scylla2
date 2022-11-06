use std::{marker::PhantomData, ops::Deref, sync::Arc};

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
    topology::{partitioner::Partitioning, ring::Partition},
};

#[derive(Debug)]
pub struct PreparedStatement {
    pub(crate) statement: String,
    pub(crate) prepared: Prepared,
    pub(crate) partitioning: Option<Partitioning>,
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

    pub fn partitioning(&self) -> Option<&Partitioning> {
        self.partitioning.as_ref()
    }
}

impl<V> Statement<V> for PreparedStatement
where
    V: QueryValues,
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
        self.partitioning
            .as_ref()
            .map(|p| p.get_partition(values, self.pk_indexes.iter().cloned()))
            .transpose()
    }

    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        self.result_specs.clone()
    }
}
