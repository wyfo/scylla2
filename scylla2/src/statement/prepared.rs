use std::{ops::Deref, sync::Arc};

use scylla2_cql::{
    request::{execute::Execute, query::values::QueryValues},
    response::result::{column_spec::ColumnSpec, prepared::Prepared},
    Consistency, SerialConsistency,
};

use crate::{
    error::PartitionKeyError,
    statement::{options::StatementOptions, Statement},
    topology::{
        partitioner::{Partitioner, SerializePartitionKey, Token},
        ring::{Partition, Ring},
    },
};

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub(crate) statement: String,
    pub(crate) prepared: Prepared,
    pub(crate) partitioning: Option<(Partitioner, Ring)>,
    pub(crate) idempotent: bool,
    pub(crate) is_lwt: Option<bool>,
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

    pub fn ring(&self) -> Option<&Ring> {
        Some(&self.partitioning.as_ref()?.1)
    }
}

impl<V> Statement<V> for PreparedStatement
where
    V: QueryValues + SerializePartitionKey,
{
    type Request<'a> = Execute<'a, V>;

    fn as_request<'a>(
        &'a self,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        Execute {
            id: &self.id,
            result_metadata_id: self.result_metadata_id.as_deref(),
            parameters: options.to_query_parameters(
                consistency,
                serial_consistency,
                self.result_specs.is_some(),
                values,
            ),
        }
    }

    fn partition(
        &self,
        values: &V,
        token: Option<Token>,
    ) -> Result<Option<Partition>, PartitionKeyError> {
        if let Some((partitioner, ring)) = self.partitioning.as_ref() {
            let token = match token {
                Some(tk) => tk,
                None => partitioner.token(values, &self.pk_indexes)?,
            };
            return Ok(Some(ring.get_partition(token)));
        }
        Ok(None)
    }

    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        self.result_specs.clone()
    }

    fn idempotent(&self) -> bool {
        self.idempotent
    }

    fn is_lwt(&self) -> Option<bool> {
        self.is_lwt
    }
}
