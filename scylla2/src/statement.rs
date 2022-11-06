use std::sync::Arc;

use config::{StatementConfig, StatementOptions};
use scylla2_cql::{
    request::{
        query::{parameters::QueryParameters as CqlQueryParameters, values::QueryValues},
        Request,
    },
    response::result::column_spec::ColumnSpec,
};

use crate::{error::PartitionKeyError, topology::ring::Partition};

pub mod batch;
pub mod config;
pub mod prepared;
pub mod query;
pub mod values;

pub trait Statement<V> {
    type Request<'a>: Request
    where
        Self: 'a;
    fn as_request<'a>(
        &'a self,
        config: &'a StatementConfig,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a>;
    fn config(&self) -> Option<&StatementConfig> {
        None
    }
    fn partition(&self, _values: &V) -> Result<Option<Partition>, PartitionKeyError> {
        Ok(None)
    }
    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        None
    }
}

impl<T, V> Statement<V> for &T
where
    T: ?Sized + Statement<V>,
{
    type Request<'a> = T::Request<'a> where Self: 'a;
    fn as_request<'a>(
        &'a self,
        config: &'a StatementConfig,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        T::as_request(self, config, options, values)
    }
    fn config(&self) -> Option<&StatementConfig> {
        T::config(self)
    }
    fn partition(&self, values: &V) -> Result<Option<Partition>, PartitionKeyError> {
        T::partition(self, values)
    }
    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        T::result_specs(self)
    }
}
