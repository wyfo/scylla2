use std::sync::Arc;

use options::StatementOptions;
use scylla2_cql::{
    request::Request, response::result::column_spec::ColumnSpec, Consistency, SerialConsistency,
};

use crate::{
    error::PartitionKeyError,
    topology::{partitioner::Token, ring::Partition},
};

pub mod batch;
pub mod options;
pub mod prepared;
pub mod query;

pub trait Statement<V> {
    type Request<'a>: Request
    where
        Self: 'a;
    fn as_request<'a>(
        &'a self,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a>;
    fn partition(
        &self,
        _values: &V,
        _token: Option<Token>,
    ) -> Result<Option<Partition>, PartitionKeyError> {
        Ok(None)
    }
    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        None
    }
    fn idempotent(&self) -> bool {
        false
    }
    fn is_lwt(&self) -> Option<bool> {
        None
    }
    fn reprepare(&self, _id: &[u8]) -> Option<&str> {
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
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        T::as_request(self, consistency, serial_consistency, options, values)
    }
    fn partition(
        &self,
        _values: &V,
        _token: Option<Token>,
    ) -> Result<Option<Partition>, PartitionKeyError> {
        T::partition(self, _values, None)
    }
    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        T::result_specs(self)
    }
    fn idempotent(&self) -> bool {
        T::idempotent(self)
    }
    fn is_lwt(&self) -> Option<bool> {
        T::is_lwt(self)
    }
    fn reprepare(&self, id: &[u8]) -> Option<&str> {
        T::reprepare(self, id)
    }
}

impl<T, V> Statement<V> for Box<T>
where
    T: ?Sized + Statement<V>,
{
    type Request<'a> = T::Request<'a> where Self: 'a;
    fn as_request<'a>(
        &'a self,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        T::as_request(self, consistency, serial_consistency, options, values)
    }
    fn partition(
        &self,
        _values: &V,
        _token: Option<Token>,
    ) -> Result<Option<Partition>, PartitionKeyError> {
        T::partition(self, _values, None)
    }
    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        T::result_specs(self)
    }
    fn idempotent(&self) -> bool {
        T::idempotent(self)
    }
    fn is_lwt(&self) -> Option<bool> {
        T::is_lwt(self)
    }
    fn reprepare(&self, id: &[u8]) -> Option<&str> {
        T::reprepare(self, id)
    }
}

impl<T, V> Statement<V> for Arc<T>
where
    T: ?Sized + Statement<V>,
{
    type Request<'a> = T::Request<'a> where Self: 'a;
    fn as_request<'a>(
        &'a self,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        T::as_request(self, consistency, serial_consistency, options, values)
    }
    fn partition(
        &self,
        _values: &V,
        _token: Option<Token>,
    ) -> Result<Option<Partition>, PartitionKeyError> {
        T::partition(self, _values, None)
    }
    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        T::result_specs(self)
    }
    fn idempotent(&self) -> bool {
        T::idempotent(self)
    }
    fn is_lwt(&self) -> Option<bool> {
        T::is_lwt(self)
    }
    fn reprepare(&self, id: &[u8]) -> Option<&str> {
        T::reprepare(self, id)
    }
}
