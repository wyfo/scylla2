use scylla2_cql::{
    request::query::{values::QueryValues, Query as CqlQuery},
    Consistency, SerialConsistency,
};

use crate::statement::{options::StatementOptions, Statement};

impl<V> Statement<V> for str
where
    V: QueryValues,
{
    type Request<'a> = CqlQuery<'a, V>;

    fn as_request<'a>(
        &'a self,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        CqlQuery {
            query: self,
            parameters: options.to_query_parameters(
                consistency,
                serial_consistency,
                Statement::<V>::result_specs(self).is_some(),
                values,
            ),
        }
    }
}

impl<V> Statement<V> for String
where
    V: QueryValues,
{
    type Request<'a> = CqlQuery<'a, V>;

    fn as_request<'a>(
        &'a self,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        str::as_request(self, consistency, serial_consistency, options, values)
    }
}

#[derive(Debug)]
pub struct Query<S = String> {
    statement: S,
    idempotent: bool,
    is_lwt: Option<bool>,
}

impl<S> Query<S> {
    pub fn new(statement: S, idempotent: bool, lwt: bool) -> Self {
        Self {
            statement,
            idempotent,
            is_lwt: Some(lwt),
        }
    }
}

impl<S> Query<S>
where
    S: AsRef<str>,
{
    pub fn statement(&self) -> &str {
        self.statement.as_ref()
    }
}

impl<S, V> Statement<V> for Query<S>
where
    S: AsRef<str>,
    V: QueryValues,
{
    type Request<'a> = CqlQuery<'a, V> where Self: 'a;

    fn as_request<'a>(
        &'a self,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        self.statement
            .as_ref()
            .as_request(consistency, serial_consistency, options, values)
    }

    fn idempotent(&self) -> bool {
        self.idempotent
    }

    fn is_lwt(&self) -> Option<bool> {
        self.is_lwt
    }
}
