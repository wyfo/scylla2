use std::marker::PhantomData;

use scylla2_cql::{
    request::query::{
        parameters::{OnlyValues, QueryParameters as CqlQueryParameters},
        values::QueryValues,
        Query as CqlQuery,
    },
    response::result::rows::PagingState,
    Consistency, SerialConsistency,
};

use crate::statement::{
    config::{StatementConfig, StatementOptions},
    Statement,
};

#[derive(Debug)]
pub struct QueryParameters<'a, V> {
    pub(super) config: &'a StatementConfig,
    pub(super) options: &'a StatementOptions,
    pub(super) skip_metadata: bool,
    pub(super) values: V,
}

impl<V> CqlQueryParameters<V> for QueryParameters<'_, V> {
    fn consistency(&self) -> Consistency {
        self.config.consistency.unwrap_or_default()
    }

    fn keyspace(&self) -> Option<&str> {
        self.config.keyspace.as_deref()
    }

    fn now_in_seconds(&self) -> Option<i32> {
        self.options.now_in_seconds
    }

    fn page_size(&self) -> Option<i32> {
        self.config.page_size
    }

    fn paging_state(&self) -> Option<&PagingState> {
        self.options.paging_state.as_ref()
    }

    fn serial_consistency(&self) -> Option<SerialConsistency> {
        self.config.serial_consistency
    }

    fn skip_metadata(&self) -> bool {
        self.skip_metadata
    }

    fn timestamp(&self) -> Option<i64> {
        self.options.timestamp
    }

    fn values(&self) -> &V {
        &self.values
    }
}

#[derive(Debug)]
pub struct Query<S = String> {
    statement: S,
    config: StatementConfig,
}

impl<S> Query<S> {
    pub fn new(statement: S) -> Self {
        Self {
            statement,
            config: Default::default(),
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
    type Request<'a> = CqlQuery<'a, QueryParameters<'a, V>, V> where Self: 'a;

    fn as_request<'a>(
        &'a self,
        config: &'a StatementConfig,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        CqlQuery {
            query: self.statement.as_ref(),
            parameters: QueryParameters {
                config,
                options,
                skip_metadata: false,
                values,
            },
            _phantom: PhantomData,
        }
    }

    fn config(&self) -> Option<&StatementConfig> {
        Some(&self.config)
    }
}

impl<V> Statement<V> for str
where
    V: QueryValues,
{
    type Request<'a> = CqlQuery<'a, QueryParameters<'a, V>, V>;

    fn as_request<'a>(
        &'a self,
        config: &'a StatementConfig,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        CqlQuery {
            query: self,
            parameters: QueryParameters {
                config,
                options,
                skip_metadata: false,
                values,
            },
            _phantom: PhantomData,
        }
    }
}

impl<V> Statement<V> for String
where
    V: QueryValues,
{
    type Request<'a> = CqlQuery<'a, QueryParameters<'a, V>, V>;

    fn as_request<'a>(
        &'a self,
        config: &'a StatementConfig,
        options: &'a StatementOptions,
        values: V,
    ) -> Self::Request<'a> {
        CqlQuery {
            query: self,
            parameters: QueryParameters {
                config,
                options,
                skip_metadata: false,
                values,
            },
            _phantom: PhantomData,
        }
    }
}

pub(crate) fn cql_query<V>(query: &str, values: V) -> CqlQuery<OnlyValues<V>, V> {
    CqlQuery {
        query,
        parameters: OnlyValues(values),
        _phantom: PhantomData,
    }
}
