use std::sync::Arc;

use scylla2_cql::{
    request::{
        batch::{Batch as CqlBatch, BatchStatement, BatchType},
        query::values::QueryValues,
    },
    response::result::column_spec::ColumnSpec,
};

use crate::{
    error::PartitionKeyError,
    statement::{
        config::{StatementConfig, StatementOptions},
        prepared::PreparedStatement,
        query::Query,
        Statement,
    },
    topology::ring::Partition,
    utils::tuples,
};

#[derive(Debug)]
pub struct Batch<S> {
    r#type: BatchType,
    statements: S,
    config: StatementConfig,
}

impl<S> Batch<S> {
    pub fn new(r#type: BatchType, statements: S) -> Self {
        Self {
            r#type,
            statements,
            config: Default::default(),
        }
    }

    pub fn logged(statements: S) -> Self {
        Self::new(BatchType::Logged, statements)
    }

    pub fn unlogged(statements: S) -> Self {
        Self::new(BatchType::Unlogged, statements)
    }

    pub fn counter(statements: S) -> Self {
        Self::new(BatchType::Counter, statements)
    }

    pub fn r#type(&self) -> BatchType {
        self.r#type
    }

    pub fn statements(&self) -> &S {
        &self.statements
    }
}

pub type BatchN<const N: usize> = Batch<[PreparedStatement; N]>;

impl<'b, S, V> Statement<&'b [V]> for Batch<&[S]>
where
    S: BatchStatement + Statement<V>,
    V: QueryValues,
{
    type Request<'a> = CqlBatch<'a, &'a [S], &'b [V]> where Self: 'a;

    fn as_request<'a>(
        &'a self,
        config: &'a StatementConfig,
        options: &'a StatementOptions,
        values: &'b [V],
    ) -> Self::Request<'a> {
        CqlBatch {
            r#type: self.r#type,
            statements: self.statements,
            values,
            consistency: config.consistency.unwrap_or_default(),
            keyspace: config.keyspace.as_deref(),
            now_in_seconds: options.now_in_seconds,
            serial_consistency: config.serial_consistency,
            timestamp: options.timestamp,
        }
    }

    fn config(&self) -> Option<&StatementConfig> {
        Some(&self.config)
    }

    fn partition(&self, values: &&'b [V]) -> Result<Option<Partition>, PartitionKeyError> {
        self.statements
            .get(0)
            .and_then(|s| Some((s, values.get(0)?)))
            .and_then(|(s, v)| s.partition(v).transpose())
            .transpose()
    }

    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        self.statements.get(0).and_then(|s| s.result_specs())
    }
}

impl<'b, S, V> Statement<&'b [V]> for Batch<Vec<S>>
where
    S: BatchStatement + Statement<V>,
    V: QueryValues,
{
    type Request<'a> = CqlBatch<'a, &'a [S], &'b [V]> where Self: 'a;

    fn as_request<'a>(
        &'a self,
        config: &'a StatementConfig,
        options: &'a StatementOptions,
        values: &'b [V],
    ) -> Self::Request<'a> {
        CqlBatch {
            r#type: self.r#type,
            statements: &self.statements,
            values,
            consistency: config.consistency.unwrap_or_default(),
            keyspace: config.keyspace.as_deref(),
            now_in_seconds: options.now_in_seconds,
            serial_consistency: config.serial_consistency,
            timestamp: options.timestamp,
        }
    }

    fn config(&self) -> Option<&StatementConfig> {
        Some(&self.config)
    }

    fn partition(&self, values: &&'b [V]) -> Result<Option<Partition>, PartitionKeyError> {
        self.statements
            .get(0)
            .and_then(|s| Some((s, values.get(0)?)))
            .and_then(|(s, v)| s.partition(v).transpose())
            .transpose()
    }

    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        self.statements.get(0).and_then(|s| s.result_specs())
    }
}

impl BatchStatement for Query {
    fn prepared(&self) -> bool {
        false
    }
    fn bytes(&self) -> &[u8] {
        self.statement().as_bytes()
    }
}

impl BatchStatement for PreparedStatement {
    fn prepared(&self) -> bool {
        true
    }
    fn bytes(&self) -> &[u8] {
        &self.id
    }
}

macro_rules! batch {
    ($($stmt:ident/$values:ident/$idx:tt),*; $len:literal) => {
        impl<'b, S0, V0, $($stmt, $values),*> Statement<(V0, $($values),*)> for (S0, $($stmt),*)
        where
            S0: BatchStatement + Statement<V0>,
            V0: QueryValues,
            $(
                $stmt: BatchStatement + Statement<$values>,
                $values: QueryValues,
            )*
        {
            type Request<'a> = CqlBatch<'a, (&'a S0, $(&'a $stmt),*), (V0, $($values),*)> where Self: 'a;

            fn as_request<'a>(
                &'a self,
                config: &'a StatementConfig,
                options: &'a StatementOptions,
                values: (V0, $($values),*),
            ) -> Self::Request<'a> {
                CqlBatch {
                    r#type: Default::default(),
                    statements: (&self.0, $(&self.$idx),*),
                    values,
                    consistency: config.consistency.unwrap_or_default(),
                    keyspace: config.keyspace.as_deref(),
                    now_in_seconds: options.now_in_seconds,
                    serial_consistency: config.serial_consistency,
                    timestamp: options.timestamp,
                }
            }

            fn config(&self) -> Option<&StatementConfig> {
                None
            }

            fn partition(&self, values: &(V0, $($values),*)) -> Result<Option<Partition>, PartitionKeyError> {
                self.0.partition(&values.0)
            }

            fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
                self.0.result_specs()
            }
        }

        impl<'b, S0, V0, $($stmt, $values),*> Statement<(V0, $($values),*)> for Batch<(S0, $($stmt),*)>
        where
            S0: BatchStatement + Statement<V0>,
            V0: QueryValues,
            $(
                $stmt: BatchStatement + Statement<$values>,
                $values: QueryValues,
            )*
        {
            type Request<'a> = CqlBatch<'a, (&'a S0, $(&'a $stmt),*), (V0, $($values),*)> where Self: 'a;

            fn as_request<'a>(
                &'a self,
                config: &'a StatementConfig,
                options: &'a StatementOptions,
                values: (V0, $($values, )*),
            ) -> Self::Request<'a> {
                CqlBatch {
                    r#type: self.r#type,
                    statements: (&self.statements.0, $(&self.statements.$idx),*),
                    values,
                    consistency: config.consistency.unwrap_or_default(),
                    keyspace: config.keyspace.as_deref(),
                    now_in_seconds: options.now_in_seconds,
                    serial_consistency: config.serial_consistency,
                    timestamp: options.timestamp,
                }
            }

            fn config(&self) -> Option<&StatementConfig> {
                Some(&self.config)
            }

            fn partition(&self, values: &(V0, $($values, )*)) -> Result<Option<Partition>, PartitionKeyError> {
                self.statements.0.partition(&values.0)
            }

            fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
                self.statements.0.result_specs()
            }
        }


        impl<'b, V0, $($values),*> Statement<(V0, $($values),*)> for BatchN<$len>
        where
            V0: QueryValues,
            $($values: QueryValues),*
        {
            type Request<'a> = CqlBatch<'a, &'a [PreparedStatement; $len], (V0, $($values),*)> where Self: 'a;

            fn as_request<'a>(
                &'a self,
                config: &'a StatementConfig,
                options: &'a StatementOptions,
                values: (V0, $($values, )*),
            ) -> Self::Request<'a> {
                CqlBatch {
                    r#type: self.r#type,
                    statements: &self.statements,
                    values,
                    consistency: config.consistency.unwrap_or_default(),
                    keyspace: config.keyspace.as_deref(),
                    now_in_seconds: options.now_in_seconds,
                    serial_consistency: config.serial_consistency,
                    timestamp: options.timestamp,
                }
            }

            fn config(&self) -> Option<&StatementConfig> {
                Some(&self.config)
            }

            fn partition(&self, values: &(V0, $($values, )*)) -> Result<Option<Partition>, PartitionKeyError> {
                self.statements[0].partition(&values.0)
            }

            fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
                 <PreparedStatement as Statement<V0>>::result_specs(&self.statements[0])
            }
        }
    };
}

tuples!(batch);
