use std::sync::Arc;

use scylla2_cql::{
    request::{
        batch::{Batch as CqlBatch, BatchStatement, BatchType},
        query::values::QueryValues,
    },
    response::result::column_spec::ColumnSpec,
    Consistency, SerialConsistency,
};

use crate::{
    error::PartitionKeyError,
    statement::{options::StatementOptions, prepared::PreparedStatement, query::Query, Statement},
    topology::{
        partitioner::{SerializePartitionKey, Token},
        ring::Partition,
    },
    utils::tuples,
};

#[derive(Debug)]
pub struct Batch<S> {
    r#type: BatchType,
    statements: S,
}

impl<S> Batch<S> {
    pub fn new(r#type: BatchType, statements: S) -> Self {
        Self { r#type, statements }
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

pub type BatchN<const N: usize, S = PreparedStatement> = Batch<[S; N]>;

impl<'b, S, V> Statement<&'b [V]> for Batch<&[S]>
where
    S: BatchStatement + Statement<V>,
    V: QueryValues,
{
    type Request<'a> = CqlBatch<'a, &'a [S], &'b [V]> where Self: 'a;

    fn as_request<'a>(
        &'a self,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        options: &'a StatementOptions,
        values: &'b [V],
    ) -> Self::Request<'a> {
        options.to_batch(
            consistency,
            serial_consistency,
            self.r#type,
            self.statements,
            values,
        )
    }

    fn partition(
        &self,
        values: &&'b [V],
        token: Option<Token>,
    ) -> Result<Option<Partition>, PartitionKeyError> {
        self.statements
            .get(0)
            .and_then(|s| Some((s, values.get(0)?)))
            .and_then(|(s, v)| s.partition(v, token).transpose())
            .transpose()
    }

    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        self.statements.get(0).and_then(|s| s.result_specs())
    }

    fn idempotent(&self) -> bool {
        self.statements.iter().all(S::idempotent)
    }

    fn is_lwt(&self) -> Option<bool> {
        self.statements.get(0)?.is_lwt()
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
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        options: &'a StatementOptions,
        values: &'b [V],
    ) -> Self::Request<'a> {
        options.to_batch(
            consistency,
            serial_consistency,
            self.r#type,
            &self.statements,
            values,
        )
    }

    fn partition(
        &self,
        values: &&'b [V],
        token: Option<Token>,
    ) -> Result<Option<Partition>, PartitionKeyError> {
        self.statements
            .get(0)
            .and_then(|s| Some((s, values.get(0)?)))
            .and_then(|(s, v)| s.partition(v, token).transpose())
            .transpose()
    }

    fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
        self.statements.get(0).and_then(|s| s.result_specs())
    }

    fn idempotent(&self) -> bool {
        self.statements.iter().all(S::idempotent)
    }

    fn is_lwt(&self) -> Option<bool> {
        self.statements.get(0)?.is_lwt()
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
                consistency: Consistency,
                serial_consistency: Option<SerialConsistency>,
                options: &'a StatementOptions,
                values: (V0, $($values),*),
            ) -> Self::Request<'a> {
                options.to_batch(
                    consistency,
                    serial_consistency,
                    Default::default(),
                    (&self.0, $(&self.$idx),*),
                    values,
                )
            }

            fn partition(&self, values: &(V0, $($values),*), token: Option<Token>) -> Result<Option<Partition>, PartitionKeyError> {
                self.0.partition(&values.0, token)
            }

            fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
                self.0.result_specs()
            }

            fn idempotent(&self) -> bool {
                self.0.idempotent() $(|| self.$idx.idempotent())*
            }

            fn is_lwt(&self) -> Option<bool> {
                self.0.is_lwt()
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
                consistency: Consistency,
                serial_consistency: Option<SerialConsistency>,
                options: &'a StatementOptions,
                values: (V0, $($values, )*),
            ) -> Self::Request<'a> {
                options.to_batch(
                    consistency,
                    serial_consistency,
                    self.r#type,
                    (&self.statements.0, $(&self.statements.$idx),*),
                    values,
                )
            }

            fn partition(&self, values: &(V0, $($values, )*), token: Option<Token>) -> Result<Option<Partition>, PartitionKeyError> {
                self.statements.0.partition(&values.0, token)
            }

            fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
                self.statements.0.result_specs()
            }

            fn idempotent(&self) -> bool {
                self.statements.0.idempotent() $(|| self.statements.$idx.idempotent())*
            }

            fn is_lwt(&self) -> Option<bool> {
                self.statements.0.is_lwt()
            }
        }


        impl<'b, S, V0, $($values),*> Statement<(V0, $($values),*)> for BatchN<$len, S>
        where
            S: BatchStatement + Statement<V0> $(+ Statement<$values>)*,
            V0: QueryValues + SerializePartitionKey,
            $($values: QueryValues),*
        {
            type Request<'a> = CqlBatch<'a, &'a [S; $len], (V0, $($values),*)> where Self: 'a;

            fn as_request<'a>(
                &'a self,
                consistency: Consistency,
                serial_consistency: Option<SerialConsistency>,
                options: &'a StatementOptions,
                values: (V0, $($values, )*),
            ) -> Self::Request<'a> {
                options.to_batch(
                    consistency,
                    serial_consistency,
                    self.r#type,
                    &self.statements,
                    values,
                )
            }

            fn partition(&self, values: &(V0, $($values, )*), token: Option<Token>) -> Result<Option<Partition>, PartitionKeyError> {
                <_ as Statement<V0>>::partition(&self.statements[0], (&values.0), token)
            }

            fn result_specs(&self) -> Option<Arc<[ColumnSpec]>> {
                 <_ as Statement<V0>>::result_specs(&self.statements[0])
            }

            fn idempotent(&self) -> bool {
                Statement::<V0>::idempotent(&self.statements[0]) $(|| Statement::<$values>::idempotent(&self.statements[$idx]))*
            }

            fn is_lwt(&self) -> Option<bool> {
                <_ as Statement<V0>>::is_lwt(&self.statements[0])
            }
        }
    };
}

tuples!(batch);
