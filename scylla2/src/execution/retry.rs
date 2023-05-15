use std::{fmt, io};

use scylla2_cql::{
    error::{DatabaseError, DatabaseErrorKind},
    response::error::WriteType,
    Consistency, LegacyConsistency, SerialConsistency,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RetryDecision {
    DoNotRetry,
    RetrySameNode(Option<Consistency>),
    RetryNextNode(Option<Consistency>),
}

#[derive(Debug, Clone, Copy)]
pub enum RetryableError<'a> {
    Io(&'a io::Error),
    Database(&'a DatabaseError),
}

pub trait RetryPolicy: fmt::Debug + Send + Sync {
    fn retry(
        &self,
        error: RetryableError<'_>,
        idempotent: bool,
        retry_count: usize,
    ) -> RetryDecision;
}

#[derive(Debug)]
pub struct FallthroughRetryPolicy;

impl RetryPolicy for FallthroughRetryPolicy {
    fn retry(
        &self,
        _error: RetryableError<'_>,
        _idempotent: bool,
        _retry_count: usize,
    ) -> RetryDecision {
        RetryDecision::DoNotRetry
    }
}

#[derive(Debug)]
pub struct DefaultRetryPolicy;

impl RetryPolicy for DefaultRetryPolicy {
    fn retry(
        &self,
        error: RetryableError<'_>,
        idempotent: bool,
        retry_count: usize,
    ) -> RetryDecision {
        if retry_count > 0 {
            return RetryDecision::DoNotRetry;
        }
        let error_kind = match error {
            RetryableError::Database(error) => &error.kind,
            _ if idempotent => return RetryDecision::RetryNextNode(None),
            _ => return RetryDecision::DoNotRetry,
        };
        match error_kind {
            DatabaseErrorKind::IsBootstrapping => RetryDecision::RetryNextNode(None),
            DatabaseErrorKind::Overloaded
            | DatabaseErrorKind::ServerError
            | DatabaseErrorKind::TruncateError
                if idempotent =>
            {
                RetryDecision::RetryNextNode(None)
            }
            DatabaseErrorKind::Unavailable { .. } => RetryDecision::RetryNextNode(None),
            DatabaseErrorKind::ReadTimeout {
                consistency,
                blockfor,
                received,
                data_present,
            } if !matches!(
                consistency,
                LegacyConsistency::Serial(SerialConsistency::Serial)
            ) && *received >= *blockfor
                && !*data_present =>
            {
                RetryDecision::RetrySameNode(None)
            }
            DatabaseErrorKind::WriteTimeout { write_type, .. }
                if idempotent && *write_type == WriteType::BatchLog =>
            {
                RetryDecision::RetrySameNode(None)
            }
            _ => RetryDecision::DoNotRetry,
        }
    }
}

#[derive(Debug)]
pub struct ConsistencyDowngradingRetryPolicy;

impl RetryPolicy for ConsistencyDowngradingRetryPolicy {
    fn retry(
        &self,
        error: RetryableError<'_>,
        idempotent: bool,
        retry_count: usize,
    ) -> RetryDecision {
        let decision = DefaultRetryPolicy.retry(error, idempotent, retry_count);
        if decision != RetryDecision::DoNotRetry || retry_count > 0 {
            return decision;
        }
        let RetryableError::Database(db_error) = error else { unreachable!() };
        if let DatabaseErrorKind::Unavailable {
            consistency,
            alive: received,
            ..
        }
        | DatabaseErrorKind::ReadTimeout {
            consistency,
            received,
            ..
        }
        | DatabaseErrorKind::WriteTimeout {
            consistency,
            received,
            ..
        } = &db_error.error.kind
        {
            if *received >= 3 {
                return RetryDecision::RetrySameNode(Some(Consistency::Three));
            } else if *received == 2 {
                return RetryDecision::RetrySameNode(Some(Consistency::Two));
            } else if *received == 1
                || matches!(
                    consistency,
                    LegacyConsistency::Regular(Consistency::EachQuorum)
                )
            {
                return RetryDecision::RetrySameNode(Some(Consistency::One));
            }
        }
        RetryDecision::DoNotRetry
    }
}
