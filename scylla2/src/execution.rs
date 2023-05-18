use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use futures::{stream::FuturesUnordered, StreamExt};
use scylla2_cql::{
    request::{Request, RequestExt},
    response::Response,
};

use crate::{
    error::{ExecutionError, RequestError},
    execution::retry::{RetryDecision, RetryableError},
    topology::{node::Node, partitioner::Token},
};

pub mod load_balancing;
mod profile;
mod result;
pub mod retry;
pub mod speculative;
pub(crate) mod utils;

pub use crate::execution::{profile::ExecutionProfile, result::ExecutionResult};
use crate::{statement::Statement, utils::SharedIterator};

pub(crate) struct Execution<'a, S, V, R> {
    pub(crate) statement: &'a S,
    pub(crate) request: &'a R,
    pub(crate) custom_payload: Option<&'a HashMap<String, Vec<u8>>>,
    pub(crate) profile: &'a ExecutionProfile,
    pub(crate) _phantom: PhantomData<V>,
}

impl<'a, S, V, R> Execution<'a, S, V, R> {
    pub(crate) fn new(
        statement: &'a S,
        request: &'a R,
        custom_payload: Option<&'a HashMap<String, Vec<u8>>>,
        profile: &'a ExecutionProfile,
    ) -> Self {
        Self {
            statement,
            request,
            custom_payload,
            profile,
            _phantom: PhantomData,
        }
    }
}

impl<'a, S, V, R> Execution<'a, S, V, R>
where
    S: Statement<V>,
    R: Request,
{
    pub(crate) async fn run(
        &self,
        query_plan: impl Iterator<Item = &Arc<Node>>,
        token: Option<Token>,
    ) -> Result<ExecutionResult, ExecutionError> {
        let execution = self.run_speculative(query_plan, token);
        if let Some(timeout) = self.profile.request_timeout {
            tokio::time::timeout(timeout, execution).await?
        } else {
            execution.await
        }
    }

    async fn run_speculative(
        &self,
        query_plan: impl Iterator<Item = &Arc<Node>>,
        token: Option<Token>,
    ) -> Result<ExecutionResult, ExecutionError> {
        if let Some(speculative) = &self.profile.speculative_execution_policy {
            let iterator = SharedIterator(Mutex::new(query_plan));
            let mut executions = FuturesUnordered::new();
            loop {
                executions.push(self.run_retry(&iterator, token));
                let Some(delay) =  speculative.next_delay(executions.len()) else {
                    return executions.next().await.unwrap()
                };
                tokio::select! {
                    biased;
                    res = executions.next() => match res {
                        None => return Err(ExecutionError::NoConnection),
                        Some(Err(ExecutionError::NoConnection)) => {},
                        Some(res) => return res
                    },
                    _ = tokio::time::sleep(delay) => {}
                }
            }
        } else {
            self.run_retry(query_plan, token).await
        }
    }

    async fn run_retry(
        &self,
        query_plan: impl Iterator<Item = &Arc<Node>>,
        token: Option<Token>,
    ) -> Result<ExecutionResult, ExecutionError> {
        let mut consistency = None;
        let mut retry_count = 0;
        for (node, conn) in query_plan.filter_map(|node| Some((node, node.get_connection(token)?)))
        {
            loop {
                let result = match consistency {
                    Some(consistency) => {
                        conn.send_queued(
                            self.request.with_consistency(consistency),
                            self.profile.tracing,
                            self.custom_payload,
                        )
                        .await
                    }
                    None => {
                        conn.send_queued(self.request, self.profile.tracing, None)
                            .await
                    }
                };
                let retry = |err| {
                    self.profile
                        .retry_policy
                        .retry(err, self.statement.idempotent(), retry_count)
                };
                let (retry_decision, error) = match result.map(Response::ok) {
                    Ok(Ok(response)) => {
                        return ExecutionResult::new(
                            response,
                            self.statement.result_specs(),
                            node.clone(),
                            token,
                            consistency.unwrap_or(self.profile.consistency),
                        );
                    }
                    Ok(Err(err)) => (retry(RetryableError::Database(&err)), err.into()),
                    Err(RequestError::Io(err)) => (retry(RetryableError::Io(&err)), err.into()),
                    Err(RequestError::ConnectionClosed | RequestError::NoStreamAvailable) => break,
                    Err(RequestError::InvalidRequest(err)) => return Err(err.into()),
                };
                retry_count += 1;
                match retry_decision {
                    RetryDecision::DoNotRetry => return Err(error),
                    RetryDecision::RetrySameNode(cons) => consistency = cons.or(consistency),
                    RetryDecision::RetryNextNode(cons) => {
                        consistency = cons.or(consistency);
                        break;
                    }
                }
            }
        }
        Err(ExecutionError::NoConnection)
    }
}
