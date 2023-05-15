use std::{collections::HashMap, sync::Arc};

use scylla2_cql::{
    request::{Request, RequestExt},
    response::Response,
    Consistency,
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

pub(crate) async fn make_request(
    request: &impl Request,
    custom_payload: Option<&HashMap<String, Vec<u8>>>,
    idempotent: bool,
    profile: &ExecutionProfile,
    query_plan: impl Iterator<Item = &Arc<Node>>,
    token: Option<Token>,
) -> Result<(Response, Arc<Node>, Consistency), ExecutionError> {
    let mut consistency = None;
    let mut retry_count = 0;
    for (node, conn) in query_plan.filter_map(|node| Some((node, node.get_connection(token)?))) {
        loop {
            let result = match consistency {
                Some(consistency) => {
                    conn.send_queued(
                        request.with_consistency(consistency),
                        profile.tracing,
                        custom_payload,
                    )
                    .await
                }
                None => conn.send_queued(request, profile.tracing, None).await,
            };
            let retry = |err| profile.retry_policy.retry(err, idempotent, retry_count);
            let (retry_decision, error): (_, ExecutionError) = match result.map(Response::ok) {
                Ok(Ok(response)) => {
                    let achieved_consistency = consistency.unwrap_or(profile.consistency);
                    return Ok((response, node.clone(), achieved_consistency));
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

// pub(crate) async fn make_request(
//     request: &impl Request,
//     custom_payload: Option<&HashMap<String, Vec<u8>>>,
//     idempotent: bool,
//     profile: Arc<ExecutionProfile>,
//     query_plan: impl Iterator<Item = &Arc<Node>>,
//     token: Option<Token>,
// ) -> Result<(Response, ExecutionInfo), ExecutionError> {
//     if let Some(speculative) = &profile.speculative_execution_policy {
//         todo!()
//     } else {
//         make_request_internal(
//             request,
//             custom_payload,
//             idempotent,
//             profile,
//             query_plan,
//             token,
//         )
//         .await
//     }
// }
