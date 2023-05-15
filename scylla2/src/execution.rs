pub mod load_balancing;
pub mod profile;
pub mod retry;
pub mod speculative;
pub(crate) mod utils;

use std::{collections::HashMap, ops::Deref, sync::Arc};

use scylla2_cql::{
    event::SchemaChangeEvent,
    request::{Request, RequestExt},
    response::{
        result::{
            column_spec::ColumnSpec,
            rows::{PagingState, RowIterator, RowParser, Rows},
            CqlResult,
        },
        Response, ResponseBody,
    },
    Consistency,
};
use uuid::Uuid;

use crate::{
    error::{ExecutionError, RequestError, RowsError},
    execution::{
        profile::ExecutionProfile,
        retry::{RetryDecision, RetryableError},
    },
    topology::{node::Node, partitioner::Token},
    utils::invalid_response,
};

#[derive(Debug)]
pub struct ExecutionInfo {
    node: Arc<Node>,
    profile: Arc<ExecutionProfile>,
    achieved_consistency: Consistency,
}

impl ExecutionInfo {
    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    pub fn profile(&self) -> &Arc<ExecutionProfile> {
        &self.profile
    }

    pub fn achieved_consistency(&self) -> Consistency {
        self.achieved_consistency
    }
}

#[derive(Debug)]
pub struct ExecutionResult {
    tracing_id: Option<Uuid>,
    custom_payload: HashMap<String, Vec<u8>>,
    warnings: Vec<String>,
    column_specs: Option<Arc<[ColumnSpec]>>,
    result: CqlResult,
    info: ExecutionInfo,
}

impl ExecutionResult {
    pub(crate) fn new(
        response: Response,
        column_specs: Option<Arc<[ColumnSpec]>>,
        info: ExecutionInfo,
    ) -> Result<Self, ExecutionError> {
        let response = response.ok()?;
        let result = match response.body {
            ResponseBody::Result(result) => result,
            other => return Err(invalid_response(other).into()),
        };
        Ok(ExecutionResult {
            tracing_id: response.tracing_id,
            custom_payload: response.custom_payload,
            warnings: response.warnings,
            result,
            column_specs,
            info,
        })
    }

    pub fn tracing_id(&self) -> Option<Uuid> {
        self.tracing_id
    }

    pub fn custom_payload(&self) -> &HashMap<String, Vec<u8>> {
        &self.custom_payload
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    pub fn paging_state(&self) -> Option<&PagingState> {
        self.as_rows()?.metadata.paging_state.as_ref()
    }

    pub fn column_specs(&self) -> Option<&Arc<[ColumnSpec]>> {
        self.column_specs
            .as_ref()
            .or_else(|| self.as_rows()?.metadata.column_specs.as_ref())
    }

    pub fn as_rows(&self) -> Option<&Rows> {
        match &self.result {
            CqlResult::Rows(rows) => Some(rows),
            _ => None,
        }
    }

    pub fn into_rows(self) -> Option<Rows> {
        match self.result {
            CqlResult::Rows(rows) => Some(rows),
            _ => None,
        }
    }

    pub fn rows<'a, P>(&'a self) -> Result<RowIterator<'a, P>, RowsError>
    where
        P: RowParser<'a>,
    {
        Ok(self
            .as_rows()
            .ok_or(RowsError::NoRows)?
            .parse(self.column_specs().map(Deref::deref))
            .ok_or(RowsError::NoMetadata)??)
    }

    pub fn as_schema_change(&self) -> Option<&SchemaChangeEvent> {
        match &self.result {
            CqlResult::SchemaChange(change) => Some(change),
            _ => None,
        }
    }

    pub fn into_schema_change(self) -> Option<SchemaChangeEvent> {
        match self.result {
            CqlResult::SchemaChange(change) => Some(change),
            _ => None,
        }
    }

    pub fn info(&self) -> &ExecutionInfo {
        &self.info
    }

    pub async fn wait_schema_agreement(&self) -> Uuid {
        self.info.node.wait_schema_agreement().await
    }
}

pub(crate) async fn make_request(
    request: &impl Request,
    custom_payload: Option<&HashMap<String, Vec<u8>>>,
    idempotent: bool,
    profile: Arc<ExecutionProfile>,
    query_plan: impl Iterator<Item = &Arc<Node>>,
    token: Option<Token>,
) -> Result<(Response, ExecutionInfo), ExecutionError> {
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
                    let info = ExecutionInfo {
                        node: node.clone(),
                        achieved_consistency: consistency.unwrap_or(profile.consistency),
                        profile,
                    };
                    return Ok((response, info));
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
