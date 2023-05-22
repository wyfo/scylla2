use std::{collections::HashMap, sync::Arc};

use scylla2_cql::{
    response::{
        result::{
            column_spec::ColumnSpec,
            lwt::LwtApplied,
            rows::{PagingState, Row, RowIterator, Rows},
            CqlResult,
        },
        Response, ResponseBody,
    },
    Consistency,
};
use uuid::Uuid;

use crate::{
    error::{ExecutionError, LwtAppliedError, RowsError},
    topology::{node::Node, partitioner::Token},
    utils::invalid_response,
};

#[derive(Debug)]
pub struct ExecutionResult {
    tracing_id: Option<Uuid>,
    custom_payload: HashMap<String, Vec<u8>>,
    warnings: Vec<String>,
    column_specs: Option<Arc<[ColumnSpec]>>,
    result: CqlResult,
    node: Arc<Node>,
    token: Option<Token>,
    achieved_consistency: Consistency,
}

impl ExecutionResult {
    pub(crate) fn new(
        response: Response,
        column_specs: Option<Arc<[ColumnSpec]>>,
        node: Arc<Node>,
        token: Option<Token>,
        achieved_consistency: Consistency,
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
            node,
            token,
            achieved_consistency,
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

    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    pub fn token(&self) -> Option<Token> {
        self.token
    }

    pub fn achieved_consistency(&self) -> Consistency {
        self.achieved_consistency
    }

    pub fn as_result(&self) -> &CqlResult {
        &self.result
    }

    pub fn into_result(self) -> CqlResult {
        self.result
    }

    fn as_rows(&self) -> Option<&Rows> {
        match &self.result {
            CqlResult::Rows(rows) => Some(rows),
            _ => None,
        }
    }

    pub fn paging_state(&self) -> Option<&PagingState> {
        self.as_rows()?.metadata.paging_state.as_ref()
    }

    pub fn column_specs(&self) -> Option<&Arc<[ColumnSpec]>> {
        self.column_specs
            .as_ref()
            .or_else(|| self.as_rows()?.metadata.column_specs.as_ref())
    }

    pub fn rows<'a, R>(&'a self) -> Result<RowIterator<'a, R>, RowsError>
    where
        R: Row<'a>,
    {
        Ok(self
            .as_rows()
            .ok_or(RowsError::NoRows)?
            .parse(self.column_specs.as_deref())
            .ok_or(RowsError::NoMetadata)??)
    }

    pub fn lwt_applied(&self) -> Result<bool, LwtAppliedError> {
        Ok(self
            .rows::<LwtApplied>()?
            .next()
            .ok_or(RowsError::NoRows)??
            .0)
    }
}
