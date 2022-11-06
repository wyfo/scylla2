use std::{collections::HashMap, io, sync::Arc};

use futures::TryFutureExt;
use scylla2_cql::{
    event::SchemaChangeEvent,
    response::{
        result::{
            column_spec::ColumnSpec,
            rows::{RowIterator, RowParser, Rows},
            CqlResult,
        },
        Response, ResponseBody,
    },
};
use uuid::Uuid;

use crate::{
    error::RowsError,
    utils::{invalid_response, other_error},
};

#[derive(Debug)]
pub struct ExecutionResult {
    tracing_id: Option<Uuid>,
    custom_payload: HashMap<String, Vec<u8>>,
    warnings: Vec<String>,
    column_specs: Option<Arc<[ColumnSpec]>>,
    result: CqlResult,
}

impl ExecutionResult {
    pub(crate) fn from_response(
        response: Response,
        column_specs: Option<Arc<[ColumnSpec]>>,
    ) -> io::Result<ExecutionResult> {
        let result = match response.body {
            ResponseBody::Result(result) => result,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Invalid response: {other:?}"),
                ))
            }
        };
        Ok(ExecutionResult {
            tracing_id: response.tracing_id,
            custom_payload: response.custom_payload,
            warnings: response.warnings,
            column_specs,
            result,
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

    pub fn schema_change(self) -> Option<SchemaChangeEvent> {
        match self.result {
            CqlResult::SchemaChange(change) => Some(change),
            _ => None,
        }
    }

    pub fn as_schema_change(&self) -> Option<&SchemaChangeEvent> {
        match self.result {
            CqlResult::SchemaChange(ref change) => Some(change),
            _ => None,
        }
    }

    pub fn rows<'a, P>(&'a self) -> Result<RowIterator<'a, P>, RowsError>
    where
        P: RowParser<'a>,
    {
        match self.result {
            CqlResult::Rows(ref rows) => Ok(rows
                .parse(self.column_specs.as_deref())
                .ok_or(RowsError::NoMetadata)??),
            _ => Err(RowsError::NoRows),
        }
    }
}

fn row_iterator<'a, P>(rows: &'a Rows) -> io::Result<impl Iterator<Item = io::Result<P>> + 'a>
where
    P: RowParser<'a> + 'a,
{
    Ok(rows
        .parse(None)
        .ok_or_else(|| other_error("Missing result metadata"))?
        .map_err(other_error)?
        .map(|row| row.map_err(other_error)))
}

pub(crate) fn cql_rows<P, T, B>(response: Response, map: impl (Fn(P) -> T) + Clone) -> io::Result<B>
where
    B: FromIterator<T>,
    P: for<'a> RowParser<'a>,
{
    match response.body {
        ResponseBody::Result(CqlResult::Rows(rows)) => row_iterator(&rows)?
            .map(|row| row.map(map.clone()))
            .collect(),
        other => Err(invalid_response(other)),
    }
}
pub(crate) fn maybe_cql_row<P>(response: Response) -> io::Result<Option<P>>
where
    P: for<'a> RowParser<'a>,
{
    match response.body {
        ResponseBody::Result(CqlResult::Rows(rows)) => {
            let mut iter = row_iterator(&rows)?.fuse();
            let res = iter.next();
            if iter.next().is_some() {
                return Err(other_error("More than one row"));
            }
            res.transpose()
        }
        other => Err(invalid_response(other)),
    }
}

pub fn peers_and_local<P, T, B>(
    peers: Response,
    local: Response,
    map: impl (Fn(P) -> T) + Clone,
) -> io::Result<B>
where
    B: FromIterator<T> + Extend<T>,
    P: for<'a> RowParser<'a>,
{
    let mut container: B = cql_rows(peers, map.clone())?;
    container.extend(maybe_cql_row(local)?.map(map));
    Ok(container)
}
