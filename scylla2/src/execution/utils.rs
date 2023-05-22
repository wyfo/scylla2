use std::io;

use scylla2_cql::{
    request::query::{parameters::QueryParameters, Query as CqlQuery},
    response::{
        result::{
            rows::{Row, Rows},
            CqlResult,
        },
        Response, ResponseBody,
    },
};

use crate::utils::{invalid_response, other_error};

pub(crate) fn cql_query<V>(query: &str, values: V) -> CqlQuery<V> {
    CqlQuery {
        query,
        parameters: QueryParameters::with_values(values),
    }
}

fn row_iterator<'a, R>(rows: &'a Rows) -> io::Result<impl Iterator<Item = io::Result<R>> + 'a>
where
    R: Row<'a> + 'a,
{
    Ok(rows
        .parse(None)
        .ok_or_else(|| other_error("Missing result metadata"))?
        .map_err(other_error)?
        .map(|row| row.map_err(other_error)))
}

pub(crate) fn cql_rows<R, T, B>(response: Response, map: impl (Fn(R) -> T) + Clone) -> io::Result<B>
where
    B: FromIterator<T>,
    R: for<'a> Row<'a>,
{
    match response.body {
        ResponseBody::Result(CqlResult::Rows(rows)) => row_iterator(&rows)?
            .map(|row| row.map(map.clone()))
            .collect(),
        other => Err(invalid_response(other)),
    }
}

pub(crate) fn maybe_cql_row<R>(response: Response) -> io::Result<Option<R>>
where
    R: for<'a> Row<'a>,
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

pub(crate) fn peers_and_local<R, T, B>(
    peers: Response,
    local: Response,
    map: impl (Fn(R) -> T) + Clone,
) -> io::Result<B>
where
    B: FromIterator<T> + Extend<T>,
    R: for<'a> Row<'a>,
{
    let mut container: B = cql_rows(peers, map.clone())?;
    container.extend(maybe_cql_row(local)?.map(map));
    Ok(container)
}
