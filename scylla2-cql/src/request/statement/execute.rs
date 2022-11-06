use crate::{
    cql::{CqlWrite, ShortBytes},
    error::ValueTooBig,
    extensions::Extensions,
    frame::envelope::OpCode,
    request::statement::{query, query_values::QueryValues, Statement, StatementParameters},
    response::result::PreparedStatement,
    ProtocolVersion,
};

impl<V> Statement<V> for PreparedStatement
where
    V: QueryValues,
{
    const OPCODE: OpCode = OpCode::Query;

    fn serialized_size(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&Extensions>,
        params: impl StatementParameters,
        values: &V,
    ) -> Result<usize, ValueTooBig> {
        Ok(ShortBytes(&self.id).cql_size()?
            + query::query_parameters_size(version, params, Some(values))?)
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&Extensions>,
        mut slice: &mut [u8],
        params: impl StatementParameters,
        values: &V,
    ) {
        ShortBytes(&self.id).write_cql(&mut slice);
        query::write_query_parameters(
            version,
            &mut slice,
            self.result_metadata.is_some(),
            params,
            Some(values),
        )
    }
}

// impl<'a, C, V, P> AsStatement<'a, C, V, P> for PreparedStatement
// where
//     V: QueryValues,
// {
//     type Statement = &'a PreparedStatement;
//
//     fn as_statement(&'a self) -> Self::Statement {
//         self
//     }
//
//     fn config(&'a self) -> Option<C> {
//         None
//     }
//
//     fn partition(&'a self, _values: &V) -> Option<P> {
//         None
//     }
// }
