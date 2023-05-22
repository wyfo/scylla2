use bytes::Bytes;

use crate::{
    cql_type::CqlType,
    error::{ParseError, TypeError},
    response::result::{column_spec::ColumnSpec, rows::Row},
    value::ReadValueExt,
};

#[derive(Debug)]
pub struct LwtApplied(pub bool);

impl<'a> Row<'a> for LwtApplied {
    fn check_column_specs(column_specs: &[ColumnSpec]) -> Result<(), TypeError> {
        match column_specs.first() {
            Some(ColumnSpec {
                r#type: CqlType::Boolean,
                name,
                ..
            }) if name.as_str() == "[applied]" => Ok(()),
            _ => Err(TypeError),
        }
    }

    fn parse_row(
        col_specs: &[ColumnSpec],
        envelope: &'a Bytes,
        bytes: &mut &'a [u8],
    ) -> Result<Self, ParseError> {
        let applied = bool::read_value_with_size(bytes, envelope)?;
        for _ in 1..col_specs.len() {
            Option::<&[u8]>::read_value_with_size(bytes, envelope)?;
        }
        Ok(LwtApplied(applied))
    }
}
