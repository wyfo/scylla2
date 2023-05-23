use crate::{
    cql_type::CqlType,
    error::{BoxedError, ParseError},
    response::result::{
        column_spec::ColumnSpec,
        rows::{Row, RowsSlice},
    },
};

#[derive(Debug)]
pub struct LwtApplied(pub bool);

impl<'a> Row<'a> for LwtApplied {
    fn check_column_specs(column_specs: &[ColumnSpec]) -> Result<(), BoxedError> {
        match column_specs.first() {
            Some(ColumnSpec {
                r#type: CqlType::Boolean,
                name,
                ..
            }) if name.as_str() == "[applied]" => Ok(()),
            _ => Err("No column [applied]".into()),
        }
    }

    fn parse_row(col_specs: &[ColumnSpec], slice: &mut RowsSlice<'a>) -> Result<Self, ParseError> {
        let applied = slice.parse_value()?;
        for _ in 1..col_specs.len() {
            slice.parse_value_slice()?;
        }
        Ok(LwtApplied(applied))
    }
}
