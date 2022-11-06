use std::{fmt, io, sync::Arc};

use crate::{cql::ReadCql, cql_type::CqlType};

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub keyspace: Arc<str>,
    pub table: Arc<str>,
    pub name: String,
    pub r#type: CqlType,
}

impl fmt::Display for ColumnSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{ks}.{tb}.{name} {tp}",
            ks = self.keyspace,
            tb = self.table,
            name = self.name,
            tp = self.r#type
        )
    }
}

pub fn deserialize_column_specs(
    columns_count: u32,
    global_table_spec: Option<(&str, &str)>,
    buf: &mut &[u8],
) -> io::Result<Vec<ColumnSpec>> {
    let global_table_spec = global_table_spec.map(|(ks, tb)| (ks.into(), tb.into()));
    let mut column_specs: Vec<ColumnSpec> = Vec::new();
    for _ in 0..columns_count {
        let (keyspace, table) = if let Some(table_spec) = global_table_spec.clone() {
            table_spec
        } else {
            let (keyspace, table) = (<&str>::read_cql(buf)?, <&str>::read_cql(buf)?);
            match column_specs.first() {
                Some(col_spec)
                    if keyspace == col_spec.keyspace.as_ref()
                        && table == col_spec.table.as_ref() =>
                {
                    (col_spec.keyspace.clone(), col_spec.table.clone())
                }
                _ => (keyspace.into(), table.into()),
            }
        };
        let name = String::read_cql(buf)?;
        let r#type = CqlType::deserialize(buf)?;
        column_specs.push(ColumnSpec {
            keyspace,
            table,
            name,
            r#type,
        })
    }
    Ok(column_specs)
}
