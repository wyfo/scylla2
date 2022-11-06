use std::{fmt, io};

use crate::{
    cql::ReadCql,
    utils::{format_with_parens, invalid_data},
};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CqlType {
    Custom(String),
    Ascii,
    BigInt,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Double,
    Float,
    Int,
    Timestamp,
    Uuid,
    Text,
    Varint,
    Timeuuid,
    Tuple(Vec<CqlType>),
    Inet,
    Date,
    Time,
    SmallInt,
    TinyInt,
    List(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),
    Set(Box<CqlType>),
    Udt {
        keyspace: String,
        type_name: String,
        fields: Vec<(String, CqlType)>,
    },
}

impl CqlType {
    pub fn deserialize(buf: &mut &[u8]) -> io::Result<CqlType> {
        Ok(match i16::read_cql(buf)? as u16 {
            0x0000 => CqlType::Custom(String::read_cql(buf)?),
            0x0001 => CqlType::Ascii,
            0x0002 => CqlType::BigInt,
            0x0003 => CqlType::Blob,
            0x0004 => CqlType::Boolean,
            0x0005 => CqlType::Counter,
            0x0006 => CqlType::Decimal,
            0x0007 => CqlType::Double,
            0x0008 => CqlType::Float,
            0x0009 => CqlType::Int,
            0x000B => CqlType::Timestamp,
            0x000C => CqlType::Uuid,
            0x000D => CqlType::Text,
            0x000E => CqlType::Varint,
            0x000F => CqlType::Timeuuid,
            0x0010 => CqlType::Inet,
            0x0011 => CqlType::Date,
            0x0012 => CqlType::Time,
            0x0013 => CqlType::SmallInt,
            0x0014 => CqlType::TinyInt,
            0x0020 => CqlType::List(Box::new(CqlType::deserialize(buf)?)),
            0x0021 => CqlType::Map(
                Box::new(CqlType::deserialize(buf)?),
                Box::new(CqlType::deserialize(buf)?),
            ),
            0x0022 => CqlType::Set(Box::new(CqlType::deserialize(buf)?)),
            0x0030 => CqlType::Udt {
                keyspace: String::read_cql(buf)?,
                type_name: String::read_cql(buf)?,
                fields: {
                    let mut vec = Vec::with_capacity(u16::read_cql(buf)? as usize);
                    for _ in 0..vec.capacity() {
                        vec.push((String::read_cql(buf)?, CqlType::deserialize(buf)?));
                    }
                    vec
                },
            },
            0x0031 => CqlType::Tuple({
                let mut vec = Vec::with_capacity(u16::read_cql(buf)? as usize);
                for _ in 0..vec.capacity() {
                    vec.push(CqlType::deserialize(buf)?);
                }
                vec
            }),
            code => {
                return Err(format!("Unsupported cql type code {code:#02x}")).map_err(invalid_data)
            }
        })
    }
}

impl fmt::Display for CqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Custom(custom) => write!(f, "{custom}"),
            Self::Ascii => write!(f, "ascii"),
            Self::BigInt => write!(f, "biging"),
            Self::Blob => write!(f, "blob"),
            Self::Boolean => write!(f, "boolean"),
            Self::Counter => write!(f, "counter"),
            Self::Decimal => write!(f, "decimal"),
            Self::Double => write!(f, "double"),
            Self::Float => write!(f, "float"),
            Self::Int => write!(f, "int"),
            Self::Timestamp => write!(f, "timestamp"),
            Self::Uuid => write!(f, "uuid"),
            Self::Text => write!(f, "text"),
            Self::Varint => write!(f, "varint"),
            Self::Timeuuid => write!(f, "timeuuid"),
            Self::Tuple(types) => write!(f, "tuple<{types}>", types = format_with_parens(types)),
            Self::Inet => write!(f, "inet"),
            Self::Date => write!(f, "date"),
            Self::Time => write!(f, "time"),
            Self::SmallInt => write!(f, "smallint"),
            Self::TinyInt => write!(f, "tinyint"),
            Self::List(tp) => write!(f, "list<{tp}>"),
            Self::Map(key, value) => write!(f, "map<{key}, {value}>"),
            Self::Set(tp) => write!(f, "set<{tp}>"),
            Self::Udt {
                keyspace,
                type_name,
                fields,
            } => {
                let fields =
                    format_with_parens(fields.iter().map(|(name, tp)| format!("{name} {tp}")));
                write!(f, "{keyspace}.{type_name} ({fields})",)
            }
        }
    }
}
