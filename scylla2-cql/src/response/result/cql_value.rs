use std::net::IpAddr;

use bigdecimal::BigDecimal;
use bytes::Bytes;
use num_bigint::BigInt;
use uuid::Uuid;

use crate::{
    cql::ReadCql,
    cql_type::CqlType,
    error::{ParseError, TypeError},
    response::result::{column_spec::ColumnSpec, rows::RowParser},
    utils::invalid_data,
    value::ReadValue,
};

const NULL_VALUE_IN_COLLECTION: &str = "Null value in collection";

#[derive(Debug)]
#[non_exhaustive]
pub enum CqlValue {
    Ascii(String),
    BigInt(i64),
    Blob(Vec<u8>),
    Boolean(bool),
    Counter(i64),
    // TODO add custom
    Decimal(BigDecimal),
    Date(u32),
    Double(f64),
    // TODO add duration
    Float(f32),
    Int(i32),
    Inet(IpAddr),
    List(Vec<CqlValue>),
    Map(Vec<(CqlValue, CqlValue)>),
    Set(Vec<CqlValue>),
    SmallInt(i16),
    Text(String),
    TinyInt(i8),
    Time(i64),
    Timestamp(i64),
    Timeuuid(Uuid),
    Tuple(Vec<Option<CqlValue>>),
    Udt {
        keyspace: String,
        type_name: String,
        fields: Vec<(String, Option<CqlValue>)>,
    },
    Uuid(Uuid),
    Varint(BigInt),
}

impl CqlValue {
    pub fn parse(tp: &CqlType, buf: &mut &[u8]) -> Result<Option<Self>, ParseError> {
        let value_size = i32::read_cql(buf)?;
        if value_size < 0 {
            return Ok(None);
        }
        let value = Self::deserialize(tp, &buf[..value_size as usize])?;
        *buf = &buf[value_size as usize..];
        Ok(Some(value))
    }

    pub fn deserialize(tp: &CqlType, slice: &[u8]) -> Result<Self, ParseError> {
        let fake_bytes = Bytes::new();
        let fake_envelope = &fake_bytes;
        let value = match tp {
            CqlType::Ascii => {
                let str = <&str>::read_value(slice, fake_envelope)?;
                if str.is_ascii() {
                    return Err(invalid_data("Not an ascii string").into());
                }
                CqlValue::Ascii(str.into())
            }
            CqlType::BigInt => CqlValue::BigInt(i64::read_value(slice, fake_envelope)?),
            CqlType::Blob => CqlValue::Blob(<&[u8]>::read_value(slice, fake_envelope)?.into()),
            CqlType::Boolean => CqlValue::Boolean(bool::read_value(slice, fake_envelope)?),
            CqlType::Counter => CqlValue::Counter(i64::read_value(slice, fake_envelope)?),
            CqlType::Date => CqlValue::Date(i32::read_value(slice, fake_envelope)? as u32),
            CqlType::Decimal => CqlValue::Decimal(BigDecimal::read_value(slice, fake_envelope)?),
            CqlType::Double => CqlValue::Double(f64::read_value(slice, fake_envelope)?),
            CqlType::Float => CqlValue::Float(f32::read_value(slice, fake_envelope)?),
            CqlType::Inet => CqlValue::Inet(IpAddr::read_value(slice, fake_envelope)?),
            CqlType::Int => CqlValue::Int(i32::read_value(slice, fake_envelope)?),
            CqlType::List(tp) => CqlValue::List(Self::parse_vec(tp, slice)?),
            CqlType::Map(key_tp, value_tp) => {
                CqlValue::Map(Self::parse_map(key_tp, value_tp, slice)?)
            }
            CqlType::Set(tp) => CqlValue::Set(Self::parse_vec(tp, slice)?),
            CqlType::SmallInt => CqlValue::SmallInt(i16::read_value(slice, fake_envelope)?),
            CqlType::Text => CqlValue::Text(<&str>::read_value(slice, fake_envelope)?.into()),
            CqlType::TinyInt => CqlValue::TinyInt(i8::read_value(slice, fake_envelope)?),
            CqlType::Time => CqlValue::Time(i64::read_value(slice, fake_envelope)?),
            CqlType::Timeuuid => CqlValue::Timeuuid(Uuid::read_value(slice, fake_envelope)?),
            CqlType::Timestamp => CqlValue::Timestamp(i64::read_value(slice, fake_envelope)?),
            CqlType::Tuple(types) => CqlValue::Tuple(Self::parse_tuple_types(types, slice)?),
            CqlType::Udt {
                keyspace,
                type_name,
                fields,
            } => CqlValue::Udt {
                keyspace: keyspace.into(),
                type_name: type_name.into(),
                fields: Self::parse_udt_fields(fields, slice)?,
            },
            CqlType::Uuid => CqlValue::Uuid(Uuid::read_value(slice, fake_envelope)?),
            CqlType::Varint => CqlValue::Varint(BigInt::read_value(slice, fake_envelope)?),
            tp => return Err(invalid_data(format!("{tp:?} is not yet supported")).into()),
        };
        Ok(value)
    }

    fn parse_vec(tp: &CqlType, mut slice: &[u8]) -> Result<Vec<CqlValue>, ParseError> {
        let length = i32::read_cql(&mut slice)?;
        let mut vec = Vec::with_capacity(length.try_into().map_err(invalid_data)?);
        for _ in 0..length {
            let value = Self::parse(tp, &mut slice)?
                .ok_or(NULL_VALUE_IN_COLLECTION)
                .map_err(invalid_data)?;
            vec.push(value);
        }
        Ok(vec)
    }

    fn parse_map(
        key_tp: &CqlType,
        value_tp: &CqlType,
        mut slice: &[u8],
    ) -> Result<Vec<(CqlValue, CqlValue)>, ParseError> {
        let length = i32::read_cql(&mut slice)?;
        let mut vec = Vec::with_capacity(length.try_into().map_err(invalid_data)?);
        for _ in 0..length {
            let key = Self::parse(key_tp, &mut slice)?
                .ok_or(NULL_VALUE_IN_COLLECTION)
                .map_err(invalid_data)?;
            let value = Self::parse(value_tp, &mut slice)?
                .ok_or(NULL_VALUE_IN_COLLECTION)
                .map_err(invalid_data)?;
            vec.push((key, value));
        }
        Ok(vec)
    }
    fn parse_tuple_types(
        types: &[CqlType],
        mut slice: &[u8],
    ) -> Result<Vec<Option<CqlValue>>, ParseError> {
        let mut vec = Vec::with_capacity(types.len());
        for tp in types {
            vec.push(Self::parse(tp, &mut slice)?);
        }
        Ok(vec)
    }
    fn parse_udt_fields(
        fields: &[(String, CqlType)],
        mut slice: &[u8],
    ) -> Result<Vec<(String, Option<CqlValue>)>, ParseError> {
        let mut vec = Vec::with_capacity(fields.len());
        for (name, tp) in fields {
            vec.push((name.into(), Self::parse(tp, &mut slice)?));
        }
        Ok(vec)
    }
}

impl<'a> RowParser<'a> for Vec<Option<CqlValue>> {
    fn check_column_specs(_column_specs: &[ColumnSpec]) -> Result<(), TypeError> {
        Ok(())
    }

    fn parse_row(
        column_specs: &[ColumnSpec],
        _envelope: &'a Bytes,
        bytes: &mut &'a [u8],
    ) -> Result<Self, ParseError> {
        let mut vec = Vec::with_capacity(column_specs.len());
        for col_spec in column_specs {
            vec.push(CqlValue::parse(&col_spec.r#type, bytes)?);
        }
        Ok(vec)
    }
}
