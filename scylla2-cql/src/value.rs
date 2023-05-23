use std::{
    hash::Hash,
    mem,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    ops::{Deref, DerefMut},
    str,
};

use bytes::{BufMut, Bytes};
use uuid::Uuid;

use crate::{
    cql::WriteCql,
    cql_type::CqlType,
    error::{BoxedError, NullError, ParseError, ValueTooBig},
    response::result::rows::RowsSlice,
    utils::{invalid_data, tuples},
};

pub mod convert;
#[cfg(feature = "cql-value")]
mod cql_value;
pub mod iterator;

#[cfg(feature = "cql-value")]
pub use cql_value::CqlValue;

fn check_size(size: usize) -> Result<usize, ValueTooBig> {
    if size <= i32::MAX as usize {
        Ok(size)
    } else {
        Err(ValueTooBig(size))
    }
}

pub trait WriteValue {
    fn value_size(&self) -> Result<usize, ValueTooBig>;
    fn write_value(&self, buf: &mut &mut [u8]);
    fn write_value_with_size(&self, buf: &mut &mut [u8]) {
        (self.value_size().unwrap() as i32).write_cql(buf);
        self.write_value(buf);
    }
}

pub(crate) trait WriteValueExt: WriteValue {
    fn value_size_with_size(&self) -> Result<usize, ValueTooBig> {
        Ok(0i32.cql_size()? + self.value_size()?)
    }
}

impl<T> WriteValueExt for T where T: WriteValue {}

pub trait ReadValue<'a>: Sized {
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError>;
    fn read_value(slice: RowsSlice<'a>) -> Result<Self, ParseError>;
    fn null() -> Result<Self, ParseError> {
        Err(NullError.into())
    }
}

macro_rules! check_type {
    ($cql_tp:ident, $pat:pat) => {
        match $cql_tp {
            $pat => Ok(()),
            tp => Err(format!("Unexpected type {tp}").into()),
        }
    };
}
pub(crate) use check_type;

macro_rules! number_value {
    ($tp:ty, $pat:pat) => {
        impl WriteValue for $tp {
            fn value_size(&self) -> Result<usize, ValueTooBig> {
                Ok(mem::size_of::<$tp>())
            }

            fn write_value(&self, buf: &mut &mut [u8]) {
                buf.put_slice(&self.to_be_bytes());
            }
        }

        impl ReadValue<'_> for $tp {
            fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
                check_type!(cql_type, $pat)
            }

            fn read_value(slice: RowsSlice) -> Result<Self, ParseError> {
                Ok(<$tp>::from_be_bytes(
                    slice.slice().try_into().map_err(invalid_data)?,
                ))
            }
        }
    };
}

number_value!(i64, CqlType::BigInt | CqlType::Time | CqlType::Timestamp);
number_value!(i32, CqlType::Int);
number_value!(i16, CqlType::SmallInt);
number_value!(i8, CqlType::TinyInt);
number_value!(f64, CqlType::Double);
number_value!(f32, CqlType::Float);
number_value!(u32, CqlType::Date);

#[cfg(feature = "num-bigint")]
impl WriteValue for num_bigint::BigInt {
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        check_size(self.to_signed_bytes_be().len())
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        let bytes = self.to_signed_bytes_be();
        buf.put_slice(&bytes);
    }

    fn write_value_with_size(&self, buf: &mut &mut [u8]) {
        let bytes = self.to_signed_bytes_be();
        (bytes.len() as i32).write_cql(buf);
        buf.put_slice(&bytes);
    }
}
#[cfg(feature = "num-bigint")]
impl<'a> ReadValue<'a> for num_bigint::BigInt {
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        check_type!(cql_type, CqlType::BigInt)
    }

    fn read_value(slice: RowsSlice) -> Result<Self, ParseError> {
        Ok(num_bigint::BigInt::from_signed_bytes_be(&slice))
    }
}

#[cfg(feature = "bigdecimal")]
impl WriteValue for bigdecimal::BigDecimal {
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        let (bigint, scale) = self.as_bigint_and_exponent();
        let scale_size = i32::try_from(scale)
            .map_err(|_| ValueTooBig(0))?
            .cql_size()?;
        Ok(scale_size + bigint.value_size()?)
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        let (bigint, scale) = self.as_bigint_and_exponent();
        let bytes = bigint.to_signed_bytes_be();
        (scale as i32).write_cql(buf);
        buf.put_slice(&bytes);
    }

    fn write_value_with_size(&self, buf: &mut &mut [u8]) {
        let (bigint, scale) = self.as_bigint_and_exponent();
        let bytes = bigint.to_signed_bytes_be();
        (4 + bytes.len() as i32).write_cql(buf);
        (scale as i32).write_cql(buf);
        buf.put_slice(&bytes);
    }
}

#[cfg(feature = "bigdecimal")]
impl<'a> ReadValue<'a> for bigdecimal::BigDecimal {
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        check_type!(cql_type, CqlType::Decimal)
    }

    fn read_value(mut slice: RowsSlice) -> Result<Self, ParseError> {
        use crate::cql::ReadCql;
        let scale = i32::read_cql(&mut slice)?;
        Ok(bigdecimal::BigDecimal::new(
            num_bigint::BigInt::from_signed_bytes_be(&slice),
            scale as i64,
        ))
    }
}

// TODO varint with zigzag

impl WriteValue for &[u8] {
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        Ok(self.len())
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        buf.put_slice(self);
    }
}
impl<'a> ReadValue<'a> for &'a [u8] {
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        check_type!(cql_type, CqlType::Ascii | CqlType::Blob | CqlType::Text)
    }

    fn read_value(slice: RowsSlice<'a>) -> Result<Self, ParseError> {
        Ok(&slice)
    }
}

impl WriteValue for Bytes {
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        check_size(self.len())
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        buf.put_slice(self);
    }
}
impl<'a> ReadValue<'a> for Bytes {
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        check_type!(cql_type, CqlType::Ascii | CqlType::Blob | CqlType::Text)
    }

    fn read_value(slice: RowsSlice) -> Result<Self, ParseError> {
        Ok(slice.bytes())
    }
}

impl WriteValue for bool {
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        (*self as i8).value_size()
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        (*self as i8).write_value(buf);
    }
}
impl ReadValue<'_> for bool {
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        check_type!(cql_type, CqlType::Boolean)
    }

    fn read_value(slice: RowsSlice) -> Result<Self, ParseError> {
        Ok(i8::read_value(slice)? != 0)
    }
}

impl WriteValue for Ipv4Addr {
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        Ok(mem::size_of::<Self>())
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        self.octets().write_cql(buf);
    }
}
impl ReadValue<'_> for Ipv4Addr {
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        check_type!(cql_type, CqlType::Inet)
    }

    fn read_value(slice: RowsSlice) -> Result<Self, ParseError> {
        Ok(Self::from(
            <[u8; 4]>::try_from(slice.slice()).map_err(invalid_data)?,
        ))
    }
}

impl WriteValue for Ipv6Addr {
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        Ok(mem::size_of::<Self>())
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        self.octets().write_cql(buf);
    }
}
impl ReadValue<'_> for Ipv6Addr {
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        check_type!(cql_type, CqlType::Inet)
    }

    fn read_value(slice: RowsSlice) -> Result<Self, ParseError> {
        Ok(Self::from(
            <[u8; 16]>::try_from(slice.slice()).map_err(invalid_data)?,
        ))
    }
}

impl WriteValue for IpAddr {
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        match self {
            Self::V4(ip) => ip.value_size(),
            Self::V6(ip) => ip.value_size(),
        }
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        match self {
            Self::V4(ip) => ip.write_value(buf),
            Self::V6(ip) => ip.write_value(buf),
        }
    }
}
impl ReadValue<'_> for IpAddr {
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        check_type!(cql_type, CqlType::Inet)
    }

    fn read_value(slice: RowsSlice) -> Result<Self, ParseError> {
        Ok(match slice.len() {
            4 => Ipv4Addr::read_value(slice)?.into(),
            16 => Ipv6Addr::read_value(slice)?.into(),
            _ => Err(invalid_data("Invalid IP length"))?,
        })
    }
}

impl WriteValue for &str {
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        check_size(self.len())
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        buf.put_slice(self.as_bytes());
    }
}
impl<'a> ReadValue<'a> for &'a str {
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        check_type!(cql_type, CqlType::Ascii | CqlType::Text)
    }

    fn read_value(slice: RowsSlice<'a>) -> Result<Self, ParseError> {
        Ok(str::from_utf8(&slice).map_err(invalid_data)?)
    }
}

impl WriteValue for Uuid {
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        self.cql_size()
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        self.write_cql(buf);
    }
}
impl ReadValue<'_> for Uuid {
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        check_type!(cql_type, CqlType::Uuid | CqlType::Timeuuid)
    }

    fn read_value(slice: RowsSlice) -> Result<Self, ParseError> {
        Ok(Uuid::from_bytes(
            slice.slice().try_into().map_err(invalid_data)?,
        ))
    }
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Udt<T>(pub T);

impl<T> From<T> for Udt<T> {
    fn from(value: T) -> Self {
        Udt(value)
    }
}

impl<T> Deref for Udt<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Udt<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

macro_rules! value_tuple {
    ($($tp:ident/$_:ident/$idx:tt),*;$len:literal) => {
        impl<$($tp,)*> WriteValue for ($($tp,)*)
        where
            $($tp: WriteValue,)*
        {
            fn value_size(&self) -> Result<usize, ValueTooBig> {
                check_size(0 $(+ self.$idx.value_size_with_size()?)*)
            }

            #[allow(unused_assignments, unused_mut, unused_variables)]
            fn write_value(&self, buf: &mut &mut [u8]) {
                $(self.$idx.write_value_with_size(buf);)*
            }

            #[allow(unused_assignments, unused_mut, unused_variables)]
            fn write_value_with_size(&self, buf: &mut &mut [u8]) {
                // Lifetime dance taken from `impl Write for &mut [u8]`.
                let (len_slice, buf_slice) = mem::take(buf).split_at_mut(4);
                *buf = buf_slice;
                let buf_len = buf.len();
                self.write_value(buf);
                len_slice.copy_from_slice(&((buf_len - buf.len()) as u32).to_be_bytes());
            }
        }
        impl<'a, $($tp,)*> ReadValue<'a> for ($($tp,)*)
        where
            $($tp: ReadValue<'a>,)*
        {
            fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
                match cql_type {
                    CqlType::Tuple(types) => {
                        if types.len() != $len {
                            return Err(format!("Unexpected tuple length {}", types.len()).into())
                        }
                        $($tp::check_type(&types[$idx])?;)*
                    }
                    tp => return Err(format!("Unexpected type {tp}").into()),
                }
                Ok(())
            }

            #[allow(unused_assignments, unused_mut, unused_variables)]
            fn read_value(mut slice: RowsSlice<'a>) -> Result<Self, ParseError> {
                Ok(($(slice.parse_value::<$tp>()?,)*))
            }
        }

        impl<$($tp,)*> WriteValue for Udt<($($tp,)*)>
        where
            $($tp: WriteValue,)*
        {
            fn value_size(&self) -> Result<usize, ValueTooBig> {
                self.0.value_size()
            }

            fn write_value(&self, buf: &mut &mut [u8]) {
                self.0.write_value(buf);
            }

            fn write_value_with_size(&self, buf: &mut &mut [u8]) {
                self.0.write_value_with_size(buf)
            }
        }

        impl<'a, $($tp,)*> ReadValue<'a> for Udt<($($tp,)*)>
        where
            $($tp: ReadValue<'a>,)*
        {
            #[allow(unused_assignments, unused_mut, unused_variables)]
            fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
                match cql_type {
                    CqlType::Udt{fields, ..} => {
                        $(if $idx < fields.len() {
                            $tp::check_type(&fields[$idx].1)?;
                        } else {
                            return Ok(());
                        })*
                    }
                    tp => return Err(format!("Unexpected type {tp}").into()),
                }
                Ok(())
            }

            #[allow(unused_assignments, unused_mut, unused_variables)]
            fn read_value(mut slice: RowsSlice<'a>) -> Result<Self, ParseError> {
                Ok((
                    $(if !slice.is_empty() {
                        slice.parse_value()?
                    } else {
                        $tp::null()?
                    },)*
                ).into())
            }
        }
    };
}
tuples!(value_tuple);

impl<T> WriteValue for &T
where
    T: ?Sized + WriteValue,
{
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        T::value_size(self)
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        T::write_value(self, buf);
    }

    fn write_value_with_size(&self, buf: &mut &mut [u8]) {
        T::write_value_with_size(self, buf);
    }
}

impl<T> WriteValue for Option<T>
where
    T: WriteValue,
{
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        self.as_ref().map_or(Ok(0), WriteValue::value_size)
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        if let Some(v) = self {
            v.write_value(buf);
        }
    }

    fn write_value_with_size(&self, buf: &mut &mut [u8]) {
        match self {
            Some(v) => v.write_value_with_size(buf),
            None => (-1i32).write_cql(buf),
        }
    }
}

impl<'a, T> ReadValue<'a> for Option<T>
where
    T: ReadValue<'a>,
{
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        T::check_type(cql_type)
    }

    fn read_value(slice: RowsSlice<'a>) -> Result<Self, ParseError> {
        Ok(Some(T::read_value(slice)?))
    }

    fn null() -> Result<Self, ParseError> {
        Ok(None)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, PartialOrd, Ord, Hash)]
pub enum MaybeValue<T> {
    Value(T),
    Null,
    #[default]
    NotSet,
}

impl<T> MaybeValue<T> {
    pub fn value(self) -> Option<T> {
        match self {
            Self::Value(v) => Some(v),
            Self::Null | MaybeValue::NotSet => None,
        }
    }

    pub fn from_not_set(opt: Option<T>) -> Self {
        match opt {
            Some(v) => Self::Value(v),
            None => Self::NotSet,
        }
    }

    pub fn as_ref(&self) -> MaybeValue<&T> {
        match self {
            Self::Value(v) => MaybeValue::Value(v),
            Self::Null => MaybeValue::Null,
            Self::NotSet => MaybeValue::NotSet,
        }
    }
}

impl<T> WriteValue for MaybeValue<T>
where
    T: WriteValue,
{
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        self.as_ref().value().map_or(Ok(0), WriteValue::value_size)
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        if let Self::Value(v) = self {
            v.write_value(buf);
        }
    }

    fn write_value_with_size(&self, buf: &mut &mut [u8]) {
        match self {
            MaybeValue::Value(v) => v.write_value_with_size(buf),
            MaybeValue::Null => (-1i32).write_cql(buf),
            MaybeValue::NotSet => (-2i32).write_cql(buf),
        }
    }
}

impl<T> From<Option<T>> for MaybeValue<T> {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => Self::Value(v),
            None => Self::Null,
        }
    }
}

impl<T> From<T> for MaybeValue<T> {
    fn from(v: T) -> Self {
        Self::Value(v)
    }
}

impl<T> From<MaybeValue<T>> for Option<T> {
    fn from(val: MaybeValue<T>) -> Self {
        val.value()
    }
}
