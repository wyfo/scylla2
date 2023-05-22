use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    ops::Deref,
    str,
    sync::Arc,
};

use bytes::Bytes;
use uuid::Uuid;

use crate::{
    cql_type::CqlType,
    error::BoxedError,
    utils::tuples,
    value::{MaybeValue, ReadValue, Udt, WriteValue},
};

pub trait AsValue {
    type Value<'a>: WriteValue
    where
        Self: 'a;
    fn as_value(&self) -> Self::Value<'_>;
}

pub trait FromValue<'a>: Sized {
    type Value: ReadValue<'a>;
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        Self::Value::check_type(cql_type)
    }
    fn from_value(value: Self::Value) -> Result<Self, BoxedError>;
}

impl<T> AsValue for MaybeValue<T>
where
    T: AsValue,
{
    type Value<'a> = MaybeValue<T::Value<'a>> where T: 'a;

    fn as_value(&self) -> <Self as AsValue>::Value<'_> {
        match self {
            Self::Value(v) => MaybeValue::Value(v.as_value()),
            Self::Null => MaybeValue::Null,
            Self::NotSet => MaybeValue::NotSet,
        }
    }
}

impl<T> AsValue for Option<T>
where
    T: AsValue,
{
    type Value<'a> = Option<T::Value<'a>> where T: 'a;

    fn as_value(&self) -> Self::Value<'_> {
        self.as_ref().map(|v| v.as_value())
    }
}

impl<'a, T> AsValue for &'a T
where
    T: ?Sized + AsValue,
{
    type Value<'b> = T::Value<'a> where 'a: 'b;

    fn as_value(&self) -> Self::Value<'_> {
        T::as_value(self)
    }
}

impl AsValue for Box<dyn WriteValue> {
    type Value<'a> = &'a dyn WriteValue where Self: 'a;

    fn as_value(&self) -> Self::Value<'_> {
        self.as_ref()
    }
}

impl AsValue for Arc<dyn WriteValue> {
    type Value<'a> = &'a dyn WriteValue where Self: 'a;

    fn as_value(&self) -> Self::Value<'_> {
        self.as_ref()
    }
}

impl<'a, T> FromValue<'a> for Option<T>
where
    T: FromValue<'a>,
{
    type Value = Option<T::Value>;

    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        T::check_type(cql_type)
    }

    fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
        value.map(T::from_value).transpose()
    }
}

macro_rules! as_from_value {
    ($($tp:ty),*) => {
        $(
        impl<'a> AsValue for $tp {
            type Value<'b> = &'b Self where Self: 'b;

            fn as_value(&self) -> Self::Value<'_> {
                self
            }
        }
        impl<'a> FromValue<'a> for $tp {
            type Value = Self;

            fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
                Ok(value)
            }
        }
        )*
    };
}
as_from_value!(bool, i8, i16, i32, i64, f32, f64, Uuid); // two statements take less lines than one
as_from_value!(&'a [u8], Bytes, &'a str, IpAddr, Ipv4Addr, Ipv6Addr);

macro_rules! value_tuple {
    ($($tp:ident/$_:ident/$idx:tt),*;$len:literal) => {
        impl<$($tp,)*> AsValue for ($($tp,)*)
        where
            $($tp: AsValue,)*
        {
            type Value<'a> = ($($tp::Value<'a>,)*) where $($tp: 'a),*;

            #[allow(clippy::unused_unit)]
            fn as_value(&self) -> Self::Value<'_> {
                ($(self.$idx.as_value(),)*)
            }
        }
        #[allow(unused_variables)]
        impl<'a, $($tp,)*> FromValue<'a> for ($($tp,)*)
        where
            $($tp: FromValue<'a>,)*
        {
            type Value = ($($tp::Value,)*);

            fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
                Ok(($($tp::from_value(value.$idx)?,)*))
            }
        }

        impl<$($tp,)*> AsValue for Udt<($($tp,)*)>
        where
            $($tp: AsValue,)*
        {
            type Value<'a> = Udt<($($tp::Value<'a>,)*)> where $($tp: 'a),*;

            #[allow(clippy::unused_unit)]
            fn as_value(&self) -> Self::Value<'_> {
                self.0.as_value().into()
            }
        }
        #[allow(unused_variables)]
        impl<'a, $($tp,)*> FromValue<'a> for Udt<($($tp,)*)>
        where
            $($tp: FromValue<'a>, $tp::Value: Default,)*
        {
            type Value = Udt<($($tp::Value,)*)>;

            fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
                Ok(<($($tp,)*)>::from_value(value.0)?.into())
            }
        }
    };
}
tuples!(value_tuple);

#[cfg(feature = "string")]
impl AsValue for string::String<Bytes> {
    type Value<'a> = &'a str where Self: 'a;

    fn as_value(&self) -> Self::Value<'_> {
        self
    }
}
#[cfg(feature = "string")]
impl<'a> FromValue<'a> for string::String<Bytes> {
    type Value = Bytes;

    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        <&str as ReadValue>::check_type(cql_type)
    }

    fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
        Ok(string::TryFrom::try_from(value).map_err(crate::utils::invalid_data)?)
    }
}

macro_rules! deref_value {
    ($($tp:ty),*; $slice:ty) => {
        $(
        impl AsValue for $tp {
            type Value<'a> = &'a $slice where Self: 'a;

            fn as_value(&self) -> Self::Value<'_> {
                self.deref()
            }
        }
        impl<'a> FromValue<'a> for $tp {
            type Value = &'a $slice;

            fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
                Ok(value.into())
            }
        }
        )*
    };
}
deref_value!(String, Box<str>, Arc<str>; str);
deref_value!(Vec<u8>, Box<[u8]>, Arc<[u8]>; [u8]);

#[cfg(feature = "chrono")]
impl AsValue for chrono::NaiveDate {
    type Value<'a> = u32 where Self: 'a;

    fn as_value(&self) -> Self::Value<'_> {
        self.signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
            .num_days() as u32
    }
}
#[cfg(feature = "chrono")]
impl<'a> FromValue<'a> for chrono::NaiveDate {
    type Value = u32;

    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        crate::value::check_type!(cql_type, CqlType::Date)
    }

    fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        if value < 2u32.pow(31) {
            epoch.checked_sub_days(chrono::Days::new(2u64.pow(31) - value as u64))
        } else {
            epoch.checked_add_days(chrono::Days::new(2u64.pow(31) + value as u64))
        }
        .ok_or_else(|| BoxedError::from(format!("Date {value} out of bound of chrono::NaiveDate")))
    }
}

#[cfg(feature = "chrono")]
impl<Tz> AsValue for chrono::DateTime<Tz>
where
    Tz: chrono::TimeZone,
{
    type Value<'a> = i64 where Self: 'a;

    fn as_value(&self) -> Self::Value<'_> {
        self.timestamp_millis()
    }
}
#[cfg(feature = "chrono")]
impl<'a> FromValue<'a> for chrono::DateTime<chrono::Utc> {
    type Value = i64;

    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        crate::value::check_type!(cql_type, CqlType::Timestamp)
    }

    fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
        use chrono::TimeZone;
        Ok(chrono::Utc.timestamp_millis_opt(value).unwrap())
    }
}
#[cfg(feature = "chrono")]
impl<'a> FromValue<'a> for chrono::DateTime<chrono::Local> {
    type Value = i64;

    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        crate::value::check_type!(cql_type, CqlType::Timestamp)
    }

    fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
        use chrono::TimeZone;
        Ok(chrono::Local.timestamp_millis_opt(value).unwrap())
    }
}

#[cfg(feature = "chrono")]
impl AsValue for chrono::NaiveTime {
    type Value<'a> = i64 where Self: 'a;

    fn as_value(&self) -> Self::Value<'_> {
        use chrono::Timelike;
        self.num_seconds_from_midnight() as i64 * 1_000_000_000 + self.nanosecond() as i64
    }
}
#[cfg(feature = "chrono")]
impl<'a> FromValue<'a> for chrono::NaiveTime {
    type Value = i64;

    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        crate::value::check_type!(cql_type, CqlType::Time)
    }

    fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
        Ok(chrono::NaiveTime::from_num_seconds_from_midnight_opt(
            (value / 1_000_000_000) as u32,
            (value % 1_000_000_000) as u32,
        )
        .unwrap())
    }
}
