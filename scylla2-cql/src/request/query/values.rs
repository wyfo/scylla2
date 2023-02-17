use std::ops::Deref;

use crate::{
    cql::WriteCql,
    error::ValueTooBig,
    utils::tuples,
    value::{convert::AsValue, WriteValue, WriteValueExt},
};

pub trait QueryValues {
    fn count(&self) -> u16;
    fn named(&self) -> bool;
    fn values_size(&self) -> Result<usize, ValueTooBig>;
    fn write_values(&self, buf: &mut &mut [u8]);
}

impl<T> QueryValues for &T
where
    T: QueryValues,
{
    fn count(&self) -> u16 {
        T::count(self)
    }

    fn named(&self) -> bool {
        T::named(self)
    }

    fn values_size(&self) -> Result<usize, ValueTooBig> {
        T::values_size(self)
    }

    fn write_values(&self, buf: &mut &mut [u8]) {
        T::write_values(self, buf)
    }
}

impl<T> QueryValues for &[T]
where
    T: AsValue,
{
    fn count(&self) -> u16 {
        self.len() as u16
    }

    fn named(&self) -> bool {
        false
    }

    fn values_size(&self) -> Result<usize, ValueTooBig> {
        self.iter()
            .map(|v| v.as_value().value_size_with_size())
            .sum()
    }

    fn write_values(&self, buf: &mut &mut [u8]) {
        for v in *self {
            v.as_value().write_value_with_size(buf);
        }
    }
}

// Wrapper to avoid blanket implementation
#[derive(Debug)]
pub struct NamedQueryValues<V>(V);

impl<V> Deref for NamedQueryValues<V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<V, K, T> QueryValues for NamedQueryValues<V>
where
    V: IntoIterator<Item = (K, T)> + Clone,
    V::IntoIter: ExactSizeIterator,
    K: AsRef<str>,
    T: AsValue,
{
    fn count(&self) -> u16 {
        self.0.clone().into_iter().len() as u16
    }

    fn named(&self) -> bool {
        true
    }

    fn values_size(&self) -> Result<usize, ValueTooBig> {
        self.0
            .clone()
            .into_iter()
            .map(|(k, v)| Ok(k.as_ref().cql_size()? + v.as_value().value_size_with_size()?))
            .sum()
    }

    fn write_values(&self, buf: &mut &mut [u8]) {
        for (k, v) in self.0.clone() {
            k.as_ref().write_cql(buf);
            v.as_value().write_value_with_size(buf);
        }
    }
}

macro_rules! query_values_tuple {
    ($($tp:ident/$_:ident/$idx:tt),*;$len:literal) => {
        impl<$($tp,)*> QueryValues for ($($tp,)*)
        where
            $($tp: AsValue,)*
        {
            fn count(&self) -> u16 {
                $len
            }

            fn named(&self) -> bool {
                false
            }

            fn values_size(&self) -> Result<usize, ValueTooBig> {
                self.as_value().value_size()
            }

            fn write_values(&self, buf: &mut &mut [u8]) {
                self.as_value().write_value(buf);
            }
        }
    };
}
tuples!(query_values_tuple);
