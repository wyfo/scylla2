use scylla2_cql::request::query::values::NamedQueryValues;

use crate::utils::tuples;

pub trait IntoValues {
    type Values;
    fn into_values(self) -> Self::Values;
}

pub trait TransparentValues {}

impl<V> TransparentValues for &V where V: TransparentValues {}

impl<V> IntoValues for V
where
    V: TransparentValues,
{
    type Values = Self;

    fn into_values(self) -> Self::Values {
        self
    }
}

impl<V> TransparentValues for &[V] {}

impl<V> TransparentValues for NamedQueryValues<V> {}

impl TransparentValues for () {}

macro_rules! into_tuple {
    ($($stmt:ident/$values:ident/$idx:tt),*; $len:literal) => {
        impl<V0, $($values, )*> TransparentValues for (V0, $($values, )*) {}
    };
}

tuples!(into_tuple);
