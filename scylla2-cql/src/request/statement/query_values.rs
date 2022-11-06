use std::collections::HashMap;

use crate::{
    cql::CqlWrite,
    error::{BoxedError, ValueTooBig},
    request::statement::batch::NotBatch,
    utils::{tuples, tuples1},
    value::{convert::AsValue, ValueWrite, ValueWriteExt},
};

pub trait QueryValues: Sized {
    fn get(&self, index: u16) -> Option<&dyn ValueWrite>;
    fn count(&self) -> u16;
    fn named(&self) -> bool;
    fn serialized_size(&self) -> Result<usize, ValueTooBig>;
    fn serialize(&self, buf: &mut &mut [u8]);
}

pub struct NamedValue<K, T>(pub K, pub T);

impl<T> QueryValues for &T
where
    T: QueryValues,
{
    fn get(&self, index: u16) -> Option<&dyn ValueWrite> {
        T::get(self, index)
    }

    fn count(&self) -> u16 {
        T::count(self)
    }

    fn named(&self) -> bool {
        T::named(self)
    }

    fn serialized_size(&self) -> Result<usize, ValueTooBig> {
        T::serialized_size(self)
    }

    fn serialize(&self, buf: &mut &mut [u8]) {
        T::serialize(self, buf)
    }
}

impl<T> QueryValues for &[T]
where
    T: ValueWrite,
{
    fn get(&self, index: u16) -> Option<&dyn ValueWrite> {
        Some(<[_]>::get(self, index as usize)? as &dyn ValueWrite)
    }

    fn count(&self) -> u16 {
        self.len() as u16
    }

    fn named(&self) -> bool {
        false
    }

    fn serialized_size(&self) -> Result<usize, ValueTooBig> {
        self.iter().map(|v| v.size_plus_serialized_size()).sum()
    }

    fn serialize(&self, buf: &mut &mut [u8]) {
        self.iter().for_each(|v| v.serialize_with_size(buf));
    }
}

impl<K, T> QueryValues for &[NamedValue<K, T>]
where
    K: AsRef<str>,
    T: ValueWrite,
{
    fn get(&self, index: u16) -> Option<&dyn ValueWrite> {
        Some(&<[_]>::get(self, index as usize)?.1 as &dyn ValueWrite)
    }

    fn count(&self) -> u16 {
        self.len() as u16
    }

    fn named(&self) -> bool {
        true
    }

    fn serialized_size(&self) -> Result<usize, ValueTooBig> {
        self.iter()
            .map(|NamedValue(k, v)| Ok(k.as_ref().cql_size()? + v.size_plus_serialized_size()?))
            .sum()
    }

    fn serialize(&self, buf: &mut &mut [u8]) {
        for NamedValue(k, v) in self.iter() {
            k.as_ref().write_cql(buf);
            v.serialize_with_size(buf);
        }
    }
}

impl<K, T> QueryValues for HashMap<K, T>
where
    K: AsRef<str>,
    T: ValueWrite,
{
    fn get(&self, _index: u16) -> Option<&dyn ValueWrite> {
        None
    }

    fn count(&self) -> u16 {
        self.len() as u16
    }

    fn named(&self) -> bool {
        true
    }

    fn serialized_size(&self) -> Result<usize, ValueTooBig> {
        self.iter()
            .map(|(k, v)| Ok(k.as_ref().cql_size()? + v.size_plus_serialized_size()?))
            .sum()
    }

    fn serialize(&self, buf: &mut &mut [u8]) {
        for (k, v) in self.iter() {
            k.as_ref().write_cql(buf);
            v.serialize_with_size(buf);
        }
    }
}

// impl<'a, T, S> AsValues<'a, S> for &[T]
// where
//     T: ValueWrite + 'a,
//     S: NotBatch,
// {
//     type Values = &'a [T];
//     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
//         Ok(self)
//     }
// }
//
// impl<'a, T, S> AsValues<'a, S> for Vec<T>
// where
//     T: ValueWrite + 'a,
//     S: NotBatch,
// {
//     type Values = &'a [T];
//     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
//         Ok(self)
//     }
// }
//
// impl<'a, K, T, S> AsValues<'a, S> for &[NamedValue<K, T>]
// where
//     K: AsRef<str> + 'a,
//     T: ValueWrite + 'a,
//     S: NotBatch,
// {
//     type Values = &'a [NamedValue<K, T>];
//     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
//         Ok(self)
//     }
// }
//
// impl<'a, K, T, S> AsValues<'a, S> for Vec<NamedValue<K, T>>
// where
//     K: AsRef<str> + 'a,
//     T: ValueWrite + 'a,
//     S: NotBatch,
// {
//     type Values = &'a [NamedValue<K, T>];
//     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
//         Ok(self)
//     }
// }
//
// impl<'a, K, T, S> AsValues<'a, S> for HashMap<K, T>
// where
//     K: AsRef<str> + 'a,
//     T: ValueWrite + 'a,
//     S: NotBatch,
// {
//     type Values = &'a HashMap<K, T>;
//     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
//         Ok(self)
//     }
// }

macro_rules! query_values_tuple {
    ($($tp:ident/$_:ident/$idx:tt),*;$len:literal) => {
        impl<$($tp,)*> QueryValues for ($($tp,)*)
        where
            $($tp: ValueWrite,)*
        {
            fn get(&self, index: u16) -> Option<&dyn ValueWrite> {
                match index {
                    $($idx => Some(&self.$idx as &dyn ValueWrite),)*
                    _ => None
                }
            }

            fn count(&self) -> u16 {
                $len
            }

            fn named(&self) -> bool {
                false
            }

            fn serialized_size(&self) -> Result<usize, ValueTooBig> {
                ValueWrite::serialized_size(self)
            }

            fn serialize(&self, buf: &mut &mut [u8]) {
                ValueWrite::serialize(self, buf);
            }
        }

        // impl<'a, S, $($tp,)*> AsValues<'a, S> for ($($tp,)*)
        // where
        //     $($tp: AsValue<'a>,)*
        //     S: NotBatch,
        // {
        //     type Values = ($($tp::Value,)*);
        //
        //     #[allow(clippy::unused_unit)]
        //     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
        //         Ok(($(self.$idx.as_value()?,)*))
        //     }
        // }

    };
}
tuples!(query_values_tuple);

macro_rules! query_values_named_tuple {
    ($($tp:ident/$_:ident/$idx:tt),*;$len:literal) => {
        impl<'s, $($tp,)*> QueryValues for ($(NamedValue<&'s str, $tp>,)*)
        where
            $($tp: ValueWrite,)*
        {
            fn get(&self, index: u16) -> Option<&dyn ValueWrite> {
                match index {
                    $($idx => Some(&self.$idx.1 as &dyn ValueWrite),)*
                    _ => None
                }
            }

            fn count(&self) -> u16 {
                $len
            }

            fn named(&self) -> bool {
                true
            }

            fn serialized_size(&self) -> Result<usize, ValueTooBig> {
                Ok(0 $(+ self.$idx.0.cql_size()? + self.$idx.1.size_plus_serialized_size()?)*)
            }

            #[allow(unused_assignments)]
            fn serialize(&self, buf: &mut &mut [u8]) {
                $(
                    self.$idx.0.write_cql(buf);
                    self.$idx.1.serialize_with_size(buf);
                )*
            }
        }

        // impl<'a, K, S, $($tp,)*> AsValues<'a, S> for ($(NamedValue<K,$tp>,)*)
        // where
        //     K: AsRef<str>,
        //     $($tp: AsValue<'a>,)*
        //     S: NotBatch,
        // {
        //     type Values = ($(NamedValue<&'a str, $tp::Value>,)*);
        //
        //     #[allow(clippy::unused_unit)]
        //     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
        //         Ok(($(NamedValue(self.$idx.0.as_ref(), self.$idx.1.as_value()?),)*))
        //     }
        // }
    };
}
tuples1!(query_values_named_tuple);
