use std::{
    collections::{
        btree_map::Iter as BTreeMapIter, btree_set::Iter as BTreeSetIter,
        hash_map::Iter as HashMapIter, hash_set::Iter as HashSetIter,
        vec_deque::Iter as VecDequeIter, BTreeMap, BTreeSet, HashMap, HashSet, VecDeque,
    },
    hash::Hash,
    iter::FusedIterator,
    marker::PhantomData,
    slice::Iter as SliceIter,
};

use crate::{
    cql::{ReadCql, WriteCql},
    cql_type::CqlType,
    error::{BoxedError, ParseError, ValueTooBig},
    response::result::rows::RowsSlice,
    utils::invalid_data,
    value::{
        convert::{AsValue, FromValue},
        ReadValue, WriteValue, WriteValueExt,
    },
};

#[derive(Debug)]
pub struct WriteValueIter<I>(pub I);

impl<I> WriteValue for WriteValueIter<I>
where
    I: Iterator + ExactSizeIterator + Clone,
    I::Item: WriteValue,
{
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        let size = (self.0.len() as i32).cql_size()?;
        self.0
            .clone()
            .try_fold(size, |size, elt| Ok(size + elt.value_size_with_size()?))
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        (self.0.len() as i32).write_cql(buf);
        self.0
            .clone()
            .for_each(|elt| elt.write_value_with_size(buf));
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ReadValueIter<'a, T> {
    slice: RowsSlice<'a>,
    remain: usize,
    _phantom: PhantomData<T>,
}

impl<'a, T> ReadValue<'a> for ReadValueIter<'a, T>
where
    T: ReadValue<'a>,
{
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        match cql_type {
            CqlType::List(inner) => T::check_type(inner),
            CqlType::Set(inner) => T::check_type(inner),
            tp => Err(format!("Unexpected type {tp}").into()),
        }
    }

    fn read_value(mut slice: RowsSlice<'a>) -> Result<Self, ParseError> {
        let remain = i32::read_cql(&mut slice)?
            .try_into()
            .map_err(invalid_data)?;
        Ok(ReadValueIter {
            slice,
            remain,
            _phantom: PhantomData,
        })
    }
}

impl<'a, T> Iterator for ReadValueIter<'a, T>
where
    T: ReadValue<'a>,
{
    type Item = Result<T, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remain == 0 {
            return None;
        }
        self.remain -= 1;
        Some(self.slice.parse_value())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remain, Some(self.remain))
    }
}

impl<'a, T> ExactSizeIterator for ReadValueIter<'a, T> where T: ReadValue<'a> {}

impl<'a, T> FusedIterator for ReadValueIter<'a, T> where T: ReadValue<'a> {}

#[derive(Debug)]
pub struct WriteKeyValueIter<I>(pub I);

impl<I, K, V> WriteValue for WriteKeyValueIter<I>
where
    I: Iterator<Item = (K, V)> + ExactSizeIterator + Clone,
    K: WriteValue,
    V: WriteValue,
{
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        let size = (self.0.len() as i32).cql_size()?;
        self.0.clone().try_fold(size, |size, (k, v)| {
            Ok(size + k.value_size_with_size()? + v.value_size_with_size()?)
        })
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        (self.0.len() as i32).write_cql(buf);
        self.0.clone().for_each(|(k, v)| {
            k.write_value_with_size(buf);
            v.write_value_with_size(buf);
        });
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ReadKeyValueIter<'a, K, V> {
    slice: RowsSlice<'a>,
    remain: usize,
    _phantom: PhantomData<(K, V)>,
}

impl<'a, K, V> ReadValue<'a> for ReadKeyValueIter<'a, K, V>
where
    K: ReadValue<'a>,
    V: ReadValue<'a>,
{
    fn check_type(cql_type: &CqlType) -> Result<(), BoxedError> {
        match cql_type {
            CqlType::Map(key, value) => {
                K::check_type(key)?;
                V::check_type(value)?;
                Ok(())
            }
            tp => Err(format!("Unexpected type {tp}").into()),
        }
    }

    fn read_value(mut slice: RowsSlice<'a>) -> Result<Self, ParseError> {
        let remain = i32::read_cql(&mut slice)?
            .try_into()
            .map_err(invalid_data)?;
        Ok(ReadKeyValueIter {
            slice,
            remain,
            _phantom: PhantomData,
        })
    }
}

impl<'a, K, V> Iterator for ReadKeyValueIter<'a, K, V>
where
    K: ReadValue<'a>,
    V: ReadValue<'a>,
{
    type Item = Result<(K, V), ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remain == 0 {
            return None;
        }
        self.remain -= 1;
        let mut parse_kv = || Ok((self.slice.parse_value()?, self.slice.parse_value()?));
        Some(parse_kv())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remain, Some(self.remain))
    }
}

impl<'a, K, V> ExactSizeIterator for ReadKeyValueIter<'a, K, V>
where
    K: ReadValue<'a>,
    V: ReadValue<'a>,
{
}

impl<'a, K, V> FusedIterator for ReadKeyValueIter<'a, K, V>
where
    K: ReadValue<'a>,
    V: ReadValue<'a>,
{
}

#[derive(Debug)]
pub struct AsValueIter<I>(pub I);

impl<'b, I, T> AsValue for AsValueIter<I>
where
    I: Iterator<Item = &'b T> + ExactSizeIterator + Clone,
    T: AsValue + 'b,
{
    type Value<'a> =
    WriteValueIter<std::iter::Map<I, fn(I::Item) -> T::Value<'b>>> where I: 'a;

    fn as_value(&self) -> Self::Value<'_> {
        WriteValueIter(self.0.clone().map(T::as_value))
    }
}

#[derive(Debug)]
pub struct FromValueIter<'a, T>(ReadValueIter<'a, T::Value>)
where
    T: FromValue<'a>;

impl<'a, T> FromValue<'a> for FromValueIter<'a, T>
where
    T: FromValue<'a>,
{
    type Value = ReadValueIter<'a, T::Value>;

    fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
        Ok(FromValueIter(value))
    }
}

impl<'a, T> Iterator for FromValueIter<'a, T>
where
    T: FromValue<'a>,
{
    type Item = Result<T, BoxedError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.0.next()?.map_err(Into::into).and_then(T::from_value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a, T> ExactSizeIterator for FromValueIter<'a, T> where T: FromValue<'a> {}

impl<'a, T> FusedIterator for FromValueIter<'a, T> where T: FromValue<'a> {}

#[derive(Debug)]
pub struct AsKeyValueIter<I>(pub I);

impl<'b, I, K, V> AsValue for AsKeyValueIter<I>
where
    I: Iterator<Item = (&'b K, &'b V)> + ExactSizeIterator + Clone,
    K: AsValue + 'b,
    V: AsValue + 'b,
{
    type Value<'a> =
    WriteValueIter<std::iter::Map<I, fn(I::Item) -> (K::Value<'b>, V::Value<'b>)>> where I: 'a;

    fn as_value(&self) -> Self::Value<'_> {
        WriteValueIter(self.0.clone().map(|(k, v)| (k.as_value(), v.as_value())))
    }
}

#[derive(Debug)]
pub struct FromKeyValueIter<'a, K, V>(ReadKeyValueIter<'a, K::Value, V::Value>)
where
    K: FromValue<'a>,
    V: FromValue<'a>;

impl<'a, K, V> FromValue<'a> for FromKeyValueIter<'a, K, V>
where
    K: FromValue<'a>,
    V: FromValue<'a>,
{
    type Value = ReadKeyValueIter<'a, K::Value, V::Value>;

    fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
        Ok(FromKeyValueIter(value))
    }
}

impl<'a, K, V> Iterator for FromKeyValueIter<'a, K, V>
where
    K: FromValue<'a>,
    V: FromValue<'a>,
{
    type Item = Result<(K, V), BoxedError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(
            self.0
                .next()?
                .map_err(Into::into)
                .and_then(|(k, v)| Ok((K::from_value(k)?, V::from_value(v)?))),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a, K, V> ExactSizeIterator for FromKeyValueIter<'a, K, V>
where
    K: FromValue<'a>,
    V: FromValue<'a>,
{
}

impl<'a, K, V> FusedIterator for FromKeyValueIter<'a, K, V>
where
    K: FromValue<'a>,
    V: FromValue<'a>,
{
}

impl<T> AsValue for [T]
where
    T: AsValue,
{
    type Value<'a> = <AsValueIter<SliceIter<'a, T>> as AsValue>::Value<'a> where T: 'a;

    fn as_value(&self) -> Self::Value<'_> {
        AsValueIter(self.iter()).as_value()
    }
}

macro_rules! value_iter {
    ($tp:tt, $iter:ident, $($bound:ident),*) => {
        impl<T> AsValue for $tp<T>
        where
            T: AsValue $(+ $bound)*,
        {
            type Value<'a> = <AsValueIter<$iter<'a, T>> as AsValue>::Value<'a> where T: 'a;

            fn as_value(&self) -> Self::Value<'_> {
                AsValueIter(self.iter()).as_value()
            }
        }

        impl<'a, T> FromValue<'a> for $tp<T>
        where
            T: FromValue<'a> $(+ $bound)*,
        {
            type Value = <FromValueIter<'a, T> as FromValue<'a>>::Value;

            fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
                FromValueIter::<'a, T>::from_value(value)?.collect()
            }
        }
    };
}
value_iter!(Vec, SliceIter,);
value_iter!(VecDeque, VecDequeIter,);
value_iter!(HashSet, HashSetIter, Eq, Hash);
value_iter!(BTreeSet, BTreeSetIter, Ord);

macro_rules! key_value_iter {
    ($tp:tt, $iter:ident, $($bound:ident),*) => {
        impl<K, V> AsValue for $tp<K, V>
        where
            K: AsValue $(+ $bound)*,
            V: AsValue,
        {
            type Value<'a> = <AsKeyValueIter<$iter<'a, K, V>> as AsValue>::Value<'a> where K: 'a, V: 'a;

            fn as_value(&self) -> Self::Value<'_> {
                AsKeyValueIter(self.iter()).as_value()
            }
        }

        impl<'a, K, V> FromValue<'a> for $tp<K, V>
        where
            K: FromValue<'a> $(+ $bound)*,
            V: FromValue<'a>,
        {
            type Value = <FromKeyValueIter<'a, K, V> as FromValue<'a>>::Value;

            fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
                FromKeyValueIter::<'a, K, V>::from_value(value)?.collect()
            }
        }
    };
}
key_value_iter!(HashMap, HashMapIter, Eq, Hash);
key_value_iter!(BTreeMap, BTreeMapIter, Ord);
