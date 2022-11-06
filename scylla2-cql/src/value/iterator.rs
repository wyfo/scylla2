use std::{
    collections::{
        btree_map::Iter as BTreeMapIter, btree_set::Iter as BTreeSetIter,
        hash_map::Iter as HashMapIter, hash_set::Iter as HashSetIter,
        vec_deque::Iter as VecDequeIter, BTreeMap, BTreeSet, HashMap, HashSet, VecDeque,
    },
    hash::Hash,
    marker::PhantomData,
    slice::Iter as SliceIter,
};

use bytes::Bytes;

use crate::{
    cql::{ReadCql, WriteCql},
    cql_type::CqlType,
    error::{BoxedError, ParseError, TypeError, ValueTooBig},
    utils::invalid_data,
    value::{
        convert::{AsValue, FromValue},
        ReadValue, WriteValue, WriteValueExt,
    },
};

pub trait CollectionValueWrite {
    fn col_serialized_size(&self) -> Result<usize, ValueTooBig>;
    fn serialize_col(&self, buf: &mut &mut [u8]);
}

pub trait CollectionValueRead<'a> {
    type Unpacked;
    fn check_type(cql_type: &CqlType) -> Result<(), TypeError>;
    fn read_col_value(
        buf: &mut &'a [u8],
        envelope: &'a Bytes,
    ) -> Result<Self::Unpacked, ParseError>;
}

impl<T> CollectionValueWrite for (T,)
where
    T: WriteValue,
{
    fn col_serialized_size(&self) -> Result<usize, ValueTooBig> {
        self.0.value_size_with_size()
    }

    fn serialize_col(&self, buf: &mut &mut [u8]) {
        self.0.write_value_with_size(buf);
    }
}

impl<'a, T> CollectionValueRead<'a> for (T,)
where
    T: ReadValue<'a>,
{
    type Unpacked = T;

    fn check_type(cql_type: &CqlType) -> Result<(), TypeError> {
        match cql_type {
            CqlType::List(inner) => T::check_type(inner),
            CqlType::Set(inner) => T::check_type(inner),
            _ => Err(TypeError),
        }
    }

    fn read_col_value(
        buf: &mut &'a [u8],
        envelope: &'a Bytes,
    ) -> Result<Self::Unpacked, ParseError> {
        T::read_value(<&[u8]>::read_cql(buf)?, envelope)
    }
}

impl<K, V> CollectionValueWrite for (K, V)
where
    K: WriteValue,
    V: WriteValue,
{
    fn col_serialized_size(&self) -> Result<usize, ValueTooBig> {
        Ok(self.0.value_size_with_size()? + self.1.value_size_with_size()?)
    }

    fn serialize_col(&self, buf: &mut &mut [u8]) {
        self.0.write_value_with_size(buf);
        self.1.write_value_with_size(buf);
    }
}

impl<'a, K, V> CollectionValueRead<'a> for (K, V)
where
    K: ReadValue<'a>,
    V: ReadValue<'a>,
{
    type Unpacked = (K, V);

    fn check_type(cql_type: &CqlType) -> Result<(), TypeError> {
        match cql_type {
            CqlType::Map(key, value) => {
                K::check_type(key)?;
                V::check_type(value)?;
                Ok(())
            }
            _ => Err(TypeError),
        }
    }

    fn read_col_value(
        buf: &mut &'a [u8],
        envelope: &'a Bytes,
    ) -> Result<Self::Unpacked, ParseError> {
        Ok((
            K::read_value(<&[u8]>::read_cql(buf)?, envelope)?,
            V::read_value(<&[u8]>::read_cql(buf)?, envelope)?,
        ))
    }
}

#[derive(Debug)]
pub struct ValueWriteIter<I> {
    pub iter: I,
    pub size: usize,
}

impl<I> WriteValue for ValueWriteIter<I>
where
    I: Iterator + Clone,
    I::Item: CollectionValueWrite,
{
    fn value_size(&self) -> Result<usize, ValueTooBig> {
        let size = (self.size as i32).cql_size()?;
        self.iter
            .clone()
            .try_fold(size, |size, value| Ok(size + value.col_serialized_size()?))
    }

    fn write_value(&self, buf: &mut &mut [u8]) {
        (self.size as i32).write_cql(buf);
        self.iter.clone().for_each(|v| v.serialize_col(buf));
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ValueReadIter<'a, T> {
    envelope: &'a Bytes,
    buf: &'a [u8],
    remain: usize,
    _phantom: PhantomData<T>,
}

impl<'a, T> ReadValue<'a> for ValueReadIter<'a, T>
where
    T: CollectionValueRead<'a>,
{
    fn check_type(cql_type: &CqlType) -> Result<(), TypeError> {
        T::check_type(cql_type)
    }

    fn read_value(mut slice: &'a [u8], envelope: &'a Bytes) -> Result<Self, ParseError> {
        let buf = &mut slice;
        let length = i32::read_cql(buf)?;
        Ok(ValueReadIter {
            envelope,
            remain: length.try_into().map_err(invalid_data)?,
            buf,
            _phantom: PhantomData,
        })
    }
}

impl<'a, T> Iterator for ValueReadIter<'a, T>
where
    T: CollectionValueRead<'a>,
{
    type Item = Result<T::Unpacked, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remain == 0 {
            return None;
        }
        self.remain -= 1;
        Some(T::read_col_value(&mut self.buf, self.envelope))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remain, Some(self.remain))
    }
}

pub trait AsCollectionValue: Copy {
    type Unpacked;
    type Value: CollectionValueWrite;
    fn from_unpacked(unpacked: Self::Unpacked) -> Self;
    fn as_col_value(self) -> Self::Value;
}

pub trait FromCollectionValue<'a>: Sized {
    type Unpacked;
    type Value: CollectionValueRead<'a>;
    fn from_col_value(
        value: <Self::Value as CollectionValueRead<'a>>::Unpacked,
    ) -> Result<Self::Unpacked, BoxedError>;
}

impl<'a, T> AsCollectionValue for (&'a T,)
where
    T: AsValue,
{
    type Unpacked = &'a T;
    type Value = (T::Value<'a>,);

    fn from_unpacked(unpacked: Self::Unpacked) -> Self {
        (unpacked,)
    }

    fn as_col_value(self) -> Self::Value {
        (self.0.as_value(),)
    }
}

impl<'a, T> FromCollectionValue<'a> for (T,)
where
    T: FromValue<'a>,
{
    type Unpacked = T;
    type Value = (T::Value,);

    fn from_col_value(
        value: <Self::Value as CollectionValueRead<'a>>::Unpacked,
    ) -> Result<Self::Unpacked, BoxedError> {
        T::from_value(value)
    }
}

impl<'a, K, V> AsCollectionValue for (&'a K, &'a V)
where
    K: AsValue,
    V: AsValue,
{
    type Unpacked = (&'a K, &'a V);
    type Value = (K::Value<'a>, V::Value<'a>);

    fn from_unpacked(unpacked: Self::Unpacked) -> Self {
        unpacked
    }

    fn as_col_value(self) -> Self::Value {
        (self.0.as_value(), self.1.as_value())
    }
}

impl<'a, K, V> FromCollectionValue<'a> for (K, V)
where
    K: FromValue<'a>,
    V: FromValue<'a>,
{
    type Unpacked = (K, V);
    type Value = (K::Value, V::Value);

    fn from_col_value(
        value: <Self::Value as CollectionValueRead<'a>>::Unpacked,
    ) -> Result<Self::Unpacked, BoxedError> {
        Ok((K::from_value(value.0)?, V::from_value(value.1)?))
    }
}

#[derive(Debug)]
pub struct AsValueIter<I> {
    pub iter: I,
    pub size: usize,
}

impl<I> AsValueIter<I> {
    pub fn new(iter: I, size: usize) -> Self {
        Self { iter, size }
    }
}
impl<I> AsValueIter<I>
where
    I: ExactSizeIterator,
{
    pub fn with_exact_size(iter: I) -> Self {
        let len = iter.len();
        Self::new(iter, len)
    }
}

impl<I> AsValue for AsValueIter<I>
where
    I: Iterator + Clone,
    I::Item: AsCollectionValue,
{
    type Value<'a> =
        ValueWriteIter<std::iter::Map<I, fn(I::Item) -> <I::Item as AsCollectionValue>::Value>> where I: 'a;

    fn as_value(&self) -> Self::Value<'_> {
        for item in self.iter.clone() {
            item.as_col_value();
        }
        ValueWriteIter {
            iter: self.iter.clone().map(I::Item::as_col_value),
            size: self.size,
        }
    }
}

#[derive(Debug)]
pub struct FromValueIter<'a, T>(ValueReadIter<'a, T::Value>)
where
    T: FromCollectionValue<'a>;

impl<'a, T> FromValue<'a> for FromValueIter<'a, T>
where
    T: FromCollectionValue<'a>,
{
    type Value = ValueReadIter<'a, T::Value>;

    fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
        Ok(FromValueIter(value))
    }
}

impl<'a, T> Iterator for FromValueIter<'a, T>
where
    T: FromCollectionValue<'a>,
{
    type Item = Result<T::Unpacked, BoxedError>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(
            self.0
                .next()?
                .map_err(Into::into)
                .and_then(T::from_col_value),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

macro_rules! collections {
    ($tp:tt, $iter:ident, $($arg:ident $(+ $bound:ident)*),+) => {
        impl<$($arg,)+> AsValue for $tp<$($arg,)+>
        where $(
            $arg: AsValue $(+ $bound)*,
        )+
        {
            type Value<'a> = <AsValueIter<std::iter::Map<$iter<'a, $($arg,)+>, fn(<($(&'a $arg,)+) as AsCollectionValue>::Unpacked) -> ($(&'a $arg,)+)>> as AsValue>::Value<'a> where $($arg: 'a),*;

            fn as_value<'a>(&'a self) -> Self::Value<'a> {
                AsValueIter::with_exact_size(self.iter().map(<($(&'a $arg,)+)>::from_unpacked as fn(<($(&'a $arg,)+) as AsCollectionValue>::Unpacked) -> ($(&'a $arg,)+)))
                .as_value()
            }
        }

        impl<'a, $($arg,)+> FromValue<'a> for $tp<$($arg,)+>
        where $(
            $arg: for<'b> FromValue<'b> $(+ $bound)*,
        )+
        {
            type Value = <FromValueIter<'a, ($($arg,)+)> as FromValue<'a>>::Value;

            fn from_value(value: Self::Value) -> Result<Self, BoxedError> {
                FromValueIter::<'a, ($($arg,)+)>::from_value(value)?.collect()
            }
        }
    };
}
collections!(Vec, SliceIter, T);
collections!(VecDeque, VecDequeIter, T);
collections!(HashSet, HashSetIter, T + Eq + Hash);
collections!(BTreeSet, BTreeSetIter, T + Ord);
collections!(HashMap, HashMapIter, K + Eq + Hash, V);
collections!(BTreeMap, BTreeMapIter, K + Ord, V);
