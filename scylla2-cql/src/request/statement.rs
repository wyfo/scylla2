use std::sync::Arc;

use crate::{
    cql::{Consistency, SerialConsistency},
    error::{BoxedError, ValueTooBig},
    extensions::Extensions,
    frame::envelope::OpCode,
    request::Request,
    ProtocolVersion,
};

pub mod batch;
pub mod execute;
pub mod query;
pub mod query_values;

// PagingState is allocated to avoid keeping a reference on previous payload,
// because it would be kept in memory during the next request otherwise
#[derive(Debug, Clone)]
pub struct PagingState(Arc<[u8]>);

impl AsRef<[u8]> for PagingState {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

pub trait StatementParameters {
    fn consistency(&self) -> Consistency;
    fn keyspace(&self) -> Option<&str>;
    fn now_in_seconds(&self) -> Option<i32>;
    fn page_size(&self) -> Option<i32>;
    fn paging_state(&self) -> Option<&PagingState>;
    fn serial_consistency(&self) -> Option<SerialConsistency>;
    fn timestamp(&self) -> Option<i64>;
    fn tracing(&self) -> bool;
    fn warnings(&self) -> bool;
}

impl<C> StatementParameters for &C
where
    C: StatementParameters,
{
    fn consistency(&self) -> Consistency {
        C::consistency(self)
    }
    fn keyspace(&self) -> Option<&str> {
        C::keyspace(self)
    }
    fn now_in_seconds(&self) -> Option<i32> {
        C::now_in_seconds(self)
    }
    fn page_size(&self) -> Option<i32> {
        C::page_size(self)
    }
    fn paging_state(&self) -> Option<&PagingState> {
        C::paging_state(self)
    }
    fn serial_consistency(&self) -> Option<SerialConsistency> {
        C::serial_consistency(self)
    }
    fn timestamp(&self) -> Option<i64> {
        C::timestamp(self)
    }
    fn tracing(&self) -> bool {
        C::tracing(self)
    }
    fn warnings(&self) -> bool {
        C::warnings(self)
    }
}

pub trait Statement<V> {
    const OPCODE: OpCode;
    fn serialized_size(
        &self,
        version: ProtocolVersion,
        extensions: Option<&Extensions>,
        params: impl StatementParameters,
        values: &V,
    ) -> Result<usize, ValueTooBig>;
    fn serialize(
        &self,
        version: ProtocolVersion,
        extensions: Option<&Extensions>,
        slice: &mut [u8],
        params: impl StatementParameters,
        values: &V,
    );
}

impl<T, V> Statement<V> for &T
where
    T: ?Sized + Statement<V>,
{
    const OPCODE: OpCode = T::OPCODE;
    fn serialized_size(
        &self,
        version: ProtocolVersion,
        extensions: Option<&Extensions>,
        params: impl StatementParameters,
        values: &V,
    ) -> Result<usize, ValueTooBig> {
        T::serialized_size(self, version, extensions, params, values)
    }
    fn serialize(
        &self,
        version: ProtocolVersion,
        extensions: Option<&Extensions>,
        slice: &mut [u8],
        params: impl StatementParameters,
        values: &V,
    ) {
        T::serialize(self, version, extensions, slice, params, values)
    }
}

#[derive(Clone)]
struct FullStatement<S, C, V> {
    statement: S,
    params: C,
    values: V,
}

impl<S, C, V> Request for FullStatement<S, C, V>
where
    S: Statement<V>,
    C: StatementParameters,
{
    fn opcode(&self) -> OpCode {
        S::OPCODE
    }

    fn serialized_size(
        &self,
        version: ProtocolVersion,
        extensions: Option<&Extensions>,
    ) -> Result<usize, ValueTooBig> {
        self.statement
            .serialized_size(version, extensions, &self.params, &self.values)
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        extensions: Option<&Extensions>,
        slice: &mut [u8],
    ) {
        self.statement
            .serialize(version, extensions, slice, &self.params, &self.values)
    }
}

// pub trait IntoStatement<C, V, P> {
//     type Statement: Statement<V>;
//     fn into_statement(self) -> Self::Statement;
//     fn config(self) -> Option<C>;
//     fn partition(self, values: &V) -> Option<P>;
// }
//
// impl<S, C, V, P> IntoStatement<C, V, P> for &S
// where
//     S: IntoStatement<C, V, P>,
// {
//     type Statement = S::Statement;
//
//     fn into_statement(self) -> Self::Statement {
//         S::into_statement(self)
//     }
//
//     fn config(self) -> Option<C> {
//         S::config(self)
//     }
//
//     fn partition(self, values: &V) -> Option<P> {
//         S::partition(self, values)
//     }
// }
//
// impl<S, C, V, P> IntoStatement<C, V, P> for Arc<S>
// where
//     S: ?Sized + IntoStatement<C, V, P>,
// {
//     type Statement = S::Statement;
//
//     fn into_statement(self) -> Self::Statement {
//         S::into_statement(self)
//     }
//
//     fn config(self) -> Option<C> {
//         S::config(self)
//     }
//
//     fn partition(self, values: &V) -> Option<P> {
//         S::partition(self, values)
//     }
// }
//
// pub trait AsValues<'a, S> {
//     type Values;
//     fn as_values(&'a self) -> Result<Self::Values, BoxedError>;
// }
//
// impl<'a, S, T> AsValues<'a, S> for &T
// where
//     T: AsValues<'a, S>,
// {
//     type Values = T::Values;
//
//     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
//         T::as_values(self)
//     }
// }
//
// // #[cfg(test)]
// // mod test {
// //     use crate::request::statement::{batch::Batch, AsStatement, AsValues};
// //
// //     fn exec<'a, S, V, T>(_stmt: S, _values: V)
// //     where
// //         S: AsStatement<'a, (), V::Values, (), Statement = T>,
// //         V: AsValues<'a, T>,
// //     {
// //     }
// //
// //     #[test]
// //     fn test_statement_with_values() {
// //         exec("", ());
// //         exec("", &[] as &[bool]);
// //         exec(("",), ((),));
// //         // exec(("", ""), ((), ()));
// //         // exec(("", "", ""), ((), (), ()));
// //         exec(Batch::logged(vec![""]), &[()] as &[_]);
// //         exec(Batch::logged(vec![""]), vec![()]);
// //     }
// // }
