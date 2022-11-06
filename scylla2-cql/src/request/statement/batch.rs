use std::str;

use crate::{
    cql::{CqlWrite, LongString, ShortBytes},
    error::{BoxedError, ValueTooBig},
    extensions::Extensions,
    frame::envelope::OpCode,
    request::statement::{
        query::{query_parameters_size, write_query_parameters},
        query_values::QueryValues,
        Statement, StatementParameters,
    },
    response::result::PreparedStatement,
    utils::tuples1,
    ProtocolVersion,
};

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
#[non_exhaustive]
pub enum BatchType {
    #[default]
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
}

#[derive(Debug, Clone, Copy)]
pub struct Batch<S> {
    pub r#type: BatchType,
    pub statements: S,
}

impl<S> Batch<S> {
    pub fn new(r#type: BatchType, statements: S) -> Self {
        Batch { r#type, statements }
    }
    pub fn logged(statements: S) -> Self {
        Batch {
            r#type: BatchType::Logged,
            statements,
        }
    }
    pub fn unlogged(statements: S) -> Self {
        Batch {
            r#type: BatchType::Unlogged,
            statements,
        }
    }
    pub fn counter(statements: S) -> Self {
        Batch {
            r#type: BatchType::Counter,
            statements,
        }
    }
}

pub trait NotBatch {
    fn prepared(&self) -> bool;
    fn batched_size(&self) -> Result<usize, ValueTooBig>;
    fn write_batched(&self, buf: &mut &mut [u8]);
}

impl NotBatch for &str {
    fn prepared(&self) -> bool {
        false
    }
    fn batched_size(&self) -> Result<usize, ValueTooBig> {
        LongString(self).cql_size()
    }
    fn write_batched(&self, buf: &mut &mut [u8]) {
        LongString(self).write_cql(buf);
    }
}

impl NotBatch for &PreparedStatement {
    fn prepared(&self) -> bool {
        true
    }
    fn batched_size(&self) -> Result<usize, ValueTooBig> {
        ShortBytes(&self.id).cql_size()
    }
    fn write_batched(&self, buf: &mut &mut [u8]) {
        ShortBytes(&self.id).write_cql(buf);
    }
}

macro_rules! batch {
    ($($stmt:ident/$values:ident/$idx:tt),*;$len:literal) => {
        impl<$($stmt,$values,)*> Statement<($($values,)*)> for Batch<($($stmt,)*)>
        where
            $(
                $stmt: Statement<$values>,
                $stmt: NotBatch,
                $values: QueryValues,
            )*
        {
            const OPCODE: OpCode = OpCode::Batch;

            fn serialized_size(
                &self,
                version: ProtocolVersion,
                _extensions: Option<&Extensions>,
                params: impl StatementParameters,
                values: &($($values,)*),
            ) -> Result<usize, ValueTooBig> {
                Ok(
                    (BatchType::default() as u8).cql_size()?
                    + ($len as u16).cql_size()?
                    $(
                        + self.statements.$idx.prepared().cql_size()?
                        + self.statements.$idx.batched_size()?
                        + values.$idx.count().cql_size()?
                        + values.$idx.serialized_size()?
                    )*
                    + query_parameters_size(version, params, None::<&()>)?)
            }

            fn serialize(
                &self,
                version: ProtocolVersion,
                _extensions: Option<&Extensions>,
                mut slice: &mut [u8],
                params: impl StatementParameters,
                values: &($($values,)*),
            ) {
                (self.r#type as u8).write_cql(&mut slice);
                ($len as u16).write_cql(&mut slice);
                $(
                    self.statements.$idx.prepared().write_cql(&mut slice);
                    self.statements.$idx.write_batched(&mut slice);
                    values.$idx.count().write_cql(&mut slice);
                    values.$idx.serialize(&mut slice);
                )*
                write_query_parameters(version, &mut slice, false, params, None::<&()>)
            }
        }

        // impl<'a, C, $($stmt, $values,)* P> AsStatement<'a, C, ($($values,)*), P> for Batch<($($stmt,)*)>
        // where
        //     $(
        //         $stmt: AsStatement<'a, C, $values, P>,
        //         $stmt::Statement: NotBatch,
        //         $values: QueryValues,
        //     )*
        // {
        //     type Statement = Batch<($($stmt::Statement,)*)>;
        //
        //     fn as_statement(&'a self) -> Self::Statement {
        //         Batch::new(self.r#type, ($(self.statements.$idx.as_statement(),)*))
        //     }
        //
        //     fn config(&'a self) -> Option<C> {
        //         None
        //     }
        //
        //     fn partition(&'a self, values: &($($values,)*)) -> Option<P> {
        //         self.statements.0.partition(&values.0)
        //     }
        // }
        //
        // impl<'a, C, $($stmt, $values,)* P> AsStatement<'a, C, ($($values,)*), P> for ($($stmt,)*)
        // where
        //     $(
        //         $stmt: AsStatement<'a, C, $values, P>,
        //         $stmt::Statement: NotBatch,
        //         $values: QueryValues,
        //     )*
        // {
        //     type Statement = Batch<($($stmt::Statement,)*)>;
        //
        //     fn as_statement(&'a self) -> Self::Statement {
        //         Batch::logged(($(self.$idx.as_statement(),)*))
        //     }
        //
        //     fn config(&'a self) -> Option<C> {
        //         None
        //     }
        //
        //     fn partition(&'a self, values: &($($values,)*)) -> Option<P> {
        //         self.0.partition(&values.0)
        //     }
        // }

        // impl<'a, $($stmt, $values,)*> AsValues<'a, Batch<($($stmt,)*)>> for ($($values,)*)
        // where
        //     $(
        //         $stmt: NotBatch,
        //         $values: AsValues<'a, $stmt>,
        //         $values::Values: QueryValues,
        //     )*
        // {
        //     type Values = ($($values::Values,)*);
        //
        //     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
        //         Ok(($(self.$idx.as_values()?,)*))
        //     }
        // }
    };
}

tuples1!(batch);
// impl<'a, C, S, V, P> AsStatement<'a, C, (V,), P> for (S,)
// where
//     S: for<'b> AsStatement<'b, C, V, P>,
//     for<'b> <S as AsStatement<'b, C, V, P>>::Statement: NotBatch,
//     V: QueryValues,
// {
//     type Statement = Batch<(<S as AsStatement<'a, C, V, P>>::Statement,)>;
//
//     fn as_statement(&'a self) -> Self::Statement {
//         Batch::logged((self.0.as_statement(),))
//     }
//
//     fn config(&'a self) -> Option<C> {
//         None
//     }
//
//     fn partition(&'a self, values: &(V,)) -> Option<P> {
//         self.0.partition(&values.0)
//     }
// }
// impl<'a, S, V> AsValues<'a, Batch<(S,)>> for (V,)
// where
//     S: NotBatch,
//     V: AsValues<'a, S>,
//     V::Values: QueryValues,
// {
//     type Values = (V::Values,);
//
//     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
//         Ok((self.0.as_values()?,))
//     }
// }
//
// type BatchMap<'a, T, S> = Batch<std::iter::Map<std::slice::Iter<'a, T>, &'a dyn Fn(&'a T) -> S>>;
//
// impl<'a, T, S, V> Statement<&[V]> for BatchMap<'a, T, S>
// where
//     S: Statement<V> + NotBatch + 'a,
//     V: QueryValues,
// {
//     const OPCODE: OpCode = OpCode::Batch;
//
//     fn serialized_size(
//         &self,
//         version: ProtocolVersion,
//         _extensions: Option<&Extensions>,
//         params: impl StatementParameters,
//         values: &&[V],
//     ) -> Result<usize, ValueTooBig> {
//         assert_eq!(self.statements.len(), values.len());
//         let stmts_size: usize = Iterator::zip(self.statements.clone(), values.iter())
//             .map(|(stmt, val)| {
//                 Ok(stmt.prepared().cql_size()?
//                     + stmt.batched_size()?
//                     + val.count().cql_size()?
//                     + val.serialized_size()?)
//             })
//             .sum::<Result<_, _>>()?;
//         Ok((BatchType::default() as i8).cql_size()?
//             + (self.statements.len() as u16).cql_size()?
//             + stmts_size
//             + query_parameters_size(version, params, None::<&()>)?)
//     }
//
//     fn serialize(
//         &self,
//         version: ProtocolVersion,
//         _extensions: Option<&Extensions>,
//         mut slice: &mut [u8],
//         params: impl StatementParameters,
//         values: &&[V],
//     ) {
//         assert_eq!(self.statements.len(), values.len());
//         (self.r#type as i8).write_cql(&mut slice);
//         (self.statements.len() as u16).write_cql(&mut slice);
//         for (stmt, val) in Iterator::zip(self.statements.clone(), values.iter()) {
//             stmt.prepared().write_cql(&mut slice);
//             stmt.write_batched(&mut slice);
//             val.count().write_cql(&mut slice);
//             val.serialize(&mut slice);
//         }
//         write_query_parameters(version, &mut slice, false, params, None::<&()>)
//     }
// }
//
// impl<'a, S, C, V, P: 'a> AsStatement<'a, C, &[V], P> for Batch<&'a [S]>
// where
//     S: AsStatement<'a, C, V, P>,
//     S::Statement: NotBatch + 'a,
//     C: 'a,
//     V: QueryValues + 'a,
//     P: 'a,
// {
//     type Statement = BatchMap<'a, S, S::Statement>;
//
//     fn as_statement(&'a self) -> Self::Statement {
//         Batch {
//             r#type: self.r#type,
//             statements: self.statements.iter().map(&AsStatement::as_statement),
//         }
//     }
//
//     fn config(&'a self) -> Option<C> {
//         None
//     }
//
//     fn partition(&'a self, values: &&[V]) -> Option<P> {
//         self.statements[0].partition(&values[0])
//     }
// }
//
// impl<'a, S, C, V, P> AsStatement<'a, C, &[V], P> for Batch<Vec<S>>
// where
//     S: AsStatement<'a, C, V, P> + 'a,
//     S::Statement: NotBatch + 'a,
//     C: 'a,
//     V: QueryValues + 'a,
//     P: 'a,
// {
//     type Statement = BatchMap<'a, S, S::Statement>;
//
//     fn as_statement(&'a self) -> Self::Statement {
//         Batch {
//             r#type: self.r#type,
//             statements: self.statements.iter().map(&AsStatement::as_statement),
//         }
//     }
//
//     fn config(&'a self) -> Option<C> {
//         None
//     }
//
//     fn partition(&'a self, values: &&[V]) -> Option<P> {
//         self.statements[0].partition(&values[0])
//     }
// }
//
// impl<'a, T, S, V> AsValues<'a, BatchMap<'a, T, S>> for &[V]
// where
//     S: NotBatch,
//     V: QueryValues + 'a,
// {
//     type Values = &'a [V];
//
//     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
//         Ok(self)
//     }
// }
//
// impl<'a, T, S, V> AsValues<'a, BatchMap<'a, T, S>> for Vec<V>
// where
//     S: NotBatch,
//     V: QueryValues + 'a,
// {
//     type Values = &'a [V];
//
//     fn as_values(&'a self) -> Result<Self::Values, BoxedError> {
//         Ok(self)
//     }
// }
