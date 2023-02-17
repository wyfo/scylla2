use enumflags2::bitflags;

use crate::{
    cql::{ShortBytes, WriteCql},
    error::{InvalidRequest, ValueTooBig},
    extensions::ProtocolExtensions,
    frame::envelope::OpCode,
    request::{query::values::QueryValues, Request},
    response::result::prepared::Prepared,
    utils::{flags, tuples1},
    Consistency, ProtocolVersion, SerialConsistency,
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

#[bitflags]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
pub enum BatchFlags {
    WithSerialConsistency = 0x0010,
    WithDefaultTimestamp = 0x0020,
    WithNamesForValues = 0x0040, // should not be used
    WithKeyspace = 0x0080,
    WithNowInSeconds = 0x0100,
}

#[derive(Debug, Clone, Copy)]
pub struct Batch<'a, S, V> {
    pub r#type: BatchType,
    pub statements: S,
    pub values: V,
    pub consistency: Consistency,
    pub keyspace: Option<&'a str>,
    pub now_in_seconds: Option<i32>,
    pub serial_consistency: Option<SerialConsistency>,
    pub timestamp: Option<i64>,
}

impl<S, V> Batch<'_, S, V> {
    fn _serialized_size(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        statement_count: u16,
        statements_size: usize,
    ) -> Result<usize, ValueTooBig> {
        fn opt_size(opt: Option<impl WriteCql>) -> Result<usize, ValueTooBig> {
            opt.as_ref().map(WriteCql::cql_size).unwrap_or(Ok(0))
        }
        let flags_size = match version {
            ProtocolVersion::V4 => 0u8.cql_size()?,
            ProtocolVersion::V5 => 0u32.cql_size()?,
        };
        Ok((self.r#type as u8).cql_size()?
            + statement_count.cql_size()?
            + statements_size
            + Consistency::default().cql_size()?
            + flags_size
            + 0i16.cql_size()?
            + opt_size(self.serial_consistency)?
            + opt_size(self.timestamp)?
            + opt_size(self.keyspace)?
            + opt_size(self.now_in_seconds)?)
    }

    fn _serialize(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        statement_count: u16,
        write_statements: impl FnOnce(&mut &mut [u8]),
        mut slice: &mut [u8],
    ) {
        (self.r#type as i8).write_cql(&mut slice);
        statement_count.write_cql(&mut slice);
        write_statements(&mut slice);
        self.consistency.write_cql(&mut slice);
        let mut flags = flags!(
            BatchFlags::WithSerialConsistency: self.serial_consistency.is_some(),
            BatchFlags::WithDefaultTimestamp: self.timestamp.is_some(),
        );
        if version >= ProtocolVersion::V5 {
            flags |= flags!(
                BatchFlags::WithKeyspace:self.keyspace.is_some(),
                BatchFlags::WithNowInSeconds: self.now_in_seconds.is_some(),
            );
        }
        match version {
            ProtocolVersion::V4 => (flags.bits() as u8).write_cql(&mut slice),
            ProtocolVersion::V5 => flags.write_cql(&mut slice),
        };
        fn opt_write(opt: Option<impl WriteCql>, buf: &mut &mut [u8]) {
            if let Some(v) = opt {
                v.write_cql(buf)
            }
        }
        opt_write(self.serial_consistency, &mut slice);
        opt_write(self.timestamp, &mut slice);
        if version >= ProtocolVersion::V5 {
            opt_write(self.keyspace, &mut slice);
            opt_write(self.now_in_seconds, &mut slice);
        }
    }
}

pub trait BatchStatement {
    fn prepared(&self) -> bool;
    fn bytes(&self) -> &[u8];
}

impl<T> BatchStatement for &T
where
    T: ?Sized + BatchStatement,
{
    fn prepared(&self) -> bool {
        T::prepared(self)
    }
    fn bytes(&self) -> &[u8] {
        T::bytes(self)
    }
}

impl BatchStatement for str {
    fn prepared(&self) -> bool {
        false
    }
    fn bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl BatchStatement for String {
    fn prepared(&self) -> bool {
        false
    }
    fn bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl BatchStatement for Prepared {
    fn prepared(&self) -> bool {
        true
    }
    fn bytes(&self) -> &[u8] {
        &self.id
    }
}

trait BatchStatementExt: BatchStatement {
    fn serialized_size(&self) -> Result<usize, ValueTooBig> {
        if self.prepared() {
            ShortBytes(self.bytes()).cql_size()
        } else {
            self.bytes().cql_size()
        }
    }

    fn serialize(&self, buf: &mut &mut [u8]) {
        if self.prepared() {
            ShortBytes(self.bytes()).write_cql(buf)
        } else {
            self.bytes().write_cql(buf)
        }
    }
}

impl<T> BatchStatementExt for T where T: BatchStatement {}

impl<S, V> Request for Batch<'_, &[S], &[V]>
where
    S: BatchStatement,
    V: QueryValues,
{
    fn opcode(&self) -> OpCode {
        OpCode::Batch
    }

    fn check(
        &self,
        _version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
    ) -> Result<(), InvalidRequest> {
        if self.statements.len() != self.values.len() {
            Err(InvalidRequest::BatchStatementsAndValuesCountNotMatching {
                statements_count: self.statements.len(),
                values_count: self.values.len(),
            })
        } else {
            Ok(())
        }
    }

    fn serialized_size(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig> {
        let statements_size: usize = Iterator::zip(self.statements.iter(), self.values.iter())
            .map(|(stmt, val)| {
                Ok(stmt.prepared().cql_size()?
                    + stmt.serialized_size()?
                    + val.count().cql_size()?
                    + val.values_size()?)
            })
            .sum::<Result<_, _>>()?;
        self._serialized_size(
            version,
            extensions,
            self.statements.len() as u16,
            statements_size,
        )
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        extensions: Option<&ProtocolExtensions>,
        slice: &mut [u8],
    ) {
        let write_statements = |buf: &mut &mut [u8]| {
            for (stmt, val) in Iterator::zip(self.statements.iter(), self.values.iter()) {
                stmt.prepared().write_cql(buf);
                stmt.serialize(buf);
                val.count().write_cql(buf);
                val.write_values(buf);
            }
        };
        self._serialize(
            version,
            extensions,
            self.statements.len() as u16,
            write_statements,
            slice,
        );
    }
}

macro_rules! batch {
    ($($stmt:ident/$values:ident/$idx:tt),*;$len:literal) => {
        impl<$($stmt, $values,)*> Request for Batch<'_, ($($stmt,)*), ($($values,)*)>
        where $(
            $stmt: BatchStatement,
            $values: QueryValues,
        )*
        {
            fn opcode(&self) -> OpCode {
                OpCode::Batch
            }

            fn serialized_size(
                &self,
                version: ProtocolVersion,
                extensions: Option<&ProtocolExtensions>,
            ) -> Result<usize, ValueTooBig> {
                let statements_size = (0 $(
                    + self.statements.$idx.prepared().cql_size()?
                    + self.statements.$idx.serialized_size()?
                    + self.values.$idx.count().cql_size()?
                    + self.values.$idx.values_size()?
                )*);
                self._serialized_size(version, extensions, $len, statements_size)
            }

            fn serialize(
                &self,
                version: ProtocolVersion,
                extensions: Option<&ProtocolExtensions>,
                slice: &mut [u8],
            ) {
                let write_statements = |buf: &mut &mut [u8]| {
                    $(
                        self.statements.$idx.prepared().write_cql(buf);
                        self.statements.$idx.serialize(buf);
                        self.values.$idx.count().write_cql(buf);
                        self.values.$idx.write_values(buf);
                    )*
                };
                self._serialize(version, extensions, $len, write_statements, slice);
            }
        }

        impl<S, $($values,)*> Request for Batch<'_, &[S; $len], ($($values,)*)>
        where
            S: BatchStatement,
            $($values: QueryValues,)*
        {
            fn opcode(&self) -> OpCode {
                OpCode::Batch
            }

            fn serialized_size(
                &self,
                version: ProtocolVersion,
                extensions: Option<&ProtocolExtensions>,
            ) -> Result<usize, ValueTooBig> {
                let statements_size = (0 $(
                    + self.statements[$idx].prepared().cql_size()?
                    + self.statements[$idx].serialized_size()?
                    + self.values.$idx.count().cql_size()?
                    + self.values.$idx.values_size()?
                )*);
                self._serialized_size(version, extensions, $len, statements_size)
            }

            fn serialize(
                &self,
                version: ProtocolVersion,
                extensions: Option<&ProtocolExtensions>,
                slice: &mut [u8],
            ) {
                let write_statements = |buf: &mut &mut [u8]| {
                    $(
                        self.statements[$idx].prepared().write_cql(buf);
                        self.statements[$idx].serialize(buf);
                        self.values.$idx.count().write_cql(buf);
                        self.values.$idx.write_values(buf);
                    )*
                };
                self._serialize(version, extensions, $len, write_statements, slice);
            }
        }
    };
}

tuples1!(batch);
