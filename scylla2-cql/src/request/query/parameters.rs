use enumflags2::bitflags;

use crate::{
    cql::WriteCql, error::ValueTooBig, extensions::ProtocolExtensions,
    request::query::values::QueryValues, response::result::rows::PagingState, utils::flags,
    Consistency, ProtocolVersion, SerialConsistency,
};

#[bitflags]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u32)]
pub enum QueryParametersFlag {
    Values = 0x0001,
    SkipMetadata = 0x0002,
    PageSize = 0x0004,
    WithPagingState = 0x0008,
    WithSerialConsistency = 0x0010,
    WithDefaultTimestamp = 0x0020,
    WithNamesForValues = 0x0040,
    WithKeyspace = 0x0080,
    WithNowInSeconds = 0x0100,
}

// Use a trait instead of a struct to allow custom statement configuration with additional
// parameters (idempotency, retry policy, etc.)
pub trait QueryParameters<V> {
    fn consistency(&self) -> Consistency;
    fn keyspace(&self) -> Option<&str>;
    fn now_in_seconds(&self) -> Option<i32>;
    fn page_size(&self) -> Option<i32>;
    fn paging_state(&self) -> Option<&PagingState>;
    fn serial_consistency(&self) -> Option<SerialConsistency>;
    fn skip_metadata(&self) -> bool;
    fn timestamp(&self) -> Option<i64>;
    fn values(&self) -> &V;
}

#[derive(Debug, Default)]
pub struct RawQueryParameters<'a, V> {
    pub consistency: Consistency,
    pub keyspace: Option<&'a str>,
    pub now_in_seconds: Option<i32>,
    pub page_size: Option<i32>,
    pub paging_state: Option<&'a PagingState>,
    pub serial_consistency: Option<SerialConsistency>,
    pub skip_metadata: bool,
    pub timestamp: Option<i64>,
    pub values: V,
}

impl<V> QueryParameters<V> for RawQueryParameters<'_, V> {
    fn consistency(&self) -> Consistency {
        self.consistency
    }
    fn keyspace(&self) -> Option<&str> {
        self.keyspace
    }
    fn now_in_seconds(&self) -> Option<i32> {
        self.now_in_seconds
    }
    fn page_size(&self) -> Option<i32> {
        self.page_size
    }
    fn paging_state(&self) -> Option<&PagingState> {
        self.paging_state
    }
    fn serial_consistency(&self) -> Option<SerialConsistency> {
        self.serial_consistency
    }
    fn skip_metadata(&self) -> bool {
        self.skip_metadata
    }
    fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }
    fn values(&self) -> &V {
        &self.values
    }
}

#[derive(Debug)]
pub struct OnlyValues<V>(pub V);

impl<V> QueryParameters<V> for OnlyValues<V> {
    fn consistency(&self) -> Consistency {
        Default::default()
    }
    fn keyspace(&self) -> Option<&str> {
        Default::default()
    }
    fn now_in_seconds(&self) -> Option<i32> {
        Default::default()
    }
    fn page_size(&self) -> Option<i32> {
        Default::default()
    }
    fn paging_state(&self) -> Option<&PagingState> {
        Default::default()
    }
    fn serial_consistency(&self) -> Option<SerialConsistency> {
        Default::default()
    }
    fn skip_metadata(&self) -> bool {
        Default::default()
    }
    fn timestamp(&self) -> Option<i64> {
        Default::default()
    }
    fn values(&self) -> &V {
        &self.0
    }
}

pub(crate) trait QueryParametersExt<V>: QueryParameters<V>
where
    V: QueryValues,
{
    fn serialized_size(
        &self,
        version: ProtocolVersion,
        _extensions: ProtocolExtensions,
    ) -> Result<usize, ValueTooBig> {
        fn opt_size(opt: Option<impl WriteCql>) -> Result<usize, ValueTooBig> {
            opt.as_ref().map(WriteCql::cql_size).unwrap_or(Ok(0))
        }
        let flags_size = match version {
            ProtocolVersion::V4 => 0u8.cql_size()?,
            ProtocolVersion::V5 => 0u32.cql_size()?,
        };
        let values_size = if self.values().count() == 0 {
            0
        } else {
            self.values().count().cql_size()? + self.values().values_size()?
        };
        Ok(Consistency::default().cql_size()?
            + flags_size
            + 0i16.cql_size()?
            + values_size
            + opt_size(self.page_size())?
            + opt_size(self.paging_state().map(AsRef::as_ref))?
            + opt_size(self.serial_consistency())?
            + opt_size(self.timestamp())?
            + opt_size(self.keyspace())?
            + opt_size(self.now_in_seconds())?)
    }

    fn serialize(
        &self,
        version: ProtocolVersion,
        _extensions: ProtocolExtensions,
        mut slice: &mut [u8],
    ) {
        self.consistency().write_cql(&mut slice);
        let has_values = self.values().count() > 0;
        let mut flags = flags!(
            QueryParametersFlag::Values: has_values,
            QueryParametersFlag::SkipMetadata: self.skip_metadata(),
            QueryParametersFlag::PageSize: self.page_size().is_some(),
            QueryParametersFlag::WithPagingState: self.paging_state().is_some(),
            QueryParametersFlag::WithSerialConsistency: self.serial_consistency().is_some(),
            QueryParametersFlag::WithDefaultTimestamp: self.timestamp().is_some(),
            QueryParametersFlag::WithNamesForValues: self.values().named(),
        );
        if version >= ProtocolVersion::V5 {
            flags |= flags!(
                QueryParametersFlag::WithKeyspace: self.keyspace().is_some(),
                QueryParametersFlag::WithNowInSeconds: self.now_in_seconds().is_some(),
            );
        }
        match version {
            ProtocolVersion::V4 => (flags.bits() as u8).write_cql(&mut slice),
            ProtocolVersion::V5 => flags.write_cql(&mut slice),
        };
        if has_values {
            self.values().count().write_cql(&mut slice);
            self.values().write_values(&mut slice);
        }
        fn opt_write(opt: Option<impl WriteCql>, buf: &mut &mut [u8]) {
            if let Some(v) = opt {
                v.write_cql(buf)
            }
        }
        opt_write(self.page_size(), &mut slice);
        opt_write(self.paging_state().map(AsRef::as_ref), &mut slice);
        opt_write(self.serial_consistency(), &mut slice);
        opt_write(self.timestamp(), &mut slice);
        if version >= ProtocolVersion::V5 {
            opt_write(self.keyspace(), &mut slice);
            opt_write(self.now_in_seconds(), &mut slice);
        }
    }
}

impl<T, V> QueryParametersExt<V> for T
where
    T: QueryParameters<V>,
    V: QueryValues,
{
}
