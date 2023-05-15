use enumflags2::bitflags;

use crate::{
    cql::WriteCql, error::ValueTooBig, extensions::ProtocolExtensions,
    request::query::values::QueryValues, utils::flags, Consistency, ProtocolVersion,
    SerialConsistency,
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

#[derive(Debug, Default, Clone)]
pub struct QueryParameters<'a, V> {
    pub consistency: Consistency,
    pub keyspace: Option<&'a str>,
    pub now_in_seconds: Option<i32>,
    pub page_size: Option<i32>,
    pub paging_state: Option<&'a [u8]>,
    pub serial_consistency: Option<SerialConsistency>,
    pub skip_metadata: bool,
    pub timestamp: Option<i64>,
    pub values: V,
}

impl<V> QueryParameters<'_, V> {
    pub fn with_values(values: V) -> Self {
        QueryParameters {
            consistency: Default::default(),
            keyspace: Default::default(),
            now_in_seconds: Default::default(),
            page_size: Default::default(),
            paging_state: Default::default(),
            serial_consistency: Default::default(),
            skip_metadata: Default::default(),
            timestamp: Default::default(),
            values,
        }
    }
}

impl<V> QueryParameters<'_, V>
where
    V: QueryValues,
{
    pub fn serialized_size(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
    ) -> Result<usize, ValueTooBig> {
        fn opt_size(opt: Option<impl WriteCql>) -> Result<usize, ValueTooBig> {
            opt.as_ref().map_or(Ok(0), WriteCql::cql_size)
        }
        let flags_size = match version {
            ProtocolVersion::V4 => 0u8.cql_size()?,
            ProtocolVersion::V5 => 0u32.cql_size()?,
        };
        let values_size = if self.values.count() == 0 {
            0
        } else {
            self.values.count().cql_size()? + self.values.values_size()?
        };
        Ok(Consistency::default().cql_size()?
            + flags_size
            + 0i16.cql_size()?
            + values_size
            + opt_size(self.page_size)?
            + opt_size(self.paging_state.map(AsRef::as_ref))?
            + opt_size(self.serial_consistency)?
            + opt_size(self.timestamp)?
            + opt_size(self.keyspace)?
            + opt_size(self.now_in_seconds)?)
    }

    pub fn serialize(
        &self,
        version: ProtocolVersion,
        _extensions: Option<&ProtocolExtensions>,
        mut slice: &mut [u8],
    ) {
        self.consistency.write_cql(&mut slice);
        let has_values = self.values.count() > 0;
        let mut flags = flags!(
            QueryParametersFlag::Values: has_values,
            QueryParametersFlag::SkipMetadata: self.skip_metadata,
            QueryParametersFlag::PageSize: self.page_size.is_some(),
            QueryParametersFlag::WithPagingState: self.paging_state.is_some(),
            QueryParametersFlag::WithSerialConsistency: self.serial_consistency.is_some(),
            QueryParametersFlag::WithDefaultTimestamp: self.timestamp.is_some(),
            QueryParametersFlag::WithNamesForValues: self.values.named(),
        );
        if version >= ProtocolVersion::V5 {
            flags |= flags!(
                QueryParametersFlag::WithKeyspace: self.keyspace.is_some(),
                QueryParametersFlag::WithNowInSeconds: self.now_in_seconds.is_some(),
            );
        }
        match version {
            ProtocolVersion::V4 => (flags.bits() as u8).write_cql(&mut slice),
            ProtocolVersion::V5 => flags.write_cql(&mut slice),
        };
        if has_values {
            self.values.count().write_cql(&mut slice);
            self.values.write_values(&mut slice);
        }
        fn opt_write(opt: Option<impl WriteCql>, buf: &mut &mut [u8]) {
            if let Some(v) = opt {
                v.write_cql(buf);
            }
        }
        opt_write(self.page_size, &mut slice);
        opt_write(self.paging_state.map(AsRef::as_ref), &mut slice);
        opt_write(self.serial_consistency, &mut slice);
        opt_write(self.timestamp, &mut slice);
        if version >= ProtocolVersion::V5 {
            opt_write(self.keyspace, &mut slice);
            opt_write(self.now_in_seconds, &mut slice);
        }
    }
}
