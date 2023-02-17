use crate::response::supported::Supported;

#[derive(Default, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProtocolExtensions {
    pub scylla_rate_limit_error: Option<u32>,
    pub scylla_lwt_add_metadata_mark: Option<u32>,
}

impl ProtocolExtensions {
    pub fn from_supported(supported: &Supported) -> Self {
        let scylla_rate_limit_error =
            supported
                .options
                .get("SCYLLA_RATE_LIMIT_ERROR")
                .and_then(|values| {
                    values
                        .iter()
                        .find_map(|s| s.strip_prefix("ERROR_CODE=")?.parse().ok())
                });
        let scylla_lwt_add_metadata_mark = supported
            .options
            .get("SCYLLA_LWT_ADD_METADATA_MARK")
            .and_then(|values| {
                values.iter().find_map(|s| {
                    s.strip_prefix("SCYLLA_LWT_OPTIMIZATION_META_BIT_MASK=")?
                        .parse()
                        .ok()
                })
            });
        Self {
            scylla_rate_limit_error,
            scylla_lwt_add_metadata_mark,
        }
    }
}
