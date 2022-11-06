use crate::response::supported::Supported;

pub const SCYLLA_RATE_LIMIT_ERROR: &str = "SCYLLA_RATE_LIMIT_ERROR";
const ERROR_CODE_PREFIX: &str = "ERROR_CODE=";

#[derive(Default, Copy, Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProtocolExtensions {
    pub rate_limit_error_code: Option<u32>,
}

impl ProtocolExtensions {
    pub fn from_supported(supported: &Supported) -> Self {
        let rate_limit_error_code =
            supported
                .options
                .get(SCYLLA_RATE_LIMIT_ERROR)
                .and_then(|values| {
                    values
                        .iter()
                        .find_map(|s| s.strip_prefix(ERROR_CODE_PREFIX)?.parse().ok())
                });
        Self {
            rate_limit_error_code,
        }
    }
}
