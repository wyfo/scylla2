#[derive(Debug, Copy, Clone, Eq, PartialEq, strum::EnumString, strum::Display, strum::AsRefStr)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum ConnectionOptions {
    Compression,
    CqlVersion,
    DriverName,
    ScyllaNrShard,
    ScyllaPartitioner,
    ScyllaShard,
    ScyllaShardAwarePort,
    ScyllaShardAwarePortSsl,
    ScyllaShardingAlgorithm,
    ScyllaShardingIgnoreMsb,
    ScyllaRateLimitError,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, strum::EnumString)]
#[strum(serialize_all = "kebab-case")]
#[non_exhaustive]
pub enum ScyllaShardingAlgorithm {
    BiasedTokenRoundRobin,
}
