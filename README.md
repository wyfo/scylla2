# scylla2

This is a POC (with more than 12kloc, maybe not a POC anymore) of a new implementation of the Scylla Rust driver, focusing on performance and memory consumption, but not only.

I've started the development at the end of October 2022, mostly on my spare time. So the development is quite messy (I work mostly during night when kids sleep), and there are still some things to do (hopefully, mostly tests at this time).

Actually, I didn't have the time to finish it completely for the Scylla Summit for which I have made this repository public. The connection pool implementation is incomplete, so it's not currently usable (the driver can still connect and send request to the database using the control connection). I hope I will be able to finish it tonight.

## Main differences with the official driver

- Builtin support of multiple native CQL protocol version(s), can be used with Cassandra 4.0 (which uses protocol v5)
- Zero-copy deserialization
- Zero/one allocation per-request in normal case (especially thanks to https://github.com/wyfo/swap-buffer-queue)
- Node distance support (so datacenter-aware and whitelist/blacklist possibility)
- Query plan caching (taking distance in account)
- Easier interface with only one method `Session::execute` instead of `Session::query`/`Session::execute`/`Se- Retry and speculative executions policies are not handled yet, but I'm still thinking about a design (and I need to fully understand speculative execution before)
- Execution metadata (token, node, failed attempts, etc.) are not available for the momentssion::batch`
- Access to database events and session events (connection opening/failure, node status update, topology update, schema agreement, etc.) using `tokio::sync::broadcast`
- No schema metadata (but it can be built as a sidecar service, using dababase schema event and session schema agreement event to trigger refresh)
- No internal API used for query execution, which means that custom execution can be built without too much effort

## TODO

- Finish connection pool implementation
- Support serverless (I've just learned about it during Scylla Summit ^^'), see https://github.com/scylladb/scylla-rust-driver/issues/640
- Retry and speculative executions policies are not handled yet, but I'm still thinking about a design (and I need to fully understand speculative execution before)
- Execution metadata (token, node, failed attempts, etc.) are not available for the moment
- tracing
- use default statement config
- test, tests, and more tests
- precise benchmark