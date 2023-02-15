# scylla2

This is a POC (with more than 12kloc, maybe not a POC anymore) of a new implementation of the Scylla Rust driver, focusing on performance and memory consumption, but not only.

I've started the development at the end of October 2022, mostly on my spare time. So the development is quite messy (I work mostly during night when kids sleep), and there are still some things to do (hopefully, mostly tests at this time).

Actually, I didn't have the time to finish it completely for the Scylla Summit for which I have made this repository public. The connection pool implementation is incomplete, so it's not currently usable (the driver can still connect and send request to the database using the control connection). I hope I will be able to finish it tonight.

## Why did I write a new driver from scratch instead of contributing to the existing one?

Actually, I'm already a contributor, you may especially check https://github.com/scylladb/scylla-rust-driver/issues/571.
Sometimes, changes require so much deep modifications that I couldn't think of doing them properly. The linked issue is an example. I had so many things in my head that I've found easier to express them from scratch, at first as a POC, but the motivation grew while I was moving away from the current implementation, so here I am.

My first issue was about unnecessary allocations and zero-copy deserialization (see the linked issue), as I manipulate a lot of strings, sometimes big, at work.

Also, as I was facing some memory issues in my applications, where I did suspect the driver, I went deep into the code and spotted indeed a lot of allocations, not necessarily related to my issue, but I love optimization challenges, and a lot of ideas came to my head. The most important was what has become https://github.com/wyfo/swap-buffer-queue.

This accumulation of ideas (which I simply enjoy coding on my spare time), with a lot of them diverging from the current driver implementation, end by giving birth to a complete driver implementation.

## Main differences with the official driver

- Builtin support of multiple native CQL protocol version(s), can be used with Cassandra 4.0 (which uses protocol v5)
- Zero-copy deserialization
- Zero/one allocation per-request in normal case (especially thanks to https://github.com/wyfo/swap-buffer-queue)
- Node distance support (so datacenter-aware and whitelist/blacklist possibility)
- Query plan caching (taking distance in account)
- Optimized execution path to improve memory locality
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
- benchmark
- prepare for tablet partitioning?

## Performance

I'm not able to measure performance right now (I have to finish connection pool worker), but assuming what is written in [previous section](#main-differences-with-the-official-driver), especially the zero/one allocation promise, I expect this implementation to be a lot faster.

However, the performance improvement regarding zero-copy deserialization has been quickly measured in https://github.com/scylladb/scylla-rust-driver/issues/571, and this point alone is already quite promising.

## What's next

I've made this project without telling Scylla developers, so, obviously I will have to talk with them.

Actually, I've a second secret project about Scylla and Rust, that I've paused when I started working on this driver. So stay tuned! ;) 