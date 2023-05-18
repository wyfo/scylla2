# scylla2

This is a POC (with more than 10kloc, maybe not a POC anymore) of a new implementation of the Scylla Rust driver, focusing on performance and memory consumption, but not only.

I've started the development at the end of October 2022, mostly on my spare times, so the development is quite messy (I often work during night when kids sleep), and there are still some things to do (hopefully, mostly tests at this time).

## Why did I write a new driver from scratch instead of contributing to the existing one?

Actually, I'm already a contributor, you may especially check https://github.com/scylladb/scylla-rust-driver/issues/571.
Sometimes, changes require so much deep modifications that I couldn't think of doing them properly. The linked issue is an example. I had so many things in my head that I've found easier to express them from scratch, at first as a POC, but the motivation grew while I was moving away from the current implementation, so here I am.

My first issue was about unnecessary allocations and zero-copy deserialization (see the linked issue), as I manipulate a lot of strings, sometimes big, at work.

Also, as I was facing some memory issues in my applications, where I did suspect the driver, I went deep into the code and spotted indeed a lot of allocations, not necessarily related to my issue, but I love optimization challenges, and a lot of ideas came to my head. The most important was what has become https://github.com/wyfo/swap-buffer-queue.

This accumulation of ideas (which I simply enjoy coding on my spare time), with a lot of them diverging from the current driver implementation, end by giving birth to a complete driver implementation.

## Main differences with the official driver

- Better [performances](#performance), CPU utilisation and memory consumption
- Builtin support of multiple native CQL protocol versions, can be used with Cassandra 4.0 (which uses protocol v5)
- Zero-copy deserialization
- Zero/one allocation per request in normal case (especially thanks to https://github.com/wyfo/swap-buffer-queue)
- Node distance support (so datacenter-aware and whitelist/blacklist possibility)
- Query plan caching (taking distance in account)
- Optimized execution path to improve memory locality
- Easier interface with only one method `Session::execute` instead of `Session::query`/`Session::execute`/`Session::batch`, and generics batches parametrized by their number of statement
- Access to database events and session events (connection opening/failure, node status update, topology update, schema agreement, etc.)
- No schema metadata (but it can be built as a sidecar service, using dababase schema event and session schema agreement event to trigger refresh)
- No internal API used for query execution, which means that custom execution can be built without too much effort

## TODO

- tracing
- test, tests, and more tests
- examples
- DOCUMENTATION -_-'
- benchmark
- prepare for serverless/tablet partitioning?
- ...

## Performance

I've started some benchmark using https://github.com/pkolaczk/latte, and as expected, this driver performs better than the official one.

Single threaded comparison with local database shows 30-40% performance improvement for example. The driver also keeps its at-most-one-allocation promise, so memory consumption is obviously lower by a lot.

Fun fact, when benchmark times are similar, mostly because of the database response time, this driver use 30-40% less CPU time.

## Unsafe

Most of the original unsafe code has been moved into [swap-buffer-queue](https://github.com/wyfo/swap-buffer-queue). `StreamNode` also has been refactored to replace its original unsafe lock-free algorithm with a simple Mutex, as there should be no contention in normal case.

However, [`Write::write_all_vectored`](https://doc.rust-lang.org/std/io/trait.Write.html#method.write_all_vectored) being currently unstable (and thus `tokio::io::AsyncWriteExt::write_all_vectored` too), it's implemented directly in the driver, but it requires an `unsafe` section because [`IoSlice::advance`](hhttps://doc.rust-lang.org/std/io/struct.IoSlice.html#method.advance) is also unstable for the moment. In order to keep the code safe, waiting for the stabilization, this implementation is hidden behind a feature flag, and replaced by a simple loop by default.

(`Write::write_all_vectored` is only used for big payloads which do not fit in the write buffer and are thus allocated)

## What's next

I've made this project without telling Scylla developers, so, obviously I will have to talk with them.

Actually, I've a second secret project about Scylla and Rust, that I've paused when I started working on this driver. So stay tuned! ;) 
