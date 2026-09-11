Memory cache comparison
=======================

This standalone benchmark compares `moat-cache-memory` with `foyer-memory` at
commit [`dd46245c45071d1036331e4e2c48e15386017b96`](https://github.com/foyer-rs/foyer/tree/dd46245c45071d1036331e4e2c48e15386017b96).
The separate workspace keeps foyer and its dependencies out of moat's production
dependency graph. The lockfile pins the comparison's transitive dependencies.

Reproduce from the repository root:

```sh
CARGO_TARGET_DIR=target/cache-memory-compare cargo build --release --locked --manifest-path benchmarks/cache-memory/Cargo.toml
MOAT_CACHE_BENCH_OPS=200000 taskset -c 0-3 target/cache-memory-compare/release/moat-cache-memory-compare > comparison.csv
```

`taskset` is optional and Linux-specific; choose available physical cores on
your machine. Build before measuring and avoid simultaneous compiler or other
CPU-intensive work. The default is 1,000,000 operations per worker; the recorded
run uses 200,000. Clippy and formatting for this standalone workspace are explicit:

```sh
CARGO_TARGET_DIR=target/cache-memory-compare cargo clippy --locked --manifest-path benchmarks/cache-memory/Cargo.toml --all-targets -- -D warnings
cargo +nightly fmt --manifest-path benchmarks/cache-memory/Cargo.toml -- --config-path rustfmt.nightly.toml
```

Workload
--------

Both implementations use 16 shards, the same deterministic standard-library
hasher, equal weighted capacity, 256 resident keys, borrowed byte-slice queries,
and ordinary shared handles released after each lookup. Capacity has sufficient
headroom to avoid shard imbalance causing misses. Keys/values are respectively
16/128, 256/4096 and 4096/65536 bytes. A hit reads the first value byte, not the
entire value. Values are not cloned on lookup.

Each case measures five samples after per-worker warmup and reports minimum,
median and maximum wall time divided by total successful hits. One and four
worker cases use the same deterministic request sequence, offset per worker.
The four-worker number is reciprocal aggregate throughput, not per-request
latency. Timing starts before workers are released and ends at their completion
barrier; startup/warmup are excluded. Pair order alternates across policies.

Each implementation uses its default configuration for the named policy.
Policy tuning and resident metadata are not identical. Moat's internal counters
remain enabled; foyer uses its default no-op metrics registry. Both perform
their usual reference counting and replacement-policy updates. There is no
origin loader, disk access, capacity eviction, or insertion in the measured loop.
This benchmark measures resident hit cost and does not compare admission quality,
eviction throughput, RSS, tail latency, or end-to-end storage performance.

Recorded run
------------

[Full CSV](results/2026-09-09-9950x.csv): AMD Ryzen 9 9950X, Linux x86-64,
`rustc 1.98.0 (88d9e12ae 2026-08-18)`, release profile with debug information,
CPU affinity restricted to physical cores 0-3. This is one development-machine
run, not a cross-machine performance guarantee. Five samples describe within-run
variation and do not establish statistical confidence across independent runs.

For 16-byte keys and 128-byte values, median nanoseconds per completed hit:

| Policy | Moat, 1 worker | Foyer, 1 worker | Moat, 4 workers | Foyer, 4 workers |
| --- | ---: | ---: | ---: | ---: |
| fifo | 36.21 | 57.37 | 17.11 | 65.01 |
| lru | 71.13 | 79.87 | 85.48 | 78.53 |
| tiny_lfu | 47.57 | 90.31 | 31.07 | 70.91 |
| s3fifo | 40.23 | 61.17 | 23.15 | 66.96 |
| sieve | 36.43 | 57.43 | 24.71 | 68.95 |

Across all 30 matched cases, the ratio of moat's median cost to foyer's ranges
from 0.26 to 1.09. The four-worker, 16-byte-key LRU case is approximately 9%
slower for moat in this run. Large keys spend more time hashing and reduce the
relative difference. Inspect every case and its min/max range in the CSV before
using these numbers for a workload decision.

Hybrid path exercise
--------------------

The production workspace also includes:

```sh
MOAT_HYBRID_BENCH_OPS=10000 cargo bench -p moat-cache --bench hybrid
```

It uses the same three key/value sizes and an actual temporary file, with
`io_uring` on Linux. A memory admission filter fixes the request mix at 50%
resident hits, 40% disk hits and 10% misses. Counter assertions verify that disk
promotion does not silently turn the exercise into an all-memory benchmark.
The file uses buffered I/O and a small warm working set. Results describe API,
codec and adapter overhead with one outstanding lookup; they are not NVMe
throughput, cold-media latency, or a comparison with foyer-storage.

The [recorded hybrid run](results/2026-09-09-hybrid.csv) contains 10,000
operations per size, with exactly 5,000 memory hits, 4,000 physical disk reads
and 1,000 misses in every case. It was run after the resident comparison,
without concurrent builds, using the same compiler and machine but unrestricted
CPU affinity. Single-run mean times include lookup validation; no percentile
or statistical comparison is implied.
