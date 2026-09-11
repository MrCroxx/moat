# Resident cache

`moat-cache-memory` implements the resident part of the cache independently of engines, I/O queues, codecs, and async runtimes. It does not depend on or reuse foyer-storage. The hybrid cache can use its synchronous `get` as the `get_memory` path and populate it after disk reads.

## Ownership and identity

Each version has one immutable, reference-counted record holding the owned key, value, properties, cached process hash, and weight. `Entry` handles share the record without requiring `Clone` on application types. Replacement, invalidation, and eviction remove residency; existing handles continue to expose their original version. Destroying the cache also clears residency before releasing its references.

Lookups use `Hash + Equivalent<K>`. A `String` accepts `str`, a `Vec<u8>` accepts `[u8]`, and applications can define equivalent composite queries. Equivalent owned and borrowed representations must hash identically. The builder accepts a configurable `BuildHasher`, defaulting to randomized `RandomState`. The process hash selects the shard and candidate buckets; exact equality decides identity. Neither serialization nor a maximum key size is imposed by the memory layer. `Cache::probe` retains the query hash for repeated checks and a matching `Probe::prepare`; it holds no lock between calls and compares the complete owned key before preparation.

## Shards and accounting

Each shard has a `HashTable` mapping to a slab of records and policy links. List links are slab indices; this implementation introduces no unsafe pointer manipulation. Capacity is divided exactly across the configured shard count. An entry must fit its own shard; capacity in another shard is not borrowed.

Weighers run once, before acquiring the shard lock. Applications can charge key bytes, value bytes, and their own overhead estimate. A zero result is charged one unit to bound entry count. `resident_weight` reports current residency; `allocated_weight` includes detached versions still held by callers. These are weight units, not allocator/RSS measurements. Policy metadata, hash-table capacity, and slab allocation are additional overhead. Shrinking the resident budget does not promise to return all previously allocated metadata to the allocator.

Hit/miss counters are per shard so independent hot shards do not update one global hit counter. Insertion, rejection and allocated-weight counters are also partitioned by shard; snapshots sum them across concurrent operations. Resident capacity accounting remains exact under the shard locks. Snapshots are approximate across concurrent operations.

## Replacement policies

| Policy | Hit path | Selection and lifetime behavior |
| --- | --- | --- |
| FIFO | Shared lock; no policy mutation | Evicts the oldest insertion. |
| LRU | Exclusive lock | Pins externally held versions. Last release returns a version to its normal or high-priority MRU list. Excess high-priority weight flows to the normal list. |
| Windowed TinyLFU | Exclusive lock | Uses an admission window, probation and protection queues, and a bounded decaying count-min frequency sketch. |
| S3FIFO | Shared lock; atomic saturating counter | Uses a small queue, a main queue, and fingerprint-only ghost history bounded by both count and weight. |
| SIEVE | Shared lock; atomic visited bit | An eviction hand sweeps insertion order and clears visited bits before selecting a victim. |

LRU cannot evict caller-pinned versions for capacity pressure. An insertion that cannot fit around existing pins is rejected as a resident entry, but still returns a valid shared handle. Reducing capacity below pinned weight temporarily exceeds the new target; last release triggers trimming. Explicit replacement, removal, and clear can detach pinned entries immediately.

Other policies may evict entries whose handles remain live. This keeps residency bounded while preserving handle validity; applications retaining arbitrary numbers of handles still control that external memory consumption.

## Admission, notifications and concurrency

Admission filters, weighers, removal listeners, and destruction of removed application values execute outside shard locks. Filters reject residency, not insertion ownership: callers receive an `Entry` regardless. A rejected or overweight replacement removes the previous resident version, preventing an old value from surviving a rejected update.

Removal notifications carry the exact immutable version and a reason. Concurrent notifications may interleave; the key may already have a newer resident version when a listener runs. Listeners may reenter the cache. Dropping the cache itself does not invoke removal listeners.

Hashing and equality are part of lookup synchronization and must not reenter the same cache. `clear` visits shards sequentially; concurrent insertions into already-cleared shards may survive. Hybrid-wide invalidation needs generation coordination above this API.

There is no source-loader `fetch`. Disk read coalescing belongs to `moat-cache-store`; logical generations and conditional fill tokens belong to `moat-cache`.

## Validation

Run `cargo test -p moat-cache-memory` for policy behavior, borrowed queries with deliberate hash collisions, admission and weight accounting, reentrant notifications/destructors, concurrent replacement and last release, and randomized mutation/resize coverage. `cargo run -p moat-cache-memory --example resident` demonstrates the public API.

Run `cargo bench -p moat-cache-memory --bench memory` for resident hits across policies, 16-byte to 4-KiB keys, 128-byte to 64-KiB values, and one/four threads. Set `MOAT_CACHE_BENCH_OPS` to control sample length. The locked-map baseline omits eviction and statistics; it is a primitive reference, not evidence of parity with foyer-memory. A separate [pinned foyer-memory comparison](../../benchmarks/cache-memory/README.md) provides matched workloads, raw results and limitations without adding foyer to production dependencies.
