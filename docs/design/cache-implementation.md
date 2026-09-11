Cache architecture and implementation status
============================================

The cache is built directly on moat chunk engines. Its three crates separate
resident policy, physical I/O coordination and logical KV correctness:

| Crate | Responsibility | Main contracts |
| --- | --- | --- |
| `moat-cache-memory` | Generic resident cache | Borrowed-key lookup, weighted shards, shared immutable entries, FIFO/LRU/Windowed TinyLFU/S3FIFO/SIEVE, admission, properties, pins, events and statistics. |
| `moat-cache-store` | Asynchronous chunk adapter | One worker per disk, bounded requests and buffer ownership, per-ID ordering, compatible read coalescing, conditional deletion, inventory, flush and close. |
| `moat-cache` | Bytes-based hybrid KV cache | XXH3-128 identity, complete-key envelopes, generation-safe promotion and optional fill tokens, disk capacity/replacement, owned views and warm recovery. |

The chunk engine remains independent of cache replacement policy and TTL.
No foyer-storage code or disk layout is used. Pinned foyer dependencies exist
only in the standalone comparison workspaces.

Request and ownership boundaries
--------------------------------

- `get_memory` performs a synchronous borrowed-key resident lookup.
- `get` checks memory and disk without invoking an origin loader.
- `lookup` may return a `FillToken`; `populate` installs a value only while its
  logical generation remains current. Callers own origin loading, singleflight,
  batching, retries, timeouts and negative caching.
- The store coalesces adjacent reads with the same ID and identical range within
  a mutation interval. Logical key equality and stale-promotion checks remain
  in the hybrid cache.
- KV keys, values and properties use `Bytes`. Disk hits return `EntryView`
  handles sharing the completed read buffer without application deserialization.
  Application encoding and any required copies occur at the boundary.
- Per-disk request admission and global atomic credits bound outstanding work.
  Compatible readers share one buffer allocation; external ownership keeps its
  credits until the last owner drops. Resident retention leaves read headroom.
- Optional completion drivers batch replies on the existing application
  executor. Direct delivery keeps the adapter independent of a specific runtime.

Engine prerequisites
---------------------

Recovery rejects foreign or unreadable segment headers and incomplete sealed
segments. Mutations become reader-visible only after successful completion;
unacknowledged duplicate puts return `Busy`. Completed inventory and conservative
write-cost accounting support upper-layer capacity admission. Node construction
rejects duplicate persistent disk identities.

Current limits and follow-up work
---------------------------------

Each complete entry, including its full key and envelope, must fit one chunk.
The cache exclusively owns its engines. Physical reclamation currently runs as
a disk barrier. External views can exhaust read credits; dropping or copying
those views is the caller's responsibility. Statistics are not process RSS.
Normal engine reads skip CRC verification; write CRC and recovery/reclaim
validation remain unchanged. The cache envelope adds no checksum.

The default identity version is 2 with XXH3-128. Older BLAKE3 identities and
version-1 envelopes require a cache rebuild. Full-key comparison prevents a
fingerprint collision from returning or deleting another key's value; colliding
writes can replace a disk slot and cause a miss.

Known performance work remains: single-disk small-entry reads trail foyer in
some measured configurations, small-entry prefill has high write amplification,
and the 64-KiB single-disk workload has intermittent slow samples. These are
explicit investigation items, not resolved by the identity-hash change. The
published matrix covers disk hits, not sustained mixed writes or GC pressure.

Contracts and verification
---------------------------

- [Resident memory](cache-memory.md)
- [Asynchronous store](cache-store.md)
- [Hybrid cache](cache-hybrid.md)
- [Owned KV views and retention](cache-views.md)
- [Memory comparison](../../benchmarks/cache-memory/README.md)
- [Disk comparison and limitations](../../benchmarks/cache-disk/README.md)

Run `cargo test --workspace` for policy, collision, mutation ordering,
cancellation, recovery, buffer retention, invalidation and stale-fill coverage.
Runnable examples accompany all three crates. Standalone benchmarks keep their
own lockfiles and checks so comparison dependencies do not enter production.
