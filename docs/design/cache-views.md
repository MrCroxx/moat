Owned KV views
==============

`Cache` exposes a bytes KV API over the hybrid generation checks, catalog,
resident policies and engine adapter. A disk hit returns an owned `EntryView`:
key, value and opaque properties share the adapter's completed read buffer.
The read path copies no KV fields and performs no application deserialization.
It still validates envelope structure, the full key, generation and disk version.

Applications encode and decode their own types at the cache boundary.
The version-2 disk envelope has a 56-byte header. Normal engine reads skip
checksum verification; write CRC and recovery/reclaim validation remain engine
concerns. Write encoding and engine log packing still copy bytes.

The default persistent identity is XXH3-128, version 2. Changing the fingerprint
algorithm or encoding requires a new identity version and a cache rebuild;
the envelope version alone does not make records from a different identity
scheme discoverable. `moat-cache-memory` remains generic.

Ownership and lifetime
----------------------

| Operation | Ownership and cost |
| --- | --- |
| `view.key()`, `value()`, `properties()` | Borrowed slices valid for the borrow of the view; no copy. |
| `view.clone()` | Shares the same immutable entry version; no KV copy. |
| `view.key_view()`, `value_view()`, `properties_view()` | Independent owned `Bytes` sharing the field's backing storage. |
| `bytes.slice(range)` | Bounds-checked shared subrange, retaining the backing buffer. |
| `view.value().to_vec()` | Explicit copy into application-owned heap memory. |
| Drop the last owner of a disk buffer | Returns its pool allocation and adapter byte credits. |

Views remain readable after overwrite, invalidation, eviction, resize and
cache shutdown. They represent the version observed by the lookup and do not
change to follow newer writes. Owned handles can move across threads or async
tasks without keeping a borrow of the cache alive. The buffer pool's backing
storage outlives outstanding views, even after the worker closes its I/O queue.

An `EntryView` retains the resident entry handle, including the existing LRU
pin semantics. An independent field `Bytes` retains only its backing storage,
so it does not by itself pin that entry in the resident replacement policy.
Releasing an external view need not release the buffer immediately: the cache
or another field view may still own it. A tiny slice can retain an entire
aligned read buffer. Copy that slice explicitly when retaining the whole buffer
would cost more than copying the required bytes.

```rust
let key = moat_cache::Bytes::from(b"example".to_vec());
let view = cache.get(&key).await?.expect("cache hit");
let value = view.value_view();
drop(view);
cache.close().await?;
assert_eq!(value.as_ref(), b"shared value");
drop(value);
```

The complete [runnable example](../../core/moat-cache/examples/views.rs) also
moves the retained value to another thread after shutdown:

```sh
cargo run -p moat-cache --example views
```

Keys and origin loading
-----------------------

`get(&Bytes)`, `lookup(&Bytes)` and `invalidate(&Bytes)` accept an already shared query key. On a
disk lookup, the transient registry retains that key without copying or
re-encoding it. Equal active keys still share canonical state and retain the
existing key-byte and lease limits. Construct shared keys outside a hot lookup
loop when the application already owns reusable keys. Converting a `Vec<u8>`
into `Bytes` can allocate and copy; this conversion is not a zero-copy promise.
`get_memory(&[u8])` accepts an ordinary borrowed key without allocation or I/O.

`lookup` returns `Lookup::Hit(EntryView)` or `Lookup::Miss(FillToken)`.
The caller chooses how to load an origin value and uses `populate(token, bytes)`
to install it conditionally. Tokens preserve the same invalidation and
generation checks. The cache does not elect a loader or impose
origin-request deduplication. The adapter continues to coalesce compatible
physical disk reads independently.

`insert` accepts shared key/value bytes. `insert_with` also accepts opaque
properties and resident priority. Both return a shared view after the accepted
mutation completes. `populate` uses default properties and priority; `populate_with` accepts custom
properties and priority for conditional fills.

Resident retention and read progress
------------------------------------

Resident weight and physical read-buffer ownership are separate budgets.
`Cache` adds a retention admission check after the configured application
admission callback. Each physical `Chunk` reserves retention once, regardless
of how many fields or coalesced callers share it. Its actual aligned allocation
size is charged, not the logical value length.

The global retention limit is `Store::Options::max_bytes - pool.max_class`,
saturating at zero. Each disk's retention limit is `pool.bytes / 2 -
pool.max_class`, also saturating at zero. These are additional limits on the
existing read credits, not extra allocated memory. Resident ownership alone
therefore leaves credit for one maximum-class read globally and per disk.
This is a progress reserve, not a guarantee of full concurrency under pressure.

When retention admission fails, a successful lookup still returns a valid
nonresident view. Rejected read promotion skips the resident shard write lock
and does not remove an existing resident entry. Explicit replacement retains
its separate semantics: a rejected insert still replaces the previous version.

A retention reservation lasts until the physical buffer's final owner drops,
including external ownership after eviction. This is conservative: the cache
may reject promotion even when some reserved buffers are no longer resident.
Heap-backed bytes do not require read-buffer retention credit.

Retention rejection does not evict a resident victim solely to release read
credits. If the physical retention limit binds before the configured resident
weight limit, new disk entries remain nonresident until existing owners release
enough storage. Size the resident capacity and pool together for workloads that
need frequent promotion and turnover.

Applications can still consume the read budget by holding many nonresident
views, query-key slices or in-flight operations. Subsequent reads wait for
buffer credit; outstanding request limits can return `Busy`. No timeout or
eviction revokes user-owned views. Release handles, bound concurrent consumers,
or copy data when retaining it beyond the I/O buffer budget is necessary.
In particular, do not retain enough buffers to block a read and then wait for
that read before releasing them.

Retention admission is built into `Cache`.

Validation
-----------

The view tests cover shared field backing, resident identity, overwrite and
invalidation while an old view survives, shutdown with external ownership,
bounded retention with continued reads, stale fill tokens, forced identifier
collisions, long keys and warm recovery. Memory tests distinguish rejected
read promotion from rejected replacement; budget tests check global/per-disk
retention rollback. The disk comparison documents the different moat view and foyer Vec return
contracts.
