Hybrid cache
============

`moat-cache` composes `moat-cache-memory` and `moat-cache-store` directly over
moat engines. It does not depend on foyer-storage, reuse its disk format, or
introduce cache replacement policy into moat-engine.

API and ownership
-----------------

`Cache` accepts shared `Bytes` keys, values and opaque properties and returns
owned `EntryView` handles. Application serialization stays outside the cache.
See [owned KV views](cache-views.md) for shared query keys, handle lifetime,
resident retention limits and the runnable example.

`Cache::new(memory_builder, store, options).await` consumes the only Store
handle. Construction rejects a shared handle without closing other owners.
After taking ownership it fences each disk, collects current live inventory,
and trims recovered entries to the configured limits. Previously admitted
adapter operations therefore cannot make a caller-supplied inventory stale.
The device set remains fixed for the cache lifetime.

- `get_memory` is synchronous and only enters the resident cache. It does not
  encode keys, allocate transient key state, or submit disk work.
- `get` checks memory and then disk, returning a shared entry, a miss, or an
  error. It never starts an origin request.
- `lookup` additionally returns a `FillToken` on a miss. The token retains
  bounded logical-key state; it neither chooses a loader nor shares origin work.
- `insert` and `insert_with` admit an unconditional replacement. They remove
  old memory residency at admission, complete the disk write, and then publish
  the new resident version if no newer mutation has superseded it.
- `populate` and `populate_with` consume a miss token and return `None` if it
  has become stale. Accepted population consumes the generation even if the
  subsequent write fails. Obtain a fresh token before retrying failed I/O.
- `invalidate` advances the logical generation even when the key is absent,
  removes memory residency, and explicitly deletes the matching disk key.
- `clear_memory` and `resize_memory` change resident capacity only. They do not
  invalidate logical keys or outstanding fill tokens.
- `flush` drains previously admitted mutations and their catalog updates,
  then flushes the store. It reports the first unobserved mutation failure
  since the previous flush, even if the caller dropped that mutation's reply.
- `close` stops admission immediately, drains accepted mutations, clears
  residency and closes the store. Shared entry handles remain readable.

Mutation, flush and close requests are admitted when their methods are called.
Dropping a returned `Request` abandons its reply, not the operation. Reads and
construction are ordinary lazy async operations. A dedicated completion thread
keeps admitted work alive independently of the caller's runtime. Completion
means the engine acknowledged the operation; durability follows the configured
engine contract and explicit flush. It is not an additional replication or
transaction guarantee.

The resident layer retains configurable hashing, borrowed lookup, shared entry
handles, weighted shards, FIFO/LRU/TinyLFU/S3FIFO/SIEVE, admission, application
properties, LRU priority and pins, resize, removal events and statistics. Memory
admission can reject an otherwise successful disk insertion; the caller still
receives a valid nonresident handle. Disk and memory replacement are independent.
Disk eviction does not revoke a still-valid memory value.

Identity and representation
---------------------------

The default disk ID is non-cryptographic XXH3-128 with the fixed seed
`u64::from_be_bytes(*b"moat-kv2")`. It hashes the concatenation of the 16-byte
namespace, little-endian 32-bit identity version, little-endian 64-bit key
length and canonical key bytes. The result is stored as 16 big-endian bytes.
The default identity version is 2; the former BLAKE3 identity used version 1.
Clear and rebuild existing caches before switching algorithms; changing the
version alone does not migrate or immediately reclaim old records.
XXH3 is a slot fingerprint, not an integrity check or a defense against
maliciously constructed collisions. It is independent of the resident hash builder. Applications choose
a stable key encoding before constructing `Bytes`; byte equality defines cache
key equality. Values and properties remain opaque bytes.

Each chunk has a 56-byte version-2 envelope followed by the complete key,
properties and value. The header records namespace, identity version, local
generation, priority and field lengths. It contains no header or payload checksum.
Lengths, flags, namespace and version are checked before interpreting field contents. The full
canonical key is compared before exposing a value. Reads return validated field
ranges directly, without deserializing application values.

Normal engine reads keep `verify_reads` disabled. The engine still generates
its existing per-block CRC32C on writes; recovery/reclaim validation and explicit
diagnostic read verification remain engine concerns. The cache performs no
additional integrity scan or CRC reuse. Version-1 envelopes are rejected; clear
and rebuild an existing cache before using this format.

Fingerprints identify a disk slot, not logical equality. A colliding write may
replace another key's disk copy; it cannot make that key return the replacement
value. Invalidating a key preserves a colliding slot that contains a different
full key. Supporting multiple colliding records in one slot is not required for
correct cache misses and is not implemented.

The complete envelope must fit `Engine::chunk_max`. Empty, short and long keys
use the same format. Oversize entries return `TooLarge` before changing resident
or disk versions. Multi-chunk object manifests are a future extension. Changing
fingerprint algorithms or encodings requires a new namespace or identity version; this does not migrate
old entries or reclaim them immediately.

`Bytes` can retain a shared slice of the adapter's read buffer without copying.
Its buffer credits remain charged while the resident value or any external
slice retains that buffer, including after close. Call `view.value().to_vec()`
for an independent heap copy; the original owners must still be released before
the disk buffer credits return. The public
statistics distinguish resident weights, held adapter bytes and pending work;
they do not estimate process RSS.

Concurrent operations
---------------------

The adapter coalesces compatible reads within the same physical mutation
interval. The hybrid completion coordinator orders mutations per ChunkId,
including colliding logical keys, while unrelated IDs progress independently.
Global fences drain all earlier keyed work before running their store barrier.

Logical-key state is transient and separate from the disk catalog. Its explicit
lease count keeps a generation alive while reads, mutations or fill tokens can
still reference it. Equal canonical keys share one key allocation. The final
lease removes the state and releases its budget; there is no permanent full-key
directory for disk entries. The envelope's generation is local bookkeeping;
the engine LSN identifies persistent versions across state retirement/restart.

Disk promotion prepares field views, admission and weight outside coordination
locks. It then checks the logical generation, pending mutations, close state
and current disk LSN before structurally publishing memory residency. Removal
notifications and retired application values are released after unlocking.
An invalidation or overwrite while a disk read is preparing promotion therefore prevents
that read from publishing its old version. Successful population similarly
advances the generation under the admission lock before publishing its job.

Callbacks may reenter synchronous cache operations. They must not synchronously
wait for work that requires the same completion thread. Callbacks
should not panic; the coordinator contains unwinding to an individual job and
releases its reservations, but a lost reply reports `Closed` rather than a
typed application panic. Hashing and equality have the resident layer's stricter
non-reentrancy requirements because they can execute inside structural locks.

Capacity and reclamation
------------------------

Each disk has a bounded catalog containing ChunkId, LSN, encoded length and
policy state, with no complete logical keys or values. A hash map indexes slots
and a B-tree orders them; their allocator overhead is additional to the encoded
byte budget. Warm recovery reconstructs insertion order from per-disk LSNs.

FIFO evicts the oldest completed insertion or overwrite. SIEVE gives disk hits
a second chance while sweeping insertion order; resident-only hits do not
update disk policy. Victims are conditionally deleted using their catalog LSN.
Active writes are excluded from victim selection.

Limits cover encoded live bytes, live entry count, physical live record bytes,
and reserved space for concurrent batch allocation. The default encoded budget
is one quarter of space remaining after segment reserves; explicit budgets may
use at most half. At least four segments are reserved for deletion, packing
and GC. The entry budget cannot exceed the engine index's conservative limit
for avoiding table growth. Physical costs come from the engine's actual record
geometry and packing configuration.

A per-disk async controller admits writes and coordinates eviction/GC. It releases
its lock before awaiting an admitted write, allowing unrelated writes to remain
in flight. Reservations survive cancellation and are released after catalog
commit or failure. The controller waits for outstanding reservations when they
can free admission capacity. When segment headroom is low it requests storage
GC; GC preserves live chunks and never chooses a cache victim. The current
adapter executes GC as a disk barrier. Conservative reservations can return
`NoSpace` before all nominal encoded capacity is usable.

Transient limits cover admitted background operation count, encoded payload
bytes, logical-key leases and unique canonical-key bytes. Exhaustion returns
`Busy`; callers choose retry or backpressure. These are retained-work budgets,
not limits on allocations made by the application outside the cache. Close has one reserved control path and can
be admitted even when mutation budgets are exhausted.

A mutation reply can become ready just before its coordinator permit is
destroyed. With very small operation budgets, immediate subsequent admission
can transiently return `Busy`; reply completion is not a reserved slot for the
caller's next request.

Validation and usage
--------------------

Run `cargo test -p moat-cache -p moat-cache-memory -p moat-cache-store` and
`cargo run -p moat-cache --example hybrid`.

Hybrid tests cover byte keys/properties, warm recovery, empty through
near-limit keys, oversize rejection, forced fingerprint collisions, absent-key
invalidation, competing fill tokens, token budgets and instance identity,
cancelled mutation replies, shared-store rejection, zero-copy buffer lifetime,
FIFO recovery trimming, deterministic stale-promotion races, mixed-size capacity
churn, failed write/delete retry, and real io_uring multi-disk recovery after
device reordering. Internal tests exercise malformed envelope headers, truncation
and overflow, shared key allocations, concurrent last-lease release, and keyed
job ordering across a global barrier. Engine tests compare write accounting
against actual live-byte changes across packing thresholds.

Additional shutdown coverage saturates mutation admission, cancels the close
reply, and observes completed drain/seal before reopening. A disk SIEVE test
checks that a disk hit changes victim selection independently of memory policy.

A [pinned foyer-memory comparison](../../benchmarks/cache-memory/README.md)
records resident hit costs across all five policies. `cargo bench -p moat-cache
--bench hybrid` exercises a fixed memory/disk/miss mixture on temporary files;
its counters verify the actual path mix. These bounded workloads do not establish
universal performance parity. The engine's existing durability and recovery
limitations remain those documented in `chunkserver-audit.md`.
