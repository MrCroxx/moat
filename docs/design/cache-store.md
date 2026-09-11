# Asynchronous chunk adapter

`moat-cache-store` turns moat engine pipelines into runtime-independent request futures. It owns one worker, queue, reader, and unique writer per disk. The adapter does not define logical cache keys, origin loading, admission/eviction policy, TTL, or a second physical storage format.

## Admission and ordering

`get`, `put`, `delete`, `inventory`, and `reclaim` admit a request when called, before its future is polled. The returned `Request` only waits for the reply. Dropping that future does not undo an accepted mutation. Request-count pressure or queued-write byte pressure returns `Busy` without admitting the operation. Admitted reads wait for buffer credit at dispatch.

Each ChunkId has an active operation and a FIFO of later operations. Compatible adjacent reads coalesce into the same operation, including followers arriving while a physical read is in flight. Compatibility currently requires exactly equal range arguments. An intervening write, delete, different range, or disk barrier prevents joining the earlier read. Read results include the completed record LSN captured when the physical read was submitted.

Different IDs on one disk have independent operation chains and may be in flight together. A bounded worker intake window preserves small-write batching while preventing a continuously active producer from starving queue progress. The worker drives nonblocking engine/queue polling on io_uring. The synchronous backend performs blocking device calls and is intended for development.

Default idle waits avoid spinning on an idle worker; zero `idle_wait` enables busy polling. CPU affinity is optional and explicit.

## Buffer ownership and backpressure

Global admission bounds request count and charged bytes. Writes charge their retained input length. Read followers retain their own request slots and share the leader's buffer credit. On completion, the leader's reservation shrinks to the actual pool buffer class. All waiters share an `Arc<Chunk>` around that one buffer, without copying the value.

The chunk retains its byte credit until the last caller drops it. A cancelled waiter cannot cancel another caller's read. When every receiver is cancelled, completion drops the unobserved buffer and returns the credit. Retained read results remain valid after the store closes.

Pending and retained reads are additionally limited to half of each disk's configured pool. The pool must contain at least sixteen maximum-size buffers; the remainder is available for write staging and physical GC. Write I/O buffers are bounded separately by the fixed pool allocation. Inventory snapshots are management allocations proportional to the engine's bounded live index, and are not covered by request payload credits.

Queued reads consume a request slot without reserving buffer bytes. The worker
reserves one maximum allocation class when starting a physical read, waiting
for credit when necessary. Completion shrinks that reservation to the actual
buffer class. Cancelled queued reads release their request slots even if other
callers retain buffer credit needed for dispatch. Admission and budget counters use
per-disk read locking and exact global atomic reservations. Flush/close acquire
all disk admission gates before publishing any fence, preserving one exclusive
admission boundary without making ordinary disk requests share one gate.

`Chunk::try_reserve_retention` lets an upper layer reserve long-lived ownership
while leaving one maximum-class read allocation of credit globally and per
disk. Shared owners charge the actual buffer allocation once; the reservation
ends with the final chunk owner. Failure leaves the returned chunk valid.
This is physical buffer accounting: the adapter does not choose resident
entries or evict cache victims. `Cache` uses the reservation to gate memory
promotion; see [owned KV views](cache-views.md) for its admission and lifetime
contract. Unreserved external views can still exhaust normal read credits.

## Optional completion delivery on an application executor

By default, disk workers send replies directly and request futures need no
particular runtime. `Options::completion_executor` can instead schedule one
completion driver per disk on an application's existing executor. Completed
reads and management fences are delivered in FIFO batches. This amortizes
cross-thread scheduling: a disk worker wakes one driver for a batch, and the
driver wakes client tasks from within the application executor. It adds tasks,
not I/O or application threads. The executor remains responsible for driving
those tasks; synchronous waiting on its only worker can prevent progress.

Implement `CompletionExecutor::spawn` with the application's spawn operation.
For Tokio, wrap an existing `tokio::runtime::Handle` and call `handle.spawn(task)`.
The store has no Tokio dependency. `spawn` must schedule without waiting for the
task to finish. Drivers yield between batches to let ready clients run.

Pending delivery retains each read's request and buffer credits, so a stalled
executor applies the existing admission and byte bounds. Compatible waiters
still share one physical buffer. Flush/inventory/reclaim replies follow prior
read delivery; close also waits for this delivery before acknowledging shutdown.
A dropped driver drains accepted batches and switches future delivery to an
ordered producer-thread fallback. Cancelling a caller only discards its own
reply. Disk operation ordering and normal-read checksum policy are unchanged.

The current batching path covers physical read completions and management
replies. Metadata misses and normal mutation acknowledgements complete directly;
the published comparison exercises populated disk hits.

## Completion and management operations

`put` always requests an explicit overwrite. `delete(id, Some(lsn))` changes the slot only if its current completed LSN matches; otherwise it returns Changed or Missing. This enables upper-layer eviction to avoid deleting a newer slot incarnation. The adapter's ordering makes the check and deletion indivisible relative to its other operations on that ID.

`inventory(disk)` is a disk barrier returning only completed live IDs, LSNs, and encoded lengths. The initial `Store::new` result includes the same inventory before any adapter request is admitted. No physical location or decoded key is duplicated into the adapter's inventory format.

`flush` admits a barrier on every disk on first poll. The admission gates prevent new requests from interleaving with publication of those fences. All flush credits are reserved before any fence is published. The operation observes every disk and returns the first error. A worker also reports mutation failures since the previous flush; observing a flush consumes that recorded error. Engine completion and explicit flush retain the engine's configured durability semantics; they do not add a new power-loss guarantee.

`reclaim(disk)` currently runs one physical GC pass as a disk barrier. Later operations on that disk wait for the pass. This favors a simple completion contract; concurrent GC scheduling is a future performance improvement. GC retains live chunks. The upper cache must issue explicit deletions and manage free-segment headroom before requesting reclaim; this adapter does not choose cache victims.

`close` stops admission, drains prior work, seals, and detaches all workers. It observes all disks even if one fails. Cancelling close after admission still leaves the workers shutting down. A second close returns Closed. Dropping the final Store handle performs background draining and sealing; explicit close is required to observe the result. Partial startup failure joins earlier workers before returning so an immediate retry can reacquire their writers.

## Multiple disks

Placement reuses moat-server's weighted rendezvous hashing over persistent disk UUIDs. Duplicate UUIDs are rejected before workers start. Reordering the same disk set preserves physical placement. Adding/removing disks requires explicit migration or rebuilding the cache; opening another list does not migrate existing chunks. The caller opens engines with the desired synchronization settings and gives the adapter exclusive ownership of their writers. Normal reads keep engine `verify_reads` disabled; write checksums and recovery/reclaim validation remain unchanged. Explicit diagnostic verification can still be enabled by the caller.

## Validation

Run `cargo test -p moat-cache-store` and `cargo run -p moat-cache-store --example chunks`.

Tests include deterministic in-flight read overlap/coalescing, cancellation of a group's first waiter, read/write/delete ordering, exact-range coalescing, count and retained-byte backpressure, mutation cancellation followed by flush, conditional deletion, inventory barriers, failed-delete retry, independent disk progress, reordered disk recovery, partial startup cleanup, explicit GC, and the io_uring file backend on Linux.
