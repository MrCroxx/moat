# moat-engine: a non-blocking API over a shared I/O queue

> Status: implemented (`core/moat-engine`), with the worker runtime in
> `core/moat-server`. Describes the public API of `moat-engine` and the
> ownership model behind it, matching the worker topology of `chunkserver.md`
> §5.3 (one kind of worker, every worker reads every disk, one owner worker
> per disk holds its writer). The on-disk format (§3, §4 of `chunkserver.md`)
> is unchanged. §11 lists where the implementation departs from the original
> proposal.

## 1. Constraints

Two requirements drive everything below.

1. **The I/O queue is shared and supplied from outside.** A worker thread owns
   exactly one io_uring instance (one buffer pool, one registered fixed-file
   table, one `io_uring_enter` per loop iteration) and drives any number of
   engines through it. The engine never builds a ring or a pool of its own.
2. **No engine call blocks on I/O.** Every method either does memory work and
   returns, enqueues I/O and returns a ticket, or reports `Busy`. Completion
   is observed only through `poll`. A slow disk can therefore never stall the
   other disks that share its worker.

Everything else is chosen to make the surface as small as those two allow.

Why both are needed: without (2) a single `put` that happens to cross a
segment boundary waits for every in-flight batch of that disk (milliseconds),
and every disk sharing the thread waits with it. Without (1) a thread with *k*
disks pays up to *k* `io_uring_enter` calls per iteration, each carrying a
handful of SQEs, cannot wait on all rings at once, and holds *k* buffer pools
that cannot lend to each other.

## 2. Ownership model

```
worker thread
 ├── IoQueue                      one ring, one pool, one fixed-file table
 │    ├── Descriptor 0             fd slot + completion inbox
 │    ├── Descriptor 1
 │    └── Descriptor 2
 ├── Writer  of engine A ── Descriptor 0
 ├── Writer  of engine B ── Descriptor 1
 └── Reader  of engine C ── Descriptor 2     (C's Writer lives on another worker)
```

- The **queue** is owned by the worker and passed to every engine call as
  `&mut dyn IoQueue`. Nothing holds a reference to it across calls, so there
  is no `Rc<RefCell<_>>`, no interior mutability, and the borrow checker
  enforces the single-issuer rule that `IORING_SETUP_SINGLE_ISSUER` assumes.
- A **descriptor** is what an engine object gets when it attaches a device
  to the queue: one *open* of the device on this queue, exactly as a Unix file
  descriptor is one open of a file. It is a `Copy` handle that names both the
  registered file (for `IORING_OP_*_FIXED`) and the inbox its completions are
  routed to. One descriptor per *pipeline*, not per device: a `Writer` and a
  `Reader` of the same engine each hold their own, so each pipeline has a
  private token space and inbox and neither has to demultiplex the other's
  completions. (Per `chunkserver.md` §5.3 a disk's `Writer` lives on its owner
  worker while every worker holds a `Reader` for it, so on any one queue a
  device has at most one `Writer` and one `Reader`.) It is scoped to the
  queue and is not the kernel's file descriptor. Descriptors are the only
  piece of queue state a pipeline remembers.
- Buffers are **moved** into the queue at submission and handed back with the
  completion, exactly as today. Ownership never crosses a call boundary by
  reference.
- An engine object is bound to the queue it attached to. Moving a `Writer`
  to another worker means: seal, poll until idle, `detach`, then `attach`
  on the new worker's queue. That is a management-plane operation and is not
  optimised.

Recovery (`open`) still uses blocking `Device::read_at`/`write_at`. It runs
before the engine is attached to any queue, once per process start, and is
parallel across disks by running it on several threads; it is not on the data
path and gains nothing from the ring.

## 3. `IoQueue`

```rust
/// One open of a device on a queue: a fixed-file slot plus a completion inbox.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Descriptor(u32);

/// The queue has `depth` operations in flight; retry after `poll`.
#[derive(Clone, Copy, Debug)]
pub struct Full;

pub struct IoCompletion {
    pub token: u64,                 // as passed at submission
    pub result: io::Result<usize>,  // bytes transferred
    pub buf: Option<PooledBuf>,     // the buffer that was submitted (None for fsync)
}

pub trait IoQueue: Send {
    /// The pool every buffer passed to this queue must come from.
    fn pool(&self) -> &Arc<BufferPool>;

    /// Registers `device` and opens an inbox for it.
    fn attach(&mut self, device: &Arc<dyn Device>) -> io::Result<Descriptor>;
    /// Closes a descriptor. Completions still in flight for it are discarded on
    /// arrival (their buffers return to the pool).
    fn detach(&mut self, desc: Descriptor);

    /// Enqueue only: never blocks, never touches the device. Rejected — with
    /// the buffer handed back untouched — when `depth` operations are already
    /// in flight; the caller keeps the operation in its own ready queue and
    /// retries after `poll`. Nothing else can fail at enqueue time; device
    /// errors arrive in the completion.
    fn read(&mut self, desc: Descriptor, buf: PooledBuf, len: usize, offset: u64, token: u64) -> Result<(), PooledBuf>;
    fn write(&mut self, desc: Descriptor, buf: PooledBuf, len: usize, offset: u64, token: u64) -> Result<(), PooledBuf>;
    fn fsync(&mut self, desc: Descriptor, token: u64) -> Result<(), Full>;
    /// `depth() - in_flight()`: how many operations can be enqueued right now
    /// without being rejected. Exact, since the caller is the only issuer.
    fn vacant(&self) -> usize;

    /// Pushes enqueued operations to the device without waiting.
    fn submit(&mut self) -> io::Result<()>;
    /// Submits, then reaps every finished operation into its descriptor's inbox.
    /// With `wait` set and anything in flight, blocks until at least one
    /// completes. Returns the number reaped. This is the worker's single
    /// blocking point.
    fn poll(&mut self, wait: bool) -> io::Result<usize>;
    /// Moves the inbox of `desc` into `out`. Returns the number moved.
    fn take(&mut self, desc: Descriptor, out: &mut Vec<IoCompletion>) -> usize;

    fn in_flight(&self) -> usize;
    fn depth(&self) -> usize;
}
```

Two implementations, as today:

- `UringQueue::new(opts: &QueueOptions)` — no file argument any more.
  `attach` calls `IORING_REGISTER_FILES_UPDATE` with the fd from
  `Device::fd()` and returns its slot index as the descriptor. The pool's arenas are
  registered as fixed buffers once, at construction.
- `SyncQueue::new(opts, order)` — `attach` keeps the `Arc<dyn Device>` and
  performs each operation immediately with `Device::read_at`/`write_at`,
  deferring the completion into the descriptor's inbox. The `BlockingIo` trait is
  removed; `Device` is the blocking interface.

`Device` loses `open_queue` and gains `fn fd(&self) -> Option<BorrowedFd<'_>>`
(`None` for `MemDevice`, which therefore only works with `SyncQueue`).

Today's `read`/`write` take the buffer by value and return
`io::ErrorKind::WouldBlock` when the ring is full — which drops the buffer the
caller just filled. Handing it back makes rejection lossless; `vacant` lets a
pipeline push exactly as many ready operations as fit and never see a
rejection in the common case.

Routing is by descriptor, not by token: the queue keeps one `Vec<IoCompletion>`
per descriptor and `take` swaps it out. Tokens stay private to the object that
submitted them. The worker never sees a raw completion.

## 4. Engine lifecycle

```rust
pub fn format(device: &dyn Device, opts: &FormatOptions) -> Result<()>;          // unchanged

/// Blocking recovery. Rebuilds the index; touches no queue. `Send`, so disks
/// recover in parallel on any threads.
pub fn open(device: Arc<dyn Device>, opts: Options) -> Result<(Engine, RecoveryReport)>;

/// The per-disk state every pipeline shares: index, segment table,
/// superblock, options. `Clone + Send + Sync`; does no I/O itself.
impl Engine {
    pub fn stat(&self, id: &ChunkId) -> Option<ChunkStat>;
    pub fn contains(&self, id: &ChunkId) -> bool;
    pub fn usage(&self) -> Usage;
    pub fn segment_size(&self) -> u64;
    pub fn chunk_max(&self) -> u32;

    /// The disk's single write pipeline, attached to `q`. Fails with
    /// `InvalidOption` if `q`'s pool cannot hold the largest buffer the
    /// writer will request, and with `Busy` if a writer already exists.
    pub fn writer(&self, q: &mut dyn IoQueue) -> Result<Writer>;
    /// A read pipeline for the calling thread, attached to `q`. Any number
    /// may exist.
    pub fn reader(&self, q: &mut dyn IoQueue) -> Result<Reader>;
}
```

Three names, three roles: an `Engine` is a disk; a `Writer` or a `Reader` is a
pipeline of that disk bound to one queue. Writers and readers are symmetric
in shape — `attach` on creation, `poll(q, out)`, `in_flight()`, `detach(q)` —
and differ only in what they submit. Today's `Opened`, `Reader` (a metadata
handle) and `ReadRing` collapse into `Engine` and `Reader` respectively; the
"ring" suffix leaked io_uring into a name that also covers `SyncQueue`.

`Options` loses `queue: QueueOptions` and `writer_pool_capacity_multiplier`.
Pool sizing is the worker's business; `attach` only checks
`pool.max_class() >= max(large_batch_len(chunk_max), batch_limit, scan_window)`.

## 5. `Writer`

All methods take the queue first, never block, and are the only path that
mutates the index, the segment table or the log tails of the disk.

```rust
pub type Ticket = u64;

impl Writer {
    // -- data ---------------------------------------------------------------

    /// Small value (< pack_threshold): copied into the pending batch.
    pub fn put(&mut self, q: &mut dyn IoQueue, id: ChunkId, value: &[u8], opts: PutOptions)
        -> Result<PutOutcome>;

    /// Large value: obtain a pool buffer to fill in place (zero copy), then
    /// hand it back with `put_large`.
    pub fn prepare_large(&mut self, q: &mut dyn IoQueue, value_len: u32) -> Result<LargeValue>;
    pub fn put_large(&mut self, q: &mut dyn IoQueue, id: ChunkId, value: LargeValue,
                     checksums: Option<&[u32]>, opts: PutOptions) -> Result<PutOutcome>;

    /// Appends a tombstone. `Missing` if the id is neither indexed nor
    /// pending; the index entry disappears when the tombstone is applied.
    pub fn delete(&mut self, q: &mut dyn IoQueue, id: &ChunkId) -> Result<DeleteOutcome>;

    // -- control ------------------------------------------------------------

    /// Barrier: completes once every write accepted before it is durable
    /// (fsync included when `sync_on_flush`).
    pub fn flush(&mut self, q: &mut dyn IoQueue) -> Result<Ticket>;
    /// `flush`, then seal both active segments so the next open needs no scan.
    pub fn seal(&mut self, q: &mut dyn IoQueue) -> Result<Ticket>;
    /// Starts one reclaim pass. `None` if there is no sealed segment.
    /// `Busy` while a previous pass is still running.
    pub fn reclaim(&mut self, q: &mut dyn IoQueue, policy: ReclaimPolicy) -> Result<Option<Ticket>>;

    // -- progress -----------------------------------------------------------

    /// Applies completions that arrived for this writer's descriptor, advances the
    /// seal / barrier / reclaim state machines, submits whatever became ready
    /// (including any partially filled pending batch), and appends the
    /// resulting completions to `out`. Never waits.
    pub fn poll(&mut self, q: &mut dyn IoQueue, out: &mut Vec<Completion>) -> Result<usize>;

    pub fn in_flight(&self) -> usize;
    pub fn free_segments(&self) -> u32;
    pub fn next_lsn(&self) -> Lsn;
    pub fn pick_victim(&self, policy: ReclaimPolicy) -> Option<u32>;

    /// Closes the descriptor. Call after `seal` has completed and `in_flight()`
    /// is zero; otherwise in-flight batches are abandoned (recovery handles
    /// that, but the segment is scanned on next open).
    pub fn detach(self, q: &mut dyn IoQueue);
}

pub enum PutOutcome    { Written { ticket: Ticket, lsn: Lsn }, Exists }
pub enum DeleteOutcome { Deleted { ticket: Ticket, lsn: Lsn }, Missing }

pub struct Completion { pub ticket: Ticket, pub result: Result<Outcome> }
pub enum Outcome { Put, Delete, Flush, Seal, Reclaim(ReclaimReport) }
```

Removed compared with today: `submit` (the worker calls `IoQueue::submit`),
`poll(wait)` (waiting is the queue's job), the blocking `flush`, `seal_active`
and `close`, and the `bool` return of `delete`.

### 5.1 Semantics

- **Tickets** come from one counter per writer. Completions for puts and
  deletes are delivered in log order (the order their batches are applied,
  which is LSN order within a batch but not necessarily ticket order across
  a small pending batch and a large batch submitted at once); a barrier
  completes after every ticket issued before it; reclaim completes whenever
  it finishes.
- **Visibility.** A put is visible to readers and durable when its
  completion reports `Ok`. A delete is invisible to *this writer* immediately
  (a subsequent `put` of the id is not `Exists`) and to readers when its
  completion reports `Ok`. Ordering between a put and a delete of the same id
  is by LSN, which is by call order.
- **Batching.** Small records accumulate in a pending batch that is submitted
  when it reaches `batch_limit` or at the next `poll`, whichever comes first.
  One worker iteration is therefore one packing window, with no timer.
- **Errors.** `Err(Busy)` means exactly one thing: the pool has no buffer of
  the requested class. Poll (to return buffers) and retry. Ring depth is not
  the caller's problem: the writer keeps a ready queue of encoded batches and
  pushes `q.vacant()` of them per `poll`; a batch the queue nevertheless
  rejects goes back to the front of that queue with its buffer. The ready
  queue is bounded by the pool, so it cannot grow without limit. All other
  errors are permanent for that call.
- **Failure of a write** fails the ticket, truncates the active segment at
  the failed batch and abandons it; later tickets on the same segment fail
  too, later puts go to a fresh segment. Unchanged from today.

### 5.2 What `poll` does, in order

1. `q.take(desc, scratch)`; match each completion to the in-flight FIFO
   (batch tokens) or to the auxiliary table (header writes, footer writes,
   fsync, reclaim reads).
2. Apply every batch at the head of the FIFO whose result has arrived:
   index insert/remove, footer entry, live-bytes accounting, completion out.
3. Advance **sealing**: an active segment that ran out of room was swapped
   for a fresh one at the time of the `put` and parked in `sealing`; once the
   FIFO holds no batch for it, write its footer, then (on that completion) its
   header, then mark it `Sealed`.
4. Advance **barriers**: a barrier records the ticket and batch token
   current when it was issued; it completes when the FIFO has passed that
   token (and, for `sync_on_flush`, when its fsync has returned).
5. Advance **reclaim**: one step of the job (issue the next window read,
   parse a returned window and enqueue relocations into the cold batch,
   wait for relocation batches to apply, wait for `pins == 0`, write the free
   header). Liveness is judged against the index *plus* the pending and
   in-flight key sets, so foreground writes never have to drain first.
6. Close the pending small batch if non-empty and append it to the ready
   queue; push up to `q.vacant()` ready batches.

Everything the old code did inside `wait_inflight`, `wait_aux`,
`read_blocking` and `thread::yield_now` becomes a state that step 2–5
re-examines on the next call. Segment header and footer writes go through the
descriptor with auxiliary tokens instead of `Device::write_at`.

## 6. `Reader`

```rust
impl Reader {
    /// Looks the chunk up and submits the read. `Busy` only if the pool is
    /// empty; a read that finds the ring full is held in the reader's ready
    /// queue and pushed by the next `poll`.
    pub fn get(&mut self, q: &mut dyn IoQueue, id: &ChunkId, range: Option<Range<u64>>, token: u64)
        -> Result<ReadOutcome>;
    /// Verifies finished reads and appends them to `out`. Never waits.
    pub fn poll(&mut self, q: &mut dyn IoQueue, out: &mut Vec<ReadCompletion>) -> Result<usize>;
    pub fn in_flight(&self) -> usize;
    pub fn detach(self, q: &mut dyn IoQueue);
}
```

Today's `ReadRing` was already a state machine; it only stops owning a queue
and drops `submit`, `poll(wait)` and `get_sync`. Every worker holds one
`Reader` per disk, all on its own queue, so a GET is served on the worker that
received it (`chunkserver.md` §5.3, §6.4).

Readers can scale across workers independently of disk ownership. Small reads
may exhaust a submission thread's CPU budget before reaching device capacity,
so the reader count is configurable. Each disk retains one writer to serialize
log allocation and index updates without cross-thread coordination.

## 7. Synchronization: the engine has no locks

The single-writer / multi-reader split is what lets the engine work without a
single mutex. Everything is either owned by one thread or shared through
lock-free structures:

| State | Touched by | Mechanism |
|---|---|---|
| Log tails, LSN / seq / ticket counters, pending batch, in-flight FIFO, free list, sealing and reclaim state machines | `Writer` only | Plain fields behind `&mut self` |
| Ring, fixed-file table, inboxes | the owning worker only | `&mut dyn IoQueue`; the borrow checker enforces single issuer |
| Buffer pool | pipelines on the owning worker only | Thread-private; `Busy` when empty. A buffer may travel to another thread (a PUT landing buffer read by the owner's ring) but is returned to the pool only by its home worker (`chunkserver.md` §5.3) |
| Index | `Writer` writes, every `Reader` reads | Single-writer open-addressing table with per-slot sequence numbers (seqlock reads, `chunkserver.md` §4.4) |
| Segment state, live bytes, pin counts | `Writer` writes, `Reader` pins/unpins | Atomics |

Neither the index nor the pool has a mutex. The index is the seqlock table
above (`index.rs`: `Index` for readers, `IndexWriter` for the single writer,
`ReaderSlot` epochs for table retirement). The pool (`moat-common/src/pool.rs`)
belongs to the thread that created it: only that thread allocates or runs the
buddy allocator, asserted at run time; a `PooledBuf` dropped on another thread
pushes its block onto a lock-free return stack that the home thread drains on
its next allocation. Buffers therefore stay plain `Send` values and need no
separate claim type.

## 8. The worker loop

```rust
let mut queue = UringQueue::new(&QueueOptions { depth: 1024, pool, ..Default::default() })?;
let mut writers: Vec<Writer> = engines.iter()
    .map(|e| e.writer(&mut queue))
    .collect::<Result<_>>()?;
let mut done = Vec::new();

loop {
    // Accept requests: writer.put(&mut queue, ..) / delete / flush / reclaim.
    // Busy → leave the request queued and try again next iteration.

    queue.poll(idle)?;                              // one io_uring_enter
    for w in &mut writers { w.poll(&mut queue, &mut done)?; }
    for c in done.drain(..) { /* ack to the client */ }
}
```

One syscall per iteration regardless of the number of disks; one place to
decide whether to busy-poll or sleep (`idle` is false in `busy` mode and true
in `adaptive` mode once nothing is pending); zero cross-thread traffic inside
the engine.

For tests and single-threaded tools a helper drives the same loop to a
predicate:

```rust
pub fn drive(q: &mut dyn IoQueue, w: &mut Writer, until: impl FnMut(&Completion) -> bool) -> Result<()>;
```

which is what the `blocking` module provides (`blocking::wait`, `flush`,
`seal`, `reclaim`, `get`, `drain`), replacing every `flush()?` / `get_sync()`
in the tests.

## 9. Change summary

| Area | Today | Proposed |
|---|---|---|
| Queue construction | `Device::open_queue` inside `open` and `Reader::ring` | Worker builds `UringQueue::new(opts)`; pipelines attach via `IoQueue::attach` |
| Types | `Opened`, `Writer`, `Reader` (metadata handle), `ReadRing` | `Engine` (disk), `Writer` and `Reader` (pipelines) |
| Queue ownership | `Writer` / `ReadRing` own `Box<dyn IoQueue>` | Worker owns it; passed as `&mut dyn IoQueue` to every call |
| Multiple devices per ring | one fd per ring | `Descriptor` per attachment, fixed-file table, per-descriptor inbox |
| `Writer::poll(out, wait)` | may block on the ring | `poll(q, out)` never blocks; `IoQueue::poll(wait)` is the only wait |
| `flush` / `seal_active` / `close` | block until durable | return a `Ticket`; `Outcome::Flush` / `Outcome::Seal` |
| `delete` | flushes and waits twice, returns `bool` | returns `DeleteOutcome::{Deleted{ticket,lsn}, Missing}` |
| `reclaim` | runs to completion inline, blocking reads | starts a job; progresses inside `poll`; `Outcome::Reclaim(report)` |
| Segment boundary in `put` | `wait_inflight` + synchronous footer | old segment parked in `sealing`, footer written asynchronously |
| Header / footer I/O | `Device::write_at` | through the descriptor with auxiliary tokens |
| Backpressure | blocks inside `alloc` / `submit_batch`; `IoQueue` drops the buffer on `WouldBlock` | `Busy` only for pool exhaustion; `IoQueue` rejects losslessly (`Err(buf)`) and exposes `vacant()`; ring depth absorbed by per-pipeline ready queues |
| `Options.queue`, `writer_pool_capacity_multiplier` | engine sizes its own pool | removed; `attach` validates the worker's pool |
| `BlockingIo` | separate trait for `SyncQueue` | removed; `SyncQueue` uses `Device` |
| `Device::open_queue` | — | replaced by `Device::fd()` |
| `Completion { ticket, lsn, result }` | puts only | `Completion { ticket, result: Result<Outcome> }` for every ticketed operation |

Unchanged: on-disk format, `format`, recovery algorithm, `Index`,
`SegmentTable` and pinning, the single-writer invariant (one `Writer` per
disk, LSNs and segment allocation stay lock-free), the metadata calls (now on `Engine`),
`ChunkData` zero-copy views.

## 10. Out of scope

- Moving reclaim's read-and-filter CPU to a helper thread (`chunkserver.md`
  §4.3 fallback). The API above does not preclude it: the job already runs as
  a state machine and only its index mutations must stay on the owner.
- `SQPOLL`. With one ring per worker it becomes an option (one kernel thread
  per worker rather than per disk), but the API is the same either way.
- Rebalancing engines between workers at run time.
- The exact seqlock table layout and resize protocol; §7 fixes the contract
  (single writer, lock-free readers), not the data structure.

## 11. Departures from the proposal

Decided while implementing; each is a simplification with the reason next
to it.

- **Relocated records keep their LSN** (§4.5 of `chunkserver.md` updated).
  Assigning a fresh LSN to a copy is only safe if no put of the same key is
  pending or in flight, which is exactly the draining the non-blocking writer
  can no longer do. With the LSN preserved, reclaim is a physical move and the
  question does not arise. Before freeing a victim the job still waits until
  every batch closed before its last decision is applied, so a tombstone or
  newer version that justified dropping a record is durable first.
- **Completion order** is log order, not ticket order (§5.1). Enforcing
  ticket order would need a reorder buffer on the hot path for a property
  nothing needs.
- **Footer room** is reserved for records in batches that are enqueued but
  not yet applied, not only for applied ones (`Active::fits`). Otherwise the
  footer can grow beyond its reserved space and overwrite the next segment.
- **Pending batches are placed when closed**, not bound to a segment when
  their staging buffer is attached. Reserving worst-case footer room up front
  (one entry per 64 bytes of staging) made small test segments fill with
  reservations; placing at close time needs no reservation, and a batch that
  cannot be placed (no free segment, no buffer for a segment header) simply
  stays pending until the next `poll`.
- **Readers do not drop corrupt index entries** (the index is single-writer).
  A read that fails verification reports `Corrupt` every time; reclaim
  verifies each record it visits and drops the ones whose value fails its
  checksums (`ReclaimReport::corrupt`) instead of aborting the pass.
- **`Options`** gained `index_capacity` (initial slots) and
  `index_memory_budget` (`Error::IndexFull` beyond it) and lost
  `index_shards`; `QueueOptions` gained `descriptors` (size of the fixed-file
  table) and lost `force_sync` (the caller picks `UringQueue` or `SyncQueue`).
  `attach` checks the pool for a maximal large batch and a staging batch
  only; the reclaim window shrinks to the pool's largest class if it has to.
- **Descriptor lifetime** extends through every accepted I/O, including staged
  submissions. Detaching closes the inbox and discards completions, but the
  slot and fixed-file binding remain reserved until outstanding I/O is reaped.
  Dropping a pipeline without detaching leaves its descriptor allocated until
  the queue goes away. Dropping a reader releases all its segment pins,
  including those for reads waiting for queue room.
- **Index pinning** revalidates both the table pointer and the entry sequence
  after taking a segment pin, so a lookup in a retired table cannot bypass
  reclaim. Capacity reporting uses atomic metadata without dereferencing a
  potentially retired table.
- **Auxiliary I/O** checks the transferred length, including segment headers
  and reclaim reads. A short reclaim read aborts the pass without freeing its
  source segment.
- **io_uring setup** uses `SINGLE_ISSUER` + `DEFER_TASKRUN` when the kernel
  accepts them (completion work runs only inside our own `io_uring_enter`,
  so a non-waiting `poll` enters with `GETEVENTS`), falling back to a plain
  ring.
- **`Options::verify_reads`** defaults to `false`: readers trust the index and
  read only the pages covering the requested range without scanning the
  payload on the CPU. Expiring records still read and validate their header.
  When enabled, every read validates the header and every complete checksum
  block touched by the range; empty ranges only validate the header. The
  separate `verify_header_on_read` option, index CRC copy and headerless
  single-block verification path have been removed. Checksum generation,
  recovery and reclaim validation are unchanged, as are the on-disk record
  headers, checksum arrays and footer encoding. `ChunkData` returns data only;
  checksum transport and client-side verification are not implemented yet.
- **CRC implementation** uses `crc-fast` throughout. The engine and node
  benchmarks disable read verification by default; set `MOAT_BENCH_VERIFY=1`
  to enable it.
- **Pool free lists park blocks** on a per-order stack (up to 8 MiB per
  order) instead of merging on every release and splitting on every
  allocation, and never touch the block's own memory on the hot path (the
  intrusive links live in blocks the device has just written, so touching
  them was a cache miss per operation). Parked blocks merge when an
  allocation finds nothing large enough.
- **`UringQueue` stages SQEs** and copies them into the submission ring once
  per submit rather than opening the ring's view per entry.
