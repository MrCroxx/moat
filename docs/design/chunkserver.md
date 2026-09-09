# A Chunkserver for Many Large NVMe Drives (design v0)

> Status: design document. `moat-common` and `moat-engine` implement sections 3
> and 4 on files and in-memory devices. `moat-server` implements disk discovery,
> placement, recovery and workers; network transports and the client remain planned.
>
> The design distils operational experience with large RDMA-attached NVMe
> storage fleets: rendezvous-style RDMA protocols, shared-CQ reactors, buffer
> ownership and QP fencing, read/write-separated disk admission, relaxed
> ordering on PCIe, NVMe disk identity, bounded-chunk layering, client-pull
> reads, per-disk write queues and per-block checksums.

---

## 0. One-page summary

| Topic | Decision |
|---|---|
| Storage unit | **Chunk**: opaque 128-bit `ChunkId` → immutable byte string of length `0 ..= CHUNK_MAX` (default 4 MiB, fixed at format time, at most 64 MiB) |
| Who handles variable length | **Two layers**: anything up to `CHUNK_MAX` is handled natively and efficiently by the chunkserver (bytes to megabytes, no upper-layer packing); anything larger is split by the **client library** into chunks written in parallel across all disks and nodes |
| Per-disk engine | **One independent log-structured engine per disk**: fixed-size segments (default 1 GiB) + append-only records + footer index per sealed segment + in-memory hash index + segment-granular reclaim |
| Metadata persistence | **No RocksDB, no separate WAL.** The log is the WAL. Recovery = read every segment header + footers of sealed segments + forward scan of at most two active segments |
| Reclaim | **GC and cache eviction are one code path**: pick a segment → decide per record whether to relocate or drop → free the segment. Storage mode uses greedy + age rules; cache mode uses FIFO + reinsertion by access bit |
| Disk placement | **Deterministic**: weighted rendezvous hashing of `ChunkId` over disks (weight = capacity). Engines are fully independent; adding, removing or losing a disk affects nothing else |
| Threads | One kind of pinned, busy-polling **worker** (private RDMA reactor + io_uring + arena), with a configurable count bounded by the CPU budget. Every worker reads every disk directly; each disk has exactly one **owner worker** that is also its writer (sole owner of the log tail, index mutations and reclaim). Clients route PUTs to the owner, so the hot path has no cross-thread hop; a forwarding ring between workers is the cold fallback. tokio for the management plane only |
| RDMA data path | RC QPs, inline SEND control messages. **PUT = server-grant / client-push**; **GET = client RDMA READ pull**; values ≤ `INLINE_MAX` (default 4 KiB) travel inline in one round trip |
| Integrity | CRC32C per 64 KiB block, end to end (client computes, server verifies, verified again on read); every header, batch and footer carries its own CRC |
| Transport abstraction | A `Transport` trait; `rdma` (verbs) is the performance path, `tcp` the fallback. The protocol state machine is transport agnostic |
| Not doing | Partial overwrite inside a chunk, server-side replication, SPDK, filesystem dependency, tokio on the data path, any third-party KV store |

---

## 1. Requirements, assumptions and non-goals

### 1.1 Requirements

1. Variable-length KV: from a few bytes to hundreds of gigabytes.
2. Single disks of 30 TiB and more; 20+ disks per machine.
3. Write model and RDMA protocol built on production-proven experience.
4. High performance, correctness and completeness, with as little machinery as those allow.
5. Open source, general purpose.
6. Rust.

### 1.2 Assumptions added by this document

- **Hardware baseline**: PCIe 5 NVMe (~10 GB/s sequential read, ~5–7 GB/s write per disk), 4–8 × 400 Gb/s RDMA NICs (~46 GB/s usable each), 96+ cores, multiple NUMA nodes. 24 disks aggregate to 150–200 GB/s, matching four 400G NICs.
- **Semantic baseline**: the chunkserver is a **single-node, durable, correct** chunk store. Once a PUT is acknowledged it must be readable after a restart unless the disk fails; the engine must **never return wrong data** (it returns MISS or CORRUPT instead). Cache semantics (eviction, TTL) are a **policy switch** on top of the engine, not a second engine.
- **Enterprise drives have power-loss protection**; by default no flush is issued on the data path. A `sync_mode` option exists for drives without it.
- **Replication is not inside the chunkserver**: replicas, erasure coding or chain replication are built above it (client-side multi-write or a separate replication layer). The engine exposes `lsn` and `if_absent` primitives so that layer can replay idempotently (see §11).

### 1.3 Non-goals

- POSIX file semantics; partial overwrite inside a chunk (emulate with read-modify-write of a new version at the client).
- Server-side range listing or prefix scans.
- Multi-tenancy isolation and encryption (flag bits are reserved in the protocol; not in v0).

---

## 2. Who handles variable-length values (the central question)

### 2.1 Three options

| Option | Description | Problem |
|---|---|---|
| A. Fixed-size chunks only | Size classes (e.g. 64 KiB..64 MiB), tiny values inline in a metadata KV store | Small objects either waste space (round up to a power of two, 25% on average) or must be packed by the upper layer, which then needs its own packing format, footers and multipart handling. The complexity only moves upward, and every upper layer redoes it |
| B. Chunks of any size | A single chunk may be 100 GB | A single chunk cannot span disks → hot spots and a large failure domain; writes need streaming / multi-RPC intermediate state; server staging buffers are unbounded; GC would move 100 GB; one read occupies a disk for tens of seconds |
| **C. Variable-length chunks in `[0, CHUNK_MAX]` (chosen)** | The engine is log-structured, so a few-byte value and a few-megabyte value are both "append one record" and pack densely; anything above `CHUNK_MAX` is split by the client library | The client library must split and keep a manifest. That is not a compromise: splitting is the **only** way to spread a 100 GB object over every disk and node and use the aggregate bandwidth |

### 2.2 Conclusion and boundary

- **The chunkserver handles all variable length up to `CHUNK_MAX`.** Small values (≤ `INLINE_MAX` = 4 KiB) ride inline in the control message and complete in one round trip; medium values (< 64 KiB) are packed by the disk writer into 4 KiB-aligned batches (group commit, no padding between records); large values are written to disk straight from the RDMA landing buffer without a copy. **Upper layers never pack small objects.**
- **The client library splits anything above `CHUNK_MAX`.** Objects are cut into fixed `CHUNK_MAX` pieces with `ChunkId = H(object_key ‖ version)[0..12] ‖ chunk_index:u32`; each chunk is hashed independently to a (node, disk). A 100 GB object becomes 25,600 4 MiB chunks spread evenly over the whole cluster, with read and write concurrency no longer bounded by a single disk. The manifest (chunk count, total length, per-chunk CRCs) is kept by the upper layer's metadata system, or in a small chunk at `chunk_index = 0`.
- **Why 4 MiB by default**: a single RDMA transfer and a single NVMe I/O are near their bandwidth ceiling at 1–8 MiB; 4 MiB bounds the per-chunk server staging memory and latency (4 MiB at 46 GB/s ≈ 90 µs); 30 TiB / 4 MiB ≈ 8 million index entries per disk, a manageable footprint. Clusters may choose 64 MiB (large S3 parts) or 1 MiB (latency-sensitive). `CHUNK_MAX` is a format-time constant written into every disk's superblock.

---

## 3. Data model and API

```rust
pub struct ChunkId(pub [u8; 16]);          // opaque to the chunkserver

pub struct PutOptions {
    pub overwrite: bool,      // false: an existing id returns Exists (immutable semantics, default)
                              // true: the new version replaces the old one
    pub expire_at: u64,       // cache-mode TTL in seconds since the epoch; 0 = never
}

// server semantics
put(id, value, block_crcs, opts) -> Ok { lsn } | Exists | Throttle | NoSpace | Err
get(id, offset, len)               -> Ok { total_len, data } | Miss | Corrupt | Throttle | Err
delete(id)                         -> Ok | Miss
stat(id)                           -> Ok { len, lsn, expire_at } | Miss
```

- **Immutable by default**: a repeated PUT of the same id returns `Exists`, which clients treat as success (naturally idempotent). With `overwrite = true` the new record supersedes the old one, which GC collects later.
- **`lsn`**: a per-disk monotonically increasing 64-bit write sequence number written into every record, returned by PUT and visible through STAT. It is both the ordering key for recovery (§4.6) and a version token for replication or anti-entropy layers above.
- **Range reads**: `get(id, offset, len)` reads and verifies only the 64 KiB blocks covering the range — a direct requirement of S3 range requests.
- **Acknowledgement**: after the data is written to the device (io_uring completion). `sync_mode = none | fdatasync_per_batch`.

---

## 4. The single-disk engine (`moat-engine`)

One `Engine` per disk, sharing **nothing** with other disks. The engine knows nothing about networking; its input is a checksummed byte buffer, its output a location or completion event, and it runs unchanged on regular files and in-memory devices for tests.

### 4.1 On-disk layout

```
+------------------+------------------+---- reserved to segment_size ----+------------------+---- ... ----+------------------+
| Superblock A 4K  | Superblock B 4K  |                                  | Segment 0        |             | Segment N-1      |
+------------------+------------------+----------------------------------+------------------+---- ... ----+------------------+
                                                                          |<--------- SEGMENT_SIZE (default 1 GiB) ---------->|
```

- **A raw block device or a regular file**, `O_DIRECT`, 4 KiB aligned. No filesystem dependency; file mode is for development and small deployments. The layout is ZNS friendly (zone = segment) but v0 does not use ZNS.
- **Superblock** (A/B alternating, generation + CRC): `magic, format_version, disk_uuid, segment_size, chunk_max, segment_count, created_at`. Written only at format time and by administrative operations; never on the data path.
- **Segment**: fixed size, numbered by physical position `seg_no: u32`; every allocation receives a monotonically increasing `seg_seq: u64` (a reused physical segment gets a fresh seq, which is how leftovers from its previous life are recognised). States `Free | Active | Sealed`, stored in the 4 KiB header page.
- Header page: `magic, crc, version, disk_uuid, seg_no, state, kind (Hot|Cold), seg_seq, footer_offset, footer_len, record_count`. Rewritten whole (one atomic 4 KiB write) on every state change.

30 TiB / 1 GiB ≈ 30,000 segments; headers total 120 MB and take under 100 ms to read at startup.

### 4.2 Record format

Every write is a **batch** (one 4 KiB-aligned `WriteFixed`), in one of three shapes:

```
Large batch (one record ≥ pack threshold)
+---------------- header page(s) --------------------+------------- value -------------+-- pad --+
| BatchHdr 64B | RecordHdr 64B | block_crcs[n] 4B*n | value bytes (page aligned)       | →4K     |
+----------------------------------------------------+---------------------------------+---------+

Inline batch (small records, header right before value, 8-byte aligned)
+-----------+-------------+-------+-------------+-------+-----+---------+
| BatchHdr  | RecordHdr 0 | val 0 | RecordHdr 1 | val 1 | ... | pad →4K |
+-----------+-------------+-------+-------------+-------+-----+---------+

Framed batch (small records whose length is within a header of a page multiple)
+-----------+-------------+-------------+-----+-- pad →4K --+-- val 0 (page aligned) --+-- val 1 --+ ... +
| BatchHdr  | RecordHdr 0 | RecordHdr 1 | ... | header area  | value pages               | value pages| ... |
+-----------+-------------+-------------+-----+--------------+---------------------------+------------+-----+
```

```rust
struct BatchHdr {           // 64 B, LE; crc covers every byte after the crc field
    magic: u32, crc: u32,
    seg_seq: u64,           // must equal the containing segment's current seq, else it is a leftover → scan stops
    batch_len: u32,         // including padding, multiple of 4 KiB
    record_count: u32,
    first_lsn: u64,
}
struct RecordHdr {          // 64 B; crc covers the rest of the header + block_crcs
    magic: u32, crc: u32,
    kind: u8,               // Data | Tombstone
    flags: u8,              // LARGE
    value_len: u32, block_count: u32,
    lsn: u64,
    key: ChunkId,           // 16 B
    expire_at: u64,
}
// followed by block_crcs: [u32; block_count]  (CRC32C per 64 KiB)
```

- Every structure begins with a magic value followed by a CRC32C over everything after it. The magic is checked by comparison, so the CRC does not need to cover it, and the protected region is one contiguous slice.
- In a large batch the value starts on the first page boundary after the headers, so the on-disk layout matches the RDMA landing buffer (the client writes to `buf + header_len`) and the batch goes to disk **without a copy**.
- Fixed overhead per record: 64 B + 4 B per 64 KiB. A large record also pays its header page(s) and tail padding (≈ 0.15% at 4 MiB, ≈ 9% at 64 KiB — hence packing below 64 KiB).
- A tombstone is a record with `value_len = 0, kind = Tombstone`; it goes into a packed batch and is essentially free.
- **Page layout for small values.** Inline records are aligned to eight bytes and moved to the next page when necessary to reduce page crossings. Values near a page multiple use a framed layout with grouped headers and page-aligned values. By default, readers use the index to read the requested value pages without a separate header; expiring records still read and validate the header. Enabling `verify_reads` expands the read to the complete checksum blocks touched by the range and includes the record header and checksum array. The index no longer caches a single-block CRC.

### 4.3 Write pipeline: one writer per disk

```
client ──PUT (routed to owner)──► owner worker of disk d ──io_uring──► NVMe
other workers ──(SPSC forwarding ring, cold path)──┘
```

Each disk has exactly one **owner worker** (§5.3), which holds the disk's `Writer` (see `engine-api.md`). It **exclusively owns** the disk's log tails (two active segments: Hot for foreground writes, Cold for records relocated by reclaim), the right to mutate the index, segment state, reclaim and footer writes. The write pipeline, per PUT that has landed and passed CRC verification:

1. Assign `lsn` (single-threaded increment, no atomics), fill `RecordHdr` **in call order**; copy small records into the writer's packing staging buffer (a pool buffer registered with io_uring), use the landing buffer directly for large ones.
2. Bump-allocate space at the Hot segment's tail; when the remainder is too small the segment is parked for asynchronous sealing and a fresh one is taken without waiting. Submit `WriteFixed` through the worker's queue.
3. On completion, **apply in submission order** (so acknowledged records always form a contiguous prefix and a scan never meets a hole): insert into the index, update the segment's live-byte counter, deliver the completion.
4. The pending packed batch is closed at the end of every worker iteration (no timer): lowest latency under light load, natural coalescing under heavy load.

No engine call blocks on I/O; sealing, barriers and reclaim are state machines advanced by `Writer::poll` on the owner's loop. A slow disk therefore never stalls the other disks or the network traffic handled by the same worker.

Why one writer rather than many workers racing on the tail with `fetch_add`:
- Small-value packing (group commit) needs a single point of aggregation.
- The tail advances in order, so after a crash the active segment's scan stops at the first invalid batch — there is no "later batch completed, earlier batch missing" hole.
- Segment allocation, footers, reclaim and index mutation are all single-threaded, so **the engine has no locks at all**: the index is a single-writer / multi-reader structure (§4.4), everything else is owned by one thread.

Single-core budget: 7 GB/s of writes is ~1,750 four-megabyte io_uring submissions per second plus memcpy of values under 64 KiB (an all-small-value workload at 7 GB/s is within one core's memcpy budget but close). Writes are therefore bandwidth bound and one core per disk is enough, which is why writing is *not* spread over workers the way reading is (§5.3). If profiling shows the owner is the bottleneck, reclaim's reading and filtering move to a helper thread; v0 does not pre-build that.

### 4.4 In-memory index

One open-addressing hash table per disk, **single writer (the owner worker), any number of readers, no locks**. Readers see a consistent entry through a per-slot sequence number (seqlock: the writer bumps it to odd, writes the 48 bytes, bumps it to even; a reader retries if the number changed or was odd). Removal tombstones the slot; the writer rehashes in place when load or tombstones exceed the threshold, publishing the new table with a single pointer swap and retiring the old one once every reader has passed a grace period (readers are busy-polling workers, so the grace period is one loop iteration of each). This is what `moat-engine/src/index.rs` implements: 64-byte cache-line slots (entry plus sequence number), linear probing, tombstones on removal, a rebuild when live entries plus tombstones reach 7/8 of the table (same size when the live count is at most 3/4, else doubled, within `index_memory_budget`), and per-reader epoch slots — a reader is "inside" a lookup while its epoch is odd — that the writer consults before freeing a retired table.

```rust
struct Entry {                 // 48 B
    key: ChunkId,              // 16 B
    seg_no: u32, offset: u32,  // location of the record header
    value_off: u32,            // location of the value (differs from the header for framed records)
    value_len: u32, flags: u32,// LARGE, FRAMED, EXPIRES, ACCESSED
    crc: u32,                  // CRC32C of the first checksum block: verifies framed reads without the header
    lsn: u64,                  // recovery ordering + external version token
}
```

- Memory ≈ 48 B / (7/8 load) ≈ 55 B per entry. 4 MiB average → 24 disks × 8 million ≈ 11 GB; 64 KiB average → ~170 GB. The extra 8 B over a minimal entry buy single-page reads for page-sized values (see §4.2). The engine enforces an `index_memory_budget`; beyond it PUT returns `NoSpace(index)`. **Deployments dominated by tiny objects must raise the budget or pack at the client.** This is a documented capacity constraint, not a hidden OOM.
- **Reader pin protocol**: a GET reads the entry, increments the target segment's `pin_count` (atomic), then rechecks the current table pointer and the entry's sequence number; if either changed, the reader unpins and retries against the current table. Reclaim removes or rewrites entries first and only then waits for `pin_count == 0`, so a reader whose pin is visible to reclaim also saw the entry before removal. A reader therefore never observes a reused segment (optional post-read verification also checks data integrity).

### 4.5 Delete, GC and eviction

**Delete** is a variant of the write path and runs on the disk's owner worker (routed like a PUT, §6.3):

1. Look the key up; absent → complete `Miss` without writing a tombstone (a key missing from the index either never existed or already has its tombstone on disk).
2. Present → remove the entry (single-writer index update, §4.4), subtract the record's footprint from its segment's live bytes.
3. Append a 64 B tombstone (`kind = Tombstone`, fresh lsn) to the current packed batch.
4. **Acknowledge only after that batch's completion**: a delete means "gone after a crash too"; acknowledging after the in-memory removal alone would let recovery resurrect the old record. Latency is one 4 KiB write plus two messages (~20–40 µs on a PLP drive); a 4 KiB page holds 60+ tombstones.

Data is not touched; space is reclaimed asynchronously. A worker mid-read has pinned the segment (§4.4) and is unaffected. `overwrite = true` needs no tombstone (the higher-lsn data record supersedes the old one). TTL expiry is not a delete: a GET that sees an expired record returns `Miss` and posts a "lazy index removal" to the owner over the forwarding ring; no tombstone is written, and recovery drops expired records on load.

**GC runs on the owner worker as a state machine advanced by `Writer::poll`, in small steps interleaved with foreground work**, not on its own thread, so it is serialised with PUT/Del by construction and needs no CAS. The reclaim procedure is identical in storage and cache mode:

```
pick_victim() → sequential read of the whole segment (io_uring, rate limited) → per record:
    Data      and index[key].loc == this record        → decide(record) ∈ {Relocate → Cold segment, Drop}
    Data      and index points elsewhere / absent      → Drop (overwritten or deleted)
    Tombstone and index[key] is Live                   → Drop (a newer data version supersedes it)
    Tombstone and this is the oldest non-Free segment  → Drop (no older data can exist)
    Tombstone otherwise                                → Relocate (must be carried forward or recovery resurrects the old version)
→ all relocations done and index repointed → wait pin_count == 0 → header state = Free
```

`pick_victim` and `decide` are the only two policy points:

| Mode | `pick_victim` | `decide` |
|---|---|---|
| Storage | Trigger: free segments < 10%. Greedy: lowest live ratio; plus a forced pick of the oldest segment when it exceeds an age threshold or total tombstone volume exceeds a threshold (bounds tombstone lifetime) | Live → Relocate |
| Cache | Trigger: free segments below a low-water mark (with hysteresis). FIFO: oldest segment | `ACCESSED` bit set → Relocate to Cold and clear the bit (S3-FIFO / SIEVE style reinsertion, ratio cap configurable); otherwise Drop; expired → Drop |

Reclaim mechanics:

- The victim is read in 16 MiB windows with a few in flight, into the writer's registered GC buffer, under a per-disk token bucket (default 30% of foreground write bandwidth, released when idle). Large relocations `WriteFixed` straight from the GC buffer (zero copy); small ones go into a packed batch.
- A relocated record **keeps its lsn** and goes into the Cold active segment. Reclaim is thereby a purely physical move: the set of `(key, lsn, kind, value)` records recovery sees is unchanged by it, the lsn a client observed for a chunk stays valid across compaction, and no ordering argument between reclaim and concurrent foreground writes is needed. **When the relocation's completion arrives, `index[key].loc` is compared with the old location in the victim**: if unchanged the entry is repointed; if a foreground PUT interleaved between the reclaim decision and the completion has overwritten the key, the entry is left alone and the copy is immediately dead data in the Cold segment (the PUT's lsn is higher, so recovery agrees).
- After every record is processed, every relocation completed and no index entry points at the victim any more, wait for `pin_count == 0`, rewrite the header as `Free`, push the segment onto the free list.
- Cost of reclaiming a 1 GiB segment ≈ read 1 GiB + write the live bytes + one index lookup per record (a segment full of 4 KiB records is 260k lookups, tens of milliseconds of CPU). At 1.5 GB/s the read takes ~0.7 s.
- Write amplification: storage mode with greedy selection and hot/cold separation is typically 1.5–3× at 85–90% utilisation, which is why storage mode reports `NoSpace` around 90% instead of filling up; cache mode is `1 + reinsertion ratio`, capped at 1.2×.
- The read path is unaffected by GC (reads bypass the writer; segments are freed only after pins drain); GC writes share the writer pipeline but foreground PUTs go first.
- **Hot/Cold active segments**: foreground writes go to Hot, relocations to Cold. Hot/cold separation markedly lowers write amplification (the classic LFS result) at the cost of scanning two active segments on recovery.

**Why the tombstone rules are correct** (the part of log-structured deletion that is easiest to get wrong): recovery keeps the highest `lsn` per key (§4.6), and relocation never changes any record's lsn, so the only lsns that matter are the ones assigned by the single writer at put/delete time, strictly increasing in call order. Hence:
- a copy made by relocation is byte-for-byte the record it copies, lsn included, so if both are ever seen by recovery (a crash between the copy landing and the victim being freed) either one wins and the outcome is the same; a live record's lsn is always higher than any *older version* of the key, so relocation never changes who wins;
- dropping a tombstone T while an older data version is still on disk would let recovery resurrect it, so T is dropped only when no older segment exists (it sits in the oldest segment) or a newer live version already outranks it;
- a relocated tombstone keeps its lsn, so a put of the same key that is still pending or in flight when the tombstone is copied (and therefore not yet in the index) keeps outranking it; reclaim never has to wait for foreground writes to drain. The one thing reclaim does wait for before it frees the victim is that every batch closed before its last decision has been applied: a record dropped because the index no longer pointed at it may owe that to a tombstone or newer version still in flight, and those must be on disk before the old copy is gone.

### 4.6 Recovery

Per disk, all disks in parallel:

1. Read superblocks A and B; take the one with the higher generation and a valid CRC.
2. Read every segment header (30,000 × 4 KiB).
3. `Sealed` segments: read the footer (`key, header offset, value offset, value_len, lsn, crc, kind, flags`, 48 B per record — exactly what an index entry needs). `Active` segments: scan batches from the start; stop at the first `BatchHdr` whose `seg_seq` does not match the segment or whose CRC fails (this is what makes leftovers from a reused segment's previous life harmless). A sealed segment whose footer fails validation falls back to the same scan and is re-sealed with a fresh footer.
4. Merge every entry by highest `lsn`; during recovery tombstones participate as `Dead{lsn}` markers and are dropped at the end.
5. Recompute per-segment live bytes; assert no index entry points at a `Free` segment.
6. Seal every segment that was active (write a footer for the records the scan found) so the writer starts on fresh segments.

Cost: headers 120 MB + footers ≈ index volume (48 B × records; 8 million ≈ 384 MB) + at most 2 GiB of active-segment scanning → **1–3 s per disk, all disks in parallel**. No periodic snapshots, no WAL replay, no RocksDB open.

**A PUT is acknowledged without waiting for its segment to be sealed; footers only speed recovery up, they are not the durability mechanism.** Acknowledged records are always recoverable from an active-segment scan because: (1) every batch is self-describing (`magic / batch_len / record_count / crc`) and needs no external index; (2) `BatchHdr.seg_seq` mismatches stop the scan, so a reused segment's leftovers (with valid CRCs) are never mistaken for data; (3) the writer keeps several batches in flight but **acknowledges in submission order**, so acknowledged records form a contiguous prefix and stopping at the first bad batch only loses the unacknowledged tail; (4) the scan verifies each record's header CRC and 64 KiB block CRCs, so a batch whose header landed but whose data was torn (NVMe internal reordering) is recognised and dropped — such a batch was never acknowledged because its completion never arrived. Footer entries need no data verification because a footer is written only after every batch in the segment has completed.

**Recovery time is O(records), not O(capacity), and the bottleneck is hash-table rebuild CPU, not I/O.** The worst case is bounded by the index memory budget: 512 GB across 24 disks is ~21 GB, ≈ 460 million entries per disk; the footers total ≈ 22 GB (a couple of seconds of sequential reads) but 460 million hash inserts (one random memory access each, 50–100 ns) take 25–45 s single-threaded. Footers also list not-yet-reclaimed dead records and tombstones, typically 10–30% on top of live records at 90% utilisation. Two remedies:

1. **Rebuild shards in parallel.** The index is already sharded by key, and each shard's rebuild (including per-key highest-lsn resolution) is independent. During recovery the machine's CPUs are idle: one thread reads footers sequentially and dispatches entries to several shard-building threads. Worst case across the machine: 11 billion entries × 80 ns / 96 cores ≈ 10 s, of the same order as the ~500 GB of random memory writes. **Worst case 10–20 s per machine; typical 1–3 s.** Parallel rebuild does not affect tombstone correctness: taking the maximum lsn is commutative and associative, so the order in which entries arrive is irrelevant (another reason to order by lsn rather than physical position); all records of one key land in one shard because keys are placed deterministically on one disk and sharded by key hash; lsns are unique per disk. The one requirement is a **per-disk barrier**: `Dead` markers may only be dropped once *all* of the disk's input (every footer and scan) has been consumed, otherwise an older data record processed later would resurrect.
2. **Fuzzy index checkpoint** (phase two; formats and interfaces reserved in v0). No writer stall, no copy-on-write:
   - At checkpoint start the writer reports a watermark: `L0` = the lowest lsn among in-flight writes (the next lsn if none), the position of the **oldest in-flight batch** in each active segment (in-flight writes sit before the tail pointer), and the table of `(seg_no → seg_seq, state)`.
   - A low-priority thread per disk copies the hash table in 1 MiB slices (readers are never blocked; a slice whose entries changed underneath, detected through the per-slot sequence numbers, is re-copied), memcpy to staging, then `WriteFixed` into `kind = Index` segments. The writer is never stalled; p99 is untouched.
   - After all slices complete, superblock A/B flips to the new image (with `L0`, replay start positions, the segment table, table capacity, hasher seed, per-slice CRCs); the old image's segments are freed.
   - Recovery: read the image straight into table memory (zero hashing, 21 GB ≈ 2 s); scan the two active segments from the recorded positions and every segment with `seg_seq` above the checkpoint's maximum; replay only records with `lsn ≥ L0`, merging with the image by **highest lsn** (image entries carry their lsn); drop entries pointing at `Free` segments or at segments whose `seg_seq` differs from the checkpoint's table (can be done lazily on read).
   - Correctness: every index change after T0 is either a record with `lsn ≥ L0` (inserts, overwrites, relocations — taking the *lowest in-flight* lsn is what keeps in-flight writes' late index updates inside the replay range; a delete's index removal and its tombstone lsn are assigned in the same job, so the tombstone has `lsn ≥ L0`), or has no record at all — only cache-mode Drop eviction (caught by the segment-table check; if the segment has not been reused the entry comes back as live, harmless for a cache) and lazy TTL removal (re-evaluated on read). Slices are copied under the lock, so no torn entries.
   - Trigger: footer bytes written since the last checkpoint reach 10% of the image size (bounding replay to one tenth of a full rebuild), or a time limit, whichever first. At worst-case scale a checkpoint every 10 minutes is ~35 MB/s per disk (0.7% of write bandwidth, 0.1 DWPD); two images occupy 0.14% of the disk. At typical scale these are two orders of magnitude smaller.
   - The gain only shows when the index is very large (worst case 10–20 s → 3–4 s; typical scale barely changes), hence phase two. A clean-shutdown image is a special case (`L0` = next lsn, empty replay); the two share one mechanism.

Each disk recovers and comes online independently; a slow disk does not block the others. For comparison: a fixed-slot engine can load a dense index snapshot in milliseconds because the slot number is an array index — impossible for variable-length records. An engine that keeps metadata in an embedded LSM store opens fast because it never rebuilds an in-memory index; the price is an LSM lookup on every GET's metadata and compaction write amplification on every write, i.e. recovery cost spread over every I/O in steady state.

### 4.7 Durability and consistency summary

- PUT acknowledged ⇔ the completion of the record's batch `WriteFixed` has returned (durable on a PLP drive). With `sync_mode = fdatasync_per_batch` an `IORING_OP_FSYNC(DATASYNC)` follows each batch.
- **Read verification is optional and disabled by default.** With `Options::verify_reads = true`, each read validates `RecordHdr.magic/crc`, key, LSN, value length and record kind, then verifies every 64 KiB checksum block touched by the requested range; failures return `Corrupt`. With verification disabled, readers rely on the index and segment pin to keep the location valid without scanning the payload. Expiring records still read and validate the header for TTL. Stored checksums and recovery/reclaim validation are unchanged. End-to-end client verification and checksum transport remain future network-layer work.
- A crash can only lose unacknowledged writes; "acknowledged but unreadable" cannot happen short of media failure.
- No partial writes inside a chunk → no COW, no chunk locks, no fragment merging.

---

## 5. The node layer (`moat-server`)

### 5.1 Disk identity and discovery

- Disk identity = **NVMe controller serial + namespace id**, never `/dev/nvmeXnY` (device names change across reboots). The superblock's `disk_uuid` is bound to it; at startup the node enumerates `nvme list` and matches against an allow-list (vendor / serial prefix / explicit list), which naturally excludes the system disk.
- Disks join and leave independently: a formatted disk joins immediately; a failing disk (consecutive EIO / timeouts) is marked `Failed` and its engine goes offline.

### 5.2 Placement across disks

`disk_of(id) = argmax_d  H(id, disk_uuid_d) ^ (1 / weight_d)` (weighted rendezvous hashing, weight = capacity).

- No central table, no state, O(disks); adding or removing a disk moves the minimum number of keys.
- The node keeps a `layout_epoch` and `[current, previous]` disk sets: GET consults the current disk first and, on a miss, the previous one if different; a background migration moves keys from previous disks that now belong elsewhere, then drops the previous layout. Cache mode may disable migration (accept one round of 1/N misses). **During a transition `Del` must be delivered to both the current and the previous disk** (each acting on its own index); otherwise the key stays on the previous disk while the current one answers Miss without a tombstone, and the GET fallback resurrects it. Overwrites are unaffected (GET checks current first).
- Why not "PUT picks the emptiest disk + a global index": deterministic placement makes the 24 engines fully independent (their own recovery, failures and GC) with no global index or cross-disk state. The price is that a slow disk slows 1/N of the keyspace; it is isolated through that disk's write window saturating → `Throttle` (in cache mode, "do not cache this batch of keys").

### 5.3 Threads

```
worker A  (owns disk X)    ┐  every worker: RDMA reactor + io_uring + arena
worker B  (owns disk Y)    │  every worker reads every disk on its own ring
...                       ├─ GET: served where received
worker C  (owns no disk)   ┘  PUT: routed to the owner; forwarding is a cold path
recovery threads (start-up only) · admin/metrics (tokio) · NVMe health probe
```

**Why one kind of worker.** Small reads can exhaust a submission thread's CPU budget before reaching device capacity. Allowing multiple workers to read each disk avoids tying read concurrency to the number of disk owners. Keeping network processing and I/O submission on the same worker avoids a cross-thread handoff on each request. Two consequences:

- Reads are issued **from the worker that received the request**, on that worker's own ring, against any disk. No hop.
- Workers are not split into a network pool and a disk pool. A split would need the same total CPU, add a hop to every operation and, worse, fix the ratio between the two pools: an all-large workload (limited by network bandwidth) idles the disk pool while an all-small workload saturates both with no slack. One kind of worker that does both adapts by itself.

**Worker.** Pinned to one core; each owns a `Reactor` (ibv context + PD + one shared CQ on the NIC nearest its NUMA node, poll batch 64), one `IoQueue` (io_uring; every disk attached as a descriptor, every arena registered as a fixed buffer), one arena (default 1 GiB, hugepages, NUMA local, `RELAXED_ORDERING`), one `Reader` per disk, a connection table, an in-flight request table and pending queues. Busy polling with no `yield` when idle (handing a pinned worker back to the scheduler visibly raises p99); slow timers throttled to once per 100 ms. Worker count is a configuration knob bounded by cores; the default leaves the management plane and the OS a few cores and pins the rest.

**Disk owner.** Each disk is assigned to one worker, preferably on the disk's NUMA node, which additionally holds the disk's `Writer` and runs its reclaim (§4.3). Ownership is published together with the worker's QP endpoint through `/status`, so clients (§7) send a PUT straight to the owner. The owner's loop is the same loop as any other worker's; write work is simply more `Writer::put` / `Writer::poll` calls on it.

**Forwarding rings (cold path).** Between every pair of workers there are two SPSC lock-free rings (256 slots each; the ring count grows quadratically with the worker count). A PUT that arrives at a non-owner (stale routing metadata, a client that does not route, the TCP fallback) is forwarded with its landing buffer; the owner writes it and returns the completion on the reverse ring; the receiving worker answers the client and frees the buffer. SPSC rather than MPSC so producers never contend; each worker scans its inbound rings once per iteration. Routing is an optimisation, never a correctness requirement.

**Buffers cross threads, but are freed at home.** A landing buffer may travel to the owner and be read by the owner's io_uring (all arenas are registered on all rings and all PDs at start-up; they never move), but it is returned to the pool only by the worker that allocated it. Pools are therefore thread private and lock free; a buffer on another thread is represented by a `Send` claim (arena index, offset, home worker) that is converted back into the pool's handle when it comes home on the reverse ring.

**Management plane**: tokio, only HTTP admin, Prometheus and topology reporting; never touches data. **Recovery**: `open` is blocking and runs on temporary threads, all disks in parallel, before the workers start; each engine is then attached to its owner's queue.

**`poll_mode = busy | adaptive`**: `adaptive` switches to `ibv_req_notify_cq` + epoll sleeping after an idle threshold, for open-source users who cannot dedicate cores. Default `busy`.

### 5.4 Admission and QoS

| Resource | Mechanism | Default |
|---|---|---|
| Per-disk read bytes in flight | `ByteWindow`, CAS reservation, RAII permit | 32 MiB (Little's law: 10 GB/s × 2 ms target × 1.5 headroom) |
| Per-disk write bytes in flight | Same, **fully separate** from the read window, no combined total | 32 MiB + writer queue depth 256 |
| Worker arena | Thread-private buddy pool (4 KiB … CHUNK_MAX + header), no lock; `Busy` when exhausted, request queued and retried after the next poll | 1 GiB per worker |
| GC / migration bandwidth | Per-disk token bucket | 30% of foreground writes |
| Queueing | Bounded queues bucketed by wait reason + deadline, `Throttle` on expiry | queue 1024 / deadline 50 ms |

Principles: admit before data moves, work-conserving, reads before writes, every permit is RAII.

---

## 6. Network and RDMA protocol (`moat-transport`)

### 6.1 Transport abstraction

```rust
trait Transport {
    fn send(&mut self, ep: EpId, msg: &Msg, inline: Option<&[u8]>);        // control message + ≤ INLINE_MAX payload
    fn grant_write(&mut self, ep: EpId, buf: BufRef) -> Grant;               // expose a landing buffer (rdma: addr+rkey; tcp: no-op)
    fn expose_read(&mut self, ep: EpId, buf: BufRef, lease: Duration) -> Handle; // expose a read source
    fn poll(&mut self, out: &mut Vec<Event>);                                // Msg / PutDelivered / ReadDone / EpError
    fn fence(&mut self, ep: EpId) -> FenceOutcome;                           // destroy the QP as a DMA fence
}
```

The protocol state machine depends on nothing but this trait, so it can be unit-tested with a mock transport; there are RDMA and TCP implementations.

### 6.2 Messages

Fixed 32 B header + type-specific body, `bytemuck` POD, little endian; DMA'd first into a `MsgRaw` that accepts any bit pattern, validated at the single decode boundary.

| Message | Direction | Body |
|---|---|---|
| `Put` | C→S | `id, len, flags, expire_at, block_crcs[]`, value inline when `len ≤ INLINE_MAX` |
| `PutGrant` | S→C | `addr, rkey` (value start = landing buffer + header length) |
| `PutResp` | S→C | `status, lsn` |
| `Get` | C→S | `id, offset, len` |
| `GetResp` | S→C | `status, total_len, block_crcs[]`, value inline when small; otherwise `addr, rkey, len, lease_ms` |
| `GetDone` | C→S | `req_ids[]` (batched; releases the server's read buffers) |
| `Del` / `Stat` / `Resp` | | |

`status ∈ {Ok, Miss, Exists, Throttle, NoSpace, Corrupt, Err}`. `req_id` is allocated by the client within its slot window (depth = RECV depth).

### 6.3 PUT: server-grant / client-push

```
Client                                        Owner worker of disk_of(id)
  |-- SEND Put{req,id,len,crcs} --------------->|  reserve write window; allocate arena len+header
  |<- SEND PutGrant{req,addr,rkey} ------------ |  (or PutResp{Throttle|Exists|NoSpace})
  |== RDMA_WRITE value → addr ================>|
  |-- RDMA_WRITE_WITH_IMM(0 B, imm=req) ------->|  RC ordering guarantees the value is visible; consumes one RECV
  |                                              |  verify block_crcs → Writer::put_large (zero copy, same thread)
  |                                              |  Writer::poll: batch on disk → index insert → Completion
  |<- SEND PutResp{req,Ok,lsn} ---------------- |  release arena / write window
```

The client computes `disk_of(id)` itself (§5.2 is deterministic) and sends the request to that disk's owner worker (§5.3, published via `/status`), so the whole PUT runs on one thread. If the request lands on another worker it is forwarded over the SPSC ring and answered from the receiving worker after the owner's completion comes back — correct, one hop slower, and only taken when routing metadata is stale.

`len ≤ INLINE_MAX`: `Put` carries the value; it goes straight into the writer's packed batch — **one round trip**.

### 6.4 GET: client RDMA READ pull

```
Client                                        Server worker
  |-- SEND Get{req,id,off,len} ---------------->|  index lookup (+pin segment) → read window → arena
  |                                              |  io_uring ReadFixed: header page + covering 4 KiB pages (one submission, ≤ 2 I/Os)
  |                                              |  verify header.key / block_crcs
  |<- SEND GetResp{req,Ok,len,addr,rkey,lease} - |
  |== RDMA_READ addr → client buf ==============|  the client decides when (receiver-driven for free)
  |   verify block_crcs after the local completion
  |-- SEND GetDone{[req]} --------------------->|  release arena / read window / unpin
```

`total_len ≤ INLINE_MAX`: `GetResp` carries the value inline, one round trip.

Read path essentials:

- **The whole path stays on the worker that received the request; there is no cross-thread hand-off.** It touches three shared things, none of them a lock: a seqlock-protected index entry (memory only), the per-disk read-window atomic, and a segment pin counter (§4.4). Cache mode's `ACCESSED` bit is an atomic or on the entry's flags.
- **I/O count.** The current reader submits one contiguous I/O per request. By default it covers only the requested value pages. With verification enabled, it includes the complete checksum blocks touched by the range and extends back to the header and checksum array. Expiring records also include the header. Separate SQEs for the header and data range are not implemented.
- **System calls**: each poll iteration submits all pending SQEs with one `io_uring_enter`; completions are read from the mmap'd ring and RDMA completions via user-space `ibv_poll_cq`. Amortised, less than one syscall per GET.
- **Server-side CRC verification is optional.** The engine defaults to `verify_reads = false`; enabling it validates the header and requested checksum blocks on every GET. Readers report corruption without mutating the index; reclaim removes corrupt records through the single writer. The planned network layer must transmit stored `block_crcs` to the client for verification after receipt; that transport path is not implemented yet.
- **A late READ hitting a reused server buffer is safe** (the client's CRC check fails and it retries), so read buffers may be reclaimed on lease expiry without destroying the QP, unlike PUT landing buffers.

Latency estimate (PCIe 5 NVMe, 60–90 µs random 4 KiB read, 8–10 GB/s per disk; 400G NIC ≈ 46 GB/s):

| Stage | 4 KiB inline GET | 4 MiB GET |
|---|---|---|
| Request SEND + decode | ~2–3 µs | ~2–3 µs |
| Index / admission / allocation | < 0.5 µs | < 0.5 µs |
| NVMe read (idle disk) | 60–90 µs | 400–600 µs |
| Server CRC verification | < 0.1 µs | ~50–80 µs |
| Response SEND | ~3–5 µs | ~3 µs |
| Client RDMA READ | — | ~90 µs + ~5 µs RTT |
| **Total (unloaded)** | **~80–110 µs** | **~0.8–1.0 ms** |
| p99 target | < 200 µs (NVMe tail) | < 1.5 ms (per-disk read queue) |

These figures are design targets, not portable benchmark results. Performance depends on the CPU, storage firmware, kernel, filesystem or raw-device mode, queue configuration and memory topology. Reproducible results must report that environment, all `MOAT_BENCH_*` variables and the corresponding `fio` configuration instead of relying on developer-machine defaults.

Throughput depends on device, network and CPU capacity. Large reads may be limited by device or network bandwidth, while small reads may be limited by the workers' submission and completion overhead. Worker count and queue depth are configurable. Server read buffers remain allocated until `GetDone` or lease expiry.

Why GET pulls while PUT pushes (both patterns have production track records):

- **The direction of data decides who should pace it.** GET's bottleneck is incast at the client NIC; READ gives the receiver control of timing and byte window without a separate ready/grant message exchange. PUT's bottleneck is the server's disks and buffers; a grant lets the server admit before any data moves.
- **Buffer safety is asymmetric.** The server never writes client memory, so the client needs no quarantine (the most intricate part of a server-push design disappears); a late READ of a reused server source only yields a CRC mismatch and a retry, so the server can reclaim on lease expiry.
- A PUT landing buffer is client-written server memory, so the rule is: **after `PutGrant`, the buffer may be reused only after the immediate-data completion arrives or the QP has been destroyed** (a late WRITE landing on a buffer being written to disk would corrupt acknowledged data — the only window that can silently damage durable data).

### 6.5 Buffer ownership and failure containment

- **Endpoint generation**: every successful handshake of a (client link) increments its generation; all completion events carry the generation; mismatches are dropped with an alert.
- **Three timers**: a soft timeout (the client treats the GET as a miss and re-fetches from origin; no buffer reclaim); an endpoint hard timeout (oldest in-flight operation makes no progress → fence); a fence timeout (destroy fails → mark the endpoint stuck, quarantine the buffers permanently and count them; beyond a budget, fail open and refuse new connections).
- **Explicit `ibv_destroy_qp` as the DMA fence** (mlx5 transitions to RESET before destroying and flushes the QP's CQEs); success proves both directions of DMA have stopped.
- Per-worker unreclaimed-buffer budget; once reached, new PUTs get `Throttle` instead of an OOM.
- CQEs of destroyed endpoints may still sit in the shared CQ: an endpoint's CQ budget and its QP number are reused only after at least `cq.capacity()` further CQEs have been drained since it was retired, which proves no stale completion can still be delivered.

### 6.6 verbs details

- RC QPs; control plane SEND/RECV (inline ≤ INLINE_MAX); data plane RDMA WRITE / WRITE_WITH_IMM (PUT), RDMA READ (GET). Server arena MR access `LOCAL_WRITE | REMOTE_WRITE | REMOTE_READ`; client MR `LOCAL_WRITE | REMOTE_WRITE` (a READ's landing is an inbound write).
- **`IBV_ACCESS_RELAXED_ORDERING` must be on, and must be registered through the `ibv_reg_mr_iova2` ABI** (bindgen cannot bind the C macro, and the old ABI silently drops optional flags at bit ≥ 20). Its performance effect depends on the platform. It acts on the PCIe inbound-write side: the server MR for PUT, the client MR for GET.
- One `Reactor` per worker = context + PD + one shared CQ, routing completions by `wc.qp_num` to `Weak<Endpoint>`; a single QP error poisons only its endpoint.
- QP parameters: `max_send_wr = depth*3`, `max_recv_wr = depth`, `sq_sig_all = false`, `min_rnr_timer 12 / timeout 14 / retry 7 / rnr_retry 7 / max_rd_atomic 16`, MTU = min of both ends' `active_mtu`, SL0, GRH off on native IB (on for RoCE, detected during the handshake).
- Connection setup: out-of-band TCP exchange of `PeerInfo{qpn, psn, lid/gid, mtu, depth, proto_version, inline_max, chunk_max}`; the TCP connection doubles as the liveness probe (EOF → fence).
- Client incast control: a per-NIC **byte window** of in-flight RDMA READs (default 8 MiB ≈ 4× the bandwidth-delay product at 400G); optional pacing off by default.
- RECV depth default 32 × INLINE_MAX 4 KiB = 128 KiB per connection; switch to SRQ beyond a few thousand connections (v1).

### 6.7 TCP fallback

Same messages; `PutGrant` degrades to a `Continue` after which the client streams the value; `GetResp` is followed by the value on the stream; `GetDone` is unnecessary. Runs on `mio`/epoll inside the same worker loop, coexisting with RDMA. The goal is "works and is correct", not line rate.

---

## 7. Client library (`moat-client`) and large objects

- Node routing: **HRW (rendezvous) hashing** over `node_id` (derived from the hostname, independent of list order); membership is a file with one hostname per line, hot-reloaded periodically; nodes report their worker endpoints **and the disk → owner-worker map with its `layout_epoch`** through HTTP `/status`. No central metadata service, no consensus.
- Worker routing inside a node: a PUT goes to the owner of `disk_of(id)` (§5.2, §5.3); a GET goes to any worker, chosen round robin or by the client's NIC locality. A stale owner map costs one forwarding hop on the server, never a wrong answer; the client refreshes it on the next `/status` poll.
- One RC connection per (client, worker); synchronous and asynchronous (tokio) APIs over a single-threaded verbs actor (bounded command queue in, completion event stream out, pinned to a NIC-local core).
- `ObjectWriter / ObjectReader`: split at `CHUNK_MAX`, configurable concurrency (default 64 in flight), per-chunk retries, manifest aggregation. A 100 GB object takes ~2 s on one 400G NIC.
- The client computes the 64 KiB block CRCs (`crc-fast`, with runtime selection of hardware-accelerated kernels).

---

## 8. Capacity and parameters (example: 24 × 30 TiB node)

| Item | Value |
|---|---|
| Segments | 30,720 per disk; headers 120 MB per disk |
| Index memory | 4 MiB average: ~11 GB; 256 KiB average: ~170 GB (budget required) |
| Worker arenas | 32 workers × 1 GiB = 32 GiB hugepages |
| Writer staging | 24 × 64 MiB |
| Recovery time | 1–3 s per disk, all disks in parallel |
| Single GET latency target | 4 KiB inline p99 < 200 µs; 4 MiB p99 < 1.5 ms |
| Throughput target | ≥ 8 GB/s read per disk, ≥ 150 GB/s per machine (NIC bound) |

Format-time constants (superblock): `segment_size = 1 GiB`, `chunk_max = 4 MiB` (`chunk_max ≤ segment_size / 16`). Runtime constants: `inline_max = 4 KiB`, `pack_threshold = 64 KiB`, `block_size = 64 KiB`.

---

## 9. Code organisation and dependencies

```
moat-common        ChunkId, CRC32C block checksums, 4 KiB alignment, AlignedBuf
moat-engine      single-disk engine: superblock, segments, writer, index, reclaim, recovery; no networking
moat-proto       wire types, status codes, constants (bytemuck POD)
moat-verbs-sys   bindgen + cc shim (exports inline functions), links = "ibverbs"
moat-verbs       safe wrappers: Context/PD/CQ/QP/MR (with iova2 dispatch); feature = "rdma"
moat-transport   Transport trait; rdma (reactor, endpoints, leases, fencing); tcp
moat-server      node: NVMe discovery, placement, workers, admission, admin/metrics
moat-client      client library: HRW routing, connection management, ObjectWriter/Reader
moat-tools       format / fsck / dump / bench
```

- Unsafe code is allowed wherever the hot path needs it (aligned allocation, io_uring fixed buffers, registered RDMA memory, hugepage arenas, zero-copy views), under two enforced rules: every `unsafe` block carries a `// SAFETY:` comment stating the invariant it relies on (`clippy::undocumented_unsafe_blocks = deny`), and unsafe operations inside `unsafe fn` are wrapped explicitly as well (`unsafe_op_in_unsafe_fn = deny`). By convention unsafe is confined to small modules that expose safe APIs (`buf.rs`, and later `arena.rs`, `ring.rs`, the verbs wrappers), which is what reviews concentrate on.
- Dependencies: `io-uring`, `libc`, `bytemuck`, `crc-fast`, `hashbrown`, `parking_lot`, `crossbeam` (queues), `bitflags`, `thiserror`, `clap`, `serde/toml`, `tracing`, `prometheus`/`opentelemetry` (management plane), `tokio` (management plane and client async API only). **No RocksDB, no SPDK, no async runtime on the data path.**
- Rust stable, edition 2024.

---

## 10. Correctness and testing

1. **Deterministic engine model tests**: `moat-engine` runs randomized operations against an in-memory reference model on a `MemDevice`, "crashing" at arbitrary I/O boundaries (truncating or damaging the tail) and asserting that acknowledged reads are consistent, unacknowledged writes are either visible or missing, and wrong data is never returned. GC, eviction and the tombstone rules are part of the same model.
2. **Protocol state-machine tests**: `moat-transport` drives PUT/GET/timeout/fence paths with a mock transport and asserts the buffer-ownership invariants (a granted buffer is never reused before the immediate completion or a fence).
3. **Fault injection**: per-disk EIO / slow disk / disk loss; QP errors, interrupted handshakes, clients dying mid-operation.
4. **Benchmarks**: `cargo bench -p moat-engine` exercises the single-disk engine with a temporary file by default or an explicitly supplied `MOAT_BENCH_DEVICE`; it reports throughput and p50/p99/p999. The future `moat-tools bench` command adds fio-style multi-client load with configurable point and log-normal size distributions for end-to-end testing.
5. `fsck`: offline verification of every header, footer and record CRC, reporting differences against a rebuilt index.

---

## 11. Deliberate omissions, trade-offs and differences from the sources

| Topic | This design | Fixed-slot cache engines | Extent + embedded-KV chunk engines | Reason |
|---|---|---|---|---|
| Value size | variable ≤ CHUNK_MAX | fixed | size classes + inline | generality; log structure makes variable length nearly free |
| On-disk structure | append-only segments + footers | fixed slots + periodic index snapshot | allocation bitmaps + embedded KV store | no embedded KV store, no snapshots, recovery O(index) |
| Delete persistence | tombstones + GC rules | none (cache) | KV store | correctness argument in §4.5 |
| Partial overwrite | unsupported | unsupported | supported (COW + locks + fragment merge) | the largest source of complexity; not needed for objects or caches |
| GET data path | client READ | server WRITE (receiver-driven) | server WRITE or client READ | two fewer messages, no client quarantine |
| PUT data path | server-grant / client-push | server-grant / client-push | server RDMA READ from client | admit before data moves |
| Replication | outside the chunkserver | none | chain replication inside storage | keep the engine single-node and composable |
| Disk placement | deterministic rendezvous | global eviction over all disks | decided by upper-layer placement | zero sharing between engines |
| Metadata snapshots | none in v0 (fuzzy checkpoint in phase two) | per-disk A/B dense snapshot | KV store | snapshot size grows linearly with record count; unaffordable for small objects as the primary mechanism |

Relation to two classic systems:

- **Bitcask** (Riak's backend) is this engine's direct ancestor: active file → segment; keydir → sharded in-memory index; hint file → footer; merge → reclaim/eviction; timestamp ordering → lsn ordering (immune to clock skew). Bitcask's three known weaknesses — merge dropping tombstones and resurrecting old values (later fixed by recording enough in the tombstone to drop it only when no older file can exist), the keydir having to fit in memory, and O(records) startup — map to §4.5's tombstone rules, §4.4's memory budget, and §4.6's parallel rebuild and checkpoint. On top of it this design adds raw devices with 4 KiB alignment, small-value packing, per-64 KiB CRCs, hot/cold active segments, concurrent readers under pin counts, and cache eviction.
- **SPDK blobstore / BlobFS** is the other road: page (4 KiB) / cluster (1 MiB allocation unit by default) / blob, extent RLE and xattrs in metadata page chains, in-place writes, no data journal, no data CRC, recovery by scanning metadata pages to rebuild bitmaps, single-threaded metadata. BlobFS is a thin "one file, one blob, flat namespace" layer for RocksDB. Not chosen because a 1 MiB allocation unit rules out small objects (supporting them means rewriting this engine inside a blob), SPDK requires VFIO unbinding, hugepages and dedicated polling cores (hurting open-source generality) while io_uring + `O_DIRECT` gets within a few percent of line rate for large I/O, and single-threaded metadata does not fit one independent engine per disk. Its allocation model is the extent-plus-bitmap family rather than a log.

Known trade-offs and extension points:

- **The all-in-memory index** is an explicit capacity constraint (§4.4); should trillions of tiny objects be required, cold shards can be spilled to disk (footers are already per-segment on-disk indexes) without changing the layout.
- **Two-phase writes (prepare/commit)** for an upper-layer chain replication: an `UNCOMMITTED` flag bit plus a 64 B commit record; the layout does not change.
- **SRQ**, **pacing**, **ZNS** and **index spilling** are all layout-preserving increments.

---

## 12. Suggested order of implementation

1. `moat-common` + `moat-engine` + model tests: get the layout, writer, index, reclaim and recovery right first — this is where all the correctness lives. *(done, including the io_uring queue, registered buffer pool and zero-copy paths; a blocking queue keeps tests deterministic and the engine portable)*
2. `moat-server` with TCP transport + `moat-client` over TCP: end-to-end usable, developable and testable on machines without RDMA. *(the node layer without a transport is done: `core/moat-server` has NVMe discovery by serial, rendezvous placement, the pinned worker with one io_uring queue per worker and a pluggable request `Handler`, parallel recovery and owner assignment, and a whole-node benchmark; the TCP transport is the next `Handler`)*
3. `moat-verbs` + `moat-transport::rdma`: reactor, endpoints, leases/fencing; drive to line rate with `moat-tools bench`.
4. Multi-disk: NVMe discovery, placement, layout epochs, admission.
5. Cache-mode policies, TTL, `fsck`, observability.
