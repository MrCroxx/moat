Chunkserver implementation audit (2026-09-09)

The baseline for this audit is `3e481443fc170233e0312ed5f3603cbe6fc75024`. It focuses on data retention, failure retries, and recovery semantics in the implemented engine and node layers. The findings concern actual code; networking, migration, checkpoints, and scheduling described in the design documents are not treated as implemented guarantees. This is not a complete proof of concurrent memory ordering or power-loss consistency.

**Changes merged in PR #65**

- Remove `ReclaimPolicy`, cache eviction, `FLAG_ACCESSED`, access-bit updates, and the dedicated conditional removal method. `reclaim(queue)` performs storage GC that retains live chunks; `pick_victim()` selects a sealed segment by live bytes, breaking ties by age.
- When a live chunk fails checksum verification, GC returns `Corrupt` and retains its index entry and original segment instead of deleting it. Remove `ReclaimReport::corrupt`, which counted these implicit deletions.
- When a sealed segment contains an invalid batch header before its known data boundary, GC returns `Corrupt` instead of treating it as the end of the scan and freeing the segment.
- Update callers, tests, the README, and design documents. These changes affect the Rust reclaim API but do not change the on-disk format.

Regression tests cover both GC corruption cases: returning an error, retaining the original data location, and successfully retrying after the damaged bytes are repaired. Another test verifies that unread chunks remain readable after GC and restart. Existing randomized model, concurrent read/write, and reclaim tests provide additional coverage.

**Findings at the audited baseline**

1. **P1: Recovery treats unreadable segments as free and can resurrect older versions.**

   `engine.rs::open` counts and skips segment headers that cannot be read, fail validation, or have mismatched identities. However, `SegmentTable::new` initializes every segment as Free, and `Writer::new` adds those segments to its free list. Reproduction: write and seal a chunk, corrupt its segment header, and reopen successfully. The chunk disappears, and a subsequent write reuses the segment. In a separate reproduction, an older version resides in an earlier segment and the newer version resides in the segment with the damaged header; recovery makes the old LSN current again.

   Recommendation: normal open should return an error when a segment's state is unknown. An explicit salvage mode must quarantine that space instead of reusing it. Quarantine alone does not prevent old versions from reappearing: the unreadable region may contain updates or tombstones. Incomplete recovery results must not be published as a normal serving view.

   Code: [header recovery](../../core/moat-engine/src/engine.rs), [initial segment state](../../core/moat-engine/src/segments.rs), [writer free list](../../core/moat-engine/src/writer.rs).

2. **P1: Footer fallback scanning can truncate acknowledged data in sealed segments.**

   After footer validation fails, `open` uses the same `scan_segment` routine as active-segment recovery. Any record validation failure ends the scan, after which recovery writes a new footer at that position and seals the segment. Reproduction: write two chunks in the same segment and wait for each write to complete, then corrupt the footer and the first value. Open succeeds, but the intact second chunk disappears. A subsequent open no longer reports a bad footer or rescans the segment.

   Recommendation: distinguish active-tail recovery from sealed-segment integrity checking. The latter must at least require the scan to cover the entire data range recorded in the header; corruption in the middle cannot be classified as an unacknowledged tail. Return an error and preserve the evidence before introducing explicit salvage. Media corruption in an active segment must not automatically be equated with an incomplete tail write either.

   Code: [open / scan_segment / seal_segment_blocking](../../core/moat-engine/src/engine.rs).

3. **P1: Retrying a failed delete returns Missing, but the chunk reappears after restart.**

   `Writer::delete` changes the shared index before the tombstone write completes, so readers immediately see Missing as well. The failure path calls `untrack` to clear operation state without restoring the old index entry. Reproduction: write and seal a chunk, submit a delete, inject EIO into its write, and observe the failed completion. After restoring the device, retrying the delete returns Missing; after restart, the original chunk still exists.

   Recommendation: separate the writer's pending-operation view from the readers' completed-operation view, publishing deletion when the tombstone completes and preserving retryable state on failure. Ordering among multiple puts and deletes of the same key, and GC dependencies on pending tombstones, must be handled together. A single index rollback is insufficient.

   Code: [delete / fail_batch / untrack / apply_record](../../core/moat-engine/src/writer.rs).

4. **P1: Exists for duplicate PUTs conflates pending and completed writes.**

   `exists` checks `unapplied`, so a second PUT for the same ID returns `Exists` without a ticket while the first PUT is still in a pending batch. In the reproduction, the shared index does not yet contain the chunk; after the first write fails, the chunk remains absent. Callers cannot treat this `Exists` as an acknowledged idempotent success.

   Recommendation: return Exists only for a completed, existing chunk. A duplicate pending write should return a waitable dependency ticket, share the eventual result, or explicitly report a pending state. This issue belongs to the same operation state machine as failed deletes and should be fixed alongside them.

   Code: [put / put_large / exists](../../core/moat-engine/src/writer.rs).

5. **P1: Duplicate disk UUIDs are accepted and collapse multi-disk placement.**

   `FormatOptions::default` supplies an all-zero UUID, which format writes unchanged. Neither `Node::open` nor `Placement::new` checks uniqueness. Identical UUIDs produce identical seeds; with equal capacities, every key selects the same disk in the list. Reordering the disk list also changes the selected physical disk. The reproduction uses two devices with default UUIDs and equal capacities: all 1,000 keys land on one disk.

   Recommendation: reject duplicate UUIDs before Node builds placement. Formatting should require an explicit unique identity, or a designated formatting entry point should generate and persist one. Do not regenerate identities on each open, which would change placement after restart.

   Code: [FormatOptions](../../core/moat-engine/src/options.rs), [format](../../core/moat-engine/src/engine.rs), [Node::open](../../core/moat-server/src/node.rs), [Placement](../../core/moat-server/src/placement.rs).

Six independent MemDevice reproductions confirmed the baseline behavior behind these five findings. PR #65 left them outstanding. The cache implementation branch subsequently added conservative header/sealed recovery, completed-state deletion, Busy responses for unacknowledged duplicates, and duplicate UUID rejection in `Node::open`, with regression tests. Active-tail media-corruption classification and non-PLP persistence ordering remain separate open questions. Formatting still accepts an explicit all-zero identity for single-device use; multi-disk node assembly requires unique persistent identities.

**Existing constraints that need explicit contracts**

`verify_reads = false` is an existing, deliberate performance choice. The network layer does not yet provide end-to-end verification, so default reads cannot also be claimed to never return corrupt data. `sync_on_flush` schedules synchronization only at explicit barriers: an individual PUT completion does not establish power-loss durability on a device without power-loss protection (PLP). The persistence ordering between GC relocations and Free headers also needs a separate proof for non-PLP support. These contracts need to be specified; this PR does not change the configuration defaults or claim hardware power-loss validation.

A single writer per disk, opaque ChunkId addressing, chunk range reads, small-object batching, physical GC, and explicitly requested overwrites are appropriate chunkserver responsibilities. They have not been removed to accommodate foyer.
