// Copyright 2026- Moat Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The in-memory chunk index: one open-addressing hash table per disk, with a
//! single writer and any number of lock-free readers.
//!
//! Every slot is 64 bytes (one cache line) and carries a sequence number that
//! works as a seqlock: the writer bumps it to odd, stores the fields, bumps it
//! to even; a reader retries a slot whose number was odd or changed under it.
//! Readers therefore never block and never observe a torn entry. Removal
//! leaves a tombstone in the slot (entries are never shifted, so a probe
//! sequence a reader is walking stays valid); the writer rebuilds the table
//! when live entries plus tombstones exceed the load limit, publishing the new
//! table with one pointer store and retiring the old one once every reader
//! that might still be inside it has left.
//!
//! Mutation goes through [`IndexWriter`], of which exactly one exists per
//! engine (created by the disk's single [`Writer`](crate::Writer), or
//! temporarily by recovery). Reads go through [`Index`], which every thread
//! shares. A thread that reads registers a [`ReaderSlot`], the epoch counter the
//! writer consults before freeing a retired table.
//!
//! The segment pin protocol is anchored here as well: a reader pins the
//! segment an entry points at and then re-checks the slot's sequence number
//! ([`Index::get_and_pin`]). Reclaim removes entries first and only then waits
//! for pins to drain, so a reader whose pin is visible to reclaim also saw the
//! entry before its removal and never observes a reused segment.

use std::sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering, fence},
};

use moat_common::ChunkId;

use crate::segments::SegmentTable;

/// Index flag: the record lives in a large batch.
pub const FLAG_LARGE: u32 = 1;
/// Index flag: the record has been read since it was written or last
/// relocated. Used by cache-mode reclaim to decide what to keep.
pub const FLAG_ACCESSED: u32 = 2;
/// Index flag (recovery only): the newest record for this key is a tombstone.
pub(crate) const FLAG_DEAD: u32 = 4;
/// Index flag: the record lives in a framed batch (header separate from the
/// page-aligned value).
pub const FLAG_FRAMED: u32 = 8;
/// Index flag: the record carries an expiry time in its header.
pub const FLAG_EXPIRES: u32 = 16;

/// Translates on-disk record flags into index flags.
pub(crate) fn flags_from_record(record_flags: u8) -> u32 {
    use crate::layout::{RECORD_FLAG_EXPIRES, RECORD_FLAG_FRAMED, RECORD_FLAG_LARGE};
    let mut flags = 0;
    if record_flags & RECORD_FLAG_LARGE != 0 {
        flags |= FLAG_LARGE;
    }
    if record_flags & RECORD_FLAG_FRAMED != 0 {
        flags |= FLAG_FRAMED;
    }
    if record_flags & RECORD_FLAG_EXPIRES != 0 {
        flags |= FLAG_EXPIRES;
    }
    flags
}

/// The physical position of a record header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Location {
    /// Physical segment number.
    pub seg_no: u32,
    /// Offset of the record header within the segment.
    pub offset: u32,
}

/// What the index knows about the newest record of a chunk: its location,
/// layout and version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexValue {
    /// Where the record header is.
    pub loc: Location,
    /// Offset of the value within the segment.
    pub value_off: u32,
    /// Value length in bytes.
    pub value_len: u32,
    /// `FLAG_*` bits.
    pub flags: u32,
    /// The record's LSN.
    pub lsn: u64,
}

impl IndexValue {
    /// Whether the record lives in a large batch.
    #[inline]
    pub fn is_large(&self) -> bool {
        self.flags & FLAG_LARGE != 0
    }

    /// Whether the record lives in a framed batch.
    #[inline]
    pub fn is_framed(&self) -> bool {
        self.flags & FLAG_FRAMED != 0
    }

    /// Whether the record has an expiry time.
    #[inline]
    pub fn expires(&self) -> bool {
        self.flags & FLAG_EXPIRES != 0
    }

    /// Whether the record has been read since written or relocated.
    #[inline]
    pub fn is_accessed(&self) -> bool {
        self.flags & FLAG_ACCESSED != 0
    }

    /// The on-disk record flags this entry was derived from, for footprint
    /// accounting.
    #[inline]
    pub(crate) fn record_flags(&self) -> u8 {
        use crate::layout::{RECORD_FLAG_EXPIRES, RECORD_FLAG_FRAMED, RECORD_FLAG_LARGE};
        let mut flags = 0;
        if self.is_large() {
            flags |= RECORD_FLAG_LARGE;
        }
        if self.is_framed() {
            flags |= RECORD_FLAG_FRAMED;
        }
        if self.expires() {
            flags |= RECORD_FLAG_EXPIRES;
        }
        flags
    }
}

/// Result of [`IndexWriter::insert_if_newer`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// No entry existed; the value was inserted.
    Inserted,
    /// An older entry was replaced.
    Replaced(IndexValue),
    /// The existing entry is at least as new; nothing changed.
    Rejected,
    /// The table is at its memory budget; nothing changed.
    Full,
}

// ---------------------------------------------------------------------------
// Slots and tables
// ---------------------------------------------------------------------------

const EMPTY: u32 = 0;
const LIVE: u32 = 1;
const TOMBSTONE: u32 = 2;

/// One cache line: a seqlock and the entry it protects.
#[repr(C, align(64))]
struct Slot {
    seq: AtomicU32,
    state: AtomicU32,
    key_lo: AtomicU64,
    key_hi: AtomicU64,
    seg_no: AtomicU32,
    offset: AtomicU32,
    value_off: AtomicU32,
    value_len: AtomicU32,
    flags: AtomicU32,
    lsn: AtomicU64,
}

/// Size of one slot in bytes.
pub const SLOT_BYTES: usize = std::mem::size_of::<Slot>();

/// A snapshot of a slot's contents, read under its seqlock.
#[derive(Clone, Copy)]
struct Snapshot {
    state: u32,
    key: ChunkId,
    value: IndexValue,
}

impl Slot {
    const fn new() -> Self {
        Self {
            seq: AtomicU32::new(0),
            state: AtomicU32::new(EMPTY),
            key_lo: AtomicU64::new(0),
            key_hi: AtomicU64::new(0),
            seg_no: AtomicU32::new(0),
            offset: AtomicU32::new(0),
            value_off: AtomicU32::new(0),
            value_len: AtomicU32::new(0),
            flags: AtomicU32::new(0),
            lsn: AtomicU64::new(0),
        }
    }

    /// Reads the slot consistently; spins while the writer is mid-update.
    /// Returns the sequence number the snapshot is valid for.
    #[inline]
    fn read(&self) -> (u32, Snapshot) {
        loop {
            let s1 = self.seq.load(Ordering::Acquire);
            if s1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let snapshot = Snapshot {
                state: self.state.load(Ordering::Relaxed),
                key: key_from(self.key_lo.load(Ordering::Relaxed), self.key_hi.load(Ordering::Relaxed)),
                value: IndexValue {
                    loc: Location {
                        seg_no: self.seg_no.load(Ordering::Relaxed),
                        offset: self.offset.load(Ordering::Relaxed),
                    },
                    value_off: self.value_off.load(Ordering::Relaxed),
                    value_len: self.value_len.load(Ordering::Relaxed),
                    flags: self.flags.load(Ordering::Relaxed),
                    lsn: self.lsn.load(Ordering::Relaxed),
                },
            };
            fence(Ordering::Acquire);
            if self.seq.load(Ordering::Relaxed) == s1 {
                return (s1, snapshot);
            }
        }
    }

    /// Writer only: publishes `state`/`key`/`value` under the seqlock.
    fn write(&self, state: u32, key: &ChunkId, value: &IndexValue) {
        let s = self.seq.load(Ordering::Relaxed);
        self.seq.store(s.wrapping_add(1), Ordering::Relaxed);
        fence(Ordering::Release);
        let (lo, hi) = key_parts(key);
        self.key_lo.store(lo, Ordering::Relaxed);
        self.key_hi.store(hi, Ordering::Relaxed);
        self.seg_no.store(value.loc.seg_no, Ordering::Relaxed);
        self.offset.store(value.loc.offset, Ordering::Relaxed);
        self.value_off.store(value.value_off, Ordering::Relaxed);
        self.value_len.store(value.value_len, Ordering::Relaxed);
        self.flags.store(value.flags, Ordering::Relaxed);
        self.lsn.store(value.lsn, Ordering::Relaxed);
        self.state.store(state, Ordering::Relaxed);
        self.seq.store(s.wrapping_add(2), Ordering::Release);
    }

    /// Writer only: marks the slot as a tombstone (the key stays readable so a
    /// concurrent reader that matched it re-checks and moves on).
    fn bury(&self) {
        let s = self.seq.load(Ordering::Relaxed);
        self.seq.store(s.wrapping_add(1), Ordering::Relaxed);
        fence(Ordering::Release);
        self.state.store(TOMBSTONE, Ordering::Relaxed);
        self.seq.store(s.wrapping_add(2), Ordering::Release);
    }
}

#[inline]
fn key_parts(key: &ChunkId) -> (u64, u64) {
    let b = key.as_bytes();
    (
        u64::from_le_bytes(b[..8].try_into().expect("8 bytes")),
        u64::from_le_bytes(b[8..].try_into().expect("8 bytes")),
    )
}

#[inline]
fn key_from(lo: u64, hi: u64) -> ChunkId {
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&lo.to_le_bytes());
    b[8..].copy_from_slice(&hi.to_le_bytes());
    ChunkId::from_bytes(b)
}

struct Table {
    slots: Box<[Slot]>,
    mask: usize,
}

impl Table {
    fn new(capacity: usize) -> Self {
        let capacity = capacity.max(16).next_power_of_two();
        let slots = (0..capacity).map(|_| Slot::new()).collect();
        Self {
            slots,
            mask: capacity - 1,
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[inline]
    fn home(&self, key: &ChunkId) -> usize {
        (key.mix() as usize) & self.mask
    }

    /// Lock-free lookup. Returns the slot index and the entry's sequence number
    /// alongside the value so callers can re-validate.
    fn find(&self, key: &ChunkId) -> Option<(usize, u32, IndexValue)> {
        let mut i = self.home(key);
        for _ in 0..self.capacity() {
            let (seq, snap) = self.slots[i].read();
            match snap.state {
                EMPTY => return None,
                LIVE if snap.key == *key => return Some((i, seq, snap.value)),
                _ => {}
            }
            i = (i + 1) & self.mask;
        }
        None
    }

    /// Writer only: the slot holding `key`, or the slot an insert of `key`
    /// would use (the first tombstone on the way, else the terminating empty
    /// slot). `None` if the table has no room at all.
    fn locate(&self, key: &ChunkId) -> Located {
        let mut i = self.home(key);
        let mut first_tombstone = None;
        for _ in 0..self.capacity() {
            let (_, snap) = self.slots[i].read();
            match snap.state {
                EMPTY => {
                    return Located::Vacant(first_tombstone.unwrap_or(i));
                }
                TOMBSTONE => {
                    first_tombstone.get_or_insert(i);
                }
                _ if snap.key == *key => return Located::Occupied(i, snap.value),
                _ => {}
            }
            i = (i + 1) & self.mask;
        }
        match first_tombstone {
            Some(i) => Located::Vacant(i),
            None => Located::Exhausted,
        }
    }
}

enum Located {
    Occupied(usize, IndexValue),
    Vacant(usize),
    Exhausted,
}

enum Room {
    /// The current table has a free slot within the load limit.
    Fits,
    /// The table was rebuilt; probe again.
    Rebuilt,
    /// The memory budget forbids growth.
    Full,
}

// ---------------------------------------------------------------------------
// Reader registration
// ---------------------------------------------------------------------------

/// Maximum number of concurrently registered readers per index.
pub const MAX_READERS: usize = 1024;

/// One reader's epoch on its own cache line: even when outside a lookup, odd
/// while inside one, zero when the slot is unused.
#[repr(align(64))]
struct Epoch(AtomicU64);

/// A registered reader of an [`Index`]; obtained from [`Index::register`] and
/// returned with [`Index::unregister`]. Not `Clone`: one per reading thread.
#[derive(Debug, PartialEq, Eq)]
pub struct ReaderSlot(usize);

impl ReaderSlot {
    /// A placeholder that is not registered anywhere.
    pub(crate) const fn detached() -> Self {
        Self(usize::MAX)
    }

    pub(crate) fn is_detached(&self) -> bool {
        self.0 == usize::MAX
    }
}

/// A retired table waiting for the readers that were inside it to leave.
pub(crate) struct Retired {
    table: Box<Table>,
    /// `(slot, epoch)` of every reader that was inside a lookup at retirement.
    inside: Vec<(usize, u64)>,
}

// ---------------------------------------------------------------------------
// Index (shared, read side)
// ---------------------------------------------------------------------------

/// The chunk index of one disk. See the [module docs](self).
pub struct Index {
    current: std::sync::atomic::AtomicPtr<Table>,
    epochs: Box<[Epoch]>,
    live: AtomicUsize,
    /// Read without dereferencing a table that the writer may retire.
    capacity: AtomicUsize,
    tombstones: AtomicUsize,
    /// Maximum table bytes; growth beyond it reports the table full.
    budget: usize,
}

impl Index {
    /// Creates an empty index with room for `capacity` slots (rounded up to a
    /// power of two) and a memory budget of `budget` bytes for the table.
    pub fn new(capacity: usize, budget: usize) -> Self {
        let table = Box::new(Table::new(capacity));
        Self {
            capacity: AtomicUsize::new(table.capacity()),
            current: std::sync::atomic::AtomicPtr::new(Box::into_raw(table)),
            epochs: (0..MAX_READERS).map(|_| Epoch(AtomicU64::new(0))).collect(),
            live: AtomicUsize::new(0),
            tombstones: AtomicUsize::new(0),
            budget,
        }
    }

    #[inline]
    fn table(&self) -> &Table {
        // SAFETY: the pointer is always a live table: the writer replaces it
        // with `Release` and frees the old one only after every reader that
        // entered while it was current has left (`Retired::is_quiescent`).
        unsafe { &*self.current.load(Ordering::Acquire) }
    }

    /// Registers the calling thread as a reader. `None` if [`MAX_READERS`]
    /// are already registered.
    pub fn register(&self) -> Option<ReaderSlot> {
        for (i, e) in self.epochs.iter().enumerate() {
            if e.0.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return Some(ReaderSlot(i));
            }
        }
        None
    }

    /// Releases a reader registration.
    pub fn unregister(&self, slot: ReaderSlot) {
        self.epochs[slot.0].0.store(0, Ordering::Release);
    }

    #[inline]
    fn enter(&self, slot: &ReaderSlot) -> &Table {
        let e = &self.epochs[slot.0].0;
        let v = e.load(Ordering::Relaxed);
        debug_assert!(v != 0 && v & 1 == 0, "reader slot re-entered");
        e.store(v + 1, Ordering::Relaxed);
        // The writer publishes a new table, fences, then reads our epoch. If
        // it saw us outside, our table load below must see its store.
        fence(Ordering::SeqCst);
        self.table()
    }

    #[inline]
    fn exit(&self, slot: &ReaderSlot) {
        let e = &self.epochs[slot.0].0;
        e.store(e.load(Ordering::Relaxed) + 1, Ordering::Release);
    }

    /// Looks a chunk up.
    pub fn get(&self, slot: &ReaderSlot, id: &ChunkId) -> Option<IndexValue> {
        let table = self.enter(slot);
        let found = table.find(id).map(|(_, _, v)| v);
        self.exit(slot);
        found
    }

    /// Looks a chunk up and pins the segment it lives in before returning,
    /// re-validating the entry after the pin so that a segment reclaim removed
    /// the entry from is never returned. Marks the entry as accessed.
    ///
    /// The caller must unpin the segment once it has finished reading.
    pub fn get_and_pin(&self, slot: &ReaderSlot, id: &ChunkId, segments: &SegmentTable) -> Option<IndexValue> {
        let mut table = self.enter(slot);
        let result = loop {
            let Some((i, seq, value)) = table.find(id) else {
                break None;
            };
            if self.try_pin(table, i, seq, value, segments) {
                break Some(value);
            }
            table = self.table();
        };
        self.exit(slot);
        result
    }

    /// Validates both the table and the slot after pinning. A retired table
    /// no longer observes removals made by reclaim in its replacement.
    fn try_pin(&self, table: &Table, i: usize, seq: u32, value: IndexValue, segments: &SegmentTable) -> bool {
        segments.pin(value.loc.seg_no);
        // Reclaim removes entries before checking pins; rebuild publishes the
        // replacement before reclaim can remove entries from it.
        fence(Ordering::SeqCst);
        if std::ptr::eq(table, self.current.load(Ordering::Acquire))
            && table.slots[i].seq.load(Ordering::Relaxed) == seq
        {
            if value.flags & FLAG_ACCESSED == 0 {
                table.slots[i].flags.fetch_or(FLAG_ACCESSED, Ordering::Relaxed);
            }
            return true;
        }
        segments.unpin(value.loc.seg_no);
        false
    }

    /// Looks a chunk up from a thread without a registered slot (management
    /// paths). Registers temporarily; `None` when no slot is free.
    pub fn get_unregistered(&self, id: &ChunkId) -> Option<IndexValue> {
        let slot = self.register()?;
        let found = self.get(&slot, id);
        self.unregister(slot);
        found
    }

    /// Number of live entries.
    pub fn len(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }

    /// Number of slots in the current table.
    pub fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    /// Bytes the current table occupies.
    pub fn table_bytes(&self) -> usize {
        self.capacity() * SLOT_BYTES
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        // SAFETY: `current` always holds a table allocated by `Box::into_raw`,
        // and no reader can exist once the index is being dropped.
        unsafe { drop(Box::from_raw(self.current.load(Ordering::Acquire))) };
    }
}

// ---------------------------------------------------------------------------
// IndexWriter (exclusive, write side)
// ---------------------------------------------------------------------------

/// The single mutator of an [`Index`].
///
/// Holds the tables retired by rebuilds until every reader has left them;
/// [`IndexWriter::gc`] frees what has become unreferenced and should be called
/// periodically (the engine's writer does so on every poll).
pub struct IndexWriter {
    index: Arc<Index>,
    retired: Vec<Retired>,
}

impl IndexWriter {
    /// Creates the writer. The caller guarantees no other `IndexWriter` for
    /// `index` exists.
    pub(crate) fn new(index: Arc<Index>) -> Self {
        Self {
            index,
            retired: Vec::new(),
        }
    }

    /// Number of tombstone slots in the current table.
    pub fn tombstones(&self) -> usize {
        self.index.tombstones.load(Ordering::Relaxed)
    }

    /// Reads without an epoch: the writer is the only party that swaps tables.
    pub fn get(&self, id: &ChunkId) -> Option<IndexValue> {
        self.index.table().find(id).map(|(_, _, v)| v)
    }

    /// Fetches the first index slot for an upcoming writer lookup.
    #[inline]
    pub(crate) fn prefetch(&self, id: &ChunkId) {
        #[cfg(target_arch = "x86_64")]
        {
            let table = self.index.table();
            let address = &table.slots[table.home(id)] as *const Slot;
            // SAFETY: only this writer replaces the table, and the masked
            // home slot is in bounds for the duration of this shared borrow.
            unsafe {
                std::arch::x86_64::_mm_prefetch(address.cast(), std::arch::x86_64::_MM_HINT_T0);
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        let _ = id;
    }

    /// Whether an insert of a new key would be refused for lack of budget.
    pub fn is_full(&self) -> bool {
        let table = self.index.table();
        let used = self.index.live.load(Ordering::Relaxed) + self.index.tombstones.load(Ordering::Relaxed);
        used + 1 > load_limit(table.capacity()) && !self.can_rebuild(table.capacity())
    }

    /// Inserts `value` unless the existing entry has an equal or higher LSN.
    ///
    /// Records may be applied out of LSN order (a large record is written
    /// before an earlier small record still sitting in a pending batch, and
    /// recovery sees records in arbitrary order), so the highest LSN must win
    /// regardless of arrival order.
    pub fn insert_if_newer(&mut self, id: ChunkId, value: IndexValue) -> InsertOutcome {
        let table = self.index.table();
        let vacant = match table.locate(&id) {
            Located::Occupied(i, existing) => {
                if existing.lsn >= value.lsn {
                    return InsertOutcome::Rejected;
                }
                table.slots[i].write(LIVE, &id, &value);
                return InsertOutcome::Replaced(existing);
            }
            Located::Vacant(i) => Some(i),
            Located::Exhausted => None,
        };
        let i = match (self.make_room(), vacant) {
            (Room::Full, _) => return InsertOutcome::Full,
            (Room::Fits, Some(i)) => i,
            // Rebuilt (or no slot before): the old table is gone, probe the
            // current one.
            _ => match self.index.table().locate(&id) {
                Located::Vacant(i) => i,
                Located::Occupied(..) | Located::Exhausted => unreachable!("key vanished or table full after rebuild"),
            },
        };
        self.fill_vacant(self.index.table(), i, &id, &value);
        InsertOutcome::Inserted
    }

    fn fill_vacant(&self, table: &Table, i: usize, id: &ChunkId, value: &IndexValue) {
        let was_tombstone = table.slots[i].state.load(Ordering::Relaxed) == TOMBSTONE;
        table.slots[i].write(LIVE, id, value);
        if was_tombstone {
            self.index.tombstones.fetch_sub(1, Ordering::Relaxed);
        }
        self.index.live.fetch_add(1, Ordering::Relaxed);
    }

    /// Removes an entry, returning it.
    pub fn remove(&mut self, id: &ChunkId) -> Option<IndexValue> {
        let table = self.index.table();
        match table.locate(id) {
            Located::Occupied(i, existing) => {
                self.bury(table, i);
                Some(existing)
            }
            _ => None,
        }
    }

    /// Removes the entry if it still points at `expected`.
    pub fn remove_if_at(&mut self, id: &ChunkId, expected: Location) -> Option<IndexValue> {
        let table = self.index.table();
        match table.locate(id) {
            Located::Occupied(i, existing) if existing.loc == expected => {
                self.bury(table, i);
                Some(existing)
            }
            _ => None,
        }
    }

    fn bury(&self, table: &Table, i: usize) {
        table.slots[i].bury();
        self.index.live.fetch_sub(1, Ordering::Relaxed);
        self.index.tombstones.fetch_add(1, Ordering::Relaxed);
    }

    /// Replaces the entry with `value` if it still points at `expected`.
    ///
    /// The accessed flag of the existing entry is *not* carried over: a
    /// relocated record starts a fresh access history.
    pub fn replace_if_at(&mut self, id: &ChunkId, expected: Location, value: IndexValue) -> bool {
        let table = self.index.table();
        match table.locate(id) {
            Located::Occupied(i, existing) if existing.loc == expected => {
                table.slots[i].write(LIVE, id, &value);
                true
            }
            _ => false,
        }
    }

    /// Removes every entry for which `keep` returns `false`.
    pub fn retain(&mut self, mut keep: impl FnMut(&ChunkId, &IndexValue) -> bool) {
        let table = self.index.table();
        for i in 0..table.capacity() {
            let (_, snap) = table.slots[i].read();
            if snap.state == LIVE && !keep(&snap.key, &snap.value) {
                self.bury(table, i);
            }
        }
    }

    /// Visits every live entry.
    pub fn for_each(&self, mut f: impl FnMut(&ChunkId, &IndexValue)) {
        let table = self.index.table();
        for slot in table.slots.iter() {
            let (_, snap) = slot.read();
            if snap.state == LIVE {
                f(&snap.key, &snap.value);
            }
        }
    }

    fn can_rebuild(&self, capacity: usize) -> bool {
        let live = self.index.live.load(Ordering::Relaxed);
        // Same size (drop tombstones) if the live count allows, else double.
        live < grow_limit(capacity) || capacity * 2 * SLOT_BYTES <= self.index.budget
    }

    /// Makes room for one more entry, rebuilding if needed.
    fn make_room(&mut self) -> Room {
        let capacity = self.index.capacity();
        let live = self.index.live.load(Ordering::Relaxed);
        let used = live + self.index.tombstones.load(Ordering::Relaxed);
        if used < load_limit(capacity) {
            return Room::Fits;
        }
        let new_capacity = if live < grow_limit(capacity) {
            capacity
        } else {
            capacity * 2
        };
        if new_capacity > capacity && new_capacity * SLOT_BYTES > self.index.budget {
            return Room::Full;
        }
        self.rebuild(new_capacity);
        Room::Rebuilt
    }

    /// Rebuilds into a table of `capacity` slots (tombstones dropped) and
    /// retires the old one.
    pub fn rebuild(&mut self, capacity: usize) {
        let old = self.index.table();
        let new = Box::new(Table::new(capacity));
        for slot in old.slots.iter() {
            let (_, snap) = slot.read();
            if snap.state == LIVE {
                match new.locate(&snap.key) {
                    Located::Vacant(i) => new.slots[i].write(LIVE, &snap.key, &snap.value),
                    _ => unreachable!("fresh table cannot hold the key or be full"),
                }
            }
        }
        self.index.tombstones.store(0, Ordering::Relaxed);
        self.index.capacity.store(new.capacity(), Ordering::Relaxed);
        let old_ptr = self.index.current.swap(Box::into_raw(new), Ordering::AcqRel);
        // Readers store an odd epoch, fence, then load the table pointer.
        // After this fence, any reader we observe outside a lookup will load
        // the new table.
        fence(Ordering::SeqCst);
        let inside: Vec<(usize, u64)> = self
            .index
            .epochs
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                let v = e.0.load(Ordering::Acquire);
                (v & 1 == 1).then_some((i, v))
            })
            .collect();
        // SAFETY: `old_ptr` came from `Box::into_raw` and is no longer
        // reachable through `current`; it is freed only when quiescent.
        let table = unsafe { Box::from_raw(old_ptr) };
        self.retired.push(Retired { table, inside });
        self.gc();
    }

    /// Frees retired tables no reader can still be inside.
    pub fn gc(&mut self) {
        let index = &self.index;
        self.retired.retain(|r| !r.is_quiescent(index));
    }

    /// Retired tables still held.
    #[cfg(test)]
    pub fn retired(&self) -> usize {
        self.retired.len()
    }
}

impl Retired {
    fn is_quiescent(&self, index: &Index) -> bool {
        self.inside
            .iter()
            .all(|&(slot, epoch)| index.epochs[slot].0.load(Ordering::Acquire) != epoch)
    }
}

impl Drop for IndexWriter {
    fn drop(&mut self) {
        // Tables still referenced by readers are leaked rather than freed
        // under them; this only happens if a writer is dropped while readers
        // are mid-lookup, which the engine's shutdown order prevents.
        self.gc();
        for r in self.retired.drain(..) {
            std::mem::forget(r.table);
        }
    }
}

/// Live plus tombstone slots allowed before a rebuild (7/8 load).
#[inline]
fn load_limit(capacity: usize) -> usize {
    capacity - capacity / 8
}

/// Live entries allowed before a rebuild must grow the table (3/4 load).
#[inline]
fn grow_limit(capacity: usize) -> usize {
    capacity - capacity / 4
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(seg: u32, off: u32, lsn: u64) -> IndexValue {
        IndexValue {
            loc: Location {
                seg_no: seg,
                offset: off,
            },
            value_off: off + 68,
            value_len: 10,
            flags: 0,
            lsn,
        }
    }

    #[test]
    fn a_retired_table_cannot_pin_an_entry_removed_after_rebuild() {
        let index = Arc::new(Index::new(16, usize::MAX));
        let mut writer = IndexWriter::new(index.clone());
        let key = ChunkId::from_u128(1);
        writer.insert_if_newer(key, value(0, 4096, 1));
        let slot = index.register().unwrap();
        let table = index.enter(&slot);
        let (i, seq, value) = table.find(&key).unwrap();
        writer.rebuild(32);
        writer.remove(&key).unwrap();
        let segments = SegmentTable::new(1);
        assert!(!index.try_pin(table, i, seq, value, &segments));
        assert_eq!(segments.pins(0), 0);
        index.exit(&slot);
        assert_eq!(index.get_and_pin(&slot, &key, &segments), None);
        index.unregister(slot);
        writer.gc();
        assert_eq!(writer.retired(), 0);
    }

    #[test]
    fn capacity_is_safe_to_read_during_rebuild_and_retirement() {
        let index = Arc::new(Index::new(16, usize::MAX));
        let mut writer = IndexWriter::new(index.clone());
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..10_000 {
                    assert!(matches!(index.capacity(), 16 | 32));
                    assert!(matches!(index.table_bytes(), 1024 | 2048));
                }
            });
            for _ in 0..1_000 {
                writer.rebuild(32);
                writer.rebuild(16);
            }
        });
    }

    #[test]
    fn newer_wins() {
        let index = Arc::new(Index::new(16, usize::MAX));
        let mut w = IndexWriter::new(index.clone());
        let id = ChunkId::from_u128(1);
        assert_eq!(w.insert_if_newer(id, value(1, 0, 5)), InsertOutcome::Inserted);
        assert_eq!(w.insert_if_newer(id, value(2, 0, 3)), InsertOutcome::Rejected);
        assert_eq!(w.insert_if_newer(id, value(1, 0, 5)), InsertOutcome::Rejected);
        assert_eq!(w.get(&id).unwrap().lsn, 5);
        assert_eq!(
            w.insert_if_newer(id, value(3, 0, 9)),
            InsertOutcome::Replaced(value(1, 0, 5))
        );
        assert_eq!(w.get(&id).unwrap().loc.seg_no, 3);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn conditional_updates() {
        let index = Arc::new(Index::new(16, usize::MAX));
        let mut w = IndexWriter::new(index.clone());
        let id = ChunkId::from_u128(2);
        w.insert_if_newer(id, value(1, 64, 1));
        let wrong = Location { seg_no: 1, offset: 128 };
        let right = Location { seg_no: 1, offset: 64 };
        assert!(!w.replace_if_at(&id, wrong, value(5, 0, 2)));
        assert!(w.replace_if_at(&id, right, value(5, 0, 2)));
        assert_eq!(w.get(&id).unwrap().loc.seg_no, 5);
        assert!(w.remove_if_at(&id, right).is_none());
        assert!(w.remove_if_at(&id, Location { seg_no: 5, offset: 0 }).is_some());
        assert_eq!(index.len(), 0);
        assert!(w.get(&id).is_none());
    }

    #[test]
    fn pin_and_access_flag() {
        let index = Arc::new(Index::new(16, usize::MAX));
        let segments = SegmentTable::new(4);
        let mut w = IndexWriter::new(index.clone());
        let id = ChunkId::from_u128(3);
        w.insert_if_newer(id, value(2, 0, 1));
        let slot = index.register().unwrap();
        let v = index.get_and_pin(&slot, &id, &segments).unwrap();
        assert!(!v.is_accessed(), "the returned snapshot predates the mark");
        assert!(index.get(&slot, &id).unwrap().is_accessed());
        assert_eq!(segments.pins(2), 1);
        segments.unpin(2);
        assert_eq!(segments.pins(2), 0);
        // Relocation starts a fresh access history.
        assert!(w.replace_if_at(&id, v.loc, value(3, 0, 2)));
        assert!(!w.get(&id).unwrap().is_accessed());
        index.unregister(slot);
    }

    #[test]
    fn grows_reuses_tombstones_and_respects_budget() {
        let index = Arc::new(Index::new(16, 64 * SLOT_BYTES));
        let mut w = IndexWriter::new(index.clone());
        for i in 0..40u128 {
            assert_eq!(
                w.insert_if_newer(ChunkId::from_u128(i), value(0, i as u32, 1)),
                InsertOutcome::Inserted,
                "insert {i}"
            );
        }
        assert_eq!(index.capacity(), 64);
        assert_eq!(index.len(), 40);
        // Churn: delete and re-insert far more keys than the table holds; the
        // same-size rebuild must clear tombstones without growing.
        for round in 0..20u128 {
            for i in 0..40u128 {
                assert!(w.remove(&ChunkId::from_u128(i + round * 40)).is_some());
                assert_eq!(
                    w.insert_if_newer(ChunkId::from_u128(i + (round + 1) * 40), value(0, 0, 1)),
                    InsertOutcome::Inserted
                );
            }
        }
        assert_eq!(index.capacity(), 64);
        assert_eq!(index.len(), 40);
        // The budget stops growth: 3/4 of 64 = 48 live entries.
        let mut inserted = 0;
        for i in 10_000..10_100u128 {
            match w.insert_if_newer(ChunkId::from_u128(i), value(0, 0, 1)) {
                InsertOutcome::Inserted => inserted += 1,
                InsertOutcome::Full => break,
                other => panic!("{other:?}"),
            }
        }
        assert!((8..=16).contains(&inserted), "inserted {inserted}");
        assert!(w.is_full());
        assert_eq!(index.capacity(), 64);
        for i in 0..40u128 {
            assert!(w.get(&ChunkId::from_u128(i + 20 * 40)).is_some());
        }
    }

    #[test]
    fn retired_tables_wait_for_readers_inside() {
        let index = Arc::new(Index::new(16, usize::MAX));
        let mut w = IndexWriter::new(index.clone());
        let slot = index.register().unwrap();
        // Simulate a reader that entered a lookup and has not left.
        let table = index.enter(&slot);
        let _ = table;
        for i in 0..100u128 {
            w.insert_if_newer(ChunkId::from_u128(i), value(0, 0, 1));
        }
        assert!(
            w.retired() >= 1,
            "the old table must be held while the reader is inside"
        );
        index.exit(&slot);
        w.gc();
        assert_eq!(w.retired(), 0);
        // Readers outside a lookup never hold a table.
        for i in 100..1000u128 {
            w.insert_if_newer(ChunkId::from_u128(i), value(0, 0, 1));
        }
        assert_eq!(w.retired(), 0);
        assert_eq!(index.get(&slot, &ChunkId::from_u128(999)).unwrap().lsn, 1);
        index.unregister(slot);
    }

    #[test]
    fn concurrent_readers_see_whole_entries() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let index = Arc::new(Index::new(1024, usize::MAX));
        let stop = Arc::new(AtomicBool::new(false));
        let keys = 512u128;
        {
            let mut w = IndexWriter::new(index.clone());
            for k in 0..keys {
                w.insert_if_newer(ChunkId::from_u128(k), value(k as u32, k as u32, 1));
            }
        }
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let index = index.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let slot = index.register().unwrap();
                    let mut n = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let k = n as u128 % keys;
                        if let Some(v) = index.get(&slot, &ChunkId::from_u128(k)) {
                            // A whole entry: every field derived from the same version.
                            assert_eq!(v.loc.offset, v.loc.seg_no);
                            assert_eq!(v.value_off, v.loc.seg_no + 68);
                        }
                        n += 1;
                    }
                    index.unregister(slot);
                    n
                })
            })
            .collect();
        let mut w = IndexWriter::new(index.clone());
        for round in 2..200u64 {
            for k in 0..keys {
                let ver = (k as u64 * 7 + round) as u32;
                w.insert_if_newer(ChunkId::from_u128(k), value(ver, ver, round));
                if k % 5 == 0 {
                    w.remove(&ChunkId::from_u128(k));
                }
            }
            if round % 50 == 0 {
                w.rebuild(index.capacity());
            }
            w.gc();
        }
        stop.store(true, Ordering::Relaxed);
        let total: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(total > 0);
    }
}
