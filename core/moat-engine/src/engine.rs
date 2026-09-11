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

//! Formatting, opening (recovery) and the per-disk [`Engine`] handle.

use std::{
    ops::ControlFlow,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use moat_common::{AlignedBuf, ChunkId, PAGE_SIZE};

use crate::{
    device::Device,
    error::{Error, Result},
    index::{FLAG_DEAD, Index, IndexValue, IndexWriter, Location, flags_from_record},
    io::IoQueue,
    layout::{
        Extent, FooterEntry, RecordGeometry, RecordKind, SEGMENT_HEADER_LEN, SUPERBLOCK_A_OFFSET, SUPERBLOCK_B_OFFSET,
        SUPERBLOCK_LEN, SegmentHeader, SegmentKind, SegmentState, Superblock, decode_footer, encode_footer, footer_len,
        large_batch_len,
    },
    options::{FormatOptions, Options},
    reader::Reader,
    scan::{parse_batch, scan_batches_blocking},
    segments::{Geometry, SegmentTable},
    shared::Shared,
    writer::{Lsn, Writer},
};

/// Formats `device`, destroying any previous contents.
///
/// Writes every segment header as free and both superblock copies. The device
/// must be at least two segments long (the first segment-sized region is
/// reserved for the superblocks).
pub fn format(device: &dyn Device, opts: &FormatOptions) -> Result<()> {
    opts.validate(device.capacity())?;
    let geometry = Geometry::for_device(device.capacity(), opts.segment_size);
    if geometry.segment_count == 0 {
        return Err(Error::InvalidOption("device holds no segments".into()));
    }
    let superblock = Superblock {
        generation: 1,
        disk_uuid: opts.disk_uuid,
        segment_size: opts.segment_size,
        chunk_max: opts.chunk_max,
        segment_count: geometry.segment_count,
        created_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };

    let mut page = AlignedBuf::zeroed(SEGMENT_HEADER_LEN as usize);
    for seg_no in 0..geometry.segment_count {
        SegmentHeader {
            disk_uuid: opts.disk_uuid,
            seg_no,
            state: SegmentState::Free,
            kind: SegmentKind::Hot,
            seq: 0,
            footer_offset: 0,
            footer_len: 0,
            record_count: 0,
        }
        .encode(&mut page);
        device.write_at(&page, geometry.segment_offset(seg_no))?;
    }

    let mut sb = AlignedBuf::zeroed(SUPERBLOCK_LEN);
    superblock.encode(&mut sb);
    device.write_at(&sb, SUPERBLOCK_A_OFFSET)?;
    device.write_at(&sb, SUPERBLOCK_B_OFFSET)?;
    device.sync()?;
    Ok(())
}

/// What recovery found while opening a device.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Segments on the device.
    pub segments: u32,
    /// Segments that were sealed and indexed from their footer.
    pub sealed: u32,
    /// Segments that were active and rebuilt by a forward scan (then sealed).
    pub scanned: u32,
    /// Sealed segments whose footer failed validation and were rebuilt by a
    /// forward scan instead (counted in `scanned` as well).
    pub bad_footers: u32,
    /// Records (data and tombstones) seen across footers and scans.
    pub records: u64,
    /// Chunks in the index after recovery.
    pub chunks: usize,
    /// The next LSN the writer will assign.
    pub next_lsn: u64,
}

/// Metadata about a stored chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkStat {
    /// Value length in bytes.
    pub len: u32,
    /// LSN of the newest record.
    pub lsn: Lsn,
    /// Physical segment holding the record.
    pub segment: u32,
    /// Offset of the value within the segment.
    pub value_offset: u32,
    /// Whether the record is stored framed (header apart from the page
    /// aligned value).
    pub framed: bool,
}

impl ChunkStat {
    pub(crate) fn from_value(v: &IndexValue) -> Self {
        Self {
            len: v.value_len,
            lsn: v.lsn,
            segment: v.loc.seg_no,
            value_offset: v.value_off,
            framed: v.is_framed(),
        }
    }
}

/// Space accounting for one prospective write under the current engine options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteAccounting {
    /// Bytes the record contributes to live-byte accounting after completion.
    pub record_bytes: u64,
    /// Upper bound for its write batch, including packing and page alignment.
    pub batch_bytes: u64,
}

/// Space usage of an engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Usage {
    /// Total number of segments.
    pub segments: u32,
    /// Segments not in use.
    pub free_segments: u32,
    /// Segments closed and eligible for reclaim.
    pub sealed_segments: u32,
    /// Bytes of records the index points at, across all segments.
    pub live_bytes: u64,
    /// Number of chunks in the index.
    pub chunks: usize,
    /// Bytes the index table occupies.
    pub index_bytes: usize,
}

/// One disk: the state every pipeline of the disk shares (index, segment
/// table, superblock, options). Cheap to clone, `Send + Sync`, and does no
/// I/O itself; create a [`Writer`] or [`Reader`] on a queue to move data.
#[derive(Clone)]
pub struct Engine {
    shared: Arc<Shared>,
}

impl Engine {
    /// Returns metadata about a chunk without touching the disk.
    ///
    /// Management path: a thread that reads regularly
    /// should use [`Reader::stat`] instead.
    pub fn stat(&self, id: &ChunkId) -> Option<ChunkStat> {
        self.shared
            .index
            .get_unregistered(id)
            .map(|v| ChunkStat::from_value(&v))
    }

    /// Whether a chunk exists in the index.
    pub fn contains(&self, id: &ChunkId) -> bool {
        self.stat(id).is_some()
    }

    /// Current space usage.
    pub fn usage(&self) -> Usage {
        let segments = &self.shared.segments;
        let mut usage = Usage {
            segments: segments.len(),
            free_segments: 0,
            sealed_segments: 0,
            live_bytes: 0,
            chunks: self.shared.index.len(),
            index_bytes: self.shared.index.table_bytes(),
        };
        for s in segments.iter() {
            match segments.state(s) {
                SegmentState::Free => usage.free_segments += 1,
                SegmentState::Sealed => usage.sealed_segments += 1,
                SegmentState::Active => {}
            }
            usage.live_bytes += segments.live_bytes(s);
        }
        usage
    }

    /// The segment size this device was formatted with.
    pub fn segment_size(&self) -> u64 {
        self.shared.superblock.segment_size
    }

    /// The largest value this device accepts.
    pub fn chunk_max(&self) -> u32 {
        self.shared.superblock.chunk_max
    }

    /// A conservative live-entry budget that avoids index-table growth even
    /// when deleted slots force a same-size rebuild. This is a management hint
    /// for upper-layer admission; it does not reserve index slots.
    pub fn index_entries_without_growth(&self) -> usize {
        let capacity = self.shared.index.capacity();
        capacity - capacity / 4 - 1
    }

    /// Reports physical write costs without allocating or submitting I/O.
    pub fn write_accounting(&self, len: u32) -> Result<WriteAccounting> {
        if len > self.chunk_max() {
            return Err(Error::ValueTooLarge {
                len: len as u64,
                max: self.chunk_max() as u64,
            });
        }
        let kind = if len >= self.shared.options.pack_threshold {
            crate::layout::BatchKind::Large
        } else {
            crate::writer::small_batch_kind(len)
        };
        let flags = match kind {
            crate::layout::BatchKind::Inline => 0,
            crate::layout::BatchKind::Framed => crate::layout::RECORD_FLAG_FRAMED,
            crate::layout::BatchKind::Large => crate::layout::RECORD_FLAG_LARGE,
        };
        let record_bytes = crate::layout::RecordGeometry::footprint(len, flags);
        let batch_bytes = if kind == crate::layout::BatchKind::Large {
            record_bytes
        } else {
            self.shared.options.batch_limit.next_power_of_two() as u64
        };
        Ok(WriteAccounting {
            record_bytes,
            batch_bytes,
        })
    }

    /// The device's identity as recorded in the superblock.
    pub fn disk_uuid(&self) -> [u8; 16] {
        self.shared.superblock.disk_uuid
    }

    /// The device capacity in bytes.
    pub fn capacity(&self) -> u64 {
        self.shared.device.capacity()
    }

    /// The disk's single write pipeline, attached to `q`.
    ///
    /// Fails with [`Error::InvalidOption`] if `q`'s pool cannot hold the
    /// largest buffer the writer will request, and with [`Error::Busy`] if a
    /// writer already exists (call [`Writer::detach`] first).
    pub fn writer(&self, q: &mut dyn IoQueue) -> Result<Writer> {
        self.check_pool(q)?;
        if self.shared.writer_taken.swap(true, Ordering::AcqRel) {
            return Err(Error::Busy);
        }
        let desc = match q.attach(&self.shared.device) {
            Ok(desc) => desc,
            Err(e) => {
                self.shared.writer_taken.store(false, Ordering::Release);
                return Err(Error::Io(e));
            }
        };
        Ok(Writer::new(self.shared.clone(), desc))
    }

    /// A read pipeline for the calling thread, attached to `q`. Any number may
    /// exist.
    pub fn reader(&self, q: &mut dyn IoQueue) -> Result<Reader> {
        self.check_pool(q)?;
        let slot = self
            .shared
            .index
            .register()
            .ok_or_else(|| Error::InvalidOption("too many readers registered on this engine".into()))?;
        let desc = match q.attach(&self.shared.device) {
            Ok(desc) => desc,
            Err(e) => {
                self.shared.index.unregister(slot);
                return Err(Error::Io(e));
            }
        };
        Ok(Reader::new(self.shared.clone(), desc, slot))
    }

    /// The pool must hold a maximal batch and a staging batch.
    fn check_pool(&self, q: &dyn IoQueue) -> Result<()> {
        let needed = self.shared.largest_buffer();
        if q.pool().max_class() < needed {
            return Err(Error::InvalidOption(format!(
                "queue pool max class {} is below the {needed} bytes this engine needs",
                q.pool().max_class()
            )));
        }
        Ok(())
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("disk_uuid", &self.shared.superblock.disk_uuid)
            .field("segments", &self.shared.geometry.segment_count)
            .field("chunks", &self.shared.index.len())
            .finish()
    }
}

impl Shared {
    /// Largest pool buffer any pipeline of this engine needs: a maximal large
    /// batch or a staging batch (the reclaim window shrinks to the pool if it
    /// has to).
    pub(crate) fn largest_buffer(&self) -> usize {
        let largest = large_batch_len(self.superblock.chunk_max) as usize;
        largest.max(self.options.batch_limit).next_power_of_two()
    }
}

/// Opens a formatted device, rebuilding the in-memory index.
///
/// Blocking, and touches no queue: it runs before the engine is attached to
/// any worker, once per process start, and disks recover in parallel by
/// calling it on several threads. Sealed segments are indexed from their
/// footers; segments left active by a crash are scanned forward, verified
/// record by record, and sealed. Every key resolves to its highest-LSN record,
/// and keys whose newest record is a tombstone are dropped. The cost is
/// proportional to the number of records on the device, not its capacity.
pub fn open(device: Arc<dyn Device>, mut options: Options) -> Result<(Engine, RecoveryReport)> {
    options.validate()?;
    let superblock = read_superblock(&*device)?;
    // A pending batch must always fit in a fresh segment together with the
    // segment header and a footer; a quarter of a segment leaves ample room.
    // The batch limit is a power of two so a staging buffer is exactly one
    // pool class, and it must hold at least one maximal small record.
    options.batch_limit = options
        .batch_limit
        .min((superblock.segment_size / 4) as usize)
        .next_power_of_two()
        .max(2 * PAGE_SIZE as usize);
    options.pack_threshold = options.pack_threshold.min((options.batch_limit / 2) as u32);
    let geometry = Geometry::for_device(device.capacity(), superblock.segment_size);
    if geometry.segment_count < superblock.segment_count {
        return Err(Error::Unformatted(format!(
            "device holds {} segments but was formatted with {}",
            geometry.segment_count, superblock.segment_count
        )));
    }
    let geometry = Geometry {
        segment_count: superblock.segment_count,
        ..geometry
    };

    let shared = Arc::new(Shared {
        device,
        index: Arc::new(Index::new(options.index_capacity, options.index_memory_budget)),
        segments: SegmentTable::new(geometry.segment_count),
        geometry,
        superblock,
        options,
        writer_taken: AtomicBool::new(false),
        next_lsn: AtomicU64::new(1),
        next_seq: AtomicU64::new(1),
    });
    let mut index = IndexWriter::new(shared.index.clone());

    let mut report = RecoveryReport {
        segments: geometry.segment_count,
        ..Default::default()
    };
    let mut actives: Vec<(SegmentHeader, u64, Vec<FooterEntry>)> = Vec::new();
    let mut max_seq = 0u64;
    let mut max_lsn = 0u64;

    for seg_no in 0..geometry.segment_count {
        let header = shared.read_segment_header(seg_no)?;
        if header.disk_uuid != shared.superblock.disk_uuid || header.seg_no != seg_no {
            return Err(Error::corrupt(format!("segment {seg_no}: header identity mismatch")));
        }
        if header.state == SegmentState::Sealed
            && (header.footer_offset < geometry.data_start()
                || !header.footer_offset.is_multiple_of(PAGE_SIZE)
                || header.footer_len == 0
                || !header.footer_len.is_multiple_of(PAGE_SIZE)
                || header
                    .footer_offset
                    .checked_add(header.footer_len)
                    .is_none_or(|end| end > geometry.segment_size))
        {
            return Err(Error::corrupt(format!("segment {seg_no}: invalid footer extent")));
        }
        max_seq = max_seq.max(header.seq);
        if header.state == SegmentState::Free {
            continue;
        }
        shared.segments.set(seg_no, header.state, header.kind, header.seq);

        let footer_entries = if header.state == SegmentState::Sealed {
            let footer = shared.read_extent(
                seg_no,
                Extent {
                    start: header.footer_offset,
                    len: header.footer_len,
                },
            )?;
            // A damaged footer is not fatal: the records themselves are still
            // self-describing, so fall back to a scan and write a new footer.
            match decode_footer(&footer, header.seq) {
                Ok(entries) => Some(entries),
                Err(_) => {
                    report.bad_footers += 1;
                    None
                }
            }
        } else {
            None
        };

        match footer_entries {
            Some(entries) => {
                report.sealed += 1;
                shared.segments.set_sealed(seg_no, header.footer_offset);
                for entry in entries {
                    report.records += 1;
                    max_lsn = max_lsn.max(entry.lsn);
                    merge_entry(&mut index, seg_no, &entry)?;
                }
            }
            None => {
                report.scanned += 1;
                let (tail, entries) = scan_segment(&shared, &header)?;
                for entry in &entries {
                    report.records += 1;
                    max_lsn = max_lsn.max(entry.lsn);
                    merge_entry(&mut index, seg_no, entry)?;
                }
                actives.push((header, tail, entries));
            }
        }
    }

    // Keys whose newest record is a tombstone are gone; the rebuild drops the
    // tombstone slots they leave behind.
    index.retain(|_, v| v.flags & FLAG_DEAD == 0);
    if index.tombstones() > 0 {
        let capacity = shared.index.capacity();
        index.rebuild(capacity);
    }

    // Live bytes follow from the final index, not from the order records
    // were merged in.
    index.for_each(|_, v| {
        shared
            .segments
            .add_live(v.loc.seg_no, RecordGeometry::footprint(v.value_len, v.record_flags()));
    });

    // Seal whatever was active so the tail is described by a footer and the
    // writer starts on fresh segments.
    for (header, tail, entries) in actives {
        seal_segment_blocking(&shared, header.seg_no, header.seq, header.kind, tail, &entries)?;
    }

    report.chunks = shared.index.len();
    report.next_lsn = max_lsn + 1;
    drop(index);
    shared.next_lsn.store(max_lsn + 1, Ordering::Release);
    shared.next_seq.store(max_seq + 1, Ordering::Release);
    Ok((Engine { shared }, report))
}

/// Scans a segment forward, verifying every record, and returns the offset at
/// which valid data ends together with a footer entry per record.
///
/// Active segments may have an incomplete tail. A sealed segment has a known
/// data boundary, so corruption before that boundary must fail recovery.
fn scan_segment(shared: &Shared, header: &SegmentHeader) -> Result<(u64, Vec<FooterEntry>)> {
    let seg_no = header.seg_no;
    let sealed = header.state == SegmentState::Sealed;
    let end = if sealed {
        header.footer_offset
    } else {
        shared.geometry.segment_size
    };
    let mut entries = Vec::new();
    let tail = scan_batches_blocking(
        shared,
        seg_no,
        header.seq,
        shared.geometry.data_start(),
        end,
        |batch_off, batch, bytes| {
            let records = match parse_batch(bytes, batch, true) {
                Ok(records) => records,
                Err(error) if sealed => return Err(error),
                Err(_) => return Ok(ControlFlow::Break(())),
            };
            for rec in records {
                entries.push(FooterEntry {
                    key: rec.header.key,
                    offset: (batch_off + rec.offset_in_batch as u64) as u32,
                    value_off: (batch_off + rec.value_in_batch as u64) as u32,
                    value_len: rec.header.value_len,
                    lsn: rec.header.lsn,
                    crc: rec.checksums.get(0).unwrap_or(0),
                    kind: rec.header.kind,
                    flags: rec.header.flags,
                });
            }
            Ok(ControlFlow::Continue(()))
        },
    )?;
    if sealed && tail != end {
        return Err(Error::corrupt(format!(
            "segment {seg_no}: scan stopped at {tail}, expected {end}"
        )));
    }
    Ok((tail, entries))
}

/// Merges one recovered record into the index: highest LSN wins, tombstones
/// are kept as dead markers until every record has been seen.
fn merge_entry(index: &mut IndexWriter, seg_no: u32, entry: &FooterEntry) -> Result<()> {
    let mut flags = flags_from_record(entry.flags);
    if entry.kind == RecordKind::Tombstone {
        flags |= FLAG_DEAD;
    }
    let outcome = index.insert_if_newer(
        entry.key,
        IndexValue {
            loc: Location {
                seg_no,
                offset: entry.offset,
            },
            value_off: entry.value_off,
            value_len: entry.value_len,
            flags,
            lsn: entry.lsn,
        },
    );
    if outcome == crate::index::InsertOutcome::Full {
        return Err(Error::IndexFull);
    }
    Ok(())
}

/// Writes a footer at `tail` and marks the segment sealed (blocking; recovery
/// only — the writer seals asynchronously through its queue).
pub(crate) fn seal_segment_blocking(
    shared: &Shared,
    seg_no: u32,
    seq: u64,
    kind: SegmentKind,
    tail: u64,
    footer: &[FooterEntry],
) -> Result<()> {
    let len = footer_len(footer.len());
    let mut buf = AlignedBuf::zeroed(len as usize);
    encode_footer(seq, footer, &mut buf);
    shared.write_segment_bytes(seg_no, tail, &buf)?;
    shared.write_segment_header(&SegmentHeader {
        footer_offset: tail,
        footer_len: len,
        record_count: footer.len() as u64,
        ..shared.segment_header(seg_no, SegmentState::Sealed, kind, seq)
    })?;
    shared.segments.set_sealed(seg_no, tail);
    Ok(())
}

fn read_superblock(device: &dyn Device) -> Result<Superblock> {
    let mut buf = AlignedBuf::zeroed(SUPERBLOCK_LEN);
    let mut best: Option<Superblock> = None;
    let mut first_err = None;
    for offset in [SUPERBLOCK_A_OFFSET, SUPERBLOCK_B_OFFSET] {
        device.read_at(&mut buf, offset)?;
        match Superblock::decode(&buf) {
            Ok(sb) => {
                if best.as_ref().is_none_or(|b| sb.generation > b.generation) {
                    best = Some(sb);
                }
            }
            Err(e) => {
                first_err.get_or_insert(e);
            }
        }
    }
    best.ok_or_else(|| first_err.expect("both copies failed"))
}
