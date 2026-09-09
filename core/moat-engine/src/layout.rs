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

//! The on-disk format.
//!
//! ```text
//! device:  | superblock A | superblock B | ...reserved to segment_size... | segment 0 | segment 1 | ...
//! segment: | header page (4 KiB) | batch | batch | ... | footer (sealed segments only) |
//! batch:   | BatchHeader (64 B) | record | record | ... | zero padding to 4 KiB |
//! record:  | RecordHeader (64 B) | block checksums (4 B each) | value |
//! ```
//!
//! Three batch shapes exist:
//!
//! - A *large* batch holds exactly one record whose value starts on the first page boundary after the headers, so the
//!   value can be written straight from an aligned landing buffer without copying.
//! - An *inline* batch holds many small records back to back (8-byte aligned, each header immediately followed by its
//!   value), with only the batch as a whole padded to a page. A record is placed so that it spans the minimum number of
//!   pages its length allows.
//! - A *framed* batch keeps all record headers in a header area at the front and every value page aligned after it. It
//!   is used for values whose length is (just under) a multiple of the page size, where an inline header would push the
//!   value across one more page: a 4 KiB value then costs exactly one page to read when verification is disabled.
//!
//! Every structure is self-describing: a magic value, then a CRC32C covering
//! everything after it. Every batch carries the
//! sequence number of the segment incarnation it was written into, and every
//! record carries its LSN. Together these make an unsealed segment recoverable
//! by a forward scan and a reused segment immune to stale data from its
//! previous life.

use std::ops::Range;

use moat_common::{ChunkId, PAGE_SIZE, align_down, align_up, block_count};

use crate::{
    codec::{self, Reader, Writer},
    error::{Error, Result},
};

/// Version of the on-disk format written by this crate.
pub const FORMAT_VERSION: u32 = 1;

const SUPERBLOCK_MAGIC: u64 = 0x314b_4253_5441_4f4d; // "MOATSBK1"
const SEGMENT_MAGIC: u64 = 0x3147_4553_5441_4f4d; // "MOATSEG1"
const FOOTER_MAGIC: u64 = 0x3154_4f46_5441_4f4d; // "MOATFOT1"
const BATCH_MAGIC: u32 = 0x4854_4342; // "BCTH"
const RECORD_MAGIC: u32 = 0x4443_5245; // "ERCD"

/// Length of each superblock copy.
pub const SUPERBLOCK_LEN: usize = PAGE_SIZE as usize;
/// Device offset of superblock copy A.
pub const SUPERBLOCK_A_OFFSET: u64 = 0;
/// Device offset of superblock copy B.
pub const SUPERBLOCK_B_OFFSET: u64 = PAGE_SIZE;

/// Length of the header page at the start of every segment.
pub const SEGMENT_HEADER_LEN: u64 = PAGE_SIZE;
/// Encoded length of a [`BatchHeader`].
pub const BATCH_HEADER_LEN: usize = 64;
/// Encoded length of a [`RecordHeader`], excluding the block checksums.
pub const RECORD_HEADER_LEN: usize = 64;
/// Encoded length of a [`FooterHeader`].
pub const FOOTER_HEADER_LEN: usize = 64;
/// Encoded length of a [`FooterEntry`].
pub const FOOTER_ENTRY_LEN: usize = 48;
/// Alignment of records inside a packed batch.
pub const RECORD_ALIGN: u64 = 8;

/// Record flag: the record lives alone in a large batch.
pub const RECORD_FLAG_LARGE: u8 = 1;
/// Record flag: the record is in a framed batch (header in the batch's header
/// area, value page aligned).
pub const RECORD_FLAG_FRAMED: u8 = 2;
/// Record flag: the record has an expiry time.
pub const RECORD_FLAG_EXPIRES: u8 = 4;

// ---------------------------------------------------------------------------
// Superblock
// ---------------------------------------------------------------------------

/// The per-device superblock. Two copies (A/B) are written alternately; the
/// one with the higher generation and a valid checksum wins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Superblock {
    /// Incremented on every superblock rewrite.
    pub generation: u64,
    /// Identity of this device; also stamped into every segment header.
    pub disk_uuid: [u8; 16],
    /// Size of every segment in bytes (including its header page).
    pub segment_size: u64,
    /// Largest value accepted by `put`.
    pub chunk_max: u32,
    /// Number of segments on the device.
    pub segment_count: u32,
    /// Unix time (seconds) the device was formatted.
    pub created_at: u64,
}

impl Superblock {
    /// Encodes into a [`SUPERBLOCK_LEN`] buffer.
    pub fn encode(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), SUPERBLOCK_LEN);
        buf.fill(0);
        Writer::new(buf)
            .u64(SUPERBLOCK_MAGIC)
            .u32(0) // crc
            .u32(FORMAT_VERSION)
            .u64(self.generation)
            .bytes(&self.disk_uuid)
            .u64(self.segment_size)
            .u32(self.chunk_max)
            .u32(self.segment_count)
            .u64(self.created_at);
        codec::seal(buf, 8);
    }

    /// Decodes and validates a superblock copy.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() != SUPERBLOCK_LEN {
            return Err(Error::corrupt("superblock buffer has wrong length"));
        }
        let mut r = Reader::new(buf);
        if r.u64() != SUPERBLOCK_MAGIC {
            return Err(Error::Unformatted("superblock magic mismatch".into()));
        }
        if !codec::verify(buf, 8) {
            return Err(Error::corrupt("superblock checksum mismatch"));
        }
        r.skip(4);
        let version = r.u32();
        if version != FORMAT_VERSION {
            return Err(Error::Unformatted(format!(
                "unsupported format version {version} (expected {FORMAT_VERSION})"
            )));
        }
        Ok(Self {
            generation: r.u64(),
            disk_uuid: r.array(),
            segment_size: r.u64(),
            chunk_max: r.u32(),
            segment_count: r.u32(),
            created_at: r.u64(),
        })
    }
}

// ---------------------------------------------------------------------------
// Segment header
// ---------------------------------------------------------------------------

/// Lifecycle state of a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SegmentState {
    /// Not in use; contents are garbage.
    Free = 0,
    /// Being appended to; recoverable only by forward scan.
    Active = 1,
    /// Closed; the footer indexes every record.
    Sealed = 2,
}

impl SegmentState {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Free),
            1 => Some(Self::Active),
            2 => Some(Self::Sealed),
            _ => None,
        }
    }
}

/// What a segment is used for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SegmentKind {
    /// Receives foreground writes.
    Hot = 0,
    /// Receives records relocated by reclaim.
    Cold = 1,
}

impl SegmentKind {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Hot),
            1 => Some(Self::Cold),
            _ => None,
        }
    }
}

/// The header page of a segment. Rewritten in place on every state change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Must match the superblock.
    pub disk_uuid: [u8; 16],
    /// Physical segment number.
    pub seg_no: u32,
    /// Lifecycle state.
    pub state: SegmentState,
    /// Usage kind.
    pub kind: SegmentKind,
    /// Sequence number of this incarnation; unique per allocation, never reused.
    pub seq: u64,
    /// Offset of the footer within the segment (sealed segments only).
    pub footer_offset: u64,
    /// Length of the footer in bytes, page aligned (sealed segments only).
    pub footer_len: u64,
    /// Number of records in the segment (sealed segments only).
    pub record_count: u64,
}

impl SegmentHeader {
    /// Encodes into a [`SEGMENT_HEADER_LEN`] buffer.
    pub fn encode(&self, buf: &mut [u8]) {
        assert_eq!(buf.len() as u64, SEGMENT_HEADER_LEN);
        buf.fill(0);
        Writer::new(buf)
            .u64(SEGMENT_MAGIC)
            .u32(0) // crc
            .u32(FORMAT_VERSION)
            .bytes(&self.disk_uuid)
            .u32(self.seg_no)
            .u8(self.state as u8)
            .u8(self.kind as u8)
            .u16(0)
            .u64(self.seq)
            .u64(self.footer_offset)
            .u64(self.footer_len)
            .u64(self.record_count);
        codec::seal(buf, 8);
    }

    /// Decodes and validates a segment header.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() as u64 != SEGMENT_HEADER_LEN {
            return Err(Error::corrupt("segment header buffer has wrong length"));
        }
        let mut r = Reader::new(buf);
        if r.u64() != SEGMENT_MAGIC {
            return Err(Error::corrupt("segment header magic mismatch"));
        }
        if !codec::verify(buf, 8) {
            return Err(Error::corrupt("segment header checksum mismatch"));
        }
        r.skip(4);
        let version = r.u32();
        if version != FORMAT_VERSION {
            return Err(Error::corrupt(format!("segment header has format version {version}")));
        }
        let disk_uuid = r.array();
        let seg_no = r.u32();
        let state = SegmentState::from_u8(r.u8()).ok_or_else(|| Error::corrupt("invalid segment state"))?;
        let kind = SegmentKind::from_u8(r.u8()).ok_or_else(|| Error::corrupt("invalid segment kind"))?;
        r.skip(2);
        Ok(Self {
            disk_uuid,
            seg_no,
            state,
            kind,
            seq: r.u64(),
            footer_offset: r.u64(),
            footer_len: r.u64(),
            record_count: r.u64(),
        })
    }
}

// ---------------------------------------------------------------------------
// Batch header
// ---------------------------------------------------------------------------

/// The shape of a batch. See the [module docs](self).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BatchKind {
    /// Small records, each header immediately followed by its value.
    Inline = 0,
    /// One record; value page aligned after its header.
    Large = 1,
    /// Headers in a front area; values page aligned after it.
    Framed = 2,
}

impl BatchKind {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Inline),
            1 => Some(Self::Large),
            2 => Some(Self::Framed),
            _ => None,
        }
    }
}

/// Header of one write unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchHeader {
    /// Sequence number of the segment incarnation this batch belongs to.
    pub seg_seq: u64,
    /// Total batch length including padding; a multiple of the page size.
    pub batch_len: u32,
    /// Number of records in the batch.
    pub record_count: u32,
    /// LSN of the first record.
    pub first_lsn: u64,
    /// The batch shape.
    pub kind: BatchKind,
    /// Framed batches: length of the header area (page aligned); the first
    /// value starts here. Zero for other kinds.
    pub header_len: u32,
}

impl BatchHeader {
    /// Encodes into a [`BATCH_HEADER_LEN`] buffer.
    pub fn encode(&self, buf: &mut [u8]) {
        assert_eq!(buf.len(), BATCH_HEADER_LEN);
        buf.fill(0);
        Writer::new(buf)
            .u32(BATCH_MAGIC)
            .u32(0) // crc
            .u64(self.seg_seq)
            .u32(self.batch_len)
            .u32(self.record_count)
            .u64(self.first_lsn)
            .u8(self.kind as u8)
            .skip(3)
            .u32(self.header_len);
        codec::seal(buf, 4);
    }

    /// Decodes and validates a batch header. Returns `None` for anything that
    /// is not a well-formed batch header (the normal "end of log" signal during
    /// a forward scan).
    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < BATCH_HEADER_LEN {
            return None;
        }
        let buf = &buf[..BATCH_HEADER_LEN];
        let mut r = Reader::new(buf);
        if r.u32() != BATCH_MAGIC || !codec::verify(buf, 4) {
            return None;
        }
        r.skip(4);
        let seg_seq = r.u64();
        let batch_len = r.u32();
        let record_count = r.u32();
        let first_lsn = r.u64();
        let kind = BatchKind::from_u8(r.u8())?;
        r.skip(3);
        let header_len = r.u32();
        Some(Self {
            seg_seq,
            batch_len,
            record_count,
            first_lsn,
            kind,
            header_len,
        })
    }
}

// ---------------------------------------------------------------------------
// Record header
// ---------------------------------------------------------------------------

/// What a record represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordKind {
    /// A chunk value.
    Data = 0,
    /// A deletion marker; `value_len` is zero.
    Tombstone = 1,
}

impl RecordKind {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Data),
            1 => Some(Self::Tombstone),
            _ => None,
        }
    }
}

/// Header of a record. Followed on disk by `block_count(value_len)` CRC32C
/// block checksums and then the value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    /// Data or tombstone.
    pub kind: RecordKind,
    /// [`RECORD_FLAG_LARGE`] and future flags.
    pub flags: u8,
    /// Value length in bytes.
    pub value_len: u32,
    /// Per-disk write sequence number.
    pub lsn: u64,
    /// The chunk identifier.
    pub key: ChunkId,
    /// Unix time (seconds) after which the record is expired; zero for never.
    pub expire_at: u64,
}

impl RecordHeader {
    /// Whether this record was written as a large batch.
    #[inline]
    pub fn is_large(&self) -> bool {
        self.flags & RECORD_FLAG_LARGE != 0
    }

    /// Whether this record was written in a framed batch.
    #[inline]
    pub fn is_framed(&self) -> bool {
        self.flags & RECORD_FLAG_FRAMED != 0
    }

    /// Length of the encoded header plus block checksums.
    #[inline]
    pub fn meta_len(&self) -> usize {
        record_meta_len(self.value_len)
    }

    /// Encodes the header and its block checksums into `buf`, which must be
    /// exactly [`RecordHeader::meta_len`] bytes.
    pub fn encode(&self, buf: &mut [u8], checksums: &[u32]) {
        assert_eq!(checksums.len(), block_count(self.value_len as u64) as usize);
        assert_eq!(buf.len(), self.meta_len());
        buf.fill(0);
        let mut w = Writer::new(buf);
        w.u32(RECORD_MAGIC)
            .u32(0) // crc
            .u8(self.kind as u8)
            .u8(self.flags)
            .u16(0)
            .u32(self.value_len)
            .u32(checksums.len() as u32)
            .u32(0)
            .u64(self.lsn)
            .bytes(self.key.as_bytes())
            .u64(self.expire_at)
            .skip(8);
        debug_assert_eq!(w.position(), RECORD_HEADER_LEN);
        for c in checksums {
            w.u32(*c);
        }
        codec::seal(buf, 4);
    }

    /// Decodes a record header and its checksums from the start of `buf`.
    ///
    /// `buf` may extend past the record metadata. Returns the header and a view
    /// of the block checksums (no copy), or `None` if the bytes are not a valid
    /// record header.
    pub fn decode(buf: &[u8]) -> Option<(Self, BlockChecksums<'_>)> {
        if buf.len() < RECORD_HEADER_LEN {
            return None;
        }
        let mut r = Reader::new(buf);
        if r.u32() != RECORD_MAGIC {
            return None;
        }
        r.skip(4);
        let kind = RecordKind::from_u8(r.u8())?;
        let flags = r.u8();
        r.skip(2);
        let value_len = r.u32();
        let n_blocks = r.u32();
        if n_blocks != block_count(value_len as u64) {
            return None;
        }
        let meta_len = record_meta_len(value_len);
        if buf.len() < meta_len || !codec::verify(&buf[..meta_len], 4) {
            return None;
        }
        r.skip(4);
        let lsn = r.u64();
        let key = ChunkId::from_bytes(r.array());
        let expire_at = r.u64();
        Some((
            Self {
                kind,
                flags,
                value_len,
                lsn,
                key,
                expire_at,
            },
            BlockChecksums(&buf[RECORD_HEADER_LEN..meta_len]),
        ))
    }
}

/// The per-block CRC32C array of a record, viewed in place in its on-disk
/// encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockChecksums<'a>(&'a [u8]);

impl BlockChecksums<'_> {
    /// Number of checksums.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len() / 4
    }

    /// Whether there are no checksums (an empty value).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The checksum of block `index`, if it exists.
    #[inline]
    pub fn get(&self, index: u32) -> Option<u32> {
        let start = index as usize * 4;
        self.0
            .get(start..start + 4)
            .map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")))
    }

    /// Copies the checksums out.
    pub fn to_vec(&self) -> Vec<u32> {
        (0..self.len() as u32).map(|i| self.get(i).expect("in range")).collect()
    }
}

// ---------------------------------------------------------------------------
// Footer
// ---------------------------------------------------------------------------

/// Header of a sealed segment's footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FooterHeader {
    /// Sequence number of the segment incarnation.
    pub seg_seq: u64,
    /// Number of [`FooterEntry`] that follow.
    pub entry_count: u32,
}

/// One record of a sealed segment, as listed in its footer. Carries exactly
/// what the index needs, so recovery rebuilds the index without reading data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FooterEntry {
    /// The chunk identifier.
    pub key: ChunkId,
    /// Offset of the record header within the segment.
    pub offset: u32,
    /// Offset of the value within the segment.
    pub value_off: u32,
    /// Value length in bytes.
    pub value_len: u32,
    /// The record's LSN.
    pub lsn: u64,
    /// CRC32C of the value's first checksum block (the whole value when it is
    /// at most one block long).
    pub crc: u32,
    /// Data or tombstone.
    pub kind: RecordKind,
    /// Record flags.
    pub flags: u8,
}

impl FooterEntry {
    fn encode(&self, buf: &mut [u8]) {
        Writer::new(buf)
            .bytes(self.key.as_bytes())
            .u32(self.offset)
            .u32(self.value_off)
            .u32(self.value_len)
            .u64(self.lsn)
            .u32(self.crc)
            .u8(self.kind as u8)
            .u8(self.flags)
            .skip(6);
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        let mut r = Reader::new(buf);
        let key = ChunkId::from_bytes(r.array());
        let offset = r.u32();
        let value_off = r.u32();
        let value_len = r.u32();
        let lsn = r.u64();
        let crc = r.u32();
        let kind = RecordKind::from_u8(r.u8()).ok_or_else(|| Error::corrupt("invalid footer entry kind"))?;
        let flags = r.u8();
        Ok(Self {
            key,
            offset,
            value_off,
            value_len,
            lsn,
            crc,
            kind,
            flags,
        })
    }
}

/// Returns the page-aligned footer length for `entry_count` entries.
pub fn footer_len(entry_count: usize) -> u64 {
    align_up((FOOTER_HEADER_LEN + FOOTER_ENTRY_LEN * entry_count) as u64, PAGE_SIZE)
}

/// Encodes a complete footer (header, entries, zero padding) into `buf`,
/// which must be exactly [`footer_len`] bytes.
pub fn encode_footer(seg_seq: u64, entries: &[FooterEntry], buf: &mut [u8]) {
    assert_eq!(buf.len() as u64, footer_len(entries.len()));
    buf.fill(0);
    Writer::new(buf)
        .u64(FOOTER_MAGIC)
        .u32(0) // crc
        .u32(entries.len() as u32)
        .u64(seg_seq);
    for (i, entry) in entries.iter().enumerate() {
        let start = FOOTER_HEADER_LEN + i * FOOTER_ENTRY_LEN;
        entry.encode(&mut buf[start..start + FOOTER_ENTRY_LEN]);
    }
    let used = FOOTER_HEADER_LEN + FOOTER_ENTRY_LEN * entries.len();
    codec::seal(&mut buf[..used], 8);
}

/// Decodes and validates a footer, checking it belongs to segment incarnation
/// `expected_seq`.
pub fn decode_footer(buf: &[u8], expected_seq: u64) -> Result<Vec<FooterEntry>> {
    if buf.len() < FOOTER_HEADER_LEN {
        return Err(Error::corrupt("footer too short"));
    }
    let mut r = Reader::new(buf);
    if r.u64() != FOOTER_MAGIC {
        return Err(Error::corrupt("footer magic mismatch"));
    }
    r.skip(4);
    let entry_count = r.u32() as usize;
    let seg_seq = r.u64();
    let used = FOOTER_HEADER_LEN + FOOTER_ENTRY_LEN * entry_count;
    if used > buf.len() {
        return Err(Error::corrupt("footer entry count exceeds footer length"));
    }
    if !codec::verify(&buf[..used], 8) {
        return Err(Error::corrupt("footer checksum mismatch"));
    }
    if seg_seq != expected_seq {
        return Err(Error::corrupt(format!(
            "footer belongs to segment incarnation {seg_seq}, expected {expected_seq}"
        )));
    }
    (0..entry_count)
        .map(|i| {
            let start = FOOTER_HEADER_LEN + i * FOOTER_ENTRY_LEN;
            FooterEntry::decode(&buf[start..start + FOOTER_ENTRY_LEN])
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Record geometry
// ---------------------------------------------------------------------------

/// Length of a record header plus its block checksums.
#[inline]
pub fn record_meta_len(value_len: u32) -> usize {
    RECORD_HEADER_LEN + 4 * block_count(value_len as u64) as usize
}

/// Offset of the value from the start of a large batch: the headers rounded
/// up to the next page so the value is page aligned.
#[inline]
pub fn large_value_offset(value_len: u32) -> u64 {
    align_up((BATCH_HEADER_LEN + record_meta_len(value_len)) as u64, PAGE_SIZE)
}

/// Total length of a large batch holding a value of `value_len` bytes.
#[inline]
pub fn large_batch_len(value_len: u32) -> u64 {
    align_up(large_value_offset(value_len) + value_len as u64, PAGE_SIZE)
}

/// Length an inline record occupies, including alignment.
#[inline]
pub fn inline_record_len(value_len: u32) -> u64 {
    align_up((record_meta_len(value_len) + value_len as usize) as u64, RECORD_ALIGN)
}

/// Whether a value of `value_len` bytes should be written framed rather than
/// inline: only when an inline header would push the value across one more
/// page boundary than the value alone needs, i.e. `value_len` is at most a
/// header short of a page multiple. Framing then costs less than one header of
/// padding and saves a page per read.
#[inline]
pub fn prefer_framed(value_len: u32) -> bool {
    let meta = record_meta_len(value_len) as u64;
    align_up(value_len as u64, PAGE_SIZE) < align_up(value_len as u64 + meta, PAGE_SIZE)
}

/// A page-aligned byte range within a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Extent {
    /// Start offset within the segment.
    pub start: u64,
    /// Length in bytes.
    pub len: u64,
}

/// Describes the pages covering a value range and an optional record header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordGeometry {
    /// The aligned range that must be read.
    pub extent: Extent,
    /// Offset of the record header relative to `extent.start`, if the header
    /// is within the extent.
    pub header_in_extent: Option<u64>,
    /// The requested value range relative to `extent.start`.
    pub data_in_extent: Range<usize>,
}

impl RecordGeometry {
    /// Computes the pages covering `range` within the value. The caller clamps
    /// it to `value_len` and expands it to checksum blocks when verifying.
    /// `with_header` also includes the complete record metadata.
    pub fn new(header_off: u64, value_off: u64, value_len: u32, range: Range<u64>, with_header: bool) -> Self {
        let data_start = value_off + range.start;
        let data_end = value_off + range.end;
        // Empty ranges use the preceding value byte when possible, so a range
        // at the end of a page-aligned value does not read past the record.
        let anchor = if range.is_empty() && range.start > 0 {
            data_start - 1
        } else {
            data_start
        };
        let mut start = align_down(anchor, PAGE_SIZE);
        let mut end = align_up(data_end.max(anchor + 1), PAGE_SIZE);
        if with_header {
            let meta = record_meta_len(value_len) as u64;
            start = start.min(align_down(header_off, PAGE_SIZE));
            end = end.max(align_up(header_off + meta, PAGE_SIZE));
        }
        Self {
            extent: Extent {
                start,
                len: end - start,
            },
            header_in_extent: with_header.then(|| header_off - start),
            data_in_extent: (data_start - start) as usize..(data_end - start) as usize,
        }
    }

    /// Bytes the record accounts for in its segment's live-byte counter.
    pub fn footprint(value_len: u32, flags: u8) -> u64 {
        if flags & RECORD_FLAG_LARGE != 0 {
            large_batch_len(value_len)
        } else if flags & RECORD_FLAG_FRAMED != 0 {
            align_up(value_len as u64, PAGE_SIZE) + record_meta_len(value_len) as u64
        } else {
            inline_record_len(value_len)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn superblock_roundtrip_and_corruption() {
        let sb = Superblock {
            generation: 3,
            disk_uuid: [7; 16],
            segment_size: 1 << 20,
            chunk_max: 1 << 16,
            segment_count: 100,
            created_at: 42,
        };
        let mut buf = vec![0u8; SUPERBLOCK_LEN];
        sb.encode(&mut buf);
        assert_eq!(Superblock::decode(&buf).unwrap(), sb);
        buf[40] ^= 1;
        assert!(matches!(Superblock::decode(&buf), Err(Error::Corrupt(_))));
        buf.fill(0);
        assert!(matches!(Superblock::decode(&buf), Err(Error::Unformatted(_))));
    }

    #[test]
    fn segment_header_roundtrip() {
        let hdr = SegmentHeader {
            disk_uuid: [1; 16],
            seg_no: 9,
            state: SegmentState::Sealed,
            kind: SegmentKind::Cold,
            seq: 77,
            footer_offset: 8192,
            footer_len: 4096,
            record_count: 12,
        };
        let mut buf = vec![0u8; SEGMENT_HEADER_LEN as usize];
        hdr.encode(&mut buf);
        assert_eq!(SegmentHeader::decode(&buf).unwrap(), hdr);
        buf[36] = 9; // invalid state byte, checksum now wrong too
        assert!(SegmentHeader::decode(&buf).is_err());
    }

    #[test]
    fn batch_header_roundtrip() {
        let hdr = BatchHeader {
            seg_seq: 5,
            batch_len: 8192,
            record_count: 3,
            first_lsn: 100,
            kind: BatchKind::Framed,
            header_len: 4096,
        };
        let mut buf = vec![0u8; BATCH_HEADER_LEN];
        hdr.encode(&mut buf);
        assert_eq!(BatchHeader::decode(&buf).unwrap(), hdr);
        assert!(BatchHeader::decode(&[0u8; 64]).is_none());
        buf[16] ^= 1;
        assert!(BatchHeader::decode(&buf).is_none());
    }

    #[test]
    fn record_header_roundtrip() {
        let hdr = RecordHeader {
            kind: RecordKind::Data,
            flags: RECORD_FLAG_LARGE,
            value_len: 65536 * 2 + 1,
            lsn: 9,
            key: ChunkId::from_u128(123),
            expire_at: 0,
        };
        let sums = [1u32, 2, 3];
        let mut buf = vec![0u8; hdr.meta_len()];
        hdr.encode(&mut buf, &sums);
        let mut extended = buf.clone();
        extended.extend_from_slice(&[0xaa; 100]);
        let (decoded, decoded_sums) = RecordHeader::decode(&extended).unwrap();
        assert_eq!(decoded, hdr);
        assert_eq!(decoded_sums.to_vec(), sums);
        assert_eq!(decoded_sums.get(2), Some(3));
        assert_eq!(decoded_sums.get(3), None);
        buf[RECORD_HEADER_LEN + 4] ^= 1; // corrupt second checksum
        assert!(RecordHeader::decode(&buf).is_none());
        // Truncated metadata is rejected rather than read out of bounds.
        assert!(RecordHeader::decode(&extended[..RECORD_HEADER_LEN + 4]).is_none());
    }

    #[test]
    fn footer_roundtrip() {
        let entries: Vec<FooterEntry> = (0..100u32)
            .map(|i| FooterEntry {
                key: ChunkId::from_u128(i as u128),
                offset: i * 4096,
                value_off: i * 4096 + 68,
                value_len: i,
                lsn: i as u64,
                crc: i.wrapping_mul(0x9e37_79b9),
                kind: if i % 7 == 0 {
                    RecordKind::Tombstone
                } else {
                    RecordKind::Data
                },
                flags: (i % 2) as u8,
            })
            .collect();
        let len = footer_len(entries.len());
        assert_eq!(len % PAGE_SIZE, 0);
        let mut buf = vec![0u8; len as usize];
        encode_footer(11, &entries, &mut buf);
        assert_eq!(decode_footer(&buf, 11).unwrap(), entries);
        assert!(decode_footer(&buf, 12).is_err());
        buf[FOOTER_HEADER_LEN + 20] ^= 1;
        assert!(decode_footer(&buf, 11).is_err());
    }

    #[test]
    fn geometry() {
        // Large record: header page then value, read together.
        let g = RecordGeometry::new(4096 + BATCH_HEADER_LEN as u64, 8192, 100_000, 0..100_000, true);
        assert_eq!(g.extent.start, 4096);
        assert_eq!(g.header_in_extent, Some(BATCH_HEADER_LEN as u64));
        assert_eq!(g.data_in_extent, 4096..104_096);
        assert_eq!(g.extent.len, align_up(4096 + 100_000, PAGE_SIZE));

        // Inline record in the middle of a page.
        let meta = record_meta_len(500) as u64;
        let g = RecordGeometry::new(8192 + 1000, 8192 + 1000 + meta, 500, 0..500, true);
        assert_eq!(g.extent, Extent { start: 8192, len: 4096 });
        assert_eq!(g.header_in_extent, Some(1000));
        assert_eq!(g.data_in_extent, (1000 + meta) as usize..(1500 + meta) as usize);

        // Framed record read without its header: exactly the value's pages.
        let g = RecordGeometry::new(64, 3 * 4096, 4096, 0..4096, false);
        assert_eq!(
            g.extent,
            Extent {
                start: 3 * 4096,
                len: 4096
            }
        );
        assert_eq!(g.header_in_extent, None);
        assert_eq!(g.data_in_extent, 0..4096);
        // ...and with the header: back to the batch's header area.
        let g = RecordGeometry::new(64, 3 * 4096, 4096, 0..4096, true);
        assert_eq!(
            g.extent,
            Extent {
                start: 0,
                len: 4 * 4096
            }
        );
        assert_eq!(g.header_in_extent, Some(64));

        // An empty value still reads its header page.
        let g = RecordGeometry::new(100, 100 + 64, 0, 0..0, true);
        assert_eq!(g.extent, Extent { start: 0, len: 4096 });
    }

    #[test]
    fn range_geometry_avoids_unrequested_pages() {
        let g = RecordGeometry::new(4160, 8192, 131072, 70000..70010, false);
        assert_eq!(
            g.extent,
            Extent {
                start: 77824,
                len: 4096
            }
        );
        assert_eq!(g.data_in_extent, 368..378);
        assert_eq!(g.header_in_extent, None);

        // A verified first block includes the header but not the second block.
        let g = RecordGeometry::new(4160, 8192, 131072, 0..65536, true);
        assert_eq!(
            g.extent,
            Extent {
                start: 4096,
                len: 69632
            }
        );
        assert_eq!(g.data_in_extent, 4096..69632);
        assert_eq!(g.header_in_extent, Some(64));

        // Empty ranges at EOF must not cross into the next page.
        let g = RecordGeometry::new(4160, 8192, 131072, 131072..131072, false);
        assert_eq!(
            g.extent,
            Extent {
                start: 135168,
                len: 4096
            }
        );
        assert_eq!(g.data_in_extent, 4096..4096);
    }

    #[test]
    fn framing_rule() {
        assert!(!prefer_framed(100));
        assert!(!prefer_framed(4000));
        // 4028 + 68 bytes of header fit one page exactly; one byte more spills.
        assert!(!prefer_framed(4028));
        assert!(prefer_framed(4029));
        assert!(prefer_framed(4096));
        assert!(!prefer_framed(4097));
        assert!(!prefer_framed(5000));
        assert!(prefer_framed(8192));
        assert!(prefer_framed(65536));
    }
}
