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

//! In-memory segment bookkeeping shared between the writer and readers.

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

use crate::layout::{SEGMENT_HEADER_LEN, SegmentKind, SegmentState};

/// Where segments live on the device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Geometry {
    /// Size of every segment in bytes.
    pub segment_size: u64,
    /// Number of segments.
    pub segment_count: u32,
}

impl Geometry {
    /// Computes the geometry for a device of `device_len` bytes.
    ///
    /// The first `segment_size` bytes of the device are reserved for the
    /// superblocks; segments follow back to back.
    pub fn for_device(device_len: u64, segment_size: u64) -> Self {
        let segment_count = (device_len / segment_size).saturating_sub(1);
        Self {
            segment_size,
            segment_count: segment_count.min(u32::MAX as u64) as u32,
        }
    }

    /// Device offset of segment `seg_no`.
    #[inline]
    pub fn segment_offset(&self, seg_no: u32) -> u64 {
        self.segment_size * (seg_no as u64 + 1)
    }

    /// Offset within a segment where record data starts.
    #[inline]
    pub const fn data_start(&self) -> u64 {
        SEGMENT_HEADER_LEN
    }
}

struct SegmentMeta {
    state: AtomicU8,
    kind: AtomicU8,
    seq: AtomicU64,
    live_bytes: AtomicU64,
    pins: AtomicU32,
    /// Sealed segments: offset where record data ends (and the footer starts).
    data_end: AtomicU64,
}

/// Per-segment state visible to all threads.
///
/// The writer owns every transition; readers only pin and unpin. All fields are
/// atomics so readers never block on the writer.
pub struct SegmentTable {
    metas: Box<[SegmentMeta]>,
}

impl SegmentTable {
    /// Creates a table of `count` free segments.
    pub fn new(count: u32) -> Self {
        let metas = (0..count)
            .map(|_| SegmentMeta {
                state: AtomicU8::new(SegmentState::Free as u8),
                kind: AtomicU8::new(SegmentKind::Hot as u8),
                seq: AtomicU64::new(0),
                live_bytes: AtomicU64::new(0),
                pins: AtomicU32::new(0),
                data_end: AtomicU64::new(0),
            })
            .collect();
        Self { metas }
    }

    /// Number of segments.
    pub fn len(&self) -> u32 {
        self.metas.len() as u32
    }

    #[inline]
    fn meta(&self, seg_no: u32) -> &SegmentMeta {
        &self.metas[seg_no as usize]
    }

    /// Lifecycle state of a segment.
    pub fn state(&self, seg_no: u32) -> SegmentState {
        match self.meta(seg_no).state.load(Ordering::Acquire) {
            1 => SegmentState::Active,
            2 => SegmentState::Sealed,
            _ => SegmentState::Free,
        }
    }

    /// Sequence number of the segment's current incarnation.
    pub fn seq(&self, seg_no: u32) -> u64 {
        self.meta(seg_no).seq.load(Ordering::Acquire)
    }

    /// Usage kind of the segment's current incarnation.
    pub fn kind(&self, seg_no: u32) -> SegmentKind {
        match self.meta(seg_no).kind.load(Ordering::Relaxed) {
            1 => SegmentKind::Cold,
            _ => SegmentKind::Hot,
        }
    }

    /// Offset within a sealed segment where its record data ends.
    pub fn data_end(&self, seg_no: u32) -> u64 {
        self.meta(seg_no).data_end.load(Ordering::Acquire)
    }

    /// Bytes of records in the segment that the index still points at.
    pub fn live_bytes(&self, seg_no: u32) -> u64 {
        self.meta(seg_no).live_bytes.load(Ordering::Relaxed)
    }

    /// Number of readers currently holding the segment.
    pub fn pins(&self, seg_no: u32) -> u32 {
        self.meta(seg_no).pins.load(Ordering::Acquire)
    }

    /// Marks the segment as in use by a reader.
    pub fn pin(&self, seg_no: u32) {
        self.meta(seg_no).pins.fetch_add(1, Ordering::AcqRel);
    }

    /// Releases a pin taken with [`SegmentTable::pin`].
    pub fn unpin(&self, seg_no: u32) {
        let prev = self.meta(seg_no).pins.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(prev > 0, "unpin without matching pin on segment {seg_no}");
    }

    pub(crate) fn set(&self, seg_no: u32, state: SegmentState, kind: SegmentKind, seq: u64) {
        let m = self.meta(seg_no);
        m.kind.store(kind as u8, Ordering::Relaxed);
        m.seq.store(seq, Ordering::Release);
        m.state.store(state as u8, Ordering::Release);
    }

    /// Marks the segment sealed with its data ending at `data_end`.
    pub(crate) fn set_sealed(&self, seg_no: u32, data_end: u64) {
        let m = self.meta(seg_no);
        m.data_end.store(data_end, Ordering::Release);
        m.state.store(SegmentState::Sealed as u8, Ordering::Release);
    }

    pub(crate) fn add_live(&self, seg_no: u32, bytes: u64) {
        self.meta(seg_no).live_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn sub_live(&self, seg_no: u32, bytes: u64) {
        let m = self.meta(seg_no);
        // Saturating rather than wrapping: recovery re-derives exact counts, and
        // an underflow here must never turn into a huge "live" value.
        let _ = m
            .live_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(bytes)));
    }

    pub(crate) fn reset_live(&self, seg_no: u32) {
        self.meta(seg_no).live_bytes.store(0, Ordering::Relaxed);
    }

    /// Iterates over all segment numbers.
    pub fn iter(&self) -> impl Iterator<Item = u32> {
        0..self.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geometry_reserves_first_segment() {
        let g = Geometry::for_device(10 << 20, 1 << 20);
        assert_eq!(g.segment_count, 9);
        assert_eq!(g.segment_offset(0), 1 << 20);
        assert_eq!(g.segment_offset(8), 9 << 20);
        assert_eq!(Geometry::for_device(1 << 20, 1 << 20).segment_count, 0);
    }

    #[test]
    fn live_bytes_saturate() {
        let t = SegmentTable::new(2);
        t.add_live(1, 10);
        t.sub_live(1, 100);
        assert_eq!(t.live_bytes(1), 0);
        t.set(1, SegmentState::Active, SegmentKind::Cold, 7);
        assert_eq!(t.state(1), SegmentState::Active);
        assert_eq!(t.seq(1), 7);
        assert_eq!(t.state(0), SegmentState::Free);
    }
}
