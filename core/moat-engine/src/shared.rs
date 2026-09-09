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

//! State shared by the writer and all readers of one engine instance.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64},
};

use moat_common::{AlignedBuf, PAGE_SIZE};

use crate::{
    device::Device,
    error::Result,
    index::Index,
    layout::{Extent, SEGMENT_HEADER_LEN, SegmentHeader, Superblock},
    options::Options,
    segments::{Geometry, SegmentTable},
};

pub(crate) struct Shared {
    pub(crate) device: Arc<dyn Device>,
    pub(crate) superblock: Superblock,
    pub(crate) geometry: Geometry,
    pub(crate) index: Arc<Index>,
    pub(crate) segments: SegmentTable,
    pub(crate) options: Options,
    /// Whether a `Writer` currently exists for this engine.
    pub(crate) writer_taken: AtomicBool,
    /// Written only by the writer (and recovery): the LSN the next record
    /// receives and the sequence number the next segment allocation receives,
    /// so a writer re-created after `detach` continues where the previous one
    /// stopped.
    pub(crate) next_lsn: AtomicU64,
    pub(crate) next_seq: AtomicU64,
}

impl Shared {
    /// Reads an aligned extent of a segment into a fresh buffer.
    pub(crate) fn read_extent(&self, seg_no: u32, extent: Extent) -> Result<AlignedBuf> {
        let mut buf = AlignedBuf::zeroed(extent.len as usize);
        self.device
            .read_at(&mut buf, self.geometry.segment_offset(seg_no) + extent.start)?;
        Ok(buf)
    }

    /// Writes `data` at `offset` within segment `seg_no` (blocking; recovery
    /// only).
    pub(crate) fn write_segment_bytes(&self, seg_no: u32, offset: u64, data: &[u8]) -> Result<()> {
        debug_assert!(offset.is_multiple_of(PAGE_SIZE) && (data.len() as u64).is_multiple_of(PAGE_SIZE));
        self.device
            .write_at(data, self.geometry.segment_offset(seg_no) + offset)?;
        Ok(())
    }

    pub(crate) fn read_segment_header(&self, seg_no: u32) -> Result<SegmentHeader> {
        let mut buf = AlignedBuf::zeroed(SEGMENT_HEADER_LEN as usize);
        self.device.read_at(&mut buf, self.geometry.segment_offset(seg_no))?;
        SegmentHeader::decode(&buf)
    }

    /// Writes a segment header (blocking; recovery only).
    pub(crate) fn write_segment_header(&self, header: &SegmentHeader) -> Result<()> {
        let mut buf = AlignedBuf::zeroed(SEGMENT_HEADER_LEN as usize);
        header.encode(&mut buf);
        self.device
            .write_at(&buf, self.geometry.segment_offset(header.seg_no))?;
        Ok(())
    }

    /// A segment header for `seg_no` in `state`.
    pub(crate) fn segment_header(
        &self,
        seg_no: u32,
        state: crate::layout::SegmentState,
        kind: crate::layout::SegmentKind,
        seq: u64,
    ) -> SegmentHeader {
        SegmentHeader {
            disk_uuid: self.superblock.disk_uuid,
            seg_no,
            state,
            kind,
            seq,
            footer_offset: 0,
            footer_len: 0,
            record_count: 0,
        }
    }
}
