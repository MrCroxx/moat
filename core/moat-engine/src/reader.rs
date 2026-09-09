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

//! Read pipelines.
//!
//! A [`Reader`] is a disk's read pipeline bound to one queue: it looks chunks
//! up in the shared index, submits the reads, optionally verifies them and
//! hands out [`ChunkData`], a view into the pool buffer the device wrote into.
//! No byte of the value is copied by the engine; the buffer is released when
//! the `ChunkData` is dropped. Every worker holds one reader per disk on its
//! own queue, so a read is served on the thread that received the request.
//!
//! Nothing here blocks: a read the queue has no room for waits in the
//! reader's ready queue and is pushed by the next [`Reader::poll`];
//! [`Error::Busy`] is reported only when the pool has no buffer for it.

use std::{
    collections::VecDeque,
    io,
    ops::{Deref, Range},
    sync::Arc,
};

use moat_common::{CHECKSUM_BLOCK_SIZE, ChunkId, PooledBuf, verify_blocks_with};

use crate::{
    engine::ChunkStat,
    error::{Error, Result},
    index::{IndexValue, ReaderSlot},
    io::{Descriptor, IoCompletion, IoQueue},
    layout::{RecordGeometry, RecordHeader, RecordKind},
    shared::Shared,
};

/// A value (or part of one) in the buffer the device read it into.
/// Verification is controlled by [`Options::verify_reads`](crate::Options::verify_reads).
///
/// Dereferences to the requested bytes. Dropping it returns the buffer to the
/// pool; [`ChunkData::into_raw`] hands the buffer over for callers that DMA
/// out of it directly.
pub struct ChunkData {
    buf: PooledBuf,
    range: Range<usize>,
}

impl ChunkData {
    /// The underlying pool buffer and the byte range of the value in it.
    pub fn into_raw(self) -> (PooledBuf, Range<usize>) {
        (self.buf, self.range)
    }
}

impl Deref for ChunkData {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.buf[self.range.clone()]
    }
}

impl AsRef<[u8]> for ChunkData {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl std::fmt::Debug for ChunkData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkData").field("len", &self.range.len()).finish()
    }
}

/// Immediate result of [`Reader::get`].
#[derive(Debug, PartialEq, Eq)]
pub enum ReadOutcome {
    /// The chunk is not in the index; nothing was submitted.
    Miss,
    /// A read was accepted; its [`ReadCompletion`] carries the caller's token.
    Submitted,
}

/// A finished read.
#[derive(Debug)]
pub struct ReadCompletion {
    /// The token passed to [`Reader::get`].
    pub token: u64,
    /// The bytes, or an error.
    pub result: Result<ChunkData>,
}

struct PendingRead {
    id: ChunkId,
    value: IndexValue,
    geometry: RecordGeometry,
    range: Range<usize>,
    first_block: u32,
    user_token: u64,
}

/// A read waiting for queue room.
struct ReadyRead {
    slot: u32,
    buf: PooledBuf,
    len: usize,
    offset: u64,
}

/// A disk's read pipeline on one queue: looks chunks up, submits reads through
/// the caller's queue and hands back zero-copy views. The io_uring backend
/// never blocks; the synchronous development backend performs I/O inline.
pub struct Reader {
    shared: Arc<Shared>,
    desc: Descriptor,
    slot: ReaderSlot,
    /// Reads accepted and not yet finished, indexed by the slot number used
    /// as the I/O token.
    pending: Vec<Option<PendingRead>>,
    free_slots: Vec<u32>,
    ready: VecDeque<ReadyRead>,
    scratch: Vec<IoCompletion>,
}

impl Reader {
    pub(crate) fn new(shared: Arc<Shared>, desc: Descriptor, slot: ReaderSlot) -> Self {
        Self {
            shared,
            desc,
            slot,
            pending: Vec::new(),
            free_slots: Vec::new(),
            ready: VecDeque::new(),
            scratch: Vec::new(),
        }
    }

    /// Returns metadata about a chunk without touching the disk.
    pub fn stat(&self, id: &ChunkId) -> Option<ChunkStat> {
        self.shared.index.get(&self.slot, id).map(|v| ChunkStat::from_value(&v))
    }

    /// Whether a chunk exists in the index.
    pub fn contains(&self, id: &ChunkId) -> bool {
        self.shared.index.get(&self.slot, id).is_some()
    }

    /// Looks a chunk up and, if present, submits the read covering `range`
    /// (the whole value when `None`). A range past the end is clamped.
    ///
    /// Fails with [`Error::Busy`] when the pool has no buffer for the read;
    /// poll and retry.
    pub fn get(
        &mut self,
        q: &mut dyn IoQueue,
        id: &ChunkId,
        range: Option<Range<u64>>,
        token: u64,
    ) -> Result<ReadOutcome> {
        let shared = &*self.shared;
        let Some(value) = shared.index.get_and_pin(&self.slot, id, &shared.segments) else {
            return Ok(ReadOutcome::Miss);
        };
        let total = value.value_len as u64;
        let range = match range {
            None => 0..total,
            Some(r) => {
                let start = r.start.min(total);
                start..r.end.min(total).max(start)
            }
        };
        let verify = shared.options.verify_reads;
        let mut covered = range.clone();
        if verify && !range.is_empty() {
            let block = CHECKSUM_BLOCK_SIZE as u64;
            covered.start = range.start / block * block;
            covered.end = (range.end.div_ceil(block) * block).min(total);
        }
        let first_block = (covered.start / CHECKSUM_BLOCK_SIZE as u64) as u32;
        let geometry = RecordGeometry::new(
            value.loc.offset as u64,
            value.value_off as u64,
            value.value_len,
            covered,
            verify,
        );
        let range = (value.value_off as u64 + range.start - geometry.extent.start) as usize
            ..(value.value_off as u64 + range.end - geometry.extent.start) as usize;
        let len = geometry.extent.len as usize;
        let Some(buf) = q.pool().alloc(len) else {
            shared.segments.unpin(value.loc.seg_no);
            return Err(Error::Busy);
        };
        let offset = shared.geometry.segment_offset(value.loc.seg_no) + geometry.extent.start;
        let slot = match self.free_slots.pop() {
            Some(s) => s,
            None => {
                self.pending.push(None);
                (self.pending.len() - 1) as u32
            }
        };
        self.pending[slot as usize] = Some(PendingRead {
            id: *id,
            value,
            geometry,
            range,
            first_block,
            user_token: token,
        });
        if let Err(buf) = q.read(self.desc, buf, len, offset, slot as u64) {
            self.ready.push_back(ReadyRead { slot, buf, len, offset });
        }
        Ok(ReadOutcome::Submitted)
    }

    /// Finishes reads, appends them to `out`, and pushes reads that
    /// were waiting for queue room. With io_uring, never waits. Returns the number appended.
    pub fn poll(&mut self, q: &mut dyn IoQueue, out: &mut Vec<ReadCompletion>) -> Result<usize> {
        // A worker visits every disk, but often has requests on only a few.
        // An idle reader cannot have completions in its private descriptor.
        if self.in_flight() == 0 {
            return Ok(0);
        }
        self.scratch.clear();
        q.take(self.desc, &mut self.scratch);
        let mut n = 0;
        let mut finished = std::mem::take(&mut self.scratch);
        for done in finished.drain(..) {
            let slot = done.token as u32;
            let Some(pending) = self.pending[slot as usize].take() else {
                continue;
            };
            self.free_slots.push(slot);
            let result = self.finish(&pending, done);
            self.shared.segments.unpin(pending.value.loc.seg_no);
            out.push(ReadCompletion {
                token: pending.user_token,
                result,
            });
            n += 1;
        }
        self.scratch = finished;
        while q.vacant() > 0 {
            let Some(r) = self.ready.pop_front() else {
                break;
            };
            if let Err(buf) = q.read(self.desc, r.buf, r.len, r.offset, r.slot as u64) {
                self.ready.push_front(ReadyRead { buf, ..r });
                break;
            }
        }
        Ok(n)
    }

    /// Reads accepted and not yet finished (including those waiting for
    /// queue room).
    pub fn in_flight(&self) -> usize {
        self.pending.len() - self.free_slots.len()
    }

    /// Closes the descriptor. Reads still in flight are abandoned (their
    /// buffers return to the pool when they complete).
    pub fn detach(self, q: &mut dyn IoQueue) {
        q.detach(self.desc);
        drop(self);
    }

    /// Applies optional verification and returns the requested range.
    fn finish(&self, pending: &PendingRead, done: IoCompletion) -> Result<ChunkData> {
        let id = &pending.id;
        let value = &pending.value;
        let geometry = &pending.geometry;
        let buf = match done.result {
            Ok(n) if n as u64 == geometry.extent.len => done.buf.expect("read returns its buffer"),
            Ok(n) => {
                return Err(Error::Io(io::Error::other(format!(
                    "chunk {id}: short read {n} of {} bytes",
                    geometry.extent.len
                ))));
            }
            Err(e) => return Err(Error::Io(e)),
        };

        if let Some(header_at) = geometry.header_in_extent {
            let (header, checksums) = RecordHeader::decode(&buf[header_at as usize..])
                .ok_or_else(|| Error::corrupt(format!("chunk {id}: record header invalid")))?;
            if header.key != *id || header.lsn != value.lsn || header.value_len != value.value_len {
                return Err(Error::corrupt(format!(
                    "chunk {id}: record header does not match index"
                )));
            }
            if header.kind != RecordKind::Data {
                return Err(Error::corrupt(format!("chunk {id}: index points at a tombstone")));
            }
            if self.shared.options.verify_reads {
                let covered = &buf[geometry.data_in_extent.clone()];
                if let Err(block) = verify_blocks_with(covered, pending.first_block, |i| checksums.get(i)) {
                    return Err(Error::corrupt(format!("chunk {id}: checksum block {block} mismatch")));
                }
            }
        }
        Ok(ChunkData {
            buf,
            range: pending.range.clone(),
        })
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        for pending in self.pending.iter().flatten() {
            self.shared.segments.unpin(pending.value.loc.seg_no);
        }
        // The slot is a `Copy`-free token; hand it back by value.
        let slot = std::mem::replace(&mut self.slot, ReaderSlot::detached());
        if !slot.is_detached() {
            self.shared.index.unregister(slot);
        }
    }
}
