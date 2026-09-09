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

//! Forward scanning of segment contents.
//!
//! Recovery (for unsealed segments) and reclaim (for every victim) both walk a
//! segment batch by batch. The scan stops at the first byte range that is not a
//! well-formed batch of the segment's current incarnation, which is exactly the
//! end of the log for an active segment and the footer for a sealed one.

use std::ops::ControlFlow;

use moat_common::{AlignedBuf, PAGE_SIZE, align_up, verify_blocks_with};

use crate::{
    error::{Error, Result},
    layout::{
        BATCH_HEADER_LEN, BatchHeader, BatchKind, BlockChecksums, RECORD_ALIGN, RecordHeader, large_batch_len,
        large_value_offset,
    },
    shared::Shared,
};

/// A record decoded out of a batch buffer.
pub(crate) struct ParsedRecord<'a> {
    pub(crate) header: RecordHeader,
    pub(crate) checksums: BlockChecksums<'a>,
    /// Offset of the record header from the start of the batch.
    pub(crate) offset_in_batch: usize,
    /// Offset of the value from the start of the batch.
    pub(crate) value_in_batch: usize,
    pub(crate) value: &'a [u8],
}

/// Decodes every record in `batch`, optionally verifying value checksums.
///
/// Fails if the batch is structurally inconsistent. Callers treat a failure as
/// "the rest of this segment is unreadable".
pub(crate) fn parse_batch<'a>(batch: &'a [u8], header: &BatchHeader, verify: bool) -> Result<Vec<ParsedRecord<'a>>> {
    let count = header.record_count as usize;
    let mut records = Vec::with_capacity(count);
    let mut pos = BATCH_HEADER_LEN;
    // Framed batches: the header area ends at `header_len`, values follow page
    // aligned in record order.
    let mut framed_value = header.header_len as usize;
    if header.kind == BatchKind::Framed
        && (framed_value < BATCH_HEADER_LEN
            || framed_value > batch.len()
            || !(framed_value as u64).is_multiple_of(PAGE_SIZE))
    {
        return Err(Error::corrupt("framed batch has an invalid header area length"));
    }
    for i in 0..count {
        if header.kind == BatchKind::Inline {
            // An inline record may have been moved to the next page to keep it
            // from straddling; the gap is zero-filled (a record header never
            // starts with four zero bytes because its magic comes first).
            if pos + 4 <= batch.len() && batch[pos..pos + 4] == [0; 4] {
                pos = align_up(pos as u64, PAGE_SIZE) as usize;
            }
        }
        if pos >= batch.len() {
            return Err(Error::corrupt(format!("record {i} starts past the end of the batch")));
        }
        let (rec, checksums) = RecordHeader::decode(&batch[pos..])
            .ok_or_else(|| Error::corrupt(format!("record {i} header invalid at batch offset {pos}")))?;
        let meta = rec.meta_len();
        let value_start = match header.kind {
            BatchKind::Large => {
                if count != 1 || pos != BATCH_HEADER_LEN || !rec.is_large() {
                    return Err(Error::corrupt("large batch must hold exactly one large record"));
                }
                large_value_offset(rec.value_len) as usize
            }
            BatchKind::Inline => {
                if rec.is_large() || rec.is_framed() {
                    return Err(Error::corrupt(format!(
                        "record {i} kind does not match its inline batch"
                    )));
                }
                pos + meta
            }
            BatchKind::Framed => {
                if !rec.is_framed() || pos + meta > header.header_len as usize {
                    return Err(Error::corrupt(format!(
                        "record {i} does not fit the framed header area"
                    )));
                }
                framed_value
            }
        };
        let value_end = value_start + rec.value_len as usize;
        if value_end > batch.len() {
            return Err(Error::corrupt(format!("record {i} value overruns batch")));
        }
        let value = &batch[value_start..value_end];
        if verify && let Err(block) = verify_blocks_with(value, 0, |b| checksums.get(b)) {
            return Err(Error::corrupt(format!(
                "record {i} ({}) block {block} checksum mismatch",
                rec.key
            )));
        }
        records.push(ParsedRecord {
            header: rec,
            checksums,
            offset_in_batch: pos,
            value_in_batch: value_start,
            value,
        });
        match header.kind {
            BatchKind::Framed => {
                pos += meta;
                framed_value = align_up(value_end as u64, PAGE_SIZE) as usize;
            }
            _ => pos = align_up(value_end as u64, RECORD_ALIGN) as usize,
        }
    }
    Ok(records)
}

/// What [`next_batch`] found at a position in a window.
pub(crate) enum BatchStep {
    /// A well-formed batch of `len` bytes starts here.
    Batch(BatchHeader, usize),
    /// The next header or batch extends past the window; the caller must
    /// read another window starting at this position.
    NeedMore,
    /// Not a batch of this segment incarnation: the end of valid data.
    End,
}

/// Inspects the batch header at `window[rel..]`.
///
/// `remaining` is the number of bytes from this position to the end of the
/// scannable region of the segment; `max_batch` bounds the largest batch the
/// engine could have written.
pub(crate) fn next_batch(window: &[u8], rel: usize, seg_seq: u64, max_batch: u64, remaining: u64) -> BatchStep {
    if remaining < BATCH_HEADER_LEN as u64 {
        return BatchStep::End;
    }
    if rel + BATCH_HEADER_LEN > window.len() {
        return BatchStep::NeedMore;
    }
    let Some(header) = BatchHeader::decode(&window[rel..]) else {
        return BatchStep::End;
    };
    let len = header.batch_len as u64;
    if header.seg_seq != seg_seq
        || len < PAGE_SIZE
        || !len.is_multiple_of(PAGE_SIZE)
        || len > max_batch
        || len > remaining
    {
        return BatchStep::End;
    }
    if rel + len as usize > window.len() {
        return BatchStep::NeedMore;
    }
    BatchStep::Batch(header, len as usize)
}

/// Largest batch the engine can write for this device.
pub(crate) fn max_batch_len(shared: &Shared) -> u64 {
    // A packed batch is bounded by the pool class its staging buffer comes
    // from, which is the batch limit rounded up to a power of two.
    large_batch_len(shared.superblock.chunk_max).max((shared.options.batch_limit as u64).next_power_of_two())
}

/// Walks the batches of segment `seg_no` (incarnation `seg_seq`) from offset
/// `start` to `end` using blocking reads, calling `f` with each batch's
/// offset, header and bytes. Used by recovery, where blocking is fine.
///
/// Returns the offset at which the scan stopped: either `end`, the first
/// offset that did not hold a valid batch, or the batch at which `f` broke out.
pub(crate) fn scan_batches_blocking(
    shared: &Shared,
    seg_no: u32,
    seg_seq: u64,
    start: u64,
    end: u64,
    mut f: impl FnMut(u64, &BatchHeader, &[u8]) -> Result<ControlFlow<()>>,
) -> Result<u64> {
    debug_assert!(start.is_multiple_of(PAGE_SIZE) && end.is_multiple_of(PAGE_SIZE));
    let max_batch = max_batch_len(shared);
    let window = align_up(shared.options.scan_window as u64, PAGE_SIZE).max(max_batch) as usize;
    let mut buf = AlignedBuf::zeroed(window);
    let mut cursor = start;
    let base = shared.geometry.segment_offset(seg_no);

    while cursor < end {
        let len = (end - cursor).min(window as u64) as usize;
        shared.device.read_at(&mut buf[..len], base + cursor)?;
        let mut rel = 0usize;
        loop {
            match next_batch(&buf[..len], rel, seg_seq, max_batch, end - cursor - rel as u64) {
                BatchStep::Batch(header, batch_len) => {
                    let off = cursor + rel as u64;
                    if let ControlFlow::Break(()) = f(off, &header, &buf[rel..rel + batch_len])? {
                        return Ok(off);
                    }
                    rel += batch_len;
                }
                // The window always holds at least one maximal batch, so
                // `NeedMore` means the next batch straddles the window end
                // or starts just beyond it: continue from its start.
                BatchStep::NeedMore => break,
                BatchStep::End => return Ok(cursor + rel as u64),
            }
        }
        if rel == 0 {
            // A batch claimed to be larger than the window: unreadable.
            return Ok(cursor);
        }
        cursor += rel as u64;
    }
    Ok(cursor)
}
