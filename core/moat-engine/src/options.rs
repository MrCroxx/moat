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

//! Runtime and format-time options.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use moat_common::{PAGE_SIZE, is_aligned};

use crate::{
    error::{Error, Result},
    layout::{BATCH_HEADER_LEN, SEGMENT_HEADER_LEN, footer_len, large_batch_len},
};

/// A source of wall-clock time in seconds, used for record expiry.
pub trait Clock: Send + Sync + 'static {
    /// Current Unix time in seconds.
    fn now_secs(&self) -> u64;
}

/// The system clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

/// A clock that only moves when told to. Intended for tests.
#[derive(Debug, Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    /// Creates a clock at `now`.
    pub fn new(now: u64) -> Self {
        Self(AtomicU64::new(now))
    }

    /// Sets the current time.
    pub fn set(&self, now: u64) {
        self.0.store(now, Ordering::Relaxed);
    }

    /// Advances the current time by `secs`.
    pub fn advance(&self, secs: u64) {
        self.0.fetch_add(secs, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_secs(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Runtime options. None of these affect the on-disk format; a device may be
/// opened with different options every time.
#[derive(Clone)]
pub struct Options {
    /// Clock used for expiry decisions.
    pub clock: Arc<dyn Clock>,
    /// Values shorter than this are packed together with other small records
    /// into one batch; longer values get a large batch of their own and are
    /// written without copying. Default: 64 KiB.
    pub pack_threshold: u32,
    /// A pending packed batch is flushed once it reaches this many bytes.
    /// Default: 1 MiB.
    pub batch_limit: usize,
    /// Read window used when scanning segments (recovery, reclaim). Grows
    /// automatically to fit the largest possible batch; reclaim shrinks it
    /// to the queue pool's largest class. Default: 4 MiB.
    pub scan_window: usize,
    /// Initial number of index slots (rounded up to a power of two; 64 bytes
    /// each). The table doubles as needed within `index_memory_budget`.
    /// Default: 1 Mi slots (64 MiB).
    pub index_capacity: usize,
    /// Maximum bytes the index table may occupy. Once reached, puts of new
    /// chunks fail with [`Error::IndexFull`](crate::Error::IndexFull).
    /// Default: unlimited.
    pub index_memory_budget: usize,
    /// Whether `flush` also flushes the device's volatile write cache. Leave
    /// off on devices with power-loss protection. Default: `false`.
    pub sync_on_flush: bool,
    /// Verify the record header and every checksum block touched by each `get`.
    /// Default: `false`; reads trust the index and do not scan the payload with
    /// the CPU. Expiring records still read and validate their header for TTL.
    /// Writing checksums and recovery/reclaim validation are unaffected.
    pub verify_reads: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            clock: Arc::new(SystemClock),
            pack_threshold: 64 * 1024,
            batch_limit: 1024 * 1024,
            scan_window: 4 * 1024 * 1024,
            index_capacity: 1 << 20,
            index_memory_budget: usize::MAX,
            sync_on_flush: false,
            verify_reads: false,
        }
    }
}

impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("pack_threshold", &self.pack_threshold)
            .field("batch_limit", &self.batch_limit)
            .field("scan_window", &self.scan_window)
            .field("index_capacity", &self.index_capacity)
            .field("index_memory_budget", &self.index_memory_budget)
            .field("sync_on_flush", &self.sync_on_flush)
            .field("verify_reads", &self.verify_reads)
            .finish_non_exhaustive()
    }
}

impl Options {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.pack_threshold as u64 > 16 * 1024 * 1024 {
            return Err(Error::InvalidOption("pack_threshold must not exceed 16 MiB".into()));
        }
        if self.batch_limit < PAGE_SIZE as usize {
            return Err(Error::InvalidOption("batch_limit must be at least one page".into()));
        }
        if self.index_memory_budget < self.index_capacity.max(16).next_power_of_two() * crate::index::SLOT_BYTES {
            return Err(Error::InvalidOption(
                "index_memory_budget must hold the initial index capacity".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_budget_must_cover_initial_capacity() {
        let options = Options {
            index_capacity: 1024,
            index_memory_budget: 1024,
            ..Default::default()
        };
        assert!(matches!(options.validate(), Err(Error::InvalidOption(_))));
        assert!(Options::default().validate().is_ok());
    }
}

/// Options fixed at format time and recorded in the superblock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FormatOptions {
    /// Segment size in bytes. Must be a multiple of the page size.
    /// Default: 1 GiB.
    pub segment_size: u64,
    /// Largest value accepted by `put`. Must fit in a segment with room to
    /// spare; at most `segment_size / 16` is recommended. Default: 4 MiB.
    pub chunk_max: u32,
    /// Identity of the device. Stamped into every segment header so segments
    /// from a different format can never be mistaken for valid data.
    pub disk_uuid: [u8; 16],
}

impl Default for FormatOptions {
    fn default() -> Self {
        Self {
            segment_size: 1 << 30,
            chunk_max: 4 << 20,
            disk_uuid: [0; 16],
        }
    }
}

impl FormatOptions {
    pub(crate) fn validate(&self, device_len: u64) -> Result<()> {
        if !is_aligned(self.segment_size, PAGE_SIZE) || self.segment_size < 4 * PAGE_SIZE {
            return Err(Error::InvalidOption(format!(
                "segment_size {} must be a multiple of {PAGE_SIZE} and at least {}",
                self.segment_size,
                4 * PAGE_SIZE
            )));
        }
        // An empty segment must be able to hold one maximal record plus a
        // footer describing it.
        let needed = SEGMENT_HEADER_LEN + large_batch_len(self.chunk_max) + footer_len(1);
        if needed > self.segment_size {
            return Err(Error::InvalidOption(format!(
                "chunk_max {} does not fit in a segment of {} bytes ({} needed)",
                self.chunk_max, self.segment_size, needed
            )));
        }
        // A packed batch of one maximal small value must also fit; covered by
        // the large case since packed records are smaller.
        debug_assert!(BATCH_HEADER_LEN < PAGE_SIZE as usize);
        if device_len < 2 * self.segment_size {
            return Err(Error::InvalidOption(format!(
                "device of {device_len} bytes is too small for segments of {} bytes",
                self.segment_size
            )));
        }
        Ok(())
    }
}
