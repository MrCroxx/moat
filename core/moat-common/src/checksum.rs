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

//! CRC32C checksums.
//!
//! Chunk values are checksummed per [`CHECKSUM_BLOCK_SIZE`] block so that a
//! range read only has to verify the blocks it touches. The same block
//! checksums can be carried by a transport to support end-to-end integrity
//! without recomputation at every hop.
//!
//! Uses `crc-fast` for both checksum generation and optional read verification.
//! `cargo bench -p moat-common` measures throughput on the current machine.

use crc_fast::{CrcAlgorithm, Digest};

/// The value checksum granularity: 64 KiB.
pub const CHECKSUM_BLOCK_SIZE: usize = 64 * 1024;

const ALGORITHM: CrcAlgorithm = CrcAlgorithm::Crc32Iscsi;

/// Computes the CRC32C (Castagnoli, RFC 3720) of `data`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    crc_fast::checksum(ALGORITHM, data) as u32
}

/// An incremental CRC32C computation over several slices.
///
/// Produces the same value as [`crc32c`] over the concatenation of everything
/// passed to [`Crc32c::update`].
#[derive(Clone, Copy, Debug)]
pub struct Crc32c(Digest);

impl Crc32c {
    /// Starts a new computation.
    #[inline]
    pub fn new() -> Self {
        Self(Digest::new(ALGORITHM))
    }

    /// Feeds more bytes.
    #[inline]
    pub fn update(&mut self, data: &[u8]) -> &mut Self {
        self.0.update(data);
        self
    }

    /// Returns the checksum of everything fed so far.
    #[inline]
    pub fn finalize(&self) -> u32 {
        self.0.finalize() as u32
    }
}

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns how many checksum blocks a value of `len` bytes has.
///
/// A zero-length value has zero blocks.
#[inline]
pub const fn block_count(len: u64) -> u32 {
    len.div_ceil(CHECKSUM_BLOCK_SIZE as u64) as u32
}

/// Computes the per-block checksums of `data`.
pub fn block_checksums(data: &[u8]) -> Vec<u32> {
    data.chunks(CHECKSUM_BLOCK_SIZE).map(crc32c).collect()
}

/// Verifies `data` against `checksums`, where `data` starts at checksum block
/// `first_block` of the original value and must end on a block boundary or at
/// the end of the value.
///
/// Returns the index (relative to the value) of the first mismatching block.
pub fn verify_blocks(data: &[u8], first_block: u32, checksums: &[u32]) -> Result<(), u32> {
    verify_blocks_with(data, first_block, |i| checksums.get(i as usize).copied())
}

/// Like [`verify_blocks`], with the expected checksums supplied by a lookup
/// function (for example a view into an on-disk header, avoiding a copy).
pub fn verify_blocks_with(data: &[u8], first_block: u32, expected: impl Fn(u32) -> Option<u32>) -> Result<(), u32> {
    for (i, block) in data.chunks(CHECKSUM_BLOCK_SIZE).enumerate() {
        let index = first_block + i as u32;
        match expected(index) {
            Some(want) if crc32c(block) == want => {}
            _ => return Err(index),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // RFC 3720 / iSCSI test vectors.
        assert_eq!(crc32c(&[0u8; 32]), 0x8a91_36aa);
        assert_eq!(crc32c(&[0xffu8; 32]), 0x62a8_ab43);
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(crc32c(&[]), 0);
    }

    #[test]
    fn incremental_matches_one_shot() {
        let data: Vec<u8> = (0..300_000u32).map(|i| (i % 253) as u8).collect();
        let mut digest = Crc32c::new();
        for part in data.chunks(7_777) {
            digest.update(part);
        }
        assert_eq!(digest.finalize(), crc32c(&data));
        assert_eq!(Crc32c::new().update(b"1234").update(b"56789").finalize(), 0xe306_9283);
    }

    #[test]
    fn counts() {
        assert_eq!(block_count(0), 0);
        assert_eq!(block_count(1), 1);
        assert_eq!(block_count(CHECKSUM_BLOCK_SIZE as u64), 1);
        assert_eq!(block_count(CHECKSUM_BLOCK_SIZE as u64 + 1), 2);
    }

    #[test]
    fn verify_partial_ranges() {
        let data: Vec<u8> = (0..(CHECKSUM_BLOCK_SIZE * 3 + 17)).map(|i| (i % 251) as u8).collect();
        let sums = block_checksums(&data);
        assert_eq!(sums.len(), 4);
        assert!(verify_blocks(&data, 0, &sums).is_ok());
        assert!(verify_blocks(&data[CHECKSUM_BLOCK_SIZE..], 1, &sums).is_ok());
        assert!(verify_blocks(&data[CHECKSUM_BLOCK_SIZE * 3..], 3, &sums).is_ok());

        let mut corrupted = data.clone();
        corrupted[CHECKSUM_BLOCK_SIZE * 2 + 5] ^= 0xff;
        assert_eq!(verify_blocks(&corrupted, 0, &sums), Err(2));
        // Blocks before the corruption still verify on their own.
        assert!(verify_blocks(&corrupted[..CHECKSUM_BLOCK_SIZE * 2], 0, &sums).is_ok());
    }

    #[test]
    fn preserves_partial_blocks_indices_and_first_error() {
        let mut data = vec![0u8; CHECKSUM_BLOCK_SIZE * 4 + 17];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = (i % 251) as u8;
        }
        let checksums = block_checksums(&data);
        let first = 7;
        let verify = |input: &[u8]| verify_blocks_with(input, first, |i| checksums.get((i - first) as usize).copied());
        assert_eq!(verify(&data), Ok(()));
        for block in 0..checksums.len() {
            let at = block * CHECKSUM_BLOCK_SIZE;
            data[at] ^= 1;
            assert_eq!(verify(&data), Err(first + block as u32));
            data[at] ^= 1;
        }
        data[CHECKSUM_BLOCK_SIZE] ^= 1;
        data[CHECKSUM_BLOCK_SIZE * 3] ^= 1;
        assert_eq!(verify(&data), Err(first + 1));
        assert_eq!(verify_blocks_with(&[], first, |_| None), Ok(()));
        assert_eq!(verify_blocks_with(&data, first, |_| None), Err(first));
    }
}
