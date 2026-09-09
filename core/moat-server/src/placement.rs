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

//! Deterministic placement of chunks on disks.
//!
//! Weighted rendezvous hashing: every disk scores a chunk from the chunk
//! identifier and the disk's own identity, scaled by the disk's weight
//! (capacity), and the highest score wins. There is no table and no state;
//! adding or removing a disk moves only the keys that land on it, and every
//! node computes the same answer from the same disk list.

use moat_common::ChunkId;

/// A disk that can hold chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Target {
    /// The disk's identity (its superblock uuid).
    pub uuid: [u8; 16],
    /// Relative weight, typically the capacity in bytes. Zero excludes the
    /// disk.
    pub weight: u64,
}

/// A fixed set of disks to place chunks on.
#[derive(Debug, Clone)]
pub struct Placement {
    targets: Vec<Target>,
    seeds: Vec<u64>,
}

impl Placement {
    /// Builds a placement over `targets`; the index into this list is what
    /// [`Placement::disk_of`] returns.
    pub fn new(targets: Vec<Target>) -> Self {
        let seeds = targets
            .iter()
            .map(|t| {
                let lo = u64::from_le_bytes(t.uuid[..8].try_into().expect("8 bytes"));
                let hi = u64::from_le_bytes(t.uuid[8..].try_into().expect("8 bytes"));
                mix(lo ^ mix(hi ^ 0x6a09_e667_f3bc_c909))
            })
            .collect();
        Self { targets, seeds }
    }

    /// The targets, in index order.
    pub fn targets(&self) -> &[Target] {
        &self.targets
    }

    /// Number of targets.
    pub fn len(&self) -> usize {
        self.targets.len()
    }

    /// Whether there are no targets.
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }

    /// The disk `id` belongs on, or `None` if every target has zero weight.
    pub fn disk_of(&self, id: &ChunkId) -> Option<usize> {
        let key = id.mix();
        let mut best: Option<(usize, f64)> = None;
        for (i, (t, seed)) in self.targets.iter().zip(&self.seeds).enumerate() {
            if t.weight == 0 {
                continue;
            }
            // A uniform draw in (0, 1] per (chunk, disk), scaled by weight:
            // the classic weighted rendezvous score.
            let h = mix(key ^ seed);
            let u = ((h >> 11) as f64 + 1.0) / ((1u64 << 53) as f64 + 1.0);
            let score = -(t.weight as f64) / u.ln();
            if best.is_none_or(|(_, s)| score > s) {
                best = Some((i, score));
            }
        }
        best.map(|(i, _)| i)
    }
}

/// SplitMix64 finalizer.
#[inline]
fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn targets(n: usize, weight: impl Fn(usize) -> u64) -> Vec<Target> {
        (0..n)
            .map(|i| Target {
                uuid: [i as u8 + 1; 16],
                weight: weight(i),
            })
            .collect()
    }

    #[test]
    fn spreads_evenly_and_respects_weights() {
        let p = Placement::new(targets(8, |_| 1));
        let mut counts = [0u32; 8];
        for i in 0..80_000u128 {
            counts[p.disk_of(&ChunkId::from_u128(i * 7919)).unwrap()] += 1;
        }
        for c in counts {
            assert!((9_000..11_000).contains(&c), "{counts:?}");
        }

        let p = Placement::new(targets(2, |i| if i == 0 { 1 } else { 3 }));
        let mut counts = [0u32; 2];
        for i in 0..40_000u128 {
            counts[p.disk_of(&ChunkId::from_u128(i)).unwrap()] += 1;
        }
        let ratio = counts[1] as f64 / counts[0] as f64;
        assert!((2.6..3.4).contains(&ratio), "{counts:?}");
    }

    #[test]
    fn removing_a_disk_moves_only_its_keys() {
        let all = Placement::new(targets(10, |_| 1));
        let mut fewer = targets(10, |_| 1);
        fewer[3].weight = 0;
        let fewer = Placement::new(fewer);
        let mut moved = 0;
        for i in 0..20_000u128 {
            let id = ChunkId::from_u128(i);
            let (a, b) = (all.disk_of(&id).unwrap(), fewer.disk_of(&id).unwrap());
            if a != b {
                assert_eq!(a, 3, "only keys of the removed disk move");
                moved += 1;
            }
        }
        assert!((1_500..2_500).contains(&moved), "{moved}");
        assert!(
            Placement::new(targets(2, |_| 0))
                .disk_of(&ChunkId::from_u128(1))
                .is_none()
        );
    }
}
