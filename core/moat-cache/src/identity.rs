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

//! Stable disk identity, independent of resident hash-table randomization.

use moat_common::ChunkId;

/// The default persistent identity version for XXH3-128 and byte keys.
/// Version 1 used BLAKE3; those caches must be rebuilt before switching defaults.
pub const DEFAULT_IDENTITY_VERSION: u32 = 2;

/// A persistent key fingerprint algorithm. It must remain stable across
/// restarts for a given namespace/version and canonical key encoding.
/// Fingerprints select slots; full-key comparison still decides cache identity.
/// Changing the algorithm requires a new identity version and a cache rebuild.
pub trait Fingerprint: Send + Sync + 'static {
    /// Computes an opaque 128-bit chunk identifier.
    fn identify(&self, namespace: &[u8; 16], version: u32, canonical_key: &[u8]) -> ChunkId;
}

/// A non-cryptographic XXH3-128 fingerprint with a fixed domain seed.
///
/// Hashes `namespace || version_le32 || key_length_le64 || key` and stores
/// the 128-bit result in big-endian order. All namespace bits participate in
/// the hash; they are not reduced to a 64-bit seed. Full-key comparison is
/// required because fingerprints are not collision-free or adversarially secure.
#[derive(Debug, Clone, Copy, Default)]
pub struct Xxh3;
impl Fingerprint for Xxh3 {
    fn identify(&self, namespace: &[u8; 16], version: u32, canonical_key: &[u8]) -> ChunkId {
        // Fixed domain seed: ASCII "moat-kv2", interpreted in big-endian order.
        let mut hasher = xxhash_rust::xxh3::Xxh3::with_seed(u64::from_be_bytes(*b"moat-kv2"));
        let mut header = [0; 28];
        header[..16].copy_from_slice(namespace);
        header[16..20].copy_from_slice(&version.to_le_bytes());
        header[20..].copy_from_slice(&(canonical_key.len() as u64).to_le_bytes());
        hasher.update(&header);
        hasher.update(canonical_key);
        ChunkId::from_u128(hasher.digest128())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistent_ids_match_reference_xxh3_128_vectors() {
        // Generated with the upstream C XXH3_128bits_withSeed implementation.
        // Fixed vectors protect the seed, field ordering, endianness and long-key path.
        for (namespace, version, length, expected) in [
            ([0; 16], 2, 0, 0x882e1276ea24b408c30ec2d2026c6cb8u128),
            ([0; 16], 2, 16, 0x7ed880d170efab5b38300113efad41f9),
            (
                std::array::from_fn(|i| i as u8),
                2,
                256,
                0x84a515305d2eecf24582dcefecbfbd3d,
            ),
            ([255; 16], u32::MAX, 4096, 0xfa4a6b56cceea4b88d4adc83f6ebf89a),
            (
                std::array::from_fn(|i| i as u8),
                7,
                65536,
                0xa2b47421fd603be38f9621ff71261eb9,
            ),
        ] {
            let key: Vec<u8> = (0..length).map(|i| (i % 251) as u8).collect();
            assert_eq!(
                Xxh3.identify(&namespace, version, &key).as_bytes(),
                &expected.to_be_bytes()
            );
        }
    }

    #[test]
    fn namespace_version_and_key_length_separate_ids() {
        let mut ids = std::collections::HashSet::new();
        for bit in 0..128 {
            let mut namespace = [0; 16];
            namespace[bit / 8] = 1 << (bit % 8);
            for version in [0, 1, DEFAULT_IDENTITY_VERSION, u32::MAX] {
                for length in [0, 1, 16, 212, 213, 256, 4096] {
                    assert!(ids.insert(Xxh3.identify(&namespace, version, &vec![0; length])));
                }
            }
        }
    }
}
