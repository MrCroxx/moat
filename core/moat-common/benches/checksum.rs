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

//! Single-core CRC32C throughput at the sizes the engine cares about.
//!
//! Run with `cargo bench -p moat-common`. Deliberately dependency-free: a
//! warm-up pass followed by timed passes, reporting the median.

use std::{hint::black_box, time::Instant};

use moat_common::{CHECKSUM_BLOCK_SIZE, block_checksums, crc32c, verify_blocks};

fn median_gib_per_s(len: usize, mut f: impl FnMut()) -> f64 {
    let passes = (2usize << 30) / len.max(1);
    let passes = passes.clamp(20, 2000);
    let mut samples: Vec<f64> = (0..7)
        .map(|_| {
            let start = Instant::now();
            for _ in 0..passes {
                f();
            }
            let secs = start.elapsed().as_secs_f64();
            (len * passes) as f64 / secs / (1u64 << 30) as f64
        })
        .collect();
    samples.sort_by(|a, b| a.total_cmp(b));
    samples[samples.len() / 2]
}

fn main() {
    let sizes = [4 << 10, CHECKSUM_BLOCK_SIZE, 1 << 20, 4 << 20];
    let buf: Vec<u8> = (0..(4usize << 20)).map(|i| (i % 251) as u8).collect();

    println!(
        "{:>10} {:>16} {:>22} {:>20}",
        "size", "crc32c GiB/s", "block_checksums GiB/s", "verify_blocks GiB/s"
    );
    for &len in &sizes {
        let data = &buf[..len];
        let one_shot = median_gib_per_s(len, || {
            black_box(crc32c(black_box(data)));
        });
        let per_block = median_gib_per_s(len, || {
            black_box(block_checksums(black_box(data)));
        });
        let checksums = block_checksums(data);
        let verify = median_gib_per_s(len, || {
            black_box(verify_blocks(black_box(data), 0, black_box(&checksums))).unwrap();
        });
        println!("{:>10} {:>16.2} {:>22.2} {:>20.2}", len, one_shot, per_block, verify);
    }
}
