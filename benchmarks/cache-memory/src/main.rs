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

//! Matched resident-hit workloads for moat-cache-memory and pinned foyer-memory.

use std::{
    hash::{BuildHasherDefault, DefaultHasher},
    hint::black_box,
    sync::Barrier,
    thread,
    time::Instant,
};

use moat_cache_memory::{Cache, LfuConfig, LruConfig, Policy, S3FifoConfig};

type Hasher = BuildHasherDefault<DefaultHasher>;

fn measure(threads: usize, operations: usize, f: impl Fn(usize) + Send + Sync) -> (f64, f64, f64) {
    let mut samples = Vec::new();
    for _ in 0..5 {
        let ready = Barrier::new(threads + 1);
        let start = Barrier::new(threads + 1);
        let finish = Barrier::new(threads + 1);
        let elapsed = thread::scope(|scope| {
            for worker in 0..threads {
                let (ready, start, finish, f) = (&ready, &start, &finish, &f);
                scope.spawn(move || {
                    // Warm policy state and code before the timed interval.
                    for i in 0..10_000_usize {
                        f(i.wrapping_mul(37).wrapping_add(worker * 11));
                    }
                    ready.wait();
                    start.wait();
                    for i in 0..operations {
                        f(i.wrapping_mul(37).wrapping_add(worker * 11));
                    }
                    finish.wait();
                });
            }
            ready.wait();
            let begin = Instant::now();
            start.wait();
            finish.wait();
            begin.elapsed()
        });
        samples.push(elapsed.as_secs_f64() * 1e9 / (operations * threads) as f64);
    }
    samples.sort_by(f64::total_cmp);
    (samples[0], samples[2], samples[4])
}
fn output(engine: &str, policy: &str, sizes: (usize, usize), threads: usize, measured: (f64, f64, f64)) {
    println!(
        "{engine},{policy},{},{},{threads},{:.2},{:.2},{:.2}",
        sizes.0, sizes.1, measured.0, measured.1, measured.2
    );
}
fn main() {
    let operations = std::env::var("MOAT_CACHE_BENCH_OPS")
        .ok()
        .map(|value| value.parse::<usize>().expect("MOAT_CACHE_BENCH_OPS must be positive"))
        .unwrap_or(1_000_000)
        .max(1);
    let policies = [
        (
            "fifo",
            Policy::Fifo,
            foyer_memory::EvictionConfig::Fifo(Default::default()),
        ),
        (
            "lru",
            Policy::Lru(LruConfig::default()),
            foyer_memory::EvictionConfig::Lru(Default::default()),
        ),
        (
            "tiny_lfu",
            Policy::Lfu(LfuConfig::default()),
            foyer_memory::EvictionConfig::Lfu(Default::default()),
        ),
        (
            "s3fifo",
            Policy::S3Fifo(S3FifoConfig::default()),
            foyer_memory::EvictionConfig::S3Fifo(Default::default()),
        ),
        (
            "sieve",
            Policy::Sieve,
            foyer_memory::EvictionConfig::Sieve(Default::default()),
        ),
    ];
    println!("engine,policy,key_bytes,value_bytes,threads,min_ns_per_hit,median_ns_per_hit,max_ns_per_hit");
    for (key_len, value_len) in [(16, 128), (256, 4096), (4096, 65536)] {
        let keys: Vec<Vec<u8>> = (0..256_u64)
            .map(|index| {
                let mut key = vec![0; key_len];
                key[..8].copy_from_slice(&index.to_le_bytes());
                key
            })
            .collect();
        let capacity = 2048 * (key_len + value_len);
        for (index, (name, moat_policy, foyer_policy)) in policies.iter().enumerate() {
            let moat = Cache::<Vec<u8>, Vec<u8>>::builder(capacity)
                .shards(16)
                .hash_builder(Hasher::default())
                .policy(moat_policy.clone())
                .weigher(|key, value, _| key.len() + value.len())
                .build()
                .unwrap();
            let foyer = foyer_memory::CacheBuilder::<Vec<u8>, Vec<u8>, _>::new(capacity)
                .with_shards(16)
                .with_hash_builder(Hasher::default())
                .with_eviction_config(foyer_policy.clone())
                .with_weighter(|key: &Vec<u8>, value: &Vec<u8>| key.len() + value.len())
                .build::<foyer_memory::CacheProperties>();
            for key in &keys {
                moat.insert(key.clone(), vec![7; value_len]);
                foyer.insert(key.clone(), vec![7; value_len]);
            }
            assert_eq!(moat.len(), keys.len());
            assert_eq!(foyer.usage(), keys.len() * (key_len + value_len));
            for threads in [1, 4] {
                let moat_hit = |i: usize| {
                    let entry = moat
                        .get(black_box(keys[i % keys.len()].as_slice()))
                        .expect("resident moat key");
                    black_box(entry.value()[0]);
                };
                let foyer_hit = |i: usize| {
                    let entry = foyer
                        .get(black_box(keys[i % keys.len()].as_slice()))
                        .expect("resident foyer key");
                    black_box(entry.value()[0]);
                };
                // Alternate pair order to reduce a fixed first-run bias.
                let (a, b) = if index % 2 == 0 {
                    (
                        measure(threads, operations, moat_hit),
                        measure(threads, operations, foyer_hit),
                    )
                } else {
                    let b = measure(threads, operations, foyer_hit);
                    (measure(threads, operations, moat_hit), b)
                };
                output("moat", name, (key_len, value_len), threads, a);
                output("foyer", name, (key_len, value_len), threads, b);
            }
        }
    }
}
