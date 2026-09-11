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

//! Resident hit benchmarks across policies and key/value sizes.
//!
//! `MOAT_CACHE_BENCH_OPS` controls operations per thread (default: 100000).
//! The locked-map result is a primitive reference, without policy or statistics,
//! and must not be interpreted as a comparison with foyer-memory.

use std::{
    collections::HashMap,
    hint::black_box,
    sync::{Arc, Barrier},
    thread,
    time::Instant,
};

use moat_cache_memory::{Cache, LfuConfig, LruConfig, Policy, S3FifoConfig};
use parking_lot::RwLock;

fn measure(threads: usize, operations: usize, f: impl Fn(usize) + Send + Sync) -> f64 {
    let mut samples = Vec::new();
    for _ in 0..3 {
        let barrier = Barrier::new(threads + 1);
        let elapsed = thread::scope(|scope| {
            for worker in 0..threads {
                let barrier = &barrier;
                let f = &f;
                scope.spawn(move || {
                    barrier.wait();
                    for i in 0..operations {
                        f(i.wrapping_mul(37).wrapping_add(worker * 11));
                    }
                    barrier.wait();
                });
            }
            let start = Instant::now();
            barrier.wait();
            barrier.wait();
            start.elapsed()
        });
        samples.push(elapsed.as_secs_f64() * 1e9 / (operations * threads) as f64);
    }
    samples.sort_by(f64::total_cmp);
    samples[1]
}

fn main() {
    let operations = std::env::var("MOAT_CACHE_BENCH_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000usize)
        .max(1);
    let policies = [
        ("fifo", Policy::Fifo),
        ("lru", Policy::Lru(LruConfig::default())),
        ("tiny_lfu", Policy::Lfu(LfuConfig::default())),
        ("s3fifo", Policy::S3Fifo(S3FifoConfig::default())),
        ("sieve", Policy::Sieve),
    ];
    println!("policy,key_bytes,value_bytes,threads,ns_per_hit");
    for (key_len, value_len) in [(16, 128), (256, 4096), (4096, 65536)] {
        let keys: Vec<Vec<u8>> = (0..256u64)
            .map(|i| {
                let mut key = vec![0; key_len];
                key[..8].copy_from_slice(&i.to_le_bytes());
                key
            })
            .collect();
        let baseline = RwLock::new(
            keys.iter()
                .map(|key| (key.clone(), Arc::new(vec![7u8; value_len])))
                .collect::<HashMap<_, _>>(),
        );
        for threads in [1, 4] {
            let cost = measure(threads, operations, |i| {
                let value = baseline
                    .read()
                    .get(black_box(keys[i % keys.len()].as_slice()))
                    .unwrap()
                    .clone();
                black_box(value[0]);
            });
            println!("locked_map,{key_len},{value_len},{threads},{cost:.2}");
        }
        for (name, policy) in &policies {
            let cache = Cache::<Vec<u8>, Vec<u8>>::builder(2048 * (key_len + value_len))
                .shards(16)
                .policy(policy.clone())
                .weigher(|key, value, _| key.len() + value.len())
                .build()
                .unwrap();
            for key in &keys {
                cache.insert(key.clone(), vec![7; value_len]);
            }
            assert_eq!(cache.len(), keys.len());
            for threads in [1, 4] {
                let cost = measure(threads, operations, |i| {
                    let entry = cache.get(black_box(keys[i % keys.len()].as_slice())).unwrap();
                    black_box(entry[0]);
                });
                println!("{name},{key_len},{value_len},{threads},{cost:.2}");
            }
        }
    }
}
