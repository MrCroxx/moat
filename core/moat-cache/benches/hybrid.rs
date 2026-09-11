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

//! Memory/disk/miss lookup mixtures on the real file-backed adapter.

use std::{hint::black_box, sync::Arc, time::Instant};

use futures_executor::block_on;
use moat_cache::{Bytes, Cache, MemoryPolicy, Options};
use moat_cache_memory::Cache as MemoryCache;
use moat_cache_store::Store;
use moat_common::{HugePages, PoolOptions};
use moat_engine::{FileDevice, FormatOptions, QueueBackend, QueueOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let operations = std::env::var("MOAT_HYBRID_BENCH_OPS")
        .ok()
        .map(|value| value.parse::<usize>().expect("MOAT_HYBRID_BENCH_OPS must be positive"))
        .unwrap_or(10_000)
        .max(10);
    println!("key_bytes,value_bytes,operations,ns_per_memory_hit,ns_per_mixed_get,memory_hits,disk_reads,misses");
    block_on(async {
        for (key_len, value_len) in [(16, 128), (256, 4096), (4096, 65536)] {
            let file = tempfile::NamedTempFile::new()?;
            let device = Arc::new(FileDevice::create(file.path(), 65 << 20, false)?);
            moat_engine::format(
                &*device,
                &FormatOptions {
                    segment_size: 1 << 20,
                    chunk_max: 128 << 10,
                    disk_uuid: [1; 16],
                },
            )?;
            let engine = moat_engine::open(
                device,
                moat_engine::Options {
                    index_capacity: 1024,
                    verify_reads: false,
                    ..Default::default()
                },
            )?
            .0;
            let (store, _) = Store::new(
                vec![engine],
                moat_cache_store::Options {
                    backend: if cfg!(target_os = "linux") {
                        QueueBackend::Uring
                    } else {
                        QueueBackend::Sync
                    },
                    queue: QueueOptions {
                        depth: 16,
                        descriptors: 4,
                        pool: PoolOptions {
                            bytes: 16 << 20,
                            max_class: 1 << 20,
                            huge_pages: HugePages::Disabled,
                        },
                    },
                    ..Default::default()
                },
            )?;
            let cache = Cache::new(
                MemoryCache::builder(64 * (key_len + value_len))
                    .shards(1)
                    .policy(MemoryPolicy::Fifo)
                    .weigher(|key: &Bytes, value: &Bytes, _| key.len() + value.len())
                    // Keep one half resident and the other half disk-only so
                    // promotion cannot silently turn this into a memory benchmark.
                    .admission(|key, _, _| key[0] < 64),
                store,
                Options::default(),
            )
            .await?;
            let keys: Vec<Bytes> = (0..192_u64)
                .map(|number| {
                    let mut key = vec![0; key_len];
                    key[..8].copy_from_slice(&number.to_le_bytes());
                    key.into()
                })
                .collect();
            for (index, key) in keys.iter().take(128).enumerate() {
                cache.insert(key.clone(), vec![index as u8; value_len].into()).await?;
            }
            assert_eq!(cache.statistics().disk_entries, 128);
            let start = Instant::now();
            for i in 0..operations {
                black_box(cache.get_memory(keys[i % 64].as_ref()).expect("resident key"));
            }
            let memory_ns = start.elapsed().as_secs_f64() * 1e9 / operations as f64;
            let before = cache.statistics();
            let start = Instant::now();
            let mut misses = 0;
            let mut expected_memory = 0;
            let mut expected_disk = 0;
            for i in 0..operations {
                let lane = i % 10;
                let offset = (i / 10 * 37 + lane * 11) % 64;
                let index = if lane < 5 {
                    expected_memory += 1;
                    offset
                } else if lane < 9 {
                    expected_disk += 1;
                    offset + 64
                } else {
                    offset + 128
                };
                let result = cache.get(black_box(&keys[index])).await?;
                match result {
                    Some(entry) => {
                        assert!(index < 128);
                        assert_eq!(entry.value().len(), value_len);
                        assert_eq!(black_box(entry.value()[0]), index as u8);
                    }
                    None => {
                        assert!(index >= 128);
                        misses += 1;
                    }
                }
            }
            let mixed_ns = start.elapsed().as_secs_f64() * 1e9 / operations as f64;
            let after = cache.statistics();
            let memory_hits = after.memory.hits - before.memory.hits;
            let disk_reads = after.store.physical_reads - before.store.physical_reads;
            assert_eq!(memory_hits, expected_memory);
            assert_eq!(disk_reads, expected_disk);
            assert_eq!(misses + memory_hits + disk_reads, operations);
            println!(
                "{key_len},{value_len},{operations},{memory_ns:.2},{mixed_ns:.2},{memory_hits},{disk_reads},{misses}"
            );
            cache.close().await?;
        }
        Ok(())
    })
}
