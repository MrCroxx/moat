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

//! Resident and disk lookups with application-owned loading and safe population.

use std::sync::Arc;

use futures_executor::block_on;
use moat_cache::{Bytes, Cache, Lookup};
use moat_cache_memory::Cache as MemoryCache;
use moat_cache_store::{Options, Store};
use moat_common::{HugePages, PoolOptions};
use moat_engine::{FormatOptions, MemDevice, QueueBackend, QueueOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    block_on(async {
        let device = Arc::new(MemDevice::new(17 << 20));
        moat_engine::format(
            &*device,
            &FormatOptions {
                segment_size: 1 << 20,
                chunk_max: 128 << 10,
                disk_uuid: [1; 16],
            },
        )?;
        let (engine, _) = moat_engine::open(
            device,
            moat_engine::Options {
                index_capacity: 128,
                verify_reads: false,
                ..Default::default()
            },
        )?;
        let (store, inventory) = Store::new(
            vec![engine],
            Options {
                backend: QueueBackend::Sync,
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
        assert!(inventory.is_empty());
        let cache = Cache::new(
            MemoryCache::builder(1 << 20)
                .shards(16)
                .weigher(|key: &Bytes, value: &Bytes, _| key.len() + value.len()),
            store,
            moat_cache::Options::default(),
        )
        .await?;
        let key = Bytes::from(b"example".to_vec());
        let entry = match cache.lookup(&key).await? {
            Lookup::Hit(entry) => entry,
            Lookup::Miss(token) => {
                // The application owns origin concurrency, retries and cancellation.
                let value = b"loaded by the application".to_vec();
                cache
                    .populate(token, value.into())
                    .await?
                    .expect("uncontended population")
            }
        };
        assert!(cache.get_memory(b"example").is_some());
        cache.clear_memory();
        let cold = cache.get(&key).await?.expect("disk hit");
        assert_eq!(entry.value(), cold.value());
        cache.invalidate(&key).await?;
        assert!(cache.get(&key).await?.is_none());
        cache.close().await?;
        assert_eq!(entry.value(), b"loaded by the application");
        Ok(())
    })
}
