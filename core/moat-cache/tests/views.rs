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

//! Shared KV views, lifetime, bounded retention, collision checks and recovery.

use std::{sync::Arc, time::Duration};

use futures_executor::block_on;
use moat_cache::{Bytes, Cache, Lookup, Options, Priority};
use moat_cache_memory::Cache as Memory;
use moat_cache_store::Store;
use moat_common::{ChunkId, HugePages, PoolOptions};
use moat_engine::{Engine, FormatOptions, MemDevice, QueueBackend, QueueOptions};

fn bytes(data: &[u8]) -> Bytes {
    Bytes::from(data.to_vec())
}
fn store(device: Arc<MemDevice>) -> Store {
    let engine: Engine = moat_engine::open(
        device,
        moat_engine::Options {
            index_capacity: 1024,
            ..Default::default()
        },
    )
    .unwrap()
    .0;
    Store::new(
        vec![engine],
        moat_cache_store::Options {
            max_requests: 128,
            max_bytes: 2 << 20,
            backend: QueueBackend::Sync,
            queue: QueueOptions {
                depth: 8,
                descriptors: 4,
                pool: PoolOptions {
                    bytes: 16 << 20,
                    max_class: 1 << 20,
                    huge_pages: HugePages::Disabled,
                },
            },
            ..Default::default()
        },
    )
    .unwrap()
    .0
}
fn device() -> Arc<MemDevice> {
    let d = Arc::new(MemDevice::new(65 << 20));
    moat_engine::format(
        &*d,
        &FormatOptions {
            segment_size: 1 << 20,
            chunk_max: 128 << 10,
            disk_uuid: [19; 16],
        },
    )
    .unwrap();
    d
}
async fn cache(device: Arc<MemDevice>, admission: bool, options: Options) -> Cache {
    Cache::new(
        Memory::<Bytes, Bytes, Bytes>::builder(128 << 20)
            .shards(1)
            .weigher(|k, v, p| k.len() + v.len() + p.len())
            .admission(move |_, _, _| admission),
        store(device),
        options,
    )
    .await
    .unwrap()
}

#[test]
fn disk_fields_share_storage_and_survive_overwrite_invalidation_and_close() {
    block_on(async {
        let device = device();
        let c = cache(device.clone(), true, Options::default()).await;
        let key = bytes(b"key");
        drop(
            c.insert_with(key.clone(), bytes(b"old value"), bytes(b"properties"), Priority::High)
                .await
                .unwrap(),
        );
        c.clear_memory();
        let old = c.get(&key).await.unwrap().unwrap();
        assert!(old.is_resident());
        assert!(old.key_view().shares_backing(&old.value_view()));
        assert!(old.properties_view().shares_backing(&old.value_view()));
        assert!(old.ptr_eq(&c.get_memory(b"key").unwrap()));
        let clone = old.clone();
        let value = old.value_view();
        drop(c.insert(key.clone(), bytes(b"new value")).await.unwrap());
        assert_eq!(old.value(), b"old value");
        assert!(!old.is_resident());
        assert!(c.invalidate(&key).await.unwrap());
        assert!(c.get(&key).await.unwrap().is_none());
        c.close().await.unwrap();
        assert_eq!(clone.properties(), b"properties");
        drop((old, clone));
        assert!(c.statistics().store.read_bytes > 0);
        assert_eq!(value.as_ref(), b"old value");
        drop(value);
        assert_eq!(c.statistics().store.read_bytes, 0);
    });
}

#[test]
fn resident_views_preserve_dispatch_headroom_and_rejected_views_still_work() {
    let (send, recv) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        block_on(async {
            let c = cache(device(), true, Options::default()).await;
            for n in 0..64u64 {
                drop(
                    c.insert(bytes(&n.to_le_bytes()), Bytes::from(vec![n as u8; 64 << 10]))
                        .await
                        .unwrap(),
                );
            }
            c.clear_memory();
            let mut resident = 0;
            let mut rejected = 0;
            for n in 0..64u64 {
                let view = c.get(&bytes(&n.to_le_bytes())).await.unwrap().unwrap();
                assert_eq!(view.value(), vec![n as u8; 64 << 10]);
                if view.is_resident() {
                    resident += 1;
                } else {
                    rejected += 1;
                }
            }
            assert_eq!(resident, 8);
            assert!(rejected > 0);
            assert!(c.statistics().store.read_bytes <= 1 << 20);
            c.clear_memory();
            assert_eq!(c.statistics().store.read_bytes, 0);
            assert!(
                c.get(&bytes(&63u64.to_le_bytes()))
                    .await
                    .unwrap()
                    .unwrap()
                    .is_resident()
            );
            c.close().await.unwrap();
            send.send(()).unwrap();
        })
    });
    recv.recv_timeout(Duration::from_secs(10))
        .expect("resident buffers blocked new reads");
    thread.join().unwrap();
}

#[test]
fn raw_miss_tokens_keep_shared_key_ownership_and_obey_invalidation() {
    block_on(async {
        let c = cache(device(), false, Options::default()).await;
        let key = bytes(b"missing");
        let token = match c.lookup(&key).await.unwrap() {
            Lookup::Miss(token) => token,
            Lookup::Hit(_) => panic!("unexpected hit"),
        };
        assert!(token.is_valid());
        c.invalidate(&key).await.unwrap();
        assert!(!token.is_valid());
        assert!(c.populate(token, bytes(b"stale")).await.unwrap().is_none());
        let token = match c.lookup(&key).await.unwrap() {
            Lookup::Miss(token) => token,
            Lookup::Hit(_) => panic!("unexpected hit"),
        };
        assert!(c.populate(token, bytes(b"fresh")).await.unwrap().is_some());
        assert_eq!(c.get(&key).await.unwrap().unwrap().value(), b"fresh");
        c.close().await.unwrap();
    });
}

struct Collision;
impl moat_cache::identity::Fingerprint for Collision {
    fn identify(&self, _: &[u8; 16], _: u32, _: &[u8]) -> ChunkId {
        ChunkId::from_u128(1)
    }
}
#[test]
fn raw_views_keep_full_key_collision_checks_and_warm_recovery() {
    block_on(async {
        let device = device();
        let opts = Options {
            fingerprint: Arc::new(Collision),
            ..Default::default()
        };
        let c = cache(device.clone(), false, opts.clone()).await;
        let a = bytes(b"a");
        let b = Bytes::from(vec![2; 4096]);
        drop(c.insert(a.clone(), bytes(b"first")).await.unwrap());
        drop(c.insert(b.clone(), bytes(b"second")).await.unwrap());
        assert!(c.get(&a).await.unwrap().is_none());
        assert!(!c.invalidate(&a).await.unwrap());
        c.close().await.unwrap();
        let c = cache(device, false, opts).await;
        let view = c.get(&b).await.unwrap().unwrap();
        assert_eq!(view.key(), b.as_ref());
        assert_eq!(view.value(), b"second");
        c.close().await.unwrap();
    });
}
