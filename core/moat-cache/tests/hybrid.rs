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

//! Hybrid persistence, logical-key isolation and conditional population.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use futures_executor::block_on;
use moat_cache::{Bytes, Cache, DiskPolicy, Error, FillToken, Lookup, Options, Priority};
use moat_cache_memory::Cache as MemoryCache;
use moat_cache_store::Store;
use moat_common::{ChunkId, HugePages, PoolOptions};
use moat_engine::{Device, FormatOptions, MemDevice, QueueBackend, QueueOptions};

fn bytes(data: impl AsRef<[u8]>) -> Bytes {
    Bytes::from(data.as_ref().to_vec())
}

const SEGMENT: u64 = 1 << 20;
const CHUNK_MAX: usize = 128 << 10;

fn device() -> Arc<MemDevice> {
    let device = Arc::new(MemDevice::new(17 * SEGMENT));
    moat_engine::format(
        &*device,
        &FormatOptions {
            segment_size: SEGMENT,
            chunk_max: CHUNK_MAX as u32,
            disk_uuid: [1; 16],
        },
    )
    .unwrap();
    device
}
fn store(device: Arc<dyn Device>) -> Store {
    store_on(vec![device], QueueBackend::Sync)
}
fn store_on(devices: Vec<Arc<dyn Device>>, backend: QueueBackend) -> Store {
    // The file fixture uses smaller chunks to bound registered memory across
    // both disks. Restart tests also need headroom for old rings to be released.
    let (batch_limit, pool_bytes, max_class) = match backend {
        QueueBackend::Uring => (64 << 10, 2 << 20, 128 << 10),
        _ => (1 << 20, 32 << 20, 1 << 20),
    };
    let engines = devices
        .into_iter()
        .map(|device| {
            moat_engine::open(
                device,
                moat_engine::Options {
                    index_capacity: 1024,
                    batch_limit,
                    verify_reads: false,
                    ..Default::default()
                },
            )
            .unwrap()
            .0
        })
        .collect();
    Store::new(
        engines,
        moat_cache_store::Options {
            max_requests: 128,
            max_bytes: 32 << 20,
            backend,
            queue: QueueOptions {
                depth: 8,
                descriptors: 4,
                pool: PoolOptions {
                    bytes: pool_bytes,
                    max_class,
                    huge_pages: HugePages::Disabled,
                },
            },
            ..Default::default()
        },
    )
    .unwrap()
    .0
}
async fn cache(device: Arc<dyn Device>, options: Options) -> Cache {
    Cache::new(MemoryCache::builder(64).shards(1), store(device), options)
        .await
        .unwrap()
}
fn miss(lookup: Lookup) -> FillToken {
    match lookup {
        Lookup::Miss(token) => token,
        Lookup::Hit(_) => panic!("expected a miss"),
    }
}

#[test]
fn bytes_keys_properties_cold_promotion_and_warm_restart() {
    block_on(async {
        let device = device();
        let cache = Cache::new(
            MemoryCache::builder(8).shards(1),
            store(device.clone()),
            Options::default(),
        )
        .await
        .unwrap();
        let held = cache
            .insert_with(
                bytes(b"key"),
                vec![7; 4096].into(),
                bytes(42u64.to_le_bytes()),
                Priority::High,
            )
            .await
            .unwrap();
        assert!(held.is_resident());
        assert!(held.ptr_eq(&cache.get_memory(b"key").unwrap()));
        assert_eq!(cache.statistics().store.physical_reads, 0);
        cache.clear_memory();
        assert!(!held.is_resident());
        let cold = cache.get(&bytes(b"key")).await.unwrap().unwrap();
        assert_eq!(cold.value(), held.value());
        assert_eq!(cold.properties(), 42u64.to_le_bytes());
        assert!(cold.is_resident());
        assert_eq!(cache.statistics().store.physical_reads, 1);
        cache.close().await.unwrap();
        assert!(!cold.is_resident());
        let reopened = Cache::new(MemoryCache::builder(8).shards(1), store(device), Options::default())
            .await
            .unwrap();
        assert_eq!(reopened.statistics().disk_entries, 1);
        let recovered = reopened.get(&bytes(b"key")).await.unwrap().unwrap();
        assert_eq!(recovered.value(), held.value());
        assert_eq!(recovered.properties(), 42u64.to_le_bytes());
        reopened.close().await.unwrap();
    });
}

#[test]
fn variable_keys_include_empty_and_chunk_boundary_and_rejection_preserves_value() {
    block_on(async {
        let cache = cache(device(), Options::default()).await;
        for len in [0, 16, 256, 4096, CHUNK_MAX - 56 - 4] {
            let key = bytes("k".repeat(len));
            cache.insert(key.clone(), vec![1; 4].into()).await.unwrap();
            cache.clear_memory();
            assert_eq!(cache.get(&key).await.unwrap().unwrap().value(), &[1; 4]);
            assert!(matches!(
                cache.insert(key.clone(), vec![0; CHUNK_MAX].into()).await,
                Err(Error::TooLarge { .. })
            ));
            assert_eq!(cache.get(&key).await.unwrap().unwrap().value(), &[1; 4]);
            assert!(cache.invalidate(&key).await.unwrap());
        }
        assert_eq!(cache.statistics().disk_entries, 0);
        cache.close().await.unwrap();
    });
}

#[test]
fn tokens_are_invalidated_by_absent_delete_insert_and_other_population() {
    block_on(async {
        let cache = cache(device(), Options::default()).await;
        let first = miss(cache.lookup(&bytes(b"key")).await.unwrap());
        let second = miss(cache.lookup(&bytes(b"key")).await.unwrap());
        assert_eq!(cache.statistics().tracked_keys, 1);
        assert_eq!(cache.statistics().tracked_key_bytes, 3);
        assert!(!cache.invalidate(&bytes(b"key")).await.unwrap());
        assert!(!first.is_valid());
        assert!(cache.populate(first, vec![1].into()).await.unwrap().is_none());
        assert!(cache.populate(second, vec![2].into()).await.unwrap().is_none());
        let first = miss(cache.lookup(&bytes(b"key")).await.unwrap());
        let second = miss(cache.lookup(&bytes(b"key")).await.unwrap());
        let accepted = cache.populate(first, vec![3].into());
        assert!(!second.is_valid());
        assert!(cache.populate(second, vec![4].into()).await.unwrap().is_none());
        assert_eq!(accepted.await.unwrap().unwrap().value(), &[3]);
        let obsolete = miss(cache.lookup(&bytes(b"other")).await.unwrap());
        cache.insert(bytes(b"other"), vec![5].into()).await.unwrap();
        assert!(cache.populate(obsolete, vec![6].into()).await.unwrap().is_none());
        assert_eq!(cache.get(&bytes(b"other")).await.unwrap().unwrap().value(), &[5]);
        assert_eq!(cache.statistics().tracked_keys, 0);
        cache.close().await.unwrap();
    });
}

#[test]
fn tokens_have_bounded_lifetime_and_instance_identity() {
    block_on(async {
        let options = Options {
            key_leases: 2,
            key_bytes: 3,
            ..Default::default()
        };
        let cache = cache(device(), options).await;
        let first = miss(cache.lookup(&bytes(b"abc")).await.unwrap());
        let second = miss(cache.lookup(&bytes(b"abc")).await.unwrap());
        assert!(matches!(cache.lookup(&bytes(b"abc")).await, Err(Error::Busy)));
        drop(first);
        assert!(matches!(cache.lookup(&bytes(b"d")).await, Err(Error::Busy)));
        drop(second);
        assert_eq!(cache.statistics().tracked_key_bytes, 0);
        let token = miss(cache.lookup(&bytes(b"abc")).await.unwrap());
        let other = Cache::new(MemoryCache::builder(1).shards(1), store(device()), Options::default())
            .await
            .unwrap();
        assert!(matches!(
            other.populate(token, vec![1].into()).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(cache.statistics().key_leases, 0);
        let token = miss(cache.lookup(&bytes(b"abc")).await.unwrap());
        cache.close().await.unwrap();
        assert!(!token.is_valid());
        drop(token);
        other.close().await.unwrap();
    });
}

struct Collision;
impl moat_cache::identity::Fingerprint for Collision {
    fn identify(&self, _: &[u8; 16], _: u32, _: &[u8]) -> ChunkId {
        ChunkId::from_u128(7)
    }
}
#[test]
fn fingerprint_collisions_never_return_or_delete_another_logical_key() {
    block_on(async {
        let options = Options {
            fingerprint: Arc::new(Collision),
            ..Default::default()
        };
        let cache = cache(device(), options).await;
        let first = cache.insert(bytes(b"a"), vec![1].into());
        let second = cache.insert(bytes(b"b"), vec![2].into());
        first.await.unwrap();
        second.await.unwrap();
        assert_eq!(cache.statistics().disk_entries, 1);
        cache.clear_memory();
        assert!(cache.get(&bytes(b"a")).await.unwrap().is_none());
        assert!(!cache.invalidate(&bytes(b"a")).await.unwrap());
        assert_eq!(cache.get(&bytes(b"b")).await.unwrap().unwrap().value(), &[2]);
        assert!(cache.invalidate(&bytes(b"b")).await.unwrap());
        assert!(cache.get(&bytes(b"b")).await.unwrap().is_none());
        cache.close().await.unwrap();
    });
}

#[test]
fn cancelling_replies_preserves_mutation_order_and_flush_catalog_bookkeeping() {
    block_on(async {
        let device = device();
        let cache = cache(device.clone(), Options::default()).await;
        drop(cache.insert(bytes(b"key"), vec![1].into()));
        drop(cache.invalidate(&bytes(b"key")));
        drop(cache.insert(bytes(b"key"), vec![2].into()));
        cache.flush().await.unwrap();
        assert_eq!(cache.statistics().disk_entries, 1);
        assert_eq!(cache.statistics().key_leases, 0);
        cache.clear_memory();
        assert_eq!(cache.get(&bytes(b"key")).await.unwrap().unwrap().value(), &[2]);
        cache.close().await.unwrap();
        let reopened = Cache::new(MemoryCache::builder(1).shards(1), store(device), Options::default())
            .await
            .unwrap();
        assert_eq!(reopened.get(&bytes(b"key")).await.unwrap().unwrap().value(), &[2]);
        reopened.close().await.unwrap();
    });
}

#[test]
fn zero_copy_values_hold_adapter_credits_until_last_handle_release() {
    block_on(async {
        let cache = Cache::new(MemoryCache::builder(1).shards(1), store(device()), Options::default())
            .await
            .unwrap();
        cache.insert(bytes(b"key"), Bytes::from(vec![9; 8192])).await.unwrap();
        cache.clear_memory();
        let held = cache.get(&bytes(b"key")).await.unwrap().unwrap();
        let slice = held.value_view().slice(1..4096).unwrap();
        assert!(slice.shares_backing(&held.value_view()));
        assert!(cache.statistics().store.read_bytes >= 8192);
        cache.close().await.unwrap();
        assert_eq!(held.value(), &[9; 8192]);
        drop(held);
        assert!(cache.statistics().store.read_bytes > 0);
        drop(slice);
        assert_eq!(cache.statistics().store.read_bytes, 0);
    });
}

#[test]
fn disk_fifo_limit_and_restart_trim_follow_completed_write_order() {
    block_on(async {
        let device = device();
        let cache = cache(device.clone(), Options::default()).await;
        for key in ["a", "b", "c"] {
            cache.insert(bytes(key), vec![1].into()).await.unwrap();
        }
        cache.close().await.unwrap();
        let mut options = Options::default();
        options.disk.entries = Some(2);
        options.disk.policy = DiskPolicy::Fifo;
        let cache = Cache::new(MemoryCache::builder(4).shards(1), store(device), options)
            .await
            .unwrap();
        assert_eq!(cache.statistics().disk_entries, 2);
        assert!(cache.get(&bytes(b"a")).await.unwrap().is_none());
        assert!(cache.get(&bytes(b"b")).await.unwrap().is_some());
        cache.insert(bytes(b"d"), vec![4].into()).await.unwrap();
        cache.clear_memory();
        assert!(cache.get(&bytes(b"b")).await.unwrap().is_none());
        assert!(cache.get(&bytes(b"c")).await.unwrap().is_some());
        assert!(cache.get(&bytes(b"d")).await.unwrap().is_some());
        cache.close().await.unwrap();
    });
}

#[test]
fn shared_store_is_rejected_without_closing_the_other_owner() {
    block_on(async {
        let store = store(device());
        assert!(store.is_unique());
        let result = Cache::new(MemoryCache::builder(1).shards(1), store.clone(), Options::default()).await;
        assert!(matches!(result, Err(Error::Invalid(_))));
        assert!(store.is_unique());
        store
            .put(ChunkId::from_u128(1), Arc::from(&b"value"[..]))
            .await
            .unwrap();
        store.close().await.unwrap();
    });
}

#[derive(Default)]
struct Gate {
    state: parking_lot::Mutex<(bool, bool)>,
    changed: parking_lot::Condvar,
}
impl Gate {
    fn arm(&self) {
        *self.state.lock() = (true, false);
    }
    fn wait(&self) {
        let mut state = self.state.lock();
        while !state.1 {
            assert!(
                !self
                    .changed
                    .wait_for(&mut state, std::time::Duration::from_secs(5))
                    .timed_out(),
                "operation did not start"
            );
        }
    }
    fn block(&self) {
        let mut state = self.state.lock();
        if state.0 {
            state.1 = true;
            self.changed.notify_all();
        }
        while state.0 {
            self.changed.wait(&mut state);
        }
    }
    fn release(&self) {
        self.state.lock().0 = false;
        self.changed.notify_all();
    }
}
struct ReleaseGate(Arc<Gate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}
#[test]
fn old_disk_reads_cannot_promote_after_invalidation_or_overwrite() {
    for overwrite in [false, true] {
        block_on(async {
            let gate = Arc::new(Gate::default());
            let armed = Arc::new(AtomicBool::new(false));
            let cache = Cache::new(
                MemoryCache::builder(4).shards(1).admission({
                    let gate = gate.clone();
                    let armed = armed.clone();
                    move |_, _, _| {
                        if armed.swap(false, Ordering::AcqRel) {
                            gate.block();
                        }
                        true
                    }
                }),
                store(device()),
                Options::default(),
            )
            .await
            .unwrap();
            cache.insert(bytes(b"key"), vec![1].into()).await.unwrap();
            cache.clear_memory();
            gate.arm();
            armed.store(true, Ordering::Release);
            let release = ReleaseGate(gate.clone());
            let reader = {
                let cache = cache.clone();
                std::thread::spawn(move || block_on(cache.get(&bytes(b"key"))))
            };
            gate.wait();
            if overwrite {
                cache.insert(bytes(b"key"), vec![2].into()).await.unwrap();
            } else {
                cache.invalidate(&bytes(b"key")).await.unwrap();
            }
            drop(release);
            let read = reader.join().unwrap().unwrap();
            if overwrite {
                assert_eq!(read.unwrap().value(), &[2]);
                assert_eq!(cache.get_memory(b"key").unwrap().value(), &[2]);
            } else {
                assert!(read.is_none());
                assert!(cache.get_memory(b"key").is_none());
            }
            assert_eq!(cache.statistics().key_leases, 0);
            cache.close().await.unwrap();
        });
    }
}

#[test]
fn disk_capacity_and_gc_sustain_small_and_large_overwrite_churn() {
    block_on(async {
        let mut options = Options::default();
        options.disk.capacity = Some(512 << 10);
        options.disk.entries = Some(16);
        let cache = cache(device(), options).await;
        for round in 0..400_u32 {
            let key = bytes(format!("key-{}", round % 24));
            let value: Bytes = vec![(round % 251) as u8; if round % 2 == 0 { 60 << 10 } else { 128 }].into();
            cache
                .insert(key.clone(), value.clone())
                .await
                .unwrap_or_else(|e| panic!("round {round}: {e}"));
            cache.clear_memory();
            assert_eq!(cache.get(&key).await.unwrap().unwrap().value(), value.as_ref());
            assert!(cache.statistics().disk_bytes <= 512 << 10);
            assert!(cache.statistics().disk_entries <= 16);
        }
        assert!(cache.statistics().disk_evictions > 0);
        cache.close().await.unwrap();
    });
}

#[test]
fn failed_writes_and_deletes_preserve_disk_versions_release_reservations_and_allow_retry() {
    block_on(async {
        let device = device();
        let cache = cache(device.clone(), Options::default()).await;
        cache.insert(bytes(b"key"), vec![1].into()).await.unwrap();
        for delete in [false, true] {
            device.fail_writes_in(Some(0..device.capacity()));
            if delete {
                assert!(cache.invalidate(&bytes(b"key")).await.is_err());
            } else {
                assert!(cache.insert(bytes(b"key"), vec![2].into()).await.is_err());
            }
            device.fail_writes_in(None);
            assert_eq!(cache.statistics().disk_entries, 1);
            assert_eq!(cache.statistics().key_leases, 0);
            assert_eq!(cache.get(&bytes(b"key")).await.unwrap().unwrap().value(), &[1]);
            assert!(cache.flush().await.is_err());
            cache.flush().await.unwrap();
        }
        assert!(cache.invalidate(&bytes(b"key")).await.unwrap());
        let token = miss(cache.lookup(&bytes(b"key")).await.unwrap());
        let sibling = miss(cache.lookup(&bytes(b"key")).await.unwrap());
        device.fail_writes_in(Some(0..device.capacity()));
        assert!(cache.populate(token, vec![2].into()).await.is_err());
        device.fail_writes_in(None);
        assert!(!sibling.is_valid());
        assert!(cache.populate(sibling, vec![3].into()).await.unwrap().is_none());
        assert!(cache.flush().await.is_err());
        let token = miss(cache.lookup(&bytes(b"key")).await.unwrap());
        assert_eq!(
            cache.populate(token, vec![4].into()).await.unwrap().unwrap().value(),
            &[4]
        );
        cache.close().await.unwrap();
    });
}

#[cfg(target_os = "linux")]
#[test]
fn uring_multiple_disks_recover_after_device_order_changes() {
    block_on(async {
        let files = [
            tempfile::NamedTempFile::new().unwrap(),
            tempfile::NamedTempFile::new().unwrap(),
        ];
        let mut devices: Vec<Arc<dyn Device>> = Vec::new();
        for (index, file) in files.iter().enumerate() {
            let device = Arc::new(moat_engine::FileDevice::create(file.path(), 17 * SEGMENT, false).unwrap());
            moat_engine::format(
                &*device,
                &FormatOptions {
                    segment_size: SEGMENT,
                    chunk_max: 64 << 10,
                    disk_uuid: [index as u8 + 1; 16],
                },
            )
            .unwrap();
            devices.push(device);
        }
        let cache = Cache::new(
            MemoryCache::builder(64).shards(1),
            store_on(devices.clone(), QueueBackend::Uring),
            Options::default(),
        )
        .await
        .unwrap();
        let writes = (0..32)
            .map(|i| cache.insert(bytes(format!("key-{i}")), vec![i as u8; 60 << 10].into()))
            .collect::<Vec<_>>();
        for result in futures_util::future::join_all(writes).await {
            result.unwrap();
        }
        assert_eq!(cache.statistics().disk_entries, 32);
        cache.close().await.unwrap();
        devices.reverse();
        let cache = Cache::new(
            MemoryCache::builder(64).shards(1),
            store_on(devices, QueueBackend::Uring),
            Options::default(),
        )
        .await
        .unwrap();
        assert_eq!(cache.statistics().disk_entries, 32);
        for i in 0..32 {
            let key = bytes(format!("key-{i}"));
            let entry = cache.get(&key).await.unwrap().unwrap();
            assert_eq!(entry.value(), &vec![i as u8; 60 << 10]);
        }
        cache.close().await.unwrap();
    });
}

struct WriteGate {
    data: Arc<MemDevice>,
    gate: Arc<Gate>,
}
impl Device for WriteGate {
    fn capacity(&self) -> u64 {
        self.data.capacity()
    }
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        self.data.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<()> {
        self.gate.block();
        self.data.write_at(buf, offset)
    }
    fn sync(&self) -> std::io::Result<()> {
        self.data.sync()
    }
    fn fd(&self) -> Option<std::os::fd::BorrowedFd<'_>> {
        None
    }
}

#[test]
fn close_bypasses_exhausted_admission_and_finishes_after_its_reply_is_cancelled() {
    block_on(async {
        let gate = Arc::new(Gate::default());
        let device = Arc::new(WriteGate {
            data: device(),
            gate: gate.clone(),
        });
        let (closed, observed) = std::sync::mpsc::channel();
        let memory = MemoryCache::<Bytes, Bytes, Bytes>::builder(4)
            .shards(1)
            .listener(move |removal| {
                if removal.entry.key().as_ref() == b"sentinel" {
                    let _ = closed.send(());
                }
            });
        let options = Options {
            pending_operations: 1,
            ..Default::default()
        };
        let cache = Cache::new(memory, store(device.clone()), options).await.unwrap();
        cache.insert(bytes(b"sentinel"), vec![1].into()).await.unwrap();
        // The reply may precede coordinator permit destruction. Start the
        // saturation scenario only once that earlier permit has been released.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while cache.statistics().pending_operations != 0 {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        let token = miss(cache.lookup(&bytes(b"miss")).await.unwrap());
        gate.arm();
        let release = ReleaseGate(gate.clone());
        let pending = cache.insert(bytes(b"pending"), vec![2].into());
        gate.wait();
        assert!(matches!(
            cache.insert(bytes(b"busy"), vec![3].into()).await,
            Err(Error::Busy)
        ));
        assert!(matches!(cache.flush().await, Err(Error::Busy)));
        drop(cache.close());
        assert!(!token.is_valid());
        drop(token);
        assert!(matches!(
            cache.insert(bytes(b"closed"), vec![4].into()).await,
            Err(Error::Closed)
        ));
        drop(release);
        assert!(!pending.await.unwrap().is_resident());
        observed.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        assert_eq!(cache.statistics().key_leases, 0);
        assert!(cache.get_memory(b"sentinel").is_none());
        let reopened = Cache::new(MemoryCache::builder(4).shards(1), store(device), Options::default())
            .await
            .unwrap();
        assert_eq!(reopened.get(&bytes(b"pending")).await.unwrap().unwrap().value(), &[2]);
        assert_eq!(reopened.get(&bytes(b"sentinel")).await.unwrap().unwrap().value(), &[1]);
        reopened.close().await.unwrap();
    });
}

#[test]
fn disk_sieve_gives_disk_hits_a_second_chance() {
    block_on(async {
        let mut options = Options::default();
        options.disk.entries = Some(3);
        options.disk.policy = DiskPolicy::Sieve;
        let cache = cache(device(), options).await;
        for key in ["a", "b", "c"] {
            cache.insert(bytes(key), vec![1].into()).await.unwrap();
        }
        cache.clear_memory();
        cache.get(&bytes(b"a")).await.unwrap().unwrap();
        cache.insert(bytes(b"d"), vec![4].into()).await.unwrap();
        cache.clear_memory();
        assert!(cache.get(&bytes(b"a")).await.unwrap().is_some());
        assert!(cache.get(&bytes(b"b")).await.unwrap().is_none());
        assert!(cache.get(&bytes(b"c")).await.unwrap().is_some());
        assert!(cache.get(&bytes(b"d")).await.unwrap().is_some());
        cache.close().await.unwrap();
    });
}
