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

//! Adapter ordering, bounded ownership, cancellation, faults and warm restart.

use std::{io, os::fd::BorrowedFd, sync::Arc, time::Duration};

use futures_executor::block_on;
use futures_util::{FutureExt, future::join_all};
use moat_cache_store::{DeleteResult, Error, Options, Store};
use moat_common::{ChunkId, HugePages, PoolOptions};
use moat_engine::{Device, Engine, FormatOptions, MemDevice, QueueBackend, QueueOptions};
use parking_lot::{Condvar, Mutex};

const SEGMENT: u64 = 1 << 20;
fn id(n: u128) -> ChunkId {
    ChunkId::from_u128(n)
}
fn bytes(value: &[u8]) -> Arc<[u8]> {
    Arc::from(value)
}
fn options() -> Options {
    Options {
        max_requests: 128,
        max_bytes: 32 << 20,
        backend: QueueBackend::Sync,
        queue: QueueOptions {
            depth: 8,
            descriptors: 4,
            pool: PoolOptions {
                bytes: 32 << 20,
                max_class: 1 << 20,
                huge_pages: HugePages::Disabled,
            },
        },
        ..Default::default()
    }
}
fn engine(device: Arc<dyn Device>) -> Engine {
    moat_engine::open(
        device,
        moat_engine::Options {
            index_capacity: 1024,
            batch_limit: 128 << 10,
            verify_reads: true,
            ..Default::default()
        },
    )
    .unwrap()
    .0
}
fn device(uuid: u8) -> Arc<GateDevice> {
    let device = Arc::new(GateDevice {
        data: MemDevice::new(SEGMENT * 17),
        gate: Mutex::new((false, false)),
        changed: Condvar::new(),
    });
    moat_engine::format(
        &*device,
        &FormatOptions {
            segment_size: SEGMENT,
            chunk_max: 128 << 10,
            disk_uuid: [uuid; 16],
        },
    )
    .unwrap();
    device
}
struct GateDevice {
    data: MemDevice,
    gate: Mutex<(bool, bool)>,
    changed: Condvar,
}
impl GateDevice {
    fn arm(&self) {
        *self.gate.lock() = (true, false);
    }
    fn wait(&self) {
        let mut state = self.gate.lock();
        while !state.1 {
            assert!(
                !self.changed.wait_for(&mut state, Duration::from_secs(5)).timed_out(),
                "worker never entered write"
            );
        }
    }
    fn release(&self) {
        self.gate.lock().0 = false;
        self.changed.notify_all();
    }
}
impl Device for GateDevice {
    fn capacity(&self) -> u64 {
        self.data.capacity()
    }
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.data.read_at(buf, offset)
    }
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let mut state = self.gate.lock();
        if state.0 {
            state.1 = true;
            self.changed.notify_all();
        }
        while state.0 {
            self.changed.wait(&mut state);
        }
        drop(state);
        self.data.write_at(buf, offset)
    }
    fn sync(&self) -> io::Result<()> {
        self.data.sync()
    }
    fn fd(&self) -> Option<BorrowedFd<'_>> {
        None
    }
}

#[test]
fn coalesced_read_waiters_share_one_buffer_and_survive_leader_cancellation() {
    let device = device(1);
    let (store, inventory) = Store::new(vec![engine(device.clone())], options()).unwrap();
    assert!(inventory.is_empty());
    block_on(store.put(id(1), bytes(b"shared"))).unwrap();
    device.arm();
    let writer = store.put(id(2), bytes(b"hold worker"));
    device.wait();
    let mut requests = (0..12).map(|_| store.get(id(1), None)).collect::<Vec<_>>();
    drop(requests.remove(0));
    device.release();
    block_on(writer).unwrap();
    let results = block_on(join_all(requests))
        .into_iter()
        .map(|r| r.unwrap().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(&**results[0], b"shared");
    for result in &results[1..] {
        assert!(Arc::ptr_eq(&results[0], result));
    }
    assert_eq!(store.statistics().physical_reads, 1);
    assert_eq!(store.statistics().coalesced_reads, 11);
    assert_eq!(store.statistics().requests, 0);
    assert!(store.statistics().read_bytes > 0);
    drop(results);
    block_on(store.flush()).unwrap();
    assert_eq!(store.statistics().bytes, 0);
    block_on(store.close()).unwrap();
}

#[test]
fn read_write_delete_order_prevents_cross_generation_coalescing() {
    let device = device(2);
    let (store, _) = Store::new(vec![engine(device.clone())], options()).unwrap();
    let old_lsn = block_on(store.put(id(1), bytes(b"old"))).unwrap();
    device.arm();
    let hold = store.put(id(2), bytes(b"hold"));
    device.wait();
    let old = store.get(id(1), None);
    let write = store.put(id(1), bytes(b"new"));
    let new = store.get(id(1), None);
    let delete = store.delete(id(1), None);
    let miss = store.get(id(1), None);
    device.release();
    block_on(hold).unwrap();
    let old = block_on(old).unwrap().unwrap();
    let written_lsn = block_on(write).unwrap();
    let new = block_on(new).unwrap().unwrap();
    assert_eq!(&**old, b"old");
    assert_eq!(old.lsn(), old_lsn);
    assert_eq!(&**new, b"new");
    assert_eq!(new.lsn(), written_lsn);
    assert!(written_lsn > old_lsn);
    assert!(matches!(block_on(delete).unwrap(), DeleteResult::Deleted(lsn) if lsn > written_lsn));
    assert!(block_on(miss).unwrap().is_none());
    assert_eq!(store.statistics().physical_reads, 2);
    block_on(store.close()).unwrap();
    // Completed buffers remain usable after the queue and writer detach.
    assert_eq!(&**old, b"old");
    assert_eq!(&**new, b"new");
}

#[test]
fn ranges_are_clamped_and_only_identical_ranges_are_coalesced() {
    let device = device(3);
    let (store, _) = Store::new(vec![engine(device.clone())], options()).unwrap();
    block_on(store.put(id(1), bytes(b"0123456789"))).unwrap();
    device.arm();
    let hold = store.put(id(2), bytes(b"hold"));
    device.wait();
    let a = store.get(id(1), Some(2..5));
    let b = store.get(id(1), Some(2..5));
    let c = store.get(id(1), Some(5..999));
    let d = store.get(id(1), Some(999..1000));
    device.release();
    block_on(hold).unwrap();
    let a = block_on(a).unwrap().unwrap();
    let b = block_on(b).unwrap().unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(&**a, b"234");
    assert_eq!(&**block_on(c).unwrap().unwrap(), b"56789");
    assert!(block_on(d).unwrap().unwrap().is_empty());
    assert_eq!(store.statistics().physical_reads, 3);
    block_on(store.close()).unwrap();
}

#[test]
fn request_and_retained_byte_budgets_recover_after_completion_and_drop() {
    let device = device(4);
    let (store, _) = Store::new(
        vec![engine(device.clone())],
        Options {
            max_requests: 2,
            ..options()
        },
    )
    .unwrap();
    device.arm();
    let a = store.put(id(1), bytes(b"a"));
    device.wait();
    let b = store.put(id(2), bytes(b"b"));
    assert!(matches!(block_on(store.get(id(1), None)), Err(Error::Busy)));
    drop(b); // Cancelling the reply must not cancel the admitted overwrite.
    device.release();
    block_on(a).unwrap();
    block_on(store.flush()).unwrap();
    assert_eq!(&**block_on(store.get(id(2), None)).unwrap().unwrap(), b"b");
    block_on(store.close()).unwrap();

    let (store, _) = Store::new(
        vec![engine(device)],
        Options {
            max_bytes: 1 << 20,
            ..options()
        },
    )
    .unwrap();
    let chunk = block_on(store.get(id(1), None)).unwrap().unwrap();
    assert!(store.statistics().read_bytes > 0);
    let mut pending = Box::pin(store.get(id(2), None));
    assert!(pending.as_mut().now_or_never().is_none());
    drop(chunk);
    let next = block_on(pending).unwrap().unwrap();
    assert_eq!(&**next, b"b");
    drop(next);
    block_on(store.flush()).unwrap();
    assert_eq!(store.statistics().bytes, 0);
    block_on(store.close()).unwrap();
}

#[test]
fn queued_reads_do_not_exhaust_buffer_credit_before_the_worker_can_progress() {
    let device = device(24);
    let mut config = options();
    config.queue.pool.bytes = 16 << 20;
    let (store, _) = Store::new(vec![engine(device.clone())], config).unwrap();
    for n in 0..32 {
        block_on(store.put(id(n), bytes(b"queued"))).unwrap();
    }
    device.arm();
    let hold = store.put(id(100), bytes(b"hold worker"));
    device.wait();
    let requests = (0..32).map(|n| store.get(id(n), None)).collect::<Vec<_>>();
    assert_eq!(store.statistics().requests, 33);
    assert_eq!(store.statistics().read_bytes, 0);
    device.release();
    block_on(hold).unwrap();
    let chunks = block_on(join_all(requests));
    for chunk in &chunks {
        assert_eq!(&***chunk.as_ref().unwrap().as_ref().unwrap(), b"queued");
    }
    assert_eq!(store.statistics().requests, 0);
    assert!(store.statistics().read_bytes <= 8 << 20);
    drop(chunks);
    // A reply can wake its receiver before delivery drops its temporary owner.
    // The fence waits for preceding delivery to finish while keeping the store open.
    block_on(store.flush()).unwrap();
    assert_eq!(store.statistics().bytes, 0);
    block_on(store.close()).unwrap();
}

#[test]
fn cancelled_reads_release_request_slots_without_waiting_for_retained_buffers() {
    let device = device(25);
    let (store, _) = Store::new(
        vec![engine(device)],
        Options {
            max_bytes: 1 << 20,
            ..options()
        },
    )
    .unwrap();
    block_on(store.put(id(1), bytes(b"held"))).unwrap();
    let held = block_on(store.get(id(1), None)).unwrap().unwrap();
    let mut waiting = Box::pin(store.get(id(1), None));
    assert!(waiting.as_mut().now_or_never().is_none());
    drop(waiting);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while store.statistics().requests != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "cancelled request retained its slot"
        );
        std::thread::yield_now();
    }
    assert_eq!(&**held, b"held");
    block_on(store.close()).unwrap();
    drop(held);
    assert_eq!(store.statistics().bytes, 0);
}

#[test]
fn inventory_fences_conditional_deletes_and_restart_keep_completed_versions() {
    let device = device(5);
    let (store, _) = Store::new(vec![engine(device.clone())], options()).unwrap();
    let first = store.put(id(1), bytes(b"one"));
    let inventory = store.inventory(0);
    let second = store.put(id(1), bytes(b"two"));
    let first_lsn = block_on(first).unwrap();
    let inventory = block_on(inventory).unwrap();
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].lsn, first_lsn);
    let second_lsn = block_on(second).unwrap();
    assert_eq!(
        block_on(store.delete(id(1), Some(first_lsn))).unwrap(),
        DeleteResult::Changed
    );
    assert_eq!(block_on(store.delete(id(99), None)).unwrap(), DeleteResult::Missing);
    block_on(store.close()).unwrap();
    assert!(matches!(block_on(store.get(id(1), None)), Err(Error::Closed)));
    let (store, inventory) = Store::new(vec![engine(device)], options()).unwrap();
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].id, id(1));
    assert_eq!(inventory[0].lsn, second_lsn);
    assert_eq!(&**block_on(store.get(id(1), None)).unwrap().unwrap(), b"two");
    assert!(matches!(
        block_on(store.delete(id(1), Some(second_lsn))).unwrap(),
        DeleteResult::Deleted(_)
    ));
    assert!(block_on(store.inventory(0)).unwrap().is_empty());
    block_on(store.close()).unwrap();
}

#[test]
fn failed_delete_keeps_the_value_and_can_be_retried() {
    let device = device(6);
    let (store, _) = Store::new(vec![engine(device.clone())], options()).unwrap();
    let lsn = block_on(store.put(id(1), bytes(b"survivor"))).unwrap();
    device.data.fail_writes_in(Some(0..device.capacity()));
    assert!(matches!(
        block_on(store.delete(id(1), Some(lsn))),
        Err(Error::Engine(_))
    ));
    device.data.fail_writes_in(None);
    assert_eq!(&**block_on(store.get(id(1), None)).unwrap().unwrap(), b"survivor");
    assert!(block_on(store.flush()).is_err()); // Observe the failed mutation.
    assert!(matches!(
        block_on(store.delete(id(1), Some(lsn))).unwrap(),
        DeleteResult::Deleted(_)
    ));
    block_on(store.close()).unwrap();
    let (_, inventory) = Store::new(vec![engine(device)], options()).unwrap();
    assert!(inventory.is_empty());
}

#[test]
fn per_disk_workers_progress_independently_and_placement_survives_reordering() {
    let a = device(7);
    let b = device(8);
    let (store, _) = Store::new(vec![engine(a.clone()), engine(b.clone())], options()).unwrap();
    let key_a = (0..1000).map(id).find(|key| store.disk_of(key) == 0).unwrap();
    let key_b = (0..1000).map(id).find(|key| store.disk_of(key) == 1).unwrap();
    a.arm();
    let held = store.put(key_a, bytes(b"a"));
    a.wait();
    block_on(store.put(key_b, bytes(b"b"))).unwrap();
    assert_eq!(&**block_on(store.get(key_b, None)).unwrap().unwrap(), b"b");
    a.release();
    block_on(held).unwrap();
    block_on(store.close()).unwrap();
    let (store, inventory) = Store::new(vec![engine(b), engine(a)], options()).unwrap();
    assert_eq!(inventory.len(), 2);
    assert_eq!(store.disk_of(&key_a), 1);
    assert_eq!(store.disk_of(&key_b), 0);
    assert_eq!(&**block_on(store.get(key_a, None)).unwrap().unwrap(), b"a");
    assert_eq!(&**block_on(store.get(key_b, None)).unwrap().unwrap(), b"b");
    block_on(store.close()).unwrap();
}

#[test]
fn startup_failure_releases_previously_acquired_writers_before_returning() {
    let a = engine(device(9));
    let b = engine(device(10));
    let mut q = options().queue.build(QueueBackend::Sync).unwrap();
    let writer = b.writer(q.as_mut()).unwrap();
    assert!(Store::new(vec![a.clone(), b.clone()], options()).is_err());
    // The first worker must have finished shutdown even though the second failed.
    let first = a.writer(q.as_mut()).unwrap();
    first.detach(q.as_mut());
    writer.detach(q.as_mut());
    let (store, _) = Store::new(vec![a, b], options()).unwrap();
    block_on(store.close()).unwrap();
}

#[test]
fn reclaim_preserves_live_chunks_and_frees_explicitly_deleted_space() {
    let device = device(11);
    let (store, _) = Store::new(vec![engine(device.clone())], options()).unwrap();
    for i in 0..32 {
        block_on(store.put(id(i), Arc::from(vec![i as u8; 100_000]))).unwrap();
    }
    for i in 0..20 {
        block_on(store.delete(id(i), None)).unwrap();
    }
    let before = store.usage(0).unwrap().free_segments;
    assert!(block_on(store.reclaim(0)).unwrap().is_some());
    assert!(store.usage(0).unwrap().free_segments >= before);
    for i in 20..32 {
        let data = block_on(store.get(id(i), None)).unwrap().unwrap();
        assert_eq!(data.len(), 100_000);
        assert!(data.iter().all(|&byte| byte == i as u8));
    }
    block_on(store.close()).unwrap();
    let (store, inventory) = Store::new(vec![engine(device)], options()).unwrap();
    assert_eq!(inventory.len(), 12);
    block_on(store.close()).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn uring_file_backend_drives_the_same_adapter_contract() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let device = Arc::new(moat_engine::FileDevice::create(file.path(), 17 * SEGMENT, false).unwrap());
    moat_engine::format(
        &*device,
        &FormatOptions {
            segment_size: SEGMENT,
            chunk_max: 128 << 10,
            disk_uuid: [12; 16],
        },
    )
    .unwrap();
    let mut options = options();
    options.backend = QueueBackend::Uring;
    // Keep registered buffers below a modest locked-memory limit.
    options.queue.pool.bytes = 4 << 20;
    options.queue.pool.max_class = 256 << 10;
    let (store, _) = Store::new(vec![engine(device)], options).unwrap();
    let writes = (0..16)
        .map(|n| store.put(id(n), Arc::from(vec![n as u8; 70_000])))
        .collect::<Vec<_>>();
    for result in block_on(join_all(writes)) {
        result.unwrap();
    }
    let reads = (0..16).map(|n| {
        let read = store.get(id(n), None);
        async move {
            let chunk = read.await.unwrap().unwrap();
            assert!(chunk.iter().all(|&byte| byte == n as u8));
        }
    });
    block_on(join_all(reads));
    block_on(store.close()).unwrap();
}

#[test]
fn corruption_is_shared_with_waiters_and_does_not_leak_buffer_credits() {
    let device = device(13);
    let opened = engine(device.clone());
    let (store, _) = Store::new(vec![opened.clone()], options()).unwrap();
    block_on(store.put(id(1), bytes(b"verified"))).unwrap();
    let stat = opened.stat(&id(1)).unwrap();
    let offset = (SEGMENT * (stat.segment as u64 + 1) + stat.value_offset as u64) as usize;
    device.data.with_data_mut(|data| data[offset] ^= 1);
    device.arm();
    let hold = store.put(id(2), bytes(b"hold"));
    device.wait();
    let a = store.get(id(1), None);
    let b = store.get(id(1), None);
    device.release();
    block_on(hold).unwrap();
    let Error::Engine(a) = block_on(a).unwrap_err() else {
        panic!("expected engine error")
    };
    let Error::Engine(b) = block_on(b).unwrap_err() else {
        panic!("expected engine error")
    };
    assert!(matches!(&*a, moat_engine::Error::Corrupt(_)));
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(store.statistics().requests, 0);
    assert_eq!(store.statistics().bytes, 0);
    device.data.with_data_mut(|data| data[offset] ^= 1);
    assert_eq!(&**block_on(store.get(id(1), None)).unwrap().unwrap(), b"verified");
    block_on(store.close()).unwrap();
}

#[test]
fn close_stops_admission_then_drains_before_releasing_the_writer() {
    let device = device(14);
    let opened = engine(device.clone());
    let (store, _) = Store::new(vec![opened.clone()], options()).unwrap();
    device.arm();
    let write = store.put(id(1), bytes(b"pending"));
    device.wait();
    let read = store.get(id(1), None);
    let mut close = Box::pin(store.close());
    assert!(block_on(async { futures_util::poll!(close.as_mut()) }).is_pending());
    assert!(matches!(block_on(store.put(id(2), bytes(b"late"))), Err(Error::Closed)));
    device.release();
    block_on(close).unwrap();
    block_on(write).unwrap();
    assert_eq!(&**block_on(read).unwrap().unwrap(), b"pending");
    let mut queue = options().queue.build(QueueBackend::Sync).unwrap();
    let writer = opened.writer(queue.as_mut()).unwrap();
    writer.detach(queue.as_mut());
    assert_eq!(store.statistics().requests, 0);
}

#[derive(Default)]
struct ManualDelivery(Mutex<Option<futures_util::future::BoxFuture<'static, ()>>>);
impl std::fmt::Debug for ManualDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManualDelivery")
    }
}
impl moat_cache_store::CompletionExecutor for ManualDelivery {
    fn spawn(&self, task: futures_util::future::BoxFuture<'static, ()>) {
        *self.0.lock() = Some(task);
    }
}

#[test]
fn batched_delivery_retains_admission_and_drains_reads_before_close() {
    let device = device(31);
    let executor = Arc::new(ManualDelivery::default());
    let mut opts = options();
    opts.completion_executor = Some(executor.clone());
    opts.max_requests = 4;
    let (store, _) = Store::new(vec![engine(device.clone())], opts).unwrap();
    block_on(store.put(id(1), bytes(b"shared"))).unwrap();
    device.arm();
    let writer = store.put(id(2), bytes(b"hold worker"));
    device.wait();
    let a = store.get(id(1), None);
    let b = store.get(id(1), None);
    device.release();
    block_on(writer).unwrap();
    let mut close = std::pin::pin!(store.close());
    assert!(close.as_mut().now_or_never().is_none());
    assert!(matches!(block_on(store.get(id(1), None)), Err(Error::Closed)));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while store.statistics().physical_reads == 0 {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(store.statistics().requests >= 2);
    let mut a = std::pin::pin!(a);
    let mut b = std::pin::pin!(b);
    assert!(a.as_mut().now_or_never().is_none());
    assert!(b.as_mut().now_or_never().is_none());
    let mut driver = executor.0.lock().take().unwrap();
    let waker = futures_util::task::noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    loop {
        let _ = driver.as_mut().poll(&mut cx);
        if let Some(result) = close.as_mut().now_or_never() {
            result.unwrap();
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    let a = a.as_mut().now_or_never().unwrap().unwrap().unwrap();
    let b = b.as_mut().now_or_never().unwrap().unwrap().unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(&**a, b"shared");
    assert_eq!(store.statistics().requests, 0);
    drop((a, b, driver));
    assert_eq!(store.statistics().bytes, 0);
}
