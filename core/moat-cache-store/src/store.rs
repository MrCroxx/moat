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

use std::{
    collections::HashSet,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
};

use futures_channel::oneshot;
use moat_common::ChunkId;
use moat_engine::{Engine, QueueBackend, QueueOptions, ReclaimReport, Usage};
use moat_server::{Placement, Target};
use parking_lot::RwLock;

use crate::{
    Chunk, DeleteResult, Error, Request, Result,
    budget::{Budget, Permit},
    command::{Command, FenceReply},
    worker,
};

/// Limits and I/O configuration for the adapter's per-disk workers.
#[derive(Debug, Clone)]
pub struct Options {
    /// Optional delivery on an application's existing executor. Batches read
    /// replies and fences to avoid one cross-thread scheduler wake per result.
    /// None delivers directly on I/O workers and needs no executor.
    pub completion_executor: Option<Arc<dyn crate::CompletionExecutor>>,
    /// Maximum admitted requests across all disks, including coalesced waiters.
    pub max_requests: usize,
    /// Maximum bytes retained by queued writes and pending/completed reads.
    /// Engine write buffers are separately bounded by each fixed I/O pool.
    pub max_bytes: usize,
    /// Per-worker queue and pool. At least sixteen maximum-size buffers are
    /// required; retained reads may use at most half of the configured pool.
    pub queue: QueueOptions,
    /// Queue backend. In-memory devices require explicit `Sync` on Linux.
    pub backend: QueueBackend,
    /// Idle wait between progress attempts. Zero enables busy polling.
    pub idle_wait: std::time::Duration,
    /// Optional CPU per disk worker; empty leaves placement to the scheduler.
    pub worker_cpus: Vec<usize>,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            completion_executor: None,
            max_requests: 4096,
            max_bytes: 256 << 20,
            queue: QueueOptions::default(),
            backend: QueueBackend::Auto,
            idle_wait: std::time::Duration::from_micros(50),
            worker_cpus: Vec::new(),
        }
    }
}

/// Stable disk geometry, in the order supplied to [`Store::new`].
#[derive(Debug, Clone, Copy)]
pub struct DiskInfo {
    /// Persistent disk UUID used for rendezvous placement.
    pub uuid: [u8; 16],
    /// Device capacity in bytes.
    pub capacity: u64,
    /// Engine segment size in bytes.
    pub segment_size: u64,
    /// Maximum encoded chunk size in bytes.
    pub chunk_max: u32,
    /// Conservative live-entry budget that does not require index growth.
    pub index_entries: usize,
}

/// One completed live chunk, without a physical location or decoded cache key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InventoryEntry {
    /// Disk index in this store's configuration.
    pub disk: usize,
    /// Opaque physical chunk identity.
    pub id: ChunkId,
    /// Completed record LSN.
    pub lsn: u64,
    /// Encoded chunk length.
    pub len: u32,
}

/// Adapter counters and currently charged resources.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Statistics {
    /// Physical reads submitted to engines.
    pub physical_reads: usize,
    /// Logical reads joined to a compatible read operation.
    pub coalesced_reads: usize,
    /// Requests admitted but not yet completed.
    pub requests: usize,
    /// Charged queued-write and pending/retained-read bytes.
    pub bytes: usize,
    /// The pending/retained-read part of `bytes`.
    pub read_bytes: usize,
}
#[derive(Default)]
#[repr(align(64))]
pub(crate) struct Counters {
    pub reads: AtomicUsize,
    pub coalesced: AtomicUsize,
}

#[repr(align(64))]
struct Admission {
    closed: bool,
    sender: mpsc::Sender<Command>,
}
struct Inner {
    admission: Vec<RwLock<Admission>>,
    engines: Vec<Engine>,
    info: Vec<DiskInfo>,
    placement: Placement,
    budget: Arc<Budget>,
    counters: Vec<Arc<Counters>>,
}

/// Cloneable, runtime-independent asynchronous access to exclusively owned
/// engine writers. Dropping the final store handle drains and seals workers
/// in the background; use [`Self::close`] to observe shutdown and its errors.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}

/// A chunk's placement within its originating store. The fixed disk set and
/// borrowed store make this reusable without repeating rendezvous hashing.
#[derive(Clone, Copy)]
pub struct ReadLocation<'a> {
    store: &'a Store,
    id: ChunkId,
    disk: usize,
}
impl ReadLocation<'_> {
    /// Index in the originating store's disk list.
    pub fn disk(&self) -> usize {
        self.disk
    }
    /// Admits a read using the already resolved placement.
    pub fn get(&self, range: Option<Range<u64>>) -> Request<Option<Arc<Chunk>>> {
        self.store.submit(self.disk, 0, true, |reply, permit| Command::Read {
            id: self.id,
            range,
            reply,
            permit,
        })
    }
}
impl Store {
    /// Whether this is the only handle to the adapter. A consumer that takes
    /// ownership can check this before maintaining an exclusive live catalog.
    /// Previously admitted requests may still be running; use inventory fences
    /// to collect their completed state after transferring the handle.
    pub fn is_unique(&self) -> bool {
        Arc::strong_count(&self.inner) == 1
    }

    /// Acquires each engine's unique writer and starts its worker. Returns the
    /// recovered live inventory before any adapter request is admitted.
    ///
    /// The caller owns device formatting and engine recovery configuration.
    /// No other writer may mutate these engines while the store is open.
    pub fn new(engines: Vec<Engine>, options: Options) -> Result<(Self, Vec<InventoryEntry>)> {
        if engines.is_empty() {
            return Err(Error::Invalid("at least one engine is required"));
        }
        if options.max_requests == 0 {
            return Err(Error::Invalid("max_requests must be nonzero"));
        }
        let class = options.queue.pool.max_class;
        if !class.is_power_of_two() || class < 4096 || options.queue.pool.bytes / class < 16 {
            return Err(Error::Invalid("pool must hold at least sixteen maximum-size buffers"));
        }
        if options.max_bytes < class {
            return Err(Error::Invalid("byte budget must cover one maximum-size read"));
        }
        if !options.worker_cpus.is_empty() && options.worker_cpus.len() != engines.len() {
            return Err(Error::Invalid("CPU list must be empty or contain one CPU per disk"));
        }
        let mut identities = HashSet::new();
        for engine in &engines {
            if !identities.insert(engine.disk_uuid()) {
                return Err(Error::Invalid("duplicate disk UUID"));
            }
        }
        let info: Vec<_> = engines
            .iter()
            .map(|engine| DiskInfo {
                uuid: engine.disk_uuid(),
                capacity: engine.capacity(),
                segment_size: engine.segment_size(),
                chunk_max: engine.chunk_max(),
                index_entries: engine.index_entries_without_growth(),
            })
            .collect();
        let placement = Placement::new(
            info.iter()
                .map(|d| Target {
                    uuid: d.uuid,
                    weight: d.capacity,
                })
                .collect(),
        );
        let budget = Budget::new(
            options.max_requests,
            options.max_bytes,
            options.queue.pool.bytes / 2,
            engines.len(),
            class,
        );
        let counters: Vec<_> = (0..engines.len()).map(|_| Arc::new(Counters::default())).collect();
        let mut senders = Vec::new();
        let mut handles = Vec::new();
        let mut inventory = Vec::new();
        for (disk, engine) in engines.iter().enumerate() {
            let (sender, receiver) = mpsc::channel();
            match worker::spawn(disk, engine.clone(), options.clone(), receiver, counters[disk].clone()) {
                Ok((entries, handle)) => {
                    inventory.extend(entries);
                    handles.push(handle);
                    senders.push(sender);
                }
                Err(error) => {
                    // Constructor failure must release every writer before it
                    // returns, so an immediate retry cannot race cleanup.
                    drop(senders);
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(error);
                }
            }
        }
        Ok((
            Self {
                inner: Arc::new(Inner {
                    admission: senders
                        .into_iter()
                        .map(|sender| RwLock::new(Admission { closed: false, sender }))
                        .collect(),
                    engines,
                    info,
                    placement,
                    budget,
                    counters,
                }),
            },
            inventory,
        ))
    }

    /// Immutable disk geometry. Disk list changes require explicit migration
    /// or rebuilding the cache; opening a different list does not migrate data.
    pub fn disks(&self) -> &[DiskInfo] {
        &self.inner.info
    }
    /// The configured disk for a ChunkId, using persistent UUID-based placement.
    pub fn disk_of(&self, id: &ChunkId) -> usize {
        self.inner.placement.disk_of(id).expect("nonempty store")
    }
    /// Resolves a read location tied to this store's fixed placement.
    pub fn locate(&self, id: ChunkId) -> ReadLocation<'_> {
        ReadLocation {
            store: self,
            id,
            disk: self.disk_of(&id),
        }
    }
    /// Current physical usage, for the upper layer's capacity controller.
    pub fn usage(&self, disk: usize) -> Result<Usage> {
        self.inner
            .engines
            .get(disk)
            .map(Engine::usage)
            .ok_or(Error::Invalid("disk index out of bounds"))
    }
    /// Physical costs of a prospective write under this disk's engine options.
    pub fn write_accounting(&self, disk: usize, len: u32) -> Result<moat_engine::WriteAccounting> {
        self.inner
            .engines
            .get(disk)
            .ok_or(Error::Invalid("disk index out of bounds"))?
            .write_accounting(len)
            .map_err(Error::from)
    }

    /// Collects an approximate resource and operation snapshot.
    pub fn statistics(&self) -> Statistics {
        let budget = self.inner.budget.snapshot();
        Statistics {
            physical_reads: self
                .inner
                .counters
                .iter()
                .map(|c| c.reads.load(Ordering::Relaxed))
                .fold(0, usize::wrapping_add),
            coalesced_reads: self
                .inner
                .counters
                .iter()
                .map(|c| c.coalesced.load(Ordering::Relaxed))
                .fold(0, usize::wrapping_add),
            requests: budget.requests,
            bytes: budget.bytes,
            read_bytes: budget.reads.iter().sum(),
        }
    }

    fn submit<T>(
        &self,
        disk: usize,
        bytes: usize,
        read: bool,
        make: impl FnOnce(oneshot::Sender<Result<T>>, Permit) -> Command,
    ) -> Request<T> {
        let Some(lock) = self.inner.admission.get(disk) else {
            return Request::ready(Err(Error::Invalid("disk index out of bounds")));
        };
        let admission = lock.read();
        if admission.closed {
            return Request::ready(Err(Error::Closed));
        }
        let permit = match self.inner.budget.reserve(bytes, read.then_some(disk)) {
            Ok(permit) => permit,
            Err(error) => return Request::ready(Err(error)),
        };
        let (reply, receiver) = oneshot::channel();
        if let Err(error) = admission.sender.send(make(reply, permit)) {
            error.0.fail(Error::Closed);
        }
        Request { receiver }
    }

    /// Reads a whole chunk or a range. Only identical range requests are
    /// coalesced, and only without an intervening mutation or disk barrier.
    /// Out-of-bounds range endpoints are clamped by the engine.
    pub fn get(&self, id: ChunkId, range: Option<Range<u64>>) -> Request<Option<Arc<Chunk>>> {
        self.locate(id).get(range)
    }
    /// Appends an explicit overwrite. Completion establishes engine visibility;
    /// power-loss durability additionally depends on explicit flush and the
    /// engine/device sync configuration.
    pub fn put(&self, id: ChunkId, value: Arc<[u8]>) -> Request<u64> {
        let disk = self.disk_of(&id);
        if value.len() > self.inner.info[disk].chunk_max as usize {
            return Request::ready(Err(moat_engine::Error::ValueTooLarge {
                len: value.len() as u64,
                max: self.inner.info[disk].chunk_max as u64,
            }
            .into()));
        }
        self.submit(disk, value.len(), false, |reply, permit| Command::Put {
            id,
            value,
            reply,
            permit,
        })
    }
    /// Deletes a chunk. When `expected_lsn` is supplied, a newer or different
    /// version is preserved and reported as [`DeleteResult::Changed`].
    pub fn delete(&self, id: ChunkId, expected_lsn: Option<u64>) -> Request<DeleteResult> {
        self.submit(self.disk_of(&id), 0, false, |reply, permit| Command::Delete {
            id,
            expected_lsn,
            reply,
            permit,
        })
    }
    /// Takes a completed inventory after all previously admitted operations on
    /// this disk. Later operations wait until the snapshot is collected.
    pub fn inventory(&self, disk: usize) -> Request<Vec<InventoryEntry>> {
        self.submit(disk, 0, false, |reply, permit| Command::Fence {
            reply: FenceReply::Inventory(reply),
            permit: Some(permit),
        })
    }
    /// Requests one physical GC pass after earlier operations on this disk.
    /// Live chunks remain live; only upper-layer explicit deletes remove data.
    pub fn reclaim(&self, disk: usize) -> Request<Option<ReclaimReport>> {
        self.submit(disk, 0, false, |reply, permit| Command::Fence {
            reply: FenceReply::Reclaim(reply),
            permit: Some(permit),
        })
    }

    fn fences(&self, close: bool) -> Vec<Request<()>> {
        // Taking every disk gate in a fixed order gives the fence one common
        // admission boundary without a global lock on ordinary submissions.
        let mut admission: Vec<_> = self.inner.admission.iter().map(|gate| gate.write()).collect();
        if admission.iter().any(|gate| gate.closed) {
            return vec![Request::ready(Err(Error::Closed))];
        }
        // Reserve all flush credits before publishing any disk fence. Close
        // has a single bounded control message per disk and needs no credit.
        let mut permits = Vec::new();
        for _ in &admission {
            if close {
                permits.push(None);
            } else {
                match self.inner.budget.reserve(0, None) {
                    Ok(permit) => permits.push(Some(permit)),
                    Err(error) => return vec![Request::ready(Err(error))],
                }
            }
        }
        if close {
            for gate in &mut admission {
                gate.closed = true;
            }
        }
        admission
            .iter()
            .zip(permits)
            .map(|(gate, permit)| {
                let (reply, receiver) = oneshot::channel();
                let reply = if close {
                    FenceReply::Close(Some(reply))
                } else {
                    FenceReply::Flush(reply)
                };
                if let Err(error) = gate.sender.send(Command::Fence { reply, permit }) {
                    error.0.fail(Error::Closed);
                }
                Request { receiver }
            })
            .collect()
    }
    /// Flushes all disks after earlier admissions, collecting every disk's
    /// outcome before returning the first error. Admission occurs on first poll.
    pub async fn flush(&self) -> Result<()> {
        wait_all(self.fences(false)).await
    }
    /// Stops new admissions, drains accepted requests, seals and detaches every
    /// worker, and returns the first failure after observing all workers.
    /// Subsequent close calls return Closed. Admission occurs on first poll.
    pub async fn close(&self) -> Result<()> {
        wait_all(self.fences(true)).await
    }
}
async fn wait_all(requests: Vec<Request<()>>) -> Result<()> {
    let mut first = None;
    for request in requests {
        if let Err(error) = request.await {
            first.get_or_insert(error);
        }
    }
    first.map_or(Ok(()), Err)
}

#[cfg(test)]
mod tests {
    use std::{
        thread,
        time::{Duration, Instant},
    };

    use super::*;

    fn gated_store() -> (Store, Vec<mpsc::Receiver<Command>>) {
        let mut receivers = Vec::new();
        let admission = (0..2)
            .map(|_| {
                let (sender, receiver) = mpsc::channel();
                receivers.push(receiver);
                RwLock::new(Admission { closed: false, sender })
            })
            .collect();
        let store = Store {
            inner: Arc::new(Inner {
                admission,
                engines: Vec::new(),
                info: Vec::new(),
                placement: Placement::new(Vec::new()),
                budget: Budget::new(8, 64, 32, 2, 16),
                counters: (0..2).map(|_| Arc::new(Counters::default())).collect(),
            }),
        };
        (store, receivers)
    }

    #[test]
    fn global_fences_publish_only_after_all_disk_gates_are_locked() {
        for close in [false, true] {
            let (store, receivers) = gated_store();
            let held = store.inner.admission[1].read();
            let copy = store.clone();
            let task = thread::spawn(move || copy.fences(close));
            let deadline = Instant::now() + Duration::from_secs(5);
            while store.inner.admission[0].try_read().is_some() {
                assert!(Instant::now() < deadline, "fence did not acquire first disk gate");
                thread::yield_now();
            }
            assert!(matches!(receivers[0].try_recv(), Err(mpsc::TryRecvError::Empty)));
            drop(held);
            let replies = task.join().unwrap();
            assert_eq!(replies.len(), 2);
            for (disk, receiver) in receivers.iter().enumerate() {
                assert!(matches!(receiver.try_recv().unwrap(), Command::Fence { .. }));
                assert_eq!(store.inner.admission[disk].read().closed, close);
            }
            assert_eq!(store.statistics().requests, 0);
            if close {
                assert!(matches!(
                    futures_executor::block_on(store.inventory(0)),
                    Err(Error::Closed)
                ));
                assert!(matches!(
                    futures_executor::block_on(store.inventory(1)),
                    Err(Error::Closed)
                ));
            }
        }
    }
}
