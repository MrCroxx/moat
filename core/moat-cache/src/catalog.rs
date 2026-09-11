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

//! A compact disk directory and explicit cache capacity controller.

use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_channel::oneshot;
use futures_util::lock::{Mutex as AsyncMutex, MutexGuard as AsyncGuard};
use moat_cache_store::{DeleteResult, InventoryEntry, Store};
use moat_common::{ChunkId, PAGE_SIZE};
use parking_lot::Mutex;

use crate::{Error, Result};

/// Disk cache replacement policy. Physical engine GC remains policy-free.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DiskPolicy {
    /// Evict the oldest completed insertion or overwrite.
    Fifo,
    /// Give visited entries a second chance while sweeping insertion order.
    #[default]
    Sieve,
}

/// Per-disk cache limits, applied independently to every configured disk.
#[derive(Debug, Clone)]
pub struct DiskOptions {
    /// Encoded live bytes per disk. None uses a quarter of non-reserved capacity.
    pub capacity: Option<u64>,
    /// Live entry limit per disk. None uses the engine's index budget that
    /// avoids growth; an explicit limit cannot exceed that budget.
    pub entries: Option<usize>,
    /// Cache victim selection, followed by explicit conditional deletion.
    pub policy: DiskPolicy,
    /// Free-segment headroom used for deletion, in-flight packing, and GC.
    /// Must be at least four; this is not a live-data eviction policy in engine.
    pub reserve_segments: u32,
}
impl Default for DiskOptions {
    fn default() -> Self {
        Self {
            capacity: None,
            entries: None,
            policy: DiskPolicy::Sieve,
            reserve_segments: 4,
        }
    }
}

#[derive(Clone, Copy)]
struct Slot {
    lsn: u64,
    len: u32,
    position: u64,
    visited: bool,
}
struct Active {
    extra: u64,
    new: bool,
    live: u64,
    allocation: u64,
}
struct State {
    slots: HashMap<ChunkId, Slot>,
    order: BTreeMap<u64, ChunkId>,
    clock: u64,
    hand: u64,
    bytes: u64,
    active: HashMap<ChunkId, Active>,
    extra: u64,
    new: usize,
    live: u64,
    allocation: u64,
    waiters: Vec<oneshot::Sender<()>>,
}
impl State {
    fn insert(&mut self, id: ChunkId, lsn: u64, len: u32) {
        self.remove(id);
        self.clock = self.clock.checked_add(1).expect("disk policy clock exhausted");
        self.slots.insert(
            id,
            Slot {
                lsn,
                len,
                position: self.clock,
                visited: false,
            },
        );
        self.order.insert(self.clock, id);
        self.bytes += len as u64;
    }
    fn remove(&mut self, id: ChunkId) {
        if let Some(old) = self.slots.remove(&id) {
            self.order.remove(&old.position);
            self.bytes -= old.len as u64;
        }
    }
    fn victim(&mut self, policy: DiskPolicy, exclude: Option<ChunkId>) -> Option<(ChunkId, u64)> {
        if policy == DiskPolicy::Fifo {
            return self
                .order
                .values()
                .find(|&&id| Some(id) != exclude && !self.active.contains_key(&id))
                .map(|&id| (id, self.slots[&id].lsn));
        }
        for _ in 0..self.slots.len().saturating_mul(2) {
            let (&position, &id) = self
                .order
                .range((Bound::Excluded(self.hand), Bound::Unbounded))
                .next()
                .or_else(|| self.order.first_key_value())?;
            self.hand = position;
            if Some(id) == exclude || self.active.contains_key(&id) {
                continue;
            }
            let slot = self.slots.get_mut(&id).expect("ordered slot");
            if slot.visited {
                slot.visited = false;
            } else {
                return Some((id, slot.lsn));
            }
        }
        None
    }
    fn wait(&mut self) -> oneshot::Receiver<()> {
        self.waiters.retain(|waiter| !waiter.is_canceled());
        let (sender, receiver) = oneshot::channel();
        self.waiters.push(sender);
        receiver
    }
    fn finish(&mut self, id: ChunkId) -> Vec<oneshot::Sender<()>> {
        let active = self.active.remove(&id).expect("reserved slot");
        self.extra -= active.extra;
        self.new -= usize::from(active.new);
        self.live -= active.live;
        self.allocation -= active.allocation;
        std::mem::take(&mut self.waiters)
    }
}
struct Disk {
    state: Mutex<State>,
    control: AsyncMutex<()>,
    capacity: u64,
    entries: usize,
    physical_live: u64,
    reserve: u64,
    segment_size: u64,
    policy: DiskPolicy,
}

pub(crate) struct Catalog {
    pub store: Store,
    disks: Vec<Arc<Disk>>,
    evictions: AtomicUsize,
}
impl Catalog {
    pub fn new(store: Store, mut inventory: Vec<InventoryEntry>, options: &DiskOptions) -> Result<Self> {
        if options.reserve_segments < 4 {
            return Err(Error::Invalid("at least four reserve segments are required"));
        }
        let mut disks = Vec::new();
        for (index, info) in store.disks().iter().enumerate() {
            let usage = store.usage(index)?;
            if usage.segments <= options.reserve_segments + 2 {
                return Err(Error::Invalid("disk has too few segments for cache GC headroom"));
            }
            if usage.free_segments < 2 {
                return Err(Error::NoSpace);
            }
            let usable = (usage.segments - options.reserve_segments) as u64 * info.segment_size;
            let capacity = options.capacity.unwrap_or(usable / 4);
            if capacity == 0 || capacity > usable / 2 {
                return Err(Error::Invalid(
                    "disk cache capacity must be positive and at most half of non-reserved space",
                ));
            }
            let entries = options.entries.unwrap_or(info.index_entries);
            if entries == 0 || entries > info.index_entries {
                return Err(Error::Invalid(
                    "disk entry limit exceeds the engine index admission budget",
                ));
            }
            disks.push(Arc::new(Disk {
                state: Mutex::new(State {
                    slots: HashMap::new(),
                    order: BTreeMap::new(),
                    clock: 0,
                    hand: 0,
                    bytes: 0,
                    active: HashMap::new(),
                    extra: 0,
                    new: 0,
                    live: 0,
                    allocation: 0,
                    waiters: Vec::new(),
                }),
                control: AsyncMutex::new(()),
                capacity,
                entries,
                physical_live: usable / 2,
                reserve: options.reserve_segments as u64 * info.segment_size,
                segment_size: info.segment_size,
                policy: options.policy,
            }));
        }
        inventory.sort_unstable_by_key(|entry| (entry.disk, entry.lsn));
        for entry in inventory {
            if entry.disk >= disks.len() || store.disk_of(&entry.id) != entry.disk {
                return Err(Error::Invalid(
                    "inventory does not match this disk placement; migration or rebuild is required",
                ));
            }
            let mut state = disks[entry.disk].state.lock();
            if state.slots.contains_key(&entry.id) {
                return Err(Error::Invalid("duplicate live inventory identity"));
            }
            state.insert(entry.id, entry.lsn, entry.len);
        }
        Ok(Self {
            store,
            disks,
            evictions: AtomicUsize::new(0),
        })
    }

    pub fn version(&self, id: ChunkId) -> Option<u64> {
        self.version_on(self.store.disk_of(&id), id)
    }
    pub fn version_on(&self, disk: usize, id: ChunkId) -> Option<u64> {
        let state = self.disks[disk].state.lock();
        if state.active.contains_key(&id) {
            None
        } else {
            state.slots.get(&id).map(|slot| slot.lsn)
        }
    }
    pub fn validate_hit(&self, disk: usize, id: ChunkId, lsn: u64) -> bool {
        let mut state = self.disks[disk].state.lock();
        if state.active.contains_key(&id) {
            return false;
        }
        if let Some(slot) = state.slots.get_mut(&id)
            && slot.lsn == lsn
        {
            slot.visited = true;
            return true;
        }
        false
    }
    pub async fn control(&self, disk: usize) -> AsyncGuard<'_, ()> {
        self.disks[disk].control.lock().await
    }
    pub fn remove(&self, id: ChunkId, expected: u64) {
        let mut state = self.disks[self.store.disk_of(&id)].state.lock();
        if state.slots.get(&id).is_some_and(|slot| slot.lsn == expected) {
            state.remove(id);
        }
    }
    pub fn snapshot(&self) -> (usize, u64, usize) {
        let (entries, bytes) = self.disks.iter().fold((0, 0), |(entries, bytes), disk| {
            let state = disk.state.lock();
            (entries + state.slots.len(), bytes + state.bytes)
        });
        (entries, bytes, self.evictions.load(Ordering::Relaxed))
    }

    async fn evict(&self, disk: usize, id: ChunkId, lsn: u64) -> Result<()> {
        match self.store.delete(id, Some(lsn)).await? {
            DeleteResult::Deleted(_) | DeleteResult::Missing => {
                self.disks[disk].state.lock().remove(id);
                self.evictions.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            DeleteResult::Changed => Err(Error::Corrupt("catalog version changed outside cache ownership")),
        }
    }

    /// Enforces possibly smaller startup limits without decoding all disk keys.
    pub async fn trim(&self) -> Result<()> {
        for disk in 0..self.disks.len() {
            let _control = self.control(disk).await;
            loop {
                let usage = self.store.usage(disk)?;
                let selected = {
                    let mut state = self.disks[disk].state.lock();
                    if state.bytes <= self.disks[disk].capacity
                        && state.slots.len() <= self.disks[disk].entries
                        && usage.live_bytes <= self.disks[disk].physical_live
                    {
                        break;
                    }
                    state.victim(self.disks[disk].policy, None).ok_or(Error::NoSpace)?
                };
                self.evict(disk, selected.0, selected.1).await?;
            }
        }
        Ok(())
    }

    pub async fn write(&self, id: ChunkId, value: Arc<[u8]>) -> Result<u64> {
        let disk_index = self.store.disk_of(&id);
        let disk = &self.disks[disk_index];
        let accounting = self.store.write_accounting(disk_index, value.len() as u32)?;
        let allocation = accounting.batch_bytes.saturating_mul(2).saturating_add(3 * PAGE_SIZE);
        if value.len() as u64 > disk.capacity || accounting.record_bytes > disk.physical_live {
            return Err(Error::NoSpace);
        }
        let control = self.control(disk_index).await;
        let mut gc_passes = 0;
        let reservation = loop {
            let usage = self.store.usage(disk_index)?;
            let action = {
                let mut state = disk.state.lock();
                if state.active.contains_key(&id) {
                    return Err(Error::Busy);
                }
                let old_len = state.slots.get(&id).map_or(0, |slot| slot.len as u64);
                let extra = (value.len() as u64).saturating_sub(old_len);
                let new = !state.slots.contains_key(&id);
                let fits = state.bytes + state.extra + extra <= disk.capacity
                    && state.slots.len() + state.new + usize::from(new) <= disk.entries
                    && usage
                        .live_bytes
                        .saturating_add(state.live)
                        .saturating_add(accounting.record_bytes)
                        <= disk.physical_live;
                let headroom = usage.free_segments as u64 * disk.segment_size
                    >= disk.reserve.saturating_add(state.allocation).saturating_add(allocation);
                if fits && headroom {
                    state.active.insert(
                        id,
                        Active {
                            extra,
                            new,
                            live: accounting.record_bytes,
                            allocation,
                        },
                    );
                    state.extra += extra;
                    state.new += usize::from(new);
                    state.live += accounting.record_bytes;
                    state.allocation += allocation;
                    Action::Reserved(Reservation {
                        disk: disk.clone(),
                        id,
                        len: value.len() as u32,
                        done: false,
                    })
                } else if !fits {
                    if let Some((victim, lsn)) = state.victim(disk.policy, Some(id)) {
                        Action::Evict(victim, lsn)
                    } else if !state.active.is_empty() {
                        Action::Wait(state.wait())
                    } else {
                        return Err(Error::NoSpace);
                    }
                } else if !state.active.is_empty() {
                    Action::Wait(state.wait())
                } else {
                    Action::Reclaim
                }
            };
            match action {
                Action::Reserved(reservation) => break reservation,
                Action::Evict(id, lsn) => self.evict(disk_index, id, lsn).await?,
                Action::Wait(wait) => {
                    let _ = wait.await;
                }
                Action::Reclaim => {
                    gc_passes += 1;
                    if gc_passes > usage.segments.saturating_mul(2) || self.store.reclaim(disk_index).await?.is_none() {
                        return Err(Error::NoSpace);
                    }
                }
            }
        };
        // Admission order is fixed while holding the disk controller, but I/O
        // completion does not hold that lock: independent writes stay pipelined.
        let request = self.store.put(id, value);
        drop(control);
        let lsn = request.await?;
        reservation.commit(lsn);
        Ok(lsn)
    }
}
enum Action {
    Reserved(Reservation),
    Evict(ChunkId, u64),
    Wait(oneshot::Receiver<()>),
    Reclaim,
}
struct Reservation {
    disk: Arc<Disk>,
    id: ChunkId,
    len: u32,
    done: bool,
}
impl Reservation {
    fn commit(mut self, lsn: u64) {
        let waiters = {
            let mut state = self.disk.state.lock();
            state.insert(self.id, lsn, self.len);
            state.finish(self.id)
        };
        self.done = true;
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.done {
            let waiters = self.disk.state.lock().finish(self.id);
            for waiter in waiters {
                let _ = waiter.send(());
            }
        }
    }
}
