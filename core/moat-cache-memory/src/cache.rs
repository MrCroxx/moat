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
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
};

use equivalent::Equivalent;
use hashbrown::HashTable;
use parking_lot::{Mutex, RwLock};
use slab::Slab;

use crate::{
    Entry, Policy, Priority,
    entry::{Record, Release},
    policy::State,
};

type Weigher<K, V, P> = dyn Fn(&K, &V, &P) -> usize + Send + Sync;
type Admission<K, V, P> = dyn Fn(&K, &V, &P) -> bool + Send + Sync;
type Listener<K, V, P> = dyn Fn(Removal<K, V, P>) + Send + Sync;
type Removed<K, V, P> = Vec<(Arc<Record<K, V, P>>, RemovalReason)>;

/// Invalid memory-cache configuration.
#[derive(Debug, thiserror::Error)]
#[error("invalid memory cache configuration: {0}")]
pub struct BuildError(pub(crate) &'static str);

/// Why a version left, or was not admitted to, the resident cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovalReason {
    /// Capacity pressure selected the version for eviction.
    Evicted,
    /// A newer insertion replaced the version.
    Replaced,
    /// The caller explicitly removed the version.
    Removed,
    /// The cache was explicitly cleared.
    Cleared,
    /// Admission, weight, or pinned capacity prevented residency.
    Rejected,
}

/// A notification delivered after releasing the shard lock.
///
/// Notifications may interleave across threads. The entry identifies the
/// exact removed version; listeners must not assume its key is still absent.
pub struct Removal<K, V, P = ()> {
    /// The removed version. Keeping this handle keeps its allocation alive.
    pub entry: Entry<K, V, P>,
    /// The cause of removal.
    pub reason: RemovalReason,
}

/// An approximate snapshot; concurrent operations may advance between fields.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Statistics {
    /// Successful lookups.
    pub hits: usize,
    /// Unsuccessful lookups.
    pub misses: usize,
    /// Insertion attempts, including rejected versions.
    pub insertions: usize,
    /// Versions evicted by capacity pressure.
    pub evictions: usize,
    /// Versions rejected at admission.
    pub rejections: usize,
    /// Number of currently resident versions.
    pub entries: usize,
    /// Sum of resident weights.
    pub resident_weight: usize,
    /// Sum of all live record weights, including handles to removed versions.
    /// This is weighted accounting, not an allocator or RSS measurement.
    pub allocated_weight: usize,
}

#[derive(Default)]
#[repr(align(64))]
struct LocalCounters {
    insertions: AtomicUsize,
    rejections: AtomicUsize,
    allocated: Arc<AtomicUsize>,
}
struct Counters {
    local: Vec<LocalCounters>,
    evictions: AtomicUsize,
    entries: AtomicUsize,
    resident: AtomicUsize,
}
impl Counters {
    fn new(shards: usize) -> Self {
        Self {
            local: (0..shards).map(|_| LocalCounters::default()).collect(),
            evictions: AtomicUsize::new(0),
            entries: AtomicUsize::new(0),
            resident: AtomicUsize::new(0),
        }
    }
}

/// Configures a cache before any records are allocated.
pub struct Builder<K, V, P = (), S = RandomState> {
    hash: S,
    capacity: usize,
    shards: usize,
    policy: Policy,
    weigher: Arc<Weigher<K, V, P>>,
    admission: Arc<Admission<K, V, P>>,
    listener: Option<Arc<Listener<K, V, P>>>,
}

impl<K, V, P> Builder<K, V, P> {
    /// Creates a builder with unit weights, SIEVE, and up to sixteen shards.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            shards: capacity.clamp(1, 16),
            policy: Policy::default(),
            hash: RandomState::new(),
            weigher: Arc::new(|_, _, _| 1),
            admission: Arc::new(|_, _, _| true),
            listener: None,
        }
    }
}

impl<K, V, P, S> Builder<K, V, P, S> {
    /// Selects the process-local hash builder. The default is randomized
    /// `std::collections::hash_map::RandomState`. This never defines persistent
    /// disk identity; the hybrid layer uses a separate stable key codec.
    pub fn hash_builder<T>(self, hash: T) -> Builder<K, V, P, T> {
        Builder {
            hash,
            capacity: self.capacity,
            shards: self.shards,
            policy: self.policy,
            weigher: self.weigher,
            admission: self.admission,
            listener: self.listener,
        }
    }

    /// Sets the shard count. Capacity is split exactly, with remainder units
    /// assigned to the first shards. An entry must fit its own shard.
    pub fn shards(mut self, shards: usize) -> Self {
        self.shards = shards;
        self
    }
    /// Selects the replacement algorithm and its configuration.
    pub fn policy(mut self, policy: Policy) -> Self {
        self.policy = policy;
        self
    }
    /// Computes immutable weight before acquiring a shard lock. Zero weights
    /// are charged one unit so entry metadata cannot grow without a bound.
    pub fn weigher(mut self, weigher: impl Fn(&K, &V, &P) -> usize + Send + Sync + 'static) -> Self {
        self.weigher = Arc::new(weigher);
        self
    }
    /// Filters new versions outside the shard lock. A rejected replacement
    /// still invalidates the previous version, preventing stale cache hits.
    pub fn admission(mut self, admission: impl Fn(&K, &V, &P) -> bool + Send + Sync + 'static) -> Self {
        self.admission = Arc::new(admission);
        self
    }
    /// Registers a listener invoked outside all cache locks. Listeners may
    /// reenter the cache. Cache destruction itself does not emit notifications.
    pub fn listener(mut self, listener: impl Fn(Removal<K, V, P>) + Send + Sync + 'static) -> Self {
        self.listener = Some(Arc::new(listener));
        self
    }
}

impl<K, V, P, S> Builder<K, V, P, S>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
    P: Send + Sync + 'static,
    S: BuildHasher + Send + Sync + 'static,
{
    /// Validates configuration and constructs an empty cache.
    pub fn build(self) -> Result<Cache<K, V, P, S>, BuildError> {
        if self.shards == 0 {
            return Err(BuildError("shards must be nonzero"));
        }
        self.policy.validate().map_err(BuildError)?;
        let shards = (0..self.shards)
            .map(|i| {
                let capacity = partition(self.capacity, self.shards, i);
                RwLock::new(Shard {
                    index: HashTable::new(),
                    nodes: Slab::new(),
                    state: State::new(self.policy.clone(), capacity),
                    weight: 0,
                    capacity,
                    hits: AtomicUsize::new(0),
                    misses: AtomicUsize::new(0),
                })
            })
            .collect();
        let pin = matches!(self.policy, Policy::Lru(_));
        let inner = Arc::new(Inner {
            shards,
            hash: self.hash,
            weigher: self.weigher,
            admission: self.admission,
            listener: self.listener,
            counters: Counters::new(self.shards),
            capacity: AtomicUsize::new(self.capacity),
            resizing: Mutex::new(()),
            mutable_hits: self.policy.mutable_hits(),
        });
        let release = if pin {
            let owner: Arc<dyn Release<K, V, P>> = inner.clone();
            Some(Arc::downgrade(&owner))
        } else {
            None
        };
        Ok(Cache { inner, release })
    }
}

/// A cloneable handle to a sharded resident cache.
///
/// Keys and values need not implement `Clone` or a serialization trait. The
/// cache has no source loader or asynchronous runtime dependency.
pub struct Cache<K, V, P = (), S = RandomState> {
    inner: Arc<Inner<K, V, P, S>>,
    release: Option<Weak<dyn Release<K, V, P>>>,
}

impl<K, V, P, S> Clone for Cache<K, V, P, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            release: self.release.clone(),
        }
    }
}

#[repr(align(64))]
struct Inner<K, V, P, S> {
    shards: Vec<RwLock<Shard<K, V, P>>>,
    hash: S,
    weigher: Arc<Weigher<K, V, P>>,
    admission: Arc<Admission<K, V, P>>,
    listener: Option<Arc<Listener<K, V, P>>>,
    counters: Counters,
    capacity: AtomicUsize,
    resizing: Mutex<()>,
    mutable_hits: bool,
}

pub(crate) struct Node<K, V, P> {
    pub record: Arc<Record<K, V, P>>,
    pub prev: Option<usize>,
    pub next: Option<usize>,
    pub queue: Option<usize>,
    pub frequency: AtomicU8,
}

#[repr(align(64))]
struct Shard<K, V, P> {
    index: HashTable<usize>,
    nodes: Slab<Node<K, V, P>>,
    state: State,
    weight: usize,
    capacity: usize,
    hits: AtomicUsize,
    misses: AtomicUsize,
}

impl<K, V, P> Shard<K, V, P> {
    fn find<Q: Equivalent<K> + ?Sized>(&self, hash: u64, key: &Q) -> Option<usize> {
        self.index
            .find(hash, |&id| key.equivalent(&self.nodes[id].record.key))
            .copied()
    }

    fn detach(&mut self, id: usize, counters: &Counters) -> Arc<Record<K, V, P>> {
        self.state.remove(&mut self.nodes, id);
        let record = self.nodes.remove(id).record;
        self.index
            .find_entry(record.hash, |&other| other == id)
            .expect("indexed node")
            .remove();
        record.resident.store(false, Ordering::Release);
        self.weight -= record.weight;
        counters.resident.fetch_sub(record.weight, Ordering::Relaxed);
        counters.entries.fetch_sub(1, Ordering::Relaxed);
        record
    }

    fn trim(&mut self, counters: &Counters, removed: &mut Removed<K, V, P>) {
        while self.weight > self.capacity {
            let Some(id) = self.state.victim(&mut self.nodes) else {
                break;
            };
            removed.push((self.detach(id, counters), RemovalReason::Evicted));
        }
    }
}

impl<K, V, P, S> Drop for Inner<K, V, P, S> {
    fn drop(&mut self) {
        for lock in &mut self.shards {
            for (_, node) in &lock.get_mut().nodes {
                node.record.resident.store(false, Ordering::Release);
            }
        }
    }
}

impl<K, V, P, S> Inner<K, V, P, S> {
    fn shard(&self, hash: u64) -> usize {
        hash as usize % self.shards.len()
    }

    fn dispatch(&self, removed: impl IntoIterator<Item = (Arc<Record<K, V, P>>, RemovalReason)>) {
        for (record, reason) in removed {
            match reason {
                RemovalReason::Evicted => {
                    self.counters.evictions.fetch_add(1, Ordering::Relaxed);
                }
                RemovalReason::Rejected => {
                    self.counters.local[self.shard(record.hash)]
                        .rejections
                        .fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
            if let Some(listener) = &self.listener {
                listener(Removal {
                    entry: Entry::acquire(record, None),
                    reason,
                });
            }
            // Without a listener, the last record reference is dropped here,
            // after the caller has released its shard lock.
        }
    }
}

impl<K, V, P, S> Release<K, V, P> for Inner<K, V, P, S>
where
    K: Send + Sync,
    V: Send + Sync,
    P: Send + Sync,
    S: Send + Sync,
{
    fn release(&self, record: &Arc<Record<K, V, P>>) {
        let mut removed = Vec::new();
        {
            let mut shard = self.shards[self.shard(record.hash)].write();
            if record.references.load(Ordering::Acquire) != 0 {
                return;
            }
            let Some(id) = shard
                .index
                .find(record.hash, |&id| Arc::ptr_eq(&shard.nodes[id].record, record))
                .copied()
            else {
                return;
            };
            let Shard { nodes, state, .. } = &mut *shard;
            state.release(nodes, id);
            shard.trim(&self.counters, &mut removed);
        }
        self.dispatch(removed);
    }
}

impl<K, V, P, S> Cache<K, V, P, S>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
    P: Send + Sync + 'static,
    S: BuildHasher + Send + Sync + 'static,
{
    /// Looks up an owned or borrowed equivalent key. Hashing an equivalent
    /// borrowed key must produce the same hash as hashing the owned key.
    pub fn get<Q: Hash + Equivalent<K> + ?Sized>(&self, key: &Q) -> Option<Entry<K, V, P>> {
        self.probe(key).get()
    }

    /// Hashes a borrowed key once for repeated lookups and delayed promotion.
    /// The probe is bound to this cache and still compares complete keys.
    pub fn probe<'a, Q: Hash + Equivalent<K> + ?Sized>(&'a self, key: &'a Q) -> Probe<'a, K, V, P, S, Q> {
        Probe {
            cache: self,
            key,
            hash: self.inner.hash.hash_one(key),
        }
    }

    fn get_hashed<Q: Equivalent<K> + ?Sized>(&self, key: &Q, hash: u64) -> Option<Entry<K, V, P>> {
        let lock = &self.inner.shards[self.inner.shard(hash)];
        if self.inner.mutable_hits {
            let mut shard = lock.write();
            let found = shard.find(hash, key);
            if found.is_some() { &shard.hits } else { &shard.misses }.fetch_add(1, Ordering::Relaxed);
            found.map(|id| {
                let entry = Entry::acquire(shard.nodes[id].record.clone(), self.release.clone());
                let Shard { state, nodes, .. } = &mut *shard;
                state.access(nodes, id);
                entry
            })
        } else {
            let shard = lock.read();
            let found = shard.find(hash, key);
            if found.is_some() { &shard.hits } else { &shard.misses }.fetch_add(1, Ordering::Relaxed);
            found.map(|id| {
                shard.state.access_shared(&shard.nodes[id]);
                Entry::acquire(shard.nodes[id].record.clone(), None)
            })
        }
    }

    /// Tests residency without touching replacement state or acquiring a handle.
    pub fn contains<Q: Hash + Equivalent<K> + ?Sized>(&self, key: &Q) -> bool {
        let hash = self.inner.hash.hash_one(key);
        self.inner.shards[self.inner.shard(hash)]
            .read()
            .find(hash, key)
            .is_some()
    }

    /// Inserts a version with default properties and normal priority.
    pub fn insert(&self, key: K, value: V) -> Entry<K, V, P>
    where
        P: Default,
    {
        self.insert_with(key, value, P::default(), Priority::Normal)
    }

    /// Inserts a version and returns a handle even if admission rejects it.
    /// Use [`Entry::is_resident`] to inspect residency. Overweight and rejected
    /// replacements remove the previous resident version.
    pub fn insert_with(&self, key: K, value: V, properties: P, priority: Priority) -> Entry<K, V, P> {
        self.prepare(key, value, properties, priority)
            .commit()
            .finish()
            .expect("inserted handle")
    }

    /// Prepares a version, running the weigher and admission filter now. The
    /// returned object can commit residency later, without running callbacks
    /// inside an enclosing application coordination lock.
    pub fn prepare(&self, key: K, value: V, properties: P, priority: Priority) -> Prepared<K, V, P, S> {
        let hash = self.inner.hash.hash_one(&key);
        self.prepare_hashed(key, value, properties, priority, hash)
    }

    fn prepare_hashed(&self, key: K, value: V, properties: P, priority: Priority, hash: u64) -> Prepared<K, V, P, S> {
        let weight = (self.inner.weigher)(&key, &value, &properties).max(1);
        let admitted = (self.inner.admission)(&key, &value, &properties);
        let counters = &self.inner.counters.local[self.inner.shard(hash)];
        counters.allocated.fetch_add(weight, Ordering::Relaxed);
        counters.insertions.fetch_add(1, Ordering::Relaxed);
        let record = Arc::new(Record {
            key,
            value,
            properties,
            hash,
            weight,
            priority,
            resident: AtomicBool::new(false),
            references: AtomicUsize::new(0),
            allocated: counters.allocated.clone(),
        });
        let entry = Entry::acquire(record, self.release.clone());
        Prepared {
            cache: self.clone(),
            entry,
            admitted,
        }
    }

    fn commit_entry(&self, entry: Entry<K, V, P>, admitted: bool) -> Change<K, V, P, S> {
        let record = entry.record.clone();
        let hash = record.hash;
        let weight = record.weight;
        let mut removed = Vec::new();
        {
            let mut shard = self.inner.shards[self.inner.shard(hash)].write();
            if let Some(id) = shard.find(hash, &record.key) {
                removed.push((shard.detach(id, &self.inner.counters), RemovalReason::Replaced));
            }
            if admitted && weight <= shard.capacity && shard.weight.checked_add(weight).is_some() {
                let id = shard.nodes.insert(Node {
                    record: record.clone(),
                    prev: None,
                    next: None,
                    queue: None,
                    frequency: AtomicU8::new(0),
                });
                let Shard {
                    index, nodes, state, ..
                } = &mut *shard;
                index.insert_unique(hash, id, |&id| nodes[id].record.hash);
                state.insert(nodes, id);
                record.resident.store(true, Ordering::Release);
                shard.weight += weight;
                self.inner.counters.resident.fetch_add(weight, Ordering::Relaxed);
                self.inner.counters.entries.fetch_add(1, Ordering::Relaxed);
                shard.trim(&self.inner.counters, &mut removed);
                // LRU may have only pinned entries left. Never let an insertion
                // grow an already full pinned working set without a bound.
                if shard.weight > shard.capacity && record.resident.load(Ordering::Relaxed) {
                    removed.push((shard.detach(id, &self.inner.counters), RemovalReason::Rejected));
                }
            } else {
                removed.push((record, RemovalReason::Rejected));
            }
        }
        Change {
            owner: self.inner.clone(),
            entry: Some(entry),
            removed,
            rejected: None,
        }
    }

    /// Removes a resident version. Existing handles remain usable.
    pub fn remove<Q: Hash + Equivalent<K> + ?Sized>(&self, key: &Q) -> Option<Entry<K, V, P>> {
        self.remove_deferred(key).finish()
    }

    /// Removes a resident version while deferring callbacks and destruction
    /// until the returned change is finished or dropped outside application locks.
    pub fn remove_deferred<Q: Hash + Equivalent<K> + ?Sized>(&self, key: &Q) -> Change<K, V, P, S> {
        let hash = self.inner.hash.hash_one(key);
        let removed = {
            let mut shard = self.inner.shards[self.inner.shard(hash)].write();
            shard.find(hash, key).map(|id| shard.detach(id, &self.inner.counters))
        };
        let entry = removed.as_ref().map(|record| Entry::acquire(record.clone(), None));
        let removed = removed
            .into_iter()
            .map(|record| (record, RemovalReason::Removed))
            .collect();
        Change {
            owner: self.inner.clone(),
            entry,
            removed,
            rejected: None,
        }
    }

    /// Clears each shard in turn. Concurrent insertions into an already cleared
    /// shard may survive. Explicit removal also removes pinned LRU versions.
    pub fn clear(&self) {
        for lock in &self.inner.shards {
            let mut removed = Vec::new();
            {
                let mut shard = lock.write();
                let ids: Vec<_> = shard.nodes.iter().map(|(id, _)| id).collect();
                for id in ids {
                    removed.push((shard.detach(id, &self.inner.counters), RemovalReason::Cleared));
                }
                shard.state.clear_history();
            }
            self.inner.dispatch(removed);
        }
    }

    /// Updates per-shard budgets and evicts excess weight. Pinned LRU entries
    /// can temporarily exceed a reduced budget; their last release trims it.
    /// Concurrent resize calls are serialized; listeners run after serialization
    /// is released and may themselves resize the cache.
    pub fn resize(&self, capacity: usize) {
        let mut removed = Vec::new();
        {
            let _serial = self.inner.resizing.lock();
            for (i, lock) in self.inner.shards.iter().enumerate() {
                let mut shard = lock.write();
                let capacity = partition(capacity, self.inner.shards.len(), i);
                shard.capacity = capacity;
                let Shard { state, nodes, .. } = &mut *shard;
                state.resize(nodes, capacity);
                shard.trim(&self.inner.counters, &mut removed);
            }
            self.inner.capacity.store(capacity, Ordering::Relaxed);
        }
        self.inner.dispatch(removed);
    }

    /// The configured total resident-weight budget.
    pub fn capacity(&self) -> usize {
        self.inner.capacity.load(Ordering::Relaxed)
    }
    /// The number of resident entries at the instant of observation.
    pub fn len(&self) -> usize {
        self.inner.counters.entries.load(Ordering::Relaxed)
    }
    /// Whether no entries are resident at the instant of observation.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Collects weighted residency, allocation and operation counters.
    pub fn statistics(&self) -> Statistics {
        let c = &self.inner.counters;
        let (hits, misses) = self.inner.shards.iter().fold((0usize, 0usize), |(hits, misses), lock| {
            let shard = lock.read();
            (
                hits.wrapping_add(shard.hits.load(Ordering::Relaxed)),
                misses.wrapping_add(shard.misses.load(Ordering::Relaxed)),
            )
        });
        Statistics {
            hits,
            misses,
            insertions: c
                .local
                .iter()
                .map(|c| c.insertions.load(Ordering::Relaxed))
                .fold(0, usize::wrapping_add),
            evictions: c.evictions.load(Ordering::Relaxed),
            rejections: c
                .local
                .iter()
                .map(|c| c.rejections.load(Ordering::Relaxed))
                .fold(0, usize::wrapping_add),
            entries: c.entries.load(Ordering::Relaxed),
            resident_weight: c.resident.load(Ordering::Relaxed),
            allocated_weight: c.local.iter().map(|c| c.allocated.load(Ordering::Relaxed)).sum(),
        }
    }
}

fn partition(capacity: usize, shards: usize, index: usize) -> usize {
    capacity / shards + usize::from(index < capacity % shards)
}

impl<K, V, P> Cache<K, V, P> {
    /// Starts configuring a cache with the given resident-weight budget.
    pub fn builder(capacity: usize) -> Builder<K, V, P> {
        Builder::new(capacity)
    }
}

/// A borrowed query and its cached hash, tied to the cache that computed it.
/// Equivalent borrowed and owned keys must obey the same hash contract as
/// [`Cache::get`]. Probes never hold shard locks between method calls.
pub struct Probe<'a, K, V, P, S, Q: ?Sized> {
    cache: &'a Cache<K, V, P, S>,
    key: &'a Q,
    hash: u64,
}
impl<K, V, P, S, Q> Probe<'_, K, V, P, S, Q>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
    P: Send + Sync + 'static,
    S: BuildHasher + Send + Sync + 'static,
    Q: Equivalent<K> + ?Sized,
{
    /// Checks current residency using the original query hash.
    pub fn get(&self) -> Option<Entry<K, V, P>> {
        self.cache.get_hashed(self.key, self.hash)
    }
    /// Prepares a matching version without hashing the owned key again.
    /// Returns `None` if the complete owned key is not equivalent to the query.
    /// Weight/admission callbacks run outside shard locks, as in `Cache::prepare`.
    pub fn prepare(&self, key: K, value: V, properties: P, priority: Priority) -> Option<Prepared<K, V, P, S>> {
        self.key
            .equivalent(&key)
            .then(|| self.cache.prepare_hashed(key, value, properties, priority, self.hash))
    }
}

/// A version whose weight and admission have already been evaluated.
/// Preparing does not change residency; dropping an unused preparation drops
/// its allocation normally. Preparations are bound to their originating cache.
pub struct Prepared<K, V, P = (), S = RandomState> {
    cache: Cache<K, V, P, S>,
    entry: Entry<K, V, P>,
    admitted: bool,
}

impl<K, V, P, S> Prepared<K, V, P, S> {
    /// Whether the admission filter accepted this version.
    pub fn is_admitted(&self) -> bool {
        self.admitted
    }

    /// Prevents residency when an external resource budget cannot retain it.
    pub fn reject_residency(&mut self) {
        self.admitted = false;
    }
    /// The immutable prepared version, whether or not it will be admitted.
    pub fn entry(&self) -> &Entry<K, V, P> {
        &self.entry
    }
}
impl<K, V, P, S> Prepared<K, V, P, S>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
    P: Send + Sync + 'static,
    S: BuildHasher + Send + Sync + 'static,
{
    /// Commits the structural mutation without running listeners or dropping
    /// removed values. Finish or drop the result after releasing application
    /// coordination locks. Key equality must still obey the cache's reentry rules.
    pub fn commit(self) -> Change<K, V, P, S> {
        self.cache.commit_entry(self.entry, self.admitted)
    }

    /// Attempts read promotion. A rejected promotion leaves existing residency
    /// intact and returns a nonresident handle without taking a shard write lock.
    /// Deferred notifications must still be finished outside coordination locks.
    pub fn promote(self) -> Change<K, V, P, S> {
        if self.admitted {
            return self.commit();
        }
        Change {
            owner: self.cache.inner,
            removed: Vec::new(),
            rejected: Some(self.entry.record.clone()),
            entry: Some(self.entry),
        }
    }
}

/// A completed structural mutation with deferred notifications and retirement.
///
/// Drop this object outside any enclosing application lock: dropping it runs
/// removal listeners and releases retired application values. This lets a
/// hybrid cache publish a version atomically with its generation check.
#[must_use = "finish or drop changes outside application coordination locks"]
pub struct Change<K, V, P = (), S = RandomState> {
    owner: Arc<Inner<K, V, P, S>>,
    entry: Option<Entry<K, V, P>>,
    removed: Removed<K, V, P>,
    rejected: Option<Arc<Record<K, V, P>>>,
}
impl<K, V, P, S> Change<K, V, P, S> {
    /// The inserted or removed handle, if the mutation produced one.
    pub fn entry(&self) -> Option<&Entry<K, V, P>> {
        self.entry.as_ref()
    }
    /// Delivers deferred notifications and returns the mutation's handle.
    pub fn finish(mut self) -> Option<Entry<K, V, P>> {
        self.notify();
        self.entry.take()
    }
    fn notify(&mut self) {
        self.owner.dispatch(
            self.removed
                .drain(..)
                .chain(self.rejected.take().map(|record| (record, RemovalReason::Rejected))),
        );
    }
}
impl<K, V, P, S> Drop for Change<K, V, P, S> {
    fn drop(&mut self) {
        self.notify();
    }
}
