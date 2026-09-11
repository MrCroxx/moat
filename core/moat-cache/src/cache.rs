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
    future::Future,
    hash::BuildHasher,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_channel::oneshot;
use futures_util::{FutureExt, future::BoxFuture};
use moat_cache_memory::{Builder as MemoryBuilder, Cache as MemoryCache, Prepared, Priority};
use moat_cache_store::{DeleteResult, Store};
use parking_lot::Mutex;

use crate::{
    Bytes, DiskOptions, EntryView, Error, FillToken, Result,
    catalog::Catalog,
    envelope::{Format, Record},
    identity::{DEFAULT_IDENTITY_VERSION, Fingerprint, Xxh3},
    registry::{Lease, Mutation, Registry},
    scheduler::{Job, Scheduler},
};

/// Hybrid cache identity and bounded-work configuration.
#[derive(Clone)]
pub struct Options {
    /// Stable namespace for this cache's exclusively owned engine set.
    pub namespace: [u8; 16],
    /// Bump when the fingerprint algorithm or key, value or property encodings change.
    pub identity_version: u32,
    /// Stable disk fingerprint implementation, independent of memory hashing.
    pub fingerprint: Arc<dyn Fingerprint>,
    /// Per-disk capacity and replacement settings.
    pub disk: DiskOptions,
    /// Maximum admitted background mutations and barriers.
    pub pending_operations: usize,
    /// Maximum encoded payload bytes retained by pending mutations.
    pub pending_bytes: usize,
    /// Maximum concurrent miss lookups, mutations and outstanding fill tokens.
    pub key_leases: usize,
    /// Maximum canonical key bytes retained in transient logical-key state.
    pub key_bytes: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            namespace: [0; 16],
            identity_version: DEFAULT_IDENTITY_VERSION,
            fingerprint: Arc::new(Xxh3),
            disk: DiskOptions::default(),
            pending_operations: 1024,
            pending_bytes: 64 << 20,
            key_leases: 4096,
            key_bytes: 16 << 20,
        }
    }
}

/// A reply to a cache operation already admitted to its background coordinator.
/// Dropping this future abandons the reply while admitted work and bookkeeping
/// continue independently of the caller's runtime.
#[must_use = "dropping the reply does not cancel an admitted mutation"]
pub struct Request<T> {
    future: BoxFuture<'static, Result<T>>,
}
impl<T: Send + 'static> Request<T> {
    fn ready(result: Result<T>) -> Self {
        Self {
            future: futures_util::future::ready(result).boxed(),
        }
    }
    fn receiver(receiver: oneshot::Receiver<Result<T>>) -> Self {
        Self {
            future: async move { receiver.await.unwrap_or(Err(Error::Closed)) }.boxed(),
        }
    }
}
impl<T> Future for Request<T> {
    type Output = Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}

/// A cache lookup, optionally retaining a miss-generation capability.
pub enum Lookup {
    /// A shared resident or disk entry without KV decoding.
    Hit(EntryView),
    /// A miss whose token can conditionally populate this cache instance.
    Miss(FillToken),
}

/// Resident, disk and transient-work statistics collected approximately.
#[derive(Debug, Clone)]
pub struct Statistics {
    /// Resident-cache counters and weighted allocation accounting.
    pub memory: moat_cache_memory::Statistics,
    /// Physical adapter counters and retained buffer credits.
    pub store: moat_cache_store::Statistics,
    /// Completed disk catalog entries.
    pub disk_entries: usize,
    /// Completed disk catalog encoded bytes.
    pub disk_bytes: u64,
    /// Explicit disk cache evictions.
    pub disk_evictions: usize,
    /// Pending coordinator jobs, including barriers.
    pub pending_operations: usize,
    /// Encoded payload bytes retained by pending jobs.
    pub pending_bytes: usize,
    /// Logical key leases, including fill tokens.
    pub key_leases: usize,
    /// Distinct keys with transient state.
    pub tracked_keys: usize,
    /// Canonical bytes retained by transient state.
    pub tracked_key_bytes: usize,
}

struct Inner<S> {
    memory: MemoryCache<Bytes, Bytes, Bytes, S>,
    catalog: Catalog,
    registry: Arc<Registry>,
    scheduler: Scheduler,
    admission: Mutex<bool>,
    failures: Mutex<Option<Error>>,
    format: Format,
    fingerprint: Arc<dyn Fingerprint>,
}

/// A shared memory/disk cache with no origin-loader API.
///
/// Successful insertion completes the disk write before publishing residency.
/// Overlapping mutations preserve physical order and prevent older results
/// from replacing newer resident versions. Memory admission may still reject
/// the returned handle; disk residency is independently capacity-managed.
pub struct Cache<S = RandomState> {
    inner: Arc<Inner<S>>,
}
impl<S> Clone for Cache<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<S: BuildHasher + Send + Sync + 'static> Cache<S> {
    /// Opens a bytes cache with exclusive store ownership and bounded residency.
    /// Disk hits return owned views; write packing copies into the engine log.
    pub async fn new(memory: MemoryBuilder<Bytes, Bytes, Bytes, S>, store: Store, options: Options) -> Result<Self> {
        if !store.is_unique() {
            return Err(Error::Invalid("cache requires the only store handle"));
        }
        let result = async {
            if options.key_leases == 0 || options.key_bytes == 0 {
                return Err(Error::Invalid("key-state budgets must be nonzero"));
            }
            let memory = memory.build()?;
            let mut inventory = Vec::new();
            for disk in 0..store.disks().len() {
                inventory.extend(store.inventory(disk).await?);
            }
            let catalog = Catalog::new(store.clone(), inventory, &options.disk)?;
            catalog.trim().await?;
            let scheduler = Scheduler::new(options.pending_operations, options.pending_bytes)?;
            Ok(Self {
                inner: Arc::new(Inner {
                    memory,
                    catalog,
                    scheduler,
                    registry: Registry::new(options.key_leases, options.key_bytes),
                    admission: Mutex::new(false),
                    failures: Mutex::new(None),
                    format: Format {
                        namespace: options.namespace,
                        identity_version: options.identity_version,
                    },
                    fingerprint: options.fingerprint,
                }),
            })
        }
        .await;
        if result.is_err() {
            let _ = store.close().await;
        }
        result
    }

    /// Looks up residency with a borrowed key, without allocation or I/O.
    pub fn get_memory(&self, key: &[u8]) -> Option<EntryView> {
        if self.inner.registry.is_closed() {
            None
        } else {
            self.inner.memory.get(key).map(Into::into)
        }
    }
    /// Returns an owned view. Field handles remain valid after eviction,
    /// overwrite, invalidation and close until their final owner releases them.
    pub async fn get(&self, key: &Bytes) -> Result<Option<EntryView>> {
        match self.lookup(key).await? {
            Lookup::Hit(entry) => Ok(Some(entry)),
            Lookup::Miss(_) => Ok(None),
        }
    }
    /// Looks up memory and disk with a shared query key. A miss returns a
    /// bounded fill token; origin loading and concurrency belong to the caller.
    pub async fn lookup(&self, key: &Bytes) -> Result<Lookup> {
        if self.inner.registry.is_closed() {
            return Err(Error::Closed);
        }
        let probe = self.inner.memory.probe(key);
        if let Some(entry) = probe.get() {
            return Ok(Lookup::Hit(entry.into()));
        }
        let lease = self.inner.registry.acquire(key.clone())?;
        let id = self.identify(&lease.key);
        let location = self.inner.catalog.store.locate(id);
        loop {
            let (generation, wait) = {
                let mut state = lease.lock();
                (
                    state.generation,
                    if state.pending == 0 { None } else { Some(state.wait()) },
                )
            };
            if let Some(wait) = wait {
                let _ = wait.await;
                continue;
            }
            if self.inner.registry.is_closed() {
                return Err(Error::Closed);
            }
            if let Some(entry) = probe.get() {
                return Ok(Lookup::Hit(entry.into()));
            }
            let version = self.inner.catalog.version_on(location.disk(), id);
            let chunk = if version.is_some() {
                location.get(None).await?
            } else {
                None
            };
            if let Some(chunk) = chunk {
                let lsn = chunk.lsn();
                let envelope = self.inner.format.decode(Bytes::from(chunk))?;
                if envelope.generation == 0 {
                    return Err(Error::Corrupt("invalid entry generation"));
                }
                if let Some(prepared) =
                    probe.prepare(envelope.key, envelope.value, envelope.properties, envelope.priority)
                {
                    let mut prepared = Some(self.retain(prepared));
                    let change = {
                        let state = lease.lock();
                        if state.generation == generation
                            && state.pending == 0
                            && !self.inner.registry.is_closed()
                            && self.inner.catalog.validate_hit(location.disk(), id, lsn)
                        {
                            Some(prepared.take().expect("prepared entry").promote())
                        } else {
                            None
                        }
                    };
                    if let Some(change) = change {
                        return Ok(Lookup::Hit(change.finish().expect("promoted handle").into()));
                    }
                    drop(prepared);
                    continue;
                }
            }
            let valid = {
                let state = lease.lock();
                state.generation == generation && state.pending == 0
            };
            if valid {
                return Ok(Lookup::Miss(FillToken { lease, generation }));
            }
        }
    }

    fn prepare(
        &self,
        key: Bytes,
        value: Bytes,
        properties: Bytes,
        priority: Priority,
    ) -> Prepared<Bytes, Bytes, Bytes, S> {
        self.retain(self.inner.memory.prepare(key, value, properties, priority))
    }

    fn retain(&self, mut prepared: Prepared<Bytes, Bytes, Bytes, S>) -> Prepared<Bytes, Bytes, Bytes, S> {
        if prepared.is_admitted()
            && !(prepared.entry().key().retain()
                && prepared.entry().value().retain()
                && prepared.entry().properties().retain())
        {
            prepared.reject_residency();
        }
        prepared
    }

    fn identify(&self, bytes: &[u8]) -> moat_common::ChunkId {
        self.inner
            .fingerprint
            .identify(&self.inner.format.namespace, self.inner.format.identity_version, bytes)
    }

    /// Inserts a value with default properties and normal priority.
    pub fn insert(&self, key: Bytes, value: Bytes) -> Request<EntryView> {
        self.insert_with(key, value, Bytes::default(), Priority::Normal)
    }
    /// Admits a write with explicit properties. Cancellation abandons only the
    /// reply; persistence, catalog update and generation checks still run.
    pub fn insert_with(&self, key: Bytes, value: Bytes, properties: Bytes, priority: Priority) -> Request<EntryView> {
        if self.inner.registry.is_closed() {
            return Request::ready(Err(Error::Closed));
        }
        let prepared = self.prepare(key, value, properties, priority);
        let lease = match self.inner.registry.acquire(prepared.entry().key().clone()) {
            Ok(lease) => lease,
            Err(error) => return Request::ready(Err(error)),
        };
        let request = self.insert_prepared(prepared, lease, None, priority);
        Request {
            future: async move {
                request
                    .await?
                    .ok_or(Error::Invalid("unconditional insertion was stale"))
            }
            .boxed(),
        }
    }
    /// Conditionally inserts a loaded value. None means the token was stale.
    /// Acceptance consumes the generation even if later storage I/O fails;
    /// a caller can obtain another token with a fresh lookup before retrying.
    pub fn populate(&self, token: FillToken, value: Bytes) -> Request<Option<EntryView>> {
        self.populate_with(token, value, Bytes::default(), Priority::Normal)
    }
    /// Conditional population with explicit application properties and priority.
    pub fn populate_with(
        &self,
        token: FillToken,
        value: Bytes,
        properties: Bytes,
        priority: Priority,
    ) -> Request<Option<EntryView>> {
        if !token.lease.belongs_to(&self.inner.registry) {
            return Request::ready(Err(Error::Invalid("fill token belongs to another cache")));
        }
        if !token.is_valid() {
            return Request::ready(Ok(None));
        }
        let key = token.lease.key.clone();
        let prepared = self.prepare(key, value, properties, priority);
        self.insert_prepared(prepared, token.lease, Some(token.generation), priority)
    }

    fn insert_prepared(
        &self,
        prepared: Prepared<Bytes, Bytes, Bytes, S>,
        lease: Lease,
        expected: Option<u64>,
        priority: Priority,
    ) -> Request<Option<EntryView>> {
        let id = self.identify(&lease.key);
        let max = self.inner.catalog.store.disks()[self.inner.catalog.store.disk_of(&id)].chunk_max as usize;
        let record = Record {
            key: &lease.key,
            value: prepared.entry().value(),
            properties: prepared.entry().properties(),
            generation: 1,
            priority,
        };
        let mut encoded = match self.inner.format.encode(record, max) {
            Ok(encoded) => encoded,
            Err(error) => return Request::ready(Err(error)),
        };
        let permit = match self.inner.scheduler.reserve(encoded.len()) {
            Ok(permit) => permit,
            Err(error) => return Request::ready(Err(error)),
        };
        let (reply, receiver) = oneshot::channel();
        let change;
        let rejected;
        {
            let closed = self.inner.admission.lock();
            if *closed {
                return Request::ready(Err(Error::Closed));
            }
            let generation = {
                let mut state = lease.lock();
                if expected.is_some_and(|expected| expected != state.generation) {
                    return Request::ready(Ok(None));
                }
                let generation = match state.advance() {
                    Ok(generation) => generation,
                    Err(error) => return Request::ready(Err(error)),
                };
                state.pending += 1;
                change = self.inner.memory.remove_deferred(prepared.entry().key());
                generation
            };
            Format::set_generation(&mut encoded, generation);
            let mutation = Mutation { lease };
            let inner = self.inner.clone();
            let future = async move {
                let result = inner.catalog.write(id, Arc::from(encoded)).await;
                let result = match result {
                    Ok(_) => {
                        let entry = prepared.entry().clone();
                        let mut prepared = Some(prepared);
                        let change = {
                            let state = mutation.lease.lock();
                            if state.generation == generation && !inner.registry.is_closed() {
                                Some(prepared.take().expect("prepared entry").commit())
                            } else {
                                None
                            }
                        };
                        if let Some(change) = change {
                            drop(change.finish());
                        }
                        Ok(Some(entry.into()))
                    }
                    Err(error) => {
                        inner.failures.lock().get_or_insert(error.clone());
                        Err(error)
                    }
                };
                drop(mutation);
                let _ = reply.send(result);
            }
            .boxed();
            rejected = self
                .inner
                .scheduler
                .submit(Job {
                    id: Some(id),
                    future,
                    permit: Some(permit),
                })
                .err();
        }
        drop(change.finish());
        drop(rejected);
        Request::receiver(receiver)
    }

    /// Invalidates this logical key. The generation changes even for an absent
    /// key, invalidating outstanding fill tokens and stale disk promotions.
    /// A colliding chunk holding another full key is preserved.
    pub fn invalidate(&self, key: &Bytes) -> Request<bool> {
        let lease = match self.inner.registry.acquire(key.clone()) {
            Ok(lease) => lease,
            Err(error) => return Request::ready(Err(error)),
        };
        let permit = match self.inner.scheduler.reserve(0) {
            Ok(permit) => permit,
            Err(error) => return Request::ready(Err(error)),
        };
        let id = self.identify(&lease.key);
        let (reply, receiver) = oneshot::channel();
        let change;
        let rejected;
        {
            let closed = self.inner.admission.lock();
            if *closed {
                return Request::ready(Err(Error::Closed));
            }
            {
                let mut state = lease.lock();
                if let Err(error) = state.advance() {
                    return Request::ready(Err(error));
                }
                state.pending += 1;
                change = self.inner.memory.remove_deferred(key);
            }
            let memory_removed = change.entry().is_some();
            let mutation = Mutation { lease };
            let inner = self.inner.clone();
            let future = async move {
                let result = async {
                    let disk = inner.catalog.store.disk_of(&id);
                    let _control = inner.catalog.control(disk).await;
                    let Some(lsn) = inner.catalog.version(id) else {
                        return Ok(memory_removed);
                    };
                    let Some(chunk) = inner.catalog.store.get(id, None).await? else {
                        inner.catalog.remove(id, lsn);
                        return Ok(memory_removed);
                    };
                    if chunk.lsn() != lsn {
                        return Err(Error::Corrupt("catalog version mismatch during invalidation"));
                    }
                    let envelope = inner.format.decode(Bytes::from(chunk))?;
                    if envelope.key.as_ref() != mutation.lease.key.as_ref() {
                        return Ok(memory_removed);
                    }
                    match inner.catalog.store.delete(id, Some(lsn)).await? {
                        DeleteResult::Deleted(_) | DeleteResult::Missing => {
                            inner.catalog.remove(id, lsn);
                            Ok(true)
                        }
                        DeleteResult::Changed => Err(Error::Corrupt("catalog changed during invalidation")),
                    }
                }
                .await;
                if let Err(error) = &result {
                    inner.failures.lock().get_or_insert(error.clone());
                }
                drop(mutation);
                let _ = reply.send(result);
            }
            .boxed();
            rejected = self
                .inner
                .scheduler
                .submit(Job {
                    id: Some(id),
                    future,
                    permit: Some(permit),
                })
                .err();
        }
        drop(change.finish());
        drop(rejected);
        Request::receiver(receiver)
    }

    /// Evicts current memory residency while keeping disk entries available.
    pub fn clear_memory(&self) {
        self.inner.memory.clear();
    }
    /// Changes resident capacity without changing disk capacity or key generations.
    pub fn resize_memory(&self, capacity: usize) {
        self.inner.memory.resize(capacity);
    }
    /// Collects residency, physical I/O, catalog and transient-state statistics.
    pub fn statistics(&self) -> Statistics {
        let (disk_entries, disk_bytes, disk_evictions) = self.inner.catalog.snapshot();
        let (key_leases, tracked_keys, tracked_key_bytes) = self.inner.registry.snapshot();
        let (pending_operations, pending_bytes) = self.inner.scheduler.snapshot();
        Statistics {
            memory: self.inner.memory.statistics(),
            store: self.inner.catalog.store.statistics(),
            disk_entries,
            disk_bytes,
            disk_evictions,
            key_leases,
            tracked_keys,
            tracked_key_bytes,
            pending_operations,
            pending_bytes,
        }
    }
    /// Flushes earlier admitted mutations after their catalog bookkeeping.
    pub fn flush(&self) -> Request<()> {
        self.fence(false)
    }
    /// Stops admission, drains mutations, clears memory and closes the store.
    /// The returned request can be cancelled without cancelling shutdown.
    pub fn close(&self) -> Request<()> {
        self.fence(true)
    }
    fn fence(&self, close: bool) -> Request<()> {
        let permit = if close {
            None
        } else {
            match self.inner.scheduler.reserve(0) {
                Ok(permit) => Some(permit),
                Err(error) => return Request::ready(Err(error)),
            }
        };
        let (reply, receiver) = oneshot::channel();
        let rejected;
        {
            let mut closed = self.inner.admission.lock();
            if *closed {
                return Request::ready(Err(Error::Closed));
            }
            if close {
                *closed = true;
                self.inner.registry.close();
            }
            let inner = self.inner.clone();
            let future = async move {
                let result = if close {
                    inner.catalog.store.close().await
                } else {
                    inner.catalog.store.flush().await
                };
                if close {
                    inner.memory.clear();
                }
                let failure = inner.failures.lock().take();
                let result = failure.map_or_else(|| result.map_err(Error::from), Err);
                let _ = reply.send(result);
            }
            .boxed();
            rejected = self
                .inner
                .scheduler
                .submit(Job {
                    id: None,
                    future,
                    permit,
                })
                .err();
        }
        drop(rejected);
        Request::receiver(receiver)
    }
}
