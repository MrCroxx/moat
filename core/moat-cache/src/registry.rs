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

//! Bounded transient logical-key state. No completed disk key directory lives here.

use std::{
    hash::{BuildHasher, RandomState},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use futures_channel::oneshot;
use hashbrown::HashTable;
use parking_lot::{Mutex, MutexGuard};

use crate::{Bytes, Error, Result};

#[repr(align(64))]
pub(crate) struct Registry {
    index: Vec<Arc<Shard>>,
    hash: RandomState,
    shared: Arc<Shared>,
}
struct Shard {
    index: Mutex<Index>,
    shared: Arc<Shared>,
}
#[derive(Default)]
#[repr(align(64))]
struct Counter(AtomicUsize);
struct Shared {
    leases: Counter,
    bytes: Counter,
    max_leases: usize,
    max_key_bytes: usize,
    closed: AtomicBool,
}
#[repr(align(64))]
struct Index {
    keys: HashTable<Registered>,
    leases: usize,
    bytes: usize,
}
struct Registered {
    key: Bytes,
    hash: u64,
    state: Arc<KeyState>,
    leases: usize,
}
struct KeyState {
    state: Mutex<State>,
}
pub(crate) struct State {
    pub generation: u64,
    pub pending: usize,
    waiters: Vec<oneshot::Sender<()>>,
}
impl State {
    pub fn advance(&mut self) -> Result<u64> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(Error::Invalid("key generation exhausted"))?;
        Ok(self.generation)
    }
    pub fn wait(&mut self) -> oneshot::Receiver<()> {
        self.waiters.retain(|waiter| !waiter.is_canceled());
        let (sender, receiver) = oneshot::channel();
        self.waiters.push(sender);
        receiver
    }
}

impl Registry {
    pub fn new(max_leases: usize, max_key_bytes: usize) -> Arc<Self> {
        let shared = Arc::new(Shared {
            leases: Counter::default(),
            bytes: Counter::default(),
            max_leases,
            max_key_bytes,
            closed: AtomicBool::new(false),
        });
        Arc::new(Self {
            index: (0..64)
                .map(|_| {
                    Arc::new(Shard {
                        index: Mutex::new(Index {
                            keys: HashTable::new(),
                            leases: 0,
                            bytes: 0,
                        }),
                        shared: shared.clone(),
                    })
                })
                .collect(),
            hash: RandomState::new(),
            shared,
        })
    }

    pub fn acquire(self: &Arc<Self>, key: Bytes) -> Result<Lease> {
        let hash = self.hash.hash_one(&key);
        self.acquire_hashed(key, hash)
    }
    fn acquire_hashed(self: &Arc<Self>, mut key: Bytes, hash: u64) -> Result<Lease> {
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let shard = &self.index[hash as usize % self.index.len()];
        let mut index = shard.index.lock();
        if !reserve(&self.shared.leases.0, 1, self.shared.max_leases) {
            return Err(Error::Busy);
        }
        let state = if let Some(registered) = index.keys.find_mut(hash, |registered| registered.key == key) {
            key = registered.key.clone();
            registered.leases += 1;
            registered.state.clone()
        } else {
            if !reserve(&self.shared.bytes.0, key.len(), self.shared.max_key_bytes) {
                self.shared.leases.0.fetch_sub(1, Ordering::Relaxed);
                return Err(Error::Busy);
            }
            let state = Arc::new(KeyState {
                state: Mutex::new(State {
                    generation: 0,
                    pending: 0,
                    waiters: Vec::new(),
                }),
            });
            index.keys.insert_unique(
                hash,
                Registered {
                    key: key.clone(),
                    hash,
                    state: state.clone(),
                    leases: 1,
                },
                |registered| registered.hash,
            );
            index.bytes += key.len();
            state
        };
        index.leases += 1;
        Ok(Lease {
            shard: shard.clone(),
            hash,
            key,
            state,
        })
    }
    pub fn close(&self) {
        self.shared.closed.store(true, Ordering::Release);
    }
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }
    pub fn snapshot(&self) -> (usize, usize, usize) {
        let shards: Vec<_> = self.index.iter().map(|s| s.index.lock()).collect();
        shards.iter().fold((0, 0, 0), |(leases, keys, bytes), s| {
            (leases + s.leases, keys + s.keys.len(), bytes + s.bytes)
        })
    }
}

fn reserve(counter: &AtomicUsize, amount: usize, limit: usize) -> bool {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
            if amount <= limit - used {
                Some(used + amount)
            } else {
                None
            }
        })
        .is_ok()
}

pub(crate) struct Lease {
    shard: Arc<Shard>,
    hash: u64,
    pub key: Bytes,
    state: Arc<KeyState>,
}
impl Lease {
    pub fn belongs_to(&self, registry: &Registry) -> bool {
        Arc::ptr_eq(&self.shard.shared, &registry.shared)
    }
    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state.state.lock()
    }
    pub fn valid(&self, generation: u64) -> bool {
        !self.shard.shared.closed.load(Ordering::Acquire) && self.lock().generation == generation
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut index = self.shard.index.lock();
        index.leases -= 1;
        self.shard.shared.leases.0.fetch_sub(1, Ordering::Relaxed);
        let mut entry = index
            .keys
            .find_entry(self.hash, |registered| Arc::ptr_eq(&registered.state, &self.state))
            .unwrap_or_else(|_| panic!("registered lease"));
        entry.get_mut().leases -= 1;
        if entry.get().leases == 0 {
            entry.remove();
            index.bytes -= self.key.len();
            self.shard.shared.bytes.0.fetch_sub(self.key.len(), Ordering::Relaxed);
        }
    }
}

// An admitted mutation always releases its pending state, even when a caller
// cancels its reply or a user callback unwinds its completion task.
pub(crate) struct Mutation {
    pub lease: Lease,
}
impl Drop for Mutation {
    fn drop(&mut self) {
        let waiters = {
            let mut state = self.lease.lock();
            state.pending -= 1;
            if state.pending == 0 {
                std::mem::take(&mut state.waiters)
            } else {
                Vec::new()
            }
        };
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }
}

/// A miss-generation capability for optional conditional population.
///
/// This token neither elects a loader nor serializes source requests. It holds
/// bounded transient key state until consumed or dropped. Tokens belong to one
/// cache instance and cannot populate a different instance after restart.
pub struct FillToken {
    pub(crate) lease: Lease,
    pub(crate) generation: u64,
}
impl FillToken {
    /// A point-in-time hint; population atomically checks validity again.
    pub fn is_valid(&self) -> bool {
        self.lease.valid(self.generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leases_keep_their_shard_and_identity_after_the_registry_is_dropped() {
        let registry = Registry::new(4, 64);
        let lease = registry.acquire(Bytes::from(vec![1; 16])).unwrap();
        let shard = Arc::downgrade(&lease.shard);
        drop(registry);
        let other = Registry::new(4, 64);
        assert!(!lease.belongs_to(&other));
        assert!(lease.valid(0));
        assert!(shard.upgrade().is_some());
        drop(lease);
        assert!(shard.upgrade().is_none());
    }

    #[test]
    fn hash_collisions_keep_distinct_generations_and_release_the_correct_key() {
        let registry = Registry::new(4, 8192);
        let a = registry.acquire_hashed(Bytes::from(vec![1; 4096]), 7).unwrap();
        let b = registry.acquire_hashed(Bytes::from(vec![2; 4096]), 7).unwrap();
        a.lock().advance().unwrap();
        assert!(b.valid(0));
        let a2 = registry.acquire_hashed(Bytes::from(vec![1; 4096]), 7).unwrap();
        assert!(a2.valid(1));
        assert!(a.key.shares_backing(&a2.key));
        drop((a, a2));
        assert_eq!(registry.snapshot(), (1, 1, 4096));
        assert!(b.valid(0));
        drop(b);
        assert_eq!(registry.snapshot(), (0, 0, 0));
    }

    #[test]
    fn shared_input_key_is_retained_without_copying() {
        let registry = Registry::new(2, 4096);
        let input = Bytes::from(vec![7; 4096]);
        let lease = registry.acquire(input.clone()).unwrap();
        assert!(lease.key.shares_backing(&input));
    }

    #[test]
    fn rejected_new_key_restores_global_lease_credit() {
        let registry = Registry::new(3, 16);
        let first = registry.acquire(Bytes::from(vec![1; 16])).unwrap();
        assert!(matches!(registry.acquire(Bytes::from(vec![2; 16])), Err(Error::Busy)));
        let second = registry.acquire(Bytes::from(vec![1; 16])).unwrap();
        let third = registry.acquire(Bytes::from(vec![1; 16])).unwrap();
        assert!(matches!(registry.acquire(Bytes::from(vec![1; 16])), Err(Error::Busy)));
        assert_eq!(registry.snapshot(), (3, 1, 16));
        drop((first, second, third));
        assert_eq!(registry.snapshot(), (0, 0, 0));
    }

    #[test]
    fn concurrent_shards_share_global_limits() {
        let registry = Registry::new(4, 64);
        let barrier = std::sync::Barrier::new(9);
        std::thread::scope(|scope| {
            for key in 0..8u8 {
                let registry = &registry;
                let barrier = &barrier;
                scope.spawn(move || {
                    let lease = registry.acquire(Bytes::from(vec![key; 16]));
                    barrier.wait();
                    barrier.wait();
                    drop(lease);
                });
            }
            barrier.wait();
            assert_eq!(registry.snapshot(), (4, 4, 64));
            barrier.wait();
        });
        assert_eq!(registry.snapshot(), (0, 0, 0));
    }

    #[test]
    fn equal_keys_share_one_allocation_and_old_leases_prevent_generation_reuse() {
        let registry = Registry::new(3, 4096);
        let first = registry.acquire(Bytes::from(vec![7; 4096])).unwrap();
        let second = registry.acquire(Bytes::from(vec![7; 4096])).unwrap();
        assert!(first.key.shares_backing(&second.key));
        assert_eq!(registry.snapshot(), (2, 1, 4096));
        first.lock().advance().unwrap();
        assert!(!second.valid(0));
        drop(first);
        let third = registry.acquire(Bytes::from(vec![7; 4096])).unwrap();
        assert!(third.valid(1));
        drop(second);
        drop(third);
        assert_eq!(registry.snapshot(), (0, 0, 0));
    }

    #[test]
    fn concurrent_last_releases_remove_state_and_restore_budget() {
        let registry = Registry::new(2, 4096);
        for _ in 0..100 {
            let first = registry.acquire(Bytes::from(vec![1; 4096])).unwrap();
            let second = registry.acquire(Bytes::from(vec![1; 4096])).unwrap();
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    barrier.wait();
                    drop(first);
                });
                barrier.wait();
                drop(second);
            });
            assert_eq!(registry.snapshot(), (0, 0, 0));
        }
    }
}
