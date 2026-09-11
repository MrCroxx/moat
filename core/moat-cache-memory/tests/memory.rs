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

//! Behavioral, lifetime and concurrency coverage for all resident policies.

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    sync::{
        Arc, Barrier, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use moat_cache_memory::{Cache, Equivalent, LfuConfig, LruConfig, Policy, Priority, RemovalReason, S3FifoConfig};

fn policies() -> Vec<Policy> {
    vec![
        Policy::Fifo,
        Policy::Lru(LruConfig::default()),
        Policy::Lfu(LfuConfig::default()),
        Policy::S3Fifo(S3FifoConfig::default()),
        Policy::Sieve,
    ]
}

#[test]
fn shared_versions_borrowed_keys_properties_and_cache_lifetime() {
    for policy in policies() {
        let cache = Cache::<String, String, usize>::builder(3)
            .shards(1)
            .policy(policy)
            .build()
            .unwrap();
        let first = cache.insert_with("key".into(), "old".into(), 42, Priority::High);
        let another = cache.get("key").unwrap();
        assert!(first.ptr_eq(&another));
        assert_eq!(*another.properties(), 42);
        let second = cache.insert_with("key".into(), "new".into(), 7, Priority::Normal);
        assert!(!first.is_resident());
        assert_eq!(first.value(), "old");
        assert_eq!(cache.get("key").unwrap().value(), "new");
        assert_eq!(cache.statistics().allocated_weight, 2);
        drop(first);
        drop(another);
        assert_eq!(cache.statistics().allocated_weight, 1);
        drop(cache);
        assert!(!second.is_resident());
        assert_eq!(second.value(), "new");
    }
}

#[derive(Eq, PartialEq, Debug)]
struct Colliding(u64);
impl Hash for Colliding {
    fn hash<H: Hasher>(&self, h: &mut H) {
        0u64.hash(h);
    }
}
struct Query(u64);
impl Hash for Query {
    fn hash<H: Hasher>(&self, h: &mut H) {
        0u64.hash(h);
    }
}
impl Equivalent<Colliding> for Query {
    fn equivalent(&self, key: &Colliding) -> bool {
        self.0 == key.0
    }
}

#[test]
fn exact_comparison_survives_collisions_and_non_clone_keys() {
    for policy in policies() {
        let cache = Cache::<Colliding, u64>::builder(64)
            .shards(1)
            .policy(policy)
            .build()
            .unwrap();
        for i in 0..64 {
            cache.insert(Colliding(i), i);
        }
        for i in 0..64 {
            assert_eq!(*cache.get(&Query(i)).unwrap(), i);
        }
        for i in (0..64).step_by(2) {
            assert_eq!(*cache.remove(&Query(i)).unwrap(), i);
        }
        for i in 0..64 {
            assert_eq!(cache.contains(&Query(i)), i % 2 == 1);
        }
        assert!(cache.get(&Query(65)).is_none());
    }
}

#[test]
fn weights_rejection_resize_and_held_allocations_are_distinct() {
    for policy in policies() {
        let cache = Cache::<u64, Vec<u8>>::builder(10)
            .shards(1)
            .policy(policy)
            .weigher(|_, value, _| value.len())
            .admission(|key, _, _| *key != 99)
            .build()
            .unwrap();
        let held = cache.insert(1, vec![1; 4]);
        let other = cache.insert(2, vec![2; 4]);
        assert_eq!(cache.statistics().resident_weight, 8);
        let rejected = cache.insert(99, vec![0; 2]);
        assert!(!rejected.is_resident());
        drop(rejected);
        // An overweight replacement must not leave the older cached value.
        let large = cache.insert(1, vec![3; 20]);
        assert!(!large.is_resident());
        assert!(!cache.contains(&1));
        assert_eq!(held.value(), &vec![1; 4]);
        assert_eq!(cache.statistics().allocated_weight, 28);
        cache.resize(0);
        drop(other);
        assert_eq!(cache.statistics().resident_weight, 0);
        assert!(cache.is_empty());
        drop(held);
        drop(large);
        assert_eq!(cache.statistics().allocated_weight, 0);
        cache.resize(1);
        let zero = cache.insert(10, vec![]);
        assert_eq!(zero.weight(), 1);
        assert!(zero.is_resident());
    }
}

#[test]
fn fifo_hits_do_not_change_order_and_sieve_hits_get_a_second_chance() {
    for (policy, victim) in [(Policy::Fifo, 1), (Policy::Sieve, 2)] {
        let cache = Cache::<u64, u64>::builder(3).shards(1).policy(policy).build().unwrap();
        for key in 1..=3 {
            cache.insert(key, key);
        }
        cache.get(&1).unwrap();
        cache.insert(4, 4);
        assert!(!cache.contains(&victim));
        assert_eq!(cache.len(), 3);
    }
}

#[test]
fn lru_pins_last_release_priority_and_capacity_rejection() {
    let cache = Cache::<u64, u64>::builder(2)
        .shards(1)
        .policy(Policy::Lru(LruConfig::default()))
        .build()
        .unwrap();
    let a = cache.insert(1, 1);
    let b = cache.insert(2, 2);
    let clone = a.clone();
    assert!(!cache.insert(3, 3).is_resident());
    drop(a);
    assert!(!cache.insert(4, 4).is_resident());
    drop(clone);
    assert!(cache.insert(5, 5).is_resident());
    assert!(!cache.contains(&1));
    assert!(cache.contains(&2));
    cache.resize(0);
    assert_eq!(cache.len(), 1);
    drop(b);
    assert!(cache.is_empty());
    cache.resize(4);
    cache.insert_with(10, 10, (), Priority::High);
    for key in 11..=20 {
        cache.insert(key, key);
    }
    assert!(cache.contains(&10));
}

#[test]
fn lru_recency_follows_last_release_including_overlapping_lookups() {
    let cache = Cache::<u64, u64>::builder(3)
        .shards(1)
        .policy(Policy::Lru(LruConfig::default()))
        .build()
        .unwrap();
    for i in 1..=3 {
        cache.insert(i, i);
    }
    let a = cache.get(&1).unwrap();
    let b = cache.get(&1).unwrap();
    drop(a);
    cache.get(&2).unwrap();
    drop(b);
    cache.insert(4, 4);
    assert!(!cache.contains(&3));
    assert!(cache.contains(&1));
    assert!(cache.contains(&2));
}

#[test]
fn frequency_policies_resist_one_hit_scan_pollution() {
    for policy in [
        Policy::Lfu(LfuConfig::default()),
        Policy::S3Fifo(S3FifoConfig::default()),
    ] {
        let cache = Cache::<u64, u64>::builder(100)
            .shards(1)
            .policy(policy.clone())
            .build()
            .unwrap();
        for i in 0..100 {
            cache.insert(i, i);
        }
        for _ in 0..20 {
            for i in 0..10 {
                cache.get(&i).unwrap();
            }
        }
        for i in 100..180 {
            cache.insert(i, i);
        }
        let retained = (0..10).filter(|i| cache.contains(i)).count();
        assert!(retained >= 9, "{policy:?} retained only {retained} hot records");
        assert!(cache.statistics().resident_weight <= 100);
    }
}

#[test]
fn callbacks_can_reenter_lookup_and_resize_without_a_cache_lock() {
    let slot = Arc::new(Mutex::new(None::<Cache<u64, u64>>));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let cache = Cache::<u64, u64>::builder(1)
        .shards(1)
        .policy(Policy::Fifo)
        .listener({
            let slot = slot.clone();
            let observed = observed.clone();
            move |event| {
                let cache = slot.lock().unwrap().as_ref().unwrap().clone();
                cache.contains(event.entry.key());
                cache.resize(1);
                observed
                    .lock()
                    .unwrap()
                    .push((*event.entry.key(), *event.entry, event.reason));
            }
        })
        .build()
        .unwrap();
    *slot.lock().unwrap() = Some(cache.clone());
    let (send, recv) = std::sync::mpsc::channel();
    let task = thread::spawn(move || {
        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.remove(&2);
        send.send(()).unwrap();
    });
    recv.recv_timeout(Duration::from_secs(5))
        .expect("listener reentry deadlocked");
    task.join().unwrap();
    slot.lock().unwrap().take();
    assert_eq!(
        *observed.lock().unwrap(),
        vec![(1, 1, RemovalReason::Evicted), (2, 2, RemovalReason::Removed)]
    );
}

struct ReenterOnDrop {
    owner: Weak<Cache<u64, ReenterOnDrop>>,
    drops: Arc<AtomicUsize>,
}
impl Drop for ReenterOnDrop {
    fn drop(&mut self) {
        if let Some(cache) = self.owner.upgrade() {
            cache.contains(&0);
        }
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn value_destruction_runs_outside_shard_locks() {
    let cache = Arc::new(Cache::builder(1).shards(1).policy(Policy::Fifo).build().unwrap());
    let drops = Arc::new(AtomicUsize::new(0));
    let (send, recv) = std::sync::mpsc::channel();
    let task = thread::spawn({
        let drops = drops.clone();
        move || {
            for i in 0..10 {
                cache.insert(
                    i,
                    ReenterOnDrop {
                        owner: Arc::downgrade(&cache),
                        drops: drops.clone(),
                    },
                );
            }
            cache.clear();
            send.send(()).unwrap();
        }
    });
    recv.recv_timeout(Duration::from_secs(5))
        .expect("destructor reentry deadlocked");
    task.join().unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 10);
}

#[test]
fn concurrent_versions_removal_and_last_release_remain_consistent() {
    for policy in policies() {
        let cache = Cache::<u64, (u64, Vec<u64>)>::builder(128)
            .shards(8)
            .policy(policy)
            .build()
            .unwrap();
        let barrier = Arc::new(Barrier::new(5));
        let mut threads = Vec::new();
        for worker in 0..4 {
            let cache = cache.clone();
            let barrier = barrier.clone();
            threads.push(thread::spawn(move || {
                barrier.wait();
                for n in 0..2000 {
                    let key = (n * 7 + worker * 13) % 100;
                    let entry = cache.insert(key, (n, vec![n; 8]));
                    let clone = entry.clone();
                    if let Some(found) = cache.get(&key) {
                        assert!(found.1.iter().all(|v| *v == found.0));
                    }
                    if n % 7 == 0 {
                        cache.remove(&key);
                    }
                    assert_eq!(clone.0, n);
                    assert!(clone.1.iter().all(|v| *v == n));
                }
            }));
        }
        barrier.wait();
        for t in threads {
            t.join().unwrap();
        }
        cache.clear();
        assert_eq!(cache.statistics().allocated_weight, 0);
        assert_eq!(cache.statistics().resident_weight, 0);
        assert_eq!(cache.len(), 0);
    }
}

#[test]
fn randomized_replacements_and_resizes_never_return_an_old_version() {
    for policy in policies() {
        let cache = Cache::<u64, (u64, usize)>::builder(37)
            .shards(1)
            .policy(policy)
            .weigher(|_, v, _| v.1)
            .build()
            .unwrap();
        let mut model = HashMap::new();
        let mut rng = 0x124871fe991234u64;
        for generation in 0..10000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let key = rng % 73;
            match rng % 10 {
                0 => {
                    cache.remove(&key);
                    model.remove(&key);
                }
                1 => {
                    cache.resize((rng >> 10) as usize % 100);
                }
                2 => {
                    cache.clear();
                    model.clear();
                }
                _ => {
                    let value = (generation, ((rng >> 16) % 12) as usize + 1);
                    cache.insert(key, value);
                    model.insert(key, value);
                }
            }
            if let Some(entry) = cache.get(&key) {
                assert_eq!(model.get(&key), Some(entry.value()));
            }
            assert!(cache.statistics().resident_weight <= cache.capacity());
        }
        cache.clear();
        assert_eq!(cache.statistics().allocated_weight, 0);
    }
}

#[test]
fn invalid_configuration_is_rejected_before_allocating_policy_state() {
    assert!(Cache::<u64, u64>::builder(1).shards(0).build().is_err());
    assert!(
        Cache::<u64, u64>::builder(1)
            .policy(Policy::Lfu(LfuConfig {
                sketch_width: 3,
                ..Default::default()
            }))
            .build()
            .is_err()
    );
    assert!(
        Cache::<u64, u64>::builder(1)
            .policy(Policy::Lfu(LfuConfig {
                window_percent: 50,
                protected_percent: 50,
                ..Default::default()
            }))
            .build()
            .is_err()
    );
    assert!(
        Cache::<u64, u64>::builder(1)
            .policy(Policy::S3Fifo(S3FifoConfig {
                small_percent: 100,
                ..Default::default()
            }))
            .build()
            .is_err()
    );
    assert!(
        Cache::<u64, u64>::builder(1)
            .policy(Policy::Lru(LruConfig {
                high_priority_percent: 101
            }))
            .build()
            .is_err()
    );
}

#[derive(Default)]
struct IdentityHasher(u64);
impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0 = bytes
            .iter()
            .fold(0, |hash, &byte| hash.wrapping_mul(257).wrapping_add(byte as u64));
    }
    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

#[test]
fn configurable_hashing_and_exact_shard_budget_partition() {
    let cache = Cache::<u64, u64>::builder(5)
        .shards(3)
        .policy(Policy::Fifo)
        .hash_builder(std::hash::BuildHasherDefault::<IdentityHasher>::default())
        .build()
        .unwrap();
    for key in [0, 3, 1, 4, 2] {
        assert!(cache.insert(key, key).is_resident());
    }
    assert_eq!(cache.len(), 5);
    cache.insert(5, 5);
    assert!(!cache.contains(&2));
    assert_eq!(cache.len(), 5);
    cache.resize(4);
    assert!(!cache.contains(&1));
    assert_eq!(cache.len(), 4);
    assert_eq!(cache.capacity(), 4);
    assert_eq!(cache.statistics().resident_weight, 4);
}

#[test]
fn prepared_mutations_defer_callbacks_and_destruction_until_application_unlocks() {
    struct Value {
        gate: Arc<Mutex<()>>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Value {
        fn drop(&mut self) {
            assert!(self.gate.try_lock().is_ok());
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    let gate = Arc::new(Mutex::new(()));
    let drops = Arc::new(AtomicUsize::new(0));
    let events = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<u64, Value>::builder(1)
        .shards(1)
        .weigher({
            let gate = gate.clone();
            move |_, _, _| {
                assert!(gate.try_lock().is_ok());
                1
            }
        })
        .admission({
            let gate = gate.clone();
            move |_, _, _| {
                assert!(gate.try_lock().is_ok());
                true
            }
        })
        .listener({
            let gate = gate.clone();
            let events = events.clone();
            move |_| {
                assert!(gate.try_lock().is_ok());
                events.fetch_add(1, Ordering::Relaxed);
            }
        })
        .build()
        .unwrap();
    cache.insert(
        1,
        Value {
            gate: gate.clone(),
            drops: drops.clone(),
        },
    );
    let prepared = cache.prepare(
        2,
        Value {
            gate: gate.clone(),
            drops: drops.clone(),
        },
        (),
        Priority::Normal,
    );
    assert!(!prepared.entry().is_resident());
    let change = {
        let _guard = gate.lock().unwrap();
        prepared.commit()
    };
    assert_eq!(events.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(change.finish());
    assert_eq!(events.load(Ordering::Relaxed), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    let change = {
        let _guard = gate.lock().unwrap();
        cache.remove_deferred(&2)
    };
    assert_eq!(events.load(Ordering::Relaxed), 1);
    drop(change);
    assert_eq!(events.load(Ordering::Relaxed), 2);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[test]
fn rejected_read_promotion_preserves_residency_but_rejected_insert_replaces_it() {
    let cache = moat_cache_memory::Cache::<u64, u64>::builder(8)
        .shards(1)
        .admission(|_, value, _| *value != 2)
        .build()
        .unwrap();
    let old = cache.insert(1, 1);
    let rejected = cache
        .prepare(1, 2, (), moat_cache_memory::Priority::Normal)
        .promote()
        .finish()
        .unwrap();
    assert!(!rejected.is_resident());
    assert!(cache.get(&1).unwrap().ptr_eq(&old));
    assert!(!cache.insert(1, 2).is_resident());
    assert!(cache.get(&1).is_none());
}

#[test]
fn probes_reuse_hashes_and_reject_non_equivalent_promotions() {
    #[derive(Clone)]
    struct Counted(Arc<AtomicUsize>);
    impl std::hash::BuildHasher for Counted {
        type Hasher = std::hash::DefaultHasher;
        fn build_hasher(&self) -> Self::Hasher {
            self.0.fetch_add(1, Ordering::Relaxed);
            Self::Hasher::new()
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let cache = Cache::<String, usize>::builder(8)
        .hash_builder(Counted(calls.clone()))
        .build()
        .unwrap();
    let probe = cache.probe("key");
    assert!(probe.get().is_none());
    let first = probe
        .prepare("key".into(), 1, (), Priority::Normal)
        .unwrap()
        .promote()
        .finish()
        .unwrap();
    assert!(probe.get().unwrap().ptr_eq(&first));
    assert!(probe.prepare("other".into(), 2, (), Priority::Normal).is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    cache.remove("key");
    assert!(probe.get().is_none());
    assert!(!first.is_resident());
    assert_eq!(*first.value(), 1);
    let newer = probe
        .prepare("key".into(), 3, (), Priority::High)
        .unwrap()
        .promote()
        .finish()
        .unwrap();
    assert!(probe.get().unwrap().ptr_eq(&newer));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}
