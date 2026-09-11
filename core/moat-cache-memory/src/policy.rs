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

//! Replacement queues use slab indices rather than pointers. Only policy
//! metadata changes under a shard lock; no user-owned key or value is dropped.

use std::{
    collections::{HashMap, VecDeque},
    sync::atomic::Ordering,
};

use slab::Slab;

use crate::cache::Node;

/// Per-entry replacement priority, interpreted by LRU.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Priority {
    /// Use the normal replacement list.
    #[default]
    Normal,
    /// Prefer the protected LRU pool, subject to its weighted budget.
    High,
}

/// LRU priority-pool configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LruConfig {
    /// Maximum percentage of shard capacity used by unpinned high-priority
    /// entries. Overflow moves to the normal LRU list.
    pub high_priority_percent: u8,
}
impl Default for LruConfig {
    fn default() -> Self {
        Self {
            high_priority_percent: 80,
        }
    }
}

/// Windowed TinyLFU configuration: admission window, probation and protection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LfuConfig {
    /// Percentage of capacity assigned to the admission window.
    pub window_percent: u8,
    /// Percentage of capacity assigned to the protected main queue.
    /// Window and protected percentages must sum to less than one hundred.
    pub protected_percent: u8,
    /// Power-of-two width of each of four frequency-sketch rows.
    /// Counters occupy four bytes per column and decay after ten widths of access.
    pub sketch_width: usize,
}
impl Default for LfuConfig {
    fn default() -> Self {
        Self {
            window_percent: 10,
            protected_percent: 80,
            sketch_width: 2048,
        }
    }
}

/// S3FIFO configuration with bounded nonresident history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3FifoConfig {
    /// Percentage of shard capacity assigned to the small probationary queue.
    pub small_percent: u8,
    /// Maximum ghost records per shard, in addition to a shard-capacity weight
    /// bound. Ghosts contain fingerprints and weights, never keys or values.
    pub ghost_entries: usize,
}
impl Default for S3FifoConfig {
    fn default() -> Self {
        Self {
            small_percent: 10,
            ghost_entries: 4096,
        }
    }
}

/// Built-in resident replacement algorithms.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Policy {
    /// First in, first out; hits do not update policy metadata.
    Fifo,
    /// Least recently released, with caller-held pins and priority pools.
    Lru(LruConfig),
    /// Windowed TinyLFU with a decaying count-min admission sketch.
    Lfu(LfuConfig),
    /// Small and main FIFO queues, bounded ghosts, and saturating hit counters.
    S3Fifo(S3FifoConfig),
    /// Insertion order with a sweeping eviction hand and atomic visited bits.
    #[default]
    Sieve,
}

impl Policy {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Lru(c) if c.high_priority_percent > 100 => Err("LRU priority percentage exceeds 100"),
            Self::Lfu(c)
                if c.window_percent == 0
                    || c.protected_percent == 0
                    || c.window_percent as u16 + c.protected_percent as u16 >= 100 =>
            {
                Err("TinyLFU requires positive window/protected percentages summing to less than 100")
            }
            Self::Lfu(c) if !c.sketch_width.is_power_of_two() || c.sketch_width > (1 << 24) => {
                Err("TinyLFU sketch width must be a power of two no larger than 2^24")
            }
            Self::S3Fifo(c) if c.small_percent == 0 || c.small_percent >= 100 => {
                Err("S3FIFO small percentage must be between 1 and 99")
            }
            _ => Ok(()),
        }
    }
    pub(crate) fn mutable_hits(&self) -> bool {
        matches!(self, Self::Lru(_) | Self::Lfu(_))
    }
}

#[derive(Default)]
struct List {
    head: Option<usize>,
    tail: Option<usize>,
    weight: usize,
}

pub(crate) struct State {
    policy: Policy,
    lists: [List; 3],
    capacity: usize,
    hand: Option<usize>,
    ghost: Ghost,
    sketch: Option<Sketch>,
}

impl State {
    pub(crate) fn new(policy: Policy, capacity: usize) -> Self {
        let sketch = match policy {
            Policy::Lfu(c) => Some(Sketch::new(c.sketch_width)),
            _ => None,
        };
        let ghost_limit = match policy {
            Policy::S3Fifo(c) => c.ghost_entries,
            _ => 0,
        };
        Self {
            policy,
            lists: Default::default(),
            capacity,
            hand: None,
            ghost: Ghost::new(ghost_limit, capacity),
            sketch,
        }
    }

    fn push<K, V, P>(&mut self, nodes: &mut Slab<Node<K, V, P>>, id: usize, queue: usize) {
        debug_assert!(nodes[id].queue.is_none());
        let list = &mut self.lists[queue];
        let tail = list.tail;
        if let Some(tail) = tail {
            nodes[tail].next = Some(id);
        } else {
            list.head = Some(id);
        }
        let node = &mut nodes[id];
        node.prev = tail;
        node.next = None;
        node.queue = Some(queue);
        list.tail = Some(id);
        list.weight += node.record.weight;
    }

    pub(crate) fn remove<K, V, P>(&mut self, nodes: &mut Slab<Node<K, V, P>>, id: usize) {
        let node = &nodes[id];
        let Some(queue) = node.queue else { return };
        let (prev, next, weight) = (node.prev, node.next, node.record.weight);
        if self.hand == Some(id) {
            self.hand = next;
        }
        if let Some(prev) = prev {
            nodes[prev].next = next;
        } else {
            self.lists[queue].head = next;
        }
        if let Some(next) = next {
            nodes[next].prev = prev;
        } else {
            self.lists[queue].tail = prev;
        }
        self.lists[queue].weight -= weight;
        let node = &mut nodes[id];
        node.prev = None;
        node.next = None;
        node.queue = None;
    }

    fn move_to<K, V, P>(&mut self, nodes: &mut Slab<Node<K, V, P>>, id: usize, queue: usize) {
        self.remove(nodes, id);
        self.push(nodes, id, queue);
    }

    pub(crate) fn insert<K, V, P>(&mut self, nodes: &mut Slab<Node<K, V, P>>, id: usize) {
        let queue = match self.policy {
            Policy::Lru(_) => 2, // The returned Entry is already externally held.
            Policy::S3Fifo(_) if self.ghost.contains(nodes[id].record.hash) => 1,
            _ => 0,
        };
        if let Some(sketch) = &mut self.sketch {
            sketch.record(nodes[id].record.hash);
        }
        self.push(nodes, id, queue);
        if let Policy::Lfu(c) = self.policy {
            // During warmup fill the main queue without rejecting candidates.
            // At capacity, victim selection compares window and main frequency.
            while self.total_weight() <= self.capacity
                && self.lists[0].weight > portion(self.capacity, c.window_percent)
            {
                let Some(id) = self.lists[0].head else { break };
                self.move_to(nodes, id, 1);
            }
        }
    }

    pub(crate) fn access_shared<K, V, P>(&self, node: &Node<K, V, P>) {
        match self.policy {
            Policy::Sieve => node.frequency.store(1, Ordering::Relaxed),
            Policy::S3Fifo(_) => {
                let _ = node
                    .frequency
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| Some((n + 1).min(3)));
            }
            Policy::Fifo => {}
            _ => unreachable!("mutable policy requires an exclusive shard lock"),
        }
    }

    pub(crate) fn access<K, V, P>(&mut self, nodes: &mut Slab<Node<K, V, P>>, id: usize) {
        match self.policy {
            Policy::Lru(_) => {
                if nodes[id].queue != Some(2) {
                    self.move_to(nodes, id, 2);
                }
            }
            Policy::Lfu(c) => {
                self.sketch
                    .as_mut()
                    .expect("TinyLFU sketch")
                    .record(nodes[id].record.hash);
                let queue = if nodes[id].queue == Some(0) { 0 } else { 2 };
                self.move_to(nodes, id, queue);
                self.demote(nodes, 2, 1, portion(self.capacity, c.protected_percent));
            }
            _ => unreachable!("shared policy uses shared access"),
        }
    }

    pub(crate) fn release<K, V, P>(&mut self, nodes: &mut Slab<Node<K, V, P>>, id: usize) {
        if let Policy::Lru(c) = self.policy {
            if nodes[id].queue != Some(2) {
                return;
            }
            let queue = usize::from(nodes[id].record.priority == Priority::High);
            self.move_to(nodes, id, queue);
            self.demote(nodes, 1, 0, portion(self.capacity, c.high_priority_percent));
        }
    }

    fn demote<K, V, P>(&mut self, nodes: &mut Slab<Node<K, V, P>>, from: usize, to: usize, limit: usize) {
        while self.lists[from].weight > limit {
            let Some(id) = self.lists[from].head else { break };
            self.move_to(nodes, id, to);
        }
    }

    // Returns a linked victim; the cache removes it from both policy and index.
    pub(crate) fn victim<K, V, P>(&mut self, nodes: &mut Slab<Node<K, V, P>>) -> Option<usize> {
        match self.policy {
            Policy::Fifo => self.lists[0].head,
            Policy::Lru(_) => self.lists[0].head.or(self.lists[1].head),
            Policy::Sieve => loop {
                let id = self.hand.or(self.lists[0].head)?;
                self.hand = nodes[id].next;
                if nodes[id].frequency.swap(0, Ordering::Relaxed) == 0 {
                    return Some(id);
                }
            },
            Policy::Lfu(c) => {
                if self.lists[0].weight > portion(self.capacity, c.window_percent)
                    && let Some(candidate) = self.lists[0].head
                {
                    let Some(victim) = self.lists[1].head else {
                        return Some(candidate);
                    };
                    let sketch = self.sketch.as_ref().expect("TinyLFU sketch");
                    if sketch.estimate(nodes[candidate].record.hash) > sketch.estimate(nodes[victim].record.hash) {
                        self.move_to(nodes, candidate, 1);
                        return Some(victim);
                    }
                    return Some(candidate);
                }
                self.lists[1].head.or(self.lists[2].head).or(self.lists[0].head)
            }
            Policy::S3Fifo(c) => loop {
                if (self.lists[0].weight > portion(self.capacity, c.small_percent) || self.lists[1].head.is_none())
                    && let Some(id) = self.lists[0].head
                {
                    if nodes[id].frequency.swap(0, Ordering::Relaxed) > 1 {
                        self.move_to(nodes, id, 1);
                    } else {
                        self.ghost.push(nodes[id].record.hash, nodes[id].record.weight);
                        return Some(id);
                    }
                } else {
                    let id = self.lists[1].head?;
                    let frequency = nodes[id].frequency.load(Ordering::Relaxed);
                    if frequency == 0 {
                        return Some(id);
                    }
                    nodes[id].frequency.store(frequency - 1, Ordering::Relaxed);
                    self.move_to(nodes, id, 1);
                }
            },
        }
    }

    pub(crate) fn resize<K, V, P>(&mut self, nodes: &mut Slab<Node<K, V, P>>, capacity: usize) {
        self.capacity = capacity;
        self.ghost.capacity = capacity;
        self.ghost.trim();
        match self.policy {
            Policy::Lru(c) => self.demote(nodes, 1, 0, portion(capacity, c.high_priority_percent)),
            Policy::Lfu(c) => self.demote(nodes, 2, 1, portion(capacity, c.protected_percent)),
            _ => {}
        }
    }

    pub(crate) fn clear_history(&mut self) {
        self.hand = None;
        self.ghost.queue.clear();
        self.ghost.counts.clear();
        self.ghost.weight = 0;
        if let Some(sketch) = &mut self.sketch {
            sketch.counters.fill(0);
            sketch.samples = 0;
        }
    }

    fn total_weight(&self) -> usize {
        self.lists.iter().map(|list| list.weight).sum()
    }
}

// Avoid multiplication overflow for capacities near usize::MAX.
fn portion(capacity: usize, percent: u8) -> usize {
    capacity / 100 * percent as usize + capacity % 100 * percent as usize / 100
}

struct Ghost {
    queue: VecDeque<(u64, usize)>,
    counts: HashMap<u64, usize>,
    weight: usize,
    capacity: usize,
    limit: usize,
}
impl Ghost {
    fn new(limit: usize, capacity: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            counts: HashMap::new(),
            weight: 0,
            capacity,
            limit,
        }
    }
    fn contains(&self, hash: u64) -> bool {
        self.counts.contains_key(&hash)
    }
    fn push(&mut self, hash: u64, weight: usize) {
        if self.limit == 0 || weight > self.capacity {
            return;
        }
        while self.weight > self.capacity - weight || self.queue.len() >= self.limit {
            self.pop();
        }
        self.queue.push_back((hash, weight));
        *self.counts.entry(hash).or_default() += 1;
        self.weight += weight;
    }
    fn pop(&mut self) {
        if let Some((hash, weight)) = self.queue.pop_front() {
            self.weight -= weight;
            let count = self.counts.get_mut(&hash).expect("ghost count");
            *count -= 1;
            if *count == 0 {
                self.counts.remove(&hash);
            }
        }
    }
    fn trim(&mut self) {
        while self.weight > self.capacity || self.queue.len() > self.limit {
            self.pop();
        }
    }
}

struct Sketch {
    counters: Vec<u8>,
    width: usize,
    samples: usize,
}
impl Sketch {
    fn new(width: usize) -> Self {
        Self {
            counters: vec![0; width * 4],
            width,
            samples: 0,
        }
    }
    fn index(&self, hash: u64, row: usize) -> usize {
        let mut h = hash.wrapping_add((row as u64).wrapping_mul(0x9e3779b97f4a7c15));
        h = (h ^ (h >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        h = (h ^ (h >> 27)).wrapping_mul(0x94d049bb133111eb);
        ((h ^ (h >> 31)) as usize & (self.width - 1)) | (row * self.width)
    }
    fn record(&mut self, hash: u64) {
        for row in 0..4 {
            let i = self.index(hash, row);
            self.counters[i] = self.counters[i].saturating_add(1);
        }
        self.samples += 1;
        if self.samples >= self.width * 10 {
            for counter in &mut self.counters {
                *counter >>= 1;
            }
            self.samples >>= 1;
        }
    }
    fn estimate(&self, hash: u64) -> u8 {
        (0..4).map(|row| self.counters[self.index(hash, row)]).min().unwrap()
    }
}
