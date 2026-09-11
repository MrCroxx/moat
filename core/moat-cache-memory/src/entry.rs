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
    fmt,
    ops::Deref,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crate::Priority;

pub(crate) struct Record<K, V, P> {
    pub key: K,
    pub value: V,
    pub properties: P,
    pub hash: u64,
    pub weight: usize,
    pub priority: Priority,
    pub resident: AtomicBool,
    pub references: AtomicUsize,
    pub allocated: Arc<AtomicUsize>,
}

impl<K, V, P> Drop for Record<K, V, P> {
    fn drop(&mut self) {
        self.allocated.fetch_sub(self.weight, Ordering::Relaxed);
    }
}

pub(crate) trait Release<K, V, P>: Send + Sync {
    fn release(&self, record: &Arc<Record<K, V, P>>);
}

/// A shared, immutable key/value and its application properties.
///
/// Cloning a handle does not clone its key or value. A handle remains valid
/// after replacement, invalidation, eviction, or destruction of the cache.
/// LRU keeps a resident entry pinned until its last external handle is dropped.
pub struct Entry<K, V, P = ()> {
    pub(crate) record: Arc<Record<K, V, P>>,
    pub(crate) release: Option<Weak<dyn Release<K, V, P>>>,
}

impl<K, V, P> Entry<K, V, P> {
    pub(crate) fn acquire(record: Arc<Record<K, V, P>>, release: Option<Weak<dyn Release<K, V, P>>>) -> Self {
        record.references.fetch_add(1, Ordering::Relaxed);
        Self { record, release }
    }

    /// The owned key, borrowed through this handle.
    pub fn key(&self) -> &K {
        &self.record.key
    }
    /// The value, borrowed through this handle.
    pub fn value(&self) -> &V {
        &self.record.value
    }
    /// Application properties supplied at insertion.
    pub fn properties(&self) -> &P {
        &self.record.properties
    }
    /// The weight computed at insertion.
    pub fn weight(&self) -> usize {
        self.record.weight
    }
    /// Whether this version is currently resident. This is a point-in-time hint.
    pub fn is_resident(&self) -> bool {
        self.record.resident.load(Ordering::Acquire)
    }
    /// Whether two handles refer to the same immutable version.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.record, &other.record)
    }
}

impl<K, V, P> Clone for Entry<K, V, P> {
    fn clone(&self) -> Self {
        Self::acquire(self.record.clone(), self.release.clone())
    }
}

impl<K, V, P> Deref for Entry<K, V, P> {
    type Target = V;
    fn deref(&self) -> &V {
        self.value()
    }
}

impl<K, V, P> Drop for Entry<K, V, P> {
    fn drop(&mut self) {
        if self.record.references.fetch_sub(1, Ordering::AcqRel) == 1
            && self.is_resident()
            && let Some(owner) = self.release.as_ref().and_then(Weak::upgrade)
        {
            // Release rechecks the reference count under the shard lock: a
            // concurrent lookup may have acquired a new handle meanwhile.
            owner.release(&self.record);
        }
    }
}

impl<K: fmt::Debug, V: fmt::Debug, P: fmt::Debug> fmt::Debug for Entry<K, V, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entry")
            .field("key", self.key())
            .field("value", self.value())
            .field("properties", self.properties())
            .field("weight", &self.weight())
            .field("resident", &self.is_resident())
            .finish()
    }
}
