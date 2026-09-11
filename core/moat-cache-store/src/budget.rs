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

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{Error, Result};

#[derive(Default)]
#[repr(align(64))]
struct Used(AtomicUsize);
impl Used {
    fn reserve(&self, amount: usize, limit: usize) -> bool {
        if amount == 0 {
            return true;
        }
        self.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                if amount <= limit - used {
                    Some(used + amount)
                } else {
                    None
                }
            })
            .is_ok()
    }
    fn release(&self, amount: usize) {
        if amount != 0 {
            let previous = self.0.fetch_sub(amount, Ordering::Relaxed);
            assert!(previous >= amount);
        }
    }
    fn get(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}
pub(crate) struct Budget {
    shared: Arc<Shared>,
    owners: Vec<Arc<Owner>>,
}
#[repr(align(64))]
struct Owner {
    shared: Arc<Shared>,
}
struct Shared {
    used_requests: Used,
    used_bytes: Used,
    used_reads: Vec<Used>,
    requests: usize,
    bytes: usize,
    read_bytes: usize,
    retained_bytes: Used,
    retained_reads: Vec<Used>,
    retention_bytes: usize,
    retention_read_bytes: usize,
}
#[derive(Clone)]
pub(crate) struct State {
    pub requests: usize,
    pub bytes: usize,
    pub reads: Vec<usize>,
}
impl Budget {
    pub fn new(requests: usize, bytes: usize, read_bytes: usize, disks: usize, read_class: usize) -> Arc<Self> {
        let shared = Arc::new(Shared {
            used_requests: Used::default(),
            used_bytes: Used::default(),
            used_reads: (0..disks).map(|_| Used::default()).collect(),
            requests,
            bytes,
            read_bytes,
            retained_bytes: Used::default(),
            retained_reads: (0..disks).map(|_| Used::default()).collect(),
            retention_bytes: bytes.saturating_sub(read_class),
            retention_read_bytes: read_bytes.saturating_sub(read_class),
        });
        Arc::new(Self {
            owners: (0..disks).map(|_| Arc::new(Owner { shared: shared.clone() })).collect(),
            shared,
        })
    }
    pub fn reserve(self: &Arc<Self>, bytes: usize, disk: Option<usize>) -> Result<Permit> {
        if !self.shared.used_requests.reserve(1, self.shared.requests) {
            return Err(Error::Busy);
        }
        if !self.shared.used_bytes.reserve(bytes, self.shared.bytes) {
            self.shared.used_requests.release(1);
            return Err(Error::Busy);
        }
        if disk.is_some_and(|disk| !self.shared.used_reads[disk].reserve(bytes, self.shared.read_bytes)) {
            self.shared.used_bytes.release(bytes);
            self.shared.used_requests.release(1);
            return Err(Error::Busy);
        }
        Ok(Permit {
            budget: self.owners[disk.unwrap_or(0)].clone(),
            bytes,
            disk,
            request: true,
        })
    }
    pub fn snapshot(&self) -> State {
        State {
            requests: self.shared.used_requests.get(),
            bytes: self.shared.used_bytes.get(),
            reads: self.shared.used_reads.iter().map(Used::get).collect(),
        }
    }
}
pub(crate) struct Permit {
    budget: Arc<Owner>,
    bytes: usize,
    disk: Option<usize>,
    request: bool,
}
impl Permit {
    pub fn retain(&self) -> Option<Retention> {
        let disk = self.disk?;
        if !self
            .budget
            .shared
            .retained_bytes
            .reserve(self.bytes, self.budget.shared.retention_bytes)
        {
            return None;
        }
        if !self.budget.shared.retained_reads[disk].reserve(self.bytes, self.budget.shared.retention_read_bytes) {
            self.budget.shared.retained_bytes.release(self.bytes);
            return None;
        }
        Some(Retention {
            budget: self.budget.clone(),
            bytes: self.bytes,
            disk,
        })
    }
    // Queued reads hold request slots. The disk worker acquires buffer credit
    // only when it can start the operation; saturation delays, not rejects, it.
    pub fn grow(&mut self, bytes: usize) -> bool {
        if bytes <= self.bytes {
            return true;
        }
        let delta = bytes - self.bytes;
        if !self.budget.shared.used_bytes.reserve(delta, self.budget.shared.bytes) {
            return false;
        }
        if self
            .disk
            .is_some_and(|disk| !self.budget.shared.used_reads[disk].reserve(delta, self.budget.shared.read_bytes))
        {
            self.budget.shared.used_bytes.release(delta);
            return false;
        }
        self.bytes = bytes;
        true
    }
    pub fn resize(&mut self, bytes: usize) {
        assert!(bytes <= self.bytes);
        let delta = self.bytes - bytes;
        if let Some(disk) = self.disk {
            self.budget.shared.used_reads[disk].release(delta);
        }
        self.budget.shared.used_bytes.release(delta);
        self.bytes = bytes;
    }
    pub fn finish_request(&mut self) {
        if self.request {
            self.budget.shared.used_requests.release(1);
            self.request = false;
        }
    }
}
pub(crate) struct Retention {
    budget: Arc<Owner>,
    bytes: usize,
    disk: usize,
}
impl Drop for Retention {
    fn drop(&mut self) {
        self.budget.shared.retained_reads[self.disk].release(self.bytes);
        self.budget.shared.retained_bytes.release(self.bytes);
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.resize(0);
        self.finish_request();
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_disk_owners_share_limits_and_keep_retention_alive_after_shutdown() {
        let budget = Budget::new(2, 64, 32, 2, 16);
        let a = budget.reserve(16, Some(0)).unwrap();
        let b = budget.reserve(16, Some(1)).unwrap();
        assert!(!Arc::ptr_eq(&a.budget, &b.budget));
        assert!(matches!(budget.reserve(0, Some(1)), Err(Error::Busy)));
        let retained = a.retain().unwrap();
        let shared = Arc::downgrade(&budget.shared);
        drop((budget, a, b));
        assert_eq!(shared.upgrade().unwrap().used_bytes.get(), 0);
        assert_eq!(shared.upgrade().unwrap().retained_bytes.get(), 16);
        drop(retained);
        assert!(shared.upgrade().is_none());
    }
    #[test]
    fn retention_reserves_read_headroom_and_rolls_back_both_limits() {
        let budget = Budget::new(8, 32, 64, 2, 16);
        let a = budget.reserve(16, Some(0)).unwrap();
        let b = budget.reserve(16, Some(1)).unwrap();
        let retained = a.retain().unwrap();
        assert!(b.retain().is_none());
        drop(retained);
        assert!(b.retain().is_some());
        assert_eq!(budget.shared.retained_bytes.get(), 0);
        let budget = Budget::new(8, 128, 32, 2, 16);
        let a = budget.reserve(16, Some(0)).unwrap();
        let b = budget.reserve(16, Some(0)).unwrap();
        let retained = a.retain().unwrap();
        assert!(b.retain().is_none());
        assert_eq!(budget.shared.retained_bytes.get(), 16);
        drop(retained);
        assert_eq!(budget.shared.retained_bytes.get(), 0);
        assert_eq!(budget.shared.retained_reads[0].get(), 0);
    }

    #[test]
    fn deferred_buffer_credit_is_bounded_and_failed_growth_rolls_back() {
        let budget = Budget::new(4, 64, 16, 2, 16);
        let held = budget.reserve(16, Some(0)).unwrap();
        let mut queued = budget.reserve(0, Some(0)).unwrap();
        assert!(!queued.grow(16));
        assert_eq!(budget.snapshot().bytes, 16);
        assert_eq!(budget.snapshot().requests, 2);
        drop(held);
        assert!(queued.grow(16));
        assert!(queued.grow(16));
        assert_eq!(budget.snapshot().bytes, 16);
        let other = budget.reserve(48, None).unwrap();
        let mut next = budget.reserve(0, Some(1)).unwrap();
        assert!(!next.grow(16));
        assert_eq!(budget.snapshot().reads, vec![16, 0]);
        drop(other);
        assert!(next.grow(16));
        drop((queued, next));
        assert_eq!(budget.snapshot().requests, 0);
        assert_eq!(budget.snapshot().bytes, 0);
    }
    #[test]
    fn rejected_disk_reservation_restores_global_credits() {
        let budget = Budget::new(4, 64, 16, 2, 16);
        let first = budget.reserve(16, Some(0)).unwrap();
        assert!(matches!(budget.reserve(1, Some(0)), Err(Error::Busy)));
        let second = budget.reserve(16, Some(1)).unwrap();
        assert_eq!(budget.snapshot().requests, 2);
        assert_eq!(budget.snapshot().bytes, 32);
        drop((first, second));
        assert_eq!(budget.snapshot().requests, 0);
        assert_eq!(budget.snapshot().bytes, 0);
    }
}
