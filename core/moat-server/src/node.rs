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

//! A node: the disks of one machine and the workers that drive them.
//!
//! Opening a node recovers every disk in parallel on temporary threads (the
//! blocking part), assigns each disk an owner worker, and can then start any
//! number of workers, each attached to every disk and holding the writers of
//! the disks it owns.

use std::{sync::Arc, thread};

use moat_common::ChunkId;
use moat_engine::{Device, Engine, Options, RecoveryReport};

use crate::{
    placement::{Placement, Target},
    worker::{DiskId, Handler, Worker, WorkerError, WorkerOptions},
};

/// Errors from assembling a node.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// A disk failed to open.
    #[error("disk {disk}: {source}")]
    Open {
        /// Index of the disk in the list passed to `open`.
        disk: DiskId,
        /// The cause.
        #[source]
        source: moat_engine::Error,
    },
    /// A worker failed to start.
    #[error(transparent)]
    Worker(#[from] WorkerError),
    /// The node has no disks.
    #[error("no disks")]
    NoDisks,
}

/// The opened disks of a machine.
pub struct Node {
    engines: Vec<Engine>,
    reports: Vec<RecoveryReport>,
    owners: Vec<usize>,
    placement: Placement,
}

impl Node {
    /// Opens every device in parallel. Use [`Self::assign_owners`] to assign
    /// disks to workers after recovery.
    pub fn open(devices: Vec<Arc<dyn Device>>, options: Options) -> Result<Self, NodeError> {
        if devices.is_empty() {
            return Err(NodeError::NoDisks);
        }
        let handles: Vec<_> = devices
            .into_iter()
            .map(|device| {
                let options = options.clone();
                thread::spawn(move || moat_engine::open(device, options))
            })
            .collect();
        // Join every recovery thread before propagating an error: no disk
        // should still be recovering after `open` has returned to its caller.
        let recovered: Vec<_> = handles
            .into_iter()
            .map(|h| {
                h.join().unwrap_or_else(|_| {
                    Err(moat_engine::Error::Io(std::io::Error::other(
                        "recovery thread panicked",
                    )))
                })
            })
            .collect();
        let mut engines = Vec::new();
        let mut reports = Vec::new();
        for (disk, result) in recovered.into_iter().enumerate() {
            let (engine, report) = result.map_err(|source| NodeError::Open { disk, source })?;
            engines.push(engine);
            reports.push(report);
        }
        let placement = Placement::new(
            engines
                .iter()
                .map(|e| Target {
                    uuid: e.disk_uuid(),
                    weight: e.capacity(),
                })
                .collect(),
        );
        let owners = vec![0; engines.len()];
        Ok(Self {
            engines,
            reports,
            owners,
            placement,
        })
    }

    /// The engines, indexed by [`DiskId`].
    pub fn engines(&self) -> &[Engine] {
        &self.engines
    }

    /// What recovery found on each disk.
    pub fn reports(&self) -> &[RecoveryReport] {
        &self.reports
    }

    /// The worker that owns `disk` (holds its writer).
    pub fn owner_of(&self, disk: DiskId) -> usize {
        self.owners[disk]
    }

    /// The owner of every disk, indexed by [`DiskId`].
    pub fn owners(&self) -> &[usize] {
        &self.owners
    }

    /// The disk `id` is placed on.
    pub fn disk_of(&self, id: &ChunkId) -> DiskId {
        self.placement.disk_of(id).expect("a node has at least one disk")
    }

    /// The placement over this node's disks.
    pub fn placement(&self) -> &Placement {
        &self.placement
    }

    /// Assigns each disk to one of `workers` workers, spreading disks evenly
    /// and preferring a worker on the disk's NUMA node when both `disk_numa`
    /// (per disk) and `worker_numa` (per worker) are known.
    pub fn assign_owners(&mut self, workers: usize, disk_numa: &[Option<usize>], worker_numa: &[Option<usize>]) {
        assert!(workers > 0, "at least one worker");
        let mut load = vec![0usize; workers];
        for disk in 0..self.engines.len() {
            let node = disk_numa.get(disk).copied().flatten();
            let local = |w: usize| node.is_some() && worker_numa.get(w).copied().flatten() == node;
            // Least loaded local worker, else least loaded worker.
            let pick = (0..workers)
                .filter(|&w| local(w))
                .min_by_key(|&w| (load[w], w))
                .or_else(|| (0..workers).min_by_key(|&w| (load[w], w)))
                .expect("workers > 0");
            self.owners[disk] = pick;
            load[pick] += 1;
        }
    }

    /// Sets the owner of every disk explicitly.
    pub fn set_owners(&mut self, owners: Vec<usize>) {
        assert_eq!(owners.len(), self.engines.len());
        self.owners = owners;
    }

    /// Starts one worker per entry of `workers`, each attached to every disk
    /// and owning the disks assigned to it, running the handler `make`
    /// produces for it.
    pub fn start<H: Handler>(
        &self,
        workers: &[WorkerOptions],
        mut make: impl FnMut(usize) -> H,
    ) -> Result<Vec<Worker<H>>, NodeError> {
        let mut started = Vec::with_capacity(workers.len());
        for (w, opts) in workers.iter().enumerate() {
            let disks = self
                .engines
                .iter()
                .enumerate()
                .map(|(d, e)| (e.clone(), self.owners[d] == w))
                .collect();
            started.push(Worker::spawn(w, opts.clone(), disks, make(w))?);
        }
        Ok(started)
    }
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("disks", &self.engines.len())
            .field("owners", &self.owners)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_error_does_not_leave_other_devices_open() {
        let bad = Arc::new(moat_engine::MemDevice::new(4 << 20));
        let good = Arc::new(moat_engine::MemDevice::new(4 << 20));
        moat_engine::format(
            &*good,
            &moat_engine::FormatOptions {
                segment_size: 1 << 20,
                chunk_max: 64 << 10,
                ..Default::default()
            },
        )
        .unwrap();
        let result = Node::open(
            vec![bad.clone(), good.clone()],
            Options {
                index_capacity: 64,
                ..Default::default()
            },
        );
        assert!(matches!(result, Err(NodeError::Open { disk: 0, .. })));
        assert_eq!(Arc::strong_count(&bad), 1);
        assert_eq!(Arc::strong_count(&good), 1);
    }

    #[test]
    fn owner_assignment_prefers_numa_and_balances() {
        let devices: Vec<Arc<dyn Device>> = (0..4)
            .map(|_| {
                let d = moat_engine::MemDevice::new(4 << 20);
                moat_engine::format(
                    &d,
                    &moat_engine::FormatOptions {
                        segment_size: 1 << 20,
                        chunk_max: 64 << 10,
                        disk_uuid: [1; 16],
                    },
                )
                .unwrap();
                Arc::new(d) as Arc<dyn Device>
            })
            .collect();
        let mut node = Node::open(
            devices,
            Options {
                index_capacity: 64,
                ..Default::default()
            },
        )
        .unwrap();
        // Disks 0,1 on node 0; 2,3 on node 1. Workers 0,1 on node 0; 2 on 1.
        node.assign_owners(3, &[Some(0), Some(0), Some(1), Some(1)], &[Some(0), Some(0), Some(1)]);
        assert_eq!(node.owners(), &[0, 1, 2, 2]);
        node.assign_owners(2, &[None; 4], &[None; 2]);
        assert_eq!(node.owners(), &[0, 1, 0, 1]);
    }
}
