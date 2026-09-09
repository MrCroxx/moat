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

//! Several workers over several in-memory disks: owners write, everyone
//! reads, shutdown seals, and a reopened node sees everything.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use moat_common::{ChunkId, HugePages, PoolOptions};
use moat_engine::{Device, Error, FormatOptions, MemDevice, Options, Outcome, PutOptions, QueueOptions, ReadOutcome};
use moat_server::{Context, Handler, Node, PollMode, QueueBackend, Step, WorkerOptions};

const DISKS: usize = 3;
const WORKERS: usize = 4;
const KEYS_PER_DISK: u64 = 200;

fn devices() -> Vec<Arc<dyn Device>> {
    (0..DISKS)
        .map(|d| {
            let dev = MemDevice::new(8 << 20);
            moat_engine::format(
                &dev,
                &FormatOptions {
                    segment_size: 1 << 20,
                    chunk_max: 64 << 10,
                    disk_uuid: [d as u8 + 1; 16],
                },
            )
            .unwrap();
            Arc::new(dev) as Arc<dyn Device>
        })
        .collect()
}

fn worker_options() -> WorkerOptions {
    WorkerOptions {
        core: None,
        queue: QueueOptions {
            depth: 64,
            pool: PoolOptions {
                bytes: 8 << 20,
                max_class: 1 << 20,
                huge_pages: HugePages::Disabled,
            },
            descriptors: 16,
        },
        backend: QueueBackend::Sync,
        poll_mode: PollMode::Adaptive {
            idle_sleep: std::time::Duration::from_micros(50),
        },
    }
}

fn key(disk: usize, i: u64) -> ChunkId {
    ChunkId::from_u128(((disk as u128) << 64) | i as u128)
}

fn value(disk: usize, i: u64) -> Vec<u8> {
    let len = 100 + (i as usize * 37) % 20_000;
    (0..len).map(|b| (b as u64 * 31 + i + disk as u64) as u8).collect()
}

/// Phase 1: every owner writes its disks' keys and waits for the tickets.
/// Phase 2: every worker reads every key of every disk. Then stop.
struct Load {
    writes_pending: HashMap<(usize, u64), (usize, u64)>,
    written: bool,
    reads_pending: HashMap<u64, (usize, u64)>,
    next_read: usize,
    reads_done: u64,
    all_written: Arc<AtomicUsize>,
    reads_total: Arc<AtomicUsize>,
    owners: usize,
}

impl Handler for Load {
    fn run(&mut self, cx: &mut Context<'_>) -> Step {
        for (disk, c) in cx.writes.drain(..) {
            let ticket = c.ticket;
            let (d, i) = self.writes_pending.remove(&(disk, ticket)).expect("known ticket");
            assert_eq!(d, disk);
            assert!(matches!(c.result, Ok(Outcome::Put { .. })), "put {i} on disk {disk}");
        }
        if !self.written {
            // Write everything the worker owns, respecting back-pressure.
            let mut issued_all = true;
            for disk in 0..cx.disks.len() {
                if !cx.owns(disk) {
                    continue;
                }
                let (q, slot) = cx.disk(disk);
                let w = slot.writer.as_mut().unwrap();
                for i in 0..KEYS_PER_DISK {
                    if self.writes_pending.values().any(|&(d, k)| d == disk && k == i)
                        || slot.engine.contains(&key(disk, i))
                    {
                        continue;
                    }
                    match w.put(q, key(disk, i), &value(disk, i), PutOptions::default()) {
                        Ok(moat_engine::PutOutcome::Written { ticket, .. }) => {
                            self.writes_pending.insert((disk, ticket), (disk, i));
                        }
                        Ok(moat_engine::PutOutcome::Exists) => {}
                        Err(Error::Busy) => {
                            issued_all = false;
                            break;
                        }
                        Err(e) => panic!("{e}"),
                    }
                }
            }
            if issued_all && self.writes_pending.is_empty() {
                self.written = true;
                if cx.disks.iter().any(|s| s.writer.is_some()) {
                    self.all_written.fetch_add(1, Ordering::AcqRel);
                }
            }
            return Step::Continue;
        }
        if self.all_written.load(Ordering::Acquire) < self.owners {
            return Step::Idle;
        }
        // Read phase: every worker reads every key of every disk, 8 at a time.
        for (disk, c) in cx.reads.drain(..) {
            let (d, i) = self.reads_pending.remove(&c.token).expect("known token");
            assert_eq!(d, disk);
            let data = c.result.unwrap().expect("not expired");
            assert_eq!(&*data, &value(disk, i)[..], "key {i} disk {disk}");
            self.reads_done += 1;
        }
        let total = DISKS * KEYS_PER_DISK as usize;
        while self.next_read < total && self.reads_pending.len() < 8 {
            let disk = self.next_read / KEYS_PER_DISK as usize;
            let i = (self.next_read % KEYS_PER_DISK as usize) as u64;
            let token = self.next_read as u64;
            let (q, slot) = cx.disk(disk);
            match slot.reader.get(q, &key(disk, i), None, token) {
                Ok(ReadOutcome::Submitted) => {
                    self.reads_pending.insert(token, (disk, i));
                    self.next_read += 1;
                }
                Ok(ReadOutcome::Miss) => panic!("missing key {i} on disk {disk}"),
                Err(Error::Busy) => break,
                Err(e) => panic!("{e}"),
            }
        }
        if self.next_read == total && self.reads_pending.is_empty() {
            self.reads_total.fetch_add(self.reads_done as usize, Ordering::AcqRel);
            return Step::Stop;
        }
        // All available reads have been issued; adaptive mode may wait for I/O.
        Step::Idle
    }
}

#[test]
fn owners_write_everyone_reads_and_a_reopened_node_recovers() {
    let devices = devices();
    let mut node = Node::open(
        devices.clone(),
        Options {
            index_capacity: 1024,
            ..Default::default()
        },
    )
    .unwrap();
    node.assign_owners(WORKERS, &[None; DISKS], &[None; WORKERS]);
    let owners: std::collections::HashSet<usize> = node.owners().iter().copied().collect();
    assert_eq!(
        owners.len(),
        DISKS,
        "each disk gets a distinct owner with more workers than disks"
    );

    let all_written = Arc::new(AtomicUsize::new(0));
    let reads_total = Arc::new(AtomicUsize::new(0));
    let workers = node
        .start(&vec![worker_options(); WORKERS], |_| Load {
            writes_pending: HashMap::new(),
            written: false,
            reads_pending: HashMap::new(),
            next_read: 0,
            reads_done: 0,
            all_written: all_written.clone(),
            reads_total: reads_total.clone(),
            owners: owners.len(),
        })
        .unwrap();
    for w in workers {
        w.join().unwrap();
    }
    assert_eq!(
        reads_total.load(Ordering::Acquire),
        WORKERS * DISKS * KEYS_PER_DISK as usize
    );
    for (d, e) in node.engines().iter().enumerate() {
        assert_eq!(e.usage().chunks, KEYS_PER_DISK as usize, "disk {d}");
    }
    drop(node);

    // Workers sealed their disks on shutdown: reopening needs no scan.
    let node = Node::open(
        devices,
        Options {
            index_capacity: 1024,
            ..Default::default()
        },
    )
    .unwrap();
    for (d, r) in node.reports().iter().enumerate() {
        assert_eq!(r.scanned, 0, "disk {d}");
        assert_eq!(r.chunks, KEYS_PER_DISK as usize, "disk {d}");
    }
    // Placement is deterministic and covers every disk.
    let mut hits = vec![0usize; DISKS];
    for i in 0..3_000u128 {
        hits[node.disk_of(&ChunkId::from_u128(i))] += 1;
    }
    assert!(hits.iter().all(|&h| h > 500), "{hits:?}");
}
