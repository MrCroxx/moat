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

//! Whole-node throughput and latency: many disks, many pinned workers, one
//! io_uring queue per worker, every worker reading every disk and each disk
//! written by its owner. The load generator is a [`Handler`] on every worker,
//! exactly where the network reactor will sit.
//!
//! ```sh
//! # List the NVMe namespaces that would be used (nothing is written):
//! cargo bench -p moat-server --bench node
//! # Format them and run (destroys their contents!):
//! MOAT_BENCH_FORMAT=yes cargo bench -p moat-server --bench node
//! ```
//!
//! Disk selection: `MOAT_BENCH_DISKS` (comma-separated device or file paths;
//! default: every NVMe namespace that is not partitioned, mounted, swap or
//! under an md/dm holder, and at least `MOAT_BENCH_MIN_TB` TB, default 1).
//! Every selected device is **formatted**; `MOAT_BENCH_FORMAT=yes` is
//! required to go past the listing.
//!
//! Knobs: `MOAT_BENCH_BYTES` (bytes used per disk, default 64 GiB; a quarter
//! per write workload), `MOAT_BENCH_WORKERS` (default: 4 per disk, bounded by
//! cores), `MOAT_BENCH_CORES` (CPU list, default: every online CPU but the
//! first two), `MOAT_BENCH_DEPTH` (queue depth, 1024), `MOAT_BENCH_POOL_MB`
//! (pool per worker, 512), `MOAT_BENCH_LARGE` / `MOAT_BENCH_SMALL` (value
//! sizes, 1 MiB / 4 KiB), `MOAT_BENCH_LARGE_INFLIGHT` (outstanding large reads
//! per worker, 4), `MOAT_BENCH_INFLIGHT` (outstanding small reads per worker,
//! comma-separated list, default 32), `MOAT_BENCH_WRITE_INFLIGHT` (outstanding
//! large puts per owned disk, 64), `MOAT_BENCH_SMALL_WRITE_INFLIGHT`
//! (outstanding small puts per owned disk; defaults to WRITE_INFLIGHT when
//! explicitly set, otherwise 512), `MOAT_BENCH_WRITE_PREFETCH` (prefetch the
//! index slot this many puts ahead, default 16; zero disables the hint),
//! `MOAT_BENCH_SECONDS` (duration of each
//! read phase, 10), `MOAT_BENCH_SYNC` (blocking queue instead of io_uring, for
//! files), `MOAT_BENCH_SEGMENT_MB` (segment size, 1024), `MOAT_BENCH_REOPEN`
//! (do not format or write; reopen the disks from a previous run with the same
//! sizes and go straight to the read phases, for profiling),
//! `MOAT_BENCH_WRITE_PHASES` (comma-separated large,small; both by default),
//! `MOAT_BENCH_READ_PHASES` (comma-separated large,small; both by default),
//! `MOAT_BENCH_REPORT_DISKS` (print per-disk phase completion counts and IOPS),
//! `MOAT_BENCH_SHARD_READS` (assign each worker one disk, for locality comparisons),
//! `MOAT_BENCH_WAIT_READS` (wait for I/O after refilling, for polling comparisons),
//! Read and write latency histograms sample one in 16 operations.
//! Default cores are spread across last-level caches, using SMT siblings last.
//! Explicit `MOAT_BENCH_CORES` preserves the supplied order.
//! `MOAT_BENCH_VERIFY` (enable header and value checksum verification on reads).
//! `MOAT_BENCH_HUGE_PAGES` selects disabled, preferred (default), or required.
//! `MOAT_BENCH_EXPECT_BACKING` checks each arena against plain, thp, 2m, or 1g;
//! thp denotes advice acceptance, not proof of promotion. `MOAT_BENCH_REPORT_POOL`
//! prints each worker's arena size and backing before its workload starts.

#[path = "support/cpus.rs"]
mod cpus;

#[path = "support/pool.rs"]
mod pool;

use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use moat_common::{ChunkId, PoolOptions, block_checksums};
use moat_engine::{
    Device, Error, FileDevice, FormatOptions, Options, PutOptions, PutOutcome, QueueOptions, ReadOutcome,
};
use moat_server::{Context, Handler, Node, PollMode, QueueBackend, Step, WorkerOptions, disk};

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn env_list(name: &str) -> Option<Vec<String>> {
    std::env::var(name).ok().map(|s| {
        s.split(',')
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect()
    })
}

// ---------------------------------------------------------------------------
// Latency histogram: 16 buckets per power of two of nanoseconds.
// ---------------------------------------------------------------------------

const HIST_BUCKETS: usize = 64 * 16;

#[derive(Clone)]
struct Hist {
    buckets: Vec<u64>,
    count: u64,
}

impl Hist {
    fn new() -> Self {
        Self {
            buckets: vec![0; HIST_BUCKETS],
            count: 0,
        }
    }

    fn record(&mut self, d: Duration) {
        let ns = (d.as_nanos() as u64).max(1);
        let exp = 63 - ns.leading_zeros() as usize;
        let mant = if exp >= 4 { (ns >> (exp - 4)) & 0xf } else { 0 } as usize;
        self.buckets[(exp << 4) | mant] += 1;
        self.count += 1;
    }

    fn merge(&mut self, other: &Hist) {
        for (a, b) in self.buckets.iter_mut().zip(&other.buckets) {
            *a += b;
        }
        self.count += other.count;
    }

    fn percentile(&self, p: f64) -> Duration {
        let target = ((self.count as f64 * p).ceil() as u64).max(1);
        let mut seen = 0;
        for (i, &n) in self.buckets.iter().enumerate() {
            seen += n;
            if seen >= target {
                let (exp, mant) = (i >> 4, (i & 0xf) as u64);
                let ns = if exp >= 4 {
                    ((16 + mant) << (exp - 4)) + (1u64 << (exp - 4)) / 2
                } else {
                    1u64 << exp
                };
                return Duration::from_nanos(ns);
            }
        }
        Duration::ZERO
    }
}

// ---------------------------------------------------------------------------
// Phases
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    WriteLarge,
    WriteSmall,
    ReadLarge { inflight: usize },
    ReadSmall { inflight: usize },
}

struct Plan {
    large: usize,
    small: usize,
    write_inflight: usize,
    small_write_inflight: usize,
    write_prefetch: u64,
    shard_reads: bool,
    wait_reads: bool,
    /// Values per disk in each size class.
    large_count: u64,
    small_count: u64,
    phases: Vec<Kind>,
}

struct SharedState {
    plan: Plan,
    /// Index into `plan.phases`, or `phases.len()` to stop.
    phase: AtomicUsize,
    /// Workers that finished the current phase.
    done: AtomicUsize,
    /// Set by the driver to end a timed phase.
    stop: AtomicBool,
    ops: AtomicU64,
    bytes: AtomicU64,
    hist: Mutex<Hist>,
    per_disk_ops: Vec<AtomicU64>,
}

fn key(kind: u128, disk: usize, i: u64) -> ChunkId {
    ChunkId::from_u128((kind << 100) | ((disk as u128) << 64) | i as u128)
}

const LARGE_KEY: u128 = 1;
const SMALL_KEY: u128 = 2;
/// Phase index before the driver starts the first phase.
const NOT_STARTED: usize = usize::MAX;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// The load generator on one worker.
struct Load {
    shared: Arc<SharedState>,
    seen_phase: Option<usize>,
    finished_phase: bool,
    rng: Rng,
    // write state
    write_cursor: Vec<u64>,
    write_pending: Vec<VecDeque<(u64, Option<Instant>)>>,
    write_outstanding: Vec<usize>,
    // read state: outstanding reads by slot (the slot number is the token)
    read_pending: Vec<Option<(usize, Option<Instant>)>>,
    read_free: Vec<usize>,
    read_outstanding: usize,
    hist: Hist,
    ops: u64,
    bytes: u64,
    per_disk: Vec<u64>,
    pattern_large: Arc<Vec<u8>>,
    sums_large: Arc<Vec<u32>>,
    pattern_small: Arc<Vec<u8>>,
}

impl Load {
    fn begin_phase(&mut self, cx: &mut Context<'_>, phase: usize) {
        self.seen_phase = Some(phase);
        self.finished_phase = false;
        self.write_cursor = vec![0; cx.disks.len()];
        self.write_pending = (0..cx.disks.len()).map(|_| VecDeque::new()).collect();
        self.write_outstanding = vec![0; cx.disks.len()];
        self.read_pending.clear();
        self.read_free.clear();
        self.read_outstanding = 0;
        self.hist = Hist::new();
        self.ops = 0;
        self.bytes = 0;
        self.per_disk = vec![0; cx.disks.len()];
    }

    fn finish_phase(&mut self) {
        self.finished_phase = true;
        let s = &self.shared;
        s.ops.fetch_add(self.ops, Ordering::AcqRel);
        s.bytes.fetch_add(self.bytes, Ordering::AcqRel);
        for (d, n) in self.per_disk.iter().enumerate() {
            s.per_disk_ops[d].fetch_add(*n, Ordering::AcqRel);
        }
        s.hist.lock().unwrap().merge(&self.hist);
        s.done.fetch_add(1, Ordering::AcqRel);
    }

    fn run_write(&mut self, cx: &mut Context<'_>, large: bool) -> Step {
        let inflight = if large {
            self.shared.plan.write_inflight
        } else {
            self.shared.plan.small_write_inflight
        };
        let (len, count, kind) = if large {
            (self.shared.plan.large, self.shared.plan.large_count, LARGE_KEY)
        } else {
            (self.shared.plan.small, self.shared.plan.small_count, SMALL_KEY)
        };
        for (disk, c) in cx.writes.drain(..) {
            // Each phase writes one record class per disk. The writer applies
            // those batches in order, so tracking needs no per-put hash lookup.
            let (ticket, started) = self.write_pending[disk].pop_front().expect("known ticket");
            assert_eq!(ticket, c.ticket);
            self.write_outstanding[disk] -= 1;
            c.result.unwrap();
            if let Some(started) = started {
                self.hist.record(started.elapsed());
            }
            self.ops += 1;
            self.bytes += len as u64;
            self.per_disk[disk] += 1;
        }
        let mut all_issued = true;
        for disk in 0..cx.disks.len() {
            if !cx.owns(disk) {
                continue;
            }
            while self.write_cursor[disk] < count && self.write_outstanding[disk] < inflight {
                let i = self.write_cursor[disk];
                let id = key(kind, disk, i);
                let (q, slot) = cx.disk(disk);
                let w = slot.writer.as_mut().unwrap();
                let ahead = self.shared.plan.write_prefetch;
                if ahead > 0 && i.saturating_add(ahead) < count {
                    w.prefetch(&key(kind, disk, i + ahead));
                }
                let outcome = if large {
                    match w.prepare_large(q, len as u32) {
                        Ok(mut v) => {
                            v.value_mut().copy_from_slice(&self.pattern_large);
                            w.put_large(q, id, v, Some(&self.sums_large), PutOptions::default())
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    w.put(q, id, &self.pattern_small, PutOptions::default())
                };
                match outcome {
                    Ok(PutOutcome::Written { ticket, .. }) => {
                        let started = ticket.is_multiple_of(16).then(Instant::now);
                        self.write_pending[disk].push_back((ticket, started));
                        self.write_outstanding[disk] += 1;
                        self.write_cursor[disk] += 1;
                    }
                    Ok(PutOutcome::Exists) => self.write_cursor[disk] += 1,
                    Err(Error::Busy) => {
                        all_issued = false;
                        break;
                    }
                    Err(e) => panic!("disk {disk}: {e}"),
                }
            }
        }
        if all_issued && self.write_outstanding.iter().all(|&n| n == 0) {
            self.finish_phase();
            return Step::Idle;
        }
        Step::Continue
    }

    fn run_read(&mut self, cx: &mut Context<'_>, large: bool, inflight: usize) -> Step {
        let (len, count, kind) = if large {
            (self.shared.plan.large, self.shared.plan.large_count, LARGE_KEY)
        } else {
            (self.shared.plan.small, self.shared.plan.small_count, SMALL_KEY)
        };
        for (disk, c) in cx.reads.drain(..) {
            let slot = c.token as usize;
            let (d, started) = self.read_pending[slot].take().expect("known token");
            self.read_free.push(slot);
            self.read_outstanding -= 1;
            debug_assert_eq!(d, disk);
            let data = c.result.unwrap().expect("not expired");
            debug_assert_eq!(data.len(), len);
            if let Some(started) = started {
                self.hist.record(started.elapsed());
            }
            self.ops += 1;
            self.bytes += len as u64;
            self.per_disk[disk] += 1;
        }
        let stop = self.shared.stop.load(Ordering::Acquire);
        if stop {
            if self.read_outstanding == 0 {
                self.finish_phase();
                return Step::Idle;
            }
            return Step::Continue;
        }
        let disks = cx.disks.len();
        while self.read_outstanding < inflight {
            let r = self.rng.next();
            let disk = if self.shared.plan.shard_reads {
                cx.worker % disks
            } else {
                (r % disks as u64) as usize
            };
            let i = (r >> 20) % count;
            let token = match self.read_free.pop() {
                Some(s) => s,
                None => {
                    self.read_pending.push(None);
                    self.read_pending.len() - 1
                }
            };
            let (q, slot) = cx.disk(disk);
            match slot.reader.get(q, &key(kind, disk, i), None, token as u64) {
                Ok(ReadOutcome::Submitted) => {
                    // Timestamps cost a vdso call each; sample one in 16.
                    let started = (r >> 40).is_multiple_of(16).then(Instant::now);
                    self.read_pending[token] = Some((disk, started));
                    self.read_outstanding += 1;
                }
                Ok(ReadOutcome::Miss) => panic!("missing key {i} on disk {disk}"),
                Err(Error::Busy) => {
                    self.read_free.push(token);
                    break;
                }
                Err(e) => panic!("{e}"),
            }
        }
        if self.shared.plan.wait_reads {
            cx.queue.poll(true).unwrap();
        }
        Step::Continue
    }
}

impl Handler for Load {
    fn start(&mut self, cx: &mut Context<'_>) {
        pool::inspect(cx.queue.pool(), cx.worker);
    }

    fn run(&mut self, cx: &mut Context<'_>) -> Step {
        let phase = self.shared.phase.load(Ordering::Acquire);
        if phase == NOT_STARTED {
            return Step::Idle;
        }
        if phase >= self.shared.plan.phases.len() {
            return Step::Stop;
        }
        if self.seen_phase != Some(phase) {
            self.begin_phase(cx, phase);
        }
        if self.finished_phase {
            // Drain stragglers (none expected) and wait for the next phase.
            cx.reads.clear();
            cx.writes.clear();
            return Step::Idle;
        }
        match self.shared.plan.phases[phase] {
            Kind::WriteLarge => self.run_write(cx, true),
            Kind::WriteSmall => self.run_write(cx, false),
            Kind::ReadLarge { inflight } => self.run_read(cx, true, inflight),
            Kind::ReadSmall { inflight } => self.run_read(cx, false, inflight),
        }
    }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

struct Selected {
    path: PathBuf,
    label: String,
    numa: Option<usize>,
}

fn select_disks() -> Vec<Selected> {
    if let Some(paths) = env_list("MOAT_BENCH_DISKS") {
        let known: HashMap<PathBuf, disk::NvmeDisk> = disk::discover()
            .unwrap_or_default()
            .into_iter()
            .map(|d| (d.path.clone(), d))
            .collect();
        return paths
            .into_iter()
            .map(|p| {
                let path = PathBuf::from(p);
                match known.get(&path) {
                    Some(d) => {
                        assert!(
                            d.is_available(),
                            "{}: in use ({:?}); refusing to format",
                            d.path.display(),
                            d.in_use
                        );
                        Selected {
                            label: format!(
                                "{} serial={} model={} {:.1} TB",
                                d.name,
                                d.serial,
                                d.model,
                                tb(d.capacity)
                            ),
                            numa: d.numa_node,
                            path,
                        }
                    }
                    None => Selected {
                        label: path.display().to_string(),
                        numa: None,
                        path,
                    },
                }
            })
            .collect();
    }
    let min_bytes = env("MOAT_BENCH_MIN_TB", 1) * 1_000_000_000_000;
    disk::discover()
        .expect("sysfs")
        .into_iter()
        .filter(|d| d.is_available() && d.capacity >= min_bytes)
        .map(|d| Selected {
            label: format!(
                "{} serial={} model={} {:.1} TB",
                d.name,
                d.serial,
                d.model,
                tb(d.capacity)
            ),
            numa: d.numa_node,
            path: d.path,
        })
        .collect()
}

fn tb(bytes: u64) -> f64 {
    bytes as f64 / 1e12
}

fn gib_per_s(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / elapsed.as_secs_f64() / (1u64 << 30) as f64
}

fn numa_of_cpus() -> HashMap<usize, usize> {
    let mut map = HashMap::new();
    for node in 0..64 {
        let cpus = disk::cpus_of_node(node);
        if cpus.is_empty() && node > 0 {
            break;
        }
        for c in cpus {
            map.insert(c, node);
        }
    }
    map
}

fn main() {
    let selected = select_disks();
    println!("{} disk(s):", selected.len());
    for s in &selected {
        println!("  {}", s.label);
    }
    if selected.is_empty() {
        return;
    }
    let sync = std::env::var_os("MOAT_BENCH_SYNC").is_some();
    let reopen = std::env::var_os("MOAT_BENCH_REOPEN").is_some();
    if !reopen && std::env::var("MOAT_BENCH_FORMAT").ok().as_deref() != Some("yes") {
        println!("set MOAT_BENCH_FORMAT=yes to format these devices and run (destroys their contents)");
        return;
    }

    let bytes = env("MOAT_BENCH_BYTES", 64 << 30);
    let large = env("MOAT_BENCH_LARGE", 1 << 20) as usize;
    let small = env("MOAT_BENCH_SMALL", 4 << 10) as usize;
    let segment = env("MOAT_BENCH_SEGMENT_MB", 1024) << 20;
    let seconds = env("MOAT_BENCH_SECONDS", 10);
    let depth = env("MOAT_BENCH_DEPTH", 1024) as u32;
    let pool_mb = env("MOAT_BENCH_POOL_MB", 512) as usize;
    let large_inflight = env("MOAT_BENCH_LARGE_INFLIGHT", 4) as usize;
    let write_inflight = env("MOAT_BENCH_WRITE_INFLIGHT", 64) as usize;
    // One packed batch holds slightly fewer than 256 four-KiB values. Keep
    // enough records outstanding to overlap several batches with encoding.
    let small_write_inflight = env("MOAT_BENCH_SMALL_WRITE_INFLIGHT", env("MOAT_BENCH_WRITE_INFLIGHT", 512)) as usize;
    assert!(write_inflight > 0 && small_write_inflight > 0);
    let inflights: Vec<usize> = env_list("MOAT_BENCH_INFLIGHT")
        .map(|l| l.iter().filter_map(|v| v.parse().ok()).collect())
        .unwrap_or_else(|| vec![32]);
    let budget = bytes / 4;
    let large_count = budget / large as u64;
    let small_count = budget / small as u64;

    // Cores and workers.
    let cores: Vec<usize> = env_list("MOAT_BENCH_CORES")
        .map(|l| l.iter().flat_map(|s| disk::parse_cpu_list(s)).collect())
        .unwrap_or_else(cpus::spread);
    let workers = env("MOAT_BENCH_WORKERS", (selected.len() * 4) as u64) as usize;
    let workers = workers.min(cores.len().max(1));
    let numa = numa_of_cpus();
    let worker_cores: Vec<Option<usize>> = (0..workers).map(|w| cores.get(w).copied()).collect();
    let worker_numa: Vec<Option<usize>> = worker_cores
        .iter()
        .map(|c| c.and_then(|c| numa.get(&c).copied()))
        .collect();

    // Format and open every disk in parallel.
    println!(
        "{} {} disk(s): segment {} MiB, chunk max 4 MiB; using {} GiB per disk",
        if reopen { "reopening" } else { "formatting" },
        selected.len(),
        segment >> 20,
        bytes >> 30
    );
    let started = Instant::now();
    let devices: Vec<Arc<dyn Device>> = selected
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let path = s.path.clone();
            std::thread::spawn(move || {
                let device = FileDevice::open(&path, !sync).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                if reopen {
                    return Arc::new(device) as Arc<dyn Device>;
                }
                let mut uuid = [0u8; 16];
                uuid[..8].copy_from_slice(&(i as u64 + 1).to_le_bytes());
                uuid[8..].copy_from_slice(&0x6d6f_6174_6265_6e63u64.to_le_bytes());
                moat_engine::format(
                    &device,
                    &FormatOptions {
                        segment_size: segment,
                        chunk_max: 4 << 20,
                        disk_uuid: uuid,
                    },
                )
                .unwrap_or_else(|e| panic!("{}: format: {e}", path.display()));
                Arc::new(device) as Arc<dyn Device>
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();
    if !reopen {
        println!("formatted in {:.1?}", started.elapsed());
    }
    let started = Instant::now();
    let index_capacity = ((large_count + small_count) as usize * 2).next_power_of_two();
    let verify_reads = std::env::var_os("MOAT_BENCH_VERIFY").is_some();
    let mut node = Node::open(
        devices,
        Options {
            index_capacity,
            verify_reads,
            ..Default::default()
        },
    )
    .expect("open");
    let disk_numa: Vec<Option<usize>> = selected.iter().map(|s| s.numa).collect();
    node.assign_owners(workers, &disk_numa, &worker_numa);
    println!("opened in {:.1?}; owners: {:?}", started.elapsed(), node.owners());
    println!(
        "{workers} worker(s) on cores {:?}, queue depth {depth}, pool {pool_mb} MiB/worker, io_uring={}, verify_reads={verify_reads}",
        &cores[..workers.min(cores.len())],
        !sync
    );

    let mut phases = Vec::new();
    if !reopen {
        let writes = env_list("MOAT_BENCH_WRITE_PHASES").unwrap_or_else(|| vec!["large".into(), "small".into()]);
        if writes.iter().any(|p| p == "large") {
            phases.push(Kind::WriteLarge);
        }
        if writes.iter().any(|p| p == "small") {
            phases.push(Kind::WriteSmall);
        }
    }
    let read_phases = env_list("MOAT_BENCH_READ_PHASES").unwrap_or_else(|| vec!["large".into(), "small".into()]);
    if read_phases.iter().any(|p| p == "large") {
        phases.push(Kind::ReadLarge {
            inflight: large_inflight,
        });
    }
    if read_phases.iter().any(|p| p == "small") {
        phases.extend(inflights.iter().map(|&inflight| Kind::ReadSmall { inflight }));
    }
    let shared = Arc::new(SharedState {
        plan: Plan {
            large,
            small,
            write_inflight,
            small_write_inflight,
            write_prefetch: env("MOAT_BENCH_WRITE_PREFETCH", 16),
            shard_reads: std::env::var_os("MOAT_BENCH_SHARD_READS").is_some(),
            wait_reads: std::env::var_os("MOAT_BENCH_WAIT_READS").is_some(),
            large_count,
            small_count,
            phases,
        },
        phase: AtomicUsize::new(NOT_STARTED),
        done: AtomicUsize::new(0),
        stop: AtomicBool::new(false),
        ops: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
        hist: Mutex::new(Hist::new()),
        per_disk_ops: (0..node.engines().len()).map(|_| AtomicU64::new(0)).collect(),
    });
    let pattern_large: Arc<Vec<u8>> = Arc::new((0..large).map(|i| (i % 253) as u8).collect());
    let sums_large = Arc::new(block_checksums(&pattern_large));
    let pattern_small: Arc<Vec<u8>> = Arc::new((0..small).map(|i| (i % 251) as u8).collect());

    let worker_opts: Vec<WorkerOptions> = (0..workers)
        .map(|w| WorkerOptions {
            core: worker_cores[w],
            queue: QueueOptions {
                depth,
                pool: PoolOptions {
                    bytes: pool_mb << 20,
                    max_class: 8 << 20,
                    huge_pages: pool::policy(),
                },
                descriptors: (node.engines().len() * 2 + 8) as u32,
            },
            backend: if sync { QueueBackend::Sync } else { QueueBackend::Uring },
            poll_mode: PollMode::Busy,
        })
        .collect();
    // Hold the phase index at "not started" until every worker is up.
    let handles = node
        .start(&worker_opts, |w| Load {
            shared: shared.clone(),
            seen_phase: None,
            finished_phase: false,
            rng: Rng(0x9e37_79b9_7f4a_7c15 ^ ((w as u64 + 1).wrapping_mul(0x1234_5678_9abc_def1))),
            write_cursor: Vec::new(),
            write_pending: Vec::new(),
            write_outstanding: Vec::new(),
            read_pending: Vec::new(),
            read_free: Vec::new(),
            read_outstanding: 0,
            hist: Hist::new(),
            ops: 0,
            bytes: 0,
            per_disk: Vec::new(),
            pattern_large: pattern_large.clone(),
            sums_large: sums_large.clone(),
            pattern_small: pattern_small.clone(),
        })
        .expect("start workers");

    let disks = node.engines().len();
    for (p, kind) in shared.plan.phases.iter().enumerate() {
        shared.done.store(0, Ordering::Release);
        shared.stop.store(false, Ordering::Release);
        shared.ops.store(0, Ordering::Release);
        shared.bytes.store(0, Ordering::Release);
        for d in &shared.per_disk_ops {
            d.store(0, Ordering::Release);
        }
        *shared.hist.lock().unwrap() = Hist::new();
        let start = Instant::now();
        shared.phase.store(p, Ordering::Release);
        let timed = matches!(kind, Kind::ReadLarge { .. } | Kind::ReadSmall { .. });
        if timed {
            std::thread::sleep(Duration::from_secs(seconds));
            shared.stop.store(true, Ordering::Release);
        }
        while shared.done.load(Ordering::Acquire) < workers {
            std::thread::sleep(Duration::from_millis(1));
        }
        let elapsed = start.elapsed();
        let ops = shared.ops.load(Ordering::Acquire);
        let bytes = shared.bytes.load(Ordering::Acquire);
        let hist = shared.hist.lock().unwrap().clone();
        let per_disk: Vec<u64> = shared.per_disk_ops.iter().map(|d| d.load(Ordering::Acquire)).collect();
        let (min, max) = (
            per_disk.iter().copied().min().unwrap_or(0),
            per_disk.iter().copied().max().unwrap_or(0),
        );
        let label = match kind {
            Kind::WriteLarge => format!("put {} KiB x {large_count}/disk", large >> 10),
            Kind::WriteSmall => format!("put {} KiB x {small_count}/disk", small >> 10),
            Kind::ReadLarge { inflight } => format!("get {} KiB, {inflight}/worker", large >> 10),
            Kind::ReadSmall { inflight } => format!("get {} KiB, {inflight}/worker", small >> 10),
        };
        println!(
            "{label:<30} {:>8.2} GiB/s {:>12.0} ops/s  p50 {:>8.1?} p99 {:>8.1?} p999 {:>8.1?}  ({disks} disks, per-disk ops {min}..{max}, {elapsed:.1?})",
            gib_per_s(bytes, elapsed),
            ops as f64 / elapsed.as_secs_f64(),
            hist.percentile(0.50),
            hist.percentile(0.99),
            hist.percentile(0.999),
        );
        if std::env::var_os("MOAT_BENCH_REPORT_DISKS").is_some() {
            for (disk, count) in per_disk.iter().enumerate() {
                println!(
                    "disk-result phase={p} disk={disk} path={} ops={count} iops={:.0}",
                    selected[disk].path.display(),
                    *count as f64 / elapsed.as_secs_f64(),
                );
            }
        }
    }
    shared.phase.store(shared.plan.phases.len(), Ordering::Release);
    let started = Instant::now();
    for h in handles {
        h.join().expect("worker");
    }
    println!("shutdown (seal + detach) in {:.1?}", started.elapsed());
}
