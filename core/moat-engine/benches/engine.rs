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

//! Single-engine throughput and latency on a real file or block device.
//!
//! ```sh
//! cargo bench -p moat-engine
//! MOAT_BENCH_DEVICE=/path/to/block-device MOAT_BENCH_BYTES=$((64<<30)) cargo bench -p moat-engine
//! ```
//!
//! Without `MOAT_BENCH_DEVICE` a 4 GiB file is created in the temp directory,
//! opened with `O_DIRECT` when the filesystem allows it. The device is
//! formatted: **never point this at a device holding data you need.**
//!
//! Knobs: `MOAT_BENCH_BYTES` (device bytes to use; a quarter goes to each
//! workload), `MOAT_BENCH_READERS` (reader threads, each with its own queue),
//! `MOAT_BENCH_INFLIGHT` (comma-separated outstanding small reads per thread;
//! each value is one run), `MOAT_BENCH_DEPTH` (queue depth, at least the
//! largest in-flight count), `MOAT_BENCH_LARGE` / `MOAT_BENCH_SMALL` (value
//! sizes), `MOAT_BENCH_SYNC` (use the blocking queue instead of io_uring),
//! `MOAT_BENCH_REOPEN` (do not format or write; reopen the device from a
//! previous run and go straight to the read phases, for profiling),
//! `MOAT_BENCH_SECONDS` (duration per read phase, default 10; zero uses
//! the original fixed request counts), `MOAT_BENCH_LARGE_INFLIGHT`
//! (outstanding large reads per thread, default 16),
//! `MOAT_BENCH_WRITE_BATCH` (puts between completion polls, default 64),
//! `MOAT_BENCH_READ_PHASES` (comma-separated large,small,latency; all by default),
//! `MOAT_BENCH_VERIFY` (enable header and value checksum verification on reads).

use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use moat_common::{ChunkId, HugePages, PoolOptions, block_checksums};
use moat_engine::{
    Engine, Error, FileDevice, FormatOptions, IoQueue, Options, PutOptions, PutOutcome, QueueOptions, ReadOutcome,
    Reader, Writer, blocking,
    io::{CompletionOrder, SyncQueue},
};

const SEGMENT: u64 = 256 << 20;
const CHUNK_MAX: u32 = 4 << 20;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn queue_options() -> QueueOptions {
    QueueOptions {
        depth: env("MOAT_BENCH_DEPTH", 256) as u32,
        pool: PoolOptions {
            bytes: 512 << 20,
            max_class: 8 << 20,
            huge_pages: HugePages::Preferred,
        },
        descriptors: 4,
    }
}

/// Builds the queue for the calling thread.
fn queue() -> Box<dyn IoQueue> {
    let opts = queue_options();
    if std::env::var_os("MOAT_BENCH_SYNC").is_some() {
        return Box::new(SyncQueue::new(&opts, CompletionOrder::Fifo).unwrap());
    }
    #[cfg(target_os = "linux")]
    {
        Box::new(moat_engine::uring::UringQueue::new(&opts).unwrap())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Box::new(SyncQueue::new(&opts, CompletionOrder::Fifo).unwrap())
    }
}

fn gib_per_s(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / elapsed.as_secs_f64() / (1u64 << 30) as f64
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn id(n: u64) -> ChunkId {
    ChunkId::from_u128(n as u128)
}

/// Writes `count` values of `len` bytes with the pipeline kept full,
/// returning aggregate throughput. Large values go through the zero-copy path.
fn bench_puts(q: &mut dyn IoQueue, writer: &mut Writer, first_id: u64, count: u64, len: usize, label: &str) {
    let pattern: Vec<u8> = (0..len).map(|i| (i % 253) as u8).collect();
    let sums = block_checksums(&pattern);
    let start = Instant::now();
    let mut done = Vec::new();
    let mut acked = 0u64;
    let batch = env("MOAT_BENCH_WRITE_BATCH", 64);
    assert!(batch > 0);
    let mut reap = |q: &mut dyn IoQueue, writer: &mut Writer, wait: bool| {
        q.poll(wait).unwrap();
        writer.poll(q, &mut done).unwrap();
        for c in done.drain(..) {
            c.result.unwrap();
            acked += 1;
        }
    };
    for i in 0..count {
        loop {
            let outcome = if len >= 64 << 10 {
                match writer.prepare_large(q, len as u32) {
                    Ok(mut large) => {
                        large.value_mut().copy_from_slice(&pattern);
                        writer.put_large(q, id(first_id + i), large, Some(&sums), PutOptions::default())
                    }
                    Err(e) => Err(e),
                }
            } else {
                writer.put(q, id(first_id + i), &pattern, PutOptions::default())
            };
            match outcome {
                Ok(o) => {
                    assert!(matches!(o, PutOutcome::Written { .. }));
                    break;
                }
                // The pool is the back-pressure signal: reap and retry.
                Err(Error::Busy) => reap(q, writer, true),
                Err(e) => panic!("{e}"),
            }
        }
        // Polling also closes the writer's partial batch. Give small puts a
        // useful packing window rather than flushing every few records.
        if (i + 1).is_multiple_of(batch) {
            reap(q, writer, false);
        }
    }
    let ticket = writer.flush(q).unwrap();
    blocking::wait_with(q, writer, ticket, &mut done).unwrap();
    for c in done.drain(..) {
        c.result.unwrap();
        acked += 1;
    }
    assert_eq!(acked, count, "every accepted put must complete successfully");
    let elapsed = start.elapsed();
    black_box(acked);
    println!(
        "{label:<34} {:>8.2} GiB/s {:>10.0} ops/s  ({count} x {len} B in {:.2?})",
        gib_per_s(count * len as u64, elapsed),
        count as f64 / elapsed.as_secs_f64(),
        elapsed
    );
}

/// Random reads with `inflight` outstanding on one queue. Returns the number
/// of reads done and the sampled latencies.
#[allow(clippy::too_many_arguments)]
fn read_loop(
    q: &mut dyn IoQueue,
    reader: &mut Reader,
    first_id: u64,
    count: u64,
    len: usize,
    inflight: usize,
    total: u64,
    seed: u64,
    duration: Duration,
) -> (u64, Vec<Duration>, Duration) {
    assert!(inflight > 0 && count > 0);
    let mut rng = seed | 1;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut latencies = Vec::with_capacity(1 << 20);
    let mut started: Vec<Option<Instant>> = vec![None; inflight];
    let mut out = Vec::new();
    let mut issued = 0u64;
    let mut completed = 0u64;
    let mut free_slots: Vec<usize> = (0..inflight).collect();
    let start = Instant::now();
    let mut stop = false;
    loop {
        if !duration.is_zero() && start.elapsed() >= duration {
            stop = true;
        }
        while !stop
            && (!duration.is_zero() || issued < total)
            && let Some(slot) = free_slots.pop()
        {
            let key = id(first_id + next() % count);
            // Timestamps cost a vdso call each; sample one request in 16.
            started[slot] = issued.is_multiple_of(16).then(Instant::now);
            match reader.get(q, &key, None, slot as u64) {
                Ok(ReadOutcome::Submitted) => issued += 1,
                Ok(ReadOutcome::Miss) => panic!("missing key"),
                Err(Error::Busy) => {
                    free_slots.push(slot);
                    break;
                }
                Err(e) => panic!("{e}"),
            }
        }
        if completed == issued && (stop || duration.is_zero() && issued == total) {
            break;
        }
        q.poll(true).unwrap();
        reader.poll(q, &mut out).unwrap();
        for c in out.drain(..) {
            let slot = c.token as usize;
            let data = c.result.unwrap();
            assert_eq!(data.len(), len);
            black_box(&*data);
            if let Some(t) = started[slot].take() {
                latencies.push(t.elapsed());
            }
            free_slots.push(slot);
            completed += 1;
        }
    }
    (completed, latencies, start.elapsed())
}

/// Random reads spread over `readers` threads, each with its own queue and
/// `inflight` outstanding requests.
#[allow(clippy::too_many_arguments)]
fn bench_reads(
    engine: &Engine,
    readers: usize,
    first_id: u64,
    count: u64,
    len: usize,
    inflight: usize,
    total: u64,
    label: &str,
) {
    assert!(readers > 0 && total >= readers as u64);
    let per_thread = total / readers as u64;
    let duration = Duration::from_secs(env("MOAT_BENCH_SECONDS", 10));
    let barrier = Arc::new(std::sync::Barrier::new(readers + 1));
    let handles: Vec<_> = (0..readers)
        .map(|t| {
            let engine = engine.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                // Queues (and their pools) are created before the clock
                // starts: mapping and registering a pool pins and clears
                // hundreds of megabytes, which is start-up cost.
                let mut q = queue();
                let mut reader = engine.reader(&mut *q).unwrap();
                barrier.wait();
                let done = read_loop(
                    &mut *q,
                    &mut reader,
                    first_id,
                    count,
                    len,
                    inflight,
                    per_thread + u64::from((t as u64) < total % readers as u64),
                    0x9e37_79b9 + t as u64,
                    duration,
                );
                reader.detach(&mut *q);
                done
            })
        })
        .collect();
    barrier.wait();
    let mut done = 0u64;
    let mut elapsed = Duration::ZERO;
    let mut latencies: Vec<Duration> = Vec::new();
    for h in handles {
        let (n, l, took) = h.join().unwrap();
        done += n;
        elapsed = elapsed.max(took);
        latencies.extend(l);
    }
    latencies.sort();
    println!(
        "{label:<34} {:>8.2} GiB/s {:>10.0} ops/s  p50 {:>8.1?} p99 {:>8.1?} p999 {:>8.1?}  ({done} ops in {elapsed:.2?})",
        gib_per_s(done * len as u64, elapsed),
        done as f64 / elapsed.as_secs_f64(),
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.99),
        percentile(&latencies, 0.999),
    );
}

fn main() {
    let bytes: u64 = env("MOAT_BENCH_BYTES", 4 << 30);
    let dir = tempfile::tempdir().unwrap();
    let device = match std::env::var("MOAT_BENCH_DEVICE") {
        Ok(path) => FileDevice::open(&path, true).expect("open device"),
        Err(_) => {
            let path = dir.path().join("bench.img");
            FileDevice::create(&path, bytes, true)
                .or_else(|_| FileDevice::create(&path, bytes, false))
                .unwrap()
        }
    };
    let device = Arc::new(device);
    let reopen = std::env::var_os("MOAT_BENCH_REOPEN").is_some();
    if !reopen {
        moat_engine::format(
            &*device,
            &FormatOptions {
                segment_size: SEGMENT,
                chunk_max: CHUNK_MAX,
                disk_uuid: [7; 16],
            },
        )
        .unwrap();
    }
    let (engine, _) = moat_engine::open(
        device,
        Options {
            index_capacity: 4 << 20,
            verify_reads: std::env::var_os("MOAT_BENCH_VERIFY").is_some(),
            ..Default::default()
        },
    )
    .unwrap();
    let mut q = queue();
    let mut writer = engine.writer(&mut *q).unwrap();
    let readers = env("MOAT_BENCH_READERS", 1) as usize;
    let large = env("MOAT_BENCH_LARGE", 1 << 20) as usize;
    let small = env("MOAT_BENCH_SMALL", 4 << 10) as usize;
    let large_inflight = env("MOAT_BENCH_LARGE_INFLIGHT", 16) as usize;
    assert!(readers > 0 && large > 0 && small > 0 && large_inflight > 0);
    let phases = std::env::var("MOAT_BENCH_READ_PHASES").unwrap_or_else(|_| "large,small,latency".into());
    let phase = |name| phases.split(',').any(|p| p.trim() == name);

    // Budget: roughly a quarter of the device per size class so nothing
    // triggers reclaim during the measurement.
    let budget = bytes / 4;
    let large_count = budget / large as u64;
    let small_count = (budget / small as u64).min(2_000_000);

    println!(
        "device {} GiB, segment {} MiB, chunk max {} MiB, {readers} reader thread(s), io_uring={}, verify_reads={}",
        bytes >> 30,
        SEGMENT >> 20,
        CHUNK_MAX >> 20,
        std::env::var_os("MOAT_BENCH_SYNC").is_none(),
        std::env::var_os("MOAT_BENCH_VERIFY").is_some()
    );
    if !reopen {
        bench_puts(
            &mut *q,
            &mut writer,
            0,
            large_count,
            large,
            &format!("put {} KiB (includes copy)", large >> 10),
        );
        bench_puts(
            &mut *q,
            &mut writer,
            1 << 40,
            small_count,
            small,
            &format!("put {} KiB (packed)", small >> 10),
        );
    }
    if phase("large") {
        bench_reads(
            &engine,
            readers,
            0,
            large_count,
            large,
            large_inflight,
            large_count.min(16_384),
            &format!("get {} KiB, {large_inflight} in flight/thread", large >> 10),
        );
    }
    let inflights: Vec<usize> = std::env::var("MOAT_BENCH_INFLIGHT")
        .ok()
        .map(|s| s.split(',').filter_map(|v| v.trim().parse().ok()).collect())
        .unwrap_or_else(|| vec![64]);
    for inflight in inflights.into_iter().filter(|_| phase("small")) {
        bench_reads(
            &engine,
            readers,
            1 << 40,
            small_count,
            small,
            inflight,
            small_count.min(2_000_000),
            &format!("get {} KiB, {inflight} in flight/thread", small >> 10),
        );
    }
    if phase("latency") {
        bench_reads(
            &engine,
            1,
            1 << 40,
            small_count,
            small,
            1,
            50_000,
            &format!("get {} KiB, 1 in flight, 1 thread", small >> 10),
        );
    }
    blocking::seal(&mut *q, &mut writer).unwrap();
    blocking::drain(&mut *q, &mut writer).unwrap();
    writer.detach(&mut *q);
}
