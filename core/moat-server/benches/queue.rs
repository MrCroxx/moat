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

//! Read-only isolation of the queue and checksum costs, without engine metadata.
//!
//! Reads existing data; never formats or writes. Select available NVMe data
//! disks automatically, or provide MOAT_BENCH_DISKS. Knobs: WORKERS (80),
//! CORES (online CPUs after the first two), DEPTH (1024), INFLIGHT (128),
//! SECONDS (10), IO_BYTES (4096), SPAN (16 GiB), OFFSET (1 GiB), all prefixed
//! with MOAT_BENCH_. SHARD_READS assigns one disk per worker. CHECKSUM performs
//! the same per-64-KiB CRC work as an engine read but has no expected digest;
//! this is a cost isolation experiment, not an integrity verification test.
//! WAIT_READS waits for a completion instead of busy polling. CRC_PASSES (1)
//! repeats the CRC work on each returned block to isolate arithmetic cost.
//! MEMORY_ONLY requires CHECKSUM and repeatedly checks initialized pool buffers
//! without issuing I/O; its throughput is memory processing, not disk I/O.
//! CRC_SOURCE (io, hot, cold) selects the CRC input: the returned I/O buffer,
//! a reused hot buffer, or a 128 MiB rotation per worker. The latter two are
//! synthetic controls and never verify the returned I/O data. MEMORY_ONLY
//! requires CRC_SOURCE=io; any non-io source requires CHECKSUM.
//! Default cores are spread across shared caches, with SMT siblings last.
//! POOL_MB (512) sets each worker's pool size. HUGE_PAGES accepts disabled,
//! preferred (default), or required. EXPECT_BACKING accepts plain, thp, 2m,
//! or 1g and rejects unexpected arena backing before timing; thp still needs
//! a smaps audit to confirm promotion. REPORT_POOL prints arena backing.
//! TOUCH_STRIDE (0, disabled) reads one byte per stride after each completed
//! I/O, independently of CHECKSUM, to isolate cache-line access costs.

#[cfg(target_os = "linux")]
#[path = "support/cpus.rs"]
mod cpus;

#[cfg(target_os = "linux")]
#[path = "support/pool.rs"]
mod pool;

#[cfg(target_os = "linux")]
use moat_common::{PoolOptions, crc32c};
#[cfg(target_os = "linux")]
use moat_engine::{Device, FileDevice, IoQueue, QueueOptions};
#[cfg(target_os = "linux")]
use moat_server::{disk, worker::pin_to_core};
#[cfg(target_os = "linux")]
use std::{
    hint::black_box,
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

#[cfg(target_os = "linux")]
fn main() {
    let available = disk::discover().unwrap();
    let selected: Vec<_> = match std::env::var("MOAT_BENCH_DISKS") {
        Ok(paths) => paths
            .split(',')
            .map(|path| {
                let found = available
                    .iter()
                    .find(|d| d.path.to_string_lossy() == path)
                    .expect("known NVMe disk");
                assert!(found.is_available(), "disk is in use");
                found.clone()
            })
            .collect(),
        Err(_) => available
            .into_iter()
            .filter(|d| d.is_available() && d.capacity >= 1_000_000_000_000)
            .collect(),
    };
    assert!(!selected.is_empty());
    let cores = std::env::var("MOAT_BENCH_CORES")
        .map(|s| disk::parse_cpu_list(&s))
        .unwrap_or_else(|_| cpus::spread());
    let workers = env("MOAT_BENCH_WORKERS", 80) as usize;
    assert!(workers > 0 && workers <= cores.len());
    let bytes = env("MOAT_BENCH_IO_BYTES", 4096) as usize;
    let span = env("MOAT_BENCH_SPAN", 16 << 30);
    let offset = env("MOAT_BENCH_OFFSET", 1 << 30);
    let inflight = env("MOAT_BENCH_INFLIGHT", 128) as usize;
    let duration = Duration::from_secs(env("MOAT_BENCH_SECONDS", 10));
    let shard = std::env::var_os("MOAT_BENCH_SHARD_READS").is_some();
    let crc_source = std::env::var("MOAT_BENCH_CRC_SOURCE").unwrap_or_else(|_| "io".into());
    assert!(["io", "hot", "cold"].contains(&crc_source.as_str()));
    let control_bytes = match crc_source.as_str() {
        "hot" => bytes,
        "cold" => 128 << 20,
        _ => 0,
    };
    let memory_only = std::env::var_os("MOAT_BENCH_MEMORY_ONLY").is_some();
    let crc_passes = env("MOAT_BENCH_CRC_PASSES", 1);
    let checksum = std::env::var_os("MOAT_BENCH_CHECKSUM").is_some();
    let touch_stride = env("MOAT_BENCH_TOUCH_STRIDE", 0) as usize;
    let wait = std::env::var_os("MOAT_BENCH_WAIT_READS").is_some();
    assert!(crc_passes > 0 && (!memory_only || checksum));
    assert!(crc_source == "io" || (checksum && !memory_only));
    assert!(bytes > 0 && bytes.is_multiple_of(4096) && offset.is_multiple_of(4096));
    assert!(span >= bytes as u64 && !duration.is_zero() && inflight > 0);
    let devices: Vec<Arc<dyn Device>> = selected
        .iter()
        .map(|d| {
            assert!(offset.checked_add(span).unwrap() <= d.capacity);
            Arc::new(FileDevice::open(&d.path, true).unwrap()) as Arc<dyn Device>
        })
        .collect();
    let options = QueueOptions {
        depth: env("MOAT_BENCH_DEPTH", 1024) as u32,
        descriptors: devices.len() as u32,
        pool: PoolOptions {
            bytes: (env("MOAT_BENCH_POOL_MB", 512) as usize) << 20,
            max_class: 8 << 20,
            huge_pages: pool::policy(),
        },
    };
    assert!(bytes <= options.pool.max_class && inflight <= options.depth as usize);
    let barrier = Arc::new(Barrier::new(workers));
    let reports = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|worker| {
                let barrier = barrier.clone();
                let devices = &devices;
                let core = cores[worker];
                let crc_source = &crc_source;
                scope.spawn(move || {
                    pin_to_core(core).unwrap();
                    let mut q = moat_engine::uring::UringQueue::new(&options).unwrap_or_else(|error| {
                        // Peers may already be waiting at the startup barrier.
                        eprintln!("queue setup failed on worker {worker}: {error}");
                        std::process::exit(1);
                    });
                    pool::inspect(q.pool(), worker);
                    let descs: Vec<_> = devices.iter().map(|d| q.attach(d).unwrap()).collect();
                    let mut rng = 0x9e37_79b9_7f4a_7c15u64 ^ (worker as u64 + 1).wrapping_mul(0x1234_5678_9abc_def1);
                    let mut done = Vec::new();
                    let mut count = 0u64;
                    let mut polls = 0u64;
                    let mut empty_polls = 0u64;
                    let mut outstanding_sum = 0u64;
                    let control: Vec<_> = (0..control_bytes / bytes)
                        .map(|_| {
                            let mut buf = q.pool().alloc(bytes).unwrap();
                            buf[..bytes].fill(0x5a);
                            buf
                        })
                        .collect();
                    let memory: Vec<_> = if memory_only {
                        (0..inflight)
                            .map(|_| {
                                let mut buf = q.pool().alloc(bytes).expect("memory control fits pool");
                                buf[..bytes].fill(0x5a);
                                buf
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                    barrier.wait();
                    let start = Instant::now();
                    loop {
                        let stop = start.elapsed() >= duration;
                        if memory_only {
                            if stop {
                                break;
                            }
                            for buf in &memory {
                                check(&buf[..bytes], crc_passes);
                                count += 1;
                            }
                            continue;
                        }
                        while !stop && q.in_flight() < inflight {
                            let Some(buf) = q.pool().alloc(bytes) else { break };
                            rng ^= rng << 13;
                            rng ^= rng >> 7;
                            rng ^= rng << 17;
                            let d = if shard {
                                worker % descs.len()
                            } else {
                                (rng % descs.len() as u64) as usize
                            };
                            let at = offset + ((rng >> 20) % (span / bytes as u64)) * bytes as u64;
                            q.read(descs[d], buf, bytes, at, 0).unwrap();
                        }
                        if stop && q.in_flight() == 0 {
                            break;
                        }
                        outstanding_sum += q.in_flight() as u64;
                        let completed = q.poll(wait).unwrap();
                        polls += 1;
                        empty_polls += u64::from(completed == 0);
                        for desc in &descs {
                            q.take(*desc, &mut done);
                        }
                        for completion in done.drain(..) {
                            assert_eq!(completion.result.unwrap(), bytes);
                            let buf = completion.buf.unwrap();
                            if touch_stride > 0 {
                                for at in (0..bytes).step_by(touch_stride) {
                                    black_box(buf[at]);
                                }
                            }
                            if checksum {
                                if crc_source == "io" {
                                    check(&buf[..bytes], crc_passes);
                                } else {
                                    check(&control[count as usize % control.len()][..bytes], crc_passes);
                                }
                            }
                            count += 1;
                        }
                    }
                    let elapsed = start.elapsed();
                    for desc in descs {
                        q.detach(desc);
                    }
                    (count, elapsed, polls, empty_polls, outstanding_sum)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect::<Vec<_>>()
    });
    let total: u64 = reports.iter().map(|r| r.0).sum();
    let elapsed = reports.iter().map(|r| r.1).max().unwrap().as_secs_f64();
    if memory_only {
        println!(
            "memory workers={workers} bytes={bytes} buffers={inflight} crc_passes={crc_passes} crc_source={crc_source}: {:.3} GB/s of payload, no device I/O",
            total as f64 * bytes as f64 / elapsed / 1e9
        );
        return;
    }
    let polls: u64 = reports.iter().map(|r| r.2).sum();
    let empty: u64 = reports.iter().map(|r| r.3).sum();
    let outstanding: u64 = reports.iter().map(|r| r.4).sum();
    println!(
        "queue disks={} workers={workers} bytes={bytes} inflight={inflight} shard={shard} checksum={checksum} wait={wait} crc_passes={crc_passes} crc_source={crc_source} touch_stride={touch_stride}: {:.3} GB/s {:.0} IOPS, avg outstanding {:.1}, {:.1}% empty polls, {:.1} completions/poll",
        devices.len(),
        total as f64 * bytes as f64 / elapsed / 1e9,
        total as f64 / elapsed,
        outstanding as f64 / polls as f64,
        empty as f64 * 100.0 / polls as f64,
        total as f64 / polls as f64
    );
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("The io_uring queue benchmark requires Linux.");
}

#[cfg(target_os = "linux")]
fn check(data: &[u8], passes: u64) {
    for block in data.chunks(64 << 10) {
        for _ in 0..passes {
            black_box(crc32c(black_box(block)));
        }
    }
}
