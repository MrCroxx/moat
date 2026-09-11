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

//! Matched disk-cache workloads; raw devices require a checked serial allowlist.

use std::{
    fs,
    hash::{BuildHasherDefault, DefaultHasher},
    io,
    os::{fd::BorrowedFd, unix::fs::FileTypeExt},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use foyer::{DeviceBuilder, HybridCacheBuilder, HybridCachePolicy};
use futures_util::future::join_all;
use hdrhistogram::Histogram;
use moat_cache::identity::{DEFAULT_IDENTITY_VERSION, Fingerprint, Xxh3};
use moat_common::{HugePages, PoolOptions};
use moat_engine::{Device, FileDevice, QueueBackend, QueueOptions};
use moat_server::{Placement, Target};
use serde::{Deserialize, Serialize};

type Hasher = BuildHasherDefault<DefaultHasher>;
type Moat = moat_cache::Cache<Hasher>;
type Foyer = foyer::HybridCache<Vec<u8>, Vec<u8>, Hasher>;
const SEGMENT: u64 = 16 << 20;

#[derive(Clone, Deserialize, Serialize)]
struct Disk {
    path: String,
    serial: String,
    #[serde(default)]
    expected_capacity: Option<u64>,
}
#[derive(Clone, Deserialize, Serialize)]
struct Config {
    host: String,
    #[serde(default)]
    forbidden_serials: Vec<String>,
    engine: String,
    disks: Vec<Disk>,
    bytes_per_disk: u64,
    records_per_disk: usize,
    key_bytes: usize,
    value_bytes: usize,
    #[serde(default)]
    client_levels: Vec<usize>,
    clients: usize,
    runtime_cpus: Vec<usize>,
    io_cpus: Vec<usize>,
    seconds: u64,
    repeats: usize,
    pool_bytes_per_disk: usize,
    #[serde(default)]
    moat_verify_reads: bool,
    #[serde(default)]
    moat_batched_completions: bool,
}

#[derive(Debug)]
struct TokioCompletionExecutor(tokio::runtime::Handle);

impl moat_cache_store::CompletionExecutor for TokioCompletionExecutor {
    fn spawn(&self, task: futures_util::future::BoxFuture<'static, ()>) {
        self.0.spawn(task);
    }
}
fn validate(config: &Config) -> Result<()> {
    ensure!(
        fs::read_to_string("/proc/sys/kernel/hostname")?.trim() == config.host,
        "wrong benchmark host"
    );
    ensure!(
        !config.disks.is_empty() && config.io_cpus.len() == config.disks.len(),
        "invalid disk/CPU list"
    );
    ensure!(
        !config.runtime_cpus.is_empty() && config.clients > 0 && config.repeats > 0,
        "empty runtime/workload"
    );
    ensure!(
        config.key_bytes >= 8 && config.value_bytes >= 16 && config.records_per_disk > 0,
        "invalid record size"
    );
    ensure!(config.bytes_per_disk.is_multiple_of(SEGMENT), "unaligned device window");
    let mut seen = std::collections::HashSet::new();
    for disk in &config.disks {
        let path = fs::canonicalize(&disk.path)?;
        ensure!(seen.insert(path.clone()), "duplicate device");
        if fs::metadata(&path)?.file_type().is_block_device() {
            let name = path
                .file_name()
                .context("device filename")?
                .to_str()
                .context("device UTF-8")?;
            let sys = Path::new("/sys/class/block").join(name);
            ensure!(!disk.serial.is_empty(), "raw device needs a serial allowlist");
            ensure!(
                fs::read_to_string(sys.join("device/serial"))?.trim() == disk.serial,
                "device serial changed"
            );
            ensure!(
                !config.forbidden_serials.is_empty(),
                "raw devices require an explicit system-device serial denylist"
            );
            ensure!(
                !config.forbidden_serials.contains(&disk.serial),
                "forbidden system device"
            );
            ensure!(!sys.join("partition").exists(), "partition is forbidden");
            ensure!(
                fs::read_dir(sys.join("holders"))?.next().is_none(),
                "device has holders"
            );
            for child in fs::read_dir(&sys)? {
                ensure!(!child?.path().join("partition").exists(), "device has partitions");
            }
            let dev = fs::read_to_string(sys.join("dev"))?;
            for mount in fs::read_to_string("/proc/self/mountinfo")?.lines() {
                ensure!(mount.split_whitespace().nth(2) != Some(dev.trim()), "mounted device");
            }
            let capacity = fs::read_to_string(sys.join("size"))?.trim().parse::<u64>()? * 512;
            ensure!(
                capacity >= config.bytes_per_disk && disk.expected_capacity == Some(capacity),
                "device capacity does not match the allowlist"
            );
        } else {
            ensure!(
                disk.serial.is_empty() && fs::metadata(&path)?.is_file(),
                "invalid benchmark file"
            );
            ensure!(
                fs::metadata(&path)?.len() >= config.bytes_per_disk,
                "benchmark file too small"
            );
        }
    }
    Ok(())
}
struct Window {
    inner: FileDevice,
    bytes: u64,
}
impl Device for Window {
    fn capacity(&self) -> u64 {
        self.bytes
    }
    fn read_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        if offset + bytes.len() as u64 > self.bytes {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.inner.read_at(bytes, offset)
    }
    fn write_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        if offset + bytes.len() as u64 > self.bytes {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.inner.write_at(bytes, offset)
    }
    fn sync(&self) -> io::Result<()> {
        self.inner.sync()
    }
    fn fd(&self) -> Option<BorrowedFd<'_>> {
        self.inner.fd()
    }
}
enum Cache {
    Moat(Moat),
    Foyer { shards: Vec<Foyer>, placement: Placement },
}
impl Cache {
    async fn open(c: &Config) -> Result<Self> {
        let targets: Vec<_> = c
            .disks
            .iter()
            .enumerate()
            .map(|(i, _)| Target {
                uuid: [i as u8 + 1; 16],
                weight: c.bytes_per_disk,
            })
            .collect();
        if c.engine == "moat" {
            let mut engines = Vec::new();
            for (disk, target) in c.disks.iter().zip(&targets) {
                let device = Arc::new(Window {
                    inner: FileDevice::open(&disk.path, true)?,
                    bytes: c.bytes_per_disk,
                });
                moat_engine::format(
                    &*device,
                    &moat_engine::FormatOptions {
                        segment_size: SEGMENT,
                        chunk_max: 512 << 10,
                        disk_uuid: target.uuid,
                    },
                )?;
                engines.push(
                    moat_engine::open(
                        device,
                        moat_engine::Options {
                            index_capacity: (c.records_per_disk * 4).max(1024).next_power_of_two(),
                            verify_reads: c.moat_verify_reads,
                            ..Default::default()
                        },
                    )?
                    .0,
                );
            }
            let (store, _) = moat_cache_store::Store::new(
                engines,
                moat_cache_store::Options {
                    completion_executor: c.moat_batched_completions.then(|| {
                        std::sync::Arc::new(TokioCompletionExecutor(tokio::runtime::Handle::current()))
                            as std::sync::Arc<dyn moat_cache_store::CompletionExecutor>
                    }),
                    max_requests: c.clients * 4 + 4096,
                    max_bytes: c.pool_bytes_per_disk * c.disks.len(),
                    backend: QueueBackend::Uring,
                    idle_wait: Duration::ZERO,
                    worker_cpus: c.io_cpus.clone(),
                    queue: QueueOptions {
                        depth: 256,
                        descriptors: 8,
                        pool: PoolOptions {
                            bytes: c.pool_bytes_per_disk,
                            max_class: 1 << 20,
                            huge_pages: HugePages::Disabled,
                        },
                    },
                },
            )?;
            let memory =
                moat_cache_memory::Cache::<moat_cache::Bytes, moat_cache::Bytes, moat_cache::Bytes>::builder(1 << 20)
                    .shards(16)
                    .hash_builder(Hasher::default())
                    .policy(moat_cache_memory::Policy::Fifo)
                    .weigher(|key, value, _| key.len() + value.len())
                    .admission(|_, _, _| false);
            let cache = moat_cache::Cache::new(
                memory,
                store,
                moat_cache::Options {
                    pending_operations: 4096,
                    pending_bytes: 512 << 20,
                    key_leases: c.clients * 4 + 4096,
                    key_bytes: 256 << 20,
                    ..Default::default()
                },
            )
            .await?;
            Ok(Self::Moat(cache))
        } else {
            ensure!(c.engine == "foyer", "unknown engine");
            let mut shards = Vec::new();
            for (i, disk) in c.disks.iter().enumerate() {
                let device = foyer::FileDeviceBuilder::new(&disk.path)
                    .with_capacity(c.bytes_per_disk as usize)
                    .with_direct(true)
                    .build()?;
                let cache = HybridCacheBuilder::new()
                    .with_name(format!("foyer-disk-{i}"))
                    .with_policy(HybridCachePolicy::WriteOnInsertion)
                    .with_flush_on_close(false)
                    .memory((1 << 20) / c.disks.len())
                    .with_shards(16)
                    .with_hash_builder(Hasher::default())
                    .with_eviction_config(foyer::FifoConfig::default())
                    .with_weighter(|key: &Vec<u8>, value: &Vec<u8>| key.len() + value.len())
                    .with_filter(|_, _| false)
                    .storage()
                    .with_io_engine_config(Box::new(
                        foyer::UringIoEngineConfig::new()
                            .with_threads(1)
                            .with_cpus(vec![c.io_cpus[i] as u32])
                            .with_io_depth(256),
                    ) as Box<dyn foyer::IoEngineConfig>)
                    .with_engine_config(
                        foyer::BlockEngineConfig::new(device)
                            .with_block_size(SEGMENT as usize)
                            .with_flushers(2)
                            .with_reclaimers(1)
                            .with_buffer_pool_size(c.pool_bytes_per_disk)
                            .with_submit_queue_size_threshold(c.pool_bytes_per_disk)
                            .with_indexer_shards(64)
                            .with_tombstone_log(false),
                    )
                    .with_recover_mode(foyer::RecoverMode::None)
                    .with_compression(foyer::Compression::None)
                    .build()
                    .await?;
                shards.push(cache);
            }
            Ok(Self::Foyer {
                shards,
                placement: Placement::new(targets),
            })
        }
    }
    fn shard(placement: &Placement, key: &[u8]) -> usize {
        placement
            .disk_of(&Xxh3.identify(&[0; 16], DEFAULT_IDENTITY_VERSION, key))
            .unwrap()
    }
    async fn put(&self, key: moat_cache::Bytes, value: Vec<u8>) -> Result<()> {
        match self {
            Self::Moat(cache) => {
                cache.insert(key, value.into()).await?;
            }
            Self::Foyer { shards, placement } => {
                shards[Self::shard(placement, &key)].insert(key.as_ref().to_vec(), value);
            }
        }
        Ok(())
    }
    async fn drain(&self) -> Result<()> {
        match self {
            Self::Moat(_) => {}
            Self::Foyer { shards, .. } => {
                for cache in shards {
                    cache.storage().wait().await;
                }
            }
        }
        Ok(())
    }
    async fn get(&self, key: &moat_cache::Bytes, expected: usize, len: usize) -> Result<()> {
        fn check(value: &[u8], expected: usize, len: usize) -> Result<()> {
            ensure!(value.len() == len, "wrong value length");
            ensure!(value[..8] == (expected as u64).to_le_bytes(), "wrong value prefix");
            ensure!(
                value[len - 8..] == (!(expected as u64)).to_le_bytes(),
                "wrong value suffix"
            );
            Ok(())
        }
        match self {
            Self::Moat(cache) => {
                let view = cache.get(key).await?.context("unexpected moat disk miss")?;
                ensure!(!view.is_resident(), "unexpected moat memory promotion");
                check(view.value(), expected, len)
            }
            Self::Foyer { shards, placement } => {
                let cache = &shards[Self::shard(placement, key)];
                let entry = cache.get(key.as_ref()).await?.context("unexpected foyer disk miss")?;
                check(entry.value(), expected, len)
            }
        }
    }
    fn assert_disk_only(&self) -> Result<()> {
        match self {
            Self::Moat(cache) => ensure!(
                cache.statistics().memory.resident_weight == 0,
                "memory residency changed"
            ),
            Self::Foyer { shards, .. } => {
                for cache in shards {
                    ensure!(cache.memory().usage() == 0, "foyer memory residency changed");
                }
            }
        }
        Ok(())
    }
    async fn close(&self) -> Result<()> {
        match self {
            Self::Moat(cache) => cache.close().await?,
            Self::Foyer { shards, .. } => {
                for cache in shards {
                    cache.close().await?;
                }
            }
        }
        Ok(())
    }
}
#[derive(Serialize)]
struct Cpu {
    user: f64,
    system: f64,
    max_rss_kib: i64,
}
fn cpu() -> Cpu {
    // SAFETY: getrusage initializes the provided writable rusage structure.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: usage is valid writable storage for the RUSAGE_SELF query.
    assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) }, 0);
    Cpu {
        user: usage.ru_utime.tv_sec as f64 + usage.ru_utime.tv_usec as f64 / 1e6,
        system: usage.ru_stime.tv_sec as f64 + usage.ru_stime.tv_usec as f64 / 1e6,
        max_rss_kib: usage.ru_maxrss,
    }
}
fn diskstats(c: &Config) -> Result<Vec<Vec<u64>>> {
    c.disks
        .iter()
        .map(|d| {
            let name = Path::new(&d.path).file_name().unwrap();
            let path = Path::new("/sys/class/block").join(name).join("stat");
            if !path.exists() {
                return Ok(vec![0; 17]);
            }
            fs::read_to_string(path)?
                .split_whitespace()
                .map(|v| Ok(v.parse()?))
                .collect()
        })
        .collect()
}
#[derive(Serialize)]
struct Report {
    phase: String,
    repeat: usize,
    clients: usize,
    operations: u64,
    seconds: f64,
    ops_per_second: f64,
    logical_bytes_per_second: f64,
    p50_us: f64,
    p99_us: f64,
    p999_us: f64,
    cpu_seconds: f64,
    cpu_cores: f64,
    max_rss_kib: i64,
    disk_delta: Vec<Vec<u64>>,
}
async fn reads(
    cache: Arc<Cache>,
    c: &Config,
    keys: Arc<Vec<moat_cache::Bytes>>,
    repeat: usize,
    seconds: u64,
) -> Result<Report> {
    let barrier = Arc::new(tokio::sync::Barrier::new(c.clients + 1));
    let begin = Instant::now();
    let deadline = begin + Duration::from_secs(seconds);
    let before_cpu = cpu();
    let before_disk = diskstats(c)?;
    let mut jobs = Vec::new();
    for client in 0..c.clients {
        let cache = cache.clone();
        let keys = keys.clone();
        let barrier = barrier.clone();
        let len = c.value_bytes;
        jobs.push(tokio::spawn(async move {
            let mut histogram = Histogram::<u64>::new(3).unwrap();
            let mut n = 0u64;
            let mut rng = (client as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
            barrier.wait().await;
            while Instant::now() < deadline {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let index = (rng as usize) % keys.len();
                let start = Instant::now();
                cache.get(&keys[index], index, len).await?;
                histogram.record(start.elapsed().as_nanos().max(1) as u64)?;
                n += 1;
            }
            Ok::<_, anyhow::Error>((n, histogram))
        }));
    }
    barrier.wait().await;
    let mut count = 0;
    let mut histogram = Histogram::<u64>::new(3)?;
    for result in join_all(jobs).await {
        let (n, h) = result??;
        count += n;
        histogram.add(h)?;
    }
    cache.assert_disk_only()?;
    let elapsed = begin.elapsed().as_secs_f64();
    let after_cpu = cpu();
    let after_disk = diskstats(c)?;
    let cpu_seconds = after_cpu.user + after_cpu.system - before_cpu.user - before_cpu.system;
    Ok(Report {
        phase: "read".into(),
        repeat,
        clients: c.clients,
        operations: count,
        seconds: elapsed,
        ops_per_second: count as f64 / elapsed,
        logical_bytes_per_second: count as f64 * c.value_bytes as f64 / elapsed,
        p50_us: histogram.value_at_quantile(0.5) as f64 / 1000.0,
        p99_us: histogram.value_at_quantile(0.99) as f64 / 1000.0,
        p999_us: histogram.value_at_quantile(0.999) as f64 / 1000.0,
        cpu_seconds,
        cpu_cores: cpu_seconds / elapsed,
        max_rss_kib: after_cpu.max_rss_kib,
        disk_delta: after_disk
            .into_iter()
            .zip(before_disk)
            .map(|(a, b)| a.into_iter().zip(b).map(|(a, b)| a - b).collect())
            .collect(),
    })
}
async fn run(c: Config) -> Result<()> {
    println!("CONFIG {}", serde_json::to_string(&c)?);
    let cache = Arc::new(Cache::open(&c).await?);
    let count = c.records_per_disk * c.disks.len();
    let keys: Arc<Vec<_>> = Arc::new(
        (0..count)
            .map(|i| {
                let mut key = vec![0x5a; c.key_bytes];
                key[..8].copy_from_slice(&(i as u64).to_le_bytes());
                moat_cache::Bytes::from(key)
            })
            .collect(),
    );
    let before_disk = diskstats(&c)?;
    let before_cpu = cpu();
    let start = Instant::now();
    for first in (0..count).step_by(256) {
        let mut jobs = Vec::new();
        for (i, key) in keys.iter().enumerate().take((first + 256).min(count)).skip(first) {
            let mut value = vec![0x7c; c.value_bytes];
            value[..8].copy_from_slice(&(i as u64).to_le_bytes());
            value[c.value_bytes - 8..].copy_from_slice(&(!(i as u64)).to_le_bytes());
            jobs.push(cache.put(key.clone(), value));
        }
        for result in join_all(jobs).await {
            result?;
        }
        cache.drain().await?;
    }
    // Both implementations have completed their writes; use an identical device sync boundary.
    for disk in &c.disks {
        FileDevice::open(&disk.path, true)?.sync()?;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let after_cpu = cpu();
    let after_disk = diskstats(&c)?;
    println!(
        "PREFILL {}",
        serde_json::json!({"operations":count, "seconds":elapsed,
        "ops_per_second":count as f64/elapsed,"logical_bytes_per_second":count as f64*c.value_bytes as f64/elapsed,
        "cpu_cores":(after_cpu.user+after_cpu.system-before_cpu.user-before_cpu.system)/elapsed,
        "disk_before":before_disk,"disk_after":after_disk})
    );
    // Cache inserts may reject asynchronously. Require a complete disk dataset
    // before measuring hits, so dropped prefill writes never improve a result.
    for first in (0..count).step_by(256) {
        let jobs = keys
            .iter()
            .enumerate()
            .take((first + 256).min(count))
            .skip(first)
            .map(|(i, key)| cache.get(key, i, c.value_bytes));
        for result in join_all(jobs).await {
            result?;
        }
    }
    cache.assert_disk_only()?;
    println!("VERIFIED {count}");
    let levels = if c.client_levels.is_empty() {
        vec![c.clients]
    } else {
        c.client_levels.clone()
    };
    for clients in levels {
        ensure!(clients <= c.clients, "client level exceeds reserved request budget");
        let mut level = c.clone();
        level.clients = clients;
        let _ = reads(cache.clone(), &level, keys.clone(), 0, 2).await?;
        for repeat in 1..=c.repeats {
            let report = reads(cache.clone(), &level, keys.clone(), repeat, c.seconds).await?;
            println!("RESULT {}", serde_json::to_string(&report)?);
        }
    }
    cache.close().await?;
    Ok(())
}
fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .context("usage: moat-cache-disk-compare CONFIG.json")?;
    let config: Config = serde_json::from_slice(&fs::read(path)?)?;
    validate(&config)?;
    let cpus = config.runtime_cpus.clone();
    let next = AtomicUsize::new(0);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpus.len())
        .thread_name("cache-bench-app")
        .enable_all()
        .on_thread_start(move || {
            let core = cpus[next.fetch_add(1, Ordering::Relaxed) % cpus.len()];
            moat_server::worker::pin_to_core(core).expect("runtime CPU affinity");
        })
        .build()?;
    runtime.block_on(run(config))
}
