<p align="center">
    <img src="https://raw.githubusercontent.com/mrcroxx/moat/main/etc/logo/slogan.svg" />
</p>

# moat

A chunkserver built for machines with many large NVMe drives.

***Work in progress. The on-disk format and APIs are not stable yet.***

moat stores immutable, variable-length chunks (from a few bytes up to a
configurable maximum, 4 MiB by default) on raw NVMe devices, with one
independent log-structured engine per disk, an RDMA data path, and a client
library that stripes arbitrarily large objects across disks and nodes. It is
designed to be correct first, fast second, and as small as those two allow.

The design document lives at [`docs/design/chunkserver.md`](docs/design/chunkserver.md).

## Status

| Crate | Purpose | State |
|---|---|---|
| [`moat-common`](core/moat-common) | Chunk identifiers, CRC32C block checksums, page alignment, huge-page arenas and the buddy buffer pool | usable |
| [`moat-engine`](core/moat-engine) | Single-disk engine: segments, index, reclaim/eviction, recovery; io_uring with registered buffers, zero-copy read and write paths | usable on raw devices, files and in memory |
| `moat-transport` | RDMA (verbs) and TCP transports behind one protocol | planned |
| [`moat-server`](core/moat-server) | Multi-disk node: NVMe discovery, placement, recovery and workers | usable without a network transport |
| `moat-client` | Node routing, connection management, large-object striping | planned |
| `moat-tools` | `format`, `fsck`, `dump`, `bench` | planned |

## Trying the engine

```rust
use std::sync::Arc;
use moat_common::ChunkId;
use moat_engine::{
    FileDevice, FormatOptions, Options, PutOptions, QueueBackend, QueueOptions, blocking,
};

let device = Arc::new(FileDevice::create("disk.img", 64 << 30, /* direct */ false)?);
moat_engine::format(&*device, &FormatOptions::default())?;

let (engine, _) = moat_engine::open(device, Options::default())?;
let mut queue = QueueOptions::default().build(QueueBackend::Auto)?;
let mut writer = engine.writer(queue.as_mut())?;
let mut reader = engine.reader(queue.as_mut())?;

let id = ChunkId::from_u128(1);
writer.put(queue.as_mut(), id, b"hello", PutOptions::default())?;
blocking::flush(queue.as_mut(), &mut writer)?;
assert_eq!(
    blocking::get(queue.as_mut(), &mut reader, &id, None)?.as_deref(),
    Some(&b"hello"[..]),
);
blocking::seal(queue.as_mut(), &mut writer)?;
writer.detach(queue.as_mut());
reader.detach(queue.as_mut());
```

On Linux, each worker owns an io_uring queue and registered buffers. Its readers
and writers share that queue across disks; each disk has a single writer.
Values move between buffers and the device without copies
(`Writer::prepare_large` / `put_large` for writes,
`ChunkData` for reads). `cargo bench -p moat-engine` measures throughput and
latency on a file or, with `MOAT_BENCH_DEVICE`, a raw device (which it
**formats**).

`cargo test --workspace` runs the unit tests plus the engine's crash-injection,
reclaim and randomized model tests against an in-memory device.

Server-side payload CRC verification is disabled by default
(`Options::verify_reads = false`). Set it to `true` to verify the record header
and every 64 KiB checksum block touched by each read. Unchecked reads cover
only the requested pages. Checksum generation and recovery/reclaim validation
are unchanged.
Client-side transport and verification are not implemented yet.

## macOS 本地开发

macOS 使用普通文件和同步 I/O 后端，用于功能开发与测试。可以直接运行：

```sh
cargo run -p moat-engine --example local
cargo test --workspace
```

示例在临时文件中写入、读取并重新打开一个 4 KiB chunk，退出后自动清理。
`QueueOptions::build(QueueBackend::Auto)` 在 Linux 选择 io_uring，在 macOS
选择 `SyncQueue`；可用 `QueueBackend::resolve()` 查看选择结果。Linux 上的
初始化错误会直接返回。显式指定 `Uring` 在 macOS 上返回 `Unsupported`；
`MemDevice` 在 Linux 上需要显式指定 `Sync`。

同步后端在调用线程执行阻塞 I/O，然后通过相同的 `poll`/completion 接口交付
结果，不作为性能路径。Reader、Writer 和磁盘格式不随后端改变。
macOS 使用 `FileDevice::create/open(..., false)`；显式请求 direct I/O、
CPU 绑核或强制 huge pages 会返回 `Unsupported`。默认 huge-page 策略降级为
普通映射，worker 默认使用 `Auto`、不绑核并在空闲时休眠。
NVMe 自动发现仍仅支持 Linux；macOS 调用方显式提供文件设备。

## Benchmarking

Run the engine benchmark without additional configuration to use a temporary
4 GiB file. The benchmark enables `O_DIRECT` when the backing filesystem
supports it and falls back to buffered I/O otherwise.

```sh
cargo bench -p moat-engine
```

To benchmark a block device, pass its path explicitly:

```sh
MOAT_BENCH_DEVICE=/path/to/block-device \
MOAT_BENCH_BYTES=$((64 << 30)) \
cargo bench -p moat-engine
```

**The benchmark formats `MOAT_BENCH_DEVICE` and destroys data on it. Never use
a system disk or a device containing data you need.** Device paths are examples
only and must not be committed as project configuration.

The workload can be adjusted with the following environment variables:

| Variable | Purpose | Default |
|---|---|---|
| `MOAT_BENCH_BYTES` | Bytes exercised by the benchmark | 4 GiB |
| `MOAT_BENCH_READERS` | Concurrent reader threads | 1 |
| `MOAT_BENCH_LARGE` | Large-value size in bytes | 1 MiB |
| `MOAT_BENCH_SMALL` | Small-value size in bytes | 4 KiB |
| `MOAT_BENCH_VERIFY` | Enable CRC verification on every read in the engine and node benchmarks | unset |
| `MOAT_BENCH_SYNC` | Use the blocking queue instead of io_uring when set | unset |

Benchmark results are hardware-specific. Published results should include the
CPU, storage device, kernel, filesystem or raw-device mode, benchmark variables,
and the corresponding `fio` configuration when making comparisons.

## Development

Rust stable (see `rust-version` in `Cargo.toml`). Development tasks follow the
[cargo-xtask](https://github.com/matklad/cargo-xtask) convention and are run
through the `cargo x` alias:

| Command | What it does |
|---|---|
| `cargo x` | The default suite: `tools`, `check`, `test`, `udeps`, `license`, `doc` |
| `cargo x tools [-y]` | Installs the helper tools the other tasks need (`typos`, `taplo`, `cargo-sort`, `cargo-machete`, `cargo-nextest`, `license-eye`) |
| `cargo x check` | Spelling, TOML and Rust formatting (applied in place, nightly rustfmt when available), clippy with warnings denied |
| `cargo x test` | `cargo nextest run` plus doctests |
| `cargo x udeps` | Unused dependencies |
| `cargo x license` | Apache 2.0 header check (`.licenserc.yaml`) |
| `cargo x doc` | `cargo doc` with warnings denied |

`cargo x tools` installs helper binaries under `CARGO_HOME`; when it is unset,
the standard `$HOME/.cargo` location is used. Downloaded archives are kept in a
temporary directory and removed after installation.

Run `cargo x` before opening a pull request; CI runs the same checks.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
