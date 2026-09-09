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

//! A single-disk, log-structured chunk engine.
//!
//! One engine instance manages one block device (an NVMe namespace, a file, or
//! an in-memory buffer). It stores immutable, variable-length chunks of up to
//! a configured maximum size, identified by an opaque 128-bit
//! [`ChunkId`](moat_common::ChunkId).
//!
//! # Design in one paragraph
//!
//! The device is divided into fixed-size *segments*. Records are appended to at
//! most two active segments (hot for foreground writes, cold for records
//! relocated by reclaim) in self-describing, checksummed *batches*. When a
//! segment fills up it is sealed with a *footer* listing every record, so
//! recovery reads footers instead of data. A lock-free in-memory hash *index*
//! maps every chunk to its newest record. Deletes append *tombstones*. Space is
//! reclaimed one segment at a time by relocating what is still live and freeing
//! the segment; the same mechanism implements cache eviction under a different
//! policy. There is no separate write-ahead log and no embedded key-value
//! store: the log is the only source of truth.
//!
//! # Queues, engines and pipelines
//!
//! A worker thread owns one [`IoQueue`] (io_uring on Linux: one ring, one
//! registered buffer pool, one fixed-file table) and drives any number of
//! engines through it. An [`Engine`] is a disk's shared state; a [`Writer`] or
//! a [`Reader`] is a pipeline of that disk attached to one queue. Exactly one
//! writer exists per disk (all mutations, sealing and reclaim go through it);
//! any number of readers may exist, one per worker in practice. With io_uring, no pipeline call
//! blocks: every method does memory work, enqueues I/O and returns a ticket,
//! or reports [`Error::Busy`] when the pool is out of buffers. Completions are
//! observed only through `poll`. Values move between pool buffers and the
//! device without copies.
//! On macOS, [`QueueOptions::build`] selects a synchronous development backend
//! for [`QueueBackend::Auto`]. It uses the same pipelines and completion API,
//! but performs blocking device I/O on the caller's thread.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//! use moat_common::{ChunkId, HugePages, PoolOptions};
//! use moat_engine::{
//!     FormatOptions, MemDevice, Options, PutOptions, PutOutcome, QueueOptions, blocking,
//!     io::{CompletionOrder, SyncQueue},
//! };
//!
//! let device = Arc::new(MemDevice::new(16 << 20));
//! moat_engine::format(&*device, &FormatOptions { segment_size: 1 << 20, chunk_max: 128 << 10, ..Default::default() })?;
//! let (engine, _report) = moat_engine::open(device, Options::default())?;
//!
//! // The worker owns the queue; the engine's pipelines attach to it.
//! let opts = QueueOptions {
//!     pool: PoolOptions { bytes: 8 << 20, max_class: 1 << 20, huge_pages: HugePages::Disabled },
//!     ..Default::default()
//! };
//! let mut q = SyncQueue::new(&opts, CompletionOrder::Fifo)?;
//! let mut writer = engine.writer(&mut q)?;
//! let mut reader = engine.reader(&mut q)?;
//!
//! let id = ChunkId::from_u128(42);
//! let PutOutcome::Written { ticket, .. } = writer.put(&mut q, id, b"hello", PutOptions::default())? else {
//!     unreachable!()
//! };
//! blocking::wait(&mut q, &mut writer, ticket)?;
//! assert_eq!(blocking::get(&mut q, &mut reader, &id, None)?.as_deref(), Some(&b"hello"[..]));
//! writer.detach(&mut q);
//! reader.detach(&mut q);
//! # Ok::<(), moat_engine::Error>(())
//! ```

pub mod blocking;
mod codec;
mod device;
mod engine;
mod error;
mod index;
pub mod io;
pub mod layout;
mod options;
mod reader;
mod scan;
mod segments;
mod shared;
#[cfg(target_os = "linux")]
pub mod uring;
mod writer;

pub use device::{Device, FileDevice, MemDevice};
pub use engine::{ChunkStat, Engine, RecoveryReport, Usage, format, open};
pub use error::{Error, Result};
pub use index::{FLAG_ACCESSED, FLAG_FRAMED, FLAG_LARGE, IndexValue, Location, MAX_READERS};
pub use io::{Descriptor, IoQueue, QueueBackend, QueueOptions};
pub use options::{FormatOptions, Options};
pub use reader::{ChunkData, ReadCompletion, ReadOutcome, Reader};
pub use writer::{
    Completion, DeleteOutcome, LargeValue, Lsn, Outcome, PutOptions, PutOutcome, ReclaimPolicy, ReclaimReport, Ticket,
    Writer,
};
