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

//! Asynchronous, batched I/O queues shared by every engine on a thread.
//!
//! A worker thread owns exactly one [`IoQueue`] and drives any number of
//! engine pipelines through it. A pipeline *attaches* a device to the queue and
//! receives a [`Descriptor`]: one open of that device on this queue, naming
//! both the registered file the operations go to and the inbox its completions
//! are routed to. Buffers are *moved* into the queue for the duration of an
//! operation and handed back with the completion, so ownership is always
//! unambiguous and no lifetime crosses the submission boundary.
//!
//! Enqueueing never blocks and never touches the device. When `depth`
//! operations are already in flight the operation is rejected *losslessly*: the
//! buffer comes back to the caller, who keeps the operation in its own ready
//! queue and retries after the next poll. [`IoQueue::vacant`] tells a caller
//! exactly how many operations it can enqueue without a rejection.
//!
//! Two implementations exist: io_uring with registered fixed buffers and a
//! fixed-file table ([`UringQueue`](crate::uring::UringQueue), Linux, the
//! production path) and [`SyncQueue`], which performs each operation
//! immediately through the blocking [`Device`] interface and merely defers the
//! completion. The sync queue keeps the engine portable and deterministic under
//! test; every piece of engine logic is identical on both.

use std::{io, sync::Arc};

use moat_common::{BufferPool, PoolOptions, PooledBuf};

use crate::device::Device;

/// Configuration of one [`IoQueue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueOptions {
    /// Maximum operations in flight.
    pub depth: u32,
    /// The buffer pool owned by the queue.
    pub pool: PoolOptions,
    /// Maximum number of attached descriptors.
    pub descriptors: u32,
}

impl Default for QueueOptions {
    fn default() -> Self {
        Self {
            depth: 256,
            pool: PoolOptions::default(),
            descriptors: 256,
        }
    }
}

/// One open of a device on a queue: a registered-file slot plus a completion
/// inbox. Scoped to the queue that issued it; not the kernel's file descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Descriptor(pub(crate) u32);

impl Descriptor {
    /// The slot index within the queue.
    #[inline]
    pub fn index(self) -> u32 {
        self.0
    }
}

/// The queue has `depth` operations in flight; retry after a poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Full;

/// A finished operation. `buf` is the buffer the operation was issued with
/// (absent for `fsync`).
pub struct IoCompletion {
    /// The token passed at submission.
    pub token: u64,
    /// Bytes transferred, or the OS error.
    pub result: io::Result<usize>,
    /// The buffer handed back to the caller.
    pub buf: Option<PooledBuf>,
}

/// An asynchronous I/O queue owned by one thread and shared by the pipelines
/// on it. See the [module docs](self).
pub trait IoQueue: Send {
    /// The pool every buffer passed to this queue must come from.
    fn pool(&self) -> &Arc<BufferPool>;

    /// Registers `device` and opens an inbox for it.
    fn attach(&mut self, device: &Arc<dyn Device>) -> io::Result<Descriptor>;

    /// Closes a descriptor. Completions still in flight for it are discarded
    /// on arrival (their buffers return to the pool).
    fn detach(&mut self, desc: Descriptor);

    /// Enqueues a read of `len` bytes at `offset` into the start of `buf`.
    /// Rejected, with the buffer handed back untouched, when `depth`
    /// operations are in flight.
    fn read(&mut self, desc: Descriptor, buf: PooledBuf, len: usize, offset: u64, token: u64) -> Result<(), PooledBuf>;

    /// Enqueues a write of the first `len` bytes of `buf` at `offset`.
    /// Rejected, with the buffer handed back untouched, when `depth`
    /// operations are in flight.
    fn write(&mut self, desc: Descriptor, buf: PooledBuf, len: usize, offset: u64, token: u64)
    -> Result<(), PooledBuf>;

    /// Enqueues a flush of the device's volatile write cache. Ordering against
    /// in-flight writes is the caller's responsibility (wait for them first).
    fn fsync(&mut self, desc: Descriptor, token: u64) -> Result<(), Full>;

    /// `depth() - in_flight()`: how many operations can be enqueued right now
    /// without being rejected.
    fn vacant(&self) -> usize {
        self.depth() - self.in_flight()
    }

    /// Pushes enqueued operations to the device without waiting.
    fn submit(&mut self) -> io::Result<()>;

    /// Submits, then reaps every finished operation into its descriptor's
    /// inbox. With `wait` set and anything in flight, blocks until at least
    /// one completes. Returns the number reaped. This is a worker's single
    /// blocking point.
    fn poll(&mut self, wait: bool) -> io::Result<usize>;

    /// Moves the inbox of `desc` into `out`. Returns the number moved.
    fn take(&mut self, desc: Descriptor, out: &mut Vec<IoCompletion>) -> usize;

    /// Operations enqueued but not yet reaped.
    fn in_flight(&self) -> usize;

    /// Maximum operations in flight.
    fn depth(&self) -> usize;
}

/// Completion inboxes indexed by descriptor, shared by both queue
/// implementations.
pub(crate) struct Inboxes {
    boxes: Vec<Option<Vec<IoCompletion>>>,
}

impl Inboxes {
    pub(crate) fn new(capacity: u32) -> Self {
        Self {
            boxes: (0..capacity.max(1)).map(|_| None).collect(),
        }
    }

    /// Opens the lowest free inbox.
    pub(crate) fn open(&mut self) -> Option<Descriptor> {
        let slot = self.boxes.iter().position(Option::is_none)?;
        self.boxes[slot] = Some(Vec::new());
        Some(Descriptor(slot as u32))
    }

    pub(crate) fn close(&mut self, desc: Descriptor) {
        if let Some(b) = self.boxes.get_mut(desc.0 as usize) {
            *b = None;
        }
    }

    pub(crate) fn is_open(&self, desc: Descriptor) -> bool {
        self.boxes.get(desc.0 as usize).is_some_and(Option::is_some)
    }

    /// Delivers a completion; completions for closed descriptors are dropped.
    pub(crate) fn deliver(&mut self, desc: Descriptor, done: IoCompletion) {
        if let Some(Some(inbox)) = self.boxes.get_mut(desc.0 as usize) {
            inbox.push(done);
        }
    }

    pub(crate) fn take(&mut self, desc: Descriptor, out: &mut Vec<IoCompletion>) -> usize {
        let Some(Some(inbox)) = self.boxes.get_mut(desc.0 as usize) else {
            return 0;
        };
        let n = inbox.len();
        if n == 0 {
            return 0;
        }
        if out.is_empty() {
            std::mem::swap(inbox, out);
        } else {
            out.append(inbox);
        }
        n
    }
}

/// How a [`SyncQueue`] orders its deferred completions. Reverse order is a
/// test aid: it exercises every consumer's handling of out-of-order
/// completion, which io_uring produces routinely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompletionOrder {
    /// Completions are reported in submission order.
    #[default]
    Fifo,
    /// Completions are reported in reverse submission order per poll.
    Reverse,
}

/// An [`IoQueue`] that performs each operation synchronously at submission
/// through [`Device::read_at`] / [`Device::write_at`] and reports the
/// completion on the next poll.
pub struct SyncQueue {
    pool: Arc<BufferPool>,
    depth: usize,
    devices: Vec<Option<Arc<dyn Device>>>,
    inboxes: Inboxes,
    /// Operations performed but not yet reaped, in submission order.
    done: Vec<(Descriptor, IoCompletion)>,
    order: CompletionOrder,
}

impl SyncQueue {
    /// Creates a queue with a fresh pool owned by the calling thread.
    pub fn new(opts: &QueueOptions, order: CompletionOrder) -> io::Result<Self> {
        Ok(Self {
            pool: BufferPool::new(opts.pool)?,
            depth: opts.depth.max(1) as usize,
            devices: (0..opts.descriptors.max(1)).map(|_| None).collect(),
            inboxes: Inboxes::new(opts.descriptors),
            done: Vec::new(),
            order,
        })
    }

    fn device(&self, desc: Descriptor) -> &Arc<dyn Device> {
        self.devices
            .get(desc.0 as usize)
            .and_then(Option::as_ref)
            .expect("operation on a detached descriptor")
    }
}

impl IoQueue for SyncQueue {
    fn pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    fn attach(&mut self, device: &Arc<dyn Device>) -> io::Result<Descriptor> {
        let desc = self
            .inboxes
            .open()
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "no free descriptor"))?;
        self.devices[desc.0 as usize] = Some(device.clone());
        Ok(desc)
    }

    fn detach(&mut self, desc: Descriptor) {
        self.inboxes.close(desc);
        if let Some(d) = self.devices.get_mut(desc.0 as usize) {
            *d = None;
        }
    }

    fn read(
        &mut self,
        desc: Descriptor,
        mut buf: PooledBuf,
        len: usize,
        offset: u64,
        token: u64,
    ) -> Result<(), PooledBuf> {
        if self.done.len() >= self.depth {
            return Err(buf);
        }
        let result = self.device(desc).read_at(&mut buf[..len], offset).map(|()| len);
        self.done.push((
            desc,
            IoCompletion {
                token,
                result,
                buf: Some(buf),
            },
        ));
        Ok(())
    }

    fn write(
        &mut self,
        desc: Descriptor,
        buf: PooledBuf,
        len: usize,
        offset: u64,
        token: u64,
    ) -> Result<(), PooledBuf> {
        if self.done.len() >= self.depth {
            return Err(buf);
        }
        let result = self.device(desc).write_at(&buf[..len], offset).map(|()| len);
        self.done.push((
            desc,
            IoCompletion {
                token,
                result,
                buf: Some(buf),
            },
        ));
        Ok(())
    }

    fn fsync(&mut self, desc: Descriptor, token: u64) -> Result<(), Full> {
        if self.done.len() >= self.depth {
            return Err(Full);
        }
        let result = self.device(desc).sync().map(|()| 0);
        self.done.push((
            desc,
            IoCompletion {
                token,
                result,
                buf: None,
            },
        ));
        Ok(())
    }

    fn submit(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn poll(&mut self, _wait: bool) -> io::Result<usize> {
        let n = self.done.len();
        let done = std::mem::take(&mut self.done);
        match self.order {
            CompletionOrder::Fifo => done.into_iter().for_each(|(d, c)| self.inboxes.deliver(d, c)),
            CompletionOrder::Reverse => done.into_iter().rev().for_each(|(d, c)| self.inboxes.deliver(d, c)),
        }
        Ok(n)
    }

    fn take(&mut self, desc: Descriptor, out: &mut Vec<IoCompletion>) -> usize {
        self.inboxes.take(desc, out)
    }

    fn in_flight(&self) -> usize {
        self.done.len()
    }

    fn depth(&self) -> usize {
        self.depth
    }
}
