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

use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, atomic::Ordering, mpsc},
    thread,
};

use moat_common::ChunkId;
use moat_engine::{
    Completion, DeleteOutcome, Engine, IoQueue, Outcome, PutOptions, PutOutcome, ReadCompletion, ReadOutcome, Reader,
    Writer,
};

use crate::{
    DeleteResult, Error, InventoryEntry, Options, Result,
    command::{Command, Fence, FenceReply, Operation, Reply, Waiter},
    delivery::Delivery,
    store::Counters,
};

#[derive(Default)]
struct Chain {
    active: Option<Operation>,
    pending: VecDeque<Operation>,
}

pub(crate) fn spawn(
    disk: usize,
    engine: Engine,
    options: Options,
    receiver: mpsc::Receiver<Command>,
    counters: Arc<Counters>,
) -> Result<(Vec<InventoryEntry>, thread::JoinHandle<()>)> {
    let (ready, wait) = mpsc::sync_channel(1);
    let handle = thread::Builder::new()
        .name(format!("moat-store-{disk}"))
        .spawn(move || {
            let worker = Worker::new(disk, engine, options, receiver, counters);
            match worker {
                Ok(mut worker) => {
                    let _ = ready.send(Ok(worker.inventory()));
                    let result = worker.run();
                    worker.finish(result);
                }
                Err(error) => {
                    let _ = ready.send(Err(error));
                }
            }
        })?;
    match wait.recv().unwrap_or(Err(Error::Closed)) {
        Ok(inventory) => Ok((inventory, handle)),
        Err(error) => {
            let _ = handle.join();
            Err(error)
        }
    }
}

struct Worker {
    delivery: Delivery,
    disk: usize,
    queue: Box<dyn IoQueue>,
    writer: Writer,
    reader: Reader,
    options: Options,
    receiver: mpsc::Receiver<Command>,
    counters: Arc<Counters>,
    chains: HashMap<ChunkId, Chain>,
    ready: VecDeque<ChunkId>,
    reads: HashMap<u64, ChunkId>,
    writes: HashMap<u64, ChunkId>,
    next_read: u64,
    read_completions: Vec<ReadCompletion>,
    write_completions: Vec<Completion>,
    fence: Option<Fence>,
    closing: bool,
    close_reply: Option<Reply<()>>,
    write_error: Option<Error>,
}
impl Worker {
    fn new(
        disk: usize,
        engine: Engine,
        options: Options,
        receiver: mpsc::Receiver<Command>,
        counters: Arc<Counters>,
    ) -> Result<Self> {
        if let Some(&cpu) = options.worker_cpus.get(disk) {
            moat_server::worker::pin_to_core(cpu)?;
        }
        let mut queue = options.queue.build(options.backend)?;
        let writer = engine.writer(queue.as_mut())?;
        let reader = engine.reader(queue.as_mut())?;
        Ok(Self {
            delivery: Delivery::new(options.completion_executor.as_ref()),
            disk,
            queue,
            writer,
            reader,
            options,
            receiver,
            counters,
            chains: HashMap::new(),
            ready: VecDeque::new(),
            reads: HashMap::new(),
            writes: HashMap::new(),
            next_read: 1,
            read_completions: Vec::new(),
            write_completions: Vec::new(),
            fence: None,
            closing: false,
            close_reply: None,
            write_error: None,
        })
    }
    fn inventory(&self) -> Vec<InventoryEntry> {
        let mut entries = Vec::new();
        self.writer.visit_chunks(|id, stat| {
            entries.push(InventoryEntry {
                disk: self.disk,
                id,
                lsn: stat.lsn,
                len: stat.len,
            })
        });
        entries
    }

    fn accept(&mut self, command: Command) {
        let (id, operation) = match command {
            Command::Read {
                id,
                range,
                reply,
                permit,
            } => (
                id,
                Operation::Read {
                    range,
                    lsn: 0,
                    waiters: vec![Waiter {
                        reply,
                        permit: Some(permit),
                    }],
                },
            ),
            Command::Put {
                id,
                value,
                reply,
                permit,
            } => (id, Operation::Put { value, reply, permit }),
            Command::Delete {
                id,
                expected_lsn,
                reply,
                permit,
            } => (
                id,
                Operation::Delete {
                    expected_lsn,
                    reply,
                    permit,
                },
            ),
            Command::Fence { reply, permit } => {
                self.fence = Some(Fence {
                    reply,
                    _permit: permit,
                    ticket: None,
                });
                return;
            }
        };
        let chain = self.chains.entry(id).or_default();
        if let Operation::Read { range, waiters, .. } = &operation {
            let last = if chain.pending.is_empty() {
                chain.active.as_mut()
            } else {
                chain.pending.back_mut()
            };
            if let Some(Operation::Read {
                range: previous,
                waiters: previous_waiters,
                ..
            }) = last
                && previous == range
            {
                debug_assert_eq!(waiters.len(), 1);
                let Operation::Read { waiters, .. } = operation else {
                    unreachable!()
                };
                for mut waiter in waiters {
                    waiter.permit.as_mut().expect("new read credit").resize(0);
                    previous_waiters.push(waiter);
                }
                self.counters.coalesced.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        if chain.active.is_none() && chain.pending.is_empty() {
            self.ready.push_back(id);
        }
        chain.pending.push_back(operation);
    }

    fn disconnected(&mut self) {
        self.fence = Some(Fence {
            reply: FenceReply::Close(None),
            _permit: None,
            ticket: None,
        });
    }

    fn run(&mut self) -> Result<()> {
        loop {
            let mut progress = false;
            if self.fence.is_none() && !self.closing {
                // Bound one packing window so a busy producer cannot starve I/O.
                for _ in 0..128 {
                    match self.receiver.try_recv() {
                        Ok(command) => {
                            self.accept(command);
                            progress = true;
                            if self.fence.is_some() {
                                break;
                            }
                        }
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => {
                            self.disconnected();
                            break;
                        }
                    }
                }
            }
            for _ in 0..self.ready.len() {
                let id = self.ready.pop_front().expect("ready chain");
                let mut chain = self.chains.remove(&id).expect("queued chain");
                let operation = chain.pending.pop_front().expect("queued operation");
                match self.start(id, operation)? {
                    Started::Active(operation) => {
                        chain.active = Some(operation);
                        progress = true;
                    }
                    Started::Retry(operation) => {
                        chain.pending.push_front(operation);
                    }
                    Started::Done => {
                        progress = true;
                    }
                }
                if chain.active.is_none() && !chain.pending.is_empty() {
                    self.ready.push_back(id);
                }
                if chain.active.is_some() || !chain.pending.is_empty() {
                    self.chains.insert(id, chain);
                }
            }
            progress |= self.queue.poll(false)? != 0;
            progress |= self.reader.poll(self.queue.as_mut(), &mut self.read_completions)? != 0;
            progress |= self.writer.poll(self.queue.as_mut(), &mut self.write_completions)? != 0;
            let mut reads = std::mem::take(&mut self.read_completions);
            for completion in reads.drain(..) {
                let id = self.reads.remove(&completion.token).expect("submitted read");
                let operation = self.complete(id);
                self.delivery
                    .push(move || operation.read_done(completion.result.map_err(Error::from)));
            }
            self.read_completions = reads;
            let mut writes = std::mem::take(&mut self.write_completions);
            for completion in writes.drain(..) {
                let result = completion.result.map_err(Error::from);
                if self
                    .fence
                    .as_ref()
                    .is_some_and(|fence| fence.ticket == Some(completion.ticket))
                {
                    self.finish_fence(result)?;
                } else {
                    let id = self.writes.remove(&completion.ticket).expect("submitted mutation");
                    if let Err(error) = &result {
                        self.write_error.get_or_insert(error.clone());
                    }
                    self.complete(id).write_done(result);
                }
            }
            self.write_completions = writes;
            if self.fence.is_some() && self.chains.is_empty() {
                progress |= self.advance_fence()?;
            }
            self.delivery.flush();
            if self.closing && self.writer.is_idle() && self.reader.in_flight() == 0 {
                return self.write_error.take().map_or(Ok(()), Err);
            }
            if !progress {
                if self.options.idle_wait.is_zero() {
                    std::hint::spin_loop();
                } else if self.fence.is_none() && !self.closing {
                    match self.receiver.recv_timeout(self.options.idle_wait) {
                        Ok(command) => self.accept(command),
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => self.disconnected(),
                    }
                } else {
                    thread::sleep(self.options.idle_wait);
                }
            }
        }
    }

    fn start(&mut self, id: ChunkId, mut operation: Operation) -> Result<Started> {
        let result = match &mut operation {
            Operation::Read { range, lsn, waiters } => {
                // Cancellation must release queued requests even while another
                // caller retains every available buffer credit.
                waiters.retain(|waiter| !waiter.reply.is_canceled());
                if waiters.is_empty() {
                    return Ok(Started::Done);
                }
                let Some(stat) = self.reader.stat(&id) else {
                    operation.miss();
                    return Ok(Started::Done);
                };
                *lsn = stat.lsn;
                if !waiters[0]
                    .permit
                    .as_mut()
                    .expect("leader byte credit")
                    .grow(self.options.queue.pool.max_class)
                {
                    return Ok(Started::Retry(operation));
                }
                let token = self.next_read;
                self.next_read = self
                    .next_read
                    .checked_add(1)
                    .ok_or(Error::Invalid("read token space exhausted"))?;
                match self.reader.get(self.queue.as_mut(), &id, range.clone(), token) {
                    Ok(ReadOutcome::Submitted) => {
                        self.reads.insert(token, id);
                        self.counters.reads.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    }
                    Ok(ReadOutcome::Miss) => {
                        operation.miss();
                        return Ok(Started::Done);
                    }
                    Err(error) => Err(error),
                }
            }
            Operation::Put { value, .. } => {
                match self
                    .writer
                    .put(self.queue.as_mut(), id, value, PutOptions { overwrite: true })
                {
                    Ok(PutOutcome::Written { ticket, .. }) => {
                        self.writes.insert(ticket, id);
                        Ok(())
                    }
                    Ok(PutOutcome::Exists) => unreachable!("overwrite enabled"),
                    Err(error) => Err(error),
                }
            }
            Operation::Delete { expected_lsn, .. } => {
                let Some(stat) = self.reader.stat(&id) else {
                    operation.deleted(DeleteResult::Missing);
                    return Ok(Started::Done);
                };
                if expected_lsn.is_some_and(|expected| expected != stat.lsn) {
                    operation.deleted(DeleteResult::Changed);
                    return Ok(Started::Done);
                }
                match self.writer.delete(self.queue.as_mut(), &id) {
                    Ok(DeleteOutcome::Deleted { ticket, .. }) => {
                        self.writes.insert(ticket, id);
                        Ok(())
                    }
                    Ok(DeleteOutcome::Missing) => {
                        operation.deleted(DeleteResult::Missing);
                        return Ok(Started::Done);
                    }
                    Err(error) => Err(error),
                }
            }
        };
        match result {
            Ok(()) => Ok(Started::Active(operation)),
            Err(moat_engine::Error::Busy) => Ok(Started::Retry(operation)),
            Err(error) => {
                let error = Error::from(error);
                if !matches!(operation, Operation::Read { .. }) {
                    self.write_error.get_or_insert(error.clone());
                }
                operation.fail(error);
                Ok(Started::Done)
            }
        }
    }

    fn complete(&mut self, id: ChunkId) -> Operation {
        let chain = self.chains.get_mut(&id).expect("in-flight chain");
        let operation = chain.active.take().expect("active operation");
        if chain.pending.is_empty() {
            self.chains.remove(&id);
        } else {
            self.ready.push_back(id);
        }
        operation
    }

    fn advance_fence(&mut self) -> Result<bool> {
        let fence = self.fence.as_ref().expect("fence");
        if fence.ticket.is_some() {
            return Ok(false);
        }
        let ticket = match fence.reply {
            FenceReply::Inventory(_) => {
                let inventory = self.inventory();
                let fence = self.fence.take().expect("fence");
                let FenceReply::Inventory(reply) = fence.reply else {
                    unreachable!()
                };
                self.delivery.push(move || {
                    drop(fence._permit);
                    let _ = reply.send(Ok(inventory));
                });
                return Ok(true);
            }
            FenceReply::Flush(_) => self.writer.flush(self.queue.as_mut()).map(Some),
            FenceReply::Close(_) => self.writer.seal(self.queue.as_mut()).map(Some),
            FenceReply::Reclaim(_) => self.writer.reclaim(self.queue.as_mut()),
        };
        match ticket {
            Ok(Some(ticket)) => {
                self.fence.as_mut().expect("fence").ticket = Some(ticket);
                Ok(true)
            }
            Ok(None) => {
                let fence = self.fence.take().expect("fence");
                let FenceReply::Reclaim(reply) = fence.reply else {
                    unreachable!()
                };
                self.delivery.push(move || {
                    drop(fence._permit);
                    let _ = reply.send(Ok(None));
                });
                Ok(true)
            }
            Err(moat_engine::Error::Busy) => Ok(false),
            Err(error) => {
                self.finish_fence(Err(error.into()))?;
                Ok(true)
            }
        }
    }

    fn finish_fence(&mut self, result: Result<Outcome>) -> Result<()> {
        let Fence {
            reply, _permit: permit, ..
        } = self.fence.take().expect("completed fence");
        match reply {
            FenceReply::Flush(reply) => {
                let result = result.map(|_| ()).and(self.write_error.take().map_or(Ok(()), Err));
                self.delivery.push(move || {
                    drop(permit);
                    let _ = reply.send(result);
                });
            }
            FenceReply::Close(reply) => {
                drop(permit);
                self.close_reply = reply;
                self.closing = true;
                result?;
            }
            FenceReply::Reclaim(reply) => {
                let result = result.and_then(|outcome| match outcome {
                    Outcome::Reclaim(report) => Ok(Some(report)),
                    _ => Err(Error::Invalid("unexpected reclaim completion")),
                });
                self.delivery.push(move || {
                    drop(permit);
                    let _ = reply.send(result);
                });
            }
            FenceReply::Inventory(_) => unreachable!("inventory has no engine ticket"),
        }
        Ok(())
    }

    fn finish(self, result: Result<()>) {
        let Self {
            mut delivery,
            mut queue,
            writer,
            reader,
            receiver,
            chains,
            fence,
            close_reply,
            ..
        } = self;
        writer.detach(queue.as_mut());
        reader.detach(queue.as_mut());
        drop(queue);
        let error = result.clone().err().unwrap_or(Error::Closed);
        for (_, chain) in chains {
            if let Some(operation) = chain.active {
                let error = error.clone();
                delivery.push(move || operation.fail(error));
            }
            for operation in chain.pending {
                let error = error.clone();
                delivery.push(move || operation.fail(error));
            }
        }
        if let Some(fence) = fence {
            let error = error.clone();
            delivery.push(move || {
                drop(fence._permit);
                fence.reply.fail(error);
            });
        }
        for command in receiver.try_iter() {
            let error = error.clone();
            delivery.push(move || command.fail(error));
        }
        if let Some(reply) = close_reply {
            delivery.push(move || {
                let _ = reply.send(result);
            });
        }
    }
}
enum Started {
    Active(Operation),
    Retry(Operation),
    Done,
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::atomic::AtomicBool,
        time::{Duration, Instant},
    };

    use futures_channel::oneshot;
    use futures_executor::block_on;
    use moat_common::{BufferPool, HugePages, PoolOptions, PooledBuf};
    use moat_engine::{
        Descriptor, Device, FormatOptions, MemDevice, QueueBackend, QueueOptions, blocking,
        io::{Full, IoCompletion},
    };

    use super::*;
    use crate::{Request, budget::Budget};

    // Hold completed reads in the queue's inbox while the worker keeps running.
    // This tests overlap without depending on device timing or scheduler luck.
    struct HoldReads {
        inner: Box<dyn IoQueue>,
        reader: Option<Descriptor>,
        release: Arc<AtomicBool>,
    }
    impl IoQueue for HoldReads {
        fn pool(&self) -> &Arc<BufferPool> {
            self.inner.pool()
        }
        fn attach(&mut self, device: &Arc<dyn Device>) -> io::Result<Descriptor> {
            self.inner.attach(device)
        }
        fn detach(&mut self, desc: Descriptor) {
            self.inner.detach(desc);
        }
        fn read(
            &mut self,
            desc: Descriptor,
            buf: PooledBuf,
            len: usize,
            offset: u64,
            token: u64,
        ) -> std::result::Result<(), PooledBuf> {
            self.reader = Some(desc);
            self.inner.read(desc, buf, len, offset, token)
        }
        fn write(
            &mut self,
            desc: Descriptor,
            buf: PooledBuf,
            len: usize,
            offset: u64,
            token: u64,
        ) -> std::result::Result<(), PooledBuf> {
            self.inner.write(desc, buf, len, offset, token)
        }
        fn fsync(&mut self, desc: Descriptor, token: u64) -> std::result::Result<(), Full> {
            self.inner.fsync(desc, token)
        }
        fn submit(&mut self) -> io::Result<()> {
            self.inner.submit()
        }
        fn poll(&mut self, wait: bool) -> io::Result<usize> {
            self.inner.poll(wait)
        }
        fn take(&mut self, desc: Descriptor, out: &mut Vec<IoCompletion>) -> usize {
            if self.reader == Some(desc) && !self.release.load(Ordering::Acquire) {
                0
            } else {
                self.inner.take(desc, out)
            }
        }
        fn in_flight(&self) -> usize {
            self.inner.in_flight()
        }
        fn depth(&self) -> usize {
            self.inner.depth()
        }
    }

    fn wait_until(mut test: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !test() {
            assert!(Instant::now() < deadline, "worker failed to make progress");
            thread::yield_now();
        }
    }

    #[test]
    fn independent_ids_overlap_and_inflight_followers_share_the_read() {
        let device = Arc::new(MemDevice::new(17 << 20));
        moat_engine::format(
            &*device,
            &FormatOptions {
                segment_size: 1 << 20,
                chunk_max: 128 << 10,
                disk_uuid: [42; 16],
            },
        )
        .unwrap();
        let engine = moat_engine::open(
            device,
            moat_engine::Options {
                index_capacity: 128,
                ..Default::default()
            },
        )
        .unwrap()
        .0;
        let options = Options {
            backend: QueueBackend::Sync,
            queue: QueueOptions {
                depth: 8,
                descriptors: 4,
                pool: PoolOptions {
                    bytes: 32 << 20,
                    max_class: 1 << 20,
                    huge_pages: HugePages::Disabled,
                },
            },
            ..Default::default()
        };
        let budget = Budget::new(32, 32 << 20, 16 << 20, 1, 1 << 20);
        let counters = Arc::new(Counters::default());
        let release = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = mpsc::channel();
        let (ready, started) = mpsc::channel();
        let handle = thread::spawn({
            let counters = counters.clone();
            let release = release.clone();
            move || {
                let mut worker = Worker::new(0, engine, options, receiver, counters).unwrap();
                for n in [1, 2] {
                    worker
                        .writer
                        .put(
                            worker.queue.as_mut(),
                            ChunkId::from_u128(n),
                            &[n as u8],
                            PutOptions::default(),
                        )
                        .unwrap();
                }
                blocking::flush(worker.queue.as_mut(), &mut worker.writer).unwrap();
                worker.queue = Box::new(HoldReads {
                    inner: worker.queue,
                    reader: None,
                    release,
                });
                ready.send(()).unwrap();
                let result = worker.run();
                worker.finish(result);
            }
        });
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let read = |n| {
            let (reply, receiver) = oneshot::channel();
            sender
                .send(Command::Read {
                    id: ChunkId::from_u128(n),
                    range: None,
                    reply,
                    permit: budget.reserve(1 << 20, Some(0)).unwrap(),
                })
                .unwrap();
            Request { receiver }
        };
        let first = read(1);
        wait_until(|| counters.reads.load(Ordering::Relaxed) == 1);
        let other = read(2);
        wait_until(|| counters.reads.load(Ordering::Relaxed) == 2);
        let follower = read(1);
        wait_until(|| counters.coalesced.load(Ordering::Relaxed) == 1);
        assert_eq!(counters.reads.load(Ordering::Relaxed), 2);
        release.store(true, Ordering::Release);
        let first = block_on(first).unwrap().unwrap();
        let other = block_on(other).unwrap().unwrap();
        let follower = block_on(follower).unwrap().unwrap();
        assert!(Arc::ptr_eq(&first, &follower));
        assert_eq!(&**first, &[1]);
        assert_eq!(&**other, &[2]);
        drop(first);
        drop(other);
        drop(follower);
        let (reply, receiver) = oneshot::channel();
        sender
            .send(Command::Fence {
                reply: FenceReply::Close(Some(reply)),
                permit: None,
            })
            .unwrap();
        block_on(Request { receiver }).unwrap();
        handle.join().unwrap();
        assert_eq!(budget.snapshot().bytes, 0);
        assert_eq!(budget.snapshot().requests, 0);
    }
}
