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

//! Worker threads.
//!
//! A node runs one kind of worker. Every worker is pinned to a core and owns
//! exactly one [`IoQueue`] (io_uring, one registered buffer pool, one
//! fixed-file table) through which it drives every disk: it holds a
//! [`Reader`] for each disk, so a read is served on the worker that receives
//! the request, and a [`Writer`] for each disk it *owns*, so each disk's log
//! has a single writer. The loop is the same on every worker; owning a disk
//! only means more `Writer` calls on it.
//!
//! What the worker does not know is where requests come from. That is the
//! [`Handler`]: the network reactor in a server, a load generator in a
//! benchmark, a script in a test. It runs once per loop iteration on the
//! worker thread, sees the completions the queue delivered since the last
//! iteration, and issues new operations through the [`Context`]. Nothing here
//! blocks except the queue's own wait, and only when the worker has nothing
//! else to do.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use moat_engine::{
    Completion, Engine, IoQueue, QueueOptions, ReadCompletion, Reader, Writer, blocking,
    io::{CompletionOrder, SyncQueue},
};

/// Index of a disk in a node's disk list.
pub type DiskId = usize;

/// How the worker behaves when it has nothing to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollMode {
    /// Spin: the lowest latency, one core per worker.
    Busy,
    /// Wait for I/O when some is in flight; sleep `idle_sleep` otherwise. For
    /// deployments that cannot dedicate cores.
    Adaptive {
        /// How long to sleep when neither I/O nor requests are pending.
        idle_sleep: Duration,
    },
}

/// Which [`IoQueue`] implementation a worker builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueBackend {
    /// io_uring (Linux). Devices must expose a file descriptor.
    Uring,
    /// The blocking queue; works with any device, used by tests and tools.
    Sync,
}

/// Configuration of one worker.
#[derive(Debug, Clone)]
pub struct WorkerOptions {
    /// Core to pin the thread to; `None` leaves scheduling to the OS.
    pub core: Option<usize>,
    /// The worker's queue: depth, pool, descriptor table.
    pub queue: QueueOptions,
    /// Queue implementation.
    pub backend: QueueBackend,
    /// Idle behaviour.
    pub poll_mode: PollMode,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            core: None,
            queue: QueueOptions::default(),
            backend: QueueBackend::Uring,
            poll_mode: PollMode::Busy,
        }
    }
}

/// What a [`Handler::run`] call reports back to the loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Work was done or is pending; loop again at once.
    Continue,
    /// Nothing to do until I/O completes or new requests arrive.
    Idle,
    /// Shut the worker down after this iteration.
    Stop,
}

/// One disk as seen from a worker: its engine, this worker's read pipeline,
/// and the write pipeline if this worker owns the disk.
pub struct DiskSlot {
    /// The disk.
    pub engine: Engine,
    /// This worker's reader for the disk.
    pub reader: Reader,
    /// The disk's writer, present on its owner only.
    pub writer: Option<Writer>,
}

/// What a [`Handler`] sees on each iteration.
pub struct Context<'a> {
    /// Index of this worker in the node.
    pub worker: usize,
    /// The worker's queue; pass it to every engine call.
    pub queue: &'a mut dyn IoQueue,
    /// Every disk, indexed by [`DiskId`].
    pub disks: &'a mut [DiskSlot],
    /// Read completions delivered since the last iteration, tagged by disk.
    /// The handler drains them.
    pub reads: &'a mut Vec<(DiskId, ReadCompletion)>,
    /// Write completions delivered since the last iteration, tagged by disk.
    /// The handler drains them.
    pub writes: &'a mut Vec<(DiskId, Completion)>,
}

impl Context<'_> {
    /// Whether this worker owns `disk` (holds its writer).
    pub fn owns(&self, disk: DiskId) -> bool {
        self.disks[disk].writer.is_some()
    }

    /// The queue and the slot of `disk`, borrowed together so the handler can
    /// call `slot.reader.get(queue, ..)` or `slot.writer.put(queue, ..)`.
    pub fn disk(&mut self, disk: DiskId) -> (&mut dyn IoQueue, &mut DiskSlot) {
        (self.queue, &mut self.disks[disk])
    }
}

/// The request source of a worker. See the [module docs](self).
pub trait Handler: Send + 'static {
    /// Called once on the worker thread before the loop starts, with every
    /// pipeline attached.
    fn start(&mut self, _cx: &mut Context<'_>) {}

    /// Called once per loop iteration, after completions were collected.
    fn run(&mut self, cx: &mut Context<'_>) -> Step;

    /// Called once on the worker thread after the loop ends, before the
    /// writers are sealed and detached.
    fn stop(&mut self, _cx: &mut Context<'_>) {}
}

/// Errors from a worker thread.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// The queue or a pipeline could not be set up, or the queue failed.
    #[error("worker {worker}: {source}")]
    Io {
        /// The worker index.
        worker: usize,
        /// The cause.
        #[source]
        source: io::Error,
    },
    /// An engine call failed during setup or shutdown.
    #[error("worker {worker}: {source}")]
    Engine {
        /// The worker index.
        worker: usize,
        /// The cause.
        #[source]
        source: moat_engine::Error,
    },
    /// The handler panicked; the worker thread is gone.
    #[error("worker {0} panicked")]
    Panicked(usize),
}

/// A running worker thread.
pub struct Worker<H: Handler> {
    index: usize,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<H, WorkerError>>>,
}

impl<H: Handler> Worker<H> {
    /// Starts worker `index`: pins it, builds its queue, attaches a reader for
    /// every engine in `disks` and a writer for those flagged as owned, then
    /// runs `handler`. Returns once the pipelines are attached, so setup
    /// errors are reported here rather than at `join`.
    pub fn spawn(
        index: usize,
        opts: WorkerOptions,
        disks: Vec<(Engine, bool)>,
        handler: H,
    ) -> Result<Self, WorkerError> {
        let stop = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), WorkerError>>();
        let stop_flag = stop.clone();
        let thread = thread::Builder::new()
            .name(format!("moat-worker-{index}"))
            .spawn(move || run_worker(index, opts, disks, handler, stop_flag, ready_tx))
            .map_err(|source| WorkerError::Io { worker: index, source })?;
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(Self {
                index,
                stop,
                thread: Some(thread),
            }),
            Ok(Err(e)) => {
                let _ = thread.join();
                Err(e)
            }
            Err(_) => {
                let _ = thread.join();
                Err(WorkerError::Panicked(index))
            }
        }
    }

    /// The worker's index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Asks the worker to stop after its current iteration.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    /// Waits for the worker to finish and returns its handler.
    pub fn join(mut self) -> Result<H, WorkerError> {
        let thread = self.thread.take().expect("joined once");
        thread.join().map_err(|_| WorkerError::Panicked(self.index))?
    }
}

impl<H: Handler> Drop for Worker<H> {
    fn drop(&mut self) {
        if let Some(t) = self.thread.take() {
            self.stop.store(true, Ordering::Release);
            let _ = t.join();
        }
    }
}

/// Pins the calling thread to `core`.
pub fn pin_to_core(core: usize) -> io::Result<()> {
    // SAFETY: a zeroed cpu_set_t is a valid empty set; CPU_SET writes within
    // its bounds for any core below CPU_SETSIZE, which we check.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if core >= libc::CPU_SETSIZE as usize {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "core out of range"));
        }
        libc::CPU_SET(core, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn build_queue(opts: &WorkerOptions) -> io::Result<Box<dyn IoQueue>> {
    match opts.backend {
        QueueBackend::Sync => Ok(Box::new(SyncQueue::new(&opts.queue, CompletionOrder::Fifo)?)),
        #[cfg(target_os = "linux")]
        QueueBackend::Uring => Ok(Box::new(moat_engine::uring::UringQueue::new(&opts.queue)?)),
        #[cfg(not(target_os = "linux"))]
        QueueBackend::Uring => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring is only available on Linux",
        )),
    }
}

fn run_worker<H: Handler>(
    index: usize,
    opts: WorkerOptions,
    disks: Vec<(Engine, bool)>,
    mut handler: H,
    stop: Arc<AtomicBool>,
    ready: mpsc::Sender<Result<(), WorkerError>>,
) -> Result<H, WorkerError> {
    let io_err = |source| WorkerError::Io { worker: index, source };
    let engine_err = |source| WorkerError::Engine { worker: index, source };

    let setup = || -> Result<(Box<dyn IoQueue>, Vec<DiskSlot>), WorkerError> {
        if let Some(core) = opts.core {
            pin_to_core(core).map_err(io_err)?;
        }
        let mut queue = build_queue(&opts).map_err(io_err)?;
        let mut slots = Vec::with_capacity(disks.len());
        for (engine, owner) in disks {
            let reader = engine.reader(&mut *queue).map_err(engine_err)?;
            let writer = if owner {
                Some(engine.writer(&mut *queue).map_err(engine_err)?)
            } else {
                None
            };
            slots.push(DiskSlot { engine, reader, writer });
        }
        Ok((queue, slots))
    };
    let (mut queue, mut slots) = match setup() {
        Ok(v) => v,
        Err(e) => {
            let _ = ready.send(Err(e));
            return Err(WorkerError::Panicked(index));
        }
    };
    let _ = ready.send(Ok(()));

    let mut reads = Vec::new();
    let mut writes = Vec::new();
    let mut read_scratch = Vec::new();
    let mut write_scratch = Vec::new();
    {
        let mut cx = Context {
            worker: index,
            queue: &mut *queue,
            disks: &mut slots,
            reads: &mut reads,
            writes: &mut writes,
        };
        handler.start(&mut cx);

        let mut idle = false;
        loop {
            // One `io_uring_enter` per iteration for every disk on this
            // worker. In busy mode never wait; in adaptive mode wait when the
            // handler had nothing to do (the queue returns at once if nothing
            // is in flight).
            let wait = idle && matches!(opts.poll_mode, PollMode::Adaptive { .. });
            cx.queue.poll(wait).map_err(io_err)?;
            for (d, slot) in cx.disks.iter_mut().enumerate() {
                slot.reader.poll(cx.queue, &mut read_scratch).map_err(engine_err)?;
                cx.reads.extend(read_scratch.drain(..).map(|c| (d, c)));
                if let Some(w) = &mut slot.writer {
                    w.poll(cx.queue, &mut write_scratch).map_err(engine_err)?;
                    cx.writes.extend(write_scratch.drain(..).map(|c| (d, c)));
                }
            }
            let step = handler.run(&mut cx);
            if step == Step::Stop || stop.load(Ordering::Acquire) {
                break;
            }
            idle = step == Step::Idle;
            if idle
                && cx.queue.in_flight() == 0
                && let PollMode::Adaptive { idle_sleep } = opts.poll_mode
            {
                thread::sleep(idle_sleep);
            }
        }
        handler.stop(&mut cx);
    }

    // Clean shutdown: seal every owned disk so the next open needs no scan,
    // then close every descriptor.
    for slot in slots.iter_mut() {
        if let Some(mut w) = slot.writer.take() {
            blocking::seal(&mut *queue, &mut w).map_err(engine_err)?;
            blocking::drain(&mut *queue, &mut w).map_err(engine_err)?;
            w.detach(&mut *queue);
        }
    }
    for slot in slots.drain(..) {
        slot.reader.detach(&mut *queue);
    }
    Ok(handler)
}
