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

//! The io_uring implementation of [`IoQueue`].
//!
//! One ring per queue (and therefore per thread). The queue's buffer pool
//! arenas are registered as fixed buffers once, so every read and write is a
//! `READ_FIXED`/`WRITE_FIXED` with no per-operation page pinning; attached
//! devices live in a sparse fixed-file table, so operations name a slot instead
//! of a file descriptor. The ring is created with `SINGLE_ISSUER` and
//! `DEFER_TASKRUN` where the kernel supports them (completion work then runs
//! only inside our own `io_uring_enter`, never as an interrupt on the pinned
//! core) and falls back to a plain ring otherwise. SQPOLL is deliberately not
//! used: a kernel polling thread per ring competes with the pinned application
//! threads for cores; the application already batches submissions per poll
//! cycle, which achieves the same syscall amortisation.

use std::{
    io,
    os::fd::{AsRawFd, RawFd},
    sync::Arc,
};

use io_uring::{IoUring, opcode, types};
use moat_common::{BufferPool, PooledBuf};

use crate::{
    device::Device,
    io::{Descriptor, Full, Inboxes, IoCompletion, IoQueue, QueueOptions},
};

/// `IORING_ENTER_GETEVENTS`: with `DEFER_TASKRUN` completion work only runs
/// when we enter with this flag, so a non-waiting reap must still set it.
const ENTER_GETEVENTS: u32 = 1;

struct Slot {
    desc: Descriptor,
    token: u64,
    buf: Option<PooledBuf>,
}

/// An io_uring backed queue. See the [module docs](self).
pub struct UringQueue {
    ring: IoUring,
    pool: Arc<BufferPool>,
    inboxes: Inboxes,
    /// In-flight operations indexed by the slot number carried in `user_data`.
    slots: Vec<Option<Slot>>,
    free_slots: Vec<u32>,
    /// Entries accepted since the last submit. They are copied into the
    /// submission ring in one go at submit time: opening the ring's view per
    /// entry would read the kernel-written head and write the tail every
    /// time, two cache-line transfers per operation.
    staged: Vec<io_uring::squeue::Entry>,
    /// Whether the ring runs with `DEFER_TASKRUN` (completions must be pulled
    /// with `GETEVENTS`).
    deferred: bool,
}

impl UringQueue {
    /// Creates a ring of `opts.depth` entries, a sparse file table of
    /// `opts.descriptors` slots, and registers the pool's arenas as fixed
    /// buffers. Must be called on the thread that will drive the queue.
    pub fn new(opts: &QueueOptions) -> io::Result<Self> {
        let depth = opts.depth.clamp(1, 32 * 1024).next_power_of_two();
        let (ring, deferred) = match IoUring::builder()
            .setup_cqsize(depth * 2)
            .setup_single_issuer()
            .setup_defer_taskrun()
            .build(depth)
        {
            Ok(ring) => (ring, true),
            Err(_) => (IoUring::builder().setup_cqsize(depth * 2).build(depth)?, false),
        };
        let files = opts.descriptors.max(1);
        ring.submitter().register_files_sparse(files)?;
        let pool = BufferPool::new(opts.pool)?;
        let iovecs: Vec<libc::iovec> = pool
            .arenas()
            .iter()
            .map(|a| libc::iovec {
                iov_base: a.as_ptr().cast(),
                iov_len: a.len(),
            })
            .collect();
        // SAFETY: the arenas are owned by `pool`, which this queue keeps alive
        // for as long as the ring exists; they never move or shrink.
        unsafe { ring.submitter().register_buffers(&iovecs)? };
        Ok(Self {
            ring,
            pool,
            inboxes: Inboxes::new(files),
            slots: (0..depth).map(|_| None).collect(),
            free_slots: (0..depth).rev().collect(),
            staged: Vec::with_capacity(depth as usize),
            deferred,
        })
    }

    /// Whether the ring was set up with `DEFER_TASKRUN`.
    pub fn deferred_taskrun(&self) -> bool {
        self.deferred
    }

    fn push(
        &mut self,
        entry: io_uring::squeue::Entry,
        slot: u32,
        desc: Descriptor,
        token: u64,
        buf: Option<PooledBuf>,
    ) {
        self.inboxes.start(desc);
        self.staged.push(entry);
        self.slots[slot as usize] = Some(Slot { desc, token, buf });
    }

    /// Moves staged entries into the submission ring. The ring holds `depth`
    /// entries and at most `depth` slots are ever reserved, and the kernel
    /// consumes the ring at every submit, so it cannot be full here.
    fn stage_to_ring(&mut self) {
        if self.staged.is_empty() {
            return;
        }
        let mut sq = self.ring.submission();
        for entry in self.staged.drain(..) {
            // SAFETY: the buffer referenced by `entry` (if any) is held in
            // `slots` until the CQE for its slot is reaped, so the memory
            // outlives the operation.
            if let Err(e) = unsafe { sq.push(&entry) } {
                unreachable!("submission queue full with a free slot: {e}");
            }
        }
    }

    fn reap(&mut self) -> usize {
        let mut n = 0;
        let (submitter, _, mut cq) = self.ring.split();
        cq.sync();
        for cqe in &mut cq {
            let slot = cqe.user_data() as u32;
            let res = cqe.result();
            let result = if res < 0 {
                Err(io::Error::from_raw_os_error(-res))
            } else {
                Ok(res as usize)
            };
            let Slot { desc, token, buf } = self.slots[slot as usize].take().expect("completion for a live slot");
            self.free_slots.push(slot);
            if self.inboxes.deliver(desc, IoCompletion { token, result, buf }) {
                let _ = submitter.register_files_update(desc.0, &[-1 as RawFd]);
            }
            n += 1;
        }
        n
    }

    fn enter(&mut self, want: u32) -> io::Result<()> {
        self.stage_to_ring();
        let to_submit = self.ring.submission().len() as u32;
        // SAFETY: no extended arguments are passed; the kernel validates the
        // ring fd and flags.
        unsafe {
            self.ring
                .submitter()
                .enter::<libc::sigset_t>(to_submit, want, ENTER_GETEVENTS, None)?;
        }
        Ok(())
    }
}

impl IoQueue for UringQueue {
    fn pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    fn attach(&mut self, device: &Arc<dyn Device>) -> io::Result<Descriptor> {
        let fd = device
            .fd()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "device has no file descriptor"))?;
        let desc = self
            .inboxes
            .open()
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "no free descriptor"))?;
        if let Err(e) = self.ring.submitter().register_files_update(desc.0, &[fd.as_raw_fd()]) {
            self.inboxes.close(desc);
            return Err(e);
        }
        Ok(desc)
    }

    fn detach(&mut self, desc: Descriptor) {
        // Staged SQEs still refer to this fixed-file slot. Keep the binding
        // until every accepted operation has completed, including staged ones.
        if self.inboxes.close(desc) {
            let _ = self.ring.submitter().register_files_update(desc.0, &[-1 as RawFd]);
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
        assert!(len <= buf.capacity(), "I/O length exceeds buffer capacity");
        let Some(slot) = self.free_slots.pop() else {
            return Err(buf);
        };
        let entry = opcode::ReadFixed::new(types::Fixed(desc.0), buf.as_mut_ptr(), len as u32, buf.arena_index())
            .offset(offset)
            .build()
            .user_data(slot as u64);
        self.push(entry, slot, desc, token, Some(buf));
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
        assert!(len <= buf.capacity(), "I/O length exceeds buffer capacity");
        let Some(slot) = self.free_slots.pop() else {
            return Err(buf);
        };
        let entry = opcode::WriteFixed::new(types::Fixed(desc.0), buf.as_ptr(), len as u32, buf.arena_index())
            .offset(offset)
            .build()
            .user_data(slot as u64);
        self.push(entry, slot, desc, token, Some(buf));
        Ok(())
    }

    fn fsync(&mut self, desc: Descriptor, token: u64) -> Result<(), Full> {
        let Some(slot) = self.free_slots.pop() else {
            return Err(Full);
        };
        let entry = opcode::Fsync::new(types::Fixed(desc.0))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(slot as u64);
        self.push(entry, slot, desc, token, None);
        Ok(())
    }

    fn submit(&mut self) -> io::Result<()> {
        self.stage_to_ring();
        if !self.ring.submission().is_empty() {
            self.ring.submit()?;
        }
        Ok(())
    }

    fn poll(&mut self, wait: bool) -> io::Result<usize> {
        let in_flight = self.in_flight();
        if wait && in_flight > 0 {
            self.enter(1)?;
        } else if self.deferred {
            if in_flight > 0 {
                self.enter(0)?;
            }
        } else {
            self.submit()?;
        }
        Ok(self.reap())
    }

    fn take(&mut self, desc: Descriptor, out: &mut Vec<IoCompletion>) -> usize {
        self.inboxes.take(desc, out)
    }

    fn in_flight(&self) -> usize {
        self.slots.len() - self.free_slots.len()
    }

    fn depth(&self) -> usize {
        self.slots.len()
    }
}

#[cfg(test)]
mod tests {
    use moat_common::{HugePages, PoolOptions};

    use super::*;
    use crate::device::FileDevice;

    #[test]
    fn oversized_io_is_rejected_before_reserving_a_slot() {
        let dir = tempfile::tempdir().unwrap();
        let device: Arc<dyn Device> = Arc::new(FileDevice::create(dir.path().join("disk.img"), 8192, false).unwrap());
        let mut queue = UringQueue::new(&crate::io::tests::detach_options()).unwrap();
        let desc = queue.attach(&device).unwrap();
        for read in [true, false] {
            let buf = queue.pool().alloc(4096).unwrap();
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if read {
                    queue.read(desc, buf, 8192, 0, 1)
                } else {
                    queue.write(desc, buf, 8192, 0, 1)
                }
            }));
            assert!(result.is_err());
            assert_eq!(queue.in_flight(), 0);
            assert_eq!(queue.pool().in_use(), 0);
        }
    }

    #[test]
    fn detached_descriptors_keep_staged_writes_on_the_original_device() {
        let dir = tempfile::tempdir().unwrap();
        let a = Arc::new(FileDevice::create(dir.path().join("a.img"), 4096, false).unwrap());
        let b = Arc::new(FileDevice::create(dir.path().join("b.img"), 4096, false).unwrap());
        let mut queue = UringQueue::new(&crate::io::tests::detach_options()).unwrap();
        crate::io::tests::check_detach(&mut queue, a, b);
    }

    #[test]
    fn fixed_buffer_write_then_read_on_two_devices() {
        let dir = tempfile::tempdir().unwrap();
        let a: Arc<dyn Device> = Arc::new(FileDevice::create(dir.path().join("a.img"), 1 << 20, false).unwrap());
        let b: Arc<dyn Device> = Arc::new(FileDevice::create(dir.path().join("b.img"), 1 << 20, false).unwrap());
        let opts = QueueOptions {
            depth: 8,
            pool: PoolOptions {
                bytes: 1 << 20,
                max_class: 64 << 10,
                huge_pages: HugePages::Disabled,
            },
            descriptors: 4,
        };
        let mut queue = UringQueue::new(&opts).unwrap();
        assert_eq!(queue.depth(), 8);
        let da = queue.attach(&a).unwrap();
        let db = queue.attach(&b).unwrap();
        assert_ne!(da, db);

        for (desc, fill) in [(da, 1u8), (db, 2u8)] {
            let mut buf = queue.pool().alloc(8192).unwrap();
            buf[..8192].fill(fill);
            queue.write(desc, buf, 8192, 16384, 1).unwrap();
        }
        assert_eq!(queue.in_flight(), 2);
        assert_eq!(queue.vacant(), 6);
        let mut done = Vec::new();
        while queue.in_flight() > 0 {
            queue.poll(true).unwrap();
        }
        for desc in [da, db] {
            assert_eq!(queue.take(desc, &mut done), 1);
            let c = done.pop().unwrap();
            assert_eq!(c.result.unwrap(), 8192);
            assert!(c.buf.is_some());
        }

        // Each device reads back what was written to it, not to the other.
        for (desc, fill) in [(da, 1u8), (db, 2u8)] {
            let buf = queue.pool().alloc(8192).unwrap();
            queue.read(desc, buf, 8192, 16384, 7).unwrap();
            while queue.take(desc, &mut done) == 0 {
                queue.poll(true).unwrap();
            }
            let c = done.pop().unwrap();
            assert_eq!(c.token, 7);
            assert!(c.buf.unwrap()[..8192].iter().all(|&x| x == fill));
        }

        queue.fsync(da, 4).unwrap();
        while queue.take(da, &mut done) == 0 {
            queue.poll(true).unwrap();
        }
        let c = done.pop().unwrap();
        assert!(c.buf.is_none());
        c.result.unwrap();

        // A full ring rejects losslessly and hands the buffer back.
        let mut held = Vec::new();
        for i in 0..8 {
            let buf = queue.pool().alloc(4096).unwrap();
            queue.read(da, buf, 4096, 0, 100 + i).unwrap();
        }
        let buf = queue.pool().alloc(4096).unwrap();
        let rejected = queue.read(da, buf, 4096, 0, 200).unwrap_err();
        held.push(rejected);
        assert_eq!(queue.vacant(), 0);
        while queue.in_flight() > 0 {
            queue.poll(true).unwrap();
        }
        assert_eq!(queue.take(da, &mut done), 8);

        // Completions for a detached descriptor are discarded.
        let buf = queue.pool().alloc(4096).unwrap();
        queue.read(db, buf, 4096, 0, 300).unwrap();
        queue.detach(db);
        while queue.in_flight() > 0 {
            queue.poll(true).unwrap();
        }
        done.clear();
        assert_eq!(queue.take(db, &mut done), 0);
        drop(held);
        assert_eq!(queue.pool().in_use(), 0);
    }
}
