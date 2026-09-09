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

//! A pool of page-aligned I/O buffers carved from pre-allocated arenas.
//!
//! Every buffer used on the data path (io_uring fixed buffers, RDMA landing and
//! source buffers, the writer's batch staging) comes from a [`BufferPool`]:
//! memory is mapped once at startup, registered once, and then recycled
//! through per-size-class free lists. Nothing on the hot path calls the system
//! allocator or zeroes memory.
//!
//! Sizes are rounded up to a power of two between one page and the configured
//! maximum and served by a binary buddy allocator per arena, so a burst of
//! large requests followed by small ones (or the reverse) never strands memory
//! in the wrong size class. Preallocated per-order stacks recycle recent frees;
//! intrusive links in free blocks support buddy coalescing. Buffers are handed
//! out as [`PooledBuf`], an owning handle that returns the memory to the pool
//! when dropped, so a buffer can be moved into an I/O queue and back without
//! any lifetime bookkeeping.
//!
//! # Threads
//!
//! A pool belongs to the thread that created it (its *home* thread) and has no
//! lock: only the home thread allocates, and only the home thread runs the
//! buddy allocator. A buffer may travel to another thread (a value read on one
//! worker and handed to another, a landing buffer forwarded to a disk's owner)
//! and be dropped there; such a drop does not touch the allocator but pushes
//! the block onto a lock-free return stack that the home thread drains on its
//! next allocation. Allocating from a foreign thread is a programming error and
//! panics.

use std::{
    cell::{Cell, UnsafeCell},
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use crate::{
    align::{PAGE_SIZE, align_up},
    arena::{Arena, HugePages},
};

/// Largest single arena; io_uring registers each arena as one fixed buffer and
/// caps those at 1 GiB.
const ARENA_MAX: usize = 1 << 30;

/// Pool configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolOptions {
    /// Total bytes to map. Rounded up to a multiple of `max_class`.
    pub bytes: usize,
    /// Largest buffer the pool hands out (a power of two, at least one page).
    pub max_class: usize,
    /// Huge page policy for the arenas.
    pub huge_pages: HugePages,
}

impl Default for PoolOptions {
    fn default() -> Self {
        Self {
            bytes: 256 << 20,
            max_class: 8 << 20,
            huge_pages: HugePages::Preferred,
        }
    }
}

/// A process-unique identifier of the calling thread that costs a thread-local
/// read (`std::thread::current()` clones an `Arc`).
fn this_thread() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    thread_local! {
        static ID: Cell<u64> = const { Cell::new(0) };
    }
    ID.with(|id| {
        if id.get() == 0 {
            id.set(NEXT.fetch_add(1, Ordering::Relaxed));
        }
        id.get()
    })
}

/// A binary buddy allocator over one arena, in units of pages.
///
/// Free blocks are threaded into one doubly linked list per order, with the
/// links stored in the first 16 bytes of the free block itself, and a bitmap
/// per order records which blocks are free so a block's buddy (`offset ^ size`)
/// can be tested and unlinked in O(1). Nothing is allocated on the heap after
/// construction: the hot path is a handful of loads and stores into memory the
/// pool already owns. Non-power-of-two arenas are seeded with the binary
/// decomposition of their length.
///
/// Coalescing is lazy: a released block is parked on a per-order stack of
/// offsets, without looking at its buddy and without touching the block
/// itself (whose memory the device just wrote and the CPU has no reason to
/// pull into its cache), while that stack holds fewer than
/// [`LAZY_BYTES_PER_ORDER`] bytes. A steady stream of same-class allocations
/// (4 KiB reads) is then a pop and a push on a small hot array rather than a
/// split down from a large block and a merge back up on every call. Beyond the
/// limit, and whenever an allocation finds nothing big enough, parked blocks
/// merge with their free buddies as usual, so large classes still recover.
struct Buddy {
    base: *mut u8,
    min_shift: u32,
    heads: Vec<usize>,
    /// Parked blocks per order: free, but not on the intrusive list and not
    /// marked in the bitmap.
    parked: Vec<Vec<usize>>,
    bitmaps: Vec<Vec<u64>>,
}

const NONE: usize = usize::MAX;

/// Bytes each order may keep on its free list before releases coalesce.
const LAZY_BYTES_PER_ORDER: usize = 8 << 20;

impl Buddy {
    fn new(arena: &Arena, min_shift: u32, max_shift: u32) -> Self {
        let orders = (max_shift - min_shift + 1) as usize;
        let bitmaps = (0..orders)
            .map(|k| {
                let blocks = arena.len() >> (k as u32 + min_shift);
                vec![0u64; blocks.div_ceil(64).max(1)]
            })
            .collect();
        let mut buddy = Self {
            base: arena.as_ptr(),
            min_shift,
            heads: vec![NONE; orders],
            parked: (0..orders)
                .map(|k| Vec::with_capacity((LAZY_BYTES_PER_ORDER >> (k as u32 + min_shift)).max(1)))
                .collect(),
            bitmaps,
        };
        let mut offset = 0usize;
        let mut remaining = arena.len();
        while remaining >= (1 << min_shift) {
            let size = (1usize << remaining.ilog2()).min(1 << max_shift);
            let order = (size.trailing_zeros() - min_shift) as usize;
            buddy.push(order, offset);
            offset += size;
            remaining -= size;
        }
        buddy
    }

    #[inline]
    fn size(&self, order: usize) -> usize {
        1 << (order as u32 + self.min_shift)
    }

    /// The `(prev, next)` links stored at the head of a free block.
    #[inline]
    fn links(&self, offset: usize) -> *mut [usize; 2] {
        // SAFETY: `offset` is a block start inside the arena (checked by the
        // callers' bitmaps), and every block is at least one page, so 16 bytes
        // of link storage fit.
        unsafe { self.base.add(offset).cast() }
    }

    #[inline]
    fn get_links(&self, offset: usize) -> [usize; 2] {
        // SAFETY: see `links`; free blocks always hold valid links written by
        // `push`.
        unsafe { self.links(offset).read() }
    }

    #[inline]
    fn set_links(&mut self, offset: usize, prev: usize, next: usize) {
        // SAFETY: see `links`; the block is free, so no user holds it.
        unsafe { self.links(offset).write([prev, next]) }
    }

    #[inline]
    fn bit(&self, order: usize, offset: usize) -> (usize, u64) {
        let index = offset >> (order as u32 + self.min_shift);
        (index / 64, 1u64 << (index % 64))
    }

    #[inline]
    fn is_free(&self, order: usize, offset: usize) -> bool {
        let (word, mask) = self.bit(order, offset);
        self.bitmaps[order].get(word).is_some_and(|w| w & mask != 0)
    }

    fn push(&mut self, order: usize, offset: usize) {
        let head = self.heads[order];
        self.set_links(offset, NONE, head);
        if head != NONE {
            let [_, next] = self.get_links(head);
            self.set_links(head, offset, next);
        }
        self.heads[order] = offset;
        let (word, mask) = self.bit(order, offset);
        self.bitmaps[order][word] |= mask;
    }

    fn unlink(&mut self, order: usize, offset: usize) {
        let [prev, next] = self.get_links(offset);
        if prev == NONE {
            self.heads[order] = next;
        } else {
            let [pp, _] = self.get_links(prev);
            self.set_links(prev, pp, next);
        }
        if next != NONE {
            let [_, nn] = self.get_links(next);
            self.set_links(next, prev, nn);
        }
        let (word, mask) = self.bit(order, offset);
        self.bitmaps[order][word] &= !mask;
    }

    fn alloc(&mut self, order: usize) -> Option<usize> {
        if let Some(offset) = self.parked[order].pop() {
            return Some(offset);
        }
        let from = match (order..self.heads.len()).find(|&k| self.heads[k] != NONE) {
            Some(from) => from,
            None => {
                // Parked blocks may merge into something big enough.
                if self.parked.iter().all(Vec::is_empty) {
                    return None;
                }
                self.compact();
                (order..self.heads.len()).find(|&k| self.heads[k] != NONE)?
            }
        };
        let offset = self.heads[from];
        self.unlink(from, offset);
        // Split down to the requested order, keeping the upper halves free.
        for k in (order..from).rev() {
            self.push(k, offset + self.size(k));
        }
        Some(offset)
    }

    fn release(&mut self, offset: usize, order: usize) {
        let stack = &mut self.parked[order];
        if stack.len() < stack.capacity() {
            stack.push(offset);
            return;
        }
        self.release_eager(offset, order);
    }

    /// Returns every parked block to the intrusive lists, merging with free
    /// buddies bottom up, so that the largest possible blocks become
    /// allocatable again.
    fn compact(&mut self) {
        for order in 0..self.heads.len() {
            while let Some(offset) = self.parked[order].pop() {
                self.release_eager(offset, order);
            }
        }
    }

    fn release_eager(&mut self, mut offset: usize, mut order: usize) {
        while order + 1 < self.heads.len() {
            let buddy = offset ^ self.size(order);
            if !self.is_free(order, buddy) {
                break;
            }
            self.unlink(order, buddy);
            offset = offset.min(buddy);
            order += 1;
        }
        self.push(order, offset);
    }
}

/// A block released from a foreign thread, waiting on the return stack. Lives
/// in the first bytes of the free block itself.
#[repr(C)]
struct Returned {
    next: usize,
    arena: usize,
    offset: usize,
    len: usize,
}

/// A pool of page-aligned buffers. See the [module docs](self).
pub struct BufferPool {
    arenas: Vec<Arena>,
    min_shift: u32,
    max_shift: u32,
    /// Only the home thread touches the allocators.
    buddies: UnsafeCell<Vec<Buddy>>,
    home: u64,
    /// Head of the intrusive stack of blocks released from foreign threads
    /// (address of the block, or zero).
    returned: AtomicUsize,
    in_use: AtomicUsize,
}

// SAFETY: `buddies` is only accessed from the home thread (`alloc` and the
// home-thread branch of `release` assert it); every other field is immutable
// or atomic. Sharing the pool across threads therefore only exposes
// thread-safe operations.
unsafe impl Sync for BufferPool {}
// SAFETY: as above; the raw arena pointers inside `Buddy` are owned by the
// arenas the pool holds.
unsafe impl Send for BufferPool {}

impl BufferPool {
    /// Maps the arenas and creates an empty pool owned by the calling thread.
    pub fn new(opts: PoolOptions) -> std::io::Result<Arc<Self>> {
        let min_shift = PAGE_SIZE.trailing_zeros();
        assert!(
            opts.max_class.is_power_of_two() && opts.max_class >= PAGE_SIZE as usize,
            "max_class must be a power of two of at least one page"
        );
        assert!(opts.max_class <= ARENA_MAX, "max_class must not exceed 1 GiB");
        let max_shift = opts.max_class.trailing_zeros();
        let total = align_up(opts.bytes.max(opts.max_class) as u64, opts.max_class as u64) as usize;

        let mut arenas = Vec::new();
        let mut remaining = total;
        while remaining > 0 {
            let len = remaining.min(ARENA_MAX);
            arenas.push(Arena::new(len, opts.huge_pages)?);
            remaining -= len;
        }
        let buddies = arenas.iter().map(|a| Buddy::new(a, min_shift, max_shift)).collect();
        Ok(Arc::new(Self {
            arenas,
            min_shift,
            max_shift,
            buddies: UnsafeCell::new(buddies),
            home: this_thread(),
            returned: AtomicUsize::new(0),
            in_use: AtomicUsize::new(0),
        }))
    }

    /// The arenas backing the pool, for registration with io_uring or an RDMA
    /// device. The index into this slice is what [`PooledBuf::arena_index`]
    /// reports.
    pub fn arenas(&self) -> &[Arena] {
        &self.arenas
    }

    /// Total mapped bytes.
    pub fn capacity(&self) -> usize {
        self.arenas.iter().map(Arena::len).sum()
    }

    /// Bytes currently handed out (class sizes, not requested sizes).
    pub fn in_use(&self) -> usize {
        self.in_use.load(Ordering::Relaxed)
    }

    /// Largest buffer this pool can allocate.
    pub fn max_class(&self) -> usize {
        1 << self.max_shift
    }

    /// Whether the calling thread is the pool's home thread.
    pub fn is_home(&self) -> bool {
        this_thread() == self.home
    }

    /// Size class a request of `len` bytes is served from.
    #[inline]
    pub fn class_size(&self, len: usize) -> usize {
        (len.max(1).next_power_of_two()).max(1 << self.min_shift)
    }

    #[inline]
    fn order(&self, len: usize) -> usize {
        (len.trailing_zeros() - self.min_shift) as usize
    }

    /// Runs `f` on the allocators. Callers must be on the home thread.
    #[inline]
    fn with_buddies<R>(&self, f: impl FnOnce(&mut [Buddy]) -> R) -> R {
        assert!(
            self.is_home(),
            "buffer pool used from a thread other than its home thread"
        );
        // SAFETY: only the home thread reaches this point (asserted above), and
        // the pool is never re-entered while the `&mut` is live: `f` is one
        // of the leaf operations below, which do not call back into the pool.
        f(unsafe { &mut *self.buddies.get() })
    }

    /// Allocates a buffer of at least `len` bytes.
    ///
    /// Returns `None` if `len` exceeds the maximum class or the pool is
    /// exhausted; callers treat that as back-pressure, not as an error.
    ///
    /// # Panics
    ///
    /// If called from a thread other than the one that created the pool.
    pub fn alloc(self: &Arc<Self>, len: usize) -> Option<PooledBuf> {
        if len > self.max_class() {
            return None;
        }
        let size = self.class_size(len);
        let order = self.order(size);
        let (arena, offset) = self.with_buddies(|buddies| {
            self.drain_returned(buddies);
            buddies
                .iter_mut()
                .enumerate()
                .find_map(|(i, b)| b.alloc(order).map(|off| (i as u32, off)))
        })?;
        self.in_use.fetch_add(size, Ordering::Relaxed);
        Some(PooledBuf {
            pool: self.clone(),
            arena,
            offset,
            len: size,
        })
    }

    /// Moves every block released from a foreign thread back into the
    /// allocators. Home thread only.
    fn drain_returned(&self, buddies: &mut [Buddy]) {
        let mut node = self.returned.swap(0, Ordering::Acquire);
        while node != 0 {
            // SAFETY: `node` is the address of a block that a foreign
            // `release` pushed after writing a `Returned` at its start; the
            // block is free, so nothing else touches that memory, and the
            // Acquire swap above (paired with the pusher's Release) makes the
            // fields visible.
            let returned = unsafe { (node as *const Returned).read() };
            buddies[returned.arena].release(returned.offset, self.order(returned.len));
            node = returned.next;
        }
    }

    fn release(&self, arena: u32, offset: usize, len: usize) {
        self.in_use.fetch_sub(len, Ordering::Relaxed);
        if self.is_home() {
            let order = self.order(len);
            self.with_buddies(|buddies| buddies[arena as usize].release(offset, order));
            return;
        }
        // Foreign thread: push the block onto the return stack. Its memory is
        // ours to reuse for the node since the buffer has been released.
        // SAFETY: `offset + len <= arena.len()` by construction, and a block is
        // at least one page, so a `Returned` fits at its start.
        let node = unsafe { self.arenas[arena as usize].as_ptr().add(offset) }.cast::<Returned>();
        let addr = node as usize;
        let mut head = self.returned.load(Ordering::Relaxed);
        loop {
            // SAFETY: as above; the block is exclusively ours until the CAS
            // below publishes it to the home thread.
            unsafe {
                node.write(Returned {
                    next: head,
                    arena: arena as usize,
                    offset,
                    len,
                });
            }
            match self
                .returned
                .compare_exchange_weak(head, addr, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(current) => head = current,
            }
        }
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("capacity", &self.capacity())
            .field("in_use", &self.in_use())
            .field("arenas", &self.arenas.len())
            .field("max_class", &self.max_class())
            .finish()
    }
}

/// An owning handle to a pool buffer. Dereferences to its full class-sized
/// capacity; the memory is returned to the pool on drop (from any thread).
///
/// Contents are whatever the previous user left (the first bytes of a block
/// hold the allocator's free-list links or return-stack node while it is
/// free): callers overwrite what they use and must not rely on zeroes.
pub struct PooledBuf {
    pool: Arc<BufferPool>,
    arena: u32,
    offset: usize,
    len: usize,
}

// SAFETY: a `PooledBuf` is the unique owner of its byte range until dropped;
// the pool never hands the same range out twice, and dropping on a foreign
// thread goes through the lock-free return stack.
unsafe impl Send for PooledBuf {}
// SAFETY: as above; shared access only yields `&[u8]`.
unsafe impl Sync for PooledBuf {}

impl PooledBuf {
    /// Capacity in bytes (the size class, at least the requested length).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.len
    }

    /// Index of the arena this buffer lives in; matches
    /// [`BufferPool::arenas`] and is the fixed-buffer index for io_uring.
    #[inline]
    pub fn arena_index(&self) -> u16 {
        self.arena as u16
    }

    /// Byte offset of this buffer within its arena.
    #[inline]
    pub fn offset_in_arena(&self) -> usize {
        self.offset
    }

    /// The pool this buffer belongs to.
    #[inline]
    pub fn pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    #[inline]
    fn base(&self) -> *mut u8 {
        // SAFETY: `offset + len <= arena.len()` by construction in `alloc`.
        unsafe { self.pool.arenas[self.arena as usize].as_ptr().add(self.offset) }
    }

    /// Raw pointer to the start of the buffer.
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.base()
    }

    /// Raw mutable pointer to the start of the buffer.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.base()
    }
}

impl Deref for PooledBuf {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        // SAFETY: the range is owned by `self` and always initialised (arenas
        // start zeroed and are only written through slices).
        unsafe { std::slice::from_raw_parts(self.base(), self.len) }
    }
}

impl DerefMut for PooledBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`; `&mut self` gives exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.base(), self.len) }
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        self.pool.release(self.arena, self.offset, self.len);
    }
}

impl std::fmt::Debug for PooledBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledBuf")
            .field("arena", &self.arena)
            .field("offset", &self.offset)
            .field("len", &self.len)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(bytes: usize, max_class: usize) -> Arc<BufferPool> {
        BufferPool::new(PoolOptions {
            bytes,
            max_class,
            huge_pages: HugePages::Disabled,
        })
        .unwrap()
    }

    #[test]
    fn classes_round_up_and_recycle() {
        let pool = pool(1 << 20, 256 << 10);
        let a = pool.alloc(1).unwrap();
        assert_eq!(a.capacity(), 4096);
        let b = pool.alloc(4097).unwrap();
        assert_eq!(b.capacity(), 8192);
        assert_eq!(pool.in_use(), 4096 + 8192);
        let (a_arena, a_off) = (a.arena_index(), a.offset_in_arena());
        drop(a);
        assert_eq!(pool.in_use(), 8192);
        // The freed slot is reused for the next request of the same class.
        let c = pool.alloc(100).unwrap();
        assert_eq!((c.arena_index(), c.offset_in_arena()), (a_arena, a_off));
        assert!(pool.alloc(256 << 10).is_some());
        assert!(pool.alloc((256 << 10) + 1).is_none(), "above max class");
    }

    #[test]
    fn exhaustion_is_none_not_panic() {
        let pool = pool(64 << 10, 64 << 10);
        let a = pool.alloc(64 << 10).unwrap();
        assert!(pool.alloc(4096).is_none());
        assert!(pool.alloc(usize::MAX).is_none());
        drop(a);
        // The freed large block is split for the small request...
        let small = pool.alloc(4096).unwrap();
        assert_eq!(small.offset_in_arena(), 0);
        // ...and a large request now fails until it is returned and coalesced.
        assert!(pool.alloc(64 << 10).is_none());
        drop(small);
        assert!(pool.alloc(64 << 10).is_some());
    }

    #[test]
    fn buddies_coalesce_across_orders() {
        let pool = pool(1 << 20, 1 << 20);
        let bufs: Vec<PooledBuf> = (0..256).map(|_| pool.alloc(4096).unwrap()).collect();
        assert_eq!(pool.in_use(), 1 << 20);
        assert!(pool.alloc(4096).is_none());
        drop(bufs);
        assert_eq!(pool.in_use(), 0);
        // Every page went back and merged into one maximal block.
        assert!(pool.alloc(1 << 20).is_some());
    }

    #[test]
    fn non_power_of_two_arena_is_fully_usable() {
        let pool = BufferPool::new(PoolOptions {
            bytes: 3 << 20,
            max_class: 1 << 20,
            huge_pages: HugePages::Disabled,
        })
        .unwrap();
        let bufs: Vec<PooledBuf> = (0..3).map(|_| pool.alloc(1 << 20).unwrap()).collect();
        assert!(pool.alloc(4096).is_none());
        drop(bufs);
        assert_eq!(pool.in_use(), 0);
    }

    #[test]
    fn buffers_are_page_aligned_and_writable() {
        let pool = pool(1 << 20, 64 << 10);
        let mut bufs: Vec<PooledBuf> = (0..8).map(|_| pool.alloc(12_345).unwrap()).collect();
        for (i, b) in bufs.iter_mut().enumerate() {
            assert_eq!(b.as_mut_ptr() as usize % PAGE_SIZE as usize, 0);
            b[0] = i as u8;
            let last = b.capacity() - 1;
            b[last] = i as u8;
        }
        for (i, b) in bufs.iter().enumerate() {
            assert_eq!(b[0], i as u8);
        }
    }

    #[test]
    fn spans_multiple_arenas_when_large() {
        // Force two arenas by exceeding the per-arena cap.
        let pool = BufferPool::new(PoolOptions {
            bytes: ARENA_MAX + (4 << 20),
            max_class: 4 << 20,
            huge_pages: HugePages::Disabled,
        })
        .unwrap();
        assert_eq!(pool.arenas().len(), 2);
    }

    #[test]
    fn foreign_drops_come_home_on_next_alloc() {
        let pool = pool(256 << 10, 64 << 10);
        let bufs: Vec<PooledBuf> = (0..4).map(|_| pool.alloc(64 << 10).unwrap()).collect();
        assert!(pool.alloc(4096).is_none());
        let handles: Vec<_> = bufs.into_iter().map(|b| std::thread::spawn(move || drop(b))).collect();
        for h in handles {
            h.join().unwrap();
        }
        // Released on foreign threads: accounted immediately, allocator
        // updated when the home thread allocates next.
        assert_eq!(pool.in_use(), 0);
        let bufs: Vec<PooledBuf> = (0..4).map(|_| pool.alloc(64 << 10).unwrap()).collect();
        assert_eq!(bufs.len(), 4);
    }

    #[test]
    fn foreign_alloc_panics() {
        let pool = pool(64 << 10, 64 << 10);
        let result = std::thread::spawn(move || pool.alloc(4096)).join();
        assert!(result.is_err());
    }
}
