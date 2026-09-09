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

//! Large, long-lived, page-aligned memory regions.
//!
//! An [`Arena`] is allocated once at startup and carved up by a
//! [`BufferPool`](crate::pool::BufferPool). It is the unit that gets registered
//! with io_uring (as a fixed buffer) and with an RDMA NIC (as a memory region),
//! so it must be contiguous, page aligned, and never move.
//!
//! Huge pages are requested when available: fewer TLB misses on the data path
//! and far fewer page-table entries for the NIC and the kernel to pin. The
//! allocation falls back from explicit huge pages (`MAP_HUGETLB`) to
//! transparent huge pages (`madvise(MADV_HUGEPAGE)`) to plain pages, so the
//! engine runs everywhere and merely gets faster where huge pages exist.

use std::{io, ptr::NonNull};

use crate::align::{PAGE_SIZE, align_up};

/// Size of a 2 MiB huge page.
pub const HUGE_PAGE_2M: u64 = 2 << 20;
/// Size of a 1 GiB huge page.
pub const HUGE_PAGE_1G: u64 = 1 << 30;

/// How an [`Arena`] should try to obtain huge pages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HugePages {
    /// Plain pages only.
    Disabled,
    /// Try explicit huge pages (1 GiB, then 2 MiB), then transparent huge
    /// pages, then plain pages. Never fails because of huge page availability.
    Preferred,
    /// Require explicit huge pages; fail if none can be mapped.
    Required,
}

/// What an [`Arena`] ended up backed by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backing {
    /// Explicit 1 GiB huge pages.
    Huge1G,
    /// Explicit 2 MiB huge pages.
    Huge2M,
    /// Plain pages with `MADV_HUGEPAGE`; the kernel may promote them.
    Transparent,
    /// Plain pages.
    Plain,
}

/// A contiguous, page-aligned, zero-initialised memory region that never moves.
pub struct Arena {
    ptr: NonNull<u8>,
    len: usize,
    backing: Backing,
}

// SAFETY: the arena exclusively owns its mapping; sharing the owner across
// threads is no different from sharing a `Box<[u8]>`.
unsafe impl Send for Arena {}
// SAFETY: shared access exposes metadata and a raw pointer, not references
// into memory that a buffer pool may be mutating.
unsafe impl Sync for Arena {}

impl Arena {
    /// Maps `len` bytes (rounded up to the page size the backing uses).
    pub fn new(len: usize, huge: HugePages) -> io::Result<Self> {
        assert!(len > 0, "arena must not be empty");
        let mut last_err = None;
        if huge != HugePages::Disabled {
            for (page, flag) in [
                (HUGE_PAGE_1G, libc::MAP_HUGETLB | libc::MAP_HUGE_1GB),
                (HUGE_PAGE_2M, libc::MAP_HUGETLB | libc::MAP_HUGE_2MB),
            ] {
                let rounded = align_up(len as u64, page) as usize;
                // Do not burn a 1 GiB page on a small arena.
                if rounded > 2 * len && page == HUGE_PAGE_1G {
                    continue;
                }
                match Self::map(rounded, flag) {
                    Ok(ptr) => {
                        let backing = if page == HUGE_PAGE_1G {
                            Backing::Huge1G
                        } else {
                            Backing::Huge2M
                        };
                        return Ok(Self {
                            ptr,
                            len: rounded,
                            backing,
                        });
                    }
                    Err(e) => last_err = Some(e),
                }
            }
            if huge == HugePages::Required {
                return Err(last_err.unwrap_or_else(|| io::Error::other("huge pages unavailable")));
            }
        }

        let rounded = align_up(len as u64, PAGE_SIZE) as usize;
        let ptr = Self::map(rounded, 0)?;
        let mut backing = Backing::Plain;
        if huge != HugePages::Disabled {
            // SAFETY: `ptr..ptr+rounded` is a mapping we own; MADV_HUGEPAGE is
            // advisory and cannot invalidate it.
            let rc = unsafe { libc::madvise(ptr.as_ptr().cast(), rounded, libc::MADV_HUGEPAGE) };
            if rc == 0 {
                backing = Backing::Transparent;
            }
        }
        Ok(Self {
            ptr,
            len: rounded,
            backing,
        })
    }

    fn map(len: usize, extra_flags: libc::c_int) -> io::Result<NonNull<u8>> {
        // SAFETY: anonymous private mapping with no address hint; the kernel
        // validates every argument and returns MAP_FAILED on error.
        let raw = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | extra_flags,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(NonNull::new(raw.cast()).expect("mmap returned null"))
    }

    /// The mapping length in bytes (a multiple of the backing page size).
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the arena is empty. Always `false`; provided for API symmetry.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// What the arena is backed by.
    #[inline]
    pub fn backing(&self) -> Backing {
        self.backing
    }

    /// The base address.
    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`len` describe exactly the mapping created in `map`.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

impl std::fmt::Debug for Arena {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Arena")
            .field("len", &self.len)
            .field("backing", &self.backing)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_arena_is_aligned_and_zeroed() {
        let arena = Arena::new(3 * PAGE_SIZE as usize + 1, HugePages::Disabled).unwrap();
        assert_eq!(arena.len(), 4 * PAGE_SIZE as usize);
        assert_eq!(arena.as_ptr() as usize % PAGE_SIZE as usize, 0);
        // SAFETY: this arena has not been given to a pool or mutated.
        let bytes = unsafe { std::slice::from_raw_parts(arena.as_ptr(), arena.len()) };
        assert!(bytes.iter().all(|&b| b == 0));
        assert_eq!(arena.backing(), Backing::Plain);
    }

    #[test]
    fn preferred_never_fails() {
        // Whatever the machine offers, `Preferred` must produce a usable arena.
        let arena = Arena::new(8 << 20, HugePages::Preferred).unwrap();
        assert!(arena.len() >= 8 << 20);
        assert_eq!(arena.as_ptr() as usize % PAGE_SIZE as usize, 0);
    }
}
