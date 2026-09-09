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

//! A page-aligned heap buffer.

use std::{
    alloc::{Layout, alloc_zeroed, dealloc},
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use crate::align::PAGE_SIZE;

/// A zero-initialised heap buffer whose address and length are both multiples
/// of [`PAGE_SIZE`].
///
/// Used for blocking direct I/O during formatting and recovery. Asynchronous
/// queues use [`PooledBuf`](crate::PooledBuf) from registered arenas.
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: the buffer exclusively owns its allocation; moving or sharing the
// owner across threads is no different from `Box<[u8]>`.
unsafe impl Send for AlignedBuf {}
// SAFETY: see the `Send` impl above; shared access is read-only through `Deref`.
unsafe impl Sync for AlignedBuf {}

impl AlignedBuf {
    /// Allocates a zeroed buffer of `len` bytes.
    ///
    /// # Panics
    ///
    /// Panics if `len` is zero or not a multiple of [`PAGE_SIZE`].
    pub fn zeroed(len: usize) -> Self {
        assert!(len > 0, "aligned buffer must not be empty");
        assert!(
            (len as u64).is_multiple_of(PAGE_SIZE),
            "aligned buffer length {len} is not a multiple of {PAGE_SIZE}"
        );
        let layout = Layout::from_size_align(len, PAGE_SIZE as usize).expect("valid layout");
        // SAFETY: `layout` has non-zero size (asserted above).
        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        Self { ptr, len }
    }

    /// Returns the buffer length in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the buffer is empty. Always `false`; provided for
    /// API symmetry with slices.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        // SAFETY: `ptr` points to `len` initialised (zeroed) bytes owned by `self`.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for AlignedBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `deref`, and `&mut self` guarantees exclusive access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.len, PAGE_SIZE as usize).expect("valid layout");
        // SAFETY: `ptr` was allocated by `alloc_zeroed` with exactly this layout.
        unsafe { dealloc(self.ptr.as_ptr(), layout) }
    }
}

impl std::fmt::Debug for AlignedBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlignedBuf").field("len", &self.len).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_and_zeroed() {
        let mut buf = AlignedBuf::zeroed(8192);
        assert_eq!(buf.len(), 8192);
        assert_eq!(buf.as_ptr() as usize % PAGE_SIZE as usize, 0);
        assert!(buf.iter().all(|&b| b == 0));
        buf[4095] = 7;
        assert_eq!(buf[4095], 7);
    }

    #[test]
    #[should_panic]
    fn rejects_unaligned_len() {
        let _ = AlignedBuf::zeroed(100);
    }
}
