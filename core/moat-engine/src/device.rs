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

//! The block device abstraction the engine runs on.
//!
//! A [`Device`] offers blocking, page-aligned positional I/O (used by
//! formatting and recovery, which run before any queue exists) and, where it
//! has one, a file descriptor for the asynchronous data path: an
//! [`IoQueue`](crate::io::IoQueue) attaches the device through
//! [`Device::fd`]. Keeping the interface this small lets the same engine run on
//! a raw NVMe namespace, a file (development), or an in-memory buffer (tests
//! with fault injection; served by the blocking
//! [`SyncQueue`](crate::io::SyncQueue)).
//!
//! All offsets and lengths passed to a [`Device`] are multiples of
//! [`PAGE_SIZE`]; implementations may rely on it. Buffers passed to a device
//! opened for direct I/O must be page aligned in memory as well, which the
//! engine guarantees by only using [`AlignedBuf`](moat_common::AlignedBuf) and
//! pool buffers.

use std::{
    fs::{File, OpenOptions},
    io,
    ops::Range,
    os::{
        fd::{AsFd, BorrowedFd},
        unix::fs::FileExt,
    },
    path::Path,
    sync::Arc,
};

use moat_common::{PAGE_SIZE, is_aligned};
use parking_lot::{Mutex, RwLock};

/// A block device.
pub trait Device: Send + Sync + 'static {
    /// The device capacity in bytes.
    fn capacity(&self) -> u64;

    /// Reads `buf.len()` bytes starting at `offset`.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()>;

    /// Writes `buf` starting at `offset`.
    ///
    /// On return the data has been accepted by the device. Whether it survives
    /// a power loss depends on the device's write cache; see [`Device::sync`].
    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;

    /// Flushes the device's volatile write cache.
    fn sync(&self) -> io::Result<()>;

    /// The file descriptor an asynchronous queue registers, if the device has
    /// one. Devices without a descriptor can only be driven through the
    /// blocking [`SyncQueue`](crate::io::SyncQueue).
    fn fd(&self) -> Option<BorrowedFd<'_>>;
}

fn check_io(buf_len: usize, offset: u64, capacity: u64) -> io::Result<()> {
    if !is_aligned(offset, PAGE_SIZE) || !is_aligned(buf_len as u64, PAGE_SIZE) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unaligned i/o: offset={offset} len={buf_len}"),
        ));
    }
    if offset + buf_len as u64 > capacity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("i/o past end of device: offset={offset} len={buf_len} capacity={capacity}"),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// FileDevice
// ---------------------------------------------------------------------------

/// A device backed by a regular file or a block device node.
pub struct FileDevice {
    file: File,
    len: u64,
}

impl FileDevice {
    /// Opens an existing file or block device.
    ///
    /// With `direct` set the file is opened with `O_DIRECT`, bypassing the page
    /// cache. This requires every buffer to be page aligned in memory.
    pub fn open(path: impl AsRef<Path>, direct: bool) -> io::Result<Self> {
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);
        #[cfg(target_os = "linux")]
        if direct {
            use std::os::unix::fs::OpenOptionsExt;
            opts.custom_flags(libc::O_DIRECT);
        }
        #[cfg(not(target_os = "linux"))]
        let _ = direct;
        let file = opts.open(path)?;
        let len = device_len(&file)?;
        Ok(Self { file, len })
    }

    /// Creates (or truncates) a regular file of `len` bytes and opens it.
    pub fn create(path: impl AsRef<Path>, len: u64, direct: bool) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        file.set_len(len)?;
        drop(file);
        Self::open(path, direct)
    }
}

fn device_len(file: &File) -> io::Result<u64> {
    let meta = file.metadata()?;
    if meta.len() > 0 {
        return Ok(meta.len());
    }
    // Block device nodes report a zero length through `metadata`; seek to the
    // end to learn the capacity instead.
    use std::io::{Seek, SeekFrom};
    let mut f = file.try_clone()?;
    f.seek(SeekFrom::End(0))
}

impl Device for FileDevice {
    fn capacity(&self) -> u64 {
        self.len
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        check_io(buf.len(), offset, self.len)?;
        self.file.read_exact_at(buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        check_io(buf.len(), offset, self.len)?;
        self.file.write_all_at(buf, offset)
    }

    fn sync(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn fd(&self) -> Option<BorrowedFd<'_>> {
        Some(self.file.as_fd())
    }
}

// ---------------------------------------------------------------------------
// MemDevice
// ---------------------------------------------------------------------------

struct MemState {
    data: RwLock<Vec<u8>>,
    /// Writes overlapping this range fail with `EIO`.
    fail_writes: Mutex<Option<Range<u64>>>,
}

/// An in-memory device for tests.
///
/// Besides the [`Device`] interface it exposes the raw bytes so tests can
/// simulate torn writes, bit rot and truncated tails between an engine
/// shutdown and the next open, plus a knob to fail writes in a byte range.
/// It has no file descriptor, so it is driven through a
/// [`SyncQueue`](crate::io::SyncQueue).
pub struct MemDevice {
    state: Arc<MemState>,
}

impl MemDevice {
    /// Creates a zero-filled device of `len` bytes.
    pub fn new(len: u64) -> Self {
        assert!(is_aligned(len, PAGE_SIZE), "device length must be page aligned");
        Self {
            state: Arc::new(MemState {
                data: RwLock::new(vec![0; len as usize]),
                fail_writes: Mutex::new(None),
            }),
        }
    }

    /// Runs `f` with mutable access to the raw device contents.
    pub fn with_data_mut<R>(&self, f: impl FnOnce(&mut [u8]) -> R) -> R {
        f(&mut self.state.data.write())
    }

    /// Runs `f` with read access to the raw device contents.
    pub fn with_data<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(&self.state.data.read())
    }

    /// Makes every write that overlaps `range` fail with `EIO` (`None` clears).
    pub fn fail_writes_in(&self, range: Option<Range<u64>>) {
        *self.state.fail_writes.lock() = range;
    }
}

impl MemState {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let data = self.data.read();
        check_io(buf.len(), offset, data.len() as u64)?;
        let start = offset as usize;
        buf.copy_from_slice(&data[start..start + buf.len()]);
        Ok(())
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        if let Some(range) = &*self.fail_writes.lock()
            && offset < range.end
            && offset + buf.len() as u64 > range.start
        {
            return Err(io::Error::from_raw_os_error(libc::EIO));
        }
        let mut data = self.data.write();
        check_io(buf.len(), offset, data.len() as u64)?;
        let start = offset as usize;
        data[start..start + buf.len()].copy_from_slice(buf);
        Ok(())
    }
}

impl Device for MemDevice {
    fn capacity(&self) -> u64 {
        self.state.data.read().len() as u64
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        self.state.read_at(buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.state.write_at(buf, offset)
    }

    fn sync(&self) -> io::Result<()> {
        Ok(())
    }

    fn fd(&self) -> Option<BorrowedFd<'_>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mem_device_rejects_unaligned() {
        let dev = MemDevice::new(16384);
        let mut buf = vec![0u8; 4096];
        assert!(dev.read_at(&mut buf, 1).is_err());
        assert!(dev.read_at(&mut buf[..100], 0).is_err());
        assert!(dev.read_at(&mut buf, 16384).is_err());
        assert!(dev.read_at(&mut buf, 12288).is_ok());
    }

    #[test]
    fn mem_device_write_fault_injection() {
        let dev = MemDevice::new(16384);
        let buf = vec![1u8; 4096];
        dev.fail_writes_in(Some(8192..12288));
        assert!(dev.write_at(&buf, 4096).is_ok());
        assert!(dev.write_at(&buf, 8192).is_err());
        dev.fail_writes_in(None);
        assert!(dev.write_at(&buf, 8192).is_ok());
    }

    #[test]
    fn file_device_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dev");
        let dev = FileDevice::create(&path, 32768, false).unwrap();
        assert_eq!(dev.capacity(), 32768);
        let payload = vec![0xabu8; 8192];
        dev.write_at(&payload, 8192).unwrap();
        let mut out = vec![0u8; 8192];
        dev.read_at(&mut out, 8192).unwrap();
        assert_eq!(out, payload);
        let reopened = FileDevice::open(&path, false).unwrap();
        assert_eq!(reopened.capacity(), 32768);
    }
}
