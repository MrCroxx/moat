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
    future::Future,
    ops::{Deref, Range},
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};

use futures_channel::oneshot;
use moat_common::PooledBuf;

use crate::budget::{Permit, Retention};

/// Errors shared by coalesced callers without losing the underlying cause.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// Admission exceeded the configured count or byte budget.
    #[error("store admission budget exhausted")]
    Busy,
    /// The store is closing or its worker stopped before returning a reply.
    #[error("store is closed")]
    Closed,
    /// Invalid adapter configuration or operation argument.
    #[error("invalid store option: {0}")]
    Invalid(&'static str),
    /// A chunk engine operation failed.
    #[error(transparent)]
    Engine(Arc<moat_engine::Error>),
    /// Queue initialization or progress failed.
    #[error(transparent)]
    Io(Arc<std::io::Error>),
}
impl From<moat_engine::Error> for Error {
    fn from(error: moat_engine::Error) -> Self {
        Self::Engine(Arc::new(error))
    }
}
impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(Arc::new(error))
    }
}
/// The adapter result type.
pub type Result<T> = std::result::Result<T, Error>;

/// A reply to an already admitted operation. Independent of any async runtime.
/// Cancelling this future does not cancel other waiters or an accepted mutation.
#[must_use = "dropping a request discards its reply, not an accepted mutation"]
pub struct Request<T> {
    pub(crate) receiver: oneshot::Receiver<Result<T>>,
}
impl<T> Request<T> {
    pub(crate) fn ready(result: Result<T>) -> Self {
        let (sender, receiver) = oneshot::channel();
        let _ = sender.send(result);
        Self { receiver }
    }
}
impl<T> Future for Request<T> {
    type Output = Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(cx)
            .map(|result| result.unwrap_or(Err(Error::Closed)))
    }
}

/// Shared bytes from one physical engine read, with retained-buffer accounting.
/// Keeping this object alive retains both the pool buffer and its byte credits.
pub struct Chunk {
    pub(crate) buf: PooledBuf,
    pub(crate) range: Range<usize>,
    pub(crate) lsn: u64,
    pub(crate) _permit: Permit,
    pub(crate) retention: OnceLock<Retention>,
}
impl Chunk {
    /// Reserves bounded long-lived retention, preserving at least one maximum
    /// read allocation globally and on this disk. Shared views charge once.
    /// The reservation lasts until the final chunk owner is dropped, including
    /// external owners after eviction. Failure does not invalidate this chunk.
    pub fn try_reserve_retention(&self) -> bool {
        if self.retention.get().is_some() {
            return true;
        }
        let Some(permit) = self._permit.retain() else {
            return self.retention.get().is_some();
        };
        let _ = self.retention.set(permit);
        true
    }

    /// Physical pool allocation retained by this chunk, including alignment.
    pub fn allocation_size(&self) -> usize {
        self.buf.len()
    }
    /// The completed logical record LSN at the time the read was submitted.
    pub fn lsn(&self) -> u64 {
        self.lsn
    }
}
impl Deref for Chunk {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.buf[self.range.clone()]
    }
}
impl AsRef<[u8]> for Chunk {
    fn as_ref(&self) -> &[u8] {
        self
    }
}
impl std::fmt::Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Chunk")
            .field("lsn", &self.lsn)
            .field("len", &self.range.len())
            .finish()
    }
}

/// The result of an unconditional or LSN-conditional deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteResult {
    /// A tombstone completed with this LSN.
    Deleted(u64),
    /// No live chunk existed when the operation reached its ordered position.
    Missing,
    /// A live chunk existed with a different LSN and was left unchanged.
    Changed,
}
