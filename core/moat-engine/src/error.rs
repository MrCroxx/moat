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

//! Engine error type.

use std::io;

/// Errors returned by the engine.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The underlying device failed.
    #[error("device i/o error: {0}")]
    Io(#[from] io::Error),

    /// On-disk data failed validation. The engine never returns data that did
    /// not pass every check; callers should treat the chunk as unreadable.
    #[error("corrupt on-disk data: {0}")]
    Corrupt(String),

    /// The device is not formatted for this engine, or the superblocks are
    /// unreadable.
    #[error("device is not formatted: {0}")]
    Unformatted(String),

    /// No free segment is available. Callers must reclaim before retrying.
    #[error("no free segment available")]
    NoSpace,

    /// The index has reached its memory budget; no new chunk can be added
    /// until others are deleted or reclaimed.
    #[error("index memory budget exhausted")]
    IndexFull,

    /// The buffer pool has no buffer of the requested class right now (or the
    /// resource asked for already exists: a second writer, a running reclaim
    /// pass). Poll to return buffers and retry.
    #[error("resource busy: no buffer available or the operation is already running")]
    Busy,

    /// The value exceeds the chunk size limit recorded in the superblock.
    #[error("value of {len} bytes exceeds the chunk limit of {max} bytes")]
    ValueTooLarge {
        /// The offending value length.
        len: u64,
        /// The maximum accepted length.
        max: u64,
    },

    /// A configuration or format option is invalid.
    #[error("invalid option: {0}")]
    InvalidOption(String),
}

/// A `Result` whose error type is [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub(crate) fn corrupt(msg: impl Into<String>) -> Self {
        Self::Corrupt(msg.into())
    }
}
