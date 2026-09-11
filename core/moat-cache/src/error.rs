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

use std::sync::Arc;

/// Errors from hybrid cache admission, encoding and storage.
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    /// The asynchronous chunk adapter failed or rejected admission.
    #[error(transparent)]
    Store(#[from] moat_cache_store::Error),
    /// Resident-cache configuration was invalid.
    #[error(transparent)]
    Memory(Arc<moat_cache_memory::BuildError>),
    /// A cache option or operation argument is invalid.
    #[error("invalid cache option: {0}")]
    Invalid(&'static str),
    /// A chunk envelope failed structural or semantic validation.
    #[error("corrupt cache entry: {0}")]
    Corrupt(&'static str),
    /// The complete encoded record does not fit a single engine chunk.
    #[error("encoded cache entry of {len} bytes exceeds chunk maximum {max}")]
    TooLarge {
        /// Complete encoded record length, including key and metadata.
        len: usize,
        /// Configured chunk maximum.
        max: usize,
    },
    /// Outstanding work or token/key bookkeeping reached its explicit budget.
    #[error("cache operation budget exhausted")]
    Busy,
    /// The cache is closing or its coordinator stopped.
    #[error("cache is closed")]
    Closed,
    /// Capacity could not be made available while retaining storage GC reserves.
    #[error("cache disk capacity exhausted")]
    NoSpace,
}
impl From<moat_cache_memory::BuildError> for Error {
    fn from(error: moat_cache_memory::BuildError) -> Self {
        Self::Memory(Arc::new(error))
    }
}
/// The hybrid cache result type.
pub type Result<T> = std::result::Result<T, Error>;
