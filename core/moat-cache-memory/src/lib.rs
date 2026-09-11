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

//! A sharded, weighted resident cache with shared entry lifetimes.
//!
//! Lookups accept borrowed keys and never perform I/O or invoke a loader.
//! FIFO, SIEVE and S3FIFO hits use shared shard locks; LRU and windowed
//! TinyLFU update their policy under an exclusive shard lock. LRU pins entries
//! while callers hold them and returns them to the replacement list on the
//! last release. Other policies may evict held entries, whose values remain
//! valid until the last handle is dropped.
//!
//! Capacity measures resident weight, not process memory. Held, removed
//! entries remain allocated and are reported separately by [`Statistics`].
//! Weighers, admission filters, removal listeners and value destructors run
//! outside shard locks. Hashing and key equality must not reenter the cache.

mod cache;
mod entry;
mod policy;

pub use cache::{BuildError, Builder, Cache, Change, Prepared, Probe, Removal, RemovalReason, Statistics};
pub use entry::Entry;
pub use equivalent::Equivalent;
pub use policy::{LfuConfig, LruConfig, Policy, Priority, S3FifoConfig};
