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

//! A foyer-like hybrid cache composed from moat's resident cache and engines.
//!
//! Logical keys use a stable identity encoding independently of the resident
//! cache's process hash. Full keys are retained in chunk envelopes and checked
//! before shared field views are returned. Origin loading remains under application control.

mod bytes;
mod cache;
mod catalog;
mod envelope;
mod error;
pub mod identity;
mod registry;
mod scheduler;
mod view;

pub use bytes::Bytes;
pub use cache::{Cache, Lookup, Options, Request, Statistics};
pub use catalog::{DiskOptions, DiskPolicy};
pub use error::{Error, Result};
pub use moat_cache_memory::{Equivalent, LfuConfig, LruConfig, Policy as MemoryPolicy, Priority, S3FifoConfig};
pub use registry::FillToken;
pub use view::EntryView;
