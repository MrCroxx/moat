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

//! Bounded asynchronous chunk access over moat's engine pipelines.
//!
//! Each disk has a worker that owns its queue, writer and reader. Compatible
//! reads share one physical operation and one reference-counted buffer. Reads,
//! overwrites and deletions of a ChunkId follow admission order; unrelated IDs
//! can be in flight together. No source loader, logical key codec or cache
//! eviction policy is embedded here.
//!
//! Requests are admitted when a method returns its [`Request`], before polling
//! the future. Dropping a future abandons its reply, not an accepted mutation.
//! Completed read buffers retain byte credits until their last shared owner
//! drops them. [`Store::close`] drains admitted operations and releases engines.

mod budget;
mod command;
mod delivery;
mod request;
mod store;
mod worker;

pub use delivery::CompletionExecutor;
pub use request::{Chunk, DeleteResult, Error, Request, Result};
pub use store::{DiskInfo, InventoryEntry, Options, ReadLocation, Statistics, Store};
