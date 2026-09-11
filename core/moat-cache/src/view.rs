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

use moat_cache_memory::Entry;

use crate::Bytes;

/// An owned immutable KV view. Clones share storage instead of copying bytes.
/// Field borrows are tied to this handle; owned field views can outlive it.
/// Disk-backed fields retain the whole physical buffer and its byte credits.
#[derive(Clone)]
pub struct EntryView {
    entry: Entry<Bytes, Bytes, Bytes>,
}
impl From<Entry<Bytes, Bytes, Bytes>> for EntryView {
    fn from(entry: Entry<Bytes, Bytes, Bytes>) -> Self {
        Self { entry }
    }
}
impl EntryView {
    /// Borrows the complete canonical key.
    pub fn key(&self) -> &[u8] {
        self.entry.key()
    }
    /// Borrows the value without deserialization.
    pub fn value(&self) -> &[u8] {
        self.entry.value()
    }
    /// Borrows the opaque application properties.
    pub fn properties(&self) -> &[u8] {
        self.entry.properties()
    }
    /// Retains the key independently. A small slice still retains its buffer.
    pub fn key_view(&self) -> Bytes {
        self.entry.key().clone()
    }
    /// Retains the value independently without copying.
    pub fn value_view(&self) -> Bytes {
        self.entry.value().clone()
    }
    /// Retains the properties independently without copying.
    pub fn properties_view(&self) -> Bytes {
        self.entry.properties().clone()
    }
    /// Whether the cache currently retains this entry as a resident version.
    pub fn is_resident(&self) -> bool {
        self.entry.is_resident()
    }
    /// Whether another handle refers to the same entry version.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.entry.ptr_eq(&other.entry)
    }
}
