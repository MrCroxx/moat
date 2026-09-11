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
    borrow::Borrow,
    fmt,
    hash::{Hash, Hasher},
    ops::{Deref, Range},
    sync::Arc,
};

use moat_cache_store::Chunk;

use crate::{Error, Result};

#[derive(Clone)]
enum Backing {
    Heap(Arc<[u8]>),
    Disk(Arc<Chunk>),
}
impl Backing {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Heap(bytes) => bytes,
            Self::Disk(chunk) => chunk,
        }
    }
}

/// A cheap shared slice over heap bytes or an adapter read buffer.
///
/// Disk field views share the original read buffer. They consume adapter
/// read-byte credits while resident or externally held. `to_vec()` makes an
/// independent heap copy; credits return only after all buffer owners drop.
#[derive(Clone)]
pub struct Bytes {
    backing: Backing,
    range: Range<usize>,
}
impl Bytes {
    pub(crate) fn retain(&self) -> bool {
        match &self.backing {
            Backing::Heap(_) => true,
            Backing::Disk(chunk) => chunk.try_reserve_retention(),
        }
    }
    /// Returns a shared subrange, rejecting reversed or out-of-bounds ranges.
    pub fn slice(&self, range: Range<usize>) -> Result<Self> {
        if range.start > range.end || range.end > self.len() {
            return Err(Error::Invalid("byte range out of bounds"));
        }
        Ok(Self {
            backing: self.backing.clone(),
            range: self.range.start + range.start..self.range.start + range.end,
        })
    }
    /// Whether two views share the same backing allocation, even if their ranges differ.
    pub fn shares_backing(&self, other: &Self) -> bool {
        match (&self.backing, &other.backing) {
            (Backing::Heap(a), Backing::Heap(b)) => Arc::ptr_eq(a, b),
            (Backing::Disk(a), Backing::Disk(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}
impl Default for Bytes {
    fn default() -> Self {
        Self::from(Arc::<[u8]>::from([]))
    }
}
impl Borrow<[u8]> for Bytes {
    fn borrow(&self) -> &[u8] {
        self
    }
}
impl From<Vec<u8>> for Bytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from(Arc::<[u8]>::from(bytes))
    }
}
impl From<Arc<[u8]>> for Bytes {
    fn from(bytes: Arc<[u8]>) -> Self {
        let len = bytes.len();
        Self {
            backing: Backing::Heap(bytes),
            range: 0..len,
        }
    }
}
impl From<Arc<Chunk>> for Bytes {
    fn from(chunk: Arc<Chunk>) -> Self {
        let len = chunk.len();
        Self {
            backing: Backing::Disk(chunk),
            range: 0..len,
        }
    }
}
impl Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.backing.bytes()[self.range.clone()]
    }
}
impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}
impl PartialEq for Bytes {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}
impl Eq for Bytes {}
impl Hash for Bytes {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}
impl fmt::Debug for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bytes").field("len", &self.len()).finish()
    }
}
