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

//! Borrowed lookups, weighted capacity, shared handles and explicit invalidation.

use moat_cache_memory::{Cache, Policy};

fn main() -> Result<(), moat_cache_memory::BuildError> {
    let cache = Cache::<String, Vec<u8>>::builder(64 << 20)
        .shards(16)
        .policy(Policy::Sieve)
        .weigher(|key, value, _| key.len() + value.len())
        .build()?;

    let first = cache.insert("object/42".into(), vec![1, 2, 3]);
    let borrowed_hit = cache.get("object/42").expect("resident entry");
    assert!(first.ptr_eq(&borrowed_hit));

    cache.insert("object/42".into(), vec![4, 5, 6]);
    assert!(!first.is_resident());
    assert_eq!(first.value(), &[1, 2, 3]);
    assert_eq!(cache.get("object/42").unwrap().value(), &[4, 5, 6]);

    cache.remove("object/42");
    assert!(cache.get("object/42").is_none());
    drop(first);
    drop(borrowed_hit);
    assert_eq!(cache.statistics().allocated_weight, 0);
    Ok(())
}
