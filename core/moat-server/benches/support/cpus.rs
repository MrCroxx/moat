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

//! CPU ordering for the load generator: distribute work across shared caches
//! before using more cores in the same cache, and use SMT siblings last.

use std::{collections::BTreeMap, fs, path::Path};

use moat_server::disk;

pub fn spread() -> Vec<usize> {
    let mut online = disk::online_cpus();
    if online.len() > 2 {
        online.drain(..2);
    }
    spread_in(Path::new("/sys/devices/system/cpu"), &online)
}

pub(super) fn spread_in(root: &Path, online: &[usize]) -> Vec<usize> {
    let mut caches: BTreeMap<Vec<usize>, BTreeMap<Vec<usize>, Vec<usize>>> = BTreeMap::new();
    for &cpu in online {
        let dir = root.join(format!("cpu{cpu}"));
        let siblings = fs::read_to_string(dir.join("topology/thread_siblings_list"))
            .map(|s| disk::parse_cpu_list(&s))
            .unwrap_or_else(|_| vec![cpu]);
        let cache = fs::read_dir(dir.join("cache"))
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let path = entry.path();
                let level = fs::read_to_string(path.join("level"))
                    .ok()?
                    .trim()
                    .parse::<u32>()
                    .ok()?;
                let kind = fs::read_to_string(path.join("type")).ok()?;
                if kind.trim() == "Instruction" {
                    return None;
                }
                let shared = fs::read_to_string(path.join("shared_cpu_list")).ok()?;
                Some((level, disk::parse_cpu_list(&shared)))
            })
            .max_by_key(|(level, _)| *level)
            .map(|(_, cpus)| cpus)
            .unwrap_or_else(|| online.to_vec());
        caches.entry(cache).or_default().entry(siblings).or_default().push(cpu);
    }
    let groups: Vec<Vec<Vec<usize>>> = caches
        .into_values()
        .map(|cores| cores.into_values().collect())
        .collect();
    let cores = groups.iter().map(Vec::len).max().unwrap_or(0);
    let siblings = groups.iter().flatten().map(Vec::len).max().unwrap_or(0);
    let mut ordered = Vec::with_capacity(online.len());
    for sibling in 0..siblings {
        for core in 0..cores {
            for group in &groups {
                if let Some(&cpu) = group.get(core).and_then(|c| c.get(sibling)) {
                    ordered.push(cpu);
                }
            }
        }
    }
    ordered
}
