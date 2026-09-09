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

//! Pool controls shared by the device and engine benchmarks.

use moat_common::{BufferPool, HugePages, arena::Backing};

pub fn policy() -> HugePages {
    match std::env::var("MOAT_BENCH_HUGE_PAGES").as_deref().unwrap_or("preferred") {
        "disabled" => HugePages::Disabled,
        "preferred" => HugePages::Preferred,
        "required" => HugePages::Required,
        other => panic!("unknown huge page policy: {other}"),
    }
}

/// Check explicit backing before timing, so a fallback cannot masquerade as
/// the intended experiment. THP promotion still needs an external smaps audit.
pub fn inspect(pool: &BufferPool, worker: usize) {
    let expected = std::env::var("MOAT_BENCH_EXPECT_BACKING")
        .ok()
        .map(|value| match value.as_str() {
            "1g" => Backing::Huge1G,
            "2m" => Backing::Huge2M,
            "thp" => Backing::Transparent,
            "plain" => Backing::Plain,
            other => {
                eprintln!("unknown expected arena backing: {other}");
                std::process::exit(1);
            }
        });
    if expected.is_some() || std::env::var_os("MOAT_BENCH_REPORT_POOL").is_some() {
        for arena in pool.arenas() {
            eprintln!(
                "pool worker={worker} bytes={} backing={:?} base={:p}",
                arena.len(),
                arena.backing(),
                arena.as_ptr()
            );
            if let Some(expected) = expected
                && arena.backing() != expected
            {
                // A worker panic can strand peers at their startup barrier.
                eprintln!("unexpected pool backing on worker {worker}: wanted {expected:?}");
                std::process::exit(1);
            }
        }
    }
}
