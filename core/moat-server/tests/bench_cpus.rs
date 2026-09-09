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

//! CPU placement tests for the benchmark load generators.

#[path = "../benches/support/cpus.rs"]
mod cpus;

#[test]
fn topology_order_contains_each_selected_cpu_once() {
    let mut expected = moat_server::disk::online_cpus();
    if expected.len() > 2 {
        expected.drain(..2);
    }
    let mut actual = cpus::spread();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
}

#[cfg(test)]
mod tests {
    use super::cpus::spread_in;
    use std::fs;

    #[test]
    fn spreads_caches_before_cores_and_smt_with_offline_siblings() {
        let temp = tempfile::tempdir().unwrap();
        for cpu in [0, 1, 2, 3, 4, 5, 6] {
            let dir = temp.path().join(format!("cpu{cpu}"));
            fs::create_dir_all(dir.join("topology")).unwrap();
            fs::create_dir_all(dir.join("cache/index3")).unwrap();
            let core = cpu % 4;
            fs::write(
                dir.join("topology/thread_siblings_list"),
                format!("{core},{}", core + 4),
            )
            .unwrap();
            let cache = if core < 2 { "0-1,4-5" } else { "2-3,6-7" };
            fs::write(dir.join("cache/index3/level"), "3").unwrap();
            fs::write(dir.join("cache/index3/type"), "Unified").unwrap();
            fs::write(dir.join("cache/index3/shared_cpu_list"), cache).unwrap();
        }
        assert_eq!(
            spread_in(temp.path(), &[0, 1, 2, 3, 4, 5, 6]),
            vec![0, 2, 1, 3, 4, 6, 5]
        );
        assert_eq!(spread_in(temp.path(), &[1, 2, 3, 5, 6]), vec![1, 2, 3, 5, 6]);
        assert_eq!(spread_in(temp.path(), &[]), Vec::<usize>::new());
    }

    #[test]
    fn missing_topology_keeps_available_cpus() {
        let temp = tempfile::tempdir().unwrap();
        assert_eq!(spread_in(temp.path(), &[2, 5, 9]), vec![2, 5, 9]);
    }
}
