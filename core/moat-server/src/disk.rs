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

//! NVMe disk discovery through sysfs.
//!
//! Disks are identified by controller serial and namespace, never by their
//! `/dev/nvmeXnY` name, which changes across reboots. Discovery also reports
//! whether a namespace is *in use* by something else (a partition table, an
//! md or dm holder, a mount, swap): such disks are never data disks and a
//! caller must not format them. This is how the system disk, typically an md
//! RAID over two small NVMe drives, stays out of the data set.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// One NVMe namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NvmeDisk {
    /// Kernel block device name, e.g. `nvme3n1`.
    pub name: String,
    /// Device node, e.g. `/dev/nvme3n1`.
    pub path: PathBuf,
    /// Controller serial number (trimmed).
    pub serial: String,
    /// Controller model (trimmed).
    pub model: String,
    /// Capacity in bytes.
    pub capacity: u64,
    /// Logical block size in bytes.
    pub block_size: u32,
    /// NUMA node of the controller, if known.
    pub numa_node: Option<usize>,
    /// Why the disk is unavailable as a data disk, if it is.
    pub in_use: Option<InUse>,
}

/// What claims a disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InUse {
    /// The namespace is partitioned.
    Partitioned,
    /// A stacked device (md, dm) sits on top of it.
    Holder(String),
    /// A filesystem is mounted from it.
    Mounted(String),
    /// It is an active swap device.
    Swap,
}

impl NvmeDisk {
    /// Whether the disk may be used (and formatted) as a data disk.
    pub fn is_available(&self) -> bool {
        self.in_use.is_none()
    }
}

fn read_trimmed(path: &Path) -> Option<String> {
    fs::read_to_string(path).ok().map(|s| s.trim().to_string())
}

#[cfg(any(target_os = "linux", test))]
fn read_num<T: std::str::FromStr>(path: &Path) -> Option<T> {
    read_trimmed(path)?.parse().ok()
}

/// Whether `name` is a whole NVMe namespace (`nvme<ctrl>n<ns>`, no partition).
#[cfg(any(target_os = "linux", test))]
fn is_namespace(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("nvme") else {
        return false;
    };
    let mut parts = rest.split('n');
    let ctrl = parts.next().unwrap_or("");
    let ns = parts.next().unwrap_or("");
    parts.next().is_none()
        && !ctrl.is_empty()
        && !ns.is_empty()
        && ctrl.chars().all(|c| c.is_ascii_digit())
        && ns.chars().all(|c| c.is_ascii_digit())
}

/// Lists every NVMe namespace on the machine, in name order.
#[cfg(target_os = "linux")]
pub fn discover() -> io::Result<Vec<NvmeDisk>> {
    discover_in(Path::new("/sys/class/block"), Path::new("/proc"))
}

/// NVMe discovery is Linux-only; open regular files explicitly on other platforms.
#[cfg(not(target_os = "linux"))]
pub fn discover() -> io::Result<Vec<NvmeDisk>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "NVMe discovery is only supported on Linux",
    ))
}

#[cfg(any(target_os = "linux", test))]
fn discover_in(block: &Path, proc: &Path) -> io::Result<Vec<NvmeDisk>> {
    let mounts = fs::read_to_string(proc.join("mounts")).unwrap_or_default();
    let swaps = fs::read_to_string(proc.join("swaps")).unwrap_or_default();
    let mut disks = Vec::new();
    let mut names: Vec<String> = fs::read_dir(block)?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| is_namespace(n))
        .collect();
    names.sort_by_key(|a| natural_key(a));
    for name in names {
        let dir = block.join(&name);
        let device = dir.join("device");
        let sectors: u64 = read_num(&dir.join("size")).unwrap_or(0);
        let block_size: u32 = read_num(&dir.join("queue/logical_block_size")).unwrap_or(512);
        let path = PathBuf::from(format!("/dev/{name}"));
        let in_use = in_use(&dir, &name, &path, &mounts, &swaps);
        let numa_node = read_num::<i64>(&device.join("device/numa_node"))
            .or_else(|| read_num::<i64>(&device.join("numa_node")))
            .filter(|&n| n >= 0)
            .map(|n| n as usize);
        disks.push(NvmeDisk {
            serial: read_trimmed(&device.join("serial")).unwrap_or_default(),
            model: read_trimmed(&device.join("model")).unwrap_or_default(),
            capacity: sectors * 512,
            block_size,
            numa_node,
            in_use,
            name,
            path,
        });
    }
    Ok(disks)
}

#[cfg(any(target_os = "linux", test))]
fn in_use(dir: &Path, name: &str, path: &Path, mounts: &str, swaps: &str) -> Option<InUse> {
    if let Ok(entries) = fs::read_dir(dir)
        && entries
            .filter_map(|e| e.ok())
            .any(|e| e.file_name().to_string_lossy().starts_with(&format!("{name}p")))
    {
        return Some(InUse::Partitioned);
    }
    if let Ok(mut holders) = fs::read_dir(dir.join("holders"))
        && let Some(Ok(h)) = holders.next()
    {
        return Some(InUse::Holder(h.file_name().to_string_lossy().into_owned()));
    }
    let node = path.to_string_lossy();
    for line in mounts.lines() {
        let mut f = line.split_whitespace();
        if let (Some(dev), Some(at)) = (f.next(), f.next())
            && dev == node
        {
            return Some(InUse::Mounted(at.to_string()));
        }
    }
    if swaps
        .lines()
        .skip(1)
        .any(|l| l.split_whitespace().next() == Some(&*node))
    {
        return Some(InUse::Swap);
    }
    None
}

/// Sort key that orders `nvme2n1` before `nvme10n1`.
#[cfg(any(target_os = "linux", test))]
fn natural_key(name: &str) -> Vec<u64> {
    name.split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse().ok())
        .collect()
}

/// The CPUs of NUMA node `node`, from sysfs. Empty if unknown.
pub fn cpus_of_node(node: usize) -> Vec<usize> {
    read_trimmed(Path::new(&format!("/sys/devices/system/node/node{node}/cpulist")))
        .map(|s| parse_cpu_list(&s))
        .unwrap_or_default()
}

/// The CPUs this process may run on.
#[cfg(target_os = "linux")]
pub fn online_cpus() -> Vec<usize> {
    // SAFETY: a zeroed cpu_set_t is a valid set for sched_getaffinity to fill.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
            return Vec::new();
        }
        (0..libc::CPU_SETSIZE as usize)
            .filter(|&c| libc::CPU_ISSET(c, &set))
            .collect()
    }
}

/// Logical worker indices based on available parallelism; not affinity IDs.
#[cfg(not(target_os = "linux"))]
pub fn online_cpus() -> Vec<usize> {
    (0..std::thread::available_parallelism().map(usize::from).unwrap_or(1)).collect()
}

/// Parses a kernel CPU list such as `0-3,8,10-11`.
pub fn parse_cpu_list(s: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in s.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((a, b)) => {
                if let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>()) {
                    cpus.extend(a..=b);
                }
            }
            None => {
                if let Ok(c) = part.parse() {
                    cpus.push(c);
                }
            }
        }
    }
    cpus
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_names() {
        assert!(is_namespace("nvme0n1"));
        assert!(is_namespace("nvme12n3"));
        assert!(!is_namespace("nvme0n1p1"));
        assert!(!is_namespace("nvme0"));
        assert!(!is_namespace("sda"));
        assert!(!is_namespace("nvme0c0n1"));
    }

    #[test]
    fn cpu_lists_and_natural_order() {
        assert_eq!(parse_cpu_list("0-3,8,10-11"), vec![0, 1, 2, 3, 8, 10, 11]);
        assert_eq!(parse_cpu_list(""), Vec::<usize>::new());
        let mut names = vec!["nvme10n1", "nvme2n1", "nvme1n2", "nvme1n1"];
        names.sort_by_key(|n| natural_key(n));
        assert_eq!(names, vec!["nvme1n1", "nvme1n2", "nvme2n1", "nvme10n1"]);
    }

    #[test]
    fn discovery_from_a_fake_sysfs() {
        let dir = tempfile::tempdir().unwrap();
        let block = dir.path().join("block");
        let proc = dir.path().join("proc");
        fs::create_dir_all(&proc).unwrap();
        for (name, serial, size, partitioned, holder) in [
            ("nvme0n1", "S1", 1u64 << 30, false, false),
            ("nvme1n1", "S2", 2u64 << 30, true, false),
            ("nvme2n1", "S3", 3u64 << 30, false, true),
            ("nvme3n1", "S4", 4u64 << 30, false, false),
        ] {
            let d = block.join(name);
            fs::create_dir_all(d.join("device/device")).unwrap();
            fs::create_dir_all(d.join("queue")).unwrap();
            fs::create_dir_all(d.join("holders")).unwrap();
            fs::write(d.join("size"), format!("{}\n", size / 512)).unwrap();
            fs::write(d.join("queue/logical_block_size"), "4096\n").unwrap();
            fs::write(d.join("device/serial"), format!("{serial}  \n")).unwrap();
            fs::write(d.join("device/model"), "Test Drive\n").unwrap();
            fs::write(d.join("device/device/numa_node"), "1\n").unwrap();
            if partitioned {
                fs::create_dir_all(d.join(format!("{name}p1"))).unwrap();
            }
            if holder {
                fs::create_dir_all(d.join("holders/md0")).unwrap();
            }
        }
        // A non-NVMe device and a partition entry must be ignored.
        fs::create_dir_all(block.join("sda")).unwrap();
        fs::create_dir_all(block.join("nvme1n1p1")).unwrap();
        fs::write(proc.join("mounts"), "/dev/nvme3n1 /data ext4 rw 0 0\n").unwrap();
        fs::write(proc.join("swaps"), "Filename Type Size Used Priority\n").unwrap();

        let disks = discover_in(&block, &proc).unwrap();
        assert_eq!(disks.len(), 4);
        assert_eq!(disks[0].serial, "S1");
        assert_eq!(disks[0].capacity, 1 << 30);
        assert_eq!(disks[0].block_size, 4096);
        assert_eq!(disks[0].numa_node, Some(1));
        assert!(disks[0].is_available());
        assert_eq!(disks[1].in_use, Some(InUse::Partitioned));
        assert_eq!(disks[2].in_use, Some(InUse::Holder("md0".into())));
        assert_eq!(disks[3].in_use, Some(InUse::Mounted("/data".into())));
    }
}
