# Copyright 2026- Moat Project Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compare disk-cache APIs using new disposable files on Linux."""

import argparse
import json
import os
import socket
import subprocess
import time
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--binary", type=Path, required=True)
parser.add_argument("--output", type=Path)
parser.add_argument("--key-bytes", type=int, default=16)
parser.add_argument("--value-bytes", type=int, default=4096)
parser.add_argument("--seconds", type=int, default=5)
parser.add_argument("--repeats", type=int, default=3)
args = parser.parse_args()
cpus = sorted(os.sched_getaffinity(0))
if len(cpus) < 3:
    parser.error("at least three allowed CPUs are required")
root = args.output or Path(__file__).resolve().parent / "local" / str(time.time_ns())
root.mkdir(parents=True, exist_ok=False)
binary = args.binary.resolve(strict=True)
for engine in ["moat", "foyer"]:
    disk = root / (engine + ".img")
    with disk.open("xb") as output:
        output.truncate(1 << 30)
    config = {
        "host": socket.gethostname(),
        "engine": engine,
        "disks": [{"path": str(disk.resolve()), "serial": ""}],
        "bytes_per_disk": 1 << 30,
        "records_per_disk": 256,
        "key_bytes": args.key_bytes,
        "value_bytes": args.value_bytes,
        "clients": 128,
        "client_levels": [8, 32, 128],
        "runtime_cpus": cpus[1:3],
        "io_cpus": cpus[:1],
        "seconds": args.seconds,
        "repeats": args.repeats,
        "pool_bytes_per_disk": 64 << 20,
        "moat_verify_reads": False,
        "moat_batched_completions": True,
    }
    name = f"{engine}-d1-k{args.key_bytes}-v{args.value_bytes}"
    path = root / (name + ".json")
    path.write_text(json.dumps(config, indent=2) + "\n")
    log_path = root / (name + ".log")
    with log_path.open("w") as log:
        subprocess.run(
            [str(binary), str(path)], stdout=log, stderr=subprocess.STDOUT, check=True
        )
    lines = log_path.read_text().splitlines()
    assert "VERIFIED 256" in lines
    assert sum(line.startswith("RESULT ") for line in lines) == args.repeats * 3
print(root)
