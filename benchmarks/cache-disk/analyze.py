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

"""Aggregate completed benchmark logs without discarding slower configurations."""

import csv
import json
from pathlib import Path
import statistics
import sys

if len(sys.argv) != 2:
    raise SystemExit("usage: analyze.py RESULT_DIRECTORY")
root = Path(sys.argv[1])
records = []
prefills = []
for path in sorted(root.glob("*-d*-k*-v*.log")):
    lines = path.read_text().splitlines()
    configs = [json.loads(line[7:]) for line in lines if line.startswith("CONFIG ")]
    assert len(configs) == 1, path
    c = configs[0]
    for line in lines:
        if line.startswith("PREFILL "):
            row = json.loads(line[8:])
            write_bytes = sum(
                (a[6] - b[6]) * 512
                for a, b in zip(row["disk_after"], row["disk_before"])
            )
            prefills.append(
                dict(
                    engine=c["engine"],
                    disks=len(c["disks"]),
                    key_bytes=c["key_bytes"],
                    value_bytes=c["value_bytes"],
                    operations=row["operations"],
                    seconds=row["seconds"],
                    ops_per_second=row["ops_per_second"],
                    logical_gib_s=row["logical_bytes_per_second"] / (1 << 30),
                    physical_gib_s=write_bytes / row["seconds"] / (1 << 30),
                    physical_write_bytes=write_bytes,
                    write_amplification=write_bytes
                    / (row["operations"] * c["value_bytes"]),
                    cpu_cores=row["cpu_cores"],
                )
            )
        if not line.startswith("RESULT "):
            continue
        row = json.loads(line[7:])
        assert row["operations"] > 0 and row["seconds"] >= c["seconds"]
        physical_bytes = sum(d[2] * 512 for d in row["disk_delta"])
        physical_reads = sum(d[0] for d in row["disk_delta"])
        assert all(d[0] > 0 for d in row["disk_delta"]), (path, "inactive disk")
        records.append(
            dict(
                engine=c["engine"],
                disks=len(c["disks"]),
                key_bytes=c["key_bytes"],
                value_bytes=c["value_bytes"],
                clients=row["clients"],
                repeat=row["repeat"],
                ops_per_second=row["ops_per_second"],
                logical_gib_s=row["logical_bytes_per_second"] / (1 << 30),
                physical_gib_s=physical_bytes / row["seconds"] / (1 << 30),
                physical_iops=physical_reads / row["seconds"],
                p50_us=row["p50_us"],
                p99_us=row["p99_us"],
                p999_us=row["p999_us"],
                cpu_cores=row["cpu_cores"],
                cpu_us_per_op=row["cpu_seconds"] * 1e6 / row["operations"],
                rss_mib=row["max_rss_kib"] / 1024,
                read_amplification=physical_bytes
                / (row["operations"] * c["value_bytes"]),
                min_disk_iops=min(d[0] for d in row["disk_delta"]) / row["seconds"],
                max_disk_iops=max(d[0] for d in row["disk_delta"]) / row["seconds"],
            )
        )


def write_csv(name, rows):
    if not rows:
        return
    with (root / name).open("w") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


write_csv("samples.csv", records)
write_csv("prefill.csv", prefills)
groups = {}
keys = ["engine", "disks", "key_bytes", "value_bytes", "clients"]
for row in records:
    groups.setdefault(tuple(row[k] for k in keys), []).append(row)
summary = []
for group, rows in sorted(groups.items()):
    entry = dict(zip(keys, group))
    entry["samples"] = len(rows)
    for field in records[0]:
        if field in keys or field == "repeat":
            continue
        entry[field] = statistics.median(r[field] for r in rows)
    entry["min_ops_per_second"] = min(r["ops_per_second"] for r in rows)
    entry["max_ops_per_second"] = max(r["ops_per_second"] for r in rows)
    summary.append(entry)
write_csv("summary.csv", summary)
print(
    json.dumps(
        dict(samples=len(records), groups=len(summary), prefills=len(prefills)),
        indent=2,
    )
)
for disks in [1, 20]:
    for key_size, value_size in sorted(
        {(r["key_bytes"], r["value_bytes"]) for r in summary}
    ):
        pair = []
        for engine in ["moat", "foyer"]:
            candidates = [
                r
                for r in summary
                if (r["engine"], r["disks"], r["key_bytes"], r["value_bytes"])
                == (engine, disks, key_size, value_size)
            ]
            if candidates:
                pair.append(max(candidates, key=lambda r: r["ops_per_second"]))
        if len(pair) == 2:
            a, b = pair
            print(
                f"d={disks} k={key_size} v={value_size}: moat={a['ops_per_second']:.0f} ({a['clients']} clients), foyer={b['ops_per_second']:.0f} ({b['clients']} clients), ratio={a['ops_per_second'] / b['ops_per_second']:.3f}"
            )
