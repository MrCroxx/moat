Disk cache comparison
=====================

This standalone workspace compares `moat-cache` with foyer pinned to
[`dd46245c45071d1036331e4e2c48e15386017b96`](https://github.com/foyer-rs/foyer/tree/dd46245c45071d1036331e4e2c48e15386017b96).
Foyer dependencies are confined to the comparison workspace.

The [published matrix](reports/2026-09-10/REPORT.md) contains single-disk and
20-disk results for four key/value sizes and three matched concurrency levels.
[Methodology and limitations](reports/2026-09-10/README.md) describe the run.
[Identity costs](reports/2026-09-10/IDENTITY.md) and
[the variable-workload follow-up](reports/2026-09-10/RECHECK.md) retain the
negative cases as well as improvements.

Build and validate on Linux
---------------------------

```sh
CARGO_TARGET_DIR=target/cache-disk-compare cargo build --release --locked --manifest-path benchmarks/cache-disk/Cargo.toml
CARGO_TARGET_DIR=target/cache-disk-compare cargo clippy --locked --manifest-path benchmarks/cache-disk/Cargo.toml --all-targets -- -D warnings
python3 benchmarks/cache-disk/run_files.py --binary target/cache-disk-compare/release/moat-cache-disk-compare --seconds 1 --repeats 1
```

The runner creates two new disposable 1-GiB files and uses the same three
available CPUs for both implementations. It rejects an existing output
directory. By default, configurations, files and logs stay in ignored `local/`.
It never selects a raw device. File-backed smoke results validate API paths;
they do not reproduce raw-NVMe performance. Hostname and paths in generated
configurations are runtime inputs and must not be committed.

Use `--key-bytes`, `--value-bytes`, `--seconds` and `--repeats` to adjust the
file workload. Physical block-device counters are unavailable for regular
files. Choose CPU affinity explicitly for controlled measurements.

Raw-device configuration
-------------------------

The Rust binary also accepts an operator-supplied JSON configuration. Raw
targets require an exact host match, a serial allowlist in `disks`, an exact
`expected_capacity` per disk, and a nonempty `forbidden_serials` list covering
system devices. Partitions, mounted targets and devices with holders or
partitions are rejected. All listed targets must be disposable: the benchmark
formats and overwrites the configured `bytes_per_disk` window.

Provision and audit raw devices outside this repository. No machine-specific
allowlist, device names, serials, SSH automation or host inventory is shipped.
The published measurements used additional before/after array and SMART checks;
the generic harness does not replace those operator checks. Its publication
cleanup replaces fixed host-specific guards with explicit configuration;
the measured cache code and workload loop remain unchanged.

For completed raw-device logs, `python3 benchmarks/cache-disk/analyze.py DIRECTORY`
generates numeric sample, summary and prefill CSVs. It retains every concurrency
level and slow sample and requires reads on every configured disk. Review and
sanitize outputs before publishing them. Private raw runs and profiling files
remain excluded through `.gitignore`.
