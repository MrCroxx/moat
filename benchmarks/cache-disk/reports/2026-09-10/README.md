Disk-cache measurements: methodology and limits
===============================================

These results come from one Linux x86-64 server with twenty NVMe data devices.
System-array devices were excluded. Hostnames, addresses, device serials,
mount tables, local paths and deployment scripts are intentionally omitted.
The numeric samples are unchanged; original operational logs remain private.

Configuration
--------------

- One or twenty data disks; a 64-GiB window per disk, 16-MiB engine segments.
- Matched application/I/O workers: 2/1 on one disk and 40/20 on twenty disks.
  CPU affinity is identical within each pair, using separate application and
  I/O cores. Moat has an additional mutation-coordinator thread.
- Direct I/O through io_uring, depth 256, 512-MiB configured pool per disk.
- Key/value bytes: 16/128, 16/4096, 256/65536 and 4096/262144.
- Records per disk: `max(2048, min(131072, 512 MiB / (key + value)))`.
- Resident admission disabled; uniform disk hits, no origin loader.
- Per-disk concurrency: 8, 32 and 128; two seconds of warmup followed by three
  five-second samples at each level. Every prefilled key is checked first.
- Foyer uses twenty independent caches on one shared application runtime for
  the multi-disk case, with the same XXH3-128/rendezvous routing as moat.
- Moat returns shared views; foyer returns Vec entries. Moat normal engine
  reads skip CRC verification, while foyer retains normal XXHash64 verification.
  Neither side adds a benchmark payload checksum scan. Return and read-checksum
  contracts differ, so these are cache API comparisons, not engine ceilings.
- Release builds use Rust 1.98.0 with no target-CPU overrides. Foyer is pinned to
  `dd46245c45071d1036331e4e2c48e15386017b96`; XXH3 uses `xxhash-rust` 0.8.18.

Reading the results
--------------------

The [full matrix](REPORT.md) reports aggregate completed API operations across
all selected disks, not mean per-disk or physical IOPS. Latency columns are
medians of per-sample percentiles, not a merged percentile. The numeric
[samples](samples.csv), [summary](summary.csv) and [prefill](prefill.csv) retain
physical block counters, process CPU cost, RSS and all observed variation.

The [identity comparison](IDENTITY.md) includes five alternating CPU-only pairs
per key size, one million calls per sample on one pinned CPU. Its BLAKE3 control
retains the former cached initialization. ID microbenchmarks and disk runs use
an exclusive benchmark lock. The before/after disk matrices are separate runs;
changed IDs also change placement. Their deltas do not isolate hash CPU cost.

The [follow-up](RECHECK.md) repeats the entire single-disk 64-KiB concurrency
sweep with XXH3, the frozen BLAKE3 version, then XXH3 again. All 27 additional
[samples](recheck.csv) are included. Medians recover near 214 Kops/s at 128
requests, but an XXH3 sample still drops to 183.8 Kops/s. The main matrix's low
values remain in the report. The cause of intermittent slowdowns is unresolved.

Small-entry prefill has high measured write amplification: on one disk with
128-byte values, moat writes about 32.4 times the logical value bytes. Its
compact layout alone does not establish effective packing in this workload.
Single-disk small-entry reads also trail foyer in some configurations. These
remain performance investigation items.

The matrix does not measure sustained mixed writes, capacity-pressure GC,
origin loading or long-held external views. Prefill waits for completed writes
and a common device sync boundary; it is not a comparison of synchronous insert
latency or identical durability semantics. Warm direct reads are not a claim
about cold-media latency. Raw block I/O must be measured on explicitly reviewed
hardware; the public file runner is only a portable correctness exercise.
