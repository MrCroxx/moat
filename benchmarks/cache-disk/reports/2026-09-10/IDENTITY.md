# XXH3-128 identity replacement

The current default is XXH3-128, identity version 2. The retained baseline
used cached BLAKE3, identity version 1. Both matrices use the bytes/view moat API.
Foyer's public routing wrapper uses the same identity algorithm and version
as moat in each matrix. Foyer's internal hashing and read verification are unchanged.

## CPU-only identity comparison

Five alternating pairs per key size, one million calls per sample, CPU 9.
The previous cached BLAKE3 implementation is the control. Each call includes
namespace, version and key length. Timings include the full ID implementation,
not just a bare hash primitive. The common benchmark lock excludes disk runs.

| Key bytes | BLAKE3 ns/ID | XXH3-128 ns/ID | Speedup |
| --- | ---: | ---: | ---: |
| 16 | 109.3 | 22.4 | 4.87x |
| 256 | 417.5 | 39.2 | 10.65x |
| 4096 | 3991.5 | 141.8 | 28.16x |

## End-to-end change at 128 requests per disk

These are separate full-matrix runs, not interleaved controls. Changed IDs
also change disk placement and record locations. Differences therefore include
run variability and placement; they do not isolate hash CPU cost.
All values are aggregate Kops/s. Slower samples remain in both datasets.

| Disks | Key / value bytes | Moat before | Moat after | Change | Foyer before | Foyer after | Change |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 16 / 128 | 742.0 | 742.9 | +0.1% | 802.2 | 845.2 | +5.4% |
| 1 | 16 / 4096 | 674.8 | 679.2 | +0.7% | 674.5 | 693.3 | +2.8% |
| 1 | 256 / 65536 | 193.5 | 189.1 | -2.3% | 214.0 | 214.0 | -0.0% |
| 1 | 4096 / 262144 | 57.8 | 57.8 | -0.0% | 57.8 | 57.8 | +0.1% |
| 20 | 16 / 128 | 3474.7 | 3560.3 | +2.5% | 582.2 | 585.2 | +0.5% |
| 20 | 16 / 4096 | 3495.4 | 3483.0 | -0.4% | 774.7 | 760.8 | -1.8% |
| 20 | 256 / 65536 | 3012.3 | 3094.4 | +2.7% | 619.4 | 720.3 | +16.3% |
| 20 | 4096 / 262144 | 1077.6 | 1078.3 | +0.1% | 586.2 | 589.0 | +0.5% |

## Process CPU cost at 128 requests per disk

Median process user+system CPU microseconds per completed API operation.
These include application, I/O and coordinator threads; IRQ and external
kernel-worker CPU are not included. Comparisons have the same run/placement
limitations as throughput. Lower CPU cost does not imply higher throughput.

| Disks | Key / value bytes | Moat before | Moat after | Foyer before | Foyer after |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | 16 / 128 | 2.92 | 2.83 | 3.73 | 3.54 |
| 1 | 16 / 4096 | 3.18 | 3.07 | 4.44 | 4.32 |
| 1 | 256 / 65536 | 8.70 | 8.68 | 11.88 | 11.54 |
| 1 | 4096 / 262144 | 26.22 | 22.65 | 40.28 | 36.94 |
| 20 | 16 / 128 | 16.88 | 16.49 | 69.97 | 68.91 |
| 20 | 16 / 4096 | 16.82 | 16.82 | 56.93 | 56.67 |
| 20 | 256 / 65536 | 19.47 | 18.91 | 68.32 | 60.63 |
| 20 | 4096 / 262144 | 40.84 | 36.02 | 97.80 | 95.56 |

The baseline single-disk 64 KiB / 128-request point has 14.7% spread;
its separate fresh-process check reached 213.9 Kops/s. The baseline also
has large-value tail excursions in follow-up runs. Small changes and
apparent gains against its slow point require caution.

See [methodology and limitations](README.md) and the [full matrix](REPORT.md).
