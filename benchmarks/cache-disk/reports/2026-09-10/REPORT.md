# XXH3-128 moat vs foyer

Moat returns owned shared views. Foyer returns Vec entries and retains
normal read verification.

Binary SHA-256: `2082f27db0b19ca472842b04fabbe95c94b8f8c83c24fe5b070cf223fa853470`.

Current identity: **XXH3-128, version 2**, for moat and foyer's routing wrapper.
See [the BLAKE3 comparison and ID microbenchmark](IDENTITY.md).
The single-disk 64 KiB workload includes slow samples; see the
[limitations](README.md) and [complete-sweep follow-up](RECHECK.md).

## Matched high-concurrency comparison

Median of three five-second samples at 128 logical requests per disk.
Throughput is aggregate completed API operations across the selected disks,
not mean per-disk IOPS. p99 is the median of per-sample p99, not a merged percentile.

| Disks | Key / value bytes | Moat Kops/s | Foyer Kops/s | Ratio | Moat p99 ms | Foyer p99 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 16 / 128 | 742.9 | 845.2 | 0.88x | 0.232 | 0.201 |
| 1 | 16 / 4096 | 679.2 | 693.3 | 0.98x | 0.249 | 0.250 |
| 1 | 256 / 65536 | 189.1 | 214.0 | 0.88x | 1.881 | 1.581 |
| 1 | 4096 / 262144 | 57.8 | 57.8 | 1.00x | 4.053 | 4.137 |
| 20 | 16 / 128 | 3560.3 | 585.2 | 6.08x | 1.483 | 29.573 |
| 20 | 16 / 4096 | 3483.0 | 760.8 | 4.58x | 1.508 | 27.394 |
| 20 | 256 / 65536 | 3094.4 | 720.3 | 4.30x | 1.698 | 20.136 |
| 20 | 4096 / 262144 | 1078.3 | 589.0 | 1.83x | 5.763 | 11.944 |

## Complete concurrency sweep

| Disks | Key / value bytes | Requests | Moat Kops/s | Foyer Kops/s | Ratio | Moat spread | Foyer spread |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 16 / 128 | 8 | 145.3 | 145.7 | 1.00x | 0.2% | 0.2% |
| 1 | 16 / 128 | 32 | 510.1 | 540.4 | 0.94x | 0.2% | 0.1% |
| 1 | 16 / 128 | 128 | 742.9 | 845.2 | 0.88x | 0.1% | 0.8% |
| 1 | 16 / 4096 | 8 | 135.8 | 135.2 | 1.00x | 0.3% | 0.0% |
| 1 | 16 / 4096 | 32 | 434.8 | 446.0 | 0.97x | 0.3% | 0.0% |
| 1 | 16 / 4096 | 128 | 679.2 | 693.3 | 0.98x | 0.3% | 0.2% |
| 1 | 256 / 65536 | 8 | 65.9 | 89.7 | 0.73x | 21.3% | 0.1% |
| 1 | 256 / 65536 | 32 | 155.8 | 206.8 | 0.75x | 0.3% | 0.1% |
| 1 | 256 / 65536 | 128 | 189.1 | 214.0 | 0.88x | 0.1% | 0.0% |
| 1 | 4096 / 262144 | 8 | 41.5 | 38.5 | 1.08x | 0.1% | 0.8% |
| 1 | 4096 / 262144 | 32 | 55.2 | 55.2 | 1.00x | 0.0% | 0.0% |
| 1 | 4096 / 262144 | 128 | 57.8 | 57.8 | 1.00x | 0.1% | 0.1% |
| 20 | 16 / 128 | 160 | 1561.2 | 578.4 | 2.70x | 0.3% | 0.2% |
| 20 | 16 / 128 | 640 | 3427.1 | 588.0 | 5.83x | 0.3% | 0.2% |
| 20 | 16 / 128 | 2560 | 3560.3 | 585.2 | 6.08x | 0.1% | 0.2% |
| 20 | 16 / 4096 | 160 | 1600.6 | 756.5 | 2.12x | 0.2% | 0.2% |
| 20 | 16 / 4096 | 640 | 3419.2 | 766.2 | 4.46x | 0.7% | 0.3% |
| 20 | 16 / 4096 | 2560 | 3483.0 | 760.8 | 4.58x | 0.6% | 0.2% |
| 20 | 256 / 65536 | 160 | 1337.4 | 700.6 | 1.91x | 0.3% | 0.4% |
| 20 | 256 / 65536 | 640 | 3015.7 | 718.6 | 4.20x | 0.1% | 0.3% |
| 20 | 256 / 65536 | 2560 | 3094.4 | 720.3 | 4.30x | 0.4% | 0.2% |
| 20 | 4096 / 262144 | 160 | 741.8 | 345.4 | 2.15x | 0.9% | 9.2% |
| 20 | 4096 / 262144 | 640 | 1022.5 | 452.3 | 2.26x | 0.0% | 4.9% |
| 20 | 4096 / 262144 | 2560 | 1078.3 | 589.0 | 1.83x | 0.0% | 1.2% |

Spread is (maximum minus minimum) / median throughput within the three samples.
A wide spread limits conclusions about small effects; slower samples are retained.

See [methodology and limitations](README.md) for workload, worker configuration,
validation and the different return/read-checksum contracts.
