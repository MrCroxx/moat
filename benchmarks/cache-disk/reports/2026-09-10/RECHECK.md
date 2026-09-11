# Single-disk 64 KiB follow-up

The full XXH3 matrix contained a variable 8-request phase and low
32/128-request phases. This sequence repeats the entire 8/32/128 sweep in
each fresh process, using XXH3, the frozen BLAKE3 binary, then XXH3 again.
All three phases retain the same worker/affinity, value checks and read policy.
Each process reformats and prefills its allowlisted data window as in the main matrix.
This tests reproducibility across complete sweeps; changed IDs also change placement.

| Phase | Requests | Median Kops/s | Min-max Kops/s | p99 ms | CPU us/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| xxh3-before | 8 | 93.9 | 93.9-94.0 | 0.127 | 14.68 |
| xxh3-before | 32 | 207.9 | 207.7-208.1 | 0.321 | 8.16 |
| xxh3-before | 128 | 213.9 | 183.8-214.0 | 1.568 | 7.97 |
| blake3-control | 8 | 93.8 | 93.8-93.9 | 0.127 | 14.96 |
| blake3-control | 32 | 208.0 | 208.0-208.1 | 0.320 | 8.35 |
| blake3-control | 128 | 213.9 | 213.9-214.0 | 1.583 | 8.17 |
| xxh3-after | 8 | 94.2 | 94.2-94.2 | 0.127 | 14.65 |
| xxh3-after | 32 | 208.2 | 208.1-208.2 | 0.320 | 8.20 |
| xxh3-after | 128 | 214.0 | 213.9-214.0 | 1.559 | 7.99 |

These follow-ups do not replace low points in the full matrix. See the
[full matrix](REPORT.md), [identity comparison](IDENTITY.md) and
[raw follow-up samples](recheck.csv).
