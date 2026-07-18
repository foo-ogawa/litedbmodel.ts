# Latency: ts-IR-interpreter vs rust-native vs go-native (sqlite in-proc)

Same 4 ops, same seed sqlite (C engine in every cell: better-sqlite3 / rusqlite-bundled /
mattn-go-sqlite3), same iteration count. Whole hot path timed (bind + exec + decode into the
typed result). p50/p99 in µs; ops/sec = 1e6 / mean latency (single-thread serial). Verbatim.

Two speedup framings are shown so nothing hides: **p50** (median, robust) and **throughput**
(mean-based, ops/sec) — they diverge for writes because sqlite fsync gives a heavy tail.

| op | cell | p50 µs | p99 µs | mean µs | ops/sec | p50 vs ts-IR | throughput vs ts-IR |
|---|---|--:|--:|--:|--:|--:|--:|
| findUnique (point read) | ts-IR (interp) | 11.58 | 20.46 | 12.60 | 79365 | — (baseline) | — (baseline) |
|  | rust-native | 6.96 | 12.46 | 7.31 | 136786 | 1.66× faster | 1.72× faster |
|  | go-native | 15.75 | 65.21 | 20.68 | 48366 | 1.36× slower | 1.64× slower |
| relComments (batched relation, 4 children) | ts-IR (interp) | 38.13 | 53.50 | 39.46 | 25345 | — (baseline) | — (baseline) |
|  | rust-native | 53.42 | 67.92 | 54.27 | 18428 | 1.40× slower | 1.38× slower |
|  | go-native | 70.00 | 311.08 | 81.38 | 12288 | 1.84× slower | 2.06× slower |
| createUser (single write) | ts-IR (interp) | 337.25 | 1183.08 | 383.17 | 2610 | — (baseline) | — (baseline) |
|  | rust-native | 343.71 | 838.63 | 378.19 | 2644 | 1.02× slower | 1.01× faster |
|  | go-native | 902.92 | 6748.33 | 1278.55 | 782 | 2.68× slower | 3.34× slower |
| createMany (batch write ×10) | ts-IR (interp) | 475.54 | 1702.96 | 538.65 | 1856 | — (baseline) | — (baseline) |
|  | rust-native | 469.25 | 2460.67 | 559.28 | 1788 | 1.01× faster | 1.04× slower |
|  | go-native | 482.83 | 2097.88 | 578.54 | 1728 | 1.02× slower | 1.07× slower |
| relScale (10 children) | ts-IR (interp) | 31.50 | 43.29 | 32.68 | 30600 | — (baseline) | — (baseline) |
|  | rust-native | 34.13 | 93.58 | 39.17 | 25532 | 1.08× slower | 1.20× slower |
|  | go-native | 37.79 | 48.13 | 38.93 | 25687 | 1.20× slower | 1.19× slower |
| relScale (100 children) | ts-IR (interp) | 175.92 | 354.63 | 180.07 | 5554 | — (baseline) | — (baseline) |
|  | rust-native | 168.50 | 292.96 | 175.06 | 5712 | 1.04× faster | 1.03× faster |
|  | go-native | 183.67 | 420.83 | 193.62 | 5165 | 1.04× slower | 1.08× slower |
| relScale (1000 children) | ts-IR (interp) | 740.08 | 1083.67 | 754.36 | 1326 | — (baseline) | — (baseline) |
|  | rust-native | 552.79 | 1085.75 | 571.55 | 1750 | 1.34× faster | 1.32× faster |
|  | go-native | 843.38 | 2081.33 | 897.63 | 1114 | 1.14× slower | 1.19× slower |
| relScale (10000 children) | ts-IR (interp) | 6577.50 | 9944.38 | 6805.16 | 147 | — (baseline) | — (baseline) |
|  | rust-native | 4293.33 | 29231.21 | 5189.60 | 193 | 1.53× faster | 1.31× faster |
|  | go-native | 8620.21 | 10744.96 | 8751.08 | 114 | 1.31× slower | 1.29× slower |

Raw per-iteration samples: `.results/<cell>.csv`; per-op summary: `.results/summary.csv`.
