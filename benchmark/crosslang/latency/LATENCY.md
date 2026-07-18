# Latency: ts-IR-interpreter vs rust-native vs go-native (sqlite in-proc)

Same 4 ops, same seed sqlite (C engine in every cell: better-sqlite3 / rusqlite-bundled /
mattn-go-sqlite3), same iteration count. Whole hot path timed (bind + exec + decode into the
typed result). p50/p99 in µs; ops/sec = 1e6 / mean latency (single-thread serial). Verbatim.

Two speedup framings are shown so nothing hides: **p50** (median, robust) and **throughput**
(mean-based, ops/sec) — they diverge for writes because sqlite fsync gives a heavy tail.

| op | cell | p50 µs | p99 µs | mean µs | ops/sec | p50 vs ts-IR | throughput vs ts-IR |
|---|---|--:|--:|--:|--:|--:|--:|
| findUnique (point read) | ts-IR (interp) | 11.96 | 21.79 | 12.91 | 77454 | — (baseline) | — (baseline) |
|  | rust-native | 7.00 | 10.71 | 7.38 | 135593 | 1.71× faster | 1.75× faster |
|  | go-native | 11.75 | 17.38 | 13.10 | 76317 | 1.02× faster | 1.01× slower |
| relComments (batched relation) | ts-IR (interp) | 37.00 | 51.29 | 38.48 | 25984 | — (baseline) | — (baseline) |
|  | rust-native | 50.46 | 64.08 | 51.62 | 19371 | 1.36× slower | 1.34× slower |
|  | go-native | 54.67 | 77.25 | 56.92 | 17569 | 1.48× slower | 1.48× slower |
| createUser (single write) | ts-IR (interp) | 337.42 | 1049.88 | 651.74 | 1534 | — (baseline) | — (baseline) |
|  | rust-native | 354.88 | 1039.63 | 396.14 | 2524 | 1.05× slower | 1.65× faster |
|  | go-native | 330.88 | 1087.79 | 372.67 | 2683 | 1.02× faster | 1.75× faster |
| createMany (batch write ×10) | ts-IR (interp) | 490.92 | 1833.38 | 577.20 | 1733 | — (baseline) | — (baseline) |
|  | rust-native | 459.00 | 1520.29 | 513.16 | 1949 | 1.07× faster | 1.12× faster |
|  | go-native | 464.58 | 1962.92 | 540.89 | 1849 | 1.06× faster | 1.07× faster |

Raw per-iteration samples: `.results/<cell>.csv`; per-op summary: `.results/summary.csv`.
