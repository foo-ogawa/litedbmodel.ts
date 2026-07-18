# Latency: ts-IR-interpreter vs rust-native vs go-native (sqlite in-proc)

Same 4 ops, same seed sqlite (C engine in every cell: better-sqlite3 / rusqlite-bundled /
mattn-go-sqlite3), same iteration count. Whole hot path timed (bind + exec + decode into the
typed result). p50/p99 in µs; ops/sec = 1e6 / mean latency (single-thread serial). Verbatim.

| op | cell | p50 µs | p99 µs | ops/sec | vs ts-IR (p50) |
|---|---|--:|--:|--:|--:|
| findUnique (point read) | ts-IR (interp) | 14.50 | 26.54 | 59371 | — (baseline) |
|  | rust-native | 9.92 | 17.58 | 95120 | 1.46× faster |
|  | go-native | 14.00 | 19.00 | 67285 | 1.04× faster |
| relComments (batched relation) | ts-IR (interp) | 51.17 | 68.46 | 18008 | — (baseline) |
|  | rust-native | 61.29 | 73.29 | 16230 | 1.20× slower |
|  | go-native | 59.75 | 77.58 | 16234 | 1.17× slower |
| createUser (single write) | ts-IR (interp) | 355.25 | 830.21 | 2570 | — (baseline) |
|  | rust-native | 352.42 | 847.79 | 2552 | 1.01× faster |
|  | go-native | 369.17 | 1431.00 | 2373 | 1.04× slower |
| createMany (batch write ×10) | ts-IR (interp) | 506.38 | 2442.58 | 1674 | — (baseline) |
|  | rust-native | 543.75 | 2746.58 | 1527 | 1.07× slower |
|  | go-native | 529.96 | 2617.17 | 1573 | 1.05× slower |

Raw per-iteration samples: `.results/<cell>.csv`; per-op summary: `.results/summary.csv`.
