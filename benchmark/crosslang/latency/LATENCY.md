# Latency: ts-IR-interpreter vs rust-native vs go-native (sqlite in-proc)

Same 4 ops, same seed sqlite (C engine in every cell: better-sqlite3 / rusqlite-bundled /
mattn-go-sqlite3), same iteration count. Whole hot path timed (bind + exec + decode into the
typed result). p50/p99 in µs; ops/sec = 1e6 / mean latency (single-thread serial). Verbatim.

Two speedup framings are shown so nothing hides: **p50** (median, robust) and **throughput**
(mean-based, ops/sec) — they diverge for writes because sqlite fsync gives a heavy tail.

| op | cell | p50 µs | p99 µs | mean µs | ops/sec | p50 vs ts-IR | throughput vs ts-IR |
|---|---|--:|--:|--:|--:|--:|--:|
| findUnique (point read) | ts-IR (interp) | 14.00 | 21.25 | 14.52 | 68884 | — (baseline) | — (baseline) |
|  | rust-native | 7.13 | 18.83 | 7.82 | 127839 | 1.96× faster | 1.86× faster |
|  | go-native | 11.63 | 20.13 | 12.54 | 79740 | 1.20× faster | 1.16× faster |
| relComments (batched relation, 4 children) | ts-IR (interp) | 44.58 | 103.67 | 50.12 | 19952 | — (baseline) | — (baseline) |
|  | rust-native | 53.21 | 75.38 | 55.06 | 18163 | 1.19× slower | 1.10× slower |
|  | go-native | 50.33 | 71.83 | 61.12 | 16361 | 1.13× slower | 1.22× slower |
| createUser (single write) | ts-IR (interp) | 400.83 | 1611.17 | 450.68 | 2219 | — (baseline) | — (baseline) |
|  | rust-native | 364.88 | 3087.67 | 465.56 | 2148 | 1.10× faster | 1.03× slower |
|  | go-native | 363.46 | 1196.38 | 401.96 | 2488 | 1.10× faster | 1.12× faster |
| createMany (batch write ×10) | ts-IR (interp) | 573.25 | 5900.29 | 798.66 | 1252 | — (baseline) | — (baseline) |
|  | rust-native | 496.25 | 2087.13 | 577.10 | 1733 | 1.16× faster | 1.38× faster |
|  | go-native | 490.75 | 1253.92 | 529.04 | 1890 | 1.17× faster | 1.51× faster |
| relScale (10 children) | ts-IR (interp) | 33.38 | 53.29 | 35.33 | 28307 | — (baseline) | — (baseline) |
|  | rust-native | 32.54 | 66.17 | 34.69 | 28829 | 1.03× faster | 1.02× faster |
|  | go-native | 38.25 | 54.04 | 40.36 | 24777 | 1.15× slower | 1.14× slower |
| relScale (100 children) | ts-IR (interp) | 180.08 | 512.04 | 196.52 | 5089 | — (baseline) | — (baseline) |
|  | rust-native | 155.67 | 191.04 | 158.11 | 6325 | 1.16× faster | 1.24× faster |
|  | go-native | 185.67 | 372.88 | 192.30 | 5200 | 1.03× slower | 1.02× faster |
| relScale (1000 children) | ts-IR (interp) | 756.17 | 1053.54 | 781.11 | 1280 | — (baseline) | — (baseline) |
|  | rust-native | 551.54 | 804.21 | 561.87 | 1780 | 1.37× faster | 1.39× faster |
|  | go-native | 830.50 | 1173.67 | 855.14 | 1169 | 1.10× slower | 1.09× slower |
| relScale (10000 children) | ts-IR (interp) | 6644.75 | 7457.67 | 6699.77 | 149 | — (baseline) | — (baseline) |
|  | rust-native | 4303.63 | 4705.08 | 4315.79 | 232 | 1.54× faster | 1.55× faster |
|  | go-native | 7946.08 | 11231.00 | 8285.81 | 121 | 1.20× slower | 1.24× slower |

Raw per-iteration samples: `.results/<cell>.csv`; per-op summary: `.results/summary.csv`.
