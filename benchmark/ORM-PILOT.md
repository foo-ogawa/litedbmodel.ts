# ORM bench — rust pilot (native codegen vs raw-driver SDK)

Each op runs the same logical operation two ways: **native** = litedbmodel-generated native module + runtime (no hand-written exec seam); **sdk** = a hand-written raw rust driver (rusqlite / postgres / mysql), litedbmodel NOT in the path. Both cells reuse prepared statements across iterations (native via the runtime prepared-statement cache, sdk via the driver statement cache) and bind params for reads and writes alike — a competent-raw-driver baseline, not a re-parse-per-call strawman. Reads/writes/batch/relations/tx across sqlite (in-proc file) + docker PostgreSQL + docker MySQL. Latency p50/p99 in ms; ops/sec from the mean; overhead = native p50 ÷ sdk p50.


## sqlite

| op | native p50 | native p99 | native ops/s | sdk p50 | sdk p99 | sdk ops/s | native÷sdk |
|---|--:|--:|--:|--:|--:|--:|--:|
| findAll | 0.117 | 0.161 | 8,313 | 0.039 | 0.048 | 25,251 | 3.00× |
| filterPaginateSort | 0.083 | 0.135 | 11,579 | 0.046 | 0.056 | 21,454 | 1.80× |
| nestedFindAll | 0.592 | 1.555 | 1,519 | 0.145 | 0.164 | 6,732 | 4.08× |
| findFirst | 0.013 | 0.019 | 72,696 | 0.016 | 0.023 | 63,052 | 0.81× |
| nestedFindFirst | 0.048 | 0.121 | 19,181 | 0.028 | 0.036 | 35,308 | 1.71× |
| findUnique | 0.011 | 0.015 | 91,358 | 0.005 | 0.006 | 184,026 | 2.20× |
| nestedFindUnique | 0.047 | 0.462 | 15,155 | 0.018 | 0.023 | 54,271 | 2.61× |
| create | 0.449 | 3.258 | 1,749 | 0.312 | 0.515 | 2,971 | 1.44× |
| nestedCreate | 0.506 | 1.776 | 1,741 | 0.375 | 0.568 | 2,517 | 1.35× |
| update | 0.011 | 0.014 | 87,858 | 0.007 | 0.010 | 137,250 | 1.57× |
| nestedUpdate | 0.025 | 0.051 | 38,811 | 0.016 | 0.024 | 60,731 | 1.56× |
| upsert | 0.409 | 1.604 | 1,956 | 0.309 | 0.491 | 3,082 | 1.32× |
| nestedUpsert | 0.486 | 3.027 | 1,544 | 0.314 | 0.487 | 2,804 | 1.55× |
| delete | 0.586 | 2.382 | 1,444 | 0.436 | 0.987 | 2,155 | 1.34× |
| createMany | 0.573 | 3.905 | 1,375 | 0.437 | 0.526 | 2,304 | 1.31× |
| upsertMany | 0.468 | 2.842 | 1,819 | 0.367 | 0.563 | 2,659 | 1.28× |
| updateMany | 0.071 | 0.318 | 11,549 | 0.019 | 0.024 | 52,121 | 3.74× |
| nestedRelations | 1.495 | 3.195 | 615 | 0.363 | 0.429 | 2,728 | 4.12× |
| compositeRelations | 0.431 | 1.451 | 1,999 | 0.043 | 0.053 | 22,568 | 10.02× |

## mysql

| op | native p50 | native p99 | native ops/s | sdk p50 | sdk p99 | sdk ops/s | native÷sdk |
|---|--:|--:|--:|--:|--:|--:|--:|
| findAll | 0.584 | 0.984 | 1,652 | 0.262 | 0.548 | 3,617 | 2.23× |
| filterPaginateSort | 0.513 | 0.602 | 1,938 | 0.265 | 0.418 | 3,667 | 1.94× |
| nestedFindAll | 1.750 | 2.686 | 558 | 0.604 | 0.947 | 1,600 | 2.90× |
| findFirst | 0.374 | 0.558 | 2,656 | 0.209 | 0.362 | 4,650 | 1.79× |
| nestedFindFirst | 0.815 | 0.990 | 1,214 | 0.416 | 2.165 | 2,071 | 1.96× |
| findUnique | 0.391 | 0.490 | 2,520 | 0.235 | 0.648 | 3,774 | 1.66× |
| nestedFindUnique | 0.820 | 1.780 | 1,181 | 0.394 | 1.955 | 2,133 | 2.08× |
| create | 0.431 | 1.139 | 2,189 | 0.237 | 0.376 | 4,026 | 1.82× |
| nestedCreate | 1.149 | 2.240 | 831 | 0.771 | 1.418 | 1,235 | 1.49× |
| update | 0.368 | 0.928 | 2,586 | 0.169 | 0.587 | 5,365 | 2.18× |
| nestedUpdate | 0.930 | 1.594 | 1,052 | 0.824 | 1.646 | 1,157 | 1.13× |
| upsert | 0.619 | 0.754 | 1,604 | 0.251 | 0.355 | 3,873 | 2.47× |
| nestedUpsert | 1.127 | 2.332 | 865 | 1.020 | 1.647 | 960 | 1.10× |
| delete | 1.151 | 1.551 | 866 | 0.814 | 1.657 | 1,160 | 1.41× |
| createMany | 0.554 | 0.709 | 1,782 | 0.287 | 0.416 | 3,381 | 1.93× |
| upsertMany | 0.594 | 0.714 | 1,685 | 0.392 | 0.795 | 2,449 | 1.52× |
| updateMany | 0.420 | 1.400 | 2,228 | 0.191 | 0.268 | 5,192 | 2.20× |
| nestedRelations | 4.166 | 14.82 | 175 | 1.370 | 2.573 | 628 | 3.04× |
| compositeRelations | 1.332 | 1.623 | 740 | 0.584 | 0.762 | 1,688 | 2.28× |

## postgres

| op | native p50 | native p99 | native ops/s | sdk p50 | sdk p99 | sdk ops/s | native÷sdk |
|---|--:|--:|--:|--:|--:|--:|--:|
| findAll | 0.386 | 0.484 | 2,549 | 0.262 | 0.490 | 3,650 | 1.47× |
| filterPaginateSort | 0.269 | 0.342 | 3,637 | 0.206 | 0.277 | 4,781 | 1.31× |
| nestedFindAll | 1.063 | 1.877 | 904 | 0.649 | 0.907 | 1,511 | 1.64× |
| findFirst | 0.207 | 0.273 | 4,819 | 0.181 | 0.298 | 5,262 | 1.14× |
| nestedFindFirst | 0.392 | 0.528 | 2,497 | 0.393 | 1.098 | 2,366 | 1.00× |
| findUnique | 0.210 | 0.284 | 4,576 | 0.191 | 0.250 | 5,139 | 1.10× |
| nestedFindUnique | 0.366 | 0.449 | 2,783 | 0.367 | 0.455 | 2,682 | 1.00× |
| create | 0.190 | 0.324 | 5,080 | 0.198 | 0.704 | 4,253 | 0.96× |
| nestedCreate | 0.779 | 2.138 | 1,181 | 0.724 | 2.304 | 1,175 | 1.08× |
| update | 0.214 | 0.308 | 4,549 | 0.195 | 0.470 | 4,685 | 1.10× |
| nestedUpdate | 0.765 | 0.987 | 1,286 | 0.717 | 1.298 | 1,359 | 1.07× |
| upsert | 0.207 | 0.307 | 4,613 | 0.192 | 0.237 | 5,150 | 1.08× |
| nestedUpsert | 0.760 | 2.191 | 1,204 | 0.892 | 1.911 | 1,084 | 0.85× |
| delete | 0.739 | 0.861 | 1,338 | 0.726 | 1.358 | 1,315 | 1.02× |
| createMany | 0.248 | 0.398 | 3,948 | 0.223 | 0.455 | 4,186 | 1.11× |
| upsertMany | 0.293 | 0.377 | 3,372 | 0.329 | 1.246 | 2,665 | 0.89× |
| updateMany | 0.234 | 0.293 | 4,236 | 0.215 | 0.270 | 4,598 | 1.09× |
| nestedRelations | 2.896 | 12.84 | 229 | 1.335 | 2.563 | 725 | 2.17× |
| compositeRelations | 0.671 | 0.839 | 1,471 | 0.639 | 1.119 | 1,484 | 1.05× |

## PostgreSQL: v2 native (this pilot) vs v1 rust native (litedbmodel.rs)

v1 measured PostgreSQL only (median_ms). Baselines differ: v1 baseline = SeaORM/Diesel; this pilot's baseline = a raw rust driver. Overlap is by the op label (ORM_OP_LABEL == v1 category).

| op (label) | v2-native p50 ms | v1-native median ms | v1 SeaORM ms | v1 Diesel ms | #129 sdk p50 ms |
|---|--:|--:|--:|--:|--:|
| Find all (limit 100) | 0.386 | 0.890 | 1.250 | 1.480 | 0.262 |
| Filter, paginate & sort | 0.269 | 1.370 | 1.970 | 1.840 | 0.206 |
| Nested find all (include posts) | 1.063 | 4.010 | 4.700 | 3.750 | 0.649 |
| Find first | 0.207 | 0.530 | 0.540 | 0.390 | 0.181 |
| Nested find first (include posts) | 0.392 | 0.550 | 1.440 | 0.940 | 0.393 |
| Find unique (by email) | 0.210 | 0.340 | 0.520 | 0.400 | 0.191 |
| Nested find unique (include posts) | 0.366 | 0.490 | 0.960 | 0.540 | 0.367 |
| Create | 0.190 | 0.970 | 1.110 | 1.170 | 0.198 |
| Nested create (with post) | 0.779 | 0.720 | 1.280 | 1.090 | 0.724 |
| Update | 0.214 | 0.740 | 1.270 | 0.930 | 0.195 |
| Nested update (update user + post) | 0.765 | 1.080 | 1.310 | 1.330 | 0.717 |
| Upsert | 0.207 | 0.940 | 1.060 | 1.030 | 0.192 |
| Nested upsert (user + post) | 0.760 | 0.800 | 1.580 | 1.330 | 0.892 |
| Delete | 0.739 | 0.850 | 1.390 | 1.110 | 0.726 |
| Create Many (10 records) | 0.248 | 0.920 | 1.180 | 1.170 | 0.223 |
| Upsert Many (10 records) | 0.293 | 1.020 | 1.820 | 1.600 | 0.329 |
| Update Many (10 different values) | 0.234 | 0.990 | 3.370 | 3.050 | 0.215 |
| Nested relations (100->1000->10000) | 2.896 | — | — | — | 1.335 |
| Nested relations (composite key, 5 tenants) | 0.671 | 7.320 | 8.900 | 6.500 | 0.639 |

## Decoder correctness — native ≡ mode-2 (byte-equal)

filterPaginateSort projects `created_at` (TIMESTAMP) + `published` (BOOLEAN on pg / TINYINT on mysql). The native de-boxed output equals the mode-2 interpreter (`execute_bundle`) output byte-for-byte on the same live DB — date → canonical `YYYY-MM-DD HH:MM:SS`, pg bool → `true`, mysql bool → `1`:

```
filterPaginateSort sqlite: native≡mode-2 byte-equal = true (20 rows)
  sample: [{"id":189,"title":"Post 189","content":"Content 189","published":1,"author_id":95,"created_at":"2020-01-01 00:03:09"},{"id":186,"title":"Post 186","content":"Content 186","published":1,"author_id":93
```

## Safety proofs (native cell)

The measured latency is the cost WITH the safety guards on. Query counts are issued at the Driver seam (a batched relation = 1 parent + 1 batched child per level; a batch write = 1 statement); the find hardLimit fires end-to-end on the guarded native read:

```
nestedFindAll queries=2 (expect 2)
nestedFindFirst queries=2 (expect 2)
nestedFindUnique queries=2 (expect 2)
nestedRelations queries=3 (expect 3)
compositeRelations queries=3 (expect 3)
nestedRelations returned comments=400 (typed tree, not discarded)
createMany queries=1 (expect 1)
updateMany queries=1 (expect 1)
hardLimit fired: context=find limit=2 fetched=3 (expect find/2/3)
```

Reader/writer routing: the native read/write adapters expose the `handler_routed(&RoutingConfig)` seam; the runtime routing resolver sends a read → reader pool, a write → writer pool, and a read inside a writer scope → writer (read-your-writes) — verified green by the runtime routing tests (`resolve_pool_reader_writer_split`, `named_routing_selects_the_pair`).
