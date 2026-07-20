# ORM bench — rust pilot (native codegen vs raw-driver SDK)

Each op runs the same logical operation two ways: **native** = litedbmodel-generated native module + runtime (no hand-written exec seam); **sdk** = a hand-written raw rust driver (rusqlite / postgres / mysql), litedbmodel NOT in the path. Both cells reuse prepared statements across iterations (native via the runtime prepared-statement cache, sdk via the driver statement cache) and bind params for reads and writes alike — a competent-raw-driver baseline, not a re-parse-per-call strawman. Reads/writes/batch/relations/tx across sqlite (in-proc file) + docker PostgreSQL + docker MySQL. Latency p50/p99 in ms; ops/sec from the mean; overhead = native p50 ÷ sdk p50.


## sqlite

| op | native p50 | native p99 | native ops/s | sdk p50 | sdk p99 | sdk ops/s | native÷sdk |
|---|--:|--:|--:|--:|--:|--:|--:|
| findAll | 0.204 | 0.248 | 4,861 | 0.040 | 0.076 | 23,276 | 5.10× |
| filterPaginateSort | 0.109 | 0.121 | 9,106 | 0.049 | 0.067 | 20,001 | 2.22× |
| nestedFindAll | 0.658 | 0.825 | 1,476 | 0.147 | 0.181 | 6,678 | 4.48× |
| findFirst | 0.014 | 0.018 | 69,650 | 0.016 | 0.020 | 62,962 | 0.88× |
| nestedFindFirst | 0.058 | 0.072 | 16,613 | 0.029 | 0.033 | 33,721 | 2.00× |
| findUnique | 0.012 | 0.015 | 85,197 | 0.006 | 0.007 | 165,631 | 2.00× |
| nestedFindUnique | 0.053 | 0.063 | 18,693 | 0.019 | 0.023 | 51,666 | 2.79× |
| create | 0.316 | 1.372 | 2,798 | 0.333 | 0.543 | 2,811 | 0.95× |
| nestedCreate | 0.387 | 0.520 | 2,543 | 0.406 | 5.004 | 1,133 | 0.95× |
| update | 0.011 | 0.014 | 89,067 | 0.008 | 0.010 | 124,378 | 1.38× |
| nestedUpdate | 0.024 | 0.044 | 41,310 | 0.017 | 0.020 | 59,259 | 1.41× |
| upsert | 0.294 | 1.535 | 2,651 | 0.335 | 9.705 | 1,693 | 0.88× |
| nestedUpsert | 0.332 | 0.401 | 2,977 | 0.347 | 6.810 | 1,940 | 0.96× |
| delete | 0.457 | 0.648 | 2,101 | 0.409 | 0.610 | 2,321 | 1.12× |
| createMany | 0.464 | 0.849 | 2,044 | 0.394 | 0.570 | 2,488 | 1.18× |
| upsertMany | 0.631 | 1.513 | 1,454 | 0.386 | 1.665 | 2,291 | 1.63× |
| updateMany | 0.067 | 0.080 | 14,495 | 0.019 | 0.024 | 51,928 | 3.53× |
| nestedRelations | 1.622 | 1.848 | 608 | 0.355 | 0.402 | 2,789 | 4.57× |
| compositeRelations | 0.438 | 0.616 | 2,248 | 0.044 | 0.053 | 22,464 | 9.95× |

## mysql

| op | native p50 | native p99 | native ops/s | sdk p50 | sdk p99 | sdk ops/s | native÷sdk |
|---|--:|--:|--:|--:|--:|--:|--:|
| findAll | 0.680 | 0.765 | 1,471 | 0.247 | 0.488 | 3,833 | 2.75× |
| filterPaginateSort | 0.555 | 0.790 | 1,769 | 0.244 | 0.309 | 4,048 | 2.27× |
| nestedFindAll | 1.884 | 4.905 | 467 | 0.572 | 0.664 | 1,732 | 3.29× |
| findFirst | 0.393 | 0.529 | 2,495 | 0.172 | 0.233 | 5,639 | 2.28× |
| nestedFindFirst | 0.888 | 1.398 | 1,092 | 0.360 | 0.444 | 2,759 | 2.47× |
| findUnique | 0.396 | 0.474 | 2,512 | 0.167 | 0.210 | 5,884 | 2.37× |
| nestedFindUnique | 0.863 | 0.964 | 1,163 | 0.360 | 0.775 | 2,669 | 2.40× |
| create | 0.470 | 0.982 | 2,046 | 0.245 | 0.336 | 4,010 | 1.92× |
| nestedCreate | 1.147 | 1.315 | 866 | 0.707 | 0.951 | 1,407 | 1.62× |
| update | 0.379 | 0.456 | 2,621 | 0.166 | 0.217 | 5,897 | 2.28× |
| nestedUpdate | 0.936 | 1.052 | 1,066 | 0.703 | 0.851 | 1,403 | 1.33× |
| upsert | 0.618 | 0.773 | 1,610 | 0.228 | 0.311 | 4,331 | 2.71× |
| nestedUpsert | 1.124 | 2.309 | 847 | 0.878 | 1.079 | 1,127 | 1.28× |
| delete | 1.180 | 1.935 | 820 | 0.666 | 0.974 | 1,469 | 1.77× |
| createMany | 0.688 | 3.074 | 1,166 | 0.272 | 0.386 | 3,633 | 2.53× |
| upsertMany | 0.833 | 2.497 | 1,033 | 0.315 | 1.367 | 2,674 | 2.64× |
| updateMany | 0.517 | 1.040 | 1,793 | 0.189 | 0.257 | 5,250 | 2.74× |
| nestedRelations | 4.622 | 16.25 | 198 | 1.139 | 1.731 | 861 | 4.06× |
| compositeRelations | 1.498 | 4.620 | 595 | 0.584 | 0.843 | 1,665 | 2.57× |

## postgres

| op | native p50 | native p99 | native ops/s | sdk p50 | sdk p99 | sdk ops/s | native÷sdk |
|---|--:|--:|--:|--:|--:|--:|--:|
| findAll | 0.424 | 0.528 | 2,310 | 0.250 | 0.308 | 3,970 | 1.70× |
| filterPaginateSort | 0.293 | 0.356 | 3,366 | 0.214 | 0.269 | 4,597 | 1.37× |
| nestedFindAll | 1.191 | 2.256 | 821 | 0.660 | 0.728 | 1,511 | 1.80× |
| findFirst | 0.187 | 0.284 | 5,271 | 0.176 | 0.283 | 5,538 | 1.06× |
| nestedFindFirst | 0.386 | 0.471 | 2,560 | 0.357 | 0.463 | 2,757 | 1.08× |
| findUnique | 0.190 | 0.303 | 5,113 | 0.183 | 0.226 | 5,414 | 1.04× |
| nestedFindUnique | 0.433 | 0.566 | 2,273 | 0.378 | 0.535 | 2,587 | 1.15× |
| create | 0.208 | 1.055 | 3,976 | 0.169 | 0.222 | 5,796 | 1.23× |
| nestedCreate | 0.795 | 1.774 | 1,190 | 0.692 | 0.968 | 1,410 | 1.15× |
| update | 0.206 | 0.258 | 4,791 | 0.188 | 0.238 | 5,234 | 1.10× |
| nestedUpdate | 0.757 | 3.382 | 1,195 | 0.707 | 1.203 | 1,353 | 1.07× |
| upsert | 0.207 | 0.279 | 4,733 | 0.174 | 0.219 | 5,681 | 1.19× |
| nestedUpsert | 0.758 | 2.499 | 1,127 | 0.874 | 1.048 | 1,139 | 0.87× |
| delete | 0.756 | 3.110 | 1,164 | 0.692 | 1.277 | 1,391 | 1.09× |
| createMany | 0.241 | 0.347 | 3,779 | 0.220 | 0.280 | 4,461 | 1.10× |
| upsertMany | 0.284 | 1.792 | 2,862 | 0.289 | 0.815 | 3,187 | 0.98× |
| updateMany | 0.209 | 0.261 | 4,743 | 0.211 | 0.246 | 4,686 | 0.99× |
| nestedRelations | 2.987 | 12.85 | 241 | 1.317 | 2.479 | 743 | 2.27× |
| compositeRelations | 0.747 | 1.247 | 1,282 | 0.628 | 0.843 | 1,553 | 1.19× |

## PostgreSQL: v2 native (this pilot) vs v1 rust native (litedbmodel.rs)

v1 measured PostgreSQL only (median_ms). Baselines differ: v1 baseline = SeaORM/Diesel; this pilot's baseline = a raw rust driver. Overlap is by the op label (ORM_OP_LABEL == v1 category).

| op (label) | v2-native p50 ms | v1-native median ms | v1 SeaORM ms | v1 Diesel ms | raw-driver sdk p50 ms |
|---|--:|--:|--:|--:|--:|
| Find all (limit 100) | 0.424 | 0.890 | 1.250 | 1.480 | 0.250 |
| Filter, paginate & sort | 0.293 | 1.370 | 1.970 | 1.840 | 0.214 |
| Nested find all (include posts) | 1.191 | 4.010 | 4.700 | 3.750 | 0.660 |
| Find first | 0.187 | 0.530 | 0.540 | 0.390 | 0.176 |
| Nested find first (include posts) | 0.386 | 0.550 | 1.440 | 0.940 | 0.357 |
| Find unique (by email) | 0.190 | 0.340 | 0.520 | 0.400 | 0.183 |
| Nested find unique (include posts) | 0.433 | 0.490 | 0.960 | 0.540 | 0.378 |
| Create | 0.208 | 0.970 | 1.110 | 1.170 | 0.169 |
| Nested create (with post) | 0.795 | 0.720 | 1.280 | 1.090 | 0.692 |
| Update | 0.206 | 0.740 | 1.270 | 0.930 | 0.188 |
| Nested update (update user + post) | 0.757 | 1.080 | 1.310 | 1.330 | 0.707 |
| Upsert | 0.207 | 0.940 | 1.060 | 1.030 | 0.174 |
| Nested upsert (user + post) | 0.758 | 0.800 | 1.580 | 1.330 | 0.874 |
| Delete | 0.756 | 0.850 | 1.390 | 1.110 | 0.692 |
| Create Many (10 records) | 0.241 | 0.920 | 1.180 | 1.170 | 0.220 |
| Upsert Many (10 records) | 0.284 | 1.020 | 1.820 | 1.600 | 0.289 |
| Update Many (10 different values) | 0.209 | 0.990 | 3.370 | 3.050 | 0.211 |
| Nested relations (100->1000->10000) | 2.987 | — | — | — | 1.317 |
| Nested relations (composite key, 5 tenants) | 0.747 | 7.320 | 8.900 | 6.500 | 0.628 |

## Decoder correctness — native ≡ mode-2 (byte-equal)

filterPaginateSort projects `created_at` (TIMESTAMP) + `published` (BOOLEAN on pg / TINYINT on mysql). The native de-boxed output equals the mode-2 interpreter (`execute_bundle`) output byte-for-byte on the same live DB — date → canonical `YYYY-MM-DD HH:MM:SS`, pg bool → `true`, mysql bool → `1`:

```
filterPaginateSort sqlite: native≡mode-2 byte-equal = true (20 rows)
  sample: [{"id":189,"title":"Post 189","content":"Content 189","published":1,"author_id":95,"created_at":"2020-01-01 00:03:09"},{"id":186,"title":"Post 186","content":"Content 186","published":1,"author_id":93
filterPaginateSort postgres: native≡mode-2 byte-equal = true (20 rows)
  sample: [{"id":189,"title":"Post 189","content":"Content 189","published":true,"author_id":95,"created_at":"2020-01-01 00:03:09"},{"id":186,"title":"Post 186","content":"Content 186","published":true,"author_
filterPaginateSort mysql: native≡mode-2 byte-equal = true (20 rows)
  sample: [{"id":189,"title":"Post 189","content":"Content 189","published":1,"author_id":95,"created_at":"2020-01-01 00:03:09"},{"id":186,"title":"Post 186","content":"Content 186","published":1,"author_id":93
```

## Safety proofs (native cell)

The measured latency is the cost WITH the safety guards on. Query counts are issued at the Driver seam (a batched relation = 1 parent + 1 batched child per level; a batch write = 1 statement); the find hardLimit fires end-to-end on the guarded native read:

```
nestedFindAll queries=2 (expect 2: 1 parent + 1 batched child)
nestedFindUnique queries=2 (expect 2)
nestedRelations queries=3 (expect 3: users + posts + comments)
compositeRelations queries=3 (expect 3)
createMany queries=1 (expect 1: one batched INSERT for 10 records)
updateMany queries=1 (expect 1)
hardLimit fired: context=find limit=2 fetched=3 (expect find/2/3)
```

Reader/writer routing: the native read/write companions expose the `handler_routed(&RoutingConfig)` seam; the runtime routing resolver sends a read → reader pool, a write → writer pool, and a read inside a writer scope → writer (read-your-writes) — verified green by the runtime routing tests (`resolve_pool_reader_writer_split`, `named_routing_selects_the_pair`).
