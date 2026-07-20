# ORM bench — rust pilot (native codegen vs raw-driver SDK)

Each op runs the same logical operation two ways: **native** = litedbmodel-generated native module + runtime (no hand-written exec seam); **sdk** = a hand-written raw rust driver (rusqlite / postgres / mysql), litedbmodel NOT in the path. Reads/writes/batch/relations/tx across sqlite (in-proc file) + docker PostgreSQL + docker MySQL. Latency p50/p99 in ms; ops/sec from the mean; overhead = native p50 ÷ sdk p50.


## sqlite

| op | native p50 | native p99 | native ops/s | sdk p50 | sdk p99 | sdk ops/s | native÷sdk |
|---|--:|--:|--:|--:|--:|--:|--:|
| findAll | 0.195 | 0.216 | 5,081 | 0.043 | 0.051 | 22,822 | 4.53× |
| filterPaginateSort | 0.112 | 0.130 | 8,800 | 0.053 | 0.067 | 18,199 | 2.11× |
| nestedFindAll | 0.666 | 0.831 | 1,474 | 0.177 | 0.220 | 5,557 | 3.76× |
| findFirst | 0.014 | 0.018 | 70,721 | 0.011 | 0.025 | 84,800 | 1.27× |
| nestedFindFirst | 0.054 | 0.065 | 18,242 | 0.026 | 0.033 | 37,943 | 2.08× |
| findUnique | 0.012 | 0.013 | 80,988 | 0.008 | 0.011 | 121,544 | 1.50× |
| nestedFindUnique | 0.053 | 0.066 | 18,586 | 0.023 | 0.030 | 42,540 | 2.30× |
| create | 0.315 | 0.871 | 2,818 | 0.366 | 0.584 | 2,600 | 0.86× |
| nestedCreate | 0.423 | 0.631 | 2,306 | 0.347 | 0.505 | 2,780 | 1.22× |
| update | 0.010 | 0.014 | 92,336 | 0.009 | 0.011 | 109,439 | 1.11× |
| nestedUpdate | 0.023 | 0.029 | 42,790 | 0.021 | 0.027 | 47,801 | 1.10× |
| upsert | 0.344 | 0.448 | 2,869 | 0.300 | 0.470 | 3,202 | 1.15× |
| nestedUpsert | 0.389 | 0.511 | 2,528 | 0.345 | 0.530 | 2,778 | 1.13× |
| delete | 0.456 | 0.604 | 2,153 | 0.394 | 1.583 | 2,186 | 1.16× |
| createMany | 0.451 | 0.752 | 2,153 | 0.449 | 0.691 | 2,128 | 1.00× |
| upsertMany | 0.381 | 0.552 | 2,470 | 0.356 | 0.555 | 2,736 | 1.07× |
| updateMany | 0.072 | 0.156 | 12,886 | 0.025 | 0.032 | 39,124 | 2.88× |
| nestedRelations | 1.652 | 1.846 | 599 | 0.434 | 0.460 | 2,294 | 3.81× |
| compositeRelations | 0.426 | 0.503 | 2,319 | 0.063 | 0.074 | 15,523 | 6.76× |

## mysql

| op | native p50 | native p99 | native ops/s | sdk p50 | sdk p99 | sdk ops/s | native÷sdk |
|---|--:|--:|--:|--:|--:|--:|--:|
| findAll | 0.749 | 0.970 | 1,318 | 0.253 | 0.314 | 3,894 | 2.96× |
| filterPaginateSort | 0.551 | 0.653 | 1,801 | 0.248 | 0.568 | 3,825 | 2.22× |
| nestedFindAll | 1.856 | 2.381 | 531 | 0.568 | 0.692 | 1,732 | 3.27× |
| findFirst | 0.379 | 0.465 | 2,657 | 0.167 | 0.241 | 5,833 | 2.27× |
| nestedFindFirst | 0.859 | 0.966 | 1,163 | 0.339 | 0.643 | 2,799 | 2.53× |
| findUnique | 0.399 | 0.606 | 2,471 | 0.168 | 0.245 | 5,788 | 2.38× |
| nestedFindUnique | 0.875 | 2.300 | 1,056 | 0.365 | 0.470 | 2,686 | 2.40× |
| create | 0.436 | 0.532 | 2,280 | 0.226 | 0.319 | 4,317 | 1.93× |
| nestedCreate | 1.125 | 1.492 | 878 | 0.724 | 0.957 | 1,349 | 1.55× |
| update | 0.426 | 0.921 | 2,195 | 0.165 | 0.209 | 5,947 | 2.58× |
| nestedUpdate | 0.922 | 1.323 | 1,069 | 0.705 | 1.200 | 1,370 | 1.31× |
| upsert | 0.635 | 0.770 | 1,568 | 0.218 | 0.287 | 4,521 | 2.91× |
| nestedUpsert | 1.112 | 2.623 | 864 | 0.879 | 1.024 | 1,124 | 1.27× |
| delete | 1.159 | 1.581 | 848 | 0.712 | 1.393 | 1,362 | 1.63× |
| createMany | 0.540 | 0.756 | 1,813 | 0.283 | 0.396 | 3,439 | 1.91× |
| upsertMany | 0.568 | 0.743 | 1,739 | 0.318 | 0.407 | 3,073 | 1.79× |
| updateMany | 0.429 | 0.533 | 2,303 | 0.196 | 0.268 | 4,969 | 2.19× |
| nestedRelations | 6.617 | 13.33 | 151 | 1.140 | 2.956 | 850 | 5.80× |
| compositeRelations | 1.538 | 4.540 | 619 | 0.544 | 0.931 | 1,781 | 2.83× |

## postgres

| op | native p50 | native p99 | native ops/s | sdk p50 | sdk p99 | sdk ops/s | native÷sdk |
|---|--:|--:|--:|--:|--:|--:|--:|
| findAll | 0.443 | 0.609 | 2,202 | 0.504 | 0.621 | 1,981 | 0.88× |
| filterPaginateSort | 0.315 | 0.862 | 2,911 | 0.450 | 0.934 | 2,156 | 0.70× |
| nestedFindAll | 1.288 | 3.233 | 694 | 1.211 | 1.488 | 815 | 1.06× |
| findFirst | 0.199 | 0.456 | 4,626 | 0.404 | 0.508 | 2,516 | 0.49× |
| nestedFindFirst | 0.422 | 1.393 | 2,063 | 0.838 | 1.416 | 1,154 | 0.50× |
| findUnique | 0.197 | 0.285 | 4,879 | 0.425 | 0.895 | 2,239 | 0.46× |
| nestedFindUnique | 0.499 | 1.768 | 1,715 | 0.876 | 1.014 | 1,135 | 0.57× |
| create | 0.212 | 1.312 | 3,753 | 0.419 | 0.502 | 2,363 | 0.51× |
| nestedCreate | 0.754 | 0.893 | 1,313 | 1.190 | 1.345 | 835 | 0.63× |
| update | 0.210 | 0.553 | 4,324 | 0.402 | 0.772 | 2,315 | 0.52× |
| nestedUpdate | 0.755 | 2.130 | 1,219 | 1.191 | 1.568 | 830 | 0.63× |
| upsert | 0.222 | 1.521 | 3,243 | 0.428 | 0.518 | 2,326 | 0.52× |
| nestedUpsert | 0.750 | 2.922 | 1,192 | 1.597 | 2.571 | 612 | 0.47× |
| delete | 0.770 | 1.920 | 1,205 | 1.190 | 1.397 | 840 | 0.65× |
| createMany | 0.252 | 2.026 | 3,216 | 0.465 | 0.546 | 2,150 | 0.54× |
| upsertMany | 0.285 | 0.423 | 3,380 | 0.535 | 0.666 | 1,869 | 0.53× |
| updateMany | 0.221 | 0.273 | 4,453 | 0.477 | 0.897 | 2,036 | 0.46× |
| nestedRelations | 2.857 | 7.009 | 290 | 2.086 | 2.293 | 480 | 1.37× |
| compositeRelations | 0.752 | 0.915 | 1,315 | 1.322 | 1.635 | 748 | 0.57× |

## PostgreSQL: v2 native (this pilot) vs v1 rust native (litedbmodel.rs)

v1 measured PostgreSQL only (median_ms). Baselines differ: v1 baseline = SeaORM/Diesel; this pilot's baseline = a raw rust driver. Overlap is by the op label (ORM_OP_LABEL == v1 category).

| op (label) | v2-native p50 ms | v1-native median ms | v1 SeaORM ms | v1 Diesel ms | #129 sdk p50 ms |
|---|--:|--:|--:|--:|--:|
| Find all (limit 100) | 0.443 | 0.890 | 1.250 | 1.480 | 0.504 |
| Filter, paginate & sort | 0.315 | 1.370 | 1.970 | 1.840 | 0.450 |
| Nested find all (include posts) | 1.288 | 4.010 | 4.700 | 3.750 | 1.211 |
| Find first | 0.199 | 0.530 | 0.540 | 0.390 | 0.404 |
| Nested find first (include posts) | 0.422 | 0.550 | 1.440 | 0.940 | 0.838 |
| Find unique (by email) | 0.197 | 0.340 | 0.520 | 0.400 | 0.425 |
| Nested find unique (include posts) | 0.499 | 0.490 | 0.960 | 0.540 | 0.876 |
| Create | 0.212 | 0.970 | 1.110 | 1.170 | 0.419 |
| Nested create (with post) | 0.754 | 0.720 | 1.280 | 1.090 | 1.190 |
| Update | 0.210 | 0.740 | 1.270 | 0.930 | 0.402 |
| Nested update (update user + post) | 0.755 | 1.080 | 1.310 | 1.330 | 1.191 |
| Upsert | 0.222 | 0.940 | 1.060 | 1.030 | 0.428 |
| Nested upsert (user + post) | 0.750 | 0.800 | 1.580 | 1.330 | 1.597 |
| Delete | 0.770 | 0.850 | 1.390 | 1.110 | 1.190 |
| Create Many (10 records) | 0.252 | 0.920 | 1.180 | 1.170 | 0.465 |
| Upsert Many (10 records) | 0.285 | 1.020 | 1.820 | 1.600 | 0.535 |
| Update Many (10 different values) | 0.221 | 0.990 | 3.370 | 3.050 | 0.477 |
| Nested relations (100->1000->10000) | 2.857 | — | — | — | 2.086 |
| Nested relations (composite key, 5 tenants) | 0.752 | 7.320 | 8.900 | 6.500 | 1.322 |

## Safety proofs (native cell)

N+1 avoidance — query counts issued at the Driver seam (a batched relation = 1 parent + 1 batched child per level; a batch write = 1 statement):

```
nestedFindAll queries=2 (expect 2: 1 parent + 1 batched child)
nestedFindUnique queries=2 (expect 2)
nestedRelations queries=3 (expect 3: users + posts + comments)
compositeRelations queries=3 (expect 3)
createMany queries=1 (expect 1: one batched INSERT for 10 records)
updateMany queries=1 (expect 1)
```
