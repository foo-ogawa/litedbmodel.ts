#!/usr/bin/env bash
# #129 rust pilot — run the NATIVE-codegen ORM-bench cell on all three real DBs and collect raw CSV.
#
# sqlite runs against the COMMITTED src/gen (sqlite-baked). For pg/mysql the per-dialect generated
# modules (baked $N / UNNEST / JSON_TABLE) are swapped into src/gen, the crate is rebuilt --release
# --features livedb, run against docker, then the committed sqlite files are restored (mirrors the e1
# livedb harness). Prereqs: `npx tsx benchmark/crosslang/codegen-build.ts` (emits /tmp/ormbench/<d>/*),
# docker pg :5433 + mysql :3307 up (`npm run docker:livedb:up`).
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP=/tmp/ormbench
RESULTS="$HERE/../../benchmark/crosslang/.results"
REPS="${REPS:-500}"
WARMUP="${WARMUP:-50}"
mkdir -p "$RESULTS"

PG_SPEC='pg:host=localhost port=5433 user=testuser password=testpass dbname=testdb'
MYSQL_SPEC='mysql:mysql://testuser:testpass@localhost:3307/testdb'

swap() { # <dialect> — overlay the dialect's generated modules into src/gen
  for f in "$TMP/$1"/generated_*.rs "$TMP/$1"/companion_*.rs; do cp "$f" "$HERE/src/gen/"; done
}
restore() { for f in "$TMP/sqlite"/generated_*.rs "$TMP/sqlite"/companion_*.rs; do cp "$f" "$HERE/src/gen/"; done; }
trap restore EXIT

# ── sqlite (committed gen, default build) ──
# Restore the sqlite gen FIRST — a prior interrupted run (or a manual pg/mysql swap) may have left a
# non-sqlite dialect's modules in src/gen, which would not compile the default (i64 `published`) build.
restore
echo "── native × sqlite (reps=$REPS) ──"
cargo build --release --quiet || { echo "sqlite build failed"; exit 1; }
rm -f /tmp/ormb_sqlite.db
{ "$HERE/target/release/orm_bench" sqlite /tmp/ormb_sqlite.db "$REPS" "$WARMUP" > "$RESULTS/native.sqlite.csv"; } || exit 1
rm -f /tmp/ormb_sqlite_s.db
"$HERE/target/release/orm_bench" safety sqlite /tmp/ormb_sqlite_s.db > "$RESULTS/native-safety.txt"
# Decoder correctness: native ≡ mode-2 (interpreter) byte-equal for the date/bool op (filterPaginateSort).
rm -f /tmp/ormb_v.db
"$HERE/target/release/orm_bench" verify sqlite /tmp/ormb_v.db > "$RESULTS/native-verify.txt"

# ── postgres ──
echo "── native × postgres (reps=$REPS) ──"
swap postgres
cargo build --release --quiet --features livedb,pg || { echo "pg build failed"; exit 1; }
{ "$HERE/target/release/orm_bench" postgres "$PG_SPEC" "$REPS" "$WARMUP" > "$RESULTS/native.postgres.csv"; } || { echo "pg run failed"; exit 1; }
"$HERE/target/release/orm_bench" verify postgres "$PG_SPEC" >> "$RESULTS/native-verify.txt"

# ── mysql ──
echo "── native × mysql (reps=$REPS) ──"
swap mysql
cargo build --release --quiet --features livedb || { echo "mysql build failed"; exit 1; }
{ "$HERE/target/release/orm_bench" mysql "$MYSQL_SPEC" "$REPS" "$WARMUP" > "$RESULTS/native.mysql.csv"; } || { echo "mysql run failed"; exit 1; }
"$HERE/target/release/orm_bench" verify mysql "$MYSQL_SPEC" >> "$RESULTS/native-verify.txt"

restore
# Merge the three per-dialect CSVs into one native.csv (single header).
{ head -1 "$RESULTS/native.sqlite.csv"; tail -n +2 -q "$RESULTS/native.sqlite.csv" "$RESULTS/native.postgres.csv" "$RESULTS/native.mysql.csv"; } > "$RESULTS/native.csv"
echo "native.csv: $(($(wc -l < "$RESULTS/native.csv") - 1)) samples"
