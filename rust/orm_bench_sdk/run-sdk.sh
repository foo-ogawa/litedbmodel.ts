#!/usr/bin/env bash
# #129 rust pilot — run the raw-driver SDK baseline cell on all three real DBs and collect raw CSV.
# The SDK cell has no generated modules (hand SQL per dialect), so no swap: one default (rusqlite) build
# for sqlite + one --features livedb build (postgres + mysql crates) for pg/mysql. Re-seeds from the SAME
# /tmp/ormbench/<dialect>/setup.json the native cell uses. Prereqs: docker pg :5433 + mysql :3307 up.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS="$HERE/../../benchmark/crosslang/.results"
REPS="${REPS:-400}"
WARMUP="${WARMUP:-40}"
mkdir -p "$RESULTS"
PG_SPEC='pg:host=localhost port=5433 user=testuser password=testpass dbname=testdb'
MYSQL_SPEC='mysql:mysql://testuser:testpass@localhost:3307/testdb'

echo "── sdk × sqlite (reps=$REPS) ──"
cargo build --release --quiet || { echo "sqlite build failed"; exit 1; }
rm -f /tmp/sdk_sqlite.db
"$HERE/target/release/orm_bench_sdk" sqlite /tmp/sdk_sqlite.db "$REPS" "$WARMUP" > "$RESULTS/sdk.sqlite.csv" || exit 1

echo "── sdk × postgres + mysql (reps=$REPS) ──"
cargo build --release --quiet --features livedb || { echo "livedb build failed"; exit 1; }
"$HERE/target/release/orm_bench_sdk" postgres "$PG_SPEC" "$REPS" "$WARMUP" > "$RESULTS/sdk.postgres.csv" || { echo "pg run failed"; exit 1; }
"$HERE/target/release/orm_bench_sdk" mysql "$MYSQL_SPEC" "$REPS" "$WARMUP" > "$RESULTS/sdk.mysql.csv" || { echo "mysql run failed"; exit 1; }

{ head -1 "$RESULTS/sdk.sqlite.csv"; tail -n +2 -q "$RESULTS/sdk.sqlite.csv" "$RESULTS/sdk.postgres.csv" "$RESULTS/sdk.mysql.csv"; } > "$RESULTS/sdk.csv"
echo "sdk.csv: $(($(wc -l < "$RESULTS/sdk.csv") - 1)) samples"
