#!/usr/bin/env bash
# #129 rust pilot — run the raw-driver SDK baseline cell on all three real DBs and collect raw CSV.
# The operation bodies are hand SQL; setup+seed load from the ONE seed SSoT (benchmark/crosslang/
# .setup/<dialect>.json, emitted from orm-domain.ts) — the cell picks the fixture by its <dialect> CLI
# arg, so there is nothing to swap into the source tree.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$HERE/../.."
RESULTS="$HERE/../../benchmark/crosslang/.results"
REPS="${REPS:-400}"
WARMUP="${WARMUP:-40}"
mkdir -p "$RESULTS"
PG_SPEC='pg:host=localhost port=5433 user=testuser password=testpass dbname=testdb'
MYSQL_SPEC='mysql:mysql://testuser:testpass@localhost:3307/testdb'

echo "── sdk × sqlite (reps=$REPS) ──"
cargo build --manifest-path "$HERE/Cargo.toml" --release --quiet || { echo "sqlite build failed"; exit 1; }
rm -f /tmp/sdk_sqlite.db
"$HERE/target/release/orm_bench_sdk" sqlite /tmp/sdk_sqlite.db "$REPS" "$WARMUP" > "$RESULTS/sdk.sqlite.csv" || exit 1

echo "── sdk × postgres (reps=$REPS) ──"
cargo build --manifest-path "$HERE/Cargo.toml" --release --quiet --features livedb || { echo "livedb build failed"; exit 1; }
"$HERE/target/release/orm_bench_sdk" postgres "$PG_SPEC" "$REPS" "$WARMUP" > "$RESULTS/sdk.postgres.csv" || { echo "pg run failed"; exit 1; }

echo "── sdk × mysql (reps=$REPS) ──"
cargo build --manifest-path "$HERE/Cargo.toml" --release --quiet --features livedb || { echo "mysql build failed"; exit 1; }
"$HERE/target/release/orm_bench_sdk" mysql "$MYSQL_SPEC" "$REPS" "$WARMUP" > "$RESULTS/sdk.mysql.csv" || { echo "mysql run failed"; exit 1; }

{ head -1 "$RESULTS/sdk.sqlite.csv"; tail -n +2 -q "$RESULTS/sdk.sqlite.csv" "$RESULTS/sdk.postgres.csv" "$RESULTS/sdk.mysql.csv"; } > "$RESULTS/sdk.csv"
echo "sdk.csv: $(($(wc -l < "$RESULTS/sdk.csv") - 1)) samples"
