#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# The payoff latency bench — ts-IR-interpreter vs rust-native vs go-native (sqlite in-proc).
# Builds all three cells, runs the SAME 4 ops × the SAME iteration count against the SAME seed, and
# collects the p50/p99/ops-sec table. Scrupulously fair: identical workload, identical C-sqlite engine,
# whole-hot-path timing, warmup then measure; the native cells are the REAL runtime-free generated code.
#
# Usage: benchmark/crosslang/latency/run.sh [warmup] [iters]
# ════════════════════════════════════════════════════════════════════════════
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
WARMUP="${1:-1000}"
ITERS="${2:-10000}"
ART="$HERE/.artifacts"
RESULTS="$HERE/.results"
GO="${GO:-$(command -v go || echo /usr/local/bin/go)}"
RUST_PROOF="$ROOT/rust/e1_native_proof"
RUST_BIN="$RUST_PROOF/target/release/e1_native_proof"
GO_BIN="$HERE/go-cell/go_bench_cell"
mkdir -p "$RESULTS"
fail=0

echo "── step 0/5: build the litedbmodel bundle (dist/scp/index.cjs) the ts-IR cell + gen consume ──"
( cd "$ROOT" && npm run build:scp --silent ) || { echo "  FAIL build:scp"; exit 1; }

echo "── step 1/5: gen — emit go-typed-native modules + shared seed DBs + fairness cross-check ──"
npx vitest run --config "$HERE/vitest.config.ts" "$HERE/gen.test.ts" || { echo "  FAIL gen"; exit 1; }

echo "── step 2/5: build the native cells (rust RELEASE, no query-counter; go optimized) ──"
# --no-default-features drops the QUERY_COUNT atomic so it never runs in the timed hot path (the proof
# legs keep it via run-proof.sh's default build). prepare_cached statement reuse is unconditional.
( cd "$RUST_PROOF" && cargo build --release --no-default-features --quiet ) || { echo "  FAIL rust build"; exit 1; }
( cd "$HERE/go-cell" && GOFLAGS=-mod=mod "$GO" build -o "$GO_BIN" . ) || { echo "  FAIL go build"; exit 1; }

echo "── step 2b: runtime-free confirmation (the 'genuinely native' half) ──"
# rust: rustc each generated module with NO --extern behavior_contracts (already proven by run-proof.sh
# leg 1; re-assert the 4 bench modules here as metadata compiles).
for m in findunique relsingle createuser createmany; do
  if rustc --edition 2021 --crate-type lib --emit metadata -o /tmp/rf_$m.rmeta "$RUST_PROOF/src/generated_$m.rs" 2>/dev/null; then
    echo "  PASS  rust generated_$m: compiles runtime-free (no --extern behavior_contracts)"
  else echo "  FAIL  rust generated_$m runtime-free compile"; fail=1; fi
done
# go: the whole cell's dependency graph must contain NO behavior-contracts go runtime.
GO_DEPS="$( cd "$HERE/go-cell" && GOFLAGS=-mod=mod "$GO" list -deps . 2>/dev/null )"
if echo "$GO_DEPS" | grep -qiE 'behavior-contracts|coderuntime|dslcontracts'; then
  echo "  FAIL  go cell depends on a bc runtime:"; echo "$GO_DEPS" | grep -iE 'behavior-contracts|coderuntime|dslcontracts'; fail=1
else
  echo "  PASS  go cell: go list -deps shows NO behavior-contracts runtime (driver mattn-go-sqlite3 only)"
fi

echo "── step 3/5: run the rust-native cell (fresh write seed) ──"
cp "$ART/write.db" "$ART/rust_write.db"
"$RUST_BIN" bench "$ART/read.db" "$ART/rust_write.db" "$ART/rel.db" "$WARMUP" "$ITERS" "$RESULTS/rust.csv" || { echo "  FAIL rust cell"; fail=1; }

echo "── step 4/5: run the go-native cell (fresh write seed) ──"
cp "$ART/write.db" "$ART/go_write.db"
"$GO_BIN" "$ART/read.db" "$ART/go_write.db" "$ART/rel.db" "$WARMUP" "$ITERS" "$RESULTS/go.csv" || { echo "  FAIL go cell"; fail=1; }

echo "── step 5/5: run the ts-IR interpreter cell (standalone tsx) + collect the table ──"
cp "$ART/write.db" "$ART/ts_write.db"
npx tsx "$HERE/ts-ir.ts" "$ART/read.db" "$ART/ts_write.db" "$ART/rel.db" "$WARMUP" "$ITERS" "$RESULTS/ts_ir.csv" || { echo "  FAIL ts cell"; fail=1; }
rm -f "$ART/rust_write.db" "$ART/go_write.db" "$ART/ts_write.db"
node "$HERE/collect.mjs"

[[ $fail -eq 0 ]] && echo "LATENCY BENCH: OK" || echo "LATENCY BENCH: FAILURES ABOVE"
exit $fail
