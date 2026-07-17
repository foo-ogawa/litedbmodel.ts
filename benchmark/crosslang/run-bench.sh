#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# Cross-language bench driver — the standalone-CSV flow.
# ════════════════════════════════════════════════════════════════════════════
#
# Brings the live DBs up (PG :5433 + MySQL :3307), sets the measurement BUDGETS via
# env (the ONLY way budgets reach each bench — there is no protocol), and runs the
# orchestrator (run.ts): build native cells → run each language's STANDALONE bench
# (each writes .results/<lang>.csv) → collect (CSVs → CROSS-LANG.md).
#
#   benchmark/crosslang/run-bench.sh                       # full authoritative run
#   BENCH_ITER=5 BENCH_WARMUP=2 benchmark/crosslang/run-bench.sh   # quick smoke
#   CROSSLANG_ONLY=ts,python benchmark/crosslang/run-bench.sh      # a subset
#   NO_DOCKER=1 benchmark/crosslang/run-bench.sh           # DBs already up
#
# Runs native arm64 (Apple Silicon): go arm64, node arm64, rust aarch64-apple-darwin.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# ── Budgets (defaults = the authoritative run; override in env for a smoke run) ──
export BENCH_WARMUP="${BENCH_WARMUP:-50}"
export BENCH_ITER="${BENCH_ITER:-300}"
export BENCH_TP_ITER="${BENCH_TP_ITER:-${BENCH_ITER}}"

# ── Node 22 (the pinned bench toolchain; v24 diverges — see the CI note) ─────────
if command -v nvm >/dev/null 2>&1; then nvm use 22 >/dev/null 2>&1 || true; fi
NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0)"
if [ "$NODE_MAJOR" != "22" ]; then
  echo "warning: node $(node -v 2>/dev/null) — the bench is pinned to Node 22 (numbers may differ)." >&2
fi

# ── Live DBs up (PG :5433 + MySQL :3307), unless told they are already running ───
DOCKER_UP=0
if [ "${NO_DOCKER:-0}" != "1" ]; then
  echo "Bringing up live DBs (PG :5433 + MySQL :3307)…" >&2
  npm run docker:livedb:up
  DOCKER_UP=1
  sleep 5
fi

cleanup() {
  if [ "$DOCKER_UP" = "1" ] && [ "${KEEP_DOCKER:-0}" != "1" ]; then
    npm run docker:livedb:down || true
  fi
}
trap cleanup EXIT

echo "Budgets: warmup=$BENCH_WARMUP iterations=$BENCH_ITER throughput-iter=$BENCH_TP_ITER" >&2
npx tsx benchmark/crosslang/run.ts
