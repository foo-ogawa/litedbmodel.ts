#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# ORM-bench NATIVE codegen driver (#141) — the REPRODUCIBLE bc-CLI pipeline.
#
#   ./gen-native.sh generate   # dump the IR, then `bc generate` → behaviors_generated.rs
#   ./gen-native.sh check      # dump the IR, then `bc check` (drift gate; exit 1 on drift)
#
# There is NO litedbmodel code in the generation OR verification path: `native-model.mts` only AUTHORS
# the SCP ops + `publishBehaviors` + dumps `contract.ir` VERBATIM; the native module is produced by
# bc's own `bc generate`, and the drift gate is bc's own `bc check` — both over the SAME committed IR
# doc + the SAME flags (defined once, below). See NATIVE_RELATION_PLAN.md.
# ════════════════════════════════════════════════════════════════════════════
set -euo pipefail
MODE="${1:-generate}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
IR="$HERE/.ir/native.ir.json"
OUT="$ROOT/rust/orm_bench/src/gen/behaviors_generated.rs"
WIRE="$ROOT/rust/litedbmodel_runtime/src/wire.rs"
BC="$ROOT/node_modules/.bin/bc"

# The SSoT flag set — `generate` and `check` MUST use identical flags (bc `check` re-generates and
# byte-diffs BOTH the covered module --out AND the shared wire-type module --shared-types-out). The
# shared wire types (WireValue/WireRow/WireList + runtime-free BehaviorError) are BC-generated (#165 /
# bc#167) into `litedbmodel_runtime/src/wire.rs` — no hand-placement. The op-agnostic leaves map to the
# runtime transport symbols execute_sql / pluck_keys / group_children.
FLAGS=(--lang rust-typed-native --in "$IR" --out "$OUT" --shared-types-out "$WIRE"
  --runtime-import litedbmodel_runtime --shared-types-import litedbmodel_runtime
  --leaf-transport executeSQL=execute_sql pluck=pluck_keys group=group_children)

# 1) Author + publish + dump the IR VERBATIM (nothing transforms it after publish).
npx tsx "$HERE/native-model.mts"

# 2) Generate (or drift-check) via bc's own CLI.
case "$MODE" in
  generate) "$BC" generate "${FLAGS[@]}"; echo "bc generate → $OUT" ;;
  check)    "$BC" check "${FLAGS[@]}" ;;
  *) echo "usage: gen-native.sh [generate|check]" >&2; exit 2 ;;
esac
