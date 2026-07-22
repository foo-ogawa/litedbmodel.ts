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
#
# The SAME IR feeds EVERY native language leg (language-agnostic): rust AND go emit from the one
# `.ir/native.ir.json`. Each leg has its own emitter (`--lang`), --out module, --shared-types-out wire
# module + import specifiers, and leaf-transport symbol map; the flag sets are otherwise identical in
# shape, so `generate` and `check` share them per-leg.
# ════════════════════════════════════════════════════════════════════════════
set -euo pipefail
MODE="${1:-generate}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
IR="$HERE/.ir/native.ir.json"
OUT="$ROOT/rust/orm_bench/src/gen/behaviors_generated.rs"
WIRE="$ROOT/rust/litedbmodel_runtime/src/wire.rs"
BC="$ROOT/node_modules/.bin/bc"

# The rust SSoT flag set — `generate` and `check` MUST use identical flags (bc `check` re-generates and
# byte-diffs BOTH the covered module --out AND the shared wire-type module --shared-types-out). The
# shared wire types (WireValue/WireRow/WireList + runtime-free BehaviorError) are BC-generated (#165 /
# bc#167) into `litedbmodel_runtime/src/wire.rs` — no hand-placement. The op-agnostic leaves map to the
# runtime transport symbols execute_sql / pluck_keys / group_children.
FLAGS=(--lang rust-typed-native --in "$IR" --out "$OUT" --shared-types-out "$WIRE"
  --runtime-import litedbmodel_runtime --shared-types-import litedbmodel_runtime
  --leaf-transport executeSQL=execute_sql pluck=pluck_keys group=group_children)

# The go SSoT flag set — the go-typed-native twin of the rust leg over the SAME IR. The covered module
# lands in the bench cell package (`go/lm_bench/lm_orm_native/gen`); the BC-OWNED shared wire types
# (WireValue/WireRow/WireList + probe kinds) are BC-generated (--shared-types-out) into the wire package
# `go/litedbmodel_runtime/wire/wire.go` — no hand-placement. The go covered module is a SEPARATE package
# from the leaf-transport runtime, so the transport symbols are package-qualified: --leaf-transport-import
# carries the runtime package path and --leaf-transport maps executeSQL/pluck/group → the exported
# ExecuteSQL / PluckKeys / GroupChildren the runtime provides.
GO_RT="github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime"
GO_WIRE_PKG="$GO_RT/wire"
GO_OUT="$ROOT/go/lm_bench/lm_orm_native/gen/behaviors_generated.go"
GO_WIRE="$ROOT/go/litedbmodel_runtime/wire/wire.go"
GO_FLAGS=(--lang go-typed-native --in "$IR" --out "$GO_OUT" --shared-types-out "$GO_WIRE"
  --runtime-import "$GO_RT" --shared-types-import "$GO_WIRE_PKG"
  --leaf-transport executeSQL=ExecuteSQL pluck=PluckKeys group=GroupChildren
  --leaf-transport-import "$GO_RT")

# The python SSoT flag set — the LITERAL (ir-exec) twin over the SAME IR (epic #123: ts/go/rust =
# native de-box; py/php = literal). The python emitter embeds the portable IR as a dict literal and
# hands it to the shared runtime core (`behavior_contracts.run_behavior`) via `bind(handlers)` — it
# generates NO per-op exec logic and NO typed wire, so the typed-native flags do NOT apply
# (`--shared-types-out`/`--leaf-transport` are rejected by the python emitter). The op-agnostic leaf
# transport (executeSQL/pluck/group) is injected at RUNTIME by the bench cell via
# `litedbmodel_runtime.make_handlers`, not baked at generate time.
PY_OUT="$ROOT/python/orm_bench/behaviors_generated.py"
PY_FLAGS=(--lang python --in "$IR" --out "$PY_OUT")

# The php SSoT flag set — the LITERAL (ir-exec) twin over the SAME IR, mirror of the python leg (epic
# #123: ts/go/rust = native de-box; py/php = literal). The php emitter embeds the portable IR as a
# native stdClass/array literal and hands it to the shared runtime core (`Behavior::runBehavior`) via
# `bind($handlers)` — it generates NO per-op exec logic and NO typed wire, so the typed-native flags do
# NOT apply (`--shared-types-out`/`--leaf-transport` are rejected by the php emitter). Unlike python
# (which consumes behavior-contracts from PyPI), the php bc runtime-core is VENDORED under
# `LiteDbModel\Runtime\BehaviorContracts` (bc php is unpublished — vendor:bc-php), so `--runtime-import`
# points the generated module's require-time gates (SpecVersions/Fingerprint) + `runBehavior` call at
# that vendored namespace. The op-agnostic leaf transport (executeSQL/pluck/group) is injected at
# RUNTIME by the bench cell via `Leaves::makeHandlers`, not baked at generate time.
PHP_OUT="$ROOT/php/orm_bench/behaviors_generated.php"
PHP_FLAGS=(--lang php --in "$IR" --out "$PHP_OUT" --runtime-import 'LiteDbModel\Runtime\BehaviorContracts')

# 1) Author + publish + dump the IR VERBATIM (nothing transforms it after publish).
npx tsx "$HERE/native-model.mts"

# 2) Generate (or drift-check) EACH native leg via bc's own CLI, over the SAME IR.
case "$MODE" in
  generate)
    "$BC" generate "${FLAGS[@]}";     echo "bc generate → $OUT"
    "$BC" generate "${GO_FLAGS[@]}";  echo "bc generate → $GO_OUT"
    "$BC" generate "${PY_FLAGS[@]}";  echo "bc generate → $PY_OUT"
    "$BC" generate "${PHP_FLAGS[@]}"; echo "bc generate → $PHP_OUT" ;;
  check)
    "$BC" check "${FLAGS[@]}"
    "$BC" check "${GO_FLAGS[@]}"
    "$BC" check "${PY_FLAGS[@]}"
    "$BC" check "${PHP_FLAGS[@]}" ;;
  *) echo "usage: gen-native.sh [generate|check]" >&2; exit 2 ;;
esac
