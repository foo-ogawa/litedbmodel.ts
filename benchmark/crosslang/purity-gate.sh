#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# codegen PURITY GATE (owner order, fail-closed):
#   1. codegen 出力（生成モジュール・companion・cgplans/cgmods）に IR データ・
#      IR 由来 fingerprint を一切含めない
#   2. codegen 実行パス（分離バイナリ / codegen セル）は JSON ライブラリ・
#      IR 参照・interpreter 呼び出しゼロ
# grep で残骸が 1 件でも出たら exit 1。ベンチ/CI の前提ゲートとして回す。
# ════════════════════════════════════════════════════════════════════════════
set -u
cd "$(dirname "$0")/../.."

FAIL=0
scan() { # scan <label> <pattern> <path...>
  local label="$1" pattern="$2"; shift 2
  local hits
  hits=$(grep -rnE "$pattern" "$@" 2>/dev/null)
  if [ -n "$hits" ]; then
    echo "✗ $label"
    echo "$hits" | head -10
    FAIL=1
  else
    echo "✓ $label"
  fi
}

GEN=benchmark/crosslang/generated/codegen
RUST_CG=benchmark/crosslang/adapters/rust-codegen/src
TS_CELL=benchmark/crosslang/adapters/ts/codegen-cell.ts
# The go codegen cell logic lives in the importable `cgcell` package; the dedicated json-free/rt-free
# binary is `go/lm_codegen`.
GO_CELL=go/lm_bench/cgcell/cell.go
GO_CODEGEN_BIN_SRC=go/lm_codegen/main.go
GO_PLANS=go/lm_bench/cgplans
GO_MODS=go/lm_bench/cgmods
# The NATIVE-ONLY runtimes (epic #44 native-only, #8): rust/go run every read/write via generated
# native code (static SQL text + typed param binding). The IR interpreter (bc run_behavior /
# RunBehavior) is DELETED from both; rust drops serde_json, go drops encoding/json OPERATIONS from
# the exec path. This gate FAILS if either is reintroduced.
RUST_RT=rust/litedbmodel_runtime/src
GO_RT=go/litedbmodel_runtime

echo "── 1. codegen 出力に IR データ / fingerprint が無い ──"
scan "generated: IR fingerprint 定数なし" \
  "IR_FINGERPRINT|IRFingerprint|irVersion" "$GEN" "$GO_MODS" "$GO_PLANS"
scan "generated: 埋め込み IR なし" \
  "\"components\"[[:space:]]*:|\"wires\"[[:space:]]*:|bundleToPortableIR" "$GEN" "$GO_MODS" "$GO_PLANS"

echo "── 2. codegen 実行系に JSON ライブラリ / 実行系 runtime なし ──"
scan "rust-codegen: serde_json なし" "serde_json|json!" "$RUST_CG"
scan "go codegen cell/binary: encoding/json import なし" "\"encoding/json\"" "$GO_CELL" "$GO_CODEGEN_BIN_SRC" "$GO_PLANS" "$GO_MODS"
# The dedicated go lm_codegen binary + the cgcell package must NOT import litedbmodel_runtime (the
# codegen path links NO interpreter/exec crate — mirrors rust-codegen's serde-free crate isolation).
scan "go codegen cell/binary: no litedbmodel_runtime import" "litedbmodel/go/litedbmodel_runtime" "$GO_CELL" "$GO_CODEGEN_BIN_SRC"
scan "generated rust: serde_json なし" "serde_json" "$GEN/rust"

echo "── 3. codegen 実行系に IR 参照 / interpreter 呼び出しなし ──"
scan "cells: fingerprint/IR 参照なし" \
  "fingerprint_component_graph|FingerprintComponentGraph|fingerprintComponentGraph|IR_FINGERPRINT|IRFingerprint|bundleToPortableIR|readGraph\.ir|ReadGraph\.IR" \
  "$RUST_CG" "$TS_CELL" "$GO_CELL" "$GO_PLANS"
scan "cells+generated: interpreter 呼び出しなし（call 形）" \
  "run_behavior\(|RunBehavior\(|runBehavior\(" "$RUST_CG" "$TS_CELL" "$GO_CELL" "$GEN"

echo "── 4. rust-codegen クレートの依存に serde_json なし ──"
if [ -d benchmark/crosslang/adapters/rust-codegen ]; then
  if (cd benchmark/crosslang/adapters/rust-codegen && cargo tree 2>/dev/null | grep -q serde); then
    echo "✗ rust-codegen: cargo tree に serde 系が存在"
    FAIL=1
  else
    echo "✓ rust-codegen: cargo tree に serde 系なし"
  fi
fi

# scan_nontest — like scan, but ignores *_test.go / tests/ files AND comment lines (a reintroduction
# in production CODE is the failure; test scaffolding + doc comments may legitimately name the symbol).
# Strips full-line `//`/`*`/`#` comments and matches the pattern only against remaining code.
scan_nontest() {
  local label="$1" pattern="$2"; shift 2
  local hits
  hits=$(grep -rnE "$pattern" "$@" 2>/dev/null \
    | grep -vE '_test\.go|/tests/' \
    | grep -vE ':[0-9]+:[[:space:]]*(//|\*|#|///|//!)')
  if [ -n "$hits" ]; then
    echo "✗ $label"
    echo "$hits" | head -10
    FAIL=1
  else
    echo "✓ $label"
  fi
}

echo "── 5. NATIVE-ONLY runtimes: NO IR interpreter (run_behavior) on the exec path ──"
# The rust/go runtimes must NEVER call the bc IR interpreter — every read/write runs native. A CALL
# form (`run_behavior(` / `RunBehavior(`) in production runtime code is a reintroduction → FAIL.
scan_nontest "rust runtime: no run_behavior call" "run_behavior[[:space:]]*\(" "$RUST_RT"
scan_nontest "go runtime: no RunBehavior call" "RunBehavior[[:space:]]*\(" "$GO_RT"

echo "── 6. NATIVE-ONLY runtimes: NO JSON library on the exec path ──"
# The go runtime LIB must carry NO `encoding/json` at all (import OR operation) — it parses/renders
# JSON through its own native codec. (A code line matching, comments stripped.)
scan_nontest "go runtime: no encoding/json import/use" \
  "\"encoding/json\"|json\.(Marshal|Unmarshal|NewDecoder|NewEncoder|Number)" "$GO_RT"

echo "── 7. NATIVE-ONLY rust runtime: NO serde_json (compile-impossible to reintroduce) ──"
# The rust runtime SOURCE must carry NO serde_json/serde CODE (`use serde…` / `serde_json::`), and
# its Cargo.toml must depend on behavior-contracts with default-features=false (drops bc's `ir`
# feature that pulls serde_json). Comment lines are stripped so the doc prose ("serde_json-free")
# does not false-positive. A `[dev-dependencies] serde_json` (tests only) is NOT in the shipped lib
# tree and is allowed — this scans the crate SOURCE, which must have zero serde CODE.
RUST_RT_SRC="$RUST_RT"
hits=$(grep -rnE "serde_json::|use serde(_json)?(::|;| )" "$RUST_RT_SRC" 2>/dev/null \
  | grep -vE ':[0-9]+:[[:space:]]*(//|///|//!|\*)')
if [ -n "$hits" ]; then
  echo "✗ rust runtime src: serde_json/serde CODE present"
  echo "$hits" | head -10
  FAIL=1
else
  echo "✓ rust runtime src: no serde_json/serde code"
fi
# The runtime crate's cargo tree (normal edges = the shipped lib) must NOT contain serde_json.
if (cd rust && cargo tree -p litedbmodel_runtime -i serde_json --edges normal 2>/dev/null | grep -q serde_json); then
  echo "✗ rust runtime: cargo tree (normal edges) contains serde_json"
  FAIL=1
else
  echo "✓ rust runtime: cargo tree (normal edges) has no serde_json"
fi

echo ""
if [ "$FAIL" -ne 0 ]; then
  echo "PURITY GATE: FAIL — codegen/runtime 面に IR/JSON 残骸あり（上記）。残骸ゼロまで完了ではない。"
  exit 1
fi
echo "PURITY GATE: PASS — codegen/runtime 面に IR interpreter/JSON-op 残骸ゼロ。"
