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
GO_CELL=go/lm_bench/codegen_cell.go
GO_PLANS=go/lm_bench/cgplans
GO_MODS=go/lm_bench/cgmods

echo "── 1. codegen 出力に IR データ / fingerprint が無い ──"
scan "generated: IR fingerprint 定数なし" \
  "IR_FINGERPRINT|IRFingerprint|irVersion" "$GEN" "$GO_MODS" "$GO_PLANS"
scan "generated: 埋め込み IR なし" \
  "\"components\"[[:space:]]*:|\"wires\"[[:space:]]*:|bundleToPortableIR" "$GEN" "$GO_MODS" "$GO_PLANS"

echo "── 2. codegen 実行系に JSON ライブラリなし ──"
scan "rust-codegen: serde_json なし" "serde_json|json!" "$RUST_CG"
scan "go codegen: encoding/json import なし" "\"encoding/json\"" "$GO_CELL" "$GO_PLANS" "$GO_MODS"
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

echo ""
if [ "$FAIL" -ne 0 ]; then
  echo "PURITY GATE: FAIL — codegen 面に IR/JSON 残骸あり（上記）。残骸ゼロまで完了ではない。"
  exit 1
fi
echo "PURITY GATE: PASS — codegen 面に IR/JSON 残骸ゼロ。"
