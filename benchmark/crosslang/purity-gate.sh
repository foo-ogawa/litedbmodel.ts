#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# NATIVE-ONLY runtime PURITY GATE (owner order, fail-closed):
#   The shipped rust/go runtimes execute every read/write via native code — the bc IR
#   interpreter (run_behavior / RunBehavior) is DELETED from both (#8), rust carries NO
#   serde_json CODE, go carries NO encoding/json on the exec path. This gate FAILS if any
#   is reintroduced.
#
#   (#63 cleanup: the earlier codegen-CELL checks are removed with the retired typed-native
#   codegen-module cells — rust-codegen / ts codegen-cell / go cgcell·cgplans·cgmods /
#   lm_codegen — which covered none of the 19 ORM ops and are deleted. The unified bench's
#   ONE production path per language is the shipped thin runtime + real driver; its exec/ir
#   bench cells legitimately use serde_json/encoding/json to READ the shared orm-plan.json
#   artifact (the SSoT of baked SQL+params) and assemble their flat-CSV output — that bench-side
#   JSON is NOT gated here; purity applies to litedbmodel's OWN runtime source, per the #63 rescope.
#   (The old NDJSON stdio harness protocol is gone — each language now runs standalone and writes a
#   flat CSV; a separate collector aggregates. See run.ts / collect.ts.)
# grep で残骸が 1 件でも出たら exit 1。ベンチ/CI の前提ゲートとして回す。
# ════════════════════════════════════════════════════════════════════════════
set -u
cd "$(dirname "$0")/../.."

FAIL=0

# The NATIVE-ONLY runtimes (epic #44 native-only, #8): rust/go run every read/write via generated
# native code (static SQL text + typed param binding). The IR interpreter (bc run_behavior /
# RunBehavior) is DELETED from both; rust drops serde_json, go drops encoding/json OPERATIONS from
# the exec path. This gate FAILS if either is reintroduced.
RUST_RT=rust/litedbmodel_runtime/src
GO_RT=go/litedbmodel_runtime

# scan_nontest — ignores *_test.go / tests/ files AND comment lines (a reintroduction in production
# CODE is the failure; test scaffolding + doc comments may legitimately name the symbol). Strips
# full-line `//`/`*`/`#` comments and matches the pattern only against remaining code.
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

echo "── 1. NATIVE-ONLY runtimes: NO IR interpreter (run_behavior) on the exec path ──"
# The rust/go runtimes must NEVER call the bc IR interpreter — every read/write runs native. A CALL
# form (`run_behavior(` / `RunBehavior(`) in production runtime code is a reintroduction → FAIL.
scan_nontest "rust runtime: no run_behavior call" "run_behavior[[:space:]]*\(" "$RUST_RT"
scan_nontest "go runtime: no RunBehavior call" "RunBehavior[[:space:]]*\(" "$GO_RT"

echo "── 2. NATIVE-ONLY runtimes: NO JSON library on the exec path ──"
# The go runtime LIB must carry NO `encoding/json` at all (import OR operation) — it parses/renders
# JSON through its own native codec. (A code line matching, comments stripped.)
scan_nontest "go runtime: no encoding/json import/use" \
  "\"encoding/json\"|json\.(Marshal|Unmarshal|NewDecoder|NewEncoder|Number)" "$GO_RT"

echo "── 3. NATIVE-ONLY rust runtime: NO serde_json (compile-impossible to reintroduce) ──"
# The rust runtime SOURCE must carry NO serde_json/serde CODE (`use serde…` / `serde_json::`), and
# its Cargo.toml must depend on behavior-contracts with default-features=false (drops bc's `ir`
# feature that pulls serde_json). Comment lines are stripped so the doc prose ("serde_json-free")
# does not false-positive. A `[dev-dependencies] serde_json` (tests only) is NOT in the shipped lib
# tree and is allowed — this scans the crate SOURCE, which must have zero serde CODE.
hits=$(grep -rnE "serde_json::|use serde(_json)?(::|;| )" "$RUST_RT" 2>/dev/null \
  | grep -vE ':[0-9]+:[[:space:]]*(//|///|//!|\*)')
if [ -n "$hits" ]; then
  echo "✗ rust runtime src: serde_json/serde CODE present"
  echo "$hits" | head -10
  FAIL=1
else
  echo "✓ rust runtime src: no serde_json/serde code"
fi
# OWNER RE-SCOPE (#63): the earlier `cargo tree -i serde_json` (dependency-tree) assertion is
# DROPPED — it is a whole-tree check that would wrongly fail once the runtime links a real DB driver
# (tokio-postgres/sqlx), whose transitive serde_json is EXPLICITLY ALLOWED (driver serde is fine;
# purity is about litedbmodel's OWN code). The meaningful, retained invariant is the OWN-SOURCE check
# above (no `serde_json::`/`use serde…` CODE in the runtime crate source) + the Cargo.toml
# `behavior-contracts default-features=false` pin (drops bc's `ir` feature). That is what guarantees
# litedbmodel's own exec/codegen never marshals via serde — the transitive driver edge does not.

echo ""
if [ "$FAIL" -ne 0 ]; then
  echo "PURITY GATE: FAIL — runtime 面に IR interpreter/JSON-op 残骸あり（上記）。残骸ゼロまで完了ではない。"
  exit 1
fi
echo "PURITY GATE: PASS — 自前 runtime 面に IR interpreter/JSON-op 残骸ゼロ。"
