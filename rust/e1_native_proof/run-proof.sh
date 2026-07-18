#!/usr/bin/env bash
# E1 (#116, epic #115) PROOF-OF-APPROACH — the out-of-process verification legs.
#
# Prerequisite: the TS leg has emitted the artifacts into /tmp/e1proof —
#   npx vitest run test/scp/e1-native-sql-port.test.ts
# which writes generated_findunique.rs (the module), proof.db (seeded per benchmark/crosslang/
# orm-domain.ts), and oracles.json (input -> the mode-2 `executeBundle` result).
#
# Legs:
#   1. RUNTIME-FREE COMPILE — rustc the generated module with NO --extern behavior_contracts.
#   2. PURITY — the comment-stripped module names no runtime/boxing/JSON primitive.
#   3. EXECUTION BYTE-EQUALITY — run the module (through the exec seam) against the seeded DB and
#      compare to the mode-2 oracle for every input.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROOF_DIR=/tmp/e1proof
MODULES=(generated_findunique generated_byids generated_recent generated_bymaybe generated_feed generated_createuser generated_renameuser generated_deleteuser)
WRITE_OPS=(createuser renameuser deleteuser)
fail=0

for m in "${MODULES[@]}"; do
  if [[ ! -f "$PROOF_DIR/$m.rs" ]]; then
    echo "FATAL: $PROOF_DIR/$m.rs missing — run: npx vitest run test/scp/e1-native-sql-port.test.ts" >&2
    exit 2
  fi
  # Keep the crate's copy of each module in lockstep with the freshly emitted one.
  cp "$PROOF_DIR/$m.rs" "$HERE/src/$m.rs"
done

work="$(mktemp -d)"

echo "── leg 1: each generated module compiles RUNTIME-FREE (no --extern behavior_contracts) ──"
for m in "${MODULES[@]}"; do
  if rustc --edition 2021 --crate-type lib --emit metadata -o "$work/$m.rmeta" "$PROOF_DIR/$m.rs" 2>"$work/err"; then
    echo "  PASS  $m: rustc --emit metadata (std only)"
  else
    echo "  FAIL  $m: rustc rejected the module:"; sed 's/^/        /' "$work/err"; fail=1
  fi
done

echo "── leg 2: purity — no runtime / boxing / JSON primitive in code (comments stripped) ──"
for m in "${MODULES[@]}"; do
  stripped="$work/$m.stripped.rs"
  perl -0777 -pe 's{("(?:[^"\\]|\\.)*")|//[^\n]*|/\*.*?\*/}{defined $1 ? $1 : ""}ges' "$PROOF_DIR/$m.rs" > "$stripped"
  bad=0
  for marker in 'serde_json' 'Box<dyn' 'dyn Any' 'run_behavior' 'behavior_contracts' 'RawValue' 'obj_native' 'run_plan'; do
    if grep -qF -- "$marker" "$stripped"; then echo "  FAIL  $m: found '$marker'"; bad=1; fail=1; fi
  done
  [[ $bad -eq 0 ]] && echo "  PASS  $m: no runtime/boxing/JSON primitive"
  # each module must carry its own SQL as a baked literal — either the single `f_sql` port, or the
  # fragmented skip shape's `f_sql_head` (head + baked WHERE fragments the seam assembles).
  if grep -qE 'f_sql(_head)?: "(SELECT|INSERT|UPDATE|DELETE)[^"]*"\.to_string\(\)' "$stripped"; then
    echo "  PASS  $m: the SQL is baked as a native literal IN the module"
  else
    echo "  FAIL  $m: the module does not carry its SQL"; fail=1
  fi
done

echo "── leg 3: execution byte-equality vs the mode-2 executeBundle oracle ──"
cargo build --quiet --manifest-path "$HERE/Cargo.toml" || { echo "  FAIL  proof crate build"; exit 1; }
BIN="$HERE/target/debug/e1_native_proof"

# findUnique — a required scalar head. byIds — the IN-list array bind (incl. the EMPTY list).
# The driver passes each input verbatim and fails on a non-zero exit, so a panic cannot pass as an
# empty result (a bash read-loop previously did exactly that — a false PASS).
node "$HERE/compare.mjs" "$BIN" "$PROOF_DIR/proof.db" findunique "$PROOF_DIR/oracles.json" || fail=1
node "$HERE/compare.mjs" "$BIN" "$PROOF_DIR/proof.db" byids "$PROOF_DIR/oracles_byids.json" || fail=1
node "$HERE/compare.mjs" "$BIN" "$PROOF_DIR/proof.db" recent "$PROOF_DIR/oracles_recent.json" || fail=1
node "$HERE/compare.mjs" "$BIN" "$PROOF_DIR/proof.db" bymaybe "$PROOF_DIR/oracles_bymaybe.json" || fail=1
node "$HERE/compare.mjs" "$BIN" "$PROOF_DIR/proof.db" feed "$PROOF_DIR/oracles_feed.json" || fail=1

echo "── leg 3b: WRITE execution + resulting DB state vs the mode-2 oracle (fresh copy per run) ──"
# A write MUTATES its DB, so each op runs on a FRESH copy of the clean seed. The binary prints
# {result, state}; the oracle carries the mode-2 {result, state}. compare_write.mjs copies the seed
# per case, runs the op, and asserts both — and fails on a non-zero exit (crash-path safe).
node "$HERE/compare_write.mjs" "$BIN" "$PROOF_DIR/write_seed.db" "$PROOF_DIR/oracles_write.json" || fail=1

echo
if [[ $fail -eq 0 ]]; then echo "E1 PROOF: ALL LEGS PASS"; else echo "E1 PROOF: FAILURES ABOVE"; fi
exit $fail
