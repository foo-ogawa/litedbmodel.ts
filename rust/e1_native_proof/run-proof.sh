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
MODULES=(generated_findunique generated_byids generated_recent generated_bymaybe generated_capped generated_feed generated_tenantfeed generated_relbatch generated_relsingle generated_createuser generated_renameuser generated_deleteuser generated_upsert generated_createmany generated_upsertmany generated_updatemany generated_txdelete generated_txnestedcreate generated_txnestedupdate generated_txnestedupsert generated_txrollback)
WRITE_OPS=(createuser renameuser deleteuser)
fail=0

for m in "${MODULES[@]}"; do
  if [[ ! -f "$PROOF_DIR/$m.rs" ]]; then
    echo "FATAL: $PROOF_DIR/$m.rs missing — run: npx vitest run test/scp/e1-native-sql-port.test.ts" >&2
    exit 2
  fi
  # Keep the crate's copy of each module in lockstep with the freshly emitted one — the bc native
  # module AND its litedbmodel-generated companion (the boundary-injected node_* handlers + wire adapter).
  cp "$PROOF_DIR/$m.rs" "$HERE/src/$m.rs"
  companion="companion_${m#generated_}"
  if [[ ! -f "$PROOF_DIR/$companion.rs" ]]; then
    echo "FATAL: $PROOF_DIR/$companion.rs missing — run: npx vitest run test/scp/e1-native-sql-port.test.ts" >&2
    exit 2
  fi
  cp "$PROOF_DIR/$companion.rs" "$HERE/src/$companion.rs"
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
node "$HERE/compare.mjs" "$BIN" "$PROOF_DIR/proof.db" tenantfeed "$PROOF_DIR/oracles_tenantfeed.json" || fail=1
node "$HERE/compare.mjs" "$BIN" "$PROOF_DIR/proof.db" relbatch "$PROOF_DIR/oracles_relbatch.json" || fail=1
node "$HERE/compare.mjs" "$BIN" "$PROOF_DIR/proof.db" relsingle "$PROOF_DIR/oracles_relsingle.json" || fail=1

echo "── leg 3c: BATCHED relation issues ONE child query (not N+1) ──"
# tenant 1 has 4 users; a batched relation runs 1 parent read + 1 batched child = 2 queries total.
# An N+1 cell would run 1 + 4 = 5. Assert exactly 2.
qc="$("$BIN" relbatch "$PROOF_DIR/proof.db" 1 2>&1 >/dev/null | sed -n 's/^queries=//p')"
if [[ "$qc" == "2" ]]; then
  echo "  PASS  relbatch(tenant 1, 4 users) issued $qc queries (1 parent + 1 BATCHED child, not N+1)"
else
  echo "  FAIL  relbatch issued $qc queries (expected 2 — batched; 5 would be N+1)"; fail=1
fi

echo "── leg 3d: BATCH write issues ONE statement for N records (not N) ──"
# createMany of 10 records: 1 batch INSERT + 1 state-read = 2 queries. An N+1 cell would be 10+1 = 11.
cmwork="$PROOF_DIR/proof.db.cmqc.work"; cp "$PROOF_DIR/write_seed.db" "$cmwork"
cmqc="$("$BIN" createmany "$cmwork" "$(printf 'q%s@x.com,' 0 1 2 3 4 5 6 7 8 9 | sed 's/,$//')" "$(printf 'N%s,' 0 1 2 3 4 5 6 7 8 9 | sed 's/,$//')" 2>&1 >/dev/null | sed -n 's/^queries=//p')"
if [[ "$cmqc" == "2" ]]; then
  echo "  PASS  createMany(10 records) issued $cmqc queries (1 BATCH insert + 1 state-read, not 11 = N+1)"
else
  echo "  FAIL  createMany issued $cmqc queries (expected 2; 11 would be N+1)"; fail=1
fi

echo "── leg 3b: WRITE execution + resulting DB state vs the mode-2 oracle (fresh copy per run) ──"
# A write MUTATES its DB, so each op runs on a FRESH copy of the clean seed. The binary prints
# {result, state}; the oracle carries the mode-2 {result, state}. compare_write.mjs copies the seed
# per case, runs the op, and asserts both — and fails on a non-zero exit (crash-path safe).
node "$HERE/compare_write.mjs" "$BIN" "$PROOF_DIR/write_seed.db" "$PROOF_DIR/oracles_write.json" || fail=1

echo "── leg 3e: TRANSACTION execution — RETURNING chain + BEGIN/COMMIT/ROLLBACK vs the mode-2 oracle ──"
# E5 (#120): each RETURNING-chained tx op (delete / nestedCreate / nestedUpdate / nestedUpsert) + the
# ROLLBACK control runs on a FRESH copy of the users+posts seed. The binary prints {result:{committed},
# state:{users,posts}}; the oracle carries the mode-2 executeTransactionBundle {committed} + resulting
# state. The state proves the chain (post.author_id IS the user's RETURNING id) and the rollback control
# proves atomicity (statement 2 fails → statement 1's effect undone → committed:false, state unchanged).
node "$HERE/compare_write.mjs" "$BIN" "$PROOF_DIR/tx_seed.db" "$PROOF_DIR/oracles_tx.json" || fail=1

echo "── leg 3f: FIND HARD-LIMIT — the auto-wired guarded native find trips LimitExceededError (#135/#136) ──"
# The capped find (findHardLimit=2 ⇒ baked LIMIT 3) over the seed (> 2 users) must trip the SHARED
# check_find_hard_limit: `run` returns RuntimeError::Limit (context=find, limit=2, N+1 fetch count=3),
# byte-equal to what mode-2's assert_find_guard raises. Proves the guarded companion `run` COMPILES
# (crate build) AND enforces the cap end-to-end (not just an emission string-assert).
capout="$("$BIN" capped "$PROOF_DIR/proof.db")"
if [[ "$capout" == "LIMIT:find:2:3" ]]; then
  echo "  PASS  capped find: guarded run() tripped $capout (cap 2 < N+1 fetch 3)"
else
  echo "  FAIL  capped find: expected 'LIMIT:find:2:3', got '$capout'"; fail=1
fi

echo
if [[ $fail -eq 0 ]]; then echo "E1 PROOF: ALL LEGS PASS"; else echo "E1 PROOF: FAILURES ABOVE"; fi
exit $fail
