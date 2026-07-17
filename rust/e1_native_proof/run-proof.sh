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
MODULE="$PROOF_DIR/generated_findunique.rs"
fail=0

if [[ ! -f "$MODULE" ]]; then
  echo "FATAL: $MODULE missing — run: npx vitest run test/scp/e1-native-sql-port.test.ts" >&2
  exit 2
fi

# Keep the crate's copy of the module in lockstep with the freshly emitted one.
cp "$MODULE" "$HERE/src/generated_findunique.rs"

echo "── leg 1: the generated module compiles RUNTIME-FREE (no --extern behavior_contracts) ──"
work="$(mktemp -d)"
if rustc --edition 2021 --crate-type lib --emit metadata -o "$work/out.rmeta" "$MODULE" 2>"$work/err"; then
  echo "  PASS  rustc --emit metadata (std only)"
else
  echo "  FAIL  rustc rejected the module:"; sed 's/^/        /' "$work/err"; fail=1
fi

echo "── leg 2: purity — no runtime / boxing / JSON primitive in code (comments stripped) ──"
stripped="$work/stripped.rs"
perl -0777 -pe 's{("(?:[^"\\]|\\.)*")|//[^\n]*|/\*.*?\*/}{defined $1 ? $1 : ""}ges' "$MODULE" > "$stripped"
for m in 'serde_json' 'Box<dyn' 'dyn Any' 'run_behavior' 'behavior_contracts' 'RawValue' 'obj_native' 'run_plan'; do
  if grep -qF -- "$m" "$stripped"; then echo "  FAIL  found '$m'"; fail=1; else echo "  PASS  no '$m'"; fi
done
if grep -qF 'SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT ?' "$stripped"; then
  echo "  PASS  the SQL is baked as a native literal IN the module"
else
  echo "  FAIL  the module does not carry its SQL"; fail=1
fi

echo "── leg 3: execution byte-equality vs the mode-2 executeBundle oracle ──"
cargo build --quiet --manifest-path "$HERE/Cargo.toml" || { echo "  FAIL  proof crate build"; exit 1; }
BIN="$HERE/target/debug/e1_native_proof"
# Replay each oracle input through the generated module + exec seam; compare byte-for-byte.
while IFS=$'\t' read -r email expected; do
  actual="$("$BIN" "$PROOF_DIR/proof.db" "$email")"
  if [[ "$actual" == "$expected" ]]; then
    echo "  PASS  $email -> $actual"
  else
    echo "  FAIL  $email"; echo "        rust  : $actual"; echo "        oracle: $expected"; fail=1
  fi
done < <(node -e '
  const o = require("/tmp/e1proof/oracles.json");
  for (const [k, v] of Object.entries(o)) process.stdout.write(k + "\t" + JSON.stringify(v) + "\n");
')

echo
if [[ $fail -eq 0 ]]; then echo "E1 PROOF: ALL LEGS PASS"; else echo "E1 PROOF: FAILURES ABOVE"; fi
exit $fail
