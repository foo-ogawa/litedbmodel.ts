#!/usr/bin/env bash
# LIVE-DB PROOF (epic #123/#124 commit 2/3) — run the native-codegen cell against a REAL docker
# Postgres (:5433) / MySQL (:3307) through litedbmodel_runtime's PostgresDriver / MysqlDriver and
# compare BYTE-FOR-BYTE to the SAME dialect-independent mode-2 oracle the sqlite proof uses.
#
# The generated modules are per-dialect (baked $N / json_each-vs-ANY / RETURNING). This harness swaps
# the freshly-emitted <dialect> modules + companions into src/ (over the committed sqlite ones), builds
# the `livedb` binary, seeds the live DB to the fixed state, runs every op, then RESTORES the committed
# sqlite files — so the committed tree stays sqlite (the per-dialect modules are regenerated artifacts).
#
#   bash run-proof-livedb.sh <postgres|mysql>
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROOF_DIR=/tmp/e1proof
DIALECT="${1:?usage: run-proof-livedb.sh <postgres|mysql>}"
case "$DIALECT" in
  postgres) SPEC='pg:host=localhost port=5433 user=testuser password=testpass dbname=testdb' ;;
  mysql)    SPEC='mysql:mysql://testuser:testpass@localhost:3307/testdb' ;;
  *) echo "unknown dialect '$DIALECT'"; exit 2 ;;
esac
fail=0
MODULES=(findunique byids recent bymaybe feed tenantfeed relbatch relsingle createuser renameuser deleteuser upsert createmany upsertmany updatemany txdelete txnestedcreate txnestedupdate txnestedupsert txrollback)

echo "── regenerate ${DIALECT} modules + companions + oracles (TS leg) ──"
( cd "$HERE/../.." && source ~/.nvm/nvm.sh >/dev/null 2>&1 && nvm use 22 >/dev/null 2>&1 && npx vitest run test/scp/e1-native-sql-port.test.ts >/dev/null 2>&1 ) || { echo "FATAL: regen failed"; exit 1; }

# Swap the <dialect> modules into src/ (backing up the committed sqlite ones), restore on exit.
BACKUP="$(mktemp -d)"
cp "$HERE"/src/generated_*.rs "$HERE"/src/companion_*.rs "$BACKUP/"
restore() { cp "$BACKUP"/*.rs "$HERE/src/"; rm -rf "$BACKUP"; }
trap restore EXIT
for m in "${MODULES[@]}"; do
  cp "$PROOF_DIR/$DIALECT/generated_$m.rs" "$HERE/src/generated_$m.rs"
  cp "$PROOF_DIR/$DIALECT/companion_$m.rs" "$HERE/src/companion_$m.rs"
done

echo "── build the livedb binary (--features livedb) ──"
cargo build --quiet --features livedb --manifest-path "$HERE/Cargo.toml" || { echo "  FAIL  livedb build"; exit 1; }
BIN="$HERE/target/debug/e1_native_proof"
# native(codegen) vs mode-2(interpreter) on the SAME live connection (compare-livedb.mjs does the whole
# matrix — reads, N+1 guard, writes, tx — re-seeding per mutating case). No sqlite oracle dependency.
node "$HERE/compare-livedb.mjs" "$DIALECT" "$SPEC" "$BIN" || fail=1

echo
if [[ $fail -eq 0 ]]; then echo "LIVE-DB PROOF ($DIALECT): ALL LEGS PASS"; else echo "LIVE-DB PROOF ($DIALECT): FAILURES ABOVE"; fi
exit $fail
