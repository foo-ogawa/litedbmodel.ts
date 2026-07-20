#!/usr/bin/env bash
# LIVE-DB PROOF (epic #123/#124 commit 2/3) — run the native-codegen cell against a REAL docker
# Postgres (:5433) / MySQL (:3307) through litedbmodel_runtime's PostgresDriver / MysqlDriver and
# execute the generated relation modules against the real database and assert batched query counts.
#
# The generated modules are per-dialect (baked $N / json_each-vs-ANY / RETURNING). This harness swaps
# the freshly-emitted <dialect> modules + adapters into src/ (over the committed sqlite ones), builds
# the `livedb` binary, seeds the live DB to the fixed state, runs every op, then RESTORES the committed
# sqlite files — so the committed tree stays sqlite (the per-dialect modules are regenerated artifacts).
#
#   bash run-proof-livedb.sh <postgres|mysql>
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$HERE/../.."
PROOF_DIR=/tmp/e1proof
DIALECT="${1:?usage: run-proof-livedb.sh <postgres|mysql>}"
case "$DIALECT" in
  postgres) SPEC='pg:host=localhost port=5433 user=testuser password=testpass dbname=testdb' ;;
  mysql)    SPEC='mysql:mysql://testuser:testpass@localhost:3307/testdb' ;;
  *) echo "unknown dialect '$DIALECT'"; exit 2 ;;
esac
fail=0
MODULES=(findunique byids recent bymaybe feed tenantfeed relbatch relsingle createuser renameuser deleteuser upsert createmany upsertmany updatemany txdelete txnestedcreate txnestedupdate txnestedupsert txrollback)

echo "── regenerate ${DIALECT} single-file modules (TS leg) ──"
( cd "$HERE/../.." && npx vitest run test/scp/e1-native-sql-port.test.ts -t 'LIVE — emit' >/dev/null 2>&1 ) || { echo "FATAL: regen failed"; exit 1; }

# Swap the <dialect> modules into src/ (backing up the committed sqlite ones), restore on exit.
BACKUP="$(mktemp -d)"
ORM_BACKUP="$(mktemp -d)"
cp "$HERE"/src/generated_*.rs "$BACKUP/"
cp "$ROOT"/rust/orm_bench/src/gen/generated_*.rs "$ORM_BACKUP/"
restore() {
  cp "$BACKUP"/*.rs "$HERE/src/"
  cp "$ORM_BACKUP"/*.rs "$ROOT/rust/orm_bench/src/gen/"
  rm -rf "$BACKUP" "$ORM_BACKUP"
}
trap restore EXIT
for m in "${MODULES[@]}"; do
  cp "$PROOF_DIR/$DIALECT/generated_$m.rs" "$HERE/src/generated_$m.rs"
done

( cd "$ROOT" && npx tsx benchmark/crosslang/codegen-build.ts sqlite && npx tsx benchmark/crosslang/oracle-fixture-build.ts )
cp /tmp/ormbench/"$DIALECT"/generated_*.rs "$ROOT/rust/orm_bench/src/gen/"

echo "── build the livedb binary (--features livedb) ──"
cargo build --quiet --features livedb --manifest-path "$HERE/Cargo.toml" || { echo "  FAIL  livedb build"; exit 1; }
BIN="$HERE/target/debug/e1_native_proof"
node --input-type=module -e "import { seedE1 } from '$HERE/livedb-seed.mjs'; await seedE1('$DIALECT', 'read')" || exit 1
for case in 'relbatch 1' 'relsingle 7'; do
  set -- $case
  qc="$("$BIN" "$1" "$SPEC" "$2" 2>&1 >/dev/null | sed -n 's/^queries=//p')"
  if [[ "$qc" == "2" ]]; then echo "  PASS  $1 uses 1 parent + 1 batch query"; else echo "  FAIL  $1 queries=$qc"; fail=1; fi
done

echo "── direct native/interpreter result + DB-state oracle ──"
cargo run --quiet --manifest-path "$ROOT/rust/Cargo.toml" -p litedbmodel_oracle --features livedb -- "$DIALECT" "$SPEC"

echo
if [[ $fail -eq 0 ]]; then echo "LIVE-DB PROOF ($DIALECT): ALL LEGS PASS"; else echo "LIVE-DB PROOF ($DIALECT): FAILURES ABOVE"; fi
exit $fail
