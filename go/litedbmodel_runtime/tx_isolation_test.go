// Phase A-3 (#77, go) — CONCURRENT-TRANSACTION ISOLATION + ATOMICITY on live PG + MySQL.
//
// The go mirror of #75's test/integration/TxIsolation.test.ts and the rust tests/tx_isolation.rs. It
// proves the per-execution connection ownership (ExecutionContext + WithTransactionDecided over an
// OWNED *sql.Tx, §3): N transactions run CONCURRENTLY in ONE process on the shared *sql.DB, each
// acquiring its OWN pooled connection via db.Begin(). The assertions run the UNMODIFIED PRODUCTION
// path (ExecuteTransactionBundle → executeTransactionCtx → WithTransactionDecided → the tx-owned
// *sql.Tx) — nothing is swapped:
//
//	(1) ISOLATION — N concurrent workers each run TWO back-to-back single-INSERT transactions with a
//	    yield between; the final table holds EXACTLY the 2·N rows, correctly paired per worker (no
//	    cross-talk).
//	(2) ATOMICITY (single-statement) — a tx whose sole INSERT collides on the PK ROLLBACKs (whole tx
//	    error); the concurrent committing transactions are unaffected (their rows present; the aborted
//	    one's absent).
//	(3) ATOMICITY (MULTI-statement, production path) — a 2-statement tx whose 2nd statement collides
//	    MUST roll back the 1st statement's already-executed INSERT (real cross-statement atomicity
//	    through WithTransactionDecided on ONE owned *sql.Tx), while a concurrently-committed tx is
//	    unaffected. This pins production ownership DIRECTLY — the faithful-mutation RED proof
//	    (documented at the bottom) reverts ownership to a fresh autocommit connection and this goes RED.
//
// Gated behind LITEDBMODEL_TX_ISOLATION=1 so it never runs in the default `go test` (which has no
// DBs). Requires the dockerized PG (:5433) + MySQL (:3307):
//
//	docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d postgres mysql
//	LITEDBMODEL_TX_ISOLATION=1 go test ./litedbmodel_runtime/ -run TxIsolation -v

package litedbmodel_runtime

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

const isoTbl = "scp_tx_iso"

func txIsoEnabled() bool { return os.Getenv("LITEDBMODEL_TX_ISOLATION") == "1" }

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// ── Bundle authoring (the shape ExecuteTransactionBundle consumes) ─────────────

// insertBundle is a single-INSERT (no RETURNING, entityFrom null) transaction bundle whose values
// come from the input scope. Committed via the production tx path on ONE owned *sql.Tx.
func insertBundle(t *testing.T, dialect string) *SqlBundle {
	t.Helper()
	src := fmt.Sprintf(`{"dialect":"%s","transaction":{"phase":"create","entityFrom":null,"statements":[
	  {"id":"tx_body_0","role":"body","op":{"sql":"INSERT INTO %s (id, worker, seq) VALUES (?, ?, ?)","params":[{"ref":["id"]},{"ref":["worker"]},{"ref":["seq"]}]}}
	]}}`, dialect, isoTbl)
	return parseBundleT(t, src)
}

// twoStmtBundle is a TWO-statement transaction bundle: stmt-1 inserts id1 (valid), stmt-2 inserts id2
// (pre-seeded to collide). One logical transaction — stmt-2's PK violation must roll back stmt-1
// (cross-statement atomicity on the owned *sql.Tx).
func twoStmtBundle(t *testing.T, dialect string, id1, id2, worker int64) *SqlBundle {
	t.Helper()
	src := fmt.Sprintf(`{"dialect":"%s","transaction":{"phase":"create","entityFrom":null,"statements":[
	  {"id":"tx_body_0","role":"body","op":{"sql":"INSERT INTO %s (id, worker, seq) VALUES (%d, %d, 0)","params":[]}},
	  {"id":"tx_body_1","role":"body","op":{"sql":"INSERT INTO %s (id, worker, seq) VALUES (%d, %d, 0)","params":[]}}
	]}}`, dialect, isoTbl, id1, worker, isoTbl, id2, worker)
	return parseBundleT(t, src)
}

func parseBundleT(t *testing.T, src string) *SqlBundle {
	t.Helper()
	b, err := ParseBundle([]byte(src))
	if err != nil {
		t.Fatalf("parse bundle: %v", err)
	}
	return b
}

func txInput(id, worker, seq int64) *bc.Obj {
	o := bc.NewObj()
	o.Set("id", id)
	o.Set("worker", worker)
	o.Set("seq", seq)
	return o
}

// ── Reading the table back through the seam ────────────────────────────────────

// readIsoRows reads (id, worker) rows sorted by id (worker != 999, filtering any pre-seed).
func readIsoRows(t *testing.T, db SQLDB) [][2]int64 {
	t.Helper()
	rows, err := queryRows(db, fmt.Sprintf("SELECT id, worker FROM %s WHERE worker <> 999", isoTbl), nil)
	if err != nil {
		t.Fatalf("read rows: %v", err)
	}
	out := make([][2]int64, 0, len(rows))
	for _, r := range rows {
		obj, ok := r.(*bc.Obj)
		if !ok {
			continue
		}
		out = append(out, [2]int64{cellInt(obj, "id"), cellInt(obj, "worker")})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	return out
}

// cellInt coerces a scanned column (float64 per scanValue, or int64 / string from PG/MySQL) to int64.
func cellInt(o *bc.Obj, k string) int64 {
	v, _ := o.Get(k)
	switch t := v.(type) {
	case int64:
		return t
	case float64:
		return int64(t)
	case string:
		var n int64
		fmt.Sscan(t, &n)
		return n
	default:
		return -1
	}
}

// ── PG / MySQL connect + reset helpers ─────────────────────────────────────────

func openPgIso(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		envOr("TEST_DB_USER", "testuser"), envOr("TEST_DB_PASSWORD", "testpass"),
		envOr("TEST_DB_HOST", "localhost"), envOr("TEST_DB_PORT", "5433"), envOr("TEST_DB_NAME", "testdb"))
	db, err := OpenPostgres(dsn)
	if err != nil {
		t.Fatalf("pg connect: %v", err)
	}
	return db
}

func openMysqlIso(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		envOr("TEST_MYSQL_USER", "testuser"), envOr("TEST_MYSQL_PASSWORD", "testpass"),
		envOr("TEST_MYSQL_HOST", "127.0.0.1"), envOr("TEST_MYSQL_PORT", "3307"), envOr("TEST_MYSQL_DB", "testdb"))
	db, err := OpenMysql(dsn)
	if err != nil {
		t.Fatalf("mysql connect: %v", err)
	}
	return db
}

func resetIso(t *testing.T, db *sql.DB, intType string) {
	t.Helper()
	if _, err := db.Exec("DROP TABLE IF EXISTS " + isoTbl); err != nil {
		t.Fatalf("drop: %v", err)
	}
	ddl := fmt.Sprintf("CREATE TABLE %s (id %s PRIMARY KEY, worker %s NOT NULL, seq %s NOT NULL)", isoTbl, intType, intType, intType)
	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("create: %v", err)
	}
}

const isoN = int64(8)

func committed(r TransactionResult) bool { return r.Committed }

func runInsertTx(t *testing.T, db *sql.DB, dialect string, id, worker, seq int64) (TransactionResult, error) {
	return ExecuteTransactionBundle(insertBundle(t, dialect), txInput(id, worker, seq), db)
}

// (1) ISOLATION — N workers each run TWO single-INSERT txs concurrently (worker k writes id=2k then
// id=2k+1); the final table holds EXACTLY the 2·N rows. Runs the production ExecuteTransactionBundle
// per tx — every BEGIN…COMMIT owns its own *sql.Tx, so concurrent workers never cross-talk.
func isolation(t *testing.T, db *sql.DB, dialect string) {
	var wg sync.WaitGroup
	for k := int64(0); k < isoN; k++ {
		wg.Add(1)
		go func(k int64) {
			defer wg.Done()
			r0, err := runInsertTx(t, db, dialect, 2*k, k, 0)
			if err != nil || !committed(r0) {
				t.Errorf("worker %d tx0 must commit: committed=%v err=%v", k, committed(r0), err)
			}
			r1, err := runInsertTx(t, db, dialect, 2*k+1, k, 1)
			if err != nil || !committed(r1) {
				t.Errorf("worker %d tx1 must commit: committed=%v err=%v", k, committed(r1), err)
			}
		}(k)
	}
	wg.Wait()

	got := readIsoRows(t, db)
	want := make([][2]int64, 0, 2*isoN)
	for k := int64(0); k < isoN; k++ {
		want = append(want, [2]int64{2 * k, k}, [2]int64{2*k + 1, k})
	}
	sort.Slice(want, func(i, j int) bool { return want[i][0] < want[j][0] })
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("%s isolation: every worker's rows present, no cross-talk\n got=%v\nwant=%v", dialect, got, want)
	}
}

// (2) SINGLE-STATEMENT ATOMICITY — worker 0 collides on a pre-seeded id=0 (PK violation → whole tx
// ROLLBACK + error); workers 1..N commit. The aborted row is ABSENT; every committed worker present.
func singleStmtAtomicity(t *testing.T, db *sql.DB, dialect string) {
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, worker, seq) VALUES (0, 999, 9)", isoTbl)); err != nil {
		t.Fatalf("preseed: %v", err)
	}
	var mu sync.Mutex
	outcomes := map[int64]bool{}
	var wg sync.WaitGroup
	for k := int64(0); k < isoN; k++ {
		wg.Add(1)
		go func(k int64) {
			defer wg.Done()
			_, err := runInsertTx(t, db, dialect, k, k, 0)
			mu.Lock()
			outcomes[k] = err == nil
			mu.Unlock()
		}(k)
	}
	wg.Wait()

	if outcomes[0] {
		t.Errorf("%s: worker 0 (PK collision) tx must FAIL", dialect)
	}
	for k := int64(1); k < isoN; k++ {
		if !outcomes[k] {
			t.Errorf("%s: worker %d must commit", dialect, k)
		}
	}
	got := readIsoRows(t, db) // worker 999 pre-seed filtered out
	want := make([][2]int64, 0, isoN-1)
	for i := int64(1); i < isoN; i++ {
		want = append(want, [2]int64{i, i})
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("%s: aborted worker 0 row ABSENT; committed workers present\n got=%v\nwant=%v", dialect, got, want)
	}
}

// (3) MULTI-STATEMENT ATOMICITY (production path) — a 2-statement tx (id=10 valid, id=20 pre-seeded
// collision) run CONCURRENTLY with a committing single-INSERT (id=30). stmt-2's collision must roll
// back stmt-1 (id=10 ABSENT), and the concurrent commit (id=30) is unaffected. Runs the UNMODIFIED
// ExecuteTransactionBundle → WithTransactionDecided on ONE owned *sql.Tx.
func multiStmtAtomicity(t *testing.T, db *sql.DB, dialect string) {
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, worker, seq) VALUES (20, 999, 9)", isoTbl)); err != nil {
		t.Fatalf("preseed 20: %v", err)
	}
	var failOk, commitOk bool
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := ExecuteTransactionBundle(twoStmtBundle(t, dialect, 10, 20, 1), txInput(0, 1, 0), db)
		failOk = err == nil
	}()
	go func() {
		defer wg.Done()
		r, err := runInsertTx(t, db, dialect, 30, 2, 0)
		commitOk = err == nil && committed(r)
	}()
	wg.Wait()

	if failOk {
		t.Errorf("%s: the 2-statement tx must FAIL on stmt-2's collision", dialect)
	}
	if !commitOk {
		t.Errorf("%s: the concurrent single-INSERT tx must commit unaffected", dialect)
	}
	got := readIsoRows(t, db) // id=20 pre-seed (worker 999) filtered out
	want := [][2]int64{{30, 2}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("%s: id=10 ROLLED BACK (cross-statement atomicity); id=30 present, unaffected\n got=%v\nwant=%v", dialect, got, want)
	}
}

func TestTxIsolationPostgres(t *testing.T) {
	if !txIsoEnabled() {
		t.Skip("skipped (set LITEDBMODEL_TX_ISOLATION=1 + docker up)")
	}
	db := openPgIso(t)
	defer db.Close()
	resetIso(t, db, "INTEGER")
	isolation(t, db, "postgres")
	resetIso(t, db, "INTEGER")
	singleStmtAtomicity(t, db, "postgres")
	resetIso(t, db, "INTEGER")
	multiStmtAtomicity(t, db, "postgres")
	t.Log("PG TX-ISOLATION PROOF: isolation + single-stmt + multi-stmt atomicity all green (per-execution ownership)")
}

func TestTxIsolationMysql(t *testing.T) {
	if !txIsoEnabled() {
		t.Skip("skipped (set LITEDBMODEL_TX_ISOLATION=1 + docker up)")
	}
	db := openMysqlIso(t)
	defer db.Close()
	resetIso(t, db, "INT")
	isolation(t, db, "mysql")
	resetIso(t, db, "INT")
	singleStmtAtomicity(t, db, "mysql")
	resetIso(t, db, "INT")
	multiStmtAtomicity(t, db, "mysql")
	t.Log("MYSQL TX-ISOLATION PROOF: isolation + single-stmt + multi-stmt atomicity all green (per-execution ownership)")
}

// ── FAITHFUL-MUTATION RED PROOF (performed during #77; reverted) ───────────────
//
// The gate's teeth were proven by a FAITHFUL mutation that reverts per-execution connection ownership
// and confirming this test goes RED. The mutation (applied to exec_context.go txConnection.Run):
// route the tx's write to a FRESH autocommit connection (the base *sql.DB via ctx.db) instead of the
// tx-OWNED *sql.Tx — i.e. the tx statements no longer run on the transaction's own connection. See
// the committed report / session log for the captured RED output; with ownership restored the write
// runs on the tx's OWN *sql.Tx and the ROLLBACK undoes it → GREEN. The mutation was applied, the RED
// captured, then FULLY reverted (the committed code owns the tx connection).
