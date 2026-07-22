// DB-backed unit tests for the native leaf transport (#141): the tx-scope wrapper (WithAmbientTransaction)
// and the single-write summary shape. These exercise ExecuteSQL against a real in-proc sqlite so the
// transport's live SQL path + tx boundary are proven independently of the bc-generated bench cell (the
// full bench is blocked on bc#174). Go twins of the rust `leaves.rs` tests.

package litedbmodel_runtime

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime/wire"

	_ "modernc.org/sqlite" // pure-go sqlite driver (registered as "sqlite")
)

// openBoundT opens a fresh in-memory sqlite (one pooled connection so schema + tx + reads share the same
// DB), creates table t, and binds the leaf transport to it. Returns the db; the caller unbinds.
func openBoundT(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("ddl: %v", err)
	}
	BindLeafTransport(db, "sqlite")
	return db
}

// insT issues one INSERT through ExecuteSQL (the covered write path). Returns the leaf result wire.
func insT(id int64, v string) (wire.WireValue, error) {
	return ExecuteSQL(false, []wire.WireValue{wire.WireInt(id), wire.WireStr(v)}, false, "INSERT INTO t (id, v) VALUES (?, ?)", true)
}

// countT reads COUNT(*) via ExecuteSQL (the covered read path) and returns the single numeric cell.
func countT(t *testing.T) string {
	t.Helper()
	out, err := ExecuteSQL(false, nil, false, "SELECT COUNT(*) AS c FROM t", false)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	lp := out.AsList()
	if lp.Kind != wireProbeGot || lp.Got.Len() != 1 {
		t.Fatalf("count: want 1 row, got kind=%d len=%d", lp.Kind, lp.Got.Len())
	}
	row := lp.Got.ElemRow(0)
	np := row.Got.ProbeNumber("c")
	if np.Kind != wireProbeGot {
		t.Fatalf("count cell not numeric (kind=%d)", np.Kind)
	}
	return np.Got
}

// A non-RETURNING write returns the uniform one-row [{changes,lastInsertRowid}] summary (rust
// execute_sql parity) — NOT an empty list. (#141 slice-2 piece 2.)
func TestExecuteSQL_WriteSummaryShape(t *testing.T) {
	db := openBoundT(t)
	defer db.Close()
	defer UnbindLeafTransport()

	out, err := insT(7, "a")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	lp := out.AsList()
	if lp.Kind != wireProbeGot || lp.Got.Len() != 1 {
		t.Fatalf("write summary: want a 1-row list, got kind=%d len=%d", lp.Kind, lp.Got.Len())
	}
	row := lp.Got.ElemRow(0)
	if row.Kind != wireProbeGot {
		t.Fatalf("write summary elem is not a row (kind=%d)", row.Kind)
	}
	if ch := row.Got.ProbeNumber("changes"); ch.Kind != wireProbeGot || ch.Got != "1" {
		t.Fatalf("changes = %+v, want 1", ch)
	}
	if li := row.Got.ProbeNumber("lastInsertRowid"); li.Kind != wireProbeGot || li.Got != "7" {
		t.Fatalf("lastInsertRowid = %+v, want 7 (the inserted PK)", li)
	}
}

// WithAmbientTransaction atomicity (#142 / slice-2 piece 1): ok body COMMITs (all writes persist);
// a mid-tx body error ROLLBACKs (no rows persist). Mirror of rust's
// tx_commits_on_ok_and_rolls_back_on_err. Proves the covered tx runner's boundary is genuinely atomic.
func TestWithAmbientTransaction_CommitsOnOkRollsBackOnErr(t *testing.T) {
	db := openBoundT(t)
	defer db.Close()
	defer UnbindLeafTransport()

	// Ok body: two inserts on the tx-owned connection → COMMIT → both rows persist.
	if err := WithAmbientTransaction(db, func() error {
		if _, e := insT(1, "a"); e != nil {
			return e
		}
		_, e := insT(2, "b")
		return e
	}); err != nil {
		t.Fatalf("committed tx returned error: %v", err)
	}
	if got := countT(t); got != "2" {
		t.Fatalf("after commit: row count = %s, want 2 (all writes persisted)", got)
	}

	// Err body: insert row 3 then fail mid-tx → ROLLBACK → row 3 must NOT persist (still 2 rows).
	boom := errors.New("mid-tx failure")
	err := WithAmbientTransaction(db, func() error {
		if _, e := insT(3, "c"); e != nil { // issued inside the tx…
			return e
		}
		return boom // …then the body errors → rollback
	})
	if !errors.Is(err, boom) {
		t.Fatalf("rolled-back tx must propagate the body error, got %v", err)
	}
	if got := countT(t); got != "2" {
		t.Fatalf("after rollback: row count = %s, want 2 (row 3 must be gone)", got)
	}
}

// The tx-scoped ambient is restored after the transaction: a plain ExecuteSQL write AFTER the tx runs
// on the bound (non-tx) connection and persists (proves the ambient swap does not leak past the body).
func TestWithAmbientTransaction_RestoresAmbient(t *testing.T) {
	db := openBoundT(t)
	defer db.Close()
	defer UnbindLeafTransport()

	if err := WithAmbientTransaction(db, func() error { _, e := insT(1, "a"); return e }); err != nil {
		t.Fatalf("tx: %v", err)
	}
	if _, err := insT(2, "b"); err != nil { // outside any tx — on the restored ambient
		t.Fatalf("post-tx write: %v", err)
	}
	if got := countT(t); got != "2" {
		t.Fatalf("row count = %s, want 2 (tx write + post-tx write)", got)
	}
}
