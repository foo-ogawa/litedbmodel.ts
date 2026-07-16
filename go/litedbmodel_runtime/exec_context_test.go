// Unit tests for the ExecutionContext + central Execute/Run seam (Phase A / #77, go). No live DB —
// an in-proc SQLite (:memory:) is the driver; these pin the seam funnel, empty-middleware
// passthrough, middleware wrap+delegate, per-execution *sql.Tx ownership (commit / rollback /
// decided-rollback), and the tx-scoped ConnectionFor resolution. The live PG/MySQL concurrent-tx
// isolation + atomicity proof lives in tx_isolation_test.go.

package litedbmodel_runtime

import (
	"database/sql"
	"testing"

	bc "github.com/foo-ogawa/behavior-contracts/go"

	_ "modernc.org/sqlite"
)

func seedSeed(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, s := range []string{
		`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO t VALUES (1, 'a')`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return db
}

func firstRowField(rows []bc.Value, key string) bc.Value {
	if len(rows) == 0 {
		return nil
	}
	if obj, ok := rows[0].(*bc.Obj); ok {
		v, _ := obj.Get(key)
		return v
	}
	return nil
}

// The READ seam funnels a SELECT to the resolved (non-tx) connection.
func TestSeamExecuteFunnelsToDB(t *testing.T) {
	db := seedSeed(t)
	defer db.Close()
	ctx := ContextForDB(db)
	rows, err := Execute(ctx, "SELECT id, v FROM t WHERE id = ?", []any{int64(1)}, ReadIntent())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(rows) != 1 || firstRowField(rows, "v") != "a" {
		t.Errorf("execute rows: got %v", rows)
	}
}

// The WRITE seam funnels a non-returning write to the resolved connection and reports affected rows.
func TestSeamRunFunnelsToDB(t *testing.T) {
	db := seedSeed(t)
	defer db.Close()
	ctx := ContextForDB(db)
	info, err := Run(ctx, "INSERT INTO t (id, v) VALUES (?, ?)", []any{int64(2), "b"}, WriteIntent())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if info.Changes != 1 {
		t.Errorf("run changes: got %d, want 1", info.Changes)
	}
}

// An empty chain returns the terminal verbatim — the byte-identical Phase A behavior.
func TestEmptyMiddlewareIsPassthrough(t *testing.T) {
	chain := NewMiddlewareChain()
	if !chain.IsEmpty() {
		t.Fatalf("new chain must be empty")
	}
	got, err := chain.wrapRead("SELECT 1", nil, func(_ string, _ []any) ([]bc.Value, error) {
		return []bc.Value{int64(42)}, nil
	})
	if err != nil || len(got) != 1 || got[0] != int64(42) {
		t.Errorf("empty passthrough: got %v err %v", got, err)
	}
}

// A middleware that appends a marker row around `next` — proves the fold + delegation order.
func TestMiddlewareWrapsAndDelegates(t *testing.T) {
	chain := NewMiddlewareChain()
	chain.read = append(chain.read, func(sql string, args []any, next SeamNext[[]bc.Value]) ([]bc.Value, error) {
		rows, err := next(sql, args)
		if err != nil {
			return nil, err
		}
		return append(rows, "mw"), nil
	})
	if chain.IsEmpty() {
		t.Fatalf("chain with a middleware must not be empty")
	}
	got, err := chain.wrapRead("SELECT 1", nil, func(_ string, _ []any) ([]bc.Value, error) {
		return []bc.Value{int64(1)}, nil
	})
	if err != nil || len(got) != 2 || got[0] != int64(1) || got[1] != "mw" {
		t.Errorf("middleware should append its marker row after next(): got %v", got)
	}
}

// BEGIN (via db.Begin) → body write on the OWNED *sql.Tx → COMMIT; the row persists.
func TestWithTransactionCommitsOnOk(t *testing.T) {
	db := seedSeed(t)
	defer db.Close()
	ctx := ContextForDB(db)
	out, err := WithTransaction(ctx, db, func(txCtx *ExecutionContext) (int, error) {
		if !txCtx.InTransaction() {
			t.Errorf("txCtx must report InTransaction() true")
		}
		if _, err := Run(txCtx, "INSERT INTO t (id, v) VALUES (?, ?)", []any{int64(2), "b"}, WriteIntent()); err != nil {
			return 0, err
		}
		return 7, nil
	})
	if err != nil || out != 7 {
		t.Fatalf("commit: out=%d err=%v", out, err)
	}
	rows, _ := Execute(ctx, "SELECT v FROM t WHERE id = ?", []any{int64(2)}, ReadIntent())
	if len(rows) != 1 {
		t.Errorf("committed row must persist, got %d rows", len(rows))
	}
}

// A body error rolls back the OWNED *sql.Tx (the write is undone) and the error propagates.
func TestWithTransactionRollsBackOnErr(t *testing.T) {
	db := seedSeed(t)
	defer db.Close()
	ctx := ContextForDB(db)
	_, err := WithTransaction(ctx, db, func(txCtx *ExecutionContext) (int, error) {
		if _, err := Run(txCtx, "INSERT INTO t (id, v) VALUES (?, ?)", []any{int64(2), "b"}, WriteIntent()); err != nil {
			return 0, err
		}
		// A duplicate PK ⇒ a driver error ⇒ WithTransaction ROLLBACKs + re-raises.
		if _, err := Run(txCtx, "INSERT INTO t (id, v) VALUES (?, ?)", []any{int64(1), "dup"}, WriteIntent()); err != nil {
			return 0, err
		}
		return 0, nil
	})
	if err == nil {
		t.Fatalf("expected the duplicate-PK error to propagate")
	}
	// The FIRST insert (id=2) must have been rolled back with the whole tx (cross-statement atomicity).
	rows, _ := Execute(ctx, "SELECT v FROM t WHERE id = ?", []any{int64(2)}, ReadIntent())
	if len(rows) != 0 {
		t.Errorf("rollback: id=2 must be undone, got %d rows", len(rows))
	}
}

// A gate-style non-error rollback: the body returns Rollback() → ROLLBACK is issued but the value is
// returned (no error) — the tx short-circuit semantics.
func TestWithTransactionDecidedRollbackReturnsValue(t *testing.T) {
	db := seedSeed(t)
	defer db.Close()
	ctx := ContextForDB(db)
	out, err := WithTransactionDecided(ctx, db, func(txCtx *ExecutionContext) (string, TxDecision, error) {
		if _, err := Run(txCtx, "INSERT INTO t (id, v) VALUES (?, ?)", []any{int64(2), "b"}, WriteIntent()); err != nil {
			return "", Commit(), err
		}
		return "gated", Rollback(), nil
	})
	if err != nil || out != "gated" {
		t.Fatalf("decided rollback: out=%q err=%v", out, err)
	}
	// The insert inside the rolled-back tx must be undone.
	rows, _ := Execute(ctx, "SELECT v FROM t WHERE id = ?", []any{int64(2)}, ReadIntent())
	if len(rows) != 0 {
		t.Errorf("decided rollback: id=2 must be undone, got %d rows", len(rows))
	}
}

// Outside a tx, ConnectionFor resolves the primary db; inside, the pinned tx connection.
func TestConnectionForResolution(t *testing.T) {
	db := seedSeed(t)
	defer db.Close()
	ctx := ContextForDB(db)
	if ctx.InTransaction() {
		t.Errorf("base ctx must not be in a transaction")
	}
	if _, ok := ctx.ConnectionFor(ReadIntent()).(dbConnection); !ok {
		t.Errorf("base ctx ConnectionFor must be a dbConnection")
	}
	_, _ = WithTransaction(ctx, db, func(txCtx *ExecutionContext) (int, error) {
		if !txCtx.InTransaction() {
			t.Errorf("tx ctx must report InTransaction() true")
		}
		if _, ok := txCtx.ConnectionFor(WriteIntent()).(txConnection); !ok {
			t.Errorf("tx ctx ConnectionFor must be a txConnection")
		}
		return 0, nil
	})
}
