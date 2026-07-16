// Phase D (#94, go) — the MIDDLEWARE layer on the LIVE PG seam (PORT 5433).
//
// The owner decision (option A) requires the runtime's OWN tx-control (BEGIN/COMMIT/ROLLBACK, + the
// isolation SET) to be MIDDLEWARE-VISIBLE in all 5 languages — full parity with the TS reference. This
// proves it on a REAL dockerized Postgres: a registered SQL-level middleware OBSERVES the runtime BEGIN
// + COMMIT (and ROLLBACK on a body error) of a REAL Transaction() boundary, running on the ONE owned
// *sql.Conn — with a RED proof that without registration the tx-control is not observed.
//
// The tx-control is now issued THROUGH the seam (Run(txCtx, "BEGIN"/"COMMIT"/"ROLLBACK")) on the owned
// connection (db.Conn), exactly like the TS reference issues literal tx-control strings through its
// seam — so the divergence the audit flagged (go's *sql.Tx made tx-control opaque) is flipped into this
// positive proof.
//
// Gated on LITEDBMODEL_TX_ISOLATION=1 + docker up. Table is UNIQUELY namespaced for the go Phase D port
// (parallel ports share PG:5433) — scp_go_mw_txctl.

package litedbmodel_runtime

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
)

const mwTxTbl = "scp_go_mw_txctl"

func resetMwTx(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("DROP TABLE IF EXISTS " + mwTxTbl); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, val TEXT NOT NULL)", mwTxTbl)); err != nil {
		t.Fatalf("create: %v", err)
	}
}

// A registered middleware observes the runtime BEGIN + COMMIT of a REAL Transaction() on live PG, and
// — RED proof — observes NOTHING when unregistered. Full TS parity for runtime-issued tx-control.
func TestPhaseDLiveTxControlVisiblePG(t *testing.T) {
	if !txIsoEnabled() {
		t.Skip("skipped (set LITEDBMODEL_TX_ISOLATION=1 + docker up: PG:5433)")
	}
	db := openPgIso(t)
	defer db.Close()
	resetMwTx(t, db)

	// (positive) a registered middleware sees BEGIN, the INSERT, and COMMIT of a real committed tx.
	var seen []string
	var mu sync.Mutex
	_, err := WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		RegisterMiddleware(scopeCtx, observeMiddleware(&seen, &mu).Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		_, e := Transaction(ctx, db, "postgres", DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
			_, err := Run(txCtx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES ($1, $2)", mwTxTbl), []any{int64(1), "a"}, WriteIntent())
			return 0, err
		})
		return struct{}{}, e
	})
	if err != nil {
		t.Fatalf("live tx: %v", err)
	}
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if !containsStr(got, "BEGIN") || !containsStr(got, "COMMIT") {
		t.Errorf("live PG: middleware must observe runtime BEGIN + COMMIT, got %v", got)
	}
	if !containsSub(got, fmt.Sprintf("INSERT INTO %s", mwTxTbl)) {
		t.Errorf("live PG: middleware must observe the tx-body INSERT, got %v", got)
	}
	// BEGIN precedes COMMIT (the envelope order).
	if indexOf(got, "BEGIN") < 0 || indexOf(got, "COMMIT") < 0 || indexOf(got, "BEGIN") > indexOf(got, "COMMIT") {
		t.Errorf("live PG: BEGIN must precede COMMIT: %v", got)
	}

	// (RED) without registration, the runtime tx-control is NOT observed (byte-identical passthrough).
	resetMwTx(t, db)
	var unseen []string
	ctx := ContextForDB(db) // no scope, no registration → empty chain
	_, err = Transaction(ctx, db, "postgres", DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
		_, e := Run(txCtx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES ($1, $2)", mwTxTbl), []any{int64(2), "b"}, WriteIntent())
		return 0, e
	})
	if err != nil {
		t.Fatalf("live tx (red): %v", err)
	}
	if len(unseen) != 0 {
		t.Errorf("RED: tx-control observed without registration: %v", unseen)
	}
}

// A registered middleware observes BEGIN + ROLLBACK on a Transaction() body error, live PG.
func TestPhaseDLiveTxControlVisibleRollbackPG(t *testing.T) {
	if !txIsoEnabled() {
		t.Skip("skipped (set LITEDBMODEL_TX_ISOLATION=1 + docker up: PG:5433)")
	}
	db := openPgIso(t)
	defer db.Close()
	resetMwTx(t, db)
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (1, 'seed')", mwTxTbl)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var seen []string
	var mu sync.Mutex
	_, _ = WithMiddlewareScope(context.Background(), func(scopeCtx context.Context) (struct{}, error) {
		RegisterMiddleware(scopeCtx, observeMiddleware(&seen, &mu).Descriptor())
		ctx := ContextForDBCtx(scopeCtx, db)
		_, e := Transaction(ctx, db, "postgres", DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
			// A PK collision fails the boundary ⇒ ROLLBACK.
			_, err := Run(txCtx, fmt.Sprintf("INSERT INTO %s (id, val) VALUES ($1, $2)", mwTxTbl), []any{int64(1), "dup"}, WriteIntent())
			return 0, err
		})
		if e == nil {
			t.Errorf("expected the PK collision to fail the boundary")
		}
		return struct{}{}, nil
	})
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if !containsStr(got, "BEGIN") || !containsStr(got, "ROLLBACK") {
		t.Errorf("live PG: middleware must observe runtime BEGIN + ROLLBACK on error, got %v", got)
	}
	if containsStr(got, "COMMIT") {
		t.Errorf("live PG: a failed tx must NOT observe COMMIT, got %v", got)
	}
}

func containsStr(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func indexOf(list []string, s string) int {
	for i, x := range list {
		if x == s {
			return i
		}
	}
	return -1
}
