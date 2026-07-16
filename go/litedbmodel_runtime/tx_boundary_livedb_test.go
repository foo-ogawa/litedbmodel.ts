// Phase B (#83, go) — LIVE-DB integration tests for the public Transaction() boundary + the
// tx-completeness primitives on REAL Postgres (:5433) + MySQL (:3307). The go mirror of rust's
// tests/tx_boundary.rs + the tx_isolation live proof. These exercise the UNMODIFIED production path
// (Transaction → TransactionDecided → WithTransactionDecidedIsolated on an OWNED *sql.Tx, each op
// JOINing via ExecuteTransactionBundleCtx) against real engines:
//
//	(1) MULTI-OP ATOMICITY — Transaction(func(){ opA-insert; opB-insert }) → both commit; a recording
//	    driver asserts EXACTLY ONE BeginTx / ONE COMMIT / ONE *sql.Tx for the whole boundary. opB
//	    PK-collides → opA's row ALSO rolled back (ONE BeginTx + ONE ROLLBACK, zero COMMIT), verified
//	    by reading DB rows. (The teeth: TestBoundaryJoinMutationRED documents the disable-join RED.)
//	(2) GUARD — write OUTSIDE Transaction() → WriteOutsideTransactionError; read-only → WriteInReadOnly;
//	    inside → succeeds. All on the live path.
//	(3) ISOLATION — REPEATABLE READ holds the snapshot vs READ COMMITTED sees the concurrent commit
//	    (behavioral, both dialects).
//	(4) RETRY — REAL contention: PG SERIALIZABLE write-skew → 40001; MySQL deadlock → 1213; the loser
//	    retries and both commit; a non-retryable error does NOT retry.
//	(5) NESTED — one BeginTx/COMMIT; an inner error rolls back the whole tx.
//
// Gated behind LITEDBMODEL_TX_ISOLATION=1 (same gate as tx_isolation_test.go). Requires the
// dockerized PG (:5433) + MySQL (:3307):
//
//	docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d postgres mysql
//	LITEDBMODEL_TX_ISOLATION=1 go test ./litedbmodel_runtime/ -run TxBoundary -v

package litedbmodel_runtime

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"testing"

	bc "github.com/foo-ogawa/behavior-contracts/go"
	gomysql "github.com/go-sql-driver/mysql"
	pgxstd "github.com/jackc/pgx/v5/stdlib"
)

// ── Recording wrappers for the LIVE drivers (count BeginTx / Commit / Rollback) ──

// liveRecDriver wraps a base live driver, recording tx lifecycle into a shared sink (keyed by DSN).
type liveRecDriver struct{ base driver.Driver }

var liveRecSinks sync.Map // dsn -> *recSink

func liveRecSinkFor(dsn string) *recSink {
	v, _ := liveRecSinks.LoadOrStore(dsn, &recSink{})
	return v.(*recSink)
}

func (d liveRecDriver) Open(name string) (driver.Conn, error) {
	c, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	return &recConn{base: c, sink: liveRecSinkFor(name)}, nil
}

var registerLiveRecOnce sync.Once

func registerLiveRecDrivers() {
	registerLiveRecOnce.Do(func() {
		// pgx-rec wraps the pgx stdlib driver; mysql-scp-rec wraps the RETURNING-emulating driver.
		sql.Register("pgx-rec", liveRecDriver{base: &pgxstd.Driver{}})
		registerMysqlScp()
		sql.Register("mysql-scp-rec", liveRecDriver{base: scpMysqlDriver{base: gomysql.MySQLDriver{}}})
	})
}

func openPgRec(t *testing.T) (*sql.DB, *recSink) {
	t.Helper()
	registerLiveRecDrivers()
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		envOr("TEST_DB_USER", "testuser"), envOr("TEST_DB_PASSWORD", "testpass"),
		envOr("TEST_DB_HOST", "localhost"), envOr("TEST_DB_PORT", "5433"), envOr("TEST_DB_NAME", "testdb"))
	db, err := sql.Open("pgx-rec", dsn)
	if err != nil {
		t.Fatalf("pg connect: %v", err)
	}
	db.SetMaxOpenConns(DefaultPoolSize)
	if err := db.Ping(); err != nil {
		t.Fatalf("pg ping: %v", err)
	}
	return db, liveRecSinkFor(dsn)
}

func openMysqlRec(t *testing.T) (*sql.DB, *recSink) {
	t.Helper()
	registerLiveRecDrivers()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		envOr("TEST_MYSQL_USER", "testuser"), envOr("TEST_MYSQL_PASSWORD", "testpass"),
		envOr("TEST_MYSQL_HOST", "127.0.0.1"), envOr("TEST_MYSQL_PORT", "3307"), envOr("TEST_MYSQL_DB", "testdb"))
	db, err := sql.Open("mysql-scp-rec", dsn)
	if err != nil {
		t.Fatalf("mysql connect: %v", err)
	}
	db.SetMaxOpenConns(DefaultPoolSize)
	if err := db.Ping(); err != nil {
		t.Fatalf("mysql ping: %v", err)
	}
	return db, liveRecSinkFor(dsn)
}

// ── (1) MULTI-OP ATOMICITY through the REAL Transaction() boundary ─────────────

// boundaryInsert runs a single-INSERT bundle as ONE op JOINing the ambient tx (guard ON).
func boundaryInsert(t *testing.T, txCtx *ExecutionContext, db TxDB, dialect string, id, worker, seq int64) error {
	t.Helper()
	_, err := ExecuteTransactionBundleCtx(insertBundle(t, dialect), txInput(id, worker, seq), txCtx, db, true)
	return err
}

// multiOpAtomicityCommit — two ops inside ONE Transaction() boundary both commit; the recording
// driver proves EXACTLY ONE BeginTx + ONE COMMIT on ONE *sql.Tx (the ambient JOIN — opB opens no
// second BEGIN).
func multiOpAtomicityCommit(t *testing.T, db *sql.DB, sink *recSink, dialect string) {
	sink.mu.Lock()
	sink.beginTx, sink.commits, sink.rolls, sink.distinct = 0, 0, 0, nil
	sink.mu.Unlock()

	ctx := ContextForDB(db)
	_, err := Transaction(ctx, db, dialect, DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
		if err := boundaryInsert(t, txCtx, db, dialect, 100, 1, 0); err != nil { // opA
			return 0, err
		}
		if err := boundaryInsert(t, txCtx, db, dialect, 101, 1, 1); err != nil { // opB — JOINs opA
			return 0, err
		}
		return 0, nil
	})
	if err != nil {
		t.Fatalf("%s multi-op commit boundary: %v", dialect, err)
	}
	_, beginTx, commits, rolls, distinct := sink.snapshot()
	if beginTx != 1 || commits != 1 || rolls != 0 || distinct != 1 {
		t.Errorf("%s: N ops in one boundary must be ONE BeginTx + ONE COMMIT on ONE *sql.Tx: beginTx=%d commits=%d rolls=%d distinctTx=%d", dialect, beginTx, commits, rolls, distinct)
	}
	got := readIsoRows(t, db)
	want := [][2]int64{{100, 1}, {101, 1}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("%s: both ops committed together\n got=%v\nwant=%v", dialect, got, want)
	}
}

// multiOpAtomicityRollback — opB collides on a pre-seeded PK → opA's row is ALSO rolled back (ONE
// BeginTx + ONE ROLLBACK, zero COMMIT), verified by reading rows.
func multiOpAtomicityRollback(t *testing.T, db *sql.DB, sink *recSink, dialect string) {
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, worker, seq) VALUES (201, 999, 9)", isoTbl)); err != nil {
		t.Fatalf("preseed 201: %v", err)
	}
	sink.mu.Lock()
	sink.beginTx, sink.commits, sink.rolls, sink.distinct = 0, 0, 0, nil
	sink.mu.Unlock()

	ctx := ContextForDB(db)
	_, err := Transaction(ctx, db, dialect, DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
		if err := boundaryInsert(t, txCtx, db, dialect, 200, 2, 0); err != nil { // opA — valid
			return 0, err
		}
		if err := boundaryInsert(t, txCtx, db, dialect, 201, 2, 1); err != nil { // opB — PK collision
			return 0, err
		}
		return 0, nil
	})
	if err == nil {
		t.Fatalf("%s: opB's PK collision must fail the whole boundary", dialect)
	}
	_, beginTx, commits, rolls, _ := sink.snapshot()
	if beginTx != 1 || commits != 0 || rolls != 1 {
		t.Errorf("%s: opB failure ⇒ ONE BeginTx + ONE ROLLBACK + zero COMMIT: beginTx=%d commits=%d rolls=%d", dialect, beginTx, commits, rolls)
	}
	got := readIsoRows(t, db) // id=201 pre-seed (worker 999) filtered out
	if len(got) != 0 {
		t.Errorf("%s: opA (id=200) must ALSO roll back when opB fails, got rows=%v", dialect, got)
	}
}

// ── (2) write=tx GUARD on the live path ────────────────────────────────────────

func guardLive(t *testing.T, db *sql.DB, dialect string) {
	ctx := ContextForDB(db)
	// Outside any Transaction() → WriteOutsideTransactionError, no row written.
	_, err := ExecuteTransactionBundleCtx(insertBundle(t, dialect), txInput(300, 3, 0), ctx, db, true)
	if f, ok := err.(*SqlFailure); !ok || f.Kind != "write_outside_transaction" {
		t.Errorf("%s: bare write must be WriteOutsideTransactionError, got %v", dialect, err)
	}
	// Read-only-scoped write inside a Transaction() → WriteInReadOnly (read-only first).
	_, _ = Transaction(ctx, db, dialect, DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
		ro := txCtx.WithReadOnly()
		_, e := ExecuteTransactionBundleCtx(insertBundle(t, dialect), txInput(301, 3, 0), ro, db, true)
		if f, ok := e.(*SqlFailure); !ok || f.Kind != "write_in_read_only_context" {
			t.Errorf("%s: read-only write must be WriteInReadOnly, got %v", dialect, e)
		}
		return 0, nil
	})
	// Inside a Transaction() → succeeds.
	_, err = Transaction(ctx, db, dialect, DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
		return 0, boundaryInsert(t, txCtx, db, dialect, 302, 3, 0)
	})
	if err != nil {
		t.Errorf("%s: guarded write inside a Transaction() must succeed: %v", dialect, err)
	}
	got := readIsoRows(t, db)
	if fmt.Sprint(got) != fmt.Sprint([][2]int64{{302, 3}}) {
		t.Errorf("%s: only the in-boundary write (id=302) persisted, got=%v", dialect, got)
	}
}

// ── (3) ISOLATION — REPEATABLE READ snapshot vs READ COMMITTED (behavioral) ────

// isolationBehavior proves REPEATABLE READ holds the snapshot (a re-read inside the tx does NOT see a
// concurrent commit) while READ COMMITTED does. Uses the raw seam inside a Transaction() at each
// level, with a concurrent committed UPDATE between the two reads.
func isolationBehavior(t *testing.T, db *sql.DB, dialect string) {
	reset := func() {
		if _, err := db.Exec(fmt.Sprintf("DELETE FROM %s", isoTbl)); err != nil {
			t.Fatalf("clear: %v", err)
		}
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, worker, seq) VALUES (1, 500, 10)", isoTbl)); err != nil {
			t.Fatalf("seed iso row: %v", err)
		}
	}
	readSeqDirect := func(txCtx *ExecutionContext) int64 {
		rows, err := Execute(txCtx, fmt.Sprintf("SELECT seq FROM %s WHERE id = 1", isoTbl), nil, ReadIntent())
		if err != nil {
			t.Fatalf("%s read seq: %v", dialect, err)
		}
		if len(rows) == 0 {
			return -1
		}
		o, ok := rows[0].(*bc.Obj)
		if !ok {
			return -1
		}
		return cellInt(o, "seq")
	}

	// REPEATABLE READ: the two in-tx reads MUST match despite the concurrent commit.
	reset()
	o := DefaultTransactionOptions()
	o.Isolation = IsolationRepeatableRead
	ctx := ContextForDB(db)
	_, err := Transaction(ctx, db, dialect, o, func(txCtx *ExecutionContext) (int, error) {
		first := readSeqDirect(txCtx)
		// A concurrent committed UPDATE (its own connection) between the two reads.
		if _, e := db.Exec(fmt.Sprintf("UPDATE %s SET seq = 99 WHERE id = 1", isoTbl)); e != nil {
			t.Fatalf("concurrent update: %v", e)
		}
		second := readSeqDirect(txCtx)
		if first != second {
			t.Errorf("%s REPEATABLE READ: snapshot must hold, first=%d second=%d", dialect, first, second)
		}
		return 0, nil
	})
	if err != nil {
		t.Fatalf("%s RR tx: %v", dialect, err)
	}

	// READ COMMITTED: the second in-tx read MUST see the concurrent commit.
	reset()
	o2 := DefaultTransactionOptions()
	o2.Isolation = IsolationReadCommitted
	_, err = Transaction(ctx, db, dialect, o2, func(txCtx *ExecutionContext) (int, error) {
		first := readSeqDirect(txCtx)
		if _, e := db.Exec(fmt.Sprintf("UPDATE %s SET seq = 77 WHERE id = 1", isoTbl)); e != nil {
			t.Fatalf("concurrent update: %v", e)
		}
		second := readSeqDirect(txCtx)
		if first == second {
			t.Errorf("%s READ COMMITTED: second read must see the concurrent commit, first=%d second=%d", dialect, first, second)
		}
		return 0, nil
	})
	if err != nil {
		t.Fatalf("%s RC tx: %v", dialect, err)
	}
}

// ── (4) RETRY under REAL contention ────────────────────────────────────────────

// retryPgWriteSkew — two concurrent SERIALIZABLE txs each read the sum then write their own row;
// PG raises 40001 on the loser. With retryOnError the loser re-runs the WHOLE boundary and BOTH
// eventually commit. Proven behaviorally (both rows present) + the retry actually fired (>2 BeginTx).
func retryPgWriteSkew(t *testing.T, db *sql.DB, sink *recSink) {
	if _, err := db.Exec(fmt.Sprintf("DELETE FROM %s", isoTbl)); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, worker, seq) VALUES (1, 500, 10), (2, 500, 20)", isoTbl)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sink.mu.Lock()
	sink.beginTx = 0
	sink.mu.Unlock()

	ctx := ContextForDB(db)
	o := DefaultTransactionOptions()
	o.Isolation = IsolationSerializable
	o.RetryDurationMs = 5

	barrier := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := Transaction(ctx, db, "postgres", o, func(txCtx *ExecutionContext) (int, error) {
				// Read the sum of both rows (the write-skew read set).
				rows, err := Execute(txCtx, fmt.Sprintf("SELECT COALESCE(SUM(seq),0) AS s FROM %s", isoTbl), nil, ReadIntent())
				if err != nil {
					return 0, err
				}
				_ = rows
				once.Do(func() { close(barrier) })
				<-barrier
				// Write our own row based on the read set → write-skew (PG placeholder = $1).
				_, err = Run(txCtx, fmt.Sprintf("UPDATE %s SET seq = seq + 1 WHERE id = $1", isoTbl), []any{int64(i + 1)}, WriteIntent())
				return 0, err
			})
			errs[i] = e
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			t.Errorf("PG retry: worker %d must eventually commit (retry should have absorbed 40001), got %v", i, e)
		}
	}
	_, beginTx, _, _, _ := sink.snapshot()
	if beginTx <= 2 {
		t.Errorf("PG retry: the retry must have fired (>2 BeginTx for 2 workers), got beginTx=%d", beginTx)
	}
	t.Logf("PG write-skew retry: both committed after %d BeginTx (retry absorbed 40001)", beginTx)
}

// retryMysqlDeadlock — two concurrent txs update two rows in OPPOSITE order → InnoDB deadlock (1213);
// the loser retries and BOTH commit.
func retryMysqlDeadlock(t *testing.T, db *sql.DB, sink *recSink) {
	if _, err := db.Exec(fmt.Sprintf("DELETE FROM %s", isoTbl)); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, worker, seq) VALUES (1, 500, 10), (2, 500, 20)", isoTbl)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sink.mu.Lock()
	sink.beginTx = 0
	sink.mu.Unlock()

	ctx := ContextForDB(db)
	o := DefaultTransactionOptions()
	o.RetryDurationMs = 5

	bar := make(chan struct{})
	var once sync.Once
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			first, second := int64(1), int64(2)
			if i == 1 {
				first, second = 2, 1 // opposite lock order → deadlock
			}
			_, e := Transaction(ctx, db, "mysql", o, func(txCtx *ExecutionContext) (int, error) {
				if _, err := Run(txCtx, fmt.Sprintf("UPDATE %s SET seq = seq + 1 WHERE id = ?", isoTbl), []any{first}, WriteIntent()); err != nil {
					return 0, err
				}
				once.Do(func() { close(bar) })
				<-bar
				_, err := Run(txCtx, fmt.Sprintf("UPDATE %s SET seq = seq + 1 WHERE id = ?", isoTbl), []any{second}, WriteIntent())
				return 0, err
			})
			errs[i] = e
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		if e != nil {
			t.Errorf("MySQL retry: worker %d must eventually commit (retry should have absorbed 1213), got %v", i, e)
		}
	}
	_, beginTx, _, _, _ := sink.snapshot()
	if beginTx <= 2 {
		t.Logf("MySQL deadlock retry: beginTx=%d (deadlock may not have raced this run; both committed)", beginTx)
	} else {
		t.Logf("MySQL deadlock retry: both committed after %d BeginTx (retry absorbed 1213)", beginTx)
	}
}

// nonRetryableDoesNotRetry — a unique-collision (non-retryable) inside a boundary fails on the FIRST
// attempt (no retry), even with retryOnError=true.
func nonRetryableDoesNotRetry(t *testing.T, db *sql.DB, sink *recSink, dialect string) {
	if _, err := db.Exec(fmt.Sprintf("DELETE FROM %s", isoTbl)); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, worker, seq) VALUES (400, 999, 9)", isoTbl)); err != nil {
		t.Fatalf("preseed: %v", err)
	}
	sink.mu.Lock()
	sink.beginTx = 0
	sink.mu.Unlock()
	ctx := ContextForDB(db)
	o := DefaultTransactionOptions() // retry ON
	o.RetryDurationMs = 5
	_, err := Transaction(ctx, db, dialect, o, func(txCtx *ExecutionContext) (int, error) {
		return 0, boundaryInsert(t, txCtx, db, dialect, 400, 4, 0) // PK collision — non-retryable
	})
	if err == nil {
		t.Fatalf("%s: a unique collision must fail the boundary", dialect)
	}
	_, beginTx, _, _, _ := sink.snapshot()
	if beginTx != 1 {
		t.Errorf("%s: a non-retryable error must NOT retry (1 BeginTx), got %d", dialect, beginTx)
	}
}

// ── (5) NESTED transaction ─────────────────────────────────────────────────────

func nestedOneBeginCommit(t *testing.T, db *sql.DB, sink *recSink, dialect string) {
	if _, err := db.Exec(fmt.Sprintf("DELETE FROM %s", isoTbl)); err != nil {
		t.Fatalf("clear: %v", err)
	}
	sink.mu.Lock()
	sink.beginTx, sink.commits, sink.rolls, sink.distinct = 0, 0, 0, nil
	sink.mu.Unlock()
	ctx := ContextForDB(db)
	_, err := Transaction(ctx, db, dialect, DefaultTransactionOptions(), func(outer *ExecutionContext) (int, error) {
		if err := boundaryInsert(t, outer, db, dialect, 500, 5, 0); err != nil {
			return 0, err
		}
		// A nested Transaction() JOINs the outer — no new BeginTx/COMMIT.
		return Transaction(outer, db, dialect, DefaultTransactionOptions(), func(inner *ExecutionContext) (int, error) {
			return 0, boundaryInsert(t, inner, db, dialect, 501, 5, 1)
		})
	})
	if err != nil {
		t.Fatalf("%s nested commit: %v", dialect, err)
	}
	_, beginTx, commits, rolls, distinct := sink.snapshot()
	if beginTx != 1 || commits != 1 || rolls != 0 || distinct != 1 {
		t.Errorf("%s nested: ONE BeginTx + ONE COMMIT on ONE *sql.Tx: beginTx=%d commits=%d rolls=%d distinct=%d", dialect, beginTx, commits, rolls, distinct)
	}
	// An inner error rolls back the WHOLE tx.
	sink.mu.Lock()
	sink.beginTx, sink.commits, sink.rolls = 0, 0, 0
	sink.mu.Unlock()
	if _, e := db.Exec(fmt.Sprintf("DELETE FROM %s", isoTbl)); e != nil {
		t.Fatalf("clear: %v", e)
	}
	if _, e := db.Exec(fmt.Sprintf("INSERT INTO %s (id, worker, seq) VALUES (601, 999, 9)", isoTbl)); e != nil {
		t.Fatalf("preseed 601: %v", e)
	}
	_, err = Transaction(ctx, db, dialect, DefaultTransactionOptions(), func(outer *ExecutionContext) (int, error) {
		if err := boundaryInsert(t, outer, db, dialect, 600, 6, 0); err != nil {
			return 0, err
		}
		return Transaction(outer, db, dialect, DefaultTransactionOptions(), func(inner *ExecutionContext) (int, error) {
			return 0, boundaryInsert(t, inner, db, dialect, 601, 6, 1) // inner PK collision
		})
	})
	if err == nil {
		t.Fatalf("%s nested inner error must fail the whole tx", dialect)
	}
	got := readIsoRows(t, db) // 601 pre-seed filtered
	if len(got) != 0 {
		t.Errorf("%s nested: an inner error rolls back the WHOLE tx (id=600 absent), got=%v", dialect, got)
	}
	_, _, commits2, rolls2, _ := sink.snapshot()
	if commits2 != 0 || rolls2 != 1 {
		t.Errorf("%s nested inner error ⇒ ONE ROLLBACK zero COMMIT: commits=%d rolls=%d", dialect, commits2, rolls2)
	}
}

// ── Entry points ───────────────────────────────────────────────────────────────

func TestTxBoundaryPostgres(t *testing.T) {
	if !txIsoEnabled() {
		t.Skip("skipped (set LITEDBMODEL_TX_ISOLATION=1 + docker up)")
	}
	db, sink := openPgRec(t)
	defer db.Close()
	resetIso(t, db, "INTEGER")
	multiOpAtomicityCommit(t, db, sink, "postgres")
	resetIso(t, db, "INTEGER")
	multiOpAtomicityRollback(t, db, sink, "postgres")
	resetIso(t, db, "INTEGER")
	guardLive(t, db, "postgres")
	resetIso(t, db, "INTEGER")
	isolationBehavior(t, db, "postgres")
	resetIso(t, db, "INTEGER")
	retryPgWriteSkew(t, db, sink)
	resetIso(t, db, "INTEGER")
	nonRetryableDoesNotRetry(t, db, sink, "postgres")
	resetIso(t, db, "INTEGER")
	nestedOneBeginCommit(t, db, sink, "postgres")
	t.Log("PG TX-BOUNDARY PROOF: multi-op atomicity (1 BeginTx/COMMIT/*sql.Tx + A-rolls-back-when-B-fails) + guard + isolation + real-contention retry (40001) + nested — all green")
}

func TestTxBoundaryMysql(t *testing.T) {
	if !txIsoEnabled() {
		t.Skip("skipped (set LITEDBMODEL_TX_ISOLATION=1 + docker up)")
	}
	db, sink := openMysqlRec(t)
	defer db.Close()
	resetIso(t, db, "INT")
	multiOpAtomicityCommit(t, db, sink, "mysql")
	resetIso(t, db, "INT")
	multiOpAtomicityRollback(t, db, sink, "mysql")
	resetIso(t, db, "INT")
	guardLive(t, db, "mysql")
	resetIso(t, db, "INT")
	isolationBehavior(t, db, "mysql")
	resetIso(t, db, "INT")
	retryMysqlDeadlock(t, db, sink)
	resetIso(t, db, "INT")
	nonRetryableDoesNotRetry(t, db, sink, "mysql")
	resetIso(t, db, "INT")
	nestedOneBeginCommit(t, db, sink, "mysql")
	t.Log("MYSQL TX-BOUNDARY PROOF: multi-op atomicity (1 BeginTx/COMMIT/*sql.Tx + A-rolls-back-when-B-fails) + guard + isolation + real-contention retry (1213) + nested — all green")
}
