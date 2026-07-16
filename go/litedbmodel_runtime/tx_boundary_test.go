// Phase B (#83, go) — UNIT tests for the tx-completeness primitives + the public Transaction()
// boundary. The go mirror of rust's tests inside exec_context.rs (`transaction_boundary_*`,
// `guard_*`, `retry_*`, `rollback_only_*`, `isolation_prelude_*`) and tx_options.rs's contract-shape
// tests. No live DB — a RECORDING sql/driver wraps in-proc SQLite (:memory:) and records every
// statement it prepares/execs + counts driver-level BeginTx / Commit / Rollback, so a Transaction()
// boundary's "exactly ONE BeginTx + ONE Commit on ONE *sql.Tx" claim is asserted at the DRIVER layer
// (the real proof the ambient JOIN opens no second physical tx). The live PG/MySQL multi-op
// atomicity + guard + isolation + real-contention retry proof lives in tx_boundary_livedb_test.go.

package litedbmodel_runtime

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	sqlited "modernc.org/sqlite"
)

// ── The recording driver: wraps modernc SQLite, records SQL + counts tx lifecycle ──

// recSink is the shared recorder every wrapped connection writes to (SQL log + tx-lifecycle counts).
type recSink struct {
	mu       sync.Mutex
	log      []string // every statement prepared/exec'd/queried, in order (incl. SET/BEGIN? — see note)
	beginTx  int      // driver-level BeginTx calls
	commits  int      // driver-level Commit calls
	rolls    int      // driver-level Rollback calls
	distinct map[driver.Tx]struct{}
}

func (s *recSink) push(sql string) {
	s.mu.Lock()
	s.log = append(s.log, sql)
	s.mu.Unlock()
}
func (s *recSink) begin(tx driver.Tx) {
	s.mu.Lock()
	s.beginTx++
	if s.distinct == nil {
		s.distinct = map[driver.Tx]struct{}{}
	}
	s.distinct[tx] = struct{}{}
	s.mu.Unlock()
}
func (s *recSink) snapshot() (log []string, beginTx, commits, rolls, distinct int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.log...), s.beginTx, s.commits, s.rolls, len(s.distinct)
}

// reset clears the recorded log + the (legacy) driver-level counters — so a test can measure ONE
// boundary in isolation. Phase D (#94) assertions read txControlCounts (the seam-visible SQL log).
func (s *recSink) reset() {
	s.mu.Lock()
	s.log = nil
	s.beginTx, s.commits, s.rolls, s.distinct = 0, 0, 0, nil
	s.mu.Unlock()
}

// txControlCounts counts the runtime's OWN tx-control SQL as it appears in the recorded statement log
// (Phase D / #94 — the tx runtime now issues BEGIN/COMMIT/ROLLBACK as REAL SQL strings THROUGH the seam
// on the owned *sql.Conn, so the driver-level BeginTx/Commit/Rollback counters are 0; the tx-control is
// OBSERVABLE as log entries — the very property that makes it middleware-visible). A leading BEGIN (any
// `BEGIN…` incl. `BEGIN ISOLATION LEVEL …`) counts as a begin.
func (s *recSink) txControlCounts() (begins, commits, rolls int) {
	log, _, _, _, _ := s.snapshot()
	for _, stmt := range log {
		u := strings.ToUpper(strings.TrimSpace(stmt))
		switch {
		case strings.HasPrefix(u, "BEGIN"):
			begins++
		case u == "COMMIT":
			commits++
		case u == "ROLLBACK":
			rolls++
		}
	}
	return begins, commits, rolls
}

// recDriver wraps a base sql/driver, recording into a per-DSN sink (looked up by DSN).
type recDriver struct{ base driver.Driver }

var recSinks sync.Map // dsn -> *recSink

func recSinkFor(dsn string) *recSink {
	v, _ := recSinks.LoadOrStore(dsn, &recSink{})
	return v.(*recSink)
}

var registerRecOnce sync.Once

func registerRecDriver() {
	registerRecOnce.Do(func() {
		sql.Register("sqlite-rec", recDriver{base: &sqlited.Driver{}})
	})
}

func (d recDriver) Open(name string) (driver.Conn, error) {
	c, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	return &recConn{base: c, sink: recSinkFor(name)}, nil
}

type recConn struct {
	base driver.Conn
	sink *recSink
}

func (c *recConn) Prepare(query string) (driver.Stmt, error) {
	c.sink.push(query)
	st, err := c.base.Prepare(query)
	if err != nil {
		return nil, err
	}
	return st, nil
}
func (c *recConn) Close() error { return c.base.Close() }

func (c *recConn) Begin() (driver.Tx, error) { //nolint:staticcheck
	tx, err := c.base.Begin() //nolint:staticcheck
	if err != nil {
		return nil, err
	}
	c.sink.begin(tx)
	return &recTx{base: tx, sink: c.sink}, nil
}

func (c *recConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var tx driver.Tx
	var err error
	if b, ok := c.base.(driver.ConnBeginTx); ok {
		tx, err = b.BeginTx(ctx, opts)
	} else {
		tx, err = c.base.Begin() //nolint:staticcheck
	}
	if err != nil {
		return nil, err
	}
	c.sink.begin(tx)
	return &recTx{base: tx, sink: c.sink}, nil
}

// PrepareContext / Query / Exec forwarding (SQLite driver implements the context variants).
func (c *recConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.sink.push(query)
	if pc, ok := c.base.(driver.ConnPrepareContext); ok {
		return pc.PrepareContext(ctx, query)
	}
	return c.base.Prepare(query)
}

// ExecContext / QueryContext forward to the base driver's context Execer/Queryer when it implements
// them (Phase D / #94): this is REQUIRED so an arg-less statement — notably the runtime's OWN
// BEGIN/COMMIT/ROLLBACK/SET tx-control, now issued as REAL SQL THROUGH the seam — takes the base
// driver's TEXT protocol instead of database/sql's Prepare-fallback. MySQL rejects BEGIN/COMMIT/SET
// through the prepared-statement protocol (Error 1295), so without this forward a wrapped MySQL conn
// would fail every tx-control. The SQL is recorded first (so txControlCounts observes it). If the base
// lacks the context Execer/Queryer, returning ErrSkip lets database/sql fall back to Prepare.
func (c *recConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.sink.push(query)
	if ec, ok := c.base.(driver.ExecerContext); ok {
		return ec.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *recConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.sink.push(query)
	if qc, ok := c.base.(driver.QueryerContext); ok {
		return qc.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

type recTx struct {
	base driver.Tx
	sink *recSink
}

func (t *recTx) Commit() error {
	t.sink.mu.Lock()
	t.sink.commits++
	t.sink.mu.Unlock()
	return t.base.Commit()
}
func (t *recTx) Rollback() error {
	t.sink.mu.Lock()
	t.sink.rolls++
	t.sink.mu.Unlock()
	return t.base.Rollback()
}

var _ io.Closer = (*recConn)(nil)

// openRec opens a recording SQLite :memory: DB (a unique DSN per test → its own sink) seeded with the
// `t` table. SQLite :memory: is per-connection, so cap the pool to ONE connection (all tx + reads on
// the same in-memory DB).
func openRec(t *testing.T, dsn string) (*sql.DB, *recSink) {
	t.Helper()
	registerRecDriver()
	db, err := sql.Open("sqlite-rec", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	for _, s := range []string{
		`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO t VALUES (1, 'a')`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return db, recSinkFor(dsn)
}

// ── tx_options.go contract-shape unit tests (mirror tx_options.rs tests) ───────

func TestDefaultsMatchThePhaseBContract(t *testing.T) {
	o := DefaultTransactionOptions()
	if !o.RetryOnError || o.RetryLimit != 3 || o.RetryDurationMs != 200 || o.RollbackOnly || o.Isolation != IsolationNone {
		t.Fatalf("defaults mismatch: %+v", o)
	}
}

func TestBeginStatementsPerDialect(t *testing.T) {
	cases := []struct {
		dialect string
		iso     IsolationLevel
		want    []string
	}{
		{"postgres", IsolationNone, []string{"BEGIN"}},
		{"postgres", IsolationSerializable, []string{"BEGIN ISOLATION LEVEL SERIALIZABLE"}},
		{"mysql", IsolationRepeatableRead, []string{"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", "BEGIN"}},
		{"mysql", IsolationNone, []string{"BEGIN"}},
	}
	for _, c := range cases {
		got, err := BeginStatements(c.dialect, c.iso)
		if err != nil {
			t.Fatalf("%s/%d: %v", c.dialect, c.iso, err)
		}
		if fmt.Sprint(got) != fmt.Sprint(c.want) {
			t.Errorf("%s/%d: got %v want %v", c.dialect, c.iso, got, c.want)
		}
	}
}

func TestSqliteIsolationIsAHardError(t *testing.T) {
	if _, err := BeginStatements("sqlite", IsolationSerializable); err == nil {
		t.Errorf("sqlite + isolation must be a hard error")
	}
	got, err := BeginStatements("sqlite", IsolationNone)
	if err != nil || fmt.Sprint(got) != fmt.Sprint([]string{"BEGIN"}) {
		t.Errorf("sqlite bare BEGIN: got %v err %v", got, err)
	}
	// The prelude form too.
	if _, _, err := isolationPrelude("sqlite", IsolationRepeatableRead); err == nil {
		t.Errorf("isolationPrelude sqlite + isolation must be a hard error")
	}
}

func TestIsolationPreludeSplit(t *testing.T) {
	// PG SET runs post-BEGIN; MySQL SET runs pre-BEGIN.
	b, a, _ := isolationPrelude("postgres", IsolationSerializable)
	if len(b) != 0 || fmt.Sprint(a) != fmt.Sprint([]string{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"}) {
		t.Errorf("pg prelude: before=%v after=%v", b, a)
	}
	b, a, _ = isolationPrelude("mysql", IsolationReadCommitted)
	if fmt.Sprint(b) != fmt.Sprint([]string{"SET TRANSACTION ISOLATION LEVEL READ COMMITTED"}) || len(a) != 0 {
		t.Errorf("mysql prelude: before=%v after=%v", b, a)
	}
	b, a, _ = isolationPrelude("postgres", IsolationNone)
	if len(b) != 0 || len(a) != 0 {
		t.Errorf("none prelude must be empty: before=%v after=%v", b, a)
	}
}

func TestRetryableClassification(t *testing.T) {
	// Typed PG serialization / deadlock codes (via errors.As on *pgconn.PgError).
	if !IsRetryableTxError(&pgconn.PgError{Code: "40001", Message: "serialize"}) {
		t.Errorf("PG 40001 must be retryable")
	}
	if !IsRetryableTxError(&pgconn.PgError{Code: "40P01", Message: "deadlock"}) {
		t.Errorf("PG 40P01 must be retryable")
	}
	// Typed MySQL deadlock / lock-wait errno (via *mysql.MySQLError).
	if !IsRetryableTxError(&gomysql.MySQLError{Number: 1213, Message: "Deadlock found"}) {
		t.Errorf("MySQL 1213 must be retryable")
	}
	if !IsRetryableTxError(&gomysql.MySQLError{Number: 1205, Message: "Lock wait timeout"}) {
		t.Errorf("MySQL 1205 must be retryable")
	}
	// String fallback (a re-surfaced SqlFailure whose Msg embeds the code text).
	if !IsRetryableTxError(&SqlFailure{Kind: KindDriverError, Policy: "fail", Msg: "could not serialize access due to concurrent update (SQLSTATE 40001)"}) {
		t.Errorf("SQLSTATE-40001 message must be retryable")
	}
	// Connection error → retryable.
	if !IsRetryableTxError(errors.New("Connection terminated unexpectedly")) {
		t.Errorf("connection error must be retryable")
	}
	// A data conflict is NOT retryable.
	if IsRetryableTxError(&pgconn.PgError{Code: "23505", Message: "duplicate key"}) {
		t.Errorf("PG 23505 (unique) must NOT be retryable")
	}
	if IsRetryableTxError(errors.New("some unrelated driver error")) {
		t.Errorf("unrelated error must NOT be retryable")
	}
}

// The typed-code path must be REACHABLE through a mapped SqlFailure (mapSqliteError wraps the concrete
// driver error; SqlFailure.Unwrap() re-exposes it) — the regression guard against the errors.As block
// silently rotting into dead code. With the string fallback DISABLED, a mapped *pgconn.PgError /
// *mysql.MySQLError must STILL classify as retryable purely via errors.As on the wrapped error.
func TestTypedRetryablePathReachableThroughMappedFailure(t *testing.T) {
	// mapSqliteError treats a non-SQLite error as a live-DB error and wraps it (wrapped: err).
	pgMapped := mapSqliteError(&pgconn.PgError{Code: "40001", Message: "could not serialize"})
	myMapped := mapSqliteError(&gomysql.MySQLError{Number: 1213, Message: "Deadlock found"})

	// errors.As must traverse the mapped SqlFailure → its wrapped concrete driver error.
	var pg *pgconn.PgError
	if !errors.As(error(pgMapped), &pg) || pg.Code != "40001" {
		t.Fatalf("mapped PG failure must expose the concrete *pgconn.PgError via Unwrap(): got %v", pg)
	}
	var my *gomysql.MySQLError
	if !errors.As(error(myMapped), &my) || my.Number != 1213 {
		t.Fatalf("mapped MySQL failure must expose the concrete *mysql.MySQLError via Unwrap(): got %v", my)
	}

	// With the STRING fallback disabled, the TYPED path alone must classify the MAPPED failures as
	// retryable — proving the typed extraction is load-bearing, not dead code behind string matching.
	disableRetryStringFallback = true
	defer func() { disableRetryStringFallback = false }()
	if !IsRetryableTxError(error(pgMapped)) {
		t.Errorf("mapped PG 40001 must be retryable via the TYPED path alone (string fallback disabled)")
	}
	if !IsRetryableTxError(error(myMapped)) {
		t.Errorf("mapped MySQL 1213 must be retryable via the TYPED path alone (string fallback disabled)")
	}
	// A mapped non-retryable code must NOT be retryable even via the typed path.
	if IsRetryableTxError(error(mapSqliteError(&pgconn.PgError{Code: "23505", Message: "unique"}))) {
		t.Errorf("mapped PG 23505 (unique) must NOT be retryable")
	}
}

func TestGuardOrderReadOnlyFirst(t *testing.T) {
	// Read-only is rejected FIRST (more specific), even with no active tx.
	e := CheckWriteAllowed("INSERT", "users", false, true)
	if f, ok := e.(*SqlFailure); !ok || f.Kind != "write_in_read_only_context" {
		t.Errorf("read-only must be rejected first: %v", e)
	}
	// No active tx (not read-only) → outside-transaction.
	e = CheckWriteAllowed("INSERT", "users", false, false)
	if f, ok := e.(*SqlFailure); !ok || f.Kind != "write_outside_transaction" {
		t.Errorf("no tx must be outside-transaction: %v", e)
	}
	// Inside a tx (not read-only) → allowed.
	if e := CheckWriteAllowed("INSERT", "users", true, false); e != nil {
		t.Errorf("inside a tx must be allowed: %v", e)
	}
}

// ── The public Transaction() boundary — driver-level proofs ────────────────────

// N ops inside ONE Transaction() boundary ⇒ exactly ONE BeginTx + ONE Commit on ONE *sql.Tx (the
// ambient JOIN — the second op does NOT open its own BEGIN/COMMIT).
func TestTransactionBoundaryOneBeginOneCommitForNOps(t *testing.T) {
	db, sink := openRec(t, "file:txb1?mode=memory&cache=shared")
	defer db.Close()
	ctx := ContextForDB(db)
	_, err := Transaction(ctx, db, "sqlite", DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
		if _, err := RunGuarded(txCtx, "INSERT INTO t (id, v) VALUES (?, ?)", []any{int64(2), "b"}, "INSERT", "t"); err != nil {
			return 0, err
		}
		// A "nested" Transaction (as a joined write would do) must NOT open a new BeginTx.
		return Transaction(txCtx, db, "sqlite", DefaultTransactionOptions(), func(inner *ExecutionContext) (int, error) {
			if _, err := RunGuarded(inner, "INSERT INTO t (id, v) VALUES (?, ?)", []any{int64(3), "c"}, "INSERT", "t"); err != nil {
				return 0, err
			}
			return 9, nil
		})
	})
	if err != nil {
		t.Fatalf("boundary: %v", err)
	}
	// Phase D (#94): tx-control is seam-issued SQL — the nested Transaction JOINs (no new BEGIN/COMMIT),
	// so N ops in one boundary produce exactly ONE BEGIN + ONE COMMIT (observable in the log).
	begins, commits, rolls := sink.txControlCounts()
	if begins != 1 || commits != 1 || rolls != 0 {
		t.Errorf("N ops in one boundary must be ONE BEGIN + ONE COMMIT on ONE owned conn: begins=%d commits=%d rolls=%d", begins, commits, rolls)
	}
	// Both rows committed together.
	rows, _ := Execute(ctx, "SELECT id FROM t WHERE id IN (2,3)", nil, ReadIntent())
	if len(rows) != 2 {
		t.Errorf("both ops committed together, want 2 rows got %d", len(rows))
	}
}

// A failing op inside the boundary ⇒ ONE BeginTx + ONE Rollback, zero Commit; the earlier op's row is
// part of the SAME physical tx (rolled back with it) — opA rolls back when opB fails.
func TestTransactionBodyErrorRollsBackTheWholeTx(t *testing.T) {
	db, sink := openRec(t, "file:txb2?mode=memory&cache=shared")
	defer db.Close()
	ctx := ContextForDB(db)
	_, err := Transaction(ctx, db, "sqlite", DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
		if _, err := RunGuarded(txCtx, "INSERT INTO t (id, v) VALUES (?, ?)", []any{int64(2), "b"}, "INSERT", "t"); err != nil {
			return 0, err
		}
		// opB collides on the PK (id=1 pre-seeded) → whole-tx error.
		if _, err := RunGuarded(txCtx, "INSERT INTO t (id, v) VALUES (?, ?)", []any{int64(1), "dup"}, "INSERT", "t"); err != nil {
			return 0, err
		}
		return 0, nil
	})
	if err == nil {
		t.Fatalf("expected opB's PK collision to propagate")
	}
	// Phase D (#94): ONE BEGIN + ONE ROLLBACK + zero COMMIT (tx-control seam-visible in the log).
	begins, commits, rolls := sink.txControlCounts()
	if begins != 1 || commits != 0 || rolls != 1 {
		t.Errorf("body error ⇒ ONE BEGIN + ONE ROLLBACK + zero COMMIT: begins=%d commits=%d rolls=%d", begins, commits, rolls)
	}
	// opA (id=2) must be rolled back with the whole tx.
	rows, _ := Execute(ctx, "SELECT id FROM t WHERE id = ?", []any{int64(2)}, ReadIntent())
	if len(rows) != 0 {
		t.Errorf("opA (id=2) must be rolled back when opB fails, got %d rows", len(rows))
	}
}

// rollbackOnly: a successful body ROLLBACKs (dry-run) but still returns its value; no Commit.
func TestTransactionRollbackOnlyReturnsValueButDoesNotCommit(t *testing.T) {
	db, sink := openRec(t, "file:txb3?mode=memory&cache=shared")
	defer db.Close()
	ctx := ContextForDB(db)
	o := DefaultTransactionOptions()
	o.RollbackOnly = true
	out, err := Transaction(ctx, db, "sqlite", o, func(txCtx *ExecutionContext) (int, error) {
		if _, err := RunGuarded(txCtx, "INSERT INTO t (id, v) VALUES (?, ?)", []any{int64(2), "b"}, "INSERT", "t"); err != nil {
			return 0, err
		}
		return 42, nil
	})
	if err != nil || out != 42 {
		t.Fatalf("rollbackOnly: out=%d err=%v", out, err)
	}
	// Phase D (#94): tx-control is issued THROUGH the seam as SQL, so it appears in the recorded log
	// (middleware-visible) — assert the tx-control SQL, not the (now-0) driver.Tx counters.
	begins, commits, rolls := sink.txControlCounts()
	if begins != 1 || commits != 0 || rolls != 1 {
		t.Errorf("rollbackOnly ⇒ BEGIN + ROLLBACK, never COMMIT: begins=%d commits=%d rolls=%d", begins, commits, rolls)
	}
	rows, _ := Execute(ctx, "SELECT id FROM t WHERE id = ?", []any{int64(2)}, ReadIntent())
	if len(rows) != 0 {
		t.Errorf("rollbackOnly: no change committed, got %d rows", len(rows))
	}
}

// The write=tx guard through the seam: a bare guarded write outside a Transaction() is rejected; a
// read-only-scoped write is rejected read-only-first; inside a Transaction() it succeeds.
func TestGuardWriteOutsideVsReadOnlyVsInside(t *testing.T) {
	db, _ := openRec(t, "file:txb4?mode=memory&cache=shared")
	defer db.Close()
	ctx := ContextForDB(db)
	// Outside any boundary → WriteOutsideTransaction, no SQL issued.
	e := func() error {
		_, err := RunGuarded(ctx, "INSERT INTO t (id,v) VALUES (5,'x')", nil, "INSERT", "t")
		return err
	}()
	if f, ok := e.(*SqlFailure); !ok || f.Kind != "write_outside_transaction" {
		t.Errorf("bare write must be WriteOutsideTransaction: %v", e)
	}
	// Read-only-scoped write inside a tx → WriteInReadOnly (read-only checked first).
	_, _ = WithTransaction(ctx, db, func(txCtx *ExecutionContext) (int, error) {
		ro := txCtx.WithReadOnly()
		e := func() error {
			_, err := RunGuarded(ro, "UPDATE t SET v='y' WHERE id=1", nil, "UPDATE", "t")
			return err
		}()
		if f, ok := e.(*SqlFailure); !ok || f.Kind != "write_in_read_only_context" {
			t.Errorf("read-only write must be WriteInReadOnly: %v", e)
		}
		return 0, nil
	})
	// Inside a Transaction() boundary → succeeds.
	_, err := Transaction(ctx, db, "sqlite", DefaultTransactionOptions(), func(txCtx *ExecutionContext) (int, error) {
		_, err := RunGuarded(txCtx, "INSERT INTO t (id,v) VALUES (6,'z')", nil, "INSERT", "t")
		return 0, err
	})
	if err != nil {
		t.Errorf("guarded write inside a Transaction() must succeed: %v", err)
	}
}

// The retry loop re-runs the WHOLE tx on a retryable error (fresh *sql.Tx per attempt); a
// non-retryable error does NOT retry. Driven by a body that fails N times with a retryable error.
func TestRetryRerunsWholeTxOnRetryableError(t *testing.T) {
	db, sink := openRec(t, "file:txb5?mode=memory&cache=shared")
	defer db.Close()
	ctx := ContextForDB(db)
	o := DefaultTransactionOptions()
	o.RetryDurationMs = 0 // no sleep in the test
	fails := 1            // fail once, succeed on attempt 2
	out, err := Transaction(ctx, db, "sqlite", o, func(txCtx *ExecutionContext) (int, error) {
		if _, err := RunGuarded(txCtx, "INSERT INTO t (id,v) VALUES (7,'w')", nil, "INSERT", "t"); err != nil {
			return 0, err
		}
		if fails > 0 {
			fails--
			// A synthetic retryable (serialization) failure — the loop must ROLLBACK + retry.
			return 0, &pgconn.PgError{Code: "40001", Message: "could not serialize access"}
		}
		return 5, nil
	})
	if err != nil || out != 5 {
		t.Fatalf("retry: out=%d err=%v", out, err)
	}
	// Phase D (#94): each attempt re-issues BEGIN through the seam on a FRESH owned conn.
	// Attempt 1: BEGIN, insert, ROLLBACK. Attempt 2: BEGIN, insert, COMMIT — 2 BEGIN + 1 COMMIT + 1 ROLLBACK.
	begins, commits, rolls := sink.txControlCounts()
	if begins != 2 || commits != 1 || rolls != 1 {
		t.Errorf("retry ⇒ 2 BEGIN + 1 COMMIT + 1 ROLLBACK: begins=%d commits=%d rolls=%d", begins, commits, rolls)
	}
}

func TestNonRetryableErrorDoesNotRetry(t *testing.T) {
	db, sink := openRec(t, "file:txb6?mode=memory&cache=shared")
	defer db.Close()
	ctx := ContextForDB(db)
	o := DefaultTransactionOptions()
	o.RetryDurationMs = 0
	calls := 0
	_, err := Transaction(ctx, db, "sqlite", o, func(txCtx *ExecutionContext) (int, error) {
		calls++
		return 0, errors.New("some non-retryable driver error")
	})
	if err == nil {
		t.Fatalf("expected the non-retryable error to surface")
	}
	if calls != 1 {
		t.Errorf("a non-retryable error must NOT retry, body called %d times", calls)
	}
	// Phase D (#94): a single attempt ⇒ 1 BEGIN + 1 ROLLBACK, no COMMIT (tx-control seam-visible).
	begins, commits, rolls := sink.txControlCounts()
	if begins != 1 || commits != 0 || rolls != 1 {
		t.Errorf("non-retryable ⇒ single attempt (1 BEGIN + 1 ROLLBACK): begins=%d commits=%d rolls=%d", begins, commits, rolls)
	}
}
