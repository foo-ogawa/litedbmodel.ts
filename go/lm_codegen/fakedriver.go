package main

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"

	sqlite "modernc.org/sqlite"
)

// ════════════════════════════════════════════════════════════════════════════
// Mock database/sql driver for the #44 Go CODEGEN cell's I/O-EXCLUDED micro-bench.
// ════════════════════════════════════════════════════════════════════════════
//
// The micro-bench times ONLY the client-side path (render + typed param bind + row hydrate) — it
// MUST mock the SQL driver (fixed rows, no real round-trip) so the timed op excludes DB I/O, exactly
// like the go/sql (lm_bench) cell's `openMockDB()`. Before this, the codegen cell's micro ran against
// the REAL in-proc sqlite driver (`seedDriver()`), so it measured real query execution — an UNFAIR
// comparison against every other cell's mocked micro (the go/codegen micro looked ~10-20× slower than
// go/sql purely because go/sql mocked the driver and codegen did not). This mock restores parity.
//
// It carries NO litedbmodel_runtime, NO encoding/json — it uses only `database/sql/driver` (the same
// purity discipline as the rest of this binary; mirrors lm_bench/fakedriver.go's mockDriver).
type mockDriver struct{}
type mockConn struct{}
type mockStmt struct{ query string }
type mockRows struct {
	cols []string
	data [][]driver.Value
	pos  int
}

func (mockDriver) Open(string) (driver.Conn, error) { return mockConn{}, nil }
func (mockConn) Prepare(q string) (driver.Stmt, error) {
	return mockStmt{query: q}, nil
}
func (mockConn) Close() error              { return nil }
func (mockConn) Begin() (driver.Tx, error) { return mockTx{}, nil }

type mockTx struct{}

func (mockTx) Commit() error   { return nil }
func (mockTx) Rollback() error { return nil }

func (s mockStmt) Close() error  { return nil }
func (s mockStmt) NumInput() int { return -1 }
func (s mockStmt) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}
func (s mockStmt) Query([]driver.Value) (driver.Rows, error) {
	cols, data := mockFixture(s.query)
	return &mockRows{cols: cols, data: data}, nil
}

func (r *mockRows) Columns() []string { return r.cols }
func (r *mockRows) Close() error      { return nil }
func (r *mockRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

// Fixed fixtures keyed by SQL shape (identical rows for every impl → comparable hydrate cost).
// Byte-for-byte the same fixtures as lm_bench/fakedriver.go's mockFixture, so the go/sql and
// go/codegen micro cells hydrate the SAME row shapes (a fair client-path comparison).
func mockFixture(sqlText string) ([]string, [][]driver.Value) {
	s := strings.ToLower(sqlText)
	st := strings.TrimSpace(s)
	if strings.HasPrefix(st, "select") {
		switch {
		case strings.Contains(s, "from comments"):
			data := make([][]driver.Value, 25)
			for i := 0; i < 25; i++ {
				data[i] = []driver.Value{int64(i + 1), int64((i % 5) + 1), "comment-" + itoa(i+1)}
			}
			return []string{"id", "post_id", "body"}, data
		case strings.Contains(s, "from users"):
			return []string{"id", "name"}, [][]driver.Value{{int64(1), "user-1"}}
		case strings.Contains(s, "from posts") || strings.Contains(s, "from "):
			data := make([][]driver.Value, 5)
			for i := 0; i < 5; i++ {
				data[i] = []driver.Value{int64(i + 1), int64(1), "post-" + itoa(i+1), "live", int64((i + 1) * 10), "2026-02-01"}
			}
			return []string{"id", "author_id", "title", "status", "views", "created_at"}, data
		}
		return []string{"1"}, [][]driver.Value{{int64(1)}}
	}
	if strings.Contains(s, "returning") {
		return []string{"id", "author_id", "title"}, [][]driver.Value{{int64(41), int64(1), "txn-post"}}
	}
	return []string{}, [][]driver.Value{}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

var mockOnce sync.Once

func openMockDB() *sql.DB {
	mockOnce.Do(func() { sql.Register("lm_codegen_mock", mockDriver{}) })
	db, _ := sql.Open("lm_codegen_mock", "")
	return db
}

// ════════════════════════════════════════════════════════════════════════════
// Trace driver — the fairness cost probe (queries/op + rows/op).
// ════════════════════════════════════════════════════════════════════════════
//
// A DRIVER-LEVEL passthrough over modernc sqlite that counts DML statements + rows read (tx-control
// excluded), mirroring lm_bench/fakedriver.go's traceDriver. The prior `countingDB` wrapped at the
// `*sql.DB` API level, so it could count Query()/Exec() calls but NEVER saw `rows.Next()` — rows/op
// rendered as 0, and Begin() was an untraced passthrough. Wrapping at the driver conn level fixes
// both: rows are counted at driver.Rows.Next() (the real number the caller reads), and the write-tx
// path (statements on a *sql.Tx over the SAME pinned conn) is traced too. Carries ONLY
// database/sql/driver + modernc — NO encoding/json, NO litedbmodel_runtime (purity preserved).
type traceCounters struct {
	queries int
	rows    int
}

var (
	cgTraceCtr  *traceCounters
	cgTraceOnce sync.Once
	cgTraceLock sync.Mutex
)

type traceDriver struct{ inner driver.Driver }
type traceConn struct{ inner driver.Conn }
type traceStmt struct {
	inner driver.Stmt
	dml   bool
}
type traceRows struct{ inner driver.Rows }

func (d traceDriver) Open(name string) (driver.Conn, error) {
	c, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return traceConn{inner: c}, nil
}
func (c traceConn) Prepare(query string) (driver.Stmt, error) {
	s, err := c.inner.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &traceStmt{inner: s, dml: !isTxControl(query)}, nil
}
func (c traceConn) Close() error              { return c.inner.Close() }
func (c traceConn) Begin() (driver.Tx, error) { return c.inner.Begin() } //nolint:staticcheck

func (s *traceStmt) Close() error  { return s.inner.Close() }
func (s *traceStmt) NumInput() int { return s.inner.NumInput() }
func (s *traceStmt) Exec(args []driver.Value) (driver.Result, error) {
	if s.dml {
		cgTraceCtr.queries++
	}
	return s.inner.Exec(args) //nolint:staticcheck
}
func (s *traceStmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.dml {
		cgTraceCtr.queries++
	}
	r, err := s.inner.Query(args) //nolint:staticcheck
	if err != nil || !s.dml {
		return r, err
	}
	return &traceRows{inner: r}, nil
}
func (r *traceRows) Columns() []string { return r.inner.Columns() }
func (r *traceRows) Close() error      { return r.inner.Close() }
func (r *traceRows) Next(dest []driver.Value) error {
	err := r.inner.Next(dest)
	if err == nil {
		cgTraceCtr.rows++
	}
	return err
}

// costViaTrace runs ONE codegen case through the trace driver, returning (queries, rows) — the
// fairness cost. Seeds from the native companion (cgplans), resets the counter after seeding, pins
// ONE conn so the write-tx conn's statements are seen, and drives cgcell.RunCodegen (the SAME path
// the timed cell runs). rows/op now reflects the real rows the codegen read materializes.
func costViaTrace(caseID string) (int, int) {
	cgTraceOnce.Do(func() {
		sql.Register("lm_codegen_trace", traceDriver{inner: &sqlite.Driver{}})
	})
	cgTraceLock.Lock()
	defer cgTraceLock.Unlock()
	cgTraceCtr = &traceCounters{}

	db, err := sql.Open("lm_codegen_trace", ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // pin ONE conn so the counter sees every statement (incl. the tx conn)
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		panic(err)
	}
	for _, s := range cgplansSchema() {
		if _, err := db.Exec(s); err != nil {
			panic(err)
		}
	}
	for _, s := range cgplansSeed() {
		if _, err := db.Exec(s); err != nil {
			panic(err)
		}
	}
	cgTraceCtr = &traceCounters{} // reset AFTER seeding so seed inserts aren't counted
	runCodegenForCost(caseID, db)
	return cgTraceCtr.queries, cgTraceCtr.rows
}
