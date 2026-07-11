package main

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"

	sqlite "modernc.org/sqlite"
)

// moderncDriver returns the modernc sqlite driver.Driver instance to wrap for the trace probe.
func moderncDriver() driver.Driver { return &sqlite.Driver{} }

// ════════════════════════════════════════════════════════════════════════════
// Fake database/sql drivers for the #44 Go bench.
// ════════════════════════════════════════════════════════════════════════════
//
//  - mockDriver:  returns FIXED rows with NO real DB round-trip — the I/O-excluded
//                 micro-bench transport (times ONLY the client-side path).
//  - traceDriver: a passthrough over modernc sqlite that COUNTS DML statements +
//                 rows read (tx-control excluded) — the fairness cost probe. It
//                 can count rows because it sees every driver-level Next().

// ── mock driver (micro-bench) ────────────────────────────────────────────────
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
func mockFixture(sql string) ([]string, [][]driver.Value) {
	s := strings.ToLower(sql)
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
	mockOnce.Do(func() { sql.Register("lm_mock", mockDriver{}) })
	db, _ := sql.Open("lm_mock", "")
	return db
}

// ── trace driver (cost probe) ────────────────────────────────────────────────
// A DRIVER-LEVEL passthrough over modernc sqlite that counts DML statements + rows read
// (tx-control excluded) through a shared counter. Wrapping at the driver conn level means the
// count is accurate even for the write-tx path (statements run on a *sql.Tx over the SAME
// underlying conn). Rows are counted at driver.Rows.Next() — the real number the caller reads.
type traceCounters struct {
	queries int
	rows    int
}

var (
	traceCtr     *traceCounters
	traceOnce    sync.Once
	traceCtrLock sync.Mutex
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
		traceCtr.queries++
	}
	return s.inner.Exec(args) //nolint:staticcheck
}
func (s *traceStmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.dml {
		traceCtr.queries++
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
		traceCtr.rows++
	}
	return err
}

func costViaTrace(impl, caseID string, a *artifact) (int, int) {
	traceOnce.Do(func() {
		sql.Register("lm_trace", traceDriver{inner: moderncDriver()})
	})
	traceCtrLock.Lock()
	defer traceCtrLock.Unlock()
	traceCtr = &traceCounters{}

	db, err := sql.Open("lm_trace", ":memory:")
	must(err)
	defer db.Close()
	db.SetMaxOpenConns(1) // pin ONE conn so the counter sees every statement (incl. the tx conn)
	_, _ = db.Exec("PRAGMA foreign_keys = ON")
	for _, s := range a.Schema {
		_, err := db.Exec(s)
		must(err)
	}
	for _, s := range a.Seed {
		_, err := db.Exec(s)
		must(err)
	}
	// Reset AFTER seeding so seed inserts aren't counted.
	traceCtr = &traceCounters{}
	if impl == "sql" {
		runSQL(caseID, db)
	} else {
		runLM(a.Cases[caseID], db)
	}
	return traceCtr.queries, traceCtr.rows
}
