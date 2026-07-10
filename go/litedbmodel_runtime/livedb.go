// litedbmodel v2 SCP — live PostgreSQL / MySQL driver wiring (Go, WS7g #36).
//
// The Go runtime already executes through the standard database/sql surface (SQLDB / TxDB), so a
// real Postgres (pgx stdlib) and MySQL (go-sql-driver) connection plug into the SAME seam the
// SQLite conformance uses — the runtime is UNCHANGED. This file only supplies the connection
// openers plus the ONE dialect divergence a raw MySQL driver can't absorb:
//
//   - Postgres: pgx's database/sql driver natively binds `$N` (exactly what a `postgres`-tagged
//     bundle renders) and supports RETURNING — nothing to adapt.
//   - MySQL: go-sql-driver binds `?` natively (what a `mysql`-tagged bundle renders), but MySQL
//     8.0 has NO `RETURNING`. We register a THIN wrapping database/sql driver (`mysql-scp`) whose
//     connection intercepts `INSERT … RETURNING <cols>`: it strips RETURNING, runs the INSERT,
//     reads LAST_INSERT_ID(), and re-selects the requested columns by the AUTO_INCREMENT PK — the
//     dialect-behavior-by-convention the WS6 TS ScpDialect uses. Because it wraps at the driver
//     layer, the emulation is transparent to BOTH the read seam (*sql.DB) and the write-tx seam
//     (*sql.Tx) with no runtime.ts / write-runtime.ts change.

package litedbmodel_runtime

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"

	gomysql "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// DefaultPoolSize aligns the *sql.DB pool ceiling with the read plan's default concurrency (spec).
// The bc Go RunPlan fans out the independent sibling relations of a stage onto goroutines bounded by
// plan.Concurrency (default 16), each running through the SQLDB seam; sizing the pool to match lets
// all those concurrent siblings hold a real connection at once without queueing (#40). The write-tx
// runs on ONE connection (a single *sql.Tx), so the pool ceiling never affects write serialization.
const DefaultPoolSize = 16

// OpenPostgres opens a live Postgres via the pgx stdlib database/sql driver ($N native, RETURNING
// native). dsn e.g. "postgres://user:pass@host:port/db?sslmode=disable". The pool is sized to the
// default plan concurrency so parallel read-relation dispatch (bc#23) has connections to spend.
func OpenPostgres(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(DefaultPoolSize)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

// OpenMysql opens a live MySQL via the RETURNING-emulating "mysql-scp" driver. dsn e.g.
// "user:pass@tcp(host:port)/db?multiStatements=false". Pool sized to the default plan concurrency.
func OpenMysql(dsn string) (*sql.DB, error) {
	registerMysqlScp()
	db, err := sql.Open("mysql-scp", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(DefaultPoolSize)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

// ── The RETURNING-emulating MySQL driver wrapper ───────────────────────────────

var (
	mysqlScpOnce sync.Once
	// INSERT [IGNORE] INTO <table> ( … ) … RETURNING <cols>
	returningInsertRe = regexp.MustCompile(`(?is)^\s*INSERT\s+(?:IGNORE\s+)?INTO\s+([A-Za-z_][A-Za-z0-9_]*)\b.*\bRETURNING\s+(.+?)\s*$`)
	stripReturningRe  = regexp.MustCompile(`(?is)\s+RETURNING\s+.+$`)
	// The INSERT column list `INSERT [IGNORE] INTO <t> (c1, c2, …)` — for client-PK re-select.
	insertColsRe = regexp.MustCompile(`(?is)^\s*INSERT\s+(?:IGNORE\s+)?INTO\s+[A-Za-z_][A-Za-z0-9_]*\s*\(([^)]*)\)`)
	// The strip-before-execute PK hint (tx.ts mysqlPkHint): ` /*scp:pk=col1,col2;ai=<col|>*/`.
	pkHintRe = regexp.MustCompile(`(?is)\s*/\*scp:pk=([^;*]*);ai=([^*]*)\*/`)
)

func registerMysqlScp() {
	mysqlScpOnce.Do(func() {
		sql.Register("mysql-scp", scpMysqlDriver{base: gomysql.MySQLDriver{}})
	})
}

type scpMysqlDriver struct{ base driver.Driver }

func (d scpMysqlDriver) Open(name string) (driver.Conn, error) {
	c, err := d.base.Open(name)
	if err != nil {
		return nil, err
	}
	return &scpMysqlConn{base: c}, nil
}

// scpMysqlConn wraps a go-sql-driver connection, intercepting RETURNING queries. It forwards every
// other driver capability to the base connection.
type scpMysqlConn struct{ base driver.Conn }

func (c *scpMysqlConn) Prepare(query string) (driver.Stmt, error) { return c.base.Prepare(query) }
func (c *scpMysqlConn) Close() error                              { return c.base.Close() }
func (c *scpMysqlConn) Begin() (driver.Tx, error)                 { return c.base.Begin() } //nolint:staticcheck

func (c *scpMysqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if b, ok := c.base.(driver.ConnBeginTx); ok {
		return b.BeginTx(ctx, opts)
	}
	return c.base.Begin() //nolint:staticcheck
}

// QueryContext intercepts an INSERT … RETURNING: run the stripped INSERT, then re-select the
// inserted rows by the REAL primary key. The strip-before-execute PK hint (tx.ts mysqlPkHint)
// carries the PK columns + the AUTO_INCREMENT column, so the re-select keys off an AUTO_INCREMENT
// range (int identity) or the client-supplied PK values (UUID / composite) pulled from the bound
// INSERT params — NOT a hardcoded `WHERE id = ?` (which breaks for UUID / composite PKs). A
// non-RETURNING query forwards to the base driver.
func (c *scpMysqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if m := returningInsertRe.FindStringSubmatch(query); m != nil {
		table := m[1]
		cols := pkHintRe.ReplaceAllString(m[2], "") // RETURNING cols with the hint stripped
		pkCols, autoInc := parsePkHint(m[2])
		insertSQL := pkHintRe.ReplaceAllString(stripReturningRe.ReplaceAllString(query, ""), "")
		// go-sql-driver's ExecerContext returns driver.ErrSkip for a parameterized statement (it
		// wants the prepared-statement path), so run the stripped INSERT via a prepared statement.
		lastID, affected, err := c.execViaStmtWithAffected(ctx, insertSQL, args)
		if err != nil {
			return nil, err
		}
		whereSQL, whereArgs := returningReselectWhere(insertSQL, pkCols, autoInc, args, lastID, affected)
		sel := fmt.Sprintf("SELECT %s FROM %s WHERE %s", cols, table, whereSQL)
		return c.queryViaStmt(ctx, sel, whereArgs)
	}
	// A non-INSERT RETURNING (UPDATE/DELETE … RETURNING): MySQL has no native RETURNING and the
	// pre-image is gone, so v1 (`mysql.ts`) strips RETURNING, runs the write, and returns NO rows.
	// Byte-faithful: execute the stripped write, return an empty row set.
	if stripReturningRe.MatchString(query) {
		writeSQL := pkHintRe.ReplaceAllString(stripReturningRe.ReplaceAllString(query, ""), "")
		if _, _, err := c.execViaStmtWithAffected(ctx, writeSQL, args); err != nil {
			return nil, err
		}
		return &emptyRows{}, nil
	}
	if q, ok := c.base.(driver.QueryerContext); ok {
		rows, err := q.QueryContext(ctx, query, args)
		if err == driver.ErrSkip {
			return c.queryViaStmt(ctx, query, args)
		}
		return rows, err
	}
	return c.queryViaStmt(ctx, query, args)
}

// emptyRows is a zero-column, zero-row driver.Rows for a non-INSERT RETURNING that MySQL cannot
// return (the write's pre-image is gone — v1 `mysql.ts` returns no rows).
type emptyRows struct{}

func (*emptyRows) Columns() []string              { return []string{} }
func (*emptyRows) Close() error                   { return nil }
func (*emptyRows) Next(dest []driver.Value) error { return io.EOF }

// ExecContext forwards to the base driver (writes never carry RETURNING on the exec/tx path).
func (c *scpMysqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if e, ok := c.base.(driver.ExecerContext); ok {
		return e.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

// execViaStmtWithAffected prepares + executes a parameterized statement via the base driver's
// statement path (used for the RETURNING-emulation INSERT, which the base ExecerContext skips),
// returning the LAST_INSERT_ID and the affected-row count (for the AUTO_INCREMENT range re-select).
func (c *scpMysqlConn) execViaStmtWithAffected(ctx context.Context, query string, args []driver.NamedValue) (int64, int64, error) {
	stmt, err := c.prepareStmt(ctx, query)
	if err != nil {
		return 0, 0, err
	}
	defer stmt.Close()
	var res driver.Result
	if se, ok := stmt.(driver.StmtExecContext); ok {
		res, err = se.ExecContext(ctx, args)
	} else {
		res, err = stmt.Exec(namedToValues(args)) //nolint:staticcheck
	}
	if err != nil {
		return 0, 0, err
	}
	lastID, _ := res.LastInsertId()
	affected, aerr := res.RowsAffected()
	if aerr != nil || affected < 1 {
		affected = 1
	}
	return lastID, affected, nil
}

// parsePkHint extracts the PK columns + AUTO_INCREMENT column from the ` /*scp:pk=…;ai=…*/` hint
// (tx.ts mysqlPkHint). Absent hint → (nil, ""), the legacy `id`-keyed path.
func parsePkHint(returningCols string) ([]string, string) {
	hm := pkHintRe.FindStringSubmatch(returningCols)
	if hm == nil {
		return nil, ""
	}
	var cols []string
	for _, c := range strings.Split(hm[1], ",") {
		if t := strings.TrimSpace(c); t != "" {
			cols = append(cols, t)
		}
	}
	return cols, strings.TrimSpace(hm[2])
}

// returningReselectWhere builds the MySQL RETURNING re-select WHERE body (`?`) + its args:
//   - AUTO_INCREMENT single-column PK → an identity range over the affected rows (v1 semantics,
//     real column name).
//   - client-supplied PK (UUID / composite, ai == "") → the PK value(s) pulled from the bound
//     INSERT params by column position (single-row client-PK insert).
//   - no hint → `id = ?` on LAST_INSERT_ID (legacy auto-`id` fallback).
func returningReselectWhere(insertSQL string, pkCols []string, autoInc string, args []driver.NamedValue, lastID, affected int64) (string, []driver.NamedValue) {
	if len(pkCols) == 0 {
		return "id = ?", []driver.NamedValue{{Ordinal: 1, Value: lastID}}
	}
	if autoInc != "" && len(pkCols) == 1 && pkCols[0] == autoInc {
		return fmt.Sprintf("%s >= ? AND %s < ?", autoInc, autoInc),
			[]driver.NamedValue{{Ordinal: 1, Value: lastID}, {Ordinal: 2, Value: lastID + affected}}
	}
	cm := insertColsRe.FindStringSubmatch(insertSQL)
	insertCols := []string{}
	if cm != nil {
		for _, c := range strings.Split(cm[1], ",") {
			insertCols = append(insertCols, strings.TrimSpace(c))
		}
	}
	var conds []string
	var whereArgs []driver.NamedValue
	for _, pk := range pkCols {
		idx := -1
		for i, ic := range insertCols {
			if ic == pk {
				idx = i
				break
			}
		}
		if idx < 0 || idx >= len(args) {
			// PK column not in the INSERT list (should not happen for a client-PK insert) — fall
			// back to the identity path to avoid a silent wrong re-select.
			return "id = ?", []driver.NamedValue{{Ordinal: 1, Value: lastID}}
		}
		conds = append(conds, fmt.Sprintf("%s = ?", pk))
		whereArgs = append(whereArgs, driver.NamedValue{Ordinal: len(whereArgs) + 1, Value: args[idx].Value})
	}
	return strings.Join(conds, " AND "), whereArgs
}

// queryViaStmt prepares + queries a parameterized statement via the base driver's statement path.
func (c *scpMysqlConn) queryViaStmt(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := c.prepareStmt(ctx, query)
	if err != nil {
		return nil, err
	}
	// The statement must outlive the returned Rows; wrap so Close() releases both.
	if sq, ok := stmt.(driver.StmtQueryContext); ok {
		rows, err := sq.QueryContext(ctx, args)
		if err != nil {
			stmt.Close()
			return nil, err
		}
		return &stmtRows{Rows: rows, stmt: stmt}, nil
	}
	rows, err := stmt.Query(namedToValues(args)) //nolint:staticcheck
	if err != nil {
		stmt.Close()
		return nil, err
	}
	return &stmtRows{Rows: rows, stmt: stmt}, nil
}

func (c *scpMysqlConn) prepareStmt(ctx context.Context, query string) (driver.Stmt, error) {
	if pc, ok := c.base.(driver.ConnPrepareContext); ok {
		return pc.PrepareContext(ctx, query)
	}
	return c.base.Prepare(query)
}

func namedToValues(args []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

// stmtRows keeps the owning prepared statement alive for the lifetime of the rows, closing both.
type stmtRows struct {
	driver.Rows
	stmt driver.Stmt
}

func (r *stmtRows) Close() error {
	err := r.Rows.Close()
	if cerr := r.stmt.Close(); err == nil {
		err = cerr
	}
	return err
}

// Ensure the base's resource cleanup is reachable (defensive; go-sql-driver implements io.Closer).
var _ io.Closer = (*scpMysqlConn)(nil)
