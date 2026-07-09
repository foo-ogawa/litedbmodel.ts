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
	"sync"

	gomysql "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// OpenPostgres opens a live Postgres via the pgx stdlib database/sql driver ($N native, RETURNING
// native). dsn e.g. "postgres://user:pass@host:port/db?sslmode=disable".
func OpenPostgres(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

// OpenMysql opens a live MySQL via the RETURNING-emulating "mysql-scp" driver. dsn e.g.
// "user:pass@tcp(host:port)/db?multiStatements=false".
func OpenMysql(dsn string) (*sql.DB, error) {
	registerMysqlScp()
	db, err := sql.Open("mysql-scp", dsn)
	if err != nil {
		return nil, err
	}
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
// inserted row by LAST_INSERT_ID(). A non-RETURNING query forwards to the base driver.
func (c *scpMysqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if m := returningInsertRe.FindStringSubmatch(query); m != nil {
		table, cols := m[1], m[2]
		insertSQL := stripReturningRe.ReplaceAllString(query, "")
		// go-sql-driver's ExecerContext returns driver.ErrSkip for a parameterized statement (it
		// wants the prepared-statement path), so run the stripped INSERT via a prepared statement.
		lastID, err := c.execViaStmt(ctx, insertSQL, args)
		if err != nil {
			return nil, err
		}
		sel := fmt.Sprintf("SELECT %s FROM %s WHERE id = ?", cols, table)
		return c.queryViaStmt(ctx, sel, []driver.NamedValue{{Ordinal: 1, Value: lastID}})
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

// ExecContext forwards to the base driver (writes never carry RETURNING on the exec/tx path).
func (c *scpMysqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if e, ok := c.base.(driver.ExecerContext); ok {
		return e.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

// execViaStmt prepares + executes a parameterized statement via the base driver's statement path
// (used for the RETURNING-emulation INSERT, which the base ExecerContext skips), returning the
// LAST_INSERT_ID.
func (c *scpMysqlConn) execViaStmt(ctx context.Context, query string, args []driver.NamedValue) (int64, error) {
	stmt, err := c.prepareStmt(ctx, query)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	var res driver.Result
	if se, ok := stmt.(driver.StmtExecContext); ok {
		res, err = se.ExecContext(ctx, args)
	} else {
		res, err = stmt.Exec(namedToValues(args)) //nolint:staticcheck
	}
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
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
