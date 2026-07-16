// litedbmodel v2 SCP — SQL driver seam (database/sql).
//
// The runtime executes rendered SQL through the standard database/sql surface, so the SAME
// runtime plugs into any driver (the conformance bar uses an in-proc pure-Go SQLite —
// modernc.org/sqlite — the sanctioned in-proc substitute for a docker integration DB; a
// pgx/mysql driver plugs into the same seam later, spec §10 language-axis + the deferred live
// PG/MySQL cross-language pass).
//
// Value marshalling at the boundary mirrors the TS reference:
//   - a rendered param (bc Value: int64/float64/string/bool/nil/[]Value/*Obj) is converted to a
//     driver-bindable arg (toDriverParam); an emit `{obj:…}` payload is JSON-serialized to a text
//     column (write-runtime.ts toDriverParam), preserving bc key order.
//   - a result row column is read back as a bc Value (int64 for integers, float64, string, []byte
//     → string, bool, nil) so the assembled result encodes identically to better-sqlite3's rows.

package litedbmodel_runtime

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// jsonEscapeString writes a JSON string literal NATIVELY (JS JSON.stringify form) — no encoding/json.
// The exec path (IN-list JSON params, emit payload text) must carry no JSON library, so string/key
// escaping is hand-written: `"`/`\` and the C0 control chars escape; unicode is left raw (matching
// JSON.stringify's default, byte-true to the TS reference + the codegen cell's jsonEscapeInto).
func jsonEscapeString(s string) string {
	var sb strings.Builder
	sb.Grow(len(s) + 2)
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		case '\b':
			sb.WriteString(`\b`)
		case '\f':
			sb.WriteString(`\f`)
		default:
			if r < 0x20 {
				sb.WriteString(`\u`)
				h := strconv.FormatInt(int64(r), 16)
				for len(h) < 4 {
					h = "0" + h
				}
				sb.WriteString(h)
			} else {
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
	return sb.String()
}

// SQLDB is the minimal database/sql surface the runtime needs (a *sql.DB or *sql.Tx satisfies it).
type SQLDB interface {
	Query(query string, args ...any) (*sql.Rows, error)
	Exec(query string, args ...any) (sql.Result, error)
}

// connSQLDB adapts an OWNED *sql.Conn to the [SQLDB] surface (Phase D / #94 tx restructure): every
// statement — including the runtime's OWN BEGIN/COMMIT/ROLLBACK/SET tx-control — runs on the SAME one
// owned pooled connection via the connection-bound ExecContext/QueryContext. This is what lets the
// runtime issue tx-control as REAL SQL strings THROUGH the seam (middleware-visible), on the pinned
// connection, without a *sql.Tx handle (whose BEGIN/Commit/Rollback are opaque method calls the seam
// can't observe). A *sql.Conn is exactly ONE pooled connection held for its lifetime — the same
// ownership guarantee a *sql.Tx gives — so concurrent transactions each own a DISTINCT connection.
type connSQLDB struct {
	conn *sql.Conn
	ctx  context.Context //nolint:containedctx // the owned-conn seam rides the tx's Go ctx (§3)
}

func (c connSQLDB) Query(query string, args ...any) (*sql.Rows, error) {
	return c.conn.QueryContext(c.ctx, query, args...)
}

func (c connSQLDB) Exec(query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(c.ctx, query, args...)
}

// toDriverParam converts a rendered bc Value to a driver-bindable arg (render-path parity with
// runtime.ts / write-runtime.ts toDriverParam). Scalars bind directly (database/sql accepts
// int64/float64/string/bool/nil). An object value (an emit payload) is JSON-serialized to a text
// column, matching TS's `JSON.stringify` (plain numbers, key insertion order preserved). An array
// (IN-list element) never reaches here as a whole (elements are flattened at render).
func toDriverParam(v bc.Value) any {
	switch t := v.(type) {
	case *bc.Obj:
		return jsStringify(t) // JSON text for the outbox payload column (JS JSON.stringify parity)
	case float64:
		// A rendered whole number arrives as float64 (toRenderParam collapses a bc int64 to a JS
		// number). go-sql-driver's binary prepared-statement protocol sends a float64 as DOUBLE,
		// which MySQL rejects for an integer slot such as `LIMIT ?` (Error 1210). Bind an integral
		// value as int64 so it lands in the integer slot; both drivers coerce int↔numeric otherwise.
		if t == math.Trunc(t) && !math.IsInf(t, 0) {
			return int64(t)
		}
		return v
	default:
		return v
	}
}

// jsStringify serializes a bc Value the way TS's `JSON.stringify` does: numbers (int64 or float64)
// print as plain JSON numbers (no `$bigint`/`{float}` tag — this is the DB-payload boundary, not
// the conformance corpus), a whole float prints as an integer, object key order is preserved.
func jsStringify(v bc.Value) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		if t {
			return "true"
		}
		return "false"
	case string:
		return jsonEscapeString(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return encodeFloat(t)
	case []bc.Value:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = jsStringify(e)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case *bc.Obj:
		parts := make([]string, 0, t.Len())
		for _, k := range t.Keys {
			parts = append(parts, jsonEscapeString(k)+":"+jsStringify(t.Vals[k]))
		}
		return "{" + strings.Join(parts, ",") + "}"
	default:
		return "null"
	}
}

// scanValue converts one scanned column (database/sql's dynamic type) to a bc Value matching the
// TS row encoding. better-sqlite3 returns integer columns as JS numbers (not bigint), so an
// integer column becomes a float64 here (a JS number) — it encodes as a plain JSON number, byte-
// true to the corpus's DB-row encoding. Text/[]byte → string, real → float64, bool → bool, NULL →
// nil. (The bc-int / `$bigint` encoding is reserved for render-path arithmetic values, not rows.)
// ScanConformanceValue is the exported column-scan conversion (used by the conformance vector
// runner's post-tx DB-state assertions, which read raw rows outside the runtime handler path).
func ScanConformanceValue(v any) bc.Value { return scanValue(v) }

func scanValue(v any) bc.Value {
	switch t := v.(type) {
	case nil:
		return nil
	case int64:
		return float64(t) // JS number (better-sqlite3 integer column → plain JSON number)
	case float64:
		return t
	case bool:
		return t
	case []byte:
		return string(t)
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}

// queryRows runs a SELECT/RETURNING statement and returns the rows as ordered bc objects (column
// order preserved via *bc.Obj), or a mapped SqlFailure.
func queryRows(db SQLDB, query string, args []any) ([]bc.Value, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, mapSqliteError(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, mapSqliteError(err)
	}
	var out []bc.Value
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, mapSqliteError(err)
		}
		obj := bc.NewObj()
		for i, c := range cols {
			obj.Set(c, scanValue(raw[i]))
		}
		out = append(out, obj)
	}
	if err := rows.Err(); err != nil {
		return nil, mapSqliteError(err)
	}
	if out == nil {
		out = []bc.Value{}
	}
	return out, nil
}

// execWrite runs a non-returning write and returns (rowsAffected, lastInsertRowid), or a mapped
// SqlFailure. Mirrors better-sqlite3's `run` → {changes, lastInsertRowid}.
func execWrite(db SQLDB, query string, args []any) (changes int64, lastInsert int64, err error) {
	res, e := db.Exec(query, args...)
	if e != nil {
		return 0, 0, mapSqliteError(e)
	}
	changes, _ = res.RowsAffected()
	lastInsert, _ = res.LastInsertId()
	return changes, lastInsert, nil
}
