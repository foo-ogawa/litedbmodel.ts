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
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// SQLDB is the minimal database/sql surface the runtime needs (a *sql.DB or *sql.Tx satisfies it).
type SQLDB interface {
	Query(query string, args ...any) (*sql.Rows, error)
	Exec(query string, args ...any) (sql.Result, error)
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
		b, _ := json.Marshal(t)
		return string(b)
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
			kb, _ := json.Marshal(k)
			parts = append(parts, string(kb)+":"+jsStringify(t.Vals[k]))
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
