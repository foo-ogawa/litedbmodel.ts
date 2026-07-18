// The go-native EXEC SEAM — the ENTIRE runtime a baked-SQL op needs (the go twin of
// rust/e1_native_proof/src/seam.rs). It is OP-AGNOSTIC: it knows only a SQL string, an ordered param
// list, and (for a read) how to decode one row. Everything else — the SQL, the params, the projection
// — is BAKED as a native literal in the generated `behaviors` module. No IR walk, no dispatch, and (per
// the bench's hard rule) NO encoding/json in the cell's dependency graph: the batch/relation JSON is
// hand-rolled, exactly as the rust seam hand-rolls it.
package main

import (
	"database/sql"
	"strconv"
	"strings"
)

// Query — the generic READ exec: run sql with params, decode each row via `decode`.
func Query[T any](db *sql.DB, query string, params []any, decode func(*sql.Rows) (T, error)) ([]T, error) {
	rows, err := db.Query(query, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]T, 0, 8)
	for rows.Next() {
		v, derr := decode(rows)
		if derr != nil {
			return nil, derr
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// Execute — the generic non-returning WRITE exec: run sql, return (changes, lastInsertId).
func Execute(db *sql.DB, query string, params []any) (int64, int64, error) {
	res, err := db.Exec(query, params...)
	if err != nil {
		return 0, 0, err
	}
	ch, _ := res.RowsAffected()
	li, _ := res.LastInsertId()
	return ch, li, nil
}

// jsonStr — hand-rolled JSON string escaping (NO encoding/json: the codegen cell's dependency graph
// stays JSON-crate-free, mirroring the rust seam's json_str). Byte-equal to JSON.stringify for a string.
func jsonStr(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString("\\\"")
		case '\\':
			b.WriteString("\\\\")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		default:
			if r < 0x20 {
				b.WriteString("\\u")
				h := strconv.FormatInt(int64(r), 16)
				for len(h) < 4 {
					h = "0" + h
				}
				b.WriteString(h)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

// QueryBatchedRelation — the generic BATCHED-RELATION exec (N+1-avoided): collect the DISTINCT parent
// keys, run the ONE baked child query binding the deduped keys as a flat JSON array (the baked
// `json_each(?)` IN-list), group children by their target key, and return the per-parent lists ALIGNED
// to itemKeys. The go twin of the rust seam's query_batched_relation.
func QueryBatchedRelation[T any](db *sql.DB, query string, itemKeys []int64, decode func(*sql.Rows) (T, error), childKey func(T) int64) ([][]T, error) {
	seen := make(map[int64]bool, len(itemKeys))
	distinct := make([]int64, 0, len(itemKeys))
	for _, k := range itemKeys {
		if !seen[k] {
			seen[k] = true
			distinct = append(distinct, k)
		}
	}
	parts := make([]string, len(distinct))
	for i, k := range distinct {
		parts[i] = strconv.FormatInt(k, 10)
	}
	jsonArr := "[" + strings.Join(parts, ",") + "]" // <-- ONE query for all parents
	children, err := Query(db, query, []any{jsonArr}, decode)
	if err != nil {
		return nil, err
	}
	groups := make(map[int64][]T, len(distinct))
	for _, c := range children {
		k := childKey(c)
		groups[k] = append(groups[k], c)
	}
	out := make([][]T, len(itemKeys))
	for i, k := range itemKeys {
		out[i] = groups[k] // nil (empty) when a parent has no children
	}
	return out, nil
}

// QueryBatchWrite — the generic BATCH-WRITE exec (ONE json_each INSERT for N records): zip the parallel
// PRE-ENCODED columns into the `[{col:val,…},…]` JSON the baked json_each(?) expands, bind it to EVERY
// `?`, and run ONCE. `cells[j][i]` is the already-JSON-encoded value of column j, row i (a string column
// is pre-quoted by the caller; the seam owns only the zip + bind). The go twin of query_batch_write.
func QueryBatchWrite[T any](db *sql.DB, query string, columns []string, cells [][]string, decode func(*sql.Rows) (T, error)) ([]T, error) {
	n := 0
	if len(cells) > 0 {
		n = len(cells[0])
	}
	objs := make([]string, n)
	for i := 0; i < n; i++ {
		fields := make([]string, len(columns))
		for j, c := range columns {
			fields[j] = jsonStr(c) + ":" + cells[j][i]
		}
		objs[i] = "{" + strings.Join(fields, ",") + "}"
	}
	jsonRecs := "[" + strings.Join(objs, ",") + "]"
	nq := strings.Count(query, "?")
	params := make([]any, nq)
	for i := range params {
		params[i] = jsonRecs
	}
	return Query(db, query, params, decode)
}
