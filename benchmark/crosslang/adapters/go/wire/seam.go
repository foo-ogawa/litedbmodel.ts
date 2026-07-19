package wire

// The exec SEAM (go twin of the rust cell's seam.rs): run baked SQL over database/sql and MATERIALIZE
// each result row into the by-name WIRE the generated modules de-box. OP-AGNOSTIC; the only per-dialect
// concerns are the driver bind (scalar vs array), the batch/relation array shape (v2 single-JSON vs v1
// UNNEST/= ANY), and — on mysql (no native RETURNING) — the generic strip-marker + re-select mechanic.

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

type DB struct {
	db      *sql.DB
	Dialect string
	cache   map[string]*sql.Stmt
}

func driverFor(dialect string) string {
	switch dialect {
	case "postgres":
		return "postgres"
	case "mysql":
		return "mysql"
	default:
		return "sqlite3"
	}
}

// mysqlDSN converts the canonical `mysql://user:pass@host:port/db` URL (the ONE target form verify-cells
// passes, shared with the rust cell) into the go-sql-driver DSN `user:pass@tcp(host:port)/db`.
func mysqlDSN(url string) string {
	s := strings.TrimPrefix(url, "mysql://")
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return s
	}
	cred, rest := s[:at], s[at+1:]
	slash := strings.Index(rest, "/")
	hostport, db := rest, ""
	if slash >= 0 {
		hostport, db = rest[:slash], rest[slash+1:]
	}
	return cred + "@tcp(" + hostport + ")/" + db
}

// Open a single-connection DB for the dialect (the driver is blank-imported by package main; database/sql
// resolves it from the global registry). Single conn (SetMaxOpenConns(1)) → the stmt cache is single-threaded.
func Open(dialect, target string) *DB {
	if dialect == "mysql" {
		target = mysqlDSN(target)
	}
	if dialect == "postgres" && !strings.Contains(target, "sslmode") {
		target += " sslmode=disable" // docker pg has no SSL (the rust cell's postgres crate uses NoTls)
	}
	d, err := sql.Open(driverFor(dialect), target)
	if err != nil {
		panic(err)
	}
	d.SetMaxOpenConns(1)
	if err := d.Ping(); err != nil {
		panic(err)
	}
	return &DB{db: d, Dialect: dialect, cache: make(map[string]*sql.Stmt)}
}
func (s *DB) Close() { s.db.Close() }
func (s *DB) Raw() *sql.DB { return s.db }

func (s *DB) stmt(query string) (*sql.Stmt, error) {
	if st, ok := s.cache[query]; ok {
		return st, nil
	}
	st, err := s.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	s.cache[query] = st
	return st, nil
}

// ── row materialization (driver-agnostic, by column name) ──
func cellOf(dbType string, v any) Cell {
	if v == nil {
		return CellNull()
	}
	switch strings.ToUpper(dbType) {
	case "INT", "INTEGER", "BIGINT", "INT4", "INT8", "INT2", "SMALLINT", "TINYINT", "MEDIUMINT", "SERIAL":
		return CellInt(toInt64(v))
	case "BOOL", "BOOLEAN":
		return CellBool(toBool(v))
	case "REAL", "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC", "FLOAT8", "FLOAT4", "DOUBLE PRECISION":
		return CellReal(toFloat(v))
	case "TIMESTAMP", "TIMESTAMPTZ", "DATETIME", "DATE":
		return CellText(toTimeStr(v))
	default: // TEXT / VARCHAR / CHAR / BPCHAR / …
		return CellText(toStr(v))
	}
}
func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	case bool:
		if x {
			return 1
		}
		return 0
	case []byte:
		n, _ := strconv.ParseInt(string(x), 10, 64)
		return n
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		return 0
	}
}
func toBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int64:
		return x != 0
	case []byte:
		return len(x) == 1 && x[0] == 1 || string(x) == "1" || strings.EqualFold(string(x), "true")
	case string:
		return x == "1" || strings.EqualFold(x, "true")
	default:
		return false
	}
}
func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case []byte:
		f, _ := strconv.ParseFloat(string(x), 64)
		return f
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	default:
		return 0
	}
}
func toStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case time.Time:
		return x.Format("2006-01-02 15:04:05")
	default:
		return ""
	}
}
func toTimeStr(v any) string {
	switch x := v.(type) {
	case time.Time:
		return x.Format("2006-01-02 15:04:05")
	case []byte:
		return string(x)
	case string:
		return x
	default:
		return ""
	}
}

func (s *DB) materialize(query string, params []any) ([]RowData, error) {
	st, err := s.stmt(query)
	if err != nil {
		return nil, err
	}
	rows, err := st.Query(params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	cts, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	dbTypes := make([]string, len(cts))
	for i, ct := range cts {
		dbTypes[i] = ct.DatabaseTypeName()
	}
	n := len(cols)
	out := make([]RowData, 0, 8)
	for rows.Next() {
		dest := make([]any, n)
		ptrs := make([]any, n)
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		cells := make([]Cell, n)
		names := make([]string, n)
		for i := range cols {
			names[i] = cols[i]
			cells[i] = cellOf(dbTypes[i], dest[i])
		}
		out = append(out, RowData{names: names, cells: cells})
	}
	return out, rows.Err()
}

// Execute — a raw non-returning write returning (changes, lastInsertId) for the SDK baseline's tx bodies
// (DELETE / INSERT post / UPDATE) that need no wire result, only success. Thin over the SSoT `exec`.
func Execute(s *DB, query string, params []any) (int64, int64, error) { return s.exec(query, params) }

func (s *DB) exec(query string, params []any) (int64, int64, error) {
	st, err := s.stmt(query)
	if err != nil {
		return 0, 0, err
	}
	res, err := st.Exec(params...)
	if err != nil {
		return 0, 0, err
	}
	ch, _ := res.RowsAffected()
	li, _ := res.LastInsertId()
	return ch, li, nil
}

// ── mysql RETURNING emulation marker (dialect-independent: emitted into the SQL ONLY for mysql) ──
const markOpen = " /*scp-reselect: "

type reselect struct {
	writeSQL, selectSQL string
	binds               []string
}

func parseReselect(sqlText string) (reselect, bool) {
	open := strings.Index(sqlText, markOpen)
	if open < 0 {
		return reselect{}, false
	}
	writeSQL := sqlText[:open]
	rest := sqlText[open+len(markOpen):]
	close := strings.LastIndex(rest, "*/")
	if close < 0 {
		return reselect{}, false
	}
	body := strings.TrimRight(rest[:close], " ")
	sep := strings.Index(body, " ::binds:: ")
	if sep < 0 {
		return reselect{}, false
	}
	selectSQL := body[:sep]
	var binds []string
	for _, t := range strings.Split(body[sep+len(" ::binds:: "):], ",") {
		if t != "" {
			binds = append(binds, t)
		}
	}
	return reselect{writeSQL: writeSQL, selectSQL: selectSQL, binds: binds}, true
}
func reselectParams(binds []string, writeParams []any, changes, lastID int64) []any {
	out := make([]any, len(binds))
	for i, t := range binds {
		switch {
		case t == "L":
			out[i] = lastID
		case t == "H":
			out[i] = lastID + changes
		case strings.HasPrefix(t, "p"):
			idx, _ := strconv.Atoi(t[1:])
			out[i] = writeParams[idx]
		default:
			panic("unknown reselect bind token '" + t + "'")
		}
	}
	return out
}

// QueryRows — a read / RETURNING write, MATERIALIZED to rows. On mysql a RETURNING write carries the
// re-select marker: strip it, run the write, re-select by the baked SELECT (the one strip+reselect
// mechanic; pg/sqlite never carry a marker so this is a no-op branch). The SSoT read path: the native
// handler (via Query) AND the SDK baseline both consume it.
func QueryRows(s *DB, sqlText string, params []any) ([]RowData, error) {
	if r, ok := parseReselect(sqlText); ok {
		ch, li, err := s.exec(r.writeSQL, params)
		if err != nil {
			return nil, err
		}
		return s.materialize(r.selectSQL, reselectParams(r.binds, params, ch, li))
	}
	return s.materialize(sqlText, params)
}

// Query — the native handler's read/returning-write entry: QueryRows wrapped as a WireValue the
// generated module de-boxes.
func Query(s *DB, sqlText string, params []any) (WireValue, error) {
	rows, err := QueryRows(s, sqlText, params)
	if err != nil {
		return Result{}, err
	}
	return Result{Rows: rows}, nil
}

// ExecuteNull — a NON-RETURNING single write (v1 default): run it (mutating) → the summary wire (the cell
// emits v1-faithful null; the summary is discarded there). Defensive marker strip.
func ExecuteNull(s *DB, sqlText string, params []any) (WireValue, error) {
	clean := sqlText
	if r, ok := parseReselect(sqlText); ok {
		clean = r.writeSQL
	}
	ch, li, err := s.exec(clean, params)
	if err != nil {
		return Result{}, err
	}
	return Summary(ch, li), nil
}

// A batch column array (typed) — the v2 JSON zip (sqlite/mysql) vs the v1 UNNEST native array (pg).
type ColArray struct {
	Ints  []int64
	Texts []string
	IsInt bool
}

func Ints(v []int64) ColArray  { return ColArray{Ints: v, IsInt: true} }
func Texts(v []string) ColArray { return ColArray{Texts: v} }
func (a ColArray) len() int {
	if a.IsInt {
		return len(a.Ints)
	}
	return len(a.Texts)
}
func (a ColArray) jsonCell(i int) string {
	if a.IsInt {
		return strconv.FormatInt(a.Ints[i], 10)
	}
	return JsonStr(a.Texts[i])
}
func (a ColArray) pgArray() any {
	if a.IsInt {
		return pq.Array(a.Ints)
	}
	return pq.Array(a.Texts)
}

// ExecuteBatchNull — a NON-RETURNING batch write (v1: createMany/upsertMany/updateMany → null): ONE
// statement for N records. sqlite/mysql zip the columns into `[{col:val,…},…]` JSON bound to every `?`;
// pg binds each column as a native `<elem>[]` (the baked `UNNEST($1::T[],…)`).
func ExecuteBatchNull(s *DB, sqlText string, columns []string, arrays []ColArray) (WireValue, error) {
	var params []any
	if s.Dialect == "postgres" {
		params = make([]any, len(arrays))
		for i, a := range arrays {
			params[i] = a.pgArray()
		}
	} else {
		n := 0
		if len(arrays) > 0 {
			n = arrays[0].len()
		}
		objs := make([]string, n)
		for i := 0; i < n; i++ {
			fields := make([]string, len(columns))
			for j, c := range columns {
				fields[j] = JsonStr(c) + ":" + arrays[j].jsonCell(i)
			}
			objs[i] = "{" + strings.Join(fields, ",") + "}"
		}
		jsonRecs := "[" + strings.Join(objs, ",") + "]"
		nq := strings.Count(sqlText, "?")
		params = make([]any, nq)
		for i := range params {
			params[i] = jsonRecs
		}
	}
	ch, li, err := s.exec(sqlText, params)
	if err != nil {
		return Result{}, err
	}
	return Summary(ch, li), nil
}

// QueryBatchedRelation — the generic BATCHED-RELATION exec (N+1-avoided): dedup the parent keys, run the
// ONE child query, group child ROWS by their target key, align per-parent, and wrap each parent's child
// list in a Result (the generated relation handler returns []WireValue, one per parent).
// sqlite/mysql bind the deduped keys as ONE single-JSON param (`encodeJSON`); pg binds native arrays
// (`pgArrays`) into `= ANY(?::int[])` / `UNNEST(?::int[],…)` (renumber `?`→`$N`, resolve the cast marker).
func QueryBatchedRelation[K comparable](
	s *DB,
	sqlText string,
	itemKeys []K,
	encodeJSON func([]K) string,
	pgArrays func([]K) []any,
	childKey func(RowData) K,
) ([]WireValue, error) {
	seen := make(map[K]bool, len(itemKeys))
	distinct := make([]K, 0, len(itemKeys))
	for _, k := range itemKeys {
		if !seen[k] {
			seen[k] = true
			distinct = append(distinct, k)
		}
	}
	var childRows []RowData
	var err error
	if s.Dialect == "postgres" {
		resolved := renumber(strings.ReplaceAll(sqlText, "@@PG_ARRAY_CAST@@", "int[]"))
		childRows, err = s.materialize(resolved, pgArrays(distinct))
	} else {
		childRows, err = s.materialize(sqlText, []any{encodeJSON(distinct)})
	}
	if err != nil {
		return nil, err
	}
	groups := make(map[K][]RowData, len(distinct))
	for _, c := range childRows {
		k := childKey(c)
		groups[k] = append(groups[k], c)
	}
	out := make([]WireValue, len(itemKeys))
	for i, k := range itemKeys {
		out[i] = Result{Rows: groups[k]}
	}
	return out, nil
}

// QueryBatchedRelationGrouped — the CHAINED-relation exec (E4/#119 level-3): a batched relation off a
// batched relation. Each over-element is a per-grandparent list of parent rows, so itemKeyLists[e] are
// that element's child-parent keys (e.g. all post ids of one user's posts). FLATTEN every list into ONE
// batched child query (N+1-free per level), group children by target key, then re-align PER over-element
// by concatenating the children of each of its keys (in the element's key order). One Result per element.
func QueryBatchedRelationGrouped[K comparable](
	s *DB,
	sqlText string,
	itemKeyLists [][]K,
	encodeJSON func([]K) string,
	pgArrays func([]K) []any,
	childKey func(RowData) K,
) ([]WireValue, error) {
	seen := make(map[K]bool)
	distinct := make([]K, 0)
	for _, ks := range itemKeyLists {
		for _, k := range ks {
			if !seen[k] {
				seen[k] = true
				distinct = append(distinct, k)
			}
		}
	}
	var childRows []RowData
	var err error
	if s.Dialect == "postgres" {
		resolved := renumber(strings.ReplaceAll(sqlText, "@@PG_ARRAY_CAST@@", "int[]"))
		childRows, err = s.materialize(resolved, pgArrays(distinct))
	} else {
		childRows, err = s.materialize(sqlText, []any{encodeJSON(distinct)})
	}
	if err != nil {
		return nil, err
	}
	groups := make(map[K][]RowData, len(distinct))
	for _, c := range childRows {
		k := childKey(c)
		groups[k] = append(groups[k], c)
	}
	out := make([]WireValue, len(itemKeyLists))
	for i, ks := range itemKeyLists {
		rows := make([]RowData, 0)
		for _, k := range ks {
			rows = append(rows, groups[k]...)
		}
		out[i] = Result{Rows: rows}
	}
	return out, nil
}

// renumber rewrites positional `?` → pg `$1..$N` (the relation child fragments carry `?`; baked
// batch/main pg SQL already emits `$N`).
func renumber(sqlText string) string {
	var b strings.Builder
	n := 0
	for _, r := range sqlText {
		if r == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Transaction — the tx ENVELOPE: BEGIN, run the generated chain runner (its handlers run their statements
// on the single pinned connection), COMMIT on Ok / ROLLBACK on error. Returns whether it committed.
func Transaction(s *DB, body func() error) bool {
	if _, err := s.db.Exec("BEGIN"); err != nil {
		return false
	}
	if err := body(); err != nil {
		_, _ = s.db.Exec("ROLLBACK")
		return false
	}
	_, err := s.db.Exec("COMMIT")
	return err == nil
}
