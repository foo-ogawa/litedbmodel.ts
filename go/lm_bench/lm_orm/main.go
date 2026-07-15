// Command lm_orm — the ORM-plan EXECUTOR + live smoke — Go (epic #63).
//
// Port of the PROVEN TS reference (benchmark/crosslang/orm-exec-ts.ts + orm-smoke.ts). Loads the
// committed language-neutral artifact benchmark/crosslang/generated/orm-plan.json and executes ALL
// 19 ORM ops × {sqlite, mysql, postgres} through the SHIPPED litedbmodel_runtime driver seam:
//   - sqlite   : modernc.org/sqlite (PURE-GO, no cgo) via database/sql
//   - postgres : rt.OpenPostgres (pgx stdlib database/sql — $N + RETURNING native)
//   - mysql    : rt.OpenMysql (go-sql-driver/mysql, RETURNING-emulating "mysql-scp" wrapper)
//
// binding the BAKED per-dialect SQL from the artifact per the bindKind protocol (NO SQL gen here).
//
// This uses database/sql DIRECTLY through the shipped driver handles (the statement-level seam the
// spec calls for), so the SAME live drivers the ir bench cell uses execute the 19 ops.
//
// Spawn convention (harness registry): the built binary
//
//	go/lm_bench/lm_orm  [--smoke]
//
// `--smoke` runs the 57-cell matrix and exits; without it, it speaks the NDJSON
// run/throughput/cost/rss/shutdown protocol over stdin/stdout (case=<opId>, dialect=<dialect>).
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	rt "github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime"
	_ "modernc.org/sqlite" // PURE-GO sqlite driver (registered as "sqlite")
)

const pgSchemaName = "scp_go_bench"
const mysqlDBName = "scp_go_bench"

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ── {{SEQ}} substitution + numeric coercion ────────────────────────────────────
var seqCounter int64

func nextSeq() int64 {
	v := seqCounter
	seqCounter++
	return v
}

// substOne replaces {{SEQ}} in string params (recursing into batch-array params) and coerces
// JSON float64 whole numbers → int64 (MySQL rejects a quoted/float LIMIT; ids must bind as ints).
func substOne(p any, seq int64) any {
	switch v := p.(type) {
	case string:
		if strings.Contains(v, "{{SEQ}}") {
			return strings.ReplaceAll(v, "{{SEQ}}", strconv.FormatInt(seq, 10))
		}
		return v
	case float64:
		if v == float64(int64(v)) {
			return int64(v)
		}
		return v
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = substOne(e, seq)
		}
		return out
	default:
		return v
	}
}

func substParams(params []any, seq int64) []any {
	out := make([]any, len(params))
	for i, p := range params {
		out[i] = substOne(p, seq)
	}
	return out
}

func stripReturning(sql string) string {
	lower := strings.ToLower(sql)
	if at := strings.LastIndex(lower, " returning "); at >= 0 {
		return sql[:at]
	}
	return sql
}
func hasReturning(sql string) bool {
	return strings.Contains(strings.ToLower(sql), " returning ")
}

// toInt64Slice converts a []any of numbers (parent keys) into a typed []int64 (pgx binds a Go
// slice as a PG array for `= ANY($1)` / UNNEST). Keys in this bench are always integer ids.
func toInt64Slice(vals []any) []int64 {
	out := make([]int64, len(vals))
	for i, v := range vals {
		switch n := v.(type) {
		case int64:
			out[i] = n
		case float64:
			out[i] = int64(n)
		default:
			out[i], _ = strconv.ParseInt(fmt.Sprint(v), 10, 64)
		}
	}
	return out
}

// keyString stringifies a scalar cell for distinct-key dedup.
func keyString(v any) (string, bool) {
	if v == nil {
		return "", false
	}
	return fmt.Sprint(v), true
}

// ── relation bind protocol (mirror bindRelation in orm-exec-ts.ts) ─────────────
type relBind struct {
	sql    string
	params []any
}

func bindRelation(stage map[string]any, parents []map[string]any) *relBind {
	kind := stage["bindKind"].(string)
	sqlText := stage["sql"].(string)
	if single, ok := stage["single"].(map[string]any); ok && single != nil {
		pk := single["parentKey"].(string)
		seen := map[string]bool{}
		var keys []any
		for _, r := range parents {
			v := r[pk]
			if s, ok := keyString(v); ok && !seen[s] {
				seen[s] = true
				keys = append(keys, v)
			}
		}
		if len(keys) == 0 {
			return nil
		}
		if kind == "pgArraySingle" {
			return &relBind{sql: sqlText, params: []any{toInt64Slice(keys)}}
		}
		// jsonParam (sqlite/mysql): ONE param = JSON string of the distinct keys.
		b, _ := json.Marshal(keys)
		return &relBind{sql: sqlText, params: []any{string(b)}}
	}
	comp := stage["composite"].(map[string]any)
	pks := comp["parentKeys"].([]any)
	p0, p1 := pks[0].(string), pks[1].(string)
	seen := map[string]bool{}
	var t0, t1 []any
	for _, r := range parents {
		k0, k1 := r[p0], r[p1]
		s0, ok0 := keyString(k0)
		s1, ok1 := keyString(k1)
		if ok0 && ok1 && !seen[s0+" "+s1] {
			seen[s0+" "+s1] = true
			t0 = append(t0, k0)
			t1 = append(t1, k1)
		}
	}
	if len(t0) == 0 {
		return nil
	}
	if kind == "pgArrayComposite" {
		return &relBind{sql: sqlText, params: []any{toInt64Slice(t0), toInt64Slice(t1)}}
	}
	// tupleExpand (sqlite/mysql composite): repeat groupTemplate per tuple, flatten params.
	group := stage["groupTemplate"].(string)
	suffix, _ := stage["suffix"].(string)
	groups := make([]string, len(t0))
	var flat []any
	for i := range t0 {
		groups[i] = group
		flat = append(flat, substOne(t0[i], 0), substOne(t1[i], 0))
	}
	return &relBind{sql: sqlText + strings.Join(groups, ", ") + suffix, params: flat}
}

// ── executor ───────────────────────────────────────────────────────────────────
type ormDriver struct {
	dialect string
	db      *sql.DB
}

// queryAll runs a SELECT and returns rows as []map[string]any (generic column scan).
func queryAll(q interface {
	Query(string, ...any) (*sql.Rows, error)
}, sqlText string, params []any) ([]map[string]any, error) {
	rows, err := q.Query(sqlText, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := make(map[string]any, len(cols))
		for i, c := range cols {
			m[c] = normalizeCell(raw[i])
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// normalizeCell coerces []byte (MySQL/PG text) to string and keeps ints/floats; used for the
// distinct-key dedup and id extraction (byte-string keys stringify identically to their number).
func normalizeCell(v any) any {
	switch b := v.(type) {
	case []byte:
		return string(b)
	default:
		return v
	}
}

func (d *ormDriver) run(plan map[string]any) (int, error) {
	if plan["kind"].(string) == "read" {
		return d.readPlan(plan)
	}
	return d.writePlan(plan)
}

func (d *ormDriver) readPlan(plan map[string]any) (int, error) {
	reads := plan["reads"].([]any)
	first := reads[0].(map[string]any)
	firstParams := substParams(asAnySlice(first["params"]), 0)
	rows, err := queryAll(d.db, first["sql"].(string), firstParams)
	if err != nil {
		return 0, err
	}
	total := len(rows)
	stageRows := [][]map[string]any{rows}
	for _, rel := range plan["relations"].([]any) {
		stage := rel.(map[string]any)
		parentStmt := int(stage["parentStmt"].(float64))
		var children []map[string]any
		if b := bindRelation(stage, stageRows[parentStmt]); b != nil {
			children, err = queryAll(d.db, b.sql, b.params)
			if err != nil {
				return 0, err
			}
		}
		total += len(children)
		stageRows = append(stageRows, children)
	}
	return total, nil
}

func (d *ormDriver) writePlan(plan map[string]any) (int, error) {
	seq := nextSeq()
	tx, err := d.db.Begin()
	if err != nil {
		return 0, err
	}
	returnedID := int64(0)
	n := 0
	for _, s := range plan["statements"].([]any) {
		st := s.(map[string]any)
		role := st["role"].(string)
		sqlText := st["sql"].(string)
		params := substParams(asAnySlice(st["params"]), seq)
		if role == "useReturn" {
			if at, ok := st["useReturnAt"].(float64); ok {
				params[int(at)] = returnedID
			}
		}
		switch {
		case role == "insertReturn":
			if d.dialect == "postgres" {
				var id int64
				if e := tx.QueryRow(sqlText, params...).Scan(&id); e != nil {
					_ = tx.Rollback()
					return 0, e
				}
				returnedID = id
			} else {
				res, e := tx.Exec(stripReturning(sqlText), params...)
				if e != nil {
					_ = tx.Rollback()
					return 0, e
				}
				returnedID, _ = res.LastInsertId()
			}
		case d.dialect == "mysql" && hasReturning(sqlText):
			// MySQL has no native RETURNING (a plain upsert RETURNING id): strip + exec.
			if _, e := tx.Exec(stripReturning(sqlText), params...); e != nil {
				_ = tx.Rollback()
				return 0, e
			}
		case hasReturning(sqlText):
			// pg native RETURNING / sqlite RETURNING: a row-returning statement → Query (drain).
			r, e := tx.Query(sqlText, params...)
			if e != nil {
				_ = tx.Rollback()
				return 0, e
			}
			r.Close()
		default:
			if _, e := tx.Exec(sqlText, params...); e != nil {
				_ = tx.Rollback()
				return 0, e
			}
		}
		n++
	}
	if e := tx.Commit(); e != nil {
		return 0, e
	}
	return n, nil
}

func asAnySlice(v any) []any {
	if v == nil {
		return nil
	}
	return v.([]any)
}

// ── artifact + seed ────────────────────────────────────────────────────────────
type artifact struct {
	raw map[string]any
}

func (a *artifact) dialects() []string {
	var out []string
	for _, d := range a.raw["dialects"].([]any) {
		out = append(out, d.(string))
	}
	return out
}
func (a *artifact) ops() []any { return a.raw["ops"].([]any) }
func (a *artifact) schema(d string) map[string]any {
	return a.raw["schema"].(map[string]any)[d].(map[string]any)
}
func (a *artifact) plan(op, d string) map[string]any {
	return a.raw["plans"].(map[string]any)[op].(map[string]any)[d].(map[string]any)
}

func loadArtifact() *artifact {
	_, thisFile, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "benchmark", "crosslang", "generated", "orm-plan.json")
	b, err := os.ReadFile(path)
	if err != nil {
		panic(fmt.Sprintf("read orm-plan.json: %v", err))
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		panic(fmt.Sprintf("parse orm-plan.json: %v", err))
	}
	return &artifact{raw: raw}
}

func strList(node map[string]any, key string) []string {
	arr, _ := node[key].([]any)
	out := make([]string, 0, len(arr))
	for _, s := range arr {
		out = append(out, s.(string))
	}
	return out
}

func pgPlaceholders(sqlText string) string {
	var b strings.Builder
	n := 0
	for _, ch := range sqlText {
		if ch == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
		} else {
			b.WriteRune(ch)
		}
	}
	return b.String()
}

func seedRows(db *sql.DB, schema map[string]any, dialect string) error {
	for _, s := range schema["seed"].([]any) {
		row := s.(map[string]any)
		sqlText := row["sql"].(string)
		if dialect == "postgres" {
			sqlText = pgPlaceholders(sqlText)
		}
		params := substParams(asAnySlice(row["params"]), 0)
		if _, err := db.Exec(sqlText, params...); err != nil {
			return fmt.Errorf("seed %q: %w", sqlText, err)
		}
	}
	return nil
}

func makeDriver(dialect string, a *artifact) (*ormDriver, error) {
	schema := a.schema(dialect)
	switch dialect {
	case "sqlite":
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1) // one in-memory connection so schema+seed+ops share the same DB
		if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
			return nil, err
		}
		for _, s := range strList(schema, "ddl") {
			if _, err := db.Exec(s); err != nil {
				return nil, err
			}
		}
		if err := seedRows(db, schema, "sqlite"); err != nil {
			return nil, err
		}
		return &ormDriver{dialect: "sqlite", db: db}, nil
	case "postgres":
		host := envOr("TEST_DB_HOST", "localhost")
		port := envOr("TEST_DB_PORT", "5433")
		user := envOr("TEST_DB_USER", "testuser")
		pass := envOr("TEST_DB_PASSWORD", "testpass")
		dbname := envOr("TEST_DB_NAME", "testdb")
		dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, pass, dbname)
		db, err := rt.OpenPostgres(dsn)
		if err != nil {
			return nil, err
		}
		if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS " + pgSchemaName); err != nil {
			return nil, err
		}
		if _, err := db.Exec("SET search_path TO " + pgSchemaName); err != nil {
			return nil, err
		}
		// Pin search_path for EVERY pooled connection (SET above only affects one conn).
		db.SetMaxOpenConns(1)
		for _, s := range strList(schema, "drop") {
			if _, err := db.Exec(s); err != nil {
				return nil, err
			}
		}
		for _, s := range strList(schema, "ddl") {
			if _, err := db.Exec(s); err != nil {
				return nil, err
			}
		}
		if err := seedRows(db, schema, "postgres"); err != nil {
			return nil, err
		}
		for _, s := range strList(schema, "seqReset") {
			if _, err := db.Exec(s); err != nil {
				return nil, err
			}
		}
		return &ormDriver{dialect: "postgres", db: db}, nil
	case "mysql":
		host := envOr("TEST_MYSQL_HOST", "127.0.0.1")
		port := envOr("TEST_MYSQL_PORT", "3307")
		user := envOr("TEST_MYSQL_USER", "testuser")
		pass := envOr("TEST_MYSQL_PASSWORD", "testpass")
		bootDB := envOr("TEST_MYSQL_DB", "testdb")
		bootDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=false&multiStatements=false", user, pass, host, port, bootDB)
		boot, err := rt.OpenMysql(bootDSN)
		if err != nil {
			return nil, err
		}
		if _, err := boot.Exec("CREATE DATABASE IF NOT EXISTS " + mysqlDBName); err != nil {
			return nil, err
		}
		boot.Close()
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=false&multiStatements=false", user, pass, host, port, mysqlDBName)
		db, err := rt.OpenMysql(dsn)
		if err != nil {
			return nil, err
		}
		for _, s := range strList(schema, "drop") {
			if _, err := db.Exec(s); err != nil {
				return nil, err
			}
		}
		for _, s := range strList(schema, "ddl") {
			if _, err := db.Exec(s); err != nil {
				return nil, err
			}
		}
		if err := seedRows(db, schema, "mysql"); err != nil {
			return nil, err
		}
		return &ormDriver{dialect: "mysql", db: db}, nil
	default:
		return nil, fmt.Errorf("unknown dialect %s", dialect)
	}
}

// ── standalone smoke (mirror orm-smoke.ts) ─────────────────────────────────────
func pad(s string, n int) string {
	if len(s) >= n {
		return s
	}
	return s + strings.Repeat(" ", n-len(s))
}

func smoke() {
	a := loadArtifact()
	dialects := a.dialects()
	drivers := map[string]*ormDriver{}
	for _, d := range dialects {
		drv, err := makeDriver(d, a)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FATAL: connect %s: %v\n", d, err)
			os.Exit(1)
		}
		drivers[d] = drv
	}
	pass, fail := 0, 0
	type cell struct{ vals []string }
	var rowsByOp []cell
	for _, opAny := range a.ops() {
		op := opAny.(map[string]any)
		id := op["id"].(string)
		c := cell{}
		for _, d := range dialects {
			n, err := drivers[d].run(a.plan(id, d))
			if err != nil {
				c.vals = append(c.vals, "ERR: "+firstLine(err.Error()))
				fail++
			} else {
				c.vals = append(c.vals, strconv.Itoa(n))
				pass++
			}
		}
		rowsByOp = append(rowsByOp, c)
	}
	fmt.Print("\n19 ORM ops x 3 DBs — rows/op (writes report statements executed) [go]:\n\n")
	fmt.Printf("%s %s %s postgres\n", pad("op", 42), pad("sqlite", 14), pad("mysql", 14))
	for i, opAny := range a.ops() {
		op := opAny.(map[string]any)
		tag := "R "
		if w, _ := op["write"].(bool); w {
			tag = "W "
		}
		label := op["label"].(string)
		v := rowsByOp[i].vals
		fmt.Printf("%s %s %s %s\n", pad(tag+label, 42), pad(v[0], 14), pad(v[1], 14), v[2])
	}
	total := pass + fail
	fmt.Printf("\n%d/%d cells green (%d ops x 3 DBs = %d).\n", pass, total, len(a.ops()), len(a.ops())*3)
	for _, d := range dialects {
		drivers[d].db.Close()
	}
	if fail > 0 {
		fmt.Fprintf(os.Stderr, "\nSMOKE FAILED: %d cell(s) errored (see ERR above).\n", fail)
		os.Exit(1)
	}
	fmt.Println("SMOKE PASS [go]: all cells DB-backed on all 3 real DBs (sqlite via PURE-GO modernc.org/sqlite).")
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// ── NDJSON protocol (harness registry drives this over stdin/stdout) ───────────
func writeMsg(obj map[string]any) {
	b, _ := json.Marshal(obj)
	os.Stdout.Write(append(b, '\n'))
}

func protocol() {
	a := loadArtifact()
	live := map[string]*ormDriver{}
	driverFor := func(d string) (*ormDriver, error) {
		if drv, ok := live[d]; ok {
			return drv, nil
		}
		drv, err := makeDriver(d, a)
		if err != nil {
			return nil, err
		}
		live[d] = drv
		return drv, nil
	}
	writeMsg(map[string]any{"kind": "ready", "language": "go", "impl": "runtime", "readyAtEpochMs": float64(time.Now().UnixNano()) / 1e6})
	dec := json.NewDecoder(os.Stdin)
	for {
		var req map[string]any
		if err := dec.Decode(&req); err != nil {
			return // EOF
		}
		kind, _ := req["kind"].(string)
		if kind == "shutdown" {
			for _, d := range live {
				d.db.Close()
			}
			os.Exit(0)
		}
		if kind == "rss" {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			writeMsg(map[string]any{"kind": "rss", "rssBytes": m.Sys})
			continue
		}
		caseID, _ := req["case"].(string)
		dialect, _ := req["dialect"].(string)
		drv, err := driverFor(dialect)
		if err != nil {
			writeMsg(map[string]any{"kind": "error", "message": err.Error()})
			continue
		}
		plan := a.plan(caseID, dialect)
		switch kind {
		case "run":
			warmup := intOf(req["warmup"])
			iters := intOf(req["iterations"])
			for i := 0; i < warmup; i++ {
				drv.run(plan)
			}
			samples := make([]float64, 0, iters)
			for i := 0; i < iters; i++ {
				t0 := time.Now()
				if _, err := drv.run(plan); err != nil {
					writeMsg(map[string]any{"kind": "error", "case": caseID, "dialect": dialect, "message": err.Error()})
					goto next
				}
				samples = append(samples, float64(time.Since(t0).Nanoseconds())/1e6)
			}
			writeMsg(map[string]any{"kind": "run", "case": caseID, "dialect": dialect, "samplesMs": samples})
		case "throughput":
			iters := intOf(req["iterations"])
			t0 := time.Now()
			for i := 0; i < iters; i++ {
				drv.run(plan)
			}
			writeMsg(map[string]any{"kind": "throughput", "case": caseID, "dialect": dialect, "elapsedMs": float64(time.Since(t0).Nanoseconds()) / 1e6, "completed": iters})
		case "cost":
			rows, err := drv.run(plan)
			if err != nil {
				writeMsg(map[string]any{"kind": "error", "case": caseID, "dialect": dialect, "message": err.Error()})
				continue
			}
			// queries/op derived from the plan shape (same for every language — the SAME plan).
			queries := 0
			if plan["kind"] == "read" {
				queries = len(asAnySlice(plan["reads"])) + len(asAnySlice(plan["relations"]))
			} else {
				queries = len(asAnySlice(plan["statements"]))
			}
			writeMsg(map[string]any{"kind": "cost", "case": caseID, "dialect": dialect, "queries": queries, "rows": rows})
		default:
			writeMsg(map[string]any{"kind": "error", "message": "unknown kind " + kind})
		}
	next:
	}
}

func intOf(v any) int {
	if f, ok := v.(float64); ok {
		return int(f)
	}
	return 0
}

func main() {
	smokeMode := false
	for _, a := range os.Args[1:] {
		if a == "--smoke" {
			smokeMode = true
		}
	}
	if smokeMode {
		smoke()
	} else {
		protocol()
	}
}
