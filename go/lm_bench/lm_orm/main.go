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
// Spawn convention (run.ts orchestrator): the built binary
//
//	go/lm_bench/lm_orm  [--smoke]
//
// `--smoke` runs the 57-cell matrix and exits; without it, it runs ALL 19 ops × 3 dialects,
// self-measures, and writes benchmark/crosslang/.results/go.csv (no stdin/stdout protocol).
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

	_ "github.com/go-sql-driver/mysql" // BARE mysql driver (registered as "mysql") — raw baseline
	_ "github.com/jackc/pgx/v5/stdlib" // BARE pgx stdlib driver (registered as "pgx") — raw baseline
	_ "modernc.org/sqlite"             // PURE-GO sqlite driver (registered as "sqlite")
)

const pgSchemaName = "scp_go_bench"
const mysqlDBName = "scp_go_bench"

// Raw-driver BASELINE (task: MEASURE litedbmodel's over-driver overhead, not assert it). For each
// op×dialect the bench ALSO runs the IDENTICAL final SQL + params the runtime issues — assembled by
// the SAME bindRelation/substParams/writePlan code — but through the BARE database/sql driver (no
// litedbmodel_runtime wrapper). Emitted as `baseline_latency_ms`; the collector splits it into an
// `impl: baseline` cell. runtime÷baseline = litedbmodel's over-driver cost (≈1.0× for the thin ops).
//
//   - sqlite   : runtime ALREADY uses the raw modernc.org/sqlite driver → baseline is a SECOND
//     *sql.DB opened the same way (the honest ≈1.0× confirmation).
//   - postgres : runtime uses rt.OpenPostgres = sql.Open("pgx", …); baseline opens the SAME pgx
//     stdlib driver directly (no wrapper) → byte-identical $N + RETURNING SQL.
//   - mysql    : runtime uses rt.OpenMysql = the RETURNING-emulating "mysql-scp" wrapper, but this
//     executor's writePlan already strips RETURNING itself and uses tx.Exec for the mysql
//     path (never the wrapper's QueryContext RETURNING interception), so the bare "mysql"
//     driver runs the SAME stripped statements byte-identically.
//
// The baseline gets its OWN isolated pg schema / mysql db, seeded identically, so the two impls never
// clobber each other's tables.
const pgBaselineSchema = "scp_go_bench_baseline"
const mysqlBaselineDB = "scp_go_bench_baseline"

// execImpl selects the runtime path vs the bare-driver baseline for makeDriver.
type execImpl string

const (
	implRuntime execImpl = "runtime"
	implRaw     execImpl = "raw"
)

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
	impl    execImpl
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

// openPostgres opens the pg *sql.DB for the given impl: `runtime` = the shipped rt.OpenPostgres
// wrapper; `raw` = the BARE pgx stdlib driver (sql.Open("pgx", …)) directly — no litedbmodel wrapper.
// The wrapper adds only pool sizing + Ping over the same pgx driver, so the SQL issued is identical.
func openPostgres(impl execImpl, dsn string) (*sql.DB, error) {
	if impl == implRaw {
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return nil, err
		}
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, err
		}
		return db, nil
	}
	return rt.OpenPostgres(dsn)
}

// openMysql opens the mysql *sql.DB for the given impl: `runtime` = rt.OpenMysql (the "mysql-scp"
// RETURNING-emulating wrapper); `raw` = the BARE go-sql-driver ("mysql") directly. This executor's
// writePlan strips RETURNING itself and drives the mysql path via tx.Exec (never the wrapper's
// QueryContext RETURNING interception), so both handles run byte-identical stripped statements.
func openMysql(impl execImpl, dsn string) (*sql.DB, error) {
	if impl == implRaw {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, err
		}
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, err
		}
		return db, nil
	}
	return rt.OpenMysql(dsn)
}

// makeDriver builds a live *ormDriver for one dialect × impl. ALL statement assembly (DDL/seed via
// the shared schema node, and at run time bindRelation/substParams/writePlan) is impl-agnostic — the
// ONLY difference is the low-level *sql.DB handle: `runtime` opens via the litedbmodel_runtime
// wrappers, `raw` opens the bare driver against an ISOLATED baseline schema/db seeded identically.
func makeDriver(dialect string, impl execImpl, a *artifact) (*ormDriver, error) {
	schema := a.schema(dialect)
	switch dialect {
	case "sqlite":
		// sqlite runtime ALREADY uses the raw modernc.org/sqlite driver; the baseline is a SECOND
		// identical in-memory *sql.DB (each :memory: open is its own isolated DB) → honest ≈1.0×.
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
		return &ormDriver{dialect: "sqlite", impl: impl, db: db}, nil
	case "postgres":
		host := envOr("TEST_DB_HOST", "localhost")
		port := envOr("TEST_DB_PORT", "5433")
		user := envOr("TEST_DB_USER", "testuser")
		pass := envOr("TEST_DB_PASSWORD", "testpass")
		dbname := envOr("TEST_DB_NAME", "testdb")
		dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, pass, dbname)
		schemaName := pgSchemaName
		if impl == implRaw {
			schemaName = pgBaselineSchema
		}
		db, err := openPostgres(impl, dsn)
		if err != nil {
			return nil, err
		}
		if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS " + schemaName); err != nil {
			return nil, err
		}
		if _, err := db.Exec("SET search_path TO " + schemaName); err != nil {
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
		return &ormDriver{dialect: "postgres", impl: impl, db: db}, nil
	case "mysql":
		host := envOr("TEST_MYSQL_HOST", "127.0.0.1")
		port := envOr("TEST_MYSQL_PORT", "3307")
		user := envOr("TEST_MYSQL_USER", "testuser")
		pass := envOr("TEST_MYSQL_PASSWORD", "testpass")
		bootDB := envOr("TEST_MYSQL_DB", "testdb")
		dbName := mysqlDBName
		if impl == implRaw {
			dbName = mysqlBaselineDB
		}
		bootDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=false&multiStatements=false", user, pass, host, port, bootDB)
		boot, err := openMysql(impl, bootDSN)
		if err != nil {
			return nil, err
		}
		if _, err := boot.Exec("CREATE DATABASE IF NOT EXISTS " + dbName); err != nil {
			return nil, err
		}
		boot.Close()
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=false&multiStatements=false", user, pass, host, port, dbName)
		db, err := openMysql(impl, dsn)
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
		return &ormDriver{dialect: "mysql", impl: impl, db: db}, nil
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
		drv, err := makeDriver(d, implRuntime, a)
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

// ── STANDALONE CSV bench (no protocol) ─────────────────────────────────────────
// ONE standalone process runs ALL 19 ops × 3 dialects, self-measures, and writes a FLAT CSV to
// benchmark/crosslang/.results/go.csv. The collector (collect.ts) reads the CSVs → CROSS-LANG.md.
// CSV schema: language,case,dialect,metric,value   (RAW values only — collector owns the math).
func envNum(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func csvField(s string) string {
	if strings.ContainsAny(s, ",\"\n") {
		return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\""
	}
	return s
}

func resultsPath() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "benchmark", "crosslang", ".results", "go.csv")
}

// artifactBytes returns the size of THIS compiled binary (the native-cell artifact); ok=false if
// the path/stat fails.
func artifactBytes() (int64, bool) {
	exe, err := os.Executable()
	if err != nil {
		return 0, false
	}
	info, err := os.Stat(exe)
	if err != nil {
		return 0, false
	}
	return info.Size(), true
}

func bench() {
	const language = "go"
	warmup := envNum("BENCH_WARMUP", 50)
	iters := envNum("BENCH_ITER", 300)
	tpDefault := iters
	if tpDefault > 2000 {
		tpDefault = 2000
	}
	tpIters := envNum("BENCH_TP_ITER", tpDefault)

	spawnedAt := float64(time.Now().UnixNano()) / 1e6
	a := loadArtifact()
	dialects := a.dialects()
	// cold = process start → runtime ready (binary start + artifact load), before any connect.
	coldMs := (float64(time.Now().UnixNano()) / 1e6) - spawnedAt
	if coldMs < 0 {
		coldMs = 0
	}

	rows := []string{"language,case,dialect,metric,value"}
	emit := func(caseID, dialect, metric, value string) {
		rows = append(rows, fmt.Sprintf("%s,%s,%s,%s,%s", language, caseID, dialect, metric, csvField(value)))
	}
	f := func(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }

	live := map[string]*ormDriver{}
	baselines := map[string]*ormDriver{}
	for _, dialect := range dialects {
		drv, err := makeDriver(dialect, implRuntime, a)
		if err != nil {
			reason := firstLine(err.Error())
			for _, opAny := range a.ops() {
				op := opAny.(map[string]any)
				emit(op["id"].(string), dialect, "skipped", fmt.Sprintf("%s unreachable (%s)", dialect, reason))
			}
			continue
		}
		live[dialect] = drv
		// Bare-driver BASELINE (same real driver, same SQL, ISOLATED baseline schema/db). A baseline
		// connect failure is NOT a whole-cell skip — the runtime numbers still stand; only the
		// baseline_latency_ms rows for that dialect are dropped (honest per-dialect skip, no fake).
		var baseline *ormDriver
		if b, berr := makeDriver(dialect, implRaw, a); berr != nil {
			fmt.Fprintf(os.Stderr, "[go] baseline %s unavailable (%s) — runtime metrics unaffected\n", dialect, firstLine(berr.Error()))
		} else {
			baseline = b
			baselines[dialect] = b
		}
		for _, opAny := range a.ops() {
			op := opAny.(map[string]any)
			caseID := op["id"].(string)
			plan := a.plan(caseID, dialect)
			// cost (fairness): queries/op from the plan shape; rows/op = executor's returned count.
			queries := 0
			if plan["kind"] == "read" {
				queries = len(asAnySlice(plan["reads"])) + len(asAnySlice(plan["relations"]))
			} else {
				queries = len(asAnySlice(plan["statements"]))
			}
			rowsCount, err := drv.run(plan)
			if err != nil {
				emit(caseID, dialect, "skipped", firstLine(err.Error()))
				continue
			}
			emit(caseID, dialect, "cost_queries", strconv.Itoa(queries))
			emit(caseID, dialect, "cost_rows", strconv.Itoa(rowsCount))
			// latency: warmup, then one row PER timed iteration.
			for i := 0; i < warmup; i++ {
				drv.run(plan)
			}
			failed := false
			for i := 0; i < iters; i++ {
				t0 := time.Now()
				if _, err := drv.run(plan); err != nil {
					emit(caseID, dialect, "skipped", firstLine(err.Error()))
					failed = true
					break
				}
				emit(caseID, dialect, "latency_ms", f(float64(time.Since(t0).Nanoseconds())/1e6))
			}
			if failed {
				continue
			}
			// throughput: a tight loop, raw elapsed + completed.
			t0 := time.Now()
			for i := 0; i < tpIters; i++ {
				drv.run(plan)
			}
			emit(caseID, dialect, "throughput_elapsed_ms", f(float64(time.Since(t0).Nanoseconds())/1e6))
			emit(caseID, dialect, "throughput_completed", strconv.Itoa(tpIters))

			// baseline latency: the IDENTICAL SQL/params (same assembly) through the BARE driver, SAME
			// warmup + timed iterations → runtime÷baseline = litedbmodel's over-driver overhead. Emitted
			// as baseline_latency_ms; the collector splits it into the `impl: baseline` cell. A baseline
			// error mid-loop is an honest per-dialect skip (drop the rows) — the runtime rows stand.
			if baseline != nil {
				bPlan := a.plan(caseID, dialect)
				for i := 0; i < warmup; i++ {
					baseline.run(bPlan)
				}
				for i := 0; i < iters; i++ {
					b0 := time.Now()
					if _, err := baseline.run(bPlan); err != nil {
						fmt.Fprintf(os.Stderr, "[go] baseline %s/%s errored (%s) — dropped, runtime unaffected\n", dialect, caseID, firstLine(err.Error()))
						break
					}
					emit(caseID, dialect, "baseline_latency_ms", f(float64(time.Since(b0).Nanoseconds())/1e6))
				}
			}
		}
	}

	emit("", "", "cold_ms", f(coldMs))
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	emit("", "", "rss_bytes", strconv.FormatUint(m.Sys, 10))
	emit("", "", "warmup", strconv.Itoa(warmup))
	// artifact_bytes: this compiled binary's own size (a native-cell metric; the interpreted cells
	// ts/python/php run on an interpreter, so they emit NO such row → the collector renders `—`).
	if bytes, ok := artifactBytes(); ok {
		emit("", "", "artifact_bytes", strconv.FormatInt(bytes, 10))
	}

	for _, d := range live {
		d.db.Close()
	}
	for _, d := range baselines {
		d.db.Close()
	}

	out := resultsPath()
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: mkdir .results: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(out, []byte(strings.Join(rows, "\n")+"\n"), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: write csv: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "[%s] wrote %s (%d rows)\n", language, out, len(rows)-1)
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
		bench()
	}
}
