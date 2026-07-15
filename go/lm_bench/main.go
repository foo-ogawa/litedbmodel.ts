// Command lm_bench is the Go leg of the litedbmodel cross-language execution-surface bench (#44).
//
// It speaks the line-delimited JSON contract over stdin/stdout for the three Go cells:
// sql / codegen / ir.
//
//	sql     — hand-optimized raw SQL via database/sql + modernc sqlite (baseline 1.0x; sqlite-shaped
//	          by construction — the sql baseline runs on sqlite only, same convention every
//	          language adapter uses)
//	codegen — the makeSQL bundle resident + integrity-verified ONCE at load, executed via the
//	          DEPENDED litedbmodel_runtime package. Wired to the in-proc sqlite driver only
//	          (matches every other language's codegen cell) — PG/MySQL DB-backed is a per-cell skip.
//	ir      — the bundle loaded FROM the generated JSON on disk, executed via the SAME runtime.
//	          DB-backed on sqlite AND real dockerized Postgres/MySQL (#53) via the runtime's live
//	          OpenPostgres (pgx)/OpenMysql (go-sql-driver, RETURNING-emulated) — the SAME seam
//	          livedb_runner/conformance already use; this just wires the bench cell to it.
//
// Consumes generated/bundles.json (the language-neutral §8 artifact) unchanged. Its compiled
// binary size is the Go artifact-size metric.
package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	conf "github.com/foo-ogawa/litedbmodel/go/conformance"
	rt "github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime"

	"github.com/foo-ogawa/litedbmodel/go/lm_bench/cgplans"

	bc "github.com/foo-ogawa/behavior-contracts/go"
	_ "modernc.org/sqlite"
)

// ── real-DB schema (mirror of domain.ts PG_SCHEMA / MYSQL_SCHEMA; isolated `scp_go_bench`
// namespace so this bench never collides with conformance's `scp_go` tables) ──────────────────
const pgSchemaName = "scp_go_bench"
const mysqlDBName = "scp_go_bench"

var pgSchemaStatements = []string{
	"DROP TABLE IF EXISTS comments CASCADE",
	"DROP TABLE IF EXISTS posts CASCADE",
	"DROP TABLE IF EXISTS users CASCADE",
	"DROP TABLE IF EXISTS uniq CASCADE",
	"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, post_count INTEGER NOT NULL DEFAULT 0)",
	"CREATE TABLE posts (id SERIAL PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, views INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL)",
	"CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL)",
	// s0 binds author_id (always numeric) — INTEGER (#53: pgx's strict binary protocol rejects an
	// int arg for a text column).
	"CREATE TABLE uniq (name TEXT NOT NULL, s0 INTEGER, f0 TEXT)",
}
var pgSeqReset = "SELECT setval('posts_id_seq', (SELECT MAX(id) FROM posts))"
var mysqlSchemaStatements = []string{
	"SET FOREIGN_KEY_CHECKS = 0",
	"DROP TABLE IF EXISTS comments",
	"DROP TABLE IF EXISTS posts",
	"DROP TABLE IF EXISTS users",
	"DROP TABLE IF EXISTS uniq",
	"SET FOREIGN_KEY_CHECKS = 1",
	"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, post_count INT NOT NULL DEFAULT 0)",
	"CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, status VARCHAR(255), views INT NOT NULL DEFAULT 0, created_at VARCHAR(255) NOT NULL)",
	"CREATE TABLE comments (id INT PRIMARY KEY, post_id INT NOT NULL, body VARCHAR(255) NOT NULL, created_at VARCHAR(255) NOT NULL)",
	"CREATE TABLE uniq (name VARCHAR(255) NOT NULL, s0 INT, f0 VARCHAR(255))",
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// Lazy, memoized live connections — one per dialect, reused across every run/throughput request
// this adapter process handles (the harness spawns ONE subprocess per (language × impl) cell).
var pgConn *sql.DB
var mysqlConn *sql.DB

// connectPG lazily opens + (re)seeds a live Postgres in the isolated scp_go_bench schema. Panics
// (fail-closed, no silent skip) if PG is unreachable.
func connectPG(a *artifact) *sql.DB {
	if pgConn != nil {
		return pgConn
	}
	host := envOr("TEST_DB_HOST", "localhost")
	port := envOr("TEST_DB_PORT", "5433")
	user := envOr("TEST_DB_USER", "testuser")
	pass := envOr("TEST_DB_PASSWORD", "testpass")
	dbname := envOr("TEST_DB_NAME", "testdb")
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable&search_path=%s", user, pass, host, port, dbname, pgSchemaName)
	db, err := rt.OpenPostgres(dsn)
	if err != nil {
		panic(fmt.Sprintf("postgres unreachable at %s:%s — %v", host, port, err))
	}
	must(exec1(db, "CREATE SCHEMA IF NOT EXISTS "+pgSchemaName))
	must(exec1(db, "SET search_path TO "+pgSchemaName))
	for _, s := range pgSchemaStatements {
		must(exec1(db, s))
	}
	for _, s := range a.Seed {
		must(exec1(db, s))
	}
	must(exec1(db, pgSeqReset))
	pgConn = db
	return db
}

// connectMysql lazily opens + (re)seeds a live MySQL in the isolated scp_go_bench database.
// Panics (fail-closed) if MySQL is unreachable.
func connectMysql(a *artifact) *sql.DB {
	if mysqlConn != nil {
		return mysqlConn
	}
	host := envOr("TEST_MYSQL_HOST", "127.0.0.1")
	port := envOr("TEST_MYSQL_PORT", "3307")
	user := envOr("TEST_MYSQL_USER", "testuser")
	pass := envOr("TEST_MYSQL_PASSWORD", "testpass")
	bootDB := envOr("TEST_MYSQL_DB", "testdb")
	bootDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, bootDB)
	boot, err := rt.OpenMysql(bootDSN)
	if err != nil {
		panic(fmt.Sprintf("mysql unreachable at %s:%s — %v", host, port, err))
	}
	must(exec1(boot, "CREATE DATABASE IF NOT EXISTS "+mysqlDBName))
	boot.Close()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, mysqlDBName)
	db, err := rt.OpenMysql(dsn)
	if err != nil {
		panic(fmt.Sprintf("mysql (%s) unreachable at %s:%s — %v", mysqlDBName, host, port, err))
	}
	for _, s := range mysqlSchemaStatements {
		must(exec1(db, s))
	}
	for _, s := range a.Seed {
		must(exec1(db, s))
	}
	mysqlConn = db
	return db
}

func exec1(db *sql.DB, stmt string) error {
	_, err := db.Exec(stmt)
	return err
}

// liveDriverFor returns the memoized live connection for "postgres"/"mysql", connecting +
// seeding lazily on first use.
func liveDriverFor(dialect string, a *artifact) *sql.DB {
	switch dialect {
	case "postgres":
		return connectPG(a)
	case "mysql":
		return connectMysql(a)
	default:
		panic("liveDriverFor: unknown dialect " + dialect)
	}
}

// ── generated artifact ────────────────────────────────────────────────────────
type caseArt struct {
	Case          string          `json:"case"`
	Kind          string          `json:"kind"`
	WithRelation  string          `json:"withRelation"`
	Bundle        json.RawMessage `json:"bundle"`
	Input         json.RawMessage `json:"input"`
	bundleObj     *bc.JObj
	relationsJObj *bc.JObj
	inputScope    *bc.Obj
}

type artifact struct {
	Schema []string
	Seed   []string
	// Cases is the sqlite map (in-proc DB-backed + fairness cost denominator);
	// CasesByDialect carries all 3 dialects for the per-dialect MICRO axis (#44 gap #1).
	Cases          map[string]*caseArt
	CasesByDialect map[string]map[string]*caseArt
}

func parseCase(c *caseArt) *caseArt {
	// Parse the bundle + input into bc-ordered nodes (the runtime consumes bc.JObj/Obj).
	bnode, err := bc.ParseJSONOrdered(c.Bundle)
	must(err)
	c.bundleObj = bnode.(*bc.JObj)
	if relN, ok := c.bundleObj.Get("relations"); ok {
		if relObj, ok := relN.(*bc.JObj); ok {
			c.relationsJObj = relObj
		}
	}
	inode, err := bc.ParseJSONOrdered(c.Input)
	must(err)
	v, err := conf.DecodeConformanceValue(inode)
	must(err)
	if obj, ok := v.(*bc.Obj); ok {
		c.inputScope = obj
	} else {
		c.inputScope = bc.NewObj()
	}
	return c
}

func loadArtifact(path string) *artifact {
	raw, err := os.ReadFile(path)
	must(err)
	var top struct {
		Schema   []string `json:"schema"`
		Seed     []string `json:"seed"`
		Dialects map[string]struct {
			Cases []*caseArt `json:"cases"`
		} `json:"dialects"`
	}
	must(json.Unmarshal(raw, &top))
	a := &artifact{Schema: top.Schema, Seed: top.Seed, CasesByDialect: map[string]map[string]*caseArt{}}
	for d, blk := range top.Dialects {
		m := map[string]*caseArt{}
		for _, c := range blk.Cases {
			m[c.Case] = parseCase(c)
		}
		a.CasesByDialect[d] = m
	}
	a.Cases = a.CasesByDialect["sqlite"]
	return a
}

func seedDB(a *artifact) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	must(err)
	_, err = db.Exec("PRAGMA foreign_keys = ON")
	must(err)
	for _, s := range a.Schema {
		_, err := db.Exec(s)
		must(err)
	}
	for _, s := range a.Seed {
		_, err := db.Exec(s)
		must(err)
	}
	return db
}

// ── sql baseline (hand-optimized raw SQL) ─────────────────────────────────────
func intArgs(n int) []any {
	a := make([]any, n)
	for i := 0; i < n; i++ {
		a[i] = i + 1
	}
	return a
}

func runSQL(caseID string, db execQuerier) {
	switch caseID {
	case "find":
		drain(db.Query("SELECT id, author_id, title, status, views, created_at FROM posts WHERE author_id = ? AND status = ? AND created_at >= ? ORDER BY id ASC", 1, "live", "2026-02-01"))
	case "complexWhere":
		args := append([]any{1, "2026-02-01", "post-%"}, intArgs(5)...)
		drain(db.Query("SELECT id, author_id, title, status, views FROM posts WHERE author_id = ? AND created_at >= ? AND title LIKE ? AND id IN (?, ?, ?, ?, ?) ORDER BY id ASC", args...))
	case "inList":
		ph := strings.Join(repeat("?", 10), ", ")
		drain(db.Query("SELECT id, title FROM posts WHERE id IN ("+ph+") ORDER BY id ASC", intArgs(10)...))
	case "belongsTo":
		r0, e0 := db.Query("SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC", 1)
		aids := dedup(scanInts(r0, e0, "author_id"))
		ph := strings.Join(repeat("?", len(aids)), ", ")
		drain(db.Query("SELECT id, name FROM users WHERE id IN ("+ph+")", toAny(aids)...))
	case "hasMany":
		r0, e0 := db.Query("SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC", 1)
		ids := scanInts(r0, e0, "id")
		ph := strings.Join(repeat("?", len(ids)), ", ")
		drain(db.Query("SELECT id, post_id, body FROM comments WHERE post_id IN ("+ph+")", toAny(ids)...))
	case "hasManyLimit":
		r0, e0 := db.Query("SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC", 1)
		ids := scanInts(r0, e0, "id")
		ph := strings.Join(repeat("?", len(ids)), ", ")
		drain(db.Query("SELECT id, post_id, body FROM (SELECT id, post_id, body, ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY id DESC) rn FROM comments WHERE post_id IN ("+ph+")) WHERE rn <= 3", toAny(ids)...))
	case "batchInsert":
		cols := []string{"author_id", "title", "status", "views", "created_at"}
		vals := make([]string, 10)
		args := make([]any, 0, 50)
		for i := 0; i < 10; i++ {
			vals[i] = "(" + strings.Join(repeat("?", len(cols)), ",") + ")"
			args = append(args, 2, fmt.Sprintf("bulk-%d", i), "live", 0, "2026-05-01")
		}
		mustExec(db.Exec("INSERT INTO posts ("+strings.Join(cols, ",")+") VALUES "+strings.Join(vals, ","), args...))
	case "writeTxGate":
		mustExec(db.Exec("BEGIN"))
		gr, ge := db.Query("SELECT 1 FROM users WHERE id = ?", 1)
		if !hasRow(gr, ge) {
			mustExec(db.Exec("ROLLBACK"))
			panic("requires_absent")
		}
		mustExec(db.Exec("INSERT INTO uniq (name, s0, f0) VALUES (?, ?, ?) ON CONFLICT DO NOTHING", "title_per_author", "1", "txn-post"))
		drain(db.Query("INSERT INTO posts (author_id, title, created_at) VALUES (?, ?, ?) RETURNING id, author_id, title", 1, "txn-post", "2026-05-01"))
		mustExec(db.Exec("UPDATE users SET post_count = post_count + ? WHERE id = ?", 1, 1))
		mustExec(db.Exec("COMMIT"))
	default:
		panic("unknown case " + caseID)
	}
}

// ── litedbmodel runtime (codegen / ir) op ─────────────────────────────────────
func runLM(c *caseArt, db rt.SQLDB) {
	bundle, err := rt.BundleFromJObj(c.bundleObj)
	must(err)
	switch c.Kind {
	case "batch":
		_, err := rt.ExecuteTransactionBundle(bundle, bc.NewObj(), db.(rt.TxDB))
		must(err)
	case "tx":
		_, err := rt.ExecuteTransactionBundle(bundle, c.inputScope, db.(rt.TxDB))
		must(err)
	case "relation":
		_, err := rt.ReadBundle(bundle, c.relationsJObj, c.inputScope, db, []string{c.WithRelation}, nil)
		must(err)
	default:
		_, err := rt.ExecuteBundle(bundle, c.inputScope, db)
		must(err)
	}
}

// ── fairness cost probe: DML statements + rows read (tx-control excluded) ──────
func isTxControl(q string) bool {
	up := strings.ToUpper(strings.TrimSpace(q))
	for _, k := range []string{"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "PRAGMA"} {
		if strings.HasPrefix(up, k) {
			return true
		}
	}
	return false
}

// cost counts the DML statements + DB rows read for one case (tx-control excluded), via the
// costDB tracing wrapper (see fakedriver.go).
func cost(impl, caseID string, a *artifact) (int, int) {
	return costViaTrace(impl, caseID, a)
}

func write(v any) {
	b, _ := json.Marshal(v)
	os.Stdout.Write(b)
	os.Stdout.Write([]byte("\n"))
}

func nowMs() float64 { return float64(time.Now().UnixNano()) / 1e6 }

func collect(warmup, iters int, op func()) []float64 {
	for i := 0; i < warmup; i++ {
		op()
	}
	s := make([]float64, iters)
	for i := 0; i < iters; i++ {
		t0 := time.Now()
		op()
		s[i] = float64(time.Since(t0).Nanoseconds()) / 1e6
	}
	return s
}

func main() {
	impl := "sql"
	for _, a := range os.Args[1:] {
		if strings.HasPrefix(a, "--impl=") {
			impl = a[len("--impl="):]
		}
	}
	exe, _ := os.Executable()
	_ = exe
	bundlesPath := os.Getenv("LM_BENCH_BUNDLES")
	if bundlesPath == "" {
		// Default: resolve relative to the repo (binary lives in benchmark/crosslang/adapters/go).
		wd, _ := os.Getwd()
		bundlesPath = wd + "/benchmark/crosslang/generated/bundles.json"
	}
	art := loadArtifact(bundlesPath)

	// codegen: force each generated module's fail-closed load + warm the native plan path once at
	// cold start (the codegen path touches NO IR data — the prepared native companion plans were
	// materialized at package init; the timed ops execute THROUGH the generated de-interpreted
	// code with the native render/tx engines, never ExecuteBundle).
	if impl == "codegen" {
		mock := openMockDB()
		for _, c := range cgplans.CaseIDs {
			runCodegen("sqlite", c, mock)
		}
		mock.Close()
	}

	write(map[string]any{"kind": "ready", "language": "go", "impl": impl, "readyAtEpochMs": nowMs()})

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var req map[string]any
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			write(map[string]any{"kind": "error", "message": "bad request: " + err.Error()})
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					write(map[string]any{"kind": "error", "message": fmt.Sprint(r)})
				}
			}()
			handle(req, impl, art)
		}()
	}
}

// `sql` is the hand-written raw-SQL baseline (sqlite-shaped by construction — every language
// adapter runs its `sql` cell on sqlite only). `codegen`'s generated module is wired to the
// in-proc sqlite driver only (matches every other language's codegen cell) — not a gap, a
// declared convention. Only `ir` gains live PG/MySQL (#53), via the SAME OpenPostgres/OpenMysql
// seam livedb_runner/conformance already use.
func dbSkipReason(impl, dialect string) string {
	if dialect == "sqlite" {
		return ""
	}
	switch impl {
	case "sql":
		return "sql baseline is hand-written sqlite SQL — not run against " + dialect + " (dialect-specific by construction)"
	case "codegen":
		return "codegen generated-module cell is wired to the in-proc sqlite driver; PG/MySQL DB-backed not wired for the generated cell — not run against " + dialect
	default:
		return "" // ir: PG/MySQL wired below (live OpenPostgres/OpenMysql).
	}
}

func handle(req map[string]any, impl string, art *artifact) {
	kind, _ := req["kind"].(string)
	dialect, _ := req["dialect"].(string)
	if dialect == "" {
		dialect = "sqlite"
	}
	switch kind {
	case "run":
		caseID := req["case"].(string)
		if reason := dbSkipReason(impl, dialect); reason != "" {
			write(map[string]any{"kind": "skipped", "case": caseID, "dialect": dialect, "reason": reason})
			return
		}
		warmup := int(req["warmup"].(float64))
		iters := int(req["iterations"].(float64))
		c := art.CasesByDialect[dialect][caseID]
		if dialect == "sqlite" {
			db := seedDB(art)
			defer db.Close()
			samples := collect(warmup, iters, func() {
				if impl == "sql" {
					runSQL(caseID, db)
				} else if impl == "codegen" {
					runCodegen(dialect, caseID, db)
				} else {
					runLM(c, db)
				}
			})
			write(map[string]any{"kind": "run", "case": caseID, "dialect": dialect, "samplesMs": samples})
		} else {
			db := liveDriverFor(dialect, art)
			samples := collect(warmup, iters, func() { runLM(c, db) })
			write(map[string]any{"kind": "run", "case": caseID, "dialect": dialect, "samplesMs": samples})
		}
	case "throughput":
		caseID := req["case"].(string)
		if reason := dbSkipReason(impl, dialect); reason != "" {
			write(map[string]any{"kind": "skipped", "case": caseID, "dialect": dialect, "reason": reason})
			return
		}
		iters := int(req["iterations"].(float64))
		c := art.CasesByDialect[dialect][caseID]
		if dialect == "sqlite" {
			db := seedDB(art)
			defer db.Close()
			t0 := time.Now()
			for i := 0; i < iters; i++ {
				if impl == "sql" {
					runSQL(caseID, db)
				} else if impl == "codegen" {
					runCodegen(dialect, caseID, db)
				} else {
					runLM(c, db)
				}
			}
			el := float64(time.Since(t0).Nanoseconds()) / 1e6
			write(map[string]any{"kind": "throughput", "case": caseID, "dialect": dialect, "elapsedMs": el, "completed": iters})
		} else {
			db := liveDriverFor(dialect, art)
			t0 := time.Now()
			for i := 0; i < iters; i++ {
				runLM(c, db)
			}
			el := float64(time.Since(t0).Nanoseconds()) / 1e6
			write(map[string]any{"kind": "throughput", "case": caseID, "dialect": dialect, "elapsedMs": el, "completed": iters})
		}
	case "micro":
		caseID := req["case"].(string)
		if impl == "sql" && dialect != "sqlite" {
			write(map[string]any{"kind": "skipped", "case": caseID, "dialect": dialect, "reason": "hand-SQL baseline is sqlite-shaped"})
			return
		}
		// The Go micro mock rides `database/sql`, whose arg-conversion layer rejects the
		// PG/MySQL IN-list ARRAY param (`[]Value`) that only the native pgx/go-sql-driver
		// bind — so the non-sqlite micro (client-path) cannot run through the mock harness
		// here. Skipped honestly (the SQLite micro client-path IS measured).
		if dialect != "sqlite" {
			write(map[string]any{"kind": "skipped", "case": caseID, "dialect": dialect, "reason": "go micro mock rides database/sql; its arg layer rejects the " + dialect + " IN-list array param ([]Value) — non-sqlite micro not run through the mock"})
			return
		}
		warmup := int(req["warmup"].(float64))
		iters := int(req["iterations"].(float64))
		mock := openMockDB()
		defer mock.Close()
		// PER-DIALECT bundle — the render/placeholder/array form differs.
		c := art.CasesByDialect[dialect][caseID]
		samples := collect(warmup, iters, func() {
			if impl == "sql" {
				runSQL(caseID, mock)
			} else if impl == "codegen" {
				runCodegen(dialect, caseID, mock)
			} else {
				runLM(c, mock)
			}
		})
		write(map[string]any{"kind": "micro", "case": caseID, "dialect": dialect, "samplesMs": samples})
	case "rss":
		write(map[string]any{"kind": "rss", "rssBytes": rssBytes()})
	case "cost":
		caseID := req["case"].(string)
		q, r := cost(impl, caseID, art)
		write(map[string]any{"kind": "cost", "case": caseID, "dialect": dialect, "queries": q, "rows": r})
	case "verify":
		// Behaviour-equality selfcheck: generated-code output == interpreter output (same rows).
		caseID := req["case"].(string)
		c := art.Cases[caseID]
		cg := runCodegenValueStr(art, caseID)
		ir := runLMValueStr(art, c)
		write(map[string]any{"kind": "verify", "case": caseID, "impl_kind": c.Kind,
			"equal": cg == ir, "cg_len": len(cg), "ir_len": len(ir)})
	case "shutdown":
		os.Exit(0)
	}
}

func rssBytes() int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		return 0
	}
	kb, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0
	}
	return kb * 1024
}

func must(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// ── small helpers shared by sql baseline ──────────────────────────────────────
type execQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
	Exec(query string, args ...any) (sql.Result, error)
}

func drain(rows *sql.Rows, err error) {
	must(err)
	defer rows.Close()
	for rows.Next() {
	}
}
func mustExec(_ sql.Result, err error) { must(err) }
func hasRow(rows *sql.Rows, err error) bool {
	must(err)
	defer rows.Close()
	return rows.Next()
}
func scanInts(rows *sql.Rows, err error, col string) []int {
	must(err)
	if rows == nil {
		return nil
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	idx := -1
	for i, c := range cols {
		if c == col {
			idx = i
		}
	}
	var out []int
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		rows.Scan(ptrs...)
		out = append(out, toInt(raw[idx]))
	}
	return out
}

func toInt(v any) int {
	switch t := v.(type) {
	case int64:
		return int(t)
	case int:
		return t
	case float64:
		return int(t)
	}
	return 0
}
func dedup(xs []int) []int {
	seen := map[int]bool{}
	var out []int
	for _, x := range xs {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}
func toAny(xs []int) []any {
	out := make([]any, len(xs))
	for i, x := range xs {
		out[i] = x
	}
	return out
}
func repeat(s string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = s
	}
	return out
}

// ── behaviour-equality verify helpers (the INTERPRETER side lives here — main.go is the ir/sql
// surface and legitimately imports the runtime; the codegen cell file imports NEITHER rt NOR
// encoding/json). Each side runs on its OWN freshly-seeded in-memory DB and canonicalizes to the
// conformance JSON string; integral float64 → int64 canonicalization compares by VALUE (the
// interpreter's scanValue floats INTEGER columns; the codegen native scan keeps int64).

func runCodegenValueStr(a *artifact, caseID string) string {
	raw := seedDB(a)
	defer raw.Close()
	out := runCodegenCase("sqlite", caseID, raw)
	return conf.EncodeConformanceJSON(coerceIntsValue(out))
}

func runLMValueStr(a *artifact, c *caseArt) string {
	raw := seedDB(a)
	defer raw.Close()
	var db rt.SQLDB = raw
	bundle, err := rt.BundleFromJObj(c.bundleObj)
	must(err)
	var out bc.Value
	switch c.Kind {
	case "batch":
		o, e := rt.ExecuteTransactionBundle(bundle, bc.NewObj(), db.(rt.TxDB))
		must(e)
		out = txResultToObj(o) // the SAME canonical shape the de-boxed codegen path emits (ser_T0)
	case "tx":
		o, e := rt.ExecuteTransactionBundle(bundle, c.inputScope, db.(rt.TxDB))
		must(e)
		out = txResultToObj(o)
	case "relation":
		o, e := rt.ReadBundle(bundle, c.relationsJObj, c.inputScope, db, []string{c.WithRelation}, nil)
		must(e)
		out = o
	default:
		o, e := rt.ExecuteBundle(bundle, c.inputScope, db)
		must(e)
		out = o
	}
	return conf.EncodeConformanceJSON(coerceIntsValue(out))
}

// txResultToObj presents the runtime's typed TransactionResult STRUCT as the canonical bc.Obj the
// write outType declares: committed / executed / shortCircuit / entity ALWAYS present
// (present-as-null), plus returnedRows ONLY when populated — mirroring the de-boxed ser_T0 output.
func txResultToObj(r rt.TransactionResult) *bc.Obj {
	out := bc.NewObj()
	out.Set("committed", r.Committed)
	execVals := make([]bc.Value, len(r.Executed))
	for i, e := range r.Executed {
		execVals[i] = e
	}
	out.Set("executed", execVals)
	if r.ShortCircuit != nil {
		sc := bc.NewObj()
		sc.Set("statementId", r.ShortCircuit.StatementID)
		sc.Set("reason", string(r.ShortCircuit.Reason))
		out.Set("shortCircuit", sc)
	} else {
		out.Set("shortCircuit", nil)
	}
	if r.Entity == nil {
		out.Set("entity", nil)
	} else {
		out.Set("entity", r.Entity)
	}
	if r.ReturnedRows != nil {
		rr := make([]bc.Value, len(r.ReturnedRows))
		for i, group := range r.ReturnedRows {
			rr[i] = group
		}
		out.Set("returnedRows", rr)
	}
	return out
}

// coerceIntsValue rewrites every INTEGRAL float64 to int64 (recursing through objects/arrays) so
// the two sides compare by VALUE. Representation-only; the row DATA is unchanged.
func coerceIntsValue(v bc.Value) bc.Value {
	switch t := v.(type) {
	case *bc.Obj:
		out := bc.NewObj()
		for _, k := range t.Keys {
			out.Set(k, coerceIntsValue(t.Vals[k]))
		}
		return out
	case []bc.Value:
		out := make([]bc.Value, len(t))
		for i, e := range t {
			out[i] = coerceIntsValue(e)
		}
		return out
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}
		return t
	default:
		return v
	}
}
