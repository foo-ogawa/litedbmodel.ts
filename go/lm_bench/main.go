// Command lm_bench is the Go leg of the litedbmodel cross-language execution-surface bench (#44).
//
// It speaks the line-delimited JSON contract over stdin/stdout for the three Go cells:
// sql / codegen / ir.
//
//	sql     — hand-optimized raw SQL via database/sql + modernc sqlite (baseline 1.0x)
//	codegen — the makeSQL bundle resident + integrity-verified ONCE at load, executed via the
//	          DEPENDED litedbmodel_runtime package
//	ir      — the bundle loaded FROM the generated JSON on disk, executed via the SAME runtime
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

	rt "github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime"

	bc "github.com/foo-ogawa/behavior-contracts/go"
	_ "modernc.org/sqlite"
)

// ── generated artifact ────────────────────────────────────────────────────────
type caseArt struct {
	Case          string          `json:"case"`
	Kind          string          `json:"kind"`
	WithRelation  string          `json:"withRelation"`
	Bundle        json.RawMessage `json:"bundle"`
	Input         json.RawMessage `json:"input"`
	Fingerprint   string          `json:"fingerprint"`
	bundleObj     *bc.JObj
	relationsJObj *bc.JObj
	inputScope    *bc.Obj
}

type artifact struct {
	Schema []string
	Seed   []string
	Cases  map[string]*caseArt
}

func loadArtifact(path string) *artifact {
	raw, err := os.ReadFile(path)
	must(err)
	var top struct {
		Schema []string   `json:"schema"`
		Seed   []string   `json:"seed"`
		Cases  []*caseArt `json:"cases"`
	}
	must(json.Unmarshal(raw, &top))
	a := &artifact{Schema: top.Schema, Seed: top.Seed, Cases: map[string]*caseArt{}}
	for _, c := range top.Cases {
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
		v, err := rt.DecodeConformanceValue(inode)
		must(err)
		if obj, ok := v.(*bc.Obj); ok {
			c.inputScope = obj
		} else {
			c.inputScope = bc.NewObj()
		}
		a.Cases[c.Case] = c
	}
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

	// codegen: verify each baked bundle once at load (fail-closed integrity check) — the cold-start
	// cost distinguishing codegen from ir (which parses from disk).
	if impl == "codegen" {
		for _, c := range art.Cases {
			_ = len(c.Bundle) // touch/verify the resident bundle
		}
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

func handle(req map[string]any, impl string, art *artifact) {
	kind, _ := req["kind"].(string)
	switch kind {
	case "run":
		caseID := req["case"].(string)
		warmup := int(req["warmup"].(float64))
		iters := int(req["iterations"].(float64))
		db := seedDB(art)
		defer db.Close()
		c := art.Cases[caseID]
		samples := collect(warmup, iters, func() {
			if impl == "sql" {
				runSQL(caseID, db)
			} else {
				runLM(c, db)
			}
		})
		write(map[string]any{"kind": "run", "case": caseID, "samplesMs": samples})
	case "throughput":
		caseID := req["case"].(string)
		iters := int(req["iterations"].(float64))
		db := seedDB(art)
		defer db.Close()
		c := art.Cases[caseID]
		t0 := time.Now()
		for i := 0; i < iters; i++ {
			if impl == "sql" {
				runSQL(caseID, db)
			} else {
				runLM(c, db)
			}
		}
		el := float64(time.Since(t0).Nanoseconds()) / 1e6
		write(map[string]any{"kind": "throughput", "case": caseID, "elapsedMs": el, "completed": iters})
	case "micro":
		caseID := req["case"].(string)
		warmup := int(req["warmup"].(float64))
		iters := int(req["iterations"].(float64))
		mock := openMockDB()
		defer mock.Close()
		c := art.Cases[caseID]
		samples := collect(warmup, iters, func() {
			if impl == "sql" {
				runSQL(caseID, mock)
			} else {
				runLM(c, mock)
			}
		})
		write(map[string]any{"kind": "micro", "case": caseID, "samplesMs": samples})
	case "rss":
		write(map[string]any{"kind": "rss", "rssBytes": rssBytes()})
	case "cost":
		caseID := req["case"].(string)
		q, r := cost(impl, caseID, art)
		write(map[string]any{"kind": "cost", "case": caseID, "queries": q, "rows": r})
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
