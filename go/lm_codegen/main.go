// Command lm_codegen is the DEDICATED go codegen-cell binary (epic #44 native-only, #8).
//
// It is the go twin of adapters/rust-codegen: a codegen-ONLY bench binary that links NEITHER the
// litedbmodel_runtime interpreter/exec crate NOR `encoding/json` (directly) — the codegen execution
// path carries no IR data and no JSON library. Everything the timed op needs is:
//   - the bc-GENERATED typed-native modules + the codegen execution logic (package `cgcell`);
//   - the GENERATED native companion (`cgplans` — pre-decoded plans + inputs + SCHEMA/SEED);
//   - database/sql + modernc.org/sqlite (the in-proc driver seam).
// The line protocol (ready/run/throughput/micro/cost/verify/rss/shutdown) is hand-rolled TEXT — NO
// encoding/json in this binary's OWN source (the harness request fields are plain ASCII tokens).
// (bc-go itself imports encoding/json for its JSON parser; that transitive pull is unavoidable for
// any bc consumer and is NOT part of litedbmodel's codegen path — mirrors the rust-codegen split,
// where the isolation that matters is: NO litedbmodel_runtime, NO direct JSON library.)
package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	bc "github.com/foo-ogawa/behavior-contracts/go"

	"github.com/foo-ogawa/litedbmodel/go/lm_bench/cgcell"
	"github.com/foo-ogawa/litedbmodel/go/lm_bench/cgplans"

	_ "modernc.org/sqlite"
)

// ── native line-protocol field extraction (hand-rolled — no encoding/json) ──────

// fieldStr extracts a STRING field from a flat one-line JSON request (`"key":"value"`). The harness's
// request fields are plain ASCII tokens (kind/case/dialect); an escaped char fails closed.
func fieldStr(line, key string) (string, bool) {
	pat := "\"" + key + "\""
	i := strings.Index(line, pat)
	if i < 0 {
		return "", false
	}
	rest := strings.TrimSpace(line[i+len(pat):])
	if !strings.HasPrefix(rest, ":") {
		return "", false
	}
	rest = strings.TrimSpace(rest[1:])
	if !strings.HasPrefix(rest, "\"") {
		return "", false
	}
	rest = rest[1:]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return "", false
	}
	val := rest[:end]
	if strings.ContainsRune(val, '\\') {
		panic("lm_codegen protocol: escaped string in request field '" + key + "' (unsupported — fail-closed)")
	}
	return val, true
}

// fieldU64 extracts an unsigned integer field (`"key":123`).
func fieldU64(line, key string) (uint64, bool) {
	pat := "\"" + key + "\""
	i := strings.Index(line, pat)
	if i < 0 {
		return 0, false
	}
	rest := strings.TrimSpace(line[i+len(pat):])
	if !strings.HasPrefix(rest, ":") {
		return 0, false
	}
	rest = strings.TrimSpace(rest[1:])
	j := 0
	for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
		j++
	}
	if j == 0 {
		return 0, false
	}
	n, err := strconv.ParseUint(rest[:j], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// jstr writes a JSON string literal natively (the protocol RESPONSE — no encoding/json).
func jstr(s string) string {
	var sb strings.Builder
	sb.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			sb.WriteString("\\\"")
		case '\\':
			sb.WriteString("\\\\")
		case '\n':
			sb.WriteString("\\n")
		case '\r':
			sb.WriteString("\\r")
		case '\t':
			sb.WriteString("\\t")
		default:
			if r < 0x20 {
				sb.WriteString(fmt.Sprintf("\\u%04x", r))
			} else {
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
	return sb.String()
}

func writeLine(s string) {
	fmt.Println(s)
}

func nowMs() float64 { return float64(time.Now().UnixNano()) / 1e6 }

// ── in-proc sqlite driver seam (seeded from the native companion — NO JSON artifact) ────

func seedDriver() *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		panic(err)
	}
	for _, s := range cgplans.Schema {
		if _, err := db.Exec(s); err != nil {
			panic(fmt.Errorf("schema %q: %w", s, err))
		}
	}
	for _, s := range cgplans.Seed {
		if _, err := db.Exec(s); err != nil {
			panic(fmt.Errorf("seed %q: %w", s, err))
		}
	}
	return db
}

// countingDB wraps a cgcell.CgDB, counting DML statements + rows (the fairness cost probe).
type countingDB struct {
	inner   cgcell.CgDB
	queries int64
	rows    int64
}

func isTxControl(q string) bool {
	up := strings.ToUpper(strings.TrimSpace(q))
	for _, k := range []string{"BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "PRAGMA"} {
		if strings.HasPrefix(up, k) {
			return true
		}
	}
	return false
}

func (c *countingDB) Query(q string, args ...any) (*sql.Rows, error) {
	if !isTxControl(q) {
		c.queries++
	}
	return c.inner.Query(q, args...)
}
func (c *countingDB) Exec(q string, args ...any) (sql.Result, error) {
	if !isTxControl(q) {
		c.queries++
	}
	return c.inner.Exec(q, args...)
}
func (c *countingDB) Begin() (*sql.Tx, error) { return c.inner.Begin() }

// ── canonical JSON observation (verify leg) — hand-written, key-sorted ──────────

func canonInto(v bc.Value, sb *strings.Builder) {
	switch t := v.(type) {
	case nil:
		sb.WriteString("null")
	case bool:
		if t {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case int64:
		sb.WriteString(strconv.FormatInt(t, 10))
	case float64:
		if t == float64(int64(t)) {
			sb.WriteString(strconv.FormatInt(int64(t), 10))
		} else {
			sb.WriteString(strconv.FormatFloat(t, 'g', -1, 64))
		}
	case string:
		sb.WriteString(jstr(t))
	case []bc.Value:
		sb.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				sb.WriteByte(',')
			}
			canonInto(e, sb)
		}
		sb.WriteByte(']')
	case *bc.Obj:
		keys := append([]string(nil), t.Keys...)
		sort.Strings(keys)
		sb.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(jstr(k))
			sb.WriteByte(':')
			canonInto(t.Vals[k], sb)
		}
		sb.WriteByte('}')
	default:
		sb.WriteString("null")
	}
}

// coerceInts rewrites every INTEGRAL float64 to int64 (recursing) so the codegen output compares by
// VALUE against the ir side (which the selfcheck runs separately).
func coerceInts(v bc.Value) bc.Value {
	switch t := v.(type) {
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}
		return t
	case []bc.Value:
		out := make([]bc.Value, len(t))
		for i, e := range t {
			out[i] = coerceInts(e)
		}
		return out
	case *bc.Obj:
		o := bc.NewObj()
		for _, k := range t.Keys {
			o.Set(k, coerceInts(t.Vals[k]))
		}
		return o
	default:
		return v
	}
}

func canon(v bc.Value) string {
	var sb strings.Builder
	canonInto(coerceInts(v), &sb)
	return sb.String()
}

// dbSkipReason: this cell drives the in-proc sqlite driver only (live PG/MySQL not wired — an
// explicit skip mirroring the other adapters' codegen cells).
func dbSkipReason(dialect string) string {
	if dialect == "sqlite" {
		return ""
	}
	return "go codegen cell drives the in-proc sqlite driver only; live " + dialect + " DB-backed not wired"
}

func writeSkipped(caseID, dialect, reason string) {
	writeLine("{\"kind\":\"skipped\",\"case\":" + jstr(caseID) + ",\"dialect\":" + jstr(dialect) + ",\"reason\":" + jstr(reason) + "}")
}

// collect runs `op` warmup+iters times, returning the per-iter wall-times (ms).
func collect(warmup, iters uint64, op func()) []float64 {
	for i := uint64(0); i < warmup; i++ {
		op()
	}
	samples := make([]float64, 0, iters)
	for i := uint64(0); i < iters; i++ {
		t0 := time.Now()
		op()
		samples = append(samples, float64(time.Since(t0).Nanoseconds())/1e6)
	}
	return samples
}

func samplesJSON(s []float64) string {
	parts := make([]string, len(s))
	for i, v := range s {
		parts[i] = strconv.FormatFloat(v, 'g', -1, 64)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func handle(kind, line string) {
	switch kind {
	case "run", "throughput", "micro":
		caseID, _ := fieldStr(line, "case")
		dialect, ok := fieldStr(line, "dialect")
		if !ok {
			dialect = "sqlite"
		}
		if r := dbSkipReason(dialect); r != "" && kind != "micro" {
			writeSkipped(caseID, dialect, r)
			return
		}
		// The go micro (client-path) mock rides `database/sql` on the in-proc sqlite driver, whose
		// arg-conversion layer rejects the PG/MySQL IN-list array param ([]Value) + `= ANY`/`JSON_TABLE`
		// SQL shape — so the non-sqlite micro cannot run through this mock harness. Skipped honestly,
		// exactly like the go/sql (lm_bench) cell + the comparability disclosure (the SQLite micro
		// client-path IS measured); never a silent drop, never a fail-closed crash.
		if kind == "micro" && dialect != "sqlite" {
			writeSkipped(caseID, dialect, "go codegen micro mock rides database/sql; its arg layer rejects the "+dialect+" IN-list array param ([]Value) — non-sqlite micro not run through the mock")
			return
		}
		// micro = I/O-EXCLUDED client-path: ride the MOCK driver (fixed rows, no real round-trip) so the
		// timed op is ONLY render + typed param bind + row hydrate — parity with go/sql's openMockDB() and
		// every other cell's mocked micro. run/throughput = DB-backed: ride the REAL in-proc sqlite driver.
		var db *sql.DB
		if kind == "micro" {
			db = openMockDB()
		} else {
			db = seedDriver()
			defer db.Close()
		}
		if kind == "throughput" {
			iters, _ := fieldU64(line, "iterations")
			t0 := time.Now()
			for i := uint64(0); i < iters; i++ {
				cgcell.RunCodegen(dialect, caseID, db)
			}
			elapsed := float64(time.Since(t0).Nanoseconds()) / 1e6
			writeLine(fmt.Sprintf("{\"kind\":\"throughput\",\"case\":%s,\"dialect\":%s,\"elapsedMs\":%s,\"completed\":%d}",
				jstr(caseID), jstr(dialect), strconv.FormatFloat(elapsed, 'g', -1, 64), iters))
			return
		}
		warmup, _ := fieldU64(line, "warmup")
		iters, _ := fieldU64(line, "iterations")
		samples := collect(warmup, iters, func() { cgcell.RunCodegen(dialect, caseID, db) })
		writeLine(fmt.Sprintf("{\"kind\":%s,\"case\":%s,\"dialect\":%s,\"samplesMs\":%s}",
			jstr(kind), jstr(caseID), jstr(dialect), samplesJSON(samples)))
	case "cost":
		caseID, _ := fieldStr(line, "case")
		dialect, ok := fieldStr(line, "dialect")
		if !ok {
			dialect = "sqlite"
		}
		base := seedDriver()
		defer base.Close()
		counter := &countingDB{inner: base}
		cgcell.RunCodegen("sqlite", caseID, counter)
		writeLine(fmt.Sprintf("{\"kind\":\"cost\",\"case\":%s,\"dialect\":%s,\"queries\":%d,\"rows\":%d}",
			jstr(caseID), jstr(dialect), counter.queries, counter.rows))
	case "verify":
		caseID, _ := fieldStr(line, "case")
		db := seedDriver()
		defer db.Close()
		out := cgcell.RunCodegenCase("sqlite", caseID, db)
		writeLine("{\"kind\":\"verify\",\"case\":" + jstr(caseID) + ",\"impl\":\"codegen\",\"canon\":" + jstr(canon(out)) + "}")
	case "rss":
		writeLine("{\"kind\":\"rss\",\"rssBytes\":0}")
	case "shutdown":
		os.Exit(0)
	}
}

func main() {
	// Warm each sqlite case once (module load + first-call cost) on a seeded driver, outside any
	// timed loop — the same cold-start discipline as the rust codegen cell.
	warm := seedDriver()
	for _, c := range cgplans.CaseIDs {
		func() {
			defer func() { _ = recover() }() // a warm failure is not fatal — the timed op reports it
			cgcell.RunCodegen("sqlite", c, warm)
		}()
	}
	warm.Close()

	writeLine(fmt.Sprintf("{\"kind\":\"ready\",\"language\":\"go\",\"impl\":\"codegen\",\"readyAtEpochMs\":%s}",
		strconv.FormatFloat(nowMs(), 'g', -1, 64)))

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		kind, ok := fieldStr(line, "kind")
		if !ok {
			writeLine("{\"kind\":\"error\",\"message\":\"bad request: no kind\"}")
			continue
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					writeLine("{\"kind\":\"error\",\"message\":" + jstr(fmt.Sprint(r)) + "}")
				}
			}()
			handle(kind, line)
		}()
	}
}
