// Command vectors_runner is the Go leg of the litedbmodel SCP conformance harness (WS7c, #32).
//
// It is the ENTRY POINT the cross-language orchestrator (conformance/vectors-run.ts) launches for
// the Go leg. It loads the FROZEN vector corpus (conformance/vectors/*.json), runs each vector
// through the Go litedbmodel_runtime — which CONSUMES the shared behavior-contracts Go core for
// all generic Expression-IR evaluation + the plan/map/wire/output orchestration — and asserts the
// Go runtime reproduces the SAME SQL text (all 3 dialects) + the SAME execution results (in-proc
// pure-Go SQLite, real) as the TS reference. It prints the SAME machine JSON summary as the TS
// runner as its LAST stdout line:
//
//	{"lang":"go","suites":{<suite>:{"pass","fail"}},"total_pass","total_fail","version_mismatch"}
//
// exit 0 (all pass) / 1 (any fail) / 2 (corpus-version mismatch).
//
// This is a REAL conformance runner: it renders + executes + asserts against the frozen expected
// fields; there is no hardcoded pass/skip. The DB seam is the standard database/sql surface, so a
// pgx/mysql driver plugs in later for the deferred live-PG/MySQL cross-language pass (spec §10) —
// the REQUIRED bar here is the vector corpus (SQL text × 3 dialects + in-proc SQLite results).
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"
	rt "github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime"

	_ "modernc.org/sqlite"
)

// supportedCorpusVersion is the corpus schema version this runner pins (bumped on additive refreeze).
const supportedCorpusVersion = 1

type tally struct {
	Pass int
	Fail int
}

// ── corpus loading ─────────────────────────────────────────────────────────────

func vectorsDir() string {
	if d := os.Getenv("LITEDBMODEL_VECTORS"); d != "" {
		return d
	}
	// default: <repo>/conformance/vectors relative to the go module (go/ → ../conformance/vectors).
	wd, _ := os.Getwd()
	return filepath.Join(wd, "..", "conformance", "vectors")
}

func getStr(o *bc.JObj, k string) string {
	if v, ok := o.Get(k); ok {
		s, _ := v.(string)
		return s
	}
	return ""
}

func getInt(o *bc.JObj, k string) int64 {
	if v, ok := o.Get(k); ok {
		if dv, err := bc.DecodeValue(v); err == nil {
			if i, ok := dv.(int64); ok {
				return i
			}
		}
	}
	return 0
}

func mustGet(o *bc.JObj, k string) bc.JNode {
	v, _ := o.Get(k)
	return v
}

func jstr(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// ── vector execution ─────────────────────────────────────────────────────────

func seedDB(schema []bc.JNode) (*sql.DB, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, err
	}
	for _, sN := range schema {
		s, _ := sN.(string)
		if _, err := db.Exec(s); err != nil {
			db.Close()
			return nil, fmt.Errorf("seed %q: %w", s, err)
		}
	}
	return db, nil
}

func inputScope(inputN bc.JNode) (*bc.Obj, error) {
	v, err := rt.DecodeConformanceValue(inputN)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return bc.NewObj(), nil
	}
	obj, ok := v.(*bc.Obj)
	if !ok {
		return nil, fmt.Errorf("vector input is not an object")
	}
	return obj, nil
}

func runRender(v *bc.JObj) (bool, string) {
	opN, _ := v.Get("operation")
	op, ok := opN.(*bc.JObj)
	if !ok {
		return false, "operation is not an object"
	}
	dialect, err := rt.DialectFor(getStr(v, "dialect"))
	if err != nil {
		return false, err.Error()
	}
	scope, err := inputScope(mustGet(v, "input"))
	if err != nil {
		return false, "input decode: " + err.Error()
	}
	rendered, err := rt.RenderOperation(op, scope, dialect)
	if err != nil {
		return false, "render threw: " + err.Error()
	}
	expectedSQL := getStr(v, "expectedSql")
	sqlOk := rendered.SQL == expectedSQL

	gotParams := encodeParamList(rendered.Params)
	wantParams := canonicalJSON(mustGet(v, "expectedParams"))
	paramsOk := gotParams == wantParams

	if sqlOk && paramsOk {
		return true, ""
	}
	var detail []string
	if !sqlOk {
		detail = append(detail, fmt.Sprintf("sql %q != %q", rendered.SQL, expectedSQL))
	}
	if !paramsOk {
		detail = append(detail, fmt.Sprintf("params %s != %s", gotParams, wantParams))
	}
	return false, strings.Join(detail, "; ")
}

func runExec(v *bc.JObj) (bool, string) {
	bundleObj, ok := mustGet(v, "bundle").(*bc.JObj)
	if !ok {
		return false, "bundle is not an object"
	}
	bundle, err := rt.BundleFromJObj(bundleObj)
	if err != nil {
		return false, "bundle parse: " + err.Error()
	}
	scope, err := inputScope(mustGet(v, "input"))
	if err != nil {
		return false, "input decode: " + err.Error()
	}
	schema, _ := mustGet(v, "schema").([]bc.JNode)
	db, err := seedDB(schema)
	if err != nil {
		return false, "seed: " + err.Error()
	}
	defer db.Close()

	result, err := rt.ExecuteBundle(bundle, scope, db)
	if err != nil {
		return false, "execute threw: " + err.Error()
	}
	got := rt.EncodeConformanceJSON(result)
	want := canonicalJSON(mustGet(v, "expectedResult"))
	if got == want {
		return true, ""
	}
	return false, fmt.Sprintf("result %s != %s", got, want)
}

func runTx(v *bc.JObj) (bool, string) {
	bundleObj, ok := mustGet(v, "bundle").(*bc.JObj)
	if !ok {
		return false, "bundle is not an object"
	}
	bundle, err := rt.BundleFromJObj(bundleObj)
	if err != nil {
		return false, "bundle parse: " + err.Error()
	}
	scope, err := inputScope(mustGet(v, "input"))
	if err != nil {
		return false, "input decode: " + err.Error()
	}
	schema, _ := mustGet(v, "schema").([]bc.JNode)
	db, err := seedDB(schema)
	if err != nil {
		return false, "seed: " + err.Error()
	}
	defer db.Close()

	result, err := rt.ExecuteTransactionBundle(bundle, scope, db)
	if err != nil {
		return false, "tx threw: " + err.Error()
	}
	got := encodeTxResult(result)
	want := canonicalJSON(mustGet(v, "expectedResult"))
	if got != want {
		return false, fmt.Sprintf("result %s != %s", got, want)
	}
	if stN, ok := v.Get("expectedDbState"); ok {
		states, _ := stN.([]bc.JNode)
		for _, sN := range states {
			s, _ := sN.(*bc.JObj)
			query := getStr(s, "query")
			rows, qerr := queryToJSON(db, query)
			if qerr != nil {
				return false, "db-state query threw: " + qerr.Error()
			}
			wantRows := canonicalJSON(mustGet(s, "rows"))
			if rows != wantRows {
				return false, fmt.Sprintf("db-state %q: %s != %s", query, rows, wantRows)
			}
		}
	}
	return true, ""
}

func runDialect(v *bc.JObj) (bool, string) {
	dialect, err := rt.DialectFor(getStr(v, "dialect"))
	if err != nil {
		return false, err.Error()
	}
	args, _ := mustGet(v, "args").(*bc.JObj)
	got := dialect.OrderByNulls(getStr(args, "expr"), getStr(args, "dir"), getStr(args, "nulls"))
	want := getStr(v, "expected")
	if got == want {
		return true, ""
	}
	return false, fmt.Sprintf("%q != %q", got, want)
}

// ── result encoding helpers ──────────────────────────────────────────────────

// encodeTxResult encodes a TransactionResult to the corpus JSON shape (write-runtime.ts
// TransactionResult, canonically encoded). Field order matches the TS object key order; an
// undefined shortCircuit is dropped by JSON.stringify, so it is omitted when nil.
func encodeTxResult(r rt.TransactionResult) string {
	parts := []string{fmt.Sprintf("\"committed\":%v", r.Committed)}
	if r.ShortCircuit != nil {
		parts = append(parts, fmt.Sprintf("\"shortCircuit\":{\"statementId\":%s,\"reason\":%s}",
			jstr(r.ShortCircuit.StatementID), jstr(string(r.ShortCircuit.Reason))))
	}
	if r.Entity == nil {
		parts = append(parts, "\"entity\":null")
	} else {
		parts = append(parts, "\"entity\":"+rt.EncodeConformanceJSON(r.Entity))
	}
	execParts := make([]string, len(r.Executed))
	for i, e := range r.Executed {
		execParts[i] = jstr(e)
	}
	parts = append(parts, "\"executed\":["+strings.Join(execParts, ",")+"]")
	return "{" + strings.Join(parts, ",") + "}"
}

func encodeParamList(params []bc.Value) string {
	parts := make([]string, len(params))
	for i, p := range params {
		parts[i] = rt.EncodeConformanceJSON(p)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// queryToJSON runs a DB-state query and encodes its rows to canonical JSON (a row column integer is
// a plain number, matching the corpus DB-row encoding).
func queryToJSON(db *sql.DB, query string) (string, error) {
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	var rowParts []string
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}
		obj := bc.NewObj()
		for i, c := range cols {
			obj.Set(c, rt.ScanConformanceValue(raw[i]))
		}
		rowParts = append(rowParts, rt.EncodeConformanceJSON(obj))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return "[" + strings.Join(rowParts, ",") + "]", nil
}

// canonicalJSON re-serializes an EXPECTED corpus JNode to the SAME canonical string the runtime's
// encoders produce, so the comparison is exact. A parsed corpus number is already in its canonical
// textual form (json.Number); a `{"$bigint"}` tag and structural objects/arrays pass through with
// key order preserved.
func canonicalJSON(n bc.JNode) string {
	switch t := n.(type) {
	case nil:
		return "null"
	case bool:
		if t {
			return "true"
		}
		return "false"
	case string:
		return jstr(t)
	case json.Number:
		return t.String()
	case []bc.JNode:
		parts := make([]string, len(t))
		for i, e := range t {
			parts[i] = canonicalJSON(e)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case *bc.JObj:
		parts := make([]string, 0, t.Len())
		for _, k := range t.Keys {
			parts = append(parts, jstr(k)+":"+canonicalJSON(t.Vals[k]))
		}
		return "{" + strings.Join(parts, ",") + "}"
	default:
		return "null"
	}
}

// ── main ─────────────────────────────────────────────────────────────────────

func runVector(v *bc.JObj) (bool, string) {
	switch getStr(v, "kind") {
	case "render":
		return runRender(v)
	case "exec":
		return runExec(v)
	case "tx":
		return runTx(v)
	case "dialect":
		return runDialect(v)
	default:
		return false, "unknown vector kind: " + getStr(v, "kind")
	}
}

type suiteData struct {
	name    string
	version int64
	vectors []*bc.JObj
}

func loadSuites(dir string) ([]suiteData, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".json" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	var suites []suiteData
	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", f, err)
		}
		root, err := bc.ParseJSONOrdered(data)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", f, err)
		}
		obj, _ := root.(*bc.JObj)
		if obj == nil {
			continue
		}
		var vecs []*bc.JObj
		if vN, ok := obj.Get("vectors"); ok {
			if arr, ok := vN.([]bc.JNode); ok {
				for _, x := range arr {
					if vo, ok := x.(*bc.JObj); ok {
						vecs = append(vecs, vo)
					}
				}
			}
		}
		suites = append(suites, suiteData{name: getStr(obj, "suite"), version: getInt(obj, "corpusVersion"), vectors: vecs})
	}
	return suites, nil
}

func main() {
	dir := vectorsDir()
	fmt.Fprintln(os.Stderr, "litedbmodel SCP conformance vectors — Go runner (consumes behavior-contracts Go core)")

	suites, err := loadSuites(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot load vectors from %s: %v\n", dir, err)
		fmt.Println(`{"lang":"go","suites":{},"total_pass":0,"total_fail":0,"version_mismatch":true}`)
		os.Exit(2)
	}

	// Pre-flight version sweep (fail-closed).
	var mismatched []string
	for _, s := range suites {
		if s.version != supportedCorpusVersion {
			mismatched = append(mismatched, s.name)
		}
	}
	if len(mismatched) > 0 {
		for _, s := range mismatched {
			fmt.Fprintf(os.Stderr, "FAIL-CLOSED: suite '%s' corpusVersion mismatch vs supported %d.\n", s, supportedCorpusVersion)
		}
		fmt.Println(`{"lang":"go","suites":{},"total_pass":0,"total_fail":0,"version_mismatch":true}`)
		os.Exit(2)
	}

	tallies := map[string]*tally{}
	var order []string
	totalPass, totalFail := 0, 0
	for _, s := range suites {
		t := &tally{}
		tallies[s.name] = t
		order = append(order, s.name)
		fmt.Fprintf(os.Stderr, "\n%s.json — %d vectors\n", s.name, len(s.vectors))
		for _, v := range s.vectors {
			ok, detail := runVector(v)
			name := getStr(v, "name")
			if ok {
				t.Pass++
				totalPass++
				fmt.Fprintf(os.Stderr, "  ok   %s\n", name)
			} else {
				t.Fail++
				totalFail++
				fmt.Fprintf(os.Stderr, "  FAIL %s\n      %s\n", name, detail)
			}
		}
	}

	fmt.Fprintf(os.Stderr, "\n%d passed, %d failed / %d vectors across %d suites\n", totalPass, totalFail, totalPass+totalFail, len(suites))

	sort.Strings(order)
	var suiteParts []string
	for _, name := range order {
		t := tallies[name]
		suiteParts = append(suiteParts, fmt.Sprintf("%s:{\"pass\":%d,\"fail\":%d}", jstr(name), t.Pass, t.Fail))
	}
	fmt.Printf("{\"lang\":\"go\",\"suites\":{%s},\"total_pass\":%d,\"total_fail\":%d,\"version_mismatch\":false}\n",
		strings.Join(suiteParts, ","), totalPass, totalFail)

	if totalFail > 0 {
		os.Exit(1)
	}
	os.Exit(0)
}
