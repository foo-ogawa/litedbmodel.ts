// Command livedb_runner is the Go leg of the litedbmodel SCP LIVE-DB conformance pass (WS7g #36).
//
// It loads the WS7g live-DB corpus (conformance/vectors-livedb/livedb.json — the exec/tx bundles
// compiled for `postgres` + `mysql`), connects to REAL dockerized Postgres + MySQL via the live
// database/sql seam (rt.OpenPostgres / rt.OpenMysql), creates the needed tables in an ISOLATED
// per-language namespace (Postgres schema / MySQL database `scp_go`), and runs each bundle through
// the SAME rt.ExecuteBundle / rt.ExecuteTransactionBundle the SQLite conformance uses. It asserts
// the assembled result equals the frozen SQLite reference (expectedResult / expectedDbState) — the
// §10 promise (same IR + input → same RESULT regardless of dialect).
//
// REAL DBs, no mock, NO silent skip: if PG/MySQL is unreachable it ERRORS OUT (exit 3). It prints
// the machine JSON summary as its LAST stdout line:
//
//	{"lang":"go-livedb","suites":{"livedb-pg":{..},"livedb-mysql":{..}},"total_pass",...}
//
// exit 0 all pass / 1 any fail / 2 corpus-version mismatch / 3 DB unreachable.
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	bc "github.com/foo-ogawa/behavior-contracts/go"
	rt "github.com/foo-ogawa/litedbmodel/go/litedbmodel_runtime"
)

const supportedCorpusVersion = 1

const (
	pgSchema = "scp_go"
	mysqlDB  = "scp_go"
)

// tables the corpus touches (drop dependents first).
var allTables = []string{"post_tags", "posts", "tags", "users", "idem", "uniq", "outbox"}

func corpusPath() string {
	if p := os.Getenv("LITEDBMODEL_LIVEDB_VECTORS"); p != "" {
		return p
	}
	wd, _ := os.Getwd()
	return filepath.Join(wd, "..", "conformance", "vectors-livedb", "livedb.json")
}

func getStr(o *bc.JObj, k string) string {
	if v, ok := o.Get(k); ok {
		s, _ := v.(string)
		return s
	}
	return ""
}

func mustGet(o *bc.JObj, k string) bc.JNode { v, _ := o.Get(k); return v }

func jstr(s string) string { b, _ := json.Marshal(s); return string(b) }

// canonicalJSON mirrors the vectors runner's canonicalizer (expected JNode → canonical string).
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

// ── schema lifecycle ───────────────────────────────────────────────────────────

func resetTables(db *sql.DB, schema []string, mysql bool) error {
	if mysql {
		if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
			return err
		}
	}
	for _, t := range allTables {
		drop := "DROP TABLE IF EXISTS " + t
		if !mysql {
			drop += " CASCADE"
		}
		if _, err := db.Exec(drop); err != nil {
			return fmt.Errorf("drop %s: %w", t, err)
		}
	}
	if mysql {
		if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
			return err
		}
	}
	for _, s := range schema {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("ddl %q: %w", s, err)
		}
	}
	return nil
}

func schemaStrings(v *bc.JObj, key string) []string {
	var out []string
	if arr, ok := mustGet(v, key).([]bc.JNode); ok {
		for _, e := range arr {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
	}
	return out
}

// ── per-dialect vector runs ──────────────────────────────────────────────────

func runExec(db *sql.DB, bundleObj *bc.JObj, v *bc.JObj) (bool, string) {
	bundle, err := rt.BundleFromJObj(bundleObj)
	if err != nil {
		return false, "bundle parse: " + err.Error()
	}
	scope, err := inputScope(mustGet(v, "input"))
	if err != nil {
		return false, "input decode: " + err.Error()
	}
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

func runTx(db *sql.DB, bundleObj *bc.JObj, v *bc.JObj) (bool, string) {
	bundle, err := rt.BundleFromJObj(bundleObj)
	if err != nil {
		return false, "bundle parse: " + err.Error()
	}
	scope, err := inputScope(mustGet(v, "input"))
	if err != nil {
		return false, "input decode: " + err.Error()
	}
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
		if states, ok := stN.([]bc.JNode); ok {
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
	}
	return true, ""
}

type tally struct{ Pass, Fail int }

func runDialectLeg(name string, db *sql.DB, mysql bool, vectors []*bc.JObj, bundleKey, schemaKey string) tally {
	var t tally
	fmt.Fprintf(os.Stderr, "\nlivedb-%s — %d vectors (real %s)\n", name, len(vectors), name)
	for _, v := range vectors {
		if err := resetTables(db, schemaStrings(v, schemaKey), mysql); err != nil {
			t.Fail++
			fmt.Fprintf(os.Stderr, "  XX  %s\n      reset: %v\n", getStr(v, "name"), err)
			continue
		}
		bundleObj, ok := mustGet(v, bundleKey).(*bc.JObj)
		if !ok {
			t.Fail++
			fmt.Fprintf(os.Stderr, "  XX  %s\n      %s is not an object\n", getStr(v, "name"), bundleKey)
			continue
		}
		var ok2 bool
		var detail string
		switch getStr(v, "kind") {
		case "exec":
			ok2, detail = runExec(db, bundleObj, v)
		case "tx":
			ok2, detail = runTx(db, bundleObj, v)
		default:
			ok2, detail = false, "unknown kind "+getStr(v, "kind")
		}
		if ok2 {
			t.Pass++
			fmt.Fprintf(os.Stderr, "  ok  %s\n", getStr(v, "name"))
		} else {
			t.Fail++
			fmt.Fprintf(os.Stderr, "  XX  %s\n      %s\n", getStr(v, "name"), detail)
		}
	}
	return t
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	fmt.Fprintln(os.Stderr, "litedbmodel SCP LIVE-DB conformance — Go runner (real PG + MySQL)")

	data, err := os.ReadFile(corpusPath())
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot load live-DB corpus: %v\n", err)
		fmt.Println(`{"lang":"go-livedb","suites":{},"total_pass":0,"total_fail":0,"version_mismatch":true}`)
		os.Exit(2)
	}
	root, err := bc.ParseJSONOrdered(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse: %v\n", err)
		os.Exit(2)
	}
	corpus, _ := root.(*bc.JObj)
	if getInt64(corpus, "corpusVersion") != supportedCorpusVersion {
		fmt.Fprintln(os.Stderr, "FAIL-CLOSED: corpusVersion mismatch")
		fmt.Println(`{"lang":"go-livedb","suites":{},"total_pass":0,"total_fail":0,"version_mismatch":true}`)
		os.Exit(2)
	}
	var vectors []*bc.JObj
	if vN, ok := corpus.Get("vectors"); ok {
		if arr, ok := vN.([]bc.JNode); ok {
			for _, x := range arr {
				if vo, ok := x.(*bc.JObj); ok {
					vectors = append(vectors, vo)
				}
			}
		}
	}

	// Postgres: connect to base testdb, create + enter the per-language schema.
	pgHost := env("TEST_DB_HOST", "localhost")
	pgPort := env("TEST_DB_PORT", "5433")
	pgDSN := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable&search_path=%s",
		env("TEST_DB_USER", "testuser"), env("TEST_DB_PASSWORD", "testpass"), pgHost, pgPort, env("TEST_DB_NAME", "testdb"), pgSchema)
	pg, err := rt.OpenPostgres(pgDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: Postgres unreachable at %s:%s — %v\n", pgHost, pgPort, err)
		os.Exit(3)
	}
	defer pg.Close()
	if _, err := pg.Exec("CREATE SCHEMA IF NOT EXISTS " + pgSchema); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: cannot create PG schema: %v\n", err)
		os.Exit(3)
	}
	if _, err := pg.Exec("SET search_path TO " + pgSchema); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: cannot set search_path: %v\n", err)
		os.Exit(3)
	}

	// MySQL: connect to base testdb, create the per-language database, reconnect into it.
	myHost := env("TEST_MYSQL_HOST", "127.0.0.1")
	myPort := env("TEST_MYSQL_PORT", "3307")
	bootDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		env("TEST_MYSQL_USER", "testuser"), env("TEST_MYSQL_PASSWORD", "testpass"), myHost, myPort, env("TEST_MYSQL_DB", "testdb"))
	boot, err := rt.OpenMysql(bootDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: MySQL unreachable at %s:%s — %v\n", myHost, myPort, err)
		os.Exit(3)
	}
	if _, err := boot.Exec("CREATE DATABASE IF NOT EXISTS " + mysqlDB); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: cannot create MySQL database: %v\n", err)
		os.Exit(3)
	}
	boot.Close()
	myDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		env("TEST_MYSQL_USER", "testuser"), env("TEST_MYSQL_PASSWORD", "testpass"), myHost, myPort, mysqlDB)
	my, err := rt.OpenMysql(myDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: MySQL (scp_go) unreachable — %v\n", err)
		os.Exit(3)
	}
	defer my.Close()

	pgT := runDialectLeg("pg", pg, false, vectors, "bundlePg", "schemaPg")
	myT := runDialectLeg("mysql", my, true, vectors, "bundleMysql", "schemaMysql")

	totalPass := pgT.Pass + myT.Pass
	totalFail := pgT.Fail + myT.Fail
	fmt.Fprintf(os.Stderr, "\n%d passed, %d failed / %d live-DB vectors\n", totalPass, totalFail, totalPass+totalFail)
	summary := fmt.Sprintf(`{"lang":"go-livedb","suites":{"livedb-pg":{"pass":%d,"fail":%d},"livedb-mysql":{"pass":%d,"fail":%d}},"total_pass":%d,"total_fail":%d,"version_mismatch":false}`,
		pgT.Pass, pgT.Fail, myT.Pass, myT.Fail, totalPass, totalFail)
	fmt.Println(summary)
	if totalFail > 0 {
		os.Exit(1)
	}
}

func getInt64(o *bc.JObj, k string) int64 {
	if v, ok := o.Get(k); ok {
		if dv, err := bc.DecodeValue(v); err == nil {
			if i, ok := dv.(int64); ok {
				return i
			}
		}
	}
	return 0
}
