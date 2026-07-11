package litedbmodel_runtime

import (
	"database/sql"
	"strings"
	"testing"

	bc "github.com/foo-ogawa/behavior-contracts/go"

	_ "modernc.org/sqlite"
)

func scope(pairs ...any) *bc.Obj {
	o := bc.NewObj()
	for i := 0; i < len(pairs); i += 2 {
		o.Set(pairs[i].(string), pairs[i+1])
	}
	return o
}

// statements parses a JSON array of static statement templates into []bc.JNode.
func statements(t *testing.T, s string) []bc.JNode {
	t.Helper()
	n, err := bc.ParseJSONOrdered([]byte(s))
	if err != nil {
		t.Fatalf("parse statements: %v", err)
	}
	arr, ok := n.([]bc.JNode)
	if !ok {
		t.Fatalf("statements is not an array")
	}
	return arr
}

// ── render: static makeSQL statements → SQL text + params (SKIP drop, connectors) ──

const feedStatements = `[
  {"sql":"SELECT id, author_id, status FROM posts","params":[]},
  {"sql":"author_id = ?","params":[{"ref":["author_id"]}],"whereFragment":true},
  {"sql":"status = ?","params":[{"ref":["status"]}],"whereFragment":true,"skip":{"not":[{"ne":[{"refOpt":["status"]},null]}]}},
  {"sql":" ORDER BY id ASC","params":[]},
  {"sql":" LIMIT ?","params":[{"coalesce":[{"refOpt":["limit"]},20]}]}
]`

func TestRenderStatementsPresentAndSkip(t *testing.T) {
	stmts := statements(t, feedStatements)

	// All present: WHERE author_id = ? AND status = ? ORDER BY … LIMIT ?
	r, err := renderStatements(stmts, "sqlite", scope("author_id", float64(7), "status", "live", "limit", float64(5)))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if r.SQL != "SELECT id, author_id, status FROM posts WHERE author_id = ? AND status = ? ORDER BY id ASC LIMIT ?" {
		t.Errorf("present render: got %q", r.SQL)
	}
	if len(r.Params) != 3 || r.Params[0] != float64(7) || r.Params[1] != "live" || r.Params[2] != float64(5) {
		t.Errorf("present params: got %v", r.Params)
	}

	// status absent (present-as-null) → skip drops it; coalesce defaults limit → 20.
	r2, _ := renderStatements(stmts, "sqlite", scope("author_id", float64(7), "status", nil, "limit", nil))
	if r2.SQL != "SELECT id, author_id, status FROM posts WHERE author_id = ? ORDER BY id ASC LIMIT ?" {
		t.Errorf("skip-drop render: got %q", r2.SQL)
	}
	if len(r2.Params) != 2 || r2.Params[0] != float64(7) || r2.Params[1] != float64(20) {
		t.Errorf("skip-drop params (coalesce default): got %v", r2.Params)
	}
}

func TestRenderPostgresDollarPlaceholders(t *testing.T) {
	stmts := statements(t, feedStatements)
	r, _ := renderStatements(stmts, "postgres", scope("author_id", float64(7), "status", "live", "limit", float64(5)))
	if r.SQL != "SELECT id, author_id, status FROM posts WHERE author_id = $1 AND status = $2 ORDER BY id ASC LIMIT $3" {
		t.Errorf("PG $N: got %q", r.SQL)
	}
}

func TestRenderInListSingleJsonParam(t *testing.T) {
	stmts := statements(t, `[
      {"sql":"SELECT id FROM posts","params":[]},
      {"sql":"id IN (SELECT value FROM json_each(?))","params":[{"__jsonArray":{"ref":["ids"]},"dialect":"sqlite"}],"whereFragment":true}
    ]`)
	r, _ := renderStatements(stmts, "sqlite", scope("ids", []bc.Value{int64(1), int64(2), int64(3)}))
	if r.SQL != "SELECT id FROM posts WHERE id IN (SELECT value FROM json_each(?))" {
		t.Errorf("IN-list SQL: got %q", r.SQL)
	}
	if len(r.Params) != 1 || r.Params[0] != "[1,2,3]" {
		t.Errorf("IN-list single JSON param: got %v", r.Params)
	}
}

func TestRenderPlaceholdersQuoteAware(t *testing.T) {
	if got := renderPlaceholders("SELECT '?' AS q WHERE a = ?", "postgres"); got != "SELECT '?' AS q WHERE a = $1" {
		t.Errorf("quote-aware PG: got %q", got)
	}
	if got := renderPlaceholders("a = ? AND b = ?", "sqlite"); got != "a = ? AND b = ?" {
		t.Errorf("sqlite untouched: got %q", got)
	}
}

func TestMysqlOrderByNullsEmulation(t *testing.T) {
	my, _ := DialectFor("mysql")
	if got := my.OrderByNulls("created_at", "DESC", "LAST"); got != "created_at IS NULL ASC, created_at DESC" {
		t.Errorf("mysql NULLS LAST: got %q", got)
	}
	if got := my.OrderByNulls("created_at", "ASC", "FIRST"); got != "created_at IS NULL DESC, created_at ASC" {
		t.Errorf("mysql NULLS FIRST: got %q", got)
	}
}

func TestUnknownDialectFailsClosed(t *testing.T) {
	if _, err := DialectFor("oracle"); err == nil {
		t.Errorf("unknown dialect must fail closed")
	}
}

// ── executeBundle: end-to-end read via bc RunBehavior + a makeSQL handler + real SQLite ──

const feedReadBundle = `{"dialect":"sqlite","name":"Feed","optionalHeads":["status","limit"],"relations":{},"readGraph":{"dialect":"sqlite","name":"Feed","optionalHeads":["status","limit"],"statementsById":{"n0":[
  {"sql":"SELECT id, author_id, status FROM posts","params":[]},
  {"sql":"author_id = ?","params":[{"ref":["author_id"]}],"whereFragment":true},
  {"sql":"status = ?","params":[{"ref":["status"]}],"whereFragment":true,"skip":{"not":[{"ne":[{"refOpt":["status"]},null]}]}},
  {"sql":" ORDER BY id ASC","params":[]},
  {"sql":" LIMIT ?","params":[{"coalesce":[{"refOpt":["limit"]},20]}]}
]},"ir":{"irVersion":1,"exprVersion":2,"components":[{"name":"Feed","inputPorts":{"author_id":{"required":true},"status":{"required":true},"limit":{"required":true}},"body":[{"id":"n0","component":"__makeSqlNode","ports":{"__scope":{"obj":{"author_id":{"ref":["author_id"]},"status":{"ref":["status"]},"limit":{"ref":["limit"]}}}}}],"output":{"ref":["n0"]},"plan":{"concurrency":16,"groups":[[0]]}}]}}}`

func seedFeed(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, s := range []string{
		`CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, status TEXT)`,
		`INSERT INTO posts VALUES (1, 7, 'live')`,
		`INSERT INTO posts VALUES (2, 7, 'draft')`,
		`INSERT INTO posts VALUES (3, 8, 'live')`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return db
}

func TestExecuteBundleStatusPresentAndAbsent(t *testing.T) {
	bundle, err := ParseBundle([]byte(feedReadBundle))
	if err != nil {
		t.Fatalf("parse bundle: %v", err)
	}
	db := seedFeed(t)
	defer db.Close()

	got := EncodeConformanceJSON(mustRun(t, bundle, scope("author_id", float64(7), "status", "live"), db))
	if got != `[{"id":1,"author_id":7,"status":"live"}]` {
		t.Errorf("status present: got %s", got)
	}

	got2 := EncodeConformanceJSON(mustRun(t, bundle, scope("author_id", float64(7)), db))
	if got2 != `[{"id":1,"author_id":7,"status":"live"},{"id":2,"author_id":7,"status":"draft"}]` {
		t.Errorf("status absent (SKIP drop): got %s", got2)
	}
}

func mustRun(t *testing.T, b *SqlBundle, input *bc.Obj, db SQLDB) bc.Value {
	t.Helper()
	v, err := ExecuteBundle(b, input, db)
	if err != nil {
		t.Fatalf("executeBundle: %v", err)
	}
	return v
}

// ── executeTransactionBundle: gate-first commit + short-circuit (spec §6) ─────

const txBundle = `{"dialect":"sqlite","name":"Create","optionalHeads":[],"relations":{},"transaction":{"phase":"create","entityFrom":"tx_body_1","onIdempotentHit":"rollback","statements":[
  {"id":"tx_requires_0","role":"gate:requires","gate":"existsElseRollback","label":"requires users","op":{"sql":"SELECT 1 FROM users WHERE id = ?","params":[{"ref":["author_id"]}]}},
  {"id":"tx_body_1","role":"body","label":"body","op":{"sql":"INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, author_id","params":[{"ref":["author_id"]},{"ref":["title"]}]}},
  {"id":"tx_derive_2","role":"derive","label":"derive","op":{"sql":"UPDATE users SET post_count = post_count + ? WHERE id = ?","params":[1,{"ref":["author_id"]}]}}
]}}`

func seedTx(t *testing.T) *sql.DB {
	t.Helper()
	db, _ := sql.Open("sqlite", ":memory:")
	for _, s := range []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, post_count INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER NOT NULL, title TEXT NOT NULL)`,
		`INSERT INTO users (id, post_count) VALUES (7, 2)`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return db
}

func TestExecuteTransactionCommit(t *testing.T) {
	bundle, err := ParseBundle([]byte(txBundle))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	db := seedTx(t)
	defer db.Close()

	res, err := ExecuteTransactionBundle(bundle, scope("author_id", float64(7), "title", "Hi"), db)
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	if !res.Committed {
		t.Fatalf("expected commit")
	}
	if len(res.Executed) != 3 {
		t.Errorf("all statements execute on commit: got %v", res.Executed)
	}
	var pc int64
	if err := db.QueryRow(`SELECT post_count FROM users WHERE id = 7`).Scan(&pc); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if pc != 3 {
		t.Errorf("derive increment: post_count=%d, want 3", pc)
	}
}

func TestExecuteTransactionGateShortCircuit(t *testing.T) {
	bundle, _ := ParseBundle([]byte(txBundle))
	db := seedTx(t)
	defer db.Close()

	res, err := ExecuteTransactionBundle(bundle, scope("author_id", float64(999), "title", "Orphan"), db)
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	if res.Committed {
		t.Fatalf("expected short-circuit, not commit")
	}
	if res.ShortCircuit == nil || res.ShortCircuit.Reason != ReasonRequiresAbsent {
		t.Errorf("expected requires_absent short-circuit, got %+v", res.ShortCircuit)
	}
	if len(res.Executed) != 1 || res.Executed[0] != "tx_requires_0" {
		t.Errorf("gate-first: only the requires gate runs, got %v", res.Executed)
	}
	var n int64
	db.QueryRow(`SELECT COUNT(*) FROM posts`).Scan(&n)
	if n != 0 {
		t.Errorf("gate-first: no body write, but posts has %d rows", n)
	}
	var pc int64
	db.QueryRow(`SELECT post_count FROM users WHERE id = 7`).Scan(&pc)
	if pc != 2 {
		t.Errorf("derive must not run on short-circuit: post_count=%d, want 2", pc)
	}
}

// M4 (re-audit): an UNKNOWN / forward-incompatible gate rule FAILS CLOSED (aligned with TS +
// Python + Rust + PHP): the tx returns an error and does NOT commit — a corrupt gate must never
// be silently skipped into a COMMIT.
const txBundleUnknownGate = `{"dialect":"sqlite","name":"Create","optionalHeads":[],"relations":{},"transaction":{"phase":"create","entityFrom":"tx_body_1","onIdempotentHit":"rollback","statements":[
  {"id":"tx_requires_0","role":"gate:requires","gate":"someFutureGateRuleThatDoesNotExist","label":"bogus gate","op":{"sql":"SELECT 1 FROM users WHERE id = ?","params":[{"ref":["author_id"]}]}},
  {"id":"tx_body_1","role":"body","label":"body","op":{"sql":"INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, author_id","params":[{"ref":["author_id"]},{"ref":["title"]}]}}
]}}`

func TestExecuteTransactionUnknownGateFailsClosed(t *testing.T) {
	bundle, _ := ParseBundle([]byte(txBundleUnknownGate))
	db := seedTx(t)
	defer db.Close()

	res, err := ExecuteTransactionBundle(bundle, scope("author_id", float64(7), "title", "X"), db)
	if err == nil {
		t.Fatalf("an unknown gate rule must fail closed (error), got committed=%v", res.Committed)
	}
	if !strings.Contains(err.Error(), "unknown gate rule") {
		t.Errorf("error should name the unknown gate rule, got: %v", err)
	}
	var n int64
	db.QueryRow(`SELECT COUNT(*) FROM posts`).Scan(&n)
	if n != 0 {
		t.Errorf("fail-closed: no commit, but posts has %d rows", n)
	}
}

// ── conformance value codec round-trip ────────────────────────────────────────

func TestConformanceCodec(t *testing.T) {
	if got := EncodeConformanceJSON(int64(5)); got != `{"$bigint":"5"}` {
		t.Errorf("int64 encode: got %s", got)
	}
	if got := EncodeConformanceJSON(float64(7)); got != `7` {
		t.Errorf("whole float encode: got %s", got)
	}
	n, _ := bc.ParseJSONOrdered([]byte(`{"$bigint":"4"}`))
	v, _ := DecodeConformanceValue(n)
	if v != int64(4) {
		t.Errorf("$bigint decode: got %T %v", v, v)
	}
	n2, _ := bc.ParseJSONOrdered([]byte(`7`))
	v2, _ := DecodeConformanceValue(n2)
	if v2 != float64(7) {
		t.Errorf("bare-int decode: got %T %v", v2, v2)
	}
}
