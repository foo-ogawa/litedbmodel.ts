package litedbmodel_runtime

import (
	"database/sql"
	"testing"

	bc "github.com/foo-ogawa/behavior-contracts/go"

	_ "modernc.org/sqlite"
)

// parseOp parses a compiled-operation JSON literal into a *bc.JObj.
func parseOp(t *testing.T, s string) *bc.JObj {
	t.Helper()
	n, err := bc.ParseJSONOrdered([]byte(s))
	if err != nil {
		t.Fatalf("parse op: %v", err)
	}
	o, ok := n.(*bc.JObj)
	if !ok {
		t.Fatalf("op is not an object")
	}
	return o
}

func scope(pairs ...any) *bc.Obj {
	o := bc.NewObj()
	for i := 0; i < len(pairs); i += 2 {
		o.Set(pairs[i].(string), pairs[i+1])
	}
	return o
}

// ── render: SKIP existence + empty-WHERE degeneration (spec §2/§3) ─────────────

func TestRenderSkipDropsFragmentAndCollapsesWhere(t *testing.T) {
	op := parseOp(t, `{"component":"Select","sql":"SELECT id FROM posts{where}","where":{"connector":"AND","fragments":[{"sql":"status = ?","params":[{"ref":["status"]}],"when":{"ne":[{"refOpt":["status"]},null]}}]},"params":[],"assembly":{"shape":"items"}}`)
	d, _ := DialectFor("sqlite")

	// status = null → fragment dropped → whole WHERE collapses (no ` WHERE `).
	r, err := RenderOperation(op, scope("status", nil), d)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if r.SQL != "SELECT id FROM posts" {
		t.Errorf("empty-WHERE degeneration: got %q", r.SQL)
	}
	if len(r.Params) != 0 {
		t.Errorf("dropped fragment must push no params, got %v", r.Params)
	}

	// status present → fragment kept.
	r2, _ := RenderOperation(op, scope("status", "live"), d)
	if r2.SQL != "SELECT id FROM posts WHERE status = ?" {
		t.Errorf("present fragment: got %q", r2.SQL)
	}
	if len(r2.Params) != 1 || r2.Params[0] != "live" {
		t.Errorf("present fragment param: got %v", r2.Params)
	}
}

// ── render: IN-list expansion + empty-array `1 = 0` degeneration (spec §5) ─────

func TestRenderInListExpansionAndEmptyDegeneration(t *testing.T) {
	op := parseOp(t, `{"component":"Select","sql":"SELECT id FROM posts{where}","where":{"connector":"AND","fragments":[{"always":true,"sql":"id IN (?)","params":[{"ref":["ids"]}],"expand":0}]},"params":[],"assembly":{"shape":"items"}}`)
	d, _ := DialectFor("sqlite")

	r, _ := RenderOperation(op, scope("ids", []bc.Value{int64(1), int64(2), int64(3)}), d)
	if r.SQL != "SELECT id FROM posts WHERE id IN (?, ?, ?)" {
		t.Errorf("IN-list N=3: got %q", r.SQL)
	}
	if len(r.Params) != 3 {
		t.Errorf("IN-list N=3 params: got %v", r.Params)
	}

	r2, _ := RenderOperation(op, scope("ids", []bc.Value{}), d)
	if r2.SQL != "SELECT id FROM posts WHERE 1 = 0" {
		t.Errorf("empty IN degeneration: got %q", r2.SQL)
	}
	if len(r2.Params) != 0 {
		t.Errorf("empty IN pushes no params, got %v", r2.Params)
	}
}

// ── dialect: `?`→`$N` PG one-pass over a fully-assembled statement (spec §8) ───

func TestPostgresDollarPlaceholderFinalPass(t *testing.T) {
	op := parseOp(t, `{"component":"Update","sql":"UPDATE users SET post_count = ?{where} RETURNING id","where":{"connector":"AND","fragments":[{"always":true,"sql":"id = ?","params":[{"ref":["id"]}]}]},"params":[{"add":[{"ref":["cur"]},1]}],"assembly":{"shape":"items"}}`)
	pg, _ := DialectFor("postgres")
	r, err := RenderOperation(op, scope("cur", int64(4), "id", int64(7)), pg)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if r.SQL != "UPDATE users SET post_count = $1 WHERE id = $2 RETURNING id" {
		t.Errorf("PG $N one-pass: got %q", r.SQL)
	}
	// bc arithmetic: add(4,1) → int64(5); the WHERE key is int64(7).
	if len(r.Params) != 2 || r.Params[0] != int64(5) || r.Params[1] != int64(7) {
		t.Errorf("PG params: got %v", r.Params)
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

// ── executeBundle: end-to-end read via bc RunBehavior + real SQLite ───────────

const feedBundle = `{"irVersion":1,"exprVersion":2,"dialect":"sqlite","component":{"body":[{"component":"Select","id":"n0","ports":{"__scope":{"obj":{"author_id":{"ref":["author_id"]},"status":{"ref":["status"]},"since":{"ref":["since"]},"limit":{"ref":["limit"]}}}}}],"inputPorts":{"author_id":{"required":true},"since":{"required":true},"status":{"required":true},"limit":{"required":true}},"name":"Feed","output":{"ref":["n0"]},"plan":{"concurrency":16,"groups":[[0]]}},"operations":{"n0":{"component":"Select","sql":"SELECT id, author_id, status FROM posts{where} ORDER BY id ASC LIMIT ?","where":{"connector":"AND","fragments":[{"always":true,"sql":"author_id = ?","params":[{"ref":["author_id"]}]},{"sql":"status = ?","params":[{"ref":["status"]}],"when":{"ne":[{"refOpt":["status"]},null]}}]},"params":[{"coalesce":[{"refOpt":["limit"]},20]}],"assembly":{"shape":"items"}}},"optionalHeads":["status","limit"],"relations":{}}`

func seedFeed(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	stmts := []string{
		`CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, status TEXT)`,
		`INSERT INTO posts VALUES (1, 7, 'live')`,
		`INSERT INTO posts VALUES (2, 7, 'draft')`,
		`INSERT INTO posts VALUES (3, 8, 'live')`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	return db
}

func TestExecuteBundleStatusPresentAndAbsent(t *testing.T) {
	bundle, err := ParseBundle([]byte(feedBundle))
	if err != nil {
		t.Fatalf("parse bundle: %v", err)
	}
	db := seedFeed(t)
	defer db.Close()

	// status present → SKIP fragment kept; only the 'live' post of author 7.
	got := EncodeConformanceJSON(mustRun(t, bundle, scope("author_id", float64(7), "status", "live", "since", "x"), db))
	if got != `[{"id":1,"author_id":7,"status":"live"}]` {
		t.Errorf("status present: got %s", got)
	}

	// status absent → normalizeInput fills status=null (optionalHeads) → fragment dropped → both posts.
	got2 := EncodeConformanceJSON(mustRun(t, bundle, scope("author_id", float64(7), "since", "x"), db))
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

const txBundle = `{"irVersion":1,"exprVersion":2,"dialect":"sqlite","component":{"body":[],"inputPorts":{},"name":"Create","output":null},"operations":{},"optionalHeads":[],"relations":{},"transaction":{"phase":"create","entityFrom":"tx_body_1","onIdempotentHit":"rollback","statements":[{"id":"tx_requires_0","role":"gate:requires","gate":"existsElseRollback","label":"requires users","op":{"component":"Select","sql":"SELECT 1 FROM users WHERE id = ?","where":null,"params":[{"ref":["author_id"]}],"assembly":{"shape":"items"}}},{"id":"tx_body_1","role":"body","label":"body","op":{"component":"Insert","sql":"INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, author_id","where":null,"params":[{"ref":["author_id"]},{"ref":["title"]}],"assembly":{"shape":"items"}}},{"id":"tx_derive_2","role":"derive","label":"derive","op":{"component":"Update","sql":"UPDATE users SET post_count = post_count + ?{where}","where":{"connector":"AND","fragments":[{"always":true,"sql":"id = ?","params":[{"ref":["author_id"]}]}]},"params":[1],"assembly":{"shape":"items"}}}]}}`

func seedTx(t *testing.T) *sql.DB {
	t.Helper()
	db, _ := sql.Open("sqlite", ":memory:")
	stmts := []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, post_count INTEGER NOT NULL DEFAULT 0)`,
		`CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER NOT NULL, title TEXT NOT NULL)`,
		`INSERT INTO users (id, post_count) VALUES (7, 2)`,
	}
	for _, s := range stmts {
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
	// derive incremented post_count 2 → 3.
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

	// author 999 does not exist → requires gate fails → ROLLBACK, tail never runs.
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
	// No body write happened.
	var n int64
	db.QueryRow(`SELECT COUNT(*) FROM posts`).Scan(&n)
	if n != 0 {
		t.Errorf("gate-first: no body write, but posts has %d rows", n)
	}
	// post_count unchanged (derive never ran).
	var pc int64
	db.QueryRow(`SELECT post_count FROM users WHERE id = 7`).Scan(&pc)
	if pc != 2 {
		t.Errorf("derive must not run on short-circuit: post_count=%d, want 2", pc)
	}
}

// ── conformance value codec round-trip ────────────────────────────────────────

func TestConformanceCodec(t *testing.T) {
	// int64 → $bigint; whole float64 → plain integer.
	if got := EncodeConformanceJSON(int64(5)); got != `{"$bigint":"5"}` {
		t.Errorf("int64 encode: got %s", got)
	}
	if got := EncodeConformanceJSON(float64(7)); got != `7` {
		t.Errorf("whole float encode: got %s", got)
	}
	// $bigint tag decodes to int64; bare integer decodes to float64 (a JS number).
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

// ── relation batch: belongsTo single + hasMany list, no N+1 (one query) ───────

func TestRunRelationOpBelongsToAndHasMany(t *testing.T) {
	db, _ := sql.Open("sqlite", ":memory:")
	defer db.Close()
	for _, s := range []string{
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`,
		`INSERT INTO users VALUES (7,'Ada'),(8,'Alan')`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	relObj := parseOp(t, `{"name":"author","kind":"belongsTo","parentKey":"author_id","targetKey":"id","query":{"component":"Select","sql":"SELECT id, name FROM users{where}","where":{"connector":"AND","fragments":[{"always":true,"sql":"id IN (?)","params":[{"ref":["__keys"]}],"expand":0}]},"params":[],"assembly":{"shape":"items"}}}`)
	op, err := relationOpFromJObj(relObj)
	if err != nil {
		t.Fatalf("rel parse: %v", err)
	}
	d, _ := DialectFor("sqlite")
	parents := []*bc.Obj{scope("author_id", float64(7)), scope("author_id", float64(8)), scope("author_id", float64(7))}
	sqlText, keys, batch, err := runRelationOp(op, parents, db, d)
	if err != nil {
		t.Fatalf("runRelationOp: %v", err)
	}
	// deduped keys → 2 → one batched IN (?, ?) query (structural no N+1).
	if sqlText != "SELECT id, name FROM users WHERE id IN (?, ?)" {
		t.Errorf("batch SQL: got %q", sqlText)
	}
	if len(keys) != 2 {
		t.Errorf("deduped keys: got %v", keys)
	}
	// distributeToParent belongsTo → single child.
	first := distributeToParent(op, parents[0], batch)
	obj, ok := first.(*bc.Obj)
	if !ok {
		t.Fatalf("belongsTo → single object, got %T", first)
	}
	if name, _ := obj.Get("name"); name != "Ada" {
		t.Errorf("author of 7: got %v", name)
	}
}
