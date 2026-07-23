// Command lm_orm — the raw-driver SDK-baseline ORM-bench cell (Go), twin of go/lm_bench/lm_orm_native.
//
// The apples-to-apples SDK comparison for the go native cell: it runs the SAME 19 ORM ops over the SAME
// canonical fixture (the codegen-owned behaviors.STATEMENTS DDL + behaviors.SEED — reused as FIXTURE
// setup, never for op execution), on the SAME in-memory sqlite storage the native cell uses
// (sql.Open("sqlite", ":memory:")) — but every op is HAND-WRITTEN SQL issued straight at database/sql.
// litedbmodel_runtime and the bc-generated RunNativeRawStruct_* runners are NOT in the path.
//
// Fairness (a strawman SDK invalidates the comparison):
//   - SAME storage: in-memory sqlite (no file → no fsync/WAL the native in-memory cell never pays).
//   - Prepared-statement REUSE: every op's SQL is prepared once and cached (map[string]*sql.Stmt),
//     matching native's runtime prepared-statement cache — not re-parsed per call.
//   - N+1-FREE relations: parent read → pluck keys → ONE batched child read (WHERE fk IN (…)) → group
//     in memory, the SAME query counts the native cell proves (nestedFindAll=2, nestedRelations=3,
//     compositeRelations=3, batch write=1, RETURNING-chained tx = BEGIN + 2 body + COMMIT = 4).
//   - SAME seed as the native twin: behaviors.STATEMENTS + behaviors.SEED, seeded ONCE (the native cell
//     seeds once too), and the SAME per-op inputs (findUnique=user500, update id=1, …).
//
// Modes:
//
//	lm_orm                  — run all 19 ops once; print per-op statement-count + row-count; assert the
//	                          N+1-free relation counts + the atomic tx statement counts (safety proof).
//	lm_orm bench [reps] [warmup]
//	                        — additionally time each op over reps iterations (after warmup) and print a
//	                          flat CSV (cell,op,iter,us) with cell label `sdk` — the go native format.
package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/foo-ogawa/litedbmodel/go/lm_bench/setup"

	_ "modernc.org/sqlite" // PURE-GO sqlite driver (registered as "sqlite") — the raw baseline
)

// ── the ONE exec seam. All DB access rides these methods, so the prepared-statement cache and the
//    per-op statement counter each live in exactly one place. ────────────────────────────────────────
type cell struct {
	db    *sql.DB
	stmts map[string]*sql.Stmt // per-SQL prepared-statement cache (reused across iterations)
	count int64                // statement counter (safety proof); bumped once per prepared statement
}

func (c *cell) prep(sqlText string) *sql.Stmt {
	if s, ok := c.stmts[sqlText]; ok {
		return s
	}
	s, err := c.db.Prepare(sqlText)
	if err != nil {
		panic(fmt.Sprintf("prepare %q: %v", sqlText, err))
	}
	c.stmts[sqlText] = s
	return s
}

// query runs a prepared SELECT and materialises EVERY column of every row (fair vs the native cell,
// which decodes full typed structs), returning each row as a []any. Only key columns are read
// downstream for batching, but all columns are scanned to pay the real decode cost.
func (c *cell) query(sqlText string, args ...any) [][]any {
	c.count++
	rows, err := c.prep(sqlText).Query(args...)
	if err != nil {
		panic(fmt.Sprintf("query %q: %v", sqlText, err))
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			panic(fmt.Sprintf("scan %q: %v", sqlText, err))
		}
		out = append(out, vals)
	}
	return out
}

// exec runs a prepared, parameterised write. Param-free control statements (BEGIN/COMMIT) go through
// execRaw so they never hit the prepared-statement path.
func (c *cell) exec(sqlText string, args ...any) {
	c.count++
	if _, err := c.prep(sqlText).Exec(args...); err != nil {
		panic(fmt.Sprintf("exec %q: %v", sqlText, err))
	}
}

// execRaw runs a param-free statement directly (BEGIN / COMMIT / ROLLBACK).
func (c *cell) execRaw(sqlText string) {
	c.count++
	if _, err := c.db.Exec(sqlText); err != nil {
		panic(fmt.Sprintf("exec %q: %v", sqlText, err))
	}
}

// insertReturningID inserts one row and returns its generated id via last_insert_rowid (sqlite).
func (c *cell) insertReturningID(sqlText string, args ...any) int64 {
	c.count++
	res, err := c.prep(sqlText).Exec(args...)
	if err != nil {
		panic(fmt.Sprintf("insert %q: %v", sqlText, err))
	}
	id, err := res.LastInsertId()
	if err != nil {
		panic(fmt.Sprintf("last_insert_id %q: %v", sqlText, err))
	}
	return id
}

// asInt coerces a scanned cell (modernc.org/sqlite returns INTEGER as int64) to int64.
func asInt(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	default:
		return 0
	}
}

// openSeeded opens a fresh in-memory sqlite (SAME storage as the native cell) and applies the ONE seed
// SSoT (.setup/sqlite.json, from orm-domain.ts) — the SAME fixture the native twin loads. It is shared
// setup, NOT the generated op runners (which the SDK bypasses entirely).
func openSeeded() *cell {
	doc, err := setup.Load("sqlite")
	if err != nil {
		panic(err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(1) // one in-memory connection so schema + seed + ops share the same DB
	db.SetMaxIdleConns(1)
	for _, group := range [][]string{doc.Schema, doc.Delete, doc.Insert} {
		for _, s := range group {
			if _, err := db.Exec(s); err != nil {
				panic(fmt.Sprintf("setup %q: %v", s, err))
			}
		}
	}
	return &cell{db: db, stmts: map[string]*sql.Stmt{}}
}

// ── batch-write inputs (mirror the native cell's userRows / the ops SSoT) ────────────────────────────
func batchRows(it int, stable bool) (emails, names []string) {
	emails = make([]string, 10)
	names = make([]string, 10)
	for i := 0; i < 10; i++ {
		if stable {
			emails[i] = fmt.Sprintf("many%d@bench.com", i)
		} else {
			emails[i] = fmt.Sprintf("many%d_%d@bench.com", it, i)
		}
		names[i] = fmt.Sprintf("Many %d", i)
	}
	return
}

// ── read helpers: ONE batched child query per level, grouped in memory (N+1-free). ──────────────────
func (c *cell) nestedPostsFor(users [][]any) {
	ids := pluck(users, 0)
	if len(ids) == 0 {
		return
	}
	sqlText := fmt.Sprintf(
		"SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN (%s) ORDER BY id ASC",
		placeholders(len(ids)))
	posts := c.query(sqlText, intArgs(ids)...)
	groupBy(posts, 2) // author_id at col2
}

func (c *cell) nestedPostsCollectIDs(users [][]any) []int64 {
	ids := pluck(users, 0)
	if len(ids) == 0 {
		return nil
	}
	sqlText := fmt.Sprintf(
		"SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN (%s) ORDER BY id ASC",
		placeholders(len(ids)))
	posts := c.query(sqlText, intArgs(ids)...)
	groupBy(posts, 2)
	return pluck(posts, 0)
}

func (c *cell) batchedComments(postIDs []int64) {
	if len(postIDs) == 0 {
		return
	}
	sqlText := fmt.Sprintf(
		"SELECT id, body, post_id FROM benchmark_comments WHERE post_id IN (%s) ORDER BY id ASC",
		placeholders(len(postIDs)))
	comments := c.query(sqlText, intArgs(postIDs)...)
	groupBy(comments, 2) // post_id at col2
}

// compositeRelations: tenant_users(tenant=1) → batched tenant_posts by (tenant_id,user_id) → batched
// tenant_comments by (tenant_id,post_id). 3 queries.
func (c *cell) compositeRelations() {
	tusers := c.query(
		"SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = ? ORDER BY user_id ASC", 1)
	if len(tusers) == 0 {
		return
	}
	pbody := tupleIn(len(tusers), 2)
	psql := "SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE (tenant_id, user_id) IN " + pbody
	pparams := make([]any, 0, len(tusers)*2)
	for _, r := range tusers {
		pparams = append(pparams, asInt(r[0]), asInt(r[1])) // tenant_id, user_id
	}
	tposts := c.query(psql, pparams...)
	if len(tposts) == 0 {
		return
	}
	cbody := tupleIn(len(tposts), 2)
	csql := "SELECT tenant_id, comment_id, post_id, body FROM benchmark_tenant_comments WHERE (tenant_id, post_id) IN " + cbody
	cparams := make([]any, 0, len(tposts)*2)
	for _, r := range tposts {
		cparams = append(cparams, asInt(r[0]), asInt(r[1])) // tenant_id, post_id
	}
	c.query(csql, cparams...)
}

// updateMany: ONE statement (CASE id … END WHERE id IN (…)) — single-statement, N+1-avoided.
func (c *cell) updateMany() {
	_, names := batchRows(0, false)
	var whens strings.Builder
	params := make([]any, 0, 30)
	for k := 0; k < 10; k++ {
		whens.WriteString(" WHEN ? THEN ?")
		params = append(params, int64(k+1), names[k])
	}
	for k := 0; k < 10; k++ {
		params = append(params, int64(k+1))
	}
	sqlText := fmt.Sprintf(
		"UPDATE benchmark_users SET name = CASE id%s END WHERE id IN (%s)",
		whens.String(), placeholders(10))
	c.exec(sqlText, params...)
}

// ── the 19 ops (native-cell order). Fixed inputs mirror the go native cell; mutating ops vary their
//    UNIQUE column by it. Reads: LIMIT/ORDER shapes match the ops SSoT (== the native generated SQL). ──
func (c *cell) op(name string, it int) {
	switch name {
	case "findAll":
		c.query("SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100")
	case "filterPaginateSort":
		c.query("SELECT id, title, content, published, author_id, created_at FROM benchmark_posts "+
			"WHERE published = ? ORDER BY created_at DESC LIMIT 20 OFFSET 10", 1)
	case "findFirst":
		c.query("SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1", "User%")
	case "findUnique":
		c.query("SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1", "user500@example.com")
	case "nestedFindAll":
		users := c.query("SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100")
		c.nestedPostsFor(users)
	case "nestedFindFirst":
		users := c.query("SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1", "User%")
		c.nestedPostsFor(users)
	case "nestedFindUnique":
		users := c.query("SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1", "user1@example.com")
		c.nestedPostsFor(users)
	case "nestedRelations":
		users := c.query("SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100")
		postIDs := c.nestedPostsCollectIDs(users)
		c.batchedComments(postIDs)
	case "compositeRelations":
		c.compositeRelations()
	case "create":
		c.exec("INSERT INTO benchmark_users (email, name) VALUES (?, ?)", fmt.Sprintf("new%d@bench.com", it), "New")
	case "update":
		c.exec("UPDATE benchmark_users SET name = ? WHERE id = ?", "Updated 1", 1)
	case "upsert":
		c.exec("INSERT INTO benchmark_users (email, name) VALUES (?, ?) "+
			"ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name",
			"user1@example.com", "Upserted One")
	case "createMany":
		emails, names := batchRows(it, false)
		c.batchInsert(emails, names, "")
	case "upsertMany":
		emails := []string{"user1@example.com", "user2@example.com"}
		for k := 0; k < 8; k++ {
			emails = append(emails, fmt.Sprintf("many%d@bench.com", k))
		}
		_, names := batchRows(it, true)
		c.batchInsert(emails, names,
			" ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name")
	case "updateMany":
		c.updateMany()
	case "nestedCreate":
		c.execRaw("BEGIN")
		uid := c.insertReturningID("INSERT INTO benchmark_users (email, name) VALUES (?, ?)",
			fmt.Sprintf("nc%d@bench.com", it), "NC")
		c.exec("INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)", uid, "NC Post")
		c.execRaw("COMMIT")
	case "nestedUpsert":
		c.execRaw("BEGIN")
		c.exec("INSERT INTO benchmark_users (email, name) VALUES (?, ?) "+
			"ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name",
			"user1@example.com", "NUp")
		rows := c.query("SELECT id FROM benchmark_users WHERE email = ?", "user1@example.com")
		uid := asInt(rows[0][0])
		c.exec("INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)", uid, "NUp Post")
		c.execRaw("COMMIT")
	case "nestedUpdate":
		c.execRaw("BEGIN")
		c.exec("UPDATE benchmark_users SET name = ? WHERE id = ?", "NU", 1)
		c.exec("UPDATE benchmark_posts SET title = ? WHERE author_id = ?", "NU Post", 1)
		c.execRaw("COMMIT")
	case "delete":
		c.execRaw("BEGIN")
		uid := c.insertReturningID("INSERT INTO benchmark_users (email, name) VALUES (?, ?)",
			fmt.Sprintf("del%d@bench.com", it), "Del")
		c.exec("DELETE FROM benchmark_users WHERE id = ?", uid)
		c.execRaw("COMMIT")
	default:
		panic("unknown op " + name)
	}
}

// batchInsert: ONE multi-row INSERT for the 10 rows (N+1-avoided), optional ON CONFLICT tail.
func (c *cell) batchInsert(emails, names []string, conflict string) {
	tuples := make([]string, 10)
	params := make([]any, 0, 20)
	for k := 0; k < 10; k++ {
		tuples[k] = "(?, ?)"
		params = append(params, emails[k], names[k])
	}
	sqlText := "INSERT INTO benchmark_users (email, name) VALUES " + strings.Join(tuples, ",") + conflict
	c.exec(sqlText, params...)
}

// ── small SQL helpers ────────────────────────────────────────────────────────────────────────────────
func placeholders(n int) string { return strings.TrimSuffix(strings.Repeat("?,", n), ",") }

// tupleIn builds a row-tuple IN body sqlite accepts: (VALUES (?,?),(?,?),…).
func tupleIn(rows, cols int) string {
	one := "(" + placeholders(cols) + ")"
	return "(VALUES " + strings.TrimSuffix(strings.Repeat(one+",", rows), ",") + ")"
}

func pluck(rows [][]any, col int) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = asInt(r[col])
	}
	return out
}

func intArgs(ids []int64) []any {
	out := make([]any, len(ids))
	for i, v := range ids {
		out[i] = v
	}
	return out
}

// groupBy stitches child rows by their parent-key column (in-memory, mirrors the runtime distribute).
func groupBy(rows [][]any, keyCol int) {
	m := make(map[int64][]int, len(rows))
	for idx, r := range rows {
		k := asInt(r[keyCol])
		m[k] = append(m[k], idx)
	}
	_ = m
}

var ops = []string{
	"findAll", "filterPaginateSort", "findFirst", "findUnique",
	"nestedFindAll", "nestedFindFirst", "nestedFindUnique", "nestedRelations", "compositeRelations",
	"create", "update", "upsert",
	"createMany", "upsertMany", "updateMany",
	"nestedCreate", "nestedUpsert", "nestedUpdate", "delete",
}

// expectedStatements — the per-op hand-issued statement count (reads + writes + tx-control BEGIN/COMMIT;
// pluck/group are in-memory and do NOT issue statements). Matches the native cell's expectations.
var expectedStatements = map[string]int{
	"findAll": 1, "filterPaginateSort": 1, "findFirst": 1, "findUnique": 1,
	"nestedFindAll": 2, "nestedFindFirst": 2, "nestedFindUnique": 2, "nestedRelations": 3, "compositeRelations": 3,
	"create": 1, "update": 1, "upsert": 1,
	"createMany": 1, "upsertMany": 1, "updateMany": 1,
	"nestedCreate": 4, "nestedUpsert": 5, "nestedUpdate": 4, "delete": 4,
}

var txOps = map[string]bool{"nestedCreate": true, "nestedUpsert": true, "nestedUpdate": true, "delete": true}

func main() {
	doBench := len(os.Args) > 1 && os.Args[1] == "bench"

	c := openSeeded()
	defer c.db.Close()

	fmt.Println("op                    statements  rows")
	fail := 0
	for _, name := range ops {
		c.count = 0
		c.op(name, 0)
		q := int(c.count)
		mark := "ok"
		if exp, okk := expectedStatements[name]; okk && exp != q {
			mark = fmt.Sprintf("STATEMENT-COUNT MISMATCH (want %d)", exp)
			fail++
		}
		kind := ""
		if txOps[name] {
			kind = " (BEGIN + body + COMMIT)"
		}
		fmt.Printf("%-20s  %-10d  %s%s\n", name, q, mark, kind)
	}

	if doBench {
		reps := 300
		warmup := 30
		if len(os.Args) > 2 {
			if n, e := strconv.Atoi(os.Args[2]); e == nil {
				reps = n
			}
		}
		if len(os.Args) > 3 {
			if n, e := strconv.Atoi(os.Args[3]); e == nil {
				warmup = n
			}
		}
		fmt.Println("\ncell,op,iter,us")
		for _, name := range ops {
			for it := 0; it < warmup; it++ {
				c.op(name, it+1)
			}
			for it := 0; it < reps; it++ {
				g := it + warmup + 1
				t := time.Now()
				c.op(name, g)
				fmt.Printf("sdk,%s,%d,%d\n", name, it, time.Since(t).Microseconds())
			}
		}
	}

	if fail > 0 {
		fmt.Fprintf(os.Stderr, "\nFAILED: %d op(s) mismatched.\n", fail)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "\nOK: 19 ops ran; relation counts N+1-free; batch writes = 1 statement; tx = BEGIN + body + COMMIT.")
}
