// The go SDK BASELINE — raw driver + hand-SQL for benchmark_* (the fair 1.0x denominator, the ONLY
// hand-written execution besides the leaf handlers). Reads the materialized wire rows DIRECTLY by column
// name (NOT via the generated de-box — that is the native path). Dialect-aware (the go twin of the rust
// SDK): pg `$N` + native arrays; sqlite/mysql `?`; mysql has no RETURNING so upsert/tx-chain inserts
// route the SAME re-select marker through the seam. v1-faithful returning (upsert→[{id}], no-returning
// writes→null, tx→{committed,state}).
package main

import (
	"strconv"
	"strings"

	"github.com/lib/pq"
	"orm_bench_go/wire"
)

// ph — the n-th positional placeholder for the dialect (`$n` pg / `?` sqlite+mysql).
func ph(db *wire.DB, n int) string {
	if db.Dialect == "postgres" {
		return "$" + strconv.Itoa(n)
	}
	return "?"
}
func sdkUserRows(db *wire.DB, sqlText string, params []any) string {
	rows, _ := wire.QueryRows(db, sqlText, params)
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = wire.UserRow(r.Int("id"), r.Str("email"), r.Str("name"))
	}
	return wire.Arrj(out)
}
func sdkNull(db *wire.DB, sqlText string, params []any) string {
	_, _ = wire.ExecuteNull(db, sqlText, params)
	return "null"
}

func sdkFindAll(db *wire.DB) string {
	return sdkUserRows(db, "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", nil)
}
func sdkFindFirst(db *wire.DB) string {
	return sdkUserRows(db, "SELECT id, email, name FROM benchmark_users WHERE name LIKE "+ph(db, 1)+" LIMIT 1", []any{"User%"})
}
func sdkFindUnique(db *wire.DB) string {
	return sdkUserRows(db, "SELECT id, email, name FROM benchmark_users WHERE email = "+ph(db, 1)+" LIMIT 1", []any{"user500@example.com"})
}
func sdkFilterPaginateSort(db *wire.DB) string {
	rows, _ := wire.QueryRows(db, "SELECT id, title, content, published, author_id, created_at FROM benchmark_posts WHERE published = "+ph(db, 1)+" ORDER BY created_at DESC LIMIT 20 OFFSET 10", []any{1})
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = wire.Objj(wire.Ki("id", r.Int("id")), wire.Ks("title", r.Str("title")), wire.Ks("content", r.Str("content")), wire.Ki("published", r.Int("published")), wire.Ki("author_id", r.Int("author_id")), wire.Ks("created_at", r.Str("created_at")))
	}
	return wire.Arrj(out)
}
func sdkCreate(db *wire.DB) string {
	return sdkNull(db, "INSERT INTO benchmark_users (email, name) VALUES ("+ph(db, 1)+", "+ph(db, 2)+")", []any{"new@bench.com", "New"})
}
func sdkUpdate(db *wire.DB) string {
	return sdkNull(db, "UPDATE benchmark_users SET name = "+ph(db, 1)+" WHERE id = "+ph(db, 2), []any{"Updated 100", 100})
}

// The upsert baseline RETURNS the pk only (v1 {returning:true}); per-dialect conflict tail (mysql uses
// ON DUPLICATE KEY + the SAME re-select marker the native path uses → identical seam emulation).
func sdkUpsertSQL(db *wire.DB) string {
	if db.Dialect == "mysql" {
		return "INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name) /*scp-reselect: SELECT id FROM benchmark_users WHERE email = ? ORDER BY id ::binds:: p0*/"
	}
	return "INSERT INTO benchmark_users (email, name) VALUES (" + ph(db, 1) + ", " + ph(db, 2) + ") ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id"
}
func sdkIDRows(rows []wire.RowData) string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = wire.Objj(wire.Ki("id", r.Int("id")))
	}
	return wire.Arrj(out)
}
func sdkUpsert(db *wire.DB) string {
	rows, _ := wire.QueryRows(db, sdkUpsertSQL(db), []any{"user1@example.com", "Upserted One"})
	return sdkIDRows(rows)
}

// The per-dialect upsertMany conflict tail (NO returning).
func upsertManyTail(db *wire.DB) string {
	if db.Dialect == "mysql" {
		return " ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name)"
	}
	return " ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name"
}
func sdkInsertManyVALUES(db *wire.DB, emails, names []string, tail string) string {
	tuples := make([]string, len(emails))
	params := make([]any, 0, len(emails)*2)
	for i := range emails {
		tuples[i] = "(" + ph(db, 2*i+1) + ", " + ph(db, 2*i+2) + ")"
		params = append(params, emails[i], names[i])
	}
	return sdkNull(db, "INSERT INTO benchmark_users (email, name) VALUES "+strings.Join(tuples, ", ")+tail, params)
}
func sdkCreateMany(db *wire.DB) string {
	return sdkInsertManyVALUES(db, wire.BatchEmails(), wire.BatchNames(), "")
}
func sdkUpsertMany(db *wire.DB) string {
	return sdkInsertManyVALUES(db, wire.UpsertManyEmails(), wire.BatchNames(), upsertManyTail(db))
}
func sdkUpdateMany(db *wire.DB) string {
	// hand-OPTIMIZED single CASE update (not a per-row loop); NO returning (v1) → null.
	names := wire.BatchNames()
	cases := make([]string, 10)
	params := make([]any, 10)
	for i := 0; i < 10; i++ {
		cases[i] = "WHEN " + strconv.Itoa(i+1) + " THEN " + ph(db, i+1)
		params[i] = names[i]
	}
	return sdkNull(db, "UPDATE benchmark_users SET name = CASE id "+strings.Join(cases, " ")+" END WHERE id IN (1,2,3,4,5,6,7,8,9,10)", params)
}

// read+rel: parent query + ONE batched IN child query + client-side group/align (N+1 avoided).
// pg binds ONE `= ANY($1::int[])` native array; sqlite/mysql bind an `IN (?,?,…)` list.
func sdkChildInClause(db *wire.DB, keys []int64) (string, []any) {
	if db.Dialect == "postgres" {
		return "= ANY($1::int[])", []any{pq.Array(keys)}
	}
	marks := make([]string, len(keys))
	params := make([]any, len(keys))
	for i, k := range keys {
		marks[i] = "?"
		params[i] = k
	}
	return "IN (" + strings.Join(marks, ",") + ")", params
}
func sdkRelSingle(db *wire.DB, parentSQL string, parentParams []any, parentKey string, parentSer func(wire.RowData) string, childSQLTmpl, childKey string, childSer func(wire.RowData) string, rel string) string {
	parents, _ := wire.QueryRows(db, parentSQL, parentParams)
	keys := make([]int64, len(parents))
	for i, r := range parents {
		keys[i] = r.Int(parentKey)
	}
	inClause, childParams := sdkChildInClause(db, keys)
	children, _ := wire.QueryRows(db, strings.Replace(childSQLTmpl, "{IN}", inClause, 1), childParams)
	groups := map[int64][]string{}
	for _, c := range children {
		groups[c.Int(childKey)] = append(groups[c.Int(childKey)], childSer(c))
	}
	ps := make([]string, len(parents))
	cs := make([]string, len(parents))
	for i, r := range parents {
		ps[i] = parentSer(r)
		cs[i] = wire.Arrj(groups[r.Int(parentKey)])
	}
	return wire.RelJSON(rel, ps, cs)
}
func userSer(r wire.RowData) string { return wire.UserRow(r.Int("id"), r.Str("email"), r.Str("name")) }
func postSer(r wire.RowData) string {
	return wire.PostRow(r.Int("id"), r.Str("title"), r.Int("author_id"))
}
func commentSer(r wire.RowData) string {
	return wire.CommentRow(r.Int("id"), r.Str("body"), r.Int("post_id"))
}
func sdkNestedFindAll(db *wire.DB) string {
	return sdkRelSingle(db, "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", nil, "id", userSer,
		"SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC", "author_id", postSer, "posts")
}
func sdkNestedFindFirst(db *wire.DB) string {
	return sdkRelSingle(db, "SELECT id, email, name FROM benchmark_users WHERE name LIKE "+ph(db, 1)+" LIMIT 1", []any{"User%"}, "id", userSer,
		"SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC", "author_id", postSer, "posts")
}
func sdkNestedFindUnique(db *wire.DB) string {
	return sdkRelSingle(db, "SELECT id, email, name FROM benchmark_users WHERE email = "+ph(db, 1)+" LIMIT 1", []any{"user1@example.com"}, "id", userSer,
		"SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC", "author_id", postSer, "posts")
}
func sdkNestedRelations(db *wire.DB) string {
	return sdkRelSingle(db, "SELECT id, title, author_id FROM benchmark_posts WHERE author_id = "+ph(db, 1)+" ORDER BY id ASC", []any{7}, "id", postSer,
		"SELECT id, body, post_id FROM benchmark_comments WHERE post_id {IN} ORDER BY id ASC", "post_id", commentSer, "comments")
}
func sdkCompositeRelations(db *wire.DB) string {
	parents, _ := wire.QueryRows(db, "SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = "+ph(db, 1)+" ORDER BY user_id ASC", []any{1})
	children, _ := wire.QueryRows(db, "SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE tenant_id = "+ph(db, 1)+" ORDER BY post_id ASC", []any{1})
	groups := map[int64][]string{}
	for _, c := range children {
		groups[c.Int("user_id")] = append(groups[c.Int("user_id")], wire.TPostRow(c.Int("tenant_id"), c.Int("post_id"), c.Int("user_id"), c.Str("title")))
	}
	ps := make([]string, len(parents))
	cs := make([]string, len(parents))
	for i, r := range parents {
		ps[i] = wire.TUserRow(r.Int("tenant_id"), r.Int("user_id"), r.Str("name"))
		cs[i] = wire.Arrj(groups[r.Int("user_id")])
	}
	return wire.RelJSON("posts", ps, cs)
}

// tx — raw BEGIN … COMMIT/ROLLBACK, then the {committed, state} snapshot. The insert-returning-id uses
// the SAME per-dialect form as native (mysql → LAST_INSERT_ID range marker).
func sdkInsertUserIDSQL(db *wire.DB) string {
	if db.Dialect == "mysql" {
		return "INSERT INTO benchmark_users (email, name) VALUES (?, ?) /*scp-reselect: SELECT id FROM benchmark_users WHERE id >= ? AND id < ? ORDER BY id ::binds:: L,H*/"
	}
	return "INSERT INTO benchmark_users (email, name) VALUES (" + ph(db, 1) + ", " + ph(db, 2) + ") RETURNING id"
}
func sdkRecoverID(db *wire.DB, sqlText string, params []any) (int64, bool) {
	rows, err := wire.QueryRows(db, sqlText, params)
	if err != nil || len(rows) == 0 {
		return 0, false
	}
	return rows[0].Int("id"), true
}
func sdkDelete(db *wire.DB) string {
	ok := wire.Transaction(db, func() error {
		id, done := sdkRecoverID(db, sdkInsertUserIDSQL(db), []any{"del0@bench.com", "Del"})
		if !done {
			return errOf("insert")
		}
		_, _, err := wire.Execute(db, "DELETE FROM benchmark_users WHERE id = "+ph(db, 1), []any{id})
		return err
	})
	return wire.TxJSON(ok, db)
}
func sdkNestedCreate(db *wire.DB) string {
	ok := wire.Transaction(db, func() error {
		id, done := sdkRecoverID(db, sdkInsertUserIDSQL(db), []any{"nc@bench.com", "NC"})
		if !done {
			return errOf("insert")
		}
		_, _, err := wire.Execute(db, "INSERT INTO benchmark_posts (author_id, title) VALUES ("+ph(db, 1)+", "+ph(db, 2)+")", []any{id, "NC Post"})
		return err
	})
	return wire.TxJSON(ok, db)
}
func sdkNestedUpdate(db *wire.DB) string {
	ok := wire.Transaction(db, func() error {
		if _, _, err := wire.Execute(db, "UPDATE benchmark_users SET name = "+ph(db, 1)+" WHERE id = "+ph(db, 2), []any{"NU", 7}); err != nil {
			return err
		}
		_, _, err := wire.Execute(db, "UPDATE benchmark_posts SET title = "+ph(db, 1)+" WHERE author_id = "+ph(db, 2), []any{"NU Post", 7})
		return err
	})
	return wire.TxJSON(ok, db)
}
func sdkNestedUpsert(db *wire.DB) string {
	ok := wire.Transaction(db, func() error {
		id, done := sdkRecoverID(db, sdkUpsertSQL(db), []any{"user1@example.com", "NUp"})
		if !done {
			return errOf("upsert")
		}
		_, _, err := wire.Execute(db, "INSERT INTO benchmark_posts (author_id, title) VALUES ("+ph(db, 1)+", "+ph(db, 2)+")", []any{id, "NUp Post"})
		return err
	})
	return wire.TxJSON(ok, db)
}

func sdkCell(op string, db *wire.DB) string {
	switch op {
	case "findAll":
		return sdkFindAll(db)
	case "filterPaginateSort":
		return sdkFilterPaginateSort(db)
	case "findFirst":
		return sdkFindFirst(db)
	case "findUnique":
		return sdkFindUnique(db)
	case "create":
		return sdkCreate(db)
	case "update":
		return sdkUpdate(db)
	case "upsert":
		return sdkUpsert(db)
	case "createMany":
		return sdkCreateMany(db)
	case "upsertMany":
		return sdkUpsertMany(db)
	case "updateMany":
		return sdkUpdateMany(db)
	case "nestedFindAll":
		return sdkNestedFindAll(db)
	case "nestedFindFirst":
		return sdkNestedFindFirst(db)
	case "nestedFindUnique":
		return sdkNestedFindUnique(db)
	case "nestedRelations":
		return sdkNestedRelations(db)
	case "compositeRelations":
		return sdkCompositeRelations(db)
	case "delete":
		return sdkDelete(db)
	case "nestedCreate":
		return sdkNestedCreate(db)
	case "nestedUpdate":
		return sdkNestedUpdate(db)
	case "nestedUpsert":
		return sdkNestedUpsert(db)
	}
	panic("sdk: unknown op " + op)
}
