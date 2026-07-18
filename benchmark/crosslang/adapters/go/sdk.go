// The go SDK BASELINE — raw driver + hand-SQL for benchmark_* (the fair 1.0x denominator). The SAME
// hand-SQL as the rust SDK (N+1-avoided parent+IN-child relations; single-CASE updateMany, not a per-row
// loop; BEGIN/COMMIT tx). Uses the seam's Query/Execute (prepared-stmt cache, matching native's driver).
package main

import (
	"database/sql"
	"strconv"
	"strings"
)

func sdkUsers(db *seamDB, query string, params []any) string {
	rows, _ := Query(db, query, params, func(r *sql.Rows) (string, error) {
		var id int64
		var e, n string
		if err := r.Scan(&id, &e, &n); err != nil {
			return "", err
		}
		return userRow(id, e, n), nil
	})
	return arrj(rows)
}
func sdkFindAll(db *seamDB) string {
	return sdkUsers(db, "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", nil)
}
func sdkFindFirst(db *seamDB) string {
	return sdkUsers(db, "SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1", []any{"User%"})
}
func sdkFindUnique(db *seamDB) string {
	return sdkUsers(db, "SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1", []any{"user500@example.com"})
}
func sdkFilterPaginateSort(db *seamDB) string {
	rows, _ := Query(db, "SELECT id, title, content, published, author_id, created_at FROM benchmark_posts WHERE published = ? ORDER BY created_at DESC LIMIT 20 OFFSET 10", []any{1}, func(r *sql.Rows) (string, error) {
		var id, pub, aid int64
		var title, created string
		var content sql.NullString
		if err := r.Scan(&id, &title, &content, &pub, &aid, &created); err != nil {
			return "", err
		}
		return objj(ki("id", id), ks("title", title), ks("content", content.String), ki("published", pub), ki("author_id", aid), ks("created_at", created)), nil
	})
	return arrj(rows)
}
func sdkCreate(db *seamDB) string {
	return sdkUsers(db, "INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id, email, name", []any{"new@bench.com", "New"})
}
func sdkUpdate(db *seamDB) string {
	return sdkUsers(db, "UPDATE benchmark_users SET name = ? WHERE id = ? RETURNING id, email, name", []any{"Updated 100", 100})
}
func sdkUpsert(db *seamDB) string {
	return sdkUsers(db, "INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id, email, name", []any{"user1@example.com", "Upserted One"})
}
func sdkInsertManyVALUES(db *seamDB, emails, names []string, conflict string) string {
	ph := make([]string, len(emails))
	params := make([]any, 0, len(emails)*2)
	for i := range emails {
		ph[i] = "(?, ?)"
		params = append(params, emails[i], names[i])
	}
	sql := "INSERT INTO benchmark_users (email, name) VALUES " + strings.Join(ph, ", ") + conflict + " RETURNING id, email, name"
	return sdkUsers(db, sql, params)
}
func sdkCreateMany(db *seamDB) string { return sdkInsertManyVALUES(db, batchEmails(), batchNames(), "") }
func sdkUpsertMany(db *seamDB) string {
	return sdkInsertManyVALUES(db, upsertManyEmails(), batchNames(), " ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name")
}
func sdkUpdateMany(db *seamDB) string {
	// hand-OPTIMIZED single CASE update (not a per-row loop).
	names := batchNames()
	cases := make([]string, 10)
	params := make([]any, 10)
	for i := 0; i < 10; i++ {
		cases[i] = "WHEN " + strconv.Itoa(i+1) + " THEN ?"
		params[i] = names[i]
	}
	sql := "UPDATE benchmark_users SET name = CASE id " + strings.Join(cases, " ") + " END WHERE id IN (1,2,3,4,5,6,7,8,9,10) RETURNING id, email, name"
	return sdkUsers(db, sql, params)
}

// read+rel: parent query + ONE batched IN child query + client-side group/align (N+1 avoided).
func sdkRelSingle(db *seamDB, parentSQL string, parentParams []any, parentScan func(*sql.Rows) (int64, string), childSQLFmt string, childScan func(*sql.Rows) (int64, string), rel string) string {
	parents, _ := Query(db, parentSQL, parentParams, func(r *sql.Rows) ([2]any, error) { k, j := parentScan(r); return [2]any{k, j}, nil })
	keys := make([]int64, len(parents))
	for i, p := range parents {
		keys[i] = p[0].(int64)
	}
	inlist := make([]string, len(keys))
	params := make([]any, len(keys))
	for i, k := range keys {
		inlist[i] = "?"
		params[i] = k
	}
	childSQL := strings.Replace(childSQLFmt, "{IN}", strings.Join(inlist, ","), 1)
	children, _ := Query(db, childSQL, params, func(r *sql.Rows) ([2]any, error) { k, j := childScan(r); return [2]any{k, j}, nil })
	groups := map[int64][]string{}
	for _, c := range children {
		k := c[0].(int64)
		groups[k] = append(groups[k], c[1].(string))
	}
	ps := make([]string, len(parents))
	cs := make([]string, len(parents))
	for i, p := range parents {
		ps[i] = p[1].(string)
		cs[i] = arrj(groups[p[0].(int64)])
	}
	return relJSON(rel, ps, cs)
}
func sdkNestedFindAll(db *seamDB) string {
	return sdkRelSingle(db, "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", nil,
		func(r *sql.Rows) (int64, string) { var id int64; var e, n string; r.Scan(&id, &e, &n); return id, userRow(id, e, n) },
		"SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN ({IN}) ORDER BY id ASC",
		func(r *sql.Rows) (int64, string) { var id, a int64; var t string; r.Scan(&id, &t, &a); return a, postRow(id, t, a) }, "posts")
}
func sdkNestedFindFirst(db *seamDB) string {
	return sdkRelSingle(db, "SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1", []any{"User%"},
		func(r *sql.Rows) (int64, string) { var id int64; var e, n string; r.Scan(&id, &e, &n); return id, userRow(id, e, n) },
		"SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN ({IN}) ORDER BY id ASC",
		func(r *sql.Rows) (int64, string) { var id, a int64; var t string; r.Scan(&id, &t, &a); return a, postRow(id, t, a) }, "posts")
}
func sdkNestedFindUnique(db *seamDB) string {
	return sdkRelSingle(db, "SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1", []any{"user1@example.com"},
		func(r *sql.Rows) (int64, string) { var id int64; var e, n string; r.Scan(&id, &e, &n); return id, userRow(id, e, n) },
		"SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN ({IN}) ORDER BY id ASC",
		func(r *sql.Rows) (int64, string) { var id, a int64; var t string; r.Scan(&id, &t, &a); return a, postRow(id, t, a) }, "posts")
}
func sdkNestedRelations(db *seamDB) string {
	return sdkRelSingle(db, "SELECT id, title, author_id FROM benchmark_posts WHERE author_id = ? ORDER BY id ASC", []any{7},
		func(r *sql.Rows) (int64, string) { var id, a int64; var t string; r.Scan(&id, &t, &a); return id, postRow(id, t, a) },
		"SELECT id, body, post_id FROM benchmark_comments WHERE post_id IN ({IN}) ORDER BY id ASC",
		func(r *sql.Rows) (int64, string) { var id, p int64; var b string; r.Scan(&id, &b, &p); return p, commentRow(id, b, p) }, "comments")
}
func sdkCompositeRelations(db *seamDB) string {
	parents, _ := Query(db, "SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = ? ORDER BY user_id ASC", []any{1}, func(r *sql.Rows) ([2]any, error) {
		var t, u int64
		var n string
		r.Scan(&t, &u, &n)
		return [2]any{u, tuserRow(t, u, n)}, nil
	})
	children, _ := Query(db, "SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE tenant_id = ? ORDER BY post_id ASC", []any{1}, func(r *sql.Rows) ([2]any, error) {
		var t, pid, u int64
		var ti string
		r.Scan(&t, &pid, &u, &ti)
		return [2]any{u, tpostRow(t, pid, u, ti)}, nil
	})
	groups := map[int64][]string{}
	for _, c := range children {
		groups[c[0].(int64)] = append(groups[c[0].(int64)], c[1].(string))
	}
	ps := make([]string, len(parents))
	cs := make([]string, len(parents))
	for i, p := range parents {
		ps[i] = p[1].(string)
		cs[i] = arrj(groups[p[0].(int64)])
	}
	return relJSON("posts", ps, cs)
}

// tx: raw BEGIN … COMMIT/ROLLBACK, then the {committed, state} snapshot.
func sdkDelete(db *seamDB) string {
	ok := Transaction(db, func() error {
		rows, err := Query(db, "INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id", []any{"del0@bench.com", "Del"}, func(r *sql.Rows) (int64, error) { var id int64; return id, r.Scan(&id) })
		if err != nil || len(rows) == 0 {
			return errOf("insert")
		}
		_, _, err = Execute(db, "DELETE FROM benchmark_users WHERE id = ?", []any{rows[0]})
		return err
	})
	return txJSON(ok, db)
}
func sdkNestedCreate(db *seamDB) string {
	ok := Transaction(db, func() error {
		rows, err := Query(db, "INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id", []any{"nc@bench.com", "NC"}, func(r *sql.Rows) (int64, error) { var id int64; return id, r.Scan(&id) })
		if err != nil || len(rows) == 0 {
			return errOf("insert")
		}
		_, _, err = Execute(db, "INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)", []any{rows[0], "NC Post"})
		return err
	})
	return txJSON(ok, db)
}
func sdkNestedUpdate(db *seamDB) string {
	ok := Transaction(db, func() error {
		if _, _, err := Execute(db, "UPDATE benchmark_users SET name = ? WHERE id = ?", []any{"NU", 7}); err != nil {
			return err
		}
		_, _, err := Execute(db, "UPDATE benchmark_posts SET title = ? WHERE author_id = ?", []any{"NU Post", 7})
		return err
	})
	return txJSON(ok, db)
}
func sdkNestedUpsert(db *seamDB) string {
	ok := Transaction(db, func() error {
		rows, err := Query(db, "INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id", []any{"user1@example.com", "NUp"}, func(r *sql.Rows) (int64, error) { var id int64; return id, r.Scan(&id) })
		if err != nil || len(rows) == 0 {
			return errOf("upsert")
		}
		_, _, err = Execute(db, "INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)", []any{rows[0], "NUp Post"})
		return err
	})
	return txJSON(ok, db)
}
