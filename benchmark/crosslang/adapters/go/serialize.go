// Canonical result serialization — hand-rolled (NO encoding/json in the codegen cell's dep graph), byte-
// matching oracle.ts canonVal/canonRow (int bare, string json-quoted) so native == SDK == oracle.
package main

import (
	"database/sql"
	"strconv"
	"strings"
)

func ki(k string, v int64) string { return jsonStr(k) + ":" + strconv.FormatInt(v, 10) }
func ks(k, v string) string       { return jsonStr(k) + ":" + jsonStr(v) }
func objj(fields ...string) string { return "{" + strings.Join(fields, ",") + "}" }
func arrj(rows []string) string    { return "[" + strings.Join(rows, ",") + "]" }

func userRow(id int64, email, name string) string { return objj(ki("id", id), ks("email", email), ks("name", name)) }
func postRow(id int64, title string, aid int64) string {
	return objj(ki("id", id), ks("title", title), ki("author_id", aid))
}
func commentRow(id int64, body string, pid int64) string {
	return objj(ki("id", id), ks("body", body), ki("post_id", pid))
}
func tuserRow(t, u int64, name string) string { return objj(ki("tenant_id", t), ki("user_id", u), ks("name", name)) }
func tpostRow(t, pid, u int64, title string) string {
	return objj(ki("tenant_id", t), ki("post_id", pid), ki("user_id", u), ks("title", title))
}
func relJSON(rel string, parents, childLists []string) string {
	return "{\"rows\":" + arrj(parents) + "," + jsonStr(rel) + ":" + arrj(childLists) + "}"
}

// users+posts snapshot — the affected-tables state a write/tx op emits (matches oracle.ts stateSnapshot).
func stateJSON(db *seamDB) string {
	users, _ := Query(db, "SELECT id, email, name FROM benchmark_users ORDER BY id", nil, func(r *sql.Rows) (string, error) {
		var id int64
		var e, n string
		if err := r.Scan(&id, &e, &n); err != nil {
			return "", err
		}
		return userRow(id, e, n), nil
	})
	posts, _ := Query(db, "SELECT id, title, author_id FROM benchmark_posts ORDER BY id", nil, func(r *sql.Rows) (string, error) {
		var id, a int64
		var t string
		if err := r.Scan(&id, &t, &a); err != nil {
			return "", err
		}
		return postRow(id, t, a), nil
	})
	return "{\"users\":" + arrj(users) + ",\"posts\":" + arrj(posts) + "}"
}
func txJSON(committed bool, db *seamDB) string {
	c := "false"
	if committed {
		c = "true"
	}
	return "{\"committed\":" + c + ",\"state\":" + stateJSON(db) + "}"
}
