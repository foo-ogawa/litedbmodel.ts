package wire

// Canonical result serialization — hand-rolled (NO encoding/json in the cell's dep graph), byte-matching
// oracle.ts canonVal/canonRow (int bare, string json-quoted). Shared by every per-op companion + the SDK.

import (
	"strconv"
	"strings"
)

func JsonStr(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString("\\\"")
		case '\\':
			b.WriteString("\\\\")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		default:
			if r < 0x20 {
				h := strconv.FormatInt(int64(r), 16)
				for len(h) < 4 {
					h = "0" + h
				}
				b.WriteString("\\u" + h)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func Ki(k string, v int64) string  { return JsonStr(k) + ":" + strconv.FormatInt(v, 10) }
func Ks(k, v string) string        { return JsonStr(k) + ":" + JsonStr(v) }
func Objj(fields ...string) string { return "{" + strings.Join(fields, ",") + "}" }
func Arrj(rows []string) string    { return "[" + strings.Join(rows, ",") + "]" }

// B2i canonicalizes a pg BOOLEAN projection to the 0/1 int the oracle's canonVal emits (sqlite/mysql read
// the column as int already; pg reads it as bool). The go twin of the rust cell's AsI64.
func B2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func UserRow(id int64, email, name string) string {
	return Objj(Ki("id", id), Ks("email", email), Ks("name", name))
}
func PostRow(id int64, title string, aid int64) string {
	return Objj(Ki("id", id), Ks("title", title), Ki("author_id", aid))
}
func CommentRow(id int64, body string, pid int64) string {
	return Objj(Ki("id", id), Ks("body", body), Ki("post_id", pid))
}
func TUserRow(t, u int64, name string) string {
	return Objj(Ki("tenant_id", t), Ki("user_id", u), Ks("name", name))
}
func TPostRow(t, pid, u int64, title string) string {
	return Objj(Ki("tenant_id", t), Ki("post_id", pid), Ki("user_id", u), Ks("title", title))
}
func RelJSON(rel string, parents, childLists []string) string {
	return "{\"rows\":" + Arrj(parents) + "," + JsonStr(rel) + ":" + Arrj(childLists) + "}"
}

// StateJSON — the users+posts affected-tables snapshot a write/tx op emits (matches oracle.ts). Read by
// id (dialect-independent) via the seam materializer.
func StateJSON(db *DB) string {
	users, _ := db.materialize("SELECT id, email, name FROM benchmark_users ORDER BY id", nil)
	posts, _ := db.materialize("SELECT id, title, author_id FROM benchmark_posts ORDER BY id", nil)
	us := make([]string, len(users))
	for i, r := range users {
		us[i] = UserRow(r.Int("id"), r.Str("email"), r.Str("name"))
	}
	ps := make([]string, len(posts))
	for i, r := range posts {
		ps[i] = PostRow(r.Int("id"), r.Str("title"), r.Int("author_id"))
	}
	return "{\"users\":" + Arrj(us) + ",\"posts\":" + Arrj(ps) + "}"
}
func TxJSON(committed bool, db *DB) string {
	c := "false"
	if committed {
		c = "true"
	}
	return "{\"committed\":" + c + ",\"state\":" + StateJSON(db) + "}"
}

// ── fixed inputs (match ops.ts / oracle.ts) ──
func BatchEmails() []string {
	v := make([]string, 10)
	for i := range v {
		v[i] = "many" + strconv.Itoa(i) + "@bench.com"
	}
	return v
}
func BatchNames() []string {
	v := make([]string, 10)
	for i := range v {
		v[i] = "Many " + strconv.Itoa(i)
	}
	return v
}
func UpsertManyEmails() []string {
	v := []string{"user1@example.com", "user2@example.com"}
	for i := 0; i < 8; i++ {
		v = append(v, "many"+strconv.Itoa(i)+"@bench.com")
	}
	return v
}
