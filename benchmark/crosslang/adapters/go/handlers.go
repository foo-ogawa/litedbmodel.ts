// The go-native HANDLERS — one Handler_<Comp> impl per op, running the CLI-generated module's baked SQL
// via the generic seam. The go twin of the rust cell's leaf handlers. Native = the generated module
// (never hand-edited); this glue is the only hand-written execution besides the SDK baseline.
package main

import (
	"database/sql"
	"strconv"
	"strings"

	"orm_bench_go/generated/compositerelations"
	"orm_bench_go/generated/create"
	"orm_bench_go/generated/createmany"
	delete_op "orm_bench_go/generated/delete"
	"orm_bench_go/generated/filterpaginatesort"
	"orm_bench_go/generated/findall"
	"orm_bench_go/generated/findfirst"
	"orm_bench_go/generated/findunique"
	"orm_bench_go/generated/nestedcreate"
	"orm_bench_go/generated/nestedfindall"
	"orm_bench_go/generated/nestedfindfirst"
	"orm_bench_go/generated/nestedfindunique"
	"orm_bench_go/generated/nestedrelations"
	"orm_bench_go/generated/nestedupdate"
	"orm_bench_go/generated/nestedupsert"
	"orm_bench_go/generated/update"
	"orm_bench_go/generated/updatemany"
	"orm_bench_go/generated/upsert"
	"orm_bench_go/generated/upsertmany"
)

func encInts(ks []int64) string {
	p := make([]string, len(ks))
	for i, k := range ks {
		p[i] = strconv.FormatInt(k, 10)
	}
	return "[" + strings.Join(p, ",") + "]"
}
func encPairs(ks [][2]int64) string {
	p := make([]string, len(ks))
	for i, k := range ks {
		p[i] = "[" + strconv.FormatInt(k[0], 10) + "," + strconv.FormatInt(k[1], 10) + "]"
	}
	return "[" + strings.Join(p, ",") + "]"
}
func userRows[T any](out []T, id func(T) int64, email, name func(T) string) string {
	rows := make([]string, len(out))
	for i, r := range out {
		rows[i] = userRow(id(r), email(r), name(r))
	}
	return arrj(rows)
}

// ── reads ──
type findAllH struct{ db *seamDB }
func (h findAllH) Node_FindAll_n0(p findall.PortsNR_FindAll_n0, _ *string) (findall.Row_FindAll_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0}, func(r *sql.Rows) (findall.T0, error) { var t findall.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return findall.Row_FindAll_n0{IsError: true, Err: e.Error()}, true }
	return findall.Row_FindAll_n0{Val: v}, true
}
func nativeFindAll(db *seamDB) string {
	out, _ := findall.RunNativeRawStruct_FindAll(findAllH{db}, findall.In_FindAll{})
	return userRows(out, func(u findall.T0) int64 { return u.Id }, func(u findall.T0) string { return u.Email }, func(u findall.T0) string { return u.Name })
}

type findFirstH struct{ db *seamDB }
func (h findFirstH) Node_FindFirst_n0(p findfirst.PortsNR_FindFirst_n0, _ *string) (findfirst.Row_FindFirst_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (findfirst.T0, error) { var t findfirst.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return findfirst.Row_FindFirst_n0{IsError: true, Err: e.Error()}, true }
	return findfirst.Row_FindFirst_n0{Val: v}, true
}
func nativeFindFirst(db *seamDB) string {
	out, _ := findfirst.RunNativeRawStruct_FindFirst(findFirstH{db}, findfirst.In_FindFirst{Name: "User%"})
	return userRows(out, func(u findfirst.T0) int64 { return u.Id }, func(u findfirst.T0) string { return u.Email }, func(u findfirst.T0) string { return u.Name })
}

type findUniqueH struct{ db *seamDB }
func (h findUniqueH) Node_FindUnique_n0(p findunique.PortsNR_FindUnique_n0, _ *string) (findunique.Row_FindUnique_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (findunique.T0, error) { var t findunique.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return findunique.Row_FindUnique_n0{IsError: true, Err: e.Error()}, true }
	return findunique.Row_FindUnique_n0{Val: v}, true
}
func nativeFindUnique(db *seamDB) string {
	out, _ := findunique.RunNativeRawStruct_FindUnique(findUniqueH{db}, findunique.In_FindUnique{Email: "user500@example.com"})
	return userRows(out, func(u findunique.T0) int64 { return u.Id }, func(u findunique.T0) string { return u.Email }, func(u findunique.T0) string { return u.Name })
}

type fpsH struct{ db *seamDB }
func (h fpsH) Node_FilterPaginateSort_n0(p filterpaginatesort.PortsNR_FilterPaginateSort_n0, _ *string) (filterpaginatesort.Row_FilterPaginateSort_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1, p.P2}, func(r *sql.Rows) (filterpaginatesort.T0, error) {
		var t filterpaginatesort.T0
		var content sql.NullString
		err := r.Scan(&t.Id, &t.Title, &content, &t.Published, &t.Author_id, &t.Created_at)
		t.Content = content.String
		return t, err
	})
	if e != nil { return filterpaginatesort.Row_FilterPaginateSort_n0{IsError: true, Err: e.Error()}, true }
	return filterpaginatesort.Row_FilterPaginateSort_n0{Val: v}, true
}
func nativeFilterPaginateSort(db *seamDB) string {
	out, _ := filterpaginatesort.RunNativeRawStruct_FilterPaginateSort(fpsH{db}, filterpaginatesort.In_FilterPaginateSort{Published: 1})
	rows := make([]string, len(out))
	for i, r := range out {
		rows[i] = objj(ki("id", r.Id), ks("title", r.Title), ks("content", r.Content), ki("published", r.Published), ki("author_id", r.Author_id), ks("created_at", r.Created_at))
	}
	return arrj(rows)
}

// ── writes ──
type createH struct{ db *seamDB }
func (h createH) Node_Create_n0(p create.PortsNR_Create_n0, _ *string) (create.Row_Create_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (create.T0, error) { var t create.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return create.Row_Create_n0{IsError: true, Err: e.Error()}, true }
	return create.Row_Create_n0{Val: v}, true
}
func nativeCreate(db *seamDB) string {
	out, _ := create.RunNativeRawStruct_Create(createH{db}, create.In_Create{Email: "new@bench.com", Name: "New"})
	return userRows(out, func(u create.T0) int64 { return u.Id }, func(u create.T0) string { return u.Email }, func(u create.T0) string { return u.Name })
}

type updateH struct{ db *seamDB }
func (h updateH) Node_Update_n0(p update.PortsNR_Update_n0, _ *string) (update.Row_Update_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (update.T0, error) { var t update.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return update.Row_Update_n0{IsError: true, Err: e.Error()}, true }
	return update.Row_Update_n0{Val: v}, true
}
func nativeUpdate(db *seamDB) string {
	out, _ := update.RunNativeRawStruct_Update(updateH{db}, update.In_Update{Name: "Updated 100", Id: 100})
	return userRows(out, func(u update.T0) int64 { return u.Id }, func(u update.T0) string { return u.Email }, func(u update.T0) string { return u.Name })
}

type upsertH struct{ db *seamDB }
func (h upsertH) Node_Upsert_n0(p upsert.PortsNR_Upsert_n0, _ *string) (upsert.Row_Upsert_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (upsert.T0, error) { var t upsert.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return upsert.Row_Upsert_n0{IsError: true, Err: e.Error()}, true }
	return upsert.Row_Upsert_n0{Val: v}, true
}
func nativeUpsert(db *seamDB) string {
	out, _ := upsert.RunNativeRawStruct_Upsert(upsertH{db}, upsert.In_Upsert{Email: "user1@example.com", Name: "Upserted One"})
	return userRows(out, func(u upsert.T0) int64 { return u.Id }, func(u upsert.T0) string { return u.Email }, func(u upsert.T0) string { return u.Name })
}

// ── batch ──
func jstrs(v []string) []string {
	o := make([]string, len(v))
	for i, s := range v {
		o[i] = jsonStr(s)
	}
	return o
}
type createManyH struct{ db *seamDB }
func (h createManyH) Node_CreateMany_n0(p createmany.PortsNR_CreateMany_n0, _ *string) (createmany.Row_CreateMany_n0, bool) {
	v, e := QueryBatchWrite(h.db, p.Sql, []string{"email", "name"}, [][]string{jstrs(p.V0), jstrs(p.V1)}, func(r *sql.Rows) (createmany.T0, error) { var t createmany.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return createmany.Row_CreateMany_n0{IsError: true, Err: e.Error()}, true }
	return createmany.Row_CreateMany_n0{Val: v}, true
}
func nativeCreateMany(db *seamDB) string {
	out, _ := createmany.RunNativeRawStruct_CreateMany(createManyH{db}, createmany.In_CreateMany{Emails: batchEmails(), Names: batchNames()})
	return userRows(out, func(u createmany.T0) int64 { return u.Id }, func(u createmany.T0) string { return u.Email }, func(u createmany.T0) string { return u.Name })
}
type upsertManyH struct{ db *seamDB }
func (h upsertManyH) Node_UpsertMany_n0(p upsertmany.PortsNR_UpsertMany_n0, _ *string) (upsertmany.Row_UpsertMany_n0, bool) {
	v, e := QueryBatchWrite(h.db, p.Sql, []string{"email", "name"}, [][]string{jstrs(p.V0), jstrs(p.V1)}, func(r *sql.Rows) (upsertmany.T0, error) { var t upsertmany.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return upsertmany.Row_UpsertMany_n0{IsError: true, Err: e.Error()}, true }
	return upsertmany.Row_UpsertMany_n0{Val: v}, true
}
func nativeUpsertMany(db *seamDB) string {
	out, _ := upsertmany.RunNativeRawStruct_UpsertMany(upsertManyH{db}, upsertmany.In_UpsertMany{Emails: upsertManyEmails(), Names: batchNames()})
	return userRows(out, func(u upsertmany.T0) int64 { return u.Id }, func(u upsertmany.T0) string { return u.Email }, func(u upsertmany.T0) string { return u.Name })
}
type updateManyH struct{ db *seamDB }
func (h updateManyH) Node_UpdateMany_n0(p updatemany.PortsNR_UpdateMany_n0, _ *string) (updatemany.Row_UpdateMany_n0, bool) {
	ids := make([]string, len(p.V0))
	for i, id := range p.V0 {
		ids[i] = strconv.FormatInt(id, 10) // numeric key encoded BARE
	}
	v, e := QueryBatchWrite(h.db, p.Sql, []string{"id", "name"}, [][]string{ids, jstrs(p.V1)}, func(r *sql.Rows) (updatemany.T0, error) { var t updatemany.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return updatemany.Row_UpdateMany_n0{IsError: true, Err: e.Error()}, true }
	return updatemany.Row_UpdateMany_n0{Val: v}, true
}
func nativeUpdateMany(db *seamDB) string {
	ids := make([]int64, 10)
	for i := range ids { ids[i] = int64(i + 1) }
	out, _ := updatemany.RunNativeRawStruct_UpdateMany(updateManyH{db}, updatemany.In_UpdateMany{Ids: ids, Names: batchNames()})
	return userRows(out, func(u updatemany.T0) int64 { return u.Id }, func(u updatemany.T0) string { return u.Email }, func(u updatemany.T0) string { return u.Name })
}

// ── read+rel (2-level slice; 3-level is #119) ──
type nfaH struct{ db *seamDB }
func (h nfaH) Node_FindAll_n0(p nestedfindall.PortsNR_FindAll_n0, _ *string) (nestedfindall.Row_FindAll_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0}, func(r *sql.Rows) (nestedfindall.T0, error) { var t nestedfindall.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return nestedfindall.Row_FindAll_n0{IsError: true, Err: e.Error()}, true }
	return nestedfindall.Row_FindAll_n0{Val: v}, true
}
func (h nfaH) Node_FindAll_rel_posts(bp nestedfindall.PortsNR_FindAll_rel_posts_batch, _ *string) (nestedfindall.Row_FindAll_rel_posts, bool) {
	if len(bp.Items) == 0 { return nestedfindall.Row_FindAll_rel_posts{}, true }
	keys := make([]int64, len(bp.Items))
	for i, it := range bp.Items { keys[i] = it.K0 }
	lists, e := QueryBatchedRelation(h.db, bp.Items[0].Sql, keys, encInts,
		func(r *sql.Rows) (nestedfindall.T1, error) { var t nestedfindall.T1; return t, r.Scan(&t.Id, &t.Title, &t.Author_id) },
		func(c nestedfindall.T1) int64 { return c.Author_id })
	if e != nil { return nestedfindall.Row_FindAll_rel_posts{IsError: true, Err: e.Error()}, true }
	rows := make([]nestedfindall.RowElem_FindAll_rel_posts, len(lists))
	for i, val := range lists { rows[i] = nestedfindall.RowElem_FindAll_rel_posts{Val: val} }
	return nestedfindall.Row_FindAll_rel_posts{Rows: rows}, true
}
func nativeNestedFindAll(db *seamDB) string {
	out, _ := nestedfindall.RunNativeRawStruct_FindAll(nfaH{db}, nestedfindall.In_FindAll{})
	parents := make([]string, len(out.Rows))
	for i, u := range out.Rows { parents[i] = userRow(u.Id, u.Email, u.Name) }
	children := make([]string, len(out.Posts))
	for i, ps := range out.Posts {
		cl := make([]string, len(ps))
		for j, p := range ps { cl[j] = postRow(p.Id, p.Title, p.Author_id) }
		children[i] = arrj(cl)
	}
	return relJSON("posts", parents, children)
}

type nffH struct{ db *seamDB }
func (h nffH) Node_FindFirst_n0(p nestedfindfirst.PortsNR_FindFirst_n0, _ *string) (nestedfindfirst.Row_FindFirst_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (nestedfindfirst.T0, error) { var t nestedfindfirst.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return nestedfindfirst.Row_FindFirst_n0{IsError: true, Err: e.Error()}, true }
	return nestedfindfirst.Row_FindFirst_n0{Val: v}, true
}
func (h nffH) Node_FindFirst_rel_posts(bp nestedfindfirst.PortsNR_FindFirst_rel_posts_batch, _ *string) (nestedfindfirst.Row_FindFirst_rel_posts, bool) {
	if len(bp.Items) == 0 { return nestedfindfirst.Row_FindFirst_rel_posts{}, true }
	keys := make([]int64, len(bp.Items))
	for i, it := range bp.Items { keys[i] = it.K0 }
	lists, e := QueryBatchedRelation(h.db, bp.Items[0].Sql, keys, encInts,
		func(r *sql.Rows) (nestedfindfirst.T1, error) { var t nestedfindfirst.T1; return t, r.Scan(&t.Id, &t.Title, &t.Author_id) },
		func(c nestedfindfirst.T1) int64 { return c.Author_id })
	if e != nil { return nestedfindfirst.Row_FindFirst_rel_posts{IsError: true, Err: e.Error()}, true }
	rows := make([]nestedfindfirst.RowElem_FindFirst_rel_posts, len(lists))
	for i, val := range lists { rows[i] = nestedfindfirst.RowElem_FindFirst_rel_posts{Val: val} }
	return nestedfindfirst.Row_FindFirst_rel_posts{Rows: rows}, true
}
func nativeNestedFindFirst(db *seamDB) string {
	out, _ := nestedfindfirst.RunNativeRawStruct_FindFirst(nffH{db}, nestedfindfirst.In_FindFirst{Name: "User%"})
	parents := make([]string, len(out.Rows))
	for i, u := range out.Rows { parents[i] = userRow(u.Id, u.Email, u.Name) }
	children := make([]string, len(out.Posts))
	for i, ps := range out.Posts {
		cl := make([]string, len(ps))
		for j, p := range ps { cl[j] = postRow(p.Id, p.Title, p.Author_id) }
		children[i] = arrj(cl)
	}
	return relJSON("posts", parents, children)
}

type nfuH struct{ db *seamDB }
func (h nfuH) Node_FindUnique_n0(p nestedfindunique.PortsNR_FindUnique_n0, _ *string) (nestedfindunique.Row_FindUnique_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (nestedfindunique.T0, error) { var t nestedfindunique.T0; return t, r.Scan(&t.Id, &t.Email, &t.Name) })
	if e != nil { return nestedfindunique.Row_FindUnique_n0{IsError: true, Err: e.Error()}, true }
	return nestedfindunique.Row_FindUnique_n0{Val: v}, true
}
func (h nfuH) Node_FindUnique_rel_posts(bp nestedfindunique.PortsNR_FindUnique_rel_posts_batch, _ *string) (nestedfindunique.Row_FindUnique_rel_posts, bool) {
	if len(bp.Items) == 0 { return nestedfindunique.Row_FindUnique_rel_posts{}, true }
	keys := make([]int64, len(bp.Items))
	for i, it := range bp.Items { keys[i] = it.K0 }
	lists, e := QueryBatchedRelation(h.db, bp.Items[0].Sql, keys, encInts,
		func(r *sql.Rows) (nestedfindunique.T1, error) { var t nestedfindunique.T1; return t, r.Scan(&t.Id, &t.Title, &t.Author_id) },
		func(c nestedfindunique.T1) int64 { return c.Author_id })
	if e != nil { return nestedfindunique.Row_FindUnique_rel_posts{IsError: true, Err: e.Error()}, true }
	rows := make([]nestedfindunique.RowElem_FindUnique_rel_posts, len(lists))
	for i, val := range lists { rows[i] = nestedfindunique.RowElem_FindUnique_rel_posts{Val: val} }
	return nestedfindunique.Row_FindUnique_rel_posts{Rows: rows}, true
}
func nativeNestedFindUnique(db *seamDB) string {
	out, _ := nestedfindunique.RunNativeRawStruct_FindUnique(nfuH{db}, nestedfindunique.In_FindUnique{Email: "user1@example.com"})
	parents := make([]string, len(out.Rows))
	for i, u := range out.Rows { parents[i] = userRow(u.Id, u.Email, u.Name) }
	children := make([]string, len(out.Posts))
	for i, ps := range out.Posts {
		cl := make([]string, len(ps))
		for j, p := range ps { cl[j] = postRow(p.Id, p.Title, p.Author_id) }
		children[i] = arrj(cl)
	}
	return relJSON("posts", parents, children)
}

type nrH struct{ db *seamDB }
func (h nrH) Node_ByAuthor_n0(p nestedrelations.PortsNR_ByAuthor_n0, _ *string) (nestedrelations.Row_ByAuthor_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0}, func(r *sql.Rows) (nestedrelations.T0, error) { var t nestedrelations.T0; return t, r.Scan(&t.Id, &t.Title, &t.Author_id) })
	if e != nil { return nestedrelations.Row_ByAuthor_n0{IsError: true, Err: e.Error()}, true }
	return nestedrelations.Row_ByAuthor_n0{Val: v}, true
}
func (h nrH) Node_ByAuthor_rel_comments(bp nestedrelations.PortsNR_ByAuthor_rel_comments_batch, _ *string) (nestedrelations.Row_ByAuthor_rel_comments, bool) {
	if len(bp.Items) == 0 { return nestedrelations.Row_ByAuthor_rel_comments{}, true }
	keys := make([]int64, len(bp.Items))
	for i, it := range bp.Items { keys[i] = it.K0 }
	lists, e := QueryBatchedRelation(h.db, bp.Items[0].Sql, keys, encInts,
		func(r *sql.Rows) (nestedrelations.T1, error) { var t nestedrelations.T1; return t, r.Scan(&t.Id, &t.Body, &t.Post_id) },
		func(c nestedrelations.T1) int64 { return c.Post_id })
	if e != nil { return nestedrelations.Row_ByAuthor_rel_comments{IsError: true, Err: e.Error()}, true }
	rows := make([]nestedrelations.RowElem_ByAuthor_rel_comments, len(lists))
	for i, val := range lists { rows[i] = nestedrelations.RowElem_ByAuthor_rel_comments{Val: val} }
	return nestedrelations.Row_ByAuthor_rel_comments{Rows: rows}, true
}
func nativeNestedRelations(db *seamDB) string {
	out, _ := nestedrelations.RunNativeRawStruct_ByAuthor(nrH{db}, nestedrelations.In_ByAuthor{Author_id: 7})
	parents := make([]string, len(out.Rows))
	for i, p := range out.Rows { parents[i] = postRow(p.Id, p.Title, p.Author_id) }
	children := make([]string, len(out.Comments))
	for i, cs := range out.Comments {
		cl := make([]string, len(cs))
		for j, c := range cs { cl[j] = commentRow(c.Id, c.Body, c.Post_id) }
		children[i] = arrj(cl)
	}
	return relJSON("comments", parents, children)
}

type crH struct{ db *seamDB }
func (h crH) Node_ByTenant_n0(p compositerelations.PortsNR_ByTenant_n0, _ *string) (compositerelations.Row_ByTenant_n0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0}, func(r *sql.Rows) (compositerelations.T0, error) { var t compositerelations.T0; return t, r.Scan(&t.Tenant_id, &t.User_id, &t.Name) })
	if e != nil { return compositerelations.Row_ByTenant_n0{IsError: true, Err: e.Error()}, true }
	return compositerelations.Row_ByTenant_n0{Val: v}, true
}
func (h crH) Node_ByTenant_rel_posts(bp compositerelations.PortsNR_ByTenant_rel_posts_batch, _ *string) (compositerelations.Row_ByTenant_rel_posts, bool) {
	if len(bp.Items) == 0 { return compositerelations.Row_ByTenant_rel_posts{}, true }
	keys := make([][2]int64, len(bp.Items))
	for i, it := range bp.Items { keys[i] = [2]int64{it.K0, it.K1} }
	lists, e := QueryBatchedRelation(h.db, bp.Items[0].Sql, keys, encPairs,
		func(r *sql.Rows) (compositerelations.T1, error) { var t compositerelations.T1; return t, r.Scan(&t.Tenant_id, &t.Post_id, &t.User_id, &t.Title) },
		func(c compositerelations.T1) [2]int64 { return [2]int64{c.Tenant_id, c.User_id} })
	if e != nil { return compositerelations.Row_ByTenant_rel_posts{IsError: true, Err: e.Error()}, true }
	rows := make([]compositerelations.RowElem_ByTenant_rel_posts, len(lists))
	for i, val := range lists { rows[i] = compositerelations.RowElem_ByTenant_rel_posts{Val: val} }
	return compositerelations.Row_ByTenant_rel_posts{Rows: rows}, true
}
func nativeCompositeRelations(db *seamDB) string {
	out, _ := compositerelations.RunNativeRawStruct_ByTenant(crH{db}, compositerelations.In_ByTenant{Tenant_id: 1})
	parents := make([]string, len(out.Rows))
	for i, u := range out.Rows { parents[i] = tuserRow(u.Tenant_id, u.User_id, u.Name) }
	children := make([]string, len(out.Posts))
	for i, ps := range out.Posts {
		cl := make([]string, len(ps))
		for j, p := range ps { cl[j] = tpostRow(p.Tenant_id, p.Post_id, p.User_id, p.Title) }
		children[i] = arrj(cl)
	}
	return relJSON("posts", parents, children)
}

// ── tx (transaction envelope + chain; {committed, state}) ──
type deleteH struct{ db *seamDB }
func (h deleteH) Node_Delete_tx_body_0(p delete_op.PortsNR_Delete_tx_body_0, _ *string) (delete_op.Row_Delete_tx_body_0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (int64, error) { var id int64; return id, r.Scan(&id) })
	if e != nil || len(v) == 0 { return delete_op.Row_Delete_tx_body_0{IsError: true, Err: "insert failed"}, true }
	return delete_op.Row_Delete_tx_body_0{Id: v[0]}, true
}
func (h deleteH) Node_Delete_tx_body_1(p delete_op.PortsNR_Delete_tx_body_1, _ *string) (delete_op.Row_Delete_tx_body_1, bool) {
	ch, li, e := Execute(h.db, p.Sql, []any{p.P0})
	if e != nil { return delete_op.Row_Delete_tx_body_1{IsError: true, Err: e.Error()}, true }
	return delete_op.Row_Delete_tx_body_1{Changes: ch, LastInsertRowid: li}, true
}
func nativeDelete(db *seamDB) string {
	ok := Transaction(db, func() error { _, err := delete_op.RunNativeRawStruct_Delete(deleteH{db}, delete_op.In_Delete{Email: "del0@bench.com", Name: "Del"}); return err })
	return txJSON(ok, db)
}

type ncH struct{ db *seamDB }
func (h ncH) Node_NestedCreate_tx_body_0(p nestedcreate.PortsNR_NestedCreate_tx_body_0, _ *string) (nestedcreate.Row_NestedCreate_tx_body_0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (int64, error) { var id int64; return id, r.Scan(&id) })
	if e != nil || len(v) == 0 { return nestedcreate.Row_NestedCreate_tx_body_0{IsError: true, Err: "insert failed"}, true }
	return nestedcreate.Row_NestedCreate_tx_body_0{Id: v[0]}, true
}
func (h ncH) Node_NestedCreate_tx_body_1(p nestedcreate.PortsNR_NestedCreate_tx_body_1, _ *string) (nestedcreate.Row_NestedCreate_tx_body_1, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (nestedcreate.Row_NestedCreate_tx_body_1, error) {
		var row nestedcreate.Row_NestedCreate_tx_body_1
		return row, r.Scan(&row.Id, &row.Author_id, &row.Title)
	})
	if e != nil || len(v) == 0 { return nestedcreate.Row_NestedCreate_tx_body_1{IsError: true, Err: "insert failed"}, true }
	return v[0], true
}
func nativeNestedCreate(db *seamDB) string {
	ok := Transaction(db, func() error { _, err := nestedcreate.RunNativeRawStruct_NestedCreate(ncH{db}, nestedcreate.In_NestedCreate{Email: "nc@bench.com", Name: "NC", Title: "NC Post"}); return err })
	return txJSON(ok, db)
}

type nuH struct{ db *seamDB }
func (h nuH) Node_NestedUpdate_tx_body_0(p nestedupdate.PortsNR_NestedUpdate_tx_body_0, _ *string) (nestedupdate.Row_NestedUpdate_tx_body_0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (nestedupdate.Row_NestedUpdate_tx_body_0, error) {
		var row nestedupdate.Row_NestedUpdate_tx_body_0
		return row, r.Scan(&row.Id, &row.Name)
	})
	if e != nil || len(v) == 0 { return nestedupdate.Row_NestedUpdate_tx_body_0{IsError: true, Err: "update failed"}, true }
	return v[0], true
}
func (h nuH) Node_NestedUpdate_tx_body_1(p nestedupdate.PortsNR_NestedUpdate_tx_body_1, _ *string) (nestedupdate.Row_NestedUpdate_tx_body_1, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (nestedupdate.Row_NestedUpdate_tx_body_1, error) {
		var row nestedupdate.Row_NestedUpdate_tx_body_1
		return row, r.Scan(&row.Id, &row.Title)
	})
	if e != nil || len(v) == 0 { return nestedupdate.Row_NestedUpdate_tx_body_1{IsError: true, Err: "update failed"}, true }
	return v[0], true
}
func nativeNestedUpdate(db *seamDB) string {
	ok := Transaction(db, func() error { _, err := nestedupdate.RunNativeRawStruct_NestedUpdate(nuH{db}, nestedupdate.In_NestedUpdate{Name: "NU", User_id: 7, Title: "NU Post"}); return err })
	return txJSON(ok, db)
}

type nupH struct{ db *seamDB }
func (h nupH) Node_NestedUpsert_tx_body_0(p nestedupsert.PortsNR_NestedUpsert_tx_body_0, _ *string) (nestedupsert.Row_NestedUpsert_tx_body_0, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (int64, error) { var id int64; return id, r.Scan(&id) })
	if e != nil || len(v) == 0 { return nestedupsert.Row_NestedUpsert_tx_body_0{IsError: true, Err: "upsert failed"}, true }
	return nestedupsert.Row_NestedUpsert_tx_body_0{Id: v[0]}, true
}
func (h nupH) Node_NestedUpsert_tx_body_1(p nestedupsert.PortsNR_NestedUpsert_tx_body_1, _ *string) (nestedupsert.Row_NestedUpsert_tx_body_1, bool) {
	v, e := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (nestedupsert.Row_NestedUpsert_tx_body_1, error) {
		var row nestedupsert.Row_NestedUpsert_tx_body_1
		return row, r.Scan(&row.Id, &row.Author_id, &row.Title)
	})
	if e != nil || len(v) == 0 { return nestedupsert.Row_NestedUpsert_tx_body_1{IsError: true, Err: "insert failed"}, true }
	return v[0], true
}
func nativeNestedUpsert(db *seamDB) string {
	ok := Transaction(db, func() error { _, err := nestedupsert.RunNativeRawStruct_NestedUpsert(nupH{db}, nestedupsert.In_NestedUpsert{Email: "user1@example.com", Name: "NUp", Title: "NUp Post"}); return err })
	return txJSON(ok, db)
}
