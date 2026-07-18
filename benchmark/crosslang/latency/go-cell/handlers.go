// The go-native HANDLERS — one Handler_<Comp> impl per op. Each Node_* runs its statement's BAKED f_sql
// (Sql) on the connection via the generic seam (Query / QueryBatchedRelation / QueryBatchWrite) and
// decodes its rows into the generated typed row struct. The go twin of rust/e1_native_proof main.rs's
// HandlerNR<Comp> impls. NO IR, NO dispatch, NO bc runtime — only the baked SQL + the thin seam. Each op
// lives in its OWN generated package (the go twin of the rust crate's per-op `mod`).
package main

import (
	"database/sql"

	"litedbmodel_latency_bench/behaviors/createmany"
	"litedbmodel_latency_bench/behaviors/createuser"
	"litedbmodel_latency_bench/behaviors/findunique"
	"litedbmodel_latency_bench/behaviors/relsingle"
)

// ── findunique (point read) ──
type findUniqueH struct{ db *seamDB }

func (h findUniqueH) Node_FindUnique_n0(p findunique.PortsNR_FindUnique_n0, _ *string) (findunique.Row_FindUnique_n0, bool) {
	val, err := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (findunique.T0, error) {
		var t findunique.T0
		return t, r.Scan(&t.Id, &t.Email, &t.Name)
	})
	if err != nil {
		return findunique.Row_FindUnique_n0{IsError: true, Err: err.Error()}, true
	}
	return findunique.Row_FindUnique_n0{Val: val}, true
}

// ── createuser (single write, RETURNING) ──
type createUserH struct{ db *seamDB }

func (h createUserH) Node_CreateUser_n0(p createuser.PortsNR_CreateUser_n0, _ *string) (createuser.Row_CreateUser_n0, bool) {
	val, err := Query(h.db, p.Sql, []any{p.P0, p.P1}, func(r *sql.Rows) (createuser.T0, error) {
		var t createuser.T0
		return t, r.Scan(&t.Id, &t.Email, &t.Name)
	})
	if err != nil {
		return createuser.Row_CreateUser_n0{IsError: true, Err: err.Error()}, true
	}
	return createuser.Row_CreateUser_n0{Val: val}, true
}

// ── createmany (batch write: ONE json_each INSERT for N records) ──
type createManyH struct{ db *seamDB }

func (h createManyH) Node_CreateMany_n0(p createmany.PortsNR_CreateMany_n0, _ *string) (createmany.Row_CreateMany_n0, bool) {
	// Parallel column arrays → pre-encoded cells (strings quoted). The seam zips + runs ONCE.
	ev := make([]string, len(p.V0))
	for i, s := range p.V0 {
		ev[i] = jsonStr(s)
	}
	nv := make([]string, len(p.V1))
	for i, s := range p.V1 {
		nv[i] = jsonStr(s)
	}
	val, err := QueryBatchWrite(h.db, p.Sql, []string{"email", "name"}, [][]string{ev, nv}, func(r *sql.Rows) (createmany.T0, error) {
		var t createmany.T0
		return t, r.Scan(&t.Id, &t.Email, &t.Name)
	})
	if err != nil {
		return createmany.Row_CreateMany_n0{IsError: true, Err: err.Error()}, true
	}
	return createmany.Row_CreateMany_n0{Val: val}, true
}

// ── relsingle (batched relation: parent posts + ONE batched comments query) ──
type relSingleH struct{ db *seamDB }

func (h relSingleH) Node_ByAuthor_n0(p relsingle.PortsNR_ByAuthor_n0, _ *string) (relsingle.Row_ByAuthor_n0, bool) {
	val, err := Query(h.db, p.Sql, []any{p.P0}, func(r *sql.Rows) (relsingle.T0, error) {
		var t relsingle.T0
		return t, r.Scan(&t.Id, &t.Title, &t.Author_id)
	})
	if err != nil {
		return relsingle.Row_ByAuthor_n0{IsError: true, Err: err.Error()}, true
	}
	return relsingle.Row_ByAuthor_n0{Val: val}, true
}

func (h relSingleH) Node_ByAuthor_rel_comments(bp relsingle.PortsNR_ByAuthor_rel_comments_batch, _ *string) (relsingle.Row_ByAuthor_rel_comments, bool) {
	if len(bp.Items) == 0 {
		return relsingle.Row_ByAuthor_rel_comments{}, true
	}
	itemKeys := make([]int64, len(bp.Items))
	for i, it := range bp.Items {
		itemKeys[i] = it.K0
	}
	lists, err := QueryBatchedRelation(h.db, bp.Items[0].Sql, itemKeys,
		func(r *sql.Rows) (relsingle.T1, error) {
			var t relsingle.T1
			return t, r.Scan(&t.Id, &t.Body, &t.Post_id)
		},
		func(c relsingle.T1) int64 { return c.Post_id }, // target-key grouping
	)
	if err != nil {
		return relsingle.Row_ByAuthor_rel_comments{IsError: true, Err: err.Error()}, true
	}
	rows := make([]relsingle.RowElem_ByAuthor_rel_comments, len(lists))
	for i, val := range lists {
		rows[i] = relsingle.RowElem_ByAuthor_rel_comments{Val: val}
	}
	return relsingle.Row_ByAuthor_rel_comments{Rows: rows}, true
}
