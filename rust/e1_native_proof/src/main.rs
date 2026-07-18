//! E1/E2/E3 (#116, epic #115) PROOF-OF-APPROACH — execute BAKED-SQL native ops against real SQLite.
//!
//! Each generated module (emitted by `generateCodegenArtifact(bundle, 'rust', …, {nativeSql:true})`
//! through bc 0.8.5's `rust-typed-native` endpoint) carries its op's per-dialect SQL as a NATIVE
//! STRING LITERAL on its concrete ports struct. So there is NO JSON catalog companion to read at
//! runtime: the query IS the module. This binary supplies the other half — the thin, op-agnostic
//! seam (`seam.rs`) — and prints rows / write effects as canonical JSON so the TS leg can assert
//! byte-equality against the mode-2 oracle.
//!
//! Read and write go through the SAME lowering and the SAME seam (owner decision — read/write are one
//! flow): the module bakes the SQL either way; only the driver's result collection differs
//! (`query` for a row list — SELECT or RETURNING — vs `execute` for an affected-row summary).

mod generated_bymaybe;
mod generated_byids;
mod generated_createmany;
mod generated_createuser;
mod generated_deleteuser;
mod generated_feed;
mod generated_findunique;
mod generated_recent;
mod generated_relbatch;
mod generated_relsingle;
mod generated_renameuser;
mod generated_tenantfeed;
mod generated_txdelete;
mod generated_txnestedcreate;
mod generated_txnestedupdate;
mod generated_txnestedupsert;
mod generated_txrollback;
mod generated_updatemany;
mod generated_upsert;
mod generated_upsertmany;
mod seam;

use rusqlite::Connection;
use seam::{json_str, query, query_batch_write, query_batched_relation, query_skip, Param, WhereFrag};

// ── per-op adapters ──────────────────────────────────────────────────────────────────────────
//
// The ONLY per-op glue: map the node's baked typed ports onto the ordered param list, and decode the
// projected columns into the module's own outType struct. Hand-written for the proof; uniform enough
// to be generated alongside the module.

struct FindUniqueSeam<'a> {
    conn: &'a Connection,
}
impl generated_findunique::HandlerNRFindUnique for FindUniqueSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_findunique::PortsNRFindUniqueN0,
        _bound: Option<String>,
    ) -> Option<generated_findunique::RawRowNRFindUniqueN0> {
        let params = [Param::Text(ports.f_p0.clone()), Param::Int(ports.f_p1)];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_findunique::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(row_or_err_fu(val))
    }
}

struct ByIdsSeam<'a> {
    conn: &'a Connection,
}
impl generated_byids::HandlerNRByIds for ByIdsSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_byids::PortsNRByIdsN0,
        _bound: Option<String>,
    ) -> Option<generated_byids::RawRowNRByIdsN0> {
        // The IN-list head is a NATIVE Vec<i64> port (bc#110); the seam performs the single-JSON bind.
        let params = [Param::ArrayInt(ports.f_p0.clone())];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_byids::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(row_or_err_bi(val))
    }
}

struct FeedSeam<'a> {
    conn: &'a Connection,
}
impl generated_feed::HandlerNRPostsWithAuthor for FeedSeam<'_> {
    // n0: the parent posts read — the module baked its SQL; the seam just runs it.
    fn node_n0(
        &self,
        ports: &generated_feed::PortsNRPostsWithAuthorN0,
        _bound: Option<String>,
    ) -> Option<generated_feed::RawRowNRPostsWithAuthorN0> {
        let params = [Param::Int(ports.f_p0)];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_feed::T0 { id: r.get(0)?, title: r.get(1)?, author_id: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_feed::RawRowNRPostsWithAuthorN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_feed::RawRowNRPostsWithAuthorN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
    // n1: the per-PARENT-ELEMENT child lookup — the module drives this once per post, binding
    // ports.f_p0 = that post's author_id (a NATIVE element-field access, `oel_n1.author_id`).
    fn node_n1(
        &self,
        ports: &generated_feed::PortsNRPostsWithAuthorN1,
        _bound: Option<String>,
    ) -> Option<generated_feed::RawElemNRPostsWithAuthorN1> {
        let params = [Param::Int(ports.f_p0)];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_feed::T1 { id: r.get(0)?, name: r.get(1)? })
        });
        Some(match val {
            Ok(val) => generated_feed::RawElemNRPostsWithAuthorN1 { is_error: false, err: String::new(), val },
            Err(e) => generated_feed::RawElemNRPostsWithAuthorN1 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

// The NATIVE BATCHED relation (E4/#119): parent read + a hasMany relation resolved in ONE batched
// child query. The bench op#19 compositeRelations surface — but batched, not N+1.
struct RelBatchSeam<'a> {
    conn: &'a Connection,
}
impl generated_relbatch::HandlerNRByTenant for RelBatchSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_relbatch::PortsNRByTenantN0,
        _bound: Option<String>,
    ) -> Option<generated_relbatch::RawRowNRByTenantN0> {
        let val = query(self.conn, &ports.f_sql, &[Param::Int(ports.f_p0)], |r| {
            Ok(generated_relbatch::T0 { tenant_id: r.get(0)?, user_id: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_relbatch::RawRowNRByTenantN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_relbatch::RawRowNRByTenantN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
    // The BATCHED relation handler — receives ALL parents' composite keys at once; runs ONE query.
    fn node_rel_posts(
        &self,
        ports: &generated_relbatch::PortsNRByTenantRelPostsBatch,
        _bound: Option<String>,
    ) -> Option<generated_relbatch::RawRowNRByTenantRelPosts> {
        // each parent's composite key (tenant_id, user_id) — all items share the same baked f_sql.
        let item_keys: Vec<(i64, i64)> = ports.items.iter().map(|it| (it.f_k0, it.f_k1)).collect();
        let sql = &ports.items[0].f_sql;
        let res = query_batched_relation(
            self.conn,
            sql,
            &item_keys,
            // composite tuple JSON for the baked `json_each(?)` membership: `[[t,u],…]` (distinct).
            |ks| format!("[{}]", ks.iter().map(|(t, u)| format!("[{},{}]", t, u)).collect::<Vec<_>>().join(",")),
            |r| Ok(generated_relbatch::T1 { tenant_id: r.get(0)?, post_id: r.get(1)?, user_id: r.get(2)?, title: r.get(3)? }),
            |c| (c.tenant_id, c.user_id), // target-key grouping
        );
        Some(match res {
            Ok(lists) => generated_relbatch::RawRowNRByTenantRelPosts {
                is_error: false,
                err: String::new(),
                rows: lists.into_iter().map(|val| generated_relbatch::RawElemNRByTenantRelPosts { is_error: false, err: String::new(), val }).collect(),
            },
            Err(e) => generated_relbatch::RawRowNRByTenantRelPosts { is_error: true, err: e.to_string(), rows: vec![] },
        })
    }
}

// The SINGLE-key native batched relation (nestedRelations): posts → comments by post_id, ONE query.
struct RelSingleSeam<'a> {
    conn: &'a Connection,
}
impl generated_relsingle::HandlerNRByAuthor for RelSingleSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_relsingle::PortsNRByAuthorN0,
        _bound: Option<String>,
    ) -> Option<generated_relsingle::RawRowNRByAuthorN0> {
        let val = query(self.conn, &ports.f_sql, &[Param::Int(ports.f_p0)], |r| {
            Ok(generated_relsingle::T0 { id: r.get(0)?, title: r.get(1)?, author_id: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_relsingle::RawRowNRByAuthorN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_relsingle::RawRowNRByAuthorN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
    fn node_rel_comments(
        &self,
        ports: &generated_relsingle::PortsNRByAuthorRelCommentsBatch,
        _bound: Option<String>,
    ) -> Option<generated_relsingle::RawRowNRByAuthorRelComments> {
        // single key per parent (post id). ONE batched child query over the deduped ids.
        let item_keys: Vec<i64> = ports.items.iter().map(|it| it.f_k0).collect();
        let sql = &ports.items[0].f_sql;
        let res = query_batched_relation(
            self.conn,
            sql,
            &item_keys,
            // single-key JSON: a flat array `[k1,k2,…]` for the baked `json_each(?)` IN-list.
            |ks| format!("[{}]", ks.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(",")),
            |r| Ok(generated_relsingle::T1 { id: r.get(0)?, body: r.get(1)?, post_id: r.get(2)? }),
            |c| c.post_id, // target-key grouping
        );
        Some(match res {
            Ok(lists) => generated_relsingle::RawRowNRByAuthorRelComments {
                is_error: false,
                err: String::new(),
                rows: lists.into_iter().map(|val| generated_relsingle::RawElemNRByAuthorRelComments { is_error: false, err: String::new(), val }).collect(),
            },
            Err(e) => generated_relsingle::RawRowNRByAuthorRelComments { is_error: true, err: e.to_string(), rows: vec![] },
        })
    }
}

struct TenantFeedSeam<'a> {
    conn: &'a Connection,
}
impl generated_tenantfeed::HandlerNRUsersWithPosts for TenantFeedSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_tenantfeed::PortsNRUsersWithPostsN0,
        _bound: Option<String>,
    ) -> Option<generated_tenantfeed::RawRowNRUsersWithPostsN0> {
        let params = [Param::Int(ports.f_p0)];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_tenantfeed::T0 { tenant_id: r.get(0)?, user_id: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_tenantfeed::RawRowNRUsersWithPostsN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_tenantfeed::RawRowNRUsersWithPostsN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
    // COMPOSITE-key child: the module drives this per parent USER, binding BOTH parent element fields
    // (f_p0 = oel.tenant_id, f_p1 = oel.user_id — two native element-field accesses) into a
    // two-column tuple join. The seam just runs the baked SQL with both params.
    fn node_n1(
        &self,
        ports: &generated_tenantfeed::PortsNRUsersWithPostsN1,
        _bound: Option<String>,
    ) -> Option<generated_tenantfeed::RawElemNRUsersWithPostsN1> {
        let params = [Param::Int(ports.f_p0), Param::Int(ports.f_p1)];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_tenantfeed::T1 { tenant_id: r.get(0)?, post_id: r.get(1)?, title: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_tenantfeed::RawElemNRUsersWithPostsN1 { is_error: false, err: String::new(), val },
            Err(e) => generated_tenantfeed::RawElemNRUsersWithPostsN1 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct ByMaybeSeam<'a> {
    conn: &'a Connection,
}
impl generated_bymaybe::HandlerNRByAuthorMaybePublished for ByMaybeSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_bymaybe::PortsNRByAuthorMaybePublishedN0,
        _bound: Option<String>,
    ) -> Option<generated_bymaybe::RawRowNRByAuthorMaybePublishedN0> {
        // SKIP args: the required `author_id` fragment is always present; the `published` fragment is
        // present iff the bc#139 Option is Some. The generic seam assembles the present fragments.
        let frags = [
            WhereFrag { sql: &ports.f_w0, present: true, params: vec![Param::Int(ports.f_w0p0)] },
            WhereFrag {
                sql: &ports.f_w1,
                present: ports.f_w1p0.is_some(),
                params: ports.f_w1p0.iter().map(|v| Param::Int(*v)).collect(),
            },
        ];
        let val = query_skip(self.conn, &ports.f_sql_head, &frags, &ports.f_sql_tail, &[], |r| {
            Ok(generated_bymaybe::T0 { id: r.get(0)?, title: r.get(1)?, author_id: r.get(2)?, published: r.get(3)? })
        });
        Some(match val {
            Ok(val) => generated_bymaybe::RawRowNRByAuthorMaybePublishedN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_bymaybe::RawRowNRByAuthorMaybePublishedN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct RecentSeam<'a> {
    conn: &'a Connection,
}
impl generated_recent::HandlerNRRecent for RecentSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_recent::PortsNRRecentN0,
        _bound: Option<String>,
    ) -> Option<generated_recent::RawRowNRRecentN0> {
        // #122: the baked LIMIT param is `in_.limit.unwrap_or(20)` — already an i64 by the time the
        // seam sees it (the default resolved natively in the module). The seam binds it plainly.
        let params = [Param::Int(ports.f_p0)];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_recent::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_recent::RawRowNRRecentN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_recent::RawRowNRRecentN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

// E3 (#118) createMany — ONE json_each batch INSERT for N records (parallel column arrays).
struct CreateManySeam<'a> {
    conn: &'a Connection,
}
impl generated_createmany::HandlerNRCreateMany for CreateManySeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_createmany::PortsNRCreateManyN0,
        _bound: Option<String>,
    ) -> Option<generated_createmany::RawRowNRCreateManyN0> {
        // the parallel column arrays (emails, names) + their column names → the seam zips + runs ONCE.
        let ev: Vec<String> = ports.f_v0.iter().map(|s| json_str(s)).collect();
        let nv: Vec<String> = ports.f_v1.iter().map(|s| json_str(s)).collect();
        let val = query_batch_write(self.conn, &ports.f_sql, &["email", "name"], &[&ev, &nv], |r| {
            Ok(generated_createmany::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_createmany::RawRowNRCreateManyN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_createmany::RawRowNRCreateManyN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

// E3 (#118) updateMany — ONE json_each batch UPDATE keyed by id. The numeric KEY column is encoded
// BARE in the JSON (so `json_extract(…) = <int id>` matches); the string SET column is quoted.
struct UpdateManySeam<'a> {
    conn: &'a Connection,
}
impl generated_updatemany::HandlerNRUpdateMany for UpdateManySeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_updatemany::PortsNRUpdateManyN0,
        _bound: Option<String>,
    ) -> Option<generated_updatemany::RawRowNRUpdateManyN0> {
        let ids: Vec<String> = ports.f_v0.iter().map(|id| id.to_string()).collect(); // numeric key → bare
        let names: Vec<String> = ports.f_v1.iter().map(|s| json_str(s)).collect(); // string → quoted
        let val = query_batch_write(self.conn, &ports.f_sql, &["id", "name"], &[&ids, &names], |r| {
            Ok(generated_updatemany::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_updatemany::RawRowNRUpdateManyN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_updatemany::RawRowNRUpdateManyN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

// E2 (#117) upsertMany — batch + onConflict: ONE json_each INSERT … ON CONFLICT … DO UPDATE. Same
// seam as createMany; only the baked f_sql carries the ON CONFLICT clause.
struct UpsertManySeam<'a> {
    conn: &'a Connection,
}
impl generated_upsertmany::HandlerNRUpsertMany for UpsertManySeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_upsertmany::PortsNRUpsertManyN0,
        _bound: Option<String>,
    ) -> Option<generated_upsertmany::RawRowNRUpsertManyN0> {
        let ev: Vec<String> = ports.f_v0.iter().map(|s| json_str(s)).collect();
        let nv: Vec<String> = ports.f_v1.iter().map(|s| json_str(s)).collect();
        let val = query_batch_write(self.conn, &ports.f_sql, &["email", "name"], &[&ev, &nv], |r| {
            Ok(generated_upsertmany::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_upsertmany::RawRowNRUpsertManyN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_upsertmany::RawRowNRUpsertManyN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

// E2 (#117) upsert — INSERT … ON CONFLICT … DO UPDATE … RETURNING. A RETURNING write like Insert:
// the module bakes the FULL upsert SQL; the seam just runs it (insert-path OR conflict-path, one stmt).
struct UpsertSeam<'a> {
    conn: &'a Connection,
}
impl generated_upsert::HandlerNRUpsertUser for UpsertSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_upsert::PortsNRUpsertUserN0,
        _bound: Option<String>,
    ) -> Option<generated_upsert::RawRowNRUpsertUserN0> {
        let params = [Param::Text(ports.f_p0.clone()), Param::Text(ports.f_p1.clone())];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_upsert::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_upsert::RawRowNRUpsertUserN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_upsert::RawRowNRUpsertUserN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct CreateUserSeam<'a> {
    conn: &'a Connection,
}
impl generated_createuser::HandlerNRCreateUser for CreateUserSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_createuser::PortsNRCreateUserN0,
        _bound: Option<String>,
    ) -> Option<generated_createuser::RawRowNRCreateUserN0> {
        // A RETURNING write is a row-returning op — `query`, same as a read. Params in placeholder
        // order: values.email, values.name (the write's SoT ports, not the SQL text).
        let params = [Param::Text(ports.f_p0.clone()), Param::Text(ports.f_p1.clone())];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_createuser::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_createuser::RawRowNRCreateUserN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_createuser::RawRowNRCreateUserN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct RenameUserSeam<'a> {
    conn: &'a Connection,
}
impl generated_renameuser::HandlerNRRenameUser for RenameUserSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_renameuser::PortsNRRenameUserN0,
        _bound: Option<String>,
    ) -> Option<generated_renameuser::RawRowNRRenameUserN0> {
        let params = [Param::Text(ports.f_p0.clone()), Param::Int(ports.f_p1)];
        let val = query(self.conn, &ports.f_sql, &params, |r| {
            Ok(generated_renameuser::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
        });
        Some(match val {
            Ok(val) => generated_renameuser::RawRowNRRenameUserN0 { is_error: false, err: String::new(), val },
            Err(e) => generated_renameuser::RawRowNRRenameUserN0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct DeleteUserSeam<'a> {
    conn: &'a Connection,
}
impl generated_deleteuser::HandlerNRDeleteUser for DeleteUserSeam<'_> {
    fn node_n0(
        &self,
        ports: &generated_deleteuser::PortsNRDeleteUserN0,
        _bound: Option<String>,
    ) -> Option<generated_deleteuser::RawRowNRDeleteUserN0> {
        // A NON-RETURNING write hands back the summary row `[{changes, lastInsertRowid}]` — SAME baked
        // SQL, SAME seam, only the collection differs (`execute`, not `query`).
        let params = [Param::Int(ports.f_p0)];
        match seam::execute(self.conn, &ports.f_sql, &params) {
            Ok(s) => Some(generated_deleteuser::RawRowNRDeleteUserN0 {
                is_error: false,
                err: String::new(),
                val: vec![generated_deleteuser::T0 { changes: s.changes, lastInsertRowid: s.last_insert_rowid }],
            }),
            Err(e) => Some(generated_deleteuser::RawRowNRDeleteUserN0 { is_error: true, err: e.to_string(), ..Default::default() }),
        }
    }
}

// ── E5 (#120): tx-chain handlers. Each node_tx_body_* runs its statement's BAKED f_sql on the pinned
//    tx connection (RETURNING → `query`, non-returning → `execute`) and decodes its SINGLE produced row.
//    The generated runner chains them (a `{ref:[producer,field]}` param bakes as `cell_<producer>.<f>`);
//    the seam's `transaction` envelope (dispatch below) wraps the whole runner in BEGIN…COMMIT/ROLLBACK.

struct TxDeleteSeam<'a> {
    conn: &'a Connection,
}
impl generated_txdelete::HandlerNRTxDelete for TxDeleteSeam<'_> {
    fn node_tx_body_0(&self, ports: &generated_txdelete::PortsNRTxDeleteTxBody0, _b: Option<String>) -> Option<generated_txdelete::RawRowNRTxDeleteTxBody0> {
        let params = [Param::Text(ports.f_p0.clone()), Param::Text(ports.f_p1.clone())];
        let rows = query(self.conn, &ports.f_sql, &params, |r| Ok(r.get::<_, i64>(0)?));
        Some(match rows {
            Ok(v) if !v.is_empty() => generated_txdelete::RawRowNRTxDeleteTxBody0 { is_error: false, err: String::new(), id: v[0] },
            Ok(_) => generated_txdelete::RawRowNRTxDeleteTxBody0 { is_error: true, err: "INSERT…RETURNING produced no row".into(), ..Default::default() },
            Err(e) => generated_txdelete::RawRowNRTxDeleteTxBody0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
    fn node_tx_body_1(&self, ports: &generated_txdelete::PortsNRTxDeleteTxBody1, _b: Option<String>) -> Option<generated_txdelete::RawRowNRTxDeleteTxBody1> {
        // A non-returning DELETE — `execute`, its produced row is the {changes, lastInsertRowid} summary.
        let params = [Param::Int(ports.f_p0)];
        Some(match seam::execute(self.conn, &ports.f_sql, &params) {
            Ok(s) => generated_txdelete::RawRowNRTxDeleteTxBody1 { is_error: false, err: String::new(), changes: s.changes, lastInsertRowid: s.last_insert_rowid },
            Err(e) => generated_txdelete::RawRowNRTxDeleteTxBody1 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct TxNestedCreateSeam<'a> {
    conn: &'a Connection,
}
impl generated_txnestedcreate::HandlerNRTxNestedCreate for TxNestedCreateSeam<'_> {
    fn node_tx_body_0(&self, ports: &generated_txnestedcreate::PortsNRTxNestedCreateTxBody0, _b: Option<String>) -> Option<generated_txnestedcreate::RawRowNRTxNestedCreateTxBody0> {
        let params = [Param::Text(ports.f_p0.clone()), Param::Text(ports.f_p1.clone())];
        let rows = query(self.conn, &ports.f_sql, &params, |r| Ok(r.get::<_, i64>(0)?));
        Some(match rows {
            Ok(v) if !v.is_empty() => generated_txnestedcreate::RawRowNRTxNestedCreateTxBody0 { is_error: false, err: String::new(), id: v[0] },
            Ok(_) => generated_txnestedcreate::RawRowNRTxNestedCreateTxBody0 { is_error: true, err: "INSERT…RETURNING produced no row".into(), ..Default::default() },
            Err(e) => generated_txnestedcreate::RawRowNRTxNestedCreateTxBody0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
    fn node_tx_body_1(&self, ports: &generated_txnestedcreate::PortsNRTxNestedCreateTxBody1, _b: Option<String>) -> Option<generated_txnestedcreate::RawRowNRTxNestedCreateTxBody1> {
        // f_p0 = the CHAINED author_id (the user's RETURNING id, native i64); f_p1 = the title.
        let params = [Param::Int(ports.f_p0), Param::Text(ports.f_p1.clone())];
        let rows = query(self.conn, &ports.f_sql, &params, |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?)));
        Some(match rows {
            Ok(v) if !v.is_empty() => generated_txnestedcreate::RawRowNRTxNestedCreateTxBody1 { is_error: false, err: String::new(), id: v[0].0, author_id: v[0].1, title: v[0].2.clone() },
            Ok(_) => generated_txnestedcreate::RawRowNRTxNestedCreateTxBody1 { is_error: true, err: "INSERT…RETURNING produced no row".into(), ..Default::default() },
            Err(e) => generated_txnestedcreate::RawRowNRTxNestedCreateTxBody1 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct TxNestedUpdateSeam<'a> {
    conn: &'a Connection,
}
impl generated_txnestedupdate::HandlerNRTxNestedUpdate for TxNestedUpdateSeam<'_> {
    fn node_tx_body_0(&self, ports: &generated_txnestedupdate::PortsNRTxNestedUpdateTxBody0, _b: Option<String>) -> Option<generated_txnestedupdate::RawRowNRTxNestedUpdateTxBody0> {
        let params = [Param::Text(ports.f_p0.clone()), Param::Int(ports.f_p1)];
        let rows = query(self.conn, &ports.f_sql, &params, |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)));
        Some(match rows {
            Ok(v) if !v.is_empty() => generated_txnestedupdate::RawRowNRTxNestedUpdateTxBody0 { is_error: false, err: String::new(), id: v[0].0, name: v[0].1.clone() },
            Ok(_) => generated_txnestedupdate::RawRowNRTxNestedUpdateTxBody0 { is_error: true, err: "UPDATE…RETURNING produced no row".into(), ..Default::default() },
            Err(e) => generated_txnestedupdate::RawRowNRTxNestedUpdateTxBody0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
    fn node_tx_body_1(&self, ports: &generated_txnestedupdate::PortsNRTxNestedUpdateTxBody1, _b: Option<String>) -> Option<generated_txnestedupdate::RawRowNRTxNestedUpdateTxBody1> {
        let params = [Param::Text(ports.f_p0.clone()), Param::Int(ports.f_p1)];
        let rows = query(self.conn, &ports.f_sql, &params, |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)));
        Some(match rows {
            Ok(v) if !v.is_empty() => generated_txnestedupdate::RawRowNRTxNestedUpdateTxBody1 { is_error: false, err: String::new(), id: v[0].0, title: v[0].1.clone() },
            Ok(_) => generated_txnestedupdate::RawRowNRTxNestedUpdateTxBody1 { is_error: true, err: "UPDATE…RETURNING produced no row".into(), ..Default::default() },
            Err(e) => generated_txnestedupdate::RawRowNRTxNestedUpdateTxBody1 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct TxNestedUpsertSeam<'a> {
    conn: &'a Connection,
}
impl generated_txnestedupsert::HandlerNRTxNestedUpsert for TxNestedUpsertSeam<'_> {
    fn node_tx_body_0(&self, ports: &generated_txnestedupsert::PortsNRTxNestedUpsertTxBody0, _b: Option<String>) -> Option<generated_txnestedupsert::RawRowNRTxNestedUpsertTxBody0> {
        let params = [Param::Text(ports.f_p0.clone()), Param::Text(ports.f_p1.clone())];
        let rows = query(self.conn, &ports.f_sql, &params, |r| Ok(r.get::<_, i64>(0)?));
        Some(match rows {
            Ok(v) if !v.is_empty() => generated_txnestedupsert::RawRowNRTxNestedUpsertTxBody0 { is_error: false, err: String::new(), id: v[0] },
            Ok(_) => generated_txnestedupsert::RawRowNRTxNestedUpsertTxBody0 { is_error: true, err: "upsert…RETURNING produced no row".into(), ..Default::default() },
            Err(e) => generated_txnestedupsert::RawRowNRTxNestedUpsertTxBody0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
    fn node_tx_body_1(&self, ports: &generated_txnestedupsert::PortsNRTxNestedUpsertTxBody1, _b: Option<String>) -> Option<generated_txnestedupsert::RawRowNRTxNestedUpsertTxBody1> {
        let params = [Param::Int(ports.f_p0), Param::Text(ports.f_p1.clone())];
        let rows = query(self.conn, &ports.f_sql, &params, |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?)));
        Some(match rows {
            Ok(v) if !v.is_empty() => generated_txnestedupsert::RawRowNRTxNestedUpsertTxBody1 { is_error: false, err: String::new(), id: v[0].0, author_id: v[0].1, title: v[0].2.clone() },
            Ok(_) => generated_txnestedupsert::RawRowNRTxNestedUpsertTxBody1 { is_error: true, err: "INSERT…RETURNING produced no row".into(), ..Default::default() },
            Err(e) => generated_txnestedupsert::RawRowNRTxNestedUpsertTxBody1 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

struct TxRollbackSeam<'a> {
    conn: &'a Connection,
}
impl generated_txrollback::HandlerNRTxRollback for TxRollbackSeam<'_> {
    fn node_tx_body_0(&self, ports: &generated_txrollback::PortsNRTxRollbackTxBody0, _b: Option<String>) -> Option<generated_txrollback::RawRowNRTxRollbackTxBody0> {
        let params = [Param::Text(ports.f_p0.clone()), Param::Text(ports.f_p1.clone())];
        let rows = query(self.conn, &ports.f_sql, &params, |r| Ok(r.get::<_, i64>(0)?));
        Some(match rows {
            Ok(v) if !v.is_empty() => generated_txrollback::RawRowNRTxRollbackTxBody0 { is_error: false, err: String::new(), id: v[0] },
            Ok(_) => generated_txrollback::RawRowNRTxRollbackTxBody0 { is_error: true, err: "INSERT…RETURNING produced no row".into(), ..Default::default() },
            Err(e) => generated_txrollback::RawRowNRTxRollbackTxBody0 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
    fn node_tx_body_1(&self, ports: &generated_txrollback::PortsNRTxRollbackTxBody1, _b: Option<String>) -> Option<generated_txrollback::RawRowNRTxRollbackTxBody1> {
        // This INSERT collides on UNIQUE(email) → the driver returns Err → is_error → the runner Errs →
        // the seam's transaction ROLLS BACK statement 0's insert (atomicity).
        let params = [Param::Text(ports.f_p0.clone()), Param::Text(ports.f_p1.clone())];
        let rows = query(self.conn, &ports.f_sql, &params, |r| Ok(r.get::<_, i64>(0)?));
        Some(match rows {
            Ok(v) if !v.is_empty() => generated_txrollback::RawRowNRTxRollbackTxBody1 { is_error: false, err: String::new(), id: v[0] },
            Ok(_) => generated_txrollback::RawRowNRTxRollbackTxBody1 { is_error: true, err: "INSERT…RETURNING produced no row".into(), ..Default::default() },
            Err(e) => generated_txrollback::RawRowNRTxRollbackTxBody1 { is_error: true, err: e.to_string(), ..Default::default() },
        })
    }
}

/// The users+posts DB state, for the tx write-state assertion — byte-matching the mode-2 oracle's
/// `txState` (`{users:[{id,email,name}…], posts:[{id,title,author_id}…]}`).
fn tx_state(conn: &Connection) -> String {
    let users = query(conn, "SELECT id, email, name FROM benchmark_users ORDER BY id", &[], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
    })
    .expect("users state read");
    let posts = query(conn, "SELECT id, title, author_id FROM benchmark_posts ORDER BY id", &[], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))
    })
    .expect("posts state read");
    let u: Vec<String> = users.iter().map(|(id, e, n)| format!("{{\"id\":{},\"email\":{},\"name\":{}}}", id, json_str(e), json_str(n))).collect();
    let p: Vec<String> = posts.iter().map(|(id, t, a)| format!("{{\"id\":{},\"title\":{},\"author_id\":{}}}", id, json_str(t), a)).collect();
    format!("{{\"users\":[{}],\"posts\":[{}]}}", u.join(","), p.join(","))
}

fn row_or_err_fu(v: rusqlite::Result<Vec<generated_findunique::T0>>) -> generated_findunique::RawRowNRFindUniqueN0 {
    match v {
        Ok(val) => generated_findunique::RawRowNRFindUniqueN0 { is_error: false, err: String::new(), val },
        Err(e) => generated_findunique::RawRowNRFindUniqueN0 { is_error: true, err: e.to_string(), ..Default::default() },
    }
}
fn row_or_err_bi(v: rusqlite::Result<Vec<generated_byids::T0>>) -> generated_byids::RawRowNRByIdsN0 {
    match v {
        Ok(val) => generated_byids::RawRowNRByIdsN0 { is_error: false, err: String::new(), val },
        Err(e) => generated_byids::RawRowNRByIdsN0 { is_error: true, err: e.to_string(), ..Default::default() },
    }
}

fn user_rows_json(items: &[(i64, String, String)]) -> String {
    let s: Vec<String> = items
        .iter()
        .map(|(id, email, name)| format!("{{\"id\":{},\"email\":{},\"name\":{}}}", id, json_str(email), json_str(name)))
        .collect();
    format!("[{}]", s.join(","))
}

/// The resulting table state, for the write DB-state assertion — a raw generic read via the seam.
fn table_state(conn: &Connection) -> String {
    let rows = query(conn, "SELECT id, email, name FROM benchmark_users ORDER BY id", &[], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
    })
    .expect("state read");
    user_rows_json(&rows)
}

// ── LATENCY BENCH (payoff proof): the rust-native cell. Same 4 ops as the go + ts-IR cells, same seed,
//    same iteration count. Times the WHOLE hot path (build input → RunNativeRawStruct = bind + exec +
//    decode into the typed struct) and writes RAW per-iteration samples (µs) as flat CSV `op,us` — the
//    collector aggregates p50/p99/ops-sec (measurement vs aggregation stay separate, per metrics.ts).
//    Reads run on a read-only seed; writes run on a fresh mutable copy with a UNIQUE input per iteration.
fn now_us() -> u128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros()
}

// The scaled-relation sweep — (op id, author, iters). Matches benchmark/crosslang/latency/behaviors.ts
// REL_SCALES; the rel.db seed defines each author's child count (10 / 100 / 1000 / 10000).
const REL_SCALES: &[(&str, i64, usize)] = &[("rel10", 101, 5000), ("rel100", 102, 5000), ("rel1000", 103, 2000), ("rel10000", 104, 300)];

fn run_bench(read_db: &str, write_db: &str, rel_db: &str, warmup: usize, iters: usize, out_csv: &str) {
    use std::io::Write;
    let mut csv = std::fs::File::create(out_csv).expect("create csv");
    writeln!(csv, "op,us").unwrap();

    // ── findunique (point read) — read-only ──
    {
        let conn = Connection::open(read_db).expect("open read db");
        let emails: Vec<String> = (0..).map(|i| format!("user{}@example.com", (i % 100) + 1)).take(warmup + iters).collect();
        for e in emails.iter().take(warmup) {
            let _ = generated_findunique::run_native_raw_struct_FindUnique(&FindUniqueSeam { conn: &conn }, generated_findunique::InNRFindUnique { email: e.clone() });
        }
        for e in emails.iter().skip(warmup) {
            let t0 = std::time::Instant::now();
            let out = generated_findunique::run_native_raw_struct_FindUnique(&FindUniqueSeam { conn: &conn }, generated_findunique::InNRFindUnique { email: e.clone() }).unwrap();
            std::hint::black_box(&out);
            writeln!(csv, "findunique,{:.3}", t0.elapsed().as_nanos() as f64 / 1000.0).unwrap();
        }
    }
    // ── relsingle (batched relation: parent posts + one batched comments query) — read-only ──
    {
        let conn = Connection::open(read_db).expect("open read db");
        for _ in 0..warmup {
            let _ = generated_relsingle::run_native_raw_struct_ByAuthor(&RelSingleSeam { conn: &conn }, generated_relsingle::InNRByAuthor { author_id: 7 });
        }
        for _ in 0..iters {
            let t0 = std::time::Instant::now();
            let out = generated_relsingle::run_native_raw_struct_ByAuthor(&RelSingleSeam { conn: &conn }, generated_relsingle::InNRByAuthor { author_id: 7 }).unwrap();
            std::hint::black_box(&out);
            writeln!(csv, "relsingle,{:.3}", t0.elapsed().as_nanos() as f64 / 1000.0).unwrap();
        }
    }
    // ── createuser (single write, RETURNING) — mutable, UNIQUE email per iteration ──
    {
        let conn = Connection::open(write_db).expect("open write db");
        let mk = |i: usize| format!("cu{}_{}@example.com", now_us(), i);
        for i in 0..warmup {
            let _ = generated_createuser::run_native_raw_struct_CreateUser(&CreateUserSeam { conn: &conn }, generated_createuser::InNRCreateUser { email: mk(i), name: "Bench".into() });
        }
        for i in 0..iters {
            let email = format!("cu_{}_{}@example.com", i, now_us());
            let t0 = std::time::Instant::now();
            let out = generated_createuser::run_native_raw_struct_CreateUser(&CreateUserSeam { conn: &conn }, generated_createuser::InNRCreateUser { email, name: "Bench".into() }).unwrap();
            std::hint::black_box(&out);
            writeln!(csv, "createuser,{:.3}", t0.elapsed().as_nanos() as f64 / 1000.0).unwrap();
        }
    }
    // ── createmany (batch write: ONE json_each INSERT for 10 records) — mutable, UNIQUE rows per iter ──
    {
        let conn = Connection::open(write_db).expect("open write db");
        let batch = |iter: usize| -> (Vec<String>, Vec<String>) {
            let ts = now_us();
            let emails: Vec<String> = (0..10).map(|k| format!("cm_{}_{}_{}@example.com", iter, k, ts)).collect();
            let names: Vec<String> = (0..10).map(|k| format!("BM{}_{}", iter, k)).collect();
            (emails, names)
        };
        for i in 0..warmup {
            let (emails, names) = batch(1_000_000 + i);
            let _ = generated_createmany::run_native_raw_struct_CreateMany(&CreateManySeam { conn: &conn }, generated_createmany::InNRCreateMany { emails, names });
        }
        for i in 0..iters {
            let (emails, names) = batch(i);
            let t0 = std::time::Instant::now();
            let out = generated_createmany::run_native_raw_struct_CreateMany(&CreateManySeam { conn: &conn }, generated_createmany::InNRCreateMany { emails, names }).unwrap();
            std::hint::black_box(&out);
            writeln!(csv, "createmany,{:.3}", t0.elapsed().as_nanos() as f64 / 1000.0).unwrap();
        }
    }
    // ── SCALED relation sweep — the SAME relsingle (ByAuthor + batched comments) at growing child counts
    //    (10 → 10000). Exposes the seam's per-parent alignment cost, which scales with total children. ──
    {
        let conn = Connection::open(rel_db).expect("open rel db");
        for (op, author, scale_iters) in REL_SCALES {
            for _ in 0..warmup.min(*scale_iters) {
                let _ = generated_relsingle::run_native_raw_struct_ByAuthor(&RelSingleSeam { conn: &conn }, generated_relsingle::InNRByAuthor { author_id: *author });
            }
            for _ in 0..*scale_iters {
                let t0 = std::time::Instant::now();
                let out = generated_relsingle::run_native_raw_struct_ByAuthor(&RelSingleSeam { conn: &conn }, generated_relsingle::InNRByAuthor { author_id: *author }).unwrap();
                std::hint::black_box(&out);
                writeln!(csv, "{},{:.3}", op, t0.elapsed().as_nanos() as f64 / 1000.0).unwrap();
            }
        }
    }
    eprintln!("rust-native bench done: 4 base ops + {} rel scales → {}", REL_SCALES.len(), out_csv);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let op = args.get(1).expect("usage: e1_native_proof <op> <db> <args...>");
    // Latency bench: `bench <read_db> <write_db> <rel_db> <warmup> <iters> <out_csv>`.
    if op == "bench" {
        let read_db = args.get(2).expect("bench <read_db>");
        let write_db = args.get(3).expect("bench <write_db>");
        let rel_db = args.get(4).expect("bench <rel_db>");
        let warmup: usize = args.get(5).expect("bench <warmup>").parse().expect("warmup");
        let iters: usize = args.get(6).expect("bench <iters>").parse().expect("iters");
        let out_csv = args.get(7).expect("bench <out_csv>");
        run_bench(read_db, write_db, rel_db, warmup, iters, out_csv);
        return;
    }
    let db_path = args.get(2).expect("usage: e1_native_proof <op> <db> <args...>");
    let conn = Connection::open(db_path).expect("open db");

    match op.as_str() {
        "findunique" => {
            let email = args.get(3).expect("findunique needs <email>").clone();
            let out = generated_findunique::run_native_raw_struct_FindUnique(&FindUniqueSeam { conn: &conn }, generated_findunique::InNRFindUnique { email })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{}", user_rows_json(&items));
        }
        "byids" => {
            let raw = args.get(3).expect("byids needs <ids>");
            let ids: Vec<i64> = if raw.is_empty() { vec![] } else { raw.split(',').map(|s| s.parse().expect("id")).collect() };
            let out = generated_byids::run_native_raw_struct_ByIds(&ByIdsSeam { conn: &conn }, generated_byids::InNRByIds { ids })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{}", user_rows_json(&items));
        }
        // relbatch: <tenant_id> — the NATIVE BATCHED composite relation (ONE child query). {rows, posts}.
        "relbatch" => {
            let tenant_id: i64 = args.get(3).expect("tenant_id").parse().expect("tenant_id int");
            let out = generated_relbatch::run_native_raw_struct_ByTenant(&RelBatchSeam { conn: &conn }, generated_relbatch::InNRByTenant { tenant_id })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let rows: Vec<String> = out
                .rows
                .iter()
                .map(|u| format!("{{\"tenant_id\":{},\"user_id\":{},\"name\":{}}}", u.tenant_id, u.user_id, json_str(&u.name)))
                .collect();
            let posts: Vec<String> = out
                .posts
                .iter()
                .map(|inner| {
                    let items: Vec<String> = inner
                        .iter()
                        .map(|p| format!("{{\"tenant_id\":{},\"post_id\":{},\"user_id\":{},\"title\":{}}}", p.tenant_id, p.post_id, p.user_id, json_str(&p.title)))
                        .collect();
                    format!("[{}]", items.join(","))
                })
                .collect();
            println!("{{\"rows\":[{}],\"posts\":[{}]}}", rows.join(","), posts.join(","));
            // Proof of BATCHING: the whole op issued exactly 2 queries (1 parent + 1 batched child)
            // regardless of parent count — NOT 1+N. Printed to stderr for the harness to assert.
            eprintln!("queries={}", seam::QUERY_COUNT.load(std::sync::atomic::Ordering::SeqCst));
        }
        // relsingle: <author_id> — the NATIVE BATCHED single-key relation (posts + comments). {rows, comments}.
        "relsingle" => {
            let author_id: i64 = args.get(3).expect("author_id").parse().expect("author_id int");
            let out = generated_relsingle::run_native_raw_struct_ByAuthor(&RelSingleSeam { conn: &conn }, generated_relsingle::InNRByAuthor { author_id })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let rows: Vec<String> = out.rows.iter().map(|p| format!("{{\"id\":{},\"title\":{},\"author_id\":{}}}", p.id, json_str(&p.title), p.author_id)).collect();
            let comments: Vec<String> = out
                .comments
                .iter()
                .map(|inner| {
                    let items: Vec<String> = inner.iter().map(|c| format!("{{\"id\":{},\"body\":{},\"post_id\":{}}}", c.id, json_str(&c.body), c.post_id)).collect();
                    format!("[{}]", items.join(","))
                })
                .collect();
            println!("{{\"rows\":[{}],\"comments\":[{}]}}", rows.join(","), comments.join(","));
            eprintln!("queries={}", seam::QUERY_COUNT.load(std::sync::atomic::Ordering::SeqCst));
        }
        // tenantfeed: <tenant_id> — a COMPOSITE-key relation (users + per-user posts joined on BOTH
        // tenant_id AND user_id). Output {posts:[[...]], users:[...]}.
        "tenantfeed" => {
            let tenant_id: i64 = args.get(3).expect("tenant_id").parse().expect("tenant_id int");
            let out = generated_tenantfeed::run_native_raw_struct_UsersWithPosts(&TenantFeedSeam { conn: &conn }, generated_tenantfeed::InNRUsersWithPosts { tenant_id })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let posts: Vec<String> = out
                .posts
                .iter()
                .map(|inner| {
                    let items: Vec<String> = inner.iter().map(|p| format!("{{\"tenant_id\":{},\"post_id\":{},\"title\":{}}}", p.tenant_id, p.post_id, json_str(&p.title))).collect();
                    format!("[{}]", items.join(","))
                })
                .collect();
            let users: Vec<String> = out
                .users
                .iter()
                .map(|u| format!("{{\"tenant_id\":{},\"user_id\":{},\"name\":{}}}", u.tenant_id, u.user_id, json_str(&u.name)))
                .collect();
            println!("{{\"posts\":[{}],\"users\":[{}]}}", posts.join(","), users.join(","));
        }
        // feed: <author_id> — a single-key relation (posts + per-post author). Output {authors, posts}.
        "feed" => {
            let author_id: i64 = args.get(3).expect("author_id").parse().expect("author_id int");
            let out = generated_feed::run_native_raw_struct_PostsWithAuthor(&FeedSeam { conn: &conn }, generated_feed::InNRPostsWithAuthor { author_id })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let authors: Vec<String> = out
                .authors
                .iter()
                .map(|inner| {
                    let items: Vec<String> = inner.iter().map(|a| format!("{{\"id\":{},\"name\":{}}}", a.id, json_str(&a.name))).collect();
                    format!("[{}]", items.join(","))
                })
                .collect();
            let posts: Vec<String> = out
                .posts
                .iter()
                .map(|p| format!("{{\"id\":{},\"title\":{},\"author_id\":{}}}", p.id, json_str(&p.title), p.author_id))
                .collect();
            println!("{{\"authors\":[{}],\"posts\":[{}]}}", authors.join(","), posts.join(","));
        }
        // bymaybe: <author_id> <published or ''>  ("" published = absent → the skip fragment drops)
        "bymaybe" => {
            let author_id: i64 = args.get(3).expect("author_id").parse().expect("author_id int");
            let raw = args.get(4).expect("published or ''");
            let published: Option<i64> = if raw.is_empty() { None } else { Some(raw.parse().expect("published")) };
            let out = generated_bymaybe::run_native_raw_struct_ByAuthorMaybePublished(&ByMaybeSeam { conn: &conn }, generated_bymaybe::InNRByAuthorMaybePublished { author_id, published })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<String> = out
                .iter()
                .map(|r| format!("{{\"id\":{},\"title\":{},\"author_id\":{},\"published\":{}}}", r.id, json_str(&r.title), r.author_id, r.published))
                .collect();
            println!("[{}]", items.join(","));
        }
        // recent: <limit>  ("" = absent → the baked .unwrap_or(20) default takes effect)
        "recent" => {
            let raw = args.get(3).expect("recent needs <limit or ''>");
            let limit: Option<i64> = if raw.is_empty() { None } else { Some(raw.parse().expect("limit")) };
            let out = generated_recent::run_native_raw_struct_Recent(&RecentSeam { conn: &conn }, generated_recent::InNRRecent { limit })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{}", user_rows_json(&items));
        }
        // A write MUTATES `db_path` (the harness passes a fresh copy). It prints {result, state} so
        // the leg asserts BOTH the returned rows/summary AND the resulting DB state vs the oracle.
        "createuser" => {
            let email = args.get(3).expect("email").clone();
            let name = args.get(4).expect("name").clone();
            let out = generated_createuser::run_native_raw_struct_CreateUser(&CreateUserSeam { conn: &conn }, generated_createuser::InNRCreateUser { email, name })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{{\"result\":{},\"state\":{}}}", user_rows_json(&items), table_state(&conn));
        }
        // createmany: <emails_csv> <names_csv> — ONE json_each batch insert for N records. {result, state}.
        "createmany" => {
            let emails: Vec<String> = args.get(3).expect("emails").split(',').map(|s| s.to_string()).collect();
            let names: Vec<String> = args.get(4).expect("names").split(',').map(|s| s.to_string()).collect();
            let out = generated_createmany::run_native_raw_struct_CreateMany(&CreateManySeam { conn: &conn }, generated_createmany::InNRCreateMany { emails, names })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{{\"result\":{},\"state\":{}}}", user_rows_json(&items), table_state(&conn));
            eprintln!("queries={}", seam::QUERY_COUNT.load(std::sync::atomic::Ordering::SeqCst));
        }
        // upsertmany: <emails_csv> <names_csv> — ONE json_each INSERT … ON CONFLICT batch. {result, state}.
        "upsertmany" => {
            let emails: Vec<String> = args.get(3).expect("emails").split(',').map(|s| s.to_string()).collect();
            let names: Vec<String> = args.get(4).expect("names").split(',').map(|s| s.to_string()).collect();
            let out = generated_upsertmany::run_native_raw_struct_UpsertMany(&UpsertManySeam { conn: &conn }, generated_upsertmany::InNRUpsertMany { emails, names })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{{\"result\":{},\"state\":{}}}", user_rows_json(&items), table_state(&conn));
            eprintln!("queries={}", seam::QUERY_COUNT.load(std::sync::atomic::Ordering::SeqCst));
        }
        // updatemany: <ids_csv> <names_csv> — ONE json_each batch UPDATE keyed by id. {result, state}.
        "updatemany" => {
            let ids: Vec<i64> = args.get(3).expect("ids").split(',').map(|s| s.parse().expect("id")).collect();
            let names: Vec<String> = args.get(4).expect("names").split(',').map(|s| s.to_string()).collect();
            let out = generated_updatemany::run_native_raw_struct_UpdateMany(&UpdateManySeam { conn: &conn }, generated_updatemany::InNRUpdateMany { ids, names })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{{\"result\":{},\"state\":{}}}", user_rows_json(&items), table_state(&conn));
        }
        // upsert: <email> <name> — INSERT or (on email conflict) UPDATE. {result, state}.
        "upsert" => {
            let email = args.get(3).expect("email").clone();
            let name = args.get(4).expect("name").clone();
            let out = generated_upsert::run_native_raw_struct_UpsertUser(&UpsertSeam { conn: &conn }, generated_upsert::InNRUpsertUser { email, name })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{{\"result\":{},\"state\":{}}}", user_rows_json(&items), table_state(&conn));
        }
        "renameuser" => {
            let id: i64 = args.get(3).expect("id").parse().expect("id int");
            let name = args.get(4).expect("name").clone();
            let out = generated_renameuser::run_native_raw_struct_RenameUser(&RenameUserSeam { conn: &conn }, generated_renameuser::InNRRenameUser { id, name })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            let items: Vec<(i64, String, String)> = out.into_iter().map(|r| (r.id, r.email, r.name)).collect();
            println!("{{\"result\":{},\"state\":{}}}", user_rows_json(&items), table_state(&conn));
        }
        "deleteuser" => {
            let id: i64 = args.get(3).expect("id").parse().expect("id int");
            let out = generated_deleteuser::run_native_raw_struct_DeleteUser(&DeleteUserSeam { conn: &conn }, generated_deleteuser::InNRDeleteUser { id })
                .unwrap_or_else(|e| panic!("behavior failed: {e}"));
            // The non-returning summary row: [{changes, lastInsertRowid}].
            let s: Vec<String> = out.iter().map(|r| format!("{{\"changes\":{},\"lastInsertRowid\":{}}}", r.changes, r.lastInsertRowid)).collect();
            println!("{{\"result\":[{}],\"state\":{}}}", s.join(","), table_state(&conn));
        }
        // E5 (#120): RETURNING-chained transactions. The seam's `transaction` envelope wraps the whole
        // generated chain runner in BEGIN…COMMIT / ROLLBACK; on any statement failure the chain Errs and
        // the tx rolls back (committed:false), leaving the DB unchanged. Output {result:{committed}, state}.
        "txdelete" => {
            let email = args.get(3).expect("email").clone();
            let name = args.get(4).expect("name").clone();
            let r = seam::transaction(&conn, |c| {
                generated_txdelete::run_native_raw_struct_TxDelete(&TxDeleteSeam { conn: c }, generated_txdelete::InNRTxDelete { email, name })
            });
            println!("{{\"result\":{{\"committed\":{}}},\"state\":{}}}", r.is_ok(), tx_state(&conn));
        }
        "txnestedcreate" => {
            let email = args.get(3).expect("email").clone();
            let name = args.get(4).expect("name").clone();
            let title = args.get(5).expect("title").clone();
            let r = seam::transaction(&conn, |c| {
                generated_txnestedcreate::run_native_raw_struct_TxNestedCreate(&TxNestedCreateSeam { conn: c }, generated_txnestedcreate::InNRTxNestedCreate { email, name, title })
            });
            println!("{{\"result\":{{\"committed\":{}}},\"state\":{}}}", r.is_ok(), tx_state(&conn));
        }
        "txnestedupdate" => {
            let user_id: i64 = args.get(3).expect("user_id").parse().expect("user_id int");
            let name = args.get(4).expect("name").clone();
            let title = args.get(5).expect("title").clone();
            let r = seam::transaction(&conn, |c| {
                generated_txnestedupdate::run_native_raw_struct_TxNestedUpdate(&TxNestedUpdateSeam { conn: c }, generated_txnestedupdate::InNRTxNestedUpdate { name, user_id, title })
            });
            println!("{{\"result\":{{\"committed\":{}}},\"state\":{}}}", r.is_ok(), tx_state(&conn));
        }
        "txnestedupsert" => {
            let email = args.get(3).expect("email").clone();
            let name = args.get(4).expect("name").clone();
            let title = args.get(5).expect("title").clone();
            let r = seam::transaction(&conn, |c| {
                generated_txnestedupsert::run_native_raw_struct_TxNestedUpsert(&TxNestedUpsertSeam { conn: c }, generated_txnestedupsert::InNRTxNestedUpsert { email, name, title })
            });
            println!("{{\"result\":{{\"committed\":{}}},\"state\":{}}}", r.is_ok(), tx_state(&conn));
        }
        "txrollback" => {
            let email = args.get(3).expect("email").clone();
            let dup_email = args.get(4).expect("dup_email").clone();
            let name = args.get(5).expect("name").clone();
            let r = seam::transaction(&conn, |c| {
                generated_txrollback::run_native_raw_struct_TxRollback(&TxRollbackSeam { conn: c }, generated_txrollback::InNRTxRollback { email, name, dup_email })
            });
            println!("{{\"result\":{{\"committed\":{}}},\"state\":{}}}", r.is_ok(), tx_state(&conn));
        }
        other => panic!("unknown op '{other}'"),
    }
}
