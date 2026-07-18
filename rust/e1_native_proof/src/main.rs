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
mod generated_createuser;
mod generated_deleteuser;
mod generated_feed;
mod generated_findunique;
mod generated_recent;
mod generated_relbatch;
mod generated_relsingle;
mod generated_renameuser;
mod generated_tenantfeed;
mod seam;

use rusqlite::Connection;
use seam::{json_str, query, query_batched_relation, query_skip, Param, WhereFrag};

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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let op = args.get(1).expect("usage: e1_native_proof <op> <db> <args...>");
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
        other => panic!("unknown op '{other}'"),
    }
}
