//! Main-bench RUST cell — the CLI-generated typed-native modules (generated/<dialect>/, NEVER hand-
//! written, runtime-free) + the generic exec seam (seam.rs — a driver abstraction over rusqlite/postgres)
//! + thin per-op leaf handlers. Also the SDK BASELINE (raw driver + hand-SQL for benchmark_*) — the only
//! hand-written execution. Both produce a CANONICAL result string a node driver byte-compares to the
//! mode-2 oracle. ONE dialect per binary (cargo feature `sqlite` (default) or `postgres`); the SAME
//! handlers compile against either DB (the generated module types are identical across dialects — only
//! the baked SQL literal differs — and the seam abstracts the driver).
//!
//! Modes:
//!   orm_bench_rust run <op> <target> <native|sdk>   → print the canonical result for one op × cell
//!   orm_bench_rust bench <seed_db> <w> <n> <csv>    → latency CSV (sqlite only)
#![allow(non_snake_case)]

#[path = "seam.rs"]
mod seam;
use seam::{col, execute, json_str, query, query_batch_write, query_batched_relation, transaction, AsI64, Cell, Db, Param};

// The generated module tree for the built dialect (`use gen::*` brings the 19 op modules into scope).
#[cfg(feature = "postgres")]
#[path = "../generated/postgres/mod.rs"]
mod gen;
#[cfg(not(feature = "postgres"))]
#[path = "../generated/sqlite/mod.rs"]
mod gen;
use gen::*;

// ── canonical row JSON: {"k":v,…} — int bare, string json-quoted; matches oracle.ts canonVal/canonRow ──
fn ji(k: &str, v: i64) -> String { format!("{}:{}", json_str(k), v) }
fn js(k: &str, v: &str) -> String { format!("{}:{}", json_str(k), json_str(v)) }
fn obj(fields: &[String]) -> String { format!("{{{}}}", fields.join(",")) }
fn arr(rows: &[String]) -> String { format!("[{}]", rows.join(",")) }
fn user_row(id: i64, email: &str, name: &str) -> String { obj(&[ji("id", id), js("email", email), js("name", name)]) }
fn post_row(id: i64, title: &str, author_id: i64) -> String { obj(&[ji("id", id), js("title", title), ji("author_id", author_id)]) }
fn comment_row(id: i64, body: &str, post_id: i64) -> String { obj(&[ji("id", id), js("body", body), ji("post_id", post_id)]) }
fn tuser_row(t: i64, u: i64, name: &str) -> String { obj(&[ji("tenant_id", t), ji("user_id", u), js("name", name)]) }
fn tpost_row(t: i64, pid: i64, u: i64, title: &str) -> String { obj(&[ji("tenant_id", t), ji("post_id", pid), ji("user_id", u), js("title", title)]) }
fn rel_json(rel: &str, parents: &[String], child_lists: &[String]) -> String { format!("{{\"rows\":{},{}:{}}}", arr(parents), json_str(rel), arr(child_lists)) }

// ── fixed inputs (match ops.ts / oracle.ts) ──
fn batch_emails() -> Vec<String> { (0..10).map(|i| format!("many{}@bench.com", i)).collect() }
fn batch_names() -> Vec<String> { (0..10).map(|i| format!("Many {}", i)).collect() }
fn upsertmany_emails() -> Vec<String> {
    let mut v = vec!["user1@example.com".to_string(), "user2@example.com".to_string()];
    v.extend((0..8).map(|i| format!("many{}@bench.com", i)));
    v
}
/// The n-th positional placeholder for the built dialect (`$n` pg / `?` sqlite) — the SDK's hand-SQL uses
/// it so ONE SDK statement string builds per dialect. (Native SQL is GENERATED, never built here.)
#[cfg(feature = "postgres")]
fn ph(n: usize) -> String { format!("${}", n) }
#[cfg(not(feature = "postgres"))]
fn ph(_n: usize) -> String { "?".to_string() }
/// Read the `published` projection as 0/1 regardless of the dialect's column type (pg BOOLEAN vs sqlite
/// INTEGER) — the SDK twin of the native `AsI64` canonicalization.
#[cfg(feature = "postgres")]
fn read_published(r: &dyn Cell, i: usize) -> i64 { r.boolean(i) as i64 }
#[cfg(not(feature = "postgres"))]
fn read_published(r: &dyn Cell, i: usize) -> i64 { r.i64(i) }
/// The `filterPaginateSort` `published` INPUT literal — pg types the input head BOOLEAN, sqlite INTEGER.
#[cfg(feature = "postgres")]
fn fps_published_input() -> bool { true }
#[cfg(not(feature = "postgres"))]
fn fps_published_input() -> i64 { 1 }

// ══ NATIVE cells — the CLI-generated module + the seam, decode via col() (type-inferred, dialect-agnostic).
macro_rules! user_flat {
    ($db:expr, $module:ident, $comp:ident, $handler:ident, $ports:ident, $row:ident, $in:expr, $bind:expr) => {{
        struct H<'a> { db: &'a Db }
        impl $module::$handler for H<'_> {
            fn node_n0(&self, ports: &$module::$ports, _b: Option<String>) -> Option<$module::$row> {
                let val = query(self.db, &ports.f_sql, &$bind(ports), |r| $module::T0 { id: col(r, 0), email: col(r, 1), name: col(r, 2) });
                Some(match val {
                    Ok(val) => $module::$row { is_error: false, err: String::new(), val },
                    Err(e) => $module::$row { is_error: true, err: e, ..Default::default() },
                })
            }
        }
        let out = $module::$comp(&H { db: $db }, $in).unwrap();
        arr(&out.iter().map(|r| user_row(r.id, &r.email, &r.name)).collect::<Vec<_>>())
    }};
}
fn native_findall(db: &Db) -> String {
    user_flat!(db, gen_findall, run_native_raw_struct_FindAll, HandlerNRFindAll, PortsNRFindAllN0, RawRowNRFindAllN0,
        gen_findall::InNRFindAll, |p: &gen_findall::PortsNRFindAllN0| [Param::Int(p.f_p0)])
}
fn native_findfirst(db: &Db) -> String {
    user_flat!(db, gen_findfirst, run_native_raw_struct_FindFirst, HandlerNRFindFirst, PortsNRFindFirstN0, RawRowNRFindFirstN0,
        gen_findfirst::InNRFindFirst { name: "User%".into() }, |p: &gen_findfirst::PortsNRFindFirstN0| [Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)])
}
fn native_findunique(db: &Db) -> String {
    user_flat!(db, gen_findunique, run_native_raw_struct_FindUnique, HandlerNRFindUnique, PortsNRFindUniqueN0, RawRowNRFindUniqueN0,
        gen_findunique::InNRFindUnique { email: "user500@example.com".into() }, |p: &gen_findunique::PortsNRFindUniqueN0| [Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)])
}
fn native_create(db: &Db) -> String {
    user_flat!(db, gen_create, run_native_raw_struct_Create, HandlerNRCreate, PortsNRCreateN0, RawRowNRCreateN0,
        gen_create::InNRCreate { email: "new@bench.com".into(), name: "New".into() }, |p: &gen_create::PortsNRCreateN0| [Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())])
}
fn native_update(db: &Db) -> String {
    user_flat!(db, gen_update, run_native_raw_struct_Update, HandlerNRUpdate, PortsNRUpdateN0, RawRowNRUpdateN0,
        gen_update::InNRUpdate { name: "Updated 100".into(), id: 100 }, |p: &gen_update::PortsNRUpdateN0| [Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)])
}
fn native_upsert(db: &Db) -> String {
    user_flat!(db, gen_upsert, run_native_raw_struct_Upsert, HandlerNRUpsert, PortsNRUpsertN0, RawRowNRUpsertN0,
        gen_upsert::InNRUpsert { email: "user1@example.com".into(), name: "Upserted One".into() }, |p: &gen_upsert::PortsNRUpsertN0| [Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())])
}
fn native_filterpaginatesort(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_filterpaginatesort::HandlerNRFilterPaginateSort for H<'_> {
        fn node_n0(&self, ports: &gen_filterpaginatesort::PortsNRFilterPaginateSortN0, _b: Option<String>) -> Option<gen_filterpaginatesort::RawRowNRFilterPaginateSortN0> {
            // `f_p0` is the `published` filter — pg types it BOOLEAN, sqlite INTEGER; `as i64` normalizes
            // both, and the pg `Param::Int`→BOOL bind (seam) restores true/false.
            let val = query(self.db, &ports.f_sql, &[Param::Int(ports.f_p0 as i64), Param::Int(ports.f_p1), Param::Int(ports.f_p2)], |r| {
                gen_filterpaginatesort::T0 { id: col(r, 0), title: col(r, 1), content: col(r, 2), published: col(r, 3), author_id: col(r, 4), created_at: col(r, 5) }
            });
            Some(match val {
                Ok(val) => gen_filterpaginatesort::RawRowNRFilterPaginateSortN0 { is_error: false, err: String::new(), val },
                Err(e) => gen_filterpaginatesort::RawRowNRFilterPaginateSortN0 { is_error: true, err: e, ..Default::default() },
            })
        }
    }
    let out = gen_filterpaginatesort::run_native_raw_struct_FilterPaginateSort(&H { db }, gen_filterpaginatesort::InNRFilterPaginateSort { published: fps_published_input() }).unwrap();
    // `published`: AsI64 canonicalizes the pg BOOLEAN / sqlite INTEGER field to 0/1 (oracle's canonVal).
    arr(&out.iter().map(|r| obj(&[ji("id", r.id), js("title", &r.title), js("content", &r.content), ji("published", r.published.as_i64()), ji("author_id", r.author_id), js("created_at", &r.created_at)])).collect::<Vec<_>>())
}

macro_rules! user_batch {
    ($db:expr, $module:ident, $comp:ident, $handler:ident, $ports:ident, $row:ident, $in:expr, $cols:expr, $arrays:expr) => {{
        struct H<'a> { db: &'a Db }
        impl $module::$handler for H<'_> {
            fn node_n0(&self, ports: &$module::$ports, _b: Option<String>) -> Option<$module::$row> {
                let arrays: Vec<Param> = $arrays(ports);
                let refs: Vec<&Param> = arrays.iter().collect();
                let val = query_batch_write(self.db, &ports.f_sql, $cols, &refs, |r| $module::T0 { id: col(r, 0), email: col(r, 1), name: col(r, 2) });
                Some(match val {
                    Ok(val) => $module::$row { is_error: false, err: String::new(), val },
                    Err(e) => $module::$row { is_error: true, err: e, ..Default::default() },
                })
            }
        }
        let out = $module::$comp(&H { db: $db }, $in).unwrap();
        arr(&out.iter().map(|r| user_row(r.id, &r.email, &r.name)).collect::<Vec<_>>())
    }};
}
fn native_createmany(db: &Db) -> String {
    user_batch!(db, gen_createmany, run_native_raw_struct_CreateMany, HandlerNRCreateMany, PortsNRCreateManyN0, RawRowNRCreateManyN0,
        gen_createmany::InNRCreateMany { emails: batch_emails(), names: batch_names() }, &["email", "name"],
        |p: &gen_createmany::PortsNRCreateManyN0| vec![Param::ArrayText(p.f_v0.clone()), Param::ArrayText(p.f_v1.clone())])
}
fn native_upsertmany(db: &Db) -> String {
    user_batch!(db, gen_upsertmany, run_native_raw_struct_UpsertMany, HandlerNRUpsertMany, PortsNRUpsertManyN0, RawRowNRUpsertManyN0,
        gen_upsertmany::InNRUpsertMany { emails: upsertmany_emails(), names: batch_names() }, &["email", "name"],
        |p: &gen_upsertmany::PortsNRUpsertManyN0| vec![Param::ArrayText(p.f_v0.clone()), Param::ArrayText(p.f_v1.clone())])
}
fn native_updatemany(db: &Db) -> String {
    user_batch!(db, gen_updatemany, run_native_raw_struct_UpdateMany, HandlerNRUpdateMany, PortsNRUpdateManyN0, RawRowNRUpdateManyN0,
        gen_updatemany::InNRUpdateMany { ids: (1..=10).collect(), names: batch_names() }, &["id", "name"],
        |p: &gen_updatemany::PortsNRUpdateManyN0| vec![Param::ArrayInt(p.f_v0.clone()), Param::ArrayText(p.f_v1.clone())])
}

// ── users+posts snapshot — the affected-tables state a write/tx op emits (matches oracle.ts) ──
fn state_json(db: &Db) -> String {
    let users = query(db, "SELECT id, email, name FROM benchmark_users ORDER BY id", &[], |r| user_row(col(r, 0), &col::<String>(r, 1), &col::<String>(r, 2))).unwrap();
    let posts = query(db, "SELECT id, title, author_id FROM benchmark_posts ORDER BY id", &[], |r| post_row(col(r, 0), &col::<String>(r, 1), col(r, 2))).unwrap();
    format!("{{\"users\":{},\"posts\":{}}}", arr(&users), arr(&posts))
}
fn tx_json(committed: bool, db: &Db) -> String { format!("{{\"committed\":{},\"state\":{}}}", committed, state_json(db)) }

// ══ NATIVE read+rel — parent Select + ONE batched relation query (2-level slice; 3-level is #119) ══════
// The single-key relation glue: encode distinct keys as the sqlite JSON AND the pg key array (the seam's
// backend uses whichever it needs), decode the child, extract its target key. ONE child query for all.
macro_rules! rel_single_handler {
    ($module:ident, $relfn:ident, $relports:ident, $relrow:ident, $relelem:ident, $t1:ident { $($f:ident : $idx:literal),+ }, $childkey:expr) => {
        fn $relfn(&self, ports: &$module::$relports, _b: Option<String>) -> Option<$module::$relrow> {
            let keys: Vec<i64> = ports.items.iter().map(|it| it.f_k0).collect();
            if keys.is_empty() { return Some($module::$relrow { is_error: false, err: String::new(), rows: vec![] }); }
            let res = query_batched_relation(self.db, &ports.items[0].f_sql, &keys,
                |ks| format!("[{}]", ks.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(",")),
                |ks| vec![Param::ArrayInt(ks.to_vec())],
                |r| $module::$t1 { $($f : col(r, $idx)),+ }, $childkey);
            Some(match res {
                Ok(l) => $module::$relrow { is_error: false, err: String::new(), rows: l.into_iter().map(|val| $module::$relelem { is_error: false, err: String::new(), val }).collect() },
                Err(e) => $module::$relrow { is_error: true, err: e, rows: vec![] },
            })
        }
    };
}
fn native_nestedfindall(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedfindall::HandlerNRFindAll for H<'_> {
        fn node_n0(&self, p: &gen_nestedfindall::PortsNRFindAllN0, _b: Option<String>) -> Option<gen_nestedfindall::RawRowNRFindAllN0> {
            let val = query(self.db, &p.f_sql, &[Param::Int(p.f_p0)], |r| gen_nestedfindall::T0 { id: col(r, 0), email: col(r, 1), name: col(r, 2) });
            Some(match val { Ok(val) => gen_nestedfindall::RawRowNRFindAllN0 { is_error: false, err: String::new(), val }, Err(e) => gen_nestedfindall::RawRowNRFindAllN0 { is_error: true, err: e, ..Default::default() } })
        }
        rel_single_handler!(gen_nestedfindall, node_rel_posts, PortsNRFindAllRelPostsBatch, RawRowNRFindAllRelPosts, RawElemNRFindAllRelPosts, T1 { id: 0, title: 1, author_id: 2 }, |c| c.author_id);
    }
    let out = gen_nestedfindall::run_native_raw_struct_FindAll(&H { db }, gen_nestedfindall::InNRFindAll).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| user_row(u.id, &u.email, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}
fn native_nestedfindfirst(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedfindfirst::HandlerNRFindFirst for H<'_> {
        fn node_n0(&self, p: &gen_nestedfindfirst::PortsNRFindFirstN0, _b: Option<String>) -> Option<gen_nestedfindfirst::RawRowNRFindFirstN0> {
            let val = query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)], |r| gen_nestedfindfirst::T0 { id: col(r, 0), email: col(r, 1), name: col(r, 2) });
            Some(match val { Ok(val) => gen_nestedfindfirst::RawRowNRFindFirstN0 { is_error: false, err: String::new(), val }, Err(e) => gen_nestedfindfirst::RawRowNRFindFirstN0 { is_error: true, err: e, ..Default::default() } })
        }
        rel_single_handler!(gen_nestedfindfirst, node_rel_posts, PortsNRFindFirstRelPostsBatch, RawRowNRFindFirstRelPosts, RawElemNRFindFirstRelPosts, T1 { id: 0, title: 1, author_id: 2 }, |c| c.author_id);
    }
    let out = gen_nestedfindfirst::run_native_raw_struct_FindFirst(&H { db }, gen_nestedfindfirst::InNRFindFirst { name: "User%".into() }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| user_row(u.id, &u.email, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}
fn native_nestedfindunique(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedfindunique::HandlerNRFindUnique for H<'_> {
        fn node_n0(&self, p: &gen_nestedfindunique::PortsNRFindUniqueN0, _b: Option<String>) -> Option<gen_nestedfindunique::RawRowNRFindUniqueN0> {
            let val = query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)], |r| gen_nestedfindunique::T0 { id: col(r, 0), email: col(r, 1), name: col(r, 2) });
            Some(match val { Ok(val) => gen_nestedfindunique::RawRowNRFindUniqueN0 { is_error: false, err: String::new(), val }, Err(e) => gen_nestedfindunique::RawRowNRFindUniqueN0 { is_error: true, err: e, ..Default::default() } })
        }
        rel_single_handler!(gen_nestedfindunique, node_rel_posts, PortsNRFindUniqueRelPostsBatch, RawRowNRFindUniqueRelPosts, RawElemNRFindUniqueRelPosts, T1 { id: 0, title: 1, author_id: 2 }, |c| c.author_id);
    }
    let out = gen_nestedfindunique::run_native_raw_struct_FindUnique(&H { db }, gen_nestedfindunique::InNRFindUnique { email: "user1@example.com".into() }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| user_row(u.id, &u.email, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}
fn native_nestedrelations(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedrelations::HandlerNRByAuthor for H<'_> {
        fn node_n0(&self, p: &gen_nestedrelations::PortsNRByAuthorN0, _b: Option<String>) -> Option<gen_nestedrelations::RawRowNRByAuthorN0> {
            let val = query(self.db, &p.f_sql, &[Param::Int(p.f_p0)], |r| gen_nestedrelations::T0 { id: col(r, 0), title: col(r, 1), author_id: col(r, 2) });
            Some(match val { Ok(val) => gen_nestedrelations::RawRowNRByAuthorN0 { is_error: false, err: String::new(), val }, Err(e) => gen_nestedrelations::RawRowNRByAuthorN0 { is_error: true, err: e, ..Default::default() } })
        }
        rel_single_handler!(gen_nestedrelations, node_rel_comments, PortsNRByAuthorRelCommentsBatch, RawRowNRByAuthorRelComments, RawElemNRByAuthorRelComments, T1 { id: 0, body: 1, post_id: 2 }, |c| c.post_id);
    }
    let out = gen_nestedrelations::run_native_raw_struct_ByAuthor(&H { db }, gen_nestedrelations::InNRByAuthor { author_id: 7 }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect();
    let children: Vec<String> = out.comments.iter().map(|cs| arr(&cs.iter().map(|c| comment_row(c.id, &c.body, c.post_id)).collect::<Vec<_>>())).collect();
    rel_json("comments", &parents, &children)
}
fn native_compositerelations(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_compositerelations::HandlerNRByTenant for H<'_> {
        fn node_n0(&self, p: &gen_compositerelations::PortsNRByTenantN0, _b: Option<String>) -> Option<gen_compositerelations::RawRowNRByTenantN0> {
            let val = query(self.db, &p.f_sql, &[Param::Int(p.f_p0)], |r| gen_compositerelations::T0 { tenant_id: col(r, 0), user_id: col(r, 1), name: col(r, 2) });
            Some(match val { Ok(val) => gen_compositerelations::RawRowNRByTenantN0 { is_error: false, err: String::new(), val }, Err(e) => gen_compositerelations::RawRowNRByTenantN0 { is_error: true, err: e, ..Default::default() } })
        }
        fn node_rel_posts(&self, ports: &gen_compositerelations::PortsNRByTenantRelPostsBatch, _b: Option<String>) -> Option<gen_compositerelations::RawRowNRByTenantRelPosts> {
            let keys: Vec<(i64, i64)> = ports.items.iter().map(|it| (it.f_k0, it.f_k1)).collect();
            if keys.is_empty() { return Some(gen_compositerelations::RawRowNRByTenantRelPosts { is_error: false, err: String::new(), rows: vec![] }); }
            let res = query_batched_relation(self.db, &ports.items[0].f_sql, &keys,
                |ks| format!("[{}]", ks.iter().map(|(t, u)| format!("[{},{}]", t, u)).collect::<Vec<_>>().join(",")),
                |ks| vec![Param::ArrayInt(ks.iter().map(|(t, _)| *t).collect()), Param::ArrayInt(ks.iter().map(|(_, u)| *u).collect())],
                |r| gen_compositerelations::T1 { tenant_id: col(r, 0), post_id: col(r, 1), user_id: col(r, 2), title: col(r, 3) }, |c| (c.tenant_id, c.user_id));
            Some(match res {
                Ok(l) => gen_compositerelations::RawRowNRByTenantRelPosts { is_error: false, err: String::new(), rows: l.into_iter().map(|val| gen_compositerelations::RawElemNRByTenantRelPosts { is_error: false, err: String::new(), val }).collect() },
                Err(e) => gen_compositerelations::RawRowNRByTenantRelPosts { is_error: true, err: e, rows: vec![] },
            })
        }
    }
    let out = gen_compositerelations::run_native_raw_struct_ByTenant(&H { db }, gen_compositerelations::InNRByTenant { tenant_id: 1 }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| tuser_row(u.tenant_id, u.user_id, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| tpost_row(p.tenant_id, p.post_id, p.user_id, &p.title)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}

// ══ NATIVE tx — the transaction envelope + the chain runner; result = {committed, state} ══════════════
fn native_delete(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_delete::HandlerNRDelete for H<'_> {
        fn node_tx_body_0(&self, p: &gen_delete::PortsNRDeleteTxBody0, _b: Option<String>) -> Option<gen_delete::RawRowNRDeleteTxBody0> {
            let rows = query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())], |r| col::<i64>(r, 0));
            Some(match rows { Ok(v) if !v.is_empty() => gen_delete::RawRowNRDeleteTxBody0 { is_error: false, err: String::new(), id: v[0] }, Ok(_) => gen_delete::RawRowNRDeleteTxBody0 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_delete::RawRowNRDeleteTxBody0 { is_error: true, err: e, ..Default::default() } })
        }
        fn node_tx_body_1(&self, p: &gen_delete::PortsNRDeleteTxBody1, _b: Option<String>) -> Option<gen_delete::RawRowNRDeleteTxBody1> {
            Some(match execute(self.db, &p.f_sql, &[Param::Int(p.f_p0)]) { Ok(s) => gen_delete::RawRowNRDeleteTxBody1 { is_error: false, err: String::new(), changes: s.changes, lastInsertRowid: s.last_insert_rowid }, Err(e) => gen_delete::RawRowNRDeleteTxBody1 { is_error: true, err: e, ..Default::default() } })
        }
    }
    let r = transaction(db, |c| gen_delete::run_native_raw_struct_Delete(&H { db: c }, gen_delete::InNRDelete { email: "del0@bench.com".into(), name: "Del".into() }));
    tx_json(r.is_ok(), db)
}
fn native_nestedcreate(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedcreate::HandlerNRNestedCreate for H<'_> {
        fn node_tx_body_0(&self, p: &gen_nestedcreate::PortsNRNestedCreateTxBody0, _b: Option<String>) -> Option<gen_nestedcreate::RawRowNRNestedCreateTxBody0> {
            let rows = query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())], |r| col::<i64>(r, 0));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedcreate::RawRowNRNestedCreateTxBody0 { is_error: false, err: String::new(), id: v[0] }, Ok(_) => gen_nestedcreate::RawRowNRNestedCreateTxBody0 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedcreate::RawRowNRNestedCreateTxBody0 { is_error: true, err: e, ..Default::default() } })
        }
        fn node_tx_body_1(&self, p: &gen_nestedcreate::PortsNRNestedCreateTxBody1, _b: Option<String>) -> Option<gen_nestedcreate::RawRowNRNestedCreateTxBody1> {
            let rows = query(self.db, &p.f_sql, &[Param::Int(p.f_p0), Param::Text(p.f_p1.clone())], |r| (col::<i64>(r, 0), col::<i64>(r, 1), col::<String>(r, 2)));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedcreate::RawRowNRNestedCreateTxBody1 { is_error: false, err: String::new(), id: v[0].0, author_id: v[0].1, title: v[0].2.clone() }, Ok(_) => gen_nestedcreate::RawRowNRNestedCreateTxBody1 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedcreate::RawRowNRNestedCreateTxBody1 { is_error: true, err: e, ..Default::default() } })
        }
    }
    let r = transaction(db, |c| gen_nestedcreate::run_native_raw_struct_NestedCreate(&H { db: c }, gen_nestedcreate::InNRNestedCreate { email: "nc@bench.com".into(), name: "NC".into(), title: "NC Post".into() }));
    tx_json(r.is_ok(), db)
}
fn native_nestedupdate(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedupdate::HandlerNRNestedUpdate for H<'_> {
        fn node_tx_body_0(&self, p: &gen_nestedupdate::PortsNRNestedUpdateTxBody0, _b: Option<String>) -> Option<gen_nestedupdate::RawRowNRNestedUpdateTxBody0> {
            let rows = query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)], |r| (col::<i64>(r, 0), col::<String>(r, 1)));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedupdate::RawRowNRNestedUpdateTxBody0 { is_error: false, err: String::new(), id: v[0].0, name: v[0].1.clone() }, Ok(_) => gen_nestedupdate::RawRowNRNestedUpdateTxBody0 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedupdate::RawRowNRNestedUpdateTxBody0 { is_error: true, err: e, ..Default::default() } })
        }
        fn node_tx_body_1(&self, p: &gen_nestedupdate::PortsNRNestedUpdateTxBody1, _b: Option<String>) -> Option<gen_nestedupdate::RawRowNRNestedUpdateTxBody1> {
            let rows = query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)], |r| (col::<i64>(r, 0), col::<String>(r, 1)));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedupdate::RawRowNRNestedUpdateTxBody1 { is_error: false, err: String::new(), id: v[0].0, title: v[0].1.clone() }, Ok(_) => gen_nestedupdate::RawRowNRNestedUpdateTxBody1 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedupdate::RawRowNRNestedUpdateTxBody1 { is_error: true, err: e, ..Default::default() } })
        }
    }
    let r = transaction(db, |c| gen_nestedupdate::run_native_raw_struct_NestedUpdate(&H { db: c }, gen_nestedupdate::InNRNestedUpdate { name: "NU".into(), user_id: 7, title: "NU Post".into() }));
    tx_json(r.is_ok(), db)
}
fn native_nestedupsert(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedupsert::HandlerNRNestedUpsert for H<'_> {
        fn node_tx_body_0(&self, p: &gen_nestedupsert::PortsNRNestedUpsertTxBody0, _b: Option<String>) -> Option<gen_nestedupsert::RawRowNRNestedUpsertTxBody0> {
            let rows = query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())], |r| col::<i64>(r, 0));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedupsert::RawRowNRNestedUpsertTxBody0 { is_error: false, err: String::new(), id: v[0] }, Ok(_) => gen_nestedupsert::RawRowNRNestedUpsertTxBody0 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedupsert::RawRowNRNestedUpsertTxBody0 { is_error: true, err: e, ..Default::default() } })
        }
        fn node_tx_body_1(&self, p: &gen_nestedupsert::PortsNRNestedUpsertTxBody1, _b: Option<String>) -> Option<gen_nestedupsert::RawRowNRNestedUpsertTxBody1> {
            let rows = query(self.db, &p.f_sql, &[Param::Int(p.f_p0), Param::Text(p.f_p1.clone())], |r| (col::<i64>(r, 0), col::<i64>(r, 1), col::<String>(r, 2)));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedupsert::RawRowNRNestedUpsertTxBody1 { is_error: false, err: String::new(), id: v[0].0, author_id: v[0].1, title: v[0].2.clone() }, Ok(_) => gen_nestedupsert::RawRowNRNestedUpsertTxBody1 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedupsert::RawRowNRNestedUpsertTxBody1 { is_error: true, err: e, ..Default::default() } })
        }
    }
    let r = transaction(db, |c| gen_nestedupsert::run_native_raw_struct_NestedUpsert(&H { db: c }, gen_nestedupsert::InNRNestedUpsert { email: "user1@example.com".into(), name: "NUp".into(), title: "NUp Post".into() }));
    tx_json(r.is_ok(), db)
}

// ══ SDK BASELINE — raw driver + hand-SQL for benchmark_* (the ONLY hand-written execution) ═════════════
// Executed through the SAME seam (thin wrapper); the hand-SQL differs only in the dialect's placeholder /
// upsert / array forms (`ph()` + a few cfg'd literals). Fair — single statements, no naive N-query loops.
fn sdk_users(db: &Db, sql: &str, params: &[Param]) -> String {
    let rows = query(db, sql, params, |r| user_row(col(r, 0), &col::<String>(r, 1), &col::<String>(r, 2))).unwrap();
    arr(&rows)
}
fn sdk_findall(db: &Db) -> String { sdk_users(db, "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", &[]) }
fn sdk_findfirst(db: &Db) -> String { sdk_users(db, &format!("SELECT id, email, name FROM benchmark_users WHERE name LIKE {} LIMIT 1", ph(1)), &[Param::Text("User%".into())]) }
fn sdk_findunique(db: &Db) -> String { sdk_users(db, &format!("SELECT id, email, name FROM benchmark_users WHERE email = {} LIMIT 1", ph(1)), &[Param::Text("user500@example.com".into())]) }
fn sdk_filterpaginatesort(db: &Db) -> String {
    let sql = format!("SELECT id, title, content, published, author_id, created_at FROM benchmark_posts WHERE published = {} ORDER BY created_at DESC LIMIT 20 OFFSET 10", ph(1));
    let rows = query(db, &sql, &[Param::Int(1)], |r| {
        obj(&[ji("id", col(r, 0)), js("title", &col::<String>(r, 1)), js("content", &col::<String>(r, 2)), ji("published", read_published(r, 3)), ji("author_id", col(r, 4)), js("created_at", &col::<String>(r, 5))])
    }).unwrap();
    arr(&rows)
}
fn sdk_create(db: &Db) -> String { sdk_users(db, &format!("INSERT INTO benchmark_users (email, name) VALUES ({}, {}) RETURNING id, email, name", ph(1), ph(2)), &[Param::Text("new@bench.com".into()), Param::Text("New".into())]) }
fn sdk_update(db: &Db) -> String { sdk_users(db, &format!("UPDATE benchmark_users SET name = {} WHERE id = {} RETURNING id, email, name", ph(1), ph(2)), &[Param::Text("Updated 100".into()), Param::Int(100)]) }
fn sdk_upsert(db: &Db) -> String {
    let excl = upsert_excluded();
    sdk_users(db, &format!("INSERT INTO benchmark_users (email, name) VALUES ({}, {}) ON CONFLICT (email) DO UPDATE SET email = {}.email, name = {}.name RETURNING id, email, name", ph(1), ph(2), excl, excl), &[Param::Text("user1@example.com".into()), Param::Text("Upserted One".into())])
}
/// The upsert `EXCLUDED`/`excluded` keyword (pg upper, sqlite lower) — the ONE dialect verb the SDK upsert
/// text differs by (the natural raw-driver form; the NATIVE upsert SQL is generated).
#[cfg(feature = "postgres")]
fn upsert_excluded() -> &'static str { "EXCLUDED" }
#[cfg(not(feature = "postgres"))]
fn upsert_excluded() -> &'static str { "excluded" }
fn sdk_createmany(db: &Db) -> String {
    let (e, n) = (batch_emails(), batch_names());
    let vals = (0..e.len()).map(|i| format!("({}, {})", ph(2 * i + 1), ph(2 * i + 2))).collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO benchmark_users (email, name) VALUES {} RETURNING id, email, name", vals);
    let mut params: Vec<Param> = Vec::new();
    for i in 0..e.len() { params.push(Param::Text(e[i].clone())); params.push(Param::Text(n[i].clone())); }
    sdk_users(db, &sql, &params)
}
fn sdk_upsertmany(db: &Db) -> String {
    let (e, n) = (upsertmany_emails(), batch_names());
    let excl = upsert_excluded();
    let vals = (0..e.len()).map(|i| format!("({}, {})", ph(2 * i + 1), ph(2 * i + 2))).collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO benchmark_users (email, name) VALUES {} ON CONFLICT (email) DO UPDATE SET email = {}.email, name = {}.name RETURNING id, email, name", vals, excl, excl);
    let mut params: Vec<Param> = Vec::new();
    for i in 0..e.len() { params.push(Param::Text(e[i].clone())); params.push(Param::Text(n[i].clone())); }
    sdk_users(db, &sql, &params)
}
fn sdk_updatemany(db: &Db) -> String {
    // Hand-OPTIMIZED: ONE CASE update for ids 1..10, RETURNING the updated rows (not a per-row loop).
    let names = batch_names();
    let cases: String = (1..=10).map(|id| format!("WHEN {} THEN {}", id, ph(id))).collect::<Vec<_>>().join(" ");
    let sql = format!("UPDATE benchmark_users SET name = CASE id {} END WHERE id IN (1,2,3,4,5,6,7,8,9,10) RETURNING id, email, name", cases);
    let params: Vec<Param> = names.iter().map(|n| Param::Text(n.clone())).collect();
    sdk_users(db, &sql, &params)
}

// SDK read+rel — raw driver: parent query + ONE batched child query (IN-list / `= ANY`) + client stitch.
fn sdk_rel_single(db: &Db, parent_sql: &str, parent_params: &[Param], parent_ser: impl Fn(&dyn Cell) -> (i64, String), child_sql_in: &str, child_ser: impl Fn(&dyn Cell) -> (i64, String), rel: &str) -> String {
    let parents = query(db, parent_sql, parent_params, |r| parent_ser(r)).unwrap();
    let keys: Vec<i64> = parents.iter().map(|(k, _)| *k).collect();
    let (child_sql, child_params) = child_in_clause(child_sql_in, &keys);
    let children = query(db, &child_sql, &child_params, |r| child_ser(r)).unwrap();
    let mut groups: std::collections::HashMap<i64, Vec<String>> = std::collections::HashMap::new();
    for (k, j) in children { groups.entry(k).or_default().push(j); }
    let ps: Vec<String> = parents.iter().map(|(_, j)| j.clone()).collect();
    let cs: Vec<String> = parents.iter().map(|(k, _)| arr(groups.get(k).map(|v| v.as_slice()).unwrap_or(&[]))).collect();
    rel_json(rel, &ps, &cs)
}
/// The raw-driver batched child IN-clause: pg binds ONE `= ANY($1::int[])` array; sqlite binds an
/// `IN (?,?,…)` list. `{IN}` in `child_sql_in` is the placeholder site.
#[cfg(feature = "postgres")]
fn child_in_clause(child_sql_in: &str, keys: &[i64]) -> (String, Vec<Param>) {
    (child_sql_in.replace("{IN}", "= ANY($1::int[])"), vec![Param::ArrayInt(keys.to_vec())])
}
#[cfg(not(feature = "postgres"))]
fn child_in_clause(child_sql_in: &str, keys: &[i64]) -> (String, Vec<Param>) {
    let inlist = keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    (child_sql_in.replace("{IN}", &format!("IN ({})", inlist)), keys.iter().map(|&k| Param::Int(k)).collect())
}
fn sdk_nestedfindall(db: &Db) -> String {
    sdk_rel_single(db, "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", &[],
        |r| (col(r, 0), user_row(col(r, 0), &col::<String>(r, 1), &col::<String>(r, 2))),
        "SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC",
        |r| (col(r, 2), post_row(col(r, 0), &col::<String>(r, 1), col(r, 2))), "posts")
}
fn sdk_nestedfindfirst(db: &Db) -> String {
    sdk_rel_single(db, &format!("SELECT id, email, name FROM benchmark_users WHERE name LIKE {} LIMIT 1", ph(1)), &[Param::Text("User%".into())],
        |r| (col(r, 0), user_row(col(r, 0), &col::<String>(r, 1), &col::<String>(r, 2))),
        "SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC",
        |r| (col(r, 2), post_row(col(r, 0), &col::<String>(r, 1), col(r, 2))), "posts")
}
fn sdk_nestedfindunique(db: &Db) -> String {
    sdk_rel_single(db, &format!("SELECT id, email, name FROM benchmark_users WHERE email = {} LIMIT 1", ph(1)), &[Param::Text("user1@example.com".into())],
        |r| (col(r, 0), user_row(col(r, 0), &col::<String>(r, 1), &col::<String>(r, 2))),
        "SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC",
        |r| (col(r, 2), post_row(col(r, 0), &col::<String>(r, 1), col(r, 2))), "posts")
}
fn sdk_nestedrelations(db: &Db) -> String {
    sdk_rel_single(db, &format!("SELECT id, title, author_id FROM benchmark_posts WHERE author_id = {} ORDER BY id ASC", ph(1)), &[Param::Int(7)],
        |r| (col(r, 0), post_row(col(r, 0), &col::<String>(r, 1), col(r, 2))),
        "SELECT id, body, post_id FROM benchmark_comments WHERE post_id {IN} ORDER BY id ASC",
        |r| (col(r, 2), comment_row(col(r, 0), &col::<String>(r, 1), col(r, 2))), "comments")
}
fn sdk_compositerelations(db: &Db) -> String {
    let parents = query(db, &format!("SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = {} ORDER BY user_id ASC", ph(1)), &[Param::Int(1)],
        |r| (col::<i64>(r, 1), tuser_row(col(r, 0), col(r, 1), &col::<String>(r, 2)))).unwrap();
    let children = query(db, &format!("SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE tenant_id = {} ORDER BY post_id ASC", ph(1)), &[Param::Int(1)],
        |r| (col::<i64>(r, 2), tpost_row(col(r, 0), col(r, 1), col(r, 2), &col::<String>(r, 3)))).unwrap();
    let mut groups: std::collections::HashMap<i64, Vec<String>> = std::collections::HashMap::new();
    for (u, j) in children { groups.entry(u).or_default().push(j); }
    let ps: Vec<String> = parents.iter().map(|(_, j)| j.clone()).collect();
    let cs: Vec<String> = parents.iter().map(|(u, _)| arr(groups.get(u).map(|v| v.as_slice()).unwrap_or(&[]))).collect();
    rel_json("posts", &ps, &cs)
}

// SDK tx — raw driver BEGIN … COMMIT/ROLLBACK via the seam envelope, then the {committed, state} snapshot.
fn sdk_tx(db: &Db, body: impl FnOnce(&Db) -> Result<(), String>) -> bool {
    transaction(db, body).is_ok()
}
fn sdk_delete(db: &Db) -> String {
    let ok = sdk_tx(db, |c| {
        let id = query(c, &format!("INSERT INTO benchmark_users (email, name) VALUES ({}, {}) RETURNING id", ph(1), ph(2)), &[Param::Text("del0@bench.com".into()), Param::Text("Del".into())], |r| col::<i64>(r, 0))?;
        let id = *id.first().ok_or("no id")?;
        execute(c, &format!("DELETE FROM benchmark_users WHERE id = {}", ph(1)), &[Param::Int(id)])?;
        Ok(())
    });
    tx_json(ok, db)
}
fn sdk_nestedcreate(db: &Db) -> String {
    let ok = sdk_tx(db, |c| {
        let id = query(c, &format!("INSERT INTO benchmark_users (email, name) VALUES ({}, {}) RETURNING id", ph(1), ph(2)), &[Param::Text("nc@bench.com".into()), Param::Text("NC".into())], |r| col::<i64>(r, 0))?;
        let id = *id.first().ok_or("no id")?;
        execute(c, &format!("INSERT INTO benchmark_posts (author_id, title) VALUES ({}, {})", ph(1), ph(2)), &[Param::Int(id), Param::Text("NC Post".into())])?;
        Ok(())
    });
    tx_json(ok, db)
}
fn sdk_nestedupdate(db: &Db) -> String {
    let ok = sdk_tx(db, |c| {
        execute(c, &format!("UPDATE benchmark_users SET name = {} WHERE id = {}", ph(1), ph(2)), &[Param::Text("NU".into()), Param::Int(7)])?;
        execute(c, &format!("UPDATE benchmark_posts SET title = {} WHERE author_id = {}", ph(1), ph(2)), &[Param::Text("NU Post".into()), Param::Int(7)])?;
        Ok(())
    });
    tx_json(ok, db)
}
fn sdk_nestedupsert(db: &Db) -> String {
    let excl = upsert_excluded();
    let ok = sdk_tx(db, |c| {
        let id = query(c, &format!("INSERT INTO benchmark_users (email, name) VALUES ({}, {}) ON CONFLICT (email) DO UPDATE SET email = {}.email, name = {}.name RETURNING id", ph(1), ph(2), excl, excl), &[Param::Text("user1@example.com".into()), Param::Text("NUp".into())], |r| col::<i64>(r, 0))?;
        let id = *id.first().ok_or("no id")?;
        execute(c, &format!("INSERT INTO benchmark_posts (author_id, title) VALUES ({}, {})", ph(1), ph(2)), &[Param::Int(id), Param::Text("NUp Post".into())])?;
        Ok(())
    });
    tx_json(ok, db)
}

// ══ dispatch ══════════════════════════════════════════════════════════════════════════════════════════
fn run_native(op: &str, db: &Db) -> String {
    match op {
        "findAll" => native_findall(db), "filterPaginateSort" => native_filterpaginatesort(db), "findFirst" => native_findfirst(db), "findUnique" => native_findunique(db),
        "create" => native_create(db), "update" => native_update(db), "upsert" => native_upsert(db),
        "createMany" => native_createmany(db), "upsertMany" => native_upsertmany(db), "updateMany" => native_updatemany(db),
        "nestedFindAll" => native_nestedfindall(db), "nestedFindFirst" => native_nestedfindfirst(db), "nestedFindUnique" => native_nestedfindunique(db),
        "nestedRelations" => native_nestedrelations(db), "compositeRelations" => native_compositerelations(db),
        "delete" => native_delete(db), "nestedCreate" => native_nestedcreate(db), "nestedUpdate" => native_nestedupdate(db), "nestedUpsert" => native_nestedupsert(db),
        _ => panic!("native: unknown op '{op}'"),
    }
}
fn run_sdk(op: &str, db: &Db) -> String {
    match op {
        "findAll" => sdk_findall(db), "filterPaginateSort" => sdk_filterpaginatesort(db), "findFirst" => sdk_findfirst(db), "findUnique" => sdk_findunique(db),
        "create" => sdk_create(db), "update" => sdk_update(db), "upsert" => sdk_upsert(db),
        "createMany" => sdk_createmany(db), "upsertMany" => sdk_upsertmany(db), "updateMany" => sdk_updatemany(db),
        "nestedFindAll" => sdk_nestedfindall(db), "nestedFindFirst" => sdk_nestedfindfirst(db), "nestedFindUnique" => sdk_nestedfindunique(db),
        "nestedRelations" => sdk_nestedrelations(db), "compositeRelations" => sdk_compositerelations(db),
        "delete" => sdk_delete(db), "nestedCreate" => sdk_nestedcreate(db), "nestedUpdate" => sdk_nestedupdate(db), "nestedUpsert" => sdk_nestedupsert(db),
        _ => panic!("sdk: unknown op '{op}'"),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("");
    match mode {
        // run <op> <target> <native|sdk> — <target> is a sqlite path OR a postgres connection string.
        "run" => {
            let op = args.get(2).expect("run <op>");
            let target = args.get(3).expect("run <op> <target>");
            let cell = args.get(4).expect("run <op> <target> <native|sdk>");
            let db = Db::open(target);
            let out = if cell == "native" { run_native(op, &db) } else { run_sdk(op, &db) };
            println!("{out}");
        }
        #[cfg(feature = "sqlite")]
        "bench" => bench_sqlite(&args),
        _ => {
            eprintln!("usage: orm_bench_rust run <op> <target> <native|sdk>");
            std::process::exit(2);
        }
    }
}

// Latency CSV (sqlite in-proc only): reads time on the seed; mutating ops reset (copy seed) per iter.
#[cfg(feature = "sqlite")]
fn bench_sqlite(args: &[String]) {
    use std::io::Write;
    const READ_OPS: &[&str] = &["findAll", "filterPaginateSort", "findFirst", "findUnique", "nestedFindAll", "nestedFindFirst", "nestedFindUnique", "nestedRelations", "compositeRelations"];
    const ALL: &[&str] = &["findAll", "filterPaginateSort", "findFirst", "findUnique", "nestedFindAll", "nestedFindFirst", "nestedFindUnique", "nestedRelations", "compositeRelations", "create", "update", "upsert", "createMany", "upsertMany", "updateMany", "delete", "nestedCreate", "nestedUpdate", "nestedUpsert"];
    let seed = args.get(2).expect("seed_db");
    let warmup: usize = args.get(3).expect("warmup").parse().unwrap();
    let iters: usize = args.get(4).expect("iters").parse().unwrap();
    let mut csv = std::fs::File::create(args.get(5).expect("out_csv")).unwrap();
    writeln!(csv, "op,cell,us").unwrap();
    for &op in ALL {
        let mutating = !READ_OPS.contains(&op);
        let n = if mutating { iters.min(500) } else { iters };
        for cell in ["native", "sdk"] {
            if !mutating {
                let db = Db::open(seed);
                for _ in 0..warmup { std::hint::black_box(run_cell(cell, op, &db)); }
                for _ in 0..n {
                    let t0 = std::time::Instant::now();
                    let r = run_cell(cell, op, &db);
                    std::hint::black_box(&r);
                    writeln!(csv, "{},{},{:.3}", op, cell, t0.elapsed().as_nanos() as f64 / 1000.0).unwrap();
                }
            } else {
                let tmp = format!("{seed}.{op}.{cell}.work");
                for i in 0..(warmup.min(50) + n) {
                    std::fs::copy(seed, &tmp).unwrap();
                    let db = Db::open(&tmp);
                    let t0 = std::time::Instant::now();
                    let r = run_cell(cell, op, &db);
                    let us = t0.elapsed().as_nanos() as f64 / 1000.0;
                    std::hint::black_box(&r);
                    drop(db);
                    if i >= warmup.min(50) { writeln!(csv, "{},{},{:.3}", op, cell, us).unwrap(); }
                }
                let _ = std::fs::remove_file(&tmp);
            }
        }
    }
    eprintln!("rust bench done: {} ops × (native, sdk)", ALL.len());
}
#[cfg(feature = "sqlite")]
fn run_cell(cell: &str, op: &str, db: &Db) -> String {
    if cell == "native" { run_native(op, db) } else { run_sdk(op, db) }
}
