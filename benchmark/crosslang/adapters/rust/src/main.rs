//! Main-bench RUST cell — the CLI-generated typed-native modules (adapters/rust/generated/, NEVER hand-
//! written) + the generic exec seam (seam.rs, generalized from e1_native_proof with prepare_cached +
//! move-not-clone) + thin per-op leaf handlers. Also the SDK BASELINE (raw rusqlite + hand-SQL for the
//! benchmark_* schema) — the only hand-written execution. Both produce a CANONICAL result string a node
//! driver byte-compares to the mode-2 oracle.
//!
//! Modes:
//!   orm_bench_rust run <op> <db> <native|sdk>            → print the canonical result for one op × cell
//!   orm_bench_rust bench <read_db> <write_db> <w> <n> <csv> → latency CSV (native + sdk per op)
//!
//! This round covers the 10 FLAT-result ops (reads / writes / batch). read+rel (5) + tx (4) are the next
//! rust slice (they need the {rows,rel} / {committed,state} serialization).
#![allow(non_snake_case)]

#[path = "seam.rs"]
mod seam;
use rusqlite::Connection;
use seam::{execute, json_str, query, query_batch_write, query_batched_relation, transaction, Param};

#[path = "../generated/gen_findall.rs"]
mod gen_findall;
#[path = "../generated/gen_nestedfindall.rs"]
mod gen_nestedfindall;
#[path = "../generated/gen_nestedfindfirst.rs"]
mod gen_nestedfindfirst;
#[path = "../generated/gen_nestedfindunique.rs"]
mod gen_nestedfindunique;
#[path = "../generated/gen_nestedrelations.rs"]
mod gen_nestedrelations;
#[path = "../generated/gen_compositerelations.rs"]
mod gen_compositerelations;
#[path = "../generated/gen_delete.rs"]
mod gen_delete;
#[path = "../generated/gen_nestedcreate.rs"]
mod gen_nestedcreate;
#[path = "../generated/gen_nestedupdate.rs"]
mod gen_nestedupdate;
#[path = "../generated/gen_nestedupsert.rs"]
mod gen_nestedupsert;
#[path = "../generated/gen_filterpaginatesort.rs"]
mod gen_filterpaginatesort;
#[path = "../generated/gen_findfirst.rs"]
mod gen_findfirst;
#[path = "../generated/gen_findunique.rs"]
mod gen_findunique;
#[path = "../generated/gen_create.rs"]
mod gen_create;
#[path = "../generated/gen_update.rs"]
mod gen_update;
#[path = "../generated/gen_upsert.rs"]
mod gen_upsert;
#[path = "../generated/gen_createmany.rs"]
mod gen_createmany;
#[path = "../generated/gen_upsertmany.rs"]
mod gen_upsertmany;
#[path = "../generated/gen_updatemany.rs"]
mod gen_updatemany;

// ── canonical row JSON: {"k":v,…} — int bare, string json-quoted; matches oracle.ts canonVal/canonRow ──
fn ji(k: &str, v: i64) -> String {
    format!("{}:{}", json_str(k), v)
}
fn js(k: &str, v: &str) -> String {
    format!("{}:{}", json_str(k), json_str(v))
}
fn obj(fields: &[String]) -> String {
    format!("{{{}}}", fields.join(","))
}
fn arr(rows: &[String]) -> String {
    format!("[{}]", rows.join(","))
}
/// A benchmark_users row {id,email,name} — the shape most read/write ops project.
fn user_row(id: i64, email: &str, name: &str) -> String {
    obj(&[ji("id", id), js("email", email), js("name", name)])
}

// ══ NATIVE cells — run the CLI-generated module + the seam, decode, canonicalize ══════════════════════

macro_rules! user_reader {
    ($conn:expr, $module:ident, $comp:ident, $handler:ident, $ports:ident, $row:ident, $in:expr, $bind:expr) => {{
        struct H<'a> { conn: &'a Connection }
        impl $module::$handler for H<'_> {
            fn node_n0(&self, ports: &$module::$ports, _b: Option<String>) -> Option<$module::$row> {
                let val = query(self.conn, &ports.f_sql, &$bind(ports), |r| {
                    Ok($module::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
                });
                Some(match val {
                    Ok(val) => $module::$row { is_error: false, err: String::new(), val },
                    Err(e) => $module::$row { is_error: true, err: e.to_string(), ..Default::default() },
                })
            }
        }
        let out = $module::$comp(&H { conn: $conn }, $in).unwrap();
        arr(&out.iter().map(|r| user_row(r.id, &r.email, &r.name)).collect::<Vec<_>>())
    }};
}

fn native_findall(conn: &Connection) -> String {
    user_reader!(conn, gen_findall, run_native_raw_struct_FindAll, HandlerNRFindAll, PortsNRFindAllN0, RawRowNRFindAllN0,
        gen_findall::InNRFindAll, |p: &gen_findall::PortsNRFindAllN0| [Param::Int(p.f_p0)])
}
fn native_findfirst(conn: &Connection) -> String {
    user_reader!(conn, gen_findfirst, run_native_raw_struct_FindFirst, HandlerNRFindFirst, PortsNRFindFirstN0, RawRowNRFindFirstN0,
        gen_findfirst::InNRFindFirst { name: "User%".into() }, |p: &gen_findfirst::PortsNRFindFirstN0| [Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)])
}
fn native_findunique(conn: &Connection) -> String {
    user_reader!(conn, gen_findunique, run_native_raw_struct_FindUnique, HandlerNRFindUnique, PortsNRFindUniqueN0, RawRowNRFindUniqueN0,
        gen_findunique::InNRFindUnique { email: "user500@example.com".into() }, |p: &gen_findunique::PortsNRFindUniqueN0| [Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)])
}

fn native_filterpaginatesort(conn: &Connection) -> String {
    struct H<'a> { conn: &'a Connection }
    impl gen_filterpaginatesort::HandlerNRFilterPaginateSort for H<'_> {
        fn node_n0(&self, ports: &gen_filterpaginatesort::PortsNRFilterPaginateSortN0, _b: Option<String>) -> Option<gen_filterpaginatesort::RawRowNRFilterPaginateSortN0> {
            let val = query(self.conn, &ports.f_sql, &[Param::Int(ports.f_p0), Param::Int(ports.f_p1), Param::Int(ports.f_p2)], |r| {
                Ok(gen_filterpaginatesort::T0 { id: r.get(0)?, title: r.get(1)?, content: r.get::<_, Option<String>>(2)?.unwrap_or_default(), published: r.get(3)?, author_id: r.get(4)?, created_at: r.get(5)? })
            });
            Some(match val {
                Ok(val) => gen_filterpaginatesort::RawRowNRFilterPaginateSortN0 { is_error: false, err: String::new(), val },
                Err(e) => gen_filterpaginatesort::RawRowNRFilterPaginateSortN0 { is_error: true, err: e.to_string(), ..Default::default() },
            })
        }
    }
    let out = gen_filterpaginatesort::run_native_raw_struct_FilterPaginateSort(&H { conn }, gen_filterpaginatesort::InNRFilterPaginateSort { published: 1 }).unwrap();
    arr(&out.iter().map(|r| obj(&[ji("id", r.id), js("title", &r.title), js("content", &r.content), ji("published", r.published), ji("author_id", r.author_id), js("created_at", &r.created_at)])).collect::<Vec<_>>())
}

macro_rules! user_writer {
    ($conn:expr, $module:ident, $comp:ident, $handler:ident, $ports:ident, $row:ident, $in:expr, $bind:expr) => {{
        struct H<'a> { conn: &'a Connection }
        impl $module::$handler for H<'_> {
            fn node_n0(&self, ports: &$module::$ports, _b: Option<String>) -> Option<$module::$row> {
                let val = query(self.conn, &ports.f_sql, &$bind(ports), |r| {
                    Ok($module::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
                });
                Some(match val {
                    Ok(val) => $module::$row { is_error: false, err: String::new(), val },
                    Err(e) => $module::$row { is_error: true, err: e.to_string(), ..Default::default() },
                })
            }
        }
        let out = $module::$comp(&H { conn: $conn }, $in).unwrap();
        arr(&out.iter().map(|r| user_row(r.id, &r.email, &r.name)).collect::<Vec<_>>())
    }};
}
fn native_create(conn: &Connection) -> String {
    user_writer!(conn, gen_create, run_native_raw_struct_Create, HandlerNRCreate, PortsNRCreateN0, RawRowNRCreateN0,
        gen_create::InNRCreate { email: "new@bench.com".into(), name: "New".into() }, |p: &gen_create::PortsNRCreateN0| [Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())])
}
fn native_update(conn: &Connection) -> String {
    user_writer!(conn, gen_update, run_native_raw_struct_Update, HandlerNRUpdate, PortsNRUpdateN0, RawRowNRUpdateN0,
        gen_update::InNRUpdate { name: "Updated 100".into(), id: 100 }, |p: &gen_update::PortsNRUpdateN0| [Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)])
}
fn native_upsert(conn: &Connection) -> String {
    user_writer!(conn, gen_upsert, run_native_raw_struct_Upsert, HandlerNRUpsert, PortsNRUpsertN0, RawRowNRUpsertN0,
        gen_upsert::InNRUpsert { email: "user1@example.com".into(), name: "Upserted One".into() }, |p: &gen_upsert::PortsNRUpsertN0| [Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())])
}

macro_rules! user_batch {
    ($conn:expr, $module:ident, $comp:ident, $handler:ident, $ports:ident, $row:ident, $in:expr, $cols:expr, $cells:expr) => {{
        struct H<'a> { conn: &'a Connection }
        impl $module::$handler for H<'_> {
            fn node_n0(&self, ports: &$module::$ports, _b: Option<String>) -> Option<$module::$row> {
                let (cols, cells): (&[&str], Vec<Vec<String>>) = $cells(ports);
                let cell_refs: Vec<&[String]> = cells.iter().map(|c| c.as_slice()).collect();
                let val = query_batch_write(self.conn, &ports.f_sql, cols, &cell_refs, |r| {
                    Ok($module::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
                });
                Some(match val {
                    Ok(val) => $module::$row { is_error: false, err: String::new(), val },
                    Err(e) => $module::$row { is_error: true, err: e.to_string(), ..Default::default() },
                })
            }
        }
        let out = $module::$comp(&H { conn: $conn }, $in).unwrap();
        let _ = $cols;
        arr(&out.iter().map(|r| user_row(r.id, &r.email, &r.name)).collect::<Vec<_>>())
    }};
}
fn native_createmany(conn: &Connection) -> String {
    user_batch!(conn, gen_createmany, run_native_raw_struct_CreateMany, HandlerNRCreateMany, PortsNRCreateManyN0, RawRowNRCreateManyN0,
        gen_createmany::InNRCreateMany { emails: batch_emails(), names: batch_names() }, (),
        |p: &gen_createmany::PortsNRCreateManyN0| -> (&[&str], Vec<Vec<String>>) {
            (&["email", "name"], vec![p.f_v0.iter().map(|s| json_str(s)).collect(), p.f_v1.iter().map(|s| json_str(s)).collect()])
        })
}
fn native_upsertmany(conn: &Connection) -> String {
    user_batch!(conn, gen_upsertmany, run_native_raw_struct_UpsertMany, HandlerNRUpsertMany, PortsNRUpsertManyN0, RawRowNRUpsertManyN0,
        gen_upsertmany::InNRUpsertMany { emails: upsertmany_emails(), names: batch_names() }, (),
        |p: &gen_upsertmany::PortsNRUpsertManyN0| -> (&[&str], Vec<Vec<String>>) {
            (&["email", "name"], vec![p.f_v0.iter().map(|s| json_str(s)).collect(), p.f_v1.iter().map(|s| json_str(s)).collect()])
        })
}
fn native_updatemany(conn: &Connection) -> String {
    struct H<'a> { conn: &'a Connection }
    impl gen_updatemany::HandlerNRUpdateMany for H<'_> {
        fn node_n0(&self, ports: &gen_updatemany::PortsNRUpdateManyN0, _b: Option<String>) -> Option<gen_updatemany::RawRowNRUpdateManyN0> {
            // updateMany: key column (id) encoded BARE (numeric) so json_extract = INTEGER matches; name quoted.
            let ids: Vec<String> = ports.f_v0.iter().map(|i| i.to_string()).collect();
            let names: Vec<String> = ports.f_v1.iter().map(|s| json_str(s)).collect();
            let cells: Vec<&[String]> = vec![ids.as_slice(), names.as_slice()];
            let val = query_batch_write(self.conn, &ports.f_sql, &["id", "name"], &cells, |r| {
                Ok(gen_updatemany::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? })
            });
            Some(match val {
                Ok(val) => gen_updatemany::RawRowNRUpdateManyN0 { is_error: false, err: String::new(), val },
                Err(e) => gen_updatemany::RawRowNRUpdateManyN0 { is_error: true, err: e.to_string(), ..Default::default() },
            })
        }
    }
    let out = gen_updatemany::run_native_raw_struct_UpdateMany(&H { conn }, gen_updatemany::InNRUpdateMany { ids: (1..=10).collect(), names: batch_names() }).unwrap();
    arr(&out.iter().map(|r| user_row(r.id, &r.email, &r.name)).collect::<Vec<_>>())
}

// ══ SDK BASELINE — raw rusqlite + hand-SQL for benchmark_* (the ONLY hand-written execution) ══════════
// Same SQL each op produces (self-consistent with native) — a raw prepare+exec, no codegen module.
fn sdk_users(conn: &Connection, sql: &str, params: &[Param]) -> String {
    let rows = query(conn, sql, params, |r| Ok(user_row(r.get::<_, i64>(0)?, &r.get::<_, String>(1)?, &r.get::<_, String>(2)?))).unwrap();
    arr(&rows)
}
fn sdk_findall(conn: &Connection) -> String {
    sdk_users(conn, "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", &[])
}
fn sdk_findfirst(conn: &Connection) -> String {
    sdk_users(conn, "SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1", &[Param::Text("User%".into())])
}
fn sdk_findunique(conn: &Connection) -> String {
    sdk_users(conn, "SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1", &[Param::Text("user500@example.com".into())])
}
fn sdk_filterpaginatesort(conn: &Connection) -> String {
    let rows = query(conn, "SELECT id, title, content, published, author_id, created_at FROM benchmark_posts WHERE published = ? ORDER BY created_at DESC LIMIT 20 OFFSET 10", &[Param::Int(1)], |r| {
        Ok(obj(&[ji("id", r.get(0)?), js("title", &r.get::<_, String>(1)?), js("content", &r.get::<_, Option<String>>(2)?.unwrap_or_default()), ji("published", r.get(3)?), ji("author_id", r.get(4)?), js("created_at", &r.get::<_, String>(5)?)]))
    }).unwrap();
    arr(&rows)
}
fn sdk_create(conn: &Connection) -> String {
    sdk_users(conn, "INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id, email, name", &[Param::Text("new@bench.com".into()), Param::Text("New".into())])
}
fn sdk_update(conn: &Connection) -> String {
    sdk_users(conn, "UPDATE benchmark_users SET name = ? WHERE id = ? RETURNING id, email, name", &[Param::Text("Updated 100".into()), Param::Int(100)])
}
fn sdk_upsert(conn: &Connection) -> String {
    sdk_users(conn, "INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id, email, name", &[Param::Text("user1@example.com".into()), Param::Text("Upserted One".into())])
}
/// Batch SDK: a hand-written multi-row VALUES … RETURNING (the natural raw-driver batch), same rows as json_each.
fn sdk_createmany(conn: &Connection) -> String {
    let (e, n) = (batch_emails(), batch_names());
    let ph = (0..e.len()).map(|_| "(?, ?)").collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO benchmark_users (email, name) VALUES {} RETURNING id, email, name", ph);
    let mut params: Vec<Param> = Vec::new();
    for i in 0..e.len() { params.push(Param::Text(e[i].clone())); params.push(Param::Text(n[i].clone())); }
    sdk_users(conn, &sql, &params)
}
fn sdk_upsertmany(conn: &Connection) -> String {
    let (e, n) = (upsertmany_emails(), batch_names());
    let ph = (0..e.len()).map(|_| "(?, ?)").collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO benchmark_users (email, name) VALUES {} ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id, email, name", ph);
    let mut params: Vec<Param> = Vec::new();
    for i in 0..e.len() { params.push(Param::Text(e[i].clone())); params.push(Param::Text(n[i].clone())); }
    sdk_users(conn, &sql, &params)
}
fn sdk_updatemany(conn: &Connection) -> String {
    // Hand-OPTIMIZED: ONE CASE update for ids 1..10, RETURNING the updated rows (not a per-row loop).
    let names = batch_names();
    let cases: String = (1..=10).map(|id| format!("WHEN {} THEN ?", id)).collect::<Vec<_>>().join(" ");
    let sql = format!("UPDATE benchmark_users SET name = CASE id {} END WHERE id IN (1,2,3,4,5,6,7,8,9,10) RETURNING id, email, name", cases);
    let params: Vec<Param> = names.iter().map(|n| Param::Text(n.clone())).collect();
    sdk_users(conn, &sql, &params)
}

// ── fixed inputs (match ops.ts / oracle.ts) ──
fn batch_emails() -> Vec<String> { (0..10).map(|i| format!("many{}@bench.com", i)).collect() }
fn batch_names() -> Vec<String> { (0..10).map(|i| format!("Many {}", i)).collect() }
fn upsertmany_emails() -> Vec<String> {
    let mut v = vec!["user1@example.com".to_string(), "user2@example.com".to_string()];
    v.extend((0..8).map(|i| format!("many{}@bench.com", i)));
    v
}

// ── more row serializers + composite result shapes ({rows,rel} / {committed,state}) ──
fn post_row(id: i64, title: &str, author_id: i64) -> String { obj(&[ji("id", id), js("title", title), ji("author_id", author_id)]) }
fn comment_row(id: i64, body: &str, post_id: i64) -> String { obj(&[ji("id", id), js("body", body), ji("post_id", post_id)]) }
fn tuser_row(t: i64, u: i64, name: &str) -> String { obj(&[ji("tenant_id", t), ji("user_id", u), js("name", name)]) }
fn tpost_row(t: i64, pid: i64, u: i64, title: &str) -> String { obj(&[ji("tenant_id", t), ji("post_id", pid), ji("user_id", u), js("title", title)]) }
fn rel_json(rel: &str, parents: &[String], child_lists: &[String]) -> String {
    format!("{{\"rows\":{},{}:{}}}", arr(parents), json_str(rel), arr(child_lists))
}
/// users+posts snapshot — the affected-tables state a write/tx op emits (matches oracle.ts stateSnapshot).
fn state_json(conn: &Connection) -> String {
    let users = query(conn, "SELECT id, email, name FROM benchmark_users ORDER BY id", &[], |r| Ok(user_row(r.get(0)?, &r.get::<_, String>(1)?, &r.get::<_, String>(2)?))).unwrap();
    let posts = query(conn, "SELECT id, title, author_id FROM benchmark_posts ORDER BY id", &[], |r| Ok(post_row(r.get(0)?, &r.get::<_, String>(1)?, r.get(2)?))).unwrap();
    format!("{{\"users\":{},\"posts\":{}}}", arr(&users), arr(&posts))
}
fn tx_json(committed: bool, conn: &Connection) -> String {
    format!("{{\"committed\":{},\"state\":{}}}", committed, state_json(conn))
}

// ══ NATIVE read+rel — parent Select + ONE batched relation query (2-level slice; 3-level is #119) ══════
fn native_nestedfindall(conn: &Connection) -> String {
    struct H<'a> { conn: &'a Connection }
    impl gen_nestedfindall::HandlerNRFindAll for H<'_> {
        fn node_n0(&self, p: &gen_nestedfindall::PortsNRFindAllN0, _b: Option<String>) -> Option<gen_nestedfindall::RawRowNRFindAllN0> {
            let val = query(self.conn, &p.f_sql, &[Param::Int(p.f_p0)], |r| Ok(gen_nestedfindall::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? }));
            Some(match val { Ok(val) => gen_nestedfindall::RawRowNRFindAllN0 { is_error: false, err: String::new(), val }, Err(e) => gen_nestedfindall::RawRowNRFindAllN0 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
        fn node_rel_posts(&self, ports: &gen_nestedfindall::PortsNRFindAllRelPostsBatch, _b: Option<String>) -> Option<gen_nestedfindall::RawRowNRFindAllRelPosts> {
            if ports.items.is_empty() { return Some(gen_nestedfindall::RawRowNRFindAllRelPosts { is_error: false, err: String::new(), rows: vec![] }); }
            let keys: Vec<i64> = ports.items.iter().map(|it| it.f_k0).collect();
            let res = query_batched_relation(self.conn, &ports.items[0].f_sql, &keys,
                |ks| format!("[{}]", ks.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(",")),
                |r| Ok(gen_nestedfindall::T1 { id: r.get(0)?, title: r.get(1)?, author_id: r.get(2)? }), |c| c.author_id);
            Some(match res { Ok(l) => gen_nestedfindall::RawRowNRFindAllRelPosts { is_error: false, err: String::new(), rows: l.into_iter().map(|val| gen_nestedfindall::RawElemNRFindAllRelPosts { is_error: false, err: String::new(), val }).collect() }, Err(e) => gen_nestedfindall::RawRowNRFindAllRelPosts { is_error: true, err: e.to_string(), rows: vec![] } })
        }
    }
    let out = gen_nestedfindall::run_native_raw_struct_FindAll(&H { conn }, gen_nestedfindall::InNRFindAll).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| user_row(u.id, &u.email, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}
fn native_nestedfindfirst(conn: &Connection) -> String {
    struct H<'a> { conn: &'a Connection }
    impl gen_nestedfindfirst::HandlerNRFindFirst for H<'_> {
        fn node_n0(&self, p: &gen_nestedfindfirst::PortsNRFindFirstN0, _b: Option<String>) -> Option<gen_nestedfindfirst::RawRowNRFindFirstN0> {
            let val = query(self.conn, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)], |r| Ok(gen_nestedfindfirst::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? }));
            Some(match val { Ok(val) => gen_nestedfindfirst::RawRowNRFindFirstN0 { is_error: false, err: String::new(), val }, Err(e) => gen_nestedfindfirst::RawRowNRFindFirstN0 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
        fn node_rel_posts(&self, ports: &gen_nestedfindfirst::PortsNRFindFirstRelPostsBatch, _b: Option<String>) -> Option<gen_nestedfindfirst::RawRowNRFindFirstRelPosts> {
            if ports.items.is_empty() { return Some(gen_nestedfindfirst::RawRowNRFindFirstRelPosts { is_error: false, err: String::new(), rows: vec![] }); }
            let keys: Vec<i64> = ports.items.iter().map(|it| it.f_k0).collect();
            let res = query_batched_relation(self.conn, &ports.items[0].f_sql, &keys,
                |ks| format!("[{}]", ks.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(",")),
                |r| Ok(gen_nestedfindfirst::T1 { id: r.get(0)?, title: r.get(1)?, author_id: r.get(2)? }), |c| c.author_id);
            Some(match res { Ok(l) => gen_nestedfindfirst::RawRowNRFindFirstRelPosts { is_error: false, err: String::new(), rows: l.into_iter().map(|val| gen_nestedfindfirst::RawElemNRFindFirstRelPosts { is_error: false, err: String::new(), val }).collect() }, Err(e) => gen_nestedfindfirst::RawRowNRFindFirstRelPosts { is_error: true, err: e.to_string(), rows: vec![] } })
        }
    }
    let out = gen_nestedfindfirst::run_native_raw_struct_FindFirst(&H { conn }, gen_nestedfindfirst::InNRFindFirst { name: "User%".into() }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| user_row(u.id, &u.email, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}
fn native_nestedfindunique(conn: &Connection) -> String {
    struct H<'a> { conn: &'a Connection }
    impl gen_nestedfindunique::HandlerNRFindUnique for H<'_> {
        fn node_n0(&self, p: &gen_nestedfindunique::PortsNRFindUniqueN0, _b: Option<String>) -> Option<gen_nestedfindunique::RawRowNRFindUniqueN0> {
            let val = query(self.conn, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)], |r| Ok(gen_nestedfindunique::T0 { id: r.get(0)?, email: r.get(1)?, name: r.get(2)? }));
            Some(match val { Ok(val) => gen_nestedfindunique::RawRowNRFindUniqueN0 { is_error: false, err: String::new(), val }, Err(e) => gen_nestedfindunique::RawRowNRFindUniqueN0 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
        fn node_rel_posts(&self, ports: &gen_nestedfindunique::PortsNRFindUniqueRelPostsBatch, _b: Option<String>) -> Option<gen_nestedfindunique::RawRowNRFindUniqueRelPosts> {
            if ports.items.is_empty() { return Some(gen_nestedfindunique::RawRowNRFindUniqueRelPosts { is_error: false, err: String::new(), rows: vec![] }); }
            let keys: Vec<i64> = ports.items.iter().map(|it| it.f_k0).collect();
            let res = query_batched_relation(self.conn, &ports.items[0].f_sql, &keys,
                |ks| format!("[{}]", ks.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(",")),
                |r| Ok(gen_nestedfindunique::T1 { id: r.get(0)?, title: r.get(1)?, author_id: r.get(2)? }), |c| c.author_id);
            Some(match res { Ok(l) => gen_nestedfindunique::RawRowNRFindUniqueRelPosts { is_error: false, err: String::new(), rows: l.into_iter().map(|val| gen_nestedfindunique::RawElemNRFindUniqueRelPosts { is_error: false, err: String::new(), val }).collect() }, Err(e) => gen_nestedfindunique::RawRowNRFindUniqueRelPosts { is_error: true, err: e.to_string(), rows: vec![] } })
        }
    }
    let out = gen_nestedfindunique::run_native_raw_struct_FindUnique(&H { conn }, gen_nestedfindunique::InNRFindUnique { email: "user1@example.com".into() }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| user_row(u.id, &u.email, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}
fn native_nestedrelations(conn: &Connection) -> String {
    struct H<'a> { conn: &'a Connection }
    impl gen_nestedrelations::HandlerNRByAuthor for H<'_> {
        fn node_n0(&self, p: &gen_nestedrelations::PortsNRByAuthorN0, _b: Option<String>) -> Option<gen_nestedrelations::RawRowNRByAuthorN0> {
            let val = query(self.conn, &p.f_sql, &[Param::Int(p.f_p0)], |r| Ok(gen_nestedrelations::T0 { id: r.get(0)?, title: r.get(1)?, author_id: r.get(2)? }));
            Some(match val { Ok(val) => gen_nestedrelations::RawRowNRByAuthorN0 { is_error: false, err: String::new(), val }, Err(e) => gen_nestedrelations::RawRowNRByAuthorN0 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
        fn node_rel_comments(&self, ports: &gen_nestedrelations::PortsNRByAuthorRelCommentsBatch, _b: Option<String>) -> Option<gen_nestedrelations::RawRowNRByAuthorRelComments> {
            if ports.items.is_empty() { return Some(gen_nestedrelations::RawRowNRByAuthorRelComments { is_error: false, err: String::new(), rows: vec![] }); }
            let keys: Vec<i64> = ports.items.iter().map(|it| it.f_k0).collect();
            let res = query_batched_relation(self.conn, &ports.items[0].f_sql, &keys,
                |ks| format!("[{}]", ks.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(",")),
                |r| Ok(gen_nestedrelations::T1 { id: r.get(0)?, body: r.get(1)?, post_id: r.get(2)? }), |c| c.post_id);
            Some(match res { Ok(l) => gen_nestedrelations::RawRowNRByAuthorRelComments { is_error: false, err: String::new(), rows: l.into_iter().map(|val| gen_nestedrelations::RawElemNRByAuthorRelComments { is_error: false, err: String::new(), val }).collect() }, Err(e) => gen_nestedrelations::RawRowNRByAuthorRelComments { is_error: true, err: e.to_string(), rows: vec![] } })
        }
    }
    let out = gen_nestedrelations::run_native_raw_struct_ByAuthor(&H { conn }, gen_nestedrelations::InNRByAuthor { author_id: 7 }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect();
    let children: Vec<String> = out.comments.iter().map(|cs| arr(&cs.iter().map(|c| comment_row(c.id, &c.body, c.post_id)).collect::<Vec<_>>())).collect();
    rel_json("comments", &parents, &children)
}
fn native_compositerelations(conn: &Connection) -> String {
    struct H<'a> { conn: &'a Connection }
    impl gen_compositerelations::HandlerNRByTenant for H<'_> {
        fn node_n0(&self, p: &gen_compositerelations::PortsNRByTenantN0, _b: Option<String>) -> Option<gen_compositerelations::RawRowNRByTenantN0> {
            let val = query(self.conn, &p.f_sql, &[Param::Int(p.f_p0)], |r| Ok(gen_compositerelations::T0 { tenant_id: r.get(0)?, user_id: r.get(1)?, name: r.get(2)? }));
            Some(match val { Ok(val) => gen_compositerelations::RawRowNRByTenantN0 { is_error: false, err: String::new(), val }, Err(e) => gen_compositerelations::RawRowNRByTenantN0 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
        fn node_rel_posts(&self, ports: &gen_compositerelations::PortsNRByTenantRelPostsBatch, _b: Option<String>) -> Option<gen_compositerelations::RawRowNRByTenantRelPosts> {
            if ports.items.is_empty() { return Some(gen_compositerelations::RawRowNRByTenantRelPosts { is_error: false, err: String::new(), rows: vec![] }); }
            let keys: Vec<(i64, i64)> = ports.items.iter().map(|it| (it.f_k0, it.f_k1)).collect();
            let res = query_batched_relation(self.conn, &ports.items[0].f_sql, &keys,
                |ks| format!("[{}]", ks.iter().map(|(t, u)| format!("[{},{}]", t, u)).collect::<Vec<_>>().join(",")),
                |r| Ok(gen_compositerelations::T1 { tenant_id: r.get(0)?, post_id: r.get(1)?, user_id: r.get(2)?, title: r.get(3)? }), |c| (c.tenant_id, c.user_id));
            Some(match res { Ok(l) => gen_compositerelations::RawRowNRByTenantRelPosts { is_error: false, err: String::new(), rows: l.into_iter().map(|val| gen_compositerelations::RawElemNRByTenantRelPosts { is_error: false, err: String::new(), val }).collect() }, Err(e) => gen_compositerelations::RawRowNRByTenantRelPosts { is_error: true, err: e.to_string(), rows: vec![] } })
        }
    }
    let out = gen_compositerelations::run_native_raw_struct_ByTenant(&H { conn }, gen_compositerelations::InNRByTenant { tenant_id: 1 }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| tuser_row(u.tenant_id, u.user_id, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| tpost_row(p.tenant_id, p.post_id, p.user_id, &p.title)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}

// ══ NATIVE tx — the transaction envelope + the chain runner; result = {committed, state} ══════════════
fn native_delete(conn: &Connection) -> String {
    struct H<'a> { conn: &'a Connection }
    impl gen_delete::HandlerNRDelete for H<'_> {
        fn node_tx_body_0(&self, p: &gen_delete::PortsNRDeleteTxBody0, _b: Option<String>) -> Option<gen_delete::RawRowNRDeleteTxBody0> {
            let rows = query(self.conn, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())], |r| r.get::<_, i64>(0));
            Some(match rows { Ok(v) if !v.is_empty() => gen_delete::RawRowNRDeleteTxBody0 { is_error: false, err: String::new(), id: v[0] }, Ok(_) => gen_delete::RawRowNRDeleteTxBody0 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_delete::RawRowNRDeleteTxBody0 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
        fn node_tx_body_1(&self, p: &gen_delete::PortsNRDeleteTxBody1, _b: Option<String>) -> Option<gen_delete::RawRowNRDeleteTxBody1> {
            Some(match execute(self.conn, &p.f_sql, &[Param::Int(p.f_p0)]) { Ok(s) => gen_delete::RawRowNRDeleteTxBody1 { is_error: false, err: String::new(), changes: s.changes, lastInsertRowid: s.last_insert_rowid }, Err(e) => gen_delete::RawRowNRDeleteTxBody1 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
    }
    let r = transaction(conn, |c| gen_delete::run_native_raw_struct_Delete(&H { conn: c }, gen_delete::InNRDelete { email: "del0@bench.com".into(), name: "Del".into() }));
    tx_json(r.is_ok(), conn)
}
fn native_nestedcreate(conn: &Connection) -> String {
    struct H<'a> { conn: &'a Connection }
    impl gen_nestedcreate::HandlerNRNestedCreate for H<'_> {
        fn node_tx_body_0(&self, p: &gen_nestedcreate::PortsNRNestedCreateTxBody0, _b: Option<String>) -> Option<gen_nestedcreate::RawRowNRNestedCreateTxBody0> {
            let rows = query(self.conn, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())], |r| r.get::<_, i64>(0));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedcreate::RawRowNRNestedCreateTxBody0 { is_error: false, err: String::new(), id: v[0] }, Ok(_) => gen_nestedcreate::RawRowNRNestedCreateTxBody0 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedcreate::RawRowNRNestedCreateTxBody0 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
        fn node_tx_body_1(&self, p: &gen_nestedcreate::PortsNRNestedCreateTxBody1, _b: Option<String>) -> Option<gen_nestedcreate::RawRowNRNestedCreateTxBody1> {
            let rows = query(self.conn, &p.f_sql, &[Param::Int(p.f_p0), Param::Text(p.f_p1.clone())], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?)));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedcreate::RawRowNRNestedCreateTxBody1 { is_error: false, err: String::new(), id: v[0].0, author_id: v[0].1, title: v[0].2.clone() }, Ok(_) => gen_nestedcreate::RawRowNRNestedCreateTxBody1 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedcreate::RawRowNRNestedCreateTxBody1 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
    }
    let r = transaction(conn, |c| gen_nestedcreate::run_native_raw_struct_NestedCreate(&H { conn: c }, gen_nestedcreate::InNRNestedCreate { email: "nc@bench.com".into(), name: "NC".into(), title: "NC Post".into() }));
    tx_json(r.is_ok(), conn)
}
fn native_nestedupdate(conn: &Connection) -> String {
    struct H<'a> { conn: &'a Connection }
    impl gen_nestedupdate::HandlerNRNestedUpdate for H<'_> {
        fn node_tx_body_0(&self, p: &gen_nestedupdate::PortsNRNestedUpdateTxBody0, _b: Option<String>) -> Option<gen_nestedupdate::RawRowNRNestedUpdateTxBody0> {
            let rows = query(self.conn, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedupdate::RawRowNRNestedUpdateTxBody0 { is_error: false, err: String::new(), id: v[0].0, name: v[0].1.clone() }, Ok(_) => gen_nestedupdate::RawRowNRNestedUpdateTxBody0 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedupdate::RawRowNRNestedUpdateTxBody0 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
        fn node_tx_body_1(&self, p: &gen_nestedupdate::PortsNRNestedUpdateTxBody1, _b: Option<String>) -> Option<gen_nestedupdate::RawRowNRNestedUpdateTxBody1> {
            let rows = query(self.conn, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedupdate::RawRowNRNestedUpdateTxBody1 { is_error: false, err: String::new(), id: v[0].0, title: v[0].1.clone() }, Ok(_) => gen_nestedupdate::RawRowNRNestedUpdateTxBody1 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedupdate::RawRowNRNestedUpdateTxBody1 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
    }
    let r = transaction(conn, |c| gen_nestedupdate::run_native_raw_struct_NestedUpdate(&H { conn: c }, gen_nestedupdate::InNRNestedUpdate { name: "NU".into(), user_id: 7, title: "NU Post".into() }));
    tx_json(r.is_ok(), conn)
}
fn native_nestedupsert(conn: &Connection) -> String {
    struct H<'a> { conn: &'a Connection }
    impl gen_nestedupsert::HandlerNRNestedUpsert for H<'_> {
        fn node_tx_body_0(&self, p: &gen_nestedupsert::PortsNRNestedUpsertTxBody0, _b: Option<String>) -> Option<gen_nestedupsert::RawRowNRNestedUpsertTxBody0> {
            let rows = query(self.conn, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())], |r| r.get::<_, i64>(0));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedupsert::RawRowNRNestedUpsertTxBody0 { is_error: false, err: String::new(), id: v[0] }, Ok(_) => gen_nestedupsert::RawRowNRNestedUpsertTxBody0 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedupsert::RawRowNRNestedUpsertTxBody0 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
        fn node_tx_body_1(&self, p: &gen_nestedupsert::PortsNRNestedUpsertTxBody1, _b: Option<String>) -> Option<gen_nestedupsert::RawRowNRNestedUpsertTxBody1> {
            let rows = query(self.conn, &p.f_sql, &[Param::Int(p.f_p0), Param::Text(p.f_p1.clone())], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?)));
            Some(match rows { Ok(v) if !v.is_empty() => gen_nestedupsert::RawRowNRNestedUpsertTxBody1 { is_error: false, err: String::new(), id: v[0].0, author_id: v[0].1, title: v[0].2.clone() }, Ok(_) => gen_nestedupsert::RawRowNRNestedUpsertTxBody1 { is_error: true, err: "no row".into(), ..Default::default() }, Err(e) => gen_nestedupsert::RawRowNRNestedUpsertTxBody1 { is_error: true, err: e.to_string(), ..Default::default() } })
        }
    }
    let r = transaction(conn, |c| gen_nestedupsert::run_native_raw_struct_NestedUpsert(&H { conn: c }, gen_nestedupsert::InNRNestedUpsert { email: "user1@example.com".into(), name: "NUp".into(), title: "NUp Post".into() }));
    tx_json(r.is_ok(), conn)
}

// ══ SDK read+rel — raw driver: parent query + ONE batched IN child query + client-side stitch ═════════
fn sdk_rel_single(conn: &Connection, parent_sql: &str, parent_params: &[Param], parent_ser: impl Fn(&rusqlite::Row) -> (i64, String), child_sql_fmt: &str, child_ser: impl Fn(&rusqlite::Row) -> (i64, String), rel: &str) -> String {
    let parents = query(conn, parent_sql, parent_params, |r| Ok(parent_ser(r))).unwrap();
    let keys: Vec<i64> = parents.iter().map(|(k, _)| *k).collect();
    let inlist = keys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = child_sql_fmt.replace("{IN}", &inlist);
    let params: Vec<Param> = keys.iter().map(|&k| Param::Int(k)).collect();
    let children = query(conn, &sql, &params, |r| Ok(child_ser(r))).unwrap();
    let mut groups: std::collections::HashMap<i64, Vec<String>> = std::collections::HashMap::new();
    for (k, j) in children { groups.entry(k).or_default().push(j); }
    let ps: Vec<String> = parents.iter().map(|(_, j)| j.clone()).collect();
    let cs: Vec<String> = parents.iter().map(|(k, _)| arr(groups.get(k).map(|v| v.as_slice()).unwrap_or(&[]))).collect();
    rel_json(rel, &ps, &cs)
}
fn sdk_nestedfindall(conn: &Connection) -> String {
    sdk_rel_single(conn, "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", &[],
        |r| (r.get(0).unwrap(), user_row(r.get(0).unwrap(), &r.get::<_, String>(1).unwrap(), &r.get::<_, String>(2).unwrap())),
        "SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN ({IN}) ORDER BY id ASC",
        |r| (r.get(2).unwrap(), post_row(r.get(0).unwrap(), &r.get::<_, String>(1).unwrap(), r.get(2).unwrap())), "posts")
}
fn sdk_nestedfindfirst(conn: &Connection) -> String {
    sdk_rel_single(conn, "SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1", &[Param::Text("User%".into())],
        |r| (r.get(0).unwrap(), user_row(r.get(0).unwrap(), &r.get::<_, String>(1).unwrap(), &r.get::<_, String>(2).unwrap())),
        "SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN ({IN}) ORDER BY id ASC",
        |r| (r.get(2).unwrap(), post_row(r.get(0).unwrap(), &r.get::<_, String>(1).unwrap(), r.get(2).unwrap())), "posts")
}
fn sdk_nestedfindunique(conn: &Connection) -> String {
    sdk_rel_single(conn, "SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1", &[Param::Text("user1@example.com".into())],
        |r| (r.get(0).unwrap(), user_row(r.get(0).unwrap(), &r.get::<_, String>(1).unwrap(), &r.get::<_, String>(2).unwrap())),
        "SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN ({IN}) ORDER BY id ASC",
        |r| (r.get(2).unwrap(), post_row(r.get(0).unwrap(), &r.get::<_, String>(1).unwrap(), r.get(2).unwrap())), "posts")
}
fn sdk_nestedrelations(conn: &Connection) -> String {
    sdk_rel_single(conn, "SELECT id, title, author_id FROM benchmark_posts WHERE author_id = ? ORDER BY id ASC", &[Param::Int(7)],
        |r| (r.get(0).unwrap(), post_row(r.get(0).unwrap(), &r.get::<_, String>(1).unwrap(), r.get(2).unwrap())),
        "SELECT id, body, post_id FROM benchmark_comments WHERE post_id IN ({IN}) ORDER BY id ASC",
        |r| (r.get(2).unwrap(), comment_row(r.get(0).unwrap(), &r.get::<_, String>(1).unwrap(), r.get(2).unwrap())), "comments")
}
fn sdk_compositerelations(conn: &Connection) -> String {
    // composite (tenant_id, user_id): parents = tenant 1's users; children = that tenant's posts grouped by user_id.
    let parents = query(conn, "SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = ? ORDER BY user_id ASC", &[Param::Int(1)],
        |r| Ok((r.get::<_, i64>(1)?, tuser_row(r.get(0)?, r.get(1)?, &r.get::<_, String>(2)?)))).unwrap();
    let children = query(conn, "SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE tenant_id = ? ORDER BY post_id ASC", &[Param::Int(1)],
        |r| Ok((r.get::<_, i64>(2)?, tpost_row(r.get(0)?, r.get(1)?, r.get(2)?, &r.get::<_, String>(3)?)))).unwrap();
    let mut groups: std::collections::HashMap<i64, Vec<String>> = std::collections::HashMap::new();
    for (u, j) in children { groups.entry(u).or_default().push(j); }
    let ps: Vec<String> = parents.iter().map(|(_, j)| j.clone()).collect();
    let cs: Vec<String> = parents.iter().map(|(u, _)| arr(groups.get(u).map(|v| v.as_slice()).unwrap_or(&[]))).collect();
    rel_json("posts", &ps, &cs)
}

// ══ SDK tx — raw driver BEGIN … COMMIT/ROLLBACK, then the {committed, state} snapshot ═════════════════
fn sdk_tx(conn: &Connection, body: impl FnOnce(&Connection) -> rusqlite::Result<()>) -> bool {
    if conn.execute_batch("BEGIN").is_err() { return false; }
    match body(conn) { Ok(()) => conn.execute_batch("COMMIT").is_ok(), Err(_) => { let _ = conn.execute_batch("ROLLBACK"); false } }
}
fn sdk_delete(conn: &Connection) -> String {
    let ok = sdk_tx(conn, |c| {
        let id: i64 = c.query_row("INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id", rusqlite::params!["del0@bench.com", "Del"], |r| r.get(0))?;
        c.execute("DELETE FROM benchmark_users WHERE id = ?", rusqlite::params![id])?;
        Ok(())
    });
    tx_json(ok, conn)
}
fn sdk_nestedcreate(conn: &Connection) -> String {
    let ok = sdk_tx(conn, |c| {
        let id: i64 = c.query_row("INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id", rusqlite::params!["nc@bench.com", "NC"], |r| r.get(0))?;
        c.execute("INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)", rusqlite::params![id, "NC Post"])?;
        Ok(())
    });
    tx_json(ok, conn)
}
fn sdk_nestedupdate(conn: &Connection) -> String {
    let ok = sdk_tx(conn, |c| {
        c.execute("UPDATE benchmark_users SET name = ? WHERE id = ?", rusqlite::params!["NU", 7])?;
        c.execute("UPDATE benchmark_posts SET title = ? WHERE author_id = ?", rusqlite::params!["NU Post", 7])?;
        Ok(())
    });
    tx_json(ok, conn)
}
fn sdk_nestedupsert(conn: &Connection) -> String {
    let ok = sdk_tx(conn, |c| {
        let id: i64 = c.query_row("INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id", rusqlite::params!["user1@example.com", "NUp"], |r| r.get(0))?;
        c.execute("INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)", rusqlite::params![id, "NUp Post"])?;
        Ok(())
    });
    tx_json(ok, conn)
}

const FLAT_OPS: &[&str] = &["findAll", "filterPaginateSort", "findFirst", "findUnique", "create", "update", "upsert", "createMany", "upsertMany", "updateMany"];
const REL_OPS: &[&str] = &["nestedFindAll", "nestedFindFirst", "nestedFindUnique", "nestedRelations", "compositeRelations"];
const TX_OPS: &[&str] = &["delete", "nestedCreate", "nestedUpdate", "nestedUpsert"];
const READ_OPS: &[&str] = &["findAll", "filterPaginateSort", "findFirst", "findUnique", "nestedFindAll", "nestedFindFirst", "nestedFindUnique", "nestedRelations", "compositeRelations"];

fn run_native(op: &str, conn: &Connection) -> String {
    match op {
        "findAll" => native_findall(conn),
        "filterPaginateSort" => native_filterpaginatesort(conn),
        "findFirst" => native_findfirst(conn),
        "findUnique" => native_findunique(conn),
        "create" => native_create(conn),
        "update" => native_update(conn),
        "upsert" => native_upsert(conn),
        "createMany" => native_createmany(conn),
        "upsertMany" => native_upsertmany(conn),
        "updateMany" => native_updatemany(conn),
        "nestedFindAll" => native_nestedfindall(conn),
        "nestedFindFirst" => native_nestedfindfirst(conn),
        "nestedFindUnique" => native_nestedfindunique(conn),
        "nestedRelations" => native_nestedrelations(conn),
        "compositeRelations" => native_compositerelations(conn),
        "delete" => native_delete(conn),
        "nestedCreate" => native_nestedcreate(conn),
        "nestedUpdate" => native_nestedupdate(conn),
        "nestedUpsert" => native_nestedupsert(conn),
        _ => panic!("native: unknown op '{op}'"),
    }
}
fn run_sdk(op: &str, conn: &Connection) -> String {
    match op {
        "findAll" => sdk_findall(conn),
        "filterPaginateSort" => sdk_filterpaginatesort(conn),
        "findFirst" => sdk_findfirst(conn),
        "findUnique" => sdk_findunique(conn),
        "create" => sdk_create(conn),
        "update" => sdk_update(conn),
        "upsert" => sdk_upsert(conn),
        "createMany" => sdk_createmany(conn),
        "upsertMany" => sdk_upsertmany(conn),
        "updateMany" => sdk_updatemany(conn),
        "nestedFindAll" => sdk_nestedfindall(conn),
        "nestedFindFirst" => sdk_nestedfindfirst(conn),
        "nestedFindUnique" => sdk_nestedfindunique(conn),
        "nestedRelations" => sdk_nestedrelations(conn),
        "compositeRelations" => sdk_compositerelations(conn),
        "delete" => sdk_delete(conn),
        "nestedCreate" => sdk_nestedcreate(conn),
        "nestedUpdate" => sdk_nestedupdate(conn),
        "nestedUpsert" => sdk_nestedupsert(conn),
        _ => panic!("sdk: unknown op '{op}'"),
    }
}

fn run_cell(cell: &str, op: &str, conn: &Connection) -> String {
    if cell == "native" { run_native(op, conn) } else { run_sdk(op, conn) }
}
fn now_us() -> u128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("");
    match mode {
        // run <op> <db> <native|sdk> — print the canonical result string (the node driver byte-compares).
        "run" => {
            let op = args.get(2).expect("run <op>");
            let db = args.get(3).expect("run <op> <db>");
            let cell = args.get(4).expect("run <op> <db> <native|sdk>");
            // Reads use the db read-only; writes/batch mutate a fresh copy the driver provides.
            let conn = Connection::open(db).expect("open db");
            let out = if cell == "native" { run_native(op, &conn) } else { run_sdk(op, &conn) };
            println!("{out}");
        }
        // bench <seed_db> <warmup> <iters> <out_csv> — latency CSV (native + sdk) for ALL 19 ops.
        // Read/read-rel ops (non-mutating) time on the seed directly. Write/batch/tx ops (mutating) RESET
        // per iteration (copy seed + open, UNTIMED) so fixed inputs don't collide on UNIQUE — only the op
        // is timed. Mutating ops use fewer iters (the reset dominates wall time).
        "bench" => {
            use std::io::Write;
            let seed = args.get(2).expect("seed_db");
            let warmup: usize = args.get(3).expect("warmup").parse().unwrap();
            let iters: usize = args.get(4).expect("iters").parse().unwrap();
            let mut csv = std::fs::File::create(args.get(5).expect("out_csv")).unwrap();
            writeln!(csv, "op,cell,us").unwrap();
            let all: Vec<&str> = READ_OPS.iter().chain(FLAT_OPS.iter().filter(|o| !READ_OPS.contains(o))).chain(TX_OPS.iter()).copied().collect();
            let _ = (now_us, REL_OPS);
            for &op in &all {
                let mutating = !READ_OPS.contains(&op);
                let n = if mutating { iters.min(500) } else { iters };
                for cell in ["native", "sdk"] {
                    if !mutating {
                        let conn = Connection::open(seed).expect("open");
                        for _ in 0..warmup { std::hint::black_box(run_cell(cell, op, &conn)); }
                        for _ in 0..n {
                            let t0 = std::time::Instant::now();
                            let r = run_cell(cell, op, &conn);
                            std::hint::black_box(&r);
                            writeln!(csv, "{},{},{:.3}", op, cell, t0.elapsed().as_nanos() as f64 / 1000.0).unwrap();
                        }
                    } else {
                        let tmp = format!("{seed}.{op}.{cell}.work");
                        for i in 0..(warmup.min(50) + n) {
                            std::fs::copy(seed, &tmp).unwrap(); // UNTIMED reset — fresh state each iter
                            let conn = Connection::open(&tmp).expect("open");
                            let t0 = std::time::Instant::now();
                            let r = run_cell(cell, op, &conn);
                            let us = t0.elapsed().as_nanos() as f64 / 1000.0;
                            std::hint::black_box(&r);
                            drop(conn);
                            if i >= warmup.min(50) { writeln!(csv, "{},{},{:.3}", op, cell, us).unwrap(); }
                        }
                        let _ = std::fs::remove_file(&tmp);
                    }
                }
            }
            eprintln!("rust bench done: {} ops × (native, sdk)", all.len());
        }
        _ => {
            eprintln!("usage: orm_bench_rust run <op> <db> <native|sdk> | bench <read_db> <write_db> <w> <n> <csv>");
            std::process::exit(2);
        }
    }
}
