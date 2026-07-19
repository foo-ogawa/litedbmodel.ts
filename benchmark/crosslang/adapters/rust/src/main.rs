//! Main-bench RUST cell — the CLI-generated typed-native modules (generated/<dialect>/, NEVER hand-
//! written) + the generic exec seam (seam.rs — a driver abstraction over rusqlite/postgres/mysql that
//! MATERIALIZES rows into a by-name WIRE the 0.8.9 modules de-box inline) + thin per-op leaf handlers.
//! Also the SDK BASELINE (raw driver + hand-SQL) — the only hand-written execution; it reads the wire
//! rows directly (not via the generated de-box). Both produce a CANONICAL result string a node driver
//! byte-compares to the mode-2 (sqlite) oracle. ONE dialect per binary (cargo feature).
//!
//! Modes:
//!   orm_bench_rust run <op> <target> <native|sdk>   → print the canonical result for one op × cell
//!   orm_bench_rust bench <seed_db> <w> <n> <csv>    → latency CSV (sqlite only)
#![allow(non_snake_case)]

#[path = "seam.rs"]
mod seam;
use seam::{execute, execute_batch, json_str, query, query_batched_relation, transaction, Db, Param, WireListData, WireResult, WireRowData};

// The generated module tree for the built dialect (`use gen::*` brings the 19 op modules into scope).
#[cfg(feature = "postgres")]
#[path = "../generated/postgres/mod.rs"]
mod gen;
#[cfg(feature = "mysql")]
#[path = "../generated/mysql/mod.rs"]
mod gen;
#[cfg(all(not(feature = "postgres"), not(feature = "mysql")))]
#[path = "../generated/sqlite/mod.rs"]
mod gen;
use gen::*;

// ══ WIRE bridge — implement each generated module's (module-local) WireValue/WireRow/WireList traits for
// the seam's materialized types. The seam speaks NEUTRAL Outcome/NumOutcome; these macros map them to the
// module's own Probe/NumProbe enums (the wire seam is generated PER module, so one impl set per module —
// ONE macro, invoked once per module; no hand-duplicated decode). The generated INLINE de-box owns
// strictness; the seam only classifies each cell's variant.
macro_rules! conv_probe {
    ($m:ident, $e:expr) => {
        match $e {
            seam::Outcome::Got(v) => $m::Probe::Got(v),
            seam::Outcome::Wrong { wire, raw } => $m::Probe::Wrong { actual_wire_type: wire, raw_value: raw },
            seam::Outcome::Null { wire, raw } => $m::Probe::Null { actual_wire_type: wire, raw_value: raw },
            seam::Outcome::Absent => $m::Probe::Absent,
        }
    };
}
macro_rules! conv_num {
    ($m:ident, $e:expr) => {
        match $e {
            seam::NumOutcome::Got { raw, wire } => $m::NumProbe::Got { raw, actual_wire_type: wire },
            seam::NumOutcome::Wrong { wire, raw } => $m::NumProbe::Wrong { actual_wire_type: wire, raw_value: raw },
            seam::NumOutcome::Null { wire, raw } => $m::NumProbe::Null { actual_wire_type: wire, raw_value: raw },
            seam::NumOutcome::Absent => $m::NumProbe::Absent,
        }
    };
}
macro_rules! impl_wire {
    ($m:ident) => {
        impl $m::WireValue for WireResult {
            type Row = WireRowData;
            fn as_string(&self) -> $m::Probe<String> { conv_probe!($m, self.v_str()) }
            fn as_number(&self) -> $m::NumProbe { conv_num!($m, self.v_num()) }
            fn as_bool(&self) -> $m::Probe<bool> { conv_probe!($m, self.v_bool()) }
            fn as_row(&self) -> $m::Probe<WireRowData> { conv_probe!($m, self.v_row()) }
            fn as_list(&self) -> $m::Probe<WireListData> { conv_probe!($m, self.v_list()) }
        }
        impl $m::WireRow for WireRowData {
            type List = WireListData;
            fn keys(&self) -> Vec<String> { self.keys_vec() }
            fn probe_string(&self, f: &str) -> $m::Probe<String> { conv_probe!($m, self.probe_str(f)) }
            fn probe_number(&self, f: &str) -> $m::NumProbe { conv_num!($m, self.probe_num(f)) }
            fn probe_bool(&self, f: &str) -> $m::Probe<bool> { conv_probe!($m, self.probe_boolean(f)) }
            fn probe_row(&self, f: &str) -> $m::Probe<WireRowData> { conv_probe!($m, self.probe_rowv(f)) }
            fn probe_list(&self, f: &str) -> $m::Probe<WireListData> { conv_probe!($m, self.probe_listv(f)) }
        }
        impl $m::WireList for WireListData {
            type Row = WireRowData;
            fn len(&self) -> usize { self.length() }
            fn elem_string(&self, i: usize) -> $m::Probe<String> { conv_probe!($m, self.elem_str(i)) }
            fn elem_number(&self, i: usize) -> $m::NumProbe { conv_num!($m, self.elem_num(i)) }
            fn elem_bool(&self, i: usize) -> $m::Probe<bool> { conv_probe!($m, self.elem_boolean(i)) }
            fn elem_row(&self, i: usize) -> $m::Probe<WireRowData> { conv_probe!($m, self.elem_rowv(i)) }
            fn elem_list(&self, i: usize) -> $m::Probe<WireListData> { conv_probe!($m, self.elem_listv(i)) }
        }
    };
}
impl_wire!(gen_findall);
impl_wire!(gen_filterpaginatesort);
impl_wire!(gen_findfirst);
impl_wire!(gen_findunique);
impl_wire!(gen_create);
impl_wire!(gen_update);
impl_wire!(gen_upsert);
impl_wire!(gen_createmany);
impl_wire!(gen_upsertmany);
impl_wire!(gen_updatemany);
impl_wire!(gen_nestedfindall);
impl_wire!(gen_nestedfindfirst);
impl_wire!(gen_nestedfindunique);
impl_wire!(gen_nestedrelations);
impl_wire!(gen_compositerelations);
impl_wire!(gen_delete);
impl_wire!(gen_nestedcreate);
impl_wire!(gen_nestedupdate);
impl_wire!(gen_nestedupsert);

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
/// The n-th positional placeholder for the built dialect (`$n` pg / `?` sqlite+mysql) — the SDK hand-SQL.
#[cfg(feature = "postgres")]
fn ph(n: usize) -> String { format!("${}", n) }
#[cfg(not(feature = "postgres"))]
fn ph(_n: usize) -> String { "?".to_string() }
/// The `filterPaginateSort` `published` INPUT literal — pg types the input head BOOLEAN, sqlite/mysql int.
#[cfg(feature = "postgres")]
fn fps_published_input() -> bool { true }
#[cfg(not(feature = "postgres"))]
fn fps_published_input() -> i64 { 1 }

// ══ NATIVE cells — the CLI-generated module (inline de-box) + the seam. The handler returns its node's
// result WIRE (`Self::Wire = WireResult`); the runner de-boxes it into the concrete row struct. ═══════════
/// A single-node READ / RETURNING-write handler: run the baked SQL, return the materialized wire.
macro_rules! run_query_cell {
    ($db:expr, $m:ident, $comp:ident, $handler:ident, $ports:ident, $in:expr, $bind:expr) => {{
        struct H<'a> { db: &'a Db }
        impl $m::$handler for H<'_> {
            type Wire = WireResult;
            fn node_n0(&self, ports: &$m::$ports, _b: Option<String>) -> Result<WireResult, $m::BehaviorError> {
                query(self.db, &ports.f_sql, &$bind(ports)).map_err(|e| $m::BehaviorError::new("LEAF_FAILURE", e))
            }
        }
        $m::$comp(&H { db: $db }, $in).unwrap()
    }};
}
/// A NO-RETURNING single write (v1 default): run it (mutating), return the summary wire; the cell emits
/// the v1-faithful `null` (the summary is discarded).
macro_rules! run_null_cell {
    ($db:expr, $m:ident, $comp:ident, $handler:ident, $ports:ident, $in:expr, $bind:expr) => {{
        struct H<'a> { db: &'a Db }
        impl $m::$handler for H<'_> {
            type Wire = WireResult;
            fn node_n0(&self, ports: &$m::$ports, _b: Option<String>) -> Result<WireResult, $m::BehaviorError> {
                execute(self.db, &ports.f_sql, &$bind(ports)).map(WireResult::summary).map_err(|e| $m::BehaviorError::new("LEAF_FAILURE", e))
            }
        }
        $m::$comp(&H { db: $db }, $in).unwrap();
        "null".to_string()
    }};
}
/// A NO-RETURNING batch write (v1: createMany/upsertMany/updateMany return null): run the ONE batch
/// statement (mutating), emit `null`. The seam zips the parallel column arrays into the single param.
macro_rules! run_null_batch_cell {
    ($db:expr, $m:ident, $comp:ident, $handler:ident, $ports:ident, $in:expr, $cols:expr, $arrays:expr) => {{
        struct H<'a> { db: &'a Db }
        impl $m::$handler for H<'_> {
            type Wire = WireResult;
            fn node_n0(&self, ports: &$m::$ports, _b: Option<String>) -> Result<WireResult, $m::BehaviorError> {
                let a: Vec<Param> = $arrays(ports);
                let refs: Vec<&Param> = a.iter().collect();
                execute_batch(self.db, &ports.f_sql, $cols, &refs).map(WireResult::summary).map_err(|e| $m::BehaviorError::new("LEAF_FAILURE", e))
            }
        }
        $m::$comp(&H { db: $db }, $in).unwrap();
        "null".to_string()
    }};
}

fn native_findall(db: &Db) -> String {
    let out = run_query_cell!(db, gen_findall, run_native_raw_struct_FindAll, HandlerNRFindAll, PortsNRFindAllN0,
        gen_findall::InNRFindAll, |p: &gen_findall::PortsNRFindAllN0| vec![Param::Int(p.f_p0)]);
    arr(&out.iter().map(|r| user_row(r.id, &r.email, &r.name)).collect::<Vec<_>>())
}
fn native_findfirst(db: &Db) -> String {
    let out = run_query_cell!(db, gen_findfirst, run_native_raw_struct_FindFirst, HandlerNRFindFirst, PortsNRFindFirstN0,
        gen_findfirst::InNRFindFirst { name: "User%".into() }, |p: &gen_findfirst::PortsNRFindFirstN0| vec![Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)]);
    arr(&out.iter().map(|r| user_row(r.id, &r.email, &r.name)).collect::<Vec<_>>())
}
fn native_findunique(db: &Db) -> String {
    let out = run_query_cell!(db, gen_findunique, run_native_raw_struct_FindUnique, HandlerNRFindUnique, PortsNRFindUniqueN0,
        gen_findunique::InNRFindUnique { email: "user500@example.com".into() }, |p: &gen_findunique::PortsNRFindUniqueN0| vec![Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)]);
    arr(&out.iter().map(|r| user_row(r.id, &r.email, &r.name)).collect::<Vec<_>>())
}
fn native_create(db: &Db) -> String {
    run_null_cell!(db, gen_create, run_native_raw_struct_Create, HandlerNRCreate, PortsNRCreateN0,
        gen_create::InNRCreate { email: "new@bench.com".into(), name: "New".into() }, |p: &gen_create::PortsNRCreateN0| vec![Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())])
}
fn native_update(db: &Db) -> String {
    run_null_cell!(db, gen_update, run_native_raw_struct_Update, HandlerNRUpdate, PortsNRUpdateN0,
        gen_update::InNRUpdate { name: "Updated 100".into(), id: 100 }, |p: &gen_update::PortsNRUpdateN0| vec![Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)])
}
// Upsert RETURNS the PK only (v1 `{returning:true}`). On mysql `ports.f_sql` carries the re-select marker;
// the seam strips+re-selects transparently, so this handler is dialect-agnostic.
fn native_upsert(db: &Db) -> String {
    let out = run_query_cell!(db, gen_upsert, run_native_raw_struct_Upsert, HandlerNRUpsert, PortsNRUpsertN0,
        gen_upsert::InNRUpsert { email: "user1@example.com".into(), name: "Upserted One".into() }, |p: &gen_upsert::PortsNRUpsertN0| vec![Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())]);
    arr(&out.iter().map(|r| obj(&[ji("id", r.id)])).collect::<Vec<_>>())
}
fn native_filterpaginatesort(db: &Db) -> String {
    // `f_p0` is the `published` filter (pg BOOLEAN / sqlite+mysql int); `as i64` normalizes both, and the
    // pg `Param::Int`→BOOL bind (seam) restores true/false. The OUTPUT `published` de-boxes as the column's
    // native type (pg `bool` / sqlite+mysql `i64`); `as i64` renders it as the canonical 0/1 the oracle uses.
    let out = run_query_cell!(db, gen_filterpaginatesort, run_native_raw_struct_FilterPaginateSort, HandlerNRFilterPaginateSort, PortsNRFilterPaginateSortN0,
        gen_filterpaginatesort::InNRFilterPaginateSort { published: fps_published_input() },
        |p: &gen_filterpaginatesort::PortsNRFilterPaginateSortN0| vec![Param::Int(p.f_p0 as i64), Param::Int(p.f_p1), Param::Int(p.f_p2)]);
    arr(&out.iter().map(|r| obj(&[ji("id", r.id), js("title", &r.title), js("content", &r.content), ji("published", r.published as i64), ji("author_id", r.author_id), js("created_at", &r.created_at)])).collect::<Vec<_>>())
}
fn native_createmany(db: &Db) -> String {
    run_null_batch_cell!(db, gen_createmany, run_native_raw_struct_CreateMany, HandlerNRCreateMany, PortsNRCreateManyN0,
        gen_createmany::InNRCreateMany { emails: batch_emails(), names: batch_names() }, &["email", "name"],
        |p: &gen_createmany::PortsNRCreateManyN0| vec![Param::ArrayText(p.f_v0.clone()), Param::ArrayText(p.f_v1.clone())])
}
fn native_upsertmany(db: &Db) -> String {
    run_null_batch_cell!(db, gen_upsertmany, run_native_raw_struct_UpsertMany, HandlerNRUpsertMany, PortsNRUpsertManyN0,
        gen_upsertmany::InNRUpsertMany { emails: upsertmany_emails(), names: batch_names() }, &["email", "name"],
        |p: &gen_upsertmany::PortsNRUpsertManyN0| vec![Param::ArrayText(p.f_v0.clone()), Param::ArrayText(p.f_v1.clone())])
}
fn native_updatemany(db: &Db) -> String {
    run_null_batch_cell!(db, gen_updatemany, run_native_raw_struct_UpdateMany, HandlerNRUpdateMany, PortsNRUpdateManyN0,
        gen_updatemany::InNRUpdateMany { ids: (1..=10).collect(), names: batch_names() }, &["id", "name"],
        |p: &gen_updatemany::PortsNRUpdateManyN0| vec![Param::ArrayInt(p.f_v0.clone()), Param::ArrayText(p.f_v1.clone())])
}

// ── users+posts snapshot — the affected-tables state a write/tx op emits (matches oracle.ts) ──
fn state_json(db: &Db) -> String {
    let users = query(db, "SELECT id, email, name FROM benchmark_users ORDER BY id", &[]).unwrap();
    let posts = query(db, "SELECT id, title, author_id FROM benchmark_posts ORDER BY id", &[]).unwrap();
    let u = arr(&users.rows.iter().map(|r| user_row(r.i64("id"), &r.text("email"), &r.text("name"))).collect::<Vec<_>>());
    let p = arr(&posts.rows.iter().map(|r| post_row(r.i64("id"), &r.text("title"), r.i64("author_id"))).collect::<Vec<_>>());
    format!("{{\"users\":{},\"posts\":{}}}", u, p)
}
fn tx_json(committed: bool, db: &Db) -> String { format!("{{\"committed\":{},\"state\":{}}}", committed, state_json(db)) }

// ══ NATIVE read+rel — parent Select + ONE batched relation query (2-level slice). The relation handler
// returns `Vec<Self::Wire>` (one wire LIST per parent element); the seam dedups keys, runs ONE child
// query, groups by target key, aligns per-parent. ══════════════════════════════════════════════════════
macro_rules! rel_handler_single {
    ($m:ident, $relfn:ident, $relports:ident, $keycol:expr) => {
        fn $relfn(&self, ports: &$m::$relports, _b: Option<String>) -> Result<Vec<WireResult>, $m::BehaviorError> {
            let keys: Vec<i64> = ports.items.iter().map(|it| it.f_k0).collect();
            if keys.is_empty() { return Ok(vec![]); }
            query_batched_relation(self.db, &ports.items[0].f_sql, &keys,
                |ks: &[i64]| format!("[{}]", ks.iter().map(|k| k.to_string()).collect::<Vec<_>>().join(",")),
                |ks: &[i64]| vec![Param::ArrayInt(ks.to_vec())],
                |row: &WireRowData| row.i64($keycol))
                .map_err(|e| $m::BehaviorError::new("LEAF_FAILURE", e))
        }
    };
}
fn native_nestedfindall(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedfindall::HandlerNRFindAll for H<'_> {
        type Wire = WireResult;
        fn node_n0(&self, p: &gen_nestedfindall::PortsNRFindAllN0, _b: Option<String>) -> Result<WireResult, gen_nestedfindall::BehaviorError> {
            query(self.db, &p.f_sql, &[Param::Int(p.f_p0)]).map_err(|e| gen_nestedfindall::BehaviorError::new("LEAF_FAILURE", e))
        }
        rel_handler_single!(gen_nestedfindall, node_rel_posts, PortsNRFindAllRelPostsBatch, "author_id");
    }
    let out = gen_nestedfindall::run_native_raw_struct_FindAll(&H { db }, gen_nestedfindall::InNRFindAll).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| user_row(u.id, &u.email, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}
fn native_nestedfindfirst(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedfindfirst::HandlerNRFindFirst for H<'_> {
        type Wire = WireResult;
        fn node_n0(&self, p: &gen_nestedfindfirst::PortsNRFindFirstN0, _b: Option<String>) -> Result<WireResult, gen_nestedfindfirst::BehaviorError> {
            query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)]).map_err(|e| gen_nestedfindfirst::BehaviorError::new("LEAF_FAILURE", e))
        }
        rel_handler_single!(gen_nestedfindfirst, node_rel_posts, PortsNRFindFirstRelPostsBatch, "author_id");
    }
    let out = gen_nestedfindfirst::run_native_raw_struct_FindFirst(&H { db }, gen_nestedfindfirst::InNRFindFirst { name: "User%".into() }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| user_row(u.id, &u.email, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}
fn native_nestedfindunique(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedfindunique::HandlerNRFindUnique for H<'_> {
        type Wire = WireResult;
        fn node_n0(&self, p: &gen_nestedfindunique::PortsNRFindUniqueN0, _b: Option<String>) -> Result<WireResult, gen_nestedfindunique::BehaviorError> {
            query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)]).map_err(|e| gen_nestedfindunique::BehaviorError::new("LEAF_FAILURE", e))
        }
        rel_handler_single!(gen_nestedfindunique, node_rel_posts, PortsNRFindUniqueRelPostsBatch, "author_id");
    }
    let out = gen_nestedfindunique::run_native_raw_struct_FindUnique(&H { db }, gen_nestedfindunique::InNRFindUnique { email: "user1@example.com".into() }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| user_row(u.id, &u.email, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}
fn native_nestedrelations(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedrelations::HandlerNRByAuthor for H<'_> {
        type Wire = WireResult;
        fn node_n0(&self, p: &gen_nestedrelations::PortsNRByAuthorN0, _b: Option<String>) -> Result<WireResult, gen_nestedrelations::BehaviorError> {
            query(self.db, &p.f_sql, &[Param::Int(p.f_p0)]).map_err(|e| gen_nestedrelations::BehaviorError::new("LEAF_FAILURE", e))
        }
        rel_handler_single!(gen_nestedrelations, node_rel_comments, PortsNRByAuthorRelCommentsBatch, "post_id");
    }
    let out = gen_nestedrelations::run_native_raw_struct_ByAuthor(&H { db }, gen_nestedrelations::InNRByAuthor { author_id: 7 }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|p| post_row(p.id, &p.title, p.author_id)).collect();
    let children: Vec<String> = out.comments.iter().map(|cs| arr(&cs.iter().map(|c| comment_row(c.id, &c.body, c.post_id)).collect::<Vec<_>>())).collect();
    rel_json("comments", &parents, &children)
}
fn native_compositerelations(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_compositerelations::HandlerNRByTenant for H<'_> {
        type Wire = WireResult;
        fn node_n0(&self, p: &gen_compositerelations::PortsNRByTenantN0, _b: Option<String>) -> Result<WireResult, gen_compositerelations::BehaviorError> {
            query(self.db, &p.f_sql, &[Param::Int(p.f_p0)]).map_err(|e| gen_compositerelations::BehaviorError::new("LEAF_FAILURE", e))
        }
        fn node_rel_posts(&self, ports: &gen_compositerelations::PortsNRByTenantRelPostsBatch, _b: Option<String>) -> Result<Vec<WireResult>, gen_compositerelations::BehaviorError> {
            let keys: Vec<(i64, i64)> = ports.items.iter().map(|it| (it.f_k0, it.f_k1)).collect();
            if keys.is_empty() { return Ok(vec![]); }
            query_batched_relation(self.db, &ports.items[0].f_sql, &keys,
                |ks: &[(i64, i64)]| format!("[{}]", ks.iter().map(|(t, u)| format!("[{},{}]", t, u)).collect::<Vec<_>>().join(",")),
                |ks: &[(i64, i64)]| vec![Param::ArrayInt(ks.iter().map(|(t, _)| *t).collect()), Param::ArrayInt(ks.iter().map(|(_, u)| *u).collect())],
                |row: &WireRowData| (row.i64("tenant_id"), row.i64("user_id")))
                .map_err(|e| gen_compositerelations::BehaviorError::new("LEAF_FAILURE", e))
        }
    }
    let out = gen_compositerelations::run_native_raw_struct_ByTenant(&H { db }, gen_compositerelations::InNRByTenant { tenant_id: 1 }).unwrap();
    let parents: Vec<String> = out.rows.iter().map(|u| tuser_row(u.tenant_id, u.user_id, &u.name)).collect();
    let children: Vec<String> = out.posts.iter().map(|ps| arr(&ps.iter().map(|p| tpost_row(p.tenant_id, p.post_id, p.user_id, &p.title)).collect::<Vec<_>>())).collect();
    rel_json("posts", &parents, &children)
}

// ══ NATIVE tx — the transaction envelope + the chain runner; result = {committed, state}. A RETURNING-id
// body runs `query` (marker-aware on mysql) → the {id} row; a NO-RETURNING body runs `execute` → the
// {changes,lastInsertRowid} summary row. ══════════════════════════════════════════════════════════════
fn native_delete(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_delete::HandlerNRDelete for H<'_> {
        type Wire = WireResult;
        fn node_tx_body_0(&self, p: &gen_delete::PortsNRDeleteTxBody0, _b: Option<String>) -> Result<WireResult, gen_delete::BehaviorError> {
            query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())]).map_err(|e| gen_delete::BehaviorError::new("LEAF_FAILURE", e))
        }
        fn node_tx_body_1(&self, p: &gen_delete::PortsNRDeleteTxBody1, _b: Option<String>) -> Result<WireResult, gen_delete::BehaviorError> {
            execute(self.db, &p.f_sql, &[Param::Int(p.f_p0)]).map(WireResult::summary).map_err(|e| gen_delete::BehaviorError::new("LEAF_FAILURE", e))
        }
    }
    let r = transaction(db, |c| gen_delete::run_native_raw_struct_Delete(&H { db: c }, gen_delete::InNRDelete { email: "del0@bench.com".into(), name: "Del".into() }));
    tx_json(r.is_ok(), db)
}
fn native_nestedcreate(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedcreate::HandlerNRNestedCreate for H<'_> {
        type Wire = WireResult;
        fn node_tx_body_0(&self, p: &gen_nestedcreate::PortsNRNestedCreateTxBody0, _b: Option<String>) -> Result<WireResult, gen_nestedcreate::BehaviorError> {
            query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())]).map_err(|e| gen_nestedcreate::BehaviorError::new("LEAF_FAILURE", e))
        }
        fn node_tx_body_1(&self, p: &gen_nestedcreate::PortsNRNestedCreateTxBody1, _b: Option<String>) -> Result<WireResult, gen_nestedcreate::BehaviorError> {
            execute(self.db, &p.f_sql, &[Param::Int(p.f_p0), Param::Text(p.f_p1.clone())]).map(WireResult::summary).map_err(|e| gen_nestedcreate::BehaviorError::new("LEAF_FAILURE", e))
        }
    }
    let r = transaction(db, |c| gen_nestedcreate::run_native_raw_struct_NestedCreate(&H { db: c }, gen_nestedcreate::InNRNestedCreate { email: "nc@bench.com".into(), name: "NC".into(), title: "NC Post".into() }));
    tx_json(r.is_ok(), db)
}
fn native_nestedupdate(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedupdate::HandlerNRNestedUpdate for H<'_> {
        type Wire = WireResult;
        fn node_tx_body_0(&self, p: &gen_nestedupdate::PortsNRNestedUpdateTxBody0, _b: Option<String>) -> Result<WireResult, gen_nestedupdate::BehaviorError> {
            execute(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)]).map(WireResult::summary).map_err(|e| gen_nestedupdate::BehaviorError::new("LEAF_FAILURE", e))
        }
        fn node_tx_body_1(&self, p: &gen_nestedupdate::PortsNRNestedUpdateTxBody1, _b: Option<String>) -> Result<WireResult, gen_nestedupdate::BehaviorError> {
            execute(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Int(p.f_p1)]).map(WireResult::summary).map_err(|e| gen_nestedupdate::BehaviorError::new("LEAF_FAILURE", e))
        }
    }
    let r = transaction(db, |c| gen_nestedupdate::run_native_raw_struct_NestedUpdate(&H { db: c }, gen_nestedupdate::InNRNestedUpdate { name: "NU".into(), user_id: 7, title: "NU Post".into() }));
    tx_json(r.is_ok(), db)
}
fn native_nestedupsert(db: &Db) -> String {
    struct H<'a> { db: &'a Db }
    impl gen_nestedupsert::HandlerNRNestedUpsert for H<'_> {
        type Wire = WireResult;
        fn node_tx_body_0(&self, p: &gen_nestedupsert::PortsNRNestedUpsertTxBody0, _b: Option<String>) -> Result<WireResult, gen_nestedupsert::BehaviorError> {
            query(self.db, &p.f_sql, &[Param::Text(p.f_p0.clone()), Param::Text(p.f_p1.clone())]).map_err(|e| gen_nestedupsert::BehaviorError::new("LEAF_FAILURE", e))
        }
        fn node_tx_body_1(&self, p: &gen_nestedupsert::PortsNRNestedUpsertTxBody1, _b: Option<String>) -> Result<WireResult, gen_nestedupsert::BehaviorError> {
            execute(self.db, &p.f_sql, &[Param::Int(p.f_p0), Param::Text(p.f_p1.clone())]).map(WireResult::summary).map_err(|e| gen_nestedupsert::BehaviorError::new("LEAF_FAILURE", e))
        }
    }
    let r = transaction(db, |c| gen_nestedupsert::run_native_raw_struct_NestedUpsert(&H { db: c }, gen_nestedupsert::InNRNestedUpsert { email: "user1@example.com".into(), name: "NUp".into(), title: "NUp Post".into() }));
    tx_json(r.is_ok(), db)
}

// ══ SDK BASELINE — raw driver + hand-SQL. Reads the materialized wire rows DIRECTLY (by column name),
// NOT via the generated de-box (that is the native path). ═══════════════════════════════════════════════
fn sdk_users(db: &Db, sql: &str, params: &[Param]) -> String {
    let res = query(db, sql, params).unwrap();
    arr(&res.rows.iter().map(|r| user_row(r.i64("id"), &r.text("email"), &r.text("name"))).collect::<Vec<_>>())
}
fn sdk_findall(db: &Db) -> String { sdk_users(db, "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", &[]) }
fn sdk_findfirst(db: &Db) -> String { sdk_users(db, &format!("SELECT id, email, name FROM benchmark_users WHERE name LIKE {} LIMIT 1", ph(1)), &[Param::Text("User%".into())]) }
fn sdk_findunique(db: &Db) -> String { sdk_users(db, &format!("SELECT id, email, name FROM benchmark_users WHERE email = {} LIMIT 1", ph(1)), &[Param::Text("user500@example.com".into())]) }
fn sdk_filterpaginatesort(db: &Db) -> String {
    let sql = format!("SELECT id, title, content, published, author_id, created_at FROM benchmark_posts WHERE published = {} ORDER BY created_at DESC LIMIT 20 OFFSET 10", ph(1));
    let res = query(db, &sql, &[Param::Int(1)]).unwrap();
    arr(&res.rows.iter().map(|r| obj(&[ji("id", r.i64("id")), js("title", &r.text("title")), js("content", &r.text("content")), ji("published", r.i64("published")), ji("author_id", r.i64("author_id")), js("created_at", &r.text("created_at"))])).collect::<Vec<_>>())
}
/// A NO-RETURNING SDK write (v1 default): run it through the seam and emit v1-faithful `null`.
fn sdk_null(db: &Db, sql: &str, params: &[Param]) -> String {
    execute(db, sql, params).unwrap();
    "null".to_string()
}
fn sdk_create(db: &Db) -> String { sdk_null(db, &format!("INSERT INTO benchmark_users (email, name) VALUES ({}, {})", ph(1), ph(2)), &[Param::Text("new@bench.com".into()), Param::Text("New".into())]) }
fn sdk_update(db: &Db) -> String { sdk_null(db, &format!("UPDATE benchmark_users SET name = {} WHERE id = {}", ph(1), ph(2)), &[Param::Text("Updated 100".into()), Param::Int(100)]) }
/// The upsert baseline RETURNS the PK only (v1 `{returning:true}`). Per-dialect conflict tail (pg/sqlite
/// `ON CONFLICT … RETURNING id`; mysql `ON DUPLICATE KEY …` + the SAME re-select marker the native path
/// uses, so the SDK goes through the IDENTICAL seam emulation — never a raw v1 id-range).
fn sdk_upsert(db: &Db) -> String {
    let res = query(db, &sdk_upsert_sql(), &[Param::Text("user1@example.com".into()), Param::Text("Upserted One".into())]).unwrap();
    arr(&res.rows.iter().map(|r| obj(&[ji("id", r.i64("id"))])).collect::<Vec<_>>())
}
#[cfg(feature = "mysql")]
fn sdk_upsert_sql() -> String {
    "INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name) /*scp-reselect: SELECT id FROM benchmark_users WHERE email = ? ORDER BY id ::binds:: p0*/".to_string()
}
#[cfg(not(feature = "mysql"))]
fn sdk_upsert_sql() -> String {
    let excl = upsert_excluded();
    format!("INSERT INTO benchmark_users (email, name) VALUES ({}, {}) ON CONFLICT (email) DO UPDATE SET email = {}.email, name = {}.name RETURNING id", ph(1), ph(2), excl, excl)
}
/// The upsert `EXCLUDED`/`excluded` keyword (pg upper, sqlite lower). Unused on mysql (VALUES() form).
#[cfg(feature = "postgres")]
#[allow(dead_code)]
fn upsert_excluded() -> &'static str { "EXCLUDED" }
#[cfg(not(feature = "postgres"))]
#[allow(dead_code)]
fn upsert_excluded() -> &'static str { "excluded" }
/// The per-dialect upsert conflict tail for the batch (upsertMany) baseline (NO returning).
#[cfg(feature = "mysql")]
fn upsert_many_tail() -> String { " ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name)".to_string() }
#[cfg(not(feature = "mysql"))]
fn upsert_many_tail() -> String { let e = upsert_excluded(); format!(" ON CONFLICT (email) DO UPDATE SET email = {}.email, name = {}.name", e, e) }
fn sdk_createmany(db: &Db) -> String {
    let (e, n) = (batch_emails(), batch_names());
    let vals = (0..e.len()).map(|i| format!("({}, {})", ph(2 * i + 1), ph(2 * i + 2))).collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO benchmark_users (email, name) VALUES {}", vals);
    let mut params: Vec<Param> = Vec::new();
    for i in 0..e.len() { params.push(Param::Text(e[i].clone())); params.push(Param::Text(n[i].clone())); }
    sdk_null(db, &sql, &params)
}
fn sdk_upsertmany(db: &Db) -> String {
    let (e, n) = (upsertmany_emails(), batch_names());
    let vals = (0..e.len()).map(|i| format!("({}, {})", ph(2 * i + 1), ph(2 * i + 2))).collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO benchmark_users (email, name) VALUES {}{}", vals, upsert_many_tail());
    let mut params: Vec<Param> = Vec::new();
    for i in 0..e.len() { params.push(Param::Text(e[i].clone())); params.push(Param::Text(n[i].clone())); }
    sdk_null(db, &sql, &params)
}
fn sdk_updatemany(db: &Db) -> String {
    // Hand-OPTIMIZED: ONE CASE update for ids 1..10 (not a per-row loop); NO returning (v1) → null.
    let names = batch_names();
    let cases: String = (1..=10).map(|id| format!("WHEN {} THEN {}", id, ph(id))).collect::<Vec<_>>().join(" ");
    let sql = format!("UPDATE benchmark_users SET name = CASE id {} END WHERE id IN (1,2,3,4,5,6,7,8,9,10)", cases);
    let params: Vec<Param> = names.iter().map(|n| Param::Text(n.clone())).collect();
    sdk_null(db, &sql, &params)
}

// SDK read+rel — raw driver: parent query + ONE batched child query (IN-list / `= ANY`) + client stitch.
fn sdk_rel_single(db: &Db, parent_sql: &str, parent_params: &[Param], parent_key: &str, parent_ser: impl Fn(&WireRowData) -> String, child_sql_in: &str, child_key: &str, child_ser: impl Fn(&WireRowData) -> String, rel: &str) -> String {
    let parents = query(db, parent_sql, parent_params).unwrap().rows;
    let keys: Vec<i64> = parents.iter().map(|r| r.i64(parent_key)).collect();
    let (child_sql, child_params) = child_in_clause(child_sql_in, &keys);
    let children = query(db, &child_sql, &child_params).unwrap().rows;
    let mut groups: std::collections::HashMap<i64, Vec<String>> = std::collections::HashMap::new();
    for c in &children { groups.entry(c.i64(child_key)).or_default().push(child_ser(c)); }
    let ps: Vec<String> = parents.iter().map(|r| parent_ser(r)).collect();
    let cs: Vec<String> = parents.iter().map(|r| arr(groups.get(&r.i64(parent_key)).map(|v| v.as_slice()).unwrap_or(&[]))).collect();
    rel_json(rel, &ps, &cs)
}
/// The raw-driver batched child IN-clause: pg binds ONE `= ANY($1::int[])` array; sqlite/mysql bind an
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
    sdk_rel_single(db, "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100", &[], "id",
        |r| user_row(r.i64("id"), &r.text("email"), &r.text("name")),
        "SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC", "author_id",
        |r| post_row(r.i64("id"), &r.text("title"), r.i64("author_id")), "posts")
}
fn sdk_nestedfindfirst(db: &Db) -> String {
    sdk_rel_single(db, &format!("SELECT id, email, name FROM benchmark_users WHERE name LIKE {} LIMIT 1", ph(1)), &[Param::Text("User%".into())], "id",
        |r| user_row(r.i64("id"), &r.text("email"), &r.text("name")),
        "SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC", "author_id",
        |r| post_row(r.i64("id"), &r.text("title"), r.i64("author_id")), "posts")
}
fn sdk_nestedfindunique(db: &Db) -> String {
    sdk_rel_single(db, &format!("SELECT id, email, name FROM benchmark_users WHERE email = {} LIMIT 1", ph(1)), &[Param::Text("user1@example.com".into())], "id",
        |r| user_row(r.i64("id"), &r.text("email"), &r.text("name")),
        "SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC", "author_id",
        |r| post_row(r.i64("id"), &r.text("title"), r.i64("author_id")), "posts")
}
fn sdk_nestedrelations(db: &Db) -> String {
    sdk_rel_single(db, &format!("SELECT id, title, author_id FROM benchmark_posts WHERE author_id = {} ORDER BY id ASC", ph(1)), &[Param::Int(7)], "id",
        |r| post_row(r.i64("id"), &r.text("title"), r.i64("author_id")),
        "SELECT id, body, post_id FROM benchmark_comments WHERE post_id {IN} ORDER BY id ASC", "post_id",
        |r| comment_row(r.i64("id"), &r.text("body"), r.i64("post_id")), "comments")
}
fn sdk_compositerelations(db: &Db) -> String {
    let parents = query(db, &format!("SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = {} ORDER BY user_id ASC", ph(1)), &[Param::Int(1)]).unwrap().rows;
    let children = query(db, &format!("SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE tenant_id = {} ORDER BY post_id ASC", ph(1)), &[Param::Int(1)]).unwrap().rows;
    let mut groups: std::collections::HashMap<i64, Vec<String>> = std::collections::HashMap::new();
    for c in &children { groups.entry(c.i64("user_id")).or_default().push(tpost_row(c.i64("tenant_id"), c.i64("post_id"), c.i64("user_id"), &c.text("title"))); }
    let ps: Vec<String> = parents.iter().map(|r| tuser_row(r.i64("tenant_id"), r.i64("user_id"), &r.text("name"))).collect();
    let cs: Vec<String> = parents.iter().map(|r| arr(groups.get(&r.i64("user_id")).map(|v| v.as_slice()).unwrap_or(&[]))).collect();
    rel_json("posts", &ps, &cs)
}

// SDK tx — raw driver BEGIN … COMMIT/ROLLBACK via the seam envelope, then the {committed, state} snapshot.
fn sdk_tx(db: &Db, body: impl FnOnce(&Db) -> Result<(), String>) -> bool {
    transaction(db, body).is_ok()
}
/// The SDK's "insert a user, recover its id" statement — pg/sqlite native `RETURNING id`; mysql bakes the
/// SAME re-select marker the native path uses (LAST_INSERT_ID range) → the IDENTICAL seam emulation.
#[cfg(feature = "mysql")]
fn sdk_insert_user_id_sql() -> String {
    "INSERT INTO benchmark_users (email, name) VALUES (?, ?) /*scp-reselect: SELECT id FROM benchmark_users WHERE id >= ? AND id < ? ORDER BY id ::binds:: L,H*/".to_string()
}
#[cfg(not(feature = "mysql"))]
fn sdk_insert_user_id_sql() -> String {
    format!("INSERT INTO benchmark_users (email, name) VALUES ({}, {}) RETURNING id", ph(1), ph(2))
}
fn sdk_recover_id(db: &Db, sql: &str, params: &[Param]) -> Result<i64, String> {
    let res = query(db, sql, params)?;
    res.rows.first().map(|r| r.i64("id")).ok_or_else(|| "no id".to_string())
}
fn sdk_delete(db: &Db) -> String {
    let ok = sdk_tx(db, |c| {
        let id = sdk_recover_id(c, &sdk_insert_user_id_sql(), &[Param::Text("del0@bench.com".into()), Param::Text("Del".into())])?;
        execute(c, &format!("DELETE FROM benchmark_users WHERE id = {}", ph(1)), &[Param::Int(id)])?;
        Ok(())
    });
    tx_json(ok, db)
}
fn sdk_nestedcreate(db: &Db) -> String {
    let ok = sdk_tx(db, |c| {
        let id = sdk_recover_id(c, &sdk_insert_user_id_sql(), &[Param::Text("nc@bench.com".into()), Param::Text("NC".into())])?;
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
    let ok = sdk_tx(db, |c| {
        // Reuse the SAME per-dialect upsert-returning-id SQL as sdk_upsert (mysql → ON DUPLICATE + marker).
        let id = sdk_recover_id(c, &sdk_upsert_sql(), &[Param::Text("user1@example.com".into()), Param::Text("NUp".into())])?;
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
        // run <op> <target> <native|sdk> — <target> is a sqlite path / pg conn string / mysql URL.
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
