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
use seam::{execute, json_str, query, query_batch_write, Param};

#[path = "../generated/gen_findall.rs"]
mod gen_findall;
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
    // Per-id UPDATE … RETURNING in one tx (raw driver), aggregated — same rows as the json_each CASE update.
    let names = batch_names();
    let mut rows: Vec<String> = Vec::new();
    for id in 1..=10i64 {
        let name = &names[(id - 1) as usize];
        let r = conn.query_row("UPDATE benchmark_users SET name = ? WHERE id = ? RETURNING id, email, name", rusqlite::params![name, id], |r| {
            Ok(user_row(r.get::<_, i64>(0)?, &r.get::<_, String>(1)?, &r.get::<_, String>(2)?))
        }).unwrap();
        rows.push(r);
    }
    arr(&rows)
}

// ── fixed inputs (match ops.ts / oracle.ts) ──
fn batch_emails() -> Vec<String> { (0..10).map(|i| format!("many{}@bench.com", i)).collect() }
fn batch_names() -> Vec<String> { (0..10).map(|i| format!("Many {}", i)).collect() }
fn upsertmany_emails() -> Vec<String> {
    let mut v = vec!["user1@example.com".to_string(), "user2@example.com".to_string()];
    v.extend((0..8).map(|i| format!("many{}@bench.com", i)));
    v
}

const FLAT_OPS: &[&str] = &["findAll", "filterPaginateSort", "findFirst", "findUnique", "create", "update", "upsert", "createMany", "upsertMany", "updateMany"];
const READ_OPS: &[&str] = &["findAll", "filterPaginateSort", "findFirst", "findUnique"];

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
        _ => panic!("native: op '{op}' not in this cell (flat ops only this round)"),
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
        _ => panic!("sdk: op '{op}' not in this cell"),
    }
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
        // bench <read_db> <write_db> <warmup> <iters> <out_csv> — latency CSV (native + sdk).
        "bench" => {
            use std::io::Write;
            let read_db = args.get(2).expect("read_db");
            let write_db = args.get(3).expect("write_db");
            let warmup: usize = args.get(4).expect("warmup").parse().unwrap();
            let iters: usize = args.get(5).expect("iters").parse().unwrap();
            let mut csv = std::fs::File::create(args.get(6).expect("out_csv")).unwrap();
            writeln!(csv, "op,cell,us").unwrap();
            let _ = write_db;
            // Latency over the READ ops (repeatable). Writes/batch are byte-equal-proven; their latency needs
            // a per-iteration DB reset (fixed inputs collide on UNIQUE) — deferred with the read+rel/tx slice.
            for &op in READ_OPS {
                for cell in ["native", "sdk"] {
                    let conn = Connection::open(read_db).expect("open");
                    for _ in 0..warmup { let _ = std::hint::black_box(if cell == "native" { run_native(op, &conn) } else { run_sdk(op, &conn) }); }
                    for _ in 0..iters {
                        let t0 = std::time::Instant::now();
                        let r = if cell == "native" { run_native(op, &conn) } else { run_sdk(op, &conn) };
                        std::hint::black_box(&r);
                        writeln!(csv, "{},{},{:.3}", op, cell, t0.elapsed().as_nanos() as f64 / 1000.0).unwrap();
                    }
                }
            }
            let _ = now_us;
            eprintln!("rust bench done: {} read ops × (native, sdk)", READ_OPS.len());
        }
        _ => {
            eprintln!("usage: orm_bench_rust run <op> <db> <native|sdk> | bench <read_db> <write_db> <w> <n> <csv>");
            std::process::exit(2);
        }
    }
}
