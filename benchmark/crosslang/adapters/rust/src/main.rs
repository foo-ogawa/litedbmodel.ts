//! litedbmodel cross-language adapter RUNNER — Rust (epic #44).
//!
//! Speaks the line-delimited JSON contract over stdin/stdout for TWO Rust cells: sql / ir.
//!
//!   sql     — hand-optimized raw SQL via the in-proc `SqliteDriver` (baseline 1.0×; sqlite-shaped
//!             by construction — the `sql` baseline runs on sqlite only, same convention as every
//!             other language adapter)
//!   ir      — the bundle loaded FROM the generated JSON on disk, executed via the shared runtime.
//!             DB-backed on sqlite (in-proc `SqliteDriver`) AND real dockerized Postgres/MySQL
//!             (#53) via the runtime's live `PostgresDriver`/`MysqlDriver` (the SAME `livedb`
//!             tokio-postgres+deadpool / sqlx seam `livedb_runner`/conformance use — no new
//!             driver code, just wiring this bench cell to it).
//!
//! The CODEGEN cell rides the DEDICATED `lm_codegen` binary (adapters/rust-codegen) — owner order:
//! the codegen path carries NO IR data and links NO serde_json, so it cannot live in this binary
//! (whose ir surface is legitimately JSON). Requesting impl=codegen here PANICS (fail-closed). The
//! generated read module is wired to the in-proc sqlite driver only, so `codegen` DB-backed runs
//! on sqlite only (matching every other language's codegen cell) — an explicit per-cell SKIP.
//!
//! Consumes generated/bundles.json (the language-neutral §8 artifact) unchanged. Its compiled
//! release binary size is the Rust artifact-size metric.

use behavior_contracts::Value;
use litedbmodel_runtime::livedb::{MysqlDriver, PostgresDriver};
use litedbmodel_runtime::{
    decode_scope, execute_bundle, execute_transaction_bundle, read_bundle_pooled, Driver,
    PreparedStatement, RunInfo, SqliteDriver,
};
use serde_json::Value as J;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::time::Instant;

// ── real-DB schema (mirror of domain.ts PG_SCHEMA / MYSQL_SCHEMA; isolated `scp_rust_bench`
// namespace so this bench never collides with conformance's `scp_rust` tables) ──────────────
const PG_SCHEMA_NAME: &str = "scp_rust_bench";
const MYSQL_DB_NAME: &str = "scp_rust_bench";

fn pg_schema_statements() -> Vec<String> {
    vec![
        "DROP TABLE IF EXISTS comments CASCADE".into(),
        "DROP TABLE IF EXISTS posts CASCADE".into(),
        "DROP TABLE IF EXISTS users CASCADE".into(),
        "DROP TABLE IF EXISTS uniq CASCADE".into(),
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, post_count INTEGER NOT NULL DEFAULT 0)".into(),
        "CREATE TABLE posts (id SERIAL PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, views INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL)".into(),
        "CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL)".into(),
        // s0 binds author_id (always numeric) — INTEGER (#53: pgx-class strict binary protocols
        // reject an int arg for a text column; this bench's Rust PgParam already type-coerces, but
        // INTEGER is the honest column type for the data it actually stores).
        "CREATE TABLE uniq (name TEXT NOT NULL, s0 INTEGER, f0 TEXT)".into(),
    ]
}
fn pg_seq_reset() -> Vec<String> {
    vec!["SELECT setval('posts_id_seq', (SELECT MAX(id) FROM posts))".into()]
}
fn mysql_schema_statements() -> Vec<String> {
    vec![
        "SET FOREIGN_KEY_CHECKS = 0".into(),
        "DROP TABLE IF EXISTS comments".into(),
        "DROP TABLE IF EXISTS posts".into(),
        "DROP TABLE IF EXISTS users".into(),
        "DROP TABLE IF EXISTS uniq".into(),
        "SET FOREIGN_KEY_CHECKS = 1".into(),
        "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, post_count INT NOT NULL DEFAULT 0)".into(),
        "CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, status VARCHAR(255), views INT NOT NULL DEFAULT 0, created_at VARCHAR(255) NOT NULL)".into(),
        "CREATE TABLE comments (id INT PRIMARY KEY, post_id INT NOT NULL, body VARCHAR(255) NOT NULL, created_at VARCHAR(255) NOT NULL)".into(),
        "CREATE TABLE uniq (name VARCHAR(255) NOT NULL, s0 INT, f0 VARCHAR(255))".into(),
    ]
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Connect + (re)seed a live Postgres in the isolated `scp_rust_bench` schema. Panics (fail-closed,
/// no silent skip) if PG is unreachable — the caller only calls this once a `run`/`throughput`
/// request for dialect=postgres has already been dispatched (docker is a prerequisite of that path).
fn connect_pg(art: &Artifact) -> PostgresDriver {
    let host = env_or("TEST_DB_HOST", "localhost");
    let port = env_or("TEST_DB_PORT", "5433");
    let user = env_or("TEST_DB_USER", "testuser");
    let password = env_or("TEST_DB_PASSWORD", "testpass");
    let dbname = env_or("TEST_DB_NAME", "testdb");
    let conn = format!("host={host} port={port} user={user} password={password} dbname={dbname}");
    let d = PostgresDriver::connect(&conn)
        .unwrap_or_else(|e| panic!("postgres unreachable at {host}:{port} — {}", e.message));
    d.exec_ddl(&[
        format!("CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA_NAME}"),
        format!("SET search_path TO {PG_SCHEMA_NAME}"),
    ])
    .expect("pg schema create");
    d.exec_ddl(&pg_schema_statements()).expect("pg ddl");
    for s in &art.seed {
        d.prepare(s).run(&[]).expect("pg seed");
    }
    d.exec_ddl(&pg_seq_reset()).expect("pg seq reset");
    d
}

/// Connect + (re)seed a live MySQL in the isolated `scp_rust_bench` database. Panics (fail-closed)
/// if MySQL is unreachable.
fn connect_mysql(art: &Artifact) -> MysqlDriver {
    let host = env_or("TEST_MYSQL_HOST", "127.0.0.1");
    let port = env_or("TEST_MYSQL_PORT", "3307");
    let user = env_or("TEST_MYSQL_USER", "testuser");
    let password = env_or("TEST_MYSQL_PASSWORD", "testpass");
    let boot_db = env_or("TEST_MYSQL_DB", "testdb");
    let boot_url = format!("mysql://{user}:{password}@{host}:{port}/{boot_db}");
    let boot = MysqlDriver::connect(&boot_url)
        .unwrap_or_else(|e| panic!("mysql unreachable at {host}:{port} — {}", e.message));
    boot.exec_ddl(&[format!("CREATE DATABASE IF NOT EXISTS {MYSQL_DB_NAME}")])
        .expect("mysql database create");
    drop(boot);
    let url = format!("mysql://{user}:{password}@{host}:{port}/{MYSQL_DB_NAME}");
    let d = MysqlDriver::connect(&url).unwrap_or_else(|e| {
        panic!(
            "mysql ({MYSQL_DB_NAME}) unreachable at {host}:{port} — {}",
            e.message
        )
    });
    d.exec_ddl(&mysql_schema_statements()).expect("mysql ddl");
    for s in &art.seed {
        d.prepare(s).run(&[]).expect("mysql seed");
    }
    d
}

// Lazy, memoized live connections — one per dialect, reused across every `run`/`throughput`
// request in this adapter process (the harness spawns ONE subprocess per (language × impl) cell,
// so a single pair of connections serves the whole cell's DB-backed axis).
thread_local! {
    static PG_CONN: RefCell<Option<PostgresDriver>> = RefCell::new(None);
    static MYSQL_CONN: RefCell<Option<MysqlDriver>> = RefCell::new(None);
}

// ── generated artifact (schema + seed + PER-DIALECT case bundles) ────────────
// `cases` is the sqlite map (the in-proc DB-backed path + fairness cost denominator);
// `cases_by_dialect` carries all 3 dialects for the per-dialect MICRO axis (#44 gap #1).
struct Artifact {
    schema: Vec<String>,
    seed: Vec<String>,
    cases: HashMap<String, J>,
    cases_by_dialect: HashMap<String, HashMap<String, J>>,
}

fn cases_map(block: &J) -> HashMap<String, J> {
    let mut cases = HashMap::new();
    for c in block["cases"].as_array().unwrap() {
        cases.insert(c["case"].as_str().unwrap().to_string(), c.clone());
    }
    cases
}

fn load_artifact(path: &str) -> Artifact {
    let raw = std::fs::read_to_string(path).expect("read bundles.json");
    let j: J = serde_json::from_str(&raw).expect("parse bundles.json");
    let schema = j["schema"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect();
    let seed = j["seed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect();
    let mut cases_by_dialect = HashMap::new();
    for (d, block) in j["dialects"].as_object().unwrap() {
        cases_by_dialect.insert(d.clone(), cases_map(block));
    }
    let cases = cases_by_dialect.get("sqlite").cloned().unwrap();
    Artifact {
        schema,
        seed,
        cases,
        cases_by_dialect,
    }
}

fn seed_driver(art: &Artifact) -> SqliteDriver {
    let d = SqliteDriver::in_memory(&art.schema).expect("schema");
    for s in &art.seed {
        d.prepare(s).run(&[]).expect("seed");
    }
    d
}

// A `Sync` view over `&SqliteDriver` so the relation read path (`read_bundle_pooled`, which
// requires `Driver + Sync`) can run. The bench is SINGLE-THREADED and every relation case has a
// single sibling, so `read_bundle_pooled` never actually dispatches concurrently — the view is
// only ever touched from one thread, making the `unsafe impl Sync` sound here.
// (`SyncDriverRef` is defined below, near `run_lm`.)

// ── sql baseline (hand-optimized raw SQL via the runtime's SqliteDriver) ─────
fn int_arr(n: usize) -> Vec<Value> {
    (1..=n as i64).map(Value::Int).collect()
}

fn run_sql(case: &str, d: &SqliteDriver) {
    match case {
        "find" => {
            d.prepare("SELECT id, author_id, title, status, views, created_at FROM posts WHERE author_id = ? AND status = ? AND created_at >= ? ORDER BY id ASC")
                .all(&[Value::Int(1), Value::Str("live".into()), Value::Str("2026-02-01".into())]).unwrap();
        }
        "complexWhere" => {
            let mut p = vec![
                Value::Int(1),
                Value::Str("2026-02-01".into()),
                Value::Str("post-%".into()),
            ];
            p.extend(int_arr(5));
            d.prepare("SELECT id, author_id, title, status, views FROM posts WHERE author_id = ? AND created_at >= ? AND title LIKE ? AND id IN (?, ?, ?, ?, ?) ORDER BY id ASC").all(&p).unwrap();
        }
        "inList" => {
            let p = int_arr(10);
            let ph = vec!["?"; 10].join(", ");
            d.prepare(&format!(
                "SELECT id, title FROM posts WHERE id IN ({ph}) ORDER BY id ASC"
            ))
            .all(&p)
            .unwrap();
        }
        "belongsTo" => {
            let posts = d
                .prepare(
                    "SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC",
                )
                .all(&[Value::Int(1)])
                .unwrap();
            let mut aids: Vec<i64> = posts
                .iter()
                .filter_map(|r| obj_int(r, "author_id"))
                .collect();
            aids.sort();
            aids.dedup();
            let ph = vec!["?"; aids.len()].join(", ");
            d.prepare(&format!("SELECT id, name FROM users WHERE id IN ({ph})"))
                .all(&aids.into_iter().map(Value::Int).collect::<Vec<_>>())
                .unwrap();
        }
        "hasMany" => {
            let posts = d
                .prepare(
                    "SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC",
                )
                .all(&[Value::Int(1)])
                .unwrap();
            let ids: Vec<i64> = posts.iter().filter_map(|r| obj_int(r, "id")).collect();
            let ph = vec!["?"; ids.len()].join(", ");
            d.prepare(&format!(
                "SELECT id, post_id, body FROM comments WHERE post_id IN ({ph})"
            ))
            .all(&ids.into_iter().map(Value::Int).collect::<Vec<_>>())
            .unwrap();
        }
        "hasManyLimit" => {
            let posts = d
                .prepare(
                    "SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC",
                )
                .all(&[Value::Int(1)])
                .unwrap();
            let ids: Vec<i64> = posts.iter().filter_map(|r| obj_int(r, "id")).collect();
            let ph = vec!["?"; ids.len()].join(", ");
            d.prepare(&format!("SELECT id, post_id, body FROM (SELECT id, post_id, body, ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY id DESC) rn FROM comments WHERE post_id IN ({ph})) WHERE rn <= 3")).all(&ids.into_iter().map(Value::Int).collect::<Vec<_>>()).unwrap();
        }
        "batchInsert" => {
            let cols = ["author_id", "title", "status", "views", "created_at"];
            let vals: Vec<String> = (0..10)
                .map(|_| format!("({})", vec!["?"; cols.len()].join(",")))
                .collect();
            let mut p = Vec::new();
            for i in 0..10 {
                p.push(Value::Int(2));
                p.push(Value::Str(format!("bulk-{i}")));
                p.push(Value::Str("live".into()));
                p.push(Value::Int(0));
                p.push(Value::Str("2026-05-01".into()));
            }
            d.prepare(&format!(
                "INSERT INTO posts ({}) VALUES {}",
                cols.join(","),
                vals.join(",")
            ))
            .run(&p)
            .unwrap();
        }
        "writeTxGate" => {
            d.prepare("BEGIN").run(&[]).unwrap();
            let g = d
                .prepare("SELECT 1 FROM users WHERE id = ?")
                .all(&[Value::Int(1)])
                .unwrap();
            if g.is_empty() {
                d.prepare("ROLLBACK").run(&[]).unwrap();
                panic!("requires_absent");
            }
            d.prepare("INSERT INTO uniq (name, s0, f0) VALUES (?, ?, ?) ON CONFLICT DO NOTHING")
                .run(&[
                    Value::Str("title_per_author".into()),
                    Value::Str("1".into()),
                    Value::Str("txn-post".into()),
                ])
                .unwrap();
            d.prepare("INSERT INTO posts (author_id, title, created_at) VALUES (?, ?, ?) RETURNING id, author_id, title").all(&[Value::Int(1), Value::Str("txn-post".into()), Value::Str("2026-05-01".into())]).unwrap();
            d.prepare("UPDATE users SET post_count = post_count + ? WHERE id = ?")
                .run(&[Value::Int(1), Value::Int(1)])
                .unwrap();
            d.prepare("COMMIT").run(&[]).unwrap();
        }
        _ => panic!("unknown case {case}"),
    }
}

fn obj_int(v: &Value, key: &str) -> Option<i64> {
    if let Value::Obj(pairs) = v {
        for (k, val) in pairs {
            if k == key {
                if let Value::Int(i) = val {
                    return Some(*i);
                }
            }
        }
    }
    None
}

// ── litedbmodel runtime (codegen / ir) op ─────────────────────────────────────
// Generic over `&dyn Driver` so the SAME op runs against SqliteDriver / PostgresDriver /
// MysqlDriver (#53 — the live PG/MySQL wiring).
fn run_lm(case: &J, d: &dyn Driver) {
    let bundle = &case["bundle"];
    let kind = case["kind"].as_str().unwrap();
    let input = &case["input"];
    match kind {
        "batch" => {
            execute_transaction_bundle(bundle, &J::Object(Default::default()), d).unwrap();
        }
        "tx" => {
            execute_transaction_bundle(bundle, input, d).unwrap();
        }
        "relation" => {
            // Relations run on the SAME seeded driver via a single-threaded Sync view.
            let with_name = case["withRelation"].as_str().unwrap().to_string();
            let sd = SyncDriverRef(d);
            let conns: HashMap<String, &(dyn Driver + Sync)> = HashMap::new();
            read_bundle_pooled(bundle, input, &sd, &[with_name], &conns).unwrap();
        }
        _ => {
            execute_bundle(bundle, input, d).unwrap();
        }
    }
}

// A borrowing Sync view over an existing &dyn Driver (single-threaded bench — sound, see above).
struct SyncDriverRef<'a>(&'a dyn Driver);
impl<'a> Driver for SyncDriverRef<'a> {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        self.0.prepare(sql)
    }
}
unsafe impl<'a> Sync for SyncDriverRef<'a> {}

// ── fairness cost probe: DML statements + rows read (tx-control excluded) ──────
use std::cell::Cell;

struct CountingDriver<'a> {
    inner: &'a SqliteDriver,
    queries: Cell<u64>,
    rows: Cell<u64>,
}
struct CountingStmt<'a> {
    inner: Box<dyn PreparedStatement + 'a>,
    is_dml: bool,
    queries: &'a Cell<u64>,
    rows: &'a Cell<u64>,
}
impl<'a> PreparedStatement for CountingStmt<'a> {
    fn all(&mut self, params: &[Value]) -> Result<Vec<Value>, litedbmodel_runtime::SqlFailure> {
        if self.is_dml {
            self.queries.set(self.queries.get() + 1);
        }
        let r = self.inner.all(params)?;
        if self.is_dml {
            self.rows.set(self.rows.get() + r.len() as u64);
        }
        Ok(r)
    }
    fn run(&mut self, params: &[Value]) -> Result<RunInfo, litedbmodel_runtime::SqlFailure> {
        if self.is_dml {
            self.queries.set(self.queries.get() + 1);
        }
        self.inner.run(params)
    }
}
impl<'a> Driver for CountingDriver<'a> {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        let up = sql.trim_start().to_uppercase();
        let is_ctrl = [
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT",
            "RELEASE",
            "PRAGMA",
        ]
        .iter()
        .any(|k| up.starts_with(k));
        Box::new(CountingStmt {
            inner: self.inner.prepare(sql),
            is_dml: !is_ctrl,
            queries: &self.queries,
            rows: &self.rows,
        })
    }
}
unsafe impl<'a> Sync for CountingDriver<'a> {}

fn cost(impl_: &str, case_name: &str, art: &Artifact) -> (u64, u64) {
    let base = seed_driver(art);
    let counter = CountingDriver {
        inner: &base,
        queries: Cell::new(0),
        rows: Cell::new(0),
    };
    if impl_ == "sql" {
        run_sql_counting(case_name, &counter);
    } else if impl_ == "codegen" {
        panic!("lm_bench serves sql/ir only; the codegen cell rides the dedicated lm_codegen binary (fail-closed)");
    } else {
        run_lm_counting(&art.cases[case_name], &counter);
    }
    (counter.queries.get(), counter.rows.get())
}

// The cost variants take a &dyn Driver so the CountingDriver is threaded through.
fn run_sql_counting(case: &str, d: &CountingDriver) {
    // Re-run the sql baseline against the counting driver by delegating through the trait.
    // Reuse the same statement text as run_sql but via the generic Driver.
    run_sql_generic(case, d);
}
fn run_lm_counting(case: &J, d: &CountingDriver) {
    let bundle = &case["bundle"];
    let kind = case["kind"].as_str().unwrap();
    let input = &case["input"];
    match kind {
        "batch" => {
            execute_transaction_bundle(bundle, &J::Object(Default::default()), d).unwrap();
        }
        "tx" => {
            execute_transaction_bundle(bundle, input, d).unwrap();
        }
        "relation" => {
            let with_name = case["withRelation"].as_str().unwrap().to_string();
            let conns: HashMap<String, &(dyn Driver + Sync)> = HashMap::new();
            read_bundle_pooled(bundle, input, d, &[with_name], &conns).unwrap();
        }
        _ => {
            execute_bundle(bundle, input, d).unwrap();
        }
    }
}

// Generic sql baseline over any Driver (used by both the timed loop's SqliteDriver and the
// counting probe's CountingDriver).
fn run_sql_generic<D: Driver + ?Sized>(case: &str, d: &D) {
    match case {
        "find" => {
            d.prepare("SELECT id, author_id, title, status, views, created_at FROM posts WHERE author_id = ? AND status = ? AND created_at >= ? ORDER BY id ASC").all(&[Value::Int(1), Value::Str("live".into()), Value::Str("2026-02-01".into())]).unwrap();
        }
        "complexWhere" => {
            let mut p = vec![
                Value::Int(1),
                Value::Str("2026-02-01".into()),
                Value::Str("post-%".into()),
            ];
            p.extend(int_arr(5));
            d.prepare("SELECT id, author_id, title, status, views FROM posts WHERE author_id = ? AND created_at >= ? AND title LIKE ? AND id IN (?, ?, ?, ?, ?) ORDER BY id ASC").all(&p).unwrap();
        }
        "inList" => {
            let p = int_arr(10);
            let ph = vec!["?"; 10].join(", ");
            d.prepare(&format!(
                "SELECT id, title FROM posts WHERE id IN ({ph}) ORDER BY id ASC"
            ))
            .all(&p)
            .unwrap();
        }
        "belongsTo" => {
            let posts = d
                .prepare(
                    "SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC",
                )
                .all(&[Value::Int(1)])
                .unwrap();
            let mut aids: Vec<i64> = posts
                .iter()
                .filter_map(|r| obj_int(r, "author_id"))
                .collect();
            aids.sort();
            aids.dedup();
            let ph = vec!["?"; aids.len()].join(", ");
            d.prepare(&format!("SELECT id, name FROM users WHERE id IN ({ph})"))
                .all(&aids.into_iter().map(Value::Int).collect::<Vec<_>>())
                .unwrap();
        }
        "hasMany" => {
            let posts = d
                .prepare(
                    "SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC",
                )
                .all(&[Value::Int(1)])
                .unwrap();
            let ids: Vec<i64> = posts.iter().filter_map(|r| obj_int(r, "id")).collect();
            let ph = vec!["?"; ids.len()].join(", ");
            d.prepare(&format!(
                "SELECT id, post_id, body FROM comments WHERE post_id IN ({ph})"
            ))
            .all(&ids.into_iter().map(Value::Int).collect::<Vec<_>>())
            .unwrap();
        }
        "hasManyLimit" => {
            let posts = d
                .prepare(
                    "SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC",
                )
                .all(&[Value::Int(1)])
                .unwrap();
            let ids: Vec<i64> = posts.iter().filter_map(|r| obj_int(r, "id")).collect();
            let ph = vec!["?"; ids.len()].join(", ");
            d.prepare(&format!("SELECT id, post_id, body FROM (SELECT id, post_id, body, ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY id DESC) rn FROM comments WHERE post_id IN ({ph})) WHERE rn <= 3")).all(&ids.into_iter().map(Value::Int).collect::<Vec<_>>()).unwrap();
        }
        "batchInsert" => {
            let cols = ["author_id", "title", "status", "views", "created_at"];
            let vals: Vec<String> = (0..10)
                .map(|_| format!("({})", vec!["?"; cols.len()].join(",")))
                .collect();
            let mut p = Vec::new();
            for i in 0..10 {
                p.push(Value::Int(2));
                p.push(Value::Str(format!("bulk-{i}")));
                p.push(Value::Str("live".into()));
                p.push(Value::Int(0));
                p.push(Value::Str("2026-05-01".into()));
            }
            d.prepare(&format!(
                "INSERT INTO posts ({}) VALUES {}",
                cols.join(","),
                vals.join(",")
            ))
            .run(&p)
            .unwrap();
        }
        "writeTxGate" => {
            d.prepare("BEGIN").run(&[]).unwrap();
            let g = d
                .prepare("SELECT 1 FROM users WHERE id = ?")
                .all(&[Value::Int(1)])
                .unwrap();
            if g.is_empty() {
                d.prepare("ROLLBACK").run(&[]).unwrap();
                panic!("requires_absent");
            }
            d.prepare("INSERT INTO uniq (name, s0, f0) VALUES (?, ?, ?) ON CONFLICT DO NOTHING")
                .run(&[
                    Value::Str("title_per_author".into()),
                    Value::Str("1".into()),
                    Value::Str("txn-post".into()),
                ])
                .unwrap();
            d.prepare("INSERT INTO posts (author_id, title, created_at) VALUES (?, ?, ?) RETURNING id, author_id, title").all(&[Value::Int(1), Value::Str("txn-post".into()), Value::Str("2026-05-01".into())]).unwrap();
            d.prepare("UPDATE users SET post_count = post_count + ? WHERE id = ?")
                .run(&[Value::Int(1), Value::Int(1)])
                .unwrap();
            d.prepare("COMMIT").run(&[]).unwrap();
        }
        _ => panic!("unknown case {case}"),
    }
}

// ── micro-bench: mock driver (fixed rows, no round-trip) ──────────────────────
struct MockDriver;
struct MockStmt {
    rows: Vec<Value>,
}
fn obj(pairs: &[(&str, Value)]) -> Value {
    Value::Obj(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}
fn fixture(sql: &str) -> Vec<Value> {
    let s = sql.to_lowercase();
    let st = s.trim_start();
    if st.starts_with("select") {
        if s.contains("from comments") {
            return (1..=25)
                .map(|i| {
                    obj(&[
                        ("id", Value::Int(i)),
                        ("post_id", Value::Int(((i - 1) % 5) + 1)),
                        ("body", Value::Str(format!("comment-{i}"))),
                    ])
                })
                .collect();
        }
        if s.contains("from users") {
            return vec![obj(&[
                ("id", Value::Int(1)),
                ("name", Value::Str("user-1".into())),
            ])];
        }
        if s.contains("from posts") || s.contains("from ") {
            return (1..=5)
                .map(|i| {
                    obj(&[
                        ("id", Value::Int(i)),
                        ("author_id", Value::Int(1)),
                        ("title", Value::Str(format!("post-{i}"))),
                        ("status", Value::Str("live".into())),
                        ("views", Value::Int(i * 10)),
                        ("created_at", Value::Str("2026-02-01".into())),
                    ])
                })
                .collect();
        }
        return vec![obj(&[("1", Value::Int(1))])];
    }
    if s.contains("returning") {
        return vec![obj(&[
            ("id", Value::Int(41)),
            ("author_id", Value::Int(1)),
            ("title", Value::Str("txn-post".into())),
        ])];
    }
    vec![]
}
impl PreparedStatement for MockStmt {
    fn all(&mut self, _params: &[Value]) -> Result<Vec<Value>, litedbmodel_runtime::SqlFailure> {
        Ok(self.rows.clone())
    }
    fn run(&mut self, _params: &[Value]) -> Result<RunInfo, litedbmodel_runtime::SqlFailure> {
        Ok(RunInfo {
            changes: 1,
            last_insert_rowid: 41,
        })
    }
}
impl Driver for MockDriver {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        Box::new(MockStmt { rows: fixture(sql) })
    }
}
unsafe impl Sync for MockDriver {}

fn run_micro(impl_: &str, case: &J, mock: &MockDriver) {
    if impl_ == "sql" {
        run_sql_generic(case["case"].as_str().unwrap(), mock);
    } else if impl_ == "codegen" {
        panic!("lm_bench serves sql/ir only; the codegen cell rides the dedicated lm_codegen binary (fail-closed)");
    } else {
        let kind = case["kind"].as_str().unwrap();
        let bundle = &case["bundle"];
        let input = &case["input"];
        match kind {
            "batch" => {
                execute_transaction_bundle(bundle, &J::Object(Default::default()), mock).unwrap();
            }
            "tx" => {
                execute_transaction_bundle(bundle, input, mock).unwrap();
            }
            "relation" => {
                let with_name = case["withRelation"].as_str().unwrap().to_string();
                let conns: HashMap<String, &(dyn Driver + Sync)> = HashMap::new();
                read_bundle_pooled(bundle, input, mock, &[with_name], &conns).unwrap();
            }
            _ => {
                execute_bundle(bundle, input, mock).unwrap();
            }
        }
    }
}


// ── verify leg (interpreter canonical observation) ────────────────────────────
//
// The codegen cell lives in the DEDICATED serde_json-free `lm_codegen` binary; behaviour equality
// is proven CROSS-BINARY: this binary's `verify` answers the INTERPRETER (ir) output's canonical
// JSON (sorted keys — serde_json over a BTreeMap map), lm_codegen's `verify` answers the codegen
// output's canonical JSON in the same form, and the selfcheck driver compares them per case.

use litedbmodel_runtime::stitch_relation;
use litedbmodel_runtime::encode_value;

/// The full-key TransactionResult `Value` (present-as-null for an absent optional field) — the
/// canonical shape the de-boxed codegen path emits (`ser_T0` always emits every declared field).
/// The runtime OMITS an absent optional key; we present the full shape without touching the row
/// DATA (representation-only), so codegen vs ir compare by VALUE.
fn normalize_tx_result_value(result: &Value) -> Value {
    let pairs = match result {
        Value::Obj(p) => p,
        other => return other.clone(),
    };
    let get = |k: &str| pairs.iter().find(|(pk, _)| pk == k).map(|(_, v)| v).cloned();
    let mut out = vec![
        ("committed".to_string(), get("committed").unwrap_or(Value::Bool(false))),
        ("executed".to_string(), get("executed").unwrap_or(Value::Arr(Vec::new()))),
        ("shortCircuit".to_string(), get("shortCircuit").unwrap_or(Value::Null)),
        ("entity".to_string(), get("entity").unwrap_or(Value::Null)),
    ];
    if let Some(rr) = get("returnedRows") {
        out.push(("returnedRows".to_string(), rr));
    }
    Value::Obj(out)
}

/// The interpreter (ir) output for one case, as pure JSON (bc Value → serde encode).
/// Canonical compact JSON with RECURSIVELY SORTED object keys (the cross-binary comparison form;
/// serde_json's `to_string` alone is insertion-ordered when the tree enables preserve_order).
fn canon_json(v: &J) -> String {
    match v {
        J::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = String::from("{");
            for (i, k) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(k).unwrap());
                out.push(':');
                out.push_str(&canon_json(&map[*k]));
            }
            out.push('}');
            out
        }
        J::Array(a) => {
            let mut out = String::from("[");
            for (i, e) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&canon_json(e));
            }
            out.push(']');
            out
        }
        other => serde_json::to_string(other).unwrap(),
    }
}

fn run_lm_value(case: &J, driver: &dyn Driver) -> serde_json::Value {
    let bundle = &case["bundle"];
    let kind = case["kind"].as_str().unwrap();
    let input = &case["input"];
    let out: Value = match kind {
        "batch" => normalize_tx_result_value(
            &execute_transaction_bundle(bundle, &J::Object(Default::default()), driver).unwrap(),
        ),
        "tx" => normalize_tx_result_value(&execute_transaction_bundle(bundle, input, driver).unwrap()),
        "relation" => {
            let with = case["withRelation"].as_str().unwrap();
            let op = &bundle["relations"][with];
            let base = execute_bundle(bundle, input, driver).unwrap();
            let rows = match base {
                Value::Arr(r) => r,
                _ => vec![],
            };
            Value::Arr(stitch_relation(op, rows, driver).unwrap())
        }
        _ => execute_bundle(bundle, input, driver).unwrap(),
    };
    encode_value(&out)
}

// ── protocol I/O ──────────────────────────────────────────────────────────────
fn write_line(v: &J) {
    let mut out = std::io::stdout();
    out.write_all(serde_json::to_string(v).unwrap().as_bytes())
        .unwrap();
    out.write_all(b"\n").unwrap();
    out.flush().unwrap();
}

fn now_ms() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        * 1000.0
}

fn main() {
    let mut impl_ = "sql".to_string();
    for a in std::env::args() {
        if let Some(v) = a.strip_prefix("--impl=") {
            impl_ = v.to_string();
        }
    }
    let here = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let bundles = here.join("../../generated/bundles.json");
    let art = load_artifact(bundles.to_str().unwrap());

    // FAIL-CLOSED: the codegen cell is the dedicated serde_json-free lm_codegen binary
    // (adapters/rust-codegen) — this binary refuses to impersonate it.
    if impl_ == "codegen" {
        panic!("lm_bench serves sql/ir only; spawn adapters/rust-codegen/target/release/lm_codegen for the codegen cell");
    }
    // decode_scope is used by the runtime; touch it so the import is exercised.
    let _ = decode_scope(&J::Object(Default::default()));

    write_line(
        &serde_json::json!({"kind":"ready","language":"rust","impl":impl_,"readyAtEpochMs":now_ms()}),
    );

    let stdin = std::io::stdin();
    for line in stdin.lock().lines() {
        let line = line.unwrap();
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let req: J = match serde_json::from_str(line) {
            Ok(r) => r,
            Err(e) => {
                write_line(
                    &serde_json::json!({"kind":"error","message":format!("bad request: {e}")}),
                );
                continue;
            }
        };
        let kind = req["kind"].as_str().unwrap_or("");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            handle(kind, &req, &impl_, &art)
        }));
        if let Err(e) = result {
            let msg = e
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| e.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic".into());
            write_line(&serde_json::json!({"kind":"error","message":msg}));
        }
    }
}

// `sql` is the hand-written raw-SQL baseline (sqlite-shaped by construction — every language
// adapter runs its `sql` cell on sqlite only). `codegen` in THIS binary always panics (fail-closed
// — it rides the dedicated lm_codegen binary); its generated read module is itself wired to the
// in-proc sqlite driver only, so codegen DB-backed is sqlite-only across every language (#53 — not
// a gap, matches the established convention). Only `ir` gains live PG/MySQL (#53).
fn db_skip_reason(impl_: &str, dialect: &str) -> Option<String> {
    if dialect == "sqlite" {
        return None;
    }
    match impl_ {
        "sql" => Some(format!(
            "sql baseline is hand-written sqlite SQL — not run against {dialect} (dialect-specific by construction)"
        )),
        "codegen" => Some(format!(
            "codegen generated-module cell is wired to the in-proc sqlite driver; PG/MySQL DB-backed not wired for the generated cell (see lm_codegen) — not run against {dialect}"
        )),
        _ => None, // ir: PG/MySQL wired below (live PostgresDriver/MysqlDriver).
    }
}

// Run `op` against the live driver for `dialect` ("postgres" | "mysql"), lazily connecting +
// seeding once per adapter process (the connection is memoized in PG_CONN/MYSQL_CONN — every
// `run`/`throughput` request in this cell's whole matrix run reuses it, same lifetime as the
// in-proc SqliteDriver's per-request re-seed would otherwise cover).
fn with_live_driver<F: FnOnce(&dyn Driver)>(dialect: &str, art: &Artifact, op: F) {
    match dialect {
        "postgres" => PG_CONN.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                *slot = Some(connect_pg(art));
            }
            op(slot.as_ref().unwrap());
        }),
        "mysql" => MYSQL_CONN.with(|cell| {
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                *slot = Some(connect_mysql(art));
            }
            op(slot.as_ref().unwrap());
        }),
        other => panic!("with_live_driver: unknown dialect {other}"),
    }
}

fn handle(kind: &str, req: &J, impl_: &str, art: &Artifact) {
    match kind {
        "run" => {
            let case = req["case"].as_str().unwrap();
            let dialect = req["dialect"].as_str().unwrap_or("sqlite");
            if let Some(reason) = db_skip_reason(impl_, dialect) {
                write_line(
                    &serde_json::json!({"kind":"skipped","case":case,"dialect":dialect,"reason":reason}),
                );
                return;
            }
            let warmup = req["warmup"].as_u64().unwrap() as usize;
            let iters = req["iterations"].as_u64().unwrap() as usize;
            let cjson = art.cases_by_dialect[dialect][case].clone();
            if dialect == "sqlite" {
                let d = seed_driver(art);
                let samples = collect(warmup, iters, || {
                    if impl_ == "sql" {
                        run_sql(case, &d);
                    } else if impl_ == "codegen" {
                        panic!("lm_bench serves sql/ir only (codegen = lm_codegen binary)");
                    } else {
                        run_lm(&cjson, &d);
                    }
                });
                write_line(
                    &serde_json::json!({"kind":"run","case":case,"dialect":dialect,"samplesMs":samples}),
                );
            } else {
                with_live_driver(dialect, art, |d| {
                    let samples = collect(warmup, iters, || run_lm(&cjson, d));
                    write_line(
                        &serde_json::json!({"kind":"run","case":case,"dialect":dialect,"samplesMs":samples}),
                    );
                });
            }
        }
        "throughput" => {
            let case = req["case"].as_str().unwrap();
            let dialect = req["dialect"].as_str().unwrap_or("sqlite");
            if let Some(reason) = db_skip_reason(impl_, dialect) {
                write_line(
                    &serde_json::json!({"kind":"skipped","case":case,"dialect":dialect,"reason":reason}),
                );
                return;
            }
            let iters = req["iterations"].as_u64().unwrap() as usize;
            let cjson = art.cases_by_dialect[dialect][case].clone();
            if dialect == "sqlite" {
                let d = seed_driver(art);
                let t0 = Instant::now();
                for _ in 0..iters {
                    if impl_ == "sql" {
                        run_sql(case, &d);
                    } else if impl_ == "codegen" {
                        panic!("lm_bench serves sql/ir only (codegen = lm_codegen binary)");
                    } else {
                        run_lm(&cjson, &d);
                    }
                }
                let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
                write_line(
                    &serde_json::json!({"kind":"throughput","case":case,"dialect":dialect,"elapsedMs":elapsed,"completed":iters}),
                );
            } else {
                with_live_driver(dialect, art, |d| {
                    let t0 = Instant::now();
                    for _ in 0..iters {
                        run_lm(&cjson, d);
                    }
                    let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
                    write_line(
                        &serde_json::json!({"kind":"throughput","case":case,"dialect":dialect,"elapsedMs":elapsed,"completed":iters}),
                    );
                });
            }
        }
        "micro" => {
            let case = req["case"].as_str().unwrap();
            let dialect = req["dialect"].as_str().unwrap_or("sqlite");
            if impl_ == "sql" && dialect != "sqlite" {
                write_line(
                    &serde_json::json!({"kind":"skipped","case":case,"dialect":dialect,"reason":"hand-SQL baseline is sqlite-shaped"}),
                );
                return;
            }
            let warmup = req["warmup"].as_u64().unwrap() as usize;
            let iters = req["iterations"].as_u64().unwrap() as usize;
            let mock = MockDriver;
            // PER-DIALECT bundle — the render/placeholder/array form differs.
            let cjson = art.cases_by_dialect[dialect][case].clone();
            let samples = collect(warmup, iters, || run_micro(impl_, &cjson, &mock));
            write_line(
                &serde_json::json!({"kind":"micro","case":case,"dialect":dialect,"samplesMs":samples}),
            );
        }
        "rss" => {
            write_line(&serde_json::json!({"kind":"rss","rssBytes":rss_bytes()}));
        }
        "cost" => {
            let case = req["case"].as_str().unwrap();
            let dialect = req["dialect"].as_str().unwrap_or("sqlite");
            let (q, r) = cost(impl_, case, art);
            write_line(
                &serde_json::json!({"kind":"cost","case":case,"dialect":dialect,"queries":q,"rows":r}),
            );
        }
        "verify" => {
            // Behaviour-equality observation (cross-binary): the INTERPRETER output's canonical
            // JSON (sorted keys — serde_json's BTreeMap map). The selfcheck driver compares it
            // against the lm_codegen binary's codegen canon per case.
            let case = req["case"].as_str().unwrap();
            let cjson = art.cases[case].clone();
            let d = seed_driver(art);
            let ir = run_lm_value(&cjson, &d);
            let canon = canon_json(&ir);
            write_line(&serde_json::json!({"kind":"verify","case":case,"impl":"ir","canon":canon}));
        }
        "shutdown" => std::process::exit(0),
        _ => {}
    }
}

fn collect<F: FnMut()>(warmup: usize, iters: usize, mut op: F) -> Vec<f64> {
    for _ in 0..warmup {
        op();
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        op();
        samples.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    samples
}

fn rss_bytes() -> u64 {
    // Best-effort RSS via ps (portable enough for the resource table; not on the hot path).
    if let Ok(out) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
    {
        if let Ok(s) = String::from_utf8(out.stdout) {
            if let Ok(kb) = s.trim().parse::<u64>() {
                return kb * 1024;
            }
        }
    }
    0
}
