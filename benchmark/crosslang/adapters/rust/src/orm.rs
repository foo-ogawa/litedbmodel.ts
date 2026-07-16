//! ORM-plan EXECUTOR + live smoke — Rust (epic #63), the `lm_orm` binary.
//!
//! Port of the PROVEN TS reference (benchmark/crosslang/orm-exec-ts.ts + orm-smoke.ts). Loads the
//! committed language-neutral artifact benchmark/crosslang/generated/orm-plan.json and executes ALL
//! 19 ORM ops × {sqlite, mysql, postgres} through the SHIPPED litedbmodel_runtime driver seam
//! (SqliteDriver in-proc + livedb PostgresDriver (tokio-postgres+deadpool) / MysqlDriver (sqlx)),
//! binding the BAKED per-dialect SQL from the artifact per the bindKind protocol (NO SQL gen here).
//!
//! The runtime's own native `Node`/`Value` codec parses the artifact + params (decode_value) — no
//! serde_json in the exec path. Output is a FLAT CSV (no wire protocol).
//!
//! Spawn convention (run.ts orchestrator): the release binary
//!     benchmark/crosslang/adapters/rust/target/release/lm_orm [--smoke]
//! `--smoke` runs the 57-cell matrix and exits; without it, it runs ALL 19 ops × 3 dialects,
//! self-measures, and writes benchmark/crosslang/.results/rust.csv (no stdin/stdout protocol).

use behavior_contracts::Value;
use litedbmodel_runtime::livedb::{MysqlDriver, PostgresDriver};
use litedbmodel_runtime::{decode_value, Driver, Node as J, SqliteDriver};
use std::cell::RefCell;
use std::collections::HashSet;
use std::time::Instant;

const PG_SCHEMA_NAME: &str = "scp_rust_bench";
const MYSQL_DB_NAME: &str = "scp_rust_bench";

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

// ── {{SEQ}} substitution + param decode ────────────────────────────────────────
thread_local! {
    static SEQ: RefCell<i64> = const { RefCell::new(0) };
}
fn next_seq() -> i64 {
    SEQ.with(|s| {
        let v = *s.borrow();
        *s.borrow_mut() = v + 1;
        v
    })
}

/// Decode a JSON param Node → bc Value, substituting `{{SEQ}}` in strings (recursively in arrays).
fn decode_param(node: &J, seq: i64) -> Value {
    match node {
        J::Str(s) if s.contains("{{SEQ}}") => Value::Str(s.replace("{{SEQ}}", &seq.to_string())),
        J::Array(a) => Value::Arr(a.iter().map(|e| decode_param(e, seq)).collect()),
        other => decode_value(other).expect("decode param"),
    }
}
fn decode_params(params: &J, seq: i64) -> Vec<Value> {
    params
        .as_array()
        .unwrap_or(&[])
        .iter()
        .map(|p| decode_param(p, seq))
        .collect()
}

fn strip_returning(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    match lower.rfind(" returning ") {
        Some(at) => sql[..at].to_string(),
        None => sql.to_string(),
    }
}
fn has_returning(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains(" returning ")
}

// ── row field access ───────────────────────────────────────────────────────────
fn obj_get<'a>(row: &'a Value, key: &str) -> Option<&'a Value> {
    if let Value::Obj(pairs) = row {
        return pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v);
    }
    None
}
fn value_key_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Int(i) => Some(i.to_string()),
        Value::Str(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Float(f) => Some(f.to_string()),
        _ => None,
    }
}

// ── relation bind protocol (mirror bindRelation in orm-exec-ts.ts) ─────────────
struct RelBind {
    sql: String,
    params: Vec<Value>,
}

fn bind_relation(stage: &J, parents: &[Value]) -> Option<RelBind> {
    let kind = stage.get("bindKind").and_then(|k| k.as_str()).unwrap();
    let sql = stage
        .get("sql")
        .and_then(|s| s.as_str())
        .unwrap()
        .to_string();
    if let Some(single) = stage.get("single").filter(|s| !s.is_null()) {
        let parent_key = single.get("parentKey").and_then(|k| k.as_str()).unwrap();
        let mut seen = HashSet::new();
        let mut keys: Vec<Value> = Vec::new();
        for r in parents {
            if let Some(v) = obj_get(r, parent_key) {
                if let Some(s) = value_key_string(v) {
                    if seen.insert(s) {
                        keys.push(v.clone());
                    }
                }
            }
        }
        if keys.is_empty() {
            return None;
        }
        if kind == "pgArraySingle" {
            // pg single-key: bind the distinct keys as ONE array param (`= ANY($1::int[])`, baked).
            return Some(RelBind {
                sql,
                params: vec![Value::Arr(keys)],
            });
        }
        // jsonParam (sqlite/mysql): ONE param = JSON string of the distinct keys.
        let json = J::Array(keys.iter().map(value_to_node).collect()).to_json_string();
        return Some(RelBind {
            sql,
            params: vec![Value::Str(json)],
        });
    }
    // composite
    let comp = stage.get("composite").unwrap();
    let pks = comp.get("parentKeys").and_then(|k| k.as_array()).unwrap();
    let (p0, p1) = (pks[0].as_str().unwrap(), pks[1].as_str().unwrap());
    let mut seen = HashSet::new();
    let mut tuples: Vec<(Value, Value)> = Vec::new();
    for r in parents {
        let (k0, k1) = (obj_get(r, p0), obj_get(r, p1));
        if let (Some(a), Some(b)) = (k0, k1) {
            if let (Some(sa), Some(sb)) = (value_key_string(a), value_key_string(b)) {
                if seen.insert(format!("{sa} {sb}")) {
                    tuples.push((a.clone(), b.clone()));
                }
            }
        }
    }
    if tuples.is_empty() {
        return None;
    }
    if kind == "pgArrayComposite" {
        // pg composite: TWO array params = the two key columns.
        let c0: Vec<Value> = tuples.iter().map(|t| t.0.clone()).collect();
        let c1: Vec<Value> = tuples.iter().map(|t| t.1.clone()).collect();
        return Some(RelBind {
            sql,
            params: vec![Value::Arr(c0), Value::Arr(c1)],
        });
    }
    // tupleExpand (sqlite/mysql composite): repeat groupTemplate per tuple, flatten params.
    let group = stage.get("groupTemplate").and_then(|g| g.as_str()).unwrap();
    let suffix = stage.get("suffix").and_then(|s| s.as_str()).unwrap_or("");
    let groups = vec![group; tuples.len()].join(", ");
    let mut flat: Vec<Value> = Vec::new();
    for (a, b) in &tuples {
        flat.push(a.clone());
        flat.push(b.clone());
    }
    Some(RelBind {
        sql: format!("{sql}{groups}{suffix}"),
        params: flat,
    })
}

fn value_to_node(v: &Value) -> J {
    match v {
        Value::Null => J::Null,
        Value::Bool(b) => J::Bool(*b),
        Value::Int(i) => J::Int(*i),
        Value::Float(f) => J::Float(*f),
        Value::Str(s) => J::Str(s.clone()),
        Value::Arr(a) => J::Array(a.iter().map(value_to_node).collect()),
        Value::Obj(o) => J::Object(
            o.iter()
                .map(|(k, x)| (k.clone(), value_to_node(x)))
                .collect(),
        ),
    }
}

// ── executor over the generic Driver seam ──────────────────────────────────────
fn all_rows(d: &dyn Driver, sql: &str, params: &[Value]) -> Vec<Value> {
    d.prepare(sql).all(params).expect("query")
}

fn read_plan(d: &dyn Driver, plan: &J) -> usize {
    let reads = plan.get("reads").and_then(|r| r.as_array()).unwrap();
    let first_sql = reads[0].get("sql").and_then(|s| s.as_str()).unwrap();
    let first_params = decode_params(reads[0].get("params").unwrap_or(&J::NULL), 0);
    let first = all_rows(d, first_sql, &first_params);
    let mut total = first.len();
    let mut stage_rows: Vec<Vec<Value>> = vec![first];
    for stage in plan.get("relations").and_then(|r| r.as_array()).unwrap() {
        let parent_stmt = stage.get("parentStmt").and_then(|p| p.as_u64()).unwrap() as usize;
        let children = match bind_relation(stage, &stage_rows[parent_stmt]) {
            Some(rel) => all_rows(d, &rel.sql, &rel.params),
            None => Vec::new(),
        };
        total += children.len();
        stage_rows.push(children);
    }
    total
}

fn write_plan(d: &dyn Driver, dialect: &str, plan: &J) -> usize {
    let seq = next_seq();
    d.prepare("BEGIN").run(&[]).expect("begin");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut returned_id: i64 = 0;
        let mut n = 0usize;
        for st in plan.get("statements").and_then(|s| s.as_array()).unwrap() {
            let role = st.get("role").and_then(|r| r.as_str()).unwrap();
            let sql = st.get("sql").and_then(|s| s.as_str()).unwrap();
            let mut params = decode_params(st.get("params").unwrap_or(&J::NULL), seq);
            if role == "useReturn" {
                if let Some(at) = st.get("useReturnAt").and_then(|a| a.as_u64()) {
                    params[at as usize] = Value::Int(returned_id);
                }
            }
            if role == "insertReturn" {
                if dialect == "postgres" {
                    let rows = all_rows(d, sql, &params);
                    returned_id = rows
                        .first()
                        .and_then(|r| obj_get(r, "id"))
                        .and_then(|v| match v {
                            Value::Int(i) => Some(*i),
                            _ => None,
                        })
                        .unwrap_or(0);
                } else {
                    // sqlite / mysql: strip RETURNING, run, use last_insert_rowid.
                    let info = d
                        .prepare(&strip_returning(sql))
                        .run(&params)
                        .expect("insert");
                    returned_id = info.last_insert_rowid;
                }
            } else if dialect == "mysql" && has_returning(sql) {
                // MySQL has no native RETURNING: strip it (a plain upsert RETURNING id).
                d.prepare(&strip_returning(sql))
                    .run(&params)
                    .expect("write");
            } else if has_returning(sql) {
                // pg native RETURNING / sqlite RETURNING: a row-returning statement must go via all()
                // (rusqlite's execute() rejects a statement that returns rows).
                all_rows(d, sql, &params);
            } else {
                d.prepare(sql).run(&params).expect("write");
            }
            n += 1;
        }
        n
    }));
    match result {
        Ok(n) => {
            d.prepare("COMMIT").run(&[]).expect("commit");
            n
        }
        Err(e) => {
            let _ = d.prepare("ROLLBACK").run(&[]);
            std::panic::resume_unwind(e);
        }
    }
}

fn run_plan(d: &dyn Driver, dialect: &str, plan: &J) -> usize {
    match plan.get("kind").and_then(|k| k.as_str()).unwrap() {
        "read" => read_plan(d, plan),
        _ => write_plan(d, dialect, plan),
    }
}

// ── artifact + seed ────────────────────────────────────────────────────────────
struct Artifact {
    raw: J,
}
impl Artifact {
    fn dialects(&self) -> Vec<String> {
        self.raw
            .get("dialects")
            .and_then(|d| d.as_array())
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap().to_string())
            .collect()
    }
    fn ops(&self) -> &[J] {
        self.raw.get("ops").and_then(|o| o.as_array()).unwrap()
    }
    fn plan(&self, op: &str, dialect: &str) -> &J {
        self.raw
            .get("plans")
            .and_then(|p| p.get(op))
            .and_then(|p| p.get(dialect))
            .unwrap()
    }
    fn schema<'a>(&'a self, dialect: &str) -> &'a J {
        self.raw.get("schema").and_then(|s| s.get(dialect)).unwrap()
    }
}

fn load_artifact() -> Artifact {
    let here = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = here.join("../../generated/orm-plan.json");
    let raw = std::fs::read_to_string(&path).expect("read orm-plan.json");
    Artifact {
        raw: J::parse(&raw).expect("parse orm-plan.json"),
    }
}

fn pg_placeholders(sql: &str) -> String {
    // Portable seed SQL binds `?`; PG wants `$N`.
    let mut out = String::with_capacity(sql.len());
    let mut n = 0;
    for ch in sql.chars() {
        if ch == '?' {
            n += 1;
            out.push('$');
            out.push_str(&n.to_string());
        } else {
            out.push(ch);
        }
    }
    out
}

fn str_list(node: &J, key: &str) -> Vec<String> {
    node.get(key)
        .and_then(|v| v.as_array())
        .map(|a| a.iter().map(|s| s.as_str().unwrap().to_string()).collect())
        .unwrap_or_default()
}

/// Adapt a DDL statement's COLUMN TYPES to the shipped Rust `livedb` cell decoder (the #44 adapter
/// convention: each language adapts column types to its driver). The typed binary decoder maps
/// TIMESTAMP → String (rejected by the tokio-postgres/sqlx binary protocol) and MySQL BOOLEAN →
/// String; storing them as TEXT/INT (as the sqlite + #44 schemas already do) makes `SELECT *` read
/// them via the shipped seam. Data + row counts are UNCHANGED (seed never inserts these columns;
/// created_at/updated_at use DEFAULT). Only the STORAGE TYPE differs — no SQL/logic is reimplemented.
fn adapt_ddl_for_rust_decoder(ddl: &[String], dialect: &str) -> Vec<String> {
    ddl.iter()
        .map(|s| {
            // Order matters: replace the full `TIMESTAMP DEFAULT <fn>` phrases FIRST (so the fn
            // default is dropped), THEN any bare `TIMESTAMP` type token. A TEXT column keeps a
            // constant default so `ORDER BY created_at` still runs (the values are never seeded).
            // MySQL TEXT columns cannot carry a DEFAULT → use VARCHAR(64); PG uses TEXT.
            let text_type = if dialect == "mysql" {
                "VARCHAR(64)"
            } else {
                "TEXT"
            };
            let mut out = s
                .replace(
                    "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                    &format!("{text_type} DEFAULT ''"),
                )
                .replace(
                    "TIMESTAMP DEFAULT NOW()",
                    &format!("{text_type} DEFAULT ''"),
                )
                .replace("TIMESTAMP", text_type);
            if dialect == "mysql" {
                out = out.replace("TINYINT(1)", "INT").replace("BOOLEAN", "INT");
            }
            out
        })
        .collect()
}

fn seed_pooled(d: &dyn Driver, schema: &J, dialect: &str) {
    for s in schema.get("seed").and_then(|s| s.as_array()).unwrap() {
        let sql_raw = s.get("sql").and_then(|s| s.as_str()).unwrap();
        let sql = if dialect == "postgres" {
            pg_placeholders(sql_raw)
        } else {
            sql_raw.to_string()
        };
        let params = decode_params(s.get("params").unwrap_or(&J::NULL), 0);
        d.prepare(&sql).run(&params).expect("seed");
    }
}

enum LiveDriver {
    Sqlite(SqliteDriver),
    Pg(PostgresDriver),
    My(MysqlDriver),
}
impl LiveDriver {
    fn as_driver(&self) -> &dyn Driver {
        match self {
            LiveDriver::Sqlite(d) => d,
            LiveDriver::Pg(d) => d,
            LiveDriver::My(d) => d,
        }
    }
}

fn make_driver(dialect: &str, art: &Artifact) -> LiveDriver {
    let schema = art.schema(dialect);
    match dialect {
        "sqlite" => {
            let ddl = str_list(schema, "ddl");
            let d = SqliteDriver::in_memory(&ddl).expect("sqlite schema");
            seed_pooled(&d, schema, "sqlite");
            LiveDriver::Sqlite(d)
        }
        "postgres" => {
            let host = env_or("TEST_DB_HOST", "localhost");
            let port = env_or("TEST_DB_PORT", "5433");
            let user = env_or("TEST_DB_USER", "testuser");
            let password = env_or("TEST_DB_PASSWORD", "testpass");
            let dbname = env_or("TEST_DB_NAME", "testdb");
            let conn =
                format!("host={host} port={port} user={user} password={password} dbname={dbname}");
            let d = PostgresDriver::connect(&conn).unwrap_or_else(|e| {
                panic!("postgres unreachable at {host}:{port} — {}", e.message)
            });
            d.exec_ddl(&[
                format!("CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA_NAME}"),
                format!("SET search_path TO {PG_SCHEMA_NAME}"),
            ])
            .expect("pg schema");
            d.exec_ddl(&str_list(schema, "drop")).expect("pg drop");
            d.exec_ddl(&adapt_ddl_for_rust_decoder(
                &str_list(schema, "ddl"),
                "postgres",
            ))
            .expect("pg ddl");
            seed_pooled(&d, schema, "postgres");
            d.exec_ddl(&str_list(schema, "seqReset"))
                .expect("pg seqReset");
            LiveDriver::Pg(d)
        }
        "mysql" => {
            let host = env_or("TEST_MYSQL_HOST", "127.0.0.1");
            let port = env_or("TEST_MYSQL_PORT", "3307");
            let user = env_or("TEST_MYSQL_USER", "testuser");
            let password = env_or("TEST_MYSQL_PASSWORD", "testpass");
            let boot_db = env_or("TEST_MYSQL_DB", "testdb");
            let boot_url = format!("mysql://{user}:{password}@{host}:{port}/{boot_db}");
            let boot = MysqlDriver::connect(&boot_url)
                .unwrap_or_else(|e| panic!("mysql unreachable at {host}:{port} — {}", e.message));
            boot.exec_ddl(&[format!("CREATE DATABASE IF NOT EXISTS {MYSQL_DB_NAME}")])
                .expect("mysql database");
            drop(boot);
            let url = format!("mysql://{user}:{password}@{host}:{port}/{MYSQL_DB_NAME}");
            let d = MysqlDriver::connect(&url)
                .unwrap_or_else(|e| panic!("mysql ({MYSQL_DB_NAME}) unreachable — {}", e.message));
            d.exec_ddl(&str_list(schema, "drop")).expect("mysql drop");
            d.exec_ddl(&adapt_ddl_for_rust_decoder(
                &str_list(schema, "ddl"),
                "mysql",
            ))
            .expect("mysql ddl");
            seed_pooled(&d, schema, "mysql");
            LiveDriver::My(d)
        }
        other => panic!("unknown dialect {other}"),
    }
}

// ── standalone smoke (mirror orm-smoke.ts) ─────────────────────────────────────
fn smoke() {
    let art = load_artifact();
    let dialects = art.dialects();
    let drivers: Vec<(String, LiveDriver)> = dialects
        .iter()
        .map(|d| (d.clone(), make_driver(d, &art)))
        .collect();
    let mut pass = 0;
    let mut fail = 0;
    // rows_by_op[op_index] -> per-dialect string.
    let ops = art.ops();
    let mut rows: Vec<Vec<String>> = Vec::new();
    for op in ops {
        let op_id = op.get("id").and_then(|i| i.as_str()).unwrap();
        let mut cells: Vec<String> = Vec::new();
        for (d, drv) in &drivers {
            let plan = art.plan(op_id, d);
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_plan(drv.as_driver(), d, plan)
            }));
            match res {
                Ok(n) => {
                    cells.push(n.to_string());
                    pass += 1;
                }
                Err(e) => {
                    let msg = e
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| e.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "panic".into());
                    cells.push(format!("ERR: {}", msg.lines().next().unwrap_or("")));
                    fail += 1;
                }
            }
        }
        rows.push(cells);
    }
    println!("\n19 ORM ops x 3 DBs — rows/op (writes report statements executed) [rust]:\n");
    let pad = |s: &str, n: usize| format!("{s:<n$}");
    println!(
        "{} {} {} postgres",
        pad("op", 42),
        pad("sqlite", 14),
        pad("mysql", 14)
    );
    for (i, op) in ops.iter().enumerate() {
        let write = op
            .get("write")
            .and_then(|w| match w {
                J::Bool(b) => Some(*b),
                _ => None,
            })
            .unwrap_or(false);
        let label = op.get("label").and_then(|l| l.as_str()).unwrap();
        let tag = if write { "W " } else { "R " };
        println!(
            "{} {} {} {}",
            pad(&format!("{tag}{label}"), 42),
            pad(&rows[i][0], 14),
            pad(&rows[i][1], 14),
            rows[i][2]
        );
    }
    let total = pass + fail;
    println!(
        "\n{pass}/{total} cells green ({} ops x 3 DBs = {}).",
        ops.len(),
        ops.len() * 3
    );
    if fail > 0 {
        eprintln!("\nSMOKE FAILED: {fail} cell(s) errored (see ERR above).");
        std::process::exit(1);
    }
    println!("SMOKE PASS [rust]: all cells DB-backed on all 3 real DBs.");
}

// ── STANDALONE CSV bench (no protocol) ─────────────────────────────────────────
// ONE standalone process runs ALL 19 ops × 3 dialects, self-measures, and writes a FLAT CSV to
// benchmark/crosslang/.results/rust.csv. The collector (collect.ts) reads the CSVs → CROSS-LANG.md.
// CSV schema: language,case,dialect,metric,value   (RAW values only — collector owns the math).
fn now_ms() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        * 1000.0
}

fn env_num(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn bench() {
    let language = "rust";
    let warmup = env_num("BENCH_WARMUP", 50);
    let iters = env_num("BENCH_ITER", 300);
    let tp_iters = env_num("BENCH_TP_ITER", iters.min(2000));

    let spawned_at = now_ms();
    let art = load_artifact();
    let dialects = art.dialects();
    // cold = process start → runtime ready (binary start + artifact load), before any connect.
    let cold_ms = (now_ms() - spawned_at).max(0.0);

    let mut rows: Vec<String> = vec!["language,case,dialect,metric,value".to_string()];
    let mut emit = |case: &str, dialect: &str, metric: &str, value: String| {
        rows.push(format!(
            "{language},{case},{dialect},{metric},{}",
            csv_field(&value)
        ));
    };

    for dialect in &dialects {
        // A connection failure (make_driver panics) is an honest per-cell skip, never a stall.
        let drv =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| make_driver(dialect, &art)));
        let drv = match drv {
            Ok(d) => d,
            Err(e) => {
                let reason = e
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| e.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "panic".into());
                let first = reason.lines().next().unwrap_or("").to_string();
                for op in art.ops() {
                    let op_id = op.get("id").and_then(|i| i.as_str()).unwrap();
                    emit(
                        op_id,
                        dialect,
                        "skipped",
                        format!("{dialect} unreachable ({first})"),
                    );
                }
                continue;
            }
        };
        let d = drv.as_driver();
        for op in art.ops() {
            let case = op.get("id").and_then(|i| i.as_str()).unwrap();
            let plan = art.plan(case, dialect).clone();
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                // cost (fairness): queries/op from the plan shape; rows/op = executor's returned count.
                let queries = if plan.get("kind").and_then(|k| k.as_str()) == Some("read") {
                    plan.get("reads")
                        .and_then(|r| r.as_array())
                        .map_or(0, |a| a.len())
                        + plan
                            .get("relations")
                            .and_then(|r| r.as_array())
                            .map_or(0, |a| a.len())
                } else {
                    plan.get("statements")
                        .and_then(|s| s.as_array())
                        .map_or(0, |a| a.len())
                };
                let rows_count = run_plan(d, dialect, &plan);
                // latency: warmup, then one row PER timed iteration.
                for _ in 0..warmup {
                    run_plan(d, dialect, &plan);
                }
                let mut samples = Vec::with_capacity(iters);
                for _ in 0..iters {
                    let t0 = Instant::now();
                    run_plan(d, dialect, &plan);
                    samples.push(t0.elapsed().as_secs_f64() * 1000.0);
                }
                // throughput: a tight loop, raw elapsed + completed.
                let t0 = Instant::now();
                for _ in 0..tp_iters {
                    run_plan(d, dialect, &plan);
                }
                let tp_elapsed = t0.elapsed().as_secs_f64() * 1000.0;
                (queries, rows_count, samples, tp_elapsed)
            }));
            match res {
                Ok((queries, rows_count, samples, tp_elapsed)) => {
                    emit(case, dialect, "cost_queries", queries.to_string());
                    emit(case, dialect, "cost_rows", rows_count.to_string());
                    for s in samples {
                        emit(case, dialect, "latency_ms", s.to_string());
                    }
                    emit(
                        case,
                        dialect,
                        "throughput_elapsed_ms",
                        tp_elapsed.to_string(),
                    );
                    emit(case, dialect, "throughput_completed", tp_iters.to_string());
                }
                Err(e) => {
                    let reason = e
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| e.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "panic".into());
                    emit(
                        case,
                        dialect,
                        "skipped",
                        reason.lines().next().unwrap_or("").to_string(),
                    );
                }
            }
        }
    }

    emit("", "", "cold_ms", cold_ms.to_string());
    emit("", "", "rss_bytes", rss_bytes().to_string());
    emit("", "", "warmup", warmup.to_string());

    let here = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let results_dir = here.join("../../.results");
    std::fs::create_dir_all(&results_dir).expect("mkdir .results");
    let out = results_dir.join(format!("{language}.csv"));
    let mut body = rows.join("\n");
    body.push('\n');
    std::fs::write(&out, body).expect("write csv");
    eprintln!(
        "[{language}] wrote {} ({} rows)",
        out.display(),
        rows.len() - 1
    );
}

fn rss_bytes() -> u64 {
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

fn main() {
    let smoke_mode = std::env::args().any(|a| a == "--smoke");
    if smoke_mode {
        smoke();
    } else {
        bench();
    }
}
