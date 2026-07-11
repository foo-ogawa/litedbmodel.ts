//! litedbmodel cross-language adapter RUNNER — Rust (epic #44).
//!
//! Speaks the line-delimited JSON contract over stdin/stdout for the three Rust cells:
//! sql / codegen / ir.
//!
//!   sql     — hand-optimized raw SQL via the in-proc `SqliteDriver` (baseline 1.0×)
//!   codegen — the makeSQL bundle resident + integrity-verified ONCE at load, executed via the
//!             DEPENDED `litedbmodel_runtime` crate
//!   ir      — the bundle loaded FROM the generated JSON on disk, executed via the SAME runtime
//!
//! Consumes generated/bundles.json (the language-neutral §8 artifact) unchanged. Its compiled
//! release binary size is the Rust artifact-size metric.

use behavior_contracts::Value;
use litedbmodel_runtime::{
    decode_scope, execute_bundle, execute_transaction_bundle, read_bundle_pooled, Driver,
    PreparedStatement, RunInfo, SqliteDriver,
};
use litedbmodel_runtime::Scope;
use serde_json::Value as J;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::time::Instant;

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
fn run_lm(case: &J, d: &SqliteDriver) {
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

// A borrowing Sync view over an existing &SqliteDriver (single-threaded bench — sound, see above).
struct SyncDriverRef<'a>(&'a SqliteDriver);
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
        run_codegen(&art.cases[case_name], &counter);
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
        run_codegen(case, mock);
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


// ════════════════════════════════════════════════════════════════════════════
// TRUE-codegen cell (#44 anti-sham fix) — execute THROUGH the bc-GENERATED module.
// ════════════════════════════════════════════════════════════════════════════
//
// The OLD codegen path was a DECORATION: `run_micro`/`run_lm` called the SAME
// `execute_bundle`/`read_bundle_pooled` the `ir` cell calls, with only a cosmetic
// resident-bundle "verify" at load — so codegen was literally an alias of `ir`
// (and even measured slightly SLOWER: verify + noise). This module fixes that: it
// COMPILES the bc-GENERATED straight-line modules
// (`generated/codegen/rust/<case>.rs`, emitted by litedbmodel `generateCodegenArtifact`
// = bc#75 straight-line, de-interpreted native source — the portable IR is NOT
// embedded, only its fingerprint) and executes each read case THROUGH the module's
// `bind(handler).call(entry, input)` — a DISTINCT code entry from `ir`'s
// `execute_bundle`, with NO `run_behavior` tree-walk. It runs the REAL fail-closed
// skew gate (recompute `fingerprint_component_graph(live readGraph.ir)` == baked
// `IR_FINGERPRINT`) mirroring the generated module header + the TS codegen cell.
// The generated modules are declared at crate root (top-level `#[path]` resolves relative to
// `src/`) and re-exported under `codegen_gen` for tidy call sites. Each is its own compile unit
// (they share top-level names: `bind`/`Bound`/`run_<Component>`), so they cannot be concatenated.
#[path = "../../../generated/codegen/rust/find.rs"]
mod cg_find;
#[path = "../../../generated/codegen/rust/complexWhere.rs"]
mod cg_complex_where;
#[path = "../../../generated/codegen/rust/inList.rs"]
mod cg_in_list;
#[path = "../../../generated/codegen/rust/belongsTo.rs"]
mod cg_belongs_to;
#[path = "../../../generated/codegen/rust/hasMany.rs"]
mod cg_has_many;
#[path = "../../../generated/codegen/rust/hasManyLimit.rs"]
mod cg_has_many_limit;
#[path = "../../../generated/codegen/rust/batchInsert.rs"]
mod cg_batch_insert;
#[path = "../../../generated/codegen/rust/writeTxGate.rs"]
mod cg_write_tx_gate;


use litedbmodel_runtime::{render_read_primary, stitch_relation};

// ── RAW-ABI codegen handlers (bc#76 de-box) ────────────────────────────────────────────────
// The generated modules use bc's RAW ABI (RawComponentExec → RawValue), so the de-box engages
// END-TO-END: the generated `run_typed_raw_*` runner calls `exec_raw_ctx` and materializes the
// node's outType STRUCT directly from the returned RawValue via `marshal_raw_T*` — the dynamic
// `Value::Obj` tree the boxed path built + re-walked is never allocated on the row/entity data
// plane. These handlers produce the RawValue at the makeSQL seam. (The driver returns rows as
// `Value` at the SQL boundary; `raw_from_value` is bc's documented seam adapter — a real wire
// consumer builds RawValue from its wire payload directly, off the hot path.)
use behavior_contracts::{RawComponentExec, RawOutcome, RawValue, raw_from_value};

// READ raw handler: render the primary read node against the evaluated `__scope`, run REAL SQL, and
// return the row list as a native `RawValue::Arr(RawValue::Row(..))`. bc's generated raw runner
// de-boxes each row straight into the concrete `T*` struct (no Value::Obj on the row data plane).
struct CodegenRawReadHandler<'a> {
    read_graph: &'a J,
    driver: &'a dyn Driver,
}
impl<'a> RawComponentExec for CodegenRawReadHandler<'a> {
    fn exec_raw(&mut self, _c: &str, _p: &[(String, Value)], _b: Option<&Value>) -> Option<RawOutcome> {
        // The single-node read graph always dispatches through exec_raw_ctx (needs the scope port).
        self.exec_raw_ctx("", _c, _p, _b)
    }
    fn exec_raw_ctx(
        &mut self,
        _node_id: &str,
        _component: &str,
        ports: &[(String, Value)],
        _bound: Option<&Value>,
    ) -> Option<RawOutcome> {
        let scope: Scope = match ports.iter().find(|(k, _)| k == "__scope").map(|(_, v)| v) {
            Some(Value::Obj(pairs)) => pairs.clone(),
            _ => return Some(RawOutcome::Error("codegen: __scope did not evaluate to an object".into())),
        };
        let rendered = match render_read_primary(self.read_graph, &scope) {
            Ok(r) => r,
            Err(e) => return Some(RawOutcome::Error(e)),
        };
        // to_driver_param equivalent: Obj emit-payload -> compact JSON, else pass through.
        let params: Vec<Value> = rendered
            .params
            .iter()
            .map(|v| match v {
                Value::Obj(_) => Value::Str(serde_json::to_string(&encode_value(v)).unwrap()),
                other => other.clone(),
            })
            .collect();
        let mut stmt = self.driver.prepare(&rendered.sql);
        match stmt.all(&params) {
            // Build the RawValue::Arr(Row..) at the wire seam — bc's raw runner de-boxes it.
            Ok(rows) => Some(RawOutcome::Ok(RawValue::Arr(rows.iter().map(raw_from_value).collect()))),
            Err(e) => Some(RawOutcome::Error(e.message)),
        }
    }
}
use litedbmodel_runtime::encode_value;

// WRITE raw handler: the generated write module's single `__makeSqlNode` boundary IS the whole
// write — its outType is the TransactionResult typed shape (obj{committed,executed,shortCircuit,
// entity,returnedRows}). We run the derived transaction plan via the shared runtime
// `execute_transaction_bundle` (gate-first, byte-parity with the thin runtime) and return the
// TransactionResult as a native `RawValue::Row`; bc's generated `marshal_raw_T0` de-boxes it into
// the concrete result struct (T1 shortCircuit, T2 entity/returnedRows rows) — NO Value::Obj on the
// entity/returnedRows data plane. The bundle rides the handler so the seam has the plan + dialect.
struct CodegenRawWriteHandler<'a> {
    bundle: &'a J,
    input: &'a J,
    driver: &'a dyn Driver,
}
impl<'a> RawComponentExec for CodegenRawWriteHandler<'a> {
    fn exec_raw(&mut self, _c: &str, _p: &[(String, Value)], _b: Option<&Value>) -> Option<RawOutcome> {
        match execute_transaction_bundle(self.bundle, self.input, self.driver) {
            // The thin runtime OMITS an absent optional field (`shortCircuit` on commit, `returnedRows`
            // with no returned rows); the typed shape's `opt<..>` de-box expects the KEY PRESENT (value
            // Null). Normalize to the full 5-key TransactionResult shape here (present-as-null) — the
            // seam adapter presents the wire shape the typed contract declares, without touching the
            // shared runtime's observed Value (whose absent-key form the mode-2 conformance pins).
            Ok(result) => Some(RawOutcome::Ok(normalize_tx_result_raw(&result))),
            Err(e) => Some(RawOutcome::Error(e.message)),
        }
    }
}

/// Present the TransactionResult `Value` as a full-5-key `RawValue::Row` (present-as-null for an
/// absent optional field), so the generated write module's `opt<..>` de-box marshaller finds every
/// key. Rows/scalars ride `raw_from_value`; only the top-level optional presence is normalized.
fn normalize_tx_result_raw(result: &Value) -> RawValue {
    match normalize_tx_result_value(result) {
        Value::Obj(pairs) => {
            let mut row = behavior_contracts::RawRow::new();
            for (k, v) in pairs {
                row.set(k, raw_from_value(&v));
            }
            RawValue::Row(row)
        }
        other => raw_from_value(&other),
    }
}

/// The input scope for a WRITE module's `run_typed_raw_*` runner. bc's `makeSqlComponentIR` node
/// evaluates `sql`/`params`/`skip` ports from `__sql`/`__sqlParams`/`__skip` bindings (the boxed read
/// path's convention), so those heads MUST be present or `ref_native` fail-closes (UNKNOWN_BINDING).
/// The generated write runner passes them to the handler as ports, but our raw write handler ignores
/// them (it drives the transaction plan from the bundle directly), so present-as-empty is exact — the
/// values are never read. This is the makeSQL surrogate input, not a fabricated default.
fn write_module_input(bundle: &J) -> Scope {
    let sql = bundle
        .get("statement")
        .and_then(|s| s.get("sql"))
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    vec![
        ("__sql".to_string(), Value::Str(sql)),
        ("__sqlParams".to_string(), Value::Arr(Vec::new())),
        ("__skip".to_string(), Value::Bool(false)),
    ]
}

/// The full-5-key TransactionResult `Value` (present-as-null for an absent optional field) — the
/// canonical shape the de-boxed codegen path emits (`ser_T0`). Used to present the interpreter
/// reference in the SAME shape for the behaviour-equality selfcheck (representation-only normalize).
// The canonical TransactionResult shape the de-boxed `ser_T0` emits: always committed / executed /
// shortCircuit / entity (present-as-null for an absent optional), PLUS returnedRows ONLY when the
// runtime actually produced it (a batch-with-RETURNING — matching `deriveWriteOutputType`, which
// includes the returnedRows field in the type ONLY for that shape). The runtime OMITS an absent
// optional key; we present the full shape without touching the row DATA (representation-only).
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

// REAL fail-closed skew gate (bc#75 straight-line): recompute the fingerprint of the LIVE
// component-graph IR the runtime would execute and assert it equals the module's baked
// IR_FINGERPRINT. For reads the live IR is `readGraph.ir`; for writes the runtime does not
// surface the portable IR, so we compare against the case-artifact fingerprint that the
// generator computed from the SAME bundle (a real generated-const vs live comparison).
fn codegen_skew_gate(case: &J, baked_fp: &str) {
    let live_fp: String = if let Some(rg) = case["bundle"].get("readGraph").filter(|g| !g.is_null()) {
        behavior_contracts::fingerprint_component_graph(&rg["ir"])
            .expect("codegen: fingerprint live readGraph.ir")
    } else {
        case["fingerprint"].as_str().unwrap().to_string()
    };
    if live_fp != baked_fp {
        panic!("codegen: generated {} fingerprint mismatch ({live_fp} != {baked_fp}) — regenerate (fail-closed)",
            case["case"].as_str().unwrap());
    }
}

// Execute ONE case THROUGH the generated de-interpreted module. Reads/relations run the
// generated `bind(handler).call(entry, input)`; the companion relation is hydrated via the
// shared runtime `stitch_relation` (same grouping SSoT as `read_bundle_pooled`). Writes force
// the generated module's fail-closed load, then defer execution to the runtime tx path.

fn rows_count(v: &serde_json::Value) -> usize {
    v.as_array().map(|a| a.len()).unwrap_or(0)
}

// Value-returning variants for the behaviour-equality selfcheck (encode bc Value -> JSON).
fn run_codegen_value(case: &J, driver: &dyn Driver) -> serde_json::Value {
    let case_name = case["case"].as_str().unwrap();
    let kind = case["kind"].as_str().unwrap();
    let input = &case["input"];
    // Reads: drive the generated module's RAW-ABI de-boxed runner (`bind_raw().call`) — the row
    // structs materialize directly from the RawValue the handler returns (no Value::Obj re-walk).
    macro_rules! read_val { ($m:ident,$e:literal)=>{{
        codegen_skew_gate(case, $m::IR_FINGERPRINT);
        let rg=&case["bundle"]["readGraph"];
        let h=CodegenRawReadHandler{read_graph:rg,driver};
        let mut b=$m::bind_raw(h);
        let sc:Scope=decode_scope(input).unwrap();
        b.call($e,&sc).unwrap()
    }};}
    // Writes: drive the generated module's RAW-ABI runner too — the TransactionResult de-boxes into
    // the concrete result struct from the RawValue the write handler returns (no Value::Obj plane).
    macro_rules! write_val { ($m:ident,$e:literal,$in:expr)=>{{
        codegen_skew_gate(case, $m::IR_FINGERPRINT);
        let h=CodegenRawWriteHandler{bundle:&case["bundle"],input:$in,driver};
        let mut b=$m::bind_raw(h);
        let msi=write_module_input(&case["bundle"]);
        b.call($e, &msi).unwrap()
    }};}
    let empty_in = J::Object(Default::default());
    let out: Value = match case_name {
        "find" => read_val!(cg_find,"Find"),
        "complexWhere" => read_val!(cg_complex_where,"ComplexWhere"),
        "inList" => read_val!(cg_in_list,"ByIds"),
        "belongsTo"|"hasMany"|"hasManyLimit" => {
            let parents = match case_name {
                "belongsTo"=>read_val!(cg_belongs_to,"Posts"),
                "hasMany"=>read_val!(cg_has_many,"Posts"),
                _=>read_val!(cg_has_many_limit,"Posts"),
            };
            let rows = match parents { Value::Arr(r)=>r, _=>vec![] };
            let with=case["withRelation"].as_str().unwrap();
            let op=&case["bundle"]["relations"][with];
            Value::Arr(stitch_relation(op, rows, driver).unwrap())
        }
        "batchInsert" => write_val!(cg_batch_insert,"BatchInsert",&empty_in),
        "writeTxGate" => write_val!(cg_write_tx_gate,"Create",input),
        _=>Value::Null,
    };
    encode_value(&out)
}

fn run_lm_value(case: &J, driver: &dyn Driver) -> serde_json::Value {
    let bundle=&case["bundle"]; let kind=case["kind"].as_str().unwrap(); let input=&case["input"];
    // For the WRITE (batch/tx) selfcheck, present the interpreter's TransactionResult in the SAME
    // canonical full-5-key shape the de-boxed codegen path emits (`ser_T0` always emits every field,
    // shortCircuit/returnedRows present-as-null). The runtime OMITS an absent optional key, so without
    // this the behaviour-equality selfcheck would flag a representation-only diff. The row DATA is
    // identical; only the top-level optional-key presence is normalized (via raw_from_value round-trip).
    let out: Value = match kind {
        "batch" => normalize_tx_result_value(&execute_transaction_bundle(bundle, &J::Object(Default::default()), driver).unwrap()),
        "tx" => normalize_tx_result_value(&execute_transaction_bundle(bundle, input, driver).unwrap()),
        "relation" => {
            let with=case["withRelation"].as_str().unwrap();
            let op=&bundle["relations"][with];
            let base=execute_bundle(bundle, input, driver).unwrap();
            let rows=match base{Value::Arr(r)=>r,_=>vec![]};
            Value::Arr(stitch_relation(op, rows, driver).unwrap())
        }
        _ => execute_bundle(bundle, input, driver).unwrap(),
    };
    encode_value(&out)
}

fn run_codegen(case: &J, driver: &dyn Driver) {
    let case_name = case["case"].as_str().unwrap();
    let kind = case["kind"].as_str().unwrap();
    let input = &case["input"];
    // Reads via the generated RAW-ABI de-boxed runner (bind_raw): row structs materialize directly
    // from the RawValue the handler returns — the de-box the boxed path only layered, gone.
    macro_rules! read_via {
        ($m:ident, $entry:literal) => {{
            codegen_skew_gate(case, $m::IR_FINGERPRINT);
            let read_graph = &case["bundle"]["readGraph"];
            let handler = CodegenRawReadHandler { read_graph, driver };
            let mut bound = $m::bind_raw(handler);
            let scope: Scope = decode_scope(input).expect("codegen: decode input");
            bound.call($entry, &scope).expect("codegen: generated call")
        }};
    }
    match case_name {
        "find" => { let _ = read_via!(cg_find, "Find"); }
        "complexWhere" => { let _ = read_via!(cg_complex_where, "ComplexWhere"); }
        "inList" => { let _ = read_via!(cg_in_list, "ByIds"); }
        "belongsTo" | "hasMany" | "hasManyLimit" => {
            let parents = match kind {
                _ => match case_name {
                    "belongsTo" => read_via!(cg_belongs_to, "Posts"),
                    "hasMany" => read_via!(cg_has_many, "Posts"),
                    "hasManyLimit" => read_via!(cg_has_many_limit, "Posts"),
                    _ => unreachable!(),
                },
            };
            let rows = match parents { Value::Arr(r) => r, _ => Vec::new() };
            let with_name = case["withRelation"].as_str().unwrap();
            let op = &case["bundle"]["relations"][with_name];
            let _ = stitch_relation(op, rows, driver).expect("codegen: stitch_relation");
        }
        // Writes drive the generated RAW-ABI runner too: the TransactionResult de-boxes into the
        // concrete result struct from the RawValue the write handler returns (no Value::Obj plane).
        "batchInsert" => {
            codegen_skew_gate(case, cg_batch_insert::IR_FINGERPRINT);
            let empty_in = J::Object(Default::default());
            let handler = CodegenRawWriteHandler { bundle: &case["bundle"], input: &empty_in, driver };
            let mut bound = cg_batch_insert::bind_raw(handler);
            bound.call("BatchInsert", &write_module_input(&case["bundle"])).expect("codegen: generated write call");
        }
        "writeTxGate" => {
            codegen_skew_gate(case, cg_write_tx_gate::IR_FINGERPRINT);
            let handler = CodegenRawWriteHandler { bundle: &case["bundle"], input, driver };
            let mut bound = cg_write_tx_gate::bind_raw(handler);
            bound.call("Create", &write_module_input(&case["bundle"])).expect("codegen: generated write call");
        }
        _ => panic!("unknown codegen case {case_name}"),
    }
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

    // codegen: run each generated module's REAL fail-closed skew gate + fail-closed load once at
    // cold start — recompute fingerprint_component_graph(live readGraph.ir) == baked IR_FINGERPRINT
    // and force the module's spec-version MODULE_CHECK via bind(). This is the GENUINE codegen
    // load cost (the old decorative `serde_json::to_string(bundle)` "verify" is deleted); the timed
    // ops below then execute THROUGH the generated de-interpreted code, not `execute_bundle`.
    if impl_ == "codegen" {
        let mock = MockDriver;
        for (_name, c) in &art.cases {
            // Exercises the skew gate + module load; the read cases also warm the generated call path.
            run_codegen(c, &mock);
        }
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

// The Rust bench adapter drives the in-proc SqliteDriver for the DB-backed axis; the
// live PG/MySQL wiring (behind the runtime's async `livedb` cargo feature — tokio +
// deadpool/sqlx) is NOT wired into this bench adapter in this pass, so PG/MySQL
// DB-backed is reported as an explicit per-cell skip (never silently dropped). The
// per-dialect MICRO axis IS covered (the mock renders each dialect's bundle).
fn db_skip_reason(dialect: &str) -> Option<String> {
    match dialect {
        "sqlite" => None,
        other => Some(format!(
            "rust bench adapter drives in-proc SqliteDriver only; live {other} (runtime `livedb` async feature: tokio+deadpool/sqlx) not wired into this bench adapter"
        )),
    }
}

fn handle(kind: &str, req: &J, impl_: &str, art: &Artifact) {
    match kind {
        "run" => {
            let case = req["case"].as_str().unwrap();
            let dialect = req["dialect"].as_str().unwrap_or("sqlite");
            if let Some(reason) = db_skip_reason(dialect) {
                write_line(
                    &serde_json::json!({"kind":"skipped","case":case,"dialect":dialect,"reason":reason}),
                );
                return;
            }
            let warmup = req["warmup"].as_u64().unwrap() as usize;
            let iters = req["iterations"].as_u64().unwrap() as usize;
            let d = seed_driver(art);
            let cjson = art.cases[case].clone();
            let samples = collect(warmup, iters, || {
                if impl_ == "sql" {
                    run_sql(case, &d);
                } else if impl_ == "codegen" {
                    run_codegen(&cjson, &d);
                } else {
                    run_lm(&cjson, &d);
                }
            });
            write_line(
                &serde_json::json!({"kind":"run","case":case,"dialect":dialect,"samplesMs":samples}),
            );
        }
        "throughput" => {
            let case = req["case"].as_str().unwrap();
            let dialect = req["dialect"].as_str().unwrap_or("sqlite");
            if let Some(reason) = db_skip_reason(dialect) {
                write_line(
                    &serde_json::json!({"kind":"skipped","case":case,"dialect":dialect,"reason":reason}),
                );
                return;
            }
            let iters = req["iterations"].as_u64().unwrap() as usize;
            let d = seed_driver(art);
            let cjson = art.cases[case].clone();
            let t0 = Instant::now();
            for _ in 0..iters {
                if impl_ == "sql" {
                    run_sql(case, &d);
                } else if impl_ == "codegen" {
                    run_codegen(&cjson, &d);
                } else {
                    run_lm(&cjson, &d);
                }
            }
            let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
            write_line(
                &serde_json::json!({"kind":"throughput","case":case,"dialect":dialect,"elapsedMs":elapsed,"completed":iters}),
            );
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
            // Behaviour-equality selfcheck: generated-code output == interpreter output (same rows).
            let case = req["case"].as_str().unwrap();
            let cjson = art.cases[case].clone();
            let kind = cjson["kind"].as_str().unwrap();
            let d1 = seed_driver(art);
            let cg = run_codegen_value(&cjson, &d1);
            let d2 = seed_driver(art);
            let ir = run_lm_value(&cjson, &d2);
            let equal = cg == ir;
            write_line(&serde_json::json!({"kind":"verify","case":case,"impl_kind":kind,"equal":equal,
                "cg_rows": rows_count(&cg), "ir_rows": rows_count(&ir)}));
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
