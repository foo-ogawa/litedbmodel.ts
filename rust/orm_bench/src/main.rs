//! NATIVE-codegen ORM-bench cell (#129, epic #123) — self-measure the 19 ORM ops through the
//! litedbmodel-GENERATED native modules + companions + `litedbmodel_runtime` Driver, and print a flat
//! CSV (`cell,dialect,op,iter,us`) the TS collector aggregates. This binary is a litedbmodel-CONSUMER:
//! for each op it builds the typed input, obtains the runtime-backed handler from the GENERATED companion
//! (`companion_<op>::handler`), and calls the GENERATED native entry
//! (`generated_<op>::run_native_raw_struct_<Comp>`). It supplies NO `node_*` of its own and holds NO
//! hand-written exec seam — every DB access rides litedbmodel_runtime's op-agnostic Driver `exec`.
//!
//! Relations (nestedFind*/nestedRelations/compositeRelations) follow #131: the native module carries the
//! PRIMARY read only; the declared relation is loaded by the runtime batch loader `stitch_relation` over
//! the primary rows (1 batched child query per level → N+1-free). The consumer only decides WHICH nested
//! includes to resolve (the include-path walk over `childRelations`) — the batch/group/distribute SSoT
//! stays in the runtime.
//!
//! Writes MUTATE state, so each op re-seeds the canonical fixture (setup.json, emitted from the
//! orm-domain SSoT) before it runs, and the ops with a UNIQUE column vary that column per iteration.
//!
//! Usage: `orm_bench <dialect> <spec> [reps] [warmup]`  (spec = sqlite file path, or — with
//! `--features livedb` — `pg:<libpq-conn>` / `mysql:<url>`); or `orm_bench safety <dialect> <spec>`.

#[path = "gen/mod.rs"]
mod gen;

use litedbmodel_runtime::driver::PreparedStatement;
use litedbmodel_runtime::exec_context::TxConnection;
use litedbmodel_runtime::{stitch_relation, Driver, Node, SqlFailure, SqliteDriver, Value};
#[cfg(feature = "livedb")]
use litedbmodel_runtime::{MysqlDriver, PostgresDriver};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

// ── query counter (consumer-side observability, #129 safety proof) ──────────────────────────────
// A `CountingDriver` decorator over the runtime Driver increments on each `prepare` (one per statement
// the runtime issues). The N+1 proof: a batched relation runs 1 parent + 1 batched child = 2 (not 1+N);
// a batch write runs 1 statement (not N). The runtime + companion stay unchanged.
static QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);
struct CountingDriver {
    inner: Box<dyn Driver>,
}
impl Driver for CountingDriver {
    fn dialect(&self) -> &'static str {
        self.inner.dialect()
    }
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
        self.inner.prepare(sql)
    }
    fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        self.inner.begin_tx()
    }
    fn acquire_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        self.inner.acquire_tx()
    }
}

fn open_driver(spec: &str) -> Box<dyn Driver> {
    #[cfg(feature = "livedb")]
    {
        if let Some(conn) = spec.strip_prefix("pg:") {
            return Box::new(PostgresDriver::connect(conn).expect("connect postgres"));
        }
        if let Some(url) = spec.strip_prefix("mysql:") {
            return Box::new(MysqlDriver::connect(url).expect("connect mysql"));
        }
    }
    Box::new(SqliteDriver::open(spec).expect("open sqlite db"))
}

fn tx_dialect(spec: &str) -> litedbmodel_runtime::TxDialect {
    if spec.starts_with("pg:") {
        litedbmodel_runtime::TxDialect::Postgres
    } else if spec.starts_with("mysql:") {
        litedbmodel_runtime::TxDialect::Mysql
    } else {
        litedbmodel_runtime::TxDialect::Sqlite
    }
}

// ── setup: exec the param-free setup.json (drops → ddl → seed → pg seq fixup) for a dialect ──────
fn setup_dir(dialect: &str) -> String {
    // The committed sqlite gen/ has its setup baked at /tmp; every dialect's setup is emitted there by
    // codegen-build.ts. The bench always re-seeds from /tmp/ormbench/<dialect>/setup.json.
    format!("/tmp/ormbench/{dialect}/setup.json")
}
fn reseed(d: &dyn Driver, dialect: &str) {
    let raw = std::fs::read_to_string(setup_dir(dialect)).expect("read setup.json");
    let stmts = Node::parse(&raw).expect("parse setup.json");
    for s in stmts.as_array().expect("setup.json is an array") {
        let sql = s.as_str().expect("setup stmt is a string");
        d.prepare(sql).run(&[]).unwrap_or_else(|e| panic!("setup `{sql}`: {}", e.message));
    }
}

// ── relation include-path walk (consumer side): stitch the declared top relation, then recurse into
//    its childRelations (flatten this level's rows, stitch each child level) — one batched query per
//    level via the runtime `stitch_relation` SSoT. Returns the top-level stitched parents. ──────────
fn obj_get<'a>(row: &'a Value, key: &str) -> Option<&'a Value> {
    match row {
        Value::Obj(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
        _ => None,
    }
}
fn stitch_with_children(op_json: &Node, parents: Vec<Value>, d: &dyn Driver) -> Vec<Value> {
    let name = op_json.get("name").and_then(|n| n.as_str()).expect("relation op name").to_string();
    let stitched = stitch_relation(op_json, parents, d).unwrap_or_else(|e| panic!("stitch '{name}': {}", e.message()));
    if let Some(children) = op_json.get("childRelations").and_then(|c| c.as_array()) {
        let mut level: Vec<Value> = Vec::new();
        for p in &stitched {
            if let Some(Value::Arr(arr)) = obj_get(p, &name) {
                level.extend(arr.iter().cloned());
            }
        }
        for child in children {
            stitch_with_children(child, level.clone(), d);
        }
    }
    stitched
}

/// Build user parent rows (id/email/name) as `Value::Obj` for the relation loader (parentKey=`id`).
fn user_parents(rows: &[(i64, String, String)]) -> Vec<Value> {
    rows.iter()
        .map(|(id, email, name)| {
            Value::Obj(vec![
                ("id".into(), Value::Int(*id)),
                ("email".into(), Value::Str(email.clone())),
                ("name".into(), Value::Str(name.clone())),
            ])
        })
        .collect()
}
/// Composite tenant-user parent rows (tenant_id/user_id/name) — parentKeys=[tenant_id,user_id].
fn tenant_parents(rows: &[(i64, i64, String)]) -> Vec<Value> {
    rows.iter()
        .map(|(t, u, name)| {
            Value::Obj(vec![
                ("tenant_id".into(), Value::Int(*t)),
                ("user_id".into(), Value::Int(*u)),
                ("name".into(), Value::Str(name.clone())),
            ])
        })
        .collect()
}

fn rel_op(json: &str, name: &str) -> Node {
    let ops = Node::parse(json).expect("parse relation_ops_json");
    ops.get(name).expect("relation op present").clone()
}

// ── the 19 ORM ops (contract.ts order). Each closure runs ONE logical op for iteration `it`; mutating
//    ops vary their UNIQUE column by `it`. Fixed inputs mirror ops.ts (the SCP SSoT). ─────────────────
fn batch_emails(it: u64) -> Vec<String> {
    (0..10).map(|k| format!("many{it}_{k}@bench.com")).collect()
}
fn batch_names() -> Vec<String> {
    (0..10).map(|k| format!("Many {k}")).collect()
}

fn run_op(op: &str, it: u64, d: &dyn Driver, spec: &str) {
    use gen::*;
    match op {
        "findAll" => {
            let _ = generated_findAll::run_native_raw_struct_FindAll(&companion_findAll::handler(d), generated_findAll::InNRFindAll).unwrap();
        }
        "filterPaginateSort" => {
            let _ = generated_filterPaginateSort::run_native_raw_struct_FilterPaginateSort(&companion_filterPaginateSort::handler(d), generated_filterPaginateSort::InNRFilterPaginateSort { published: 1 }).unwrap();
        }
        "findFirst" => {
            let _ = generated_findFirst::run_native_raw_struct_FindFirst(&companion_findFirst::handler(d), generated_findFirst::InNRFindFirst { name: "User%".into() }).unwrap();
        }
        "findUnique" => {
            let _ = generated_findUnique::run_native_raw_struct_FindUnique(&companion_findUnique::handler(d), generated_findUnique::InNRFindUnique { email: "user500@example.com".into() }).unwrap();
        }
        "nestedFindAll" => {
            let rows = generated_nestedFindAll::run_native_raw_struct_FindAll(&companion_nestedFindAll::handler(d), generated_nestedFindAll::InNRFindAll).unwrap();
            let parents = user_parents(&rows.into_iter().map(|r| (r.id, r.email, r.name)).collect::<Vec<_>>());
            stitch_with_children(&rel_op(companion_nestedFindAll::relation_ops_json(), "posts"), parents, d);
        }
        "nestedFindFirst" => {
            let rows = generated_nestedFindFirst::run_native_raw_struct_FindFirst(&companion_nestedFindFirst::handler(d), generated_nestedFindFirst::InNRFindFirst { name: "User%".into() }).unwrap();
            let parents = user_parents(&rows.into_iter().map(|r| (r.id, r.email, r.name)).collect::<Vec<_>>());
            stitch_with_children(&rel_op(companion_nestedFindFirst::relation_ops_json(), "posts"), parents, d);
        }
        "nestedFindUnique" => {
            let rows = generated_nestedFindUnique::run_native_raw_struct_FindUnique(&companion_nestedFindUnique::handler(d), generated_nestedFindUnique::InNRFindUnique { email: "user1@example.com".into() }).unwrap();
            let parents = user_parents(&rows.into_iter().map(|r| (r.id, r.email, r.name)).collect::<Vec<_>>());
            stitch_with_children(&rel_op(companion_nestedFindUnique::relation_ops_json(), "posts"), parents, d);
        }
        "nestedRelations" => {
            let rows = generated_nestedRelations::run_native_raw_struct_FindAll(&companion_nestedRelations::handler(d), generated_nestedRelations::InNRFindAll).unwrap();
            let parents = user_parents(&rows.into_iter().map(|r| (r.id, r.email, r.name)).collect::<Vec<_>>());
            stitch_with_children(&rel_op(companion_nestedRelations::relation_ops_json(), "posts"), parents, d);
        }
        "compositeRelations" => {
            let rows = generated_compositeRelations::run_native_raw_struct_ByTenant(&companion_compositeRelations::handler(d), generated_compositeRelations::InNRByTenant { tenant_id: 1 }).unwrap();
            let parents = tenant_parents(&rows.into_iter().map(|r| (r.tenant_id, r.user_id, r.name)).collect::<Vec<_>>());
            stitch_with_children(&rel_op(companion_compositeRelations::relation_ops_json(), "posts"), parents, d);
        }
        "create" => {
            let _ = generated_create::run_native_raw_struct_Create(&companion_create::handler(d), generated_create::InNRCreate { email: format!("new{it}@bench.com"), name: "New".into() }).unwrap();
        }
        "update" => {
            let _ = generated_update::run_native_raw_struct_Update(&companion_update::handler(d), generated_update::InNRUpdate { id: 100, name: "Updated 100".into() }).unwrap();
        }
        "upsert" => {
            let _ = generated_upsert::run_native_raw_struct_Upsert(&companion_upsert::handler(d), generated_upsert::InNRUpsert { email: "user1@example.com".into(), name: "Upserted One".into() }).unwrap();
        }
        "createMany" => {
            let _ = generated_createMany::run_native_raw_struct_CreateMany(&companion_createMany::handler(d), generated_createMany::InNRCreateMany { emails: batch_emails(it), names: batch_names() }).unwrap();
        }
        "upsertMany" => {
            let mut emails: Vec<String> = vec!["user1@example.com".into(), "user2@example.com".into()];
            emails.extend((0..8).map(|k| format!("many{k}@bench.com")));
            let _ = generated_upsertMany::run_native_raw_struct_UpsertMany(&companion_upsertMany::handler(d), generated_upsertMany::InNRUpsertMany { emails, names: batch_names() }).unwrap();
        }
        "updateMany" => {
            let _ = generated_updateMany::run_native_raw_struct_UpdateMany(&companion_updateMany::handler(d), generated_updateMany::InNRUpdateMany { ids: (1..=10).collect(), names: batch_names() }).unwrap();
        }
        // ── tx ops: the native RETURNING chain runs through the companion `run_on` (BEGIN…COMMIT). ──
        "delete" => {
            let _ = companion_delete::run_on(litedbmodel_runtime::ConnSource::Driver(d), None, tx_dialect(spec), &litedbmodel_runtime::TransactionOptions::default(), || generated_delete::InNRDelete { email: format!("del{it}@bench.com"), name: "Del".into() });
        }
        "nestedCreate" => {
            let _ = companion_nestedCreate::run_on(litedbmodel_runtime::ConnSource::Driver(d), None, tx_dialect(spec), &litedbmodel_runtime::TransactionOptions::default(), || generated_nestedCreate::InNRNestedCreate { email: format!("nc{it}@bench.com"), name: "NC".into(), title: "NC Post".into() });
        }
        "nestedUpdate" => {
            let _ = companion_nestedUpdate::run_on(litedbmodel_runtime::ConnSource::Driver(d), None, tx_dialect(spec), &litedbmodel_runtime::TransactionOptions::default(), || generated_nestedUpdate::InNRNestedUpdate { user_id: 7, name: "NU".into(), title: "NU Post".into() });
        }
        "nestedUpsert" => {
            let _ = companion_nestedUpsert::run_on(litedbmodel_runtime::ConnSource::Driver(d), None, tx_dialect(spec), &litedbmodel_runtime::TransactionOptions::default(), || generated_nestedUpsert::InNRNestedUpsert { email: "user1@example.com".into(), name: "NUp".into(), title: "NUp Post".into() });
        }
        other => panic!("unknown op '{other}'"),
    }
}

const OPS: &[&str] = &[
    "findAll", "filterPaginateSort", "findFirst", "findUnique", "nestedFindAll", "nestedFindFirst",
    "nestedFindUnique", "create", "nestedCreate", "update", "nestedUpdate", "upsert", "nestedUpsert",
    "delete", "createMany", "upsertMany", "updateMany", "nestedRelations", "compositeRelations",
];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("safety") {
        let dialect = args.get(2).expect("safety <dialect> <spec>");
        let spec = args.get(3).expect("safety <dialect> <spec>");
        run_safety(dialect, spec);
        return;
    }
    let dialect = args.get(1).expect("usage: orm_bench <dialect> <spec> [reps] [warmup]").clone();
    let spec = args.get(2).expect("usage: orm_bench <dialect> <spec> [reps] [warmup]").clone();
    let reps: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(300);
    let warmup: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(30);

    let driver = open_driver(&spec);
    println!("cell,dialect,op,iter,us");
    for op in OPS {
        // Re-seed the canonical fixture before each op so reads see the seed state and writes start clean.
        reseed(driver.as_ref(), &dialect);
        for it in 0..warmup {
            run_op(op, it, driver.as_ref(), &spec);
        }
        for it in 0..reps {
            let g = it + warmup;
            let t = Instant::now();
            run_op(op, g, driver.as_ref(), &spec);
            let us = t.elapsed().as_micros();
            println!("native,{dialect},{op},{it},{us}");
        }
    }
}

// ── #129 safety proof: N+1-avoidance (query counts) via the CountingDriver. hardLimit + reader/writer
//    routing are proven by the dedicated companion entries (capped find, handler_routed) — see report. ─
fn run_safety(dialect: &str, spec: &str) {
    let counting = CountingDriver { inner: open_driver(spec) };
    let d: &dyn Driver = &counting;
    reseed(d, dialect);
    let count = |op: &str| {
        QUERY_COUNT.store(0, Ordering::SeqCst);
        run_op(op, 0, d, spec);
        QUERY_COUNT.load(Ordering::SeqCst)
    };
    // A single-level batched relation = 1 parent + 1 batched child = 2 (not 1+N).
    println!("nestedFindAll queries={} (expect 2: 1 parent + 1 batched child)", count("nestedFindAll"));
    println!("nestedFindUnique queries={} (expect 2)", count("nestedFindUnique"));
    // A 3-level chain = 1 per level = 3 (not N+1 per level).
    println!("nestedRelations queries={} (expect 3: users + posts + comments)", count("nestedRelations"));
    println!("compositeRelations queries={} (expect 3)", count("compositeRelations"));
    // A batch write = 1 statement for N records (not N).
    reseed(d, dialect);
    println!("createMany queries={} (expect 1: one batched INSERT for 10 records)", count("createMany"));
    reseed(d, dialect);
    println!("updateMany queries={} (expect 1)", count("updateMany"));
}
