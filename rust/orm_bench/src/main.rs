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
use litedbmodel_runtime::{
    encode_value, execute_bundle, Driver, Node, SqlFailure, SqliteDriver, Value,
};
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
        d.prepare(sql)
            .run(&[])
            .unwrap_or_else(|e| panic!("setup `{sql}`: {}", e.message));
    }
}

// ── the 19 ORM ops (contract.ts order). Each closure runs ONE logical op for iteration `it`; mutating
//    ops vary their UNIQUE column by `it`. Fixed inputs mirror ops.ts (the SCP SSoT). ─────────────────
fn batch_emails(it: u64) -> Vec<String> {
    (0..10).map(|k| format!("many{it}_{k}@bench.com")).collect()
}
fn batch_names() -> Vec<String> {
    (0..10).map(|k| format!("Many {k}")).collect()
}

// ── SETUP/TIMED SEPARATION (#138): `prepare_op` builds — ONCE per op, OUTSIDE the timed loop — the
//    closure the timed region calls. All per-op SETUP that is NOT the op itself (relation metadata
//    `Node::parse`, which is STATIC, and any input list construction that does not vary the op's DB work)
//    is hoisted HERE so the timed region measures ONLY the op's real execution — never a JSON re-parse or
//    other harness scaffolding (which would be pure measurement pollution, asymmetric with the SDK cell).
//    The returned closure runs ONE logical op for iteration `it`; mutating ops vary their UNIQUE column
//    by `it`. Fixed inputs mirror ops.ts (the SCP SSoT).
fn prepare_op<'a>(op: &str, d: &'a dyn Driver, spec: &str) -> Box<dyn FnMut(u64) + 'a> {
    use gen::*;
    let txd = tx_dialect(spec);
    match op {
        "findAll" => Box::new(move |_it| {
            let _ = generated_findAll::run_native_raw_struct_FindAll(
                &companion_findAll::handler(d),
                generated_findAll::InNRFindAll,
            )
            .unwrap();
        }),
        "filterPaginateSort" => Box::new(move |_it| {
            // `published` is a BOOLEAN column on postgres (native port type `bool`) but INTEGER on
            // sqlite/mysql (`i64`) — the ONE dialect-typed input in the 19 ops. The per-dialect gen is
            // swapped in per build, so select the matching literal at compile time via the `pg` feature.
            #[cfg(feature = "pg")]
            let published = true;
            #[cfg(not(feature = "pg"))]
            let published = 1;
            let _ = generated_filterPaginateSort::run_native_raw_struct_FilterPaginateSort(
                &companion_filterPaginateSort::handler(d),
                generated_filterPaginateSort::InNRFilterPaginateSort { published },
            )
            .unwrap();
        }),
        "findFirst" => Box::new(move |_it| {
            let _ = generated_findFirst::run_native_raw_struct_FindFirst(
                &companion_findFirst::handler(d),
                generated_findFirst::InNRFindFirst {
                    name: "User%".into(),
                },
            )
            .unwrap();
        }),
        "findUnique" => Box::new(move |_it| {
            let _ = generated_findUnique::run_native_raw_struct_FindUnique(
                &companion_findUnique::handler(d),
                generated_findUnique::InNRFindUnique {
                    email: "user500@example.com".into(),
                },
            )
            .unwrap();
        }),
        // Relation ops: the timed cell is TWO lines — (1) the generated TYPED native read, (2) the
        // litedbmodel TYPED lazy-load (`hydrate_relation`) with a natural key accessor (`|r| r.id`). ALL
        // orchestration (batched child read, group, multi-level stitch) is INSIDE litedbmodel; the bench
        // reconstructs NO `Value::Obj` and parses NO metadata per iteration (the relation op is parsed
        // ONCE here in setup). SDK cell keeps its hand orchestration (the comparison baseline).
        "nestedFindAll" => {
            let posts = litedbmodel_runtime::relation_op(
                companion_nestedFindAll::relation_ops_json(),
                "posts",
            )
            .unwrap();
            Box::new(move |_it| {
                let users = generated_nestedFindAll::run_native_raw_struct_FindAll(
                    &companion_nestedFindAll::handler(d),
                    generated_nestedFindAll::InNRFindAll,
                )
                .unwrap();
                let _ = litedbmodel_runtime::hydrate_relation(&posts, users, |r| r.id, d).unwrap();
            })
        }
        "nestedFindFirst" => {
            let posts = litedbmodel_runtime::relation_op(
                companion_nestedFindFirst::relation_ops_json(),
                "posts",
            )
            .unwrap();
            Box::new(move |_it| {
                let users = generated_nestedFindFirst::run_native_raw_struct_FindFirst(
                    &companion_nestedFindFirst::handler(d),
                    generated_nestedFindFirst::InNRFindFirst {
                        name: "User%".into(),
                    },
                )
                .unwrap();
                let _ = litedbmodel_runtime::hydrate_relation(&posts, users, |r| r.id, d).unwrap();
            })
        }
        "nestedFindUnique" => {
            let posts = litedbmodel_runtime::relation_op(
                companion_nestedFindUnique::relation_ops_json(),
                "posts",
            )
            .unwrap();
            Box::new(move |_it| {
                let users = generated_nestedFindUnique::run_native_raw_struct_FindUnique(
                    &companion_nestedFindUnique::handler(d),
                    generated_nestedFindUnique::InNRFindUnique {
                        email: "user1@example.com".into(),
                    },
                )
                .unwrap();
                let _ = litedbmodel_runtime::hydrate_relation(&posts, users, |r| r.id, d).unwrap();
            })
        }
        "nestedRelations" => {
            let posts = litedbmodel_runtime::relation_op(
                companion_nestedRelations::relation_ops_json(),
                "posts",
            )
            .unwrap();
            Box::new(move |_it| {
                let users = generated_nestedRelations::run_native_raw_struct_FindAll(
                    &companion_nestedRelations::handler(d),
                    generated_nestedRelations::InNRFindAll,
                )
                .unwrap();
                let _ = litedbmodel_runtime::hydrate_relation(&posts, users, |r| r.id, d).unwrap();
            })
        }
        "compositeRelations" => {
            let posts = litedbmodel_runtime::relation_op(
                companion_compositeRelations::relation_ops_json(),
                "posts",
            )
            .unwrap();
            Box::new(move |_it| {
                let rows = generated_compositeRelations::run_native_raw_struct_ByTenant(
                    &companion_compositeRelations::handler(d),
                    generated_compositeRelations::InNRByTenant { tenant_id: 1 },
                )
                .unwrap();
                let _ = litedbmodel_runtime::hydrate_relation(
                    &posts,
                    rows,
                    |r| (r.tenant_id, r.user_id),
                    d,
                )
                .unwrap();
            })
        }
        "create" => Box::new(move |it| {
            let _ = generated_create::run_native_raw_struct_Create(
                &companion_create::handler(d),
                generated_create::InNRCreate {
                    email: format!("new{it}@bench.com"),
                    name: "New".into(),
                },
            )
            .unwrap();
        }),
        "update" => Box::new(move |_it| {
            let _ = generated_update::run_native_raw_struct_Update(
                &companion_update::handler(d),
                generated_update::InNRUpdate {
                    id: 100,
                    name: "Updated 100".into(),
                },
            )
            .unwrap();
        }),
        "upsert" => Box::new(move |_it| {
            let _ = generated_upsert::run_native_raw_struct_Upsert(
                &companion_upsert::handler(d),
                generated_upsert::InNRUpsert {
                    email: "user1@example.com".into(),
                    name: "Upserted One".into(),
                },
            )
            .unwrap();
        }),
        "createMany" => Box::new(move |it| {
            let _ = generated_createMany::run_native_raw_struct_CreateMany(
                &companion_createMany::handler(d),
                generated_createMany::InNRCreateMany {
                    emails: batch_emails(it),
                    names: batch_names(),
                },
            )
            .unwrap();
        }),
        "upsertMany" => Box::new(move |_it| {
            let mut emails: Vec<String> =
                vec!["user1@example.com".into(), "user2@example.com".into()];
            emails.extend((0..8).map(|k| format!("many{k}@bench.com")));
            let _ = generated_upsertMany::run_native_raw_struct_UpsertMany(
                &companion_upsertMany::handler(d),
                generated_upsertMany::InNRUpsertMany {
                    emails,
                    names: batch_names(),
                },
            )
            .unwrap();
        }),
        "updateMany" => Box::new(move |_it| {
            let _ = generated_updateMany::run_native_raw_struct_UpdateMany(
                &companion_updateMany::handler(d),
                generated_updateMany::InNRUpdateMany {
                    ids: (1..=10).collect(),
                    names: batch_names(),
                },
            )
            .unwrap();
        }),
        // ── tx ops: the native RETURNING chain runs through the companion `run_on` (BEGIN…COMMIT). ──
        "delete" => Box::new(move |it| {
            let _ = companion_delete::run_on(
                litedbmodel_runtime::ConnSource::Driver(d),
                None,
                txd,
                &litedbmodel_runtime::TransactionOptions::default(),
                || generated_delete::InNRDelete {
                    email: format!("del{it}@bench.com"),
                    name: "Del".into(),
                },
            );
        }),
        "nestedCreate" => Box::new(move |it| {
            let _ = companion_nestedCreate::run_on(
                litedbmodel_runtime::ConnSource::Driver(d),
                None,
                txd,
                &litedbmodel_runtime::TransactionOptions::default(),
                || generated_nestedCreate::InNRNestedCreate {
                    email: format!("nc{it}@bench.com"),
                    name: "NC".into(),
                    title: "NC Post".into(),
                },
            );
        }),
        "nestedUpdate" => Box::new(move |_it| {
            let _ = companion_nestedUpdate::run_on(
                litedbmodel_runtime::ConnSource::Driver(d),
                None,
                txd,
                &litedbmodel_runtime::TransactionOptions::default(),
                || generated_nestedUpdate::InNRNestedUpdate {
                    user_id: 7,
                    name: "NU".into(),
                    title: "NU Post".into(),
                },
            );
        }),
        "nestedUpsert" => Box::new(move |_it| {
            let _ = companion_nestedUpsert::run_on(
                litedbmodel_runtime::ConnSource::Driver(d),
                None,
                txd,
                &litedbmodel_runtime::TransactionOptions::default(),
                || generated_nestedUpsert::InNRNestedUpsert {
                    email: "user1@example.com".into(),
                    name: "NUp".into(),
                    title: "NUp Post".into(),
                },
            );
        }),
        other => panic!("unknown op '{other}'"),
    }
}

const OPS: &[&str] = &[
    "findAll",
    "filterPaginateSort",
    "findFirst",
    "findUnique",
    "nestedFindAll",
    "nestedFindFirst",
    "nestedFindUnique",
    "create",
    "nestedCreate",
    "update",
    "nestedUpdate",
    "upsert",
    "nestedUpsert",
    "delete",
    "createMany",
    "upsertMany",
    "updateMany",
    "nestedRelations",
    "compositeRelations",
];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("safety") {
        let dialect = args.get(2).expect("safety <dialect> <spec>");
        let spec = args.get(3).expect("safety <dialect> <spec>");
        run_safety(dialect, spec);
        return;
    }
    if args.get(1).map(String::as_str) == Some("verify") {
        let dialect = args.get(2).expect("verify <dialect> <spec>");
        let spec = args.get(3).expect("verify <dialect> <spec>");
        run_verify(dialect, spec);
        return;
    }
    let dialect = args
        .get(1)
        .expect("usage: orm_bench <dialect> <spec> [reps] [warmup]")
        .clone();
    let spec = args
        .get(2)
        .expect("usage: orm_bench <dialect> <spec> [reps] [warmup]")
        .clone();
    let reps: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(300);
    let warmup: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(30);

    let driver = open_driver(&spec);
    println!("cell,dialect,op,iter,us");
    for op in OPS {
        // Re-seed the canonical fixture before each op so reads see the seed state and writes start clean.
        reseed(driver.as_ref(), &dialect);
        // SETUP (outside the timed loop): build the op closure ONCE — relation metadata is parsed here,
        // never per iteration. The timed region below is then EXACTLY the op call, nothing else.
        let mut run = prepare_op(op, driver.as_ref(), &spec);
        for it in 0..warmup {
            run(it);
        }
        for it in 0..reps {
            let g = it + warmup;
            let t = Instant::now();
            run(g);
            let us = t.elapsed().as_micros();
            println!("native,{dialect},{op},{it},{us}");
        }
    }
}

// ── #129 decoder correctness: filterPaginateSort projects created_at (TIMESTAMP) + published
//    (BOOLEAN/TINYINT) — the columns the a6012dc read-decoder arms cover. Prove the NATIVE de-boxed
//    output ≡ the mode-2 INTERPRETER (`execute_bundle`) output BYTE-FOR-BYTE on the same live DB, using
//    the SAME `encode_value` serializer (the #124 native-vs-mode-2 oracle framework). Both paths read
//    the SAME driver Value (the shared decoder), so equality proves the decoder is a genuine runtime fix
//    (date→canonical string, bool→Int/bool), not a bench-only fudge. ────────────────────────────────
fn run_verify(dialect: &str, spec: &str) {
    let driver = open_driver(spec);
    let d: &dyn Driver = driver.as_ref();
    reseed(d, dialect);

    // NATIVE: run the generated filterPaginateSort, re-encode the typed rows through encode_value.
    #[cfg(feature = "pg")]
    let published = true;
    #[cfg(not(feature = "pg"))]
    let published = 1;
    let rows = gen::generated_filterPaginateSort::run_native_raw_struct_FilterPaginateSort(
        &gen::companion_filterPaginateSort::handler(d),
        gen::generated_filterPaginateSort::InNRFilterPaginateSort { published },
    )
    .expect("native filterPaginateSort");
    let native_arr: Vec<Value> = rows
        .iter()
        .map(|r| {
            #[cfg(feature = "pg")]
            let pub_val = Value::Bool(r.published);
            #[cfg(not(feature = "pg"))]
            let pub_val = Value::Int(r.published);
            Value::Obj(vec![
                ("id".into(), Value::Int(r.id)),
                ("title".into(), Value::Str(r.title.clone())),
                ("content".into(), Value::Str(r.content.clone())),
                ("published".into(), pub_val),
                ("author_id".into(), Value::Int(r.author_id)),
                ("created_at".into(), Value::Str(r.created_at.clone())),
            ])
        })
        .collect();
    let native_json = encode_value(&Value::Arr(native_arr)).to_json_string();

    // MODE-2 (interpreter): execute_bundle over the SAME bundle + input on the SAME connection.
    let base = format!("/tmp/ormbench/{dialect}");
    let bundle = Node::parse(
        &std::fs::read_to_string(format!("{base}/bundle_filterPaginateSort.json"))
            .expect("read bundle"),
    )
    .expect("parse bundle");
    let input = Node::parse(
        &std::fs::read_to_string(format!("{base}/input_filterPaginateSort.json"))
            .expect("read input"),
    )
    .expect("parse input");
    let m2 = execute_bundle(&bundle, &input, d)
        .unwrap_or_else(|e| panic!("mode-2 filterPaginateSort: {}", e.message()));
    let mode2_json = encode_value(&m2).to_json_string();

    let ok = native_json == mode2_json;
    println!(
        "filterPaginateSort {dialect}: native≡mode-2 byte-equal = {ok} ({} rows)",
        rows.len()
    );
    if ok {
        // Show the created_at (TIMESTAMP→canonical string) + published (BOOLEAN/TINYINT) of row 0 as evidence.
        let sample: String = native_json.chars().take(200).collect();
        println!("  sample: {sample}");
    } else {
        println!(
            "  NATIVE: {}",
            native_json.chars().take(300).collect::<String>()
        );
        println!(
            "  MODE-2: {}",
            mode2_json.chars().take(300).collect::<String>()
        );
    }
}

// ── #129 safety proof: N+1-avoidance (query counts) via the CountingDriver. hardLimit + reader/writer
//    routing are proven by the dedicated companion entries (capped find, handler_routed) — see report. ─
fn run_safety(dialect: &str, spec: &str) {
    let counting = CountingDriver {
        inner: open_driver(spec),
    };
    let d: &dyn Driver = &counting;
    reseed(d, dialect);
    let count = |op: &str| {
        QUERY_COUNT.store(0, Ordering::SeqCst);
        prepare_op(op, d, spec)(0);
        QUERY_COUNT.load(Ordering::SeqCst)
    };
    // A single-level batched relation = 1 parent + 1 batched child = 2 (not 1+N).
    println!(
        "nestedFindAll queries={} (expect 2: 1 parent + 1 batched child)",
        count("nestedFindAll")
    );
    println!(
        "nestedFindUnique queries={} (expect 2)",
        count("nestedFindUnique")
    );
    // A 3-level chain = 1 per level = 3 (not N+1 per level).
    println!(
        "nestedRelations queries={} (expect 3: users + posts + comments)",
        count("nestedRelations")
    );
    println!(
        "compositeRelations queries={} (expect 3)",
        count("compositeRelations")
    );
    // A batch write = 1 statement for N records (not N).
    reseed(d, dialect);
    println!(
        "createMany queries={} (expect 1: one batched INSERT for 10 records)",
        count("createMany")
    );
    reseed(d, dialect);
    println!("updateMany queries={} (expect 1)", count("updateMany"));

    // ── find hardLimit (#135/#136): the GUARDED native find (findHardLimit=2, baked LIMIT 3) over the
    //    seed (>2 users) trips the shared check_find_hard_limit — the same guard core the ORM read
    //    companions carry. Proves the guard FIRES end-to-end (not just an emission assert). ──
    reseed(d, dialect);
    match gen::companion_cappedFindAll::run(d, gen::generated_cappedFindAll::InNRCappedFind {}) {
        Ok(rows) => println!(
            "hardLimit NOT tripped — got {} rows (BUG: guard did not fire)",
            rows.len()
        ),
        Err(litedbmodel_runtime::RuntimeError::Limit(l)) => {
            println!(
                "hardLimit fired: context={} limit={} fetched={} (expect find/2/3)",
                l.context, l.limit, l.count
            )
        }
        Err(litedbmodel_runtime::RuntimeError::Sql(e)) => {
            println!("hardLimit: unexpected SQL error: {}", e.message)
        }
    }
}
