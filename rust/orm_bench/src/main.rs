//! NATIVE-codegen ORM-bench cell (#129, epic #123) — self-measure the 19 ORM ops through the
//! litedbmodel-GENERATED native modules + `litedbmodel_runtime` Driver, and print a flat
//! CSV (`cell,dialect,op,iter,us`) the TS collector aggregates. This binary is a litedbmodel-CONSUMER:
//! for each op it builds the typed input and calls the sole GENERATED public entry
//! (`generated_<op>::run`). The runtime adapter is co-located in that generated file. The consumer
//! supplies NO `node_*` of its own and holds NO
//! hand-written exec seam — every DB access rides litedbmodel_runtime's op-agnostic Driver `exec`.
//!
//! Relations (nestedFind*/nestedRelations/compositeRelations) follow #131: the native module carries the
//! PRIMARY read only; the declared relation is loaded by the runtime batch loader `stitch_relation` over
//! the primary rows (1 batched child query per level → N+1-free). The consumer only decides WHICH nested
//! includes to resolve (the include-path walk over `childRelations`) — the batch/group/distribute SSoT
//! stays in the runtime.
//!
//! Writes MUTATE state, so each op re-seeds from the compiled `generated_setup::STATEMENTS` emitted
//! from the orm-domain SSoT, and the ops with a UNIQUE column vary that column per iteration.
//!
//! Usage: `orm_bench <dialect> <spec> [reps] [warmup]`  (spec = sqlite file path, or — with
//! `--features livedb` — `pg:<libpq-conn>` / `mysql:<url>`); or `orm_bench safety <dialect> <spec>`.

#[path = "gen/mod.rs"]
mod gen;

use litedbmodel_runtime::driver::PreparedStatement;
use litedbmodel_runtime::exec_context::TxConnection;
use litedbmodel_runtime::{Driver, SqlFailure, SqliteDriver};
#[cfg(feature = "livedb")]
use litedbmodel_runtime::{MysqlDriver, PostgresDriver};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

// ── query counter (consumer-side observability, #129 safety proof) ──────────────────────────────
// A `CountingDriver` decorator over the runtime Driver increments on each `prepare` (one per statement
// the runtime issues). The N+1 proof: a batched relation runs 1 parent + 1 batched child = 2 (not 1+N);
// a batch write runs 1 statement (not N). The runtime + adapter stay unchanged.
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

// ── setup: execute the generated native static statements ──────────────────────────────────────
fn reseed(d: &dyn Driver, _dialect: &str) {
    for sql in gen::generated_setup::STATEMENTS {
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
            let _ = generated_findAll::run(d, generated_findAll::InNRFindAll).unwrap();
        }),
        "filterPaginateSort" => Box::new(move |_it| {
            // `published` is a BOOLEAN column on postgres (native port type `bool`) but INTEGER on
            // sqlite/mysql (`i64`) — the ONE dialect-typed input in the 19 ops. The per-dialect gen is
            // swapped in per build, so select the matching literal at compile time via the `pg` feature.
            #[cfg(feature = "pg")]
            let published = true;
            #[cfg(not(feature = "pg"))]
            let published = 1;
            let _ = generated_filterPaginateSort::run(
                d,
                generated_filterPaginateSort::InNRFilterPaginateSort { published },
            )
            .unwrap();
        }),
        "findFirst" => Box::new(move |_it| {
            let _ = generated_findFirst::run(
                d,
                generated_findFirst::InNRFindFirst {
                    name: "User%".into(),
                },
            )
            .unwrap();
        }),
        "findUnique" => Box::new(move |_it| {
            let _ = generated_findUnique::run(
                d,
                generated_findUnique::InNRFindUnique {
                    email: "user500@example.com".into(),
                },
            )
            .unwrap();
        }),
        // Relation ops: the timed cell is TWO lines — (1) the generated TYPED native read, (2) the
        // generated TYPED hydrator with a natural key accessor (`|r| r.id`). SQL/key metadata and nested
        // calls are compiled into it. Each timed relation cell is exactly: primary read + hydrate.
        "nestedFindAll" => Box::new(move |_it| {
            let users =
                generated_nestedFindAll::run(d, generated_nestedFindAll::InNRFindAll).unwrap();
            let _ = generated_nestedFindAll::hydrate_posts(users, d).unwrap();
        }),
        "nestedFindFirst" => Box::new(move |_it| {
            let users = generated_nestedFindFirst::run(
                d,
                generated_nestedFindFirst::InNRFindFirst {
                    name: "User%".into(),
                },
            )
            .unwrap();
            let _ = generated_nestedFindFirst::hydrate_posts(users, d).unwrap();
        }),
        "nestedFindUnique" => Box::new(move |_it| {
            let users = generated_nestedFindUnique::run(
                d,
                generated_nestedFindUnique::InNRFindUnique {
                    email: "user1@example.com".into(),
                },
            )
            .unwrap();
            let _ = generated_nestedFindUnique::hydrate_posts(users, d).unwrap();
        }),
        "nestedRelations" => Box::new(move |_it| {
            let users =
                generated_nestedRelations::run(d, generated_nestedRelations::InNRFindAll).unwrap();
            let _ = generated_nestedRelations::hydrate_posts(users, d).unwrap();
        }),
        "compositeRelations" => Box::new(move |_it| {
            let rows = generated_compositeRelations::run(
                d,
                generated_compositeRelations::InNRByTenant { tenant_id: 1 },
            )
            .unwrap();
            let _ = generated_compositeRelations::hydrate_posts(rows, d).unwrap();
        }),
        "create" => Box::new(move |it| {
            let _ = generated_create::run(
                d,
                generated_create::InNRCreate {
                    email: format!("new{it}@bench.com"),
                    name: "New".into(),
                },
            )
            .unwrap();
        }),
        "update" => Box::new(move |_it| {
            let _ = generated_update::run(
                d,
                generated_update::InNRUpdate {
                    id: 100,
                    name: "Updated 100".into(),
                },
            )
            .unwrap();
        }),
        "upsert" => Box::new(move |_it| {
            let _ = generated_upsert::run(
                d,
                generated_upsert::InNRUpsert {
                    email: "user1@example.com".into(),
                    name: "Upserted One".into(),
                },
            )
            .unwrap();
        }),
        "createMany" => Box::new(move |it| {
            let _ = generated_createMany::run(
                d,
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
            let _ = generated_upsertMany::run(
                d,
                generated_upsertMany::InNRUpsertMany {
                    emails,
                    names: batch_names(),
                },
            )
            .unwrap();
        }),
        "updateMany" => Box::new(move |_it| {
            let _ = generated_updateMany::run(
                d,
                generated_updateMany::InNRUpdateMany {
                    ids: (1..=10).collect(),
                    names: batch_names(),
                },
            )
            .unwrap();
        }),
        // ── tx ops: the native RETURNING chain runs through the adapter `run_on` (BEGIN…COMMIT). ──
        "delete" => Box::new(move |it| {
            let _ = generated_delete::run_on(
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
            let _ = generated_nestedCreate::run_on(
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
            let _ = generated_nestedUpdate::run_on(
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
            let _ = generated_nestedUpsert::run_on(
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
        // Build the op closure once. The timed region below is exactly the op call.
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

// ── #129 safety proof: N+1-avoidance (query counts) via the CountingDriver. hardLimit + reader/writer
//    routing are proven by the dedicated adapter entries (capped find, handler_routed) — see report. ─
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
    let expect_queries = |op: &str, expected: usize| {
        let actual = count(op);
        assert_eq!(actual, expected, "{op} query-count regression");
        println!("{op} queries={actual} (expect {expected})");
    };
    // Every relation benchmark cell is executed and fail-closed on its fixed batched query count.
    expect_queries("nestedFindAll", 2);
    expect_queries("nestedFindFirst", 2);
    expect_queries("nestedFindUnique", 2);
    expect_queries("nestedRelations", 3);
    expect_queries("compositeRelations", 3);
    reseed(d, dialect);
    let users = gen::generated_nestedRelations::run(d, gen::generated_nestedRelations::InNRFindAll)
        .expect("nested relation parents");
    let tree = gen::generated_nestedRelations::hydrate_posts(users, d)
        .expect("nested relation typed tree");
    let comment_count: usize = tree
        .iter()
        .flat_map(|(_, posts)| posts.iter())
        .map(|(_, comments)| comments.len())
        .sum();
    assert!(
        comment_count > 0,
        "nested relation comments must survive in the returned typed tree"
    );
    println!("nestedRelations returned comments={comment_count} (typed tree, not discarded)");
    // A batch write = 1 statement for N records (not N).
    reseed(d, dialect);
    expect_queries("createMany", 1);
    reseed(d, dialect);
    expect_queries("updateMany", 1);

    // ── find hardLimit (#135/#136): the GUARDED native find (findHardLimit=2, baked LIMIT 3) over the
    //    seed (>2 users) trips the shared check_find_hard_limit — the same guard core the ORM read
    //    adapters carry. Proves the guard FIRES end-to-end (not just an emission assert). ──
    reseed(d, dialect);
    match gen::generated_cappedFindAll::run(d, gen::generated_cappedFindAll::InNRCappedFind {}) {
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
