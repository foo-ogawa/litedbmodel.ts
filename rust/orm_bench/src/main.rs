//! NATIVE-codegen ORM-bench cell (#141 rust-native pilot, epic #123) — self-measure the covered ORM
//! ops through the litedbmodel-GENERATED post-#164 wire-passthrough modules + `litedbmodel_runtime`'s
//! op-agnostic leaves, and print a flat CSV (`cell,dialect,op,iter,us`) the TS collector aggregates.
//!
//! This binary is a litedbmodel-CONSUMER: for each op it builds the CONCRETE `InNR_<comp>` input and
//! calls the sole GENERATED public entry `run_native_raw_struct_<comp>(in_)`. It supplies NO `node_*`
//! and holds NO hand-written exec seam — the covered runner calls the op-agnostic leaf transports
//! (`execute_sql`/`pluck_keys`/`group_children`) in `litedbmodel_runtime`, which run every DB access
//! through the runtime's central execute/run seam over the AMBIENT driver the consumer brackets each op
//! with (`with_ambient_driver`). Relations are N+1-free: `parents → pluck → executeSQL(WHERE fk IN …)
//! → group` runs 1 batched child query per level (nestedFindAll=2, nestedRelations=3).
//!
//! Usage: `orm_bench <dialect> <spec> [reps] [warmup]`, or `orm_bench safety <dialect> <spec>`.

#[path = "gen/mod.rs"]
mod gen;

use litedbmodel_runtime::driver::PreparedStatement;
use litedbmodel_runtime::exec_context::TxConnection;
use litedbmodel_runtime::{with_ambient_driver, Driver, SqlFailure, SqliteDriver, WireList, WireRow, WireValue};
#[cfg(feature = "livedb")]
use litedbmodel_runtime::{MysqlDriver, PostgresDriver};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use gen::behaviors_generated as bg;

// ── query counter (consumer-side observability, N+1 proof) ──────────────────────────────────────
// A `CountingDriver` decorator over the runtime Driver increments on each `prepare` (one per statement
// the runtime issues). The N+1 proof: a batched relation runs 1 parent + 1 batched child per level =
// 2 / 3 (not 1+N). The runtime + generated runner stay unchanged — the count rides the Driver seam.
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

fn schema() -> Vec<String> {
    gen::generated_setup::STATEMENTS.iter().map(|s| s.to_string()).collect()
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
    let _ = spec; // sqlite pilot: an in-memory DB seeded from the generated schema (spec ignored).
    Box::new(SqliteDriver::in_memory(&schema()).expect("open in-memory sqlite"))
}

// ── seed: DELETE + INSERT the canonical fixture (schema already applied at open) ────────────────────
// Re-run before each op so reads see a stable seed and writes start clean. Real nested data (users →
// posts → comments; tenant users → tenant posts) so the N+1 proof is meaningful (2/3 queries returning
// real children, not 1+N). Runs on the driver directly (not through the leaves) — no ambient needed.
fn seed(d: &dyn Driver) {
    const STMTS: &[&str] = &[
        "DELETE FROM benchmark_comments",
        "DELETE FROM benchmark_posts",
        "DELETE FROM benchmark_users",
        "DELETE FROM benchmark_tenant_comments",
        "DELETE FROM benchmark_tenant_posts",
        "DELETE FROM benchmark_tenant_users",
        "INSERT INTO benchmark_users (id, email, name) VALUES \
         (1,'user1@example.com','User 1'),(2,'user2@example.com','User 2'),\
         (3,'user3@example.com','User 3'),(4,'user4@example.com','User 4'),(5,'user5@example.com','User 5')",
        "INSERT INTO benchmark_posts (id, title, content, published, author_id) VALUES \
         (1,'P1','c',1,1),(2,'P2','c',1,1),(3,'P3','c',1,2),(4,'P4','c',1,2),(5,'P5','c',1,3),(6,'P6','c',1,3)",
        "INSERT INTO benchmark_comments (id, body, post_id) VALUES \
         (1,'b',1),(2,'b',1),(3,'b',2),(4,'b',3),(5,'b',5)",
        "INSERT INTO benchmark_tenant_users (tenant_id, user_id, name) VALUES (1,1,'TU1'),(1,2,'TU2'),(1,3,'TU3')",
        "INSERT INTO benchmark_tenant_posts (tenant_id, post_id, user_id, title) VALUES (1,10,1,'TP1'),(1,11,2,'TP2')",
        // composite level-3: comments keyed on (tenant_id, post_id) — real children so the composite
        // grouping is exercised (grouped by the full 2-col tuple, not a cartesian cross).
        "INSERT INTO benchmark_tenant_comments (tenant_id, comment_id, post_id, body) VALUES \
         (1,100,10,'tc'),(1,101,10,'tc'),(1,102,11,'tc')",
    ];
    for s in STMTS {
        d.prepare(s).run(&[]).unwrap_or_else(|e| panic!("seed `{s}`: {}", e.message));
    }
}

// ── the covered ops. Each runs ONE logical op for iteration `it` (mutating ops vary their UNIQUE
//    column). The driver is the ambient one (bracketed by the caller); the runner takes no driver. ──
fn run_op(op: &str, it: u64) {
    match op {
        "findAll" => {
            bg::run_native_raw_struct_findAll(bg::InNRFindAll).unwrap();
        }
        "filterPaginateSort" => {
            // `published` is BOOLEAN on postgres (native port `bool`) but INTEGER on sqlite/mysql — the
            // ONE dialect-typed input; select the matching wire literal at compile time via `pg`.
            #[cfg(feature = "pg")]
            let published = WireValue::Bool(true);
            #[cfg(not(feature = "pg"))]
            let published = WireValue::int(1);
            bg::run_native_raw_struct_filterPaginateSort(bg::InNRFilterPaginateSort { published }).unwrap();
        }
        "findFirst" => {
            bg::run_native_raw_struct_findFirst(bg::InNRFindFirst {
                name: WireValue::Str("User%".into()),
                ..Default::default()
            })
            .unwrap();
        }
        "findUnique" => {
            bg::run_native_raw_struct_findUnique(bg::InNRFindUnique {
                email: WireValue::Str("user1@example.com".into()),
            })
            .unwrap();
        }
        "nestedFindAll" => {
            bg::run_native_raw_struct_nestedFindAll(bg::InNRNestedFindAll).unwrap();
        }
        "nestedFindFirst" => {
            bg::run_native_raw_struct_nestedFindFirst(bg::InNRNestedFindFirst {
                name: WireValue::Str("User%".into()),
                ..Default::default()
            })
            .unwrap();
        }
        "nestedFindUnique" => {
            bg::run_native_raw_struct_nestedFindUnique(bg::InNRNestedFindUnique {
                email: WireValue::Str("user1@example.com".into()),
            })
            .unwrap();
        }
        "nestedRelations" => {
            bg::run_native_raw_struct_nestedRelations(bg::InNRNestedRelations).unwrap();
        }
        "compositeRelations" => {
            bg::run_native_raw_struct_compositeRelations(bg::InNRCompositeRelations).unwrap();
        }
        "create" => {
            bg::run_native_raw_struct_create(bg::InNRCreate {
                email: WireValue::Str(format!("new{it}@bench.com")),
                name: WireValue::Str("New".into()),
            })
            .unwrap();
        }
        "update" => {
            bg::run_native_raw_struct_update(bg::InNRUpdate {
                id: WireValue::int(1),
                name: WireValue::Str("Updated 1".into()),
            })
            .unwrap();
        }
        "upsert" => {
            bg::run_native_raw_struct_upsert(bg::InNRUpsert {
                email: WireValue::Str("user1@example.com".into()),
                name: WireValue::Str("Upserted One".into()),
            })
            .unwrap();
        }
        "createMany" => {
            // 10 fresh rows — email is UNIQUE NOT NULL, so vary per iteration to stay insertable.
            bg::run_native_raw_struct_createMany(bg::InNRCreateMany { rows: user_rows(it, false) }).unwrap();
        }
        "upsertMany" => {
            // 10 rows keyed on email (ON CONFLICT DO UPDATE) — idempotent across iterations.
            bg::run_native_raw_struct_upsertMany(bg::InNRUpsertMany { rows: user_rows(it, true) }).unwrap();
        }
        "updateMany" => {
            // 10 rows keyed on id (1..=10) — updates the seeded users, no-op for absent ids.
            let items: Vec<WireValue> = (1..=10)
                .map(|id| {
                    WireValue::Row(WireRow {
                        entries: vec![
                            ("id".to_string(), WireValue::int(id)),
                            ("name".to_string(), WireValue::Str(format!("Many {id}"))),
                        ],
                    })
                })
                .collect();
            bg::run_native_raw_struct_updateMany(bg::InNRUpdateMany { rows: WireValue::List(WireList { items }) }).unwrap();
        }
        other => panic!("unknown op '{other}'"),
    }
}

// Build the 10-row batch record set for createMany/upsertMany as ONE opaque `rows` wire array (the
// json_each/JSON_TABLE batch param). `stable` reuses fixed emails (upsertMany — conflict-updates); else
// the email varies by iteration so a plain INSERT stays insertable under the UNIQUE(email) constraint.
fn user_rows(it: u64, stable: bool) -> WireValue {
    let items: Vec<WireValue> = (0..10)
        .map(|i| {
            let email = if stable { format!("many{i}@bench.com") } else { format!("many{it}_{i}@bench.com") };
            WireValue::Row(WireRow {
                entries: vec![
                    ("email".to_string(), WireValue::Str(email)),
                    ("name".to_string(), WireValue::Str(format!("Many {i}"))),
                ],
            })
        })
        .collect();
    WireValue::List(WireList { items })
}

// The covered ops exposed on the combined struct-native path (bg::COMPONENT_NAMES_NATIVE_RAW).
const OPS: &[&str] = &[
    "findAll",
    "filterPaginateSort",
    "findFirst",
    "findUnique",
    "nestedFindAll",
    "nestedFindFirst",
    "nestedFindUnique",
    "nestedRelations",
    "compositeRelations",
    "create",
    "update",
    "upsert",
    "createMany",
    "upsertMany",
    "updateMany",
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
    let d: &dyn Driver = driver.as_ref();
    println!("cell,dialect,op,iter,us");
    for op in OPS {
        // Re-seed the fixture before each op, then run the whole warmup+timed loop with the ambient
        // driver installed (the covered runner resolves it inside `execute_sql`).
        seed(d);
        with_ambient_driver(d, || {
            for it in 0..warmup {
                run_op(op, it);
            }
            for it in 0..reps {
                let g = it + warmup;
                let t = Instant::now();
                run_op(op, g);
                let us = t.elapsed().as_micros();
                println!("native,{dialect},{op},{it},{us}");
            }
        });
    }
}

// ── N+1-avoidance proof (query counts) via the CountingDriver + the ambient seam. ──────────────────
fn run_safety(_dialect: &str, spec: &str) {
    let counting = CountingDriver { inner: open_driver(spec) };
    let d: &dyn Driver = &counting;
    seed(d);
    let count = |op: &str| -> usize {
        QUERY_COUNT.store(0, Ordering::SeqCst);
        with_ambient_driver(d, || run_op(op, 0));
        QUERY_COUNT.load(Ordering::SeqCst)
    };
    // Each relation op is executed and fail-closed on its fixed batched query count: 1 parent + 1
    // batched child per relation level (N+1-free), INDEPENDENT of the row count.
    for (op, expected) in [
        ("nestedFindAll", 2usize),
        ("nestedFindFirst", 2),
        ("nestedFindUnique", 2),
        ("nestedRelations", 3),
        // composite 3-level chain: 1 parent + 1 batched child per level = 3 (N+1-free, composite key).
        ("compositeRelations", 3),
    ] {
        let actual = count(op);
        assert_eq!(actual, expected, "{op} query-count regression");
        println!("{op} queries={actual} (expect {expected})");
    }
    // Batch writes are ONE statement for N records (the json_each/JSON_TABLE batch form) — the whole
    // record set rides as ONE param, so the query count is a fixed 1, INDEPENDENT of the row count
    // (the safety guarantee: no per-row statement fan-out).
    for (op, expected) in [("createMany", 1usize), ("upsertMany", 1), ("updateMany", 1)] {
        let actual = count(op);
        assert_eq!(actual, expected, "{op} query-count regression");
        println!("{op} queries={actual} (expect {expected})");
    }
}
