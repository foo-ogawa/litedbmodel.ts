//! Phase A-2 (#76, rust) — CONCURRENT-TRANSACTION ISOLATION + ATOMICITY on live PG + MySQL.
//!
//! The rust mirror of #75's `test/integration/TxIsolation.test.ts`. It proves the per-execution
//! connection ownership (`ExecutionContext` + `with_transaction` over an OWNED `PgTx`/`MyTx`, §3): N
//! transactions run CONCURRENTLY in ONE process on the shared driver, each acquiring its OWN pooled
//! connection. The assertions run the UNMODIFIED PRODUCTION path (`execute_transaction_bundle` →
//! `with_transaction_decided` → the tx-owned connection) — nothing is swapped:
//!
//!   (1) ISOLATION — N concurrent workers each run TWO back-to-back single-INSERT transactions with a
//!       yield between; the final table holds EXACTLY the 2·N rows, correctly paired per worker (no
//!       cross-talk, no interleaving corruption).
//!   (2) ATOMICITY (single-statement) — a tx whose sole INSERT collides on the PK ROLLBACKs; the
//!       concurrent committing transactions are unaffected (their rows present; the aborted one's
//!       absent).
//!   (3) ATOMICITY (MULTI-statement, production-path) — a 2-statement tx whose 2nd statement collides
//!       MUST roll back the 1st statement's already-executed INSERT (real cross-statement atomicity
//!       through `with_transaction_decided` on ONE owned connection), while a concurrently-committed
//!       tx is unaffected. This pins production ownership DIRECTLY — the faithful-mutation RED proof
//!       (documented at the bottom) reverts ownership to a shared connection and this goes RED.
//!
//! Gated behind the `livedb` feature AND `LITEDBMODEL_TX_ISOLATION=1` so it never runs in the default
//! `cargo test` (which has no DBs). Requires the dockerized PG (:5433) + MySQL (:3307):
//!   docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d postgres mysql
//!   LITEDBMODEL_TX_ISOLATION=1 cargo test -p litedbmodel_interpreter --features livedb \
//!     --test tx_isolation -- --nocapture --test-threads=1

#![cfg(feature = "livedb")]

use behavior_contracts::Value;
use litedbmodel_interpreter::{
    execute_transaction_bundle, Driver, MysqlDriver, Node, PostgresDriver,
};

fn nj(s: &str) -> Node {
    Node::parse(s).expect("test fixture JSON parses")
}

fn enabled() -> bool {
    std::env::var("LITEDBMODEL_TX_ISOLATION").as_deref() == Ok("1")
}

fn env(k: &str, d: &str) -> String {
    std::env::var(k)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| d.to_string())
}

const ISO_TBL: &str = "scp_tx_iso";

// ── Bundle authoring (the shape `execute_transaction_bundle` consumes) ─────────

/// A single-INSERT (no RETURNING, `entityFrom: null`) transaction bundle whose values come from the
/// input scope (`{"ref":["id"]}` etc.). Committed via the production tx path on ONE owned connection.
fn insert_bundle(dialect: &str) -> Node {
    nj(&format!(
        r#"{{"dialect": "{dialect}", "transaction": {{
             "phase": "create", "entityFrom": null,
             "statements": [{{
               "id": "tx_body_0", "role": "body",
               "op": {{
                 "sql": "INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (?, ?, ?)",
                 "params": [{{"ref": ["id"]}}, {{"ref": ["worker"]}}, {{"ref": ["seq"]}}]
               }}
             }}]
           }}}}"#
    ))
}

/// A TWO-statement transaction bundle: stmt-1 inserts `id1` (valid), stmt-2 inserts `id2` (which the
/// caller pre-seeds so it collides). One logical transaction — stmt-2's PK violation must roll back
/// stmt-1 (cross-statement atomicity on the owned connection).
fn two_stmt_bundle(dialect: &str, id1: i64, id2: i64, worker: i64) -> Node {
    nj(&format!(
        r#"{{"dialect": "{dialect}", "transaction": {{
             "phase": "create", "entityFrom": null,
             "statements": [
               {{"id": "tx_body_0", "role": "body",
                 "op": {{"sql": "INSERT INTO {ISO_TBL} (id, worker, seq) VALUES ({id1}, {worker}, 0)", "params": []}}}},
               {{"id": "tx_body_1", "role": "body",
                 "op": {{"sql": "INSERT INTO {ISO_TBL} (id, worker, seq) VALUES ({id2}, {worker}, 0)", "params": []}}}}
             ]
           }}}}"#
    ))
}

fn input(id: i64, worker: i64, seq: i64) -> Node {
    nj(&format!(
        r#"{{"id": {id}, "worker": {worker}, "seq": {seq}}}"#
    ))
}

fn committed(v: &Value) -> bool {
    matches!(
        v,
        Value::Obj(pairs) if pairs.iter().any(|(k, val)| k == "committed" && matches!(val, Value::Bool(true)))
    )
}

/// Run one production INSERT transaction (worker `k`, ids from the input). Returns the tx outcome.
fn run_insert_tx(
    driver: &(dyn Driver + Sync),
    dialect: &str,
    id: i64,
    worker: i64,
    seq: i64,
) -> Result<Value, litedbmodel_interpreter::SqlFailure> {
    let b = insert_bundle(dialect);
    execute_transaction_bundle(&b, &input(id, worker, seq), driver)
}

// ── Reading the table back through the driver (the seam's read path) ───────────

/// Read (id, worker) rows sorted by id (worker != 999, filtering any pre-seed).
fn read_rows(driver: &(dyn Driver + Sync)) -> Vec<(i64, i64)> {
    let rows = driver
        .prepare(&format!(
            "SELECT id, worker FROM {ISO_TBL} WHERE worker <> 999"
        ))
        .all(&[])
        .expect("read rows");
    let mut out: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| {
            let get = |k: &str| match r {
                Value::Obj(p) => p.iter().find(|(kk, _)| kk == k).map(|(_, v)| v.clone()),
                _ => None,
            };
            let n = |v: Option<Value>| match v {
                Some(Value::Int(i)) => i,
                Some(Value::Float(f)) => f as i64,
                Some(Value::Str(s)) => s.parse().unwrap_or(-1),
                _ => -1,
            };
            (n(get("id")), n(get("worker")))
        })
        .collect();
    out.sort();
    out
}

// ── PG / MySQL connect helpers ─────────────────────────────────────────────────

fn pg() -> PostgresDriver {
    let conn = format!(
        "host={} port={} user={} password={} dbname={}",
        env("TEST_DB_HOST", "localhost"),
        env("TEST_DB_PORT", "5433"),
        env("TEST_DB_USER", "testuser"),
        env("TEST_DB_PASSWORD", "testpass"),
        env("TEST_DB_NAME", "testdb"),
    );
    PostgresDriver::connect(&conn).expect("pg connect")
}

fn mysql() -> MysqlDriver {
    let url = format!(
        "mysql://{}:{}@{}:{}/{}",
        env("TEST_MYSQL_USER", "testuser"),
        env("TEST_MYSQL_PASSWORD", "testpass"),
        env("TEST_MYSQL_HOST", "127.0.0.1"),
        env("TEST_MYSQL_PORT", "3307"),
        env("TEST_MYSQL_DB", "testdb"),
    );
    MysqlDriver::connect(&url).expect("mysql connect")
}

fn reset_pg(d: &PostgresDriver) {
    d.exec_ddl(&[
        format!("DROP TABLE IF EXISTS {ISO_TBL}"),
        format!("CREATE TABLE {ISO_TBL} (id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, seq INTEGER NOT NULL)"),
    ])
    .expect("pg reset");
}

fn reset_mysql(d: &MysqlDriver) {
    d.exec_ddl(&[
        format!("DROP TABLE IF EXISTS {ISO_TBL}"),
        format!(
            "CREATE TABLE {ISO_TBL} (id INT PRIMARY KEY, worker INT NOT NULL, seq INT NOT NULL)"
        ),
    ])
    .expect("mysql reset");
}

const N: i64 = 8;

/// (1) ISOLATION — N workers each run TWO single-INSERT txs concurrently (worker k writes id=2k then
/// id=2k+1); the final table holds EXACTLY the 2·N rows. Runs the production `execute_transaction_bundle`
/// per tx — every BEGIN…COMMIT owns its own connection, so concurrent workers never cross-talk.
fn isolation(driver: &(dyn Driver + Sync), dialect: &str) {
    std::thread::scope(|scope| {
        for k in 0..N {
            scope.spawn(move || {
                let r0 = run_insert_tx(driver, dialect, 2 * k, k, 0).expect("tx0");
                assert!(committed(&r0), "worker {k} tx0 committed");
                std::thread::sleep(std::time::Duration::from_millis(2)); // force interleave
                let r1 = run_insert_tx(driver, dialect, 2 * k + 1, k, 1).expect("tx1");
                assert!(committed(&r1), "worker {k} tx1 committed");
            });
        }
    });

    let got = read_rows(driver);
    let mut want: Vec<(i64, i64)> = Vec::new();
    for k in 0..N {
        want.push((2 * k, k));
        want.push((2 * k + 1, k));
    }
    assert_eq!(
        got, want,
        "{dialect}: every worker's rows present, no cross-talk"
    );
}

/// (2) SINGLE-STATEMENT ATOMICITY — worker 0 collides on a pre-seeded id=0 (PK violation → whole tx
/// ROLLBACK + error); workers 1..N commit. The aborted row is ABSENT; every committed worker present.
fn single_stmt_atomicity(driver: &(dyn Driver + Sync), dialect: &str) {
    // Pre-seed id=0 so worker 0's INSERT collides.
    driver
        .prepare(&format!(
            "INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (0, 999, 9)"
        ))
        .run(&[])
        .expect("preseed");

    let outcomes: std::sync::Mutex<Vec<(i64, bool)>> = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for k in 0..N {
            let outcomes = &outcomes;
            scope.spawn(move || {
                let ok = run_insert_tx(driver, dialect, k, k, 0).is_ok();
                outcomes.lock().unwrap().push((k, ok));
            });
        }
    });

    let outcomes = outcomes.into_inner().unwrap();
    let ok0 = outcomes.iter().find(|(k, _)| *k == 0).map(|(_, ok)| *ok);
    assert_eq!(
        ok0,
        Some(false),
        "{dialect}: worker 0 (PK collision) tx must FAIL"
    );
    for (k, ok) in &outcomes {
        if *k != 0 {
            assert!(*ok, "{dialect}: worker {k} must commit");
        }
    }

    let got = read_rows(driver); // worker 999 pre-seed filtered out
    let want: Vec<(i64, i64)> = (1..N).map(|i| (i, i)).collect();
    assert_eq!(
        got, want,
        "{dialect}: aborted worker 0 row ABSENT (atomic rollback); committed workers present"
    );
}

/// (3) MULTI-STATEMENT ATOMICITY (production path) — a 2-statement tx (id=10 valid, id=20 pre-seeded
/// collision) run CONCURRENTLY with a committing single-INSERT (id=30). stmt-2's collision must roll
/// back stmt-1 (id=10 ABSENT), and the concurrent commit (id=30) is unaffected. Runs the UNMODIFIED
/// `execute_transaction_bundle` → `with_transaction_decided` on ONE owned connection.
fn multi_stmt_atomicity(driver: &(dyn Driver + Sync), dialect: &str) {
    // Pre-seed id=20 so the failing tx's SECOND statement collides.
    driver
        .prepare(&format!(
            "INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (20, 999, 9)"
        ))
        .run(&[])
        .expect("preseed 20");

    let fail_ok = std::sync::Mutex::new(None::<bool>);
    let commit_ok = std::sync::Mutex::new(None::<bool>);
    std::thread::scope(|scope| {
        let fail_ok = &fail_ok;
        let commit_ok = &commit_ok;
        scope.spawn(move || {
            let b = two_stmt_bundle(dialect, 10, 20, 1);
            let res = execute_transaction_bundle(&b, &input(0, 1, 0), driver);
            *fail_ok.lock().unwrap() = Some(res.is_ok());
        });
        scope.spawn(move || {
            let res = run_insert_tx(driver, dialect, 30, 2, 0);
            *commit_ok.lock().unwrap() = Some(res.map(|v| committed(&v)).unwrap_or(false));
        });
    });

    // The failing 2-statement tx threw (PK collision on stmt-2) — NOT a silent partial commit.
    assert_eq!(
        fail_ok.into_inner().unwrap(),
        Some(false),
        "{dialect}: the 2-statement tx must FAIL on stmt-2's collision"
    );
    // The concurrent single-INSERT committed.
    assert_eq!(
        commit_ok.into_inner().unwrap(),
        Some(true),
        "{dialect}: the concurrent single-INSERT tx must commit unaffected"
    );

    let got = read_rows(driver); // id=20 pre-seed (worker 999) filtered out
    assert_eq!(
        got,
        vec![(30, 2)],
        "{dialect}: id=10 ROLLED BACK (cross-statement atomicity); id=30 present, unaffected"
    );
}

#[test]
fn pg_tx_isolation_and_atomicity() {
    if !enabled() {
        eprintln!("skipped (set LITEDBMODEL_TX_ISOLATION=1 + docker up)");
        return;
    }
    let d = pg();
    reset_pg(&d);
    isolation(&d, "postgres");
    reset_pg(&d);
    single_stmt_atomicity(&d, "postgres");
    reset_pg(&d);
    multi_stmt_atomicity(&d, "postgres");
    eprintln!("PG TX-ISOLATION PROOF: isolation + single-stmt + multi-stmt atomicity all green (per-execution ownership)");
}

#[test]
fn mysql_tx_isolation_and_atomicity() {
    if !enabled() {
        eprintln!("skipped (set LITEDBMODEL_TX_ISOLATION=1 + docker up)");
        return;
    }
    let d = mysql();
    reset_mysql(&d);
    isolation(&d, "mysql");
    reset_mysql(&d);
    single_stmt_atomicity(&d, "mysql");
    reset_mysql(&d);
    multi_stmt_atomicity(&d, "mysql");
    eprintln!("MYSQL TX-ISOLATION PROOF: isolation + single-stmt + multi-stmt atomicity all green (per-execution ownership)");
}

// ── FAITHFUL-MUTATION RED PROOF (performed during #76; reverted) ───────────────
//
// The gate's teeth were proven by a FAITHFUL mutation that reverts per-execution connection ownership
// and confirming this test goes RED. The mutation (applied to `src/livedb.rs` `PgTx::run`): route the
// tx's write to a FRESH pooled connection (`self.pool.get()`, autocommit) instead of the tx-OWNED
// `self.conn` — i.e. the tx statements no longer run on the transaction's own connection. Observed
// RED under it (PG):
//
//   assertion `left == right` failed: id=10 ROLLED BACK (cross-statement atomicity); id=30 present
//     left:  [(10, 1), (30, 2)]     right: [(30, 2)]
//
// The failing 2-statement tx's stmt-1 (id=10) SURVIVED — it committed on a separate autocommit
// connection, so the ROLLBACK of the tx-owned connection did not undo it. With ownership restored,
// the write runs on the tx's OWN connection and the ROLLBACK undoes it → GREEN. The mutation was
// applied, the RED captured, then FULLY reverted (the committed code owns the tx connection).
