//! Phase B (#82, rust) — the FULL REAL transaction on live PG + MySQL: the user-controlled
//! `transaction()` boundary + tx-completeness primitives (#81) + write=tx guard (#86).
//!
//! The rust mirror of the TS `TxBoundary` / `TxCompleteness` / `tx_isolation` integration tests.
//! Every assertion runs the UNMODIFIED PRODUCTION path — the public
//! [`litedbmodel_interpreter::transaction`] boundary + [`execute_transaction_bundle_ctx`] (the ambient
//! JOIN + guard), on the live [`PostgresDriver`] / [`MysqlDriver`] over REAL dockerized PG (:5433) +
//! MySQL (:3307). Nothing is swapped or mocked.
//!
//!   (1) MULTI-OP ATOMICITY — `transaction(|tx| { opA-insert; opB-insert })` commits BOTH; a
//!       call-counting driver wrapper asserts exactly ONE begin_tx / ONE BEGIN / ONE COMMIT on ONE
//!       connection. Then opB PK-collides → opA's row is ALSO rolled back (ONE BEGIN + ONE ROLLBACK,
//!       zero COMMIT), verified by reading the DB rows.
//!   (2) GUARD — a write OUTSIDE `transaction()` → WriteOutsideTransactionError; a read-only write →
//!       WriteInReadOnlyContextError; a write INSIDE `transaction()` → succeeds.
//!   (3) ISOLATION — under `repeatable read` a re-read inside one tx does NOT see a concurrent
//!       committed change; under `read committed` it DOES (real behavioral, both dialects).
//!   (4) RETRY (REAL contention) — genuine PG SERIALIZABLE write-skew (40001) / MySQL deadlock
//!       (1213) with two concurrent txs → the loser retries and both commit; a non-retryable error
//!       does NOT retry.
//!   (5) NESTED — a nested `transaction()` = ONE BEGIN/COMMIT; an inner error rolls back the WHOLE.
//!
//! Gated behind `livedb` + `LITEDBMODEL_TX_BOUNDARY=1` (never runs in the default `cargo test`):
//!   docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d postgres mysql
//!   LITEDBMODEL_TX_BOUNDARY=1 cargo test -p litedbmodel_interpreter --features livedb \
//!     --test tx_boundary -- --nocapture --test-threads=1

#![cfg(feature = "livedb")]

use std::cell::RefCell;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use behavior_contracts::Value;
use litedbmodel_interpreter::exec_context::{run, transaction, StatementIntent};
use litedbmodel_interpreter::tx_options::{Dialect, IsolationLevel, TransactionOptions};
use litedbmodel_interpreter::{
    execute_transaction_bundle_ctx, for_driver, with_transaction_decided, Driver, MysqlDriver,
    Node, PostgresDriver, PreparedStatement, RunInfo, SqlFailure, TxConnection, TxDecision,
};

fn nj(s: &str) -> Node {
    Node::parse(s).expect("test fixture JSON parses")
}

fn enabled() -> bool {
    std::env::var("LITEDBMODEL_TX_BOUNDARY").as_deref() == Ok("1")
}

fn env(k: &str, d: &str) -> String {
    std::env::var(k)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| d.to_string())
}

const TBL: &str = "scp_tx_boundary";

// ── A call-counting Driver wrapper: counts begin_tx + BEGIN/COMMIT/ROLLBACK statements ─────

/// Wraps a live `&dyn Driver`, counting `begin_tx*` calls and each tx-control statement (via a
/// counting `TxConnection`). Proves N ops in one `transaction()` produce exactly ONE begin_tx / ONE
/// BEGIN / ONE COMMIT (or ROLLBACK) on ONE owned connection.
struct CountingDriver<'a> {
    inner: &'a dyn Driver,
    begin_tx_calls: Arc<AtomicU32>,
    commits: Arc<AtomicU32>,
    rollbacks: Arc<AtomicU32>,
}

impl<'a> CountingDriver<'a> {
    fn new(inner: &'a dyn Driver) -> Self {
        CountingDriver {
            inner,
            begin_tx_calls: Arc::new(AtomicU32::new(0)),
            commits: Arc::new(AtomicU32::new(0)),
            rollbacks: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl Driver for CountingDriver<'_> {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        self.inner.prepare(sql)
    }
    fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        self.begin_tx_isolated(&[], &[])
    }
    fn begin_tx_isolated(
        &self,
        before_begin: &[String],
        after_begin: &[String],
    ) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        let inner = self.inner.begin_tx_isolated(before_begin, after_begin)?;
        Ok(Box::new(CountingTx {
            inner,
            commits: self.commits.clone(),
            rollbacks: self.rollbacks.clone(),
        }))
    }
    // #93: the tx runtime acquires the owned connection here (NO BEGIN) then seam-issues BEGIN/COMMIT/
    // ROLLBACK — so this counts the ONE acquire (= one owned tx connection), and CountingTx::run counts
    // the seam-issued BEGIN/COMMIT/ROLLBACK by inspecting the SQL. Same intent (ONE begin / ONE commit
    // on ONE owned connection), now observed via the seam-routed path.
    fn acquire_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        self.begin_tx_calls.fetch_add(1, Ordering::SeqCst);
        let inner = self.inner.acquire_tx()?;
        Ok(Box::new(CountingTx {
            inner,
            commits: self.commits.clone(),
            rollbacks: self.rollbacks.clone(),
        }))
    }
}

struct CountingTx<'a> {
    inner: Box<dyn TxConnection + 'a>,
    commits: Arc<AtomicU32>,
    rollbacks: Arc<AtomicU32>,
}

impl TxConnection for CountingTx<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        self.inner.execute(sql, params)
    }
    fn run(&mut self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        // #93: COMMIT/ROLLBACK are now seam-issued through `run` on the owned connection — count them
        // HERE (the tx runtime no longer calls TxConnection::commit/rollback for the user boundary).
        let head = sql.trim_start().to_ascii_uppercase();
        if head.starts_with("COMMIT") {
            self.commits.fetch_add(1, Ordering::SeqCst);
        } else if head.starts_with("ROLLBACK") {
            self.rollbacks.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.run(sql, params)
    }
    fn release(self: Box<Self>, poison: bool) -> Result<(), SqlFailure> {
        self.inner.release(poison)
    }
    fn commit(self: Box<Self>) -> Result<(), SqlFailure> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        self.inner.commit()
    }
    fn rollback(self: Box<Self>) -> Result<(), SqlFailure> {
        self.rollbacks.fetch_add(1, Ordering::SeqCst);
        self.inner.rollback()
    }
}

// ── Bundle authoring ───────────────────────────────────────────────────────────

/// A single-INSERT (no RETURNING) bundle whose values come from the input scope.
fn insert_bundle(dialect: &str) -> Node {
    nj(&format!(
        r#"{{"dialect": "{dialect}", "transaction": {{
             "phase": "create", "entityFrom": null,
             "statements": [{{
               "id": "tx_body_0", "role": "body",
               "op": {{ "sql": "INSERT INTO {TBL} (id, val) VALUES (?, ?)",
                        "params": [{{"ref": ["id"]}}, {{"ref": ["val"]}}] }}
             }}]
           }}}}"#
    ))
}

fn input(id: i64, val: i64) -> Node {
    nj(&format!(r#"{{"id": {id}, "val": {val}}}"#))
}

// ── PG / MySQL connect + reset helpers ─────────────────────────────────────────

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
        format!("DROP TABLE IF EXISTS {TBL}"),
        format!("CREATE TABLE {TBL} (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)"),
    ])
    .expect("pg reset");
}

fn reset_mysql(d: &MysqlDriver) {
    d.exec_ddl(&[
        format!("DROP TABLE IF EXISTS {TBL}"),
        format!("CREATE TABLE {TBL} (id INT PRIMARY KEY, val INT NOT NULL)"),
    ])
    .expect("mysql reset");
}

/// Read all (id, val) rows (val != 999 filters any pre-seed), sorted by id.
fn read_rows(driver: &dyn Driver) -> Vec<(i64, i64)> {
    let rows = driver
        .prepare(&format!("SELECT id, val FROM {TBL} WHERE val <> 999"))
        .all(&[])
        .expect("read rows");
    let n = |v: Option<Value>| match v {
        Some(Value::Int(i)) => i,
        Some(Value::Float(f)) => f as i64,
        Some(Value::Str(s)) => s.parse().unwrap_or(-1),
        _ => -1,
    };
    let get = |r: &Value, k: &str| match r {
        Value::Obj(p) => p.iter().find(|(kk, _)| kk == k).map(|(_, v)| v.clone()),
        _ => None,
    };
    let mut out: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| (n(get(r, "id")), n(get(r, "val"))))
        .collect();
    out.sort();
    out
}

fn preseed(driver: &dyn Driver, id: i64) {
    driver
        .prepare(&format!("INSERT INTO {TBL} (id, val) VALUES ({id}, 999)"))
        .run(&[])
        .expect("preseed");
}

fn committed(v: &Value) -> bool {
    matches!(v, Value::Obj(p) if p.iter().any(|(k, val)| k == "committed" && matches!(val, Value::Bool(true))))
}

// ── (1) MULTI-OP ATOMICITY ─────────────────────────────────────────────────────

/// Two INSERTs inside ONE transaction() boundary commit together — with a call-counting driver
/// asserting exactly ONE begin_tx / ONE COMMIT / zero ROLLBACK. Then opB collides → opA ALSO rolls
/// back (ONE begin_tx / ONE ROLLBACK / zero COMMIT), verified by reading the DB.
fn multi_op_atomicity(base: &dyn Driver, dialect: &str) {
    let d = Dialect::parse(dialect).unwrap();

    // --- Happy path: both commit, one BEGIN + one COMMIT on one connection. ---
    let cd = CountingDriver::new(base);
    let ctx = for_driver(&cd);
    transaction(&ctx, d, &TransactionOptions::default(), |tx| {
        execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(1, 10), tx, true)?;
        execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(2, 20), tx, true)?;
        Ok(())
    })
    .expect("multi-op tx commits");
    assert_eq!(
        cd.begin_tx_calls.load(Ordering::SeqCst),
        1,
        "{dialect}: N ops = exactly ONE begin_tx (one connection)"
    );
    assert_eq!(
        cd.commits.load(Ordering::SeqCst),
        1,
        "{dialect}: exactly ONE COMMIT"
    );
    assert_eq!(
        cd.rollbacks.load(Ordering::SeqCst),
        0,
        "{dialect}: zero ROLLBACK"
    );
    assert_eq!(
        read_rows(base),
        vec![(1, 10), (2, 20)],
        "{dialect}: both rows committed"
    );

    // --- Rollback path: opB collides on a pre-seeded id → opA rolls back too. ---
    // Reset + pre-seed id=2 so opB's INSERT collides.
    base.prepare(&format!("DELETE FROM {TBL}"))
        .run(&[])
        .expect("clear");
    preseed(base, 2);

    let cd = CountingDriver::new(base);
    let ctx = for_driver(&cd);
    let res: Result<(), SqlFailure> = transaction(&ctx, d, &TransactionOptions::default(), |tx| {
        execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(1, 10), tx, true)?; // opA
        execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(2, 20), tx, true)?; // opB collides
        Ok(())
    });
    assert!(res.is_err(), "{dialect}: opB collision fails the whole tx");
    assert_eq!(
        cd.begin_tx_calls.load(Ordering::SeqCst),
        1,
        "{dialect}: still ONE begin_tx (one connection)"
    );
    assert_eq!(
        cd.commits.load(Ordering::SeqCst),
        0,
        "{dialect}: zero COMMIT"
    );
    assert_eq!(
        cd.rollbacks.load(Ordering::SeqCst),
        1,
        "{dialect}: exactly ONE ROLLBACK"
    );
    assert_eq!(
        read_rows(base),
        Vec::<(i64, i64)>::new(),
        "{dialect}: opA's id=1 ROLLED BACK when opB failed (cross-op atomicity)"
    );
}

// ── (2) GUARD ──────────────────────────────────────────────────────────────────

fn guard(base: &dyn Driver, dialect: &str) {
    let d = Dialect::parse(dialect).unwrap();
    let ctx = for_driver(base);

    // A write OUTSIDE any transaction() → WriteOutsideTransactionError, NO row written.
    let e = execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(1, 10), &ctx, true)
        .expect_err("write outside a transaction must be rejected");
    assert_eq!(
        e.kind, "write_outside_transaction",
        "{dialect}: bare write rejected"
    );
    assert_eq!(
        read_rows(base),
        Vec::<(i64, i64)>::new(),
        "{dialect}: no row leaked"
    );

    // A write in a READ-ONLY scope → WriteInReadOnlyContextError (read-only checked first).
    let e = with_transaction_decided(&ctx, |tx| {
        let ro = tx.with_read_only();
        let err = execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(1, 10), &ro, true)
            .expect_err("read-only write must be rejected");
        Ok::<_, SqlFailure>(TxDecision::Rollback(err))
    })
    .unwrap();
    assert_eq!(
        e.kind, "write_in_read_only_context",
        "{dialect}: read-only write rejected"
    );

    // A write INSIDE transaction() → succeeds.
    transaction(&ctx, d, &TransactionOptions::default(), |tx| {
        let r = execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(1, 10), tx, true)?;
        assert!(committed(&r), "{dialect}: in-tx write committed");
        Ok(())
    })
    .expect("in-tx write succeeds");
    assert_eq!(
        read_rows(base),
        vec![(1, 10)],
        "{dialect}: the guarded in-tx write persisted"
    );
}

// ── (3) ISOLATION (behavioral) ─────────────────────────────────────────────────

/// Under `repeatable read` a re-read inside one tx does NOT see a concurrent committed change; under
/// `read committed` it DOES. Real behavioral, driven through the production transaction() boundary.
fn isolation(base: &dyn Driver, dialect: &str) {
    let d = Dialect::parse(dialect).unwrap();
    // Seed id=1 with val=100.
    base.prepare(&format!("INSERT INTO {TBL} (id, val) VALUES (1, 100)"))
        .run(&[])
        .expect("seed");

    // Helper: read val inside a tx via the read seam.
    fn read_val_in_tx(tx: &litedbmodel_interpreter::ExecutionContext) -> i64 {
        let rows = litedbmodel_interpreter::exec_context::execute(
            tx,
            &format!("SELECT val FROM {TBL} WHERE id = 1"),
            &[],
            &StatementIntent::read(),
        )
        .expect("read val");
        match rows.first() {
            Some(Value::Obj(p)) => match p.iter().find(|(k, _)| k == "val").map(|(_, v)| v) {
                Some(Value::Int(i)) => *i,
                Some(Value::Float(f)) => *f as i64,
                Some(Value::Str(s)) => s.parse().unwrap_or(-1),
                _ => -1,
            },
            _ => -1,
        }
    }

    // REPEATABLE READ: read val (100), a concurrent autocommit UPDATE to 200 commits, re-read → still
    // 100 (snapshot held).
    let ctx = for_driver(base);
    let observed = RefCell::new((0i64, 0i64));
    transaction(
        &ctx,
        d,
        &TransactionOptions {
            isolation: Some(IsolationLevel::RepeatableRead),
            ..Default::default()
        },
        |tx| {
            let first = read_val_in_tx(tx);
            // A concurrent committed change on a SEPARATE connection (the base driver autocommits).
            base.prepare(&format!("UPDATE {TBL} SET val = 200 WHERE id = 1"))
                .run(&[])
                .expect("concurrent update");
            let second = read_val_in_tx(tx);
            *observed.borrow_mut() = (first, second);
            Ok(())
        },
    )
    .expect("repeatable-read tx");
    let (first, second) = *observed.borrow();
    assert_eq!(first, 100, "{dialect}: repeatable-read first read = 100");
    assert_eq!(
        second, 100,
        "{dialect}: repeatable-read re-read does NOT see the concurrent commit (still 100)"
    );

    // Reset to a known state: id=1 val=100 (the concurrent update left it at 200).
    base.prepare(&format!("UPDATE {TBL} SET val = 100 WHERE id = 1"))
        .run(&[])
        .expect("reset val");

    // READ COMMITTED: read val (100), a concurrent commit to 300, re-read → 300 (sees the commit).
    let observed = RefCell::new((0i64, 0i64));
    transaction(
        &ctx,
        d,
        &TransactionOptions {
            isolation: Some(IsolationLevel::ReadCommitted),
            ..Default::default()
        },
        |tx| {
            let first = read_val_in_tx(tx);
            base.prepare(&format!("UPDATE {TBL} SET val = 300 WHERE id = 1"))
                .run(&[])
                .expect("concurrent update 2");
            let second = read_val_in_tx(tx);
            *observed.borrow_mut() = (first, second);
            Ok(())
        },
    )
    .expect("read-committed tx");
    let (first, second) = *observed.borrow();
    assert_eq!(first, 100, "{dialect}: read-committed first read = 100");
    assert_eq!(
        second, 300,
        "{dialect}: read-committed re-read DOES see the concurrent commit (300)"
    );
}

// ── (4) RETRY (real contention) ────────────────────────────────────────────────

/// Two concurrent SERIALIZABLE (PG) / write-locking (MySQL) txs contend on the same rows; the loser
/// hits a retryable error (PG 40001 write-skew / MySQL 1213 deadlock) and the transaction() retry
/// loop re-runs it → BOTH ultimately commit. Proven by both txs returning Ok and both effects landing.
fn retry_real_contention(base: &(dyn Driver + Sync), dialect: &str) {
    let d = Dialect::parse(dialect).unwrap();
    // Seed two rows.
    base.prepare(&format!(
        "INSERT INTO {TBL} (id, val) VALUES (1, 0), (2, 0)"
    ))
    .run(&[])
    .expect("seed rows");

    // Each worker, under SERIALIZABLE, reads BOTH rows then writes its own row = sum — classic
    // write-skew that PG SERIALIZABLE rejects with 40001 (one worker retries). MySQL uses SELECT …
    // FOR UPDATE to force a lock-ordering deadlock (1213) between the two workers.
    let opts = TransactionOptions {
        isolation: Some(IsolationLevel::Serializable),
        retry_on_error: true,
        retry_limit: 10,
        retry_duration_ms: 5,
        ..Default::default()
    };

    let work = |base: &(dyn Driver + Sync), my_id: i64, other_id: i64| -> Result<(), SqlFailure> {
        let ctx = for_driver(base);
        transaction(&ctx, d, &opts, |tx| {
            // Lock/read both rows (order differs per worker → deadlock on MySQL).
            let lock = if dialect == "mysql" {
                " FOR UPDATE"
            } else {
                ""
            };
            litedbmodel_interpreter::exec_context::execute(
                tx,
                &format!("SELECT val FROM {TBL} WHERE id = {my_id}{lock}"),
                &[],
                &StatementIntent::read(),
            )?;
            litedbmodel_interpreter::exec_context::execute(
                tx,
                &format!("SELECT val FROM {TBL} WHERE id = {other_id}{lock}"),
                &[],
                &StatementIntent::read(),
            )?;
            run(
                tx,
                &format!("UPDATE {TBL} SET val = val + 1 WHERE id = {my_id}"),
                &[],
                &StatementIntent::write(),
            )?;
            Ok(())
        })
    };

    let r = std::thread::scope(|s| {
        let h1 = s.spawn(|| work(base, 1, 2));
        let h2 = s.spawn(|| work(base, 2, 1));
        (h1.join().unwrap(), h2.join().unwrap())
    });
    assert!(
        r.0.is_ok() && r.1.is_ok(),
        "{dialect}: both contending txs commit (the loser retried): {:?} / {:?}",
        r.0.as_ref().err().map(|e| &e.message),
        r.1.as_ref().err().map(|e| &e.message),
    );
    // Both effects landed: id=1 and id=2 each incremented to 1.
    assert_eq!(
        read_rows(base),
        vec![(1, 1), (2, 1)],
        "{dialect}: both increments committed after retry"
    );

    // A NON-retryable error (PK collision) does NOT retry: pre-seed id=5, one tx tries to insert id=5.
    base.prepare(&format!("DELETE FROM {TBL}"))
        .run(&[])
        .expect("clear");
    preseed(base, 5);
    let attempts = AtomicU32::new(0);
    let ctx = for_driver(base);
    let res: Result<(), SqlFailure> = transaction(&ctx, d, &opts, |tx| {
        attempts.fetch_add(1, Ordering::SeqCst);
        execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(5, 50), tx, true)?;
        Ok(())
    });
    assert!(res.is_err(), "{dialect}: PK collision fails");
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "{dialect}: a non-retryable (unique-violation) error is NOT retried"
    );
}

// ── (5) NESTED ─────────────────────────────────────────────────────────────────

fn nested(base: &dyn Driver, dialect: &str) {
    let d = Dialect::parse(dialect).unwrap();

    // Nested transaction() = ONE begin_tx / ONE COMMIT (the inner JOINS the outer).
    let cd = CountingDriver::new(base);
    let ctx = for_driver(&cd);
    transaction(&ctx, d, &TransactionOptions::default(), |tx| {
        execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(1, 10), tx, true)?;
        transaction(tx, d, &TransactionOptions::default(), |inner| {
            execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(2, 20), inner, true)?;
            Ok(())
        })?;
        Ok(())
    })
    .expect("nested tx commits");
    assert_eq!(
        cd.begin_tx_calls.load(Ordering::SeqCst),
        1,
        "{dialect}: nested = ONE begin_tx (inner joined)"
    );
    assert_eq!(
        cd.commits.load(Ordering::SeqCst),
        1,
        "{dialect}: ONE COMMIT"
    );
    assert_eq!(
        read_rows(base),
        vec![(1, 10), (2, 20)],
        "{dialect}: both rows committed"
    );

    // An inner error rolls back the WHOLE tx (outer op included).
    base.prepare(&format!("DELETE FROM {TBL}"))
        .run(&[])
        .expect("clear");
    preseed(base, 2); // inner op will collide on id=2

    let cd = CountingDriver::new(base);
    let ctx = for_driver(&cd);
    let res: Result<(), SqlFailure> = transaction(&ctx, d, &TransactionOptions::default(), |tx| {
        execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(1, 10), tx, true)?; // outer op
        transaction(tx, d, &TransactionOptions::default(), |inner| {
            execute_transaction_bundle_ctx(&insert_bundle(dialect), &input(2, 20), inner, true)?; // collides
            Ok(())
        })?;
        Ok(())
    });
    assert!(res.is_err(), "{dialect}: inner error propagates");
    assert_eq!(
        cd.commits.load(Ordering::SeqCst),
        0,
        "{dialect}: zero COMMIT"
    );
    assert_eq!(
        cd.rollbacks.load(Ordering::SeqCst),
        1,
        "{dialect}: ONE ROLLBACK"
    );
    assert_eq!(
        read_rows(base),
        Vec::<(i64, i64)>::new(),
        "{dialect}: the outer op's id=1 rolled back with the inner failure (whole-tx atomicity)"
    );
}

// ── Test entrypoints ────────────────────────────────────────────────────────────

#[test]
fn pg_tx_boundary() {
    if !enabled() {
        eprintln!("skipped (set LITEDBMODEL_TX_BOUNDARY=1 + docker up)");
        return;
    }
    let d = pg();
    reset_pg(&d);
    multi_op_atomicity(&d, "postgres");
    reset_pg(&d);
    guard(&d, "postgres");
    reset_pg(&d);
    isolation(&d, "postgres");
    reset_pg(&d);
    retry_real_contention(&d, "postgres");
    reset_pg(&d);
    nested(&d, "postgres");
    eprintln!("PG TX-BOUNDARY PROOF: multi-op atomicity + guard + isolation + real-contention retry + nested all green");
}

#[test]
fn mysql_tx_boundary() {
    if !enabled() {
        eprintln!("skipped (set LITEDBMODEL_TX_BOUNDARY=1 + docker up)");
        return;
    }
    let d = mysql();
    reset_mysql(&d);
    multi_op_atomicity(&d, "mysql");
    reset_mysql(&d);
    guard(&d, "mysql");
    reset_mysql(&d);
    isolation(&d, "mysql");
    reset_mysql(&d);
    retry_real_contention(&d, "mysql");
    reset_mysql(&d);
    nested(&d, "mysql");
    eprintln!("MYSQL TX-BOUNDARY PROOF: multi-op atomicity + guard + isolation + real-contention retry + nested all green");
}
