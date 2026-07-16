//! Phase D (#93) — the SCP MIDDLEWARE layer on the LIVE PG seam (the rust mirror of the TS
//! `test/integration/ScpMiddleware.test.ts`).
//!
//! The lib unit suite (`src/middleware.rs` tests) proves the hook mechanics on the sync in-proc
//! rusqlite seam; THIS test proves the SAME contract on the PRODUCTION live seam against a REAL
//! Postgres, so a registered middleware + Logger + raw execute/query all compose with the async
//! pooled driver (the `PostgresDriver` impls the sync `Driver` seam, blocking on its tokio runtime;
//! `seam_execute`/`seam_run` fold the ambient middleware exactly as in-proc):
//!
//!   D1 a registered SQL middleware intercepts EVERY seam statement (body read/write), and per-scope
//!      isolation holds (two THREADS in a `with_middleware_scope` body don't cross-talk). RED:
//!      unregister ⇒ the interception assertion goes empty.
//!   D3 raw `raw_execute`/`raw_query` go THROUGH the seam — a registered middleware sees them; the
//!      Logger records real SQL/params/timing for a live statement.
//!
//! Gated behind the `livedb` feature AND `LITEDBMODEL_LIVEDB_PARALLEL=1` (same switch as
//! `livedb_parallel`) so it never runs in the default `cargo test`. Namespaces its table UNIQUELY for
//! rust (`scp_mw_rust`) so it does not collide with the other ports running the same docker PG:5433.
//!
//!   npm run docker:livedb:up
//!   LITEDBMODEL_LIVEDB_PARALLEL=1 cargo test -p litedbmodel_runtime --features livedb --test livedb_middleware -- --nocapture

#![cfg(feature = "livedb")]

use std::sync::{Arc, Mutex};

use behavior_contracts::Value;
use litedbmodel_runtime::{
    create_middleware, for_driver, logger, raw_execute, raw_query, seam_execute, seam_run,
    use_middleware, with_middleware_scope, PostgresDriver, SeamResult, SqlFailure, SqlHookFn,
    SqlNext, StatementIntent,
};

// The rust-namespaced live table PREFIX is `scp_mw_rust_*` (must not collide with the go/py/php ports
// on the shared PG). Each test appends a unique suffix so the 4 tests in THIS suite never race on one
// table.

fn enabled() -> bool {
    std::env::var("LITEDBMODEL_LIVEDB_PARALLEL").as_deref() == Ok("1")
}

fn env(k: &str, d: &str) -> String {
    std::env::var(k)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| d.to_string())
}

fn connect() -> PostgresDriver {
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

fn reset(db: &PostgresDriver, tbl: &str) {
    // DDL through the seam terminal (no middleware registered here). Each test passes its OWN
    // rust-unique table so the 4 tests in this suite never race on one table (they run in parallel).
    let ctx = for_driver(db);
    seam_run(
        &ctx,
        &format!("DROP TABLE IF EXISTS {tbl}"),
        &[],
        &StatementIntent::write(),
    )
    .unwrap();
    seam_run(
        &ctx,
        &format!("CREATE TABLE {tbl} (id INTEGER PRIMARY KEY, val TEXT NOT NULL)"),
        &[],
        &StatementIntent::write(),
    )
    .unwrap();
}

fn observer(
    log: Arc<Mutex<Vec<String>>>,
) -> SqlHookFn<impl Fn(&str, &[Value], &SqlNext) -> Result<SeamResult, SqlFailure> + Send + Sync> {
    SqlHookFn(move |sql: &str, params: &[Value], next: &SqlNext| {
        log.lock().unwrap().push(sql.to_string());
        next(sql, params)
    })
}

#[test]
fn d1_live_middleware_intercepts_every_seam_statement() {
    if !enabled() {
        eprintln!("skipped (set LITEDBMODEL_LIVEDB_PARALLEL=1 + docker up)");
        return;
    }
    const TBL: &str = "scp_mw_rust_d1";
    let db = connect();
    reset(&db, TBL);
    let ctx = for_driver(&db);
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen2 = seen.clone();
    with_middleware_scope(|| {
        let mw = create_middleware::<(), _, fn() -> ()>(Some(observer(seen2)), None);
        use_middleware(&mw);
        seam_run(
            &ctx,
            &format!("INSERT INTO {TBL} (id, val) VALUES ($1, $2)"),
            &[Value::Int(1), Value::Str("a".into())],
            &StatementIntent::write(),
        )
        .unwrap();
        let rows = seam_execute(
            &ctx,
            &format!("SELECT val FROM {TBL} WHERE id = $1"),
            &[Value::Int(1)],
            &StatementIntent::read(),
        )
        .unwrap();
        assert!(format!("{rows:?}").contains('a'));
    });
    let observed = seen.lock().unwrap().clone();
    assert!(
        observed.iter().any(|s| s.contains("INSERT INTO")),
        "INSERT observed: {observed:?}"
    );
    assert!(
        observed.iter().any(|s| s.contains("SELECT val")),
        "SELECT observed: {observed:?}"
    );
}

#[test]
fn d1_red_live_without_registration_nothing_observed() {
    if !enabled() {
        eprintln!("skipped");
        return;
    }
    const TBL: &str = "scp_mw_rust_red";
    let db = connect();
    reset(&db, TBL);
    let ctx = for_driver(&db);
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    // No registration → byte-identical passthrough, the observer never fires.
    seam_run(
        &ctx,
        &format!("INSERT INTO {TBL} (id, val) VALUES ($1, $2)"),
        &[Value::Int(2), Value::Str("b".into())],
        &StatementIntent::write(),
    )
    .unwrap();
    seam_execute(
        &ctx,
        &format!("SELECT val FROM {TBL} WHERE id = $1"),
        &[Value::Int(2)],
        &StatementIntent::read(),
    )
    .unwrap();
    assert!(seen.lock().unwrap().is_empty());
}

#[test]
fn d1_live_concurrent_scope_isolation() {
    if !enabled() {
        eprintln!("skipped");
        return;
    }
    // Two THREADS each in a `with_middleware_scope` body must not see each other's middleware — the
    // thread_local REGISTRY_SCOPE reproduces the TS concurrent-scope isolation on the live seam.
    fn scope(tag: &'static str) -> Vec<String> {
        let db = connect();
        let ctx = for_driver(&db);
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let seen2 = seen.clone();
        let tagc = tag.to_string();
        with_middleware_scope(|| {
            let mw = create_middleware::<(), _, fn() -> ()>(
                Some(SqlHookFn(
                    move |sql: &str, params: &[Value], next: &SqlNext| {
                        seen2.lock().unwrap().push(format!("{tagc}:{sql}"));
                        next(sql, params)
                    },
                )),
                None,
            );
            use_middleware(&mw);
            std::thread::sleep(std::time::Duration::from_millis(if tag == "A" {
                15
            } else {
                1
            }));
            seam_execute(
                &ctx,
                if tag == "A" { "SELECT 1" } else { "SELECT 2" },
                &[],
                &StatementIntent::read(),
            )
            .unwrap();
        });
        Arc::try_unwrap(seen).unwrap().into_inner().unwrap()
    }
    let ha = std::thread::spawn(|| scope("A"));
    let hb = std::thread::spawn(|| scope("B"));
    let (sa, sb) = (ha.join().unwrap(), hb.join().unwrap());
    assert_eq!(sa, vec!["A:SELECT 1".to_string()]);
    assert_eq!(sb, vec!["B:SELECT 2".to_string()]);
}

#[test]
fn d3_live_raw_execute_query_through_seam_and_logger() {
    if !enabled() {
        eprintln!("skipped");
        return;
    }
    const TBL: &str = "scp_mw_rust_d3";
    let db = connect();
    reset(&db, TBL);
    let ctx = for_driver(&db);
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen2 = seen.clone();
    with_middleware_scope(|| {
        let mw = create_middleware::<(), _, fn() -> ()>(Some(observer(seen2)), None);
        use_middleware(&mw);
        let ins = raw_execute(
            &ctx,
            &format!("INSERT INTO {TBL} (id, val) VALUES ($1, $2)"),
            &[Value::Int(3), Value::Str("c".into())],
            true,
        )
        .unwrap();
        assert_eq!(ins.row_count, Some(1));
        let rows = raw_query(
            &ctx,
            &format!("SELECT val FROM {TBL} WHERE id = $1"),
            &[Value::Int(3)],
        )
        .unwrap();
        assert!(format!("{rows:?}").contains('c'));
    });
    assert_eq!(
        seen.lock().unwrap().len(),
        2,
        "raw execute + raw query both funneled through the seam: {:?}",
        seen.lock().unwrap()
    );

    // Logger records SQL/params/timing for a live statement.
    reset(&db, TBL);
    with_middleware_scope(|| {
        let lg = logger();
        use_middleware(&lg);
        seam_run(
            &ctx,
            &format!("INSERT INTO {TBL} (id, val) VALUES ($1, $2)"),
            &[Value::Int(4), Value::Str("d".into())],
            &StatementIntent::write(),
        )
        .unwrap();
        seam_execute(
            &ctx,
            &format!("SELECT val FROM {TBL} WHERE id = $1"),
            &[Value::Int(4)],
            &StatementIntent::read(),
        )
        .unwrap();
        let entries = lg.state().unwrap().entries();
        assert_eq!(entries.len(), 2, "two statements logged");
        assert!(entries[0].sql.contains("INSERT INTO"));
        assert!(entries[1].sql.contains("SELECT val"));
    });
}
