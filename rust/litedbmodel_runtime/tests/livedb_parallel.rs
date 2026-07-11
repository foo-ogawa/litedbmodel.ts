//! #40 — LIVE-DB proof: the async+pool PG/MySQL drivers run independent sibling reads CONCURRENTLY
//! against REAL Postgres + MySQL.
//!
//! Gated behind the `livedb` feature AND `LITEDBMODEL_LIVEDB_PARALLEL=1` so it never runs in the
//! default `cargo test` (which has no DBs). It connects to the dockerized PG/MySQL (the same ports
//! `docker-compose.livedb.yml` publishes), then dispatches N sibling relation queries via
//! `dispatch_read_nodes_parallel`. Each sibling query blocks the connection server-side
//! (`pg_sleep` / `SLEEP`), so if the pool truly runs them on distinct connections concurrently the
//! wall time is ≈ one sleep, not N sleeps — proving the plan's `concurrency` cashes out as REAL
//! parallel DB I/O.
//!
//!   cargo test -p litedbmodel_runtime --features livedb --test livedb_parallel -- --nocapture
//!   (with LITEDBMODEL_LIVEDB_PARALLEL=1 + the docker DBs up)

#![cfg(feature = "livedb")]

use std::time::{Duration, Instant};

use litedbmodel_runtime::{dispatch_read_nodes_parallel, MysqlDriver, PostgresDriver, Scope};
use serde_json::json;

fn enabled() -> bool {
    std::env::var("LITEDBMODEL_LIVEDB_PARALLEL").as_deref() == Ok("1")
}

fn env(k: &str, d: &str) -> String {
    std::env::var(k)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| d.to_string())
}

/// A read graph of N sibling nodes, each a query that sleeps `sleep_expr` server-side then SELECTs.
fn sleepy_graph(n: usize, sleep_expr: &str) -> serde_json::Value {
    let mut statements = serde_json::Map::new();
    for i in 0..n {
        statements.insert(
            format!("rel{i}"),
            json!([{ "sql": format!("SELECT {sleep_expr} AS slept, {i} AS v"), "params": [] }]),
        );
    }
    json!({ "dialect": "sqlite", "statementsById": statements })
}

fn nodes(n: usize) -> Vec<(String, Scope)> {
    (0..n).map(|i| (format!("rel{i}"), Vec::new())).collect()
}

#[test]
fn pg_sibling_reads_run_concurrently() {
    if !enabled() {
        eprintln!("skipped (set LITEDBMODEL_LIVEDB_PARALLEL=1 + docker up)");
        return;
    }
    let conn = format!(
        "host={} port={} user={} password={} dbname={}",
        env("TEST_DB_HOST", "localhost"),
        env("TEST_DB_PORT", "5433"),
        env("TEST_DB_USER", "testuser"),
        env("TEST_DB_PASSWORD", "testpass"),
        env("TEST_DB_NAME", "testdb"),
    );
    let pg = PostgresDriver::connect(&conn).expect("pg connect");
    const N: usize = 8;
    // Each sibling sleeps 0.3s server-side (pg_sleep returns void → cast to text for a stable row).
    let graph = sleepy_graph(N, "pg_sleep(0.3)::text");
    let t0 = Instant::now();
    let rows =
        dispatch_read_nodes_parallel(&pg, &graph, "postgres", &nodes(N), 16).expect("pg dispatch");
    let elapsed = t0.elapsed();
    assert_eq!(rows.len(), N);
    // Serial would be 8 × 0.3 = 2.4s; concurrent ≈ 0.3s. Under 1s proves real parallel connections.
    assert!(
        elapsed < Duration::from_millis(1200),
        "PG not concurrent: {elapsed:?}"
    );
    eprintln!("LIVE PG PARALLEL PROOF: {N} sibling pg_sleep(0.3) queries → wall {elapsed:?} (serial would be ~2.4s)");
}

#[test]
fn mysql_sibling_reads_run_concurrently() {
    if !enabled() {
        eprintln!("skipped (set LITEDBMODEL_LIVEDB_PARALLEL=1 + docker up)");
        return;
    }
    let url = format!(
        "mysql://{}:{}@{}:{}/{}",
        env("TEST_MYSQL_USER", "testuser"),
        env("TEST_MYSQL_PASSWORD", "testpass"),
        env("TEST_MYSQL_HOST", "127.0.0.1"),
        env("TEST_MYSQL_PORT", "3307"),
        env("TEST_MYSQL_DB", "testdb"),
    );
    let my = MysqlDriver::connect(&url).expect("mysql connect");
    const N: usize = 8;
    let graph = sleepy_graph(N, "SLEEP(0.3)");
    let t0 = Instant::now();
    let rows =
        dispatch_read_nodes_parallel(&my, &graph, "mysql", &nodes(N), 16).expect("mysql dispatch");
    let elapsed = t0.elapsed();
    assert_eq!(rows.len(), N);
    assert!(
        elapsed < Duration::from_millis(1200),
        "MySQL not concurrent: {elapsed:?}"
    );
    eprintln!("LIVE MYSQL PARALLEL PROOF: {N} sibling SLEEP(0.3) queries → wall {elapsed:?} (serial would be ~2.4s)");
}
