//! Phase C (#88, rust) — CONNECTION ROUTING + CONFIG on live PG + MySQL.
//!
//! The rust mirror of #87's `test/integration/ConnectionRouting.test.ts`. It proves the completion of
//! `connection_for(intent)`'s resolution (design §3 steps 2-4) on the Phase A/B exec-context seam,
//! against REAL databases (PG:5433 + MySQL:3307). Every live assertion has a faithful-mutation RED
//! proof (break the routing/sticky/timeout/cap ⇒ the assertion goes RED) — see the `MUTATION:` blocks.
//!
//!   C1 reader/writer separation + writer-sticky (injectable clock) + with_writer
//!   C2 multi-DB connection registry + name→connection routing (PG = A, MySQL = B) + tx-pin precedence
//!   C3 setConfig: query_timeout (server statement timeout), maxPool SOLE cap, session reset-on-release
//!
//! rust-namespaced to avoid colliding with the parallel language ports sharing docker PG:5433 +
//! MySQL:3307: PG uses the schema `phase_c_routing_rust` (tables qualified), MySQL uses the `scp_rust`
//! database. Gated behind the `livedb` feature AND `LITEDBMODEL_PHASE_C=1` so the default `cargo test`
//! (no DBs) never runs it. Bring up + run:
//!   docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d postgres mysql
//!   LITEDBMODEL_PHASE_C=1 cargo test -p litedbmodel_runtime --features livedb \
//!     --test connection_routing -- --nocapture --test-threads=1

#![cfg(feature = "livedb")]

use std::sync::{Arc, Mutex};

use behavior_contracts::Value;
use litedbmodel_runtime::{
    build_routing_config, for_routing, seam_execute, seam_run, transaction_on, ConfigDialect,
    ConnectionConfig, ConnectionRegistry, ConnectionSetup, Driver, ManualClock, MysqlDriver,
    PostgresDriver, PreparedStatement, ReaderWriterPools, RoutingConfig, SessionConnection,
    StatementIntent, StickyOptions, TransactionOptions, TxConnection, TxDialect, WriterStickyClock,
};

fn enabled() -> bool {
    std::env::var("LITEDBMODEL_PHASE_C").as_deref() == Ok("1")
}

fn env(k: &str, d: &str) -> String {
    std::env::var(k)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| d.to_string())
}

// PG: a rust-specific SCHEMA so the table name never collides with the other ports on the shared PG.
const PG_SCHEMA: &str = "phase_c_routing_rust";
// MySQL: the rust-owned database (testuser has full rights; the go/py/php/ts ports use their own DBs).
const MYSQL_DB: &str = "scp_rust";
const TBL: &str = "scp_route_rust";

fn pg_tbl() -> String {
    format!("{PG_SCHEMA}.{TBL}")
}

// ── Recording driver: logs a label on every connection checkout (prepare / begin_tx / session) ──

/// A [`Driver`] wrapper that records a `label` on every statement/connection checkout into a shared
/// log, delegating to a real driver — the rust analogue of the TS `recordingPool`. Lets a test assert
/// WHICH pool (reader vs writer vs DB-A vs DB-B) served each statement. It labels on `prepare` (the
/// non-tx per-statement path), `begin_tx*` (a tx checkout), and `session_connection` (a configured
/// checkout) — the three ways the seam reaches a connection.
struct RecordingDriver {
    inner: Arc<dyn Driver + Send + Sync>,
    label: &'static str,
    log: Arc<Mutex<Vec<String>>>,
}

impl RecordingDriver {
    fn wrap(
        inner: Arc<dyn Driver + Send + Sync>,
        label: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    ) -> Arc<dyn Driver + Send + Sync> {
        Arc::new(RecordingDriver { inner, label, log })
    }
    fn record(&self) {
        self.log.lock().unwrap().push(self.label.to_string());
    }
}

impl Driver for RecordingDriver {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        self.record();
        self.inner.prepare(sql)
    }
    fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, litedbmodel_runtime::SqlFailure> {
        self.record();
        self.inner.begin_tx()
    }
    fn begin_tx_isolated(
        &self,
        before: &[String],
        after: &[String],
    ) -> Result<Box<dyn TxConnection + '_>, litedbmodel_runtime::SqlFailure> {
        self.record();
        self.inner.begin_tx_isolated(before, after)
    }
    fn acquire_tx(&self) -> Result<Box<dyn TxConnection + '_>, litedbmodel_runtime::SqlFailure> {
        self.record();
        self.inner.acquire_tx()
    }
    fn session_connection(
        &self,
        setup: &[String],
        reset: &[String],
    ) -> Result<Box<dyn SessionConnection + '_>, litedbmodel_runtime::SqlFailure> {
        self.record();
        self.inner.session_connection(setup, reset)
    }
}

// ── PG / MySQL connect helpers ─────────────────────────────────────────────────

fn pg_driver() -> Arc<dyn Driver + Send + Sync> {
    let conn = format!(
        "host={} port={} user={} password={} dbname={}",
        env("TEST_DB_HOST", "localhost"),
        env("TEST_DB_PORT", "5433"),
        env("TEST_DB_USER", "testuser"),
        env("TEST_DB_PASSWORD", "testpass"),
        env("TEST_DB_NAME", "testdb"),
    );
    Arc::new(PostgresDriver::connect(&conn).expect("pg connect"))
}

fn mysql_driver() -> Arc<dyn Driver + Send + Sync> {
    let url = format!(
        "mysql://{}:{}@{}:{}/{}",
        env("TEST_MYSQL_USER", "testuser"),
        env("TEST_MYSQL_PASSWORD", "testpass"),
        env("TEST_MYSQL_HOST", "127.0.0.1"),
        env("TEST_MYSQL_PORT", "3307"),
        MYSQL_DB,
    );
    Arc::new(MysqlDriver::connect(&url).expect("mysql connect"))
}

fn reset_pg() {
    let conn = format!(
        "host={} port={} user={} password={} dbname={}",
        env("TEST_DB_HOST", "localhost"),
        env("TEST_DB_PORT", "5433"),
        env("TEST_DB_USER", "testuser"),
        env("TEST_DB_PASSWORD", "testpass"),
        env("TEST_DB_NAME", "testdb"),
    );
    let d = PostgresDriver::connect(&conn).expect("pg connect");
    d.exec_ddl(&[
        format!("CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA}"),
        format!("DROP TABLE IF EXISTS {}", pg_tbl()),
        format!(
            "CREATE TABLE {} (id INTEGER PRIMARY KEY, val TEXT NOT NULL)",
            pg_tbl()
        ),
    ])
    .expect("pg reset");
}

fn reset_mysql() {
    let url = format!(
        "mysql://{}:{}@{}:{}/{}",
        env("TEST_MYSQL_USER", "testuser"),
        env("TEST_MYSQL_PASSWORD", "testpass"),
        env("TEST_MYSQL_HOST", "127.0.0.1"),
        env("TEST_MYSQL_PORT", "3307"),
        MYSQL_DB,
    );
    let d = MysqlDriver::connect(&url).expect("mysql connect");
    d.exec_ddl(&[
        format!("DROP TABLE IF EXISTS {TBL}"),
        format!("CREATE TABLE {TBL} (id INT PRIMARY KEY, val TEXT NOT NULL)"),
    ])
    .expect("mysql reset");
}

fn log() -> Arc<Mutex<Vec<String>>> {
    Arc::new(Mutex::new(Vec::new()))
}
fn snapshot(l: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    l.lock().unwrap().clone()
}
fn n_of(v: &Value, key: &str) -> i64 {
    match v {
        Value::Obj(p) => p
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, val)| match val {
                Value::Int(i) => *i,
                Value::Float(f) => *f as i64,
                Value::Str(s) => s.parse().unwrap_or(-1),
                _ => -1,
            })
            .unwrap_or(-1),
        _ => -1,
    }
}
fn str_of(v: &Value, key: &str) -> Option<String> {
    match v {
        Value::Obj(p) => p
            .iter()
            .find(|(k, _)| k == key)
            .and_then(|(_, val)| match val {
                Value::Str(s) => Some(s.clone()),
                _ => None,
            }),
        _ => None,
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// C1 — reader/writer separation + writer-sticky + with_writer
// ════════════════════════════════════════════════════════════════════════════════

/// A read routes to the READER pool, a write to the WRITER pool — recording drivers capture the split.
fn c1_reader_writer_split() {
    let l = log();
    // TWO distinct recording drivers over the SAME live PG (a real replica split targets replicas; the
    // recording label proves the SELECTION regardless). Both real → the SQL actually executes.
    let reader = RecordingDriver::wrap(pg_driver(), "reader", l.clone());
    let writer = RecordingDriver::wrap(pg_driver(), "writer", l.clone());
    let routing = RoutingConfig {
        registry: ConnectionRegistry::from_default(ReaderWriterPools::split(reader, writer))
            .build()
            .unwrap(),
        sticky: WriterStickyClock::disabled(),
    };
    let ctx = for_routing(&routing).unwrap();

    // A plain READ (no tx, no sticky, no with_writer) → reader.
    let rows = seam_execute(&ctx, "SELECT 1 AS one", &[], &StatementIntent::read()).unwrap();
    assert_eq!(n_of(&rows[0], "one"), 1);
    // A WRITE → writer.
    seam_run(
        &ctx,
        &format!(
            "INSERT INTO {} (id, val) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            pg_tbl()
        ),
        &[Value::Int(1), Value::Str("a".into())],
        &StatementIntent::write(),
    )
    .unwrap();
    assert_eq!(snapshot(&l), vec!["reader", "writer"]);

    // MUTATION (RED proof) — collapse the reader/writer split to ONE driver (the faithful "separation
    // deleted" mutation) and re-run the SAME read+write through the SAME seam. Both statements now land
    // on the one driver ⇒ ['solo','solo'], NOT ['reader','writer'].
    let ml = log();
    let solo = RecordingDriver::wrap(pg_driver(), "solo", ml.clone());
    let mrouting = RoutingConfig {
        registry: ConnectionRegistry::from_default(ReaderWriterPools::single(solo))
            .build()
            .unwrap(),
        sticky: WriterStickyClock::disabled(),
    };
    let mctx = for_routing(&mrouting).unwrap();
    seam_execute(&mctx, "SELECT 1 AS one", &[], &StatementIntent::read()).unwrap();
    seam_run(
        &mctx,
        &format!(
            "INSERT INTO {} (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING",
            pg_tbl()
        ),
        &[Value::Int(1), Value::Str("a".into())],
        &StatementIntent::write(),
    )
    .unwrap();
    assert_eq!(
        snapshot(&ml),
        vec!["solo", "solo"],
        "split removed ⇒ read no longer distinguishable from write (the split was load-bearing)"
    );
    println!("C1 reader/writer split: green + RED proof");
}

/// Writer-sticky: after a committed tx, a read within the window routes to the WRITER (read-your-
/// writes), then back to the READER after it elapses — deterministic via the injectable ManualClock.
fn c1_writer_sticky() {
    let l = log();
    let clock = Arc::new(ManualClock::new(1_000_000));
    let reader = RecordingDriver::wrap(pg_driver(), "reader", l.clone());
    let writer = RecordingDriver::wrap(pg_driver(), "writer", l.clone());
    let routing = RoutingConfig {
        registry: ConnectionRegistry::from_default(ReaderWriterPools::split(reader, writer))
            .build()
            .unwrap(),
        sticky: WriterStickyClock::new(StickyOptions {
            use_writer_after_transaction: true,
            writer_sticky_duration: 5000,
            clock: clock.clone(),
        }),
    };
    let ctx = for_routing(&routing).unwrap();

    // Read BEFORE any tx → reader (sticky not armed).
    seam_execute(&ctx, "SELECT 1", &[], &StatementIntent::read()).unwrap();
    assert_eq!(snapshot(&l).last().unwrap(), "reader");

    // Commit a tx → arms the sticky clock (records last_write_at = clock).
    transaction_on(
        &ctx,
        None,
        TxDialect::Postgres,
        &TransactionOptions::default(),
        |tx| {
            seam_run(
                tx,
                &format!(
                    "INSERT INTO {} (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING",
                    pg_tbl()
                ),
                &[Value::Int(2), Value::Str("b".into())],
                &StatementIntent::write(),
            )?;
            Ok(())
        },
    )
    .unwrap();

    // Read 100ms later (within the 5s window) → WRITER (read-your-writes).
    clock.advance(100);
    seam_execute(&ctx, "SELECT 1", &[], &StatementIntent::read()).unwrap();
    assert_eq!(snapshot(&l).last().unwrap(), "writer");

    // Read after the window elapses → back to READER.
    clock.advance(6000);
    seam_execute(&ctx, "SELECT 1", &[], &StatementIntent::read()).unwrap();
    assert_eq!(snapshot(&l).last().unwrap(), "reader");

    // MUTATION (RED proof) — disable writer-sticky (the faithful "sticky deleted" mutation) and re-run
    // the SAME commit-then-read. The in-window read now lands on the READER (no read-your-writes).
    let ml = log();
    let mclock = Arc::new(ManualClock::new(2_000_000));
    let mreader = RecordingDriver::wrap(pg_driver(), "reader", ml.clone());
    let mwriter = RecordingDriver::wrap(pg_driver(), "writer", ml.clone());
    let mrouting = RoutingConfig {
        registry: ConnectionRegistry::from_default(ReaderWriterPools::split(mreader, mwriter))
            .build()
            .unwrap(),
        sticky: WriterStickyClock::new(StickyOptions {
            use_writer_after_transaction: false, // sticky OFF
            writer_sticky_duration: 5000,
            clock: mclock.clone(),
        }),
    };
    let mctx = for_routing(&mrouting).unwrap();
    transaction_on(
        &mctx,
        None,
        TxDialect::Postgres,
        &TransactionOptions::default(),
        |tx| {
            seam_run(
                tx,
                &format!(
                    "INSERT INTO {} (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING",
                    pg_tbl()
                ),
                &[Value::Int(3), Value::Str("c".into())],
                &StatementIntent::write(),
            )?;
            Ok(())
        },
    )
    .unwrap();
    ml.lock().unwrap().clear(); // ignore the tx's own writer checkout; observe only the post-commit read
    mclock.advance(100);
    seam_execute(&mctx, "SELECT 1", &[], &StatementIntent::read()).unwrap();
    assert_eq!(
        snapshot(&ml),
        vec!["reader"],
        "sticky off ⇒ in-window read hits the reader (read-your-writes lost)"
    );
    println!("C1 writer-sticky (ManualClock): green + RED proof");
}

/// with_writer: reads inside the scope route to the WRITER; a write inside is rejected (read-only).
fn c1_with_writer() {
    let l = log();
    let reader = RecordingDriver::wrap(pg_driver(), "reader", l.clone());
    let writer = RecordingDriver::wrap(pg_driver(), "writer", l.clone());
    let routing = RoutingConfig {
        registry: ConnectionRegistry::from_default(ReaderWriterPools::split(reader, writer))
            .build()
            .unwrap(),
        sticky: WriterStickyClock::disabled(),
    };
    let ctx = for_routing(&routing).unwrap();

    // A read inside with_writer → WRITER.
    let wctx = ctx.with_writer();
    let rows = seam_execute(&wctx, "SELECT 1 AS one", &[], &StatementIntent::read()).unwrap();
    assert_eq!(n_of(&rows[0], "one"), 1);
    assert_eq!(snapshot(&l), vec!["writer"]);

    // A read OUTSIDE with_writer → reader (proves the scope, not a permanent divert).
    seam_execute(&ctx, "SELECT 1", &[], &StatementIntent::read()).unwrap();
    assert_eq!(snapshot(&l), vec!["writer", "reader"]);

    // A guarded write inside with_writer is REJECTED (the scope is read-only) — v1 parity.
    let e = litedbmodel_runtime::run_guarded(
        &wctx,
        &format!("UPDATE {} SET val='x'", pg_tbl()),
        &[],
        "UPDATE",
        Some("X"),
    )
    .unwrap_err();
    assert_eq!(e.kind, "write_in_read_only_context");

    // MUTATION (RED proof) — run the SAME read through the SAME seam but OUTSIDE the with_writer scope
    // (the faithful "with_writer divert removed" mutation). Without the scope it lands on the READER.
    l.lock().unwrap().clear();
    seam_execute(&ctx, "SELECT 1 AS one", &[], &StatementIntent::read()).unwrap();
    assert_eq!(
        snapshot(&l),
        vec!["reader"],
        "outside the scope ⇒ reader; the in-scope 'writer' was load-bearing"
    );
    println!("C1 with_writer: green + RED proof");
}

// ════════════════════════════════════════════════════════════════════════════════
// C2 — multi-DB name routing (PG = A default, MySQL = B) + tx-pin precedence
// ════════════════════════════════════════════════════════════════════════════════

fn c2_multi_db_routing() {
    let l = log();
    let a = RecordingDriver::wrap(pg_driver(), "A", l.clone());
    let b = RecordingDriver::wrap(mysql_driver(), "B", l.clone());
    let routing = RoutingConfig {
        registry: ConnectionRegistry::from_default(ReaderWriterPools::single(a))
            .add("B", ReaderWriterPools::single(b))
            .build()
            .unwrap(),
        sticky: WriterStickyClock::disabled(),
    };
    let ctx = for_routing(&routing).unwrap();

    // Untagged read → default (DB A = PG). PG placeholder + real query.
    let ra = seam_execute(&ctx, "SELECT 42 AS n", &[], &StatementIntent::read()).unwrap();
    assert_eq!(n_of(&ra[0], "n"), 42);
    // "B"-tagged read → DB B = MySQL. MySQL executes `SELECT 7 AS n`.
    let rb = seam_execute(
        &ctx,
        "SELECT 7 AS n",
        &[],
        &StatementIntent {
            write: false,
            db: Some("B".into()),
        },
    )
    .unwrap();
    assert_eq!(n_of(&rb[0], "n"), 7);
    assert_eq!(snapshot(&l), vec!["A", "B"]);

    // Prove REAL cross-DB: write a distinct row into each DB via its tagged pool, read it back.
    seam_run(
        &ctx,
        &format!(
            "INSERT INTO {} (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING",
            pg_tbl()
        ),
        &[Value::Int(100), Value::Str("in-A".into())],
        &StatementIntent::write(),
    )
    .unwrap();
    seam_run(
        &ctx,
        &format!("INSERT INTO {TBL} (id, val) VALUES (?,?) ON DUPLICATE KEY UPDATE val=val"),
        &[Value::Int(200), Value::Str("in-B".into())],
        &StatementIntent {
            write: true,
            db: Some("B".into()),
        },
    )
    .unwrap();
    let in_a = seam_execute(
        &ctx,
        &format!("SELECT val FROM {} WHERE id=$1", pg_tbl()),
        &[Value::Int(100)],
        &StatementIntent::read(),
    )
    .unwrap();
    let in_b = seam_execute(
        &ctx,
        &format!("SELECT val FROM {TBL} WHERE id=?"),
        &[Value::Int(200)],
        &StatementIntent {
            write: false,
            db: Some("B".into()),
        },
    )
    .unwrap();
    assert_eq!(str_of(&in_a[0], "val").as_deref(), Some("in-A"));
    assert_eq!(str_of(&in_b[0], "val").as_deref(), Some("in-B"));
    // The A-only row is NOT in B (separate databases).
    let miss = seam_execute(
        &ctx,
        &format!("SELECT val FROM {TBL} WHERE id=?"),
        &[Value::Int(100)],
        &StatementIntent {
            write: false,
            db: Some("B".into()),
        },
    )
    .unwrap();
    assert_eq!(miss.len(), 0);

    // MUTATION (RED proof): if named routing IGNORED intent.db and always used the default (PG), the
    // "B"-tagged MySQL query (a `?` placeholder) sent to PG (which uses `$N`) would THROW. Force the
    // "B"-tagged read onto the default pool (db:None ⇒ PG) and confirm it fails on PG.
    let miss_route = seam_execute(
        &ctx,
        &format!("SELECT val FROM {TBL} WHERE id=?"),
        &[Value::Int(200)],
        &StatementIntent::read(), // db:None ⇒ default (PG); the `?` placeholder is rejected by PG
    );
    assert!(
        miss_route.is_err(),
        "a MySQL `?`-placeholder query on the PG default pool must fail (routing ignored ⇒ RED)"
    );
    println!("C2 multi-DB routing (PG=A/MySQL=B): green + RED proof");
}

/// A missing connection name is a LOUD error (never a silent default fallback).
fn c2_unknown_name_loud() {
    let a = pg_driver();
    let routing = RoutingConfig::single(a);
    let ctx = for_routing(&routing).unwrap();
    let e = seam_execute(
        &ctx,
        "SELECT 1",
        &[],
        &StatementIntent {
            write: false,
            db: Some("ghost".into()),
        },
    )
    .unwrap_err();
    assert!(
        e.message
            .contains("no connection registered under name 'ghost'"),
        "loud unknown-name error, got: {}",
        e.message
    );
    println!("C2 unknown-name loud: green");
}

/// tx-pin precedence: a named-DB transaction runs its WHOLE body on ONE pinned writer connection of
/// that DB — the active-tx pin STILL wins over routing (Phase B unbroken). Prove: inside a tx to "B",
/// even a read-intent statement resolves the SAME pinned MySQL connection (label recorded ONCE at
/// begin_tx, then the pinned conn serves every statement — no further checkouts on B's recording driver).
fn c2_tx_pin_precedence() {
    let l = log();
    let a = RecordingDriver::wrap(pg_driver(), "A", l.clone());
    let b = RecordingDriver::wrap(mysql_driver(), "B", l.clone());
    let routing = RoutingConfig {
        registry: ConnectionRegistry::from_default(ReaderWriterPools::single(a))
            .add("B", ReaderWriterPools::single(b))
            .build()
            .unwrap(),
        sticky: WriterStickyClock::disabled(),
    };
    let ctx = for_routing(&routing).unwrap();

    // A transaction routed to "B": one checkout on B's recording driver (begin_tx), and every statement
    // in the body — write AND read — runs on the pinned owned connection (NOT a fresh checkout).
    transaction_on(&ctx, Some("B"), TxDialect::Mysql, &TransactionOptions::default(), |tx| {
        seam_run(
            tx,
            &format!("INSERT INTO {TBL} (id, val) VALUES (?,?) ON DUPLICATE KEY UPDATE val=VALUES(val)"),
            &[Value::Int(300), Value::Str("tx-B".into())],
            &StatementIntent::write(),
        )?;
        // A read INSIDE the tx: the pin wins over routing, so it runs on the SAME MySQL connection
        // (sees the just-inserted row before COMMIT) — NOT routed afresh to any pool.
        let r = seam_execute(
            tx,
            &format!("SELECT val FROM {TBL} WHERE id=?"),
            &[Value::Int(300)],
            &StatementIntent::read(),
        )?;
        assert_eq!(str_of(&r[0], "val").as_deref(), Some("tx-B"));
        Ok(())
    })
    .unwrap();

    // The recording log shows EXACTLY ONE "B" checkout (the begin_tx); the body's write + read reused
    // the pinned connection (no extra checkouts). No "A" checkout at all (the tx never touched default).
    let log = snapshot(&l);
    assert_eq!(
        log,
        vec!["B"],
        "tx-pin: one begin_tx checkout on B; body statements reuse the pinned conn (pin wins over routing)"
    );

    // Read-back OUTSIDE the tx (routed): the committed row is on B, absent from A.
    let on_b = seam_execute(
        &ctx,
        &format!("SELECT val FROM {TBL} WHERE id=?"),
        &[Value::Int(300)],
        &StatementIntent {
            write: false,
            db: Some("B".into()),
        },
    )
    .unwrap();
    assert_eq!(str_of(&on_b[0], "val").as_deref(), Some("tx-B"));
    println!("C2 tx-pin precedence (named-DB tx on ONE pinned conn): green");
}

// ════════════════════════════════════════════════════════════════════════════════
// C3 — setConfig: query_timeout, maxPool SOLE cap, session reset-on-release
// ════════════════════════════════════════════════════════════════════════════════

/// query_timeout FIRES a real server statement_timeout on a slow query (PG); an unconfigured pool does
/// NOT (the RED proof — the timeout is caused by the config, not the query).
fn c3_query_timeout_pg() {
    // A configured PG pool (200ms statement_timeout) via build_routing_config from config.
    let handle = build_routing_config(
        vec![ConnectionSetup {
            name: None,
            config: ConnectionConfig {
                driver: Some(ConfigDialect::Postgres),
                host: Some(env("TEST_DB_HOST", "localhost")),
                port: Some(env("TEST_DB_PORT", "5433").parse().unwrap()),
                database: Some(env("TEST_DB_NAME", "testdb")),
                user: Some(env("TEST_DB_USER", "testuser")),
                password: Some(env("TEST_DB_PASSWORD", "testpass")),
                query_timeout: Some(200),
                ..Default::default()
            },
            pool_factory: &litedbmodel_runtime::pg_pool_factory(),
            separate_writer: false,
        }],
        StickyOptions {
            use_writer_after_transaction: false,
            ..Default::default()
        },
    )
    .unwrap();
    let ctx = for_routing(handle.routing()).unwrap();

    // `pg_sleep(2)` (2s) exceeds the 200ms server statement_timeout → the SERVER aborts it. tokio-
    // postgres abbreviates the Display to `db error (SQLSTATE 57014)`; SQLSTATE 57014 IS query_canceled
    // (the code a fired statement_timeout raises — the stable signal, matching the retryable-classifier
    // convention of keying on the code not fragile text).
    let e = seam_execute(&ctx, "SELECT pg_sleep(2)", &[], &StatementIntent::read()).unwrap_err();
    let m = e.message.to_lowercase();
    assert!(
        m.contains("statement timeout") || m.contains("canceling statement") || m.contains("57014"),
        "expected a fired statement_timeout (SQLSTATE 57014), got: {}",
        e.message
    );
    handle.close();

    // MUTATION (RED proof): the SAME slow query on an UNCONFIGURED pool (no statement_timeout) does NOT
    // time out at 200ms — it completes — so the timeout above is caused by the config, not the query.
    let plain = build_routing_config(
        vec![ConnectionSetup {
            name: None,
            config: ConnectionConfig {
                driver: Some(ConfigDialect::Postgres),
                host: Some(env("TEST_DB_HOST", "localhost")),
                port: Some(env("TEST_DB_PORT", "5433").parse().unwrap()),
                database: Some(env("TEST_DB_NAME", "testdb")),
                user: Some(env("TEST_DB_USER", "testuser")),
                password: Some(env("TEST_DB_PASSWORD", "testpass")),
                // NO query_timeout ⇒ no statement_timeout SET
                ..Default::default()
            },
            pool_factory: &litedbmodel_runtime::pg_pool_factory(),
            separate_writer: false,
        }],
        StickyOptions {
            use_writer_after_transaction: false,
            ..Default::default()
        },
    )
    .unwrap();
    let pctx = for_routing(plain.routing()).unwrap();
    // Sleep 300ms (> the 200ms cap the configured pool used) then return a readable INT — proving the
    // UNCONFIGURED pool does NOT abort at 200ms (the timeout was the config's doing). `pg_sleep` returns
    // `void`, so project an int after it (a scalar the row reader materializes cleanly).
    let ok = seam_execute(
        &pctx,
        "SELECT 1 AS done FROM (SELECT pg_sleep(0.3)) s",
        &[],
        &StatementIntent::read(),
    )
    .unwrap(); // 300ms > 200ms; no timeout set ⇒ succeeds
    assert_eq!(n_of(&ok[0], "done"), 1);
    plain.close();
    println!("C3 query_timeout PG (server statement_timeout): green + RED proof");
}

/// query_timeout FIRES a real server max_execution_time on a HEAVY query (MySQL — SLEEP is exempt from
/// max_execution_time, so a CPU-heavy cross-join is used); an unconfigured pool completes (RED proof).
fn c3_query_timeout_mysql() {
    let handle = build_routing_config(
        vec![ConnectionSetup {
            name: None,
            config: ConnectionConfig {
                driver: Some(ConfigDialect::Mysql),
                host: Some(env("TEST_MYSQL_HOST", "127.0.0.1")),
                port: Some(env("TEST_MYSQL_PORT", "3307").parse().unwrap()),
                database: Some(MYSQL_DB.to_string()),
                user: Some(env("TEST_MYSQL_USER", "testuser")),
                password: Some(env("TEST_MYSQL_PASSWORD", "testpass")),
                query_timeout: Some(200),
                ..Default::default()
            },
            pool_factory: &litedbmodel_runtime::mysql_pool_factory(),
            separate_writer: false,
        }],
        StickyOptions {
            use_writer_after_transaction: false,
            ..Default::default()
        },
    )
    .unwrap();
    let ctx = for_routing(handle.routing()).unwrap();

    // A CPU-heavy SELECT (NOT a SLEEP — max_execution_time is documented not to apply to SLEEP): a
    // cartesian cross-join with a SHA2 per row → millions of hashes, well over 200ms.
    let heavy = "SELECT COUNT(*) AS n FROM \
        information_schema.COLLATIONS a, \
        information_schema.COLLATIONS b, \
        information_schema.COLLATIONS c \
        WHERE SHA2(CONCAT(a.ID, b.ID, c.ID, RAND()), 256) > ''";
    let e = seam_execute(&ctx, heavy, &[], &StatementIntent::read()).unwrap_err();
    let m = e.message.to_lowercase();
    assert!(
        m.contains("max_execution_time")
            || m.contains("execution was interrupted")
            || m.contains("3024")
            || m.contains("query execution"),
        "expected a fired max_execution_time, got: {}",
        e.message
    );
    handle.close();

    // MUTATION (RED proof): the SAME class of heavy query on an UNCONFIGURED pool COMPLETES (a smaller
    // cross-join that finishes in a few seconds uncapped) — so the abort above is the config's doing.
    let plain = build_routing_config(
        vec![ConnectionSetup {
            name: None,
            config: ConnectionConfig {
                driver: Some(ConfigDialect::Mysql),
                host: Some(env("TEST_MYSQL_HOST", "127.0.0.1")),
                port: Some(env("TEST_MYSQL_PORT", "3307").parse().unwrap()),
                database: Some(MYSQL_DB.to_string()),
                user: Some(env("TEST_MYSQL_USER", "testuser")),
                password: Some(env("TEST_MYSQL_PASSWORD", "testpass")),
                ..Default::default()
            },
            pool_factory: &litedbmodel_runtime::mysql_pool_factory(),
            separate_writer: false,
        }],
        StickyOptions {
            use_writer_after_transaction: false,
            ..Default::default()
        },
    )
    .unwrap();
    let pctx = for_routing(plain.routing()).unwrap();
    let small = "SELECT COUNT(*) AS n FROM \
        information_schema.COLLATIONS a, \
        information_schema.COLLATIONS b \
        WHERE SHA2(CONCAT(a.ID, b.ID), 256) > ''";
    let ok = seam_execute(&pctx, small, &[], &StatementIntent::read()).unwrap();
    assert!(n_of(&ok[0], "n") > 0, "uncapped ⇒ completes");
    plain.close();
    println!("C3 query_timeout MySQL (max_execution_time, heavy not sleep): green + RED proof");
}

/// maxPool is the SOLE cap (applied at construction): 5 concurrent slow queries against a maxPool=2 PG
/// pool keep total_connections ≤ 2 mid-flight; DELETE maxPool (defaults to 10) and it exceeds 2 (RED).
fn c3_max_pool_sole_cap() {
    // Build a maxPool=2 PG pool via the factory (max applied at construction — the SOLE cap source).
    let cfg = |max: Option<u32>| ConnectionConfig {
        driver: Some(ConfigDialect::Postgres),
        host: Some(env("TEST_DB_HOST", "localhost")),
        port: Some(env("TEST_DB_PORT", "5433").parse().unwrap()),
        database: Some(env("TEST_DB_NAME", "testdb")),
        user: Some(env("TEST_DB_USER", "testuser")),
        password: Some(env("TEST_DB_PASSWORD", "testpass")),
        max_pool: max,
        ..Default::default()
    };

    // A factory that captures the constructed PostgresDriver so the test can read live total_connections.
    let captured: Arc<Mutex<Option<Arc<PostgresDriver>>>> = Arc::new(Mutex::new(None));
    let cap2 = captured.clone();
    let capturing = move |config: &litedbmodel_runtime::ResolvedConnectionConfig,
                          _role: litedbmodel_runtime::PoolRole| {
        let d = Arc::new(PostgresDriver::connect_with_config(config)?);
        *cap2.lock().unwrap() = Some(d.clone());
        Ok(litedbmodel_runtime::BuiltPool {
            pool: d as Arc<dyn Driver + Send + Sync>,
        })
    };
    let handle = build_routing_config(
        vec![ConnectionSetup {
            name: None,
            config: cfg(Some(2)),
            pool_factory: &capturing,
            separate_writer: false,
        }],
        StickyOptions {
            use_writer_after_transaction: false,
            ..Default::default()
        },
    )
    .unwrap();
    let pg = captured.lock().unwrap().clone().expect("captured pool");
    let routing = handle.routing();

    // Fire 5 concurrent slow queries; the pool CANNOT exceed maxPool=2 (2 live, the rest queued).
    let max_seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for _ in 0..5 {
            scope.spawn(move || {
                let ctx = for_routing(routing).unwrap();
                let _ = seam_execute(
                    &ctx,
                    "SELECT pg_sleep(0.25) AS d",
                    &[],
                    &StatementIntent::read(),
                );
            });
        }
        // Sample total_connections mid-flight.
        for _ in 0..8 {
            std::thread::sleep(std::time::Duration::from_millis(40));
            let t = pg.total_connections();
            max_seen.fetch_max(t, std::sync::atomic::Ordering::SeqCst);
        }
    });
    let capped = max_seen.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        capped <= 2,
        "maxPool=2 is the sole cap: total_connections stayed ≤ 2 mid-flight (saw {capped})"
    );
    handle.close();

    // MUTATION (RED proof): DELETE maxPool (defaults to 10); the SAME 5 concurrent queries now open >2
    // connections. If maxPool were dead surface (ignored), this would ALSO cap at 2 and falsely pass.
    let captured_m: Arc<Mutex<Option<Arc<PostgresDriver>>>> = Arc::new(Mutex::new(None));
    let cap_m2 = captured_m.clone();
    let capturing_m = move |config: &litedbmodel_runtime::ResolvedConnectionConfig,
                            _role: litedbmodel_runtime::PoolRole| {
        let d = Arc::new(PostgresDriver::connect_with_config(config)?);
        *cap_m2.lock().unwrap() = Some(d.clone());
        Ok(litedbmodel_runtime::BuiltPool {
            pool: d as Arc<dyn Driver + Send + Sync>,
        })
    };
    let handle_m = build_routing_config(
        vec![ConnectionSetup {
            name: None,
            config: cfg(None), // NO maxPool ⇒ default 10
            pool_factory: &capturing_m,
            separate_writer: false,
        }],
        StickyOptions {
            use_writer_after_transaction: false,
            ..Default::default()
        },
    )
    .unwrap();
    let pg_m = captured_m.lock().unwrap().clone().expect("captured pool M");
    let routing_m = handle_m.routing();
    let max_seen_m = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for _ in 0..5 {
            scope.spawn(move || {
                let ctx = for_routing(routing_m).unwrap();
                let _ = seam_execute(
                    &ctx,
                    "SELECT pg_sleep(0.25) AS d",
                    &[],
                    &StatementIntent::read(),
                );
            });
        }
        for _ in 0..8 {
            std::thread::sleep(std::time::Duration::from_millis(40));
            let t = pg_m.total_connections();
            max_seen_m.fetch_max(t, std::sync::atomic::Ordering::SeqCst);
        }
    });
    let uncapped = max_seen_m.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        uncapped > 2,
        "without maxPool (default 10) the pool exceeds 2 (saw {uncapped}) — maxPool is the sole, \
         load-bearing cap (RED if it were 'always 2')"
    );
    handle_m.close();

    println!(
        "C3 maxPool SOLE cap (capped {capped} ≤ 2 vs uncapped {uncapped} > 2): green + RED proof"
    );
}

/// Session reset-on-release: a searchPath/charset set for a configured connection is RESET before the
/// pooled connection returns, so it does NOT leak to the next caller drawing the SAME connection.
/// Proven against live PG: a configured pool (search_path = the rust schema, maxPool=1 so the SAME
/// underlying connection is reused) resolves an unqualified table name to the rust schema DURING a
/// statement, and after release a RAW (unconfigured) query on the SAME pool does NOT see the search_path.
fn c3_session_reset_on_release() {
    reset_pg();
    // Seed a row into the rust-schema table so the search_path resolution is observable.
    {
        let d = pg_driver();
        d.prepare(&format!(
            "INSERT INTO {} (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING",
            pg_tbl()
        ))
        .run(&[Value::Int(7), Value::Str("via-searchpath".into())])
        .expect("seed");
    }

    // A configured pool: search_path = the rust schema, maxPool=1 (so the SAME underlying connection is
    // reused across calls — the strongest reset-leak test). A statement issued through it can reference
    // the table UNQUALIFIED and resolve to the rust schema.
    let handle = build_routing_config(
        vec![ConnectionSetup {
            name: None,
            config: ConnectionConfig {
                driver: Some(ConfigDialect::Postgres),
                host: Some(env("TEST_DB_HOST", "localhost")),
                port: Some(env("TEST_DB_PORT", "5433").parse().unwrap()),
                database: Some(env("TEST_DB_NAME", "testdb")),
                user: Some(env("TEST_DB_USER", "testuser")),
                password: Some(env("TEST_DB_PASSWORD", "testpass")),
                search_path: Some(format!("{PG_SCHEMA},public")),
                max_pool: Some(1),
                ..Default::default()
            },
            pool_factory: &litedbmodel_runtime::pg_pool_factory(),
            separate_writer: false,
        }],
        StickyOptions {
            use_writer_after_transaction: false,
            ..Default::default()
        },
    )
    .unwrap();
    let ctx = for_routing(handle.routing()).unwrap();

    // WITH the configured search_path: the UNQUALIFIED table resolves to the rust schema (row found).
    let found = seam_execute(
        &ctx,
        &format!("SELECT val FROM {TBL} WHERE id=$1"),
        &[Value::Int(7)],
        &StatementIntent::read(),
    )
    .unwrap();
    assert_eq!(
        str_of(&found[0], "val").as_deref(),
        Some("via-searchpath"),
        "the session search_path resolved the unqualified table to the rust schema"
    );

    // A second statement through the SAME configured pool (same underlying conn, maxPool=1): the
    // search_path was RESET on release after the first call, then RE-APPLIED at the second checkout —
    // so it STILL resolves (the config re-sets it each acquire). This proves the per-checkout apply.
    let again = seam_execute(
        &ctx,
        &format!(
            "SELECT current_setting('search_path') AS sp, (SELECT val FROM {TBL} WHERE id=$1) AS v"
        ),
        &[Value::Int(7)],
        &StatementIntent::read(),
    )
    .unwrap();
    assert_eq!(str_of(&again[0], "v").as_deref(), Some("via-searchpath"));
    handle.close();

    // ── The DIRECT, load-bearing reset-on-release proof (observe the SAME connection) ─────────────
    //
    // Build a maxPool=1 RAW PG pool so a `session_connection` holds THE only underlying connection and
    // we can inspect its search_path before/after the reset. The reset statement `RESET search_path`
    // restores the server default (`"$user", public`). Sequence on the SAME connection:
    //   1. baseline search_path (default, NOT the rust schema);
    //   2. a session_connection with SET search_path applied → search_path IS the rust schema (apply);
    //   3. finish(clean) runs `RESET search_path` (the reset-on-release);
    //   4. a FRESH session_connection with NO setup on the SAME pooled connection → search_path is back
    //      to the default (NOT the rust schema) ⇒ the reset really ran (no leak).
    // If step-3's reset were DROPPED (the faithful "reset removed" mutation), step-4 would STILL see the
    // rust schema (the SET leaked on the reused connection) — so this assertion is RED under that break.
    // maxPool=1 so every session_connection reuses THE ONE underlying connection (the reset state is
    // observable across checkouts).
    let raw: Arc<dyn Driver + Send + Sync> = Arc::new(
        PostgresDriver::connect_with_config(
            &ConnectionConfig {
                driver: Some(ConfigDialect::Postgres),
                host: Some(env("TEST_DB_HOST", "localhost")),
                port: Some(env("TEST_DB_PORT", "5433").parse().unwrap()),
                database: Some(env("TEST_DB_NAME", "testdb")),
                user: Some(env("TEST_DB_USER", "testuser")),
                password: Some(env("TEST_DB_PASSWORD", "testpass")),
                max_pool: Some(1),
                ..Default::default()
            }
            .resolve(),
        )
        .expect("pg maxpool=1 connect"),
    );
    let default_sp = current_search_path(&raw, &[]); // no setup ⇒ server default
    assert!(
        !default_sp.contains(PG_SCHEMA),
        "baseline search_path should be the default (not the rust schema), got: {default_sp}"
    );

    // Apply the SET on an owned session connection, observe it IS the rust schema, then finish (reset).
    let setup = vec![format!("SET search_path TO {PG_SCHEMA},public")];
    let reset = vec!["RESET search_path".to_string()];
    let mut sc = raw
        .session_connection(&setup, &reset)
        .expect("session conn");
    let applied = sc
        .execute("SELECT current_setting('search_path') AS sp", &[])
        .expect("read search_path");
    assert!(
        str_of(&applied[0], "sp")
            .unwrap_or_default()
            .contains(PG_SCHEMA),
        "the SET applied the rust schema on the owned session connection"
    );
    sc.finish(false).expect("finish runs RESET + releases"); // reset-on-release

    // A FRESH session with NO setup on the SAME maxPool=1 pooled connection: search_path is the default
    // again (the reset ran — no leak). This is the load-bearing assertion (RED if the reset were dropped).
    let after_sp = current_search_path(&raw, &[]);
    assert!(
        !after_sp.contains(PG_SCHEMA),
        "reset-on-release: after finish(clean) the reused connection's search_path is back to the \
         default (RED if the RESET were dropped — the SET would leak), got: {after_sp}"
    );
    assert_eq!(
        after_sp, default_sp,
        "the reset restored EXACTLY the server default search_path"
    );
    println!("C3 session search_path apply + reset-on-release (same-conn default→set→reset→default): green + RED proof");
}

/// Read `current_setting('search_path')` on an owned session connection with the given `setup`
/// statements applied (empty ⇒ the connection's current/default search_path). maxPool=1 pools reuse the
/// SAME underlying connection, so this observes the reset state directly.
fn current_search_path(driver: &Arc<dyn Driver + Send + Sync>, setup: &[String]) -> String {
    let mut sc = driver.session_connection(setup, &[]).expect("session conn");
    let rows = sc
        .execute("SELECT current_setting('search_path') AS sp", &[])
        .expect("read search_path");
    let sp = str_of(&rows[0], "sp").unwrap_or_default();
    sc.finish(false).expect("finish");
    sp
}

// ── The single #[test] entry (serial; each phase prints its own PASS line) ──────

#[test]
fn phase_c_connection_routing_and_config() {
    if !enabled() {
        eprintln!(
            "skipping Phase C live routing test (set LITEDBMODEL_PHASE_C=1 + docker PG/MySQL)"
        );
        return;
    }
    reset_pg();
    reset_mysql();

    // C1
    c1_reader_writer_split();
    c1_writer_sticky();
    c1_with_writer();
    // C2
    c2_multi_db_routing();
    c2_unknown_name_loud();
    c2_tx_pin_precedence();
    // C3
    c3_query_timeout_pg();
    c3_query_timeout_mysql();
    c3_max_pool_sole_cap();
    c3_session_reset_on_release();

    println!("PHASE C ROUTING+CONFIG PROOF: all C1/C2/C3 green (reader/writer, sticky, with_writer, \
              multi-DB, tx-pin, query_timeout PG+MySQL, maxPool sole cap, session reset-on-release)");
}
