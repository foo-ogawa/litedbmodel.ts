//! litedbmodel v2 SCP — SQL driver seam (Rust, WS7e).
//!
//! The minimal SQL-driver surface the runtime needs, mirroring the TS `SqliteDb` seam
//! (`prepare(sql).all(...) / .run(...)`) and the audited Python/PHP `Driver` seams. The conformance
//! bar executes against an in-process `rusqlite` connection ([`SqliteDriver`]) — the sanctioned
//! in-proc substitute for a docker integration DB (#34 AC).
//!
//! ## Sync seam, async pools underneath (#40)
//!
//! This [`Driver`] trait is a SYNCHRONOUS FACADE: `prepare(sql).all(...) / .run(...)` return
//! eagerly. The in-proc [`SqliteDriver`] is genuinely synchronous (rusqlite, in-process, no I/O
//! wait). The LIVE PostgreSQL / MySQL drivers ([`crate::livedb`], `livedb` feature) implement this
//! SAME trait but are backed by ASYNC connection POOLS — `tokio-postgres` + `deadpool-postgres`
//! (PG) and `sqlx` (MySQL/SQLite) on a shared tokio runtime, restoring the old `litedbmodel.rs`
//! execution model. Each pooled driver is `Send + Sync` (the pool is `Clone`-cheap and internally
//! synchronized), so the facade's `all()`/`run()` block-on the pooled future while DISTINCT threads
//! checking out DISTINCT pooled connections run REAL parallel DB I/O. That is what lets the plan's
//! `concurrency` (default 16) become concurrent sibling-relation dispatch at the executor layer
//! without changing the generated SQL text or
//! the trait: the seam is dialect- and execution-model-agnostic. The runtime only ever binds
//! already-rendered scalar params (a bc [`Value`] → the driver's native param type).

use behavior_contracts::Value;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::Connection;

use std::sync::Arc;

use crate::connection_routing::{
    session_reset_statements, session_statements, ResolvedConnectionConfig,
};
use crate::errors::{map_sqlite_error, SqlFailure};
use crate::exec_context::{SessionConnection, TxConnection};

/// The summary of a non-returning write: affected-row count + last insert rowid.
#[derive(Debug, Clone, Copy)]
pub struct RunInfo {
    pub changes: i64,
    pub last_insert_rowid: i64,
}

/// A prepared statement: `all` returns the row list (SELECT/RETURNING); `run` a write summary.
pub trait PreparedStatement {
    fn all(&mut self, params: &[Value]) -> Result<Vec<Value>, SqlFailure>;
    fn run(&mut self, params: &[Value]) -> Result<RunInfo, SqlFailure>;
}

/// The synchronous SQL-driver seam (mirrors the TS `SqliteDb`).
pub trait Driver {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_>;

    /// The connection's SQL dialect (`"sqlite"` / `"postgres"` / `"mysql"`) — a CONNECTION property,
    /// not a per-op flag. The native-codegen exec seam reads it (via the ctx's driver) to resolve the
    /// dialect placeholder style (`?`→`$N` for postgres, via [`crate::sql_render::render_placeholders`])
    /// AT RUNTIME, after the final SQL is assembled — so the generated modules bake dialect-NEUTRAL `?`
    /// and the ONE runtime exec point does the renumber (no per-op / generation-time split). Defaults to
    /// `"sqlite"` (the in-proc conformance driver); the live PG/MySQL drivers + wrappers override.
    fn dialect(&self) -> &'static str {
        "sqlite"
    }

    /// Begin a transaction, returning an OWNED [`TxConnection`] handle (§3 per-execution connection
    /// ownership — the rust analogue of v1 `PoolTransaction`). BEGIN is issued when the handle is
    /// built. A single-connection driver (the in-proc `rusqlite` seam) forwards every tx statement +
    /// the final COMMIT/ROLLBACK to its one connection ([`forwarding_tx`]). A POOLED live driver
    /// (PG/MySQL) checks out ONE pooled connection and pins it in the handle, so concurrent
    /// transactions each own a DISTINCT connection ⇒ isolated (the old driver-global `writer` slot is
    /// gone). Every implementor MUST issue BEGIN in this method so the returned handle is live.
    fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure>;

    /// Begin a transaction with an isolation-level prelude (Phase B / #82). `before_begin` statements
    /// run on the newly-acquired OWNED connection BEFORE its `BEGIN` (MySQL `SET TRANSACTION
    /// ISOLATION LEVEL …`, which scopes the very next tx); `after_begin` statements run as the FIRST
    /// statements inside the just-opened tx (Postgres `SET TRANSACTION ISOLATION LEVEL …`, valid as
    /// the leading statement). Both empty ⇒ identical to [`Driver::begin_tx`] (the Phase A path).
    ///
    /// The default implementation composes it from the primitives already every driver has: the
    /// `before_begin` statements ride the (soon-to-own) connection via a throwaway prepared run — but
    /// since a pooled driver has no way to run a statement on a specific not-yet-owned connection, the
    /// robust default acquires the tx first (plain BEGIN), then runs BOTH slots on the OWNED handle.
    /// For MySQL this is WRONG (its SET must precede BEGIN), so [`crate::livedb::MysqlDriver`]
    /// OVERRIDES this to acquire → SET → BEGIN in the right order. The single-connection
    /// [`forwarding_tx`] and Postgres use the default (PG's isolation SET is valid post-BEGIN).
    fn begin_tx_isolated(
        &self,
        before_begin: &[String],
        after_begin: &[String],
    ) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        // Default: BEGIN (via begin_tx), then run before_begin + after_begin on the OWNED handle.
        // Correct for PG (SET ISOLATION valid as the first in-tx statement) and for the
        // single-connection sqlite seam (which ignores isolation — the caller never passes it there).
        let mut tx = self.begin_tx()?;
        for sql in before_begin.iter().chain(after_begin.iter()) {
            tx.run(sql, &[])?;
        }
        Ok(tx)
    }

    /// Acquire an OWNED tx connection WITHOUT issuing BEGIN (#93 / owner option A). The Phase D tx
    /// runtime pins THIS connection, then issues the isolation SET + `BEGIN` + `COMMIT`/`ROLLBACK`
    /// THROUGH the central seam (`run(txctx, …)`), so a registered middleware observes the tx-control —
    /// full TS parity (TS issues `runAsync(txCtx, 'BEGIN'/'COMMIT'/…)`). The connection is the SAME one
    /// every body statement (and the seam-issued tx-control) resolves via `connection_for`, so
    /// per-execution ownership + the concurrent-tx isolation are UNCHANGED — only WHERE the BEGIN/COMMIT
    /// text is issued from moved (into the seam), not which connection runs it.
    ///
    /// The default acquires the SAME owned connection [`Driver::begin_tx`] does, but SUPPRESSES the
    /// BEGIN: it wraps a freshly-acquired connection in a handle that runs no BEGIN. A single-connection
    /// driver (sqlite) has nothing to acquire — it returns a bare forwarding handle (no BEGIN). Pooled
    /// live drivers (PG/MySQL) OVERRIDE this to check out one pooled connection with NO BEGIN.
    fn acquire_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure>;

    /// Acquire an OWNED connection with the SESSION-config `setup` statements already applied, plus the
    /// `reset` statements to run when the returned handle is `finish`ed (Phase C / #88 — the rust
    /// analogue of the TS `configuredPool` acquire/release). This is the primitive
    /// [`ConfiguredDriver`] uses so a whole seam call (one `prepare(sql).all()/run()`) runs on ONE
    /// pooled connection carrying the session knobs, and the knobs are RESET before the connection
    /// returns to the pool — so they never leak to the next caller. A `setup` statement failure poisons
    /// the connection (dropped, not returned).
    ///
    /// The default implementation acquires an owned connection via [`Driver::begin_tx`] (a transaction
    /// handle IS an owned connection), runs `setup`, and wraps it in a [`SessionOverTx`] that runs
    /// `reset` + COMMITs (a clean release) or ROLLBACKs (poison) on `finish`. Live pooled drivers
    /// (PG/MySQL) OVERRIDE this to hold a plain pooled connection WITHOUT a transaction (a session SET +
    /// a bare statement + a session RESET must NOT be wrapped in a tx — a fired statement_timeout mid-tx
    /// would abort the tx, and a session SET is a connection property, not a tx one). The
    /// single-connection sqlite seam uses this default (no server session ⇒ `setup`/`reset` are empty in
    /// practice — sqlite emits no session statements).
    fn session_connection(
        &self,
        setup: &[String],
        reset: &[String],
    ) -> Result<Box<dyn SessionConnection + '_>, SqlFailure> {
        let mut tx = self.begin_tx()?;
        for sql in setup {
            if let Err(e) = tx.run(sql, &[]) {
                let _ = tx.rollback(); // a failed session setup poisons the connection — drop it.
                return Err(e);
            }
        }
        Ok(Box::new(SessionOverTx {
            tx: Some(tx),
            reset: reset.to_vec(),
        }))
    }
}

/// Build the single-connection [`TxConnection`] for a driver that is genuinely ONE connection (the
/// in-proc `rusqlite` seam): issue BEGIN, then forward every statement (and COMMIT/ROLLBACK) to that
/// driver. There is no per-execution ownership to enforce — a tx runs BEGIN…COMMIT on the one
/// connection — but it satisfies the SAME [`TxConnection`] contract the seam threads. Shared by any
/// `Driver::begin_tx` impl over a single-connection driver.
pub fn forwarding_tx(driver: &dyn Driver) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
    driver.prepare("BEGIN").run(&[])?;
    Ok(Box::new(ForwardingTx { driver }))
}

/// Build the single-connection [`TxConnection`] WITHOUT issuing BEGIN (#93 seam-routed tx control): the
/// tx runtime seam-issues BEGIN on the forwarded connection. The object-safe default `acquire_tx` for a
/// single-connection [`Driver`] that has no override (it takes `&dyn Driver`, so a concrete impl calls
/// `forwarding_tx_no_begin(self)`).
pub fn forwarding_tx_no_begin(
    driver: &dyn Driver,
) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
    Ok(Box::new(ForwardingTx { driver }))
}

/// The single-connection [`TxConnection`]: forward every statement (and COMMIT/ROLLBACK) to the
/// driver it borrows. Built via [`forwarding_tx`] (which issues BEGIN first).
pub struct ForwardingTx<'a> {
    driver: &'a dyn Driver,
}

impl TxConnection for ForwardingTx<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        self.driver.prepare(sql).all(params)
    }
    fn run(&mut self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        self.driver.prepare(sql).run(params)
    }
    fn release(self: Box<Self>, _poison: bool) -> Result<(), SqlFailure> {
        // Single-connection sqlite seam: nothing to release (the one connection is the driver itself).
        // The COMMIT/ROLLBACK was already seam-issued; dropping the handle is a no-op.
        Ok(())
    }
    fn commit(self: Box<Self>) -> Result<(), SqlFailure> {
        self.driver.prepare("COMMIT").run(&[]).map(|_| ())
    }
    fn rollback(self: Box<Self>) -> Result<(), SqlFailure> {
        self.driver.prepare("ROLLBACK").run(&[]).map(|_| ())
    }
}

/// The default [`SessionConnection`] built over a [`TxConnection`] owned handle (used by the
/// single-connection sqlite seam + any driver that does not override [`Driver::session_connection`]).
/// It runs the seam statement on the tx-owned connection and, on `finish`, runs the RESET statements
/// then COMMITs (clean) / ROLLBACKs (poison). For sqlite the session `setup`/`reset` are empty in
/// practice (no server session), so this collapses to a bare BEGIN…COMMIT around one statement —
/// harmless (a single autocommit statement wrapped in a tx is byte-identical result-wise).
struct SessionOverTx<'a> {
    tx: Option<Box<dyn TxConnection + 'a>>,
    reset: Vec<String>,
}

impl SessionConnection for SessionOverTx<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        self.tx
            .as_mut()
            .expect("session tx present")
            .execute(sql, params)
    }
    fn run(&mut self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        self.tx
            .as_mut()
            .expect("session tx present")
            .run(sql, params)
    }
    fn finish(mut self: Box<Self>, poison: bool) -> Result<(), SqlFailure> {
        let tx = self.tx.take().expect("session tx present");
        if poison {
            let _ = tx.rollback(); // poisoned: drop without resetting.
            return Ok(());
        }
        // Clean: run the RESET statements on the owned connection, then COMMIT (release).
        // A reset here forwards through the tx handle (single-connection). If a reset fails, roll back.
        // NB: the resets are issued via a fresh execute on the owned handle.
        let mut tx = tx;
        for sql in &self.reset {
            if let Err(e) = tx.run(sql, &[]) {
                let _ = tx.rollback();
                return Err(e);
            }
        }
        tx.commit()
    }
}

/// A session-CONFIGURED [`Driver`] wrapper (Phase C / #88 — the rust analogue of the TS
/// `configuredPool`). Every `prepare(sql).all()/run()` acquires ONE owned connection via
/// [`Driver::session_connection`] (with the SESSION `setup` SET statements applied), runs the target
/// statement on it, then `finish`es the handle (running the RESET statements + releasing) — so a
/// session knob (statement_timeout / search_path / charset) set for THIS configured connection does
/// NOT leak to the next caller that draws the SAME pooled connection.
///
/// A config with NO session knobs (all defaults) ⇒ `setup`/`reset` are EMPTY ⇒ [`ConfiguredDriver`] is
/// a transparent passthrough (byte-identical to the wrapped driver — backward-compat). Held as an
/// `Arc<dyn Driver>` because a routing registry owns its drivers behind `Arc`.
pub struct ConfiguredDriver {
    inner: Arc<dyn Driver + Send + Sync>,
    setup: Vec<String>,
    reset: Vec<String>,
}

impl ConfiguredDriver {
    /// Wrap `inner` with the SESSION statements derived from `config`. If `config` sets no session
    /// knobs the wrapper is a transparent passthrough — but the caller ([`crate::connection_routing`]'s
    /// build path) can skip wrapping entirely in that case for a true byte-identical zero-overhead path.
    pub fn new(inner: Arc<dyn Driver + Send + Sync>, config: &ResolvedConnectionConfig) -> Self {
        ConfiguredDriver {
            inner,
            setup: session_statements(config),
            reset: session_reset_statements(config),
        }
    }

    /// The session SET statements this configured driver applies at each checkout (diagnostics/tests).
    pub fn session(&self) -> &[String] {
        &self.setup
    }
}

impl Driver for ConfiguredDriver {
    fn dialect(&self) -> &'static str {
        self.inner.dialect()
    }

    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        Box::new(ConfiguredPrepared {
            driver: self,
            sql: sql.to_string(),
        })
    }

    /// A tx on a configured driver acquires a session-configured owned connection (SET applied), issues
    /// BEGIN on it, and delegates every statement + COMMIT/ROLLBACK to it. The isolation prelude is
    /// forwarded to the inner driver's `begin_tx_isolated`, and the SESSION setup/reset ride the owned
    /// connection around the whole tx (finish runs the resets after COMMIT/ROLLBACK).
    fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        self.begin_tx_isolated(&[], &[])
    }

    fn begin_tx_isolated(
        &self,
        before_begin: &[String],
        after_begin: &[String],
    ) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        // The tx owns one inner connection; the session SET statements run as part of the tx prelude
        // (before the isolation SET and BEGIN), and the reset statements run just before COMMIT/ROLLBACK
        // via the ConfiguredTx wrapper. Compose the session setup with the isolation before_begin.
        let mut before: Vec<String> = self.setup.clone();
        before.extend_from_slice(before_begin);
        let tx = self.inner.begin_tx_isolated(&before, after_begin)?;
        Ok(Box::new(ConfiguredTx {
            inner: Some(tx),
            reset: self.reset.clone(),
        }))
    }

    /// Acquire a session-configured owned tx connection WITHOUT issuing BEGIN (#93 seam-routed tx
    /// control): the session SET statements are applied at acquisition (on the owned inner connection),
    /// but the isolation SET + BEGIN are seam-issued by the tx runtime. The RESET statements run on
    /// [`ConfiguredTx::release`] (clean) before the connection returns to the pool.
    fn acquire_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        // Acquire the inner owned tx connection (no BEGIN) and apply ONLY the session setup on it now —
        // the isolation SET + BEGIN come through the seam. `acquire_tx` on the inner suppresses BEGIN;
        // the session setup runs on the owned connection immediately (a connection property, not a tx
        // statement — matches the pre-#93 order where setup rode the tx prelude before BEGIN).
        let mut tx = self.inner.acquire_tx()?;
        for sql in &self.setup {
            if let Err(e) = tx.run(sql, &[]) {
                let _ = tx.release(true); // a failed session setup poisons the connection.
                return Err(e);
            }
        }
        Ok(Box::new(ConfiguredTx {
            inner: Some(tx),
            reset: self.reset.clone(),
        }))
    }
}

/// A prepared statement on a [`ConfiguredDriver`]: acquire a session-configured owned connection, run
/// the statement, then finish (reset + release). One connection per seam call — the session knobs
/// apply to exactly this statement and are reset before the connection returns to the pool.
struct ConfiguredPrepared<'a> {
    driver: &'a ConfiguredDriver,
    sql: String,
}

impl PreparedStatement for ConfiguredPrepared<'_> {
    fn all(&mut self, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        let mut conn = self
            .driver
            .inner
            .session_connection(&self.driver.setup, &self.driver.reset)?;
        match conn.execute(&self.sql, params) {
            Ok(rows) => {
                conn.finish(false)?; // clean: reset + release
                Ok(rows)
            }
            Err(e) => {
                // Poisoned (e.g. a fired statement_timeout aborts the connection): skip reset, drop it.
                let _ = conn.finish(true);
                Err(e)
            }
        }
    }
    fn run(&mut self, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        let mut conn = self
            .driver
            .inner
            .session_connection(&self.driver.setup, &self.driver.reset)?;
        match conn.run(&self.sql, params) {
            Ok(info) => {
                conn.finish(false)?;
                Ok(info)
            }
            Err(e) => {
                let _ = conn.finish(true);
                Err(e)
            }
        }
    }
}

/// The tx handle over a [`ConfiguredDriver`]: it delegates every statement to the inner tx-owned
/// connection and, on commit/rollback, runs the RESET statements first (so the session knobs do not
/// leak when the connection returns to the pool), then COMMIT/ROLLBACK.
struct ConfiguredTx<'a> {
    inner: Option<Box<dyn TxConnection + 'a>>,
    reset: Vec<String>,
}

impl TxConnection for ConfiguredTx<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        self.inner
            .as_mut()
            .expect("configured tx present")
            .execute(sql, params)
    }
    fn run(&mut self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        self.inner
            .as_mut()
            .expect("configured tx present")
            .run(sql, params)
    }
    fn release(mut self: Box<Self>, poison: bool) -> Result<(), SqlFailure> {
        let mut inner = self.inner.take().expect("configured tx present");
        if poison {
            // Poisoned: skip the reset (would fail on an aborted connection), destroy the connection.
            return inner.release(true);
        }
        // Clean: run the RESET statements (so session knobs don't leak) on the owned connection BEFORE
        // releasing it. A reset failure poisons the connection. The COMMIT/ROLLBACK was seam-issued.
        for sql in &self.reset {
            if let Err(e) = inner.run(sql, &[]) {
                let _ = inner.release(true);
                return Err(e);
            }
        }
        inner.release(false)
    }
    fn commit(mut self: Box<Self>) -> Result<(), SqlFailure> {
        let mut inner = self.inner.take().expect("configured tx present");
        // Reset the session knobs BEFORE COMMIT (still on the owned connection).
        for sql in &self.reset {
            if let Err(e) = inner.run(sql, &[]) {
                let _ = inner.rollback();
                return Err(e);
            }
        }
        inner.commit()
    }
    fn rollback(mut self: Box<Self>) -> Result<(), SqlFailure> {
        let inner = self.inner.take().expect("configured tx present");
        // On rollback the connection state is discarded anyway; skip the reset and just roll back.
        inner.rollback()
    }
}

/// Convert a rendered bc [`Value`] param to a `rusqlite` bindable value.
///
/// Only scalar shapes reach the driver: the write path serializes emit `{obj:…}` payloads to a
/// compact JSON string upstream (`to_driver_param`), and IN-list arrays are flattened during
/// render. An unexpected array/object here is a bug — surface it loudly rather than fabricate.
fn to_sql_value(v: &Value) -> Result<SqlValue, SqlFailure> {
    match v {
        Value::Null => Ok(SqlValue::Null),
        Value::Bool(b) => Ok(SqlValue::Integer(if *b { 1 } else { 0 })),
        Value::Int(i) => Ok(SqlValue::Integer(*i)),
        Value::Float(f) => Ok(SqlValue::Real(*f)),
        Value::Str(s) => Ok(SqlValue::Text(s.clone())),
        // A scalar-array IN-list / relation-key param: SQLite has no native array — bind the
        // `json_each(?)` JSON string (the array-bind SSoT; the Postgres driver binds a native array).
        // SQLite's `json_each` coerces JSON booleans natively, so keep `true`/`false`. The render sites
        // pass the raw `Value::Arr` and never branch on dialect.
        Value::Arr(elems) => Ok(SqlValue::Text(crate::value_codec::array_param_json(
            elems, false,
        ))),
        Value::Obj(_) => Err(SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: format!(
                "scp driver: a {} reached the param binder (expected a scalar or array)",
                v.type_name()
            ),
        }),
    }
}

/// Convert a fetched SQLite cell to a bc [`Value`] (row assembly).
fn from_sql_ref(r: ValueRef<'_>) -> Value {
    match r {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::Int(i),
        ValueRef::Real(f) => Value::Float(f),
        ValueRef::Text(bytes) => Value::Str(String::from_utf8_lossy(bytes).into_owned()),
        ValueRef::Blob(bytes) => Value::Str(String::from_utf8_lossy(bytes).into_owned()),
    }
}

/// An in-process `rusqlite` driver implementing the [`Driver`] seam.
///
/// This is the runnable conformance seam: it binds `?` placeholders positionally, so a
/// Postgres-tagged bundle's `$N` text is NOT what runs here — the exec/tx vectors run only the
/// SQLite-tagged bundles (the §10 promise: same IR + input → same RESULT regardless of dialect
/// text). PG/MySQL SQL-text conformance is proven on the render axis; live PG/MySQL execution is
/// the coordinated docker pass.
pub struct SqliteDriver {
    conn: Connection,
}

impl SqliteDriver {
    /// Open an in-memory database with foreign keys enforced and the given schema applied.
    pub fn in_memory(schema: &[String]) -> Result<Self, SqlFailure> {
        let conn = Connection::open_in_memory().map_err(|e| map_sqlite_error(&e))?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")
            .map_err(|e| map_sqlite_error(&e))?;
        for stmt in schema {
            conn.execute_batch(stmt).map_err(|e| map_sqlite_error(&e))?;
        }
        Ok(SqliteDriver { conn })
    }

    /// Open an on-disk database FILE with foreign keys enforced (the schema is already present — an
    /// already-seeded file). The in-proc conformance seam over a persisted DB (used by the
    /// native-codegen proof cell, which reads/mutates a seeded file to compare byte-for-byte with the
    /// mode-2 oracle over the SAME file).
    pub fn open(path: &str) -> Result<Self, SqlFailure> {
        let conn = Connection::open(path).map_err(|e| map_sqlite_error(&e))?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")
            .map_err(|e| map_sqlite_error(&e))?;
        Ok(SqliteDriver { conn })
    }
}

impl Driver for SqliteDriver {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        Box::new(SqlitePrepared {
            conn: &self.conn,
            sql: sql.to_string(),
        })
    }

    /// The in-proc `rusqlite` seam is a single connection: a tx runs BEGIN…COMMIT/ROLLBACK on it via
    /// the forwarding handle (byte-identical to the pre-seam `prepare("BEGIN").run()` path).
    fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        forwarding_tx(self)
    }

    /// Acquire the single-connection tx handle WITHOUT BEGIN (#93 seam-routed tx control): the tx
    /// runtime seam-issues BEGIN/COMMIT/ROLLBACK on this one connection — byte-identical SQL to the
    /// pre-#93 forwarding path, only issued from the seam (so a middleware observes it).
    fn acquire_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        forwarding_tx_no_begin(self)
    }
}

struct SqlitePrepared<'a> {
    conn: &'a Connection,
    sql: String,
}

impl SqlitePrepared<'_> {
    fn bind(&self, params: &[Value]) -> Result<Vec<SqlValue>, SqlFailure> {
        params.iter().map(to_sql_value).collect()
    }
}

impl PreparedStatement for SqlitePrepared<'_> {
    fn all(&mut self, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        let bound = self.bind(params)?;
        let mut stmt = self
            .conn
            .prepare(&self.sql)
            .map_err(|e| map_sqlite_error(&e))?;
        let col_names: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let param_refs: Vec<&dyn rusqlite::ToSql> =
            bound.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
        let mut rows = stmt
            .query(param_refs.as_slice())
            .map_err(|e| map_sqlite_error(&e))?;
        let mut out: Vec<Value> = Vec::new();
        loop {
            match rows.next() {
                Ok(Some(row)) => {
                    let mut obj: Vec<(String, Value)> = Vec::with_capacity(col_names.len());
                    for (i, name) in col_names.iter().enumerate() {
                        let cell = row.get_ref(i).map_err(|e| map_sqlite_error(&e))?;
                        obj.push((name.clone(), from_sql_ref(cell)));
                    }
                    out.push(Value::Obj(obj));
                }
                Ok(None) => break,
                Err(e) => return Err(map_sqlite_error(&e)),
            }
        }
        Ok(out)
    }

    fn run(&mut self, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        let bound = self.bind(params)?;
        let param_refs: Vec<&dyn rusqlite::ToSql> =
            bound.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
        let mut stmt = self
            .conn
            .prepare(&self.sql)
            .map_err(|e| map_sqlite_error(&e))?;
        let changes = stmt
            .execute(param_refs.as_slice())
            .map_err(|e| map_sqlite_error(&e))?;
        Ok(RunInfo {
            changes: changes as i64,
            last_insert_rowid: self.conn.last_insert_rowid(),
        })
    }
}
