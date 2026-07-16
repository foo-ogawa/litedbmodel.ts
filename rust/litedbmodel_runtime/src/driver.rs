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
//! (`static_bundle::dispatch_read_nodes_parallel`) without changing the IR, the dialect SQL text, or
//! the trait: the seam is dialect- and execution-model-agnostic. The runtime only ever binds
//! already-rendered scalar params (a bc [`Value`] → the driver's native param type).

use behavior_contracts::Value;
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::Connection;

use crate::errors::{map_sqlite_error, SqlFailure};
use crate::exec_context::TxConnection;

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
    fn commit(self: Box<Self>) -> Result<(), SqlFailure> {
        self.driver.prepare("COMMIT").run(&[]).map(|_| ())
    }
    fn rollback(self: Box<Self>) -> Result<(), SqlFailure> {
        self.driver.prepare("ROLLBACK").run(&[]).map(|_| ())
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
        Value::Arr(_) | Value::Obj(_) => Err(SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: format!(
                "scp driver: a {} reached the param binder (expected a scalar)",
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
