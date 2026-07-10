//! litedbmodel v2 SCP — live PostgreSQL / MySQL drivers (Rust, WS7g #36; async+pool #40).
//!
//! The SAME [`Driver`](crate::driver::Driver) seam the SQLite conformance uses, now backed by
//! ASYNC connection POOLS — restoring the old `litedbmodel.rs` execution model that the v2
//! migration had regressed to synchronous single-connection `postgres`/`mysql` crates (#40):
//!
//!   - Postgres: `tokio-postgres` + `deadpool-postgres` (`Pool::Postgres(deadpool_postgres::Pool)`
//!     in the old `.rs`). `$N` is the native placeholder and RETURNING is native — nothing to adapt.
//!   - MySQL / SQLite: `sqlx` on the tokio runtime (`Pool::Mysql(MySqlPool)` /
//!     `Pool::Sqlite(SqlitePool)` in the old `.rs`). MySQL uses `?` natively; MySQL 8.0 has NO
//!     `RETURNING`, so an `INSERT … RETURNING` is emulated at this seam (strip → INSERT → re-select
//!     the AUTO_INCREMENT PK) — the dialect-behavior-by-convention the WS6 TS ScpDialect uses.
//!
//! ## Why the sync [`Driver`] facade over an async pool
//!
//! The runtime's [`Driver`] trait is a synchronous facade (`prepare(sql).all()/.run()`). Each live
//! driver owns a shared multi-thread tokio [`Runtime`] and `block_on`s the pooled future. Because a
//! pool is `Clone`-cheap and internally synchronized, the driver is `Send + Sync`: DISTINCT threads
//! calling `all()` concurrently each check out a DISTINCT pooled connection, so the plan's
//! `concurrency` (default 16) becomes REAL parallel DB I/O when the executor dispatches independent
//! sibling relations in parallel (`static_bundle::execute_read_graph_parallel`).
//!
//! ## Write-tx stays SERIAL on ONE pinned connection
//!
//! A write bundle rides `execute_transaction_bundle`, which issues `BEGIN` … `COMMIT`/`ROLLBACK`
//! as literal statements bracketing the gate-first tx-DAG. A pool would hand each statement a
//! DIFFERENT connection, splitting the transaction. So on `BEGIN` the driver PINS one pooled
//! connection into a single writer slot and routes every subsequent statement to it until
//! `COMMIT`/`ROLLBACK` releases it — one connection, tx-DAG topological order, gate-first
//! short-circuit, exactly as before. Writes are NEVER parallelized.

use std::sync::Mutex;

use behavior_contracts::Value;
use deadpool_postgres::{Config as PgConfig, Pool as PgPool, Runtime as PgRuntime};
use sqlx::mysql::{MySqlPoolOptions, MySqlRow};
use sqlx::{Column, MySqlPool, Row, TypeInfo, ValueRef as SqlxValueRef};
use tokio::runtime::Runtime;
use tokio_postgres::types::{ToSql, Type as PgType};
use tokio_postgres::NoTls;

use crate::driver::{Driver, PreparedStatement, RunInfo};
use crate::errors::SqlFailure;

fn driver_failure(msg: impl Into<String>) -> SqlFailure {
    SqlFailure {
        kind: "driver_error".into(),
        policy: "fail".into(),
        sqlite_code: None,
        message: msg.into(),
    }
}

/// The plan's default concurrency (spec) — the pool is sized to match so `concurrency` sibling
/// relations can each hold a live connection without starving.
pub const DEFAULT_POOL_SIZE: usize = 16;

fn is_txn_control(sql: &str) -> bool {
    let s = sql.trim().to_ascii_uppercase();
    s == "BEGIN" || s == "COMMIT" || s == "ROLLBACK" || s == "START TRANSACTION"
}

// ── Postgres (tokio-postgres + deadpool-postgres) ──────────────────────────────

/// A live Postgres driver: a `deadpool-postgres` pool over `tokio-postgres`, on a shared tokio
/// runtime. Reads check out a pooled connection per call (parallel-safe); a write-tx pins ONE
/// connection for the BEGIN…COMMIT span.
pub struct PostgresDriver {
    rt: Runtime,
    pool: PgPool,
    /// The pinned writer connection for the active transaction (single-slot). `None` between txns.
    writer: Mutex<Option<deadpool_postgres::Object>>,
}

impl PostgresDriver {
    /// Connect with a libpq-style conn string, e.g.
    /// "host=localhost port=5433 user=testuser password=testpass dbname=testdb".
    pub fn connect(conn: &str) -> Result<Self, SqlFailure> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(DEFAULT_POOL_SIZE)
            .enable_all()
            .build()
            .map_err(|e| driver_failure(format!("postgres runtime: {e}")))?;

        let pg_conf: tokio_postgres::Config = conn
            .parse()
            .map_err(|e| driver_failure(format!("postgres conn parse: {e}")))?;
        let mut cfg = PgConfig::new();
        cfg.host = pg_conf.get_hosts().first().map(|h| match h {
            tokio_postgres::config::Host::Tcp(s) => s.clone(),
            #[cfg(unix)]
            tokio_postgres::config::Host::Unix(p) => p.to_string_lossy().into_owned(),
        });
        cfg.port = pg_conf.get_ports().first().copied();
        cfg.user = pg_conf.get_user().map(str::to_string);
        cfg.password = pg_conf
            .get_password()
            .map(|p| String::from_utf8_lossy(p).into_owned());
        cfg.dbname = pg_conf.get_dbname().map(str::to_string);
        cfg.options = pg_conf.get_options().map(str::to_string);
        cfg.pool = Some(deadpool_postgres::PoolConfig::new(DEFAULT_POOL_SIZE));

        let pool = cfg
            .create_pool(Some(PgRuntime::Tokio1), NoTls)
            .map_err(|e| driver_failure(format!("postgres pool: {e}")))?;

        Ok(PostgresDriver {
            rt,
            pool,
            writer: Mutex::new(None),
        })
    }

    pub fn exec_ddl(&self, statements: &[String]) -> Result<(), SqlFailure> {
        self.rt.block_on(async {
            let client = self
                .pool
                .get()
                .await
                .map_err(|e| driver_failure(format!("postgres pool get: {e}")))?;
            for stmt in statements {
                client
                    .batch_execute(stmt)
                    .await
                    .map_err(|e| driver_failure(format!("postgres ddl {stmt:?}: {e}")))?;
            }
            Ok(())
        })
    }
}

impl Driver for PostgresDriver {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        Box::new(PgPrepared {
            driver: self,
            sql: sql.to_string(),
        })
    }
}

struct PgPrepared<'a> {
    driver: &'a PostgresDriver,
    sql: String,
}

/// An owned param that implements `tokio_postgres::types::ToSql` for the scalar shapes render emits.
#[derive(Debug)]
enum PgParam {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

impl ToSql for PgParam {
    fn to_sql(
        &self,
        ty: &PgType,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            PgParam::Null => Ok(tokio_postgres::types::IsNull::Yes),
            PgParam::Bool(b) => b.to_sql(ty, out),
            // Serialize the integer in the WIDTH the target column expects (int2/int4/int8): bc has
            // one i64 int, but PG's binary protocol requires the exact width for the inferred param
            // type — binding i64 into an int4 slot fails ("incorrect binary data format").
            PgParam::Int(i) => match *ty {
                PgType::INT2 => (*i as i16).to_sql(ty, out),
                PgType::INT4 => (*i as i32).to_sql(ty, out),
                PgType::INT8 => i.to_sql(ty, out),
                // A non-integer target (e.g. text created_at) — bind the decimal text so the server
                // coerces it exactly as it would a literal.
                PgType::TEXT | PgType::VARCHAR | PgType::BPCHAR | PgType::UNKNOWN => {
                    i.to_string().to_sql(ty, out)
                }
                _ => i.to_sql(ty, out),
            },
            PgParam::Float(f) => match *ty {
                PgType::FLOAT4 => (*f as f32).to_sql(ty, out),
                _ => f.to_sql(ty, out),
            },
            PgParam::Str(s) => s.to_sql(ty, out),
        }
    }
    // Accept any target column type — the DB coerces the text/number param. We never reject at the
    // client so a well-formed rendered param binds; a genuine type error surfaces from the server.
    fn accepts(_ty: &PgType) -> bool {
        true
    }
    tokio_postgres::types::to_sql_checked!();
}

fn to_pg_param(v: &Value) -> Result<PgParam, SqlFailure> {
    match v {
        Value::Null => Ok(PgParam::Null),
        Value::Bool(b) => Ok(PgParam::Bool(*b)),
        Value::Int(i) => Ok(PgParam::Int(*i)),
        Value::Float(f) => Ok(PgParam::Float(*f)),
        Value::Str(s) => Ok(PgParam::Str(s.clone())),
        other => Err(driver_failure(format!(
            "scp pg driver: a {} reached the param binder (expected a scalar)",
            other.type_name()
        ))),
    }
}

/// Read one Postgres column into a bc [`Value`], matching the SQLite conformance row encoding.
fn pg_cell_to_value(row: &tokio_postgres::Row, idx: usize) -> Result<Value, SqlFailure> {
    let col = &row.columns()[idx];
    let ty = col.type_();
    let v = match *ty {
        PgType::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .map(|o| o.map(|i| Value::Int(i as i64))),
        PgType::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .map(|o| o.map(|i| Value::Int(i as i64))),
        PgType::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .map(|o| o.map(Value::Int)),
        PgType::FLOAT4 => row
            .try_get::<_, Option<f32>>(idx)
            .map(|o| o.map(|f| Value::Float(f as f64))),
        PgType::FLOAT8 => row
            .try_get::<_, Option<f64>>(idx)
            .map(|o| o.map(Value::Float)),
        PgType::BOOL => row
            .try_get::<_, Option<bool>>(idx)
            .map(|o| o.map(Value::Bool)),
        _ => row
            .try_get::<_, Option<String>>(idx)
            .map(|o| o.map(Value::Str)),
    }
    .map_err(|e| driver_failure(format!("postgres read col {}: {e}", col.name())))?;
    Ok(v.unwrap_or(Value::Null))
}

fn pg_rows_to_values(rows: &[tokio_postgres::Row]) -> Result<Vec<Value>, SqlFailure> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut obj: Vec<(String, Value)> = Vec::with_capacity(row.columns().len());
        for (i, col) in row.columns().iter().enumerate() {
            obj.push((col.name().to_string(), pg_cell_to_value(row, i)?));
        }
        out.push(Value::Obj(obj));
    }
    Ok(out)
}

impl PreparedStatement for PgPrepared<'_> {
    fn all(&mut self, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        let owned: Vec<PgParam> = params.iter().map(to_pg_param).collect::<Result<_, _>>()?;
        let refs: Vec<&(dyn ToSql + Sync)> =
            owned.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let sql = self.sql.clone();
        let driver = self.driver;
        // TAKE the pinned writer out of the mutex (leaving None) so the guard is NEVER held across
        // an await — clippy::await_holding_lock. In a write-tx the pinned conn is used + restored;
        // outside one (the parallel read path) we check out a pooled conn, so DISTINCT threads never
        // contend for the same connection.
        let pinned = driver.writer.lock().unwrap().take();
        driver.rt.block_on(async move {
            match pinned {
                Some(client) => {
                    let res = client.query(sql.as_str(), refs.as_slice()).await;
                    *driver.writer.lock().unwrap() = Some(client); // restore
                    let rows =
                        res.map_err(|e| driver_failure(format!("postgres query [{sql}]: {e}")))?;
                    pg_rows_to_values(&rows)
                }
                None => {
                    let client = driver
                        .pool
                        .get()
                        .await
                        .map_err(|e| driver_failure(format!("postgres pool get: {e}")))?;
                    let rows = client
                        .query(sql.as_str(), refs.as_slice())
                        .await
                        .map_err(|e| driver_failure(format!("postgres query [{sql}]: {e}")))?;
                    pg_rows_to_values(&rows)
                }
            }
        })
    }

    fn run(&mut self, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        let sql = self.sql.clone();
        let driver = self.driver;

        // Transaction-control literals PIN / RELEASE the single writer connection.
        if params.is_empty() && is_txn_control(&sql) {
            let upper = sql.trim().to_ascii_uppercase();
            return driver.rt.block_on(async move {
                if upper == "BEGIN" || upper == "START TRANSACTION" {
                    let client = driver
                        .pool
                        .get()
                        .await
                        .map_err(|e| driver_failure(format!("postgres pool get: {e}")))?;
                    client
                        .batch_execute("BEGIN")
                        .await
                        .map_err(|e| driver_failure(format!("postgres BEGIN: {e}")))?;
                    *driver.writer.lock().unwrap() = Some(client);
                } else {
                    // COMMIT / ROLLBACK on the pinned writer, then release it back to the pool.
                    let client = driver.writer.lock().unwrap().take();
                    if let Some(client) = client {
                        client
                            .batch_execute(&upper)
                            .await
                            .map_err(|e| driver_failure(format!("postgres {upper}: {e}")))?;
                    }
                }
                Ok(RunInfo {
                    changes: 0,
                    last_insert_rowid: 0,
                })
            });
        }

        let owned: Vec<PgParam> = params.iter().map(to_pg_param).collect::<Result<_, _>>()?;
        let refs: Vec<&(dyn ToSql + Sync)> =
            owned.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let pinned = driver.writer.lock().unwrap().take();
        driver.rt.block_on(async move {
            let changes = match pinned {
                Some(client) => {
                    let res = client.execute(sql.as_str(), refs.as_slice()).await;
                    *driver.writer.lock().unwrap() = Some(client); // restore
                    res.map_err(|e| driver_failure(format!("postgres execute: {e}")))?
                }
                None => {
                    let client = driver
                        .pool
                        .get()
                        .await
                        .map_err(|e| driver_failure(format!("postgres pool get: {e}")))?;
                    client
                        .execute(sql.as_str(), refs.as_slice())
                        .await
                        .map_err(|e| driver_failure(format!("postgres execute: {e}")))?
                }
            };
            Ok(RunInfo {
                changes: changes as i64,
                last_insert_rowid: 0, // PG has no lastInsertId; the RETURNING path uses `all`.
            })
        })
    }
}

// ── MySQL (sqlx MySqlPool) ─────────────────────────────────────────────────────

/// A live MySQL driver: a `sqlx` `MySqlPool` on a shared tokio runtime (the old `.rs`
/// `Pool::Mysql(MySqlPool)`). `?` is native; emulates the missing `INSERT … RETURNING`.
pub struct MysqlDriver {
    rt: Runtime,
    pool: MySqlPool,
    /// Pinned writer connection for the active transaction (single-slot). `None` between txns.
    writer: Mutex<Option<sqlx::pool::PoolConnection<sqlx::MySql>>>,
}

impl MysqlDriver {
    /// Connect via a URL, e.g. "mysql://testuser:testpass@127.0.0.1:3307/scp_rust".
    pub fn connect(url: &str) -> Result<Self, SqlFailure> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(DEFAULT_POOL_SIZE)
            .enable_all()
            .build()
            .map_err(|e| driver_failure(format!("mysql runtime: {e}")))?;
        let pool = rt.block_on(async {
            MySqlPoolOptions::new()
                .max_connections(DEFAULT_POOL_SIZE as u32)
                .connect(url)
                .await
                .map_err(|e| driver_failure(format!("mysql connect: {e}")))
        })?;
        Ok(MysqlDriver {
            rt,
            pool,
            writer: Mutex::new(None),
        })
    }

    pub fn exec_ddl(&self, statements: &[String]) -> Result<(), SqlFailure> {
        self.rt.block_on(async {
            for stmt in statements {
                sqlx::query(stmt)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| driver_failure(format!("mysql ddl {stmt:?}: {e}")))?;
            }
            Ok(())
        })
    }
}

impl Driver for MysqlDriver {
    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        Box::new(MyPrepared {
            driver: self,
            sql: sql.to_string(),
        })
    }
}

struct MyPrepared<'a> {
    driver: &'a MysqlDriver,
    sql: String,
}

/// Bind the scalar bc [`Value`] params onto a sqlx MySQL query.
fn bind_my<'q>(
    mut q: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    params: &[Value],
) -> Result<sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>, SqlFailure> {
    for v in params {
        q = match v {
            Value::Null => q.bind(Option::<String>::None),
            Value::Bool(b) => q.bind(if *b { 1_i64 } else { 0_i64 }),
            Value::Int(i) => q.bind(*i),
            Value::Float(f) => q.bind(*f),
            Value::Str(s) => q.bind(s.clone()),
            other => {
                return Err(driver_failure(format!(
                    "scp mysql driver: a {} reached the param binder (expected a scalar)",
                    other.type_name()
                )))
            }
        };
    }
    Ok(q)
}

fn my_cell_to_value(row: &MySqlRow, idx: usize) -> Result<Value, SqlFailure> {
    let col = row.column(idx);
    let raw = row
        .try_get_raw(idx)
        .map_err(|e| driver_failure(format!("mysql read col {}: {e}", col.name())))?;
    if raw.is_null() {
        return Ok(Value::Null);
    }
    let type_name = col.type_info().name().to_ascii_uppercase();
    // Integer families → Int; float/decimal → Float; everything else → string.
    let v = if type_name.contains("INT") {
        row.try_get::<i64, _>(idx).map(Value::Int)
    } else if type_name.contains("FLOAT")
        || type_name.contains("DOUBLE")
        || type_name.contains("DECIMAL")
    {
        row.try_get::<f64, _>(idx).map(Value::Float)
    } else {
        // Fall back to string for text/date/blob; try i64 first for count(*) style BIGINT aliases.
        row.try_get::<String, _>(idx).map(Value::Str)
    }
    .map_err(|e| driver_failure(format!("mysql decode col {}: {e}", col.name())))?;
    Ok(v)
}

fn my_rows_to_values(rows: &[MySqlRow]) -> Result<Vec<Value>, SqlFailure> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut obj: Vec<(String, Value)> = Vec::with_capacity(row.columns().len());
        for (i, col) in row.columns().iter().enumerate() {
            obj.push((col.name().to_string(), my_cell_to_value(row, i)?));
        }
        out.push(Value::Obj(obj));
    }
    Ok(out)
}

/// Parse `INSERT [IGNORE] INTO <table> ( … ) … RETURNING <cols>` → (table, cols, stripped-insert).
fn parse_mysql_returning(sql: &str) -> Option<(String, String, String)> {
    let lower = sql.to_ascii_lowercase();
    let ret_pos = lower.rfind(" returning ")?;
    let trimmed = lower.trim_start();
    if !trimmed.starts_with("insert ") {
        return None;
    }
    let into_pos = lower.find(" into ")? + " into ".len();
    let after_into = &sql[into_pos..];
    let table: String = after_into
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    let cols = sql[ret_pos + " returning ".len()..].trim().to_string();
    let insert = sql[..ret_pos].to_string();
    Some((table, cols, insert))
}

impl PreparedStatement for MyPrepared<'_> {
    fn all(&mut self, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        let sql = self.sql.clone();
        let driver = self.driver;
        let params = params.to_vec();
        // TAKE the pinned writer out of the mutex so no guard is held across an await
        // (clippy::await_holding_lock). It is threaded through the future by value and RESTORED
        // afterwards. Outside a write-tx (`pinned == None`) we run on a pooled connection, so
        // DISTINCT parallel-read threads never contend for the same connection.
        let mut pinned = driver.writer.lock().unwrap().take();
        let result = driver.rt.block_on(async {
            // MySQL RETURNING emulation: strip → INSERT → re-select the AUTO_INCREMENT PK's cols.
            if let Some((table, cols, insert_sql)) = parse_mysql_returning(&sql) {
                let q = bind_my(sqlx::query(&insert_sql), &params)?;
                let last_id = match pinned.as_mut() {
                    Some(conn) => q
                        .execute(&mut **conn)
                        .await
                        .map_err(|e| driver_failure(format!("mysql exec insert: {e}")))?
                        .last_insert_id() as i64,
                    None => q
                        .execute(&driver.pool)
                        .await
                        .map_err(|e| driver_failure(format!("mysql exec insert: {e}")))?
                        .last_insert_id() as i64,
                };
                let sel = format!("SELECT {cols} FROM {table} WHERE id = ?");
                let rows = match pinned.as_mut() {
                    Some(conn) => sqlx::query(&sel).bind(last_id).fetch_all(&mut **conn).await,
                    None => {
                        sqlx::query(&sel)
                            .bind(last_id)
                            .fetch_all(&driver.pool)
                            .await
                    }
                }
                .map_err(|e| driver_failure(format!("mysql re-select: {e}")))?;
                return my_rows_to_values(&rows);
            }

            let q = bind_my(sqlx::query(&sql), &params)?;
            let rows = match pinned.as_mut() {
                Some(conn) => q.fetch_all(&mut **conn).await,
                None => q.fetch_all(&driver.pool).await,
            }
            .map_err(|e| driver_failure(format!("mysql query [{sql}]: {e}")))?;
            my_rows_to_values(&rows)
        });
        if let Some(conn) = pinned {
            *driver.writer.lock().unwrap() = Some(conn); // restore the pinned writer
        }
        result
    }

    fn run(&mut self, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        let sql = self.sql.clone();
        let driver = self.driver;
        let params = params.to_vec();

        if params.is_empty() && is_txn_control(&sql) {
            let upper = sql.trim().to_ascii_uppercase();
            return driver.rt.block_on(async move {
                // Tx-control must run UNPREPARED: MySQL rejects BEGIN/COMMIT/ROLLBACK in the
                // prepared-statement protocol (error 1295). `raw_sql` uses the simple-query path.
                if upper == "BEGIN" || upper == "START TRANSACTION" {
                    let mut conn = driver
                        .pool
                        .acquire()
                        .await
                        .map_err(|e| driver_failure(format!("mysql acquire: {e}")))?;
                    sqlx::raw_sql("BEGIN")
                        .execute(&mut *conn)
                        .await
                        .map_err(|e| driver_failure(format!("mysql BEGIN: {e}")))?;
                    *driver.writer.lock().unwrap() = Some(conn);
                } else {
                    let conn = driver.writer.lock().unwrap().take();
                    if let Some(mut conn) = conn {
                        sqlx::raw_sql(if upper == "COMMIT" {
                            "COMMIT"
                        } else {
                            "ROLLBACK"
                        })
                        .execute(&mut *conn)
                        .await
                        .map_err(|e| driver_failure(format!("mysql {upper}: {e}")))?;
                    }
                }
                Ok(RunInfo {
                    changes: 0,
                    last_insert_rowid: 0,
                })
            });
        }

        let mut pinned = driver.writer.lock().unwrap().take();
        let result = driver.rt.block_on(async {
            let q = bind_my(sqlx::query(&sql), &params)?;
            let res = match pinned.as_mut() {
                Some(conn) => q.execute(&mut **conn).await,
                None => q.execute(&driver.pool).await,
            }
            .map_err(|e| driver_failure(format!("mysql exec [{sql}]: {e}")))?;
            Ok(RunInfo {
                changes: res.rows_affected() as i64,
                last_insert_rowid: res.last_insert_id() as i64,
            })
        });
        if let Some(conn) = pinned {
            *driver.writer.lock().unwrap() = Some(conn); // restore the pinned writer
        }
        result
    }
}
