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
//! sibling relations in parallel (`static_bundle::dispatch_read_nodes_parallel`).
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

/// An owned param that implements `tokio_postgres::types::ToSql` for the shapes render emits: the
/// scalar values, plus a homogeneous ARRAY for a no-cast `= ANY($1)` IN-list / relation-batch
/// `= ANY($1::T[])` (#46 — the authored PG IN-list binds the list as ONE array param, letting PG
/// infer the element type from the column: int / uuid / empty).
#[derive(Debug)]
enum PgParam {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Array(Vec<PgParam>),
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
                // A `numeric` target (`= ANY($1)` over a NUMERIC/DECIMAL column, #46 item 4): PG
                // infers `numeric[]` and expects each element in NUMERIC binary — a bare f64 into a
                // numeric slot fails ("db error"). Encode the exact NUMERIC via rust_decimal.
                PgType::NUMERIC => {
                    let d = rust_decimal::Decimal::try_from(*f).map_err(
                        |e| -> Box<dyn std::error::Error + Sync + Send> {
                            format!("scp pg driver: numeric param {f} not representable: {e}")
                                .into()
                        },
                    )?;
                    d.to_sql(ty, out)
                }
                _ => f.to_sql(ty, out),
            },
            // A UUID column/element: PG's binary protocol expects the 16-byte form, and
            // `String::to_sql` only accepts text types — so serialize the canonical hex string to
            // its 16 bytes. This keeps the authored no-cast `= ANY($1)` form (#46 uuid IN-list): PG
            // infers `uuid[]` from the column, and each element binds as a real uuid.
            PgParam::Str(s) if *ty == PgType::UUID => {
                let bytes = uuid_text_to_bytes(s).ok_or_else(
                    || -> Box<dyn std::error::Error + Sync + Send> {
                        format!("scp pg driver: malformed uuid text {s:?}").into()
                    },
                )?;
                out.extend_from_slice(&bytes);
                Ok(tokio_postgres::types::IsNull::No)
            }
            // A timestamp/date target (`= ANY($1)` over a TIMESTAMP/DATE column, #46 item 4): PG
            // infers `timestamp[]` and expects TIMESTAMP binary — `String::to_sql` only accepts text
            // types and fails. Parse the canonical `YYYY-MM-DD[ HH:MM:SS]` text via chrono and bind
            // the native temporal (tokio-postgres with-chrono-0_4 encodes the binary form).
            PgParam::Str(s) if matches!(*ty, PgType::TIMESTAMP | PgType::TIMESTAMPTZ) => {
                let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .or_else(|_| {
                        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                    })
                    .map_err(|e| -> Box<dyn std::error::Error + Sync + Send> {
                        format!("scp pg driver: malformed timestamp text {s:?}: {e}").into()
                    })?;
                dt.to_sql(ty, out)
            }
            PgParam::Str(s) if *ty == PgType::DATE => {
                let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").map_err(
                    |e| -> Box<dyn std::error::Error + Sync + Send> {
                        format!("scp pg driver: malformed date text {s:?}: {e}").into()
                    },
                )?;
                d.to_sql(ty, out)
            }
            PgParam::Str(s) => s.to_sql(ty, out),
            // A `= ANY($1)` / `= ANY($1::T[])` array param: `ty` is the ARRAY type PG inferred from
            // the column (e.g. `_int4`, `_uuid`, `_text`). Delegate to tokio-postgres's slice array
            // serialization, which reads `ty.kind() == Array(member)` and serializes each element in
            // the width the column's element type expects (the same width-aware path the scalars use
            // above, since each element is itself a `PgParam` whose `to_sql` sees the member type).
            PgParam::Array(elems) => elems
                .as_slice()
                .to_sql(ty, out)
                .map(|_| tokio_postgres::types::IsNull::No),
        }
    }
    // Accept any target column type — the DB coerces the text/number param. We never reject at the
    // client so a well-formed rendered param binds; a genuine type error surfaces from the server.
    fn accepts(_ty: &PgType) -> bool {
        true
    }
    tokio_postgres::types::to_sql_checked!();
}

/// A `uuid` column read as its canonical text. tokio-postgres has no built-in `String`
/// `FromSql` for `uuid` (that needs the `uuid` crate feature), so read the raw 16 bytes and format
/// the canonical `8-4-4-4-12` hex — the SAME uuid-as-text form SQLite/MySQL return, so the assembled
/// row encodes identically across dialects.
struct PgUuidText(String);

impl<'a> tokio_postgres::types::FromSql<'a> for PgUuidText {
    fn from_sql(
        _ty: &PgType,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err(format!(
                "scp pg driver: uuid column is {} bytes (expected 16)",
                raw.len()
            )
            .into());
        }
        let h = |b: u8| format!("{b:02x}");
        let hex: String = raw.iter().map(|b| h(*b)).collect();
        // Insert hyphens at the canonical 8-4-4-4-12 boundaries.
        let text = format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32],
        );
        Ok(PgUuidText(text))
    }

    fn accepts(ty: &PgType) -> bool {
        *ty == PgType::UUID
    }
}

/// Parse a canonical UUID text (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, hyphens optional) to its 16
/// raw bytes — the wire form PG's binary `uuid` param expects. Returns `None` on a malformed input.
fn uuid_text_to_bytes(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

fn to_pg_param(v: &Value) -> Result<PgParam, SqlFailure> {
    match v {
        Value::Null => Ok(PgParam::Null),
        Value::Bool(b) => Ok(PgParam::Bool(*b)),
        Value::Int(i) => Ok(PgParam::Int(*i)),
        Value::Float(f) => Ok(PgParam::Float(*f)),
        Value::Str(s) => Ok(PgParam::Str(s.clone())),
        // A no-cast `= ANY($1)` IN-list / relation-batch `= ANY($1::T[])` binds the list as ONE
        // array param (#46) — recurse to build the homogeneous element vector.
        Value::Arr(elems) => Ok(PgParam::Array(
            elems
                .iter()
                .map(to_pg_param)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        other => Err(driver_failure(format!(
            "scp pg driver: a {} reached the param binder (expected a scalar or array)",
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
        // A `uuid` column: tokio-postgres has no String FromSql for uuid, so read the raw 16 bytes
        // and format the canonical text — matching the SQLite/MySQL uuid-as-text row encoding.
        PgType::UUID => row
            .try_get::<_, Option<PgUuidText>>(idx)
            .map(|o| o.map(|u| Value::Str(u.0))),
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
    // Integer families → Int; float/decimal → Float; everything else → string. An UNSIGNED integer
    // column (e.g. the `ROW_NUMBER() OVER (...) AS _rn` window column MySQL types as BIGINT UNSIGNED
    // in a limited-hasMany relation batch) decodes as u64 in sqlx, so read it as u64 then narrow to
    // the bc i64 int (the corpus values are well within the i64 range).
    let v = if type_name.contains("INT") {
        if type_name.contains("UNSIGNED") {
            row.try_get::<u64, _>(idx).map(|u| Value::Int(u as i64))
        } else {
            row.try_get::<i64, _>(idx).map(Value::Int)
        }
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

/// The parsed pieces of an `INSERT … RETURNING` for the MySQL RETURNING emulation.
struct MysqlReturning {
    table: String,
    /// RETURNING columns text with the strip-before-execute PK hint removed.
    cols: String,
    /// The INSERT with the RETURNING clause AND the PK hint stripped (byte-clean for execution).
    insert: String,
    /// The real PK columns (from the ` /*scp:pk=…*/` hint); empty ⇒ legacy `id` path.
    pk_cols: Vec<String>,
    /// The AUTO_INCREMENT column name (from the hint), or empty for a client-supplied PK.
    auto_inc: String,
    /// The INSERT column list (for pulling client-PK values by position).
    insert_cols: Vec<String>,
}

/// Parse `INSERT [IGNORE] INTO <table> ( … ) … RETURNING <cols> [/*scp:pk=…;ai=…*/]`.
fn parse_mysql_returning(sql: &str) -> Option<MysqlReturning> {
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
    let cols_raw = sql[ret_pos + " returning ".len()..].trim().to_string();
    let insert_raw = sql[..ret_pos].to_string();

    // Parse + strip the PK hint ` /*scp:pk=col1,col2;ai=<col|>*/`.
    let (pk_cols, auto_inc) = parse_pk_hint(&cols_raw);
    let cols = strip_pk_hint(&cols_raw).trim().to_string();
    let insert = strip_pk_hint(&insert_raw);

    // Parse the INSERT column list `INSERT [IGNORE] INTO <t> (c1, c2, …)`.
    let insert_cols = parse_insert_cols(&insert);

    Some(MysqlReturning {
        table,
        cols,
        insert,
        pk_cols,
        auto_inc,
        insert_cols,
    })
}

/// If `sql` is a NON-INSERT statement carrying a RETURNING clause (UPDATE/DELETE … RETURNING),
/// return the statement with the RETURNING clause + any PK hint stripped; else None.
fn strip_non_insert_returning(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let ret_pos = lower.rfind(" returning ")?;
    if lower.trim_start().starts_with("insert ") {
        return None; // handled by parse_mysql_returning
    }
    Some(strip_pk_hint(&sql[..ret_pos]))
}

/// Strip a ` /*scp:pk=…*/` hint comment from a fragment.
fn strip_pk_hint(s: &str) -> String {
    if let (Some(a), Some(rel)) = (s.find("/*scp:pk="), s.find("*/")) {
        // `rel` is the FIRST `*/`; guard it comes after the hint start.
        if rel > a {
            let mut out = String::new();
            out.push_str(s[..a].trim_end());
            out.push_str(&s[rel + 2..]);
            return out;
        }
    }
    s.to_string()
}

/// Parse the PK hint → (pk_columns, auto_inc). Absent ⇒ (empty, empty).
fn parse_pk_hint(s: &str) -> (Vec<String>, String) {
    let start = match s.find("/*scp:pk=") {
        Some(i) => i + "/*scp:pk=".len(),
        None => return (Vec::new(), String::new()),
    };
    let rest = &s[start..];
    let end = match rest.find("*/") {
        Some(i) => i,
        None => return (Vec::new(), String::new()),
    };
    let body = &rest[..end]; // `col1,col2;ai=<col|>`
    let mut parts = body.splitn(2, ";ai=");
    let cols_part = parts.next().unwrap_or("");
    let ai = parts.next().unwrap_or("").trim().to_string();
    let pk_cols: Vec<String> = cols_part
        .split(',')
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .collect();
    (pk_cols, ai)
}

/// Parse the INSERT column list `INSERT [IGNORE] INTO <t> (c1, c2, …)`.
fn parse_insert_cols(insert: &str) -> Vec<String> {
    if let (Some(open), Some(close)) = (insert.find('('), insert.find(')')) {
        if close > open {
            return insert[open + 1..close]
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
        }
    }
    Vec::new()
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
            // MySQL RETURNING emulation: strip → INSERT → re-select the inserted rows by the REAL
            // primary key. The strip-before-execute PK hint (tx.ts mysqlPkHint) carries the PK
            // columns + the AUTO_INCREMENT column, so the re-select keys off an AUTO_INCREMENT range
            // (int identity) or the client-supplied PK values (UUID / composite) pulled from the
            // bound INSERT params — NOT a hardcoded `WHERE id = ?` (which breaks for UUID/composite).
            if let Some(r) = parse_mysql_returning(&sql) {
                let q = bind_my(sqlx::query(&r.insert), &params)?;
                let exec_res = match pinned.as_mut() {
                    Some(conn) => q
                        .execute(&mut **conn)
                        .await
                        .map_err(|e| driver_failure(format!("mysql exec insert: {e}")))?,
                    None => q
                        .execute(&driver.pool)
                        .await
                        .map_err(|e| driver_failure(format!("mysql exec insert: {e}")))?,
                };
                let last_id = exec_res.last_insert_id() as i64;
                let affected = exec_res.rows_affected().max(1) as i64;

                // Build the re-select WHERE + its bound params.
                let (where_sql, where_params): (String, Vec<Value>) = if r.pk_cols.is_empty() {
                    ("id = ?".to_string(), vec![Value::Int(last_id)])
                } else if !r.auto_inc.is_empty()
                    && r.pk_cols.len() == 1
                    && r.pk_cols[0] == r.auto_inc
                {
                    (
                        format!("{ai} >= ? AND {ai} < ?", ai = r.auto_inc),
                        vec![Value::Int(last_id), Value::Int(last_id + affected)],
                    )
                } else {
                    // Client-supplied PK: pull each PK column's inserted value from the bound
                    // INSERT params by column position (single-row client-PK insert).
                    let mut conds: Vec<String> = Vec::new();
                    let mut vals: Vec<Value> = Vec::new();
                    let mut ok = true;
                    for pk in &r.pk_cols {
                        match r.insert_cols.iter().position(|c| c == pk) {
                            Some(idx) if idx < params.len() => {
                                conds.push(format!("{pk} = ?"));
                                vals.push(params[idx].clone());
                            }
                            _ => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    if ok {
                        (conds.join(" AND "), vals)
                    } else {
                        ("id = ?".to_string(), vec![Value::Int(last_id)])
                    }
                };

                let sel = format!(
                    "SELECT {cols} FROM {table} WHERE {where_sql}",
                    cols = r.cols,
                    table = r.table
                );
                let q2 = bind_my(sqlx::query(&sel), &where_params)?;
                let rows = match pinned.as_mut() {
                    Some(conn) => q2.fetch_all(&mut **conn).await,
                    None => q2.fetch_all(&driver.pool).await,
                }
                .map_err(|e| driver_failure(format!("mysql re-select: {e}")))?;
                return my_rows_to_values(&rows);
            }

            // A non-INSERT RETURNING (UPDATE/DELETE … RETURNING): MySQL has no native RETURNING and
            // the pre-image is gone, so v1 (`mysql.ts`) strips RETURNING, runs the write, returns NO
            // rows. Byte-faithful: execute the stripped write, return an empty row set.
            if let Some(write_sql) = strip_non_insert_returning(&sql) {
                let q = bind_my(sqlx::query(&write_sql), &params)?;
                match pinned.as_mut() {
                    Some(conn) => q.execute(&mut **conn).await,
                    None => q.execute(&driver.pool).await,
                }
                .map_err(|e| driver_failure(format!("mysql write [{write_sql}]: {e}")))?;
                return Ok(Vec::new());
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
