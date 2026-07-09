//! litedbmodel v2 SCP — live PostgreSQL / MySQL drivers (Rust, WS7g #36; `livedb` feature).
//!
//! The SAME synchronous [`Driver`](crate::driver::Driver) seam the SQLite conformance uses, now
//! backed by REAL synchronous `postgres` (Postgres) / `mysql` (MySQL) clients — proving the
//! deferred live-DB execution axis (spec §10 dialect axis). The runtime is UNCHANGED: it renders
//! the dialect-tagged bundle (Postgres → `$N`, MySQL → `?`), binds the rendered scalar params, and
//! calls `prepare(sql).all(...)` / `.run(...)`; the tx envelope issues `BEGIN`/`COMMIT`/`ROLLBACK`
//! as literal statements. Each live driver:
//!
//!   - Postgres: `$N` is the crate's native placeholder and RETURNING is native — nothing to adapt.
//!   - MySQL: `?` is native, but MySQL 8.0 has NO `RETURNING`, so an `INSERT … RETURNING` is
//!     emulated at this seam (strip → INSERT → re-select the AUTO_INCREMENT PK) — the
//!     dialect-behavior-by-convention the WS6 TS ScpDialect uses.
//!
//! Both clients run in autocommit mode so the runtime's explicit BEGIN/COMMIT/ROLLBACK literals
//! bracket a REAL transaction on the live DB (the gate-first write-tx).

use std::cell::RefCell;

use behavior_contracts::Value;

use crate::driver::{Driver, PreparedStatement, RunInfo};
use crate::errors::SqlFailure;

/// Expand a `postgres::Error` to include its DB-error source (the crate's Display is terse).
fn pg_err(e: &postgres::Error) -> String {
    use std::error::Error;
    match e.source() {
        Some(src) => format!("{e}: {src}"),
        None => format!("{e}"),
    }
}

fn driver_failure(msg: impl Into<String>) -> SqlFailure {
    SqlFailure {
        kind: "driver_error".into(),
        policy: "fail".into(),
        sqlite_code: None,
        message: msg.into(),
    }
}

// ── Postgres ────────────────────────────────────────────────────────────────

/// A live Postgres driver (`postgres` crate) implementing the [`Driver`] seam. Renders a
/// `postgres`-tagged bundle → `$N` (the crate's native placeholder); RETURNING is native.
pub struct PostgresDriver {
    client: RefCell<postgres::Client>,
}

impl PostgresDriver {
    /// Connect over TCP. `conn` e.g. "host=localhost port=5433 user=testuser password=testpass dbname=testdb".
    pub fn connect(conn: &str) -> Result<Self, SqlFailure> {
        let client = postgres::Client::connect(conn, postgres::NoTls)
            .map_err(|e| driver_failure(format!("postgres connect: {e}")))?;
        Ok(PostgresDriver {
            client: RefCell::new(client),
        })
    }

    pub fn exec_ddl(&self, statements: &[String]) -> Result<(), SqlFailure> {
        let mut c = self.client.borrow_mut();
        for stmt in statements {
            c.batch_execute(stmt)
                .map_err(|e| driver_failure(format!("postgres ddl {stmt:?}: {e}")))?;
        }
        Ok(())
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

/// An owned param that implements `postgres::types::ToSql` for the scalar shapes the render emits.
#[derive(Debug)]
enum PgParam {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

impl postgres::types::ToSql for PgParam {
    fn to_sql(
        &self,
        ty: &postgres::types::Type,
        out: &mut postgres::types::private::BytesMut,
    ) -> Result<postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        use postgres::types::Type;
        match self {
            PgParam::Null => Ok(postgres::types::IsNull::Yes),
            PgParam::Bool(b) => b.to_sql(ty, out),
            // Serialize the integer in the WIDTH the target column expects (int2/int4/int8). bc has
            // one i64 int, but PG's binary protocol requires the exact width for the inferred param
            // type — binding i64 into an int4 slot fails ("incorrect binary data format").
            PgParam::Int(i) => match *ty {
                Type::INT2 => (*i as i16).to_sql(ty, out),
                Type::INT4 => (*i as i32).to_sql(ty, out),
                Type::INT8 => i.to_sql(ty, out),
                // A non-integer target (e.g. text created_at, numeric) — bind the decimal text so
                // the server coerces it exactly as it would a literal.
                Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::UNKNOWN => {
                    i.to_string().to_sql(ty, out)
                }
                _ => i.to_sql(ty, out),
            },
            PgParam::Float(f) => match *ty {
                Type::FLOAT4 => (*f as f32).to_sql(ty, out),
                _ => f.to_sql(ty, out),
            },
            PgParam::Str(s) => s.to_sql(ty, out),
        }
    }
    // Accept any target column type — the DB coerces the text/number param (e.g. an i64 bound to a
    // text created_at, or a string to a varchar). We never reject at the client so a well-formed
    // rendered param binds; a genuine type error surfaces from the server, loud.
    fn accepts(_ty: &postgres::types::Type) -> bool {
        true
    }
    postgres::types::to_sql_checked!();
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

/// Read one Postgres column into a bc [`Value`], matching the SQLite conformance row encoding
/// (integer column → number, text → string, bool → bool, NULL → null).
fn pg_cell_to_value(row: &postgres::Row, idx: usize) -> Result<Value, SqlFailure> {
    use postgres::types::Type;
    let col = &row.columns()[idx];
    let ty = col.type_();
    let v = match *ty {
        Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .map(|o| o.map(|i| Value::Int(i as i64))),
        Type::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .map(|o| o.map(|i| Value::Int(i as i64))),
        Type::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .map(|o| o.map(Value::Int)),
        Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(idx)
            .map(|o| o.map(|f| Value::Float(f as f64))),
        Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(idx)
            .map(|o| o.map(Value::Float)),
        Type::BOOL => row
            .try_get::<_, Option<bool>>(idx)
            .map(|o| o.map(Value::Bool)),
        _ => row
            .try_get::<_, Option<String>>(idx)
            .map(|o| o.map(Value::Str)),
    }
    .map_err(|e| driver_failure(format!("postgres read col {}: {e}", col.name())))?;
    Ok(v.unwrap_or(Value::Null))
}

impl PreparedStatement for PgPrepared<'_> {
    fn all(&mut self, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        let owned: Vec<PgParam> = params.iter().map(to_pg_param).collect::<Result<_, _>>()?;
        let refs: Vec<&(dyn postgres::types::ToSql + Sync)> = owned
            .iter()
            .map(|p| p as &(dyn postgres::types::ToSql + Sync))
            .collect();
        let mut c = self.driver.client.borrow_mut();
        let rows = c.query(self.sql.as_str(), refs.as_slice()).map_err(|e| {
            driver_failure(format!("postgres query [{}]: {}", self.sql, pg_err(&e)))
        })?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut obj: Vec<(String, Value)> = Vec::with_capacity(row.columns().len());
            for (i, col) in row.columns().iter().enumerate() {
                obj.push((col.name().to_string(), pg_cell_to_value(row, i)?));
            }
            out.push(Value::Obj(obj));
        }
        Ok(out)
    }

    fn run(&mut self, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        // BEGIN/COMMIT/ROLLBACK + non-returning writes. `execute` runs a single statement; the tx
        // literals carry no params.
        let owned: Vec<PgParam> = params.iter().map(to_pg_param).collect::<Result<_, _>>()?;
        let refs: Vec<&(dyn postgres::types::ToSql + Sync)> = owned
            .iter()
            .map(|p| p as &(dyn postgres::types::ToSql + Sync))
            .collect();
        let mut c = self.driver.client.borrow_mut();
        let changes = c
            .execute(self.sql.as_str(), refs.as_slice())
            .map_err(|e| driver_failure(format!("postgres execute: {e}")))?;
        Ok(RunInfo {
            changes: changes as i64,
            last_insert_rowid: 0, // PG has no lastInsertId; the RETURNING path uses `all` instead.
        })
    }
}

// ── MySQL ─────────────────────────────────────────────────────────────────────

/// A live MySQL driver (`mysql` crate) implementing the [`Driver`] seam. Renders a `mysql`-tagged
/// bundle → `?` (native); emulates the missing `INSERT … RETURNING` (strip → insert → re-select).
pub struct MysqlDriver {
    conn: RefCell<mysql::Conn>,
}

impl MysqlDriver {
    /// Connect via a URL, e.g. "mysql://testuser:testpass@127.0.0.1:3307/scp_rust".
    pub fn connect(url: &str) -> Result<Self, SqlFailure> {
        let opts =
            mysql::Opts::from_url(url).map_err(|e| driver_failure(format!("mysql opts: {e}")))?;
        let conn =
            mysql::Conn::new(opts).map_err(|e| driver_failure(format!("mysql connect: {e}")))?;
        Ok(MysqlDriver {
            conn: RefCell::new(conn),
        })
    }

    pub fn exec_ddl(&self, statements: &[String]) -> Result<(), SqlFailure> {
        use mysql::prelude::Queryable;
        let mut c = self.conn.borrow_mut();
        for stmt in statements {
            c.query_drop(stmt)
                .map_err(|e| driver_failure(format!("mysql ddl {stmt:?}: {e}")))?;
        }
        Ok(())
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

fn to_my_param(v: &Value) -> Result<mysql::Value, SqlFailure> {
    Ok(match v {
        Value::Null => mysql::Value::NULL,
        Value::Bool(b) => mysql::Value::Int(if *b { 1 } else { 0 }),
        Value::Int(i) => mysql::Value::Int(*i),
        Value::Float(f) => mysql::Value::Double(*f),
        Value::Str(s) => mysql::Value::Bytes(s.clone().into_bytes()),
        other => {
            return Err(driver_failure(format!(
                "scp mysql driver: a {} reached the param binder (expected a scalar)",
                other.type_name()
            )))
        }
    })
}

fn my_cell_to_value(v: &mysql::Value) -> Value {
    match v {
        mysql::Value::NULL => Value::Null,
        mysql::Value::Int(i) => Value::Int(*i),
        mysql::Value::UInt(u) => Value::Int(*u as i64),
        mysql::Value::Float(f) => Value::Float(*f as f64),
        mysql::Value::Double(f) => Value::Float(*f),
        mysql::Value::Bytes(b) => Value::Str(String::from_utf8_lossy(b).into_owned()),
        // Date/Time — stringify (not exercised by the exec/tx corpus, but never fabricate).
        other => Value::Str(format!("{other:?}")),
    }
}

fn my_rows_to_values(rows: Vec<mysql::Row>) -> Vec<Value> {
    rows.into_iter()
        .map(|row| {
            let cols = row.columns();
            let mut obj: Vec<(String, Value)> = Vec::with_capacity(cols.len());
            for (i, col) in cols.iter().enumerate() {
                let cell = row.as_ref(i).cloned().unwrap_or(mysql::Value::NULL);
                obj.push((col.name_str().into_owned(), my_cell_to_value(&cell)));
            }
            Value::Obj(obj)
        })
        .collect()
}

/// Parse `INSERT [IGNORE] INTO <table> ( … ) … RETURNING <cols>` → (table, cols, stripped-insert).
fn parse_mysql_returning(sql: &str) -> Option<(String, String, String)> {
    let lower = sql.to_ascii_lowercase();
    let ret_pos = lower.rfind(" returning ")?;
    let trimmed = lower.trim_start();
    if !trimmed.starts_with("insert ") {
        return None;
    }
    // Table name: after "into ".
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
        use mysql::prelude::Queryable;
        let bound: Vec<mysql::Value> = params.iter().map(to_my_param).collect::<Result<_, _>>()?;

        // MySQL RETURNING emulation: strip → INSERT → re-select the AUTO_INCREMENT PK's columns.
        if let Some((table, cols, insert_sql)) = parse_mysql_returning(&self.sql) {
            let mut c = self.driver.conn.borrow_mut();
            let stmt = c
                .prep(&insert_sql)
                .map_err(|e| driver_failure(format!("mysql prep insert: {e}")))?;
            c.exec_drop(&stmt, mysql::Params::Positional(bound))
                .map_err(|e| driver_failure(format!("mysql exec insert: {e}")))?;
            let last_id = c.last_insert_id() as i64;
            let sel = format!("SELECT {cols} FROM {table} WHERE id = ?");
            let rows: Vec<mysql::Row> = c
                .exec(&sel, (last_id,))
                .map_err(|e| driver_failure(format!("mysql re-select: {e}")))?;
            return Ok(my_rows_to_values(rows));
        }

        let mut c = self.driver.conn.borrow_mut();
        let stmt = c
            .prep(&self.sql)
            .map_err(|e| driver_failure(format!("mysql prep: {e}")))?;
        let rows: Vec<mysql::Row> = c
            .exec(&stmt, mysql::Params::Positional(bound))
            .map_err(|e| driver_failure(format!("mysql query: {e}")))?;
        Ok(my_rows_to_values(rows))
    }

    fn run(&mut self, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        use mysql::prelude::Queryable;
        let mut c = self.driver.conn.borrow_mut();
        // Transaction-control literals (BEGIN/COMMIT/ROLLBACK) carry no params — run as plain query.
        if params.is_empty() && is_txn_control(&self.sql) {
            c.query_drop(&self.sql)
                .map_err(|e| driver_failure(format!("mysql {}: {e}", self.sql)))?;
            return Ok(RunInfo {
                changes: 0,
                last_insert_rowid: 0,
            });
        }
        let bound: Vec<mysql::Value> = params.iter().map(to_my_param).collect::<Result<_, _>>()?;
        let stmt = c
            .prep(&self.sql)
            .map_err(|e| driver_failure(format!("mysql prep: {e}")))?;
        c.exec_drop(&stmt, mysql::Params::Positional(bound))
            .map_err(|e| driver_failure(format!("mysql exec: {e}")))?;
        Ok(RunInfo {
            changes: c.affected_rows() as i64,
            last_insert_rowid: c.last_insert_id() as i64,
        })
    }
}

fn is_txn_control(sql: &str) -> bool {
    let s = sql.trim().to_ascii_uppercase();
    s == "BEGIN" || s == "COMMIT" || s == "ROLLBACK" || s == "START TRANSACTION"
}
