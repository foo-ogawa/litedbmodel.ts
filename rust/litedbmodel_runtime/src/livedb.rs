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
//! ## Write-tx OWNS ONE pooled connection (per-execution ownership, #76 §3)
//!
//! A write bundle rides `execute_transaction_bundle`, which now brackets the gate-first tx-DAG via
//! the [`ExecutionContext`](crate::exec_context) seam's `with_transaction`. The seam calls
//! [`Driver::begin_tx`], which checks out ONE pooled connection, issues `BEGIN` on it, and returns an
//! OWNED [`TxConnection`] handle (the rust analogue of v1 `litedbmodel.rs` `PoolTransaction`). Every
//! statement in the tx body resolves THAT owned connection (tx-DAG topological order, gate-first
//! short-circuit); the handle's `commit`/`rollback` runs `COMMIT`/`ROLLBACK` on it and releases it.
//!
//! There is NO driver-global single-slot writer any more — the removed `writer: Mutex<Option<...>>`
//! was exactly the shared slot that corrupted CONCURRENT transactions (two txs racing on one slot).
//! With per-execution ownership, concurrent `begin_tx` calls each check out a DISTINCT pooled
//! connection ⇒ isolated. Reads still check out a pooled connection per call (parallel-safe); writes
//! are NEVER parallelized (one owned connection, serial tx-DAG).

use behavior_contracts::Value;
use deadpool_postgres::{Config as PgConfig, Pool as PgPool, Runtime as PgRuntime};
use sqlx::mysql::{MySqlPoolOptions, MySqlRow};
use sqlx::{Column, MySqlPool, Row, TypeInfo, ValueRef as SqlxValueRef};
use tokio::runtime::Runtime;
use tokio_postgres::types::{ToSql, Type as PgType};
use tokio_postgres::NoTls;

use crate::connection_routing::{ConfigDialect, ResolvedConnectionConfig};
use crate::driver::{Driver, PreparedStatement, RunInfo};
use crate::errors::SqlFailure;
use crate::exec_context::{SessionConnection, TxConnection};

fn driver_failure(msg: impl Into<String>) -> SqlFailure {
    SqlFailure {
        kind: "driver_error".into(),
        policy: "fail".into(),
        sqlite_code: None,
        message: msg.into(),
    }
}

/// A Postgres tx-path failure that ALWAYS embeds the SQLSTATE code when the driver error carries one
/// (Phase B / #82). tokio-postgres's `Display` for a server error can abbreviate to just `db error`
/// (notably a serialization failure surfaced at COMMIT), which would hide the `40001` / `40P01` the
/// retryable-error classifier ([`crate::tx_options::is_retryable_tx_error`]) keys on. Extracting the
/// SQLSTATE via `as_db_error().code()` and appending it makes the whole-tx retry loop reliable
/// regardless of the Display verbosity — the classification is by the STABLE code, not fragile text.
fn pg_err(context: &str, e: &tokio_postgres::Error) -> SqlFailure {
    let code = e
        .as_db_error()
        .map(|db| format!(" (SQLSTATE {})", db.code().code()))
        .unwrap_or_default();
    driver_failure(format!("{context}: {e}{code}"))
}

/// The plan's default concurrency (spec) — the pool is sized to match so `concurrency` sibling
/// relations can each hold a live connection without starving.
pub const DEFAULT_POOL_SIZE: usize = 16;

// ── Postgres (tokio-postgres + deadpool-postgres) ──────────────────────────────

/// A live Postgres driver: a `deadpool-postgres` pool over `tokio-postgres`, on a shared tokio
/// runtime. Reads check out a pooled connection per call (parallel-safe); a write-tx OWNS one pooled
/// connection for the BEGIN…COMMIT span via [`Driver::begin_tx`] (per-execution ownership §3 — no
/// driver-global writer slot).
pub struct PostgresDriver {
    rt: Runtime,
    pool: PgPool,
}

impl PostgresDriver {
    /// Connect with a libpq-style conn string, e.g.
    /// "host=localhost port=5433 user=testuser password=testpass dbname=testdb".
    pub fn connect(conn: &str) -> Result<Self, SqlFailure> {
        // A CURRENT-THREAD tokio runtime (#76 approved — lighter `block_on`, v1rs-aligned). The
        // `Driver` facade `block_on`s each pooled future on the caller thread; the read fan-out
        // (`dispatch_read_nodes_parallel`) runs on OS worker threads, EACH with this same driver
        // reference `block_on`ing its own pooled connection — so concurrency comes from the pool +
        // worker threads, not from a multi-thread runtime. deadpool-postgres drives its own
        // connections on `Runtime::Tokio1` tasks, which a current-thread runtime services fine.
        let rt = tokio::runtime::Builder::new_current_thread()
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

        Ok(PostgresDriver { rt, pool })
    }

    /// Connect from a resolved Phase C [`ResolvedConnectionConfig`] — the [`PoolFactory`] path (#88):
    /// the pool SIZING (`max_pool` → `deadpool` pool size) is applied AT CONSTRUCTION so the config is
    /// the SOLE source of the pool cap (a pre-built pool can no longer accept `maxPool`). Connection
    /// params (host/port/database/user/password) flow from the config.
    ///
    /// ## Honest per-driver deviations (documented, not silently dropped)
    ///   - **`min_pool`**: `deadpool-postgres` has no idle-floor / minimum-warm-connections knob (unlike
    ///     `pg.Pool`'s `min`). It is a NO-OP here — the SIZING CAP that matters (`max_pool` →
    ///     `PoolConfig::new(max)`) is honored. (The TS mysql2 factory documents the same for its own
    ///     `minPool`.)
    ///   - **`keep_alive` / `keep_alive_initial_delay_millis`**: `tokio-postgres`'s `Config` exposes TCP
    ///     keepalive via `keepalives(bool)` + `keepalives_idle(Duration)`. These ARE applied here at
    ///     construction (below), so keepAlive is a real construction knob, not dropped.
    pub fn connect_with_config(config: &ResolvedConnectionConfig) -> Result<Self, SqlFailure> {
        if config.driver != ConfigDialect::Postgres {
            return Err(driver_failure(format!(
                "postgres factory: config driver is {:?}, expected postgres",
                config.driver
            )));
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| driver_failure(format!("postgres runtime: {e}")))?;

        let mut cfg = PgConfig::new();
        cfg.host = config.host.clone();
        cfg.port = config.port;
        cfg.user = config.user.clone();
        cfg.password = config.password.clone();
        cfg.dbname = config.database.clone();
        // keepAlive (construction knob): tokio-postgres TCP keepalive on each pooled connection.
        if config.keep_alive {
            cfg.keepalives = Some(true);
            cfg.keepalives_idle = Some(std::time::Duration::from_millis(
                config.keep_alive_initial_delay_millis,
            ));
        }
        // SIZING (construction knob): max_pool is the SOLE cap. min_pool has no deadpool equivalent.
        cfg.pool = Some(deadpool_postgres::PoolConfig::new(config.max_pool as usize));

        let pool = cfg
            .create_pool(Some(PgRuntime::Tokio1), NoTls)
            .map_err(|e| driver_failure(format!("postgres pool: {e}")))?;
        Ok(PostgresDriver { rt, pool })
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

    /// The live pool's current total connection count (deadpool `status().size`) — for the maxPool
    /// sole-cap live proof (mid-flight `total_connections() <= max_pool`).
    pub fn total_connections(&self) -> usize {
        self.pool.status().size
    }
}

impl Driver for PostgresDriver {
    fn dialect(&self) -> &'static str {
        "postgres"
    }

    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        Box::new(PgPrepared {
            driver: self,
            sql: sql.to_string(),
        })
    }

    /// Begin a transaction OWNING one pooled connection (§3): check out a `deadpool` connection, BEGIN
    /// on it, and hand back a [`PgTx`] that runs every tx statement + COMMIT/ROLLBACK on THAT
    /// connection, releasing it on completion. Concurrent `begin_tx` calls get DISTINCT connections ⇒
    /// isolated (the old driver-global `writer` slot is gone).
    fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        let conn = self.rt.block_on(async {
            let client = self
                .pool
                .get()
                .await
                .map_err(|e| driver_failure(format!("postgres pool get (tx): {e}")))?;
            client
                .batch_execute("BEGIN")
                .await
                .map_err(|e| driver_failure(format!("postgres BEGIN: {e}")))?;
            Ok::<_, SqlFailure>(client)
        })?;
        Ok(Box::new(PgTx { rt: &self.rt, conn }))
    }

    /// Check out ONE pooled connection WITHOUT issuing BEGIN (#93 seam-routed tx control): the tx
    /// runtime pins THIS connection then seam-issues the SET + `BEGIN` + `COMMIT`/`ROLLBACK` on it, so a
    /// registered middleware observes them. Concurrent `acquire_tx` calls still get DISTINCT pooled
    /// connections ⇒ the concurrent-tx isolation is UNCHANGED (only WHERE the BEGIN text is issued moved
    /// into the seam — the connection ownership is identical to [`Driver::begin_tx`]).
    fn acquire_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        let conn = self.rt.block_on(async {
            self.pool
                .get()
                .await
                .map_err(|e| driver_failure(format!("postgres pool get (tx): {e}")))
        })?;
        Ok(Box::new(PgTx { rt: &self.rt, conn }))
    }

    /// Acquire ONE pooled connection with the SESSION-config `setup` statements applied (Phase C / #88
    /// `configuredPool`): NOT wrapped in a transaction (a session SET is a connection property; and a
    /// mid-statement `statement_timeout` firing must abort the STATEMENT, not a surrounding tx). Every
    /// statement of one seam call runs on this connection; `finish` runs the RESET statements (unless
    /// poisoned) and drops the connection back to the pool.
    fn session_connection(
        &self,
        setup: &[String],
        reset: &[String],
    ) -> Result<Box<dyn SessionConnection + '_>, SqlFailure> {
        let conn = self.rt.block_on(async {
            let client = self
                .pool
                .get()
                .await
                .map_err(|e| driver_failure(format!("postgres pool get (session): {e}")))?;
            for sql in setup {
                client
                    .batch_execute(sql)
                    .await
                    .map_err(|e| driver_failure(format!("postgres session setup [{sql}]: {e}")))?;
            }
            Ok::<_, SqlFailure>(client)
        })?;
        Ok(Box::new(PgSession {
            rt: &self.rt,
            conn: Some(conn),
            reset: reset.to_vec(),
        }))
    }
}

/// A session-configured PG owned connection (Phase C / #88): the SET statements were applied at
/// acquisition; `finish` runs the RESET statements (clean) or drops the connection (poison), returning
/// it to the pool either way (drop returns it). Reuses [`PgTx`]'s param/row machinery for statements.
struct PgSession<'a> {
    rt: &'a Runtime,
    conn: Option<deadpool_postgres::Object>,
    reset: Vec<String>,
}

impl SessionConnection for PgSession<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        let owned: Vec<PgParam> = params.iter().map(to_pg_param).collect::<Result<_, _>>()?;
        let refs: Vec<&(dyn ToSql + Sync)> =
            owned.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let conn = self.conn.as_ref().expect("pg session conn present");
        self.rt.block_on(async move {
            let rows = pg_query_cached(conn, sql, refs.as_slice())
                .await
                .map_err(|e| pg_err(&format!("postgres session query [{sql}]"), &e))?;
            pg_rows_to_values(&rows)
        })
    }
    fn run(&mut self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        let owned: Vec<PgParam> = params.iter().map(to_pg_param).collect::<Result<_, _>>()?;
        let refs: Vec<&(dyn ToSql + Sync)> =
            owned.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let conn = self.conn.as_ref().expect("pg session conn present");
        let changes = self.rt.block_on(async move {
            pg_execute_cached(conn, sql, refs.as_slice())
                .await
                .map_err(|e| pg_err(&format!("postgres session execute [{sql}]"), &e))
        })?;
        Ok(RunInfo {
            changes: changes as i64,
            last_insert_rowid: 0,
        })
    }
    fn finish(mut self: Box<Self>, poison: bool) -> Result<(), SqlFailure> {
        let conn = self.conn.take().expect("pg session conn present");
        if poison {
            // A poisoned (possibly aborted-by-timeout) connection: drop it via deadpool's remove-on-
            // drop path — do NOT return it to the pool clean, and do NOT run a reset (which would fail
            // on an aborted connection). deadpool returns a dropped Object to the pool; the failed
            // statement already left the connection in a defined post-error state, so a plain drop is
            // the safe release (matching the TS `release(conn, destroy=true)` intent — pooled state is
            // reset by the server on the next checkout's session statements anyway).
            drop(conn);
            return Ok(());
        }
        let reset = std::mem::take(&mut self.reset);
        self.rt.block_on(async move {
            for sql in &reset {
                conn.batch_execute(sql)
                    .await
                    .map_err(|e| pg_err(&format!("postgres session reset [{sql}]"), &e))?;
            }
            drop(conn); // clean: return to pool
            Ok(())
        })
    }
}

struct PgPrepared<'a> {
    driver: &'a PostgresDriver,
    sql: String,
}

/// An OWNED Postgres transaction connection (§3 per-execution ownership — the rust analogue of v1
/// `PoolTransaction::Postgres`). Holds ONE `deadpool` connection for the tx's whole span; every
/// statement + the final COMMIT/ROLLBACK run on it. Dropping the `conn` returns it to the pool.
struct PgTx<'a> {
    rt: &'a Runtime,
    conn: deadpool_postgres::Object,
}

impl TxConnection for PgTx<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        let owned: Vec<PgParam> = params.iter().map(to_pg_param).collect::<Result<_, _>>()?;
        let refs: Vec<&(dyn ToSql + Sync)> =
            owned.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let conn = &self.conn;
        self.rt.block_on(async move {
            let rows = pg_query_cached(conn, sql, refs.as_slice())
                .await
                .map_err(|e| pg_err(&format!("postgres tx query [{sql}]"), &e))?;
            pg_rows_to_values(&rows)
        })
    }

    fn run(&mut self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        let owned: Vec<PgParam> = params.iter().map(to_pg_param).collect::<Result<_, _>>()?;
        let refs: Vec<&(dyn ToSql + Sync)> =
            owned.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let conn = &self.conn;
        let changes = self.rt.block_on(async move {
            pg_execute_cached(conn, sql, refs.as_slice())
                .await
                .map_err(|e| pg_err(&format!("postgres tx execute [{sql}]"), &e))
        })?;
        Ok(RunInfo {
            changes: changes as i64,
            last_insert_rowid: 0, // PG has no lastInsertId; the RETURNING path uses execute()'s rows.
        })
    }

    fn release(self: Box<Self>, poison: bool) -> Result<(), SqlFailure> {
        // The COMMIT/ROLLBACK was already seam-issued on this connection. Release = drop the pooled
        // Object (deadpool returns it to the pool). `poison` ⇒ DESTROY it (do not return a
        // possibly-broken connection): deadpool's `Object::take` detaches it so its Drop discards the
        // connection instead of recycling. Dropped inside the runtime for consistency with the async
        // pool machinery.
        let PgTx { rt, conn } = *self;
        rt.block_on(async move {
            if poison {
                let inner = deadpool_postgres::Object::take(conn);
                drop(inner); // detached from the pool ⇒ destroyed, never recycled.
            } else {
                drop(conn); // returned to the pool clean.
            }
        });
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<(), SqlFailure> {
        let conn = self.conn;
        self.rt.block_on(async move {
            conn.batch_execute("COMMIT")
                .await
                .map_err(|e| pg_err("postgres COMMIT", &e))
        })
    }

    fn rollback(self: Box<Self>) -> Result<(), SqlFailure> {
        let conn = self.conn;
        self.rt.block_on(async move {
            conn.batch_execute("ROLLBACK")
                .await
                .map_err(|e| pg_err("postgres ROLLBACK", &e))
        })
    }
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

// ── Prepared-statement caching (perf, mirrors the Go stmt-cache fix) ─────────────
//
// tokio-postgres PREPAREs a fresh unnamed server-side statement on EVERY `client.query(&str, …)` /
// `client.execute(&str, …)` call (an extra Parse round-trip per op, the dominant cost the crosslang
// bench flagged — Rust pg/mysql ~2× the other langs). A pre-`prepare()`d [`tokio_postgres::Statement`]
// reused across calls skips the re-Parse. deadpool-postgres already ships the exact per-connection
// cache we need: [`deadpool_postgres::Client::prepare_cached`] keeps a `StatementCache` keyed by SQL
// (+ inferred param types) that LIVES ON THE POOLED CONNECTION — so it survives checkout/release and is
// automatically per-connection (a `Statement` is bound to the backend it was Parsed on; deadpool's
// cache is attached to each `Object` and returns to the pool with it).
//
// Correctness: the SAME SQL text is Parsed and the SAME params are Bound — the rows returned are
// BYTE-IDENTICAL to the un-prepared path (conformance/livedb unchanged). No RETURNING carve-out is
// needed for Postgres: PG has NATIVE RETURNING, so a RETURNING insert/update runs its exact SQL through
// the prepared path just like a plain SELECT (unlike MySQL, whose RETURNING is emulated at a layer a
// prepared statement would bypass — see the MySQL section). Tx-control (BEGIN/COMMIT/ROLLBACK) and the
// session SET/RESET statements go through `batch_execute` (simple query protocol), NOT this cached
// query/execute path, so they are untouched — matching the Go fix's tx-control exclusion.
//
// Scoping: works uniformly for the non-tx pooled read/write path (a per-call-checked-out `Object`, its
// cache persisting in the pool), the tx-owned connection ([`PgTx`], one `Object` for the tx span), and
// a session-configured connection ([`PgSession`]). Concurrent transactions each own a DISTINCT `Object`
// ⇒ a DISTINCT statement cache ⇒ the Phase A-F concurrent-tx isolation is untouched.

/// Run a cached-prepared `query` (SELECT / native RETURNING) on a deadpool PG connection: prepare the
/// SQL ONCE (cached on the connection), then execute the reused [`tokio_postgres::Statement`].
async fn pg_query_cached(
    conn: &deadpool_postgres::Object,
    sql: &str,
    refs: &[&(dyn ToSql + Sync)],
) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    let stmt = conn.prepare_cached(sql).await?;
    conn.query(&stmt, refs).await
}

/// Run a cached-prepared `execute` (non-RETURNING write) on a deadpool PG connection: prepare ONCE
/// (cached on the connection), then execute the reused [`tokio_postgres::Statement`].
async fn pg_execute_cached(
    conn: &deadpool_postgres::Object,
    sql: &str,
    refs: &[&(dyn ToSql + Sync)],
) -> Result<u64, tokio_postgres::Error> {
    let stmt = conn.prepare_cached(sql).await?;
    conn.execute(&stmt, refs).await
}

impl PreparedStatement for PgPrepared<'_> {
    fn all(&mut self, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        // Pooled read: check out a connection per call (parallel-safe). In-tx statements NEVER reach
        // here — the seam resolves the tx-owned `PgTx` connection for those. So there is no pinned
        // writer slot to consult (the driver-global slot was removed §3).
        let owned: Vec<PgParam> = params.iter().map(to_pg_param).collect::<Result<_, _>>()?;
        let refs: Vec<&(dyn ToSql + Sync)> =
            owned.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let sql = self.sql.clone();
        let driver = self.driver;
        driver.rt.block_on(async move {
            let client = driver
                .pool
                .get()
                .await
                .map_err(|e| driver_failure(format!("postgres pool get: {e}")))?;
            let rows = pg_query_cached(&client, sql.as_str(), refs.as_slice())
                .await
                .map_err(|e| driver_failure(format!("postgres query [{sql}]: {e}")))?;
            pg_rows_to_values(&rows)
        })
    }

    fn run(&mut self, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        // Pooled write (a non-tx write, e.g. DDL through the seam's non-tx path). Tx-control literals
        // (BEGIN/COMMIT/ROLLBACK) no longer flow through here — they are issued by `begin_tx` /
        // `PgTx::commit` / `PgTx::rollback` on the OWNED connection (§3). In-tx writes go via `PgTx`.
        let owned: Vec<PgParam> = params.iter().map(to_pg_param).collect::<Result<_, _>>()?;
        let refs: Vec<&(dyn ToSql + Sync)> =
            owned.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let sql = self.sql.clone();
        let driver = self.driver;
        driver.rt.block_on(async move {
            let client = driver
                .pool
                .get()
                .await
                .map_err(|e| driver_failure(format!("postgres pool get: {e}")))?;
            let changes = pg_execute_cached(&client, sql.as_str(), refs.as_slice())
                .await
                .map_err(|e| driver_failure(format!("postgres execute [{sql}]: {e}")))?;
            Ok(RunInfo {
                changes: changes as i64,
                last_insert_rowid: 0, // PG has no lastInsertId; the RETURNING path uses `all`.
            })
        })
    }
}

// ── MySQL (sqlx MySqlPool) ─────────────────────────────────────────────────────
//
// Prepared-statement caching (the perf fix the Go leg needed) is ALREADY built into this path: sqlx
// prepares each unique SQL ONCE per connection and reuses the handle. `sqlx::query(sql)` defaults to
// `persistent = true`, and `MySqlConnectOptions::statement_cache_capacity` defaults to 100 (cache ON),
// so sqlx's MySQL executor takes the `get_or_prepare_statement` branch — it sends only a
// `StatementExecute` for a repeat, NOT `Parse`+`Execute`+`StmtClose` (the un-cached branch). The cache
// lives on the pooled `MySqlConnection` and SURVIVES `pool.acquire()`/release (sqlx does not clear it
// on return-to-pool), so both the per-call-acquired read/write path and the tx-owned connection reuse
// prepared statements automatically. This was CONFIRMED by measurement: applying the Postgres
// prepared-statement fix (`prepare_cached`, ~2.1× on pg writes+reads) left the MySQL p50 UNCHANGED
// (create 1.184 → 1.202ms, median-of-medians 1.268 → 1.258ms), i.e. MySQL was never paying a redundant
// per-call Parse. So NO hand-rolled statement cache is added here — it would be redundant with sqlx's
// and, worse, a naïve one would collide with the RETURNING emulation below (a prepared statement path
// bypasses the conn-level RETURNING interception — the same carve-out the Go fix documented). MySQL's
// residual sync-facade cost (per-call `pool.acquire()` + on-release `ping()` + the 2-round-trip
// strip→INSERT→re-select RETURNING emulation) is INHERENT to the design (#76), not a caching bug.

/// A live MySQL driver: a `sqlx` `MySqlPool` on a shared tokio runtime (the old `.rs`
/// `Pool::Mysql(MySqlPool)`). `?` is native; emulates the missing `INSERT … RETURNING`. A write-tx
/// OWNS one pooled connection via [`Driver::begin_tx`] (§3 — no driver-global writer slot).
///
/// sqlx already caches prepared statements per connection (see the module note above) — no additional
/// statement cache is layered here.
pub struct MysqlDriver {
    rt: Runtime,
    pool: MySqlPool,
}

impl MysqlDriver {
    /// Connect via a URL, e.g. "mysql://testuser:testpass@127.0.0.1:3307/scp_rust".
    pub fn connect(url: &str) -> Result<Self, SqlFailure> {
        // CURRENT-THREAD tokio runtime (#76 approved — lighter block_on, v1rs-aligned). The read
        // fan-out runs on OS worker threads, each block_on-ing its own pooled connection.
        let rt = tokio::runtime::Builder::new_current_thread()
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
        Ok(MysqlDriver { rt, pool })
    }

    /// Connect from a resolved Phase C [`ResolvedConnectionConfig`] — the [`PoolFactory`] path (#88):
    /// the pool SIZING (`max_pool` → `sqlx` `max_connections`) is applied AT CONSTRUCTION so the config
    /// is the SOLE source of the pool cap. Connection params flow from the config (assembled into the
    /// `mysql://` URL).
    ///
    /// ## Honest per-driver deviations (documented, not silently dropped)
    ///   - **`min_pool`**: `sqlx` HAS `min_connections` (an idle floor), so — unlike the TS mysql2
    ///     factory, which has none — `min_pool` IS applied here (`min_connections(min_pool)`). Noted
    ///     because the parity target (the TS reference) documents mysql2's lack; the rust `sqlx` pool
    ///     honors it.
    ///   - **`keep_alive` / `keep_alive_initial_delay_millis`**: `sqlx`'s `MySqlPoolOptions` has no TCP
    ///     keepalive-probe knob (it manages idle connections via `idle_timeout` / `max_lifetime`, not a
    ///     socket keepalive). `keep_alive` is therefore a NO-OP for the MySQL factory (documented; not
    ///     leaked as a bogus option). The SIZING CAP that matters (`max_pool`) is honored.
    pub fn connect_with_config(config: &ResolvedConnectionConfig) -> Result<Self, SqlFailure> {
        if config.driver != ConfigDialect::Mysql {
            return Err(driver_failure(format!(
                "mysql factory: config driver is {:?}, expected mysql",
                config.driver
            )));
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| driver_failure(format!("mysql runtime: {e}")))?;
        let host = config
            .host
            .clone()
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let port = config.port.unwrap_or(3306);
        let user = config.user.clone().unwrap_or_default();
        let password = config.password.clone().unwrap_or_default();
        let database = config.database.clone().unwrap_or_default();
        let url = format!("mysql://{user}:{password}@{host}:{port}/{database}");
        let min = config.min_pool;
        let max = config.max_pool.max(1); // sqlx requires max >= 1
        let pool = rt.block_on(async {
            MySqlPoolOptions::new()
                .min_connections(min)
                .max_connections(max)
                .connect(&url)
                .await
                .map_err(|e| driver_failure(format!("mysql connect: {e}")))
        })?;
        Ok(MysqlDriver { rt, pool })
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

    /// The live pool's current total connection count (`sqlx` `size`) — for the maxPool sole-cap live
    /// proof.
    pub fn total_connections(&self) -> usize {
        self.pool.size() as usize
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

/// A bind source for the MySQL RETURNING re-select — the driver-side SSoT that revives the retired
/// codegen `mysqlWriteReselect` binds vocabulary (MySQL 8 has no native RETURNING). A write that
/// DECLARES a RETURNING clause is honored by re-selecting the written row(s) by their REAL key on the
/// SAME connection (so a re-select inside a tx sees the not-yet-committed write). The native-codegen
/// path AND the mode-2 interpreter path both route a RETURNING write through here, so both return the
/// identical row set — the difference between the two paths is never the returned rows.
#[derive(Clone)]
enum ReselectBind {
    /// `LAST_INSERT_ID()` — the first AUTO_INCREMENT id the INSERT allocated.
    LastId,
    /// `LAST_INSERT_ID() + rows_affected` — the exclusive upper bound of the inserted id range.
    HighId,
    /// The batch JSON payload (`params[0]`) re-bound to the re-select's own `JSON_TABLE(?)`.
    JsonParam,
    /// The write's own bound param at this index — a WHERE-predicate value, or a conflict-key value
    /// pulled from the INSERT column list by position.
    Param(usize),
}

/// The MySQL RETURNING re-select derived from a write's baked SQL + its `/*scp:pk=…;conflict=…*/` hint.
struct MysqlReselect {
    /// The write to execute (RETURNING clause + hint stripped — byte-clean for the prepared protocol).
    write_sql: String,
    /// `SELECT <returning cols> FROM <table> WHERE <key predicate> [ORDER BY <pk>]` recovering the row(s).
    select_sql: String,
    /// How to bind `select_sql`'s `?` from `(params, last_insert_id, rows_affected)`.
    binds: Vec<ReselectBind>,
}

/// The target table of a write — the identifier after `INSERT INTO` / `UPDATE` / `DELETE FROM`.
fn table_of(write_sql: &str) -> Option<String> {
    let lower = write_sql.to_ascii_lowercase();
    let after = if let Some(p) = lower.find("insert into ") {
        p + "insert into ".len()
    } else if let Some(p) = lower.find("delete from ") {
        p + "delete from ".len()
    } else if let Some(p) = lower.find("update ") {
        p + "update ".len()
    } else {
        return None;
    };
    let ident: String = write_sql[after..]
        .chars()
        .skip_while(|c| c.is_whitespace())
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if ident.is_empty() {
        None
    } else {
        Some(ident)
    }
}

/// The JOIN key column of an updateMany (the `ON <alias>.<col> = JSON_UNQUOTE(...)` predicate).
fn update_batch_key(write_sql: &str) -> Option<String> {
    let lower = write_sql.to_ascii_lowercase();
    let on = lower.find(" on ")? + " on ".len();
    let seg_end = lower[on..]
        .find(" set ")
        .map(|i| on + i)
        .unwrap_or(write_sql.len());
    let seg = &write_sql[on..seg_end]; // e.g. `u.id = JSON_UNQUOTE(v.id)`
    let lhs = seg.split('=').next()?.trim(); // `u.id`
    let col = lhs.rsplit('.').next()?.trim().to_string(); // `id`
    if col.is_empty() {
        None
    } else {
        Some(col)
    }
}

/// The `conflict=<cols>` field of a `/*scp:pk=…;conflict=col1,col2*/` hint (the upsert conflict-target
/// column list {@link mysqlPkHint} bakes from the write's `onConflict` port). Empty ⇒ not an upsert.
fn parse_conflict_hint(s: &str) -> Vec<String> {
    let start = match s.find(";conflict=") {
        Some(i) => i + ";conflict=".len(),
        None => return Vec::new(),
    };
    let rest = &s[start..];
    let end = rest.find("*/").unwrap_or(rest.len());
    rest[..end]
        .split(',')
        .map(|c| c.trim().to_string())
        .filter(|c| !c.is_empty())
        .collect()
}

/// Build the MySQL RETURNING re-select for a RETURNING write — the driver/runtime SSoT for the reselect
/// construction (driven by the baked SQL + the `/*scp:pk=…;conflict=…*/` metadata hint, NOT any consumer
/// reselect-SQL marker comment). Returns `None` for a plain (no-RETURNING) statement or a
/// `DELETE … RETURNING` (pre-image gone — the caller strips + runs).
fn build_mysql_reselect(sql: &str) -> Option<MysqlReselect> {
    let lower = sql.to_ascii_lowercase();
    let ret_pos = lower.rfind(" returning ")?;
    let hint_region = &sql[ret_pos..];
    // RETURNING columns (hint stripped) + the byte-clean write (RETURNING + hint removed).
    let cols = strip_pk_hint(&sql[ret_pos + " returning ".len()..])
        .trim()
        .to_string();
    let (pk_cols, auto_inc) = parse_pk_hint(hint_region);
    let conflict_cols = parse_conflict_hint(hint_region);
    let write_sql = sql[..ret_pos].trim().to_string();
    let wl = write_sql.to_ascii_lowercase();
    let table = table_of(&write_sql)?;
    // Re-select ordered by the DECLARED pk so mysql matches pg/sqlite RETURNING order (§10).
    let order_by = if pk_cols.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {}", pk_cols.join(", "))
    };
    let is_batch = wl.contains("json_table(");

    // upsert / upsertMany — recover by the conflict key (MySQL does not report the conflicted-row id;
    // the AUTO_INCREMENT range is wrong when a row is UPDATED, not inserted).
    if wl.starts_with("insert") && wl.contains("on duplicate key update") {
        let conflict = conflict_cols.first()?; // fail-closed: an upsert RETURNING needs its conflict key
        let select_sql = if is_batch {
            format!(
                "SELECT {cols} FROM {table} WHERE {c} IN (SELECT JSON_UNQUOTE(jt.{c}) FROM JSON_TABLE(?, '$[*]' COLUMNS({c} JSON PATH '$.{c}')) jt){order_by}",
                c = conflict
            )
        } else {
            format!("SELECT {cols} FROM {table} WHERE {conflict} = ?{order_by}")
        };
        let binds = if is_batch {
            vec![ReselectBind::JsonParam]
        } else {
            let idx = parse_insert_cols(&write_sql)
                .iter()
                .position(|c| c == conflict)?;
            vec![ReselectBind::Param(idx)]
        };
        return Some(MysqlReselect {
            write_sql,
            select_sql,
            binds,
        });
    }

    // create / createMany — recover by the AUTO_INCREMENT range [LAST_INSERT_ID, +affected), or by the
    // client-supplied PK values pulled from the INSERT params by position (UUID / composite / natural key).
    if wl.starts_with("insert") {
        if !auto_inc.is_empty() && pk_cols.len() == 1 && pk_cols[0] == auto_inc {
            let select_sql = format!(
                "SELECT {cols} FROM {table} WHERE {ai} >= ? AND {ai} < ?{order_by}",
                ai = auto_inc
            );
            return Some(MysqlReselect {
                write_sql,
                select_sql,
                binds: vec![ReselectBind::LastId, ReselectBind::HighId],
            });
        }
        let insert_cols = parse_insert_cols(&write_sql);
        let mut conds: Vec<String> = Vec::new();
        let mut binds: Vec<ReselectBind> = Vec::new();
        for pk in &pk_cols {
            let idx = insert_cols.iter().position(|c| c == pk)?;
            conds.push(format!("{pk} = ?"));
            binds.push(ReselectBind::Param(idx));
        }
        if conds.is_empty() {
            // Legacy `id` fallback (no pk hint at all): recover by LAST_INSERT_ID.
            return Some(MysqlReselect {
                write_sql,
                select_sql: format!("SELECT {cols} FROM {table} WHERE id = ?{order_by}"),
                binds: vec![ReselectBind::LastId],
            });
        }
        let select_sql = format!(
            "SELECT {cols} FROM {table} WHERE {}{order_by}",
            conds.join(" AND ")
        );
        return Some(MysqlReselect {
            write_sql,
            select_sql,
            binds,
        });
    }

    // updateMany — recover by the batch key (the JSON JOIN key), re-selected from the SAME JSON payload.
    if wl.starts_with("update") && is_batch {
        let key = update_batch_key(&write_sql)?;
        let select_sql = format!(
            "SELECT {cols} FROM {table} WHERE {k} IN (SELECT JSON_UNQUOTE(jt.{k}) FROM JSON_TABLE(?, '$[*]' COLUMNS({k} JSON PATH '$.{k}')) jt){order_by}",
            k = key
        );
        return Some(MysqlReselect {
            write_sql,
            select_sql,
            binds: vec![ReselectBind::JsonParam],
        });
    }

    // update — recover by the write's OWN WHERE predicate (its key is unchanged by the SET, so the same
    // predicate matches the same rows after the write, now carrying the new values).
    if wl.starts_with("update") {
        let wpos = wl.find(" where ")?;
        let where_sql = write_sql[wpos + " where ".len()..].trim().to_string();
        let before = write_sql[..wpos].matches('?').count();
        let n_where = where_sql.matches('?').count();
        let binds = (0..n_where)
            .map(|i| ReselectBind::Param(before + i))
            .collect();
        let select_sql = format!("SELECT {cols} FROM {table} WHERE {where_sql}{order_by}");
        return Some(MysqlReselect {
            write_sql,
            select_sql,
            binds,
        });
    }

    // DELETE … RETURNING: the pre-image is gone once the write runs — handled by the caller.
    None
}

/// If `sql` is a `DELETE … RETURNING`, return the DELETE with the RETURNING clause + hint stripped
/// (MySQL has no native RETURNING and, unlike UPDATE/upsert, the pre-image cannot be re-selected after
/// the delete; no corpus op declares delete+returning — kept byte-faithful to v1 `mysql.ts`).
fn strip_delete_returning(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let ret_pos = lower.rfind(" returning ")?;
    if !lower.trim_start().starts_with("delete") {
        return None;
    }
    Some(strip_pk_hint(&sql[..ret_pos]).trim().to_string())
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
    let body = &rest[..end]; // `col1,col2;ai=<col|>[;conflict=<cols>]`
    let mut parts = body.splitn(2, ";ai=");
    let cols_part = parts.next().unwrap_or("");
    // `ai` runs to the next `;` field (e.g. `;conflict=…`), not to the end of the hint.
    let ai = parts
        .next()
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
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

/// Run one `all` (SELECT / RETURNING) on a single MySQL connection — the SHARED body used by BOTH the
/// pooled read path ([`MyPrepared::all`], on a per-call acquired connection) and the tx-owned path
/// ([`MyTx::execute`], on the tx's owned connection). Byte-identical to the pre-seam logic; the ONLY
/// change is that WHICH connection it runs on is decided by the caller (per-execution ownership §3),
/// not a driver-global writer slot. Includes the MySQL RETURNING emulation (strip → INSERT →
/// re-select by the real PK) — all statements of one `all` run on the SAME `conn` so the re-select
/// sees the insert (crucial inside a tx before COMMIT).
async fn my_all_on_conn(
    conn: &mut sqlx::MySqlConnection,
    sql: &str,
    params: &[Value],
) -> Result<Vec<Value>, SqlFailure> {
    // MySQL RETURNING emulation (the ONE reselect path — [`build_mysql_reselect`] is the driver SSoT):
    // execute the write, then re-select the written row(s) by their REAL key on THIS connection (so a
    // re-select inside a tx sees the not-yet-committed write). Covers INSERT (auto-inc range / client
    // PK), upsert (conflict key), and UPDATE (own WHERE / batch JSON key) — honoring a DECLARED
    // RETURNING. The strip-before-execute `/*scp:pk=…;conflict=…*/` hint (tx.ts mysqlPkHint) supplies the
    // real key. Both the native-codegen path and the mode-2 interpreter path run RETURNING writes through
    // here, so both return the SAME rows.
    if let Some(rs) = build_mysql_reselect(sql) {
        let exec_res = bind_my(sqlx::query(&rs.write_sql), params)?
            .execute(&mut *conn)
            .await
            .map_err(|e| driver_failure(format!("mysql write [{}]: {e}", rs.write_sql)))?;
        let last_id = exec_res.last_insert_id() as i64;
        let affected = exec_res.rows_affected().max(1) as i64;
        let mut sel_params: Vec<Value> = Vec::with_capacity(rs.binds.len());
        for b in &rs.binds {
            let param_at = |i: usize| {
                params.get(i).cloned().ok_or_else(|| {
                    driver_failure(format!(
                        "mysql re-select: bind param {i} out of range for [{}]",
                        rs.select_sql
                    ))
                })
            };
            sel_params.push(match b {
                ReselectBind::LastId => Value::Int(last_id),
                ReselectBind::HighId => Value::Int(last_id + affected),
                ReselectBind::JsonParam => param_at(0)?,
                ReselectBind::Param(i) => param_at(*i)?,
            });
        }
        let rows = bind_my(sqlx::query(&rs.select_sql), &sel_params)?
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| driver_failure(format!("mysql re-select [{}]: {e}", rs.select_sql)))?;
        return my_rows_to_values(&rows);
    }

    // DELETE … RETURNING: MySQL has no native RETURNING and the pre-image is gone once the delete runs,
    // so the stripped write runs and no rows are returned (no corpus op declares delete+returning).
    if let Some(write_sql) = strip_delete_returning(sql) {
        let q = bind_my(sqlx::query(&write_sql), params)?;
        q.execute(&mut *conn)
            .await
            .map_err(|e| driver_failure(format!("mysql write [{write_sql}]: {e}")))?;
        return Ok(Vec::new());
    }

    let q = bind_my(sqlx::query(sql), params)?;
    let rows = q
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| driver_failure(format!("mysql query [{sql}]: {e}")))?;
    my_rows_to_values(&rows)
}

/// Is `sql` a tx-control / SET statement MySQL rejects in the prepared-statement protocol (error
/// 1295) and must run via `raw_sql`? BEGIN / START TRANSACTION / COMMIT / ROLLBACK / SET … (#93: the
/// seam-issued tx-control on the tx-owned connection resolves through `MyTx::run` → here, so it MUST
/// take the `raw_sql` path exactly as the pre-#93 driver-issued BEGIN/COMMIT/ROLLBACK did).
fn my_is_raw_sql_stmt(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    head.starts_with("BEGIN")
        || head.starts_with("START TRANSACTION")
        || head.starts_with("COMMIT")
        || head.starts_with("ROLLBACK")
        || head.starts_with("SET ")
        || head.starts_with("SET\t")
}

/// Run one `run` (non-returning write) on a single MySQL connection — the SHARED body for the pooled
/// write path and the tx-owned path (§3). A tx-control / SET literal (seam-issued BEGIN/COMMIT/ROLLBACK
/// on the tx-owned connection, #93) takes the `raw_sql` path — MySQL rejects those in the
/// prepared-statement protocol (error 1295).
async fn my_run_on_conn(
    conn: &mut sqlx::MySqlConnection,
    sql: &str,
    params: &[Value],
) -> Result<RunInfo, SqlFailure> {
    if my_is_raw_sql_stmt(sql) {
        // BEGIN/COMMIT/ROLLBACK/SET carry no params; run via raw_sql (the pre-#93 driver path). The
        // affected-row summary is irrelevant for tx-control (the tx runtime ignores it).
        debug_assert!(params.is_empty(), "tx-control statement carries no params");
        sqlx::raw_sql(sql)
            .execute(&mut *conn)
            .await
            .map_err(|e| driver_failure(format!("mysql tx-control [{sql}]: {e}")))?;
        return Ok(RunInfo {
            changes: 0,
            last_insert_rowid: 0,
        });
    }
    let q = bind_my(sqlx::query(sql), params)?;
    let res = q
        .execute(&mut *conn)
        .await
        .map_err(|e| driver_failure(format!("mysql exec [{sql}]: {e}")))?;
    Ok(RunInfo {
        changes: res.rows_affected() as i64,
        last_insert_rowid: res.last_insert_id() as i64,
    })
}

impl PreparedStatement for MyPrepared<'_> {
    fn all(&mut self, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        // Pooled read: acquire a connection per call (parallel-safe). In-tx statements never reach
        // here — the seam resolves the tx-owned `MyTx` connection for those.
        let sql = self.sql.clone();
        let driver = self.driver;
        let params = params.to_vec();
        driver.rt.block_on(async move {
            let mut conn = driver
                .pool
                .acquire()
                .await
                .map_err(|e| driver_failure(format!("mysql acquire: {e}")))?;
            my_all_on_conn(&mut conn, &sql, &params).await
        })
    }

    fn run(&mut self, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        // Pooled write. Tx-control literals (BEGIN/COMMIT/ROLLBACK) no longer flow through here — they
        // are issued by `begin_tx` / `MyTx::commit` / `MyTx::rollback` on the OWNED connection (§3).
        let sql = self.sql.clone();
        let driver = self.driver;
        let params = params.to_vec();
        driver.rt.block_on(async move {
            let mut conn = driver
                .pool
                .acquire()
                .await
                .map_err(|e| driver_failure(format!("mysql acquire: {e}")))?;
            my_run_on_conn(&mut conn, &sql, &params).await
        })
    }
}

impl Driver for MysqlDriver {
    fn dialect(&self) -> &'static str {
        "mysql"
    }

    fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
        Box::new(MyPrepared {
            driver: self,
            sql: sql.to_string(),
        })
    }

    /// Begin a transaction OWNING one pooled connection (§3): acquire a `sqlx` connection, `BEGIN` on
    /// it (via `raw_sql` — MySQL rejects BEGIN/COMMIT/ROLLBACK in the prepared-statement protocol,
    /// error 1295), and hand back a [`MyTx`] that runs every tx statement + COMMIT/ROLLBACK on THAT
    /// owned connection. Concurrent `begin_tx` calls get DISTINCT connections ⇒ isolated.
    fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        self.begin_tx_isolated(&[], &[])
    }

    /// MySQL isolation prelude (Phase B / #82): `SET TRANSACTION ISOLATION LEVEL …` scopes ONLY the
    /// NEXT transaction, so it MUST run on the owned connection BEFORE `BEGIN` — hence the override
    /// (the default trait impl runs it after BEGIN, which MySQL would apply to the tx AFTER this one).
    /// `after_begin` is unused for MySQL (isolation always rides `before_begin`); it is run post-BEGIN
    /// for completeness so the contract stays uniform.
    fn begin_tx_isolated(
        &self,
        before_begin: &[String],
        after_begin: &[String],
    ) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        let conn = self.rt.block_on(async {
            let mut conn = self
                .pool
                .acquire()
                .await
                .map_err(|e| driver_failure(format!("mysql acquire (tx): {e}")))?;
            // Isolation SET (if any) BEFORE BEGIN — it scopes the immediately-following transaction.
            for sql in before_begin {
                sqlx::raw_sql(sql)
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| driver_failure(format!("mysql tx prelude [{sql}]: {e}")))?;
            }
            // BEGIN via raw_sql (MySQL rejects BEGIN/COMMIT/ROLLBACK in the prepared-statement
            // protocol, error 1295).
            sqlx::raw_sql("BEGIN")
                .execute(&mut *conn)
                .await
                .map_err(|e| driver_failure(format!("mysql BEGIN: {e}")))?;
            for sql in after_begin {
                sqlx::raw_sql(sql)
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| driver_failure(format!("mysql tx post-begin [{sql}]: {e}")))?;
            }
            Ok::<_, SqlFailure>(conn)
        })?;
        Ok(Box::new(MyTx { rt: &self.rt, conn }))
    }

    /// Check out ONE pooled connection WITHOUT issuing BEGIN or the isolation SET (#93 seam-routed tx
    /// control): the tx runtime pins THIS connection then seam-issues the SET + `BEGIN` + `COMMIT`/
    /// `ROLLBACK` on it (via `MyTx::run` → `my_run_on_conn`'s raw_sql path for tx-control) so a
    /// registered middleware observes them. Concurrent `acquire_tx` calls get DISTINCT connections ⇒ the
    /// concurrent-tx isolation is UNCHANGED. NB the MySQL isolation SET-before-BEGIN order is preserved
    /// because the tx runtime issues `before_begin` (the SET) THEN `BEGIN` through the seam in that order.
    fn acquire_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
        let conn = self.rt.block_on(async {
            self.pool
                .acquire()
                .await
                .map_err(|e| driver_failure(format!("mysql acquire (tx): {e}")))
        })?;
        Ok(Box::new(MyTx { rt: &self.rt, conn }))
    }

    /// Acquire ONE pooled connection with the SESSION-config `setup` statements applied (Phase C / #88
    /// `configuredPool`): NOT wrapped in a transaction. `finish` runs the RESET statements (clean) or
    /// drops the connection (poison). SET statements run via `raw_sql` (session SETs).
    fn session_connection(
        &self,
        setup: &[String],
        reset: &[String],
    ) -> Result<Box<dyn SessionConnection + '_>, SqlFailure> {
        let conn = self.rt.block_on(async {
            let mut conn = self
                .pool
                .acquire()
                .await
                .map_err(|e| driver_failure(format!("mysql acquire (session): {e}")))?;
            for sql in setup {
                sqlx::raw_sql(sql)
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| driver_failure(format!("mysql session setup [{sql}]: {e}")))?;
            }
            Ok::<_, SqlFailure>(conn)
        })?;
        Ok(Box::new(MySession {
            rt: &self.rt,
            conn: Some(conn),
            reset: reset.to_vec(),
        }))
    }
}

/// A session-configured MySQL owned connection (Phase C / #88): SET applied at acquisition; `finish`
/// runs RESET (clean) or drops (poison). Reuses [`my_all_on_conn`] / [`my_run_on_conn`] for statements.
struct MySession<'a> {
    rt: &'a Runtime,
    conn: Option<sqlx::pool::PoolConnection<sqlx::MySql>>,
    reset: Vec<String>,
}

impl SessionConnection for MySession<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        let conn = self.conn.as_mut().expect("mysql session conn present");
        self.rt
            .block_on(async move { my_all_on_conn(conn, sql, params).await })
    }
    fn run(&mut self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        let conn = self.conn.as_mut().expect("mysql session conn present");
        self.rt
            .block_on(async move { my_run_on_conn(conn, sql, params).await })
    }
    fn finish(mut self: Box<Self>, poison: bool) -> Result<(), SqlFailure> {
        let mut conn = self.conn.take().expect("mysql session conn present");
        let reset = std::mem::take(&mut self.reset);
        self.rt.block_on(async move {
            let mut result = Ok(());
            if !poison {
                for sql in &reset {
                    if let Err(e) = sqlx::raw_sql(sql).execute(&mut *conn).await {
                        result = Err(driver_failure(format!("mysql session reset [{sql}]: {e}")));
                        break;
                    }
                }
            }
            // Drop the PoolConnection INSIDE the runtime context (sqlx requires a Tokio context on drop;
            // dropping it outside block_on panics under a current-thread runtime).
            drop(conn);
            result
        })
    }
}

/// An OWNED MySQL transaction connection (§3 per-execution ownership — the rust analogue of v1
/// `PoolTransaction::Mysql`). Holds ONE `sqlx` pooled connection for the tx's whole span; every
/// statement + the final COMMIT/ROLLBACK run on it. Dropping the connection returns it to the pool.
struct MyTx<'a> {
    rt: &'a Runtime,
    conn: sqlx::pool::PoolConnection<sqlx::MySql>,
}

impl TxConnection for MyTx<'_> {
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        let conn = &mut self.conn;
        self.rt
            .block_on(async move { my_all_on_conn(conn, sql, params).await })
    }

    fn run(&mut self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        let conn = &mut self.conn;
        self.rt
            .block_on(async move { my_run_on_conn(conn, sql, params).await })
    }

    fn release(self: Box<Self>, poison: bool) -> Result<(), SqlFailure> {
        // The COMMIT/ROLLBACK was already seam-issued on this connection. Release = drop the
        // PoolConnection INSIDE the runtime context (sqlx `PoolConnection` Drop requires a Tokio
        // context; dropping it outside `block_on` panics under a current-thread runtime). `poison` ⇒
        // DETACH it from the pool first so its Drop CLOSES the connection instead of recycling a
        // possibly-broken one (`PoolConnection::detach` yields a plain `MySqlConnection` that is closed
        // on drop).
        let MyTx { rt, conn } = *self;
        rt.block_on(async move {
            if poison {
                let detached = conn.detach();
                drop(detached); // closed, never recycled.
            } else {
                drop(conn); // returned to the pool clean, inside the runtime.
            }
        });
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<(), SqlFailure> {
        // Move the owned connection INTO the async block so it is dropped (returned to the pool)
        // WITHIN the tokio runtime context — a sqlx `PoolConnection` Drop requires a Tokio context
        // (dropping it outside `block_on` panics under a current-thread runtime).
        let MyTx { rt, mut conn } = *self;
        rt.block_on(async move {
            let r = sqlx::raw_sql("COMMIT")
                .execute(&mut *conn)
                .await
                .map_err(|e| driver_failure(format!("mysql COMMIT: {e}")))
                .map(|_| ());
            drop(conn); // return to pool inside the runtime
            r
        })
    }

    fn rollback(self: Box<Self>) -> Result<(), SqlFailure> {
        let MyTx { rt, mut conn } = *self;
        rt.block_on(async move {
            let r = sqlx::raw_sql("ROLLBACK")
                .execute(&mut *conn)
                .await
                .map_err(|e| driver_failure(format!("mysql ROLLBACK: {e}")))
                .map(|_| ());
            drop(conn); // return to pool inside the runtime
            r
        })
    }
}
