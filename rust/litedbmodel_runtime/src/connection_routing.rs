//! litedbmodel v2 SCP — the **connection routing + config contract** (Phase C / #88, rust port).
//!
//! The rust mirror of the TS API REFERENCE `src/scp/connection-routing.ts` (#87). It builds ON the
//! Phase A [`ExecutionContext`](crate::exec_context) seam + the Phase A/B owned-connection tx runtime;
//! it does NOT re-implement the seam — it supplies the pieces
//! [`ExecutionContext::connection_for`](crate::exec_context) uses to complete its resolution
//! (design §3 steps 2-4). Phase A wired only step 1 (the tx pin) + the single-driver case; Phase C
//! adds the registry / reader-writer split / writer-sticky / named-DB routing / session config.
//!
//! ## The rust "pool" = a [`Driver`]
//!
//! The TS reference's pool abstraction is `AsyncConnectionPool` (`acquire`/`release`). In the rust
//! runtime the [`Driver`] trait IS the pool: each live [`Driver`](crate::driver::Driver)
//! ([`PostgresDriver`](crate::livedb::PostgresDriver) / [`MysqlDriver`](crate::livedb::MysqlDriver))
//! owns its OWN internal async connection pool (`deadpool-postgres` / `sqlx`), and the synchronous
//! `prepare(sql).all()/run()` facade checks out one pooled connection per call. So a
//! [`ReaderWriterPools`] here is a pair of `Arc<dyn Driver + Sync>` and a [`ConnectionRegistry`] is a
//! `name → ReaderWriterPools` map. `Arc` (not a bare `&`) because a registry OWNS its drivers (the
//! `buildRoutingConfig` factory constructs them) and a single-pool pair shares the SAME `Arc` for
//! reader and writer (`Arc::ptr_eq` ⇒ byte-identical single-pool routing).
//!
//! ## The `connection_for(intent)` resolution order (design §3, v1 `DBModel.ts:313` parity)
//!
//!   1. **active tx connection** — inside a transaction, always the tx-owned connection (Phase A,
//!      resolved in [`ExecutionContext`](crate::exec_context) BEFORE this module, since only the ctx
//!      holds the tx pin).
//!   2. **writer scope / writer-sticky** — inside [`with_writer`](crate::exec_context) or within
//!      `writer_sticky_duration` after a committed tx, a READ goes to the WRITER pool (read-your-writes).
//!   3. **read = reader / write = writer** — otherwise a read → reader pool, a write → writer pool
//!      (single-pool config ⇒ reader === writer).
//!   4. **named-DB routing** — the pair is selected FIRST by `intent.db` (the connection NAME the
//!      bundle/model metadata carries; decorator-free, Phase F wires decorators later), falling back
//!      to [`DEFAULT_CONNECTION`] when unnamed; an unregistered name is a LOUD error.
//!
//! ## Backward-compat (the hard constraint)
//!
//! Single DB, reader === writer (one driver), empty config, unnamed connection ⇒ BYTE-IDENTICAL to
//! the Phase A/B single-pool behavior. The existing [`for_driver`](crate::exec_context::for_driver)
//! base ctx carries NO routing config and resolves straight to its one driver — nothing here runs on
//! that path. A [`ConnectionRegistry::single_default`] built from ONE driver routes every intent to
//! that one driver, and the writer-sticky clock only ever diverts to a pool that is the SAME `Arc` —
//! so nothing observable changes.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::driver::Driver;
use crate::errors::SqlFailure;
use crate::exec_context::StatementIntent;

// ── The runtime config (C3) — mirrors the TS ConnectionConfig / v1 DBConfig ────

/// The driver dialect a connection targets (mirrors the TS `ConnectionConfig.driver` union). Kept
/// distinct from [`crate::tx_options::Dialect`] so it carries the config-file default (`postgres`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigDialect {
    Postgres,
    Mysql,
    Sqlite,
}

/// Per-connection database config (C3) — the knobs a pool is BUILT with. Mirrors the TS
/// [`ConnectionConfig`] (`connection-routing.ts`) field-for-field: connection target + pool sizing +
/// per-statement/keepalive/session knobs. Every field is optional with a documented default (applied
/// by [`ConnectionConfig::resolve`]); the port MUST expose the SAME field names + defaults. This is a
/// DATA contract — it describes how to BUILD a pool; the actual pool construction lives in the
/// [`PoolFactory`] (the driver adapters), which read a [`ResolvedConnectionConfig`].
#[derive(Debug, Clone, Default)]
pub struct ConnectionConfig {
    /// Driver dialect for this connection. Default `postgres`.
    pub driver: Option<ConfigDialect>,
    /// DB host (server-based dialects).
    pub host: Option<String>,
    /// DB port.
    pub port: Option<u16>,
    /// DB name (or file path for sqlite).
    pub database: Option<String>,
    /// Username.
    pub user: Option<String>,
    /// Password.
    pub password: Option<String>,
    /// Per-statement timeout in MILLISECONDS, applied as a session `statement_timeout` (PG) /
    /// `max_execution_time` (MySQL) so a runaway query is aborted by the SERVER. `0`/absent ⇒ no
    /// timeout. Default 0.
    pub query_timeout: Option<u64>,
    /// Enable TCP keepalive on pooled connections. Default false.
    pub keep_alive: Option<bool>,
    /// ms before the first keepalive probe. Default 10000 (when `keep_alive`).
    pub keep_alive_initial_delay_millis: Option<u64>,
    /// Minimum pooled connections kept warm. Default 0.
    pub min_pool: Option<u32>,
    /// Maximum pooled connections. Default 10.
    pub max_pool: Option<u32>,
    /// PG `search_path` set on each pooled connection at checkout (schema routing).
    pub search_path: Option<String>,
    /// MySQL connection charset / PG `client_encoding` set on each pooled connection.
    pub charset: Option<String>,
}

impl ConnectionConfig {
    /// Apply the C3 defaults (query_timeout=0, keep_alive=false, min_pool=0, max_pool=10,
    /// keep_alive_initial_delay_millis=10000). Mirrors the TS `resolveConnectionConfig`.
    pub fn resolve(&self) -> ResolvedConnectionConfig {
        ResolvedConnectionConfig {
            driver: self.driver.unwrap_or(ConfigDialect::Postgres),
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
            user: self.user.clone(),
            password: self.password.clone(),
            query_timeout: self.query_timeout.unwrap_or(0),
            keep_alive: self.keep_alive.unwrap_or(false),
            keep_alive_initial_delay_millis: self.keep_alive_initial_delay_millis.unwrap_or(10_000),
            min_pool: self.min_pool.unwrap_or(0),
            max_pool: self.max_pool.unwrap_or(10),
            search_path: self.search_path.clone(),
            charset: self.charset.clone(),
        }
    }
}

/// The resolved (defaults-applied) config the pool builder consumes — no `None` holes on the knobs.
/// Mirrors the TS [`ResolvedConnectionConfig`].
#[derive(Debug, Clone)]
pub struct ResolvedConnectionConfig {
    pub driver: ConfigDialect,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub query_timeout: u64,
    pub keep_alive: bool,
    pub keep_alive_initial_delay_millis: u64,
    pub min_pool: u32,
    pub max_pool: u32,
    pub search_path: Option<String>,
    pub charset: Option<String>,
}

/// The SESSION statements a connection must run at checkout to honor a [`ResolvedConnectionConfig`]
/// (issued once per acquired connection, in order). The per-dialect mapping the port mirrors; pure
/// (no connection contact) so it is testable in isolation:
///
///   - **statement timeout** (`query_timeout > 0`): PG `SET statement_timeout = <ms>`; MySQL
///     `SET SESSION max_execution_time = <ms>` (both server-side, ms).
///   - **search_path**: PG `SET search_path TO <path>`; MySQL has no schema search path ⇒ ignored.
///   - **charset**: MySQL `SET NAMES <charset>`; PG `SET client_encoding TO <charset>`.
///
/// A key with no value emits nothing (⇒ empty for an all-default config ⇒ session untouched,
/// backward-compatible). sqlite has no server session ⇒ empty. Mirrors the TS `sessionStatements`.
pub fn session_statements(config: &ResolvedConnectionConfig) -> Vec<String> {
    let mut out = Vec::new();
    let dialect = config.driver;
    if dialect == ConfigDialect::Sqlite {
        return out;
    }
    if config.query_timeout > 0 {
        out.push(if dialect == ConfigDialect::Postgres {
            format!("SET statement_timeout = {}", config.query_timeout)
        } else {
            format!("SET SESSION max_execution_time = {}", config.query_timeout)
        });
    }
    if let Some(path) = &config.search_path {
        if dialect == ConfigDialect::Postgres {
            out.push(format!("SET search_path TO {path}"));
        }
    }
    if let Some(cs) = &config.charset {
        out.push(if dialect == ConfigDialect::Mysql {
            format!("SET NAMES {cs}")
        } else {
            format!("SET client_encoding TO {cs}")
        });
    }
    out
}

/// The RESET statements that undo [`session_statements`] on release (per dialect), so a session knob
/// set for THIS configured connection does NOT leak to the next caller that draws the SAME underlying
/// pooled connection (the pools do NOT auto-reset session state on release). Only the knobs `config`
/// actually set are reset (an all-default config ⇒ nothing to reset). Mirrors the TS
/// `sessionResetStatements`.
pub fn session_reset_statements(config: &ResolvedConnectionConfig) -> Vec<String> {
    let mut out = Vec::new();
    let dialect = config.driver;
    if dialect == ConfigDialect::Sqlite {
        return out;
    }
    if config.query_timeout > 0 {
        out.push(if dialect == ConfigDialect::Postgres {
            "RESET statement_timeout".to_string()
        } else {
            "SET SESSION max_execution_time = DEFAULT".to_string()
        });
    }
    if config.search_path.is_some() && dialect == ConfigDialect::Postgres {
        out.push("RESET search_path".to_string());
    }
    if config.charset.is_some() {
        out.push(if dialect == ConfigDialect::Mysql {
            "SET NAMES DEFAULT".to_string()
        } else {
            "RESET client_encoding".to_string()
        });
    }
    out
}

// ── Reader/writer pool pair (C1) ───────────────────────────────────────────────

/// A reader/writer pool PAIR for ONE named connection (C1). `reader` serves read-intent statements;
/// `writer` serves write-intent statements, [`with_writer`](crate::exec_context) reads, and
/// writer-sticky reads. When a connection has no separate replica, `reader` and `writer` are the SAME
/// `Arc` — reader/writer routing then always lands on that one driver (the single-pool backward-compat
/// case). A "pool" is a [`Driver`] (each owns its own internal connection pool). Mirrors the TS
/// [`ReaderWriterPools`].
#[derive(Clone)]
pub struct ReaderWriterPools {
    pub reader: Arc<dyn Driver + Send + Sync>,
    pub writer: Arc<dyn Driver + Send + Sync>,
}

impl std::fmt::Debug for ReaderWriterPools {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `dyn Driver` is not Debug; report identity (whether reader === writer) for diagnostics.
        f.debug_struct("ReaderWriterPools")
            .field("single_pool", &Arc::ptr_eq(&self.reader, &self.writer))
            .finish()
    }
}

impl ReaderWriterPools {
    /// A pair where reader === writer (single-pool, backward-compat). Mirrors the TS `singlePoolPair`.
    pub fn single(pool: Arc<dyn Driver + Send + Sync>) -> Self {
        ReaderWriterPools {
            reader: pool.clone(),
            writer: pool,
        }
    }

    /// A pair from a distinct reader + writer (reader/writer separation). Mirrors `readerWriterPair`.
    pub fn split(
        reader: Arc<dyn Driver + Send + Sync>,
        writer: Arc<dyn Driver + Send + Sync>,
    ) -> Self {
        ReaderWriterPools { reader, writer }
    }
}

// ── The connection registry (C2) — name → reader/writer pools ──────────────────

/// The reserved name of the DEFAULT (unnamed) connection. An `intent.db` of `None` uses this.
/// Mirrors the TS `DEFAULT_CONNECTION`.
pub const DEFAULT_CONNECTION: &str = "default";

/// The multi-DB connection registry (C2): a map from a connection NAME → its [`ReaderWriterPools`].
/// [`resolve_pool`] selects the pair by `intent.db` (the connection name the bundle/model metadata
/// carries), falling back to [`DEFAULT_CONNECTION`] when unnamed. Selecting a name that was never
/// registered is a LOUD error (a real wiring bug — never a silent default fallback, which would run a
/// query on the wrong DB). Mirrors the TS [`ConnectionRegistry`].
///
/// A single-DB deployment registers exactly one connection under [`DEFAULT_CONNECTION`] with
/// reader === writer ⇒ every intent routes to that one driver ⇒ byte-identical to Phase A/B.
///
/// The map is a `BTreeMap` so `names()` is deterministic (diagnostics / `close`).
#[derive(Clone, Default)]
pub struct ConnectionRegistry {
    connections: BTreeMap<String, ReaderWriterPools>,
}

impl ConnectionRegistry {
    /// Build a registry from an explicit name → pools map. Mirrors the TS `new ConnectionRegistry`.
    pub fn new(connections: BTreeMap<String, ReaderWriterPools>) -> Self {
        ConnectionRegistry { connections }
    }

    /// Build a registry from ONE driver as the default connection (reader === writer) — the
    /// backward-compat path. Mirrors the TS `ConnectionRegistry.singleDefault`.
    pub fn single_default(pool: Arc<dyn Driver + Send + Sync>) -> Self {
        let mut connections = BTreeMap::new();
        connections.insert(
            DEFAULT_CONNECTION.to_string(),
            ReaderWriterPools::single(pool),
        );
        ConnectionRegistry { connections }
    }

    /// Start an incremental builder from a default connection's pools. Mirrors `ConnectionRegistry.fromDefault`.
    pub fn from_default(pools: ReaderWriterPools) -> ConnectionRegistryBuilder {
        ConnectionRegistryBuilder::new().add(DEFAULT_CONNECTION, pools)
    }

    /// The reader/writer pair for `name` (or [`DEFAULT_CONNECTION`] when `None`). Loud on a missing
    /// name (mirrors the TS `pairFor`).
    pub fn pair_for(&self, name: Option<&str>) -> Result<&ReaderWriterPools, SqlFailure> {
        let key = name.unwrap_or(DEFAULT_CONNECTION);
        self.connections.get(key).ok_or_else(|| {
            let known = self
                .connections
                .keys()
                .map(|k| format!("'{k}'"))
                .collect::<Vec<_>>()
                .join(", ");
            let known = if known.is_empty() {
                "<none>".to_string()
            } else {
                known
            };
            SqlFailure {
                kind: "driver_error".into(),
                policy: "fail".into(),
                sqlite_code: None,
                message: format!(
                    "scp connection routing: no connection registered under name '{key}' \
                     (known: {known}). Register it via setConfig/ConnectionRegistry, or drop the \
                     connection tag on the bundle/model."
                ),
            }
        })
    }

    /// The registered connection names (deterministic order; diagnostics / close). Mirrors `names`.
    pub fn names(&self) -> Vec<&str> {
        self.connections.keys().map(|k| k.as_str()).collect()
    }
}

/// Incremental [`ConnectionRegistry`] builder (name → pools). Mirrors the TS `ConnectionRegistryBuilder`.
#[derive(Default)]
pub struct ConnectionRegistryBuilder {
    connections: BTreeMap<String, ReaderWriterPools>,
}

impl ConnectionRegistryBuilder {
    pub fn new() -> Self {
        ConnectionRegistryBuilder::default()
    }

    /// Register `name` → its reader/writer pools (chainable). Re-adding a name overwrites it.
    pub fn add(mut self, name: &str, pools: ReaderWriterPools) -> Self {
        self.connections.insert(name.to_string(), pools);
        self
    }

    /// Finalize into an immutable [`ConnectionRegistry`]. Loud if empty (must have the default).
    pub fn build(self) -> Result<ConnectionRegistry, SqlFailure> {
        if self.connections.is_empty() {
            return Err(SqlFailure {
                kind: "driver_error".into(),
                policy: "fail".into(),
                sqlite_code: None,
                message:
                    "scp connection routing: ConnectionRegistry must have at least the default \
                          connection"
                        .into(),
            });
        }
        Ok(ConnectionRegistry::new(self.connections))
    }
}

// ── Writer-sticky (C1) — injectable clock ──────────────────────────────────────

/// A monotonic-ish clock the writer-sticky uses. The default is wall time; tests inject a fixed /
/// advanceable clock ([`ManualClock`]) so sticky expiry is deterministic (mirror the TS `now` injectable).
pub trait Clock: Send + Sync {
    /// The current time in MILLISECONDS (any epoch; only differences matter).
    fn now_ms(&self) -> u64;
}

/// The default wall-clock (`Instant`-based, monotonic). Used when no clock is injected.
#[derive(Default)]
pub struct SystemClock {
    start: std::sync::OnceLock<std::time::Instant>,
}

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        let start = self.start.get_or_init(std::time::Instant::now);
        start.elapsed().as_millis() as u64
    }
}

/// A test clock the caller advances explicitly (`set`/`advance`) — the rust analogue of the TS
/// `now: () => clock` injectable, letting sticky-expiry be proven deterministically without sleeping.
#[derive(Default)]
pub struct ManualClock {
    ms: std::sync::atomic::AtomicU64,
}

impl ManualClock {
    pub fn new(start_ms: u64) -> Self {
        ManualClock {
            ms: std::sync::atomic::AtomicU64::new(start_ms),
        }
    }
    /// Set the clock to an absolute ms value.
    pub fn set(&self, ms: u64) {
        self.ms.store(ms, std::sync::atomic::Ordering::SeqCst);
    }
    /// Advance the clock by `delta` ms.
    pub fn advance(&self, delta: u64) {
        self.ms
            .fetch_add(delta, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.ms.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// A writer-sticky CLOCK (C1, read-your-writes; mirror v1 `_shouldUseWriterSticky` + the TS
/// [`WriterStickyClock`]). After a transaction (or a bare write) COMMITs, reads within
/// `sticky_duration_ms` route to the WRITER pool so a just-committed row is visible despite reader-
/// replica lag. The ctx owns ONE clock; the tx runtime `.mark()`s it on every successful write/commit;
/// [`resolve_pool`] reads `.is_sticky()`.
///
/// `use_writer_after_transaction=false` disables it entirely (`.is_sticky()` always false). A
/// single-pool deployment (reader === writer) is unaffected by stickiness — the diverted pool is the
/// SAME `Arc`. Interior-mutable (`AtomicU64`) so the shared ctx (`&self`) can `mark()` it.
pub struct WriterStickyClock {
    last_write_at: std::sync::atomic::AtomicU64,
    enabled: bool,
    sticky_duration_ms: u64,
    clock: Arc<dyn Clock>,
}

/// Options for [`WriterStickyClock`] (mirror the TS sticky opts). `now` is the injectable clock.
pub struct StickyOptions {
    /// Enable writer-sticky (read-your-writes after a committed tx). Default true.
    pub use_writer_after_transaction: bool,
    /// The sticky window in ms after a committed write. Default 5000.
    pub writer_sticky_duration: u64,
    /// The clock (wall clock by default; a [`ManualClock`] in tests).
    pub clock: Arc<dyn Clock>,
}

impl Default for StickyOptions {
    fn default() -> Self {
        StickyOptions {
            use_writer_after_transaction: true,
            writer_sticky_duration: 5000,
            clock: Arc::new(SystemClock::default()),
        }
    }
}

impl WriterStickyClock {
    /// Build from [`StickyOptions`] (mirror the TS `new WriterStickyClock(opts)`).
    pub fn new(opts: StickyOptions) -> Self {
        WriterStickyClock {
            last_write_at: std::sync::atomic::AtomicU64::new(0),
            enabled: opts.use_writer_after_transaction,
            sticky_duration_ms: opts.writer_sticky_duration,
            clock: opts.clock,
        }
    }

    /// A DISABLED sticky clock (`is_sticky()` always false) — the single-pool backward-compat default
    /// the base ctx synthesizes (so every intent lands on the one pool).
    pub fn disabled() -> Self {
        WriterStickyClock::new(StickyOptions {
            use_writer_after_transaction: false,
            ..StickyOptions::default()
        })
    }

    /// Record that a write/commit just happened (the tx runtime calls this on success).
    pub fn mark(&self) {
        if self.enabled {
            self.last_write_at
                .store(self.clock.now_ms(), std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Is a read currently sticky-to-writer (within `writer_sticky_duration` of the last write)?
    pub fn is_sticky(&self) -> bool {
        if !self.enabled {
            return false;
        }
        let last = self.last_write_at.load(std::sync::atomic::Ordering::SeqCst);
        if last == 0 {
            return false;
        }
        self.clock.now_ms().saturating_sub(last) < self.sticky_duration_ms
    }

    /// Reset the clock (e.g. between tests / on close).
    pub fn reset(&self) {
        self.last_write_at
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }
}

// ── The routing config a PooledContext carries (C1+C2+C3) ──────────────────────

/// The routing configuration a routed [`ExecutionContext`](crate::exec_context) carries to complete
/// its `connection_for(intent)` resolution (steps 2-4): the multi-DB [`ConnectionRegistry`] + the
/// [`WriterStickyClock`]. Absent (the base [`for_driver`](crate::exec_context::for_driver) ctx) ⇒ the
/// ctx resolves straight to its single driver — the byte-identical Phase A/B path. Mirrors the TS
/// [`RoutingConfig`].
pub struct RoutingConfig {
    pub registry: ConnectionRegistry,
    pub sticky: WriterStickyClock,
}

impl RoutingConfig {
    /// Build a routing config over a single driver (default-only registry, reader === writer, sticky
    /// disabled) — the explicit single-pool form (byte-identical routing to the base ctx). Useful for
    /// a ctx that wants the registry surface without reader/writer separation.
    pub fn single(pool: Arc<dyn Driver + Send + Sync>) -> Self {
        RoutingConfig {
            registry: ConnectionRegistry::single_default(pool),
            sticky: WriterStickyClock::disabled(),
        }
    }
}

// ── The core resolution (steps 2-4) — the ONE routing function the ports mirror ─

/// Resolve WHICH pool (a [`Driver`]) serves a statement given its [`StatementIntent`] and the routing
/// config + the `in_writer_scope` flag — the completion of `connection_for`'s steps 2-4 (step 1, the
/// tx-pin, is handled by the ctx BEFORE calling this). The order (mirror the TS `resolvePool`):
///
///   1. **named-DB** (`intent.db`) selects the [`ReaderWriterPools`] pair (loud on unknown name).
///   2. within that pair: a WRITE ⇒ the writer pool.
///   3. a READ in a writer scope OR within writer-sticky ⇒ the writer pool (read-your-writes).
///   4. otherwise a READ ⇒ the reader pool.
///
/// Single-pool (reader === writer) ⇒ every branch returns the SAME `Arc` (backward-compat). Returns a
/// borrowed `&Arc<dyn Driver>` from the registry so the caller (the ctx seam) can build a
/// [`DriverConnection`](crate::exec_context::DriverConnection) over it without cloning per statement.
///
/// `in_writer_scope` is threaded EXPLICITLY (the approved rust-idiomatic decision — no task-local; the
/// TS reads an async-local `inWriterScope()`). The ctx carries it as a field derived by
/// [`ExecutionContext::with_writer`](crate::exec_context).
pub fn resolve_pool<'r>(
    intent: &StatementIntent,
    routing: &'r RoutingConfig,
    in_writer_scope: bool,
) -> Result<&'r Arc<dyn Driver + Send + Sync>, SqlFailure> {
    let pair = routing.registry.pair_for(intent.db.as_deref())?;
    if intent.write {
        return Ok(&pair.writer); // writes always to the writer
    }
    if in_writer_scope || routing.sticky.is_sticky() {
        return Ok(&pair.writer); // read-your-writes
    }
    Ok(&pair.reader) // plain read → reader
}

// ── setConfig / PoolFactory / closeAllPools (C3 public surface) ────────────────

/// A built pool: the [`Driver`] (owning its internal connection pool) as an `Arc` (shared reader ===
/// writer, or a distinct writer), plus the SIZING config it was built with (so the caller can wrap it
/// with a [`ConfiguredDriver`](crate::driver::ConfiguredDriver) for the session knobs). Mirrors the TS
/// factory return `{ pool, close }` — in rust the "close" is dropping the `Arc` (a live driver's pool
/// closes on drop), so a separate closer is not returned; [`RoutingHandle::close`] drops all drivers.
pub struct BuiltPool {
    /// The constructed driver (its pool sized/keepAlive-applied AT CONSTRUCTION — the sole cap source).
    pub pool: Arc<dyn Driver + Send + Sync>,
}

/// The role a pool is being built for (reader vs writer replica split). Mirrors the TS factory `role`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolRole {
    Reader,
    Writer,
}

/// A driver's pool FACTORY (Phase C / #88 — the rust analogue of the TS `PoolFactory`): BUILD a pool
/// from a [`ResolvedConnectionConfig`] + a [`PoolRole`], returning the constructed [`Driver`]. This is
/// where the CONSTRUCTION knobs — pool sizing (`min_pool`/`max_pool`) + `keep_alive` — reach the real
/// driver (`PostgresDriver::connect_with_config` / `MysqlDriver::connect_with_config`), because those
/// are pool-CONSTRUCTION options a pre-built pool can no longer accept. [`build_routing_config`] OWNS
/// the call to this factory with the RESOLVED config, so the configured sizing is the SOLE source of
/// the pool's cap — there is no second raw pool-construction path. `role` lets a factory build a
/// distinct replica pool for the reader vs. the writer (e.g. a different host) while sharing the sizing
/// config; a factory that returns the SAME driver for both roles collapses to single-pool.
///
/// The module stays driver-AGNOSTIC: the factory is supplied by the caller (this module constructs no
/// live driver — `pg_pool_factory` / `mysql_pool_factory` live behind the `livedb` feature). `Fn` (not
/// `FnMut`) + `Send + Sync` so a builder can call it once per role.
pub type PoolFactory<'f> =
    dyn Fn(&ResolvedConnectionConfig, PoolRole) -> Result<BuiltPool, SqlFailure> + 'f;

/// One connection's inputs to [`build_routing_config`] (Phase C / #88 — the rust analogue of the TS
/// `ConnectionSetup`): its NAME (default when absent), its [`ConnectionConfig`], a [`PoolFactory`] that
/// `build_routing_config` CALLS with the resolved config to construct the pool(s) — so sizing/keepAlive
/// are applied at construction and the config is the sole cap source — and whether to build a distinct
/// writer pool (reader/writer replica split).
pub struct ConnectionSetup<'f> {
    /// The connection name (default connection when `None`).
    pub name: Option<String>,
    /// The connection config (connection params + sizing + keepAlive + session knobs).
    pub config: ConnectionConfig,
    /// The driver pool factory (called by `build_routing_config` with the resolved config).
    pub pool_factory: &'f PoolFactory<'f>,
    /// Build a distinct writer pool via the factory (replica split). Default false ⇒ reader === writer.
    pub separate_writer: bool,
}

/// A handle owning the constructed [`RoutingConfig`] + all its pools, with an explicit
/// [`RoutingHandle::close`] that drops every pool (closing each driver's internal connection pool). The
/// rust analogue of the TS `{ routing, close }`. Because a live driver's pool closes on `Drop`, `close`
/// simply drops the routing (and thus all `Arc`s). Deref to the [`RoutingConfig`] for building a ctx.
pub struct RoutingHandle {
    routing: RoutingConfig,
}

impl RoutingHandle {
    /// The [`RoutingConfig`] to build a routed ctx from
    /// ([`for_routing`](crate::exec_context::for_routing)).
    pub fn routing(&self) -> &RoutingConfig {
        &self.routing
    }

    /// Close every constructed pool (drops all driver `Arc`s ⇒ each internal pool closes). Consumes the
    /// handle. Mirrors the TS `closeAllPools`. After close, a ctx built from this routing must not be
    /// used (its drivers are gone).
    pub fn close(self) {
        drop(self.routing);
    }
}

/// The C3 `setConfig`: build a [`RoutingConfig`] (registry + writer-sticky) from one or more
/// [`ConnectionSetup`]s, CONSTRUCTING each pool via its [`PoolFactory`] with the RESOLVED config — so
/// `min_pool`/`max_pool`/`keep_alive` are applied AT CONSTRUCTION (the config is the SOLE source of the
/// cap) — then wrapping each pool with a [`ConfiguredDriver`](crate::driver::ConfiguredDriver) so the
/// SESSION knobs (queryTimeout/searchPath/charset) apply on checkout WITH reset-on-release. Mirrors the
/// TS `buildRoutingConfig`.
///
/// The setup named `default` (or the first unnamed) is the default connection. Returns a
/// [`RoutingHandle`] owning the routing + all pools (`close()` drops them).
pub fn build_routing_config(
    setups: Vec<ConnectionSetup<'_>>,
    sticky: StickyOptions,
) -> Result<RoutingHandle, SqlFailure> {
    if setups.is_empty() {
        return Err(SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: "scp setConfig: at least one connection setup is required".into(),
        });
    }
    let mut builder = ConnectionRegistryBuilder::new();
    for s in setups {
        let resolved = s.config.resolve();
        // CONSTRUCT the reader pool from the resolved config (sizing/keepAlive land at construction).
        let reader_built = (s.pool_factory)(&resolved, PoolRole::Reader)?;
        // Wrap with the session config so the SESSION knobs apply on checkout WITH reset-on-release.
        // All-default config ⇒ empty session ⇒ skip the wrapper for a byte-identical zero-overhead path.
        let reader = wrap_configured(reader_built.pool, &resolved);
        let pair = if s.separate_writer {
            let writer_built = (s.pool_factory)(&resolved, PoolRole::Writer)?;
            let writer = wrap_configured(writer_built.pool, &resolved);
            ReaderWriterPools::split(reader, writer)
        } else {
            ReaderWriterPools::single(reader) // reader === writer (one constructed pool)
        };
        builder = builder.add(s.name.as_deref().unwrap_or(DEFAULT_CONNECTION), pair);
    }
    let routing = RoutingConfig {
        registry: builder.build()?,
        sticky: WriterStickyClock::new(sticky),
    };
    Ok(RoutingHandle { routing })
}

/// Wrap a built pool with a [`ConfiguredDriver`](crate::driver::ConfiguredDriver) IF the config sets
/// any session knobs; otherwise return the pool unwrapped (byte-identical zero-overhead passthrough for
/// an all-default config — the backward-compat path).
fn wrap_configured(
    pool: Arc<dyn Driver + Send + Sync>,
    config: &ResolvedConnectionConfig,
) -> Arc<dyn Driver + Send + Sync> {
    if session_statements(config).is_empty() {
        pool
    } else {
        Arc::new(crate::driver::ConfiguredDriver::new(pool, config))
    }
}

/// A [`PoolFactory`] for the live PostgreSQL driver (Phase C / #88): constructs a [`PostgresDriver`]
/// from a [`ResolvedConnectionConfig`] via `connect_with_config`, applying the pool SIZING (`max_pool`)
/// and keepAlive AT CONSTRUCTION — the config is the sole cap source. `role` is accepted for the
/// reader/writer replica split (the caller may vary host per role via the config it passes); behind
/// the `livedb` feature (needs the live driver crates).
#[cfg(feature = "livedb")]
pub fn pg_pool_factory(
) -> impl Fn(&ResolvedConnectionConfig, PoolRole) -> Result<BuiltPool, SqlFailure> {
    |config: &ResolvedConnectionConfig, _role: PoolRole| {
        let driver = crate::livedb::PostgresDriver::connect_with_config(config)?;
        Ok(BuiltPool {
            pool: Arc::new(driver),
        })
    }
}

/// A [`PoolFactory`] for the live MySQL driver (Phase C / #88): constructs a [`MysqlDriver`] from a
/// [`ResolvedConnectionConfig`] via `connect_with_config`, applying the pool SIZING (`max_pool` /
/// `min_pool`) AT CONSTRUCTION. Behind the `livedb` feature.
#[cfg(feature = "livedb")]
pub fn mysql_pool_factory(
) -> impl Fn(&ResolvedConnectionConfig, PoolRole) -> Result<BuiltPool, SqlFailure> {
    |config: &ResolvedConnectionConfig, _role: PoolRole| {
        let driver = crate::livedb::MysqlDriver::connect_with_config(config)?;
        Ok(BuiltPool {
            pool: Arc::new(driver),
        })
    }
}

// ── Unit tests (no DB) — the pure routing / config surface ─────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{PreparedStatement, RunInfo};
    use crate::exec_context::TxConnection;
    use behavior_contracts::Value;

    /// A stub driver whose ONLY use is `Arc`-identity (a test asserts WHICH driver a resolution returns
    /// via `Arc::ptr_eq`). It never actually executes SQL in these no-DB unit tests.
    struct StubDriver;
    struct StubStmt;
    impl PreparedStatement for StubStmt {
        fn all(&mut self, _p: &[Value]) -> Result<Vec<Value>, SqlFailure> {
            Ok(Vec::new())
        }
        fn run(&mut self, _p: &[Value]) -> Result<RunInfo, SqlFailure> {
            Ok(RunInfo {
                changes: 0,
                last_insert_rowid: 0,
            })
        }
    }
    impl Driver for StubDriver {
        fn prepare(&self, _sql: &str) -> Box<dyn PreparedStatement + '_> {
            Box::new(StubStmt)
        }
        fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
            crate::driver::forwarding_tx(self)
        }
    }
    fn stub(_label: &'static str) -> Arc<dyn Driver + Send + Sync> {
        Arc::new(StubDriver)
    }
    fn label_of(
        d: &Arc<dyn Driver + Send + Sync>,
        candidates: &[(&'static str, &Arc<dyn Driver + Send + Sync>)],
    ) -> &'static str {
        candidates
            .iter()
            .find(|(_, c)| Arc::ptr_eq(d, c))
            .map(|(l, _)| *l)
            .unwrap_or("?")
    }

    #[test]
    fn resolve_defaults() {
        let a = ConnectionConfig::default().resolve();
        assert_eq!(a.driver, ConfigDialect::Postgres);
        assert_eq!(a.query_timeout, 0);
        assert!(!a.keep_alive);
        assert_eq!(a.min_pool, 0);
        assert_eq!(a.max_pool, 10);
        assert_eq!(a.keep_alive_initial_delay_millis, 10_000);
    }

    #[test]
    fn session_statements_per_dialect() {
        let pg_timeout = ConnectionConfig {
            driver: Some(ConfigDialect::Postgres),
            query_timeout: Some(250),
            ..Default::default()
        }
        .resolve();
        assert_eq!(
            session_statements(&pg_timeout),
            vec!["SET statement_timeout = 250".to_string()]
        );
        let pg_search = ConnectionConfig {
            driver: Some(ConfigDialect::Postgres),
            search_path: Some("app,public".into()),
            ..Default::default()
        }
        .resolve();
        assert_eq!(
            session_statements(&pg_search),
            vec!["SET search_path TO app,public".to_string()]
        );
        let pg_charset = ConnectionConfig {
            driver: Some(ConfigDialect::Postgres),
            charset: Some("UTF8".into()),
            ..Default::default()
        }
        .resolve();
        assert_eq!(
            session_statements(&pg_charset),
            vec!["SET client_encoding TO UTF8".to_string()]
        );
        let my_timeout = ConnectionConfig {
            driver: Some(ConfigDialect::Mysql),
            query_timeout: Some(250),
            ..Default::default()
        }
        .resolve();
        assert_eq!(
            session_statements(&my_timeout),
            vec!["SET SESSION max_execution_time = 250".to_string()]
        );
        let my_charset = ConnectionConfig {
            driver: Some(ConfigDialect::Mysql),
            charset: Some("utf8mb4".into()),
            ..Default::default()
        }
        .resolve();
        assert_eq!(
            session_statements(&my_charset),
            vec!["SET NAMES utf8mb4".to_string()]
        );
        // MySQL has no search path → ignored.
        let my_search = ConnectionConfig {
            driver: Some(ConfigDialect::Mysql),
            search_path: Some("x".into()),
            ..Default::default()
        }
        .resolve();
        assert!(session_statements(&my_search).is_empty());
        // All-default → EMPTY (backward-compat: session untouched).
        assert!(session_statements(&ConnectionConfig::default().resolve()).is_empty());
    }

    #[test]
    fn session_reset_mirrors_the_set() {
        let cfg = ConnectionConfig {
            driver: Some(ConfigDialect::Postgres),
            query_timeout: Some(250),
            search_path: Some("app".into()),
            charset: Some("UTF8".into()),
            ..Default::default()
        }
        .resolve();
        assert_eq!(
            session_reset_statements(&cfg),
            vec![
                "RESET statement_timeout".to_string(),
                "RESET search_path".to_string(),
                "RESET client_encoding".to_string(),
            ]
        );
        let my = ConnectionConfig {
            driver: Some(ConfigDialect::Mysql),
            query_timeout: Some(250),
            charset: Some("utf8mb4".into()),
            ..Default::default()
        }
        .resolve();
        assert_eq!(
            session_reset_statements(&my),
            vec![
                "SET SESSION max_execution_time = DEFAULT".to_string(),
                "SET NAMES DEFAULT".to_string(),
            ]
        );
        // All-default ⇒ nothing to reset.
        assert!(session_reset_statements(&ConnectionConfig::default().resolve()).is_empty());
    }

    #[test]
    fn unknown_name_is_loud() {
        let reg = ConnectionRegistry::single_default(stub("a"));
        let e = reg.pair_for(Some("ghost")).unwrap_err();
        assert!(e
            .message
            .contains("no connection registered under name 'ghost'"));
        // The default is reachable.
        assert!(reg.pair_for(None).is_ok());
    }

    #[test]
    fn empty_registry_build_is_loud() {
        assert!(ConnectionRegistryBuilder::new().build().is_err());
    }

    #[test]
    fn resolve_pool_reader_writer_split() {
        let reader = stub("reader");
        let writer = stub("writer");
        let reg = ConnectionRegistry::from_default(ReaderWriterPools::split(
            reader.clone(),
            writer.clone(),
        ))
        .build()
        .unwrap();
        let routing = RoutingConfig {
            registry: reg,
            sticky: WriterStickyClock::disabled(),
        };
        let cands = [("reader", &reader), ("writer", &writer)];
        // A read → reader; a write → writer.
        let r = resolve_pool(&StatementIntent::read(), &routing, false).unwrap();
        assert_eq!(label_of(r, &cands), "reader");
        let w = resolve_pool(&StatementIntent::write(), &routing, false).unwrap();
        assert_eq!(label_of(w, &cands), "writer");
        // A read in a writer scope → writer (read-your-writes).
        let rw = resolve_pool(&StatementIntent::read(), &routing, true).unwrap();
        assert_eq!(label_of(rw, &cands), "writer");
    }

    #[test]
    fn resolve_pool_single_is_same_arc() {
        let pool = stub("solo");
        let routing = RoutingConfig::single(pool.clone());
        // Every branch returns the SAME Arc (backward-compat).
        let r = resolve_pool(&StatementIntent::read(), &routing, false).unwrap();
        let w = resolve_pool(&StatementIntent::write(), &routing, false).unwrap();
        assert!(Arc::ptr_eq(r, &pool));
        assert!(Arc::ptr_eq(w, &pool));
    }

    #[test]
    fn writer_sticky_expiry_with_manual_clock() {
        let clock = Arc::new(ManualClock::new(1_000_000));
        let sticky = WriterStickyClock::new(StickyOptions {
            use_writer_after_transaction: true,
            writer_sticky_duration: 5000,
            clock: clock.clone(),
        });
        // Not armed → not sticky.
        assert!(!sticky.is_sticky());
        // A committed write arms it.
        sticky.mark();
        clock.advance(100);
        assert!(sticky.is_sticky(), "within the 5s window → sticky");
        clock.advance(6000);
        assert!(!sticky.is_sticky(), "past the window → not sticky");
    }

    #[test]
    fn writer_sticky_disabled_never_sticks() {
        let sticky = WriterStickyClock::disabled();
        sticky.mark();
        assert!(!sticky.is_sticky());
    }

    #[test]
    fn named_routing_selects_the_pair() {
        let a = stub("a");
        let b = stub("b");
        let reg = ConnectionRegistry::from_default(ReaderWriterPools::single(a.clone()))
            .add("B", ReaderWriterPools::single(b.clone()))
            .build()
            .unwrap();
        let routing = RoutingConfig {
            registry: reg,
            sticky: WriterStickyClock::disabled(),
        };
        // Untagged → default (a); tagged "B" → b.
        let untagged = resolve_pool(&StatementIntent::read(), &routing, false).unwrap();
        assert!(Arc::ptr_eq(untagged, &a));
        let tagged = resolve_pool(
            &StatementIntent {
                write: false,
                db: Some("B".into()),
            },
            &routing,
            false,
        )
        .unwrap();
        assert!(Arc::ptr_eq(tagged, &b));
        // An unknown tag is loud.
        assert!(resolve_pool(
            &StatementIntent {
                write: false,
                db: Some("ghost".into()),
            },
            &routing,
            false,
        )
        .is_err());
    }
}
