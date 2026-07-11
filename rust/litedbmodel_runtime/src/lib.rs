//! litedbmodel v2 SCP — Rust thin runtime (WS7e, #34).
//!
//! Interprets the language-neutral §8 published bundle (`SqlBundle`: sql + fragment tree +
//! closed-set Expression-IR param slots + transaction plan, dialect-tagged) and executes it
//! against a SQL driver, semantics-identical to the TS reference (`src/scp`) and the audited
//! Python/PHP sibling runtimes. The generic Expression-IR evaluation and the plan/map/wire/output
//! orchestration are delegated to the shared common core `behavior-contracts` crate
//! (`run_behavior` / `evaluate_expression`), mirroring the TS reference's npm dependency — this
//! crate re-implements NO generic evaluator and NO SQL generation. The SQL text comes wholly from
//! the published bundle; the old standalone `litedbmodel.rs` SQL generation is retired.
//!
//! Module map (mirrors the Python/Go/PHP ports):
//!   - [`dialect`]       — the `?`→`$N` finalize + orderByNulls dialect strategy (spec §4/§8/§10).
//!   - [`static_bundle`] — the static makeSQL render/execute (port of `src/scp/makesql/*`); the
//!     SOLE read/render path (the reduced fragment-tree render is retired).
//!   - [`driver`]        — the synchronous SQL-driver seam (in-proc `rusqlite`; PG/MySQL later).
//!   - [`errors`]        — SQLite error → structured `SqlFailure` (kind + honored bc Policy Kind).
//!   - [`value`]         — JSON ⇄ bc `Value` conversion + the `$bigint` conformance codec.
//!   - [`runtime`]       — the thin facade dispatching to the read graph executor + the gate-first
//!     write transaction.

pub mod dialect;
pub mod driver;
pub mod errors;
pub mod relation;
pub mod runtime;
pub mod static_bundle;
pub mod value;

/// WS7g (#36) live PostgreSQL / MySQL drivers — behind the `livedb` feature so the default
/// conformance build (SQLite bar) needs neither the `postgres` nor `mysql` crate.
#[cfg(feature = "livedb")]
pub mod livedb;

/// Version mirrored from package.json by scripts/sync-versions.mjs (SSoT).
pub const VERSION: &str = "2.0.1";

// ── public surface (mirrors the Python `__all__`) ──────────────────────────────
pub use dialect::{dialect_for, to_dollar_placeholders, Dialect};
pub use driver::{Driver, PreparedStatement, RunInfo, SqliteDriver};
pub use errors::{map_sqlite_error, re_error_to_sql_failure, SqlFailure};
#[cfg(feature = "livedb")]
pub use livedb::{MysqlDriver, PostgresDriver};
pub use relation::{read_bundle_pooled, stitch_relation};
pub use runtime::{
    execute_bundle, execute_bundle_pooled, execute_transaction_bundle, order_by_nulls,
    render_read_primary_bundle, ENTITY_ROOT,
};
pub use static_bundle::{
    dispatch_read_nodes_parallel, execute_read_graph, execute_read_graph_pooled,
    execute_read_graph_via_bc_for_test, render_placeholders, render_read_primary,
    render_statements, render_tx_op, RenderedSql, NODE_COMPONENT, SCOPE_PORT,
};
pub use value::{decode_scope, decode_value, encode_value, Scope};
