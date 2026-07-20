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

pub mod codegen_exec;
pub mod connection_routing;
pub mod dialect;
pub mod driver;
pub mod errors;
pub mod exec_context;
pub mod middleware;
pub mod node;
pub mod relation;
pub mod runtime;
pub mod static_bundle;
pub mod tx_options;
pub mod value;

/// WS7g (#36) live PostgreSQL / MySQL drivers — behind the `livedb` feature so the default
/// conformance build (SQLite bar) needs neither the `postgres` nor `mysql` crate.
#[cfg(feature = "livedb")]
pub mod livedb;

/// Version mirrored from package.json by scripts/sync-versions.mjs (SSoT).
pub const VERSION: &str = "2.1.0";

// ── public surface (mirrors the Python `__all__`) ──────────────────────────────
pub use connection_routing::{
    build_routing_config, resolve_pool, session_reset_statements, session_statements, BuiltPool,
    Clock, ConfigDialect, ConnectionConfig, ConnectionRegistry, ConnectionRegistryBuilder,
    ConnectionSetup, ManualClock, PoolFactory, PoolRole, ReaderWriterPools,
    ResolvedConnectionConfig, RoutingConfig, RoutingHandle, StickyOptions, SystemClock,
    WriterStickyClock, DEFAULT_CONNECTION,
};
#[cfg(feature = "livedb")]
pub use connection_routing::{mysql_pool_factory, pg_pool_factory};
pub use dialect::{dialect_for, to_dollar_placeholders, Dialect};
pub use driver::{
    forwarding_tx, forwarding_tx_no_begin, ConfiguredDriver, Driver, ForwardingTx,
    PreparedStatement, RunInfo, SqliteDriver,
};
pub use errors::{
    check_find_hard_limit, map_sqlite_error, re_error_to_sql_failure, LimitExceededError,
    RuntimeError, SqlFailure, LIMIT_CONTEXT_FIND, LIMIT_CONTEXT_RELATION,
};
pub use exec_context::{
    execute as seam_execute, for_driver, for_routing, run as seam_run, run_guarded, transaction,
    transaction_decided, transaction_decided_on, transaction_on, with_transaction,
    with_transaction_decided, with_transaction_decided_isolated,
    with_transaction_decided_isolated_on, Connection, DriverConnection, Dyn, ExecutionContext,
    Middleware, MiddlewareChain, SeamResult, SessionConnection, StatementIntent, TxConnection,
    TxConnectionRef, TxDecision,
};
// Phase D (#93) — the middleware layer (SQL-level `execute` hook + method-level hooks + Logger +
// raw execute/query), mirroring the TS `src/scp/middleware.ts` API reference.
#[cfg(feature = "livedb")]
pub use livedb::{MysqlDriver, PostgresDriver};
pub use middleware::{
    active_sql_empty, active_sql_hooks, clear_middlewares, create_middleware, logger, raw_execute,
    raw_query, register_method_hook, register_middleware, run_method, use_middleware,
    with_middleware_scope, with_middleware_scope_opts, LogEntry, LoggerState, MethodHook,
    MethodKind, MethodNext, MiddlewareDescriptor, MiddlewareHandle, RawResult, SqlHook, SqlHookFn,
    SqlNext, StateAny,
};
// Native-codegen exec seam (epic #123/#124): the op-agnostic query-exec functions the litedbmodel
// generated companion's `node_*` handlers call, plus the `Wire` adapter (`wire_impls!` bridges the
// module-local wire traits to it). `wire_impls!`/`__wire_probe!`/`__wire_num!` are `#[macro_export]`ed
// at the crate root.
pub use codegen_exec::{
    build_batch_params, build_skip_params, exec, run_transaction, run_transaction_on, wp, wp_array,
    ArrayParamShape, ConnSource, ExecMode, RtNum, RtProbe, SkipFrag, ToWireArray, ToWireParam,
    Wire,
};
pub use node::{decode_value, encode_value, eval_expr, EvalError, Node};
pub use relation::{
    hydrate_relation, read_bundle_pooled, relation_op, stitch_relation, stitch_relation_tree,
    IntoKeyTuple,
};
pub use runtime::{
    execute_bundle, execute_bundle_pooled, execute_transaction_bundle,
    execute_transaction_bundle_ctx, order_by_nulls, render_read_primary_bundle, ENTITY_ROOT,
};
pub use static_bundle::{
    dispatch_read_nodes_parallel, execute_read_graph, execute_read_graph_orchestrator_for_test,
    execute_read_graph_pooled, render_placeholders, render_read_primary, render_statements,
    render_tx_op, RenderedSql, NODE_COMPONENT, SCOPE_PORT,
};
pub use tx_options::{
    begin_statements, check_write_allowed, is_connection_error, is_retryable_tx_error,
    isolation_prelude, write_in_read_only, write_outside_transaction, Dialect as TxDialect,
    IsolationLevel, TransactionOptions,
};
pub use value::{decode_scope, Scope};

/// The bc runtime `Value` (re-exported from `behavior-contracts`) — the read/exec result type of
/// [`execute_bundle`] / [`read_bundle_pooled`] and the row shape [`stitch_relation`] takes/returns.
pub use behavior_contracts::Value;
