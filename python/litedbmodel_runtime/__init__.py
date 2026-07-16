"""litedbmodel v2 SCP — Python runtime (WS7b, #31).

The Python leg of the multi-language SCP runtime. It interprets the language-neutral §8 published
bundle (``SqlBundle``: sql text + fragment tree + closed-set Expression-IR param slots +
transaction plan, dialect-tagged) and executes it against a SQL driver, semantics-identical to
the TS reference (``src/scp``). The generic Expression-IR evaluation (SKIP guards, param slots)
and the plan/map/wire/output orchestration are delegated to the shared common core
``behavior-contracts`` (PyPI) — this package re-implements NO generic evaluator, only the
SQL-backend concerns (render → bind → execute → assembly + gate-first transaction), exactly like
the TS runtime.
"""

from __future__ import annotations

from .dialect import SQLITE, POSTGRES, MYSQL, Dialect, dialect_for, to_dollar_placeholders
from .driver import Driver, MysqlDriver, PostgresDriver, PreparedStatement, RunInfo, SqliteDriver, TxConnection
from .errors import SqlFailure, map_sqlite_error
from .exec_context import (
    Connection,
    ExecutionContext,
    MiddlewareChain,
    StatementIntent,
    READ_INTENT,
    WRITE_INTENT,
    TxDecision,
    ambient_in_transaction,
    ambient_read_only,
    as_context,
    check_write_allowed_ambient,
    commit,
    context_for_driver,
    current_context,
    execute,
    rollback,
    run,
    run_guarded,
    run_with_pinned_context,
    transaction,
    with_transaction,
    with_transaction_decided,
)
from .tx_options import (
    IsolationLevel,
    TransactionOptions,
    WriteInReadOnlyContextError,
    WriteOutsideTransactionError,
    begin_statements,
    check_write_allowed,
    is_connection_error,
    is_retryable_tx_error,
    isolation_prelude,
)
from .static_bundle import (
    assemble_make_sql,
    compose_make_sql,
    execute_read_graph,
    render_placeholders,
    render_read_primary,
    render_statements,
)
from .runtime import (
    ENTITY_ROOT,
    execute_bundle,
    execute_transaction_bundle,
    order_by_nulls,
)
from .relation import (
    dedupe_keys,
    distribute_to_parent,
    read_bundle,
    run_relation_op,
)

__version__ = "2.1.0"

__all__ = [
    "__version__",
    # dialect
    "SQLITE",
    "POSTGRES",
    "MYSQL",
    "Dialect",
    "dialect_for",
    "to_dollar_placeholders",
    # driver seam
    "Driver",
    "PreparedStatement",
    "RunInfo",
    "SqliteDriver",
    "PostgresDriver",
    "MysqlDriver",
    "TxConnection",
    # errors
    "SqlFailure",
    "map_sqlite_error",
    # ExecutionContext + central execute/run seam (Phase A / #78)
    "Connection",
    "ExecutionContext",
    "MiddlewareChain",
    "StatementIntent",
    "READ_INTENT",
    "WRITE_INTENT",
    "TxDecision",
    "ambient_in_transaction",
    "ambient_read_only",
    "as_context",
    "check_write_allowed_ambient",
    "commit",
    "context_for_driver",
    "current_context",
    "execute",
    "rollback",
    "run",
    "run_guarded",
    "run_with_pinned_context",
    "transaction",
    "with_transaction",
    "with_transaction_decided",
    # tx-completeness primitives (Phase B / #84)
    "IsolationLevel",
    "TransactionOptions",
    "WriteOutsideTransactionError",
    "WriteInReadOnlyContextError",
    "begin_statements",
    "check_write_allowed",
    "is_connection_error",
    "is_retryable_tx_error",
    "isolation_prelude",
    # static makeSQL bundle runtime (the sole read/render path)
    "assemble_make_sql",
    "compose_make_sql",
    "execute_read_graph",
    "render_placeholders",
    "render_read_primary",
    "render_statements",
    # runtime
    "ENTITY_ROOT",
    "execute_bundle",
    "execute_transaction_bundle",
    "order_by_nulls",
    # read-relation batch execution + hydration (#43)
    "dedupe_keys",
    "distribute_to_parent",
    "read_bundle",
    "run_relation_op",
]
