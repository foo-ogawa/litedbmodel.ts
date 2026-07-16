"""litedbmodel SCP Python runtime — the static makeSQL bundle interpreter (epic #43/#45).

The thin Python leg of the multi-language SCP runtime. It consumes the language-neutral, portable
static-makeSQL artifacts the corpus ships (pure JSON: a read ``ReadGraph`` = a bc surrogate
``ComponentGraphIR`` + per-node STATIC statement templates, or a write ``TransactionPlan`` of
gate-first makeSQL statements) and executes them against a SQL driver — SEMANTICS-IDENTICAL to the
TS reference (``src/scp/runtime.ts`` + ``src/scp/makesql/*``).

It re-implements NO generic expression evaluation: the NATIVE read-graph walker (#12) owns the
plan/map/wire/output orchestration and bc's ``evaluate_expression`` owns the CLOSED Expression-IR
(ref/refOpt/coalesce/eq/…, the SKIP guards + deferred value-specs) — NO bc ``run_behavior``. This
module adds ONLY the SQL-backend concerns (spec §11): assemble the static makeSQL statements per dialect → bind params →
execute REAL SQL → row→result assembly; and, for a Command bundle, the gate-first write transaction
(``execute_transaction_bundle``). This mirrors the TS runtime's division of labor exactly (bc-core
+ a makeSQL handler). The reduced fragment-tree render path (``render.py``) is RETIRED for the SQL
path — makeSQL is the sole read/render path.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Union

from .dialect import dialect_for
from .driver import Driver
from .errors import SqlFailure, map_sqlite_error
from .exec_context import (
    WRITE_INTENT,
    ExecutionContext,
    as_context,
    check_write_allowed_ambient,
    current_context,
    execute as seam_execute,
    run as seam_run,
    with_transaction_decided,
    commit as tx_commit,
    rollback as tx_rollback,
)
from .static_bundle import (
    assemble_make_sql,
    execute_read_graph,
    render_placeholders,
    render_read_primary,
    render_statements,
)

# The reserved binding the body write's RETURNING row is exposed under (mirrors TS ENTITY_ROOT).
ENTITY_ROOT = "__entity"

_RETURNING_RE = re.compile(r"\breturning\b", re.IGNORECASE)
_SELECT_RE = re.compile(r"\bselect\b", re.IGNORECASE)

__all__ = [
    "ENTITY_ROOT",
    "execute_bundle",
    "execute_transaction_bundle",
    "render_read_primary",
    "order_by_nulls",
]


# ── Value normalization at the SQL boundary (mirrors TS tx.ts toDriverParam) ───


def _to_driver_param_tx(v: Any) -> Any:
    """Convert a tx-rendered param to a driver-bindable value (mirrors TS tx toDriverParam).

    bc-Python evaluates integers to a plain unbounded ``int`` (no bigint), so no narrowing is
    needed. An emit payload evaluates to a plain ``dict`` (a ``{obj:{…}}`` payload): serialize it to
    compact JSON (no spaces) for a text column — byte-identical to the TS ``JSON.stringify``. A bool
    passes through (sqlite3 binds it 0/1).
    """
    if isinstance(v, bool):
        return v
    if isinstance(v, dict):
        import json

        return json.dumps(v, separators=(",", ":"), ensure_ascii=False)
    return v


# ── Read/exec bundle execution ─────────────────────────────────────────────────


def execute_bundle(
    bundle: Mapping[str, Any],
    input_scope: Mapping[str, Any],
    driver: Union[Driver, ExecutionContext],
) -> Any:
    """Execute a read/exec SqlBundle end-to-end (native read-graph walker; no bc run_behavior).

    The SAME code path a consumer runtime follows: it consumes ONLY the serialized bundle + bc
    runtime-core, never re-running litedbmodel's Backend-Compile. A read bundle carries a
    ``readGraph`` (the surrogate IR + static statements): bc drives map/Φ/wiring and the makeSQL
    handler renders + executes each node. Returns the component's Φ output — byte-identical to the
    TS ``executeBundle``.

    ``driver`` is EITHER a raw :class:`Driver` (wrapped via the backward-compat
    :func:`context_for_driver`, §6 — byte-identical) OR an already-built :class:`ExecutionContext`;
    the read graph then funnels every SQL through the central seam.
    """
    read_graph = bundle.get("readGraph")
    if read_graph is None:
        raise ValueError(
            f"scp runtime: bundle '{bundle.get('name')}' carries no read graph "
            "(single-statement writes ride execute_transaction_bundle / the write path)"
        )
    return execute_read_graph(read_graph, input_scope, as_context(driver))


# ── Write-time relations: gate-first transaction (spec §6 — port of tx.ts) ─────


def _render_tx_statement(op: Mapping[str, Any], scope: Mapping[str, Any], dialect_name: str) -> Dict[str, Any]:
    """Render a tx statement's makeSQL op against the tx scope (mirrors TS renderStatement).

    Evaluate each deferred Expression-IR param to a concrete value (bc ``evaluate_expression``),
    build a concrete makeSQL, assemble + render placeholders. The SAME assemble/render the read path
    uses — only the param values come from the tx scope, not a compile-time input.
    """
    from behavior_contracts import evaluate_expression

    concrete = [evaluate_expression(p, scope) for p in op["params"]]
    node = {"sql": op["sql"], "params": concrete}
    assembled = assemble_make_sql(node)
    return {
        "sql": render_placeholders(assembled["sql"], dialect_name),
        "params": [_to_driver_param_tx(p) for p in assembled["params"]],
    }


def _gate_short_circuit(gate: str, rows: List[Dict[str, Any]], changes: int) -> Optional[str]:
    """Evaluate a gate rule on a statement result → short-circuit reason, or None to continue."""
    if gate == "existsElseRollback":
        return "requires_absent" if len(rows) == 0 else None
    if gate == "insertedElseRollback":
        return "unique_collision" if changes == 0 else None
    if gate == "insertedElseNoop":
        return "idempotent_duplicate" if changes == 0 else None
    raise ValueError(f"scp write: unknown gate rule '{gate}'")


def _exec_statement(op: Mapping[str, Any], scope: Mapping[str, Any], dialect_name: str, ctx: ExecutionContext):
    """Render + execute one tx statement THROUGH THE SEAM; return (rows, changes). Mirrors TS
    execStatement / go execTxStatement. Every tx statement carries WRITE_INTENT (it targets the
    writer / tx-owned connection); a SELECT/RETURNING funnels through the read seam, a non-returning
    write through the write seam — both resolving the tx's pinned connection via ``connection_for``.
    """
    rendered = _render_tx_statement(op, scope, dialect_name)
    sql = rendered["sql"]
    has_return = bool(_SELECT_RE.search(sql[:8])) or bool(_RETURNING_RE.search(sql))
    if has_return:
        rows = seam_execute(ctx, sql, rendered["params"], WRITE_INTENT)
        return rows, len(rows)
    info = seam_run(ctx, sql, rendered["params"], WRITE_INTENT)
    return [], info.changes


def _run_tx_plan(
    plan: Mapping[str, Any],
    input_scope: Mapping[str, Any],
    dialect_name: str,
    tx_ctx: ExecutionContext,
    is_batch: bool,
):
    """Run the gate-first plan on a tx-scoped ctx, returning ``(result_dict, rolled_back)``.

    Statements run in the plan's fixed order (requires → idempotency → unique → body → derive → edges
    → emits, topo-ordered for composite DAGs); a failing gate short-circuits (``rolled_back=True``, a
    legitimate ``committed:false`` outcome — NOT a raised error) and the tail never executes. On
    success returns ``(committed-dict, False)``. This is the SHARED plan runner both the own-tx path
    (:func:`with_transaction_decided`, which turns ``rolled_back`` into a ROLLBACK of the owned
    connection) and the ambient-JOIN path (where a gate short-circuit returns ``committed:false``
    WITHOUT rolling back the outer tx — the outer owns its COMMIT/ROLLBACK, go ``TransactionDecided``
    nested parity) drive.
    """
    executed: List[str] = []
    scope: Dict[str, Any] = dict(input_scope)
    entity: Optional[Dict[str, Any]] = None
    returned_rows: List[List[Dict[str, Any]]] = []

    for stmt in plan["statements"]:
        rows, changes = _exec_statement(stmt["op"], scope, dialect_name, tx_ctx)
        executed.append(stmt["id"])

        gate = stmt.get("gate")
        if gate is not None:
            reason = _gate_short_circuit(gate, rows, changes)
            if reason is not None:
                return (
                    {
                        "committed": False,
                        "shortCircuit": {"statementId": stmt["id"], "reason": reason},
                        "entity": None,
                        "executed": executed,
                    },
                    True,
                )

        if stmt["id"] == plan.get("entityFrom"):
            entity = rows[0] if rows else None
            if entity is not None:
                scope[ENTITY_ROOT] = entity

        binds = stmt.get("binds")
        if binds is not None and rows:
            scope[binds] = rows[0]

        if is_batch and stmt.get("role") == "body" and rows:
            returned_rows.append(rows)

    out: Dict[str, Any] = {"committed": True, "entity": entity, "executed": executed}
    if returned_rows:
        out["returnedRows"] = returned_rows
    return out, False


def execute_transaction_bundle(
    bundle: Mapping[str, Any],
    input_scope: Mapping[str, Any],
    driver: Union[Driver, ExecutionContext],
    guard: bool = True,
) -> Dict[str, Any]:
    """Execute a SqlBundle's derived transaction plan as ONE real transaction (gate-first).

    Byte-for-byte port of the TS ``executeTransactionBundle`` → ``executeTransaction`` (spec §6), over
    the Phase A **per-execution connection ownership** seam (#78) + the Phase B **ambient-tx JOIN +
    write=tx guard** (#84/#86):

    - **Ambient-tx JOIN (the core #86 fix)**: if this call runs INSIDE a user :func:`transaction`
      boundary — detected via the ambient contextvar-propagated tx ctx (:func:`current_context`) — the
      plan JOINs the ambient tx: it runs on the OUTER's pinned owned connection with NO new
      BEGIN/COMMIT/acquire. So N ops inside one ``transaction(fn)`` produce exactly ONE BEGIN + ONE
      COMMIT on ONE connection. A gate short-circuit inside a JOIN returns ``committed:false`` WITHOUT
      rolling back the outer tx (the outer decides its own COMMIT/ROLLBACK — go ``TransactionDecided``
      nested parity). An ERROR still propagates and rolls back the whole outer tx.

    - **Own-tx path (outside a boundary)**: the whole plan runs inside :func:`with_transaction_decided`,
      which acquires ONE owned connection (``driver.begin_tx()``), pins it, and COMMITs / ROLLBACKs on
      the SAME owned connection — never a driver-global writer slot. Concurrent transactions each own a
      DISTINCT connection ⇒ isolated. This preserves the Phase A per-command auto-tx (the conformance /
      livedb corpus runs here, byte-identically).

    - **write=tx guard**: ``guard`` (default ON) enforces :func:`check_write_allowed_ambient` at ENTRY
      — a write OUTSIDE a :func:`transaction` boundary raises :class:`WriteOutsideTransactionError`;
      one in a read-only scope raises :class:`WriteInReadOnlyContextError` (read-only first). The
      ``guard=False`` opt-out is INTERNAL-only (the conformance / livedb / ownership paths that run a
      per-command auto-tx without a user boundary) — it is NOT a user-facing surface (per the #86
      audit send-off: never expose ``guard:false`` on a user write facade). The default-ON callers pass
      nothing; the internal per-command paths pass ``guard=False``.

    ``driver`` is EITHER a raw :class:`Driver` (wrapped via the backward-compat
    :func:`context_for_driver`, §6 — byte-identical) OR an already-built :class:`ExecutionContext`.
    """
    plan = bundle.get("transaction")
    if plan is None:
        raise ValueError("scp write: this bundle carries no transaction plan (not a write-time-relations Command bundle)")
    dialect_name = bundle["dialect"]
    ctx = as_context(driver)

    # write=tx guard (default ON): a write must be inside a user transaction() boundary. The guard
    # reads the AMBIENT contextvar markers, so it fires BEFORE any connection is acquired.
    if guard:
        check_write_allowed_ambient("WRITE", bundle.get("name"))

    # Batch mode (createMany/updateMany/deleteMany): a gate-free, ref-free plan (entityFrom is null,
    # every statement a plain body) — accumulate each body statement's RETURNING rows in order.
    is_batch = plan.get("entityFrom") is None and all(
        s.get("gate") is None and s.get("binds") is None and s.get("role") == "body" for s in plan["statements"]
    )

    # AMBIENT-TX JOIN: inside a user transaction() the ambient contextvar carries the outer's pinned tx
    # ctx — run the plan on THAT connection with NO new BEGIN/COMMIT (the nested-join). A gate
    # short-circuit returns committed:false without rolling back the outer; an error propagates.
    ambient = current_context()
    if ambient is not None and ambient.in_transaction():
        try:
            result, _rolled_back = _run_tx_plan(plan, input_scope, dialect_name, ambient, is_batch)
            return result
        except SqlFailure:
            raise
        except Exception as e:
            raise map_sqlite_error(e)

    # OWN-TX path: acquire + own ONE connection for this plan (the per-command auto-tx, Phase A).
    def body(tx_ctx: ExecutionContext):
        result, rolled_back = _run_tx_plan(plan, input_scope, dialect_name, tx_ctx, is_batch)
        # A failed gate rolls back the owned connection (a legitimate outcome, NOT an error); success
        # commits it.
        return tx_rollback(result) if rolled_back else tx_commit(result)

    try:
        return with_transaction_decided(ctx, body)
    except SqlFailure:
        raise
    except Exception as e:
        raise map_sqlite_error(e)


# ── Dialect primitive (render axis + dialect suite) ────────────────────────────


def order_by_nulls(expr: str, direction: str, nulls: str, dialect_name: str) -> str:
    """The dialect NULLS-ordering primitive (native for PG/SQLite, IS NULL for MySQL)."""
    return dialect_for(dialect_name).order_by_nulls(expr, direction, nulls)
