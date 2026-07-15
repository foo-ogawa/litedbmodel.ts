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
from typing import Any, Dict, List, Mapping, Optional

from .dialect import dialect_for
from .driver import Driver
from .errors import SqlFailure, map_sqlite_error
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


def execute_bundle(bundle: Mapping[str, Any], input_scope: Mapping[str, Any], driver: Driver) -> Any:
    """Execute a read/exec SqlBundle end-to-end (native read-graph walker; no bc run_behavior).

    The SAME code path a consumer runtime follows: it consumes ONLY the serialized bundle + bc
    runtime-core, never re-running litedbmodel's Backend-Compile. A read bundle carries a
    ``readGraph`` (the surrogate IR + static statements): bc drives map/Φ/wiring and the makeSQL
    handler renders + executes each node. Returns the component's Φ output — byte-identical to the
    TS ``executeBundle``.
    """
    read_graph = bundle.get("readGraph")
    if read_graph is None:
        raise ValueError(
            f"scp runtime: bundle '{bundle.get('name')}' carries no read graph "
            "(single-statement writes ride execute_transaction_bundle / the write path)"
        )
    return execute_read_graph(read_graph, input_scope, driver)


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


def _exec_statement(op: Mapping[str, Any], scope: Mapping[str, Any], dialect_name: str, driver: Driver):
    """Render + execute one tx statement; return (rows, changes). Mirrors TS execStatement."""
    rendered = _render_tx_statement(op, scope, dialect_name)
    sql = rendered["sql"]
    stmt = driver.prepare(sql)
    has_return = bool(_SELECT_RE.search(sql[:8])) or bool(_RETURNING_RE.search(sql))
    if has_return:
        rows = stmt.all(rendered["params"])
        return rows, len(rows)
    info = stmt.run(rendered["params"])
    return [], info.changes


def execute_transaction_bundle(bundle: Mapping[str, Any], input_scope: Mapping[str, Any], driver: Driver) -> Dict[str, Any]:
    """Execute a SqlBundle's derived transaction plan as ONE real transaction (gate-first).

    Byte-for-byte port of the TS ``executeTransactionBundle`` → ``executeTransaction`` (spec §6):
    statements run in the plan's fixed order (requires → idempotency → unique → body → derive →
    edges → emits, topo-ordered for composite DAGs); a failing gate ROLLBACKs and the tail never
    executes. On success COMMITs and returns the ``$.entity`` RETURNING row. A short-circuit returns
    ``committed:false`` (a legitimate gate outcome), NOT a raised failure; a driver failure ROLLBACKs
    then raises a mapped SqlFailure.
    """
    plan = bundle.get("transaction")
    if plan is None:
        raise ValueError("scp write: this bundle carries no transaction plan (not a write-time-relations Command bundle)")
    dialect_name = bundle["dialect"]

    driver.prepare("BEGIN").run([])
    executed: List[str] = []
    scope: Dict[str, Any] = dict(input_scope)
    entity: Optional[Dict[str, Any]] = None
    # Batch mode (createMany/updateMany/deleteMany): a gate-free, ref-free plan (entityFrom is null,
    # every statement a plain body) — accumulate each body statement's RETURNING rows in order.
    is_batch = plan.get("entityFrom") is None and all(
        s.get("gate") is None and s.get("binds") is None and s.get("role") == "body" for s in plan["statements"]
    )
    returned_rows: List[List[Dict[str, Any]]] = []

    try:
        for stmt in plan["statements"]:
            rows, changes = _exec_statement(stmt["op"], scope, dialect_name, driver)
            executed.append(stmt["id"])

            gate = stmt.get("gate")
            if gate is not None:
                reason = _gate_short_circuit(gate, rows, changes)
                if reason is not None:
                    driver.prepare("ROLLBACK").run([])
                    return {
                        "committed": False,
                        "shortCircuit": {"statementId": stmt["id"], "reason": reason},
                        "entity": None,
                        "executed": executed,
                    }

            if stmt["id"] == plan.get("entityFrom"):
                entity = rows[0] if rows else None
                if entity is not None:
                    scope[ENTITY_ROOT] = entity

            binds = stmt.get("binds")
            if binds is not None and rows:
                scope[binds] = rows[0]

            if is_batch and stmt.get("role") == "body" and rows:
                returned_rows.append(rows)

        driver.prepare("COMMIT").run([])
        out: Dict[str, Any] = {"committed": True, "entity": entity, "executed": executed}
        if returned_rows:
            out["returnedRows"] = returned_rows
        return out
    except SqlFailure:
        _safe_rollback(driver)
        raise
    except Exception as e:
        _safe_rollback(driver)
        raise map_sqlite_error(e)


def _safe_rollback(driver: Driver) -> None:
    try:
        driver.prepare("ROLLBACK").run([])
    except Exception:
        pass  # best-effort; the original failure is surfaced by the caller


# ── Dialect primitive (render axis + dialect suite) ────────────────────────────


def order_by_nulls(expr: str, direction: str, nulls: str, dialect_name: str) -> str:
    """The dialect NULLS-ordering primitive (native for PG/SQLite, IS NULL for MySQL)."""
    return dialect_for(dialect_name).order_by_nulls(expr, direction, nulls)
