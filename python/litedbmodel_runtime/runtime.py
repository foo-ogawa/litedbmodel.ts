"""litedbmodel SCP Python runtime — the §8 bundle interpreter (WS7b, #31).

The thin Python leg of the multi-language SCP runtime. It consumes the language-neutral §8
published :class:`SqlBundle` (pure JSON: `sql` + fragment tree + closed-set Expression-IR param
slots + optional transaction plan, dialect-tagged) and executes it against a SQL driver —
SEMANTICS-IDENTICAL to the TS reference (``src/scp/runtime.ts`` + ``write-runtime.ts``).

It re-implements NO generic execution/expression evaluation: bc's ``run_behavior`` owns the
plan/map/wire/output orchestration and its ``evaluate_expression`` owns the CLOSED Expression-IR
(ref/refOpt/coalesce/eq/…, the SKIP guards + param slots). This module adds ONLY the SQL-backend
concerns (spec §11): render the pre-compiled op per dialect (``render_operation``) → bind params
→ execute REAL SQL → row→result assembly; and, for a Command bundle, the gate-first write
transaction (``execute_transaction_bundle``). This mirrors the TS runtime's division of labor
exactly (bc-core + a SQL handler).
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Mapping, Optional

from behavior_contracts import run_behavior

from .dialect import Dialect, dialect_for
from .driver import Driver
from .errors import SqlFailure, map_sqlite_error
from .render import render_operation

# The synthetic port that carries a SQL node's render scope (mirrors TS SCOPE_PORT).
SCOPE_PORT = "__scope"
# The reserved binding the body write's RETURNING row is exposed under (mirrors TS ENTITY_ROOT).
ENTITY_ROOT = "__entity"

_RETURNING_RE = re.compile(r"\breturning\b", re.IGNORECASE)


# ── Value normalization at the SQL boundary ───────────────────────────────────


def _to_driver_param(v: Any) -> Any:
    """Convert a rendered param to a driver-bindable value (mirrors TS toDriverParam).

    bc evaluates integers to plain Python `int` (not bigint), so no bigint narrowing is needed;
    a bool is left as-is (sqlite3 binds it as 0/1, matching better-sqlite3). An object/dict is an
    emit payload (`{obj:…}`) serialized to compact JSON for a text column — byte-identical to the
    TS `JSON.stringify` (no spaces). Lists are IN-list elements handled during expansion, never
    reaching here as a single param on the write path; pass through.
    """
    if isinstance(v, bool):
        return v
    if isinstance(v, dict):
        # Compact separators to match JS JSON.stringify (`{"a":1,"b":2}`, no spaces).
        return json.dumps(v, separators=(",", ":"), ensure_ascii=False)
    return v


def _is_return_stmt(op: Mapping[str, Any], sql: str) -> bool:
    """A SELECT or a write with RETURNING yields rows (mirrors TS hasReturn)."""
    return op.get("component") == "Select" or bool(_RETURNING_RE.search(sql))


# ── Handler: render → execute → assembly ──────────────────────────────────────


def _execute_rendered(
    driver: Driver,
    op: Mapping[str, Any],
    scope: Mapping[str, Any],
    dialect: Dialect,
) -> Mapping[str, Any]:
    """Render + execute one op; return a bc ExecOutcome (`{ok}` / `{error}`). Mirrors TS."""
    rendered = render_operation(op, scope, dialect)
    params = [_to_driver_param(p) for p in rendered.params]
    try:
        stmt = driver.prepare(rendered.sql)
    except Exception as e:  # driver error at prepare → mapped {error}
        return {"error": map_sqlite_error(e).args[0] if e.args else str(e)}
    try:
        if _is_return_stmt(op, rendered.sql):
            rows = stmt.all(params)
            return {"ok": rows}
        info = stmt.run(params)
        # A non-returning write returns the RETURNING-less single-row summary shape.
        return {"ok": [{"changes": info.changes, "lastInsertRowid": info.last_insert_rowid}]}
    except Exception as e:
        return {"error": str(map_sqlite_error(e))}


def _build_handlers(
    driver: Driver,
    operations: Mapping[str, Any],
    dialect: Dialect,
):
    """Build the SQL handler registry: one handler per SQL Catalog name (spec §11 item 4)."""

    def handle(ports: Mapping[str, Any], ctx: Mapping[str, Any]) -> Mapping[str, Any]:
        node_id = ctx["nodeId"]
        op = operations.get(node_id)
        if op is None:
            return {"error": f"scp runtime: no compiled operation for node '{node_id}' ({ctx.get('component')})"}
        scope = ports.get(SCOPE_PORT)
        if not isinstance(scope, dict):
            return {"error": f"scp runtime: node '{node_id}' surrogate scope did not evaluate to an object"}
        return _execute_rendered(driver, op, scope, dialect)

    return {"Select": handle, "Insert": handle, "Update": handle, "Delete": handle}


# ── Input normalization (schema-driven — SSoT, no ad-hoc code default) ─────────


def _normalize_input(
    component: Mapping[str, Any],
    optional_heads: List[str],
    input_scope: Mapping[str, Any],
) -> Dict[str, Any]:
    """Normalize omitted OPTIONAL bindings to present-as-null (mirrors TS normalizeInput).

    "Optional" comes from the SSoT: EITHER the component's Input Port schema marks the port
    `required != True`, OR the head is listed in the bundle's `optionalHeads` (a SKIP-guarded /
    refOpt head the TS compile derived). A REQUIRED, non-optional missing head is left absent so a
    real wiring bug surfaces loudly as bc's UNKNOWN_BINDING — never silently defaulted.
    """
    out = dict(input_scope)
    for port, schema in component.get("inputPorts", {}).items():
        if schema.get("required") is not True and port not in out:
            out[port] = None
    for head in optional_heads:
        if head not in out:
            out[head] = None
    return out


# ── Public runtime entrypoints ────────────────────────────────────────────────


def render_operation_bundle(operation: Mapping[str, Any], scope: Mapping[str, Any], dialect_name: str) -> Dict[str, Any]:
    """Render a §8 CompiledOperation → {"sql", "params"} for a dialect name (render axis API)."""
    rendered = render_operation(operation, scope, dialect_for(dialect_name))
    return {"sql": rendered.sql, "params": rendered.params}


def execute_bundle(bundle: Mapping[str, Any], input_scope: Mapping[str, Any], driver: Driver) -> Any:
    """Execute a §8 read/exec SqlBundle end-to-end (bc run_behavior + SQL handlers).

    The SAME code path a consumer runtime follows: it consumes ONLY the serialized bundle + bc
    runtime-core, never re-running litedbmodel's Backend-Compile. Returns the component's `output`
    (Φ merge) with each SQL node's slot filled by its executed row list — byte-identical to the TS
    `executeBundle`.
    """
    surrogate = bundle["component"]
    ir = {
        "irVersion": bundle["irVersion"],
        "exprVersion": bundle["exprVersion"],
        "components": [surrogate],
    }
    handlers = _build_handlers(driver, bundle["operations"], dialect_for(bundle["dialect"]))
    normalized = _normalize_input(surrogate, list(bundle.get("optionalHeads", [])), input_scope)
    try:
        return run_behavior(ir, handlers, normalized, surrogate["name"])
    except Exception as e:
        raise _re_error_to_sql_failure(e)


# ── Write-time relations: gate-first transaction (spec §6 — port of write-runtime.ts) ──


def _gate_short_circuit(gate: str, rows: List[Dict[str, Any]], changes: int) -> Optional[str]:
    """Evaluate a gate rule on a statement result → short-circuit reason, or None to continue."""
    if gate == "existsElseRollback":
        return "requires_absent" if len(rows) == 0 else None
    if gate == "insertedElseRollback":
        return "unique_collision" if changes == 0 else None
    if gate == "insertedElseNoop":
        return "idempotent_duplicate" if changes == 0 else None
    raise ValueError(f"scp write: unknown gate rule '{gate}'")


def _exec_statement(
    driver: Driver,
    op: Mapping[str, Any],
    scope: Mapping[str, Any],
    dialect: Dialect,
):
    """Render + execute one tx statement; return (rows, changes). Mirrors TS execStatement."""
    rendered = render_operation(op, scope, dialect)
    params = [_to_driver_param(p) for p in rendered.params]
    stmt = driver.prepare(rendered.sql)
    if _is_return_stmt(op, rendered.sql):
        rows = stmt.all(params)
        return rows, len(rows)
    info = stmt.run(params)
    return [], info.changes


def execute_transaction_bundle(bundle: Mapping[str, Any], input_scope: Mapping[str, Any], driver: Driver) -> Dict[str, Any]:
    """Execute a §8 SqlBundle's derived transaction plan as ONE real transaction (gate-first).

    Byte-for-byte port of the TS `executeTransactionBundle` → `executeTransaction` (spec §6):
    statements run in the plan's fixed order (requires → idempotency → unique → body → derive →
    edges → emits); a failing gate ROLLBACKs and the tail never executes. On success COMMITs and
    returns the `$.entity` RETURNING row. A short-circuit returns `committed:false` (a legitimate
    gate outcome), NOT a raised failure; a driver failure ROLLBACKs then raises a mapped SqlFailure.
    """
    plan = bundle.get("transaction")
    if plan is None:
        raise ValueError("scp write: this bundle carries no transaction plan (not a write-time-relations Command bundle)")
    dialect = dialect_for(bundle["dialect"])

    driver.prepare("BEGIN").run([])
    executed: List[str] = []
    scope: Dict[str, Any] = dict(input_scope)
    entity: Optional[Dict[str, Any]] = None

    try:
        for stmt in plan["statements"]:
            rows, changes = _exec_statement(driver, stmt["op"], scope, dialect)
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

        driver.prepare("COMMIT").run([])
        result: Dict[str, Any] = {"committed": True, "entity": entity, "executed": executed}
        return result
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


# ── Error re-surfacing (mirrors TS reErrorToSqlFailure) ────────────────────────


def _re_error_to_sql_failure(e: Exception) -> Exception:
    """Re-surface a structured SqlFailure from a bc OP_FAILED whose message embeds a SQLITE_ code."""
    if isinstance(e, SqlFailure):
        return e
    message = str(e)
    m = re.search(r"(SQLITE_[A-Z_]+)", message)
    if m:
        return map_sqlite_error(Exception(message))
    return e
