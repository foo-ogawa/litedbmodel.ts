"""litedbmodel v2 SCP — the STATIC, PORTABLE makeSQL bundle RUNTIME (Python port, epic #43/#45).

Byte-for-byte port of the TS ``src/scp/makesql/static-bundle.ts`` + ``makesql.ts`` + ``handler.ts``
runtime halves — the SOLE makeSQL read/render path. It consumes the PRE-COMPILED, portable
artifacts the corpus ships (a read ``ReadGraph`` = a bc ``ComponentGraphIR`` of ``__makeSqlNode``
surrogate nodes + per-node STATIC statement templates), and EXECUTES them via the shared
``behavior-contracts`` core (``run_behavior`` drives map / Φ-merge / wiring; ``evaluate_expression``
resolves the deferred value-specs + skip). This module re-implements NO generic evaluator and does
NO SQL re-derivation — every statement's ``sql`` is fixed text already; the runtime only evaluates
its deferred params + skip, resolves the WHERE connector from the present set, assembles + renders
placeholders, and binds.

A statement template (``StaticStatement``) is ``{sql, params, skip?, whereFragment?}``:
  - ``sql``           — complete tuned dialect text (``?`` placeholders), value-independent.
  - ``params``        — deferred value-specs = closed-set bc Expression IR, 1:1 with the top ``?``.
  - ``skip``          — optional bc presence expression; truthy ⇒ the whole statement drops.
  - ``whereFragment`` — a bare predicate body; the runtime prepends `` WHERE ``/`` AND `` from the
                        present set (a skipped earlier fragment never leaves a dangling connector).

An IN-list value-spec is the marker ``{"__jsonArray": <spec>, "dialect": <d>}``: postgres binds the
array as-is (a ``text[]`` param); mysql/sqlite JSON-encode it to a single param (server-side
expansion). This mirrors the TS ``evalSpec``.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Mapping, Sequence

from behavior_contracts import evaluate_expression, run_behavior

from .dialect import Dialect, dialect_for
from .driver import Driver
from .errors import SqlFailure, map_sqlite_error

# The synthetic port that carries a SQL node's render scope (mirrors TS SCOPE_PORT).
SCOPE_PORT = "__scope"
# The makeSQL catalog leaf name every rewritten SQL node references (mirrors TS NODE_COMPONENT).
NODE_COMPONENT = "__makeSqlNode"

_RETURNING_RE = re.compile(r"\breturning\b", re.IGNORECASE)


# ── Value normalization at the SQL boundary (mirrors TS toDriverParam) ─────────


def _to_driver_param(v: Any) -> Any:
    """Convert a bc-evaluated value to a driver-bindable value.

    bc-Python evaluates integers to a plain unbounded ``int`` (no bigint type), so no narrowing is
    needed; a bool is left as-is (sqlite3 binds it 0/1, matching better-sqlite3). This matches the
    TS ``toDriverParam`` for every value in the corpus (all within the JS safe-integer range).
    """
    return v


# ── makeSQL assembly (port of makesql.ts assembleMakeSQL / composeMakeSQL) ─────


def _is_make_sql(p: Any) -> bool:
    """A nested-makeSQL param has a string ``sql`` + a list ``params`` (mirrors TS isMakeSQL)."""
    return isinstance(p, dict) and isinstance(p.get("sql"), str) and isinstance(p.get("params"), list)


def assemble_make_sql(node: Mapping[str, Any]) -> Dict[str, Any]:
    """Assemble one makeSQL ``{sql, params, skip?}`` → ``{sql, params}`` (splice nested, drop skip).

    Byte-for-byte port of the TS ``assembleMakeSQL``: split the literal ``sql`` on ``?`` and
    interleave each param — a bound value emits a single ``?`` + its value; a nested makeSQL splices
    its assembled ``sql`` + flows its assembled params (recursively).
    """
    if node.get("skip") is True:
        return {"sql": "", "params": []}
    sql_text = node["sql"]
    node_params = node["params"]
    chunks = sql_text.split("?")
    if len(chunks) - 1 != len(node_params):
        raise ValueError(
            f"makeSQL placeholder/param mismatch: {len(chunks) - 1} '?' vs {len(node_params)} params "
            f"in {json.dumps(sql_text)}"
        )
    sql = chunks[0]
    params: List[Any] = []
    for i, p in enumerate(node_params):
        if _is_make_sql(p):
            inner = assemble_make_sql(p)
            sql += inner["sql"] + chunks[i + 1]
            params.extend(inner["params"])
        else:
            sql += "?" + chunks[i + 1]
            params.append(p)
    return {"sql": sql, "params": params}


def compose_make_sql(nodes: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """Concatenate the assembled sql + params of every PRESENT makeSQL node (mirrors TS composeMakeSQL)."""
    sql = ""
    params: List[Any] = []
    for node in nodes:
        r = assemble_make_sql(node)
        sql += r["sql"]
        params.extend(r["params"])
    return {"sql": sql, "params": params}


# ── Dialect placeholder render (port of handler.ts renderPlaceholders) ─────────


def render_placeholders(sql: str, dialect_name: str) -> str:
    """Render ``?`` → the dialect placeholder form: PG ``$N`` (quote-aware), MySQL/SQLite keep ``?``.

    Byte-for-byte port of the TS ``renderPlaceholders``: PostgreSQL rewrites each ``?`` to ``$1,
    $2, …`` left-to-right, skipping any ``?`` inside a single-quoted string literal. MySQL/SQLite
    leave the text unchanged.
    """
    if dialect_name != "postgres":
        return sql
    out: List[str] = []
    index = 0
    in_string = False
    for ch in sql:
        if in_string:
            out.append(ch)
            if ch == "'":
                in_string = False
        elif ch == "'":
            out.append(ch)
            in_string = True
        elif ch == "?":
            index += 1
            out.append(f"${index}")
        else:
            out.append(ch)
    return "".join(out)


# ── Deferred value-spec evaluation (port of static-bundle.ts evalSpec) ─────────


def _eval_spec(spec: Any, scope: Mapping[str, Any], dialect_name: str) -> Any:
    """Evaluate one deferred value-spec against the scope (handling the JSON-array marker).

    A ``{"__jsonArray": <spec>, "dialect": <d>}`` marker JSON-encodes the evaluated array as ONE
    param for mysql/sqlite (server-side ``JSON_TABLE``/``json_each`` expansion); postgres keeps the
    array as-is (bound as a ``text[]`` param). Everything else is a plain bc Expression IR value.
    """
    if isinstance(spec, dict) and set(spec.keys()) >= {"__jsonArray"} and "__jsonArray" in spec:
        arr = evaluate_expression(spec["__jsonArray"], scope)
        if not isinstance(arr, list):
            raise ValueError("static-bundle: IN-list value-spec did not evaluate to an array")
        if spec.get("dialect") == "postgres":
            return [_to_driver_param(e) for e in arr]
        # Compact separators to match the TS JSON.stringify byte form (`[1,2]`, no spaces).
        return json.dumps([_to_driver_param(e) for e in arr], separators=(",", ":"), ensure_ascii=False)
    return _to_driver_param(evaluate_expression(spec, scope))


# ── Deferred PG array-cast resolution (#46 — mirrors compile-relation.ts) ──────

# The DEFERRED PG array-cast token: a placeholder in the STATIC SQL where the `= ANY(?::<T>[])`
# element type is unknown at symbolic compile (a schema-less `whereIn`). Resolved at render from
# the BOUND array via infer_pg_array_type — the same render-layer step as `?`→`$N`.
PG_ARRAY_CAST_TOKEN = "@@PG_ARRAY_CAST@@"


def infer_pg_array_type(values: Sequence[Any], sql_cast: Any = None) -> str:
    """Port of the ORIGINAL ``inferPgArrayType`` (v1 ``LazyRelation``): sql_cast wins, else the
    element type is inferred from the sample values. ``bool`` is checked before ``int`` because
    ``bool`` is an ``int`` subclass in Python."""
    if sql_cast:
        return f"{sql_cast}[]"
    if len(values) == 0:
        return "text[]"
    sample = values[0]
    if isinstance(sample, bool):
        return "boolean[]"
    if isinstance(sample, int):
        return "int[]"
    if isinstance(sample, float):
        return "numeric[]"
    return "text[]"


def resolve_pg_array_cast(sql: str, values: Sequence[Any]) -> str:
    """Resolve the FIRST unresolved PG array-cast token to the element type inferred from
    ``values`` (mirrors TS ``resolvePgArrayCast``). SQL with no token is unchanged."""
    at = sql.find(PG_ARRAY_CAST_TOKEN)
    if at < 0:
        return sql
    return sql[:at] + infer_pg_array_type(values) + sql[at + len(PG_ARRAY_CAST_TOKEN):]


# ── Statement-list render (port of static-bundle.ts renderStatements) ──────────


def render_statements(
    statements: Sequence[Mapping[str, Any]],
    dialect_name: str,
    scope: Mapping[str, Any],
) -> Dict[str, Any]:
    """Evaluate a list of statement templates against a scope → final ``{sql, params}``.

    Byte-for-byte port of the TS ``renderStatements``: drop skipped statements (skip truthy),
    resolve each surviving WHERE-fragment's `` WHERE ``/`` AND `` connector from the present set,
    resolve any deferred PG array cast from the bound array, build concrete makeSQL nodes, then
    compose + render placeholders to the dialect form.
    """
    nodes: List[Dict[str, Any]] = []
    where_seen = False
    for stmt in statements:
        if stmt.get("skip") is not None:
            drop = evaluate_expression(stmt["skip"], scope)
            if drop is not None and drop is not False:
                continue
        sql = stmt["sql"]
        if stmt.get("whereFragment") is True:
            sql = (" AND " if where_seen else " WHERE ") + stmt["sql"]
            where_seen = True
        params = [_eval_spec(p, scope, dialect_name) for p in stmt["params"]]
        # Resolve any deferred PG array cast (#46) from the bound array param, left-to-right —
        # each postgres __jsonArray param resolves exactly one cast token in order.
        if dialect_name == "postgres":
            for p in params:
                if not isinstance(p, list):
                    continue
                if PG_ARRAY_CAST_TOKEN not in sql:
                    break
                sql = resolve_pg_array_cast(sql, p)
        nodes.append({"sql": sql, "params": params})
    assembled = compose_make_sql(nodes)
    return {"sql": render_placeholders(assembled["sql"], dialect_name), "params": assembled["params"]}


# ── Input normalization (SSoT-driven — mirrors TS normalizeInput) ──────────────


def _normalize_optional_heads(optional_heads: Sequence[str], input_scope: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize omitted OPTIONAL heads to present-as-null (absent-key SKIP; mirrors TS)."""
    out = dict(input_scope)
    for head in optional_heads:
        if head not in out:
            out[head] = None
    return out


# ── ReadGraph render axis (port of static-bundle.ts renderReadPrimary) ─────────


def render_read_primary(graph: Mapping[str, Any], input_scope: Mapping[str, Any]) -> Dict[str, Any]:
    """Render the PRIMARY read node's statements of a ReadGraph → dialect SQL text + params.

    Byte-for-byte port of the TS ``renderReadPrimary``: the primary node is the first body node in
    the surrogate IR order (map nodes reference it). Optional heads are normalized to present-as-null
    first (absent-key SKIP), so an omitted optional head renders the SAME text a runtime would.
    """
    statements_by_id: Mapping[str, Any] = graph["statementsById"]
    ids = set(statements_by_id.keys())
    body = graph["ir"]["components"][0]["body"]
    body_ids = [n["id"] for n in body if n["id"] in ids]
    if not body_ids:
        raise ValueError("static-bundle: read graph has no primary node to render")
    primary_id = body_ids[0]
    scope = _normalize_optional_heads(list(graph.get("optionalHeads", [])), input_scope)
    return render_statements(statements_by_id[primary_id], graph["dialect"], scope)


# ── ReadGraph execution (port of static-bundle.ts executeReadGraph) ────────────


def execute_read_graph(graph: Mapping[str, Any], input_scope: Mapping[str, Any], driver: Driver) -> Any:
    """Execute a compiled ReadGraph via bc ``run_behavior`` + a makeSQL handler.

    Byte-for-byte port of the TS ``executeReadGraph``: bc drives map iteration / wire binding /
    Φ output; the handler renders each node's static statements against the evaluated ``__scope``
    and runs REAL SQL. Returns the component's Φ output. This is the design's "bc composes,
    makeSQL executes" — the SAME path the TS runtime follows.
    """
    statements_by_id: Mapping[str, Any] = graph["statementsById"]
    dialect_name: str = graph["dialect"]

    def handle(ports: Mapping[str, Any], ctx: Mapping[str, Any]) -> Mapping[str, Any]:
        node_id = ctx["nodeId"]
        stmts = statements_by_id.get(node_id)
        if stmts is None:
            return {"error": f"static-bundle: no statements for node '{node_id}'"}
        scope = ports.get(SCOPE_PORT)
        if not isinstance(scope, dict):
            return {"error": f"static-bundle: node '{node_id}' surrogate scope did not evaluate to an object"}
        rendered = render_statements(stmts, dialect_name, scope)
        try:
            rows = driver.prepare(rendered["sql"]).all(rendered["params"])
            return {"ok": rows}
        except Exception as e:  # driver error → mapped {error}
            return {"error": str(map_sqlite_error(e))}

    handlers = {NODE_COMPONENT: handle}
    ir = graph["ir"]
    normalized = _normalize_optional_heads(list(graph.get("optionalHeads", [])), input_scope)
    try:
        return run_behavior(ir, handlers, normalized, graph["name"])
    except Exception as e:
        raise _re_error_to_sql_failure(e)


def _re_error_to_sql_failure(e: Exception) -> Exception:
    """Re-surface a structured SqlFailure from a bc OP_FAILED whose message embeds a SQLITE_ code."""
    if isinstance(e, SqlFailure):
        return e
    message = str(e)
    m = re.search(r"(SQLITE_[A-Z_]+)", message)
    if m:
        return map_sqlite_error(Exception(message))
    return e
