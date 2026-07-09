"""litedbmodel v2 SCP — fragment-tree render + param assembly (Python port of ``src/scp/render.ts``).

A byte-for-byte port of the NORMATIVE dynamic-expansion reference
(`docs/proposal/sql-dynamic-expansion-spec.md`). Given a §8 CompiledOperation and a bound input
scope, it deterministically produces the final SQL text (`?` placeholders, or `$N` for Postgres
via the dialect's final pass) + the flat params list — reproducing the TS `renderOperation`
exactly. The four moving parts:

  §2 SKIP → fragment existence: a fragment with `when` is present iff `when` evaluates to a
      present (non-null / non-false) binding; an absent fragment contributes NO SQL and NO params.
  §3 empty-WHERE degeneration: if no fragment is present the whole ` WHERE …` splice collapses.
  §4 AND/OR structure + parenthesization: a nested tree renders `(… <connector> …)`.
  §5 IN-list array expansion: an `expand` slot turns its `(?)` into `(?, ?, …)` per element (0
      elements → the `1 = 0` always-false degeneration).

The CLOSED Expression-IR evaluation (ref/refOpt/coalesce/eq/…) is delegated to
behavior-contracts (`evaluate_expression`) — this module re-implements NO generic evaluator,
exactly like the TS reference imports `evaluateExpression` from `behavior-contracts`.
"""

from __future__ import annotations

from typing import Any, List, Mapping

from behavior_contracts import evaluate_expression

from .dialect import Dialect, SQLITE

# The literal `{where}` splice marker inside CompiledOperation.sql (spec §8 / ir.ts WHERE_SLOT).
WHERE_SLOT = "{where}"


class RenderedSql:
    """The result of rendering: final SQL text + flat params (1:1 with `?`)."""

    __slots__ = ("sql", "params")

    def __init__(self, sql: str, params: List[Any]) -> None:
        self.sql = sql
        self.params = params


def _is_tree(node: Mapping[str, Any]) -> bool:
    """A fragment tree carries a `connector`; a leaf fragment carries `sql`."""
    return "connector" in node


def _fragment_present(fragment: Mapping[str, Any], scope: Mapping[str, Any]) -> bool:
    """SKIP existence (spec §2): present iff `always`, or `when` evaluates truthy-present.

    `null` and `false` are absent; everything else (including `0`, `""`) is present — mirroring
    the TS `fragmentPresent`. `when` is an explicit presence/bool Expression evaluated fail-closed
    by bc's `evaluate_expression`. Neither `always` nor `when` set ⇒ fail-closed absent.
    """
    if fragment.get("always") is True:
        return True
    if "when" not in fragment:
        return False  # fail-closed: neither always nor when
    v = evaluate_expression(fragment["when"], scope)
    return v is not None and v is not False


def _render_fragment(fragment: Mapping[str, Any], scope: Mapping[str, Any], params: List[Any]) -> str:
    """Render one leaf fragment's SQL + params into the accumulator (IN-list expansion, §5)."""
    expand = fragment.get("expand")
    frag_params = fragment["params"]
    if expand is None:
        for slot in frag_params:
            params.append(evaluate_expression(slot, scope))
        return fragment["sql"]

    # IN-list expansion. Evaluate all slots; the `expand` slot must be an array.
    sql = fragment["sql"]
    for i, slot in enumerate(frag_params):
        v = evaluate_expression(slot, scope)
        if i == expand:
            if not isinstance(v, list):
                kind = "null" if v is None else type(v).__name__
                raise ValueError(
                    f"IN-list expansion slot {i} did not bind to an array (got {kind})"
                )
            if len(v) == 0:
                # Empty-array degeneration (spec §5): `col IN (?)` collapses to the always-false
                # sentinel `1 = 0`. No params pushed for this slot. Byte-identical to TS/v1.
                sql = "1 = 0"
            else:
                # Replace the single `(?)` with `(?, ?, …)`; push each element.
                sql = sql.replace("(?)", "(" + ", ".join("?" for _ in v) + ")", 1)
                for el in v:
                    params.append(el)
        else:
            params.append(v)
    return sql


def _render_tree(tree: Mapping[str, Any], scope: Mapping[str, Any], params: List[Any]) -> str:
    """Render a fragment tree into a WHERE body (no leading ` WHERE `). Empty when none present (§3)."""
    parts: List[str] = []
    for node in tree["fragments"]:
        if _is_tree(node):
            inner = _render_tree(node, scope, params)
            if inner != "":
                parts.append(f"({inner})")
        elif _fragment_present(node, scope):
            parts.append(_render_fragment(node, scope, params))
    if not parts:
        return ""
    return (f" {tree['connector']} ").join(parts)


def _count_placeholders(sql: str) -> int:
    """Count `?` placeholders in a static SQL segment (no fragment markers present)."""
    return sql.count("?")


def render_operation(
    operation: Mapping[str, Any],
    input_scope: Mapping[str, Any],
    dialect: Dialect = SQLITE,
) -> RenderedSql:
    """Render a §8 CompiledOperation to final SQL + params for a bound input scope.

    Byte-for-byte port of the TS `renderOperation`. Param order matches SQL text order (spec §6):
    pre-WHERE statics, then fragment params in tree order, then post-WHERE statics. A single
    left-to-right walk of the spliced SQL yields the canonical placeholder order; the dialect's
    `finalize_placeholders` applies the `?`→`$N` pass ONCE over the fully-assembled text.
    """
    params: List[Any] = []
    op_sql = operation["sql"]
    op_params = operation["params"]
    marker_idx = op_sql.find(WHERE_SLOT)

    if marker_idx == -1:
        # No dynamic WHERE: all params are static, in position order.
        for slot in op_params:
            params.append(evaluate_expression(slot, input_scope))
        return RenderedSql(dialect.finalize_placeholders(op_sql), params)

    before = op_sql[:marker_idx]
    after = op_sql[marker_idx + len(WHERE_SLOT):]

    # Static params are partitioned by whether their `?` sits before or after the marker.
    before_q = _count_placeholders(before)
    pre_statics = op_params[:before_q]
    post_statics = op_params[before_q:]

    for slot in pre_statics:
        params.append(evaluate_expression(slot, input_scope))

    where_sql = ""
    where_tree = operation.get("where")
    if where_tree is not None:
        body = _render_tree(where_tree, input_scope, params)
        if body != "":
            where_sql = f" WHERE {body}"  # degeneration §3: drop keyword when empty

    for slot in post_statics:
        params.append(evaluate_expression(slot, input_scope))

    return RenderedSql(dialect.finalize_placeholders(before + where_sql + after), params)
