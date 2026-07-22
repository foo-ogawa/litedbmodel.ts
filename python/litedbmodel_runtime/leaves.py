"""litedbmodel v2 SCP — the op-INDEPENDENT leaf transport (#141), Python port of ``src/scp/leaves.ts``.

The three op-agnostic (NOT per-op) leaves the bc python emitter's ir-exec runner (``run_behavior``)
calls by catalog name via boundary injection (``bind(handlers)``). Each is a bc handler
(``handler(ports, ctx) -> {"ok": value} | {"error": str}``) — the SAME contract the rust/go
typed-native runners call positionally (``rust/litedbmodel_runtime/src/leaves.rs``
``execute_sql``/``pluck_keys``/``group_children``), reproduced for the python literal (ir-exec) path
(epic #123: ts/go/rust = native de-box; py/php = literal). Python's native value model is the plain
``dict`` record, so there is NO ``WireValue`` conversion — the wire IS the dict.

  - ``executeSQL`` — the SOLE SQL transport: render ``?``→dialect placeholders, bind params (an array
    param — a relation key set from ``pluck`` or a batch record set — rides per dialect: sqlite/mysql
    JSON-encode it for ``json_each``/``JSON_TABLE``, postgres binds the array as-is), and run it through
    the runtime's central execute/run seam (:func:`exec_context.execute` / :func:`exec_context.run`) on
    the bound driver — the ONLY driver contact. A non-returning write returns a one-row
    ``[{changes, lastInsertRowid}]`` summary so the leaf output shape is uniform (a list of rows).
  - ``pluck`` — rows + the ordered key-column TUPLE → the deduped, non-null batch key set (single-key →
    a flat scalar array; composite → an array-of-tuples). Delegates the dedupe to the shared grouping
    core (:func:`grouping.dedupe_key_tuples`) — the SAME SSoT the runtime relation path uses.
  - ``group`` — parents + flat children → each parent with its children nested under ``into`` per
    cardinality. Delegates to the shared grouping core (:func:`grouping.group_by_key` /
    :func:`grouping.attach_to_parent`) — the SAME SSoT, no duplicated grouping.

The leaf is injected driver-bound (a closure over the :class:`ExecutionContext` + dialect) rather than
resolving a thread-local ambient driver: the bc python boundary is ``bind(handlers)``, so the transport
is handed in directly (the rust/go typed-native path resolves an ambient driver because the generated
code calls the leaf with no driver arg — the python ir-exec path injects it).
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Mapping, Sequence, Union

from .driver import Driver
from .errors import SqlFailure
from .exec_context import (
    READ_INTENT,
    WRITE_INTENT,
    ExecutionContext,
    as_context,
)
from .exec_context import execute as seam_execute
from .exec_context import run as seam_run
from .grouping import attach_to_parent, dedupe_key_tuples, group_by_key
from .static_bundle import render_placeholders

__all__ = ["make_handlers"]

# The bc handler outcome contract (behavior.py): a handler returns ``{"ok": value}`` on success or
# ``{"error": message}`` on a fail-closed transport failure (``run_behavior`` propagates it).
Outcome = Mapping[str, Any]
Handler = Callable[[Mapping[str, Any], Mapping[str, Any]], Outcome]


def _bind_params(params: Sequence[Any], dialect: str) -> List[Any]:
    """Bind a leaf's resolved param list for the driver per dialect (mirror of the rust driver's
    ``WireValue`` → param encoding + ``relation.py`` ``_bind_keys``). An array param (a relation key set
    from ``pluck`` or a batch record set) is server-side-expanded: sqlite/mysql JSON-encode it as ONE
    scalar string (``json_each``/``JSON_TABLE``); postgres binds the array as-is (native ``= ANY($1)`` /
    ``unnest``). A scalar param binds unchanged."""
    if dialect == "postgres":
        return list(params)
    return [json.dumps(p, separators=(",", ":"), ensure_ascii=False) if isinstance(p, list) else p for p in params]


def make_handlers(driver_or_ctx: Union[Driver, ExecutionContext], dialect: str) -> Dict[str, Handler]:
    """The op-agnostic leaf transport handlers (``executeSQL``/``pluck``/``group``), bound to a driver
    (or an :class:`ExecutionContext`) + its ``dialect``, ready to inject into a bc-generated python
    module's ``bind(handlers)``. Every SQL access funnels through the central execute/run seam over the
    bound driver — the SAME seam the runtime read/relation path uses (middleware-visible, N+1-free)."""
    ctx = as_context(driver_or_ctx)

    def execute_sql(ports: Mapping[str, Any], _ctx: Mapping[str, Any]) -> Outcome:
        sql = render_placeholders(ports["sql"], dialect)
        params = _bind_params(ports["params"], dialect)
        try:
            if ports.get("write") and not ports.get("returning"):
                info = seam_run(ctx, sql, params, WRITE_INTENT)
                # The affected-write summary row (uniform ``items`` output shape — TS ``writeSummary``).
                return {"ok": [{"changes": info.changes, "lastInsertRowid": info.last_insert_rowid}]}
            return {"ok": seam_execute(ctx, sql, params, READ_INTENT)}
        except SqlFailure as e:
            return {"error": e.message}

    def pluck(ports: Mapping[str, Any], _ctx: Mapping[str, Any]) -> Outcome:
        col: Sequence[str] = ports["col"]
        tuples = dedupe_key_tuples(ports["rows"], col)
        # single-key → a flat scalar key array (json_each scalar ``value``); composite → an
        # array-of-tuples (json_each per-ordinal ``$[i]``) — the SAME shape ``relation.py`` binds.
        keys = [t[0] for t in tuples] if len(col) == 1 else [list(t) for t in tuples]
        return {"ok": keys}

    def group(ports: Mapping[str, Any], _ctx: Mapping[str, Any]) -> Outcome:
        into = ports["into"]
        single = ports["single"]
        pk: Sequence[str] = ports["pk"]
        by_key = group_by_key(ports["children"], ports["fk"])
        # {...par, [into]: nested}: shallow-copy each parent (the input is not mutated — TS spread).
        out = [{**par, into: attach_to_parent(par, pk, by_key, single)} for par in ports["parents"]]
        return {"ok": out}

    return {"executeSQL": execute_sql, "pluck": pluck, "group": group}
