"""litedbmodel v2 SCP — read-relation batch EXECUTION (Python port of ``src/scp/relation.ts``, #43).

Byte-for-byte port of the TS reference relation runtime: the STATIC pre-compiled batch op
(``bundle.relations[name]`` — pure JSON) is EXECUTED, never regenerated. A ``RelationOp`` carries
the batched child SELECT text with ONE ``?`` for the deduped-key array param; the runtime dedupes
the parent keys, resolves the deferred PG array cast from the REAL keys, renders ``?``→``$N``,
short-circuits an empty key set (NO query), runs the batch, groups the child rows by target key,
and distributes them onto the parents per cardinality (``hasMany`` → list, ``belongsTo``/``hasOne``
→ single or None). This is the SAME ``runRelationOp`` / ``distributeToParent`` / ``dedupeKeys`` the
TS eager path (``buildResultSet``) uses — the non-TS runtimes now reproduce it.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from .driver import Driver
from .static_bundle import PG_ARRAY_CAST_TOKEN, render_placeholders, resolve_pg_array_cast

__all__ = ["dedupe_keys", "run_relation_op", "distribute_to_parent", "read_bundle"]


def dedupe_keys(parents: Sequence[Mapping[str, Any]], parent_key: str) -> List[Any]:
    """The deduped, non-null parent-key values (insertion order preserved — deterministic).

    Byte-for-byte port of the TS ``dedupeKeys``: skip ``None`` keys, dedupe on the STRINGIFIED key
    (so ``1`` and ``"1"`` collapse exactly as TS ``String(v)``), preserve first-seen order.
    """
    seen: set[str] = set()
    out: List[Any] = []
    for p in parents:
        v = p.get(parent_key)
        if v is None:
            continue
        s = _stringify(v)
        if s in seen:
            continue
        seen.add(s)
        out.append(v)
    return out


def _stringify(v: Any) -> str:
    """Mirror TS ``String(v)`` for the key-identity used by dedupe + grouping (bool → 'true'/'false')."""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _bind_keys(op: Mapping[str, Any], keys: Sequence[Any]) -> Any:
    """Bind the deduped key set to the op's single array param per dialect (mirrors TS ``bindKeys``).

    PG binds the array verbatim (``= ANY(?::t[])`` — a native list param); MySQL/SQLite bind the
    JSON-encoded array string (server-side ``json_each``/``JSON_TABLE`` expansion). Compact JSON to
    match the TS ``JSON.stringify`` byte form.
    """
    if op["dialect"] == "postgres":
        return list(keys)
    return json.dumps(list(keys), separators=(",", ":"), ensure_ascii=False)


def run_relation_op(
    op: Mapping[str, Any],
    parents: Sequence[Mapping[str, Any]],
    driver: Driver,
) -> Dict[str, Any]:
    """Run ONE relation batch op for a set of parent rows (byte-for-byte port of TS ``runRelationOp``).

    Dedup the parent keys, resolve the deferred PG array cast from the REAL deduped keys BEFORE the
    ``?``→``$N`` render (PG only; MySQL/SQLite carry no cast token), render placeholders, then — on a
    NON-empty key set — execute the batch binding the keys as the SINGLE array param and group the
    child rows into ``{ str(target_key): [rows] }``. An EMPTY key set issues NO query (the correct
    empty-set behaviour), matching TS. Returns ``{sql, keys, batch}``.
    """
    keys = dedupe_keys(parents, op["parentKey"])
    batch: Dict[str, List[Dict[str, Any]]] = {}
    cast = resolve_pg_array_cast(op["sql"], keys) if op["dialect"] == "postgres" else op["sql"]
    sql = render_placeholders(cast, op["dialect"])
    if len(keys) == 0:
        return {"sql": sql, "keys": keys, "batch": batch}
    rows = driver.prepare(sql).all([_bind_keys(op, keys)])
    for row in rows:
        k = _stringify(row[op["targetKey"]])
        batch.setdefault(k, []).append(row)
    return {"sql": sql, "keys": keys, "batch": batch}


def distribute_to_parent(
    op: Mapping[str, Any],
    parent: Mapping[str, Any],
    batch: Mapping[str, List[Dict[str, Any]]],
) -> Union[List[Dict[str, Any]], Dict[str, Any], None]:
    """Distribute a resolved batch onto ONE parent per cardinality (port of TS ``distributeToParent``).

    ``hasMany`` → the child list (``[]`` when none); ``belongsTo``/``hasOne`` → the single child (or
    ``None``). Keyed by ``str(parent[parentKey])`` — the declared cardinality's empty representation.
    """
    key = parent.get(op["parentKey"])
    rows = None if key is None else batch.get(_stringify(key))
    if op["kind"] == "hasMany":
        return rows if rows is not None else []
    return rows[0] if rows else None


def read_bundle(
    bundle: Mapping[str, Any],
    input_scope: Mapping[str, Any],
    driver: Driver,
    with_names: Sequence[str],
) -> List[Dict[str, Any]]:
    """Run a READ bundle's primary row list, then batch-load + hydrate the selected relations.

    Mirrors the TS ``readBundle`` typed-object surface restricted to the declarative-select path
    (``buildResultSet`` with ``with``): the primary read output must be a bare row list; each named
    relation in ``with_names`` is batch-prefetched ONCE over the whole page (staged, no N+1) via the
    SAME ``run_relation_op`` and attached onto each parent as an own key. Independent sibling
    relations are naturally free of ordering: the batch is grouped-then-distributed by key, so the
    hydrated result is deterministic regardless of query-completion order (#40 parallel-safe).
    """
    from .runtime import execute_bundle

    out = execute_bundle(bundle, input_scope, driver)
    if not isinstance(out, list):
        raise ValueError(
            "scp read: the read behavior output is not a row list "
            f"(got {'null' if out is None else type(out).__name__}); the typed-object read surface "
            "expects a Select-shaped output"
        )
    rows: List[Dict[str, Any]] = [dict(r) for r in out]
    relations: Mapping[str, Any] = bundle.get("relations") or {}
    for name in with_names:
        op = relations.get(name)
        if op is None:
            raise ValueError(f"declarative select: relation '{name}' is not declared on this model")
        batch = run_relation_op(op, rows, driver)["batch"]
        for o in rows:
            o[name] = distribute_to_parent(op, o, batch)
    return rows
