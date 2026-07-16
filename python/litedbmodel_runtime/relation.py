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
from .exec_context import READ_INTENT, ExecutionContext, as_context, execute as seam_execute
from .static_bundle import PG_ARRAY_CAST_TOKEN, render_placeholders, resolve_pg_array_cast

__all__ = ["dedupe_keys", "run_relation_op", "distribute_to_parent", "read_bundle"]


def _parent_key_cols(op: Mapping[str, Any]) -> List[str]:
    """The ordered PARENT key columns (single-key → 1-element; composite → the tuple)."""
    pk = op.get("parentKeys")
    return list(pk) if pk is not None else [op["parentKey"]]


def _target_key_cols(op: Mapping[str, Any]) -> List[str]:
    """The ordered CHILD key columns (single-key → 1-element; composite → the tuple)."""
    tk = op.get("targetKeys")
    return list(tk) if tk is not None else [op["targetKey"]]


def _stringify(v: Any) -> str:
    """Mirror TS ``String(v)`` for the key-identity used by dedupe + grouping (bool → 'true'/'false')."""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _key_identity(values: Sequence[Any]) -> str:
    """The stringified key identity for dedupe/grouping (tuple → space-joined scalars, mirror of TS)."""
    return " ".join(_stringify(v) for v in values)


def dedupe_keys(parents: Sequence[Mapping[str, Any]], key_cols: Sequence[str]) -> List[List[Any]]:
    """The deduped, non-null parent-key TUPLES (insertion order preserved). Drop a tuple if ANY key
    column is None; dedupe on the stringified tuple identity. Port of TS ``dedupeKeys``."""
    seen: set[str] = set()
    out: List[List[Any]] = []
    for p in parents:
        tuple_ = [p.get(c) for c in key_cols]
        if any(v is None for v in tuple_):
            continue
        s = _key_identity(tuple_)
        if s in seen:
            continue
        seen.add(s)
        out.append(tuple_)
    return out


def _bind_keys(op: Mapping[str, Any], tuples: Sequence[Sequence[Any]]) -> List[Any]:
    """Bind the deduped keys to the op's params per dialect + arity (mirrors TS ``bindKeys``).

    Single-key: PG → ONE scalar list param; MySQL/SQLite → ONE JSON scalar-array string. Composite:
    PG → ONE list param PER key column (transposed tuples → ``unnest(?::t1[], ?::t2[])``);
    MySQL/SQLite → ONE JSON array-of-tuples string. Returns the positional param list.
    """
    composite = op.get("parentKeys") is not None
    if op["dialect"] == "postgres":
        if not composite:
            return [[t[0] for t in tuples]]  # ONE scalar array param
        n = len(_parent_key_cols(op))
        return [[t[col] for t in tuples] for col in range(n)]  # one array param per column
    payload = [list(t) for t in tuples] if composite else [t[0] for t in tuples]
    return [json.dumps(payload, separators=(",", ":"), ensure_ascii=False)]


def run_relation_op(
    op: Mapping[str, Any],
    parents: Sequence[Mapping[str, Any]],
    driver: Union[Driver, ExecutionContext],
) -> Dict[str, Any]:
    """Run ONE relation batch op for a set of parent rows (byte-for-byte port of TS ``runRelationOp``).

    Dedup the parent-key tuples, resolve the deferred PG array cast(s) from the REAL keys (one per
    key column for composite) BEFORE the ``?``→``$N`` render, render placeholders, then — on a
    NON-empty key set — execute (THROUGH THE CENTRAL SEAM, ``READ_INTENT``) binding the keys (single
    array / per-column arrays / JSON tuples) and group the child rows by their target-key identity.
    EMPTY key set → NO query. Returns ``{sql, keys, batch}`` (``keys`` = the deduped parent-key tuples).

    ``driver`` is EITHER a raw :class:`Driver` (wrapped via :func:`context_for_driver` — byte-identical)
    OR an :class:`ExecutionContext`.
    """
    ctx = as_context(driver)
    p_cols = _parent_key_cols(op)
    keys = dedupe_keys(parents, p_cols)
    batch: Dict[str, List[Dict[str, Any]]] = {}
    cast = op["sql"]
    if op["dialect"] == "postgres":
        for col in range(len(p_cols)):
            cast = resolve_pg_array_cast(cast, [t[col] for t in keys])
    sql = render_placeholders(cast, op["dialect"])
    if len(keys) == 0:
        return {"sql": sql, "keys": keys, "batch": batch}
    t_cols = _target_key_cols(op)
    rows = seam_execute(ctx, sql, _bind_keys(op, keys), READ_INTENT)
    for row in rows:
        k = _key_identity([row[c] for c in t_cols])
        batch.setdefault(k, []).append(row)
    return {"sql": sql, "keys": keys, "batch": batch}


def distribute_to_parent(
    op: Mapping[str, Any],
    parent: Mapping[str, Any],
    batch: Mapping[str, List[Dict[str, Any]]],
) -> Union[List[Dict[str, Any]], Dict[str, Any], None]:
    """Distribute a resolved batch onto ONE parent per cardinality (port of TS ``distributeToParent``).

    ``hasMany`` → the child list (``[]`` when none); ``belongsTo``/``hasOne`` → the single child (or
    ``None``). Keyed by the parent's key-tuple identity.
    """
    tuple_ = [parent.get(c) for c in _parent_key_cols(op)]
    rows = None if any(v is None for v in tuple_) else batch.get(_key_identity(tuple_))
    if op["kind"] == "hasMany":
        return rows if rows is not None else []
    return rows[0] if rows else None


def _driver_for_op(op: Mapping[str, Any], driver: Driver, connections: Optional[Mapping[str, Driver]]) -> Driver:
    """The driver a relation runs against: its tagged cross-DB connection, else the primary ``driver``.

    CROSS-DB (V0 R1): a relation whose op carries a ``connection`` tag (its target model lives in a
    DIFFERENT DB — v1 ``LazyRelation.ts:236``) routes to ``connections[tag]``. Loud failure when the
    tag has no registered driver (a real wiring bug — never a silent same-DB fallback that would run
    the target's query on the wrong DB). Untagged (same-DB) relations use the primary ``driver``.
    """
    tag = op.get("connection")
    if tag is None:
        return driver
    d = (connections or {}).get(tag)
    if d is None:
        raise ValueError(
            f"cross-DB relation '{op.get('name')}': no driver registered for connection '{tag}' "
            "(pass it in read_bundle connections)"
        )
    return d


def read_bundle(
    bundle: Mapping[str, Any],
    input_scope: Mapping[str, Any],
    driver: Driver,
    with_names: Sequence[str],
    connections: Optional[Mapping[str, Driver]] = None,
) -> List[Dict[str, Any]]:
    """Run a READ bundle's primary row list, then batch-load + hydrate the selected relations.

    Mirrors the TS ``readBundle`` typed-object surface restricted to the declarative-select path
    (``buildResultSet`` with ``with``): the primary read output must be a bare row list; each named
    relation in ``with_names`` is batch-prefetched ONCE over the whole page (staged, no N+1) via the
    SAME ``run_relation_op`` and attached onto each parent as an own key. Independent sibling
    relations are naturally free of ordering: the batch is grouped-then-distributed by key, so the
    hydrated result is deterministic regardless of query-completion order (#40 parallel-safe).

    CROSS-DB (V0 R1): a relation op carrying a ``connection`` tag is batched against
    ``connections[tag]`` (its target model's DB) instead of the primary ``driver``; untagged
    relations ignore ``connections``. Omit ``connections`` for a single-DB read.
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
        rel_driver = _driver_for_op(op, driver, connections)
        batch = run_relation_op(op, rows, rel_driver)["batch"]
        for o in rows:
            o[name] = distribute_to_parent(op, o, batch)
    return rows
