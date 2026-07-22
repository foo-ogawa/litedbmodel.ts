"""litedbmodel v2 SCP — the SHARED relation-grouping CORE (#141), Python port.

The ONE implementation of relation key-identity + dedupe + parent grouping over plain dict
records, behaviour-identical to the TS SSoT ``src/scp/grouping.ts`` and the Rust port
``rust/litedbmodel_runtime/src/grouping.rs``. It is consumed by BOTH relation surfaces so there is
a single source of truth (no duplicated grouping logic):

  - the op-INDEPENDENT ``pluck`` / ``group`` leaves (``./leaves``) — the eager N+1-free graph
    (``parents → pluck → executeSQL(WHERE fk = ANY(?)) → group``);
  - the RUNTIME batch relation path (``./relation`` ``run_relation_op`` / ``distribute_to_parent``),
    which groups already-fetched rows over the SAME core.

Nothing here touches SQL or a driver: it is pure in-memory grouping over already-fetched rows
(plain ``dict`` records — Python's native value model IS the wire, so no ``WireValue`` enum). Ordered
TUPLE keys are supported (composite keys), matching TS/Rust.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence

__all__ = ["key_identity", "dedupe_key_tuples", "group_by_key", "attach_to_parent"]

#: A separator no scalar rendering contains, so distinct tuples never collide (matches TS ``KEY_SEP``).
_KEY_SEP = " "


def _stringify(v: Any) -> str:
    """Mirror JS ``String(v)`` for the key identity (matches Rust ``stringify_key``).

    ``bool`` → ``'true'``/``'false'`` (checked FIRST — ``bool`` is a subclass of ``int``); a WHOLE
    float prints as integer text (a scanned INT column can arrive as a whole ``float`` — JS
    ``String(1.0) === '1'``); a fractional float its shortest round-trip form; ``None`` → ``'null'``
    (a null key is dropped before it is ever stringified, so this arm never affects a grouping result —
    it exists only for totality); anything else its ``str``.
    """
    if isinstance(v, bool):
        return "true" if v else "false"
    if v is None:
        return "null"
    if isinstance(v, float):
        if v == v and v not in (float("inf"), float("-inf")) and v.is_integer():
            return str(int(v))
        return str(v)
    return str(v)


def key_identity(values: Sequence[Any]) -> str:
    """The stringified key identity for dedupe/grouping. Single scalar → its ``str`` rendering; a tuple
    → the renderings joined by a single space (mirror of TS ``keyIdentity`` / Rust ``key_identity``)."""
    return _KEY_SEP.join(_stringify(v) for v in values)


def dedupe_key_tuples(rows: Sequence[Mapping[str, Any]], key_cols: Sequence[str]) -> List[List[Any]]:
    """The deduped, non-null key TUPLES of ``rows`` over ``key_cols`` (insertion order preserved —
    deterministic). A tuple is DROPPED if ANY of its key columns is absent or ``None`` (no partial
    keys); deduped on the stringified tuple identity. Port of TS ``dedupeKeyTuples``."""
    seen: set[str] = set()
    out: List[List[Any]] = []
    for r in rows:
        tuple_ = [r.get(c) for c in key_cols]
        if any(v is None for v in tuple_):
            continue
        ident = key_identity(tuple_)
        if ident in seen:
            continue
        seen.add(ident)
        out.append(tuple_)
    return out


def group_by_key(
    children: Sequence[Mapping[str, Any]], fk_cols: Sequence[str]
) -> Dict[str, List[Mapping[str, Any]]]:
    """Group ``children`` by their ``fk_cols`` tuple identity (a null/absent key drops the child). Child
    order within a bucket is the input order. Port of TS ``groupByKey``."""
    by_key: Dict[str, List[Mapping[str, Any]]] = {}
    for c in children:
        tuple_ = [c.get(col) for col in fk_cols]
        if any(v is None for v in tuple_):
            continue
        by_key.setdefault(key_identity(tuple_), []).append(c)
    return by_key


def attach_to_parent(
    parent: Mapping[str, Any],
    pk_cols: Sequence[str],
    by_key: Mapping[str, List[Mapping[str, Any]]],
    single: bool,
) -> Any:
    """Distribute grouped children onto ONE parent per cardinality (port of TS ``attachToParent``):
    ``single is False`` (hasMany) → the child list (``[]`` when none); ``single is True``
    (belongsTo/hasOne) → the single child (or ``None``). Keyed by the parent's ``pk_cols`` tuple
    identity; a null/absent parent key matches nothing (``[]``/``None``)."""
    tuple_ = [parent.get(c) for c in pk_cols]
    rows = None if any(v is None for v in tuple_) else by_key.get(key_identity(tuple_))
    if not single:
        return rows if rows is not None else []
    return rows[0] if rows else None
