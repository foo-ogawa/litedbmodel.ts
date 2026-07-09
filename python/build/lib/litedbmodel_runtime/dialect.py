"""litedbmodel v2 SCP — dialect strategy table (Python port of ``src/scp/dialect.ts``).

The SINGLE SOURCE OF TRUTH for every SQL-dialect difference the render pipeline needs, ported
byte-for-byte from the TS reference (spec §4/§5/§8/§10). The dialect axis is compiled ONCE
TS-side; the published bundle carries `?` placeholders and a `dialect` tag, and this module only
needs the render-time concerns a thin runtime touches:

  - ``finalize_placeholders`` — the `?`→`$N` final one-pass (Postgres only; SQLite/MySQL identity).
  - ``order_by_nulls`` — deterministic NULLS ordering (native for PG/SQLite, `IS NULL` emulation
    for MySQL) — the WS6-flagged dialect primitive exercised by the conformance `dialect` suite.

The INSERT-conflict / guard-INSERT strategy methods are NOT needed by the runtime: those are a
compile-time concern (the published bundle's `operations[*].sql` already carries the fully
rendered conflict clause — e.g. `ON CONFLICT DO NOTHING`), so the runtime never re-derives them.
This mirrors the TS runtime, which likewise only calls `finalizePlaceholders` / `orderByNulls`.
"""

from __future__ import annotations

from typing import Callable, Dict

# Known dialects (spec §4 breadth: PG/MySQL/SQLite).
DIALECT_NAMES = ("sqlite", "postgres", "mysql")


def to_dollar_placeholders(sql: str) -> str:
    """Replace each `?` with `$1, $2, …` left-to-right (Postgres §8 final one-pass).

    Byte-identical to the TS `toDollarPlaceholders`: it runs ONCE over the fully-assembled,
    param-flattened SQL text, so placeholder numbering is a plain running counter (the
    number-reassignment problem cannot reappear). Every `?` on the compiled surface is a bound
    param position — the render pipeline never emits a literal `?` inside a string literal.
    """
    n = 0
    out = []
    for ch in sql:
        if ch == "?":
            n += 1
            out.append(f"${n}")
        else:
            out.append(ch)
    return "".join(out)


def _identity(sql: str) -> str:
    return sql


def _order_by_nulls_native(expr: str, direction: str, nulls: str) -> str:
    # Postgres / SQLite (3.30+): native `NULLS FIRST/LAST`.
    return f"{expr} {direction} NULLS {nulls}"


def _order_by_nulls_mysql(expr: str, direction: str, nulls: str) -> str:
    # MySQL has no NULLS FIRST/LAST — emulate with a leading `IS NULL` sort key.
    # In MySQL NULL sorts LOWEST; `expr IS NULL` is 1 for null, 0 otherwise.
    #   NULLS FIRST: nulls must come first → order the IS-NULL flag DESC (1 before 0).
    #   NULLS LAST:  nulls must come last  → order the IS-NULL flag ASC  (0 before 1).
    flag_dir = "DESC" if nulls == "FIRST" else "ASC"
    return f"{expr} IS NULL {flag_dir}, {expr} {direction}"


class Dialect:
    """A frozen dialect strategy: the render-time text producers a thin runtime consumes."""

    __slots__ = ("name", "_finalize", "_order_by_nulls")

    def __init__(
        self,
        name: str,
        finalize: Callable[[str], str],
        order_by_nulls: Callable[[str, str, str], str],
    ) -> None:
        self.name = name
        self._finalize = finalize
        self._order_by_nulls = order_by_nulls

    def finalize_placeholders(self, sql: str) -> str:
        return self._finalize(sql)

    def order_by_nulls(self, expr: str, direction: str, nulls: str) -> str:
        return self._order_by_nulls(expr, direction, nulls)


SQLITE = Dialect("sqlite", _identity, _order_by_nulls_native)
POSTGRES = Dialect("postgres", to_dollar_placeholders, _order_by_nulls_native)
MYSQL = Dialect("mysql", _identity, _order_by_nulls_mysql)

_DIALECTS: Dict[str, Dialect] = {"sqlite": SQLITE, "postgres": POSTGRES, "mysql": MYSQL}


def dialect_for(name: str) -> Dialect:
    """Resolve a dialect name to its strategy (fail-closed — no silent default)."""
    d = _DIALECTS.get(name)
    if d is None:
        raise ValueError(
            f"scp dialect: unknown dialect '{name}' (known: {', '.join(_DIALECTS)})"
        )
    return d
