"""Dialect-primitive unit tests (WS7b, #31): the orderByNulls NULLS-ordering strategy.

PG/SQLite emit native `NULLS FIRST/LAST`; MySQL (no native support) emulates with a leading
`<expr> IS NULL` sort key. This is the WS6-flagged primitive the conformance `dialect` suite pins.
"""

from __future__ import annotations

import pytest

from litedbmodel_runtime import order_by_nulls


@pytest.mark.parametrize("dialect", ["sqlite", "postgres"])
@pytest.mark.parametrize("direction", ["ASC", "DESC"])
@pytest.mark.parametrize("nulls", ["FIRST", "LAST"])
def test_native_nulls_ordering(dialect, direction, nulls):
    assert order_by_nulls("created_at", direction, nulls, dialect) == f"created_at {direction} NULLS {nulls}"


@pytest.mark.parametrize(
    "direction,nulls,expected",
    [
        ("ASC", "FIRST", "created_at IS NULL DESC, created_at ASC"),
        ("ASC", "LAST", "created_at IS NULL ASC, created_at ASC"),
        ("DESC", "FIRST", "created_at IS NULL DESC, created_at DESC"),
        ("DESC", "LAST", "created_at IS NULL ASC, created_at DESC"),
    ],
)
def test_mysql_is_null_emulation(direction, nulls, expected):
    assert order_by_nulls("created_at", direction, nulls, "mysql") == expected
