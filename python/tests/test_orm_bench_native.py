"""ORM-bench native (ir-exec) READ-path conformance (#141 / epic #123, python leg, slice 1).

Proves the litedbmodel python runtime runs the READ/relation ops through the bc-GENERATED ir-exec
module (``orm_bench.behaviors_generated``, verbatim ``bc generate --lang python``) bound to the
op-agnostic leaf transport (``make_handlers``) — the python literal path (ts/go/rust = native de-box;
py/php = literal). Pins: every read op executes, and every batched relation is N+1-free (1 parent + 1
batched child per level, INDEPENDENT of the row count). Schema/seed/harness are imported from the bench
cell (``orm_bench.main``) — the SAME fixture the CSV cell measures (no duplicated setup)."""

from __future__ import annotations

import pytest

from orm_bench.main import (
    READ_OPS,
    RELATION_QUERY_COUNTS,
    bound_ops,
    open_driver,
    run_op,
    seed,
)


@pytest.fixture()
def fns():
    driver = open_driver("sqlite", counting=True)
    seed(driver)
    return driver, bound_ops(driver, "sqlite")


def test_every_read_op_executes(fns):
    _driver, fn_map = fns
    for op in READ_OPS:
        result = run_op(fn_map, op)
        assert isinstance(result, list)  # every read op returns a row list


def test_find_ops_return_expected_rows(fns):
    _driver, fn_map = fns
    assert len(run_op(fn_map, "findAll")) == 5  # 5 seeded users
    # findUnique(email) → the one matching user (id widened to float by the concrete outType de-box).
    unique = run_op(fn_map, "findUnique")
    assert len(unique) == 1 and unique[0]["email"] == "user1@example.com"
    # findFirst(name LIKE 'User%') LIMIT 1 → exactly one row.
    assert len(run_op(fn_map, "findFirst")) == 1


def test_nested_relations_hydrate_children(fns):
    _driver, fn_map = fns
    users = run_op(fn_map, "nestedFindAll")
    by_id = {u["id"]: u for u in users}
    # user 1 has 2 posts (P1, P2) batch-loaded under `posts` — N+1-free grouping.
    assert [p["title"] for p in by_id[1]["posts"]] == ["P1", "P2"]

    deep = run_op(fn_map, "nestedRelations")
    u1 = next(u for u in deep if u["id"] == 1)
    # 3-level chain: user → posts → comments (comments nested on each post).
    assert [c["id"] for c in u1["posts"][0]["comments"]] == [1, 2]


def test_composite_relations_group_by_full_tuple(fns):
    _driver, fn_map = fns
    tenants = run_op(fn_map, "compositeRelations")
    tu1 = next(t for t in tenants if t["user_id"] == 1)
    # composite (tenant_id,user_id) → posts; each post → composite (tenant_id,post_id) → comments.
    assert [p["post_id"] for p in tu1["posts"]] == [10]
    assert [c["comment_id"] for c in tu1["posts"][0]["comments"]] == [100, 101]


@pytest.mark.parametrize("op,expected", list(RELATION_QUERY_COUNTS.items()))
def test_relations_are_n_plus_one_free(fns, op, expected):
    driver, fn_map = fns
    driver.query_count = 0
    run_op(fn_map, op)
    assert driver.query_count == expected, f"{op}: {driver.query_count} queries (N+1-free expects {expected})"
