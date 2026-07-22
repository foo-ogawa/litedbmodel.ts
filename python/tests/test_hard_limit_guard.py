"""The FIND-context runaway guard primitive (#74 / #99, python port).

Pins :func:`check_find_hard_limit` and its SSoT core :meth:`LimitExceededError.check` behaviour-identical
to the rust ``check_find_hard_limit`` / ``LimitExceededError::check`` and the go ``CheckFindHardLimit``:
the compile injects ``LIMIT hardLimit + 1``, so a post-fetch ``count`` of ``hardLimit + 1`` means the
TRUE total exceeds the cap and the read fails LOUD (``context='find'``) instead of loading an unbounded
set. The 19 native ops bake explicit LIMITs, so the guard is not wired into them (same as rust/go) — it
is the available guard primitive, unit-tested here. The relation-context arm of the SAME core is pinned
in test_grouping / the relation path; this file pins the find arm + the shared comparison."""

from __future__ import annotations

import pytest

from litedbmodel_runtime import LimitExceededError, check_find_hard_limit


def test_within_cap_is_a_no_op():
    # count <= limit: the LIMIT cap+1 fetch returned at most `limit` rows ⇒ within cap, no raise.
    assert check_find_hard_limit(100, 100, "benchmark_users") is None
    assert check_find_hard_limit(100, 99, "benchmark_users") is None
    assert LimitExceededError.check(5, 5, "find", "m") is None


def test_over_cap_raises_find_context_error():
    # count == limit+1 (the injected LIMIT hardLimit+1 tripped): the true total exceeds the cap ⇒ LOUD.
    with pytest.raises(LimitExceededError) as ei:
        check_find_hard_limit(100, 101, "benchmark_users")
    e = ei.value
    assert e.name == "LimitExceededError"
    assert e.limit == 100 and e.count == 101
    assert e.context == "find" and e.model == "benchmark_users" and e.relation is None
    # find context reports "more than <limit>" (the cap+1 fetch only KNOWS the total exceeds the cap).
    assert "find() on benchmark_users returned more than 100 records" in str(e)
    assert "but limit is 100" in str(e)


def test_shared_check_core_is_context_parametric():
    # The SAME `count > limit ⇒ raise` core serves the relation context (exact count) too (SSoT).
    with pytest.raises(LimitExceededError) as ei:
        LimitExceededError.check(2, 7, "relation", "benchmark_posts", "posts")
    e = ei.value
    assert e.context == "relation" and e.count == 7 and e.relation == "posts"
    assert "relation 'posts' on benchmark_posts returned 7 records" in str(e)  # relation reports EXACT count
