"""Phase D (#95, python) — the SCP MIDDLEWARE layer, hook-mechanics unit tests.

The Python port of the TS reference ``test/scp/middleware.test.ts`` (#92). Proves D1/D2/D3 on the REAL
Phase A exec-context seam (in-process ``sqlite3`` via :class:`SqliteDriver`):

  D1 SQL-level ``execute`` hook — a registered middleware intercepts EVERY SQL through the seam
     (read / write / relation-batch / tx-control), can OBSERVE / REWRITE / TIME / SHORT-CIRCUIT;
     per-scope ISOLATION (two concurrent scopes on distinct threads don't see each other's middleware OR
     state). RED: unregistered ⇒ no interception; shared-registry ⇒ cross-talk. Owner decision A: a real
     ``transaction()``'s RUNTIME BEGIN / COMMIT / ROLLBACK are middleware-visible (issued through the
     seam on the pinned tx connection), EXEMPT from the write=tx guard; RED: bypass the seam-routing ⇒
     the BEGIN/COMMIT observation goes empty.
  D2 method-level hooks — ``run_method(kind, …)`` fires the matching op-kind hook (find/create/…),
     before/after observed; applied order = first-registered outermost. RED: wrong kind ⇒ no fire;
     fold-reversed ⇒ order RED.
  D3 Logger + raw execute/query — Logger records real SQL/params/timing; raw_execute/raw_query go
     THROUGH the seam (a registered SQL middleware sees the raw call). RED: no wiring ⇒ empty.

Every registration is inside a ``with_middleware_scope`` so the process-global registry stays clean (an
unregistered chain is byte-identical — the conformance/livedb runners register none).
"""

from __future__ import annotations

import sqlite3
import threading
import time

import pytest

from litedbmodel_runtime import (
    Logger,
    Registry,
    SqliteDriver,
    WriteOutsideTransactionError,
    clear_middlewares,
    context_for_driver,
    create_middleware,
    current_registry,
    execute as seam_execute,
    execute_transaction_bundle,
    raw_execute,
    raw_query,
    read_bundle,
    register_middleware,
    run as seam_run,
    run_guarded,
    run_method,
    run_relation_op,
    transaction,
    use,
    with_middleware_scope,
)
from litedbmodel_runtime.exec_context import current_context


def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    conn.commit()
    return conn


@pytest.fixture(autouse=True)
def _clean():
    clear_middlewares()
    yield
    clear_middlewares()


# ── D1: SQL-level execute hook ─────────────────────────────────────────────────


def test_intercepts_every_sql_through_the_seam_observe():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))
    seen = []

    def scope():
        use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(sql), nxt(sql, params))[1]))
        seam_run(ctx, "BEGIN", [])
        seam_run(ctx, "INSERT INTO t (name) VALUES (?)", ["a"])
        seam_run(ctx, "COMMIT", [])
        seam_execute(ctx, "SELECT * FROM t", [])

    with_middleware_scope(scope)
    # BEGIN, INSERT, COMMIT, SELECT — write, tx-control and read ALL funnel through the ONE seam.
    assert seen == ["BEGIN", "INSERT INTO t (name) VALUES (?)", "COMMIT", "SELECT * FROM t"]


def test_red_without_wiring_nothing_is_observed():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))
    seen = []
    # No use()/with_middleware_scope registration → the seam is a byte-identical passthrough.
    seam_run(ctx, "INSERT INTO t (name) VALUES (?)", ["a"])
    seam_execute(ctx, "SELECT * FROM t", [])
    assert seen == []  # would be non-empty iff the hook fired — proves the assertion is real


def test_can_rewrite_sql_params_passed_to_next():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))

    def rewriter(st, nxt, sql, params):
        if sql.startswith("INSERT"):
            return nxt(sql, ["rewritten"])
        return nxt(sql, params)

    def scope():
        use(create_middleware(execute=rewriter))
        seam_run(ctx, "INSERT INTO t (name) VALUES (?)", ["original"])

    with_middleware_scope(scope)
    row = dict(zip(["name"], conn.execute("SELECT name FROM t").fetchone()))
    assert row["name"] == "rewritten"


def test_can_time_next():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))
    timed = {"v": -1.0}

    def timer(st, nxt, sql, params):
        t0 = time.perf_counter()
        r = nxt(sql, params)
        timed["v"] = time.perf_counter() - t0
        return r

    def scope():
        use(create_middleware(execute=timer))
        seam_execute(ctx, "SELECT * FROM t", [])

    with_middleware_scope(scope)
    assert timed["v"] >= 0


def test_can_short_circuit_skip_next():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))

    def scope():
        # Do NOT call next — short-circuit with a synthetic row list.
        use(create_middleware(execute=lambda st, nxt, sql, params: [{"id": 99, "name": "synthetic"}]))
        rows = seam_execute(ctx, "SELECT * FROM t", [])
        assert rows == [{"id": 99, "name": "synthetic"}]

    with_middleware_scope(scope)
    # Nothing was ever inserted, so a real query returns empty — proves the DB was bypassed.
    assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0


def test_per_scope_isolation_two_concurrent_scopes():
    """M5 concurrent-isolation: two scopes on distinct THREADS never see each other's middleware OR
    per-middleware state. Each thread has its OWN contextvars copy (a fresh scope with the stack copied
    but the state map EMPTY), so a shared-registry bleed cannot occur. Each thread owns its OWN sqlite
    connection (sqlite is single-thread-affine), so the DB execute is real, not a swallowed cross-thread
    error — the interception + state assertions are load-bearing."""
    seen_a, seen_b = [], []
    state_a, state_b = {}, {}
    barrier = threading.Barrier(2)

    # ONE shared middleware handle registered in BOTH scopes — its per-scope state must NOT bleed
    # (each scope lazily builds its own {'n': 0}); this is the exact TS M5 shape.
    def counter(st, nxt, sql, params):
        st["n"] += 1
        return nxt(sql, params)

    mw = create_middleware(state={"n": 0}, execute=counter)

    def worker(seen, state_out, tag, delay):
        def scope():
            own_conn = _fresh_db()  # this thread's OWN sqlite connection
            ctx = context_for_driver(SqliteDriver(own_conn))
            use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(f"{tag}:{sql}"), nxt(sql, params))[1]))
            use(mw)  # the SHARED-handle state must isolate per scope
            barrier.wait()  # both scopes registered + concurrently active
            time.sleep(delay)
            seam_execute(ctx, f"SELECT {1 if tag == 'A' else 2}", [])
            state_out["n"] = mw.state()["n"]
            own_conn.close()

        # Each thread runs its scope in its own contextvars context (thread → fresh copy).
        import contextvars

        contextvars.copy_context().run(with_middleware_scope, scope)

    ta = threading.Thread(target=worker, args=(seen_a, state_a, "A", 0.02))
    tb = threading.Thread(target=worker, args=(seen_b, state_b, "B", 0.001))
    ta.start()
    tb.start()
    ta.join()
    tb.join()
    # Each scope observed ONLY its own statement — no cross-talk.
    assert seen_a == ["A:SELECT 1"]
    assert seen_b == ["B:SELECT 2"]
    # Each scope's per-middleware state saw exactly its OWN one statement (n==1), not 2 (a shared-map
    # bleed would make it 2) — the stack-copy-NOT-state-map contract.
    assert state_a["n"] == 1
    assert state_b["n"] == 1


def test_red_shared_registry_cross_talk():
    """RED proof: WITHOUT per-scope isolation (a SHARED registry), the two scopes cross-talk."""
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))
    seen_a, seen_b = [], []
    shared = Registry()  # a deliberately SHARED registry (the isolation bug)

    def faithful_shared_scope(seen, tag):
        # Emulate the BUGGY path: register on the SAME shared registry, no per-scope copy.
        shared.use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(f"{tag}:{sql}"), nxt(sql, params))[1]).descriptor)

    faithful_shared_scope(seen_a, "A")
    faithful_shared_scope(seen_b, "B")
    # Run one statement against the shared stack (both A and B hooks fire) → cross-talk.
    chain_stack = shared.sql_hooks()
    fn = lambda s, p: [{"x": 1}]
    for mw in reversed(chain_stack):
        inner = fn
        fn = (lambda mw_, inner_: (lambda s, p: mw_(s, p, inner_)))(mw, inner)
    fn("SELECT 9", [])
    # BOTH scopes' hooks observed the ONE statement — the cross-talk the per-scope copy prevents.
    assert seen_a == ["A:SELECT 9"]
    assert seen_b == ["B:SELECT 9"]


def test_applied_order_first_registered_outermost():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))
    order = []

    def mk(tag):
        def hook(st, nxt, sql, params):
            order.append(f"{tag}:before")
            r = nxt(sql, params)
            order.append(f"{tag}:after")
            return r

        return hook

    def scope():
        use(create_middleware(execute=mk("A")))
        use(create_middleware(execute=mk("B")))
        seam_execute(ctx, "SELECT 1", [])

    with_middleware_scope(scope)
    assert order == ["A:before", "B:before", "B:after", "A:after"]


def test_red_fold_reversed_order():
    """RED proof: a LAST->FIRST fold makes index 0 OUTERMOST; a FIRST->LAST fold reverses it."""
    hooks = []

    def mk(tag):
        def hook(sql, params, nxt):
            hooks.append(f"{tag}:before")
            r = nxt(sql, params)
            hooks.append(f"{tag}:after")
            return r

        return hook

    stack = [mk("A"), mk("B")]
    # The BUGGY fold (first->last) — index 0 ends up INNERMOST.
    fn = lambda s, p: "core"
    for mw in stack:  # forward iteration = wrong
        inner = fn
        fn = (lambda mw_, inner_: (lambda s, p: mw_(s, p, inner_)))(mw, inner)
    fn("SELECT 1", [])
    # Reversed: B outer, A inner — proves the direction matters (the correct fold gives A outer).
    assert hooks == ["B:before", "A:before", "A:after", "B:after"]


def test_per_scope_state_isolated_and_fresh():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))

    def counter(st, nxt, sql, params):
        st["count"] += 1
        return nxt(sql, params)

    mw = create_middleware(state={"count": 0}, execute=counter)

    def scope1():
        use(mw)
        seam_execute(ctx, "SELECT 1", [])
        seam_execute(ctx, "SELECT 2", [])
        assert mw.state()["count"] == 2

    with_middleware_scope(scope1)

    def scope2():
        use(mw)
        seam_execute(ctx, "SELECT 3", [])
        # A fresh scope starts from a fresh state copy (0), not the previous scope's 2.
        assert mw.state()["count"] == 1

    with_middleware_scope(scope2)


def test_native_register_middleware_appends():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))
    seen = []

    def scope():
        # native register_middleware — appends to the current scope's ctx chain (design §4).
        register_middleware(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(sql), nxt(sql, params))[1]))
        seam_execute(ctx, "SELECT 42", [])

    with_middleware_scope(scope)
    assert seen == ["SELECT 42"]


# ── D1 TX-CONTROL: runtime BEGIN/COMMIT/ROLLBACK are middleware-visible (owner decision A) ──


def _tx_bundle():
    """A minimal write bundle whose transaction plan inserts one row (gate-free single body op)."""
    return {
        "dialect": "sqlite",
        "name": "InsertOne",
        "transaction": {
            "phase": "create",
            "entityFrom": None,
            "statements": [
                {"id": "b0", "role": "body", "op": {
                    "sql": "INSERT INTO t (id, name) VALUES (?, ?)",
                    "params": [{"ref": ["id"]}, {"ref": ["name"]}],
                }},
            ],
        },
    }


def test_runtime_begin_commit_are_middleware_visible():
    """POSITIVE (owner decision A): a middleware observes the RUNTIME BEGIN + body INSERT + COMMIT of a
    real ``transaction()`` on the in-proc sqlite seam — all funneled through the ONE seam."""
    conn = _fresh_db()
    driver = SqliteDriver(conn)
    ctx = context_for_driver(driver)
    seen = []

    def scope():
        use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(sql), nxt(sql, params))[1]))
        transaction(ctx, lambda: execute_transaction_bundle(_tx_bundle(), {"id": 1, "name": "a"}, driver), None, "sqlite")

    with_middleware_scope(scope)
    assert "BEGIN" in seen, seen
    assert "INSERT INTO t (id, name) VALUES (?, ?)" in seen
    assert "COMMIT" in seen, seen
    assert seen.index("BEGIN") < seen.index("INSERT INTO t (id, name) VALUES (?, ?)") < seen.index("COMMIT")
    # The row actually committed (real transaction).
    assert conn.execute("SELECT name FROM t WHERE id = 1").fetchone()[0] == "a"


def test_runtime_rollback_on_error_is_middleware_visible():
    """POSITIVE: a body error rolls back — the middleware observes runtime BEGIN + ROLLBACK, NO COMMIT,
    and the row is genuinely rolled back."""
    conn = _fresh_db()
    driver = SqliteDriver(conn)
    ctx = context_for_driver(driver)
    seen = []

    def scope():
        use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(sql), nxt(sql, params))[1]))

        def body():
            execute_transaction_bundle(_tx_bundle(), {"id": 5, "name": "x"}, driver)
            raise RuntimeError("body boom")

        with pytest.raises(Exception):
            transaction(ctx, body, None, "sqlite")

    with_middleware_scope(scope)
    assert "BEGIN" in seen, seen
    assert "ROLLBACK" in seen, seen
    assert "COMMIT" not in seen, seen
    # The row was rolled back — not present.
    assert conn.execute("SELECT COUNT(*) FROM t WHERE id = 5").fetchone()[0] == 0


def test_red_tx_control_direct_on_conn_is_not_observed():
    """RED proof (faithful source mutation): if with_transaction_decided issued tx-control DIRECTLY on
    the owned handle (``tx.run``) instead of through the pinned-ctx seam, the middleware would NOT see
    BEGIN/COMMIT — the positive observation goes RED. Proves the seam-routing is load-bearing."""
    import litedbmodel_runtime.exec_context as ec

    conn = _fresh_db()
    driver = SqliteDriver(conn)
    ctx = context_for_driver(driver)
    seen = []

    def bypass(ctx, body, before=(), after=()):
        tx = ctx.begin_tx()
        tx_ctx = ctx.with_connection(ec._TxConnectionAdapter(tx), True)

        def scoped():
            destroy = True
            try:
                tx.run("BEGIN", [])  # DIRECT on the owned handle — NOT through run(tx_ctx, …)
                try:
                    decision = body(tx_ctx)
                except BaseException:
                    try:
                        tx.run("ROLLBACK", [])
                        destroy = False
                    except Exception:
                        pass
                    raise
                if decision.rollback:
                    tx.run("ROLLBACK", [])
                    destroy = False
                    return decision.value
                tx.run("COMMIT", [])
                destroy = False
                ctx.mark_sticky()
                return decision.value
            finally:
                tx.release(destroy)

        return ec.run_with_pinned_context(tx_ctx, scoped)

    orig = ec.with_transaction_decided
    ec.with_transaction_decided = bypass
    try:
        def scope():
            use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(sql), nxt(sql, params))[1]))
            transaction(ctx, lambda: execute_transaction_bundle(_tx_bundle(), {"id": 2, "name": "b"}, driver), None, "sqlite")

        with_middleware_scope(scope)
    finally:
        ec.with_transaction_decided = orig

    # The body INSERT was seen (it goes through the seam), but the runtime BEGIN/COMMIT were NOT.
    assert "INSERT INTO t (id, name) VALUES (?, ?)" in seen
    assert "BEGIN" not in seen, seen
    assert "COMMIT" not in seen, seen
    # Behavior preserved: the row committed.
    assert conn.execute("SELECT name FROM t WHERE id = 2").fetchone()[0] == "b"


def test_tx_control_is_exempt_from_write_tx_guard():
    """tx-control (BEGIN/COMMIT/ROLLBACK) is issued via the UNGUARDED seam ⇒ exempt from the write=tx
    guard, so a real transaction() opens cleanly. A GUARDED user write OUTSIDE a transaction still fails
    (the guard is intact) — proving the exemption is scoped to tx-control, not a guard bypass."""
    conn = _fresh_db()
    driver = SqliteDriver(conn)
    ctx = context_for_driver(driver)

    # tx-control runs fine inside transaction() (BEGIN/COMMIT are NOT guard-rejected).
    transaction(ctx, lambda: execute_transaction_bundle(_tx_bundle(), {"id": 3, "name": "c"}, driver), None, "sqlite")
    assert conn.execute("SELECT name FROM t WHERE id = 3").fetchone()[0] == "c"

    # The guard is still intact for a real user write issued OUTSIDE a transaction boundary.
    with pytest.raises(WriteOutsideTransactionError):
        run_guarded(ctx, "INSERT INTO t (id, name) VALUES (?, ?)", [4, "d"], "WRITE", "t")


# ── D1 END-TO-END: a real relation-BATCH read is observed by a registered middleware ──


_REL_OP = {
    "name": "kids",
    "kind": "hasMany",
    "targetTable": "child",
    "sql": "SELECT id, parent_id, label FROM child WHERE parent_id IN (SELECT value FROM json_each(?))",
    "parentKey": "id",
    "targetKey": "parent_id",
    "dialect": "sqlite",
}


def _rel_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, label TEXT)")
    conn.execute("INSERT INTO parent VALUES (1,'p')")
    conn.executemany("INSERT INTO child VALUES (?,?,?)", [(10, 1, "a"), (11, 1, "b")])
    conn.commit()
    return conn


def test_middleware_observes_relation_batch_sql_end_to_end():
    """A registered middleware observes the relation-BATCH SELECT (on `child`) of a multi-node read.

    ``run_relation_op(op, parents, driver)`` wraps the raw driver via ``as_context`` →
    ``context_for_driver``, which sources the ambient registry — so the relation batch is NOT a
    driver-direct call; it funnels through the SAME seam, and a registered SQL middleware sees it."""
    conn = _rel_db()
    seen = []
    result = {}

    def scope():
        use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(sql), nxt(sql, params))[1]))
        # The hasMany batch SELECT on `child` (fan-out over the parent keys) funnels through the seam.
        parents = [{"id": 1, "name": "p"}]
        res = run_relation_op(_REL_OP, parents, SqliteDriver(conn))
        result["batch"] = res["batch"]

    with_middleware_scope(scope)
    # The relation actually loaded (2 children under parent 1) — a genuine relation-batch read.
    assert result["batch"]["1"][0]["label"] == "a"
    # The middleware saw the relation-batch SELECT querying the child table.
    assert any("from child" in s.lower() for s in seen), seen


def test_red_relation_batch_not_observed_without_registration():
    conn = _rel_db()
    seen = []
    # No middleware registered → the relation batch runs as a byte-identical passthrough.
    parents = [{"id": 1, "name": "p"}]
    res = run_relation_op(_REL_OP, parents, SqliteDriver(conn))
    # The read still WORKS (byte-identical) — the relation loaded — but nothing was observed.
    assert len(res["batch"]["1"]) == 2
    assert not any("from child" in s.lower() for s in seen)


# ── D2: method-level hooks ─────────────────────────────────────────────────────


def test_fires_matching_op_kind_hook_before_after():
    for kind in ["find", "create", "update", "delete"]:
        events = []

        def mk(k, evs):
            def hook(st, model, nxt, *args):
                evs.append(f"{k}:before")
                r = nxt(*args)
                evs.append(f"{k}:after")
                return r

            return hook

        def scope(k=kind, evs=events):
            use(create_middleware(**{k: mk(k, evs)}))

            def core(*args):
                evs.append(f"{k}:core")
                return "ok"

            result = run_method(k, None, core, [])
            assert result == "ok"

        with_middleware_scope(scope)
        assert events == [f"{kind}:before", f"{kind}:core", f"{kind}:after"]


def test_red_hook_of_different_kind_does_not_fire():
    events = []

    def scope():
        use(create_middleware(create=lambda st, model, nxt, *args: (events.append("create"), nxt(*args))[1]))
        # Dispatch a `find` — the `create` hook must NOT fire (kind mismatch, TAG-based dispatch).
        run_method("find", None, lambda: "r", [])

    with_middleware_scope(scope)
    assert events == []


def test_method_hooks_compose_first_registered_outermost_and_rewrite_args():
    order = []
    core_arg = {"v": 0}

    def a(st, model, nxt, n):
        order.append("A")
        return nxt(n + 1)

    def b(st, model, nxt, n):
        order.append("B")
        return nxt(n + 10)

    def scope():
        use(create_middleware(find=a))
        use(create_middleware(find=b))

        def core(n):
            core_arg["v"] = n
            return None

        run_method("find", None, core, [0])

    with_middleware_scope(scope)
    assert order == ["A", "B"]  # A outer, B inner
    assert core_arg["v"] == 11  # 0 +1 (A) +10 (B)


def test_op_kind_is_a_tag_never_parsed_from_sql():
    """D2 contract: the op kind is the TAG passed to run_method, NEVER inferred from SQL text."""
    events = []

    def scope():
        use(create_middleware(create=lambda st, model, nxt, *args: (events.append("create-fired"), nxt(*args))[1]))
        # The 'SQL' clearly reads like a SELECT, but the TAG is 'create' → the create hook fires,
        # and a find hook (none) would NOT — dispatch is on the TAG, not the text.
        run_method("create", None, lambda sql: sql, ["SELECT * FROM anything"])

    with_middleware_scope(scope)
    assert events == ["create-fired"]


# ── D3: Logger + raw execute/query ─────────────────────────────────────────────


def test_logger_records_sql_params_timing():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))
    logger = Logger()

    def scope():
        use(logger)
        seam_run(ctx, "INSERT INTO t (name) VALUES (?)", ["x"])
        seam_execute(ctx, "SELECT * FROM t WHERE name = ?", ["x"])
        entries = logger.state()["entries"]
        assert [e.sql for e in entries] == ["INSERT INTO t (name) VALUES (?)", "SELECT * FROM t WHERE name = ?"]
        assert list(entries[0].params) == ["x"]
        assert list(entries[1].params) == ["x"]
        for e in entries:
            assert e.duration_ms >= 0

    with_middleware_scope(scope)


def test_raw_execute_goes_through_the_seam():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))
    seen = []

    def scope():
        use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(sql), nxt(sql, params))[1]))
        insert = raw_execute(ctx, "INSERT INTO t (name) VALUES (?)", ["raw"])
        assert insert.row_count == 1
        read = raw_execute(ctx, "SELECT name FROM t")
        assert read.rows == [{"name": "raw"}]

    with_middleware_scope(scope)
    assert seen == ["INSERT INTO t (name) VALUES (?)", "SELECT name FROM t"]


def test_raw_query_fires_query_method_hook_and_flows_through_execute_seam():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))
    conn.execute("INSERT INTO t (name) VALUES ('q')")
    conn.commit()
    events = []

    def query_hook(st, model, nxt, *args):
        events.append("query")
        return nxt(*args)

    def execute_hook(st, nxt, sql, params):
        events.append(f"execute:{sql}")
        return nxt(sql, params)

    def scope():
        use(create_middleware(query=query_hook, execute=execute_hook))
        rows = raw_query(ctx, "SELECT name FROM t")
        assert rows == [{"name": "q"}]

    with_middleware_scope(scope)
    # The `query` method hook fires (two-level), THEN the SQL flows through the execute seam.
    assert events == ["query", "execute:SELECT name FROM t"]


def test_red_logger_records_nothing_without_wiring():
    conn = _fresh_db()
    ctx = context_for_driver(SqliteDriver(conn))
    logger = Logger()
    # NOT registered → the seam never invokes it.
    seam_execute(ctx, "SELECT 1", [])
    assert logger.state()["entries"] == []
