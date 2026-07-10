"""#40 — proof that independent sibling-relation read nodes are dispatched CONCURRENTLY (Python).

Like Go, the Python bc ``run_plan`` dispatches the INDEPENDENT members of a plan stage on a
``ThreadPoolExecutor`` when ``concurrency > 1`` (bc#23), and ``run_behavior`` drives ``run_plan``.
A single DB-API connection is not safe for concurrent use, so the live PG/MySQL drivers were made
POOLED (#40): each ``prepare().all()`` checks out its own pooled connection, so distinct sibling
threads run on distinct connections in parallel.

This proves it with a LATENCY-INJECTING, in-flight-counting :class:`Driver` over a multi-sibling
read graph carrying ``concurrency: 16``: N siblings @ D each overlap (wall ≈ D, not N·D), the peak
simultaneous in-flight count reaches N, and the Φ-merged result is deterministic (declaration
order). A separate check confirms ``concurrency: 1`` stays serial (peak = 1).
"""

from __future__ import annotations

import threading
import time

from litedbmodel_runtime.static_bundle import execute_read_graph

SCOPE_PORT = "__scope"
NODE_COMPONENT = "__makeSqlNode"


class _LatencyPrepared:
    def __init__(self, driver: "_LatencyDriver", sql: str) -> None:
        self._driver = driver
        self._sql = sql

    def all(self, params):
        d = self._driver
        with d._lock:
            d.calls += 1
            d.in_flight += 1
            d.peak = max(d.peak, d.in_flight)
        time.sleep(d.latency)
        with d._lock:
            d.in_flight -= 1
        return [{"sql": self._sql}]

    def run(self, params):  # pragma: no cover - read path only
        raise AssertionError("run() not used on the read path")


class _LatencyDriver:
    """A thread-safe Driver that sleeps `latency` per query and tracks peak in-flight count."""

    def __init__(self, latency: float) -> None:
        self.latency = latency
        self.calls = 0
        self.in_flight = 0
        self.peak = 0
        self._lock = threading.Lock()

    def prepare(self, sql: str) -> _LatencyPrepared:
        return _LatencyPrepared(self, sql)


def _sibling_graph(n: int, concurrency: int = 16) -> dict:
    """A read graph of n independent sibling nodes in ONE plan stage, each a trivial SELECT."""
    body = []
    output_obj = {}
    statements_by_id = {}
    for i in range(n):
        node_id = f"rel{i}"
        body.append({"id": node_id, "component": NODE_COMPONENT, "ports": {SCOPE_PORT: {"obj": {}}}})
        output_obj[node_id] = {"ref": [node_id]}
        statements_by_id[node_id] = [{"sql": f"SELECT {i}", "params": []}]
    return {
        "dialect": "sqlite",
        "name": "Siblings",
        "ir": {
            "irVersion": 1,
            "exprVersion": 2,
            "components": [
                {
                    "name": "Siblings",
                    "inputPorts": {},
                    "body": body,
                    "output": {"obj": output_obj},
                    "plan": {"concurrency": concurrency, "groups": [list(range(n))]},
                }
            ],
        },
        "statementsById": statements_by_id,
        "optionalHeads": [],
    }


def test_sibling_relations_dispatch_concurrently():
    n = 8
    latency = 0.06  # 60ms
    driver = _LatencyDriver(latency)
    graph = _sibling_graph(n)

    t0 = time.monotonic()
    out = execute_read_graph(graph, {}, driver)
    elapsed = time.monotonic() - t0

    # 1. Overlap: 8 × 60ms serial = 480ms; concurrent ≈ 60ms. Well under half proves overlap.
    assert elapsed < (latency * n) / 2, f"expected concurrent, took {elapsed:.3f}s"
    # 2. All N ran and all N were simultaneously in flight.
    assert driver.calls == n
    assert driver.peak == n, f"expected all {n} siblings in flight at once, peak={driver.peak}"

    # 3. Determinism: the Φ-merged result carries each sibling keyed by node id, in order.
    assert list(out.keys()) == [f"rel{i}" for i in range(n)]
    for i in range(n):
        assert out[f"rel{i}"] == [{"sql": f"SELECT {i}"}]

    print(
        f"PY PARALLEL PROOF: {n} sibling queries @ {latency*1000:.0f}ms each → wall "
        f"{elapsed*1000:.0f}ms (serial would be {latency*n*1000:.0f}ms), peak in-flight = {driver.peak}"
    )


def test_concurrency_one_stays_serial():
    n = 4
    latency = 0.03
    driver = _LatencyDriver(latency)
    graph = _sibling_graph(n, concurrency=1)
    execute_read_graph(graph, {}, driver)
    assert driver.peak == 1, f"concurrency=1 must be serial, peak={driver.peak}"
