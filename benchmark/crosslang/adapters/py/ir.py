"""The python IR cell — litedbmodel's SHIPPED python runtime INTERPRETER executing the 19 bench ops.

This is the honest "ir(interpreter)" tier (NOT native codegen — py/php native codegen is a known bc
capability gap). It consumes the JSON-serialized §8 bundle (adapters/py/bundles.json, built by
gen-bundles.ts) and runs each op through litedbmodel_runtime's PUBLIC entry points — the SAME paths
the conformance corpus drives:
  • read            → execute_bundle
  • read + relation → read_bundle  (batch-load + hydrate the `with` relation)
  • single write    → execute_bundle (the write node's read-graph; RETURNING rows / [])
  • batch write     → execute_transaction_bundle  (gate-free batch tx plan)
  • transaction     → execute_transaction_bundle
Results are canonicalized to the SAME dialect-independent oracle string every other cell matches
(canon.py): reads → row list, read+rel → {rows, <rel>}, no-returning write → null, upsert → [{id}],
tx → {committed, state}.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any, Dict, List

HERE = os.path.dirname(os.path.abspath(__file__))
# litedbmodel python runtime + its bc core. The bc PHP/py ports are UNPUBLISHED here, so — mirroring
# litedbmodel's own use of the local bc CLI — the local source dirs go on the path when present
# (verify-cells exports LITEDBMODEL_PY / BC_PY); else the installed packages are used.
for env in ("LITEDBMODEL_PY", "BC_PY"):
    p = os.environ.get(env)
    if p and p not in sys.path:
        sys.path.insert(0, p)

from litedbmodel_runtime import (  # noqa: E402
    MysqlDriver,
    PostgresDriver,
    SqliteDriver,
    distribute_to_parent,
    execute_bundle,
    read_bundle,
    run_relation_op,
)
from litedbmodel_runtime.runtime import _execute_transaction_bundle  # noqa: E402  internal guard opt-out (per-command auto-tx, corpus parity)

from canon import FIELDS, REL_FIELDS, REL3_FIELDS, canon_row, canon_rows, rel3_json, rel_json, tx_json  # noqa: E402
from db import dialect_of, _parse_mysql_url  # noqa: E402

READ_OPS = {"findAll", "filterPaginateSort", "findFirst", "findUnique"}
REL_OPS = set(REL_FIELDS.keys())  # 2-level (nestedFind*): read_bundle
REL3_OPS = set(REL3_FIELDS.keys())  # 3-level chains (#119): parent → posts → comments
# ALL writes ride execute_transaction_bundle (litedbmodel's real py/php write path). v1 returning:
# no-returning writes → null; upsert returns its PK (the tx `entity`); tx ops → {committed, state}.
NULL_WRITE_OPS = {"create", "update", "createMany", "upsertMany", "updateMany"}
TX_OPS = {"delete", "nestedCreate", "nestedUpdate", "nestedUpsert"}

_BUNDLES: Dict[str, Any] = json.load(open(os.path.join(HERE, "bundles.json"), encoding="utf-8"))


def open_driver(target: str):
    dialect = dialect_of(target)
    if dialect == "postgres":
        # libpq `key=val` conninfo → the driver's connect kwargs.
        kv = dict(tok.split("=", 1) for tok in target.split())
        return dialect, PostgresDriver.connect(host=kv.get("host", "localhost"), port=int(kv.get("port", "5432")), user=kv["user"], password=kv["password"], dbname=kv["dbname"])
    if dialect == "mysql":
        c = _parse_mysql_url(target)
        return dialect, MysqlDriver.connect(host=c["host"], port=c["port"], user=c["user"], password=c["password"], dbname=c["database"])
    import sqlite3

    conn = sqlite3.connect(target)
    conn.execute("PRAGMA foreign_keys = ON")
    return dialect, SqliteDriver(conn)


def _state_snapshot(driver) -> str:
    users = driver.prepare("SELECT id, email, name FROM benchmark_users ORDER BY id").all([])
    posts = driver.prepare("SELECT id, title, author_id FROM benchmark_posts ORDER BY id").all([])
    return users, posts


def ir_cell(op: str, target: str) -> str:
    dialect, driver = open_driver(target)
    try:
        entry = _BUNDLES[dialect][op]
        bundle, inp = entry["bundle"], entry["input"]
        if op in READ_OPS:
            return canon_rows(execute_bundle(bundle, inp, driver), FIELDS[op])
        if op in REL_OPS:
            m = REL_FIELDS[op]
            rows = read_bundle(bundle, inp, driver, [m["rel"]])
            parents = [canon_row(r, m["parent"]) for r in rows]
            children = ["[" + ",".join(canon_row(c, m["child"]) for c in (r.get(m["rel"]) or [])) + "]" for r in rows]
            return rel_json(m["rel"], parents, children)
        if op in REL3_OPS:
            # FULL 3-level chain (#119): parent read → level-2 (posts) batch → level-3 (comments) batch,
            # via the runtime's run_relation_op / distribute_to_parent (mirror oracle.ts). The level-2
            # relation carries its level-3 childRelations. comments flattened per parent, in post order.
            m = REL3_FIELDS[op]
            posts_rel = bundle["relations"][entry["withRel"]]
            comments_rel = posts_rel["childRelations"][0]
            parents = execute_bundle(bundle, inp, driver)
            p_batch = run_relation_op(posts_rel, parents, driver)["batch"]
            per_parent_posts = [distribute_to_parent(posts_rel, p, p_batch) for p in parents]
            all_posts = [p for ps in per_parent_posts for p in ps]
            c_batch = run_relation_op(comments_rel, all_posts, driver)["batch"]
            per_parent_comments = [[c for p in ps for c in distribute_to_parent(comments_rel, p, c_batch)] for ps in per_parent_posts]
            return rel3_json(
                [canon_row(r, m["parent"]) for r in parents],
                [canon_rows(ps, m["posts"]) for ps in per_parent_posts],
                [canon_rows(cs, m["comments"]) for cs in per_parent_comments],
            )
        if op == "upsert":
            res = _execute_transaction_bundle(bundle, inp, driver, guard=False)
            return canon_rows([res["entity"]], FIELDS["upsert"])  # v1 upsert returns the PK
        if op in NULL_WRITE_OPS:
            _execute_transaction_bundle(bundle, inp, driver, guard=False)  # mutate; v1 no-returning → null
            return "null"
        if op in TX_OPS:
            res = _execute_transaction_bundle(bundle, inp, driver, guard=False)
            users, posts = _state_snapshot(driver)
            return tx_json(bool(res.get("committed")), users, posts)
        raise ValueError(f"ir: unknown op {op}")
    finally:
        driver.close()
