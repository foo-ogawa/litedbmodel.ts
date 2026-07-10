#!/usr/bin/env python3
"""litedbmodel SCP LIVE-DB conformance — Python runner (WS7g, #36).

The Python leg of the coordinated cross-language LIVE-DB validation pass (spec §10 dialect axis).
It loads the WS7g live-DB corpus (``conformance/vectors-livedb/livedb.json``) — the exec/tx bundles
compiled for `postgres` + `mysql` — connects to REAL dockerized Postgres + MySQL, creates the
needed tables in an ISOLATED per-language namespace, and runs each bundle through the SAME
``litedbmodel_runtime`` (``execute_bundle`` / ``execute_transaction_bundle``) that the SQLite
conformance uses — now backed by the live :class:`PostgresDriver` / :class:`MysqlDriver` seam. It
asserts the assembled result equals the frozen SQLite reference (`expectedResult` /
`expectedDbState`) — the §10 promise (same IR + input → same RESULT regardless of dialect).

REAL DBs, no mock, NO silent skip: if PG or MySQL is unreachable the runner ERRORS OUT LOUDLY
(exit 3) — it never passes vacuously. Connection config is env-driven (matching
docker-compose.test.yml / the WS6 host defaults):

    TEST_DB_HOST/PORT/USER/PASSWORD/NAME      (Postgres, default localhost:5433)
    TEST_MYSQL_HOST/PORT/USER/PASSWORD/DB     (MySQL,    default localhost:3307)

Emits the SAME machine-readable JSON summary the orchestrator expects as its LAST stdout line:
    {"lang":"py-livedb","suites":{"livedb-pg":{..},"livedb-mysql":{..}},"total_pass",...}
Exit: 0 all pass, 1 any fail, 2 corpus-version mismatch, 3 DB unreachable.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

_PKG_PARENT = str(Path(__file__).resolve().parent.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from litedbmodel_runtime import (  # noqa: E402
    MysqlDriver,
    PostgresDriver,
    execute_bundle,
    execute_transaction_bundle,
    read_bundle,
)

SUPPORTED_CORPUS_VERSION = 2
# A distinct namespace per language so 4 languages share ONE docker stack without cross-talk.
PG_SCHEMA = os.environ.get("LIVEDB_PG_SCHEMA", "scp_py")
MYSQL_DB = os.environ.get("LIVEDB_MYSQL_DB", "scp_py")


def _corpus_path() -> Path:
    env = os.environ.get("LITEDBMODEL_LIVEDB_VECTORS")
    if env:
        return Path(env)
    return Path(__file__).resolve().parent.parent.parent / "conformance" / "vectors-livedb" / "livedb.json"


def _pg_cfg() -> Dict[str, Any]:
    return dict(
        host=os.environ.get("TEST_DB_HOST", "localhost"),
        port=int(os.environ.get("TEST_DB_PORT", "5433")),
        user=os.environ.get("TEST_DB_USER", "testuser"),
        password=os.environ.get("TEST_DB_PASSWORD", "testpass"),
        dbname=os.environ.get("TEST_DB_NAME", "testdb"),
    )


def _mysql_cfg() -> Dict[str, Any]:
    return dict(
        host=os.environ.get("TEST_MYSQL_HOST", "localhost"),
        port=int(os.environ.get("TEST_MYSQL_PORT", "3307")),
        user=os.environ.get("TEST_MYSQL_USER", "testuser"),
        password=os.environ.get("TEST_MYSQL_PASSWORD", "testpass"),
        dbname=os.environ.get("TEST_MYSQL_DB", "testdb"),
    )


# ── value equality (numeric canon, mirror of vectors_runner) ───────────────────


def _numeric_canon(x: Any) -> Any:
    if isinstance(x, dict):
        if len(x) == 1 and "$bigint" in x:
            return int(x["$bigint"])
        return {k: _numeric_canon(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_numeric_canon(v) for v in x]
    return x


def _canon(x: Any) -> str:
    return json.dumps(_numeric_canon(x), sort_keys=True, ensure_ascii=False)


def _eq(a: Any, b: Any) -> bool:
    return _canon(a) == _canon(b)


def _encode(v: Any) -> Any:
    """Encode a runtime value to pure JSON (mirror of vectors_runner.encode_value)."""
    if isinstance(v, bool):
        return v
    if v is None or isinstance(v, (int, float, str)):
        return v
    if isinstance(v, list):
        return [_encode(x) for x in v]
    if isinstance(v, dict):
        return {k: _encode(x) for k, x in v.items()}
    return v


# ── per-dialect table lifecycle ────────────────────────────────────────────────

# The tables the corpus touches (drop order respects FK dependents-first).
_ALL_TABLES = ["post_tags", "comments", "posts", "tags", "docs", "users", "idem", "uniq", "outbox"]


def _reset_pg(driver: "PostgresDriver", schema: List[str]) -> None:
    """Fresh namespace: drop the corpus tables (CASCADE) then apply the vector's DDL.

    Dropping + recreating restarts SERIAL identity at 1, so the tx `entity.id` matches the SQLite
    AUTOINCREMENT reference.
    """
    driver.exec_ddl([f"DROP TABLE IF EXISTS {t} CASCADE" for t in _ALL_TABLES])
    driver.exec_ddl(list(schema))


def _reset_mysql(driver: "MysqlDriver", schema: List[str]) -> None:
    stmts = ["SET FOREIGN_KEY_CHECKS = 0"]
    stmts += [f"DROP TABLE IF EXISTS {t}" for t in _ALL_TABLES]
    stmts += ["SET FOREIGN_KEY_CHECKS = 1"]
    driver.exec_ddl(stmts)
    driver.exec_ddl(list(schema))


def _run_exec(driver, bundle, vector) -> Dict[str, Any]:
    result = _encode(execute_bundle(bundle, dict(vector["input"]), driver))
    ok = _eq(result, vector["expectedResult"])
    return {"ok": ok, "detail": None if ok else f"result {json.dumps(result)} != {json.dumps(vector['expectedResult'])}"}


def _run_read(driver, bundle, vector, expected_key) -> Dict[str, Any]:
    """A read-RELATION EXECUTION vector: run the parent read + batch-load/hydrate ``with`` relations.

    The hydrated shape is compared to the PER-DIALECT golden (``expected_key`` = ``expectedResultPg``
    / ``expectedResultMysql``) — a limited hasMany's ``_rn`` window column is present on MySQL but
    projected away by PG's LATERAL form (the ONE documented dialect divergence in the batch SQL).
    """
    expected = vector[expected_key]
    result = _encode(read_bundle(bundle, dict(vector["input"]), driver, list(vector["with"])))
    ok = _eq(result, expected)
    return {"ok": ok, "detail": None if ok else f"result {json.dumps(result)} != {json.dumps(expected)}"}


def _run_tx(driver, bundle, vector) -> Dict[str, Any]:
    result = _encode(execute_transaction_bundle(bundle, dict(vector["input"]), driver))
    result_ok = _eq(result, vector["expectedResult"])
    state_ok = True
    detail: List[str] = []
    if not result_ok:
        detail.append(f"result {json.dumps(result)} != {json.dumps(vector['expectedResult'])}")
    for s in vector.get("expectedDbState", []) or []:
        got = _encode(driver.prepare(s["query"]).all([]))
        if not _eq(got, s["rows"]):
            state_ok = False
            detail.append(f"db-state '{s['query']}': {json.dumps(got)} != {json.dumps(s['rows'])}")
    ok = result_ok and state_ok
    return {"ok": ok, "detail": None if ok else "; ".join(detail)}


def _run_dialect_leg(dialect: str, driver, reset_fn, corpus, bundle_key, schema_key, read_expected_key) -> Dict[str, int]:
    t = {"pass": 0, "fail": 0}
    sys.stderr.write(f"\nlivedb-{dialect} — {len(corpus['vectors'])} vectors (real {dialect})\n")
    for v in corpus["vectors"]:
        reset_fn(driver, list(v[schema_key]))
        bundle = v[bundle_key]
        try:
            if v["kind"] == "exec":
                r = _run_exec(driver, bundle, v)
            elif v["kind"] == "read":
                r = _run_read(driver, bundle, v, read_expected_key)
            elif v["kind"] == "tx":
                r = _run_tx(driver, bundle, v)
            else:
                r = {"ok": False, "detail": f"unknown kind {v['kind']}"}
        except Exception as e:  # a live-DB failure is a vector FAILURE, never a fake pass
            import traceback

            r = {"ok": False, "detail": f"threw: {e}\n{traceback.format_exc()}"}
        if r["ok"]:
            t["pass"] += 1
            sys.stderr.write(f"  ok  {v['name']}\n")
        else:
            t["fail"] += 1
            sys.stderr.write(f"  XX  {v['name']}\n      {r.get('detail')}\n")
    return t


def main() -> int:
    sys.stderr.write("litedbmodel SCP LIVE-DB conformance — Python runner (real PG + MySQL)\n")
    corpus = json.loads(_corpus_path().read_text(encoding="utf-8"))
    if corpus.get("corpusVersion") != SUPPORTED_CORPUS_VERSION:
        sys.stderr.write(f"FAIL-CLOSED: corpusVersion {corpus.get('corpusVersion')} != {SUPPORTED_CORPUS_VERSION}\n")
        print(json.dumps({"lang": "py-livedb", "suites": {}, "total_pass": 0, "total_fail": 0, "version_mismatch": True}))
        return 2

    # Connect (LOUD failure if unreachable — never skip).
    try:
        pg = PostgresDriver.connect(**_pg_cfg())
        pg.exec_ddl([f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA}", f"SET search_path TO {PG_SCHEMA}"])
    except Exception as e:
        sys.stderr.write(f"FATAL: Postgres unreachable at {_pg_cfg()['host']}:{_pg_cfg()['port']} — {e}\n")
        return 3
    try:
        my = MysqlDriver.connect(**_mysql_cfg())
        my.exec_ddl([f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}", f"USE {MYSQL_DB}"])
    except Exception as e:
        sys.stderr.write(f"FATAL: MySQL unreachable at {_mysql_cfg()['host']}:{_mysql_cfg()['port']} — {e}\n")
        return 3

    try:
        pg_t = _run_dialect_leg("pg", pg, _reset_pg, corpus, "bundlePg", "schemaPg", "expectedResultPg")
        my_t = _run_dialect_leg("mysql", my, _reset_mysql, corpus, "bundleMysql", "schemaMysql", "expectedResultMysql")
    finally:
        pg.close()
        my.close()

    suites = {"livedb-pg": pg_t, "livedb-mysql": my_t}
    total_pass = pg_t["pass"] + my_t["pass"]
    total_fail = pg_t["fail"] + my_t["fail"]
    sys.stderr.write(f"\n{total_pass} passed, {total_fail} failed / {total_pass + total_fail} live-DB vectors\n")
    print(json.dumps({"lang": "py-livedb", "suites": suites, "total_pass": total_pass, "total_fail": total_fail, "version_mismatch": False}))
    return 1 if total_fail > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
