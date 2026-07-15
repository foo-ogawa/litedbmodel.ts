#!/usr/bin/env python3
"""ORM-plan EXECUTOR + live smoke — Python (epic #63).

Port of the PROVEN TS reference (benchmark/crosslang/orm-exec-ts.ts + orm-smoke.ts). Loads the
committed language-neutral artifact benchmark/crosslang/generated/orm-plan.json and executes ALL
19 ORM ops × {sqlite, mysql, postgres} through the SHIPPED litedbmodel_runtime driver seam
(SqliteDriver / PostgresDriver / MysqlDriver — psycopg3 / PyMySQL / stdlib sqlite3), binding the
BAKED per-dialect SQL from the artifact per the bindKind protocol (NO SQL generation here).

Spawn convention (harness registry): the runner subcommand
    python3 benchmark/crosslang/adapters/python/orm_exec.py --orm-plan [--smoke]
`--smoke` runs the standalone 57-cell matrix and exits; without it, it speaks the NDJSON
run/throughput/cost/rss/shutdown protocol over stdin/stdout (case=<opId>, dialect=<dialect>).
"""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent.parent
sys.path.insert(0, str(REPO / "python"))

from litedbmodel_runtime.driver import SqliteDriver  # noqa: E402

ARTIFACT_PATH = HERE.parent.parent / "generated" / "orm-plan.json"

# Isolated namespace (never the shared testdb fixture tables); mirrors the #44 adapter convention.
PG_SCHEMA_NAME = "scp_python_bench"
MYSQL_DB_NAME = "scp_python_bench"

ORM_OPS = None  # filled from artifact


def _pg_cfg():
    return dict(host=os.environ.get("TEST_DB_HOST", "localhost"), port=int(os.environ.get("TEST_DB_PORT", "5433")),
                user=os.environ.get("TEST_DB_USER", "testuser"), password=os.environ.get("TEST_DB_PASSWORD", "testpass"),
                dbname=os.environ.get("TEST_DB_NAME", "testdb"))


def _mysql_cfg():
    return dict(host=os.environ.get("TEST_MYSQL_HOST", "localhost"), port=int(os.environ.get("TEST_MYSQL_PORT", "3307")),
                user=os.environ.get("TEST_MYSQL_USER", "testuser"), password=os.environ.get("TEST_MYSQL_PASSWORD", "testpass"),
                dbname=os.environ.get("TEST_MYSQL_DB", "testdb"))


# ── {{SEQ}} substitution: a per-op-invocation incrementing int for unique-email writes ─────────
_SEQ = 0


def _next_seq() -> int:
    global _SEQ
    v = _SEQ
    _SEQ += 1
    return v


def _subst_one(p, seq):
    # Recursive: a batch write (createMany/upsertMany) carries its records as an ARRAY param (pg
    # UNNEST) or a JSON STRING param (sqlite/mysql), and the {{SEQ}} marker lives INSIDE those — a
    # flat replace leaves batch emails literal → dup-key on the 2nd invocation.
    if isinstance(p, str):
        return p.replace("{{SEQ}}", str(seq)) if "{{SEQ}}" in p else p
    if isinstance(p, list):
        return [_subst_one(e, seq) for e in p]
    return p


def _subst(params, seq):
    return [_subst_one(p, seq) for p in params]


def _strip_returning(sql):
    # Case-insensitive strip of a trailing ` RETURNING …` clause (MySQL has no native RETURNING).
    lo = sql.lower()
    at = lo.rfind(" returning ")
    return sql[:at] if at >= 0 else sql


def _pg_placeholders(sql):
    # Portable seed SQL binds `?`; PG wants `$N` (the pooled driver's _xform then maps $N→%s).
    out = []
    n = 0
    for ch in sql:
        if ch == "?":
            n += 1
            out.append(f"${n}")
        else:
            out.append(ch)
    return "".join(out)


# ── relation bind protocol (mirror bindRelation in orm-exec-ts.ts) ─────────────────────────────
def _distinct_single_keys(stage, parents):
    seen, out = set(), []
    pk = stage["single"]["parentKey"]
    for r in parents:
        k = r.get(pk)
        if k is None:
            continue
        s = str(k)
        if s not in seen:
            seen.add(s)
            out.append(k)
    return out


def _distinct_tuples(stage, parents):
    seen, out = set(), []
    p0, p1 = stage["composite"]["parentKeys"]
    for r in parents:
        k0, k1 = r.get(p0), r.get(p1)
        if k0 is None or k1 is None:
            continue
        s = str(k0) + " " + str(k1)
        if s not in seen:
            seen.add(s)
            out.append((k0, k1))
    return out


def _bind_relation(stage, parents):
    """Bind resolved DISTINCT parent keys/tuples onto stage.sql per bindKind. None = no parents."""
    kind = stage["bindKind"]
    if "single" in stage and stage["single"]:
        keys = _distinct_single_keys(stage, parents)
        if not keys:
            return None
        if kind == "pgArraySingle":
            # pg binds a list as ONE array param; the ::int[] cast is already baked (int keys).
            return {"sql": stage["sql"], "params": [keys], "kind": kind}
        return {"sql": stage["sql"], "params": [json.dumps(keys, separators=(",", ":"))], "kind": kind}  # jsonParam
    tuples = _distinct_tuples(stage, parents)
    if not tuples:
        return None
    if kind == "pgArrayComposite":
        return {"sql": stage["sql"], "params": [[t[0] for t in tuples], [t[1] for t in tuples]], "kind": kind}
    # tupleExpand (sqlite/mysql composite): repeat the group per tuple, flatten params.
    groups = ", ".join(stage["groupTemplate"] for _ in tuples)
    flat = [x for t in tuples for x in t]
    return {"sql": stage["sql"] + groups + stage.get("suffix", ""), "params": flat, "kind": kind}


# ── drivers (all speak the shipped .prepare(sql).all/.run seam) ────────────────────────────────
class OrmDriver:
    def __init__(self, dialect, driver):
        self.dialect = dialect
        self.driver = driver

    def _all(self, sql, params):
        return self.driver.prepare(sql).all(list(params))

    def _run(self, sql, params):
        return self.driver.prepare(sql).run(list(params))

    def run(self, plan):
        return self._read(plan) if plan["kind"] == "read" else self._write(plan)

    def _read(self, plan):
        rows = self._all(plan["reads"][0]["sql"], plan["reads"][0]["params"])
        total = len(rows)
        stage_rows = [rows]
        for stage in plan["relations"]:
            parents = stage_rows[stage["parentStmt"]]
            rel = _bind_relation(stage, parents)
            children = self._all(rel["sql"], rel["params"]) if rel else []
            total += len(children)
            stage_rows.append(children)
        return total

    def _write(self, plan):
        seq = _next_seq()
        self._run("BEGIN", [])
        try:
            returned_id = 0
            n = 0
            for st in plan["statements"]:
                params = _subst(st["params"], seq)
                if st["role"] == "useReturn" and st.get("useReturnAt") is not None:
                    params = [returned_id if i == st["useReturnAt"] else p for i, p in enumerate(params)]
                if st["role"] == "insertReturn":
                    returned_id, params = self._insert_return(st["sql"], params)
                elif self.dialect == "mysql" and " returning " in st["sql"].lower():
                    # A plain upsert carrying RETURNING id: MySQL strips it (no id chained downstream).
                    self._run(_strip_returning(st["sql"]), params)
                else:
                    self._all_or_run(st["sql"], params)
                n += 1
            self._run("COMMIT", [])
            return n
        except Exception:
            self._run("ROLLBACK", [])
            raise

    def _all_or_run(self, sql, params):
        # PG native RETURNING comes back via .all(); other statements via .run().
        if self.dialect == "postgres" and " returning " in sql.lower():
            self._all(sql, params)
        else:
            self._run(sql, params)

    def _insert_return(self, sql, params):
        if self.dialect == "postgres":
            rows = self._all(sql, params)
            rid = int(rows[0]["id"]) if rows else 0
            return rid, params
        # sqlite / mysql: strip RETURNING, run, use lastrowid.
        info = self._run(_strip_returning(sql), params)
        return int(info.last_insert_rowid), params

    def close(self):
        try:
            self.driver.close()
        except Exception:
            pass


def _seed(driver, schema):
    for s in schema.get("drop", []):
        driver.prepare(s).run([])
    for s in schema["ddl"]:
        driver.prepare(s).run([])
    for s in schema["seed"]:
        driver.prepare(s["sql"]).run(list(s["params"]))
    for s in schema.get("seqReset", []) or []:
        driver.prepare(s).run([])


def make_driver(dialect, artifact):
    schema = artifact["schema"][dialect]
    if dialect == "sqlite":
        d = SqliteDriver.in_memory(list(schema["ddl"]))
        # SqliteDriver.in_memory only runs DDL; seed via prepare().run() (autocommit).
        d.conn.execute("PRAGMA foreign_keys = ON")
        for s in schema["seed"]:
            d.prepare(s["sql"]).run(list(s["params"]))
        d.conn.commit()
        d.conn.isolation_level = None  # BEGIN/COMMIT drive real txns for writes
        return OrmDriver("sqlite", d)
    from litedbmodel_runtime import PostgresDriver, MysqlDriver
    if dialect == "postgres":
        d = PostgresDriver.connect(**_pg_cfg())
        d.exec_ddl([f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA_NAME}", f"SET search_path TO {PG_SCHEMA_NAME}"])
    else:
        d = MysqlDriver.connect(**_mysql_cfg())
        d.exec_ddl([f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB_NAME}", f"USE {MYSQL_DB_NAME}"])
    d.exec_ddl(list(schema.get("drop", [])))
    d.exec_ddl(list(schema["ddl"]))
    for s in schema["seed"]:
        sql = _pg_placeholders(s["sql"]) if dialect == "postgres" else s["sql"]
        d.prepare(sql).run(list(s["params"]))
    if schema.get("seqReset"):
        d.exec_ddl(list(schema["seqReset"]))
    return OrmDriver(dialect, d)


def load_artifact():
    global ORM_OPS
    art = json.loads(ARTIFACT_PATH.read_text())
    ORM_OPS = art["ops"]
    return art


# ── standalone smoke (mirror orm-smoke.ts) ─────────────────────────────────────────────────────
def smoke():
    art = load_artifact()
    dialects = art["dialects"]
    drivers = {d: make_driver(d, art) for d in dialects}
    rows_by_op = {}
    passed = failed = 0
    for op in ORM_OPS:
        rows_by_op[op["id"]] = {}
        for d in dialects:
            try:
                n = drivers[d].run(art["plans"][op["id"]][d])
                rows_by_op[op["id"]][d] = n
                passed += 1
            except Exception as e:  # noqa: BLE001
                rows_by_op[op["id"]][d] = f"ERR: {str(e).splitlines()[0] if str(e) else type(e).__name__}"
                failed += 1
    print("\n19 ORM ops x 3 DBs — rows/op (writes report statements executed) [python]:\n")

    def pad(s, n):
        return str(s).ljust(n)
    print(pad("op", 42), pad("sqlite", 14), pad("mysql", 14), "postgres")
    for op in ORM_OPS:
        r = rows_by_op[op["id"]]
        print(pad(("W " if op["write"] else "R ") + op["label"], 42), pad(r["sqlite"], 14), pad(r["mysql"], 14), r["postgres"])
    total = passed + failed
    print(f"\n{passed}/{total} cells green ({len(ORM_OPS)} ops x 3 DBs = {len(ORM_OPS) * 3}).")
    for d in dialects:
        drivers[d].close()
    if failed:
        print(f"\nSMOKE FAILED: {failed} cell(s) errored (see ERR above).", file=sys.stderr)
        sys.exit(1)
    print("SMOKE PASS [python]: all cells DB-backed on all 3 real DBs.")


# ── NDJSON protocol (harness registry drives this over stdin/stdout) ───────────────────────────
def _write(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def protocol():
    art = load_artifact()
    live = {}

    def driver_for(d):
        if d not in live:
            live[d] = make_driver(d, art)
        return live[d]

    _write({"kind": "ready", "language": "python", "impl": "runtime", "readyAtEpochMs": time.time() * 1000.0})
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except Exception as e:  # noqa: BLE001
            _write({"kind": "error", "message": f"bad request line: {e}"})
            continue
        try:
            kind = req["kind"]
            if kind == "shutdown":
                for d in live.values():
                    d.close()
                sys.exit(0)
            if kind == "rss":
                try:
                    import resource
                    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                    if sys.platform != "darwin":
                        rss *= 1024
                except Exception:
                    rss = 0
                _write({"kind": "rss", "rssBytes": rss})
                continue
            case, dialect = req["case"], req["dialect"]
            plan = art["plans"][case][dialect]
            drv = driver_for(dialect)
            if kind == "run":
                warmup = req.get("warmup", 0)
                iters = req.get("iterations", 1)
                for _ in range(warmup):
                    drv.run(plan)
                samples = []
                for _ in range(iters):
                    t0 = time.perf_counter()
                    drv.run(plan)
                    samples.append((time.perf_counter() - t0) * 1000.0)
                _write({"kind": "run", "case": case, "dialect": dialect, "samplesMs": samples})
            elif kind == "throughput":
                iters = req.get("iterations", 1)
                t0 = time.perf_counter()
                for _ in range(iters):
                    drv.run(plan)
                _write({"kind": "throughput", "case": case, "dialect": dialect,
                        "elapsedMs": (time.perf_counter() - t0) * 1000.0, "completed": iters})
            elif kind == "cost":
                rows = drv.run(plan)
                # queries/op derived from the plan shape (same for every language — the SAME plan).
                queries = (len(plan["reads"]) + len(plan["relations"])) if plan["kind"] == "read" else len(plan["statements"])
                _write({"kind": "cost", "case": case, "dialect": dialect, "queries": queries, "rows": rows})
            else:
                _write({"kind": "error", "message": f"unknown kind {kind}"})
        except Exception as e:  # noqa: BLE001
            _write({"kind": "error", "message": str(e), "stack": traceback.format_exc()})


def main():
    args = sys.argv[1:]
    if "--orm-plan" not in args and "--smoke" not in args:
        print("usage: orm_exec.py --orm-plan [--smoke]", file=sys.stderr)
        sys.exit(2)
    if "--smoke" in args:
        smoke()
    else:
        protocol()


if __name__ == "__main__":
    main()
