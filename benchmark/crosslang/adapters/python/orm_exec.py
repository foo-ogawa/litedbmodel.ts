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
import sqlite3
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent.parent
sys.path.insert(0, str(REPO / "python"))

from litedbmodel_runtime.driver import SqliteDriver  # noqa: E402

ARTIFACT_PATH = HERE.parent.parent / "generated" / "orm-plan.json"

# Isolated namespace (never the shared testdb fixture tables); mirrors the #44 adapter convention.
PG_SCHEMA_NAME = "scp_python_bench"
MYSQL_DB_NAME = "scp_python_bench"

# ── RAW-driver BASELINE (task: MEASURE litedbmodel_runtime's over-driver overhead) ─────────────
# The baseline runs the IDENTICAL final SQL + params the runtime issues (assembled by the SAME
# shared OrmDriver._read/_write code below → byte-identical SQL) but binds them through the BARE
# database driver — no litedbmodel_runtime seam, no per-cell de-box. `runtime÷baseline` isolates
# litedbmodel's cost over the raw driver. The baseline gets its OWN isolated PG schema / MySQL db so
# the two impls never clobber each other's seeded tables (they may run back-to-back).
PG_BASELINE_SCHEMA = f"{PG_SCHEMA_NAME}_baseline"
MYSQL_BASELINE_DB = f"{MYSQL_DB_NAME}_baseline"

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
def _row_get(r, key):
    # Named-column access on a parent row for the relation key-extraction (shared assembly). The
    # runtime seam returns plain dicts (`.get`); the RAW baseline returns the driver's OWN native
    # named-row type — psycopg dict_row / PyMySQL DictCursor are dicts, sqlite3.Row is indexed by
    # name via `[]` (no `.get`). This shim reads either without adding a de-box layer to the row.
    if isinstance(r, dict):
        return r.get(key)
    try:
        return r[key]
    except (IndexError, KeyError):
        return None


def _distinct_single_keys(stage, parents):
    seen, out = set(), []
    pk = stage["single"]["parentKey"]
    for r in parents:
        k = _row_get(r, pk)
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
        k0, k1 = _row_get(r, p0), _row_get(r, p1)
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
    def __init__(self, dialect, driver, impl="runtime"):
        self.dialect = dialect
        self.driver = driver
        # `runtime` = shipped litedbmodel_runtime.driver seam; `raw` = bare-driver baseline. The seam
        # object in `self.driver` carries the difference; the read/write assembly below is IDENTICAL
        # for both impls, so the SQL + params it issues are byte-identical (only the low-level
        # prepare/execute differs). `impl` is kept for isolation-naming + honest reporting.
        self.impl = impl

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


# ── RAW-driver BASELINE seams (mirror the .prepare(sql).all/.run shape, but BARE) ───────────────
# Each raw seam speaks the SAME `.prepare(sql).all(params)/.run(params)/.close()` surface the shipped
# runtime driver does, so OrmDriver's read/write assembly is reused UNCHANGED (byte-identical SQL).
# The ONLY difference vs the runtime seam is the low-level per-statement execution:
# The row shape is the DRIVER'S OWN native named-row type (sqlite3.Row / psycopg dict_row / PyMySQL
# DictCursor) — NOT litedbmodel's `dict(zip(cols, row))` reconstruction + per-cell `_scalar()` de-box
# loop, which is exactly the runtime cost being measured. Named columns are load-bearing for the
# SHARED relation-key extraction (`_row_get`) and pg id-chaining (`rows[0]["id"]`), so a bare tuple
# row would break the shared assembly; the driver-native factory is the honest baseline.
#   * sqlite : stdlib sqlite3, `conn.row_factory = sqlite3.Row` (driver-native); `?` binds natively
#              (NO placeholder translation) — drops the runtime seam's explicit dict(zip) mapping.
#   * pg     : bare psycopg, `dict_row` factory; same driver-MANDATED `$N`→`%s` translation the
#              runtime does, but drops the runtime's per-cell `_scalar()` de-box loop.
#   * mysql  : bare PyMySQL, DictCursor; same driver-MANDATED `?`→`%s` translation.
# `run()` returns a RunInfo-shaped object (`.last_insert_rowid`) so `_insert_return` reads it as-is.


class _RawRunInfo:
    __slots__ = ("last_insert_rowid",)

    def __init__(self, last_insert_rowid):
        self.last_insert_rowid = last_insert_rowid


class _RawSqlitePrepared:
    __slots__ = ("_conn", "_sql")

    def __init__(self, conn, sql):
        self._conn = conn
        self._sql = sql

    def all(self, params):
        # BARE: raw cursor + fetchall() tuples — NO dict(zip) row-mapping the runtime seam applies.
        return self._conn.execute(self._sql, tuple(params)).fetchall()

    def run(self, params):
        cur = self._conn.execute(self._sql, tuple(params))
        return _RawRunInfo(cur.lastrowid if cur.lastrowid is not None else 0)


class _RawSqliteSeam:
    """Bare stdlib sqlite3 seam (no litedbmodel_runtime). `?` binds positionally (no translation)."""

    def __init__(self, conn):
        self.conn = conn

    def prepare(self, sql):
        return _RawSqlitePrepared(self.conn, sql)

    def close(self):
        self.conn.close()


class _RawDbapiPrepared:
    __slots__ = ("_conn", "_sql", "_xform")

    def __init__(self, conn, sql, xform):
        self._conn = conn
        self._sql = sql
        self._xform = xform

    def all(self, params):
        cur = self._conn.cursor()
        cur.execute(self._xform(self._sql), tuple(params))
        rows = cur.fetchall() if cur.description is not None else []
        cur.close()
        return rows

    def run(self, params):
        cur = self._conn.cursor()
        cur.execute(self._xform(self._sql), tuple(params))
        last = getattr(cur, "lastrowid", None)
        cur.close()
        return _RawRunInfo(last if last is not None else 0)


class _RawDbapiSeam:
    """Bare DB-API seam (raw psycopg / PyMySQL) — driver-MANDATED placeholder translation only,
    NO litedbmodel_runtime `_scalar` de-box. Autocommit ON so the shared BEGIN…COMMIT bracket real
    transactions, exactly as the runtime driver's pooled connections do."""

    def __init__(self, conn, xform):
        self.conn = conn
        self._xform = xform

    def prepare(self, sql):
        return _RawDbapiPrepared(self.conn, sql, self._xform)

    def close(self):
        try:
            self.conn.close()
        except Exception:  # noqa: BLE001
            pass


def _raw_dollar_to_pyformat(sql):
    # Driver-mandated: psycopg binds `%s`, so PG's rendered `$N` must be translated (same as the
    # runtime's _dollar_to_pyformat) — the SQL STATEMENT is byte-identical, only the placeholder
    # token the bare driver requires differs. Literal `%` doubled for safety.
    import re as _re
    return _re.sub(r"\$\d+", "%s", sql.replace("%", "%%"))


def _raw_qmark_to_pyformat(sql):
    # Driver-mandated: PyMySQL binds `%s`, so MySQL's rendered `?` must be translated (same as the
    # runtime's _qmark_to_pyformat).
    return sql.replace("%", "%%").replace("?", "%s")


def make_baseline_driver(dialect, artifact):
    """Build the RAW-driver baseline for `dialect`, seeded IDENTICALLY into an ISOLATED namespace
    (its own PG schema / MySQL db) so it never clobbers the runtime driver's tables. Raises on a
    connect/seed failure — the caller treats that as a per-dialect baseline skip (never a whole-cell
    skip: the runtime metrics for that dialect still stand)."""
    schema = artifact["schema"][dialect]
    if dialect == "sqlite":
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row  # driver-native named rows (NOT litedbmodel dict(zip)/_scalar)
        conn.execute("PRAGMA foreign_keys = ON")
        for s in schema["ddl"]:
            conn.execute(s)
        for s in schema["seed"]:
            conn.execute(s["sql"], tuple(s["params"]))
        conn.commit()
        conn.isolation_level = None  # BEGIN/COMMIT drive real txns for writes (mirror the runtime path)
        return OrmDriver("sqlite", _RawSqliteSeam(conn), impl="raw")
    if dialect == "postgres":
        import psycopg
        cfg = _pg_cfg()
        # Bootstrap the isolated baseline schema on a throwaway autocommit connection, then pin it.
        conn = psycopg.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                               password=cfg["password"], dbname=cfg["dbname"], autocommit=True)
        conn.row_factory = psycopg.rows.dict_row  # driver-native named rows (NOT litedbmodel _scalar)
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {PG_BASELINE_SCHEMA}")
        cur.execute(f"SET search_path TO {PG_BASELINE_SCHEMA}")
        for s in schema.get("drop", []):
            cur.execute(s)
        for s in schema["ddl"]:
            cur.execute(s)
        for s in schema["seed"]:
            cur.execute(_raw_dollar_to_pyformat(_pg_placeholders(s["sql"])), tuple(s["params"]))
        for s in schema.get("seqReset", []) or []:
            cur.execute(s)
        cur.close()
        return OrmDriver("postgres", _RawDbapiSeam(conn, _raw_dollar_to_pyformat), impl="raw")
    # mysql
    import pymysql
    cfg = _mysql_cfg()
    conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user=cfg["user"],
                           password=cfg["password"], autocommit=True,
                           cursorclass=pymysql.cursors.DictCursor)  # driver-native named rows
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_BASELINE_DB}")
    cur.execute(f"USE {MYSQL_BASELINE_DB}")
    for s in schema.get("drop", []):
        cur.execute(s)
    for s in schema["ddl"]:
        cur.execute(s)
    for s in schema["seed"]:
        cur.execute(_raw_qmark_to_pyformat(s["sql"]), tuple(s["params"]))
    for s in schema.get("seqReset", []) or []:
        cur.execute(s)
    cur.close()
    return OrmDriver("mysql", _RawDbapiSeam(conn, _raw_qmark_to_pyformat), impl="raw")


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


# ── STANDALONE CSV bench (no protocol) ─────────────────────────────────────────────────────────
# ONE standalone process runs ALL 19 ops × 3 dialects, self-measures, and writes a FLAT CSV to
# benchmark/crosslang/.results/python.csv. The collector (collect.ts) reads the CSVs → CROSS-LANG.md.
# CSV schema: language,case,dialect,metric,value   (RAW values only — collector owns the math).
LANGUAGE = "python"
RESULTS_DIR = HERE.parent.parent / ".results"


def _proc_rss_bytes():
    try:
        import resource
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return rss if sys.platform == "darwin" else rss * 1024
    except Exception:  # noqa: BLE001
        return 0


def _csv_field(v):
    s = str(v)
    return '"' + s.replace('"', '""') + '"' if any(c in s for c in ',"\n') else s


def bench():
    warmup = int(os.environ.get("BENCH_WARMUP", "50"))
    iters = int(os.environ.get("BENCH_ITER", "300"))
    tp_iters = int(os.environ.get("BENCH_TP_ITER", str(min(iters, 2000))))

    spawned_at = time.time() * 1000.0
    art = load_artifact()
    dialects = art["dialects"]
    # cold = process start → runtime ready (interpreter + module/artifact load), before any connect.
    cold_ms = max(0.0, (time.time() * 1000.0) - spawned_at)

    rows = ["language,case,dialect,metric,value"]

    def emit(case, dialect, metric, value):
        rows.append(f"{LANGUAGE},{case},{dialect},{metric},{_csv_field(value)}")

    live = {}
    baselines = {}
    for dialect in dialects:
        try:
            drv = live[dialect] = make_driver(dialect, art)
        except Exception as e:  # noqa: BLE001
            reason = str(e).splitlines()[0] if str(e) else type(e).__name__
            for op in ORM_OPS:
                emit(op["id"], dialect, "skipped", f"{dialect} unreachable ({reason})")
            continue
        # The bare-driver BASELINE (same real driver, same SQL, no litedbmodel_runtime). A baseline
        # connect/seed failure is NOT a whole-cell skip — the runtime numbers still stand; only the
        # `baseline_latency_ms` rows for this dialect drop (the ÷raw ratio can't be computed for it).
        baseline = None
        try:
            baseline = baselines[dialect] = make_baseline_driver(dialect, art)
        except Exception as e:  # noqa: BLE001
            reason = str(e).splitlines()[0] if str(e) else type(e).__name__
            sys.stderr.write(f"[{LANGUAGE}] baseline {dialect} unreachable ({reason}) — runtime metrics unaffected\n")
        for op in ORM_OPS:
            case = op["id"]
            plan = art["plans"][case][dialect]
            try:
                # cost (fairness): queries/op from the plan shape; rows/op = executor's returned count.
                queries = (len(plan["reads"]) + len(plan["relations"])) if plan["kind"] == "read" else len(plan["statements"])
                rows_count = drv.run(plan)
                emit(case, dialect, "cost_queries", queries)
                emit(case, dialect, "cost_rows", rows_count)
                # latency: warmup, then one row PER timed iteration.
                for _ in range(warmup):
                    drv.run(plan)
                for _ in range(iters):
                    t0 = time.perf_counter()
                    drv.run(plan)
                    emit(case, dialect, "latency_ms", (time.perf_counter() - t0) * 1000.0)
                # throughput: a tight loop, raw elapsed + completed.
                t0 = time.perf_counter()
                for _ in range(tp_iters):
                    drv.run(plan)
                emit(case, dialect, "throughput_elapsed_ms", (time.perf_counter() - t0) * 1000.0)
                emit(case, dialect, "throughput_completed", tp_iters)
                # baseline latency: the IDENTICAL SQL/params through the BARE driver (no runtime seam),
                # SAME warmup + timed iterations → runtime÷baseline = litedbmodel's over-driver cost.
                # Emitted as `baseline_latency_ms`; the collector splits it into the `impl: baseline`
                # cell. A per-op baseline error must NOT abort the runtime metrics already emitted.
                if baseline is not None:
                    try:
                        for _ in range(warmup):
                            baseline.run(plan)
                        for _ in range(iters):
                            b0 = time.perf_counter()
                            baseline.run(plan)
                            emit(case, dialect, "baseline_latency_ms", (time.perf_counter() - b0) * 1000.0)
                    except Exception as e:  # noqa: BLE001
                        emit(case, dialect, "baseline_skipped", str(e).splitlines()[0] if str(e) else type(e).__name__)
            except Exception as e:  # noqa: BLE001
                emit(case, dialect, "skipped", str(e).splitlines()[0] if str(e) else type(e).__name__)

    emit("", "", "cold_ms", cold_ms)
    emit("", "", "rss_bytes", _proc_rss_bytes())
    emit("", "", "warmup", warmup)

    for d in live.values():
        d.close()
    for d in baselines.values():
        d.close()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULTS_DIR / f"{LANGUAGE}.csv"
    out.write_text("\n".join(rows) + "\n")
    sys.stderr.write(f"[{LANGUAGE}] wrote {out} ({len(rows) - 1} rows)\n")


def main():
    args = sys.argv[1:]
    if "--smoke" in args:
        smoke()
    else:
        bench()


if __name__ == "__main__":
    main()
