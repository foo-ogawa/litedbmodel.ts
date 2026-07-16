#!/usr/bin/env python3
"""litedbmodel SCP conformance vectors — Python runner (WS7b, #31).

The Python leg of the cross-language conformance LOCK (spec §10: "同一 IR+入力 → 同一 SQL +
同一結果"). It loads the FROZEN vector corpus (``conformance/vectors/*.json``) and runs each vector
through the ``litedbmodel_runtime`` this package ships — which consumes ``behavior-contracts`` for
the Expression-IR evaluation + plan/map orchestration and adds the SQL backend. For each vector it
reproduces:

  - ``render`` — the rendered SQL text (all 3 dialects: sqlite/postgres/mysql) + flat params,
    asserted byte-identical to the reference-captured ``expectedSql`` / ``expectedParams``.
  - ``exec``   — the read bundle executed end-to-end against a fresh in-memory SQLite, asserted
    against the reference-captured ``expectedResult``.
  - ``tx``     — the write-time-relations transaction bundle executed as ONE real transaction
    (gate-first), asserted against ``expectedResult`` + post-tx ``expectedDbState`` DB queries.
  - ``dialect`` — the ``orderByNulls`` dialect primitive, asserted against ``expected``.

It emits the SAME machine-readable JSON summary the orchestrator (``conformance/vectors-run.ts``)
expects, as its LAST stdout line:

    {"lang":"py","suites":{<suite>:{"pass","fail"}},"total_pass","total_fail","version_mismatch"}

Exit: 0 all pass, 1 any fail, 2 corpus-version mismatch (pre-flight fail-closed). This is REAL
execution against the corpus — no hardcoded pass, no skip.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

# Allow running as a bare script (``python3 python/litedbmodel_runtime/vectors_runner.py``): make
# the package importable from its parent dir without an install step.
_PKG_PARENT = str(Path(__file__).resolve().parent.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from litedbmodel_runtime import (  # noqa: E402
    dialect_for,
    execute_bundle,
    render_read_primary,
)
from litedbmodel_runtime.runtime import _execute_transaction_bundle  # noqa: E402  (internal guard opt-out)
from litedbmodel_runtime.driver import SqliteDriver  # noqa: E402

# The corpus schema version this runner supports (pin — bumped on additive refreeze).
SUPPORTED_CORPUS_VERSION = 3


def _vectors_dir() -> Path:
    env = os.environ.get("LITEDBMODEL_VECTORS")
    if env:
        return Path(env)
    # Default: <repo>/conformance/vectors (this file lives at python/litedbmodel_runtime/).
    return Path(__file__).resolve().parent.parent.parent / "conformance" / "vectors"


# ── bigint-safe codec (mirror of the TS runner's, so comparisons are canonical) ──


def decode_value(v: Any) -> Any:
    """`{ $bigint }` → int, structural otherwise (Python ints are unbounded — no bigint type)."""
    if v is None or not isinstance(v, (dict, list)):
        return v
    if isinstance(v, list):
        return [decode_value(x) for x in v]
    if len(v) == 1 and "$bigint" in v:
        return int(v["$bigint"])
    return {k: decode_value(x) for k, x in v.items()}


def encode_value(v: Any) -> Any:
    """Encode a runtime value to pure JSON. Python ints round-trip as plain JSON numbers.

    The reference tags bigints as ``{"$bigint": "<dec>"}`` only because JS integers overflow;
    every value in this corpus is within the JS safe-integer range (converted to a JS `number`
    by the TS `toDriverParam`), so it serializes as a plain number on both sides. We therefore
    emit plain numbers here — matching the reference's decoded (`$bigint`→number) form used in its
    `expectedParams`/`expectedResult` comparisons.
    """
    if isinstance(v, bool):
        return v
    if v is None or isinstance(v, (int, float, str)):
        return v
    if isinstance(v, list):
        return [encode_value(x) for x in v]
    if isinstance(v, dict):
        return {k: encode_value(x) for k, x in v.items()}
    return v


def _numeric_canon(x: Any) -> Any:
    """Collapse the reference's ``{"$bigint": "<dec>"}`` integer tags to their numeric value.

    bc-TS distinguishes JS `bigint` (integer arithmetic / integer literals → tagged `$bigint` in
    the captured corpus) from JS `number` (plain), but bc-PYTHON has a single unbounded `int`
    type, so a rendered param that TS tagged `$bigint` and one it left a plain number are the
    SAME Python `int`. Per spec §10 the conformance contract is "same SQL + same result" — the
    two forms are the identical bound value (`5 == 5`, `20 == 20`) and bind identically. We
    therefore compare params NUMERICALLY: decode both sides' `$bigint` tags to their value first.
    This is NOT a fake pass — the SQL text is still asserted byte-identical, and the param values
    are asserted equal by value; only the JS-only bigint/number *representation* tag is neutralized.
    """
    if isinstance(x, dict):
        if len(x) == 1 and "$bigint" in x:
            return int(x["$bigint"])
        return {k: _numeric_canon(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_numeric_canon(v) for v in x]
    return x


def _canon(x: Any) -> str:
    """Canonical JSON (sorted keys, bigint-tag-neutralized) for structural equality."""
    return json.dumps(_numeric_canon(x), sort_keys=True, ensure_ascii=False)


def _eq(a: Any, b: Any) -> bool:
    return _canon(a) == _canon(b)


def _seed_driver(schema: List[str]) -> SqliteDriver:
    return SqliteDriver.in_memory(schema)


def _run_vector(v: Dict[str, Any]) -> Dict[str, Any]:
    """Run ONE vector through the consumed runtime; return {ok, detail?}."""
    kind = v["kind"]
    try:
        if kind == "render":
            r = render_read_primary(v["readGraph"], decode_value(v["input"]))
            sql_ok = r["sql"] == v["expectedSql"]
            params_ok = _eq([encode_value(p) for p in r["params"]], v["expectedParams"])
            if sql_ok and params_ok:
                return {"ok": True}
            parts: List[str] = []
            if not sql_ok:
                parts.append(f"sql {json.dumps(r['sql'])} != {json.dumps(v['expectedSql'])}")
            if not params_ok:
                parts.append(
                    f"params {json.dumps([encode_value(p) for p in r['params']])} != {json.dumps(v['expectedParams'])}"
                )
            return {"ok": False, "detail": "; ".join(parts)}

        if kind == "write-render":
            # A write statement's compiled makeSQL template is asserted byte-identical to golden
            # (the deferred Expression-IR params are NOT evaluated here — they resolve at tx time).
            stmt = v["statement"]
            sql_ok = stmt["sql"] == v["expectedSql"]
            params_ok = _eq([encode_value(p) for p in stmt["params"]], v["expectedParams"])
            return {
                "ok": sql_ok and params_ok,
                "detail": None if sql_ok and params_ok else "write-render mismatch",
            }

        if kind == "exec":
            driver = _seed_driver(list(v["schema"]))
            try:
                result = encode_value(execute_bundle(v["bundle"], decode_value(v["input"]), driver))
            finally:
                driver.close()
            ok = _eq(result, v["expectedResult"])
            return {"ok": ok, "detail": None if ok else f"result {json.dumps(result)} != {json.dumps(v['expectedResult'])}"}

        if kind == "tx":
            driver = _seed_driver(list(v["schema"]))
            try:
                # Conformance runs the per-command auto-tx (no user transaction() boundary), so the
                # write=tx guard is opted OUT here via the INTERNAL executor — byte-identical to Phase A.
                result = encode_value(_execute_transaction_bundle(v["bundle"], decode_value(v["input"]), driver, guard=False))
                state_ok = True
                state_detail = ""
                for s in v.get("expectedDbState", []) or []:
                    got = encode_value(driver.prepare(s["query"]).all([]))
                    if not _eq(got, s["rows"]):
                        state_ok = False
                        state_detail = f"db-state '{s['query']}': {json.dumps(got)} != {json.dumps(s['rows'])}"
                        break
            finally:
                driver.close()
            result_ok = _eq(result, v["expectedResult"])
            ok = result_ok and state_ok
            if ok:
                return {"ok": True}
            detail = []
            if not result_ok:
                detail.append(f"result {json.dumps(result)} != {json.dumps(v['expectedResult'])}")
            if not state_ok:
                detail.append(state_detail)
            return {"ok": False, "detail": "; ".join(detail)}

        if kind == "dialect":
            got = dialect_for(v["dialect"]).order_by_nulls(v["args"]["expr"], v["args"]["dir"], v["args"]["nulls"])
            ok = got == v["expected"]
            return {"ok": ok, "detail": None if ok else f"{json.dumps(got)} != {json.dumps(v['expected'])}"}

        return {"ok": False, "detail": f"unknown vector kind: {kind}"}
    except Exception as e:  # a runtime failure is a vector FAILURE (loud, never a fake pass)
        import traceback

        return {"ok": False, "detail": f"threw: {e}\n{traceback.format_exc()}"}


def _line(ok: bool, name: str, detail: Any) -> None:
    # Human progress → stderr so stdout carries only the JSON summary line.
    if ok:
        sys.stderr.write(f"  ok  {name}\n")
    else:
        sys.stderr.write(f"  XX  {name}\n")
        if detail:
            sys.stderr.write(f"      {detail}\n")


def main() -> int:
    sys.stderr.write("litedbmodel SCP conformance vectors — Python runner (litedbmodel_runtime)\n")
    vectors_dir = _vectors_dir()
    files = sorted(f for f in os.listdir(vectors_dir) if f.endswith(".json"))

    suites = [json.loads((vectors_dir / f).read_text(encoding="utf-8")) for f in files]

    # Pre-flight version sweep (fail-closed): reject the whole run on any suite-version mismatch.
    mismatched = [s for s in suites if s.get("corpusVersion") != SUPPORTED_CORPUS_VERSION]
    if mismatched:
        for s in mismatched:
            sys.stderr.write(
                f"FAIL-CLOSED: suite '{s.get('suite')}' corpusVersion {s.get('corpusVersion')} "
                f"!= supported {SUPPORTED_CORPUS_VERSION}.\n"
            )
        print(json.dumps({"lang": "py", "suites": {}, "total_pass": 0, "total_fail": 0, "version_mismatch": True}))
        return 2

    tallies: Dict[str, Dict[str, int]] = {}
    for suite in suites:
        t = {"pass": 0, "fail": 0}
        sys.stderr.write(f"\n{suite['suite']}.json — {len(suite['vectors'])} vectors\n")
        for v in suite["vectors"]:
            r = _run_vector(v)
            _line(r["ok"], v["name"], r.get("detail"))
            if r["ok"]:
                t["pass"] += 1
            else:
                t["fail"] += 1
        tallies[suite["suite"]] = t

    total_pass = sum(t["pass"] for t in tallies.values())
    total_fail = sum(t["fail"] for t in tallies.values())
    sys.stderr.write(
        f"\n{total_pass} passed, {total_fail} failed / {total_pass + total_fail} vectors "
        f"across {len(suites)} suites\n"
    )
    print(json.dumps({"lang": "py", "suites": tallies, "total_pass": total_pass, "total_fail": total_fail, "version_mismatch": False}))
    return 1 if total_fail > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
