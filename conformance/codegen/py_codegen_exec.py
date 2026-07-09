#!/usr/bin/env python3
"""litedbmodel v2 SCP — Python codegen executor (WS7f, #35; spec §9 exec-mode 3).

The Python leg of the mode-3 codegen byte-identity proof. It is invoked by the TS codegen
conformance runner (``conformance/codegen/codegen-runner.ts``) with a JSON job on argv[1]:

    {
      "modulePath":  "<abs path to the bc-generated behaviors module .py>",
      "companion":   {operations, dialect, optionalHeads, relations, transaction?},
      "input":       <bigint-encoded input scope>,
      "schema":      [<DDL/seed statements>],
      "expectedResult": <bigint-encoded reference result>,
      "kind":        "exec" | "tx",
      "expectedDbState": [{query, rows}]   # tx only
    }

It IMPORTS the bc-generated module (so its load-time fail-closed checks — spec-version /
fingerprint — actually run), pairs the generated ``bind(handlers)`` with the SAME SQL handlers the
Python THIN-RUNTIME builds from the companion (boundary injection), EXECUTES it against a freshly
seeded in-process SQLite, and prints the canonical result. This is a REAL cross-language execution
of the emitted source — not a stand-in — so the TS runner can assert the Python codegen output is
byte-identical to the frozen vector (== the mode-2 thin-runtime).

A ``tx`` bundle's execution path is the derived transaction plan (not ``bind()``), so it is driven
through the thin-runtime's ``execute_transaction_bundle`` over the companion-reassembled bundle —
structurally identical to mode-2. Prints one JSON line: {"result": <encoded>, "dbState": [...]}.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, os.path.join(REPO, "python"))

from litedbmodel_runtime.driver import SqliteDriver  # noqa: E402
from litedbmodel_runtime.dialect import dialect_for  # noqa: E402
from litedbmodel_runtime.runtime import (  # noqa: E402
    _build_handlers,
    _normalize_input,
    execute_transaction_bundle,
)


def encode_value(v):
    """bigint-safe encode (Python ints are unbounded → emit plain numbers; mirror the runner)."""
    if isinstance(v, bool):
        return v
    if v is None or isinstance(v, (int, float, str)):
        return v
    if isinstance(v, list):
        return [encode_value(x) for x in v]
    if isinstance(v, dict):
        return {k: encode_value(x) for k, x in v.items()}
    return v


def decode_value(v):
    if isinstance(v, dict):
        if len(v) == 1 and "$bigint" in v:
            return int(v["$bigint"])
        return {k: decode_value(x) for k, x in v.items()}
    if isinstance(v, list):
        return [decode_value(x) for x in v]
    return v


def _import_generated(module_path: str):
    spec = importlib.util.spec_from_file_location("lm_generated_behaviors", module_path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # runs the module's load-time fail-closed checks
    return mod


def main() -> int:
    job = json.loads(sys.argv[1])
    companion = job["companion"]
    dialect = dialect_for(companion["dialect"])
    driver = SqliteDriver.in_memory(list(job["schema"]))
    try:
        if job["kind"] == "exec":
            mod = _import_generated(job["modulePath"])
            component = mod.IR["components"][0]
            handlers = _build_handlers(driver, companion["operations"], dialect)
            normalized = _normalize_input(component, list(companion.get("optionalHeads", [])), decode_value(job["input"]))
            bound = mod.bind(handlers)
            name = mod.COMPONENT_NAMES[0]
            result = bound[name](normalized)
            print(json.dumps({"result": encode_value(result), "dbState": []}))
            return 0

        # tx: reassemble the §8 bundle from the baked IR (via the generated module) + companion,
        # then drive the SAME thin-runtime transaction path (the plan, not bind()).
        mod = _import_generated(job["modulePath"])
        bundle = {
            "irVersion": mod.IR["irVersion"],
            "exprVersion": mod.IR["exprVersion"],
            "dialect": companion["dialect"],
            "component": mod.IR["components"][0],
            "operations": companion["operations"],
            "optionalHeads": list(companion.get("optionalHeads", [])),
            "relations": companion.get("relations", {}),
            "transaction": companion["transaction"],
        }
        result = execute_transaction_bundle(bundle, decode_value(job["input"]), driver)
        db_state = []
        for s in job.get("expectedDbState", []) or []:
            rows = driver.prepare(s["query"]).all([])
            db_state.append({"query": s["query"], "rows": encode_value(rows)})
        print(json.dumps({"result": encode_value(result), "dbState": db_state}))
        return 0
    finally:
        driver.close()


if __name__ == "__main__":
    sys.exit(main())
