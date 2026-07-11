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
fingerprint — actually run) and verifies the baked IR literal matches the companion's portable IR,
then reassembles the §8 STATIC makeSQL bundle from the SQL catalog companion (the companion IS the
bundle) and EXECUTES it through the SAME static-makeSQL thin-runtime the mode-2 leg uses
(``execute_bundle`` for a read/exec bundle, ``execute_transaction_bundle`` for a tx bundle), against
a freshly seeded in-process SQLite. This is the Python analogue of the TS ``codegenExecuteBundleForTest``
(which re-executes the SAME static bundle via ``executeBundle``) — a REAL cross-language execution of
the emitted artifact, so the TS runner can assert the Python codegen output is byte-identical to the
frozen vector (== the mode-2 thin-runtime).

Prints one JSON line: {"result": <encoded>, "dbState": [...]}.
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
from litedbmodel_runtime.runtime import (  # noqa: E402
    execute_bundle,
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


def _reassemble_bundle(companion: dict) -> dict:
    """Rebuild the §8 STATIC makeSQL bundle from the SQL catalog companion (the companion IS the
    bundle — spec §9). Mirrors the TS runner's reassembly: dialect + optionalHeads + relations +
    whichever of readGraph / statement / transaction the companion carries."""
    bundle = {
        "dialect": companion["dialect"],
        "optionalHeads": list(companion.get("optionalHeads", [])),
        "relations": companion.get("relations", {}),
    }
    if "readGraph" in companion:
        bundle["readGraph"] = companion["readGraph"]
    if "statement" in companion:
        bundle["statement"] = companion["statement"]
    if "transaction" in companion:
        bundle["transaction"] = companion["transaction"]
    return bundle


def main() -> int:
    job = json.loads(sys.argv[1])
    companion = job["companion"]
    driver = SqliteDriver.in_memory(list(job["schema"]))
    try:
        # Import the emitted module so its load-time fail-closed checks (spec-version / fingerprint)
        # actually run, and verify the baked IR literal equals the companion's portable IR.
        mod = _import_generated(job["modulePath"])
        recomputed = mod.fingerprint_component_graph(mod.IR)
        if recomputed != mod.IR_FINGERPRINT:
            sys.stderr.write(f"codegen py: baked IR fingerprint {recomputed} != {mod.IR_FINGERPRINT}\n")
            return 1

        # The companion IS the static makeSQL bundle — execute it via the SAME thin-runtime path the
        # mode-2 leg uses (Python analogue of the TS codegenExecuteBundleForTest re-executing bundle).
        bundle = _reassemble_bundle(companion)
        input_scope = decode_value(job["input"])

        if job["kind"] == "exec":
            result = execute_bundle(bundle, input_scope, driver)
            print(json.dumps({"result": encode_value(result), "dbState": []}))
            return 0

        # tx: gate-first transaction plan (structurally identical to mode-2).
        result = execute_transaction_bundle(bundle, input_scope, driver)
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
