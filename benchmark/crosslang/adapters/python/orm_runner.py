#!/usr/bin/env python3
"""ORM-plan STANDALONE bench entry point — Python (epic #63).

The orchestrator (run.ts) spawns this as `python3 orm_runner.py` — ONE standalone process that
runs ALL 19 ops × 3 dialects, self-measures, and writes a FLAT CSV to
benchmark/crosslang/.results/python.csv. There is NO stdin/stdout protocol. `--smoke` runs the
standalone 57-cell matrix instead. See orm_exec.py for the full executor (driver seam, bindKind
protocol, per-op writes) and the `bench()` CSV writer.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import orm_exec  # noqa: E402


def main():
    args = sys.argv[1:]
    orm_exec.load_artifact()  # populate ORM_OPS
    if "--smoke" in args:
        orm_exec.smoke()
    else:
        orm_exec.bench()


if __name__ == "__main__":
    main()
