#!/usr/bin/env python3
"""ORM-plan NDJSON runner entry point — Python (epic #63), harness registry spawn target.

The harness registry (contract.ts) spawns this as `python3 orm_runner.py`. It delegates to the
executor in orm_exec.py: with no args (or `--orm-plan`) it speaks the NDJSON run/throughput/cost/
rss/shutdown protocol; `--smoke` runs the standalone 57-cell matrix. See orm_exec.py for the full
executor (driver seam, bindKind protocol, per-op writes).
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
        orm_exec.protocol()


if __name__ == "__main__":
    main()
