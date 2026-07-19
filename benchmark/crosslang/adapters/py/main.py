#!/usr/bin/env python3
"""Main-bench PYTHON cell — the two honestly-labelled tiers for the 19-op cross-lang bench.

py/php native codegen is a known bc capability gap (graphddb dropped py/php codegen in #342), so python
runs as TWO tiers, NEVER "native":
  • sdk : a hand-written raw-driver baseline (sqlite3 / psycopg / pymysql + hand-SQL) — the py twin of
          the rust/go/ts SDK cell (the fair per-language 1.0x denominator).
  • ir  : litedbmodel's SHIPPED python runtime INTERPRETER (execute_bundle / read_bundle /
          execute_transaction_bundle) over the serialized §8 bundle — the interpreter tier.

Both run one op on one dialect and print the canonical result (verify-cells compares to the oracle):
    python3 adapters/py/main.py run <op> <target> <sdk|ir>
"""

import sys


def main() -> int:
    if len(sys.argv) < 5 or sys.argv[1] != "run":
        sys.stderr.write("usage: main.py run <op> <target> <sdk|ir>\n")
        return 2
    _, _mode, op, target, cell = sys.argv[:5]
    if cell == "ir":
        from ir import ir_cell

        print(ir_cell(op, target))
    elif cell == "sdk":
        from db import RawDB
        from sdk import sdk_cell

        db = RawDB(target)
        try:
            print(sdk_cell(op, db))
        finally:
            db.close()
    else:
        sys.stderr.write(f"unknown cell '{cell}'\n")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
