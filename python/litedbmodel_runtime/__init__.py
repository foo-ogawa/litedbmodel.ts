"""litedbmodel v2 SCP — Python runtime (WS7b scaffold, #30).

This package is the Python leg of the multi-language SCP runtime. It interprets the
language-neutral §8 published bundle (``SqlBundle``: sql text + fragment tree + closed-set
Expression-IR param slots + transaction plan, dialect-tagged) and executes it against a SQL
driver, semantics-identical to the TS reference (``src/scp``). The generic Expression-IR
evaluation (SKIP guards, param slots) is delegated to the shared common core
``behavior-contracts`` (PyPI) — this package re-implements NO generic evaluator, only the
SQL-backend concerns (render → bind → execute → assembly), exactly like the TS runtime.

WS7a delivers ONLY this buildable skeleton + the conformance runner entry stub. The runtime
body (render / execute / transaction) is WS7b.
"""

__version__ = "1.2.10"

# WS7b fills these in (render_operation / execute_bundle / execute_transaction_bundle).
__all__: list[str] = []
