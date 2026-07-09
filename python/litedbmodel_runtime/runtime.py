"""litedbmodel SCP Python runtime — the §8 bundle interpreter (WS7b, #30).

WS7b_E_RUNTIME_STUB — WS7a scaffold only. The functions below define the runtime SURFACE the
conformance runner and consumers depend on; their bodies are implemented in WS7b (render the
fragment tree via behavior-contracts' Expression-IR evaluator, bind params, execute against a
DB-API driver, assemble rows; execute the transaction plan gate-first). They raise until then
so a premature call fails loudly instead of returning a fake result.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence


def render_operation(operation: Mapping[str, Any], scope: Mapping[str, Any], dialect: str) -> dict[str, Any]:
    """Render a §8 CompiledOperation → {"sql", "params"} (dynamic-expansion spec). WS7b."""
    raise NotImplementedError("litedbmodel-runtime: render_operation is WS7b (WS7a scaffold only)")


def execute_bundle(bundle: Mapping[str, Any], input: Mapping[str, Any], driver: Any) -> Any:
    """Execute a §8 read/exec SqlBundle end-to-end (bc runBehavior + SQL handlers). WS7b."""
    raise NotImplementedError("litedbmodel-runtime: execute_bundle is WS7b (WS7a scaffold only)")


def execute_transaction_bundle(bundle: Mapping[str, Any], input: Mapping[str, Any], driver: Any) -> Any:
    """Execute a §8 write-tx SqlBundle as one gate-first transaction. WS7b."""
    raise NotImplementedError("litedbmodel-runtime: execute_transaction_bundle is WS7b (WS7a scaffold only)")


def order_by_nulls(expr: str, direction: str, nulls: str, dialect: str) -> str:
    """The dialect NULLS-ordering primitive (native for PG/SQLite, IS NULL for MySQL). WS7b."""
    raise NotImplementedError("litedbmodel-runtime: order_by_nulls is WS7b (WS7a scaffold only)")
