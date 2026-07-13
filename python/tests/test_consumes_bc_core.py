"""Guardrail test (WS7b, #31): bc runtime-core is CONSUMED, not reimplemented.

The hard rule: the Python runtime delegates the CLOSED Expression-IR evaluation + the
plan/map/wire/output orchestration to behavior-contracts (the published PyPI package), exactly
like the TS reference imports `behavior-contracts` from npm. This asserts the runtime modules
actually import bc's `run_behavior` / `evaluate_expression` (no local generic evaluator), and
that the declared dependency is the PyPI package spec with NO local `../` path.
"""

from __future__ import annotations

import re

import ast
from pathlib import Path

PKG = Path(__file__).resolve().parent.parent / "litedbmodel_runtime"
PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"


def _imports_from(module: str) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.parse((PKG / module).read_text(encoding="utf-8"))):
        if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("behavior_contracts"):
            names.update(a.name for a in node.names)
    return names


def test_static_bundle_consumes_bc_evaluate_expression():
    names = _imports_from("static_bundle.py")
    assert "evaluate_expression" in names
    assert "run_behavior" in names


def test_no_local_generic_evaluator_reimplemented():
    # The runtime must not define its own expression evaluator (that would reimplement bc-core).
    src = (PKG / "static_bundle.py").read_text(encoding="utf-8") + (PKG / "runtime.py").read_text(encoding="utf-8")
    for banned in ("def evaluate_expression", "def evaluate(", "def _eval_expr", "def run_behavior"):
        assert banned not in src, f"runtime reimplements bc-core: found '{banned}'"


def test_pyproject_declares_bc_as_pypi_dep_no_local_path():
    text = PYPROJECT.read_text(encoding="utf-8")
    # bc is consumed as an EXACTLY-PINNED PyPI dependency (`behavior-contracts==<semver>`), never a
    # range and never a local path — the runtime + the generated §8 bundle must stay in lockstep
    # (WS7a #30). Assert the pinned FORM, version-agnostically, so this survives version bumps.
    assert re.search(r'"behavior-contracts==\d+\.\d+\.\d+"', text), (
        "pyproject must pin behavior-contracts to an exact PyPI version (behavior-contracts==X.Y.Z)"
    )
    # No local path dep (the no-local-deps gate forbids `../` / file:// / path = ...).
    assert "../" not in text
    assert "file://" not in text
    assert "behavior_contracts @" not in text


def test_bc_is_importable_and_provides_core():
    import behavior_contracts as bc

    assert callable(bc.run_behavior)
    assert callable(bc.evaluate_expression)
