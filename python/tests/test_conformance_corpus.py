"""Frozen-corpus conformance test (WS7b, #31) — the §10 language-axis bar, in pytest.

This runs the ACTUAL frozen vector corpus (`conformance/vectors/*.json`) through the SAME
`vectors_runner._run_vector` the orchestrator launches, and asserts every applicable vector
passes — a real execution against the reference-captured expected SQL/params/results, NOT a
hardcoded pass. If the corpus or the runtime drifts, this fails loudly.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from litedbmodel_runtime import vectors_runner

REPO = Path(__file__).resolve().parent.parent.parent
VECTORS_DIR = REPO / "conformance" / "vectors"


def _all_vectors():
    out = []
    for f in sorted(VECTORS_DIR.glob("*.json")):
        suite = json.loads(f.read_text(encoding="utf-8"))
        assert suite["corpusVersion"] == vectors_runner.SUPPORTED_CORPUS_VERSION
        for v in suite["vectors"]:
            out.append((suite["suite"], v))
    return out


@pytest.mark.parametrize("suite,vector", _all_vectors(), ids=lambda x: x if isinstance(x, str) else x.get("name", "?"))
def test_vector_passes(suite, vector):
    result = vectors_runner._run_vector(vector)
    assert result["ok"], f"[{suite}] {vector['name']}: {result.get('detail')}"


def test_corpus_has_all_four_suites_and_49_vectors():
    vs = _all_vectors()
    suites = {s for s, _ in vs}
    assert suites == {"render", "exec", "tx", "dialect"}
    # 47 baseline + 2 WS8a composite (multi-write) tx-DAG vectors (#28).
    assert len(vs) == 49
