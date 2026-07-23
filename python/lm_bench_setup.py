"""Load the ONE cross-lang ORM-bench seed SSoT — benchmark/crosslang/.setup/<dialect>.json, emitted
from orm-domain.ts by emit-setup.ts — for BOTH python bench cells (orm_bench + orm_bench_sdk). No
python cell hand-writes a schema or seed: each applies ``schema`` once at open and ``delete``+``insert``
(the canonical 110-user fixture, literal SQL) per op. This is the single python-side reader of the JSON.
"""

from __future__ import annotations

import json
import os
from typing import Dict, List


def load(dialect: str) -> Dict[str, List[str]]:
    """Return the dialect's setup doc: ``{schema, delete, insert}`` (each a list of literal SQL). The
    path is anchored to this file (repo-root-relative), so it resolves regardless of the cwd."""
    here = os.path.dirname(os.path.abspath(__file__))  # <repo>/python
    root = os.path.dirname(here)  # <repo>
    path = os.path.join(root, "benchmark", "crosslang", ".setup", f"{dialect}.json")
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    return doc
