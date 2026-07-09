# litedbmodel-runtime (Python)

The Python leg of the litedbmodel v2 SCP multi-language runtime. Interprets the language-neutral
§8 published bundle (`SqlBundle`) and executes it against a DB-API SQL driver,
semantics-identical to the TS reference (`src/scp`).

**Status: WS7a scaffold.** The buildable package skeleton + the conformance runner entry point are
here; the runtime body (render / execute / transaction) is **WS7b**.

## behavior-contracts dependency

The runtime delegates the CLOSED Expression-IR evaluation to the shared common core
[`behavior-contracts`](https://pypi.org/project/behavior-contracts/) — **consumed from PyPI**
(`behavior-contracts==0.2.0`), exactly as the TS reference imports it from npm. No local path
dependency (the `check-no-local-deps` gate forbids `../`-escaping deps).

## Layout

```
python/
  pyproject.toml                       # PyPI package (litedbmodel-runtime), version-synced from package.json
  litedbmodel_runtime/
    __init__.py
    runtime.py                         # WS7b: the §8 bundle interpreter surface
    vectors_runner.py                  # conformance runner entry (WS7b body)
  tests/                               # WS7b runtime tests
```
