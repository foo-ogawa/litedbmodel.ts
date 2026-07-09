# litedbmodel/runtime (PHP)

The PHP leg of the litedbmodel v2 SCP multi-language runtime. Interprets the language-neutral §8
published bundle (`SqlBundle`) and executes it against a PDO SQL driver, semantics-identical to
the TS reference (`src/scp`).

**Status: implemented (WS7d, #33).** The §8 bundle interpreter (`renderOperation` /
`executeBundle` / `executeTransactionBundle` / `orderByNulls`), the SQL handlers (PDO), the
gate-first write transaction, and the conformance vector runner are live. The PHP runtime
reproduces the frozen corpus (`conformance/vectors/*.json`) byte-for-byte: same SQL across all
three dialects + same PDO-SQLite execution results. Live Postgres/MySQL PDO execution is deferred
to a coordinated cross-language docker pass; the SQL-handler seam takes any PDO connection.

## behavior-contracts dependency — VENDORED (not a registry dep)

The behavior-contracts **PHP port is NOT published to Packagist** (owner decision, mirrored from
graphddb — `foo-ogawa/behavior-contracts#7`). Like graphddb, this runtime therefore consumes it by
**vendoring** a mechanical copy into `php/src/BehaviorContracts/` behind a sync script + a CI drift
gate (`scripts/vendor-behavior-contracts-php.mjs`, mirroring graphddb's). This is NOT a local `../`
path dependency — it is a committed, drift-checked vendored copy, so the `check-no-local-deps`
gate is satisfied and the published artifact is self-contained.

Re-vendor from the behavior-contracts SSoT and check for drift:

```bash
npm run vendor:bc-php          # (re)vendor from ../behavior-contracts/php/src
npm run vendor:bc-php:check    # CI: fail on drift (never hand-edit src/BehaviorContracts/)
```

The composer.json therefore declares **no** behavior-contracts requirement; the vendored classes
autoload under the package's own PSR-4 root (`LiteDbModel\Runtime\` → `src/`, which covers
`LiteDbModel\Runtime\BehaviorContracts\` → `src/BehaviorContracts/`).

## Layout

```
php/
  composer.json                       # package litedbmodel/runtime (no bc dep — bc is vendored)
  src/Runtime.php                     # the §8 bundle interpreter surface (render/exec/tx/orderByNulls)
  src/Render.php                      # normative dynamic-expansion render (port of src/scp/render.ts)
  src/Dialect.php                     # dialect strategy (finalizePlaceholders / orderByNulls)
  src/WriteRuntime.php                # gate-first write transaction (port of src/scp/write-runtime.ts)
  src/SqlFailure.php                  # PDO error → SCP failure mapping (port of src/scp/errors.ts)
  src/BehaviorContracts/              # vendored bc PHP port (drift-gated), NOT hand-edited
  conformance/vectors_runner.php      # conformance runner entry (loads conformance/vectors/*.json)
  tests/                              # phpunit runtime tests
```

## Running

```bash
# Conformance vectors (real PDO SQLite; reproduces expected SQL + results):
php php/conformance/vectors_runner.php

# phpunit unit + integration tests:
cd php && composer install && ./vendor/bin/phpunit
```
