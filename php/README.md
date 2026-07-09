# litedbmodel/runtime (PHP)

The PHP leg of the litedbmodel v2 SCP multi-language runtime. Interprets the language-neutral §8
published bundle (`SqlBundle`) and executes it against a PDO SQL driver, semantics-identical to
the TS reference (`src/scp`).

**Status: WS7a scaffold.** Buildable composer package skeleton + conformance runner entry point;
the runtime body is **WS7e**.

## behavior-contracts dependency — VENDORED (not a registry dep)

The behavior-contracts **PHP port is NOT published to Packagist** (owner decision, mirrored from
graphddb — `foo-ogawa/behavior-contracts#7`). Like graphddb, this runtime therefore consumes it by
**vendoring** a mechanical copy into `php/src/BehaviorContracts/` behind a sync script + a CI drift
gate (WS7e wires `scripts/vendor-behavior-contracts-php.mjs`, mirroring graphddb's). This is NOT a
local `../` path dependency — it is a committed, drift-checked vendored copy, so the
`check-no-local-deps` gate is satisfied and the published artifact is self-contained.

The composer.json therefore declares **no** behavior-contracts requirement; the vendored classes
autoload under the package's own PSR-4 root.

## Layout

```
php/
  composer.json                       # package litedbmodel/runtime (no bc dep — bc is vendored)
  src/Runtime.php                     # WS7e: the §8 bundle interpreter surface
  src/BehaviorContracts/              # WS7e: vendored bc PHP port (drift-gated), NOT hand-edited
  conformance/vectors_runner.php      # conformance runner entry (WS7e body)
  tests/                              # WS7e runtime tests
```
