<?php

declare(strict_types=1);

/**
 * ORM-plan NDJSON runner entry point — PHP (epic #63), harness registry spawn target.
 *
 * The harness registry (contract.ts) spawns this as `php orm-runner.php`. It delegates to the
 * executor in orm_exec.php: with no args it speaks the NDJSON run/throughput/cost/rss/shutdown
 * protocol; `--smoke` runs the standalone 57-cell matrix. See orm_exec.php for the full executor
 * (LiveDb PDO seam, bindKind protocol, per-op writes).
 */

require __DIR__ . '/orm_exec.php';

ormExecMain($argv, dirname(__DIR__, 2) . '/generated/orm-plan.json');
