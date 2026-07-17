<?php

declare(strict_types=1);

/**
 * ORM-plan STANDALONE bench entry point — PHP.
 *
 * The orchestrator (run.ts) spawns this as `php orm-runner.php` — ONE standalone process that runs
 * ALL 19 ops × 3 dialects, self-measures, and writes a FLAT CSV to
 * benchmark/crosslang/.results/php.csv. There is NO stdin/stdout protocol. `--smoke` runs the
 * standalone 57-cell matrix instead. See orm_exec.php for the full executor (LiveDb PDO seam,
 * bindKind protocol, per-op writes) and the `bench()` CSV writer.
 */

require __DIR__ . '/orm_exec.php';

ormExecMain($argv, dirname(__DIR__, 2) . '/generated/orm-plan.json');
