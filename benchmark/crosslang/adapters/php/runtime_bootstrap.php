<?php

declare(strict_types=1);

/**
 * Load the litedbmodel PHP runtime + its VENDORED behavior-contracts core (no composer needed) — the
 * SINGLE require site both php cells (sdk connections, ir execution) share. Mirrors the require order of
 * php/conformance/livedb_runner.php lines 22-38.
 *
 * The runtime root is env `LITEDBMODEL_PHP` (verify-cells exports it to the cell process); it falls back
 * to `<repo>/php` (this file is at benchmark/crosslang/adapters/php → up 4 = repo root).
 */

$phpRoot = getenv('LITEDBMODEL_PHP');
if ($phpRoot === false || $phpRoot === '') {
    $phpRoot = dirname(__DIR__, 4) . '/php';
}

foreach ([
    'BehaviorContracts/Constants',
    'BehaviorContracts/ExprFailure',
    'BehaviorContracts/ExprEval',
    'BehaviorContracts/PlanFailure',
    'BehaviorContracts/Plan',
    'BehaviorContracts/BehaviorFailure',
    'BehaviorContracts/Behavior',
    'Dialect',
    'SqlFailure',
    'ExecutionContext',
    'Middleware',
    'StaticBundle',
    'WriteRuntime',
    'Runtime',
    'Relation',
    'LiveDb',
] as $rel) {
    require_once $phpRoot . '/src/' . $rel . '.php';
}
