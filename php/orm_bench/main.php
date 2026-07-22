<?php

declare(strict_types=1);

/**
 * NATIVE-codegen ORM-bench cell CLI entry (php leg, epic #123) — twin of `python -m orm_bench.main`.
 *
 * Usage:
 *   php orm_bench/main.php <dialect> <spec> [reps] [warmup]   # print the CSV
 *   php orm_bench/main.php safety <dialect> <spec>            # assert + print the safety statement counts
 *
 * All covered logic lives in {@see \LiteDbModel\Bench\OrmBench} (the SAME class the phpunit conformance
 * test reuses — no duplicated setup). This file only parses argv and dispatches.
 */

use LiteDbModel\Bench\OrmBench;

require __DIR__ . '/../vendor/autoload.php';

$argv = $_SERVER['argv'];
array_shift($argv); // drop the script name

if (($argv[0] ?? null) === 'safety') {
    OrmBench::safety($argv[1] ?? 'sqlite', $argv[2] ?? 'sqlite');
    return;
}

$dialect = $argv[0] ?? 'sqlite';
$spec = $argv[1] ?? 'sqlite';
$reps = isset($argv[2]) ? (int) $argv[2] : 300;
$warmup = isset($argv[3]) ? (int) $argv[3] : 30;
OrmBench::measure($dialect, $spec, $reps, $warmup);
