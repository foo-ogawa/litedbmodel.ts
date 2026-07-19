<?php

declare(strict_types=1);

/**
 * Main-bench PHP cell — the two honestly-labelled tiers for the 19-op cross-lang bench (the php twin of
 * adapters/py/main.py).
 *
 * py/php native codegen is a known bc capability gap (graphddb dropped py/php codegen in #342), so php
 * runs as TWO tiers, NEVER "native":
 *   • sdk : a hand-written raw-PDO baseline (sqlite / pg / mysql + hand-SQL) — the php twin of the
 *           rust/go/ts SDK cell (the fair per-language 1.0x denominator).
 *   • ir  : litedbmodel's SHIPPED php runtime INTERPRETER (executeBundle / readBundle /
 *           executeTransactionBundleInternal) over the serialized §8 bundle — the interpreter tier.
 *
 * Both run one op on one dialect and print the canonical result (verify-cells compares to the oracle):
 *     php adapters/php/main.php run <op> <target> <sdk|ir>
 */

if ($argc < 5 || $argv[1] !== 'run') {
    fwrite(STDERR, "usage: main.php run <op> <target> <sdk|ir>\n");
    exit(2);
}
[, , $op, $target, $cell] = $argv;

if ($cell === 'ir') {
    require __DIR__ . '/ir.php';
    echo ir_cell($op, $target) . "\n";
} elseif ($cell === 'sdk') {
    require __DIR__ . '/sdk.php';
    $db = new RawDb($target);
    echo sdk_cell($op, $db) . "\n";
} else {
    fwrite(STDERR, "unknown cell '{$cell}'\n");
    exit(2);
}
