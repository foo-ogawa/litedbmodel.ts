<?php

declare(strict_types=1);

/**
 * litedbmodel SCP conformance vectors — PHP runner (WS7d, #33).
 *
 * The ENTRY POINT the cross-language orchestrator (conformance/vectors-run.ts) launches for the
 * PHP leg. It loads the FROZEN vector corpus (conformance/vectors/*.json), runs each vector
 * through the litedbmodel PHP runtime (LiteDbModel\Runtime\Runtime, which consumes the VENDORED
 * behavior-contracts PHP port for Expression-IR eval + component-graph execution), and asserts
 * the runtime reproduces the SAME SQL text (all 3 dialects) + the SAME execution results
 * (against a REAL in-process PDO SQLite). It prints the SAME machine JSON summary as the TS
 * runner as the LAST stdout line:
 *
 *   {"lang":"php","suites":{"<suite>":{"pass":N,"fail":N}},"total_pass":N,"total_fail":N,"version_mismatch":false}
 *
 * exit 0 (all pass) / 1 (any fail) / 2 (corpus-version mismatch).
 *
 * ## Cross-language value encoding (the §10 numeric-identity contract)
 *
 * The corpus is reference-generated from TS, which has TWO integer types (bigint / number); its
 * `encodeValue` tags every bigint as `{"$bigint":"<dec>"}` (a JSON round-trip artifact). PHP has
 * ONE integer type, so it cannot — and MUST NOT — reproduce that tag distinction: two runtimes
 * conform when they produce the SAME NUMERIC VALUE (spec §10 "同一 IR+入力 → 同一 SQL + 同一結果").
 * The comparator therefore normalizes `{"$bigint":"N"}` and the bare integer `N` to the SAME
 * canonical integer before comparing (a genuine semantic equality, NOT a fudge): PHP's int IS the
 * value the tag encodes. Input `{"$bigint":"N"}` is likewise decoded to a PHP int before binding.
 */

$root = dirname(__DIR__, 2); // php/conformance -> php -> repo root
require $root . '/php/src/BehaviorContracts/Constants.php';
require $root . '/php/src/BehaviorContracts/ExprFailure.php';
require $root . '/php/src/BehaviorContracts/ExprEval.php';
require $root . '/php/src/BehaviorContracts/PlanFailure.php';
require $root . '/php/src/BehaviorContracts/Plan.php';
require $root . '/php/src/BehaviorContracts/BehaviorFailure.php';
require $root . '/php/src/BehaviorContracts/Behavior.php';
require $root . '/php/src/Dialect.php';
require $root . '/php/src/SqlFailure.php';
require $root . '/php/src/ExecutionContext.php';
require $root . '/php/src/StaticBundle.php';
require $root . '/php/src/WriteRuntime.php';
require $root . '/php/src/Runtime.php';

use LiteDbModel\Runtime\Runtime;

/** The corpus schema version this runner supports (pin — bumped on additive refreeze). */
const SUPPORTED_CORPUS_VERSION = 3;

$vectorsDir = getenv('LITEDBMODEL_VECTORS');
if ($vectorsDir === false || $vectorsDir === '') {
    $vectorsDir = $root . '/conformance/vectors';
}

/**
 * Decode a corpus-encoded value (json_decode(.., false) shape) to a runtime value:
 *   `{"$bigint":"N"}` → PHP int N; objects/arrays recurse; scalars pass through.
 * The IR-node structure of a bundle is preserved (bundles are consumed as-is by the runtime and
 * bc); this decode only touches the `input`/expected VALUE encodings.
 */
function decodeValue(mixed $v): mixed
{
    if ($v instanceof \stdClass) {
        $props = get_object_vars($v);
        if (count($props) === 1 && array_key_exists('$bigint', $props) && is_string($props['$bigint'])) {
            return (int) $props['$bigint'];
        }
        $out = new \stdClass();
        foreach ($props as $k => $val) {
            $out->{$k} = decodeValue($val);
        }
        return $out;
    }
    if (is_array($v)) {
        return array_map('decodeValue', $v);
    }
    return $v;
}

/**
 * Canonicalize a value for cross-language comparison: `{"$bigint":"N"}` → int N; stdClass →
 * assoc array (recursively, key-sorted for order-independence); arrays recurse. This maps both
 * the PHP runtime output and the (TS-encoded) expected value into the SAME numeric/structural
 * form so equality is by VALUE, not by the TS bigint-tag artifact.
 */
function canonical(mixed $v): mixed
{
    if ($v instanceof \stdClass) {
        $props = get_object_vars($v);
        if (count($props) === 1 && array_key_exists('$bigint', $props) && is_string($props['$bigint'])) {
            return (int) $props['$bigint'];
        }
        $out = [];
        foreach ($props as $k => $val) {
            $out[$k] = canonical($val);
        }
        ksort($out);
        return ['__obj__' => $out];
    }
    if (is_array($v)) {
        // Distinguish JSON object (assoc) from list, then canonicalize element-wise.
        $isList = array_is_list($v);
        $out = [];
        foreach ($v as $k => $val) {
            $out[$k] = canonical($val);
        }
        if (!$isList) {
            ksort($out);
            return ['__obj__' => $out];
        }
        return $out;
    }
    return $v;
}

function valuesEqual(mixed $a, mixed $b): bool
{
    return canonical($a) === canonical($b);
}

/** A scope: an assoc array from a decoded input stdClass (top-level object). @return array<string,mixed> */
function inputToScope(mixed $decodedInput): array
{
    if ($decodedInput instanceof \stdClass) {
        return get_object_vars($decodedInput);
    }
    if (is_array($decodedInput)) {
        return $decodedInput;
    }
    return [];
}

/** Fresh in-memory PDO SQLite seeded from the vector's schema/seed statement list. */
function seedDb(array $schema): PDO
{
    $db = new PDO('sqlite::memory:');
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $db->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, false);
    $db->exec('PRAGMA foreign_keys = ON');
    foreach ($schema as $stmt) {
        $db->exec((string) $stmt);
    }
    return $db;
}

/**
 * Run ONE vector through the consumed runtime.
 *
 * @return array{ok:bool, detail?:string}
 */
function runVector(\stdClass $v): array
{
    try {
        $kind = (string) ($v->kind ?? '');
        if ($kind === 'render') {
            // Render the PRIMARY read node's static makeSQL statements of the ReadGraph → dialect
            // SQL + flat params, asserted byte-identical to the reference-captured golden.
            $scope = inputToScope(decodeValue($v->input));
            $r = Runtime::renderReadPrimary($v->readGraph, $scope);
            $sqlOk = $r['sql'] === (string) $v->expectedSql;
            $expectedParams = is_array($v->expectedParams) ? $v->expectedParams : [];
            $paramsOk = valuesEqual($r['params'], $expectedParams);
            if ($sqlOk && $paramsOk) {
                return ['ok' => true];
            }
            $parts = [];
            if (!$sqlOk) {
                $parts[] = 'sql ' . json_encode($r['sql']) . ' != ' . json_encode((string) $v->expectedSql);
            }
            if (!$paramsOk) {
                $parts[] = 'params ' . json_encode($r['params']) . ' != ' . json_encode($expectedParams);
            }
            return ['ok' => false, 'detail' => implode('; ', $parts)];
        }
        if ($kind === 'write-render') {
            // A write statement's compiled makeSQL template is asserted byte-identical to golden
            // (the deferred Expression-IR params are NOT evaluated here — they resolve at tx time).
            $stmt = $v->statement;
            $sqlOk = (string) ($stmt->sql ?? '') === (string) $v->expectedSql;
            $expectedParams = is_array($v->expectedParams) ? $v->expectedParams : [];
            $actualParams = is_array($stmt->params ?? null) ? $stmt->params : [];
            $paramsOk = valuesEqual($actualParams, $expectedParams);
            return $sqlOk && $paramsOk
                ? ['ok' => true]
                : ['ok' => false, 'detail' => 'write-render mismatch'];
        }
        if ($kind === 'exec') {
            $schema = is_array($v->schema) ? $v->schema : [];
            $db = seedDb($schema);
            $result = Runtime::executeBundle($v->bundle, inputToScope(decodeValue($v->input)), $db);
            $ok = valuesEqual($result, $v->expectedResult);
            return $ok ? ['ok' => true] : ['ok' => false, 'detail' => 'result ' . json_encode($result) . ' != ' . json_encode($v->expectedResult)];
        }
        if ($kind === 'tx') {
            $schema = is_array($v->schema) ? $v->schema : [];
            $db = seedDb($schema);
            $result = Runtime::executeTransactionBundle($v->bundle, inputToScope(decodeValue($v->input)), $db);
            $stateOk = true;
            $stateDetail = '';
            foreach (($v->expectedDbState ?? []) as $s) {
                $rows = $db->query((string) $s->query)->fetchAll(PDO::FETCH_OBJ);
                if (!valuesEqual($rows, $s->rows)) {
                    $stateOk = false;
                    $stateDetail = 'db-state mismatch on `' . (string) $s->query . '`: '
                        . json_encode($rows) . ' != ' . json_encode($s->rows);
                    break;
                }
            }
            $ok = valuesEqual($result, $v->expectedResult) && $stateOk;
            if ($ok) {
                return ['ok' => true];
            }
            $detail = valuesEqual($result, $v->expectedResult)
                ? $stateDetail
                : 'result ' . json_encode($result) . ' != ' . json_encode($v->expectedResult);
            return ['ok' => false, 'detail' => $detail];
        }
        if ($kind === 'dialect') {
            $got = Runtime::orderByNulls((string) $v->args->expr, (string) $v->args->dir, (string) $v->args->nulls, (string) $v->dialect);
            $ok = $got === (string) $v->expected;
            return $ok ? ['ok' => true] : ['ok' => false, 'detail' => json_encode($got) . ' != ' . json_encode((string) $v->expected)];
        }
        return ['ok' => false, 'detail' => "unknown vector kind: {$kind}"];
    } catch (\Throwable $e) {
        return ['ok' => false, 'detail' => 'threw: ' . $e->getMessage()];
    }
}

function main(string $vectorsDir): int
{
    fwrite(STDERR, "litedbmodel SCP conformance vectors — PHP runner (consumed src runtime + vendored bc-php)\n");
    $files = glob(rtrim($vectorsDir, '/') . '/*.json');
    if ($files === false) {
        $files = [];
    }
    sort($files);

    // Pre-flight version sweep (fail-closed): reject the whole run on any suite-version mismatch.
    $suites = [];
    foreach ($files as $f) {
        $doc = json_decode((string) file_get_contents($f), false, 512, JSON_THROW_ON_ERROR);
        $suites[] = $doc;
    }
    $mismatched = array_filter($suites, static fn ($s) => ($s->corpusVersion ?? null) !== SUPPORTED_CORPUS_VERSION);
    if (count($mismatched) > 0) {
        foreach ($mismatched as $s) {
            fwrite(STDERR, "FAIL-CLOSED: suite '" . ($s->suite ?? '?') . "' corpusVersion " . json_encode($s->corpusVersion ?? null)
                . " != supported " . SUPPORTED_CORPUS_VERSION . ".\n");
        }
        echo json_encode(['lang' => 'php', 'suites' => new \stdClass(), 'total_pass' => 0, 'total_fail' => 0, 'version_mismatch' => true]), "\n";
        return 2;
    }

    $tallies = [];
    foreach ($suites as $suite) {
        $name = (string) ($suite->suite ?? '?');
        $vectors = is_array($suite->vectors ?? null) ? $suite->vectors : [];
        $t = ['pass' => 0, 'fail' => 0];
        fwrite(STDERR, "\n{$name}.json — " . count($vectors) . " vectors\n");
        foreach ($vectors as $v) {
            $r = runVector($v);
            if ($r['ok']) {
                fwrite(STDERR, '  [pass] ' . (string) ($v->name ?? '?') . "\n");
                $t['pass']++;
            } else {
                fwrite(STDERR, '  [FAIL] ' . (string) ($v->name ?? '?') . "\n");
                if (isset($r['detail'])) {
                    fwrite(STDERR, '      ' . $r['detail'] . "\n");
                }
                $t['fail']++;
            }
        }
        $tallies[$name] = $t;
    }

    $totalPass = array_sum(array_map(static fn ($t) => $t['pass'], $tallies));
    $totalFail = array_sum(array_map(static fn ($t) => $t['fail'], $tallies));
    fwrite(STDERR, "\n{$totalPass} passed, {$totalFail} failed / " . ($totalPass + $totalFail)
        . ' vectors across ' . count($suites) . " suites\n");

    echo json_encode([
        'lang' => 'php',
        'suites' => $tallies === [] ? new \stdClass() : $tallies,
        'total_pass' => $totalPass,
        'total_fail' => $totalFail,
        'version_mismatch' => false,
    ]), "\n";
    return $totalFail > 0 ? 1 : 0;
}

exit(main($vectorsDir));
