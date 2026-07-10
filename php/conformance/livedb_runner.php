<?php

declare(strict_types=1);

/**
 * litedbmodel SCP LIVE-DB conformance — PHP runner (WS7g, #36).
 *
 * The PHP leg of the coordinated cross-language live-DB pass (spec §10 dialect axis). It loads the
 * WS7g live-DB corpus (conformance/vectors-livedb/livedb.json — the exec/tx bundles compiled for
 * `postgres` + `mysql`), connects to REAL dockerized Postgres + MySQL via the live PDO seam
 * ({@see LiteDbModel\Runtime\LiveDb}), creates the needed tables in an ISOLATED per-language
 * database (scp_php), and runs each bundle through the SAME Runtime::executeBundle /
 * Runtime::executeTransactionBundle the SQLite conformance uses. It asserts the assembled result
 * equals the frozen SQLite reference (expectedResult / expectedDbState) — the §10 promise.
 *
 * REAL DBs, no mock, NO silent skip: if PG/MySQL is unreachable it ERRORS OUT (exit 3). Emits the
 * machine JSON summary as its LAST stdout line:
 *   {"lang":"php-livedb","suites":{"livedb-pg":{..},"livedb-mysql":{..}},"total_pass",...}
 * exit 0 all pass / 1 any fail / 2 corpus-version mismatch / 3 DB unreachable.
 */

$root = dirname(__DIR__, 2); // php/conformance -> php -> repo root
require $root . '/php/src/BehaviorContracts/ExprFailure.php';
require $root . '/php/src/BehaviorContracts/ExprEval.php';
require $root . '/php/src/BehaviorContracts/PlanFailure.php';
require $root . '/php/src/BehaviorContracts/Plan.php';
require $root . '/php/src/BehaviorContracts/BehaviorFailure.php';
require $root . '/php/src/BehaviorContracts/Behavior.php';
require $root . '/php/src/Dialect.php';
require $root . '/php/src/SqlFailure.php';
require $root . '/php/src/StaticBundle.php';
require $root . '/php/src/WriteRuntime.php';
require $root . '/php/src/Runtime.php';
require $root . '/php/src/Relation.php';
require $root . '/php/src/LiveDb.php';

use LiteDbModel\Runtime\LiveDb;
use LiteDbModel\Runtime\Relation;
use LiteDbModel\Runtime\Runtime;

const SUPPORTED_CORPUS_VERSION = 2;
const PG_SCHEMA = 'scp_php';
const MYSQL_DB = 'scp_php';

$corpusPath = getenv('LITEDBMODEL_LIVEDB_VECTORS');
if ($corpusPath === false || $corpusPath === '') {
    $corpusPath = $root . '/conformance/vectors-livedb/livedb.json';
}

// ── value canon (mirror of vectors_runner.php) ─────────────────────────────────

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

function inputToScope(mixed $decoded): array
{
    if ($decoded instanceof \stdClass) {
        return get_object_vars($decoded);
    }
    return is_array($decoded) ? $decoded : [];
}

// The tables the corpus touches (drop dependents first).
const ALL_TABLES = ['post_tags', 'order_lines', 'comments', 'posts', 'tags', 'docs', 'docs2', 'revs', 'typed', 'users', 'users2', 'idem', 'uniq', 'outbox'];

function resetPg(PDO $db, array $schema): void
{
    foreach (ALL_TABLES as $t) {
        $db->exec("DROP TABLE IF EXISTS {$t} CASCADE");
    }
    foreach ($schema as $stmt) {
        $db->exec((string) $stmt);
    }
}

function resetMysql(PDO $db, array $schema): void
{
    $db->exec('SET FOREIGN_KEY_CHECKS = 0');
    foreach (ALL_TABLES as $t) {
        $db->exec("DROP TABLE IF EXISTS {$t}");
    }
    $db->exec('SET FOREIGN_KEY_CHECKS = 1');
    foreach ($schema as $stmt) {
        $db->exec((string) $stmt);
    }
}

/** @return array{ok:bool, detail?:string} */
function runExec(PDO $db, \stdClass $bundle, \stdClass $v): array
{
    $result = Runtime::executeBundle($bundle, inputToScope($v->input), $db);
    $ok = valuesEqual($result, $v->expectedResult);
    return $ok ? ['ok' => true] : ['ok' => false, 'detail' => 'result ' . json_encode($result) . ' != ' . json_encode($v->expectedResult)];
}

/**
 * A read-RELATION EXECUTION vector: run the parent read + batch-load/hydrate the `with` relations,
 * compare to the PER-DIALECT golden ($expectedKey = expectedResultPg / expectedResultMysql — a
 * limited hasMany's `_rn` window column is present on MySQL but projected away by PG's LATERAL form).
 *
 * @return array{ok:bool, detail?:string}
 */
function runRead(PDO $db, \stdClass $bundle, \stdClass $v, string $expectedKey): array
{
    $withNames = array_map('strval', (array) ($v->with ?? []));
    $result = Relation::readBundle($bundle, inputToScope($v->input), $db, $withNames);
    $expected = $v->{$expectedKey};
    $ok = valuesEqual($result, $expected);
    return $ok ? ['ok' => true] : ['ok' => false, 'detail' => 'result ' . json_encode($result) . ' != ' . json_encode($expected)];
}

/** @return array{ok:bool, detail?:string} */
function runTx(PDO $db, \stdClass $bundle, \stdClass $v, string $txExpectedKey): array
{
    // A write may GENUINELY diverge by dialect (DELETE…RETURNING returns rows on PG, [] on MySQL);
    // the mysql leg then carries `expectedResultMysql`. Fall back to the shared `expectedResult`.
    $expected = isset($v->{$txExpectedKey}) ? $v->{$txExpectedKey} : $v->expectedResult;
    $result = Runtime::executeTransactionBundle($bundle, inputToScope($v->input), $db);
    $resultOk = valuesEqual($result, $expected);
    $stateOk = true;
    $detail = [];
    if (!$resultOk) {
        $detail[] = 'result ' . json_encode($result) . ' != ' . json_encode($expected);
    }
    foreach (($v->expectedDbState ?? []) as $s) {
        $rows = $db->query((string) $s->query)->fetchAll(PDO::FETCH_OBJ);
        if (!valuesEqual($rows, $s->rows)) {
            $stateOk = false;
            $detail[] = "db-state `{$s->query}`: " . json_encode($rows) . ' != ' . json_encode($s->rows);
        }
    }
    $ok = $resultOk && $stateOk;
    return $ok ? ['ok' => true] : ['ok' => false, 'detail' => implode('; ', $detail)];
}

/**
 * @param array<int,\stdClass> $vectors
 * @return array{pass:int, fail:int}
 */
function runDialectLeg(string $dialect, PDO $db, callable $reset, array $vectors, string $bundleKey, string $schemaKey, string $readExpectedKey): array
{
    $t = ['pass' => 0, 'fail' => 0];
    fwrite(STDERR, "\nlivedb-{$dialect} — " . count($vectors) . " vectors (real {$dialect})\n");
    foreach ($vectors as $v) {
        $schema = array_map('strval', (array) $v->{$schemaKey});
        $reset($db, $schema);
        $bundle = $v->{$bundleKey};
        try {
            $kind = (string) $v->kind;
            $r = match ($kind) {
                'exec' => runExec($db, $bundle, $v),
                'read' => runRead($db, $bundle, $v, $readExpectedKey),
                'tx' => runTx($db, $bundle, $v, $readExpectedKey),
                default => ['ok' => false, 'detail' => "unknown kind {$kind}"],
            };
        } catch (\Throwable $e) {
            $r = ['ok' => false, 'detail' => 'threw: ' . $e->getMessage() . "\n" . $e->getTraceAsString()];
        }
        if ($r['ok']) {
            $t['pass']++;
            fwrite(STDERR, "  ok  {$v->name}\n");
        } else {
            $t['fail']++;
            fwrite(STDERR, "  XX  {$v->name}\n      " . ($r['detail'] ?? '') . "\n");
        }
    }
    return $t;
}

// ── main ───────────────────────────────────────────────────────────────────────

fwrite(STDERR, "litedbmodel SCP LIVE-DB conformance — PHP runner (real PG + MySQL)\n");

$corpus = json_decode((string) file_get_contents($corpusPath), false);
if (($corpus->corpusVersion ?? null) !== SUPPORTED_CORPUS_VERSION) {
    fwrite(STDERR, "FAIL-CLOSED: corpusVersion mismatch\n");
    echo json_encode(['lang' => 'php-livedb', 'suites' => new stdClass(), 'total_pass' => 0, 'total_fail' => 0, 'version_mismatch' => true]) . "\n";
    exit(2);
}

$pgHost = getenv('TEST_DB_HOST') ?: 'localhost';
$pgPort = (int) (getenv('TEST_DB_PORT') ?: '5433');
$myHost = getenv('TEST_MYSQL_HOST') ?: '127.0.0.1';
$myPort = (int) (getenv('TEST_MYSQL_PORT') ?: '3307');

try {
    // Bootstrap the per-language PG schema on the base testdb, then connect into it.
    $bootPg = LiveDb::postgres($pgHost, $pgPort, getenv('TEST_DB_USER') ?: 'testuser', getenv('TEST_DB_PASSWORD') ?: 'testpass', getenv('TEST_DB_NAME') ?: 'testdb');
    $bootPg->exec('CREATE SCHEMA IF NOT EXISTS ' . PG_SCHEMA);
    $bootPg->exec('SET search_path TO ' . PG_SCHEMA);
    $pg = $bootPg;
} catch (\Throwable $e) {
    fwrite(STDERR, "FATAL: Postgres unreachable at {$pgHost}:{$pgPort} — {$e->getMessage()}\n");
    exit(3);
}

try {
    // Create the per-language MySQL database, then connect into it.
    $bootMy = LiveDb::mysql($myHost, $myPort, getenv('TEST_MYSQL_USER') ?: 'testuser', getenv('TEST_MYSQL_PASSWORD') ?: 'testpass', getenv('TEST_MYSQL_DB') ?: 'testdb');
    $bootMy->exec('CREATE DATABASE IF NOT EXISTS ' . MYSQL_DB);
    $my = LiveDb::mysql($myHost, $myPort, getenv('TEST_MYSQL_USER') ?: 'testuser', getenv('TEST_MYSQL_PASSWORD') ?: 'testpass', MYSQL_DB);
} catch (\Throwable $e) {
    fwrite(STDERR, "FATAL: MySQL unreachable at {$myHost}:{$myPort} — {$e->getMessage()}\n");
    exit(3);
}

$vectors = $corpus->vectors;
$pgT = runDialectLeg('pg', $pg, 'resetPg', $vectors, 'bundlePg', 'schemaPg', 'expectedResultPg');
$myT = runDialectLeg('mysql', $my, 'resetMysql', $vectors, 'bundleMysql', 'schemaMysql', 'expectedResultMysql');

$suites = ['livedb-pg' => $pgT, 'livedb-mysql' => $myT];
$totalPass = $pgT['pass'] + $myT['pass'];
$totalFail = $pgT['fail'] + $myT['fail'];
fwrite(STDERR, "\n{$totalPass} passed, {$totalFail} failed / " . ($totalPass + $totalFail) . " live-DB vectors\n");
echo json_encode(['lang' => 'php-livedb', 'suites' => $suites, 'total_pass' => $totalPass, 'total_fail' => $totalFail, 'version_mismatch' => false]) . "\n";
exit($totalFail > 0 ? 1 : 0);
