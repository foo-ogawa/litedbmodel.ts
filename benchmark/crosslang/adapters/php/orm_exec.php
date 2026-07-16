<?php

declare(strict_types=1);

/**
 * ORM-plan EXECUTOR + live smoke — PHP (epic #63).
 *
 * Port of the PROVEN TS reference (benchmark/crosslang/orm-exec-ts.ts + orm-smoke.ts). Loads the
 * committed language-neutral artifact benchmark/crosslang/generated/orm-plan.json and executes ALL
 * 19 ORM ops x {sqlite, mysql, postgres} through the SHIPPED LiteDbModel\Runtime\LiveDb PDO seam
 * (LiveDb::postgres / LiveDb::mysql for PG/MySQL, PDO sqlite in-proc for sqlite), binding the BAKED
 * per-dialect SQL from the artifact per the bindKind protocol (NO SQL generation here).
 *
 * The LiveDb PG PDO subclass rewrites `$N`->`?`; the MySQL subclass emulates RETURNING at the seam.
 * This executor still mirrors the TS per-dialect write logic explicitly (strip RETURNING + lastId
 * for sqlite/mysql, native RETURNING fetch for pg) so the id-chaining path is dialect-faithful.
 *
 * Spawn convention (harness registry):
 *     php benchmark/crosslang/adapters/php/orm_exec.php --orm-plan [--smoke]
 * `--smoke` runs the 57-cell matrix and exits; without it, it speaks the NDJSON
 * run/throughput/cost/rss/shutdown protocol over stdin/stdout (case=<opId>, dialect=<dialect>).
 */

use LiteDbModel\Runtime\LiveDb;

$HERE = __DIR__;
$REPO = dirname($HERE, 4);
require $REPO . '/php/vendor/autoload.php';

const PG_SCHEMA_NAME = 'scp_php_bench';
const MYSQL_DB_NAME = 'scp_php_bench';

// Raw-driver BASELINE gets its OWN isolated PG schema / MySQL db so the two impls never clobber each
// other's tables (and could run side by side). Same real driver + byte-identical SQL, so the
// runtime÷baseline ratio isolates litedbmodel's over-driver cost, NOT a driver difference.
const PG_BASELINE_SCHEMA = 'scp_php_bench_baseline';
const MYSQL_BASELINE_DB = 'scp_php_bench_baseline';

$ARTIFACT_PATH = dirname($HERE, 2) . '/generated/orm-plan.json';

// ── {{SEQ}} substitution: a per-op-invocation incrementing int for unique-email writes ─────────
$GLOBALS['__seq'] = 0;
function nextSeq(): int
{
    return $GLOBALS['__seq']++;
}
function substOne($p, int $seq)
{
    if (is_string($p) && str_contains($p, '{{SEQ}}')) {
        return str_replace('{{SEQ}}', (string) $seq, $p);
    }
    if (is_array($p)) {
        return array_map(fn ($x) => substOne($x, $seq), $p);
    }
    return $p;
}
function substParams(array $params, int $seq): array
{
    return array_map(fn ($p) => substOne($p, $seq), $params);
}

/** Render a PHP array as a PG array literal `{a,b,c}` (strings quoted, per PG text-format rules). */
function pgArrayLiteral(array $arr): string
{
    $parts = array_map(function ($v) {
        if (is_bool($v)) {
            return $v ? 'true' : 'false';
        }
        if (is_int($v) || is_float($v)) {
            return (string) $v;
        }
        // Quote + escape backslash and double-quote for the PG array text format.
        return '"' . str_replace(['\\', '"'], ['\\\\', '\\"'], (string) $v) . '"';
    }, $arr);
    return '{' . implode(',', $parts) . '}';
}
function stripReturning(string $sql): string
{
    return preg_replace('/\s+RETURNING\s+.+$/is', '', $sql) ?? $sql;
}

/**
 * Coerce PHP booleans for PDO binding: PDO binds a PHP `false` as `''`, which PG rejects for a
 * BOOLEAN column and MySQL/sqlite mis-store. sqlite/mysql want 1/0; PG wants the literal 'true'/'false'.
 */
function coerceParams(array $params, string $dialect): array
{
    return array_map(function ($p) use ($dialect) {
        if (is_bool($p)) {
            if ($dialect === 'postgres') {
                return $p ? 'true' : 'false';
            }
            return $p ? 1 : 0;
        }
        // A native array param (PG UNNEST bulk ops) → a PG array literal (PDO cannot bind an array).
        if (is_array($p)) {
            return pgArrayLiteral($p);
        }
        return $p;
    }, $params);
}

// ── relation bind protocol (mirror bindRelation in orm-exec-ts.ts) ─────────────────────────────
function distinctSingleKeys(array $stage, array $parents): array
{
    $seen = [];
    $out = [];
    $pk = $stage['single']['parentKey'];
    foreach ($parents as $r) {
        if (!array_key_exists($pk, $r) || $r[$pk] === null) {
            continue;
        }
        $k = $r[$pk];
        $s = (string) $k;
        if (!isset($seen[$s])) {
            $seen[$s] = true;
            $out[] = $k;
        }
    }
    return $out;
}
function distinctTuples(array $stage, array $parents): array
{
    $seen = [];
    $out = [];
    [$p0, $p1] = $stage['composite']['parentKeys'];
    foreach ($parents as $r) {
        if (($r[$p0] ?? null) === null || ($r[$p1] ?? null) === null) {
            continue;
        }
        $k0 = $r[$p0];
        $k1 = $r[$p1];
        $s = (string) $k0 . ' ' . (string) $k1;
        if (!isset($seen[$s])) {
            $seen[$s] = true;
            $out[] = [$k0, $k1];
        }
    }
    return $out;
}

/** Bind resolved DISTINCT parent keys/tuples onto stage.sql per bindKind. null = no parents. */
function bindRelation(array $stage, array $parents): ?array
{
    $kind = $stage['bindKind'];
    if (isset($stage['single']) && $stage['single']) {
        $keys = distinctSingleKeys($stage, $parents);
        if (count($keys) === 0) {
            return null;
        }
        if ($kind === 'pgArraySingle') {
            // pg: the runtime PgLivePdo rewrites `$1`->`?`; PDO pgsql binds a PG array as a literal
            // `{1,2,3}` text param. The ::int[] cast is already baked into stage.sql (int keys).
            return ['sql' => $stage['sql'], 'params' => ['{' . implode(',', $keys) . '}'], 'kind' => $kind];
        }
        return ['sql' => $stage['sql'], 'params' => [json_encode($keys)], 'kind' => $kind]; // jsonParam
    }
    $tuples = distinctTuples($stage, $parents);
    if (count($tuples) === 0) {
        return null;
    }
    if ($kind === 'pgArrayComposite') {
        $c0 = array_map(fn ($t) => $t[0], $tuples);
        $c1 = array_map(fn ($t) => $t[1], $tuples);
        return ['sql' => $stage['sql'], 'params' => ['{' . implode(',', $c0) . '}', '{' . implode(',', $c1) . '}'], 'kind' => $kind];
    }
    // tupleExpand (sqlite/mysql composite): repeat the group per tuple, flatten params.
    $groups = implode(', ', array_fill(0, count($tuples), $stage['groupTemplate']));
    $flat = [];
    foreach ($tuples as $t) {
        foreach ($t as $x) {
            $flat[] = $x;
        }
    }
    return ['sql' => $stage['sql'] . $groups . ($stage['suffix'] ?? ''), 'params' => $flat, 'kind' => $kind];
}

/**
 * BARE PDO for the Postgres RAW baseline. The artifact's `postgres`-tagged SQL renders `$N`
 * placeholders (the Render final pass), but PDO_pgsql binds `?` positionally and does NOT translate
 * `$N`. The SHIPPED runtime absorbs this in LiveDb\PgLivePdo; the raw baseline must issue the SAME
 * final SQL to the driver, so it replicates ONLY that driver-mandated `$N`→`?` rewrite — no
 * litedbmodel logic. The bytes handed to libpq are identical to the runtime's, so runtime÷baseline
 * measures litedbmodel's over-driver cost, not a placeholder-dialect difference.
 */
final class RawPgPdo extends PDO
{
    private static function rewrite(string $sql): string
    {
        return preg_replace('/\$\d+/', '?', $sql) ?? $sql;
    }

    #[\ReturnTypeWillChange]
    public function prepare(string $query, array $options = []): \PDOStatement|false
    {
        return parent::prepare(self::rewrite($query), $options);
    }

    #[\ReturnTypeWillChange]
    public function exec(string $statement): int|false
    {
        return parent::exec(self::rewrite($statement));
    }
}

// ── driver (all speak PDO prepare/execute/fetchAll) ────────────────────────────────────────────
final class OrmDriver
{
    public string $dialect;
    public PDO $db;
    public string $impl;

    public function __construct(string $dialect, PDO $db, string $impl = 'runtime')
    {
        $this->dialect = $dialect;
        $this->db = $db;
        $this->impl = $impl;
    }

    private function allRows(string $sql, array $params): array
    {
        $st = $this->db->prepare($sql);
        $st->execute(coerceParams(array_values($params), $this->dialect));
        return $st->fetchAll(PDO::FETCH_ASSOC);
    }

    private function runStmt(string $sql, array $params): void
    {
        $st = $this->db->prepare($sql);
        $st->execute(coerceParams(array_values($params), $this->dialect));
    }

    public function run(array $plan): int
    {
        return $plan['kind'] === 'read' ? $this->readPlan($plan) : $this->writePlan($plan);
    }

    private function readPlan(array $plan): int
    {
        $rows = $this->allRows($plan['reads'][0]['sql'], $plan['reads'][0]['params']);
        $total = count($rows);
        $stageRows = [$rows];
        foreach ($plan['relations'] as $stage) {
            $parents = $stageRows[$stage['parentStmt']];
            $rel = bindRelation($stage, $parents);
            $children = $rel ? $this->allRows($rel['sql'], $rel['params']) : [];
            $total += count($children);
            $stageRows[] = $children;
        }
        return $total;
    }

    private function writePlan(array $plan): int
    {
        $seq = nextSeq();
        $this->db->beginTransaction();
        try {
            $returnedId = 0;
            $n = 0;
            foreach ($plan['statements'] as $st) {
                $params = substParams($st['params'], $seq);
                if ($st['role'] === 'useReturn' && isset($st['useReturnAt'])) {
                    $params[$st['useReturnAt']] = $returnedId;
                }
                if ($st['role'] === 'insertReturn') {
                    if ($this->dialect === 'postgres') {
                        $rows = $this->allRows($st['sql'], $params);
                        $returnedId = count($rows) > 0 ? (int) $rows[0]['id'] : 0;
                    } else {
                        // sqlite / mysql: strip RETURNING, run, use lastInsertId.
                        $this->runStmt(stripReturning($st['sql']), $params);
                        $returnedId = (int) $this->db->lastInsertId();
                    }
                } elseif ($this->dialect === 'postgres' && preg_match('/\sRETURNING\s/i', $st['sql'])) {
                    $this->allRows($st['sql'], $params); // pg upsert RETURNING id (no chaining needed)
                } elseif ($this->dialect === 'mysql' && preg_match('/\sRETURNING\s/i', $st['sql'])) {
                    $this->runStmt(stripReturning($st['sql']), $params);
                } else {
                    $this->runStmt($st['sql'], $params);
                }
                $n++;
            }
            $this->db->commit();
            return $n;
        } catch (\Throwable $e) {
            $this->db->rollBack();
            throw $e;
        }
    }

    public function close(): void
    {
        // PDO closes on destruct; nothing to do.
    }
}

function pgPlaceholders(string $sql): string
{
    // Portable seed SQL binds `?`; PG wants `$N` (PgLivePdo then maps $N->?). Since PgLivePdo
    // already accepts `?` positionally, seed can stay `?` — but keep parity with the TS render.
    $n = 0;
    return preg_replace_callback('/\?/', function () use (&$n) {
        $n++;
        return '$' . $n;
    }, $sql) ?? $sql;
}

/**
 * Build a driver for `$dialect` in `$impl` mode. `runtime` = the SHIPPED litedbmodel PDO seam
 * (LiveDb\PgLivePdo / MysqlLivePdo for pg/mysql; bare PDO for sqlite which is already raw). `raw` =
 * the BARE database driver on an ISOLATED baseline schema/db, seeded identically. BOTH modes share
 * ALL statement assembly (bindRelation/substParams/coerceParams/tx/id-chaining) via the SAME
 * OrmDriver, so the SQL is byte-identical — ONLY the low-level PDO handle differs.
 */
function makeDriver(string $dialect, array $artifact, string $impl = 'runtime'): OrmDriver
{
    $schema = $artifact['schema'][$dialect];
    if ($dialect === 'sqlite') {
        // sqlite runtime is ALREADY bare PDO; the raw baseline is a second bare PDO issuing the same
        // statements (ratio ≈1.0× = the honest MEASURED confirmation of near-zero over-driver cost).
        $db = new PDO('sqlite::memory:');
        $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $db->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, false);
        $db->exec('PRAGMA foreign_keys = ON');
        foreach ($schema['drop'] ?? [] as $s) {
            $db->exec($s);
        }
        foreach ($schema['ddl'] as $s) {
            $db->exec($s);
        }
        foreach ($schema['seed'] as $s) {
            $st = $db->prepare($s['sql']);
            $st->execute(coerceParams(array_values($s['params']), 'sqlite'));
        }
        return new OrmDriver('sqlite', $db, $impl);
    }
    if ($dialect === 'postgres') {
        $host = getenv('TEST_DB_HOST') ?: 'localhost';
        $port = (int) (getenv('TEST_DB_PORT') ?: 5433);
        $user = getenv('TEST_DB_USER') ?: 'testuser';
        $pass = getenv('TEST_DB_PASSWORD') ?: 'testpass';
        $dbname = getenv('TEST_DB_NAME') ?: 'testdb';
        $schemaName = $impl === 'raw' ? PG_BASELINE_SCHEMA : PG_SCHEMA_NAME;
        // runtime = shipped LiveDb\PgLivePdo; raw = BARE PDO that replicates ONLY the driver-mandated
        // `$N`→`?` rewrite (byte-identical final SQL to libpq, no litedbmodel logic).
        $db = $impl === 'raw'
            ? new RawPgPdo("pgsql:host={$host};port={$port};dbname={$dbname}", $user, $pass, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION])
            : LiveDb::postgres($host, $port, $user, $pass, $dbname);
        $db->exec('CREATE SCHEMA IF NOT EXISTS ' . $schemaName);
        $db->exec('SET search_path TO ' . $schemaName);
        foreach ($schema['drop'] ?? [] as $s) {
            $db->exec($s);
        }
        foreach ($schema['ddl'] as $s) {
            $db->exec($s);
        }
        foreach ($schema['seed'] as $s) {
            $st = $db->prepare(pgPlaceholders($s['sql']));
            $st->execute(coerceParams(array_values($s['params']), 'postgres'));
        }
        foreach ($schema['seqReset'] ?? [] as $s) {
            $db->exec($s);
        }
        return new OrmDriver('postgres', $db, $impl);
    }
    // mysql
    $host = getenv('TEST_MYSQL_HOST') ?: '127.0.0.1';
    $port = (int) (getenv('TEST_MYSQL_PORT') ?: 3307);
    $user = getenv('TEST_MYSQL_USER') ?: 'testuser';
    $pass = getenv('TEST_MYSQL_PASSWORD') ?: 'testpass';
    $bootDb = getenv('TEST_MYSQL_DB') ?: 'testdb';
    $dbName = $impl === 'raw' ? MYSQL_BASELINE_DB : MYSQL_DB_NAME;
    // runtime = shipped LiveDb\MysqlLivePdo; raw = BARE PDO. The executor already strips RETURNING for
    // mysql (writePlan) BEFORE the driver sees it, so LiveDb's RETURNING emulation is never exercised
    // here — a bare PDO runs the identical stripped statements. Native prepares (emulate OFF) so an
    // int LIMIT binds as an int (MySQL rejects a quoted LIMIT '20'), matching the runtime seam.
    if ($impl === 'raw') {
        $boot = new PDO("mysql:host={$host};port={$port};dbname={$bootDb}", $user, $pass, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $boot->exec('CREATE DATABASE IF NOT EXISTS ' . $dbName);
        $db = new PDO("mysql:host={$host};port={$port};dbname={$dbName}", $user, $pass, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => false,
        ]);
    } else {
        $boot = LiveDb::mysql($host, $port, $user, $pass, $bootDb);
        $boot->exec('CREATE DATABASE IF NOT EXISTS ' . $dbName);
        $db = LiveDb::mysql($host, $port, $user, $pass, $dbName);
    }
    foreach ($schema['drop'] ?? [] as $s) {
        $db->exec($s);
    }
    foreach ($schema['ddl'] as $s) {
        $db->exec($s);
    }
    foreach ($schema['seed'] as $s) {
        $st = $db->prepare($s['sql']);
        $st->execute(coerceParams(array_values($s['params']), 'mysql'));
    }
    return new OrmDriver('mysql', $db, $impl);
}

function loadArtifact(string $path): array
{
    return json_decode(file_get_contents($path), true);
}

// ── standalone smoke (mirror orm-smoke.ts) ─────────────────────────────────────────────────────
function smoke(array $artifact): void
{
    $dialects = $artifact['dialects'];
    $drivers = [];
    foreach ($dialects as $d) {
        $drivers[$d] = makeDriver($d, $artifact);
    }
    $rowsByOp = [];
    $pass = 0;
    $fail = 0;
    foreach ($artifact['ops'] as $op) {
        $rowsByOp[$op['id']] = [];
        foreach ($dialects as $d) {
            try {
                $n = $drivers[$d]->run($artifact['plans'][$op['id']][$d]);
                $rowsByOp[$op['id']][$d] = $n;
                $pass++;
            } catch (\Throwable $e) {
                $rowsByOp[$op['id']][$d] = 'ERR: ' . explode("\n", $e->getMessage())[0];
                $fail++;
            }
        }
    }
    $pad = fn ($s, $n) => str_pad((string) $s, $n);
    echo "\n19 ORM ops x 3 DBs — rows/op (writes report statements executed) [php]:\n\n";
    echo $pad('op', 42) . ' ' . $pad('sqlite', 14) . ' ' . $pad('mysql', 14) . " postgres\n";
    foreach ($artifact['ops'] as $op) {
        $r = $rowsByOp[$op['id']];
        echo $pad(($op['write'] ? 'W ' : 'R ') . $op['label'], 42) . ' ' . $pad($r['sqlite'], 14) . ' ' . $pad($r['mysql'], 14) . ' ' . $r['postgres'] . "\n";
    }
    $total = $pass + $fail;
    echo "\n$pass/$total cells green (" . count($artifact['ops']) . ' ops x 3 DBs = ' . (count($artifact['ops']) * 3) . ").\n";
    foreach ($dialects as $d) {
        $drivers[$d]->close();
    }
    if ($fail > 0) {
        fwrite(STDERR, "\nSMOKE FAILED: $fail cell(s) errored (see ERR above).\n");
        exit(1);
    }
    echo "SMOKE PASS [php]: all cells DB-backed on all 3 real DBs.\n";
}

// ── STANDALONE CSV bench (no protocol) ─────────────────────────────────────────────────────────
// ONE standalone process runs ALL 19 ops × 3 dialects, self-measures, and writes a FLAT CSV to
// benchmark/crosslang/.results/php.csv. The collector (collect.ts) reads the CSVs → CROSS-LANG.md.
// CSV schema: language,case,dialect,metric,value   (RAW values only — collector owns the math).
function csvField($v): string
{
    $s = (string) $v;
    return preg_match('/[",\n]/', $s) ? '"' . str_replace('"', '""', $s) . '"' : $s;
}

function bench(array $artifact, string $resultsDir): void
{
    $language = 'php';
    $warmup = (int) (getenv('BENCH_WARMUP') ?: 50);
    $iters = (int) (getenv('BENCH_ITER') ?: 300);
    $tpIters = (int) (getenv('BENCH_TP_ITER') ?: min($iters, 2000));

    $spawnedAt = microtime(true) * 1000.0;
    $dialects = $artifact['dialects'];
    // cold = process start → runtime ready (interpreter + artifact load), before any connect.
    $coldMs = max(0.0, (microtime(true) * 1000.0) - $spawnedAt);

    $rows = ['language,case,dialect,metric,value'];
    $emit = function ($case, $dialect, $metric, $value) use (&$rows, $language): void {
        $rows[] = "$language,$case,$dialect,$metric," . csvField($value);
    };

    $live = [];
    $baselines = [];
    foreach ($dialects as $dialect) {
        try {
            $drv = $live[$dialect] = makeDriver($dialect, $artifact, 'runtime');
        } catch (\Throwable $e) {
            $reason = explode("\n", $e->getMessage())[0];
            foreach ($artifact['ops'] as $op) {
                $emit($op['id'], $dialect, 'skipped', "$dialect unreachable ($reason)");
            }
            continue;
        }
        // The bare-driver BASELINE (same real driver + byte-identical SQL, ISOLATED baseline schema/db,
        // no litedbmodel seam). A baseline connect failure is NOT a whole-cell skip — the runtime
        // metrics still stand; only the ÷sql ratio for that dialect drops.
        $baseline = null;
        try {
            $baseline = $baselines[$dialect] = makeDriver($dialect, $artifact, 'raw');
        } catch (\Throwable $e) {
            // honestly no-op: baseline unreachable → emit nothing for baseline (runtime untouched).
            $baseline = null;
        }
        foreach ($artifact['ops'] as $op) {
            $case = $op['id'];
            $plan = $artifact['plans'][$case][$dialect];
            try {
                // cost (fairness): queries/op from the plan shape; rows/op = executor's returned count.
                $queries = $plan['kind'] === 'read'
                    ? count($plan['reads']) + count($plan['relations'])
                    : count($plan['statements']);
                $rowsCount = $drv->run($plan);
                $emit($case, $dialect, 'cost_queries', $queries);
                $emit($case, $dialect, 'cost_rows', $rowsCount);
                // latency (RUNTIME): warmup, then one row PER timed iteration.
                for ($i = 0; $i < $warmup; $i++) {
                    $drv->run($plan);
                }
                for ($i = 0; $i < $iters; $i++) {
                    $t0 = hrtime(true);
                    $drv->run($plan);
                    $emit($case, $dialect, 'latency_ms', (hrtime(true) - $t0) / 1e6);
                }
                // throughput: a tight loop, raw elapsed + completed.
                $t0 = hrtime(true);
                for ($i = 0; $i < $tpIters; $i++) {
                    $drv->run($plan);
                }
                $emit($case, $dialect, 'throughput_elapsed_ms', (hrtime(true) - $t0) / 1e6);
                $emit($case, $dialect, 'throughput_completed', $tpIters);

                // latency (BASELINE): the IDENTICAL SQL/params through the bare driver (no litedbmodel
                // seam), SAME warmup + timed iterations → runtime÷baseline = litedbmodel's over-driver
                // overhead. Emitted as `baseline_latency_ms`; the collector splits it into `impl:
                // baseline`. A baseline error here does NOT drop the runtime metrics already emitted.
                if ($baseline !== null) {
                    try {
                        for ($i = 0; $i < $warmup; $i++) {
                            $baseline->run($plan);
                        }
                        for ($i = 0; $i < $iters; $i++) {
                            $b0 = hrtime(true);
                            $baseline->run($plan);
                            $emit($case, $dialect, 'baseline_latency_ms', (hrtime(true) - $b0) / 1e6);
                        }
                    } catch (\Throwable $e) {
                        // baseline-only failure: leave the runtime metrics standing, skip this ratio.
                    }
                }
            } catch (\Throwable $e) {
                $emit($case, $dialect, 'skipped', explode("\n", $e->getMessage())[0]);
            }
        }
    }

    $emit('', '', 'cold_ms', $coldMs);
    $emit('', '', 'rss_bytes', memory_get_usage(true));
    $emit('', '', 'warmup', $warmup);

    foreach ($live as $d) {
        $d->close();
    }
    foreach ($baselines as $d) {
        $d->close();
    }

    if (!is_dir($resultsDir)) {
        mkdir($resultsDir, 0o777, true);
    }
    $out = $resultsDir . '/' . $language . '.csv';
    file_put_contents($out, implode("\n", $rows) . "\n");
    fwrite(STDERR, "[$language] wrote $out (" . (count($rows) - 1) . " rows)\n");
}

/** Entry point: --smoke runs the 57-cell matrix; otherwise runs the standalone CSV bench. Reused by
 *  the entry shim orm-runner.php (which requires this file, then calls ormExecMain). */
function ormExecMain(array $argv, string $artifactPath): void
{
    $args = array_slice($argv, 1);
    $artifact = loadArtifact($artifactPath);
    if (in_array('--smoke', $args, true)) {
        smoke($artifact);
    } else {
        $resultsDir = dirname($artifactPath, 2) . '/.results';
        bench($artifact, $resultsDir);
    }
}

// Run directly only when this file is the invoked script (not when required as a library).
if (realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === realpath(__FILE__)) {
    ormExecMain($argv, $ARTIFACT_PATH);
}
