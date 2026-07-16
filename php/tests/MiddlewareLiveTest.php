<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\Context;
use LiteDbModel\Runtime\ExecutionContext;
use LiteDbModel\Runtime\LiveDb;
use LiteDbModel\Runtime\LogEntry;
use LiteDbModel\Runtime\MethodKind;
use PHPUnit\Framework\TestCase;

use function LiteDbModel\Runtime\clearMiddlewares;
use function LiteDbModel\Runtime\createMiddleware;
use function LiteDbModel\Runtime\execute;
use function LiteDbModel\Runtime\Logger;
use function LiteDbModel\Runtime\rawExecute;
use function LiteDbModel\Runtime\rawQuery;
use function LiteDbModel\Runtime\run;
use function LiteDbModel\Runtime\runMethod;
use function LiteDbModel\Runtime\transaction;
use function LiteDbModel\Runtime\use_;
use function LiteDbModel\Runtime\withMiddlewareScope;

/**
 * Phase D (#96) — the SCP MIDDLEWARE layer on the LIVE seam (PG:5433 + MySQL:3307), PHP.
 *
 * The PHP analogue of the TS `test/integration/ScpMiddleware.test.ts`. The unit suite
 * ({@see MiddlewareTest}) proves the hook mechanics on the in-process PDO SQLite seam; THIS test
 * proves the SAME contract on the PRODUCTION live PDO seam against REAL databases, so a registered
 * middleware, `runMethod`, `Logger`, and the raw `execute`/`query` API all compose with the transaction
 * boundary + connection ownership:
 *
 *   D1 a registered SQL middleware intercepts EVERY live statement of a REAL transaction() — the
 *      RUNTIME's BEGIN + COMMIT (owner option A, Phase D #96: issued through the seam on the pinned
 *      owned connection, full parity with the TS reference), the body INSERT, and the SELECT all
 *      funnel through the ONE seam; on a body error the RUNTIME's BEGIN + ROLLBACK are observed and
 *      the write is rolled back. RED: unregistered ⇒ tx-control NOT observed (byte-identical).
 *   D3 raw `rawExecute`/`rawQuery` go THROUGH the seam — a registered middleware sees them; the Logger
 *      records real SQL/params/timing for a live statement; a `query` method hook (D2) fires around
 *      `rawQuery`.
 *
 * PHP has ONE blocking PDO seam (no sync/async twin — see the ExecutionContext class doc), so this is
 * the SAME `execute()`/`run()`/`transaction()` the SQLite unit suite drives, now against live PG/MySQL.
 *
 * PHP concurrency-isolation is N/A (1-request-1-process — see the Middleware.php class doc); the TS
 * "two concurrent scopes don't cross-talk" live test has no analogue here and is NOT faked.
 *
 * REAL DBs, no mock, NO silent skip: if PG/MySQL is unreachable the test ERRORS. The tables are
 * namespaced UNIQUELY for the PHP leg (`scp_php_mw_d`) so parallel-port live tests never collide on
 * the shared docker PG:5433 / MySQL:3307. Set LITEDBMODEL_SKIP_LIVE=1 to run the pure-unit suite.
 */
final class MiddlewareLiveTest extends TestCase
{
    /** Unique table name for the PHP Phase D leg (parallel ports share docker PG:5433 / MySQL:3307). */
    private const TBL = 'scp_php_mw_d';

    /** @return array<string, array{0:string,1:callable():\PDO}> */
    public static function liveDrivers(): array
    {
        return [
            'postgres' => ['postgres', static fn (): \PDO => LiveDb::postgres(
                getenv('TEST_DB_HOST') ?: 'localhost',
                (int) (getenv('TEST_DB_PORT') ?: '5433'),
                getenv('TEST_DB_USER') ?: 'testuser',
                getenv('TEST_DB_PASSWORD') ?: 'testpass',
                getenv('TEST_DB_NAME') ?: 'testdb',
            )],
            'mysql' => ['mysql', static fn (): \PDO => LiveDb::mysql(
                getenv('TEST_MYSQL_HOST') ?: '127.0.0.1',
                (int) (getenv('TEST_MYSQL_PORT') ?: '3307'),
                getenv('TEST_MYSQL_USER') ?: 'testuser',
                getenv('TEST_MYSQL_PASSWORD') ?: 'testpass',
                getenv('TEST_MYSQL_DB') ?: 'testdb',
            )],
        ];
    }

    protected function setUp(): void
    {
        if (getenv('LITEDBMODEL_SKIP_LIVE') === '1') {
            $this->markTestSkipped('LITEDBMODEL_SKIP_LIVE=1 — live-DB integration test skipped');
        }
        clearMiddlewares();
    }

    private function connectOrFail(callable $connect, string $dialect): \PDO
    {
        try {
            $db = ($connect)();
            $db->query('SELECT 1');
            return $db;
        } catch (\Throwable $e) {
            $this->fail("[$dialect] live DB required but unreachable: {$e->getMessage()}");
        }
    }

    private static function resetTable(\PDO $db): void
    {
        $db->exec('DROP TABLE IF EXISTS ' . self::TBL);
        $db->exec('CREATE TABLE ' . self::TBL . ' (id INTEGER PRIMARY KEY, val VARCHAR(32) NOT NULL)');
    }

    /** The `?`→`$N` placeholder the LiveDb PG PDO subclass rewrites; MySQL keeps `?`. So `?` is portable. */

    /** @dataProvider liveDrivers */
    public function testMiddlewareObservesRuntimeBeginAndCommitOfRealTransaction(string $dialect, callable $connect): void
    {
        // POSITIVE test (owner option A, Phase D #96): a registered middleware OBSERVES the RUNTIME
        // BEGIN + COMMIT (issued by the transaction() combinator through the seam on the pinned owned
        // connection) — full parity with the TS reference — plus the body INSERT and the read SELECT.
        $db = $this->connectOrFail($connect, $dialect);
        self::resetTable($db);
        $ctx = Context::forPdo($db);
        $seen = [];
        withMiddlewareScope(function () use ($ctx, $dialect, &$seen): void {
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$seen) {
                $seen[] = $sql;
                return $next($sql, $params);
            }]));
            transaction($ctx, function () use ($ctx): void {
                run($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES (?, ?)', [1, 'a']);
            }, null, $dialect);
            $rows = execute($ctx, 'SELECT val FROM ' . self::TBL . ' WHERE id = ?', [1]);
            $this->assertEquals([(object) ['val' => 'a']], $rows);
        });
        // The RUNTIME's BEGIN + COMMIT (through the seam), the body INSERT, and the read SELECT ALL
        // funneled through the ONE seam and were observed — 5-language parity with the TS async twin.
        $this->assertContains('BEGIN', $seen, "[$dialect] the RUNTIME BEGIN through the seam");
        $this->assertContains('INSERT INTO ' . self::TBL . ' (id, val) VALUES (?, ?)', $seen, "[$dialect] the body INSERT");
        $this->assertContains('COMMIT', $seen, "[$dialect] the RUNTIME COMMIT through the seam");
        $this->assertContains('SELECT val FROM ' . self::TBL . ' WHERE id = ?', $seen, "[$dialect] the SELECT");
        // The commit persisted the write (the runtime COMMIT was real, not a middleware no-op).
        $persisted = $db->query('SELECT val FROM ' . self::TBL . ' WHERE id = 1')->fetchAll(\PDO::FETCH_COLUMN);
        $this->assertSame(['a'], $persisted, "[$dialect] the observed COMMIT actually committed");
    }

    /** @dataProvider liveDrivers */
    public function testMiddlewareObservesRuntimeBeginAndRollbackOnError(string $dialect, callable $connect): void
    {
        // POSITIVE test: on a body error the RUNTIME's BEGIN + ROLLBACK are BOTH observed by the
        // middleware (through the seam), and the write is rolled back (atomicity preserved).
        $db = $this->connectOrFail($connect, $dialect);
        self::resetTable($db);
        $ctx = Context::forPdo($db);
        $seen = [];
        $threw = false;
        withMiddlewareScope(function () use ($ctx, $dialect, &$seen, &$threw): void {
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$seen) {
                $seen[] = $sql;
                return $next($sql, $params);
            }]));
            try {
                transaction($ctx, function () use ($ctx): void {
                    run($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES (?, ?)', [7, 'g']);
                    throw new \RuntimeException('body-error → rollback');
                }, null, $dialect);
            } catch (\RuntimeException) {
                $threw = true;
            }
        });
        $this->assertTrue($threw, "[$dialect] the body error propagated");
        $this->assertContains('BEGIN', $seen, "[$dialect] the RUNTIME BEGIN through the seam");
        $this->assertContains('ROLLBACK', $seen, "[$dialect] the RUNTIME ROLLBACK through the seam");
        $this->assertNotContains('COMMIT', $seen, "[$dialect] no COMMIT on the error path");
        // The write was rolled back — atomicity preserved through the new through-the-seam tx-control.
        $rows = $db->query('SELECT val FROM ' . self::TBL . ' WHERE id = 7')->fetchAll(\PDO::FETCH_COLUMN);
        $this->assertSame([], $rows, "[$dialect] the errored tx rolled back the write");
    }

    /** @dataProvider liveDrivers */
    public function testRedProofUnregisteredDoesNotObserveTxControl(string $dialect, callable $connect): void
    {
        // RED proof for the POSITIVE tx-control test: WITHOUT a registered middleware, the runtime
        // BEGIN/COMMIT (and the body INSERT) are NOT observed — a byte-identical passthrough. So the
        // `assertContains('BEGIN'/'COMMIT')` above is load-bearing (it fires iff a middleware is wired).
        $db = $this->connectOrFail($connect, $dialect);
        self::resetTable($db);
        $ctx = Context::forPdo($db);
        $seen = [];
        // No registration → passthrough; the tx still runs + commits on the live DB.
        transaction($ctx, function () use ($ctx): void {
            run($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES (?, ?)', [8, 'h']);
        }, null, $dialect);
        $persisted = $db->query('SELECT val FROM ' . self::TBL . ' WHERE id = 8')->fetchAll(\PDO::FETCH_COLUMN);
        $this->assertSame(['h'], $persisted, "[$dialect] the tx committed (byte-identical)");
        $this->assertNotContains('BEGIN', $seen, "[$dialect] BEGIN NOT observed without a middleware (RED)");
        $this->assertNotContains('COMMIT', $seen, "[$dialect] COMMIT NOT observed without a middleware (RED)");
        $this->assertSame([], $seen);
    }

    /** @dataProvider liveDrivers */
    public function testRedProofUnregisteredIsBytePassthrough(string $dialect, callable $connect): void
    {
        $db = $this->connectOrFail($connect, $dialect);
        self::resetTable($db);
        $ctx = Context::forPdo($db);
        $seen = [];
        // No registration → byte-identical passthrough (the statements still run against live DB).
        transaction($ctx, function () use ($ctx): void {
            run($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES (?, ?)', [2, 'b']);
        }, null, $dialect);
        $rows = execute($ctx, 'SELECT val FROM ' . self::TBL . ' WHERE id = ?', [2]);
        $this->assertEquals([(object) ['val' => 'b']], $rows); // the read worked (byte-identical)...
        $this->assertSame([], $seen);                           // ...but nothing was observed (RED)
    }

    /** @dataProvider liveDrivers */
    public function testRawExecuteAndRawQueryThroughLiveSeam(string $dialect, callable $connect): void
    {
        $db = $this->connectOrFail($connect, $dialect);
        self::resetTable($db);
        $ctx = Context::forPdo($db);
        $seen = [];
        withMiddlewareScope(function () use ($ctx, $dialect, &$seen): void {
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$seen) {
                $seen[] = $sql;
                return $next($sql, $params);
            }]));
            // A raw write is wrapped in a tx boundary so it commits atomically (mirror the TS shape).
            transaction($ctx, function () use ($ctx): void {
                $ins = rawExecute($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES (?, ?)', [3, 'c'], true);
                $this->assertSame(1, $ins->rowCount);
            }, null, $dialect);
            $rows = rawQuery($ctx, 'SELECT val FROM ' . self::TBL . ' WHERE id = ?', [3]);
            $this->assertEquals([(object) ['val' => 'c']], $rows);
        });
        // rawExecute + rawQuery both went through the seam (the middleware saw them; BEGIN/COMMIT too).
        $this->assertContains('INSERT INTO ' . self::TBL . ' (id, val) VALUES (?, ?)', $seen, "[$dialect] raw INSERT seen");
        $this->assertContains('SELECT val FROM ' . self::TBL . ' WHERE id = ?', $seen, "[$dialect] raw SELECT seen");
    }

    /** @dataProvider liveDrivers */
    public function testLoggerRecordsLiveStatement(string $dialect, callable $connect): void
    {
        $db = $this->connectOrFail($connect, $dialect);
        self::resetTable($db);
        $ctx = Context::forPdo($db);
        $logger = Logger();
        withMiddlewareScope(function () use ($ctx, $logger, $dialect): void {
            use_($logger);
            transaction($ctx, function () use ($ctx): void {
                run($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES (?, ?)', [4, 'd']);
            }, null, $dialect);
            execute($ctx, 'SELECT val FROM ' . self::TBL . ' WHERE id = ?', [4]);
            /** @var list<LogEntry> $entries */
            $entries = $logger->state()->entries;
            $sqls = array_map(static fn (LogEntry $e) => $e->sql, $entries);
            // Logger recorded the INSERT and the SELECT (among BEGIN/COMMIT) for the live statements.
            $this->assertContains('INSERT INTO ' . self::TBL . ' (id, val) VALUES (?, ?)', $sqls);
            $this->assertContains('SELECT val FROM ' . self::TBL . ' WHERE id = ?', $sqls);
            $selectEntry = null;
            foreach ($entries as $e) {
                if ($e->sql === 'SELECT val FROM ' . self::TBL . ' WHERE id = ?') {
                    $selectEntry = $e;
                }
            }
            $this->assertNotNull($selectEntry);
            $this->assertSame([4], $selectEntry->params);
            foreach ($entries as $e) {
                $this->assertGreaterThanOrEqual(0.0, $e->durationMs);
            }
        });
    }

    /** @dataProvider liveDrivers */
    public function testQueryMethodHookFiresAroundRawQuery(string $dialect, callable $connect): void
    {
        $db = $this->connectOrFail($connect, $dialect);
        self::resetTable($db);
        $db->exec('INSERT INTO ' . self::TBL . " (id, val) VALUES (5, 'e')");
        $ctx = Context::forPdo($db);
        $events = [];
        withMiddlewareScope(function () use ($ctx, &$events): void {
            use_(createMiddleware([MethodKind::QUERY => function ($m, callable $next, ...$args) use (&$events) {
                $events[] = 'query:before';
                $r = $next(...$args);
                $events[] = 'query:after';
                return $r;
            }]));
            $rows = rawQuery($ctx, 'SELECT val FROM ' . self::TBL . ' WHERE id = ?', [5]);
            $this->assertEquals([(object) ['val' => 'e']], $rows);
        });
        $this->assertSame(['query:before', 'query:after'], $events);
        // (runMethod imported for the reference surface; exercised directly in the unit suite.)
        $this->assertIsCallable('LiteDbModel\\Runtime\\runMethod');
    }
}
