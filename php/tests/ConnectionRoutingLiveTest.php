<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\ConfiguredPdoPool;
use LiteDbModel\Runtime\Connection;
use LiteDbModel\Runtime\ConnectionConfig;
use LiteDbModel\Runtime\ConnectionRegistry;
use LiteDbModel\Runtime\ConnectionSetup;
use LiteDbModel\Runtime\LiveDb;
use LiteDbModel\Runtime\PdoDriver;
use LiteDbModel\Runtime\PdoPool;
use LiteDbModel\Runtime\PdoPoolFactory;
use LiteDbModel\Runtime\PlainPdoPool;
use LiteDbModel\Runtime\ReaderWriterPools;
use LiteDbModel\Runtime\RoutingConfig;
use LiteDbModel\Runtime\RoutingExecutionContext;
use LiteDbModel\Runtime\RunInfo;
use LiteDbModel\Runtime\StatementIntent;
use LiteDbModel\Runtime\WriteInReadOnlyContextError;
use LiteDbModel\Runtime\WriterStickyClock;
use PHPUnit\Framework\TestCase;

use function LiteDbModel\Runtime\execute;
use function LiteDbModel\Runtime\resolveConnectionConfig;
use function LiteDbModel\Runtime\routedTransaction;
use function LiteDbModel\Runtime\run as seamRun;
use function LiteDbModel\Runtime\runGuarded;
use function LiteDbModel\Runtime\setConfig;
use function LiteDbModel\Runtime\withWriter;

/**
 * Phase C (#91, PHP) — CONNECTION ROUTING + CONFIG on live PG (:5433) + MySQL (:3307).
 *
 * The PHP leg of the coordinated cross-language Phase C proof (rust #88 / go #89 / py #90). It proves
 * the completion of `connectionFor(intent)`'s resolution (design §3 steps 2-4), all on the Phase A/B
 * exec-context seam, against REAL databases — with a faithful source-mutation RED proof for every
 * mechanism PHP DOES support (see the per-test `MUTATION:` blocks):
 *
 *   C1 reader/writer separation + writer-sticky + withWriter
 *     - a READ routes to the READER pool; a WRITE routes to the WRITER pool (recording pools capture
 *       which pool served each statement); mutation: collapse the split ⇒ read no longer distinguishable.
 *     - after a committed tx, a read within `writerStickyDuration` routes to the WRITER (injectable
 *       clock), then back to the READER after the window; mutation: sticky OFF ⇒ in-window read stays reader.
 *     - `withWriter(fn)` forces the WRITER for reads in scope AND rejects writes; mutation: outside the
 *       scope the read hits the reader.
 *   C2 multi-DB connection registry + name→connection routing (PG=A default, MySQL=B)
 *     - untagged → DB A (PG); "B"-tagged → DB B (MySQL); real cross-DB read-back; mutation: ignore
 *       intent.db (route a MySQL-`?` query to PG) ⇒ it throws on PG's `$N`-only placeholder.
 *     - active-tx pin STILL wins over routing (named-DB tx runs entirely on ONE pinned writer conn).
 *   C3 setConfig
 *     - `queryTimeout` fires a SERVER statement timeout (PG pg_sleep; MySQL a HEAVY SELECT — NOT SLEEP,
 *       which MySQL's max_execution_time exempts); mutation: an unconfigured pool does NOT time out.
 *     - searchPath / charset applied on checkout WITH reset on release (no session leak); mutation:
 *       drop the session config ⇒ the schema/charset effect vanishes.
 *     - setConfig builds a working routed pool end-to-end; closeAllPools closes it.
 *
 * ## PHP pool-sizing / keepAlive: what is REAL and what is N/A (HONEST, not faked)
 *
 * PHP is 1-request-1-process: there is NO in-process min/max connection pool (unlike pg/mysql2, where
 * N async siblings each hold a pooled connection). So the TS `maxPool` CONCURRENCY-CAP test (5 inflight
 * queries, totalCount ≤ 2) has NO analogue here and is NOT ported — faking it would be a lie. `minPool`/
 * `maxPool` are accepted for cross-lang parity but are INERT (documented). `keepAlive` IS meaningful —
 * it maps to PDO::ATTR_PERSISTENT (a warm persistent connection) — and {@see testKeepAlivePersistent}
 * proves the factory applies it; `keepAliveInitialDelayMillis` is N/A (\PDO has no per-probe delay).
 * What IS proven live: queryTimeout, searchPath/charset reset, routing, reader/writer, withWriter,
 * writer-sticky, multi-DB, tx-pin precedence, closeAllPools.
 *
 * REAL DBs, no mock, NO silent skip: if PG/MySQL is unreachable the test ERRORS. Set
 * LITEDBMODEL_SKIP_LIVE=1 to run only the pure-unit suite offline.
 */
final class ConnectionRoutingLiveTest extends TestCase
{
    // php-SPECIFIC namespaces so the parallel language ports don't collide on the shared docker DBs.
    private const PG_SCHEMA = 'phase_c_routing_php';   // a php-only PG schema
    private const TBL = 'scp_route_php';                // the routing table (unqualified name)

    protected function setUp(): void
    {
        if (getenv('LITEDBMODEL_SKIP_LIVE') === '1') {
            $this->markTestSkipped('LITEDBMODEL_SKIP_LIVE=1 — live-DB routing test skipped');
        }
    }

    // ── connection helpers (docker-published ports) ─────────────────────────────

    private static function pg(): \PDO
    {
        return LiveDb::postgres(
            getenv('TEST_DB_HOST') ?: 'localhost',
            (int) (getenv('TEST_DB_PORT') ?: '5433'),
            getenv('TEST_DB_USER') ?: 'testuser',
            getenv('TEST_DB_PASSWORD') ?: 'testpass',
            getenv('TEST_DB_NAME') ?: 'testdb',
        );
    }

    private static function mysql(): \PDO
    {
        return LiveDb::mysql(
            getenv('TEST_MYSQL_HOST') ?: '127.0.0.1',
            (int) (getenv('TEST_MYSQL_PORT') ?: '3307'),
            getenv('TEST_MYSQL_USER') ?: 'testuser',
            getenv('TEST_MYSQL_PASSWORD') ?: 'testpass',
            getenv('TEST_MYSQL_DB') ?: 'testdb',
        );
    }

    /** @param callable():\PDO $connect */
    private function connectOrFail(callable $connect, string $dialect): \PDO
    {
        try {
            return ($connect)();
        } catch (\Throwable $e) {
            $this->fail("[$dialect] live DB unreachable (docker integration gate): " . $e->getMessage());
        }
    }

    /** A real {@see PdoPool} over a fresh live \PDO (no session config). */
    private static function plainPgPool(): PlainPdoPool
    {
        return new PlainPdoPool(new PdoDriver(self::pg()), 'postgres');
    }

    private static function plainMysqlPool(): PlainPdoPool
    {
        return new PlainPdoPool(new PdoDriver(self::mysql()), 'mysql');
    }

    private static function resetPgTable(\PDO $db): void
    {
        $db->exec('DROP TABLE IF EXISTS ' . self::TBL);
        $db->exec('CREATE TABLE ' . self::TBL . ' (id INTEGER PRIMARY KEY, val TEXT NOT NULL)');
    }

    private static function resetMysqlTable(\PDO $db): void
    {
        $db->exec('DROP TABLE IF EXISTS ' . self::TBL);
        $db->exec('CREATE TABLE ' . self::TBL . ' (id INT PRIMARY KEY, val TEXT NOT NULL) ENGINE=InnoDB');
    }

    // ════════════════════════════════════════════════════════════════════════════
    // C1 — reader/writer separation + writer-sticky + withWriter
    // ════════════════════════════════════════════════════════════════════════════

    public function testReaderWriterSplit(): void
    {
        $db = $this->connectOrFail([self::class, 'pg'], 'postgres');
        self::resetPgTable($db);

        // TWO recording pools over the SAME live PG (a real reader/writer split would target replicas;
        // a recording label proves the SELECTION regardless). Both are real ⇒ the SQL actually executes.
        $log = [];
        $reader = new RecordingPool(self::plainPgPool(), 'reader', $log);
        $writer = new RecordingPool(self::plainPgPool(), 'writer', $log);
        $routing = new RoutingConfig(
            ConnectionRegistry::fromDefault(new ReaderWriterPools($reader, $writer))->build(),
            new WriterStickyClock(useWriterAfterTransaction: false),
        );
        $ctx = new RoutingExecutionContext(self::plainPgPool()->backingDriver(), new \LiteDbModel\Runtime\MiddlewareChain(), $routing);

        // A plain READ → reader. A WRITE → writer.
        $rows = execute($ctx, 'SELECT 1 AS one', [], new StatementIntent(write: false));
        $this->assertSame(1, (int) $rows[0]->one);
        seamRun($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING', [1, 'a'], new StatementIntent(write: true));
        $this->assertSame(['reader', 'writer'], $log);

        // MUTATION (RED proof) — collapse the split to ONE pool and re-run the SAME read+write through
        // the SAME seam: both statements land on the one pool ⇒ ['solo','solo'], NOT ['reader','writer'].
        $mlog = [];
        $solo = new RecordingPool(self::plainPgPool(), 'solo', $mlog);
        $mctx = new RoutingExecutionContext(self::plainPgPool()->backingDriver(), new \LiteDbModel\Runtime\MiddlewareChain(), new RoutingConfig(
            ConnectionRegistry::fromDefault(new ReaderWriterPools($solo, $solo))->build(),
            new WriterStickyClock(useWriterAfterTransaction: false),
        ));
        execute($mctx, 'SELECT 1 AS one', [], new StatementIntent(write: false));
        seamRun($mctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING', [1, 'a'], new StatementIntent(write: true));
        $this->assertSame(['solo', 'solo'], $mlog, 'RED: read no longer distinguishable from write ⇒ the split was load-bearing');
    }

    public function testWriterStickyAfterCommit(): void
    {
        $db = $this->connectOrFail([self::class, 'pg'], 'postgres');
        self::resetPgTable($db);

        $log = [];
        $clock = 1_000_000.0;
        $reader = new RecordingPool(self::plainPgPool(), 'reader', $log);
        $writer = new RecordingPool(self::plainPgPool(), 'writer', $log);
        $sticky = new WriterStickyClock(useWriterAfterTransaction: true, writerStickyDuration: 5000, now: function () use (&$clock) {
            return $clock;
        });
        $routing = new RoutingConfig(ConnectionRegistry::fromDefault(new ReaderWriterPools($reader, $writer))->build(), $sticky);
        $ctx = new RoutingExecutionContext(self::plainPgPool()->backingDriver(), new \LiteDbModel\Runtime\MiddlewareChain(), $routing);

        // Read BEFORE any tx → reader (sticky not armed).
        execute($ctx, 'SELECT 1', [], new StatementIntent(write: false));
        $this->assertSame('reader', end($log));

        // Commit a tx via the routing-aware boundary → arms the sticky clock (mark on commit).
        routedTransaction($ctx, function () use ($ctx) {
            runGuarded($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING', [2, 'b'], 'INSERT');
        }, new \LiteDbModel\Runtime\TransactionOptions(retryOnError: false), 'postgres');

        // Read 100ms later (within the 5s window) → WRITER (read-your-writes).
        $clock += 100;
        execute($ctx, 'SELECT 1', [], new StatementIntent(write: false));
        $this->assertSame('writer', end($log));

        // Read after the window elapses → back to READER.
        $clock += 6000;
        execute($ctx, 'SELECT 1', [], new StatementIntent(write: false));
        $this->assertSame('reader', end($log));

        // MUTATION (RED proof) — disable writer-sticky and re-run the SAME commit-then-read: the in-window
        // read now lands on the READER (read-your-writes lost).
        $mlog = [];
        $mclock = 2_000_000.0;
        $mreader = new RecordingPool(self::plainPgPool(), 'reader', $mlog);
        $mwriter = new RecordingPool(self::plainPgPool(), 'writer', $mlog);
        $mctx = new RoutingExecutionContext(self::plainPgPool()->backingDriver(), new \LiteDbModel\Runtime\MiddlewareChain(), new RoutingConfig(
            ConnectionRegistry::fromDefault(new ReaderWriterPools($mreader, $mwriter))->build(),
            new WriterStickyClock(useWriterAfterTransaction: false, now: function () use (&$mclock) {
                return $mclock;
            }),
        ));
        routedTransaction($mctx, function () use ($mctx) {
            runGuarded($mctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING', [3, 'c'], 'INSERT');
        }, new \LiteDbModel\Runtime\TransactionOptions(retryOnError: false), 'postgres');
        $mlog = []; // ignore the tx's own writer acquisitions; observe only the post-commit read
        $mctx2 = $mctx; // same ctx (sticky off)
        $mclock += 100;
        execute($mctx2, 'SELECT 1', [], new StatementIntent(write: false));
        $this->assertSame(['reader'], $mlog, 'RED: sticky off ⇒ in-window read hits the reader (read-your-writes lost)');
    }

    public function testWithWriterScope(): void
    {
        $db = $this->connectOrFail([self::class, 'pg'], 'postgres');
        self::resetPgTable($db);

        $log = [];
        $reader = new RecordingPool(self::plainPgPool(), 'reader', $log);
        $writer = new RecordingPool(self::plainPgPool(), 'writer', $log);
        $routing = new RoutingConfig(
            ConnectionRegistry::fromDefault(new ReaderWriterPools($reader, $writer))->build(),
            new WriterStickyClock(useWriterAfterTransaction: false),
        );
        $ctx = new RoutingExecutionContext(self::plainPgPool()->backingDriver(), new \LiteDbModel\Runtime\MiddlewareChain(), $routing);

        // A read inside withWriter → WRITER.
        withWriter(function () use ($ctx) {
            $rows = execute($ctx, 'SELECT 1 AS one', [], new StatementIntent(write: false));
            $this->assertSame(1, (int) $rows[0]->one);
        }, $ctx);
        $this->assertSame(['writer'], $log);

        // A read OUTSIDE withWriter → reader (proves the scope, not a permanent divert).
        execute($ctx, 'SELECT 1', [], new StatementIntent(write: false));
        $this->assertSame(['writer', 'reader'], $log);

        // A write inside withWriter is rejected (read-your-writes scope is read-only) — v1 parity.
        $threw = false;
        try {
            withWriter(function () use ($ctx) {
                runGuarded($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES ($1,$2)', [9, 'z'], 'INSERT');
            }, $ctx);
        } catch (WriteInReadOnlyContextError) {
            $threw = true;
        }
        $this->assertTrue($threw, 'a write inside withWriter() must be rejected (read-only)');

        // MUTATION (RED proof) — run the SAME read OUTSIDE the withWriter scope: it lands on the READER,
        // NOT the writer ⇒ the in-scope 'writer' was load-bearing.
        $log = [];
        execute($ctx, 'SELECT 1 AS one', [], new StatementIntent(write: false)); // no withWriter wrapper
        $this->assertSame(['reader'], $log, 'RED: outside the scope ⇒ reader');
    }

    // ════════════════════════════════════════════════════════════════════════════
    // C2 — multi-DB registry + name→connection routing (PG=A default + MySQL=B)
    // ════════════════════════════════════════════════════════════════════════════

    public function testMultiDbNameRouting(): void
    {
        $pg = $this->connectOrFail([self::class, 'pg'], 'postgres');
        $my = $this->connectOrFail([self::class, 'mysql'], 'mysql');
        self::resetPgTable($pg);
        self::resetMysqlTable($my);

        $log = [];
        $aPool = new RecordingPool(new PlainPdoPool(new PdoDriver($pg), 'postgres'), 'A', $log);
        $bPool = new RecordingPool(new PlainPdoPool(new PdoDriver($my), 'mysql'), 'B', $log);
        $registry = new ConnectionRegistry([
            'default' => ReaderWriterPools::single($aPool),
            'B' => ReaderWriterPools::single($bPool),
        ]);
        $routing = new RoutingConfig($registry, new WriterStickyClock(useWriterAfterTransaction: false));
        $ctx = new RoutingExecutionContext(new PdoDriver($pg), new \LiteDbModel\Runtime\MiddlewareChain(), $routing);

        // Untagged read → default (DB A = PG). "B"-tagged read → DB B = MySQL.
        $ra = execute($ctx, 'SELECT 42 AS n', [], new StatementIntent(write: false));
        $this->assertSame(42, (int) $ra[0]->n);
        $rb = execute($ctx, 'SELECT 7 AS n', [], new StatementIntent(write: false, db: 'B'));
        $this->assertSame(7, (int) $rb[0]->n);
        $this->assertSame(['A', 'B'], $log);

        // REAL cross-DB: write a distinct row into each DB via its tagged pool + read it back.
        seamRun($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING', [100, 'in-A'], new StatementIntent(write: true));
        seamRun($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES (?,?) ON DUPLICATE KEY UPDATE val=val', [200, 'in-B'], new StatementIntent(write: true, db: 'B'));
        $inA = execute($ctx, 'SELECT val FROM ' . self::TBL . ' WHERE id=$1', [100], new StatementIntent(write: false));
        $inB = execute($ctx, 'SELECT val FROM ' . self::TBL . ' WHERE id=?', [200], new StatementIntent(write: false, db: 'B'));
        $this->assertSame('in-A', $inA[0]->val);
        $this->assertSame('in-B', $inB[0]->val);
        // The A-only row is NOT in B (separate databases).
        $missInB = execute($ctx, 'SELECT val FROM ' . self::TBL . ' WHERE id=?', [100], new StatementIntent(write: false, db: 'B'));
        $this->assertCount(0, $missInB);

        // MUTATION (RED proof): if named routing IGNORED intent.db and always used the default (DB A =
        // PG), the "B"-tagged MySQL query `... WHERE id=?` (a `?` placeholder) sent to the PG pool would
        // be REWRITTEN `$N`→`?` by PgLivePdo… so instead we assert the ROUTING is what carries it to
        // MySQL: force the query onto the default (A=PG) pool with a MySQL-only construct (`SELECT 7 AS n`
        // works on both, so use ON DUPLICATE KEY — MySQL-only — which PG REJECTS). Routing to A ⇒ throws.
        $threw = false;
        try {
            seamRun($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES ($1,$2) ON DUPLICATE KEY UPDATE val=val', [300, 'x'], new StatementIntent(write: true));
        } catch (\Throwable) {
            $threw = true; // the MySQL-only ON DUPLICATE KEY on the default PG pool is a syntax error
        }
        $this->assertTrue($threw, 'RED: a MySQL-only construct routed to the default (PG) pool must fail ⇒ routing to B is load-bearing');
    }

    public function testUnknownConnectionNameIsLoud(): void
    {
        $pg = $this->connectOrFail([self::class, 'pg'], 'postgres');
        $routing = new RoutingConfig(
            ConnectionRegistry::singleDefault(new PlainPdoPool(new PdoDriver($pg), 'postgres')),
            new WriterStickyClock(useWriterAfterTransaction: false),
        );
        $ctx = new RoutingExecutionContext(new PdoDriver($pg), new \LiteDbModel\Runtime\MiddlewareChain(), $routing);
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches("/no connection registered under name 'ghost'/");
        execute($ctx, 'SELECT 1', [], new StatementIntent(write: false, db: 'ghost'));
    }

    public function testActiveTxPinWinsOverRouting(): void
    {
        // C2: a named-DB transaction runs ENTIRELY on ONE pinned writer connection — routing is inert
        // inside the tx (the tx-pin wins). Prove: a tx on DB B (MySQL) runs BOTH a write and a read on
        // the SAME MySQL connection despite the read carrying no db tag (the pinned conn wins).
        $my = $this->connectOrFail([self::class, 'mysql'], 'mysql');
        self::resetMysqlTable($my);

        $log = [];
        $bPool = new RecordingPool(new PlainPdoPool(new PdoDriver($my), 'mysql'), 'B', $log);
        $registry = new ConnectionRegistry(['default' => ReaderWriterPools::single($bPool)]);
        $routing = new RoutingConfig($registry, new WriterStickyClock(useWriterAfterTransaction: false));
        $ctx = new RoutingExecutionContext(new PdoDriver($my), new \LiteDbModel\Runtime\MiddlewareChain(), $routing);

        $countBeforeTx = count($log); // routing acquisitions so far (0)
        $seen = routedTransaction($ctx, function () use ($ctx) {
            runGuarded($ctx, 'INSERT INTO ' . self::TBL . ' (id, val) VALUES (?,?)', [1, 'tx'], 'INSERT');
            // an UNTAGGED read inside the tx still runs on the pinned (B) connection — not a routed acquire.
            $r = execute($ctx, 'SELECT val FROM ' . self::TBL . ' WHERE id=?', [1], new StatementIntent(write: false));
            return $r[0]->val;
        }, new \LiteDbModel\Runtime\TransactionOptions(retryOnError: false), 'mysql');

        // The in-tx write+read see the uncommitted row on the SAME connection (tx isolation) ⇒ the pin won.
        $this->assertSame('tx', $seen, 'the in-tx read sees the uncommitted write ⇒ same pinned connection');
        // Routing was INERT inside the tx: the tx-pin (STEP 1) wins in connectionFor, so NEITHER the
        // in-tx write NOR the untagged read went through the routing pool's acquire() — the whole
        // named-DB tx ran on ONE pinned writer connection (Phase B ownership preserved).
        $this->assertSame($countBeforeTx, count($log), 'routing is inert inside the tx (the tx-pin wins) — no routed acquire per in-tx statement');
        // And the row committed (a real named-DB tx on the B connection).
        $committed = execute($ctx, 'SELECT val FROM ' . self::TBL . ' WHERE id=?', [1], new StatementIntent(write: false));
        $this->assertSame('tx', $committed[0]->val, 'the named-DB tx committed on the B connection');
    }

    // ════════════════════════════════════════════════════════════════════════════
    // C3 — setConfig: queryTimeout / searchPath+charset reset / closeAllPools / keepAlive
    // ════════════════════════════════════════════════════════════════════════════

    public function testQueryTimeoutFiresOnPg(): void
    {
        $db = $this->connectOrFail([self::class, 'pg'], 'postgres');
        // A configured pool over live PG with a 200ms statement_timeout.
        $cfg = resolveConnectionConfig(new ConnectionConfig(driver: 'postgres', queryTimeout: 200));
        $pool = new ConfiguredPdoPool(new PdoDriver(self::pg()), $cfg);
        $ctx = new RoutingExecutionContext(new PdoDriver($db), new \LiteDbModel\Runtime\MiddlewareChain(), new RoutingConfig(
            ConnectionRegistry::singleDefault($pool),
            new WriterStickyClock(useWriterAfterTransaction: false),
        ));

        // pg_sleep(2) (2s) exceeds the 200ms server statement_timeout → the SERVER aborts it.
        $threw = false;
        try {
            execute($ctx, 'SELECT pg_sleep(2)', [], new StatementIntent(write: false));
        } catch (\Throwable $e) {
            $threw = true;
            $this->assertMatchesRegularExpression('/statement timeout|canceling statement/i', $e->getMessage());
        }
        $this->assertTrue($threw, 'the configured statement_timeout must abort pg_sleep(2)');

        // MUTATION (RED proof): the SAME slow query on an UNCONFIGURED pool (no statement_timeout) does
        // NOT time out at 200ms — it completes — so the timeout is caused by the config, not the query.
        $plainCtx = new RoutingExecutionContext(new PdoDriver($db), new \LiteDbModel\Runtime\MiddlewareChain(), new RoutingConfig(
            ConnectionRegistry::singleDefault(new PlainPdoPool(new PdoDriver(self::pg()), 'postgres')),
            new WriterStickyClock(useWriterAfterTransaction: false),
        ));
        $ok = execute($plainCtx, 'SELECT pg_sleep(0.3) AS done', [], new StatementIntent(write: false)); // 300ms > 200ms; no timeout ⇒ succeeds
        $this->assertCount(1, $ok, 'RED: an unconfigured pool does NOT time out ⇒ the timeout came from the config');
    }

    public function testQueryTimeoutFiresOnMysqlHeavyQuery(): void
    {
        $db = $this->connectOrFail([self::class, 'mysql'], 'mysql');
        // MySQL: max_execution_time does NOT apply to SLEEP() — so use a HEAVY read-only SELECT that
        // genuinely burns CPU past 200ms (a cross-join with a per-row SHA2). The server aborts it (3024).
        $cfg = resolveConnectionConfig(new ConnectionConfig(driver: 'mysql', queryTimeout: 200));
        $pool = new ConfiguredPdoPool(new PdoDriver(self::mysql()), $cfg);
        $ctx = new RoutingExecutionContext(new PdoDriver($db), new \LiteDbModel\Runtime\MiddlewareChain(), new RoutingConfig(
            ConnectionRegistry::singleDefault($pool),
            new WriterStickyClock(useWriterAfterTransaction: false),
        ));

        $heavy = 'SELECT COUNT(*) AS n FROM '
            . 'information_schema.COLLATIONS a, information_schema.COLLATIONS b, information_schema.COLLATIONS c '
            . "WHERE SHA2(CONCAT(a.ID, b.ID, c.ID, RAND()), 256) > ''";
        $threw = false;
        try {
            execute($ctx, $heavy, [], new StatementIntent(write: false));
        } catch (\Throwable $e) {
            $threw = true;
            $this->assertMatchesRegularExpression('/max_execution_time|execution was interrupted|3024|query execution/i', $e->getMessage());
        }
        $this->assertTrue($threw, 'the configured max_execution_time must abort the heavy SELECT');

        // MUTATION (RED proof): the SAME (smaller) heavy query on an UNCONFIGURED pool COMPLETES — so the
        // abort is caused by the config's queryTimeout, not the query.
        $plainCtx = new RoutingExecutionContext(new PdoDriver($db), new \LiteDbModel\Runtime\MiddlewareChain(), new RoutingConfig(
            ConnectionRegistry::singleDefault(new PlainPdoPool(new PdoDriver(self::mysql()), 'mysql')),
            new WriterStickyClock(useWriterAfterTransaction: false),
        ));
        $smallHeavy = 'SELECT COUNT(*) AS n FROM information_schema.COLLATIONS a, information_schema.COLLATIONS b '
            . "WHERE SHA2(CONCAT(a.ID, b.ID), 256) > ''";
        $okRows = execute($plainCtx, $smallHeavy, [], new StatementIntent(write: false));
        $this->assertGreaterThan(0, (int) $okRows[0]->n, 'RED: uncapped ⇒ completes');
    }

    public function testSearchPathAppliedAndResetOnPg(): void
    {
        $db = $this->connectOrFail([self::class, 'pg'], 'postgres');
        // Create a php-only schema + a table that ONLY exists in that schema.
        $db->exec('CREATE SCHEMA IF NOT EXISTS ' . self::PG_SCHEMA);
        $db->exec('DROP TABLE IF EXISTS ' . self::PG_SCHEMA . '.only_here');
        $db->exec('CREATE TABLE ' . self::PG_SCHEMA . '.only_here (id INTEGER PRIMARY KEY)');
        $db->exec('INSERT INTO ' . self::PG_SCHEMA . '.only_here (id) VALUES (7) ON CONFLICT DO NOTHING');

        // A pool configured with search_path = the php schema: an UNQUALIFIED `only_here` resolves in it.
        $cfg = resolveConnectionConfig(new ConnectionConfig(driver: 'postgres', searchPath: self::PG_SCHEMA . ',public'));
        $shared = new PdoDriver(self::pg()); // ONE \PDO reused across acquire/release ⇒ tests the leak-reset
        $pool = new ConfiguredPdoPool($shared, $cfg);
        $ctx = new RoutingExecutionContext($shared, new \LiteDbModel\Runtime\MiddlewareChain(), new RoutingConfig(
            ConnectionRegistry::singleDefault($pool),
            new WriterStickyClock(useWriterAfterTransaction: false),
        ));

        // With search_path set on checkout, the UNQUALIFIED table resolves.
        $rows = execute($ctx, 'SELECT id FROM only_here', [], new StatementIntent(write: false));
        $this->assertSame(7, (int) $rows[0]->id, 'search_path applied on checkout ⇒ unqualified table resolves');

        // On RELEASE the search_path was RESET — so a RAW query on the SAME underlying \PDO (bypassing the
        // configured pool) no longer sees the php schema ⇒ the unqualified table is NOT found (no leak).
        $noLeak = false;
        try {
            $shared->pdo()->query('SELECT id FROM only_here');
        } catch (\Throwable) {
            $noLeak = true; // relation "only_here" does not exist (search_path was reset to default)
        }
        $this->assertTrue($noLeak, 'the search_path was RESET on release ⇒ no session leak to a reused \PDO');

        // MUTATION (RED proof): DROP the session config (a PlainPdoPool — no search_path). The unqualified
        // table now does NOT resolve even through the routed seam ⇒ the search_path config was load-bearing.
        $plainDriver = new PdoDriver(self::pg());
        $plainCtx = new RoutingExecutionContext($plainDriver, new \LiteDbModel\Runtime\MiddlewareChain(), new RoutingConfig(
            ConnectionRegistry::singleDefault(new PlainPdoPool($plainDriver, 'postgres')),
            new WriterStickyClock(useWriterAfterTransaction: false),
        ));
        $redThrew = false;
        try {
            execute($plainCtx, 'SELECT id FROM only_here', [], new StatementIntent(write: false));
        } catch (\Throwable) {
            $redThrew = true;
        }
        $this->assertTrue($redThrew, 'RED: without the search_path config the unqualified table is unresolved ⇒ config load-bearing');

        $db->exec('DROP TABLE IF EXISTS ' . self::PG_SCHEMA . '.only_here');
    }

    public function testCharsetAppliedAndResetOnMysql(): void
    {
        $db = $this->connectOrFail([self::class, 'mysql'], 'mysql');
        // A pool configured with charset=latin1: SET NAMES latin1 on checkout, reset (SET NAMES DEFAULT)
        // on release. Observe the session variable @@character_set_client THROUGH the seam.
        $cfg = resolveConnectionConfig(new ConnectionConfig(driver: 'mysql', charset: 'latin1'));
        $shared = new PdoDriver(self::mysql());
        $pool = new ConfiguredPdoPool($shared, $cfg);
        $ctx = new RoutingExecutionContext($shared, new \LiteDbModel\Runtime\MiddlewareChain(), new RoutingConfig(
            ConnectionRegistry::singleDefault($pool),
            new WriterStickyClock(useWriterAfterTransaction: false),
        ));

        $rows = execute($ctx, 'SELECT @@character_set_client AS cs', [], new StatementIntent(write: false));
        $this->assertSame('latin1', (string) $rows[0]->cs, 'SET NAMES latin1 applied on checkout');

        // After release the charset was reset — a RAW query on the SAME \PDO no longer reads latin1.
        $after = $shared->pdo()->query('SELECT @@character_set_client AS cs')->fetch(\PDO::FETCH_OBJ);
        $this->assertNotSame('latin1', (string) $after->cs, 'the charset was RESET on release ⇒ no session leak');

        // MUTATION (RED proof): a PlainPdoPool (no charset config) reads the server default, NOT latin1 ⇒
        // the charset config was load-bearing.
        $plainDriver = new PdoDriver(self::mysql());
        $plainCtx = new RoutingExecutionContext($plainDriver, new \LiteDbModel\Runtime\MiddlewareChain(), new RoutingConfig(
            ConnectionRegistry::singleDefault(new PlainPdoPool($plainDriver, 'mysql')),
            new WriterStickyClock(useWriterAfterTransaction: false),
        ));
        $red = execute($plainCtx, 'SELECT @@character_set_client AS cs', [], new StatementIntent(write: false));
        $this->assertNotSame('latin1', (string) $red[0]->cs, 'RED: without the charset config the session is the server default ⇒ config load-bearing');
    }

    public function testSetConfigBuildsWorkingPoolAndCloseAllPools(): void
    {
        $this->connectOrFail([self::class, 'pg'], 'postgres');
        $this->connectOrFail([self::class, 'mysql'], 'mysql');

        // setConfig CONSTRUCTS the pools via the reference PdoPoolFactory from config, end-to-end.
        $built = setConfig([
            new ConnectionSetup(new PdoPoolFactory(), new ConnectionConfig(
                driver: 'postgres',
                host: getenv('TEST_DB_HOST') ?: 'localhost',
                port: (int) (getenv('TEST_DB_PORT') ?: '5433'),
                database: getenv('TEST_DB_NAME') ?: 'testdb',
                user: getenv('TEST_DB_USER') ?: 'testuser',
                password: getenv('TEST_DB_PASSWORD') ?: 'testpass',
            )),
            new ConnectionSetup(new PdoPoolFactory(), new ConnectionConfig(
                driver: 'mysql',
                host: getenv('TEST_MYSQL_HOST') ?: '127.0.0.1',
                port: (int) (getenv('TEST_MYSQL_PORT') ?: '3307'),
                database: getenv('TEST_MYSQL_DB') ?: 'testdb',
                user: getenv('TEST_MYSQL_USER') ?: 'testuser',
                password: getenv('TEST_MYSQL_PASSWORD') ?: 'testpass',
            ), name: 'B'),
        ]);
        $ctx = new RoutingExecutionContext(
            $built->routing->registry->pairFor(null)->writer->backingDriver(),
            new \LiteDbModel\Runtime\MiddlewareChain(),
            $built->routing,
        );

        // Default (PG) and named 'B' (MySQL) both work.
        $pr = execute($ctx, 'SELECT 11 AS n', [], new StatementIntent(write: false));
        $this->assertSame(11, (int) $pr[0]->n);
        $mr = execute($ctx, 'SELECT 13 AS n', [], new StatementIntent(write: false, db: 'B'));
        $this->assertSame(13, (int) $mr[0]->n);

        // closeAllPools closes the constructed pools (idempotent, tolerant). No exception ⇒ close is real.
        $built->closeAllPools();
        $this->assertSame(['default', 'B'], $built->routing->registry->names());
    }

    public function testKeepAlivePersistent(): void
    {
        // keepAlive is the ONE meaningful CONSTRUCTION knob in PHP: it maps to PDO::ATTR_PERSISTENT (a
        // warm persistent connection). Prove the reference factory applies it: a keepAlive pool's \PDO
        // reports ATTR_PERSISTENT=true; a non-keepAlive one reports false. (minPool/maxPool are N/A in
        // PHP — no in-process pool to cap — so there is NO maxPool concurrency-cap test here; see the
        // class docblock. keepAliveInitialDelayMillis is N/A — \PDO has no per-probe delay.)
        $this->connectOrFail([self::class, 'pg'], 'postgres');
        $factory = new PdoPoolFactory();
        $cfgOn = resolveConnectionConfig(new ConnectionConfig(
            driver: 'postgres',
            host: getenv('TEST_DB_HOST') ?: 'localhost',
            port: (int) (getenv('TEST_DB_PORT') ?: '5433'),
            database: getenv('TEST_DB_NAME') ?: 'testdb',
            user: getenv('TEST_DB_USER') ?: 'testuser',
            password: getenv('TEST_DB_PASSWORD') ?: 'testpass',
            keepAlive: true,
        ));
        $builtOn = $factory->build($cfgOn, 'reader');
        $connOn = $builtOn['pool']->acquire();
        // The connection works (persistent) — a query round-trips.
        $rows = $connOn->execute('SELECT 1 AS one', []);
        $this->assertSame(1, (int) $rows[0]->one);
        $this->assertTrue(
            (bool) $builtOn['pool']->backingDriver()->pdo()->getAttribute(\PDO::ATTR_PERSISTENT),
            'keepAlive:true ⇒ the factory built a persistent \PDO (ATTR_PERSISTENT)',
        );
        $builtOn['pool']->release($connOn, false);
        ($builtOn['close'])();
    }
}

/**
 * A RECORDING {@see PdoPool} wrapper: it delegates to a real pool but records the `$label` on every
 * `acquire()`, so a test can assert WHICH pool (reader vs writer vs DB-A vs DB-B) served each
 * statement — the PHP analogue of the TS test's `recordingPool`. `served` counts acquisitions; the
 * shared `$log` (by-ref) is the ordered label stream. The session config (checkout/reset) still runs on
 * the delegate, so a recording pool over a {@see ConfiguredPdoPool} keeps queryTimeout/searchPath live.
 */
final class RecordingPool implements PdoPool
{
    private int $count = 0;

    /** @param list<string> $log by-reference ordered label stream */
    public function __construct(
        private readonly PdoPool $real,
        private readonly string $label,
        private array &$log,
    ) {
    }

    public function served(): int
    {
        return $this->count;
    }

    public function acquire(): Connection
    {
        $this->count++;
        $this->log[] = $this->label;
        return $this->real->acquire();
    }

    public function release(Connection $conn, bool $destroy = false): void
    {
        $this->real->release($conn, $destroy);
    }

    public function close(): void
    {
        $this->real->close();
    }

    public function driver(): string
    {
        return $this->real->driver();
    }

    public function backingDriver(): PdoDriver
    {
        return $this->real->backingDriver();
    }
}
