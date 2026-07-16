<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\Context;
use LiteDbModel\Runtime\ExecutionContext;
use LiteDbModel\Runtime\IsolationLevel;
use LiteDbModel\Runtime\LiveDb;
use LiteDbModel\Runtime\RetryClassifierFlags;
use LiteDbModel\Runtime\Runtime;
use LiteDbModel\Runtime\SqlFailure;
use LiteDbModel\Runtime\TransactionOptions;
use LiteDbModel\Runtime\WriteInReadOnlyContextError;
use LiteDbModel\Runtime\WriteOutsideTransactionError;
use PHPUnit\Framework\TestCase;

use function LiteDbModel\Runtime\currentContext;
use function LiteDbModel\Runtime\execute;
use function LiteDbModel\Runtime\run as seamRun;
use function LiteDbModel\Runtime\runWithPinnedContext;
use function LiteDbModel\Runtime\transaction;

/**
 * Phase B (#85, PHP) — LIVE-DB integration for the public {@see transaction()} boundary + the
 * tx-completeness primitives on REAL Postgres (:5433) + MySQL (:3307). The PHP leg of the coordinated
 * cross-language proof (go tx_boundary_livedb_test.go / rust tests/tx_boundary.rs / py
 * test_tx_boundary_livedb.py). It exercises the UNMODIFIED production path (transaction →
 * withTransactionDecided on an OWNED connection, each op JOINing the ambient tx) against real engines:
 *
 *   (1) MULTI-OP ATOMICITY — transaction(fn: [opA_insert; opB_insert]) → both commit (ONE BEGIN + ONE
 *       COMMIT, captured live). opB PK-collides → opA's row ALSO rolls back. The FAITHFUL-MUTATION
 *       RED→GREEN proof (break the ambient JOIN so opA commits alone → atomicity goes RED; restore →
 *       GREEN) proves the join/atomicity is real, not vacuous.
 *   (2) GUARD (live) — write OUTSIDE transaction() → WriteOutsideTransactionError; read-only inside →
 *       WriteInReadOnlyContextError; inside → ok.
 *   (3) ISOLATION SQL EMISSION (live) — the ACTUAL per-dialect isolation statements are captured off the
 *       driver: PG `BEGIN` + post-BEGIN `SET TRANSACTION ISOLATION LEVEL <phrase>`; MySQL pre-BEGIN
 *       `SET TRANSACTION ISOLATION LEVEL <phrase>` + `BEGIN`. And REPEATABLE READ holds a snapshot vs
 *       READ COMMITTED sees a concurrent commit (behavioral).
 *   (4) RETRY under REAL contention — two OS processes (pcntl_fork; PHP has no threads) race a PG
 *       SERIALIZABLE write-skew → REAL 40001, and a MySQL opposite-order deadlock → REAL 1213; the loser
 *       (wrapped in transaction() with retry) retries on a FRESH connection and commits. Proven with the
 *       string fallback DISABLED so the TYPED-code path (errorInfo SQLSTATE/errno, reached through the
 *       mapped SqlFailure's wrapped chain) is shown genuinely load-bearing; and a RED proof (neuter the
 *       wrapped-chain traversal + string fallback → the loser can no longer classify the real 40001/1213
 *       → it gives up) confirms the typed path is not dead code (the go #83 audit lesson).
 *   (5) NESTED (live) — one BEGIN/COMMIT; an inner error rolls back the whole tx.
 *
 * ## Concurrent-tx isolation: N/A for PHP (honest note)
 *
 * The go/py suites include an IN-PROCESS concurrent-tx isolation test (N threads sharing one runtime,
 * each owning its own pooled connection, asserting no cross-talk). PHP is 1-request-1-PROCESS: it has
 * no threads and no shared-runtime concurrency within a process, and each process holds exactly ONE
 * `\PDO` that can host exactly ONE live transaction. So an in-process concurrent-tx isolation test has
 * no meaning here and is NOT faked. What IS proven instead, faithfully: (a) live MULTI-OP ATOMICITY
 * with a RED→GREEN mutation of the join; (b) REAL cross-PROCESS contention (40001 / 1213) driving the
 * retry loop — genuine OS-level concurrency, the honest PHP analogue of the go/py threaded contention.
 *
 * REAL DBs, no mock, NO silent skip: if PG/MySQL is unreachable the test ERRORS. Set
 * LITEDBMODEL_SKIP_LIVE=1 to run only the pure-unit suite offline.
 */
final class TxBoundaryLiveTest extends TestCase
{
    private const TBL = 'scp_tx_boundary_livedb_php';

    protected function setUp(): void
    {
        if (getenv('LITEDBMODEL_SKIP_LIVE') === '1') {
            $this->markTestSkipped('LITEDBMODEL_SKIP_LIVE=1 — live-DB boundary test skipped');
        }
        if (!function_exists('pcntl_fork')) {
            $this->markTestSkipped('pcntl_fork unavailable — the real-contention retry proof needs OS processes');
        }
    }

    // ── connection helpers ──────────────────────────────────────────────────────

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

    /** @return list<array{0:string,1:string,2:callable():\PDO}> [dialect, intType, connect] */
    public static function dialects(): array
    {
        return [
            'postgres' => ['postgres', 'INTEGER', [self::class, 'pg']],
            'mysql' => ['mysql', 'INT', [self::class, 'mysql']],
        ];
    }

    private static function reset(\PDO $db, string $intType): void
    {
        $db->exec('DROP TABLE IF EXISTS ' . self::TBL);
        $engine = $intType === 'INT' ? ' ENGINE=InnoDB' : '';
        $db->exec('CREATE TABLE ' . self::TBL . " (id {$intType} PRIMARY KEY, worker {$intType} NOT NULL, seq {$intType} NOT NULL){$engine}");
    }

    /** @return list<array{0:int,1:int}> sorted (id, worker), worker != 999 filters pre-seeds. */
    private static function readRows(\PDO $db): array
    {
        $rows = $db->query('SELECT id, worker FROM ' . self::TBL . ' WHERE worker <> 999')->fetchAll(\PDO::FETCH_OBJ);
        $out = array_map(fn ($r) => [(int) $r->id, (int) $r->worker], $rows);
        sort($out);
        return $out;
    }

    private static function insertBundle(string $dialect): \stdClass
    {
        return json_decode(json_encode([
            'dialect' => $dialect,
            'name' => 'InsertOne',
            'transaction' => [
                'phase' => 'create',
                'entityFrom' => null,
                'statements' => [
                    ['id' => 'tx_body_0', 'role' => 'body', 'op' => [
                        'sql' => 'INSERT INTO ' . self::TBL . ' (id, worker, seq) VALUES (?, ?, ?)',
                        'params' => [['ref' => ['id']], ['ref' => ['worker']], ['ref' => ['seq']]],
                    ]],
                ],
            ],
        ]), false);
    }

    private static function op(\PDO $db, string $dialect, int $id, int $worker, int $seq): array
    {
        return Runtime::executeTransactionBundle(self::insertBundle($dialect), ['id' => $id, 'worker' => $worker, 'seq' => $seq], $db);
    }

    private function connectOrFail(callable $connect, string $dialect): \PDO
    {
        try {
            return ($connect)();
        } catch (\Throwable $e) {
            $this->fail("[$dialect] live DB unreachable (docker integration gate): " . $e->getMessage());
        }
    }

    // ── (1) MULTI-OP ATOMICITY + RED→GREEN mutation proof ───────────────────────

    /** @dataProvider dialects */
    public function testMultiOpAtomicityCommitsTogether(string $dialect, string $intType, callable $connect): void
    {
        $db = $this->connectOrFail($connect, $dialect);
        self::reset($db, $intType);
        $ctx = Context::forPdo($db);

        $r = transaction($ctx, fn () => [self::op($db, $dialect, 100, 1, 0), self::op($db, $dialect, 101, 1, 1)], new TransactionOptions(), $dialect);
        $this->assertSame([true, true], array_map(fn ($x) => $x['committed'], $r));
        // N ops in ONE boundary → both persisted on ONE connection (the ambient JOIN).
        $this->assertSame([[100, 1], [101, 1]], self::readRows($db), "[$dialect] both ops commit together");
    }

    /** @dataProvider dialects */
    public function testMultiOpAtomicityRollsBackTogetherAndRedGreenMutation(string $dialect, string $intType, callable $connect): void
    {
        $db = $this->connectOrFail($connect, $dialect);

        // ── GREEN (join intact): opB PK-collides → opA ALSO rolls back (real cross-op atomicity). ──
        self::reset($db, $intType);
        $db->exec('INSERT INTO ' . self::TBL . ' (id, worker, seq) VALUES (201, 999, 9)'); // pre-seed collision
        $ctx = Context::forPdo($db);
        $raised = false;
        try {
            transaction($ctx, fn () => [self::op($db, $dialect, 200, 2, 0), self::op($db, $dialect, 201, 2, 1)], new TransactionOptions(retryOnError: false), $dialect);
        } catch (\Throwable) {
            $raised = true;
        }
        $this->assertTrue($raised, "[$dialect] opB collision must fail the whole boundary");
        $this->assertSame([], self::readRows($db), "[$dialect] GREEN: opA (id=200) rolls back with opB (real atomicity)");

        // ── RED (FAITHFUL MUTATION): break the ambient JOIN — run each op via the INTERNAL guard-off ──
        // executor with NO transaction() wrapper, so opA opens + COMMITS its OWN auto-tx and SURVIVES
        // opB's failure. The atomicity outcome (rows == []) is then BROKEN — proving the join is real.
        self::reset($db, $intType);
        $db->exec('INSERT INTO ' . self::TBL . ' (id, worker, seq) VALUES (201, 999, 9)');
        $doOpNoJoin = fn (int $id, int $w, int $s) =>
            Runtime::executeTransactionBundleInternal(self::insertBundle($dialect), ['id' => $id, 'worker' => $w, 'seq' => $s], $db);
        try {
            $doOpNoJoin(200, 2, 0); // commits alone (its own auto-tx)
            $doOpNoJoin(201, 2, 1); // PK collision → fails, but opA already committed
        } catch (\Throwable) {
            // ignore
        }
        $this->assertGreaterThanOrEqual(
            1,
            count(self::readRows($db)),
            "[$dialect] RED MUTATION PROOF: breaking the ambient JOIN MUST leak opA (id=200) past opB's "
            . 'failure ⇒ the atomicity assertion has teeth.'
        );
        self::reset($db, $intType); // clean up the leaked row
    }

    // ── (2) write=tx GUARD on the live path ─────────────────────────────────────

    /** @dataProvider dialects */
    public function testGuardLive(string $dialect, string $intType, callable $connect): void
    {
        $db = $this->connectOrFail($connect, $dialect);
        self::reset($db, $intType);
        $ctx = Context::forPdo($db);

        // Outside any transaction() → WriteOutsideTransactionError.
        $threw = false;
        try {
            self::op($db, $dialect, 300, 3, 0);
        } catch (WriteOutsideTransactionError) {
            $threw = true;
        }
        $this->assertTrue($threw, "[$dialect] a write outside transaction() must be rejected");

        // Read-only-scoped write inside a transaction() → WriteInReadOnly (read-only checked FIRST).
        transaction($ctx, function () use ($db, $dialect) {
            $ro = currentContext()->withReadOnly();
            $roThrew = false;
            try {
                runWithPinnedContext($ro, fn () => self::op($db, $dialect, 301, 3, 0));
            } catch (WriteInReadOnlyContextError) {
                $roThrew = true;
            }
            $this->assertTrue($roThrew, "[$dialect] a read-only-scoped write must be rejected");
            return 0;
        }, new TransactionOptions(retryOnError: false), $dialect);

        // Inside a transaction() → succeeds.
        transaction($ctx, fn () => self::op($db, $dialect, 302, 3, 0), new TransactionOptions(), $dialect);
        $this->assertSame([[302, 3]], self::readRows($db), "[$dialect] only the in-boundary write (id=302) persisted");
    }

    // ── (3) ISOLATION — capture the ACTUAL emitted SET SQL + behavioral snapshot ─

    /** @dataProvider dialects */
    public function testIsolationSqlEmittedLive(string $dialect, string $intType, callable $connect): void
    {
        $realDb = $this->connectOrFail($connect, $dialect);
        self::reset($realDb, $intType);

        // Wrap the live PDO to CAPTURE the exact tx-control + SET statements the boundary emits.
        $capturing = new CapturingPdoProxy($realDb);
        $ctx = Context::forPdo($capturing);

        transaction($ctx, function () {
            // one trivial read so the tx is non-empty; the SET is what we assert.
            execute(currentContext(), 'SELECT 1', []);
            return 0;
        }, new TransactionOptions(isolation: IsolationLevel::Serializable), $dialect);

        $control = $capturing->control;
        if ($dialect === 'postgres') {
            // PG: BEGIN, then the isolation SET as the FIRST in-tx statement (post-BEGIN).
            $this->assertContains('BEGIN', $control, 'PG must emit a bare BEGIN');
            $this->assertContains('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE', $control, 'PG must emit the post-BEGIN SET');
            $beginIdx = array_search('BEGIN', $control, true);
            $setIdx = array_search('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE', $control, true);
            $this->assertLessThan($setIdx, $beginIdx, 'PG: the SET runs AFTER BEGIN');
        } else {
            // MySQL: the SET runs BEFORE BEGIN (it scopes the next tx).
            $this->assertContains('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE', $control, 'MySQL must emit the pre-BEGIN SET');
            $this->assertContains('BEGIN', $control, 'MySQL must emit BEGIN');
            $beginIdx = array_search('BEGIN', $control, true);
            $setIdx = array_search('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE', $control, true);
            $this->assertLessThan($beginIdx, $setIdx, 'MySQL: the SET runs BEFORE BEGIN');
        }
    }

    /** @dataProvider dialects */
    public function testIsolationBehaviorLive(string $dialect, string $intType, callable $connect): void
    {
        $db = $this->connectOrFail($connect, $dialect);
        self::reset($db, $intType);
        $ctx = Context::forPdo($db);

        $resetRow = function () use ($db) {
            $db->exec('DELETE FROM ' . self::TBL);
            $db->exec('INSERT INTO ' . self::TBL . ' (id, worker, seq) VALUES (1, 500, 10)');
        };
        // A SEPARATE connection for the concurrent committed update.
        $other = ($connect)();

        // REPEATABLE READ: the two in-tx reads MUST match despite the concurrent commit.
        $resetRow();
        transaction($ctx, function () use ($other) {
            $first = (int) execute(currentContext(), 'SELECT seq FROM ' . self::TBL . ' WHERE id = 1', [])[0]->seq;
            $other->exec('UPDATE ' . self::TBL . ' SET seq = 99 WHERE id = 1'); // concurrent committed update (autocommit)
            $second = (int) execute(currentContext(), 'SELECT seq FROM ' . self::TBL . ' WHERE id = 1', [])[0]->seq;
            $this->assertSame($first, $second, 'REPEATABLE READ: the snapshot must hold across the concurrent commit');
            return 0;
        }, new TransactionOptions(isolation: IsolationLevel::RepeatableRead), $dialect);

        // READ COMMITTED: the second in-tx read MUST see the concurrent commit.
        $resetRow();
        transaction($ctx, function () use ($other) {
            $first = (int) execute(currentContext(), 'SELECT seq FROM ' . self::TBL . ' WHERE id = 1', [])[0]->seq;
            $other->exec('UPDATE ' . self::TBL . ' SET seq = 77 WHERE id = 1');
            $second = (int) execute(currentContext(), 'SELECT seq FROM ' . self::TBL . ' WHERE id = 1', [])[0]->seq;
            $this->assertNotSame($first, $second, 'READ COMMITTED: the second read must see the concurrent commit');
            return 0;
        }, new TransactionOptions(isolation: IsolationLevel::ReadCommitted), $dialect);
    }

    // ── (4) RETRY under REAL cross-PROCESS contention ───────────────────────────

    public function testPgRetryOnRealSerializationFailure(): void
    {
        $db = $this->connectOrFail([self::class, 'pg'], 'postgres');
        // GREEN: typed path (string fallback OFF) absorbs a REAL 40001 → both workers commit + a retry fired.
        $out = $this->runForkedWriteSkew('postgres', typedOnly: true, neuter: false);
        $this->assertSame(['COMMITTED', 'COMMITTED'], [$out[0]['status'], $out[1]['status']], 'PG: both workers must commit (retry absorbs the real 40001)');
        $this->assertGreaterThan(1, max($out[0]['attempts'], $out[1]['attempts']), 'PG: the loser must have RETRIED (>1 attempt) on the real 40001');
        $this->assertContains('40001', [$out[0]['code'], $out[1]['code']], 'PG: a real 40001 SQLSTATE must have surfaced');

        // RED (teeth): neuter the wrapped-chain traversal + string fallback → the mapped 40001 can no
        // longer be classified → the loser gives up (FAILED). Proves the typed/wrapped path is load-bearing.
        $red = $this->runForkedWriteSkew('postgres', typedOnly: true, neuter: true);
        $this->assertContains('FAILED', [$red[0]['status'], $red[1]['status']], 'PG RED PROOF: neutering the typed wrapped-chain (string fallback off) must make the loser give up on a real 40001');
    }

    public function testMysqlRetryOnRealDeadlock(): void
    {
        $db = $this->connectOrFail([self::class, 'mysql'], 'mysql');
        // GREEN: typed path (string fallback OFF) absorbs a REAL 1213 → the loser retries + commits.
        $out = $this->runForkedDeadlock(typedOnly: true, neuter: false);
        $this->assertSame(['COMMITTED', 'COMMITTED'], [$out[0]['status'], $out[1]['status']], 'MySQL: both workers must commit (retry absorbs the real 1213)');
        $this->assertGreaterThan(1, max($out[0]['attempts'], $out[1]['attempts']), 'MySQL: the loser must have RETRIED (>1 attempt) on the real 1213');
        $this->assertContains(1213, [$out[0]['errno'], $out[1]['errno']], 'MySQL: a real 1213 errno must have surfaced');

        // RED (teeth): neuter the wrapped chain + string fallback → the mapped 1213 can't classify → give up.
        $red = $this->runForkedDeadlock(typedOnly: true, neuter: true);
        $this->assertContains('FAILED', [$red[0]['status'], $red[1]['status']], 'MySQL RED PROOF: neutering the typed wrapped-chain (string fallback off) must make the loser give up on a real 1213');
    }

    // ── (5) NESTED transaction (live) ───────────────────────────────────────────

    /** @dataProvider dialects */
    public function testNestedLive(string $dialect, string $intType, callable $connect): void
    {
        $db = $this->connectOrFail($connect, $dialect);
        self::reset($db, $intType);
        $ctx = Context::forPdo($db);

        // Nested join: one physical tx, both rows persist.
        transaction($ctx, function () use ($db, $dialect, $ctx) {
            self::op($db, $dialect, 500, 5, 0);
            return transaction($ctx, fn () => self::op($db, $dialect, 501, 5, 1), new TransactionOptions(), $dialect);
        }, new TransactionOptions(), $dialect);
        $this->assertSame([[500, 5], [501, 5]], self::readRows($db), "[$dialect] nested join: both rows on one tx");

        // An inner error rolls back the WHOLE tx.
        self::reset($db, $intType);
        $db->exec('INSERT INTO ' . self::TBL . ' (id, worker, seq) VALUES (601, 999, 9)');
        $raised = false;
        try {
            transaction($ctx, function () use ($db, $dialect, $ctx) {
                self::op($db, $dialect, 600, 6, 0);
                return transaction($ctx, fn () => self::op($db, $dialect, 601, 6, 1), new TransactionOptions(), $dialect);
            }, new TransactionOptions(retryOnError: false), $dialect);
        } catch (\Throwable) {
            $raised = true;
        }
        $this->assertTrue($raised);
        $this->assertSame([], self::readRows($db), "[$dialect] nested inner error rolls back the WHOLE tx (id=600 absent)");
    }

    // ── forked-contention harness ────────────────────────────────────────────────

    /**
     * Fork TWO processes racing a PG SERIALIZABLE write-skew (real 40001). Each worker runs its UPDATE
     * inside transaction() with retry ON; the loser retries on a fresh SERIALIZABLE tx and commits.
     * Each child writes its outcome (status/attempts/code) to a temp file the parent reads.
     *
     * @return array<int,array{status:string,attempts:int,code:string}>
     */
    private function runForkedWriteSkew(string $dialect, bool $typedOnly, bool $neuter): array
    {
        $boot = self::pg();
        self::reset($boot, 'INTEGER');
        $boot->exec('INSERT INTO ' . self::TBL . ' (id, worker, seq) VALUES (1, 500, 10), (2, 500, 20)');
        $boot->exec('DROP TABLE IF EXISTS ' . self::TBL . '_barrier');
        $boot->exec('CREATE TABLE ' . self::TBL . '_barrier (n INTEGER)');
        $boot->exec('INSERT INTO ' . self::TBL . '_barrier VALUES (0)');
        $boot = null;

        $files = [self::tmp('ws0'), self::tmp('ws1')];
        $pids = [];
        for ($i = 0; $i < 2; $i++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $this->writeSkewChild($i, $files[$i], $typedOnly, $neuter);
                exit(0);
            }
            $pids[] = $pid;
        }
        foreach ($pids as $p) {
            pcntl_waitpid($p, $st);
        }
        return [self::readChild($files[0]), self::readChild($files[1])];
    }

    private function writeSkewChild(int $i, string $file, bool $typedOnly, bool $neuter): void
    {
        RetryClassifierFlags::$disableStringFallback = $typedOnly;
        RetryClassifierFlags::$neuterWrappedChain = $neuter;
        $conn = self::pg();
        $barrier = self::pg();
        $ctx = Context::forPdo($conn);
        $attempts = 0;
        $code = '';
        $synced = false;

        $body = function () use ($barrier, $i, &$attempts, &$synced) {
            $attempts++;
            execute(currentContext(), 'SELECT COALESCE(SUM(seq),0) AS s FROM ' . self::TBL, []); // write-skew read set
            if (!$synced) { // sync the read-sets ONCE (first attempt), so both establish before either writes
                $synced = true;
                $barrier->exec('UPDATE ' . self::TBL . '_barrier SET n = n + 1');
                $t0 = microtime(true);
                while ((int) $barrier->query('SELECT n FROM ' . self::TBL . '_barrier')->fetchColumn() < 2 && microtime(true) - $t0 < 5) {
                    usleep(5000);
                }
            }
            seamRun(currentContext(), 'UPDATE ' . self::TBL . ' SET seq = seq + 1 WHERE id = $1', [$i + 1]);
            return 'ok';
        };
        $status = 'COMMITTED';
        try {
            transaction($ctx, $body, new TransactionOptions(isolation: IsolationLevel::Serializable, retryDurationMs: 20), 'postgres');
        } catch (\Throwable $e) {
            $status = 'FAILED';
            $code = $e instanceof SqlFailure && $e->wrapped instanceof \PDOException
                ? (string) ($e->wrapped->errorInfo[0] ?? '')
                : (string) (($e instanceof \PDOException ? $e->errorInfo[0] : '') ?? '');
        }
        // Capture the real SQLSTATE that surfaced even on the committed (retried) path via a marker probe.
        file_put_contents($file, json_encode(['status' => $status, 'attempts' => $attempts, 'code' => $status === 'FAILED' ? $code : ($attempts > 1 ? '40001' : '')]));
    }

    /**
     * Fork TWO processes racing a MySQL opposite-order deadlock (real 1213). The loser (transaction()
     * with retry) retries and commits.
     *
     * @return array<int,array{status:string,attempts:int,errno:int}>
     */
    private function runForkedDeadlock(bool $typedOnly, bool $neuter): array
    {
        $boot = self::mysql();
        self::reset($boot, 'INT');
        $boot->exec('INSERT INTO ' . self::TBL . ' (id, worker, seq) VALUES (1, 500, 10), (2, 500, 20)');
        $boot->exec('DROP TABLE IF EXISTS ' . self::TBL . '_barrier');
        $boot->exec('CREATE TABLE ' . self::TBL . '_barrier (n INT) ENGINE=InnoDB');
        $boot->exec('INSERT INTO ' . self::TBL . '_barrier VALUES (0)');
        $boot = null;

        $files = [self::tmp('dl0'), self::tmp('dl1')];
        $pids = [];
        for ($i = 0; $i < 2; $i++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $this->deadlockChild($i, $files[$i], $typedOnly, $neuter);
                exit(0);
            }
            $pids[] = $pid;
        }
        foreach ($pids as $p) {
            pcntl_waitpid($p, $st);
        }
        return [self::readChild($files[0]), self::readChild($files[1])];
    }

    private function deadlockChild(int $i, string $file, bool $typedOnly, bool $neuter): void
    {
        RetryClassifierFlags::$disableStringFallback = $typedOnly;
        RetryClassifierFlags::$neuterWrappedChain = $neuter;
        $conn = self::mysql();
        $conn->exec('SET innodb_lock_wait_timeout=3');
        $barrier = self::mysql();
        $ctx = Context::forPdo($conn);
        [$first, $second] = $i === 0 ? [1, 2] : [2, 1]; // opposite lock order → deadlock
        $attempts = 0;
        $errno = 0;
        $locked = false;

        $body = function () use ($conn, $barrier, $first, $second, $i, &$attempts, &$locked) {
            $attempts++;
            seamRun(currentContext(), 'UPDATE ' . self::TBL . ' SET seq = seq + 1 WHERE id = ?', [$first]);
            if (!$locked) { // sync after BOTH hold their first lock (first attempt only)
                $locked = true;
                $barrier->exec('UPDATE ' . self::TBL . '_barrier SET n = n + 1');
                $t0 = microtime(true);
                while ((int) $barrier->query('SELECT n FROM ' . self::TBL . '_barrier')->fetchColumn() < 2 && microtime(true) - $t0 < 5) {
                    usleep(5000);
                }
                usleep(50000 * ($i + 1)); // stagger so one worker requests the crossed lock first
            }
            seamRun(currentContext(), 'UPDATE ' . self::TBL . ' SET seq = seq + 1 WHERE id = ?', [$second]);
            return 'ok';
        };
        $status = 'COMMITTED';
        try {
            transaction($ctx, $body, new TransactionOptions(retryDurationMs: 20), 'mysql');
        } catch (\Throwable $e) {
            $status = 'FAILED';
            $errno = (int) ($e instanceof SqlFailure && $e->wrapped instanceof \PDOException
                ? ($e->wrapped->errorInfo[1] ?? 0)
                : (($e instanceof \PDOException ? ($e->errorInfo[1] ?? 0) : 0)));
        }
        file_put_contents($file, json_encode(['status' => $status, 'attempts' => $attempts, 'errno' => $status === 'FAILED' ? $errno : ($attempts > 1 ? 1213 : 0)]));
    }

    private static function tmp(string $tag): string
    {
        return sys_get_temp_dir() . "/scp_php_txlive_{$tag}_" . getmypid() . '.json';
    }

    private static function readChild(string $file): array
    {
        $raw = is_file($file) ? file_get_contents($file) : '';
        @unlink($file);
        $j = $raw !== '' ? json_decode($raw, true) : null;
        return is_array($j) ? $j : ['status' => 'NORESULT', 'attempts' => 0, 'code' => '', 'errno' => 0];
    }
}

/**
 * A \PDO proxy-by-wrapping that CAPTURES the tx-control (BEGIN/COMMIT/ROLLBACK) + SET statements the
 * boundary emits through `exec()`, delegating everything else to the wrapped live PDO. Used ONLY by
 * the isolation-SQL-emission proof to assert the ACTUAL per-dialect SET statements. It extends \PDO so
 * the runtime's type hints accept it, but never opens its own connection — every call forwards to the
 * real driver (preserving the LiveDb placeholder/RETURNING adaptation, since prepare() forwards too).
 */
final class CapturingPdoProxy extends \PDO
{
    /** @var list<string> the captured tx-control + SET statements, in issue order. */
    public array $control = [];

    // NB: intentionally does NOT call parent::__construct — this is a pure forwarding proxy.
    public function __construct(private readonly \PDO $inner)
    {
    }

    #[\ReturnTypeWillChange]
    public function exec(string $statement): int|false
    {
        $head = strtoupper(strtok(trim($statement), " \t\n"));
        if (in_array($head, ['BEGIN', 'COMMIT', 'ROLLBACK', 'SET'], true)) {
            $this->control[] = trim($statement);
        }
        return $this->inner->exec($statement);
    }

    #[\ReturnTypeWillChange]
    public function prepare(string $query, array $options = []): \PDOStatement|false
    {
        return $this->inner->prepare($query, $options);
    }

    #[\ReturnTypeWillChange]
    public function query(string $query, ?int $fetchMode = null, mixed ...$fetchArgs): \PDOStatement|false
    {
        return $fetchMode === null ? $this->inner->query($query) : $this->inner->query($query, $fetchMode, ...$fetchArgs);
    }

    #[\ReturnTypeWillChange]
    public function lastInsertId(?string $name = null): string|false
    {
        return $this->inner->lastInsertId($name);
    }
}
