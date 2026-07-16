<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\Context;
use LiteDbModel\Runtime\ExecutionContext;
use LiteDbModel\Runtime\IsolationLevel;
use LiteDbModel\Runtime\Runtime;
use LiteDbModel\Runtime\SqlFailure;
use LiteDbModel\Runtime\TransactionOptions;
use LiteDbModel\Runtime\WriteInReadOnlyContextError;
use LiteDbModel\Runtime\WriteOutsideTransactionError;
use PHPUnit\Framework\TestCase;

use function LiteDbModel\Runtime\currentContext;
use function LiteDbModel\Runtime\isolationPrelude;
use function LiteDbModel\Runtime\isRetryableTxError;
use function LiteDbModel\Runtime\retryableByTypedCode;
use function LiteDbModel\Runtime\runWithPinnedContext;
use function LiteDbModel\Runtime\transaction;

/**
 * Phase B (#85, PHP) — UNIT tests for the public {@see transaction()} boundary (in-proc SQLite) + the
 * tx-completeness primitives (isolation SQL, retryable classifier), the PHP mirror of python's
 * test_transaction_boundary.py + go's TestBoundary* / rust's tx_boundary unit tests.
 *
 * No live DB — an in-proc SQLite {@see \PDO} (wrapped in {@see RecordingPdo}, which counts
 * BEGIN/COMMIT/ROLLBACK/SET issued through `exec()`) proves the boundary mechanics that DON'T need
 * PG/MySQL:
 *
 *   (1) MULTI-OP ATOMICITY — transaction(fn: [opA_insert; opB_insert]) → both commit; the recording
 *       PDO asserts EXACTLY ONE BEGIN / ONE COMMIT for the whole boundary (the ambient JOIN — opB
 *       opens no second BEGIN). opB PK-collides → opA's row ALSO rolls back (ONE BEGIN + ONE ROLLBACK,
 *       zero COMMIT), verified by reading real rows.
 *   (2) MUTATION RED (teeth) — disabling the ambient-JOIN (opB opens its own auto-tx via the INTERNAL
 *       guard-off executor) makes the A-rolls-back-when-B-fails assertion go RED, proving the join is
 *       load-bearing.
 *   (3) GUARD — a write OUTSIDE transaction() → WriteOutsideTransactionError; a read-only write inside
 *       → WriteInReadOnlyContextError; inside a boundary → ok.
 *   (4) NESTED — one BEGIN/COMMIT; an inner error rolls back the whole tx.
 *   (5) rollback_only — the body runs + returns its value, but NOTHING commits (dry-run).
 *   (6) SQLite isolation is a HARD ERROR at the boundary (before any connection is acquired).
 *   (7) The typed retryable classifier (errorInfo SQLSTATE/errno through the wrapped chain) +
 *       isolation-prelude SQL emission per dialect.
 *
 * The live-PG/MySQL isolation + real-contention-retry proof lives in TxBoundaryLiveTest.php.
 */
final class TxBoundaryTest extends TestCase
{
    private const TBL = 'scp_tx_boundary_php';

    private static function makeDriver(RecordingSink $sink): RecordingPdo
    {
        $db = new RecordingPdo('sqlite::memory:', $sink);
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->exec('CREATE TABLE ' . self::TBL . ' (id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, seq INTEGER NOT NULL)');
        $sink->reset(); // ignore the DDL
        return $db;
    }

    /** A single-INSERT (no gate) tx bundle whose values come from the input scope. */
    private static function insertBundle(): \stdClass
    {
        return json_decode(json_encode([
            'dialect' => 'sqlite',
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

    /** @return list<array{0:int,1:int}> sorted (id, worker) rows (worker != 999 filters pre-seeds). */
    private static function readRows(\PDO $db): array
    {
        $rows = $db->query('SELECT id, worker FROM ' . self::TBL . ' WHERE worker <> 999')->fetchAll(\PDO::FETCH_OBJ);
        $out = array_map(fn ($r) => [(int) $r->id, (int) $r->worker], $rows);
        sort($out);
        return $out;
    }

    /** The op the boundary body issues — a PUBLIC guarded write that JOINs the ambient tx. */
    private static function op(\PDO $db, int $id, int $worker, int $seq): array
    {
        return Runtime::executeTransactionBundle(self::insertBundle(), ['id' => $id, 'worker' => $worker, 'seq' => $seq], $db);
    }

    // ── (1) MULTI-OP ATOMICITY — commit path ────────────────────────────────────

    public function testMultiOpBoundaryOneBeginOneCommit(): void
    {
        $sink = new RecordingSink();
        $db = self::makeDriver($sink);
        $ctx = Context::forPdo($db);

        $result = transaction($ctx, fn () => [self::op($db, 100, 1, 0), self::op($db, 101, 1, 1)], new TransactionOptions(), 'sqlite');

        $this->assertSame([true, true], array_map(fn ($r) => $r['committed'], $result));
        // N ops in one boundary ⇒ ONE BEGIN + ONE COMMIT (the ambient JOIN — opB opens no 2nd BEGIN).
        $this->assertSame(1, $sink->begins, 'expected 1 BEGIN');
        $this->assertSame(1, $sink->commits, 'expected 1 COMMIT');
        $this->assertSame(0, $sink->rolls);
        $this->assertSame([[100, 1], [101, 1]], self::readRows($db));
    }

    // ── (1) MULTI-OP ATOMICITY — rollback path (B fails ⇒ A also rolls back) ─────

    public function testMultiOpBoundaryOpBFailRollsBackOpA(): void
    {
        $sink = new RecordingSink();
        $db = self::makeDriver($sink);
        $db->exec('INSERT INTO ' . self::TBL . ' (id, worker, seq) VALUES (201, 999, 9)'); // pre-seed collision
        $sink->reset();
        $ctx = Context::forPdo($db);

        $raised = false;
        try {
            transaction($ctx, fn () => [self::op($db, 200, 2, 0), self::op($db, 201, 2, 1)], new TransactionOptions(retryOnError: false), 'sqlite');
        } catch (\Throwable) {
            $raised = true;
        }
        $this->assertTrue($raised, 'opB PK collision must fail the whole boundary');
        $this->assertSame(1, $sink->begins);
        $this->assertSame(0, $sink->commits);
        $this->assertSame(1, $sink->rolls, 'opB failure ⇒ ONE ROLLBACK');
        // opA (id=200) must ALSO have rolled back — the whole boundary is atomic.
        $this->assertSame([], self::readRows($db), 'opA must roll back when opB fails (cross-op atomicity)');
    }

    // ── (2) MUTATION RED — disabling the ambient JOIN breaks the atomic outcome ──

    public function testDisablingAmbientJoinGoesRed(): void
    {
        // Baseline GREEN (join intact) as reference is proven by the test above. Here we FAITHFULLY
        // DISABLE the join: each op runs through the INTERNAL guard-off executor, which — because it is
        // NOT inside a transaction() (we call it directly, no ambient pin) — opens its OWN auto-tx. So
        // opA commits on its own connection tx and SURVIVES opB's failure ⇒ the atomic green outcome no
        // longer holds. Proves the ambient JOIN is load-bearing (mirror py test_disabling_ambient_join).
        $sink = new RecordingSink();
        $db = self::makeDriver($sink);
        $db->exec('INSERT INTO ' . self::TBL . ' (id, worker, seq) VALUES (201, 999, 9)');
        $sink->reset();

        $doOpNoJoin = fn (int $id, int $w, int $s) =>
            Runtime::executeTransactionBundleInternal(self::insertBundle(), ['id' => $id, 'worker' => $w, 'seq' => $s], $db);

        try {
            // No transaction() wrapper → each internal call is its OWN auto-tx (BEGIN…COMMIT per op).
            $doOpNoJoin(200, 2, 0); // commits alone
            $doOpNoJoin(201, 2, 1); // PK collision → fails, but opA already committed
        } catch (\Throwable) {
            // ignore — we only care that opA leaked past.
        }

        // Under the disabled join, opA (id=200) COMMITTED independently ⇒ the atomic "rows == []"
        // outcome is BROKEN. Assert the mutation actually breaks atomicity (teeth).
        $this->assertGreaterThanOrEqual(
            1,
            count(self::readRows($db)),
            'MUTATION PROOF: disabling the ambient JOIN (each op = its own auto-tx) MUST leak opA past '
            . 'the failure ⇒ the atomicity test has teeth. It did not.'
        );
        // Two independent auto-txs ⇒ more than one BEGIN (each op opened its own), unlike the joined path.
        $this->assertGreaterThan(1, $sink->begins, 'disabled join ⇒ each op opens its own BEGIN (>1)');
    }

    // ── (3) write=tx GUARD ──────────────────────────────────────────────────────

    public function testGuardOutsideBoundaryRejectsWrite(): void
    {
        $sink = new RecordingSink();
        $db = self::makeDriver($sink);
        // A bare write OUTSIDE any transaction() → WriteOutsideTransactionError, nothing written.
        $this->expectException(WriteOutsideTransactionError::class);
        try {
            self::op($db, 300, 3, 0);
        } finally {
            $this->assertSame([], self::readRows($db));
        }
    }

    public function testGuardReadOnlyInsideBoundaryRejectsWrite(): void
    {
        $sink = new RecordingSink();
        $db = self::makeDriver($sink);
        $ctx = Context::forPdo($db);

        // Read-only-scoped write inside a boundary → WriteInReadOnlyContextError (read-only checked FIRST).
        $body = function () use ($db): int {
            $ro = currentContext()->withReadOnly(); // the pinned tx ctx, derived read-only
            $threw = false;
            try {
                runWithPinnedContext($ro, fn () => self::op($db, 301, 3, 0));
            } catch (WriteInReadOnlyContextError) {
                $threw = true;
            }
            $this->assertTrue($threw, 'a read-only-scoped write inside a boundary must be rejected');
            return 0;
        };
        transaction($ctx, $body, new TransactionOptions(retryOnError: false), 'sqlite');
    }

    public function testGuardInsideBoundaryAllowsWrite(): void
    {
        $sink = new RecordingSink();
        $db = self::makeDriver($sink);
        $ctx = Context::forPdo($db);
        $r = transaction($ctx, fn () => self::op($db, 302, 3, 0), new TransactionOptions(), 'sqlite');
        $this->assertTrue($r['committed']);
        $this->assertSame([[302, 3]], self::readRows($db));
    }

    // ── (4) NESTED transaction = one begin/commit ───────────────────────────────

    public function testNestedTransactionOneBeginCommit(): void
    {
        $sink = new RecordingSink();
        $db = self::makeDriver($sink);
        $ctx = Context::forPdo($db);

        $outer = function () use ($db, $ctx) {
            self::op($db, 500, 5, 0);
            // A NESTED transaction() JOINs the outer — no new BEGIN/COMMIT.
            return transaction($ctx, fn () => self::op($db, 501, 5, 1), new TransactionOptions(), 'sqlite');
        };
        transaction($ctx, $outer, new TransactionOptions(), 'sqlite');

        $this->assertSame(1, $sink->begins);
        $this->assertSame(1, $sink->commits);
        $this->assertSame(0, $sink->rolls);
        $this->assertSame([[500, 5], [501, 5]], self::readRows($db));
    }

    public function testNestedInnerErrorRollsBackWholeTx(): void
    {
        $sink = new RecordingSink();
        $db = self::makeDriver($sink);
        $db->exec('INSERT INTO ' . self::TBL . ' (id, worker, seq) VALUES (601, 999, 9)'); // collision for inner
        $sink->reset();
        $ctx = Context::forPdo($db);

        $outer = function () use ($db, $ctx) {
            self::op($db, 600, 6, 0);
            return transaction($ctx, fn () => self::op($db, 601, 6, 1), new TransactionOptions(), 'sqlite');
        };
        $raised = false;
        try {
            transaction($ctx, $outer, new TransactionOptions(retryOnError: false), 'sqlite');
        } catch (\Throwable) {
            $raised = true;
        }
        $this->assertTrue($raised);
        $this->assertSame(0, $sink->commits);
        $this->assertSame(1, $sink->rolls);
        $this->assertSame([], self::readRows($db), 'an inner error rolls back the WHOLE tx (id=600 absent)');
    }

    // ── (5) rollback_only (dry-run) ─────────────────────────────────────────────

    public function testRollbackOnlyReturnsValueButCommitsNothing(): void
    {
        $sink = new RecordingSink();
        $db = self::makeDriver($sink);
        $ctx = Context::forPdo($db);
        $r = transaction($ctx, fn () => self::op($db, 700, 7, 0), new TransactionOptions(rollbackOnly: true), 'sqlite');
        $this->assertTrue($r['committed']); // the body's own view: its statement ran + returned
        // …but the boundary ROLLED BACK, so nothing persisted.
        $this->assertSame(1, $sink->begins);
        $this->assertSame(0, $sink->commits);
        $this->assertSame(1, $sink->rolls);
        $this->assertSame([], self::readRows($db), 'rollback_only must commit nothing');
    }

    // ── (6) SQLite isolation is a hard error at the boundary ────────────────────

    public function testSqliteIsolationRequestIsHardError(): void
    {
        $sink = new RecordingSink();
        $db = self::makeDriver($sink);
        $ctx = Context::forPdo($db);
        $threw = false;
        try {
            transaction($ctx, fn () => null, new TransactionOptions(isolation: IsolationLevel::Serializable), 'sqlite');
        } catch (\RuntimeException $e) {
            $threw = true;
            $this->assertStringContainsString('SQLite does not support a per-transaction isolation level', $e->getMessage());
        }
        $this->assertTrue($threw, 'a SQLite isolation request must be a hard error');
        // The hard-error fires BEFORE any connection is acquired.
        $this->assertSame(0, $sink->begins);
    }

    // ── (7a) isolation-prelude SQL emission per dialect ─────────────────────────

    public function testIsolationPreludePostgresPostBegin(): void
    {
        $this->assertSame([[], []], isolationPrelude('postgres', null));
        $this->assertSame([[], ['SET TRANSACTION ISOLATION LEVEL SERIALIZABLE']], isolationPrelude('postgres', IsolationLevel::Serializable));
        $this->assertSame([[], ['SET TRANSACTION ISOLATION LEVEL READ COMMITTED']], isolationPrelude('postgres', IsolationLevel::ReadCommitted));
    }

    public function testIsolationPreludeMysqlPreBegin(): void
    {
        $this->assertSame([['SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'], []], isolationPrelude('mysql', IsolationLevel::RepeatableRead));
        $this->assertSame([[], []], isolationPrelude('mysql', null));
    }

    // ── (7b) typed retryable classifier — errorInfo SQLSTATE/errno, NOT string ──

    public function testTypedCodeClassifiesPgSerializationFailure(): void
    {
        // A synthetic \PDOException carrying the PG 40001 SQLSTATE in errorInfo[0] (the SAME shape
        // PDO_pgsql sets on a real serialization failure). No 40001 in the MESSAGE — so ONLY the typed
        // errorInfo path can classify it. This is the load-bearing path the live retry relies on.
        $e = self::pdoException(['40001', null, 'oops'], 'a driver failure with no code in text');
        $this->assertTrue(retryableByTypedCode($e), 'PG 40001 SQLSTATE (errorInfo[0]) must classify typed');
        $this->assertTrue(isRetryableTxError($e));
    }

    public function testTypedCodeClassifiesMysqlDeadlockErrno(): void
    {
        // A synthetic \PDOException carrying the MySQL 1213 errno in errorInfo[1]. No 1213 in the text.
        $e = self::pdoException(['40001', 1213, 'a lock problem'], 'a driver failure with no code in text');
        $this->assertTrue(retryableByTypedCode($e), 'MySQL 1213 errno (errorInfo[1]) must classify typed');
        $this->assertTrue(isRetryableTxError($e));
    }

    public function testTypedCodeReachesWrappedPdoThroughSqlFailure(): void
    {
        // The COMMIT-time shape: a raw \PDOException (PG 40001) mapped into a SqlFailure. The typed
        // classifier must TRAVERSE the wrapped chain (SqlFailure->wrapped / getPrevious) to reach the
        // concrete errorInfo — even though the SqlFailure's OWN message is a flattened text (go
        // SqlFailure.Unwrap() parity). This is the exact defect go #83's audit caught.
        $raw = self::pdoException(['40001', null, 'serialization failure'], 'boom');
        $wrapped = SqlFailure::fromPdo($raw);
        $this->assertTrue(retryableByTypedCode($wrapped), 'typed classifier must reach errorInfo through the wrapped chain');
        $this->assertTrue(isRetryableTxError($wrapped));
    }

    public function testNonRetryableDataConflictIsNotRetryable(): void
    {
        // A unique-violation SQLSTATE (23505 on PG) is a DATA conflict — NOT retryable (re-running fails
        // identically). Neither the typed path nor the string fallback should classify it retryable.
        $e = self::pdoException(['23505', 1062, 'duplicate key value violates unique constraint'], 'unique_violation');
        $this->assertFalse(retryableByTypedCode($e));
        $this->assertFalse(isRetryableTxError($e), 'a unique/data conflict must NOT be retryable');
    }

    /** Build a \PDOException with a specific errorInfo triple (the live-driver shape). */
    private static function pdoException(array $errorInfo, string $message): \PDOException
    {
        $e = new \PDOException($message);
        $e->errorInfo = $errorInfo;
        return $e;
    }
}

/** A shared sink counting BEGIN / COMMIT / ROLLBACK issued through {@see RecordingPdo::exec()}. */
final class RecordingSink
{
    public int $begins = 0;
    public int $commits = 0;
    public int $rolls = 0;
    /** @var list<string> the raw tx-control / SET statements, in issue order (for isolation SQL capture). */
    public array $control = [];

    public function reset(): void
    {
        $this->begins = $this->commits = $this->rolls = 0;
        $this->control = [];
    }
}

/**
 * A \PDO that records the tx-control (BEGIN/COMMIT/ROLLBACK) + SET statements the owned-connection tx
 * path emits through `exec()` — so the boundary tests assert EXACTLY the BEGIN/COMMIT/ROLLBACK counts
 * (the ambient JOIN: N ops = ONE BEGIN). Everything else passes through verbatim.
 */
final class RecordingPdo extends \PDO
{
    public function __construct(string $dsn, private readonly RecordingSink $sink)
    {
        parent::__construct($dsn);
    }

    #[\ReturnTypeWillChange]
    public function exec(string $statement): int|false
    {
        $head = strtoupper(strtok(trim($statement), " \t\n"));
        if ($head === 'BEGIN') {
            $this->sink->begins++;
        } elseif ($head === 'COMMIT') {
            $this->sink->commits++;
        } elseif ($head === 'ROLLBACK') {
            $this->sink->rolls++;
        }
        if ($head === 'BEGIN' || $head === 'COMMIT' || $head === 'ROLLBACK' || $head === 'SET') {
            $this->sink->control[] = trim($statement);
        }
        return parent::exec($statement);
    }
}
