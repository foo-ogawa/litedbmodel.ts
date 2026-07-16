<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\Connection;
use LiteDbModel\Runtime\Context;
use LiteDbModel\Runtime\ExecutionContext;
use LiteDbModel\Runtime\MiddlewareChain;
use LiteDbModel\Runtime\RunInfo;
use LiteDbModel\Runtime\StatementIntent;
use PHPUnit\Framework\TestCase;

use function LiteDbModel\Runtime\commit;
use function LiteDbModel\Runtime\execute;
use function LiteDbModel\Runtime\rollbackWith;
use function LiteDbModel\Runtime\run;
use function LiteDbModel\Runtime\withTransaction;
use function LiteDbModel\Runtime\withTransactionDecided;

/**
 * Phase A / #79 — ExecutionContext + central execute/run seam UNIT tests.
 *
 * These pin the seam contract independently of the makeSQL runtime: the funnel (every SQL through
 * ① middleware ② connectionFor ③ execute), the empty-chain passthrough (byte-identity), the
 * middleware fold order, per-execution connection ownership (the tx body resolves the pinned
 * connection), the commit/rollback ordering, and the leak-guard (release EXACTLY once on every path,
 * including a throwing commit/rollback — the #78 defect).
 */
final class ExecutionContextSeamTest extends TestCase
{
    private static function seed(): \PDO
    {
        $db = new \PDO('sqlite::memory:');
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
        return $db;
    }

    // ── The seam funnels through the connection (§2 ②③) ─────────────────────────

    public function testExecuteFunnelsToConnection(): void
    {
        $db = self::seed();
        $ctx = Context::forPdo($db);
        run($ctx, 'INSERT INTO t (id, v) VALUES (?, ?)', [1, 'a']);
        $rows = execute($ctx, 'SELECT v FROM t WHERE id = ?', [1]);
        $this->assertCount(1, $rows);
        $this->assertSame('a', $rows[0]->v);
    }

    public function testRunReportsChanges(): void
    {
        $db = self::seed();
        $ctx = Context::forPdo($db);
        run($ctx, 'INSERT INTO t (id, v) VALUES (?, ?)', [1, 'a']);
        run($ctx, 'INSERT INTO t (id, v) VALUES (?, ?)', [2, 'b']);
        $info = run($ctx, 'UPDATE t SET v = ?', ['x']);
        $this->assertInstanceOf(RunInfo::class, $info);
        $this->assertSame(2, $info->changes);
    }

    // ── Empty chain = pure passthrough (byte-identity) ─────────────────────────

    public function testEmptyChainIsPassthrough(): void
    {
        $chain = new MiddlewareChain();
        $this->assertTrue($chain->isEmpty());
        $seen = null;
        $out = $chain->wrap('SELECT 1', [7], function (string $s, array $p) use (&$seen) {
            $seen = [$s, $p];
            return 'RESULT';
        });
        $this->assertSame('RESULT', $out);
        $this->assertSame(['SELECT 1', [7]], $seen); // verbatim, no wrapping
    }

    // ── Middleware fold: outermost-first ordering around next ───────────────────

    public function testMiddlewareFoldOrder(): void
    {
        $order = [];
        $mwA = function (string $s, array $p, callable $next) use (&$order) {
            $order[] = 'A-before';
            $r = $next($s, $p);
            $order[] = 'A-after';
            return $r;
        };
        $mwB = function (string $s, array $p, callable $next) use (&$order) {
            $order[] = 'B-before';
            $r = $next($s, $p);
            $order[] = 'B-after';
            return $r;
        };
        $chain = new MiddlewareChain([$mwA, $mwB]);
        $this->assertFalse($chain->isEmpty());
        $out = $chain->wrap('SQL', [], function () use (&$order) {
            $order[] = 'terminal';
            return 42;
        });
        $this->assertSame(42, $out);
        // A wraps B wraps terminal — A outermost (registered first), unwinds last.
        $this->assertSame(['A-before', 'B-before', 'terminal', 'B-after', 'A-after'], $order);
    }

    public function testMiddlewareCanRewriteSqlAndParams(): void
    {
        $db = self::seed();
        // A middleware that rewrites the bound id on the INSERT (proving the chain reaches the real
        // execute path). It only touches the 2-param INSERT; the param-less SELECT passes through.
        $mw = fn (string $s, array $p, callable $next) => $next($s, count($p) === 2 ? [$p[0] + 100, $p[1]] : $p);
        $ctx = new ExecutionContext(
            new \LiteDbModel\Runtime\PdoDriver($db),
            new MiddlewareChain([$mw]),
        );
        run($ctx, 'INSERT INTO t (id, v) VALUES (?, ?)', [1, 'z']);
        $rows = execute($ctx, 'SELECT id FROM t', []);
        $this->assertSame(101, (int) $rows[0]->id); // 1 + 100 via the middleware
    }

    // ── connectionFor resolution: tx-owned pinned connection wins ──────────────

    public function testConnectionForResolvesPinnedInTx(): void
    {
        $db = self::seed();
        $base = Context::forPdo($db);
        $this->assertFalse($base->inTransaction());
        $marker = new class implements Connection {
            public bool $used = false;
            public function execute(string $sql, array $params): array
            {
                $this->used = true;
                return [];
            }
            public function run(string $sql, array $params): RunInfo
            {
                $this->used = true;
                return new RunInfo(0, 0);
            }
            public function control(string $sql): void
            {
                $this->used = true;
            }
        };
        $txCtx = $base->withConnection($marker, true);
        $this->assertTrue($txCtx->inTransaction());
        // In the tx-scoped ctx, connectionFor MUST return the pinned marker, not the base pdo.
        $this->assertSame($marker, $txCtx->connectionFor(StatementIntent::write()));
        execute($txCtx, 'SELECT 1', []);
        $this->assertTrue($marker->used);
        // The base ctx still resolves the real pdo connection.
        $this->assertNotSame($marker, $base->connectionFor(StatementIntent::read()));
    }

    // ── Transaction commit / rollback (per-execution ownership) ────────────────

    public function testWithTransactionCommits(): void
    {
        $db = self::seed();
        $ctx = Context::forPdo($db);
        $r = withTransaction($ctx, function (ExecutionContext $txCtx): string {
            run($txCtx, 'INSERT INTO t (id, v) VALUES (?, ?)', [1, 'committed']);
            return 'done';
        });
        $this->assertSame('done', $r);
        $rows = execute($ctx, 'SELECT v FROM t', []);
        $this->assertCount(1, $rows);
        $this->assertSame('committed', $rows[0]->v);
    }

    public function testWithTransactionRollsBackOnThrow(): void
    {
        $db = self::seed();
        $ctx = Context::forPdo($db);
        try {
            withTransaction($ctx, function (ExecutionContext $txCtx): void {
                run($txCtx, 'INSERT INTO t (id, v) VALUES (?, ?)', [1, 'doomed']);
                throw new \RuntimeException('boom');
            });
            $this->fail('expected the body exception to propagate');
        } catch (\RuntimeException $e) {
            $this->assertSame('boom', $e->getMessage());
        }
        // The insert must have been rolled back — no rows.
        $rows = execute($ctx, 'SELECT COUNT(*) AS n FROM t', []);
        $this->assertSame(0, (int) $rows[0]->n);
    }

    public function testWithTransactionDecidedRollbackReturnsValue(): void
    {
        $db = self::seed();
        $ctx = Context::forPdo($db);
        // A legitimate non-error rollback (gate short-circuit): rolls back but returns a value.
        $out = withTransactionDecided($ctx, function (ExecutionContext $txCtx): \LiteDbModel\Runtime\TxDecision {
            run($txCtx, 'INSERT INTO t (id, v) VALUES (?, ?)', [1, 'gated']);
            return rollbackWith(['committed' => false]);
        });
        $this->assertSame(['committed' => false], $out);
        $rows = execute($ctx, 'SELECT COUNT(*) AS n FROM t', []);
        $this->assertSame(0, (int) $rows[0]->n); // rolled back
    }

    public function testWithTransactionDecidedCommitReturnsValue(): void
    {
        $db = self::seed();
        $ctx = Context::forPdo($db);
        $out = withTransactionDecided($ctx, function (ExecutionContext $txCtx): \LiteDbModel\Runtime\TxDecision {
            run($txCtx, 'INSERT INTO t (id, v) VALUES (?, ?)', [1, 'kept']);
            return commit(['committed' => true]);
        });
        $this->assertSame(['committed' => true], $out);
        $rows = execute($ctx, 'SELECT v FROM t', []);
        $this->assertSame('kept', $rows[0]->v);
    }

    // ── Leak guard (#78): release EXACTLY once on every path, incl. throwing commit ──
    //
    // The combinator's `finally` is the SOLE releaser. We drive the REAL withTransactionDecided over a
    // \PDO whose COMMIT / ROLLBACK can be made to throw, and assert (a) the exception propagates, (b)
    // the connection is NOT left stuck in an open transaction (release cleared it), and — via a direct
    // PdoTxConnection unit — (c) release runs EXACTLY once and is idempotent.

    public function testThrowingCommitPropagatesAndLeavesConnectionUsable(): void
    {
        $db = new ThrowingPdo('sqlite::memory:', throwOn: 'COMMIT');
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)');
        $ctx = Context::forPdo($db);
        try {
            withTransactionDecided($ctx, function (ExecutionContext $txCtx): \LiteDbModel\Runtime\TxDecision {
                run($txCtx, 'INSERT INTO t (id, v) VALUES (?, ?)', [1, 'x']);
                return commit(null);
            });
            $this->fail('expected the throwing COMMIT to propagate');
        } catch (\Throwable $e) {
            $this->assertStringContainsString('boom-COMMIT', $e->getMessage());
        }
        // The #78 leak: without the finally's release (which best-effort ROLLBACKs the poisoned
        // connection), the \PDO would still be inside an open transaction and a fresh BEGIN would
        // throw. Prove the connection is clean by starting + committing a new tx on it.
        $db->armed = false; // let COMMIT succeed now
        $ctx2 = Context::forPdo($db);
        withTransaction($ctx2, function (ExecutionContext $txCtx): void {
            run($txCtx, 'INSERT INTO t (id, v) VALUES (?, ?)', [2, 'y']);
        });
        $rows = execute($ctx2, 'SELECT id FROM t ORDER BY id', []);
        // Row 1 (from the throwing-commit tx) was rolled back by release; row 2 committed cleanly.
        $this->assertSame([2], array_map(fn ($r) => (int) $r->id, $rows));
    }

    public function testReleaseIsIdempotentAndCountsExactlyOnce(): void
    {
        $db = self::seed();
        $tx = new \LiteDbModel\Runtime\PdoTxConnection($db);
        $this->assertSame(0, $tx->releaseCount());
        $this->assertNull($tx->releasedDestroy());
        $tx->release(false);
        $this->assertSame(1, $tx->releaseCount());
        $this->assertFalse($tx->releasedDestroy());
        // A second release is a no-op — the count stays 1 (no double-release).
        $tx->release(true);
        $this->assertSame(1, $tx->releaseCount());
        $this->assertFalse($tx->releasedDestroy()); // unchanged by the no-op second call
    }
}

/** A \PDO whose `exec()` throws when the (uppercased) statement matches `$throwOn` and it is armed. */
final class ThrowingPdo extends \PDO
{
    public bool $armed = true;

    public function __construct(string $dsn, private readonly string $throwOn)
    {
        parent::__construct($dsn);
    }

    #[\ReturnTypeWillChange]
    public function exec(string $statement): int|false
    {
        if ($this->armed && strtoupper(trim($statement)) === $this->throwOn) {
            throw new \RuntimeException('boom-' . $this->throwOn);
        }
        return parent::exec($statement);
    }
}
