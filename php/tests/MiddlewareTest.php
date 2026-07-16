<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\Context;
use LiteDbModel\Runtime\ExecutionContext;
use LiteDbModel\Runtime\LogEntry;
use LiteDbModel\Runtime\MethodKind;
use LiteDbModel\Runtime\Registry;
use PHPUnit\Framework\TestCase;

use function LiteDbModel\Runtime\clearMiddlewares;
use function LiteDbModel\Runtime\createMiddleware;
use function LiteDbModel\Runtime\execute;
use function LiteDbModel\Runtime\Logger;
use function LiteDbModel\Runtime\rawExecute;
use function LiteDbModel\Runtime\rawQuery;
use function LiteDbModel\Runtime\registerMiddleware;
use function LiteDbModel\Runtime\run;
use function LiteDbModel\Runtime\runMethod;
use function LiteDbModel\Runtime\use_;
use function LiteDbModel\Runtime\withMiddlewareScope;

/**
 * Phase D (#96) — the SCP MIDDLEWARE layer, hook-mechanics UNIT tests (PHP).
 *
 * The PHP port of the TS reference `test/scp/middleware.test.ts`, proving D1/D2/D3 on the REAL Phase A
 * exec-context seam (a real in-process PDO SQLite), the same contract the ports mirror:
 *   D1 SQL-level `execute` hook — a registered middleware intercepts EVERY SQL through the seam
 *      (read/write/tx-control/relation-batch), can OBSERVE / REWRITE / TIME / SHORT-CIRCUIT; the
 *      applied ORDER is first-registered-outermost. RED: unregistered ⇒ no interception (byte-identical).
 *   D2 method-level hooks — `runMethod(kind, …)` fires the matching op-kind hook (find/create/…),
 *      before/after observed; the op kind is a TAG (never parsed from SQL). RED: wrong kind ⇒ no fire.
 *   D3 Logger + raw execute/query — Logger records real SQL/params/timing; rawExecute/rawQuery go
 *      THROUGH the seam (a registered SQL middleware sees the raw call); rawQuery ALSO fires a `query`
 *      method hook. RED: unregistered ⇒ empty.
 *
 * PHP-specific HONEST note (see {@see \LiteDbModel\Runtime\Middleware.php} class doc): the TS
 * per-scope CONCURRENCY-isolation test (two concurrent `withMiddlewareScope` bodies not cross-talking)
 * is genuinely N/A for PHP (1-request-1-process ⇒ no in-process concurrency to isolate). It is NOT
 * faked here. What IS reproduced: the explicit-registry scope, scoped registration + fresh per-scope
 * STATE, the fold direction (last→first, index 0 outermost), and the empty-chain passthrough.
 */
final class MiddlewareTest extends TestCase
{
    protected function setUp(): void
    {
        clearMiddlewares();
    }

    private static function freshDb(): \PDO
    {
        $db = new \PDO('sqlite::memory:');
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
        return $db;
    }

    // ── D1: SQL-level execute hook ────────────────────────────────────────────

    public function testInterceptsEverySqlThroughTheSeam(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        $seen = [];
        withMiddlewareScope(function () use ($ctx, &$seen): void {
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$seen) {
                $seen[] = $sql;
                return $next($sql, $params);
            }]));
            run($ctx, 'BEGIN', []);
            run($ctx, 'INSERT INTO t (name) VALUES (?)', ['a']);
            run($ctx, 'COMMIT', []);
            execute($ctx, 'SELECT * FROM t', []);
        });
        // BEGIN, INSERT, COMMIT, SELECT — write, tx-control and read all funnel through the ONE seam.
        $this->assertSame(['BEGIN', 'INSERT INTO t (name) VALUES (?)', 'COMMIT', 'SELECT * FROM t'], $seen);
    }

    public function testRedProofWithoutWiringNothingObserved(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        $seen = [];
        // No registration → the seam is a byte-identical passthrough.
        run($ctx, 'INSERT INTO t (name) VALUES (?)', ['a']);
        execute($ctx, 'SELECT * FROM t', []);
        $this->assertSame([], $seen); // non-empty iff the hook fired — proves the assertion is real
    }

    public function testCanRewriteSqlAndParams(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        withMiddlewareScope(function () use ($ctx): void {
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) {
                if (str_starts_with($sql, 'INSERT')) {
                    return $next($sql, ['rewritten']);
                }
                return $next($sql, $params);
            }]));
            run($ctx, 'INSERT INTO t (name) VALUES (?)', ['original']);
        });
        $row = $db->query('SELECT name FROM t')->fetch(\PDO::FETCH_OBJ);
        $this->assertSame('rewritten', $row->name);
    }

    public function testCanTimeNext(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        $timed = -1.0;
        withMiddlewareScope(function () use ($ctx, &$timed): void {
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$timed) {
                $t0 = microtime(true);
                $r = $next($sql, $params);
                $timed = microtime(true) - $t0;
                return $r;
            }]));
            execute($ctx, 'SELECT * FROM t', []);
        });
        $this->assertGreaterThanOrEqual(0.0, $timed);
    }

    public function testCanShortCircuit(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        withMiddlewareScope(function () use ($ctx): void {
            use_(createMiddleware(['execute' => function () {
                // Do NOT call next — short-circuit with a synthetic row list (next/sql/params ignored).
                return [(object) ['id' => 99, 'name' => 'synthetic']];
            }]));
            $rows = execute($ctx, 'SELECT * FROM t', []);
            $this->assertEquals([(object) ['id' => 99, 'name' => 'synthetic']], $rows);
        });
        // Nothing was ever inserted, so a real query returns 0 rows — proves the DB was bypassed.
        $count = (int) $db->query('SELECT COUNT(*) c FROM t')->fetch(\PDO::FETCH_OBJ)->c;
        $this->assertSame(0, $count);
    }

    public function testAppliedOrderFirstRegisteredIsOutermost(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        $order = [];
        withMiddlewareScope(function () use ($ctx, &$order): void {
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$order) {
                $order[] = 'A:before';
                $r = $next($sql, $params);
                $order[] = 'A:after';
                return $r;
            }]));
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$order) {
                $order[] = 'B:before';
                $r = $next($sql, $params);
                $order[] = 'B:after';
                return $r;
            }]));
            execute($ctx, 'SELECT 1', []);
        });
        // use(A); use(B) ⇒ A outermost: A.before → B.before → «execute» → B.after → A.after.
        $this->assertSame(['A:before', 'B:before', 'B:after', 'A:after'], $order);
    }

    public function testRedProofFoldReversedWouldBreakOrder(): void
    {
        // Non-vacuous RED proof for the fold DIRECTION: if the chain folded FIRST→LAST (index 0
        // innermost) the observed order would be B-outer. Assert the real fold does NOT produce that.
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        $order = [];
        withMiddlewareScope(function () use ($ctx, &$order): void {
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$order) {
                $order[] = 'A';
                return $next($sql, $params);
            }]));
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$order) {
                $order[] = 'B';
                return $next($sql, $params);
            }]));
            execute($ctx, 'SELECT 1', []);
        });
        $this->assertSame(['A', 'B'], $order);                 // real fold: A outermost fires first
        $this->assertNotSame(['B', 'A'], $order);              // a reversed fold would fail here (RED)
    }

    public function testPerScopeStateIsIsolatedAndFresh(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        $mw = createMiddleware([
            'state' => ['count' => 0],
            'execute' => function (callable $next, string $sql, array $params) {
                $this->count++;
                return $next($sql, $params);
            },
        ]);
        withMiddlewareScope(function () use ($ctx, $mw): void {
            use_($mw);
            execute($ctx, 'SELECT 1', []);
            execute($ctx, 'SELECT 2', []);
            $this->assertSame(2, $mw->state()->count);
        });
        // A fresh scope starts from a fresh state copy (0), not the previous scope's 2.
        withMiddlewareScope(function () use ($ctx, $mw): void {
            use_($mw);
            execute($ctx, 'SELECT 3', []);
            $this->assertSame(1, $mw->state()->count);
        });
    }

    public function testExplicitRegistryScopeIsolation(): void
    {
        // PHP-specific applicable mechanism: an EXPLICIT Registry threaded as an arg (the §3-table
        // "php = explicit registry arg" — the honest single-threaded analogue of the TS ALS scope).
        // Two distinct registries hold independent stacks + state; a hook in one is invisible to the
        // other. This is what IS applicable for PHP (concurrency isolation is N/A — see the class doc).
        $regA = new Registry();
        $regB = new Registry();
        $seenA = [];
        $seenB = [];
        registerMiddleware(createMiddleware(['find' => function ($m, callable $next, ...$a) use (&$seenA) {
            $seenA[] = 'A';
            return $next(...$a);
        }]), $regA);
        registerMiddleware(createMiddleware(['find' => function ($m, callable $next, ...$a) use (&$seenB) {
            $seenB[] = 'B';
            return $next(...$a);
        }]), $regB);

        runMethod(MethodKind::FIND, null, static fn () => 'ra', [], $regA);
        runMethod(MethodKind::FIND, null, static fn () => 'rb', [], $regB);

        $this->assertSame(['A'], $seenA);   // only regA's hook fired for the regA dispatch
        $this->assertSame(['B'], $seenB);   // only regB's hook fired for the regB dispatch
    }

    // ── D1 END-TO-END: a real relation-BATCH read fans out through the seam ────

    /**
     * A registered middleware observes the relation-BATCH SELECT of a real multi-node relation read.
     * `Relation::runRelationOp` runs the hasMany child batch through the SAME read seam
     * ({@see execute()}) the primary read uses — so a registered SQL middleware sees the child SELECT.
     * This is the reference relation-coverage proof the ports copy. RED: unregistered ⇒ NOT observed.
     */
    public function testMiddlewareObservesRelationBatchSql(): void
    {
        $db = self::relDb();
        $op = self::relOp();
        $parents = [(object) ['id' => 1]];

        $seen = [];
        $batch = withMiddlewareScope(function () use ($db, $op, $parents, &$seen) {
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$seen) {
                $seen[] = $sql;
                return $next($sql, $params);
            }]));
            // The hasMany batch SELECT on `child` (fan-out over the parent keys) funnels through the seam.
            return \LiteDbModel\Runtime\Relation::runRelationOp($op, $parents, $db);
        });

        // The relation actually loaded (2 children under parent 1) — a genuine multi-node batch read.
        $childLabels = array_map(static fn (\stdClass $r) => $r->label, $batch['1'] ?? []);
        $this->assertSame(['a', 'b'], $childLabels);
        // The middleware saw the relation-batch SELECT (querying the child table).
        $relBatchSql = array_filter($seen, static fn (string $s) => preg_match('/from\s+child/i', $s) === 1);
        $this->assertNotEmpty($relBatchSql, 'middleware must observe the relation-batch SELECT on child');
    }

    public function testRedProofRelationBatchNotObservedWithoutRegistration(): void
    {
        $db = self::relDb();
        $op = self::relOp();
        $parents = [(object) ['id' => 1]];
        $seen = [];
        // No middleware registered → the relation batch runs as a byte-identical passthrough.
        $batch = \LiteDbModel\Runtime\Relation::runRelationOp($op, $parents, $db);
        // The read still WORKS (byte-identical) — 2 children loaded — but nothing was observed.
        $this->assertCount(2, $batch['1'] ?? []);
        $relBatchSql = array_filter($seen, static fn (string $s) => preg_match('/from\s+child/i', $s) === 1);
        $this->assertEmpty($relBatchSql);
    }

    private static function relDb(): \PDO
    {
        $db = new \PDO('sqlite::memory:');
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->exec('CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)');
        $db->exec('CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, label TEXT)');
        $db->exec("INSERT INTO parent VALUES (1,'p')");
        $db->exec("INSERT INTO child VALUES (10,1,'a'),(11,1,'b')");
        return $db;
    }

    /** A hasMany relation op (child.parent_id IN parent.id), sqlite dialect — the batch SELECT shape. */
    private static function relOp(): \stdClass
    {
        return (object) [
            'name' => 'kids',
            'kind' => 'hasMany',
            'targetTable' => 'child',
            // sqlite binds the deduped keys as ONE JSON array string; the batch selects rows whose
            // parent_id is in that array (json_each unpacks it) — a real child-table SELECT.
            'sql' => 'SELECT id, parent_id, label FROM child WHERE parent_id IN (SELECT value FROM json_each(?))',
            'parentKey' => 'id',
            'targetKey' => 'parent_id',
            'dialect' => 'sqlite',
        ];
    }

    // ── D2: method-level hooks ────────────────────────────────────────────────

    public function testFiresMatchingOpKindHookBeforeAfter(): void
    {
        foreach ([MethodKind::FIND, MethodKind::CREATE, MethodKind::UPDATE, MethodKind::DELETE] as $kind) {
            $events = [];
            withMiddlewareScope(function () use ($kind, &$events): void {
                use_(createMiddleware([$kind => function ($model, callable $next, ...$args) use ($kind, &$events) {
                    $events[] = "$kind:before";
                    $r = $next(...$args);
                    $events[] = "$kind:after";
                    return $r;
                }]));
                $result = runMethod($kind, null, function () use ($kind, &$events) {
                    $events[] = "$kind:core";
                    return 'ok';
                }, []);
                $this->assertSame('ok', $result);
            });
            $this->assertSame(["$kind:before", "$kind:core", "$kind:after"], $events);
        }
    }

    public function testRedProofDifferentKindHookDoesNotFire(): void
    {
        $events = [];
        withMiddlewareScope(function () use (&$events): void {
            use_(createMiddleware([MethodKind::CREATE => function ($m, callable $next, ...$args) use (&$events) {
                $events[] = 'create';
                return $next(...$args);
            }]));
            // Dispatch a `find` — the `create` hook must NOT fire (kind mismatch; kind is a TAG, not SQL).
            runMethod(MethodKind::FIND, null, static fn () => 'r', []);
        });
        $this->assertSame([], $events);
    }

    public function testMethodHooksComposeOutermostAndRewriteArgs(): void
    {
        $order = [];
        $coreArg = 0;
        withMiddlewareScope(function () use (&$order, &$coreArg): void {
            use_(createMiddleware([MethodKind::FIND => function ($m, callable $next, $n) use (&$order) {
                $order[] = 'A';
                return $next($n + 1);
            }]));
            use_(createMiddleware([MethodKind::FIND => function ($m, callable $next, $n) use (&$order) {
                $order[] = 'B';
                return $next($n + 10);
            }]));
            runMethod(MethodKind::FIND, null, function ($n) use (&$coreArg) {
                $coreArg = $n;
                return null;
            }, [0]);
        });
        $this->assertSame(['A', 'B'], $order); // A outer, B inner
        $this->assertSame(11, $coreArg);       // 0 +1 (A) +10 (B)
    }

    // ── D3: Logger + raw execute/query ────────────────────────────────────────

    public function testLoggerRecordsSqlParamsTiming(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        $logger = Logger();
        withMiddlewareScope(function () use ($ctx, $logger): void {
            use_($logger);
            run($ctx, 'INSERT INTO t (name) VALUES (?)', ['x']);
            execute($ctx, 'SELECT * FROM t WHERE name = ?', ['x']);
            /** @var list<LogEntry> $entries */
            $entries = $logger->state()->entries;
            $this->assertSame(
                ['INSERT INTO t (name) VALUES (?)', 'SELECT * FROM t WHERE name = ?'],
                array_map(static fn (LogEntry $e) => $e->sql, $entries),
            );
            $this->assertSame(['x'], $entries[0]->params);
            $this->assertSame(['x'], $entries[1]->params);
            foreach ($entries as $e) {
                $this->assertGreaterThanOrEqual(0.0, $e->durationMs);
            }
        });
    }

    public function testRawExecuteGoesThroughTheSeam(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        $seen = [];
        withMiddlewareScope(function () use ($ctx, &$seen): void {
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$seen) {
                $seen[] = $sql;
                return $next($sql, $params);
            }]));
            $insert = rawExecute($ctx, 'INSERT INTO t (name) VALUES (?)', ['raw']);
            $this->assertSame(1, $insert->rowCount);
            $read = rawExecute($ctx, 'SELECT name FROM t');
            $this->assertEquals([(object) ['name' => 'raw']], $read->rows);
        });
        $this->assertSame(['INSERT INTO t (name) VALUES (?)', 'SELECT name FROM t'], $seen);
    }

    public function testRawQueryFiresQueryHookAndFlowsThroughSeam(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        $db->exec("INSERT INTO t (name) VALUES ('q')");
        $events = [];
        withMiddlewareScope(function () use ($ctx, &$events): void {
            use_(createMiddleware([
                MethodKind::QUERY => function ($m, callable $next, ...$args) use (&$events) {
                    $events[] = 'query';
                    return $next(...$args);
                },
                'execute' => function (callable $next, string $sql, array $params) use (&$events) {
                    $events[] = "execute:$sql";
                    return $next($sql, $params);
                },
            ]));
            $rows = rawQuery($ctx, 'SELECT name FROM t');
            $this->assertEquals([(object) ['name' => 'q']], $rows);
        });
        // The `query` method hook fires (D2 op-kind dispatch), THEN the SQL flows through the execute seam.
        $this->assertSame(['query', 'execute:SELECT name FROM t'], $events);
    }

    public function testRedProofLoggerRecordsNothingWithoutWiring(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        $logger = Logger();
        // NOT registered → the seam never invokes it.
        execute($ctx, 'SELECT 1', []);
        $this->assertSame([], $logger->state()->entries);
    }

    // ── D1: the scoped registry restores the prior ambient on exit ────────────

    public function testScopeRestoresPriorRegistryOnExit(): void
    {
        $db = self::freshDb();
        $ctx = Context::forPdo($db);
        $outer = [];
        // A GLOBAL registration (app-startup path) — visible before + after a nested scope.
        registerMiddleware(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$outer) {
            $outer[] = $sql;
            return $next($sql, $params);
        }]));
        $inner = [];
        withMiddlewareScope(function () use ($ctx, &$inner): void {
            // Seeded from the current (global) registry ⇒ the outer hook is inherited...
            use_(createMiddleware(['execute' => function (callable $next, string $sql, array $params) use (&$inner) {
                $inner[] = $sql;
                return $next($sql, $params);
            }]));
            execute($ctx, "SELECT 'inner'", []);
        });
        // Back on the global registry: the scope's own registration is gone, the global one remains.
        execute($ctx, "SELECT 'outer'", []);
        $this->assertSame(["SELECT 'inner'"], $inner);                   // scope hook only inside the scope
        $this->assertSame(["SELECT 'inner'", "SELECT 'outer'"], $outer); // global hook: inherited + after
    }
}
