<?php

declare(strict_types=1);

namespace LiteDbModel\Bench;

use LiteDbModel\Runtime\Context;
use LiteDbModel\Runtime\ExecutionContext;
use LiteDbModel\Runtime\Leaves;

use function LiteDbModel\Runtime\clearMiddlewares;
use function LiteDbModel\Runtime\createMiddleware;
use function LiteDbModel\Runtime\registerMiddleware;
use function LiteDbModel\Runtime\withTransaction;

/**
 * NATIVE-codegen ORM-bench cell (php leg, epic #123) — the twin of the python `orm_bench.main`.
 *
 * Self-measures the covered ORM ops through the litedbmodel-GENERATED ir-exec module
 * (`behaviors_generated.php`, verbatim `bc generate --lang php`) + `litedbmodel_runtime`'s op-agnostic
 * leaf transport ({@see Leaves::makeHandlers}), and prints a flat CSV (`cell,dialect,op,iter,us`) the
 * TS collector aggregates.
 *
 * This cell is a litedbmodel-CONSUMER: it binds the leaf transport (`makeHandlers` →
 * executeSQL/pluck/group) into the generated module's `bind($handlers)` (boundary injection — the php
 * literal/ir-exec path, epic #123: ts/go/rust = native de-box; py/php = literal) and calls the
 * resulting per-op callables. It holds NO hand-written exec seam and NO hand-written BEGIN/COMMIT:
 *
 *   - reads/single-writes/batches run the bound op callable directly; the leaf funnels every DB access
 *     through the runtime central execute/run seam. Relations are N+1-free: parents → pluck →
 *     executeSQL(WHERE fk IN …) → group = 1 batched child query per level (nestedFindAll=2,
 *     nestedRelations=3, composite=3). Batch writes are ONE json_each statement.
 *   - RETURNING-chained TRANSACTIONS run THROUGH the runtime tx boundary {@see withTransaction()}
 *     (BEGIN → body → COMMIT on ok / ROLLBACK on error) — the consumer's tx-boundary responsibility.
 *     The generated `.map` runner emits its 2 body statements via the leaf; `withTransaction` pins the
 *     tx-owned connection (the leaf resolves it via `currentContext()`) and brackets BEGIN/COMMIT.
 */
final class OrmBench
{
    /**
     * The generated schema (the committed native fixture — identical to the rust/go/python cells) +
     * the canonical nested seed. FIXTURE setup, not covered code: the harness measures the GENERATED
     * op callables, it does not hand-write them.
     *
     * @var list<string>
     */
    public const SCHEMA = [
        "CREATE TABLE benchmark_users (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        email TEXT NOT NULL UNIQUE,\n        name TEXT,\n        created_at TEXT DEFAULT (datetime('now')),\n        updated_at TEXT DEFAULT (datetime('now'))\n      )",
        "CREATE TABLE benchmark_posts (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        title TEXT NOT NULL,\n        content TEXT,\n        published INTEGER DEFAULT 0,\n        author_id INTEGER,\n        created_at TEXT DEFAULT (datetime('now'))\n      )",
        "CREATE TABLE benchmark_comments (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        body TEXT NOT NULL,\n        post_id INTEGER,\n        created_at TEXT DEFAULT (datetime('now'))\n      )",
        "CREATE TABLE benchmark_tenant_users (\n        tenant_id INTEGER NOT NULL,\n        user_id INTEGER NOT NULL,\n        name TEXT,\n        PRIMARY KEY (tenant_id, user_id)\n      )",
        "CREATE TABLE benchmark_tenant_posts (\n        tenant_id INTEGER NOT NULL,\n        post_id INTEGER NOT NULL,\n        user_id INTEGER NOT NULL,\n        title TEXT NOT NULL,\n        PRIMARY KEY (tenant_id, post_id)\n      )",
        "CREATE TABLE benchmark_tenant_comments (\n        tenant_id INTEGER NOT NULL,\n        comment_id INTEGER NOT NULL,\n        post_id INTEGER NOT NULL,\n        body TEXT NOT NULL,\n        PRIMARY KEY (tenant_id, comment_id)\n      )",
    ];

    /** @var list<string> */
    public const SEED = [
        'DELETE FROM benchmark_comments',
        'DELETE FROM benchmark_posts',
        'DELETE FROM benchmark_users',
        'DELETE FROM benchmark_tenant_comments',
        'DELETE FROM benchmark_tenant_posts',
        'DELETE FROM benchmark_tenant_users',
        "INSERT INTO benchmark_users (id, email, name) VALUES "
        . "(1,'user1@example.com','User 1'),(2,'user2@example.com','User 2'),"
        . "(3,'user3@example.com','User 3'),(4,'user4@example.com','User 4'),(5,'user5@example.com','User 5')",
        "INSERT INTO benchmark_posts (id, title, content, published, author_id) VALUES "
        . "(1,'P1','c',1,1),(2,'P2','c',1,1),(3,'P3','c',1,2),(4,'P4','c',1,2),(5,'P5','c',1,3),(6,'P6','c',1,3)",
        "INSERT INTO benchmark_comments (id, body, post_id) VALUES (1,'b',1),(2,'b',1),(3,'b',2),(4,'b',3),(5,'b',5)",
        "INSERT INTO benchmark_tenant_users (tenant_id, user_id, name) VALUES (1,1,'TU1'),(1,2,'TU2'),(1,3,'TU3')",
        "INSERT INTO benchmark_tenant_posts (tenant_id, post_id, user_id, title) VALUES (1,10,1,'TP1'),(1,11,2,'TP2')",
        "INSERT INTO benchmark_tenant_comments (tenant_id, comment_id, post_id, body) VALUES "
        . "(1,100,10,'tc'),(1,101,10,'tc'),(1,102,11,'tc')",
    ];

    /**
     * All 19 covered ops in generated declaration order (COMPONENT_NAMES).
     *
     * @var list<string>
     */
    public const OPS = [
        'findAll', 'filterPaginateSort', 'findFirst', 'findUnique',
        'nestedFindAll', 'nestedFindFirst', 'nestedFindUnique', 'nestedRelations', 'compositeRelations',
        'create', 'update', 'upsert', 'createMany', 'upsertMany', 'updateMany',
        'nestedCreate', 'nestedUpsert', 'nestedUpdate', 'delete',
    ];

    /**
     * The RETURNING-chained transactions — run THROUGH the runtime tx boundary. The generated runner
     * emits no BEGIN/COMMIT; the boundary is the consumer's (BEGIN + 2 body + COMMIT).
     *
     * @var list<string>
     */
    public const TX_OPS = ['nestedCreate', 'nestedUpsert', 'nestedUpdate', 'delete'];

    // ── safety expectations ──────────────────────────────────────────────────────────
    /** Batched relation: 1 parent + 1 batched child per level, INDEPENDENT of the row count. @var array<string,int> */
    public const RELATION_QUERY_COUNTS = [
        'nestedFindAll' => 2, 'nestedFindFirst' => 2, 'nestedFindUnique' => 2, 'nestedRelations' => 3, 'compositeRelations' => 3,
    ];
    /** Batch write: ONE json_each statement for N records (no per-row fan-out). @var array<string,int> */
    public const BATCH_QUERY_COUNTS = ['createMany' => 1, 'upsertMany' => 1, 'updateMany' => 1];
    /** RETURNING-chained tx: BEGIN + 2 body (the RETURNING write + the dependent write) + COMMIT = 4. @var array<string,int> */
    public const TX_STMT_COUNTS = ['nestedCreate' => 4, 'nestedUpsert' => 4, 'nestedUpdate' => 4, 'delete' => 4];

    /**
     * An in-memory sqlite DB built from the generated schema (autocommit, so the runtime tx boundary's
     * explicit BEGIN/COMMIT works). `$spec` reserved for the live pg/mysql legs.
     */
    public static function openDriver(string $spec = 'sqlite'): \PDO
    {
        unset($spec);
        $db = new \PDO('sqlite::memory:');
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->setAttribute(\PDO::ATTR_STRINGIFY_FETCHES, false);
        foreach (self::SCHEMA as $stmt) {
            $db->exec($stmt);
        }
        return $db;
    }

    /**
     * DELETE + INSERT the canonical nested fixture (runs on the PDO DIRECTLY — not through the seam, so
     * it is never counted by the safety middleware).
     */
    public static function seed(\PDO $db): void
    {
        foreach (self::SEED as $stmt) {
            $db->exec($stmt);
        }
    }

    /**
     * Bind the op-agnostic leaf transport into the generated module — the per-op callables. The
     * generated module `return`s the factory object; `bind` injects the handlers (boundary injection).
     *
     * @return array<string,callable>
     */
    public static function boundOps(\PDO|ExecutionContext $driver, string $dialect): array
    {
        $mod = require __DIR__ . '/behaviors_generated.php';
        return ($mod->bind)(Leaves::makeHandlers(Context::of($driver), $dialect));
    }

    /**
     * The per-op input scope (the emitter-declared `value` input ports). Mutating ops vary their UNIQUE
     * column by iteration (matching the rust/python bench cell); a read with no input ports gets `[]`.
     *
     * @return array<string,mixed>
     */
    public static function opInput(string $op, int $it): array
    {
        return match ($op) {
            'filterPaginateSort' => ['published' => 1],
            'findFirst', 'nestedFindFirst' => ['name' => 'User%'],
            'findUnique', 'nestedFindUnique' => ['email' => 'user1@example.com'],
            'create' => ['email' => "new{$it}@bench.com", 'name' => 'New'],
            'update' => ['id' => 1, 'name' => 'Updated 1'],
            'upsert' => ['email' => 'user1@example.com', 'name' => 'Upserted One'],
            'createMany' => ['rows' => self::userRows($it, false)],
            'upsertMany' => ['rows' => self::userRows($it, true)],
            'updateMany' => ['rows' => array_map(static fn (int $i) => ['id' => $i, 'name' => "Many {$i}"], range(1, 10))],
            'nestedCreate' => ['email' => "nc{$it}@bench.com", 'name' => 'NC', 'title' => 'NC Post'],
            'nestedUpsert' => ['email' => 'user1@example.com', 'name' => 'NUp', 'title' => 'NUp Post'],
            'nestedUpdate' => ['id' => 1, 'name' => 'NU', 'title' => 'NU Post'],
            'delete' => ['email' => "del{$it}@bench.com", 'name' => 'Del'],
            default => [],
        };
    }

    /**
     * The 10-row batch record set for createMany/upsertMany (ONE opaque `rows` array — the json_each
     * batch param). `$stable` reuses fixed emails (upsertMany — conflict-updates); else the email
     * varies by iteration so a plain INSERT stays insertable under UNIQUE(email).
     *
     * @return list<array<string,string>>
     */
    private static function userRows(int $it, bool $stable): array
    {
        return array_map(
            static fn (int $i) => [
                'email' => $stable ? "many{$i}@bench.com" : "many{$it}_{$i}@bench.com",
                'name' => "Many {$i}",
            ],
            range(0, 9),
        );
    }

    /**
     * Run ONE covered op through its generated callable. A RETURNING-chained tx op runs THROUGH the
     * runtime tx boundary ({@see withTransaction()} over the driver ctx) so BEGIN/COMMIT bracket the
     * leaf's body statements on the tx-owned connection; every other op runs the bound callable
     * directly.
     *
     * @param array<string,callable> $fns
     */
    public static function runOp(array $fns, \PDO|ExecutionContext $driver, string $op, int $it): mixed
    {
        $inp = self::opInput($op, $it);
        if (in_array($op, self::TX_OPS, true)) {
            return withTransaction(Context::of($driver), static fn (ExecutionContext $_tx): mixed => $fns[$op]($inp));
        }
        return $fns[$op]($inp);
    }

    /**
     * Run each guarded op ONCE and return its statement count, observed at the runtime middleware seam
     * (every read / batch write / tx-control statement funnels through execute/run/control →
     * MiddlewareChain::wrap). The seed runs on the PDO directly (off-seam), so it is never counted.
     *
     * @param array<string,callable> $fns
     * @return array<string,int>
     */
    public static function safetyCounts(\PDO $driver, array $fns): array
    {
        $counter = new \stdClass();
        $counter->n = 0;
        $mw = createMiddleware([
            'execute' => function (callable $next, string $sql, array $params) use ($counter): mixed {
                $counter->n++;
                return $next($sql, $params);
            },
        ]);

        clearMiddlewares();
        $unregister = registerMiddleware($mw);
        $out = [];
        try {
            $ops = array_merge(
                array_keys(self::RELATION_QUERY_COUNTS),
                array_keys(self::BATCH_QUERY_COUNTS),
                array_keys(self::TX_STMT_COUNTS),
            );
            foreach ($ops as $op) {
                self::seed($driver); // clean fixture per op; not counted (runs off-seam)
                $counter->n = 0;
                self::runOp($fns, $driver, $op, 0);
                $out[$op] = $counter->n;
            }
        } finally {
            $unregister();
            clearMiddlewares();
        }
        return $out;
    }

    /** The measurement loop: for each op, re-seed then time `$reps` runs; print `cell,dialect,op,iter,us`. */
    public static function measure(string $dialect, string $spec, int $reps, int $warmup): void
    {
        $driver = self::openDriver($spec);
        $fns = self::boundOps($driver, $dialect);
        echo "cell,dialect,op,iter,us\n";
        foreach (self::OPS as $op) {
            self::seed($driver); // re-seed before each op so writes/reads start from the canonical fixture
            for ($it = 0; $it < $warmup; $it++) {
                self::runOp($fns, $driver, $op, $it);
            }
            for ($it = 0; $it < $reps; $it++) {
                $g = $it + $warmup; // unique iteration id (UNIQUE-email ops stay insertable across warmup+timed)
                $t = hrtime(true);
                self::runOp($fns, $driver, $op, $g);
                $us = intdiv(hrtime(true) - $t, 1000);
                echo "native,{$dialect},{$op},{$it},{$us}\n";
            }
        }
    }

    /** The safety mode: assert each guarded op's statement count matches its expectation; print it. */
    public static function safety(string $dialect, string $spec): void
    {
        $driver = self::openDriver($spec);
        $fns = self::boundOps($driver, $dialect);
        $counts = self::safetyCounts($driver, $fns);
        $expected = self::RELATION_QUERY_COUNTS + self::BATCH_QUERY_COUNTS + self::TX_STMT_COUNTS;
        foreach ($expected as $op => $want) {
            $got = $counts[$op];
            if ($got !== $want) {
                throw new \RuntimeException("{$op} statement-count regression: got {$got}, expect {$want}");
            }
            $kind = isset(self::TX_STMT_COUNTS[$op]) ? 'statements (BEGIN + 2 body + COMMIT)' : 'queries';
            echo "{$op} {$kind}={$got} (expect {$want})\n";
        }
    }
}
