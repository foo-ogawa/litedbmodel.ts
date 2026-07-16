<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\Connection;
use LiteDbModel\Runtime\Context;
use LiteDbModel\Runtime\ExecutionContext;
use LiteDbModel\Runtime\LiveDb;
use LiteDbModel\Runtime\MiddlewareChain;
use LiteDbModel\Runtime\PdoConnection;
use LiteDbModel\Runtime\PdoDriver;
use LiteDbModel\Runtime\Runtime;
use LiteDbModel\Runtime\RunInfo;
use LiteDbModel\Runtime\SqlFailure;
use LiteDbModel\Runtime\StatementIntent;
use PHPUnit\Framework\TestCase;

/**
 * Phase A / #79 — DOCKER INTEGRATION: multi-statement transaction ATOMICITY on live PG:5433 +
 * MySQL:3307, through the PRODUCTION `Runtime::executeTransactionBundle` → per-execution
 * connection-ownership path (§3).
 *
 * The teeth: a 2-statement transaction whose SECOND statement fails (a duplicate-PK violation) MUST
 * roll back the FIRST statement's write — asserted by REAL row membership on the live DB. Because
 * `withTransactionDecided` acquires ONE owned connection and the seam pins it, BOTH statements run in
 * the SAME transaction, so the driver failure rolls the whole thing back.
 *
 * The FAITHFUL MUTATION proof (that the test has teeth): a {@see MisRoutingContext} routes the tx's
 * writes to a FRESH AUTOCOMMIT connection instead of the owned tx connection — exactly the bug the
 * ownership contract prevents. Under the mutation the first write escapes the transaction and
 * SURVIVES the rollback, so the atomicity assertion goes RED. We assert that RED explicitly, then the
 * un-mutated path GREEN — proving the assertion is load-bearing.
 *
 * REAL DBs, no mock, NO silent skip: if PG/MySQL is unreachable the test ERRORS (the docker
 * integration gate). Set LITEDBMODEL_SKIP_LIVE=1 only to run the pure-unit suite offline.
 */
final class TxAtomicityLiveTest extends TestCase
{
    /** @return list<array{0:string,1:callable():\PDO}> */
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
    }

    /** DDL for the `atom` table the atomicity vector writes (id PK forces a 2nd-stmt collision). */
    private static function resetAtom(\PDO $db): void
    {
        $db->exec('DROP TABLE IF EXISTS atom');
        $db->exec('CREATE TABLE atom (id INTEGER PRIMARY KEY, v VARCHAR(32))');
    }

    /**
     * A 2-statement tx plan: stmt-1 inserts (id=1), stmt-2 inserts (id=1 AGAIN) → PK collision, the
     * driver raises → the whole tx rolls back → stmt-1's row (id=1) MUST be absent. Both statements
     * are plain body writes (no gate); the second's failure is a genuine driver error (spec §11).
     *
     * @param 'postgres'|'mysql' $dialect
     */
    private static function collisionBundle(string $dialect): \stdClass
    {
        // A 2-body-statement plan; entityFrom picks one body id so isBatch stays false (real gate-less
        // tx path, not the createMany batch path). The op params are bare-literal bc expressions.
        return json_decode(json_encode([
            'dialect' => $dialect,
            'transaction' => [
                'entityFrom' => 's1',
                'statements' => [
                    ['id' => 's1', 'role' => 'body', 'op' => ['sql' => 'INSERT INTO atom (id, v) VALUES (?, ?)', 'params' => [1, 'first']]],
                    ['id' => 's2', 'role' => 'body', 'op' => ['sql' => 'INSERT INTO atom (id, v) VALUES (?, ?)', 'params' => [1, 'second']]],
                ],
            ],
        ]), false);
    }

    /** @dataProvider liveDrivers */
    public function testMultiStatementAtomicityRollsBackFirstOnSecondFailure(string $dialect, callable $connect): void
    {
        $db = $this->connectOrFail($connect, $dialect);
        self::resetAtom($db);
        $bundle = self::collisionBundle($dialect);

        // The PRODUCTION path: executeTransactionBundle → withTransactionDecided → owned connection.
        try {
            Runtime::executeTransactionBundle($bundle, [], $db);
            $this->fail("[$dialect] expected the 2nd INSERT (duplicate PK) to raise a driver failure");
        } catch (SqlFailure $e) {
            $this->addToAssertionCount(1); // the driver failure surfaced (mapped)
        }

        // ATOMICITY: the first insert (id=1) MUST have been rolled back with the failed second.
        $rows = $db->query('SELECT id FROM atom')->fetchAll(\PDO::FETCH_COLUMN);
        $this->assertZeroRows($rows, $dialect);
    }

    /** @dataProvider liveDrivers */
    public function testFaithfulMutationMakesAtomicityRed(string $dialect, callable $connect): void
    {
        $db = $this->connectOrFail($connect, $dialect);
        self::resetAtom($db);
        $bundle = self::collisionBundle($dialect);

        // FAITHFUL MUTATION: a ctx that mis-routes the tx's WRITES onto a FRESH AUTOCOMMIT connection
        // (a second live PDO) instead of the owned tx connection — the exact ownership violation the
        // contract forbids. The first write commits immediately (autocommit) and SURVIVES the rollback.
        $autocommit = ($connect)();
        $mutated = new MisRoutingContext(new PdoDriver($db), new MiddlewareChain(), new PdoConnection($autocommit));

        try {
            Runtime::executeTransactionBundle($bundle, [], $mutated);
            // The 2nd insert also mis-routes to autocommit; the duplicate PK still raises there.
        } catch (\Throwable) {
            // ignore — we only care that the FIRST write leaked past the rollback.
        }

        // Under the mutation, the first write escaped the tx (autocommit) → it PERSISTS. So the
        // atomicity assertion WOULD be RED. Prove the mutation actually broke atomicity:
        $rows = $db->query('SELECT id FROM atom')->fetchAll(\PDO::FETCH_COLUMN);
        $this->assertGreaterThanOrEqual(
            1,
            count($rows),
            "[$dialect] MUTATION PROOF: mis-routing the tx write to an autocommit connection MUST leak "
            . 'the first row past the rollback (⇒ the atomicity test has teeth). It did not — the '
            . 'atomicity assertion would pass even when broken.'
        );
        // Clean up the leaked row so a re-run starts fresh.
        self::resetAtom($db);
    }

    private function connectOrFail(callable $connect, string $dialect): \PDO
    {
        try {
            return ($connect)();
        } catch (\Throwable $e) {
            $this->fail("[$dialect] live DB unreachable (docker integration gate): " . $e->getMessage());
        }
    }

    /** Assert the atomicity outcome: ZERO rows survived (the rollback took the first write with it). */
    private function assertZeroRows(array $rows, string $dialect): void
    {
        $this->assertCount(
            0,
            $rows,
            "[$dialect] ATOMICITY VIOLATED: the 1st INSERT survived the 2nd's failure — the two "
            . 'statements did not share one owned transaction. Rows present: ' . json_encode($rows)
        );
    }
}

/**
 * The FAITHFUL MUTATION: an ExecutionContext that resolves a statement's connection to a fresh
 * AUTOCOMMIT connection for WRITES (escaping the owned tx) instead of the pinned tx connection. Used
 * ONLY by the mutation proof to show the atomicity assertion has teeth — never in production.
 */
final class MisRoutingContext extends ExecutionContext
{
    public function __construct(
        PdoDriver $driver,
        MiddlewareChain $middleware,
        private readonly Connection $escapeConn,
        private readonly bool $pinned = false,
    ) {
        parent::__construct($driver, $middleware, $pinned ? $escapeConn : null);
    }

    public function connectionFor(StatementIntent $intent): Connection
    {
        // The teeth of the mutation: send tx WRITES to the autocommit escape connection, so the first
        // insert commits immediately and survives the rollback.
        if ($intent->write) {
            return $this->escapeConn;
        }
        return parent::connectionFor($intent);
    }

    public function withConnection(Connection $conn, bool $tx): ExecutionContext
    {
        // Preserve the mis-routing across the tx-scoped derivation (withTransactionDecided derives a
        // tx-scoped ctx; the mutation must persist so the write still escapes).
        return new MisRoutingContext($this->driver(), $this->middleware, $this->escapeConn, $tx);
    }
}
