<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\SqlFailure;
use PHPUnit\Framework\TestCase;

/**
 * Error-mapping tests (WS7d, #33): the PHP port of src/scp/errors.ts. The frozen vector corpus's
 * commit + gate-short-circuit paths never hit a driver error, so the mapping is proven directly:
 * a real PDO constraint violation maps to the SAME kind/policy the TS reference assigns.
 */
final class SqlFailureTest extends TestCase
{
    public function testUniqueConstraintMapsToConstraintViolationFail(): void
    {
        $db = new \PDO('sqlite::memory:');
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->exec('CREATE TABLE t (id INTEGER PRIMARY KEY)');
        $db->exec('INSERT INTO t (id) VALUES (1)');
        try {
            $db->exec('INSERT INTO t (id) VALUES (1)'); // duplicate PK
            $this->fail('expected a PDOException');
        } catch (\PDOException $e) {
            $f = SqlFailure::fromPdo($e);
            $this->assertSame(SqlFailure::KIND_CONSTRAINT, $f->kind);
            $this->assertSame('fail', $f->policy);
            $this->assertStringContainsString('SQLITE_CONSTRAINT', $f->getMessage());
        }
    }

    public function testForeignKeyViolationMapsToFkKind(): void
    {
        $db = new \PDO('sqlite::memory:');
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->exec('PRAGMA foreign_keys = ON');
        $db->exec('CREATE TABLE u (id INTEGER PRIMARY KEY)');
        $db->exec('CREATE TABLE p (id INTEGER PRIMARY KEY, uid INTEGER REFERENCES u(id))');
        try {
            $db->exec('INSERT INTO p (id, uid) VALUES (1, 999)'); // no such parent
            $this->fail('expected a PDOException');
        } catch (\PDOException $e) {
            $f = SqlFailure::fromPdo($e);
            $this->assertSame(SqlFailure::KIND_FOREIGN_KEY, $f->kind);
            $this->assertSame('SQLITE_CONSTRAINT_FOREIGNKEY', $f->sqliteCode);
        }
    }

    public function testBusyMapsToRetryable(): void
    {
        // Direct code mapping (the boundary re-map path from a wrapped OP_FAILED message).
        $f = SqlFailure::fromCode('SQLITE_BUSY', 'database is locked');
        $this->assertSame(SqlFailure::KIND_RETRYABLE, $f->kind);
        $this->assertSame('retry', $f->policy);
    }

    public function testUnknownCodeMapsToDriverErrorFail(): void
    {
        $f = SqlFailure::fromCode('SQLITE_IOERR', 'disk I/O error');
        $this->assertSame(SqlFailure::KIND_DRIVER, $f->kind);
        $this->assertSame('fail', $f->policy);
    }
}
