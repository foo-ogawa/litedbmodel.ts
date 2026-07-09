<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\Runtime;
use PHPUnit\Framework\TestCase;

/**
 * Render-path unit tests (WS7d, #33): the normative dynamic-expansion rules ported from
 * src/scp/render.ts + dialect.ts. These assert the PHP runtime reproduces the reference render
 * behavior (SKIP existence, IN-list expansion, empty-WHERE degeneration, `?`→`$N` for Postgres,
 * canonical param order) beyond the frozen vector corpus.
 */
final class RenderTest extends TestCase
{
    /** Decode an IR fixture the way the runtime consumes a bundle (json_decode(.., false)). */
    private static function op(array $shape): \stdClass
    {
        return json_decode((string) json_encode($shape), false, 512, JSON_THROW_ON_ERROR);
    }

    public function testSelectEqAndInSqlite(): void
    {
        $op = self::op([
            'component' => 'Select',
            'sql' => 'SELECT id FROM posts{where} ORDER BY id ASC',
            'where' => [
                'connector' => 'AND',
                'fragments' => [
                    ['always' => true, 'sql' => 'author_id = ?', 'params' => [['ref' => ['authorId']]]],
                    ['always' => true, 'sql' => 'id IN (?)', 'params' => [['ref' => ['ids']]], 'expand' => 0],
                ],
            ],
            'params' => [],
            'assembly' => ['shape' => 'items'],
        ]);
        $r = Runtime::renderOperation($op, ['authorId' => 7, 'ids' => [1, 2, 3]], 'sqlite');
        $this->assertSame('SELECT id FROM posts WHERE author_id = ? AND id IN (?, ?, ?) ORDER BY id ASC', $r['sql']);
        $this->assertSame([7, 1, 2, 3], $r['params']);
    }

    public function testPostgresPlaceholderConversionIsOnePass(): void
    {
        $op = self::op([
            'component' => 'Select',
            'sql' => 'SELECT id FROM posts{where}',
            'where' => [
                'connector' => 'AND',
                'fragments' => [
                    ['always' => true, 'sql' => 'author_id = ?', 'params' => [['ref' => ['authorId']]]],
                    ['always' => true, 'sql' => 'id IN (?)', 'params' => [['ref' => ['ids']]], 'expand' => 0],
                ],
            ],
            'params' => [],
            'assembly' => ['shape' => 'items'],
        ]);
        $r = Runtime::renderOperation($op, ['authorId' => 7, 'ids' => [1, 2]], 'postgres');
        // A single left-to-right pass over the FULLY assembled text: no renumbering problem.
        $this->assertSame('SELECT id FROM posts WHERE author_id = $1 AND id IN ($2, $3)', $r['sql']);
        $this->assertSame([7, 1, 2], $r['params']);
    }

    public function testSkipDropsFragmentWhenGuardFalse(): void
    {
        $op = self::op([
            'component' => 'Select',
            'sql' => 'SELECT id FROM posts{where}',
            'where' => [
                'connector' => 'AND',
                'fragments' => [
                    ['always' => true, 'sql' => 'author_id = ?', 'params' => [['ref' => ['authorId']]]],
                    [
                        'sql' => 'status = ?',
                        'params' => [['ref' => ['status']]],
                        'when' => ['ne' => [['refOpt' => ['status']], null]],
                    ],
                ],
            ],
            'params' => [],
            'assembly' => ['shape' => 'items'],
        ]);
        // status = null → the SKIP fragment drops (no SQL, no param).
        $dropped = Runtime::renderOperation($op, ['authorId' => 7, 'status' => null], 'sqlite');
        $this->assertSame('SELECT id FROM posts WHERE author_id = ?', $dropped['sql']);
        $this->assertSame([7], $dropped['params']);
        // status present → the fragment renders.
        $present = Runtime::renderOperation($op, ['authorId' => 7, 'status' => 'live'], 'sqlite');
        $this->assertSame('SELECT id FROM posts WHERE author_id = ? AND status = ?', $present['sql']);
        $this->assertSame([7, 'live'], $present['params']);
    }

    public function testEmptyWhereDegeneration(): void
    {
        $op = self::op([
            'component' => 'Select',
            'sql' => 'SELECT id FROM posts{where}',
            'where' => [
                'connector' => 'AND',
                'fragments' => [
                    [
                        'sql' => 'status = ?',
                        'params' => [['ref' => ['status']]],
                        'when' => ['ne' => [['refOpt' => ['status']], null]],
                    ],
                ],
            ],
            'params' => [],
            'assembly' => ['shape' => 'items'],
        ]);
        // The sole fragment is SKIP-dropped → the whole ` WHERE ` keyword collapses (§3).
        $r = Runtime::renderOperation($op, ['status' => null], 'sqlite');
        $this->assertSame('SELECT id FROM posts', $r['sql']);
        $this->assertSame([], $r['params']);
    }

    public function testEmptyInListDegeneratesToAlwaysFalse(): void
    {
        $op = self::op([
            'component' => 'Select',
            'sql' => 'SELECT id FROM posts{where}',
            'where' => [
                'connector' => 'AND',
                'fragments' => [
                    ['always' => true, 'sql' => 'id IN (?)', 'params' => [['ref' => ['ids']]], 'expand' => 0],
                ],
            ],
            'params' => [],
            'assembly' => ['shape' => 'items'],
        ]);
        // Empty IN → `1 = 0` with NO params pushed (§5, byte-identical to v1).
        $r = Runtime::renderOperation($op, ['ids' => []], 'sqlite');
        $this->assertSame('SELECT id FROM posts WHERE 1 = 0', $r['sql']);
        $this->assertSame([], $r['params']);
    }

    public function testCoalesceLimitDefaultFromIr(): void
    {
        // The LIMIT default is a coalesce IN THE IR (SSoT), not an ad-hoc code default.
        $op = self::op([
            'component' => 'Select',
            'sql' => 'SELECT id FROM posts LIMIT ?',
            'where' => null,
            'params' => [['coalesce' => [['refOpt' => ['limit']], 20]]],
            'assembly' => ['shape' => 'items'],
        ]);
        $this->assertSame([20], Runtime::renderOperation($op, ['limit' => null], 'sqlite')['params']);
        $this->assertSame([5], Runtime::renderOperation($op, ['limit' => 5], 'sqlite')['params']);
    }

    /**
     * @dataProvider orderByNullsProvider
     */
    public function testOrderByNulls(string $dialect, string $dir, string $nulls, string $expected): void
    {
        $this->assertSame($expected, Runtime::orderByNulls('created_at', $dir, $nulls, $dialect));
    }

    /** @return array<int,array{0:string,1:string,2:string,3:string}> */
    public static function orderByNullsProvider(): array
    {
        return [
            ['sqlite', 'ASC', 'FIRST', 'created_at ASC NULLS FIRST'],
            ['postgres', 'DESC', 'LAST', 'created_at DESC NULLS LAST'],
            ['mysql', 'ASC', 'FIRST', 'created_at IS NULL DESC, created_at ASC'],
            ['mysql', 'DESC', 'LAST', 'created_at IS NULL ASC, created_at DESC'],
        ];
    }

    public function testUnknownDialectFailsClosed(): void
    {
        $this->expectException(\RuntimeException::class);
        Runtime::orderByNulls('c', 'ASC', 'FIRST', 'oracle');
    }
}
