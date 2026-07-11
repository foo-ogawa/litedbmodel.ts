<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\Runtime;
use LiteDbModel\Runtime\StaticBundle;
use PHPUnit\Framework\TestCase;

/**
 * Static-makeSQL render-axis unit tests (epic #43/#45).
 *
 * These assert the PHP static-makeSQL render path reproduces the render edge cases directly —
 * SKIP-fragment drop, WHERE/AND connector resolution from the present set, IN-list single-JSON
 * param, LIMIT deferral, and `?`→`$N` for Postgres (quote-aware) — independent of the frozen
 * corpus, so a render regression fails loudly here too. They are the SAME semantics the vector
 * corpus pins byte-for-byte. The CLOSED Expression-IR evaluation is delegated to the vendored
 * behavior-contracts PHP port (`ExprEval::evaluate`).
 */
final class RenderTest extends TestCase
{
    /**
     * Decode static statement templates the way the runtime consumes a bundle
     * (json_decode(.., false) → list<\stdClass>).
     *
     * @param list<array<string,mixed>> $shape
     * @return list<\stdClass>
     */
    private static function stmts(array $shape): array
    {
        return json_decode((string) json_encode($shape), false, 512, JSON_THROW_ON_ERROR);
    }

    /** The canonical Feed read node's static statement templates (SELECT + WHERE + LIMIT). */
    private static function feedStatements(): array
    {
        return self::stmts([
            ['sql' => 'SELECT id, author_id, title, status FROM posts', 'params' => []],
            ['sql' => 'author_id = ?', 'params' => [['ref' => ['author_id']]], 'whereFragment' => true],
            [
                'sql' => 'status = ?',
                'params' => [['ref' => ['status']]],
                'whereFragment' => true,
                'skip' => ['not' => [['ne' => [['refOpt' => ['status']], null]]]],
            ],
            ['sql' => ' ORDER BY id ASC', 'params' => []],
            ['sql' => ' LIMIT ?', 'params' => [['coalesce' => [['refOpt' => ['limit']], 20]]]],
        ]);
    }

    public function testRenderAllFragmentsPresent(): void
    {
        $r = StaticBundle::renderStatements(self::feedStatements(), 'sqlite', ['author_id' => 7, 'status' => 'live', 'limit' => 5]);
        $this->assertSame('SELECT id, author_id, title, status FROM posts WHERE author_id = ? AND status = ? ORDER BY id ASC LIMIT ?', $r['sql']);
        $this->assertSame([7, 'live', 5], $r['params']);
    }

    public function testRenderSkipDropsStatusAndDefaultsLimit(): void
    {
        // status absent (present-as-null) → skip drops the fragment; coalesce defaults the limit.
        $r = StaticBundle::renderStatements(self::feedStatements(), 'sqlite', ['author_id' => 7, 'status' => null, 'limit' => null]);
        $this->assertSame('SELECT id, author_id, title, status FROM posts WHERE author_id = ? ORDER BY id ASC LIMIT ?', $r['sql']);
        $this->assertSame([7, 20], $r['params']);
    }

    public function testRenderPostgresPlaceholderRewrite(): void
    {
        $r = StaticBundle::renderStatements(self::feedStatements(), 'postgres', ['author_id' => 7, 'status' => 'live', 'limit' => 5]);
        $this->assertSame('SELECT id, author_id, title, status FROM posts WHERE author_id = $1 AND status = $2 ORDER BY id ASC LIMIT $3', $r['sql']);
        $this->assertSame([7, 'live', 5], $r['params']);
    }

    public function testRenderInListSingleJsonParamSqlite(): void
    {
        $stmts = self::stmts([
            ['sql' => 'SELECT id FROM posts', 'params' => []],
            [
                'sql' => 'id IN (SELECT value FROM json_each(?))',
                'params' => [['__jsonArray' => ['ref' => ['ids']], 'dialect' => 'sqlite']],
                'whereFragment' => true,
            ],
        ]);
        $r = StaticBundle::renderStatements($stmts, 'sqlite', ['ids' => [1, 2, 3]]);
        $this->assertSame('SELECT id FROM posts WHERE id IN (SELECT value FROM json_each(?))', $r['sql']);
        $this->assertSame(['[1,2,3]'], $r['params']); // single JSON param (server-side expansion)
    }

    public function testRenderInListPostgresBindsArray(): void
    {
        $stmts = self::stmts([
            ['sql' => 'SELECT id FROM posts', 'params' => []],
            [
                'sql' => 'id = ANY(?)',
                'params' => [['__jsonArray' => ['ref' => ['ids']], 'dialect' => 'postgres']],
                'whereFragment' => true,
            ],
        ]);
        $r = StaticBundle::renderStatements($stmts, 'postgres', ['ids' => [1, 2, 3]]);
        $this->assertSame('SELECT id FROM posts WHERE id = ANY($1)', $r['sql']);
        $this->assertSame([[1, 2, 3]], $r['params']); // array bound as ONE text[] param
    }

    public function testPlaceholderRewriteQuoteAware(): void
    {
        // A `?` inside a string literal is NOT a placeholder (mirrors TS renderPlaceholders).
        $this->assertSame("SELECT '?' AS q WHERE a = \$1", StaticBundle::renderPlaceholders("SELECT '?' AS q WHERE a = ?", 'postgres'));
        $this->assertSame('a = ? AND b = ?', StaticBundle::renderPlaceholders('a = ? AND b = ?', 'sqlite'));
    }

    public function testRenderReadPrimaryPicksFirstBodyNode(): void
    {
        $graph = json_decode((string) json_encode([
            'dialect' => 'sqlite',
            'name' => 'Feed',
            'statementsById' => (object) [
                'n0' => [
                    ['sql' => 'SELECT id, author_id, title, status FROM posts', 'params' => []],
                    ['sql' => 'author_id = ?', 'params' => [['ref' => ['author_id']]], 'whereFragment' => true],
                    [
                        'sql' => 'status = ?',
                        'params' => [['ref' => ['status']]],
                        'whereFragment' => true,
                        'skip' => ['not' => [['ne' => [['refOpt' => ['status']], null]]]],
                    ],
                    ['sql' => ' ORDER BY id ASC', 'params' => []],
                    ['sql' => ' LIMIT ?', 'params' => [['coalesce' => [['refOpt' => ['limit']], 20]]]],
                ],
            ],
            'optionalHeads' => ['status', 'limit'],
            'ir' => ['irVersion' => 1, 'exprVersion' => 2, 'components' => [['name' => 'Feed', 'body' => [['id' => 'n0']]]]],
        ]), false, 512, JSON_THROW_ON_ERROR);

        // status + limit omitted → normalized present-as-null → skip drop + coalesce default.
        $r = Runtime::renderReadPrimary($graph, ['author_id' => 7]);
        $this->assertSame('SELECT id, author_id, title, status FROM posts WHERE author_id = ? ORDER BY id ASC LIMIT ?', $r['sql']);
        $this->assertSame([7, 20], $r['params']);
    }

    /**
     * @dataProvider orderByNullsCases
     */
    public function testOrderByNulls(string $dialect, string $dir, string $nulls, string $expected): void
    {
        $this->assertSame($expected, Runtime::orderByNulls('created_at', $dir, $nulls, $dialect));
    }

    /** @return list<array{string,string,string,string}> */
    public static function orderByNullsCases(): array
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
