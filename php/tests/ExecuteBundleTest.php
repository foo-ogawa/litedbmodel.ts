<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\Runtime;
use PHPUnit\Framework\TestCase;

/**
 * End-to-end runtime tests against a REAL in-process PDO SQLite (WS7d, #33). These drive the SAME
 * §8 bundles the frozen corpus uses (loaded from conformance/vectors/*.json) through
 * executeBundle / executeTransactionBundle, plus a couple of hand-built bundles, asserting the
 * runtime consumes the published JSON alone (bc runtime-core + a SQL handler) and reproduces the
 * reference results.
 */
final class ExecuteBundleTest extends TestCase
{
    private static function vectors(string $suite): \stdClass
    {
        $path = dirname(__DIR__, 2) . "/conformance/vectors/{$suite}.json";
        return json_decode((string) file_get_contents($path), false, 512, JSON_THROW_ON_ERROR);
    }

    private static function seed(array $schema): \PDO
    {
        $db = new \PDO('sqlite::memory:');
        $db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $db->setAttribute(\PDO::ATTR_STRINGIFY_FETCHES, false);
        $db->exec('PRAGMA foreign_keys = ON');
        foreach ($schema as $s) {
            $db->exec((string) $s);
        }
        return $db;
    }

    /** @return array<string,mixed> */
    private static function scope(\stdClass $input): array
    {
        return get_object_vars($input);
    }

    public function testExecFeedStatusPresent(): void
    {
        $v = self::vectors('exec')->vectors[0];
        $db = self::seed($v->schema);
        $result = Runtime::executeBundle($v->bundle, self::scope($v->input), $db);

        // Φ output: posts (row list) + authors (per-post author lists via the map).
        $this->assertObjectHasProperty('posts', $result);
        $this->assertObjectHasProperty('authors', $result);
        $this->assertCount(1, $result->posts);         // author_id=7 + status=live + since filter
        $this->assertSame(1, $result->posts[0]->id);
        $this->assertSame('Hello', $result->posts[0]->title);
        // belongsTo author resolved through the surrogate map node.
        $this->assertSame('Ada', $result->authors[0][0]->name);
    }

    public function testExecFeedStatusAbsentDropsSkipFragment(): void
    {
        // Vector 1: status omitted → the SKIP fragment drops, returning BOTH of author 7's posts.
        $v = self::vectors('exec')->vectors[1];
        $db = self::seed($v->schema);
        $result = Runtime::executeBundle($v->bundle, self::scope($v->input), $db);
        $this->assertCount(2, $result->posts); // both 'live' and 'draft' posts of author 7
    }

    public function testTxGateFirstCommit(): void
    {
        $v = self::vectors('tx')->vectors[0];
        $db = self::seed($v->schema);
        $result = Runtime::executeTransactionBundle($v->bundle, self::scope($v->input), $db);

        $this->assertTrue($result['committed']);
        $this->assertSame(['tx_requires_0', 'tx_idem_1', 'tx_unique_2', 'tx_body_3', 'tx_derive_4', 'tx_emit_5'], $result['executed']);
        $this->assertSame(1, $result['entity']['id']);
        // Body write happened; derive incremented the counter; emit wrote the outbox row.
        $posts = $db->query('SELECT id, author_id, title FROM posts ORDER BY id')->fetchAll(\PDO::FETCH_OBJ);
        $this->assertCount(1, $posts);
        $this->assertSame('New Post', $posts[0]->title);
        $users = $db->query('SELECT id, post_count FROM users WHERE id = 7')->fetchAll(\PDO::FETCH_OBJ);
        $this->assertSame(3, $users[0]->post_count); // seeded 2 + derive +1
        $outbox = $db->query('SELECT type, payload FROM outbox')->fetchAll(\PDO::FETCH_OBJ);
        $this->assertSame('PostCreated', $outbox[0]->type);
    }

    public function testTxGateShortCircuitRollsBack(): void
    {
        // Vector 1: author 999 does not exist → the `requires` gate fails FIRST; body never runs.
        $v = self::vectors('tx')->vectors[1];
        $db = self::seed($v->schema);
        $result = Runtime::executeTransactionBundle($v->bundle, self::scope($v->input), $db);

        $this->assertFalse($result['committed']);
        $this->assertSame('requires_absent', $result['shortCircuit']['reason']);
        $this->assertSame('tx_requires_0', $result['shortCircuit']['statementId']);
        $this->assertNull($result['entity']);
        // Gate-first is REAL: only the requires probe executed; ROLLBACK left the DB unchanged.
        $this->assertSame(['tx_requires_0'], $result['executed']);
        $this->assertCount(0, $db->query('SELECT id FROM posts')->fetchAll());
        $this->assertSame(2, (int) $db->query('SELECT post_count FROM users WHERE id = 7')->fetchColumn());
    }

    public function testExecuteBundleFromPublishedJsonAlone(): void
    {
        // Prove self-sufficiency: round-trip the bundle through JSON (no PHP object identity) and
        // execute the re-parsed copy — the runtime consumes ONLY the published artifact.
        $v = self::vectors('exec')->vectors[0];
        $reparsed = json_decode((string) json_encode($v->bundle), false, 512, JSON_THROW_ON_ERROR);
        $db = self::seed($v->schema);
        $result = Runtime::executeBundle($reparsed, self::scope($v->input), $db);
        $this->assertSame(1, $result->posts[0]->id);
    }
}
