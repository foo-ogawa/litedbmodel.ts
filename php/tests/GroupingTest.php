<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime\Tests;

use LiteDbModel\Runtime\Grouping;
use PHPUnit\Framework\TestCase;

/**
 * Unit tests for the SHARED relation-grouping CORE (#141), asserting the PHP port is
 * behaviour-identical to the TS SSoT `src/scp/grouping.ts` (and the Rust port `grouping.rs`):
 * `String(v)` key identity, null/absent-drop, order-preserving dedupe, composite tuple keys, and
 * the per-cardinality attach (hasMany list / [] when none, single first-child / null).
 */
final class GroupingTest extends TestCase
{
    /** Build a `\stdClass` record row (the record type the runtime uses). */
    private static function row(array $pairs): \stdClass
    {
        return (object) $pairs;
    }

    public function testKeyIdentityMirrorsJsString(): void
    {
        // whole float → integer text (a scanned INT column arrives as a whole float), int/string
        // verbatim, bool → 'true'/'false', fractional float its string form, tuple space-joined.
        self::assertSame('1', Grouping::keyIdentity([1.0]));
        self::assertSame('2', Grouping::keyIdentity([2]));
        self::assertSame('x', Grouping::keyIdentity(['x']));
        self::assertSame('true', Grouping::keyIdentity([true]));
        self::assertSame('false', Grouping::keyIdentity([false]));
        self::assertSame('1.5', Grouping::keyIdentity([1.5]));
        self::assertSame('1 a', Grouping::keyIdentity([1, 'a']));
    }

    public function testDedupeDropsNullAndDedupesPreservingOrder(): void
    {
        $rows = [
            self::row(['id' => 2]),
            self::row(['id' => 1]),
            self::row(['id' => 2]),          // dup
            self::row(['id' => null]),       // dropped (null)
            self::row(['other' => 9]),       // dropped (absent id)
        ];
        $keys = Grouping::dedupeKeyTuples($rows, ['id']);
        $flat = array_map(static fn (array $t) => $t[0], $keys);
        self::assertSame([2, 1], $flat); // insertion order, deduped, null/absent dropped
    }

    public function testDedupeCompositeTuple(): void
    {
        $rows = [
            self::row(['t' => 1, 'u' => 9]),
            self::row(['t' => 1, 'u' => 9]),    // dup tuple
            self::row(['t' => 1, 'u' => 8]),
            self::row(['t' => 1, 'u' => null]), // dropped (partial null)
        ];
        $keys = Grouping::dedupeKeyTuples($rows, ['t', 'u']);
        self::assertCount(2, $keys);
        self::assertSame('1 9', Grouping::keyIdentity($keys[0]));
        self::assertSame('1 8', Grouping::keyIdentity($keys[1]));
    }

    public function testGroupAndAttachHasMany(): void
    {
        $parents = [self::row(['id' => 1]), self::row(['id' => 2])];
        $children = [
            self::row(['author_id' => 1, 't' => 'a']),
            self::row(['author_id' => 1, 't' => 'b']),
            self::row(['author_id' => 2, 't' => 'c']),
            self::row(['author_id' => null, 't' => 'x']), // dropped (null fk)
        ];
        $byKey = Grouping::groupByKey($children, ['author_id']);

        // parent 1 → two children in input order
        $a1 = Grouping::attachToParent($parents[0], ['id'], $byKey, false);
        self::assertIsArray($a1);
        self::assertCount(2, $a1);
        self::assertSame('a', $a1[0]->t);
        self::assertSame('b', $a1[1]->t);

        // parent 2 → one child
        $a2 = Grouping::attachToParent($parents[1], ['id'], $byKey, false);
        self::assertIsArray($a2);
        self::assertCount(1, $a2);

        // a parent with no matches → empty list (not null)
        $a3 = Grouping::attachToParent(self::row(['id' => 3]), ['id'], $byKey, false);
        self::assertSame([], $a3);
    }

    public function testAttachSingleReturnsFirstOrNull(): void
    {
        $children = [
            self::row(['post_id' => 5, 'b' => 'first']),
            self::row(['post_id' => 5, 'b' => 'second']),
        ];
        $byKey = Grouping::groupByKey($children, ['post_id']);

        // single → the FIRST matching child (input order)
        $one = Grouping::attachToParent(self::row(['id' => 5]), ['id'], $byKey, true);
        self::assertInstanceOf(\stdClass::class, $one);
        self::assertSame('first', $one->b);

        // single, no match → null
        $none = Grouping::attachToParent(self::row(['id' => 6]), ['id'], $byKey, true);
        self::assertNull($none);
    }

    public function testAttachNullParentKeyMatchesNothing(): void
    {
        $byKey = Grouping::groupByKey([self::row(['fk' => 1, 'v' => 'a'])], ['fk']);
        // a null/absent parent key → hasMany []; single null (never buckets under "null").
        self::assertSame([], Grouping::attachToParent(self::row(['id' => null]), ['id'], $byKey, false));
        self::assertNull(Grouping::attachToParent(self::row(['other' => 1]), ['id'], $byKey, true));
    }
}
