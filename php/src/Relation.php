<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — read-relation batch EXECUTION (PHP port of src/scp/relation.ts, #43).
 *
 * Byte-for-byte port of the TS reference relation runtime: the STATIC pre-compiled batch op
 * (`bundle->relations->{name}` — pure JSON) is EXECUTED, never regenerated. A RelationOp carries
 * the batched child SELECT text with ONE `?` for the deduped-key array param; the runtime dedupes
 * the parent keys, resolves the deferred PG array cast from the REAL keys, renders `?`→`$N`, short-
 * circuits an empty key set (NO query), runs the batch, groups the child rows by target key, and
 * distributes them onto the parents per cardinality (hasMany → list, belongsTo/hasOne → single or
 * null). The SAME runRelationOp / distributeToParent / dedupeKeys the TS eager path uses.
 *
 * The PG array binds as the `{…}` array-literal text at the read seam (StaticBundle::pgArrayLiteral),
 * matching the existing #46 IN-list binding — the `::int[]` cast is resolved from the real keys, so
 * PG receives `= ANY($1::int[])`. MySQL/SQLite bind the JSON-encoded array string. Grouped-then-
 * distributed by key → deterministic regardless of #40 sibling-relation completion order.
 */
final class Relation
{
    private const PG_ARRAY_CAST_TOKEN = '@@PG_ARRAY_CAST@@';

    /**
     * Mirror TS `String(v)` for the key-identity used by dedupe + grouping (bool → 'true'/'false',
     * a numeric key prints without a trailing `.0` — a scanned int column is a PHP int/string).
     */
    private static function stringifyKey(mixed $v): string
    {
        if (is_bool($v)) {
            return $v ? 'true' : 'false';
        }
        if (is_float($v)) {
            // A whole float prints as an integer (JS String(7) === "7").
            if ($v === floor($v) && is_finite($v)) {
                return (string) (int) $v;
            }
            return (string) $v;
        }
        return (string) $v;
    }

    /**
     * The deduped, non-null parent-key values (insertion order preserved — deterministic). A
     * byte-for-byte port of the TS dedupeKeys (skip null, dedupe on String(v), keep first-seen order).
     *
     * @param list<\stdClass> $parents
     * @return list<mixed>
     */
    public static function dedupeKeys(array $parents, string $parentKey): array
    {
        $seen = [];
        $out = [];
        foreach ($parents as $p) {
            $v = $p->{$parentKey} ?? null;
            if ($v === null) {
                continue;
            }
            $s = self::stringifyKey($v);
            if (isset($seen[$s])) {
                continue;
            }
            $seen[$s] = true;
            $out[] = $v;
        }
        return $out;
    }

    /**
     * Bind the deduped key set to the op's single array param per dialect (mirrors TS bindKeys):
     * PG → the `{…}` array-literal text PDO binds to `= ANY($1::t[])`; MySQL/SQLite → the JSON-
     * encoded array string (server-side json_each/JSON_TABLE expansion). Compact JSON matches the
     * TS JSON.stringify byte form.
     *
     * @param list<mixed> $keys
     */
    private static function bindKeys(\stdClass $op, array $keys): string
    {
        if ((string) $op->dialect === 'postgres') {
            return StaticBundle::pgArrayLiteral(array_values($keys));
        }
        return json_encode(array_values($keys), JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }

    /**
     * Run ONE relation batch op for a set of parent rows (byte-for-byte port of TS runRelationOp).
     * Dedup the parent keys, resolve the deferred PG array cast from the REAL keys BEFORE the
     * `?`→`$N` render (PG only), render placeholders; on a NON-empty key set execute the batch
     * binding the keys as the SINGLE array param and group the child rows by target key into
     * ['<targetKey>' => [rows]]. An EMPTY key set issues NO query (the correct empty-set behaviour).
     *
     * @param list<\stdClass> $parents
     * @return array<string, list<\stdClass>>
     */
    public static function runRelationOp(\stdClass $op, array $parents, \PDO $db): array
    {
        $parentKey = (string) $op->parentKey;
        $targetKey = (string) $op->targetKey;
        $dialect = (string) $op->dialect;
        $keys = self::dedupeKeys($parents, $parentKey);
        $batch = [];
        $sql = (string) $op->sql;
        if ($dialect === 'postgres') {
            $sql = StaticBundle::resolvePgArrayCast($sql, $keys);
        }
        $sql = StaticBundle::renderPlaceholders($sql, $dialect);
        if (count($keys) === 0) {
            return $batch;
        }
        $stmt = $db->prepare($sql);
        $stmt->execute([self::bindKeys($op, $keys)]);
        $rows = $stmt->fetchAll(\PDO::FETCH_OBJ);
        foreach (is_array($rows) ? $rows : [] as $row) {
            $k = self::stringifyKey($row->{$targetKey} ?? null);
            $batch[$k][] = $row;
        }
        return $batch;
    }

    /**
     * Distribute a resolved batch onto ONE parent per cardinality (port of TS distributeToParent):
     * hasMany → the child list ([] when none); belongsTo/hasOne → the single child (or null). Keyed
     * by String(parent[parentKey]).
     *
     * @param array<string, list<\stdClass>> $batch
     * @return list<\stdClass>|\stdClass|null
     */
    public static function distributeToParent(\stdClass $op, \stdClass $parent, array $batch): mixed
    {
        $key = $parent->{(string) $op->parentKey} ?? null;
        $rows = $key === null ? null : ($batch[self::stringifyKey($key)] ?? null);
        if ((string) $op->kind === 'hasMany') {
            return $rows ?? [];
        }
        return ($rows !== null && count($rows) > 0) ? $rows[0] : null;
    }

    /**
     * Run a READ bundle's primary row list, then batch-load + hydrate the selected relations onto
     * each parent (port of the TS readBundle typed-object surface, declarative-select path). The
     * primary read output must be a bare row list; each named relation in $withNames is batch-
     * prefetched ONCE over the whole page (staged, no N+1) via the SAME runRelationOp and attached
     * onto each parent as an own property.
     *
     * @param array<string,mixed> $input
     * @param list<string> $withNames
     * @return list<\stdClass>
     */
    public static function readBundle(\stdClass $bundle, array $input, \PDO $db, array $withNames): array
    {
        $out = Runtime::executeBundle($bundle, $input, $db);
        if (!is_array($out) || !array_is_list($out)) {
            throw new \RuntimeException(
                'scp read: the read behavior output is not a row list; the typed-object read surface '
                . 'expects a Select-shaped output'
            );
        }
        /** @var list<\stdClass> $rows */
        $rows = $out;
        $relations = $bundle->relations ?? null;
        foreach ($withNames as $name) {
            if (!($relations instanceof \stdClass) || !property_exists($relations, $name)) {
                throw new \RuntimeException("declarative select: relation '{$name}' is not declared on this model");
            }
            $op = $relations->{$name};
            $batch = self::runRelationOp($op, $rows, $db);
            foreach ($rows as $o) {
                $o->{$name} = self::distributeToParent($op, $o, $batch);
            }
        }
        return $rows;
    }
}
