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
     * The ordered PARENT / CHILD key columns (single-key → 1-element; composite → the tuple, #47 item 1).
     *
     * @return list<string>
     */
    private static function parentKeyCols(\stdClass $op): array
    {
        if (isset($op->parentKeys) && is_array($op->parentKeys)) {
            return array_map('strval', $op->parentKeys);
        }
        return [(string) $op->parentKey];
    }

    /** @return list<string> */
    private static function targetKeyCols(\stdClass $op): array
    {
        if (isset($op->targetKeys) && is_array($op->targetKeys)) {
            return array_map('strval', $op->targetKeys);
        }
        return [(string) $op->targetKey];
    }

    /**
     * The stringified key identity for dedupe/grouping (tuple → space-joined scalars, mirror of TS).
     *
     * @param list<mixed> $values
     */
    private static function keyIdentity(array $values): string
    {
        return implode(' ', array_map([self::class, 'stringifyKey'], $values));
    }

    /**
     * The deduped, non-null parent-key TUPLES (insertion order preserved). Drop a tuple if ANY key
     * column is null; dedupe on the stringified tuple identity. Port of TS dedupeKeys.
     *
     * @param list<\stdClass> $parents
     * @param list<string> $keyCols
     * @return list<list<mixed>>
     */
    public static function dedupeKeys(array $parents, array $keyCols): array
    {
        $seen = [];
        $out = [];
        foreach ($parents as $p) {
            $tuple = [];
            $anyNull = false;
            foreach ($keyCols as $c) {
                $v = $p->{$c} ?? null;
                if ($v === null) {
                    $anyNull = true;
                    break;
                }
                $tuple[] = $v;
            }
            if ($anyNull) {
                continue;
            }
            $s = self::keyIdentity($tuple);
            if (isset($seen[$s])) {
                continue;
            }
            $seen[$s] = true;
            $out[] = $tuple;
        }
        return $out;
    }

    /**
     * Bind the deduped keys to the op's params per dialect + arity (mirrors TS bindKeys). Single-key:
     * PG → ONE `{…}` array-literal param; MySQL/SQLite → ONE JSON scalar-array string. Composite: PG →
     * ONE `{…}` array-literal PER key column (transposed tuples); MySQL/SQLite → ONE JSON
     * array-of-tuples string. Returns the positional param list.
     *
     * @param list<list<mixed>> $tuples
     * @return list<string>
     */
    private static function bindKeys(\stdClass $op, array $tuples): array
    {
        $composite = isset($op->parentKeys);
        if ((string) $op->dialect === 'postgres') {
            $nCols = $composite ? count(self::parentKeyCols($op)) : 1;
            $args = [];
            for ($col = 0; $col < $nCols; $col++) {
                $colArr = array_map(static fn ($t) => $t[$col], $tuples);
                $args[] = StaticBundle::pgArrayLiteral($colArr);
            }
            return $args;
        }
        $payload = $composite
            ? array_map(static fn ($t) => array_values($t), $tuples)
            : array_map(static fn ($t) => $t[0], $tuples);
        return [json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE)];
    }

    /**
     * Run ONE relation batch op for a set of parent rows (byte-for-byte port of TS runRelationOp).
     * Dedup the parent-key tuples, resolve the deferred PG array cast(s) from the REAL keys (one per
     * key column for composite) BEFORE the `?`→`$N` render; on a NON-empty key set execute binding
     * the keys (single array / per-column arrays / JSON tuples) and group the child rows by
     * target-key identity. An EMPTY key set issues NO query.
     *
     * @param list<\stdClass> $parents
     * @return array<string, list<\stdClass>>
     */
    public static function runRelationOp(\stdClass $op, array $parents, \PDO $db): array
    {
        $dialect = (string) $op->dialect;
        $pCols = self::parentKeyCols($op);
        $keys = self::dedupeKeys($parents, $pCols);
        $batch = [];
        $sql = (string) $op->sql;
        if ($dialect === 'postgres') {
            foreach ($pCols as $col => $_) {
                $colVals = array_map(static fn ($t) => $t[$col], $keys);
                $sql = StaticBundle::resolvePgArrayCast($sql, $colVals);
            }
        }
        $sql = StaticBundle::renderPlaceholders($sql, $dialect);
        if (count($keys) === 0) {
            return $batch;
        }
        $tCols = self::targetKeyCols($op);
        $stmt = $db->prepare($sql);
        $stmt->execute(self::bindKeys($op, $keys));
        $rows = $stmt->fetchAll(\PDO::FETCH_OBJ);
        foreach (is_array($rows) ? $rows : [] as $row) {
            $tuple = array_map(static fn ($c) => $row->{$c} ?? null, $tCols);
            $k = self::keyIdentity($tuple);
            $batch[$k][] = $row;
        }
        return $batch;
    }

    /**
     * Distribute a resolved batch onto ONE parent per cardinality (port of TS distributeToParent):
     * hasMany → the child list ([] when none); belongsTo/hasOne → the single child (or null). Keyed
     * by the parent's key-tuple identity.
     *
     * @param array<string, list<\stdClass>> $batch
     * @return list<\stdClass>|\stdClass|null
     */
    public static function distributeToParent(\stdClass $op, \stdClass $parent, array $batch): mixed
    {
        $tuple = [];
        $anyNull = false;
        foreach (self::parentKeyCols($op) as $c) {
            $v = $parent->{$c} ?? null;
            if ($v === null) {
                $anyNull = true;
                break;
            }
            $tuple[] = $v;
        }
        $rows = $anyNull ? null : ($batch[self::keyIdentity($tuple)] ?? null);
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
