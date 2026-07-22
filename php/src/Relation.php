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
     * The deduped, non-null parent-key TUPLES (insertion order preserved). Thin delegator to the
     * shared grouping core ({@link Grouping::dedupeKeyTuples}) — the SSoT for the drop/dedupe/tuple
     * semantics (no duplicated grouping logic).
     *
     * @param list<\stdClass> $parents
     * @param list<string> $keyCols
     * @return list<list<mixed>>
     */
    public static function dedupeKeys(array $parents, array $keyCols): array
    {
        return Grouping::dedupeKeyTuples($parents, $keyCols);
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
    public static function runRelationOp(\stdClass $op, array $parents, \PDO|ExecutionContext $db): array
    {
        $ctx = Context::of($db);
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
        // The central READ seam (§2): the ONE driver contact for the relation batch. Byte-identical to
        // the pre-seam `$db->prepare($sql)->execute(bindKeys)->fetchAll(OBJ)`.
        $rows = execute($ctx, $sql, self::bindKeys($op, $keys));
        // Hard-limit runaway guard (Phase E-2, epic #74; v1 `_selectForRelation`): POST-fetch, if the
        // batch TOTAL exceeds the baked cap, throw with the EXACT count (the batch is fetched in full,
        // no N+1). ⚠️ field mapping mirrors the TS reference: `model` = the relation TARGET TABLE,
        // `relation` = the relation NAME. ABSENT `op->hardLimit` ⇒ NO check (disabled / an intrinsic
        // per-parent-`limit` relation whose fanout is already bounded). Thrown BEFORE grouping so an
        // over-cap read never assembles an unbounded result set. Cap read from the artifact ONLY.
        $rowList = is_array($rows) ? $rows : [];
        if (isset($op->hardLimit)) {
            // The relation-context arm of the shared runaway check (SSoT) — the SAME `count > limit ⇒
            // throw` primitive, so the comparison + error assembly live in ONE place (mirror of
            // python relation.py). ABSENT `op->hardLimit` ⇒ NO check.
            LimitExceededError::check(
                (int) $op->hardLimit,
                count($rowList),
                'relation',
                isset($op->targetTable) ? (string) $op->targetTable : null,
                (string) $op->name,
            );
        }
        // Group the child rows by target-key identity via the shared core (a null/absent target key
        // drops the child — the SSoT drop semantics).
        return Grouping::groupByKey($rowList, $tCols);
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
        // Delegate to the shared grouping core; cardinality maps onto its `single` flag (hasMany →
        // list, belongsTo/hasOne → single or null). No duplicated grouping logic.
        $single = (string) $op->kind !== 'hasMany';
        return Grouping::attachToParent($parent, self::parentKeyCols($op), $batch, $single);
    }

    /**
     * The driver a relation runs against: its tagged cross-DB connection, else the primary $db.
     * CROSS-DB (V0 R1): a relation whose op carries a `connection` tag (its target model lives in a
     * DIFFERENT DB — v1 LazyRelation.ts:236) routes to $connections[tag]. Loud failure when the tag
     * has no registered driver (a real wiring bug — never a silent same-DB fallback that would run
     * the target's query on the wrong DB). Untagged relations use the primary $db.
     *
     * @param array<string,ExecutionContext> $connections
     */
    private static function driverForOp(\stdClass $op, ExecutionContext $ctx, array $connections): ExecutionContext
    {
        $tag = $op->connection ?? null;
        if ($tag === null) {
            return $ctx;
        }
        if (!isset($connections[$tag])) {
            throw new \RuntimeException(
                "cross-DB relation '" . ($op->name ?? '?') . "': no driver registered for connection "
                . "'{$tag}' (pass it in readBundle connections)"
            );
        }
        return $connections[$tag];
    }

    /**
     * Run a READ bundle's primary row list, then batch-load + hydrate the selected relations onto
     * each parent (port of the TS readBundle typed-object surface, declarative-select path). The
     * primary read output must be a bare row list; each named relation in $withNames is batch-
     * prefetched ONCE over the whole page (staged, no N+1) via the SAME runRelationOp and attached
     * onto each parent as an own property.
     *
     * CROSS-DB (V0 R1): a relation op carrying a `connection` tag is batched against
     * $connections[tag] (its target model's DB) instead of the primary $db; untagged relations
     * ignore $connections. Pass an empty array for a single-DB read.
     *
     * @param array<string,mixed> $input
     * @param list<string> $withNames
     * @param array<string,\PDO|ExecutionContext> $connections
     * @return list<\stdClass>
     */
    public static function readBundle(\stdClass $bundle, array $input, \PDO|ExecutionContext $db, array $withNames, array $connections = []): array
    {
        $ctx = Context::of($db);
        $connCtx = array_map(static fn (\PDO|ExecutionContext $c): ExecutionContext => Context::of($c), $connections);
        $out = Runtime::executeBundle($bundle, $input, $ctx);
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
            self::hydrateRelation($relations->{$name}, $rows, $ctx, $connCtx, $name);
        }
        return $rows;
    }

    /**
     * Hydrate ONE relation edge over $parents (ONE batched query, N+1-free), then RECURSE into
     * $op->childRelations — the batched-map-over-batched-map chain the native codegen path lowers,
     * reproduced for the runtime/ir-exec path.
     *
     * One edge = one query, INDEPENDENT of the parent count: runRelationOp dedupes the parent keys and
     * fetches ALL children with ONE `WHERE fk IN (…)` batch, then the grouping SSoT nests them onto each
     * parent via distributeToParent. A nested level batches over the FLATTENED child rows fetched here —
     * the EXACT objects attached to the parents (PHP object handles), so grandchildren hydrate in place
     * (users→posts→comments = 3 queries, not 1 + N + N·M). No new mechanism: every level runs the SAME
     * runRelationOp + grouping core.
     *
     * @param list<\stdClass>              $parents
     * @param array<string,ExecutionContext> $connCtx
     */
    private static function hydrateRelation(\stdClass $op, array $parents, ExecutionContext $ctx, array $connCtx, string $attachName): void
    {
        $relCtx = self::driverForOp($op, $ctx, $connCtx);
        $batch = self::runRelationOp($op, $parents, $relCtx);
        foreach ($parents as $p) {
            $p->{$attachName} = self::distributeToParent($op, $p, $batch);
        }
        $childOps = $op->childRelations ?? null;
        if (is_array($childOps) && $childOps !== []) {
            // The flattened child rows (each child appears ONCE, keyed by its target tuple) = the next
            // level's parent set. Empty ⇒ no grandchild query (short-circuit, still N+1-free).
            $childRows = [];
            foreach ($batch as $group) {
                foreach ($group as $child) {
                    $childRows[] = $child;
                }
            }
            if ($childRows !== []) {
                foreach ($childOps as $childOp) {
                    self::hydrateRelation($childOp, $childRows, $ctx, $connCtx, (string) $childOp->name);
                }
            }
        }
    }
}
