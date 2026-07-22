<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — the op-INDEPENDENT leaf transport (#141), PHP port of `src/scp/leaves.ts`
 * and the twin of the python `litedbmodel_runtime.leaves.make_handlers`.
 *
 * The three op-agnostic (NOT per-op) leaves the bc PHP emitter's ir-exec runner
 * (`Behavior::runBehavior`) calls by catalog name via boundary injection (`bind($handlers)`). Each is
 * a bc handler (`fn(array $ports, array $ctx): array` returning `['ok' => Value] | ['error' => str]`)
 * — the SAME contract the rust/go typed-native runners call positionally, reproduced for the PHP
 * literal (ir-exec) path (epic #123: ts/go/rust = native de-box; py/php = literal). PHP's native value
 * model is the plain `\stdClass` record (the runtime `execute` seam returns `PDO::FETCH_OBJ` rows), so
 * there is NO WireValue conversion — the wire IS the `\stdClass`.
 *
 *   - `executeSQL` — the SOLE SQL transport: render `?` → dialect placeholders, bind params (an array
 *     param — a relation key set from `pluck` or a batch record set — rides per dialect: sqlite/mysql
 *     JSON-encode it for `json_each`/`JSON_TABLE`, postgres binds the array as-is), and run it through
 *     the runtime's central {@see execute()} / {@see run()} seam on the bound context — the ONLY driver
 *     contact. A non-returning write returns a one-row `[{changes, lastInsertRowid}]` summary so the
 *     leaf output shape is uniform (a list of rows).
 *   - `pluck` — rows + the ordered key-column TUPLE → the deduped, non-null batch key set (single-key →
 *     a flat scalar array; composite → an array-of-tuples). Delegates the dedupe to the shared grouping
 *     core ({@see Grouping::dedupeKeyTuples}) — the SAME SSoT the runtime relation path uses.
 *   - `group` — parents + flat children → each parent with its children nested under `into` per
 *     cardinality. Delegates to the shared grouping core ({@see Grouping::groupByKey} /
 *     {@see Grouping::attachToParent}) — the SAME SSoT, no duplicated grouping.
 *
 * The leaf is injected context-bound (a closure over the {@see ExecutionContext} + dialect) rather than
 * resolving an ambient driver: the bc PHP boundary is `bind($handlers)`, so the transport is handed in
 * directly. `executeSQL` resolves the AMBIENT tx-scoped ctx ({@see currentContext()}) first so every
 * statement inside a `withTransaction` scope runs on the tx-OWNED connection (the tx boundary is the
 * runtime's BEGIN/COMMIT/ROLLBACK, never baked into the generated runner); outside a tx it falls back
 * to the bound ctx.
 */
final class Leaves
{
    /**
     * Bind a leaf's resolved param list for the driver per dialect (mirror of python `_bind_params` /
     * the rust driver `WireValue` → param encoding). An array param (a relation key set from `pluck` or
     * a batch record set) is server-side-expanded: sqlite/mysql JSON-encode it as ONE scalar string
     * (`json_each`/`JSON_TABLE`); postgres binds the array as-is (native `= ANY($1)` / `unnest`). A
     * scalar param binds unchanged.
     *
     * @param list<mixed> $params
     * @return list<mixed>
     */
    private static function bindParams(array $params, string $dialect): array
    {
        if ($dialect === 'postgres') {
            return array_values($params);
        }
        return array_map(
            static fn ($p) => is_array($p)
                ? json_encode($p, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE)
                : $p,
            $params,
        );
    }

    /**
     * The op-agnostic leaf transport handlers (`executeSQL`/`pluck`/`group`), bound to a driver (a raw
     * `\PDO`) or an {@see ExecutionContext} + its `$dialect`, ready to inject into a bc-generated PHP
     * module's `bind($handlers)`. Every SQL access funnels through the central {@see execute()} /
     * {@see run()} seam over the bound ctx — the SAME seam the runtime read/relation path uses
     * (middleware-visible, N+1-free).
     *
     * @return array{executeSQL: callable, pluck: callable, group: callable}
     */
    public static function makeHandlers(\PDO|ExecutionContext $driverOrCtx, string $dialect): array
    {
        $ctx = Context::of($driverOrCtx);

        $executeSQL = static function (array $ports, array $_ctx) use ($ctx, $dialect): array {
            // Resolve the AMBIENT tx-scoped ctx when this leaf runs inside a `withTransaction` scope
            // (the combinator pins it), so every statement resolves the tx-OWNED connection — the tx
            // boundary is the runtime's, not baked into the generated runner. Outside a tx,
            // `currentContext()` is null ⇒ the bound ctx.
            $active = currentContext() ?? $ctx;
            $sql = StaticBundle::renderPlaceholders($ports['sql'], $dialect);
            $params = self::bindParams($ports['params'], $dialect);
            try {
                if (($ports['write'] ?? false) && !($ports['returning'] ?? false)) {
                    $info = run($active, $sql, $params, StatementIntent::write());
                    // The affected-write summary row (uniform list output shape — TS `writeSummary`).
                    return ['ok' => [(object) ['changes' => $info->changes, 'lastInsertRowid' => $info->lastInsertRowid]]];
                }
                return ['ok' => execute($active, $sql, $params, StatementIntent::read())];
            } catch (SqlFailure $e) {
                return ['error' => $e->getMessage()];
            }
        };

        $pluck = static function (array $ports, array $_ctx): array {
            /** @var list<string> $col */
            $col = $ports['col'];
            $tuples = Grouping::dedupeKeyTuples($ports['rows'], $col);
            // single-key → a flat scalar key array (json_each scalar `value`); composite → an
            // array-of-tuples (json_each per-ordinal `$[i]`) — the SAME shape `Relation` binds.
            $keys = count($col) === 1
                ? array_map(static fn (array $t) => $t[0], $tuples)
                : array_map(static fn (array $t) => array_values($t), $tuples);
            return ['ok' => $keys];
        };

        $group = static function (array $ports, array $_ctx): array {
            $into = (string) $ports['into'];
            $single = (bool) $ports['single'];
            /** @var list<string> $pk */
            $pk = $ports['pk'];
            $byKey = Grouping::groupByKey($ports['children'], $ports['fk']);
            // {...par, [into]: nested}: shallow-clone each parent (the input is not mutated — TS spread).
            $out = array_map(static function (\stdClass $par) use ($pk, $into, $byKey, $single): \stdClass {
                $o = clone $par;
                $o->{$into} = Grouping::attachToParent($par, $pk, $byKey, $single);
                return $o;
            }, $ports['parents']);
            return ['ok' => $out];
        };

        return ['executeSQL' => $executeSQL, 'pluck' => $pluck, 'group' => $group];
    }
}
