<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

use LiteDbModel\Runtime\BehaviorContracts\ExprEval;

/**
 * litedbmodel v2 SCP — fragment-tree render + param assembly (PHP port of src/scp/render.ts, WS7d #33).
 *
 * The NORMATIVE render reference ported BYTE-FOR-BYTE from the TS `renderOperation`
 * (docs/proposal/sql-dynamic-expansion-spec.md). Given a §8 `CompiledOperation` (as a stdClass
 * decoded from the bundle JSON via json_decode(.., false)) and a bound input scope (assoc array),
 * it deterministically produces the final SQL text + flat params, semantics-identical to TS:
 *
 *   §2 SKIP → fragment existence (`when` truthy/present; absent = no SQL, no params)
 *   §3 empty-WHERE degeneration (no present fragment ⇒ ` WHERE ` keyword collapses)
 *   §4 AND/OR structure + parenthesization
 *   §5 IN-list array expansion (`(?)` → `(?, ?, …)`; empty ⇒ `1 = 0`)
 *   §6 param order = SQL text order (pre-`{where}` statics, fragment params, post statics)
 *   §8 dialect final placeholder pass (`?`→`$N` for Postgres, once over the assembled text)
 *
 * All Expression-IR value evaluation (param slots + SKIP `when` guards) is delegated to the
 * VENDORED behavior-contracts `ExprEval::evaluate` — the runtime re-implements NO expr eval.
 */
final class Render
{
    /** The literal `{where}` splice marker inside `CompiledOperation.sql` (ir.ts WHERE_SLOT). */
    public const WHERE_SLOT = '{where}';

    /**
     * Render a §8 compiled operation (stdClass) against a bound scope for a dialect.
     *
     * @param \stdClass $op    the §8 CompiledOperation (json_decode(.., false) shape).
     * @param array<string,mixed> $scope the bound input scope (bc flat scope).
     * @return array{sql:string, params:list<mixed>} final SQL + flat params (1:1 with `?`/`$N`).
     */
    public static function renderOperation(\stdClass $op, array $scope, Dialect $dialect): array
    {
        $params = [];
        $sqlText = (string) ($op->sql ?? '');
        $markerIdx = strpos($sqlText, self::WHERE_SLOT);

        if ($markerIdx === false) {
            // No dynamic WHERE: all params are static, in position order.
            foreach (self::opParams($op) as $slot) {
                $params[] = ExprEval::evaluate($slot, $scope);
            }
            return ['sql' => $dialect->finalizePlaceholders($sqlText), 'params' => $params];
        }

        $before = substr($sqlText, 0, $markerIdx);
        $after = substr($sqlText, $markerIdx + strlen(self::WHERE_SLOT));

        // Static params are partitioned by whether their `?` sits before or after the marker.
        $beforeQ = self::countPlaceholders($before);
        $allStatic = self::opParams($op);
        $preStatics = array_slice($allStatic, 0, $beforeQ);
        $postStatics = array_slice($allStatic, $beforeQ);

        foreach ($preStatics as $slot) {
            $params[] = ExprEval::evaluate($slot, $scope);
        }

        $whereSql = '';
        $where = $op->where ?? null;
        if ($where instanceof \stdClass) {
            $body = self::renderTree($where, $scope, $params);
            if ($body !== '') {
                $whereSql = ' WHERE ' . $body; // degeneration §3: drop keyword when empty
            }
        }

        foreach ($postStatics as $slot) {
            $params[] = ExprEval::evaluate($slot, $scope);
        }

        return [
            'sql' => $dialect->finalizePlaceholders($before . $whereSql . $after),
            'params' => $params,
        ];
    }

    /** The op's static param slots as a plain list (json array from the bundle). @return list<mixed> */
    private static function opParams(\stdClass $op): array
    {
        $p = $op->params ?? [];
        return is_array($p) ? array_values($p) : [];
    }

    /**
     * A fragment tree carries a `connector`; a fragment carries `sql` (render.ts `isTree`).
     */
    private static function isTree(\stdClass $node): bool
    {
        return property_exists($node, 'connector');
    }

    /**
     * SKIP existence rule (spec §2): present iff `always === true`, or `when` evaluates to a
     * PRESENT binding (`null`/`false` absent; everything else present). Fail-closed: neither
     * `always` nor `when` ⇒ absent. Mirrors render.ts `fragmentPresent`.
     *
     * @param array<string,mixed> $scope
     */
    private static function fragmentPresent(\stdClass $f, array $scope): bool
    {
        if (($f->always ?? null) === true) {
            return true;
        }
        if (!property_exists($f, 'when')) {
            return false; // fail-closed: neither always nor when
        }
        $v = ExprEval::evaluate($f->when, $scope);
        return $v !== null && $v !== false;
    }

    /**
     * Render one leaf fragment's SQL + params (render.ts `renderFragment`). Handles IN-list
     * expansion (spec §5): the `expand` slot is an array; its `(?)` becomes `(?, ?, …)` and each
     * element is pushed as its own param; empty array degenerates the whole fragment to `1 = 0`.
     *
     * @param array<string,mixed> $scope
     * @param list<mixed> $params accumulator (by ref)
     */
    private static function renderFragment(\stdClass $f, array $scope, array &$params): string
    {
        $slots = is_array($f->params ?? null) ? array_values($f->params) : [];
        $expand = property_exists($f, 'expand') ? $f->expand : null;
        $fSql = (string) ($f->sql ?? '');

        if ($expand === null) {
            foreach ($slots as $slot) {
                $params[] = ExprEval::evaluate($slot, $scope);
            }
            return $fSql;
        }

        // IN-list expansion. Evaluate all slots; the `expand` slot must be an array.
        $sql = $fSql;
        $n = count($slots);
        for ($i = 0; $i < $n; $i++) {
            $v = ExprEval::evaluate($slots[$i], $scope);
            if ($i === $expand) {
                if (!is_array($v)) {
                    $got = $v === null ? 'null' : gettype($v);
                    throw new \RuntimeException(
                        "IN-list expansion slot {$i} did not bind to an array (got {$got})"
                    );
                }
                if (count($v) === 0) {
                    // Empty-array degeneration (spec §5): `col IN (?)` collapses to `1 = 0`;
                    // NO params pushed for this slot (byte-identical to v1's empty-IN handling).
                    $sql = '1 = 0';
                } else {
                    $placeholders = implode(', ', array_fill(0, count($v), '?'));
                    $sql = self::replaceFirst($sql, '(?)', '(' . $placeholders . ')');
                    foreach ($v as $el) {
                        $params[] = $el;
                    }
                }
            } else {
                $params[] = $v;
            }
        }
        return $sql;
    }

    /**
     * Render a fragment tree into a WHERE clause body (no leading ` WHERE `). Present fragments
     * are joined by ` <connector> `; a nested tree is parenthesized (spec §4). Empty string when
     * NO fragment is present (§3). Mirrors render.ts `renderTree`.
     *
     * @param array<string,mixed> $scope
     * @param list<mixed> $params accumulator (by ref)
     */
    private static function renderTree(\stdClass $tree, array $scope, array &$params): string
    {
        $parts = [];
        $fragments = is_array($tree->fragments ?? null) ? $tree->fragments : [];
        foreach ($fragments as $node) {
            if (!($node instanceof \stdClass)) {
                continue;
            }
            if (self::isTree($node)) {
                $inner = self::renderTree($node, $scope, $params);
                if ($inner !== '') {
                    $parts[] = '(' . $inner . ')';
                }
            } elseif (self::fragmentPresent($node, $scope)) {
                $parts[] = self::renderFragment($node, $scope, $params);
            }
        }
        if (count($parts) === 0) {
            return '';
        }
        $connector = (string) ($tree->connector ?? 'AND');
        return implode(' ' . $connector . ' ', $parts);
    }

    /** Count `?` placeholders in a static SQL segment (render.ts `countPlaceholders`). */
    private static function countPlaceholders(string $sql): int
    {
        return substr_count($sql, '?');
    }

    /** Replace only the FIRST occurrence of `$needle` in `$haystack` (TS String.replace semantics). */
    private static function replaceFirst(string $haystack, string $needle, string $replace): string
    {
        $pos = strpos($haystack, $needle);
        if ($pos === false) {
            return $haystack;
        }
        return substr($haystack, 0, $pos) . $replace . substr($haystack, $pos + strlen($needle));
    }
}
