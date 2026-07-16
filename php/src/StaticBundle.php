<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

use LiteDbModel\Runtime\BehaviorContracts\ExprEval;

/**
 * litedbmodel v2 SCP — the STATIC, PORTABLE makeSQL bundle RUNTIME (PHP port, epic #43/#45).
 *
 * Byte-for-byte port of the TS `src/scp/makesql/static-bundle.ts` + `makesql.ts` + `handler.ts`
 * runtime halves — the SOLE makeSQL read/render path. It consumes the PRE-COMPILED, portable
 * artifacts the corpus ships (a read `ReadGraph` = `compileBehaviors`' REAL `Select`/map
 * `ComponentGraphIR` + per-node STATIC statement templates keyed by node id; #12 — no surrogate),
 * and EXECUTES them via the NATIVE read-graph walker (owns map / Φ-merge / wiring; NO bc
 * `runBehavior`) + the vendored behavior-contracts PHP port `ExprEval::evaluate` for the deferred
 * value-specs + skip. It re-implements NO generic
 * evaluator and does NO SQL re-derivation — every statement's `sql` is fixed text; the runtime only
 * evaluates its deferred params + skip, resolves the WHERE connector from the present set, assembles
 * + renders placeholders, and binds.
 *
 * A statement template (StaticStatement) is `{sql, params, skip?, whereFragment?}`:
 *   - sql           — complete tuned dialect text (`?` placeholders), value-independent.
 *   - params        — deferred value-specs = closed-set bc Expression IR, 1:1 with the top `?`.
 *   - skip          — optional bc presence expression; truthy ⇒ the whole statement drops.
 *   - whereFragment — a bare predicate body; the runtime prepends ` WHERE `/` AND ` from the
 *                     present set (a skipped earlier fragment never leaves a dangling connector).
 *
 * An IN-list value-spec is the marker `{"__jsonArray": <spec>, "dialect": <d>}`: postgres binds the
 * array as-is (a text[] param); mysql/sqlite JSON-encode it to a single param (server-side
 * expansion). This mirrors the TS `evalSpec`.
 */
final class StaticBundle
{
    // ── makeSQL assembly (port of makesql.ts assembleMakeSQL / composeMakeSQL) ──

    /**
     * Assemble one makeSQL `{sql, params}` → ['sql'=>…, 'params'=>…]: split the literal `sql` on
     * `?` and interleave each concrete param (mirrors TS assembleMakeSQL). Our concrete runtime
     * nodes carry only bound values (nested-makeSQL splicing is compile-time in the corpus text), so
     * this is the value-fill flatten with a placeholder/param arity check.
     *
     * @param array{sql:string, params:list<mixed>} $node
     * @return array{sql:string, params:list<mixed>}
     */
    private static function assemble(array $node): array
    {
        $chunks = explode('?', $node['sql']);
        if (count($chunks) - 1 !== count($node['params'])) {
            throw new \RuntimeException(
                'makeSQL placeholder/param mismatch: ' . (count($chunks) - 1) . " '?' vs "
                . count($node['params']) . ' params in ' . json_encode($node['sql'])
            );
        }
        $sql = $chunks[0];
        $params = [];
        foreach ($node['params'] as $i => $p) {
            $sql .= '?' . $chunks[$i + 1];
            $params[] = $p;
        }
        return ['sql' => $sql, 'params' => $params];
    }

    /**
     * Concatenate the assembled sql + params of every present makeSQL node (mirrors TS composeMakeSQL).
     *
     * @param list<array{sql:string, params:list<mixed>}> $nodes
     * @return array{sql:string, params:list<mixed>}
     */
    private static function compose(array $nodes): array
    {
        $sql = '';
        $params = [];
        foreach ($nodes as $node) {
            $r = self::assemble($node);
            $sql .= $r['sql'];
            foreach ($r['params'] as $p) {
                $params[] = $p;
            }
        }
        return ['sql' => $sql, 'params' => $params];
    }

    // ── Dialect placeholder render (port of handler.ts renderPlaceholders) ──────

    /**
     * Render `?` → the dialect placeholder form: PG `$N` (quote-aware), MySQL/SQLite keep `?`.
     * Byte-for-byte port of the TS renderPlaceholders: a `?` inside a single-quoted string literal
     * is NOT a placeholder.
     */
    public static function renderPlaceholders(string $sql, string $dialectName): string
    {
        if ($dialectName !== 'postgres') {
            return $sql;
        }
        $out = '';
        $index = 0;
        $inString = false;
        $len = strlen($sql);
        for ($i = 0; $i < $len; $i++) {
            $ch = $sql[$i];
            if ($inString) {
                $out .= $ch;
                if ($ch === "'") {
                    $inString = false;
                }
            } elseif ($ch === "'") {
                $out .= $ch;
                $inString = true;
            } elseif ($ch === '?') {
                $index += 1;
                $out .= '$' . $index;
            } else {
                $out .= $ch;
            }
        }
        return $out;
    }

    // ── Deferred value-spec evaluation (port of static-bundle.ts evalSpec) ──────

    /**
     * Evaluate one deferred value-spec against the scope, handling the `__jsonArray` marker:
     * postgres keeps the array as-is (a text[] param); mysql/sqlite JSON-encode it to ONE string
     * param (server-side expansion). Everything else is a plain bc Expression IR value.
     *
     * @param array<string,mixed> $scope
     */
    private static function evalSpec(mixed $spec, array $scope, string $dialectName): mixed
    {
        if ($spec instanceof \stdClass) {
            $props = get_object_vars($spec);
            if (array_key_exists('__jsonArray', $props)) {
                $arr = ExprEval::evaluate($props['__jsonArray'], $scope);
                if (!is_array($arr)) {
                    throw new \RuntimeException('static-bundle: IN-list value-spec did not evaluate to an array');
                }
                $specDialect = isset($props['dialect']) ? (string) $props['dialect'] : '';
                if ($specDialect === 'postgres') {
                    return array_values($arr); // bound as ONE text[] param
                }
                // MySQL/SQLite single-JSON IN-list param. A BOOLEAN element is encoded as `1`/`0`
                // for MySQL (NOT JSON `true`/`false`): MySQL's `JSON_UNQUOTE(v)` yields the STRING
                // `'true'`, which coerces to `0` against a TINYINT(1) — a silent mismatch. `1`/`0`
                // is what v1's `col IN (?)` bound. SQLite's `json_each` coerces JSON booleans
                // natively, so it keeps the plain form.
                $elems = array_values($arr);
                if ($specDialect === 'mysql') {
                    $elems = array_map(fn ($e) => is_bool($e) ? ($e ? 1 : 0) : $e, $elems);
                }
                // Single JSON param — compact form matching the TS JSON.stringify byte shape.
                return json_encode($elems, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
            }
        }
        return ExprEval::evaluate($spec, $scope);
    }

    /**
     * Encode a flat scalar array to the Postgres array-literal text form (`{1,3}` /
     * `{"a","b"}`) PDO can bind to a single `= ANY($1)` placeholder. Elements are quoted +
     * escaped so text/uuid values (and empty `{}`) round-trip; PG coerces each element to the
     * column's element type. Mirrors the JS pg driver's array serialization.
     *
     * @param list<mixed> $arr
     */
    public static function pgArrayLiteral(array $arr): string
    {
        $parts = [];
        foreach ($arr as $e) {
            if ($e === null) {
                $parts[] = 'NULL';
            } elseif (is_bool($e)) {
                $parts[] = $e ? 't' : 'f';
            } elseif (is_int($e) || is_float($e)) {
                $parts[] = (string) $e;
            } else {
                // Quote + escape backslashes and double-quotes (PG array-literal escaping).
                $parts[] = '"' . str_replace(['\\', '"'], ['\\\\', '\\"'], (string) $e) . '"';
            }
        }
        return '{' . implode(',', $parts) . '}';
    }

    // ── Deferred PG array-cast resolution (#46 — mirrors compile-relation.ts) ────

    /** The DEFERRED PG array-cast placeholder, resolved at render from the bound array. */
    private const PG_ARRAY_CAST_TOKEN = '@@PG_ARRAY_CAST@@';

    /**
     * Port of the ORIGINAL inferPgArrayType (v1 LazyRelation): the element type inferred from the
     * sample values (no sqlCast at this schema-less surface). PHP has no bool/int subclass trap.
     *
     * @param list<mixed> $values
     */
    private static function inferPgArrayType(array $values): string
    {
        if (count($values) === 0) {
            return 'text[]';
        }
        $sample = $values[0];
        if (is_bool($sample)) {
            return 'boolean[]';
        }
        if (is_int($sample)) {
            return 'int[]';
        }
        if (is_float($sample)) {
            return 'numeric[]';
        }
        return 'text[]';
    }

    /**
     * Resolve the FIRST unresolved cast token to the element type inferred from $values (mirrors TS
     * resolvePgArrayCast). SQL with no token is unchanged.
     *
     * @param list<mixed> $values
     */
    public static function resolvePgArrayCast(string $sql, array $values): string
    {
        $at = strpos($sql, self::PG_ARRAY_CAST_TOKEN);
        if ($at === false) {
            return $sql;
        }
        return substr($sql, 0, $at) . self::inferPgArrayType($values)
            . substr($sql, $at + strlen(self::PG_ARRAY_CAST_TOKEN));
    }

    // ── Statement-list render (port of static-bundle.ts renderStatements) ───────

    /**
     * Evaluate a list of static statement templates against a scope → final ['sql'=>…, 'params'=>…].
     * Byte-for-byte port of the TS renderStatements: drop skipped statements (skip truthy), resolve
     * each WHERE-fragment's ` WHERE `/` AND ` connector from the present set, resolve any deferred PG
     * array cast from the bound array, compose + render placeholders.
     *
     * @param list<\stdClass> $statements
     * @param array<string,mixed> $scope
     * @return array{sql:string, params:list<mixed>}
     */
    public static function renderStatements(array $statements, string $dialectName, array $scope): array
    {
        $nodes = [];
        $whereSeen = false;
        foreach ($statements as $stmt) {
            if (!($stmt instanceof \stdClass)) {
                continue;
            }
            if (property_exists($stmt, 'skip') && $stmt->skip !== null) {
                $drop = ExprEval::evaluate($stmt->skip, $scope);
                if ($drop !== null && $drop !== false) {
                    continue; // truthy ⇒ drop the whole statement
                }
            }
            $sqlText = (string) ($stmt->sql ?? '');
            if (($stmt->whereFragment ?? null) === true) {
                $sqlText = ($whereSeen ? ' AND ' : ' WHERE ') . $sqlText;
                $whereSeen = true;
            }
            $params = [];
            $specs = is_array($stmt->params ?? null) ? $stmt->params : [];
            foreach ($specs as $spec) {
                $params[] = self::evalSpec($spec, $scope, $dialectName);
            }
            // Resolve any deferred PG array cast (#46) from the bound array param, left-to-right —
            // each postgres __jsonArray param resolves exactly one cast token in order.
            if ($dialectName === 'postgres') {
                foreach ($params as $p) {
                    if (is_array($p)) {
                        if (strpos($sqlText, self::PG_ARRAY_CAST_TOKEN) === false) {
                            break;
                        }
                        $sqlText = self::resolvePgArrayCast($sqlText, $p);
                    }
                }
            }
            $nodes[] = ['sql' => $sqlText, 'params' => $params];
        }
        $assembled = self::compose($nodes);
        return ['sql' => self::renderPlaceholders($assembled['sql'], $dialectName), 'params' => $assembled['params']];
    }

    // ── Input normalization (SSoT-driven — mirrors TS normalizeInput) ──────────

    /**
     * Normalize omitted OPTIONAL heads to present-as-null (absent-key SKIP). Optional = the read
     * graph's component schema-optional ports OR the graph's `optionalHeads` (SKIP-guarded / refOpt).
     *
     * @param array<string,mixed> $input
     * @return array<string,mixed>
     */
    private static function normalizeInput(\stdClass $graph, array $input): array
    {
        $out = $input;
        $component = self::primaryComponent($graph);
        if ($component instanceof \stdClass && ($component->inputPorts ?? null) instanceof \stdClass) {
            foreach (get_object_vars($component->inputPorts) as $port => $schema) {
                $required = ($schema instanceof \stdClass) ? ($schema->required ?? null) : null;
                if ($required !== true && !array_key_exists((string) $port, $out)) {
                    $out[(string) $port] = null;
                }
            }
        }
        $heads = is_array($graph->optionalHeads ?? null) ? $graph->optionalHeads : [];
        foreach ($heads as $head) {
            if (is_string($head) && !array_key_exists($head, $out)) {
                $out[$head] = null;
            }
        }
        return $out;
    }

    /** The surrogate IR's first component (components[0]). */
    private static function primaryComponent(\stdClass $graph): ?\stdClass
    {
        $ir = $graph->ir ?? null;
        if (!($ir instanceof \stdClass) || !is_array($ir->components ?? null) || count($ir->components) === 0) {
            return null;
        }
        $c = $ir->components[0];
        return $c instanceof \stdClass ? $c : null;
    }

    // ── ReadGraph render axis (port of static-bundle.ts renderReadPrimary) ──────

    /**
     * Render the PRIMARY read node's statements of a ReadGraph → dialect SQL + params (the render
     * axis for conformance golden). The primary node is the first body node in the surrogate IR
     * order (map nodes reference it). Optional heads are normalized to present-as-null first.
     *
     * @param array<string,mixed> $input
     * @return array{sql:string, params:list<mixed>}
     */
    public static function renderReadPrimary(\stdClass $graph, array $input): array
    {
        $primaryId = self::primaryNodeId($graph);
        $scope = self::normalizeInput($graph, $input);
        $statements = self::statementsFor($graph, $primaryId);
        return self::renderStatements($statements, (string) $graph->dialect, $scope);
    }

    /** The first body node id that has compiled statements (the SELECT the relations map over). */
    private static function primaryNodeId(\stdClass $graph): string
    {
        $component = self::primaryComponent($graph);
        $byId = $graph->statementsById ?? null;
        if (!($component instanceof \stdClass) || !is_array($component->body ?? null) || !($byId instanceof \stdClass)) {
            throw new \RuntimeException('static-bundle: read graph has no primary node to render');
        }
        foreach ($component->body as $n) {
            if ($n instanceof \stdClass) {
                $id = (string) ($n->id ?? '');
                if ($id !== '' && property_exists($byId, $id)) {
                    return $id;
                }
            }
        }
        throw new \RuntimeException('static-bundle: read graph has no primary node to render');
    }

    /**
     * The static statement templates for a node id (a JSON list of stdClass).
     *
     * @return list<\stdClass>
     */
    private static function statementsFor(\stdClass $graph, string $nodeId): array
    {
        $byId = $graph->statementsById ?? null;
        if ($byId instanceof \stdClass && property_exists($byId, $nodeId)) {
            $stmts = $byId->{$nodeId};
            if (is_array($stmts)) {
                return array_values($stmts);
            }
        }
        return [];
    }

    // ── ReadGraph execution (port of static-bundle.ts executeReadGraph) ─────────

    /**
     * Render node `$nodeId`'s `statementsById` fragments against `$scope` and execute on PDO.
     * The ONE render→execute step of the native walker (#12): SQL text comes from the fragments,
     * only the deferred params + skip are evaluated against the walk scope. Returns the row list.
     *
     * @param array<string,mixed> $scope
     * @return list<mixed>
     */
    private static function renderExecuteNode(\stdClass $graph, string $nodeId, array $scope, ExecutionContext $ctx): array
    {
        $dialectName = (string) $graph->dialect;
        $byId = $graph->statementsById ?? null;
        if (!($byId instanceof \stdClass) || !property_exists($byId, $nodeId)) {
            throw new \RuntimeException("static-bundle: no statements for node '{$nodeId}'");
        }
        $stmts = is_array($byId->{$nodeId}) ? array_values($byId->{$nodeId}) : [];
        $rendered = self::renderStatements($stmts, $dialectName, $scope);
        $params = array_map(static function (mixed $v) use ($dialectName): mixed {
            if (is_bool($v)) {
                return $v ? 1 : 0;
            }
            // A postgres IN-list / relation-batch array param (`= ANY($1)` / `= ANY($1::int[])`):
            // PDO cannot bind a PHP array to a single placeholder, so encode it to the PG array
            // literal text form (`{1,3}`) the server parses. The no-cast `= ANY($1)` still lets PG
            // infer the element type from the column (int / uuid / empty), so this is a pure
            // BINDING adaptation — no SQL-form change (#46).
            if ($dialectName === 'postgres' && is_array($v)) {
                return self::pgArrayLiteral($v);
            }
            return $v;
        }, $rendered['params']);
        try {
            // The central READ seam (§2): ① middleware ② connectionFor ③ execute — the ONLY driver
            // contact. Byte-identical to the pre-seam `$db->prepare()->execute()->fetchAll(OBJ)`.
            return execute($ctx, $rendered['sql'], array_values($params));
        } catch (\PDOException $e) {
            throw SqlFailure::fromPdo($e);
        }
    }

    /**
     * Execute a compiled ReadGraph via the NATIVE walker (#12): NO bc `runBehavior`.
     *
     * Walks `compileBehaviors`' REAL `Select`/`Count`/map node IR in `plan.groups` stage order —
     * computing each node's rows from its `statementsById` fragments (rendered against the walk scope)
     * and committing `nodeId => value` — then evaluates the component `output` Φ expression.
     * litedbmodel owns map iteration / wire binding / Φ output; no `__makeSqlNode`/`__scope` surrogate.
     * The SAME native model the TS/rust/go/python runtimes follow — interpreter-free.
     *
     * @param array<string,mixed> $input
     */
    public static function executeReadGraph(\stdClass $graph, array $input, \PDO|ExecutionContext $db): mixed
    {
        $ctx = Context::of($db);
        $component = $graph->ir->components[0];
        $body = is_array($component->body ?? null) ? $component->body : [];
        $output = $component->output ?? null;
        $normalized = self::normalizeInput($graph, $input);

        /** @var list<array{0:string,1:mixed}> $results */
        $results = [];
        foreach (self::planStages($component, count($body)) as $stage) {
            $base = $normalized;
            foreach ($results as [$nid, $val]) {
                $base[$nid] = $val;
            }
            sort($stage); // ascending body index — deterministic failure precedence
            foreach ($stage as $idx) {
                $node = $body[$idx];
                $val = self::computeReadNode($graph, $node, $base, $ctx);
                $results[] = [(string) $node->id, $val];
                $base[(string) $node->id] = $val;
            }
        }
        $scope = $normalized;
        foreach ($results as [$nid, $val]) {
            $scope[$nid] = $val;
        }
        return ExprEval::evaluate($output, $scope);
    }

    /**
     * The plan stages (`groups`) as body-index lists, or one-node-per-stage in body order.
     *
     * @return list<list<int>>
     */
    private static function planStages(\stdClass $component, int $bodyLen): array
    {
        $plan = $component->plan ?? null;
        if ($plan instanceof \stdClass && is_array($plan->groups ?? null)) {
            $stages = [];
            foreach ($plan->groups as $st) {
                if (is_array($st)) {
                    $stages[] = array_values(array_filter($st, 'is_int'));
                }
            }
            return $stages;
        }
        return array_map(static fn (int $i): array => [$i], range(0, $bodyLen - 1));
    }

    /**
     * Compute ONE read-graph body node's value: a `cond` join, a `map` (per-element render+execute
     * under the `as` binding), or a componentRef (render+execute).
     *
     * @param array<string,mixed> $base
     */
    private static function computeReadNode(\stdClass $graph, \stdClass $node, array $base, ExecutionContext $ctx): mixed
    {
        if (property_exists($node, 'cond')) {
            return ExprEval::evaluate($node->cond, $base);
        }
        $nodeId = (string) ($node->id ?? '');
        if (property_exists($node, 'map')) {
            $m = $node->map;
            $over = ExprEval::evaluate($m->over ?? null, $base);
            if (!is_array($over)) {
                throw new \RuntimeException("static-bundle: map '{$nodeId}': 'over' is not an array");
            }
            $asName = (string) ($m->as ?? '$');
            $out = [];
            foreach ($over as $el) {
                $elemScope = $base;
                $elemScope[$asName] = $el;
                $out[] = self::renderExecuteNode($graph, $nodeId, $elemScope, $ctx);
            }
            return $out;
        }
        return self::renderExecuteNode($graph, $nodeId, $base, $ctx);
    }

    // ── Tx op render (port of tx.ts renderStatement) ───────────────────────────

    /**
     * Render a tx statement's makeSQL op `{sql, params}` against the tx scope: evaluate each deferred
     * Expression-IR param, assemble + render placeholders (the SAME assemble the read path uses).
     *
     * @param array<string,mixed> $scope
     * @return array{sql:string, params:list<mixed>}
     */
    public static function renderTxOp(\stdClass $op, array $scope, string $dialectName): array
    {
        $sqlText = (string) ($op->sql ?? '');
        $specs = is_array($op->params ?? null) ? $op->params : [];
        $concrete = [];
        foreach ($specs as $spec) {
            $concrete[] = ExprEval::evaluate($spec, $scope);
        }
        $assembled = self::assemble(['sql' => $sqlText, 'params' => $concrete]);
        return ['sql' => self::renderPlaceholders($assembled['sql'], $dialectName), 'params' => $assembled['params']];
    }
}
