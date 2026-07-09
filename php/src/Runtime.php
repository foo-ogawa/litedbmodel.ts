<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

use LiteDbModel\Runtime\BehaviorContracts\Behavior;

/**
 * litedbmodel v2 SCP — PHP runtime (WS7d, #33).
 *
 * The PHP leg of the litedbmodel v2 SCP multi-language runtime. It interprets the
 * language-neutral §8 published bundle (`SqlBundle`: sql + fragment tree + closed-set
 * Expression-IR param slots + transaction plan, dialect-tagged) and executes it against a PDO
 * SQL driver, semantics-identical to the TS reference (`src/scp/runtime.ts`).
 *
 * It is a THIN runtime: the generic component-graph execution (`runBehavior` — plan-stage
 * execution / Skip propagation / Policy Kind / map iteration / Φ output assembly) and all
 * Expression-IR evaluation are delegated to the VENDORED behavior-contracts PHP port
 * (`src/BehaviorContracts/`). This runtime adds ONLY the SQL-backend concerns (spec §11):
 * the per-catalog SQL handlers (render → PDO execute → row→object), the input normalization
 * from the bundle's `optionalHeads`, and the gate-first write transaction envelope.
 *
 * ## Value model (byte-identity with the reference)
 *
 * The bundle JSON is decoded with `json_decode($json, false)` so IR nodes are `stdClass` and
 * lists are PHP arrays — exactly what the vendored bc `ExprEval`/`Behavior` expect. SQL rows
 * are returned to bc as `stdClass` so that (a) a map element's `{ref:[$e, col]}` resolves via
 * property access and (b) the Φ `{obj:{…}}` output JSON-encodes to the same object shape as the
 * TS reference. bc evaluates integer params to PHP `int` (PDO binds them directly).
 *
 * ## Dialect / driver seam
 *
 * The conformance bar executes against a REAL in-process PDO SQLite database. The dialect axis
 * (`sqlite`/`postgres`/`mysql`) governs the rendered SQL TEXT (`?`→`$N` for Postgres); the
 * seeded DB is SQLite regardless — so a PG/MySQL-tagged bundle's rendered params still bind
 * positionally, and the EXECUTED result is dialect-invariant (the §10 promise). Live PG/MySQL
 * PDO execution is DEFERRED to a coordinated cross-language docker pass; the handler seam takes
 * any PDO connection, so pgsql/mysql plug in unchanged when that pass runs.
 */
final class Runtime
{
    /** The synthetic port that carries a SQL node's render scope (runtime.ts SCOPE_PORT). */
    private const SCOPE_PORT = '__scope';

    /** Version mirrored from package.json by scripts/sync-versions.mjs (SSoT). */
    public const VERSION = '2.0.0';

    /**
     * Render a §8 CompiledOperation against a scope for a dialect → ['sql'=>…, 'params'=>…].
     *
     * @param \stdClass $operation the §8 CompiledOperation (json_decode(.., false) shape).
     * @param array<string,mixed> $scope
     * @return array{sql:string, params:list<mixed>}
     */
    public static function renderOperation(\stdClass $operation, array $scope, string $dialect): array
    {
        return Render::renderOperation($operation, $scope, Dialect::forName($dialect));
    }

    /** The dialect NULLS-ordering primitive (dialect.ts `orderByNulls`). */
    public static function orderByNulls(string $expr, string $dir, string $nulls, string $dialect): string
    {
        return Dialect::forName($dialect)->orderByNulls($expr, $dir, $nulls);
    }

    /**
     * Execute a §8 read/exec `SqlBundle` end-to-end (runtime.ts `executeBundle`): feed bc
     * `runBehavior` the bundle's surrogate component (plan / map / wire / output orchestration)
     * with SQL handlers that render the bundle's `CompiledOperation`s and run REAL SQL via PDO.
     * Consumes ONLY the serialized bundle + vendored bc runtime-core — never re-deriving
     * litedbmodel's Backend-Compile.
     *
     * @param \stdClass $bundle the §8 published bundle (json_decode(.., false) shape).
     * @param array<string,mixed> $input the bound input scope.
     * @return mixed the component's Φ output.
     */
    public static function executeBundle(\stdClass $bundle, array $input, \PDO $db): mixed
    {
        $surrogate = $bundle->component;
        $ir = self::wrapIr($bundle, $surrogate);
        $dialect = Dialect::forName((string) $bundle->dialect);
        $operations = self::operationsMap($bundle);
        $handlers = self::buildHandlers($db, $operations, $dialect);

        $optionalHeads = self::optionalHeadsSet($bundle);
        $normalized = self::normalizeInput($surrogate, $optionalHeads, $input);

        try {
            return Behavior::runBehavior($ir, $handlers, $normalized, (string) ($surrogate->name ?? ''));
        } catch (\Throwable $e) {
            throw self::reErrorToSqlFailure($e);
        }
    }

    /**
     * Execute a §8 write-tx `SqlBundle`'s derived transaction plan as ONE real transaction with
     * gate-first short-circuit (runtime.ts `executeTransactionBundle` + write-runtime.ts
     * `executeTransaction`). Consumes ONLY the serialized `TransactionPlan` (pure JSON) + the
     * render pipeline + PDO.
     *
     * @param \stdClass $bundle
     * @param array<string,mixed> $input
     * @return array{committed:bool, shortCircuit?:array{statementId:string,reason:string}, entity:?array<string,mixed>, executed:list<string>}
     */
    public static function executeTransactionBundle(\stdClass $bundle, array $input, \PDO $db): array
    {
        $plan = $bundle->transaction ?? null;
        if (!($plan instanceof \stdClass)) {
            throw new \RuntimeException(
                'scp write: this bundle carries no transaction plan (not a write-time-relations Command bundle)'
            );
        }
        return WriteRuntime::executeTransaction($db, $plan, $input, Dialect::forName((string) $bundle->dialect));
    }

    // ── Bundle helpers ────────────────────────────────────────────────────────

    /** Build the ComponentGraphIR stdClass bc `runBehavior` consumes: `{components:[surrogate]}`. */
    private static function wrapIr(\stdClass $bundle, \stdClass $surrogate): \stdClass
    {
        $ir = new \stdClass();
        $ir->irVersion = $bundle->irVersion ?? 1;
        $ir->exprVersion = $bundle->exprVersion ?? 1;
        $ir->components = [$surrogate];
        return $ir;
    }

    /** The bundle's `operations` map (nodeId → CompiledOperation stdClass). @return array<string,\stdClass> */
    private static function operationsMap(\stdClass $bundle): array
    {
        $ops = [];
        $operations = $bundle->operations ?? new \stdClass();
        if ($operations instanceof \stdClass) {
            foreach (get_object_vars($operations) as $id => $op) {
                if ($op instanceof \stdClass) {
                    $ops[(string) $id] = $op;
                }
            }
        }
        return $ops;
    }

    /** The bundle's `optionalHeads` as a set (present-as-null normalization keys). @return array<string,true> */
    private static function optionalHeadsSet(\stdClass $bundle): array
    {
        $set = [];
        $heads = $bundle->optionalHeads ?? [];
        if (is_array($heads)) {
            foreach ($heads as $h) {
                if (is_string($h)) {
                    $set[$h] = true;
                }
            }
        }
        return $set;
    }

    // ── SQL handlers (render → PDO execute → row→object) ───────────────────────

    /**
     * Build the SQL handler registry: one handler per SQL Catalog name (spec §11 item 4). All
     * four CRUD names share the render→execute handler; the pre-compiled op keyed by nodeId (via
     * the handler ctx) encodes the per-node operation (runtime.ts `buildHandlers`).
     *
     * @param array<string,\stdClass> $operations
     * @return array<string,callable>
     */
    private static function buildHandlers(\PDO $db, array $operations, Dialect $dialect): array
    {
        $handle = static function (array $ports, array $ctx) use ($db, $operations, $dialect): array {
            $nodeId = (string) ($ctx['nodeId'] ?? '');
            $component = (string) ($ctx['component'] ?? '');
            $op = $operations[$nodeId] ?? null;
            if (!($op instanceof \stdClass)) {
                return ['error' => "scp runtime: no compiled operation for node '{$nodeId}' ({$component})"];
            }
            $scopeVal = $ports[self::SCOPE_PORT] ?? null;
            if (!($scopeVal instanceof \stdClass)) {
                return ['error' => "scp runtime: node '{$nodeId}' surrogate scope did not evaluate to an object"];
            }
            $scope = self::objToScope($scopeVal);
            return self::executeRendered($db, $op, $scope, $dialect);
        };
        return [
            'Select' => $handle,
            'Insert' => $handle,
            'Update' => $handle,
            'Delete' => $handle,
        ];
    }

    /**
     * Execute one rendered SQL statement against PDO and return the row list (assembly). A
     * SELECT / RETURNING statement returns its rows (as stdClass); a non-returning write returns
     * a single-row summary `[{changes, lastInsertRowid}]`. A driver error is mapped
     * ({@link SqlFailure}) and returned as bc `{error}` so the node's Policy Kind governs
     * propagation (runtime.ts `executeRendered`).
     *
     * @param array<string,mixed> $scope
     * @return array{ok:mixed}|array{error:string}
     */
    private static function executeRendered(\PDO $db, \stdClass $op, array $scope, Dialect $dialect): array
    {
        $rendered = Render::renderOperation($op, $scope, $dialect);
        $params = array_map([self::class, 'toDriverParam'], $rendered['params']);
        $component = (string) ($op->component ?? '');
        $hasReturn = $component === 'Select' || self::hasReturning($rendered['sql']);
        try {
            $stmt = $db->prepare($rendered['sql']);
            $stmt->execute(array_values($params));
            if ($hasReturn) {
                $rows = self::fetchObjects($stmt);
                return ['ok' => $rows];
            }
            $changes = $stmt->rowCount();
            $lastId = self::lastInsertId($db);
            $summary = new \stdClass();
            $summary->changes = $changes;
            $summary->lastInsertRowid = $lastId;
            return ['ok' => [$summary]];
        } catch (\PDOException $e) {
            return ['error' => SqlFailure::fromPdo($e)->getMessage()];
        }
    }

    /** True if a rendered SQL statement carries a RETURNING clause (case-insensitive word). */
    private static function hasReturning(string $sql): bool
    {
        return preg_match('/\breturning\b/i', $sql) === 1;
    }

    /** Best-effort integer lastInsertId (SQLite AUTOINCREMENT). Non-numeric → 0. */
    private static function lastInsertId(\PDO $db): int
    {
        $id = $db->lastInsertId();
        return is_numeric($id) ? (int) $id : 0;
    }

    /** Fetch all rows as stdClass (so map elements / Φ output property-access + JSON-encode match TS). @return list<\stdClass> */
    private static function fetchObjects(\PDOStatement $stmt): array
    {
        $rows = $stmt->fetchAll(\PDO::FETCH_OBJ);
        return is_array($rows) ? array_values($rows) : [];
    }

    /**
     * Convert bc-evaluated param values to PDO-bindable values. bc evaluates ints to PHP `int`
     * (PDO binds directly). A plain object (an emit payload `{obj:{…}}`) is serialized to a JSON
     * text column; arrays are IN-list elements already flattened by the renderer. Booleans bind
     * as 1/0 (SQLite has no bool). Mirrors runtime.ts/write-runtime.ts `toDriverParam`.
     */
    private static function toDriverParam(mixed $v): mixed
    {
        if (is_bool($v)) {
            return $v ? 1 : 0;
        }
        if ($v instanceof \stdClass) {
            return json_encode($v, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        }
        return $v;
    }

    /**
     * Flatten a surrogate `__scope` stdClass to an assoc render scope. bc already evaluated each
     * head's `{ref:[…]}` in ITS scope, so the object's props ARE the resolved binding values
     * (map element rows, sibling results, input names). @return array<string,mixed>
     */
    private static function objToScope(\stdClass $obj): array
    {
        return get_object_vars($obj);
    }

    // ── Input normalization (schema/bundle-driven — SSoT) ──────────────────────

    /**
     * Normalize the caller input to `null` (present-as-null) for every OPTIONAL binding the
     * caller omitted, so a SKIP guard using `refOpt` evaluates to `false` and drops its fragment
     * (absent-key SKIP). "Optional" is the SSoT, NOT an ad-hoc code default: a head is optional
     * iff the component's Input Port schema marks it `required !== true`, OR it is in the
     * bundle's `optionalHeads` (the SKIP fragment / `opt(…)` declaration). A required, non-SKIP
     * missing head is left absent so a real wiring bug surfaces loudly as bc's UNKNOWN_BINDING
     * (runtime.ts `normalizeInput`).
     *
     * @param array<string,true> $optionalHeads
     * @param array<string,mixed> $input
     * @return array<string,mixed>
     */
    private static function normalizeInput(\stdClass $component, array $optionalHeads, array $input): array
    {
        $out = $input;
        $inputPorts = $component->inputPorts ?? new \stdClass();
        if ($inputPorts instanceof \stdClass) {
            foreach (get_object_vars($inputPorts) as $port => $schema) {
                $required = ($schema instanceof \stdClass) ? ($schema->required ?? null) : null;
                if ($required !== true && !array_key_exists((string) $port, $out)) {
                    $out[(string) $port] = null;
                }
            }
        }
        foreach ($optionalHeads as $head => $_) {
            if (!array_key_exists($head, $out)) {
                $out[$head] = null;
            }
        }
        return $out;
    }

    /**
     * If a `runPlan` `OP_FAILED` carries a mapped-failure message, re-surface the structured
     * {@link SqlFailure} (the message embeds the original SQLite code). Non-driver errors are
     * re-thrown verbatim (runtime.ts `reErrorToSqlFailure`).
     */
    private static function reErrorToSqlFailure(\Throwable $e): \Throwable
    {
        $message = $e->getMessage();
        if (preg_match('/(SQLITE_[A-Z_]+)/', $message, $m) === 1) {
            return SqlFailure::fromCode($m[1], $message);
        }
        return $e;
    }
}
