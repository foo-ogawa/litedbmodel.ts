<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — PHP runtime (WS7d, #33; static-makeSQL flip, epic #43/#45).
 *
 * The PHP leg of the litedbmodel v2 SCP multi-language runtime. It interprets the
 * language-neutral §8 published STATIC makeSQL `SqlBundle` (a read `readGraph` = `compileBehaviors`'
 * REAL `Select`/map `ComponentGraphIR` + per-node STATIC statement templates, or a write
 * `transaction` plan of gate-first makeSQL statements, dialect-tagged) and executes it against
 * a PDO SQL driver, semantics-identical to the TS reference (`src/scp/runtime.ts` +
 * `src/scp/makesql/*`).
 *
 * It is a THIN runtime: the read-graph execution (plan-stage / map iteration / wiring / Φ output
 * assembly) is the NATIVE walker in {@link StaticBundle} (#12 — no bc `runBehavior`); only the
 * deferred Expression-IR value-specs + skip are evaluated by the VENDORED behavior-contracts PHP
 * port (`src/BehaviorContracts/` `ExprEval`); the static makeSQL assemble/render/execute lives in
 * {@link StaticBundle} + {@link WriteRuntime}. This runtime is the thin FACADE that dispatches a
 * bundle to the read graph executor or the gate-first write transaction. The reduced
 * fragment-tree render path (`Render::renderOperation`) is RETIRED for the SQL path — makeSQL is
 * the sole read/render path.
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
    /** Version mirrored from package.json by scripts/sync-versions.mjs (SSoT). */
    public const VERSION = '2.1.0';

    /**
     * Render the PRIMARY read node's statements of a `ReadGraph` against a scope for a dialect →
     * ['sql'=>…, 'params'=>…] (static-bundle.ts `renderReadPrimary`). The render axis for the
     * conformance golden.
     *
     * @param \stdClass $readGraph the compiled ReadGraph (json_decode(.., false) shape).
     * @param array<string,mixed> $scope
     * @return array{sql:string, params:list<mixed>}
     */
    public static function renderReadPrimary(\stdClass $readGraph, array $scope): array
    {
        return StaticBundle::renderReadPrimary($readGraph, $scope);
    }

    /** The dialect NULLS-ordering primitive (dialect.ts `orderByNulls`). */
    public static function orderByNulls(string $expr, string $dir, string $nulls, string $dialect): string
    {
        return Dialect::forName($dialect)->orderByNulls($expr, $dir, $nulls);
    }

    /**
     * Execute a §8 read/exec `SqlBundle` end-to-end (runtime.ts `executeBundle`): delegate to
     * {@link StaticBundle::executeReadGraph}, the NATIVE read-graph walker (#12 — NO bc
     * `runBehavior`) which owns the plan / map / wire / output orchestration itself, renders each
     * node's static statement templates against the walk scope, and runs REAL SQL via PDO.
     * Consumes ONLY the serialized bundle + vendored bc `ExprEval` (deferred params + skip) —
     * never re-deriving litedbmodel's Backend-Compile.
     *
     * @param \stdClass $bundle the §8 published bundle (json_decode(.., false) shape).
     * @param array<string,mixed> $input the bound input scope.
     * @return mixed the component's Φ output.
     */
    public static function executeBundle(\stdClass $bundle, array $input, \PDO $db): mixed
    {
        $readGraph = $bundle->readGraph ?? null;
        if (!($readGraph instanceof \stdClass)) {
            throw new \RuntimeException(
                "scp runtime: bundle '" . (string) ($bundle->name ?? '')
                . "' carries no read graph (single-statement writes ride the write path)"
            );
        }
        try {
            return StaticBundle::executeReadGraph($readGraph, $input, $db);
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
