<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — write-time transaction runtime (PHP port of src/scp/write-runtime.ts,
 * WS7d #33; spec §6 / §3 / §11).
 *
 * Executes a derived `TransactionPlan` (pure JSON, from the §8 bundle) against REAL SQL (PDO) as
 * ONE transaction with gate-first short-circuit (spec §6). It renders each ordered statement's
 * static makeSQL op with the SAME {@link StaticBundle::renderTxOp} assemble/render the read path
 * uses (evaluate deferred Expression-IR params → assemble → dialect placeholders), and drives an
 * explicit `BEGIN` / `COMMIT` / `ROLLBACK` envelope through PDO. Semantics-identical to the TS
 * reference (`src/scp/makesql/tx.ts`).
 *
 *   BEGIN;
 *     for each ordered statement:
 *       render (input + accumulated $.entity row) → execute REAL SQL
 *       if it is a GATE and its rule FAILS  → ROLLBACK and stop (remaining SQL never runs)
 *   COMMIT;   (or ROLLBACK on a driver failure / gate fail)
 *
 * The body write's RETURNING row is captured and exposed to derive/edges/emits under
 * `$.entity.*` (`__entity`). A gate short-circuit returns a structured result (`committed:false`)
 * rather than throwing; a driver failure IS mapped + thrown (spec §11 item 5).
 *
 * ## Per-execution connection ownership (Phase A / #79)
 *
 * The BEGIN/COMMIT/ROLLBACK envelope is no longer a raw `$db->exec('BEGIN')` on a threaded `\PDO`:
 * it is owned by the {@see withTransactionDecided()} combinator, which acquires ONE tx-owned
 * connection ({@see PdoTxConnection}), pins it into a tx-scoped {@see ExecutionContext}, and runs the
 * body on it. EVERY statement in the body funnels through the central WRITE/READ seam
 * ({@see run()}/{@see execute()}), which resolves the pinned tx connection — so a statement can never
 * escape onto a different (autocommit) connection. A gate short-circuit is a non-error ROLLBACK
 * (returns `committed:false`); a driver failure throws (⇒ the combinator rolls back + re-raises). The
 * combinator releases the owned connection EXACTLY ONCE in a `finally` (the #78 leak-guard).
 */
final class WriteRuntime
{
    private const ENTITY_ROOT = '__entity';

    /**
     * Execute a derived transaction plan as ONE real transaction with gate-first short-circuit.
     *
     * @param \stdClass $plan the derived TransactionPlan (json_decode(.., false) shape).
     * @param array<string,mixed> $input the Command input scope ($.input.* = bc flat scope).
     * @return array{committed:bool, shortCircuit?:array{statementId:string,reason:string}, entity:?array<string,mixed>, executed:list<string>}
     * @throws SqlFailure a mapped driver failure (the transaction is ROLLBACKed first).
     */
    public static function executeTransaction(\PDO|ExecutionContext $db, \stdClass $plan, array $input, Dialect $dialect): array
    {
        $ctx = Context::of($db);
        $statements = is_array($plan->statements ?? null) ? $plan->statements : [];
        $entityFrom = $plan->entityFrom ?? null;

        // Batch mode (createMany/updateMany/deleteMany): gate-free, ref-free plan (entityFrom null,
        // every statement a plain body) — accumulate each body statement's RETURNING rows in order.
        $isBatch = $entityFrom === null;
        foreach ($statements as $s) {
            if ($s instanceof \stdClass && (($s->gate ?? null) !== null || ($s->binds ?? null) !== null || ($s->role ?? '') !== 'body')) {
                $isBatch = false;
                break;
            }
        }

        // Per-execution connection ownership (§3): the tx owns ONE connection for its whole span;
        // BEGIN/COMMIT/ROLLBACK are the combinator's, and every body statement funnels through the
        // seam onto the pinned tx connection. A gate short-circuit is a non-error ROLLBACK decision
        // (returns committed:false); a driver / gate-abort failure throws (the combinator rolls back +
        // re-raises the mapped SqlFailure). The combinator releases the connection exactly once.
        try {
            return withTransactionDecided($ctx, function (ExecutionContext $txCtx) use (
                $statements,
                $entityFrom,
                $isBatch,
                $input,
                $dialect
            ): TxDecision {
                /** @var list<list<array<string,mixed>>> $returnedRows */
                $returnedRows = [];
                /** @var list<string> $executed */
                $executed = [];
                // The evolving scope: input names at the top level (bc flat scope) + the body RETURNING
                // row exposed under `__entity` once the body runs. Defaults live in the
                // declaration/schema, so NO ad-hoc code default is injected here.
                $scope = $input;
                $entity = null;

                foreach ($statements as $stmt) {
                    if (!($stmt instanceof \stdClass)) {
                        continue;
                    }
                    $result = self::runOne($txCtx, $stmt, $scope, $executed, $dialect);

                    // Gate-first: a failing gate short-circuits — a NON-error ROLLBACK decision (the
                    // tail never runs; the combinator rolls back the owned connection).
                    $gate = $stmt->gate ?? null;
                    if (is_string($gate)) {
                        $reason = self::gateShortCircuit($gate, $result);
                        if ($reason !== null) {
                            return rollbackWith([
                                'committed' => false,
                                'shortCircuit' => ['statementId' => (string) ($stmt->id ?? ''), 'reason' => $reason],
                                'entity' => null,
                                'executed' => $executed,
                            ]);
                        }
                    }

                    // Capture the SOLE body RETURNING row as `$.entity` (WS5 single-write back-compat).
                    if ($entityFrom !== null && (string) ($stmt->id ?? '') === (string) $entityFrom) {
                        $entity = count($result['rows']) > 0 ? $result['rows'][0] : null;
                        if ($entity instanceof \stdClass) {
                            $scope[self::ENTITY_ROOT] = $entity;
                        }
                    }

                    // WS8a composite: bind THIS statement's RETURNING row under its `binds` name so a
                    // later `$.ref.<binds>.<field>` resolves against it (the tx-DAG data-dependency
                    // edge). Self-describing — bind the row the plan named; no re-derivation.
                    $binds = $stmt->binds ?? null;
                    if (is_string($binds) && count($result['rows']) > 0) {
                        $row = $result['rows'][0];
                        if ($row instanceof \stdClass) {
                            $scope[$binds] = $row;
                        }
                    }

                    if ($isBatch && ($stmt->role ?? '') === 'body' && count($result['rows']) > 0) {
                        $returnedRows[] = array_map([self::class, 'entityToArray'], $result['rows']);
                    }
                }
                $out = [
                    'committed' => true,
                    'entity' => self::entityToArray($entity),
                    'executed' => $executed,
                ];
                if (count($returnedRows) > 0) {
                    $out['returnedRows'] = $returnedRows;
                }
                return commit($out);
            });
        } catch (SqlFailure $e) {
            throw $e;
        } catch (\PDOException $e) {
            // A driver failure surfacing from the body: map + re-throw the structured SqlFailure (spec
            // §11). The combinator already ROLLBACKed the owned connection + released it.
            throw SqlFailure::fromPdo($e);
        }
    }

    /**
     * Render + execute one statement's compiled op through the seam on the pinned tx connection, in
     * the given scope, returning the rows (SELECT / RETURNING) plus the affected-row count. Records it
     * as executed. Mirrors write-runtime.ts `execStatement` + `runOne`.
     *
     * @param array<string,mixed> $scope
     * @param list<string> $executed by ref
     * @return array{rows:list<\stdClass>, changes:int}
     */
    private static function runOne(ExecutionContext $ctx, \stdClass $stmt, array $scope, array &$executed, Dialect $dialect): array
    {
        $op = $stmt->op;
        $rendered = StaticBundle::renderTxOp($op, $scope, $dialect->name);
        $params = array_map(fn ($p) => self::toDriverParam($p, $dialect->name), $rendered['params']);
        $sql = $rendered['sql'];
        // A returning statement (SELECT-prefixed gate/derive, or RETURNING body) yields rows;
        // a plain write yields an affected-row count (mirrors TS execStatement hasReturn).
        $hasReturn = preg_match('/^\s*select\b/i', $sql) === 1 || preg_match('/\breturning\b/i', $sql) === 1;
        $executed[] = (string) ($stmt->id ?? '');

        // The central seam (§2): a returning statement rides the READ seam (rows), a plain write the
        // WRITE seam (affected count) — BOTH resolve the pinned tx connection, so every statement runs
        // on the tx's OWNED connection (never a fresh/autocommit one). `changes` mirrors the pre-seam
        // shape byte-for-byte: count($rows) for a returning statement, rowCount() for a plain write —
        // the gate rules (`insertedElseRollback`/`insertedElseNoop`) read this exact value.
        if ($hasReturn) {
            $rows = execute($ctx, $sql, array_values($params));
            return ['rows' => $rows, 'changes' => count($rows)];
        }
        return ['rows' => [], 'changes' => run($ctx, $sql, array_values($params))->changes];
    }

    /**
     * Evaluate a gate rule on a statement result → the short-circuit reason, or null to continue
     * (write-runtime.ts `gateShortCircuit`).
     *
     * @param array{rows:list<\stdClass>, changes:int} $result
     */
    private static function gateShortCircuit(string $gate, array $result): ?string
    {
        // An unknown / forward-incompatible gate rule is FAIL-CLOSED (aligned with Python + Rust +
        // TS + Go): a corrupt gate MUST NOT silently continue (fail-open would skip a malformed gate
        // and let the write COMMIT). Throwing aborts the tx (the caller ROLLBACKs on the exception).
        return match ($gate) {
            'existsElseRollback' => count($result['rows']) === 0 ? 'requires_absent' : null,
            'insertedElseRollback' => $result['changes'] === 0 ? 'unique_collision' : null,
            'insertedElseNoop' => $result['changes'] === 0 ? 'idempotent_duplicate' : null,
            default => throw new \RuntimeException("scp write: unknown gate rule '{$gate}'"),
        };
    }

    /**
     * Convert a bc-evaluated param to a PDO-bindable value. bc gives ints as PHP int (bind
     * directly). An emit payload (`{obj:{…}}`) evaluates to stdClass → serialize to the outbox
     * JSON text column. Booleans → 1/0. Mirrors write-runtime.ts `toDriverParam`.
     */
    private static function toDriverParam(mixed $v, string $dialect = 'sqlite'): mixed
    {
        if (is_bool($v)) {
            return $v ? 1 : 0;
        }
        if ($v instanceof \stdClass) {
            return json_encode($v, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        }
        // A batch write (createMany/updateMany) on PG binds a native array to one `$N` (UNNEST /
        // `= ANY`). PDO_pgsql cannot bind a PHP array — convert it to the PG `{…}` array-literal text
        // (the SAME conversion the relation-batch read path uses, StaticBundle::pgArrayLiteral).
        if (is_array($v)) {
            return $dialect === 'postgres' ? StaticBundle::pgArrayLiteral($v) : json_encode($v, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        }
        return $v;
    }

    /** The `$.entity` RETURNING row as an assoc array (the structured TransactionResult shape). */
    private static function entityToArray(?\stdClass $entity): ?array
    {
        return $entity === null ? null : get_object_vars($entity);
    }
}
