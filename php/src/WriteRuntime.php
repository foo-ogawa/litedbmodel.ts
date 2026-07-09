<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — write-time transaction runtime (PHP port of src/scp/write-runtime.ts,
 * WS7d #33; spec §6 / §3 / §11).
 *
 * Executes a derived `TransactionPlan` (pure JSON, from the §8 bundle) against REAL SQL (PDO) as
 * ONE transaction with gate-first short-circuit (spec §6). It renders each ordered statement with
 * the SAME normative {@link Render::renderOperation} the read path uses, and drives an explicit
 * `BEGIN` / `COMMIT` / `ROLLBACK` envelope through PDO. Semantics-identical to the TS reference.
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
    public static function executeTransaction(\PDO $db, \stdClass $plan, array $input, Dialect $dialect): array
    {
        $statements = is_array($plan->statements ?? null) ? $plan->statements : [];
        $entityFrom = $plan->entityFrom ?? null;

        $db->exec('BEGIN');
        /** @var list<string> $executed */
        $executed = [];
        // The evolving scope: input names at the top level (bc flat scope) + the body RETURNING
        // row exposed under `__entity` once the body runs. Defaults live in the declaration/schema,
        // so NO ad-hoc code default is injected here.
        $scope = $input;
        $entity = null;

        try {
            foreach ($statements as $stmt) {
                if (!($stmt instanceof \stdClass)) {
                    continue;
                }
                $result = self::runOne($db, $stmt, $scope, $executed, $dialect);

                // Gate-first: a failing gate short-circuits — ROLLBACK and STOP (tail never runs).
                $gate = $stmt->gate ?? null;
                if (is_string($gate)) {
                    $reason = self::gateShortCircuit($gate, $result);
                    if ($reason !== null) {
                        $db->exec('ROLLBACK');
                        return [
                            'committed' => false,
                            'shortCircuit' => ['statementId' => (string) ($stmt->id ?? ''), 'reason' => $reason],
                            'entity' => null,
                            'executed' => $executed,
                        ];
                    }
                }

                // Capture the body RETURNING row as `$.entity` for derive/edges/emits.
                if ($entityFrom !== null && (string) ($stmt->id ?? '') === (string) $entityFrom) {
                    $entity = count($result['rows']) > 0 ? $result['rows'][0] : null;
                    if ($entity instanceof \stdClass) {
                        $scope[self::ENTITY_ROOT] = $entity;
                    }
                }
            }
            $db->exec('COMMIT');
            return [
                'committed' => true,
                'entity' => self::entityToArray($entity),
                'executed' => $executed,
            ];
        } catch (SqlFailure $e) {
            self::bestEffortRollback($db);
            throw $e;
        } catch (\PDOException $e) {
            // A driver failure: ROLLBACK, then map + re-throw the structured SqlFailure (spec §11).
            self::bestEffortRollback($db);
            throw SqlFailure::fromPdo($e);
        }
    }

    /**
     * Render + execute one statement's compiled op against PDO in the given scope, returning the
     * rows (SELECT / RETURNING) plus the affected-row count. Records it as executed. Mirrors
     * write-runtime.ts `execStatement` + `runOne`.
     *
     * @param array<string,mixed> $scope
     * @param list<string> $executed by ref
     * @return array{rows:list<\stdClass>, changes:int}
     */
    private static function runOne(\PDO $db, \stdClass $stmt, array $scope, array &$executed, Dialect $dialect): array
    {
        $op = $stmt->op;
        $rendered = Render::renderOperation($op, $scope, $dialect);
        $params = array_map([self::class, 'toDriverParam'], $rendered['params']);
        $component = (string) ($op->component ?? '');
        $hasReturn = $component === 'Select' || preg_match('/\breturning\b/i', $rendered['sql']) === 1;

        $pdoStmt = $db->prepare($rendered['sql']);
        $pdoStmt->execute(array_values($params));
        $executed[] = (string) ($stmt->id ?? '');

        if ($hasReturn) {
            $rows = $pdoStmt->fetchAll(\PDO::FETCH_OBJ);
            $rows = is_array($rows) ? array_values($rows) : [];
            return ['rows' => $rows, 'changes' => count($rows)];
        }
        return ['rows' => [], 'changes' => $pdoStmt->rowCount()];
    }

    /**
     * Evaluate a gate rule on a statement result → the short-circuit reason, or null to continue
     * (write-runtime.ts `gateShortCircuit`).
     *
     * @param array{rows:list<\stdClass>, changes:int} $result
     */
    private static function gateShortCircuit(string $gate, array $result): ?string
    {
        return match ($gate) {
            'existsElseRollback' => count($result['rows']) === 0 ? 'requires_absent' : null,
            'insertedElseRollback' => $result['changes'] === 0 ? 'unique_collision' : null,
            'insertedElseNoop' => $result['changes'] === 0 ? 'idempotent_duplicate' : null,
            default => null,
        };
    }

    /**
     * Convert a bc-evaluated param to a PDO-bindable value. bc gives ints as PHP int (bind
     * directly). An emit payload (`{obj:{…}}`) evaluates to stdClass → serialize to the outbox
     * JSON text column. Booleans → 1/0. Mirrors write-runtime.ts `toDriverParam`.
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

    /** The `$.entity` RETURNING row as an assoc array (the structured TransactionResult shape). */
    private static function entityToArray(?\stdClass $entity): ?array
    {
        return $entity === null ? null : get_object_vars($entity);
    }

    private static function bestEffortRollback(\PDO $db): void
    {
        try {
            $db->exec('ROLLBACK');
        } catch (\Throwable) {
            // ROLLBACK best-effort; the original failure is surfaced by the caller.
        }
    }
}
