/**
 * litedbmodel v2 SCP — write-time transaction runtime (WS5, #25; spec §6 / §3 / §11).
 *
 * Executes a derived {@link TransactionPlan} against REAL SQLite as ONE transaction with
 * gate-first short-circuit (spec §6). It renders each ordered statement with the SAME normative
 * {@link renderOperation} the read path uses (fragment tree + Expression-IR param slots), and
 * drives an explicit `BEGIN` / `COMMIT` / `ROLLBACK` envelope through the SAME synchronous
 * driver seam ({@link SqliteDb}) the WS3 runtime uses — no mock, no separate interpreter.
 *
 * ## The transaction envelope (spec §6) — Tx is derived, not authored
 *
 *   BEGIN;
 *     for each ordered statement:
 *       render (input + accumulated $.entity row) → execute REAL SQL
 *       if it is a GATE and its rule FAILS  → ROLLBACK and stop (remaining SQL never runs)
 *   COMMIT;   (or ROLLBACK on a driver failure / gate fail)
 *
 * The body write's RETURNING row is captured and exposed to the derive/edges/emits stages under
 * `$.entity.*` ({@link ENTITY_ROOT}) — the §6 example's `$.entity.id`.
 *
 * ## Gate-first is a REAL behavior (spec §6 "Gate First"), proven, not textual
 *
 * A gate statement's {@link GateRule} is evaluated on its actual driver result:
 *   - `existsElseRollback` — `SELECT 1 …` returned ZERO rows ⇒ the required row is absent ⇒
 *     ROLLBACK; the body + all later statements are NEVER prepared/executed (a real query-count
 *     drop, observable via a counting driver, and the DB is unchanged).
 *   - `insertedElseRollback` — a `unique` guard `INSERT … ON CONFLICT DO NOTHING` affected ZERO
 *     rows ⇒ a collision ⇒ ROLLBACK.
 *   - `insertedElseNoop` — an `idempotency` token INSERT affected ZERO rows ⇒ a DUPLICATE request
 *     ⇒ short-circuit as a no-op: ROLLBACK the (empty-so-far) transaction, so a duplicate
 *     `request_id` performs NO double write (idempotent).
 *
 * A short-circuit returns a structured {@link TransactionResult} (`committed:false`) rather than
 * throwing — the caller distinguishes a legitimate gate outcome (absent requires / unique
 * collision / duplicate) from a driver Failure (which IS mapped + thrown, spec §11 item 5).
 */

import type { Scope, Value } from 'behavior-contracts';
import type { CompiledOperation } from './ir';
import { renderOperation } from './render';
import { mapSqliteError } from './errors';
import { ENTITY_ROOT } from './writes';
import type { GateRule, TransactionPlan, TxStatement } from './write-plan';
import type { SqliteDb } from './runtime';

/** Why a transaction did not commit (a gate short-circuit outcome; not a driver error). */
export type ShortCircuitReason = 'requires_absent' | 'unique_collision' | 'idempotent_duplicate';

/** The structured outcome of executing a {@link TransactionPlan}. */
export interface TransactionResult {
  /** Whether the transaction COMMITted (`true`) or short-circuited + ROLLBACKed (`false`). */
  readonly committed: boolean;
  /** When short-circuited: which gate stopped it (and thus which later SQL never ran). */
  readonly shortCircuit?: { readonly statementId: string; readonly reason: ShortCircuitReason };
  /** The body write's RETURNING row (`$.entity`), or null when the tx short-circuited/omitted RETURNING. */
  readonly entity: Record<string, unknown> | null;
  /** The ordered ids of the statements actually EXECUTED (gate-first short-circuit drops the tail). */
  readonly executed: readonly string[];
}

/** bc evaluates ints to bigint; convert a rendered param to a driver-bindable value. */
function toDriverParam(v: Value): unknown {
  if (typeof v === 'bigint') {
    if (v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(v);
    return v;
  }
  // An emit payload evaluates to a plain object (`{obj:{…}}`); serialize it to the outbox JSON
  // text column. Arrays are IN-list elements (not used in the write path); pass through.
  if (v !== null && typeof v === 'object' && !Array.isArray(v)) return JSON.stringify(v);
  return v;
}

/**
 * Execute one statement's compiled op against the driver in the given scope, returning the
 * rows (SELECT / RETURNING) plus the affected-row count (non-returning writes). A driver error
 * is mapped and thrown (the caller ROLLBACKs). This is the SAME render→bind→execute pipeline as
 * the WS3 read/write handlers; only the transaction envelope + gate interpretation is new.
 */
function execStatement(
  db: SqliteDb,
  op: CompiledOperation,
  scope: Scope,
): { rows: Record<string, unknown>[]; changes: number } {
  const rendered = renderOperation(op, scope);
  const params = rendered.params.map(toDriverParam);
  const stmt = db.prepare(rendered.sql);
  const hasReturn = op.component === 'Select' || /\breturning\b/i.test(rendered.sql);
  if (hasReturn) {
    const rows = stmt.all(...params) as Record<string, unknown>[];
    return { rows, changes: rows.length };
  }
  const info = stmt.run(...params);
  return { rows: [], changes: info.changes };
}

/** Evaluate a gate rule on a statement result → the short-circuit reason, or null to continue. */
function gateShortCircuit(gate: GateRule, result: { rows: Record<string, unknown>[]; changes: number }): ShortCircuitReason | null {
  switch (gate) {
    case 'existsElseRollback':
      return result.rows.length === 0 ? 'requires_absent' : null;
    case 'insertedElseRollback':
      return result.changes === 0 ? 'unique_collision' : null;
    case 'insertedElseNoop':
      return result.changes === 0 ? 'idempotent_duplicate' : null;
  }
}

/**
 * Execute a derived {@link TransactionPlan} as ONE real SQLite transaction with gate-first
 * short-circuit (spec §6). Statements run in the plan's fixed order (requires → idempotency →
 * unique → body → derive → edges → emits); a failing gate ROLLBACKs and the remaining statements
 * are never executed. On success COMMITs and returns the `$.entity` RETURNING row.
 *
 * @param db     the synchronous SQLite driver (better-sqlite3 `Database`), supporting `BEGIN` /
 *               `COMMIT` / `ROLLBACK` via `prepare(...).run()`.
 * @param plan   the derived transaction plan (pure JSON; from `deriveTransactionPlan` or the bundle).
 * @param input  the Command input scope (`$.input.*` = bc flat scope).
 * @throws {SqlFailure} a mapped driver failure (the transaction is ROLLBACKed first, spec §11).
 */
export function executeTransaction(db: SqliteDb, plan: TransactionPlan, input: Scope): TransactionResult {
  db.prepare('BEGIN').run();
  const executed: string[] = [];
  // The evolving scope: input names at the top level (bc flat scope) + the body RETURNING row
  // exposed under `__entity` once the body runs. Defaults live in the declaration/schema, so no
  // ad-hoc code default is injected here.
  const scope: Scope = { ...input };
  let entity: Record<string, unknown> | null = null;

  try {
    for (const stmt of plan.statements) {
      const result = runOne(db, stmt, scope, executed);

      // Gate-first: a failing gate short-circuits — ROLLBACK and STOP (tail never executes).
      if (stmt.gate !== undefined) {
        const reason = gateShortCircuit(stmt.gate, result);
        if (reason !== null) {
          db.prepare('ROLLBACK').run();
          return { committed: false, shortCircuit: { statementId: stmt.id, reason }, entity: null, executed };
        }
      }

      // Capture the body RETURNING row as `$.entity` for the derive/edges/emits stages.
      if (stmt.id === plan.entityFrom) {
        entity = result.rows.length > 0 ? result.rows[0] : null;
        if (entity !== null) scope[ENTITY_ROOT] = entity as unknown as Value;
      }
    }
    db.prepare('COMMIT').run();
    return { committed: true, entity, executed };
  } catch (e) {
    // A driver failure: ROLLBACK, then map + re-throw the structured SqlFailure (spec §11).
    try {
      db.prepare('ROLLBACK').run();
    } catch {
      /* ROLLBACK best-effort; surface the original failure below */
    }
    throw mapSqliteError(e);
  }
}

/** Render + execute one statement, recording it as executed. */
function runOne(
  db: SqliteDb,
  stmt: TxStatement,
  scope: Scope,
  executed: string[],
): { rows: Record<string, unknown>[]; changes: number } {
  const result = execStatement(db, stmt.op, scope);
  executed.push(stmt.id);
  return result;
}

/**
 * A counting {@link SqliteDb} wrapper: forwards every call to the wrapped driver but records
 * each PREPARED SQL string. Tests use it to PROVE gate-first short-circuit — after an absent
 * `requires`, the body/derive/edges/emits SQL is never prepared (a real, observable query-count
 * drop), not merely absent from a text list. This is a test aid living beside the runtime.
 */
export function countingDriver(db: SqliteDb): { db: SqliteDb; prepared: string[] } {
  const prepared: string[] = [];
  const wrapped: SqliteDb = {
    prepare(sql: string) {
      prepared.push(sql);
      return db.prepare(sql);
    },
  };
  return { db: wrapped, prepared };
}
