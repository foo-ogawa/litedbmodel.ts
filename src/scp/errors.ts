/**
 * litedbmodel v2 SCP — Error Mapping (WS3, #23; spec §11 item 5).
 *
 * Maps a SQLite driver error (better-sqlite3 `SqliteError`, carrying a `SQLITE_*` `code`)
 * to an SCP {@link SqlFailure}: a structured Failure with a stable `kind` and the bc
 * Execution-Plan {@link PolicyKind} the runtime honors (fail / retry / continue). The
 * handler returns `{ error }` to bc's `runBehavior`; a `fail`-policy node re-throws through
 * `runPlan` (`OP_FAILED`), so the driver surfaces this structured failure at the boundary.
 *
 * The mapping is closed and explicit (no silent catch-all that hides a driver error): an
 * unrecognized SQLite code maps to `kind: 'driver_error'` with `policy: 'fail'` — loud, and
 * carrying the original code + message for diagnosis.
 */

import type { PolicyKind } from 'behavior-contracts';

/** The SCP failure kinds litedbmodel maps SQL driver errors to (spec §11 item 5). */
export type SqlFailureKind =
  | 'constraint_violation' // UNIQUE / PRIMARY KEY / CHECK / NOT NULL — a durable data conflict
  | 'foreign_key_violation' // FK constraint — a referential-integrity conflict
  | 'retryable' // BUSY / LOCKED — a transient contention error (Policy Kind: retry)
  | 'driver_error'; // anything else — loud, non-retryable

/**
 * A mapped SCP failure: the SCP `kind`, the honored bc `PolicyKind`, the original SQLite
 * `code`, and a human message. Thrown at the runtime boundary (or carried as `{error}`
 * into `runBehavior` for Policy-Kind interpretation by `runPlan`).
 */
export class SqlFailure extends Error {
  constructor(
    readonly kind: SqlFailureKind,
    readonly policy: PolicyKind,
    readonly sqliteCode: string | undefined,
    message: string,
  ) {
    super(message);
    this.name = 'SqlFailure';
  }
}

/**
 * The context a {@link LimitExceededError} was raised from (spec §E-2 / v1 `LimitExceededError`).
 *   - `'find'`   — a top-level read (find/read) exceeded `findHardLimit`. The read injects
 *     `LIMIT hardLimit + 1` when the author set no explicit limit, so the reported `count` is the
 *     N+1 fetch size: the TOTAL is only known to be MORE than `hardLimit`.
 *   - `'relation'` — a hasMany relation batch exceeded `hasManyHardLimit`. The batch is fetched in
 *     full (no N+1), so the reported `count` is the EXACT batch total.
 */
export type LimitExceededContext = 'find' | 'relation';

/**
 * The SHARED cross-language runaway-prevention contract (Phase E-2, epic #74; v1 parity —
 * `DBModel` find hard-limit + `_selectForRelation`). Thrown by the TS runtime post-fetch guard when
 * a read / relation batch returns MORE rows than the configured hard limit, so an accidental
 * missing-WHERE / N+1 pattern fails LOUD instead of loading an unbounded result.
 *
 * This is the REFERENCE error shape the rust/go/py/php ports (#100-103) mirror byte-for-byte:
 *   - fields: `limit` (the cap), `count` (rows fetched — see {@link LimitExceededContext}),
 *     `context` (`'find'` | `'relation'`), `model` (the read/parent model), `relation?`
 *     (the relation name, `'relation'` context only);
 *   - message: `Query limit exceeded: <where> returned <count-phrase> records, but limit is
 *     <limit>. This usually indicates a missing WHERE clause or an N+1 query pattern. Set a higher
 *     limit or use pagination.` — `find` reports `more than <limit>` (N+1 fetch), `relation`
 *     reports the exact `<count>`.
 *
 * NOT a {@link SqlFailure}: a runaway guard is a litedbmodel-level policy error, not a mapped driver
 * failure, and it carries no `SQLITE_*` code (so `reErrorToSqlFailure` propagates it unchanged).
 */
export class LimitExceededError extends Error {
  constructor(
    readonly limit: number,
    /**
     * Rows fetched. `find` context: the `LIMIT hardLimit + 1` fetch size (the true total is only
     * known to EXCEED `limit`). `relation` context: the EXACT batch-total row count.
     */
    readonly count: number,
    readonly context: LimitExceededContext,
    readonly model?: string,
    readonly relation?: string,
  ) {
    const where =
      context === 'find'
        ? `find() on ${model ?? 'unknown'}`
        : `relation '${relation ?? 'unknown'}' on ${model ?? 'unknown'}`;
    const countPhrase = context === 'find' ? `more than ${limit}` : `${count}`;
    super(
      `Query limit exceeded: ${where} returned ${countPhrase} records, ` +
        `but limit is ${limit}. This usually indicates a missing WHERE clause or ` +
        `an N+1 query pattern. Set a higher limit or use pagination.`,
    );
    this.name = 'LimitExceededError';
  }
}

/** True if `e` looks like a better-sqlite3 `SqliteError` (has a `SQLITE_*` string `code`). */
function isSqliteError(e: unknown): e is { code: string; message: string } {
  return (
    typeof e === 'object' &&
    e !== null &&
    typeof (e as { code?: unknown }).code === 'string' &&
    (e as { code: string }).code.startsWith('SQLITE_')
  );
}

/**
 * Map a caught driver error to an {@link SqlFailure}. The `SQLITE_CONSTRAINT_*` family maps
 * to constraint / FK kinds (Policy `fail` — a data conflict is not retryable); the
 * `SQLITE_BUSY` / `SQLITE_LOCKED` family maps to `retryable` (Policy `retry`). Anything else
 * (including a non-SQLite error) maps to `driver_error` (Policy `fail`), preserving the
 * original message.
 */
export function mapSqliteError(e: unknown): SqlFailure {
  if (!isSqliteError(e)) {
    const message = e instanceof Error ? e.message : String(e);
    return new SqlFailure('driver_error', 'fail', undefined, `non-SQLite driver error: ${message}`);
  }
  const { code, message } = e;
  // The message embeds the `SQLITE_*` code so it survives being wrapped by bc's `runPlan`
  // (`OP_FAILED: <handler message>`); the runtime re-maps from it to re-surface the
  // structured SqlFailure (kind / policy / code) at the boundary.
  const tagged = `[${code}] ${message}`;
  if (code === 'SQLITE_CONSTRAINT_FOREIGNKEY') {
    return new SqlFailure('foreign_key_violation', 'fail', code, tagged);
  }
  if (code.startsWith('SQLITE_CONSTRAINT')) {
    // UNIQUE / PRIMARYKEY / CHECK / NOTNULL / … — a durable constraint conflict.
    return new SqlFailure('constraint_violation', 'fail', code, tagged);
  }
  if (code === 'SQLITE_BUSY' || code === 'SQLITE_LOCKED') {
    return new SqlFailure('retryable', 'retry', code, tagged);
  }
  return new SqlFailure('driver_error', 'fail', code, tagged);
}
