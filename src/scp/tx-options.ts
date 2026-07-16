/**
 * litedbmodel v2 SCP — the **tx-completeness contract** (Phase B-1 / #81): the TransactionOptions
 * shape, the guard errors, the per-dialect isolation-level SQL, and the retryable-error classifier.
 *
 * This module is the **API REFERENCE** for Phase B — the 4 native ports (rust #82 / go #83 / py #84
 * / php #85) mirror THIS contract exactly (option field names + defaults, guard-error semantics,
 * isolation-level→SQL mapping per dialect, retryable-error classification, retry-loop policy). It is
 * dialect-neutral and driver-agnostic on purpose: `withTransactionAsync` (exec-context.ts) consumes
 * it; nothing here touches a connection.
 *
 * It builds ON the Phase A {@link import('./exec-context').ExecutionContext} + owned-connection
 * ownership (`withTransactionAsync`); it does NOT re-implement connection ownership — it layers the
 * options/guards/retry/isolation on top of that seam. It mirrors v1 `src/DBModel.ts`
 * (`transaction` :2787, `checkWriteAllowed` :886, `isRetryableError` :2865) but on the SCP seam.
 */

import { WriteOutsideTransactionError, WriteInReadOnlyContextError } from '../types';
import type { Dialect } from './makesql/handler';

export { WriteOutsideTransactionError, WriteInReadOnlyContextError };

// ── Isolation level (the portable enum + per-dialect SQL) ─────────────────────

/**
 * The three portable SQL isolation levels the tx API exposes (lowercased, matching the v2 public
 * surface). READ UNCOMMITTED is deliberately NOT offered — neither PG (which silently upgrades it to
 * READ COMMITTED) nor a correctness-minded default wants it, and omitting it keeps the 4 ports' enum
 * identical. The value maps to the canonical SQL phrase via {@link isolationSql}.
 */
export type IsolationLevel = 'read committed' | 'repeatable read' | 'serializable';

/** The canonical SQL phrase for an {@link IsolationLevel} (e.g. `SERIALIZABLE`). */
export function isolationPhrase(level: IsolationLevel): string {
  switch (level) {
    case 'read committed':
      return 'READ COMMITTED';
    case 'repeatable read':
      return 'REPEATABLE READ';
    case 'serializable':
      return 'SERIALIZABLE';
    default: {
      // Fail-CLOSED on an unknown level (a corrupt/forward-incompatible value must NOT silently run
      // at the engine default — that would hide a mis-set isolation). Mirrors the tx gate's policy.
      const bad: never = level;
      throw new Error(`scp tx: unknown isolation level '${String(bad)}'`);
    }
  }
}

/**
 * The tx-start statements for `dialect` at `isolation` (in issue order). Per-dialect because the
 * three engines express per-transaction isolation DIFFERENTLY:
 *
 *   - **postgres**: `BEGIN ISOLATION LEVEL <phrase>` — one statement, the level rides the BEGIN.
 *   - **mysql**: `SET TRANSACTION ISOLATION LEVEL <phrase>` MUST precede `BEGIN` (it scopes the very
 *     NEXT transaction only), so this returns TWO statements: the SET, then a bare `BEGIN`.
 *   - **sqlite**: SQLite has NO per-transaction isolation-level knob. Its isolation is a
 *     process/PRAGMA-level property (`journal_mode=WAL` gives snapshot reads; the default rollback
 *     journal serializes writers), NOT something a `BEGIN` clause selects. So an isolation request on
 *     SQLite is a HARD ERROR here — we do NOT silently drop it (that would fake honoring the level).
 *     The conformance/bench SQLite path never passes `isolation`, so it always emits a bare `BEGIN`.
 *
 * With no `isolation`, every dialect emits a single bare `BEGIN` (the Phase A behavior, byte-identical).
 */
export function beginStatements(dialect: Dialect, isolation?: IsolationLevel): readonly string[] {
  if (isolation === undefined) return ['BEGIN'];
  const phrase = isolationPhrase(isolation);
  switch (dialect) {
    case 'postgres':
      return [`BEGIN ISOLATION LEVEL ${phrase}`];
    case 'mysql':
      // The SET scopes ONLY the next tx; it must be issued before BEGIN, on the SAME connection.
      return [`SET TRANSACTION ISOLATION LEVEL ${phrase}`, 'BEGIN'];
    case 'sqlite':
      throw new Error(
        `scp tx: SQLite does not support a per-transaction isolation level ('${isolation}'). ` +
          `SQLite isolation is a connection/PRAGMA property (e.g. journal_mode=WAL for snapshot reads), ` +
          `not a BEGIN clause — set it on the connection, and omit TransactionOptions.isolation for SQLite.`,
      );
    default: {
      const bad: never = dialect;
      throw new Error(`scp tx: unknown dialect '${String(bad)}'`);
    }
  }
}

// ── TransactionOptions (the Phase B public option shape) ──────────────────────

/**
 * Options for a transaction (the Phase B contract; mirrors v1 `TransactionOptions` in `src/types.ts`
 * plus the new `isolation`). Every field is optional with a stable default; the 4 native ports MUST
 * expose the SAME field names + defaults.
 */
export interface TransactionOptions {
  /**
   * Per-transaction isolation level. Issued via {@link beginStatements} on the tx-owned connection
   * (PG: on BEGIN; MySQL: a preceding SET). Omit for the engine default (PG/MySQL: READ COMMITTED /
   * REPEATABLE READ respectively). SQLite has no per-tx level ⇒ passing this on SQLite is an error.
   */
  readonly isolation?: IsolationLevel;
  /** Retry the whole tx on a retryable error (deadlock / serialization / connection). @default true */
  readonly retryOnError?: boolean;
  /** Max attempts before giving up (the FIRST try counts as attempt 1). @default 3 */
  readonly retryLimit?: number;
  /** Backoff base in ms; attempt k waits `retryDuration * 2^(k-1)` (exponential). @default 200 */
  readonly retryDuration?: number;
  /**
   * ROLLBACK instead of COMMIT at the end of a SUCCESSFUL body (dry-run / preview): the body runs and
   * its result is returned, but NO change is committed. A body error still ROLLBACKs + re-raises as
   * usual. @default false
   */
  readonly rollbackOnly?: boolean;
}

/** The resolved (defaults-applied) options the tx runtime uses — no `undefined` holes. */
export interface ResolvedTxOptions {
  readonly isolation?: IsolationLevel;
  readonly retryOnError: boolean;
  readonly retryLimit: number;
  readonly retryDuration: number;
  readonly rollbackOnly: boolean;
}

/** Apply the Phase B defaults (v1-parity: retryOnError=true, retryLimit=3, retryDuration=200). */
export function resolveTxOptions(opts: TransactionOptions = {}): ResolvedTxOptions {
  return {
    isolation: opts.isolation,
    retryOnError: opts.retryOnError ?? true,
    retryLimit: opts.retryLimit ?? 3,
    retryDuration: opts.retryDuration ?? 200,
    rollbackOnly: opts.rollbackOnly ?? false,
  };
}

// ── Retryable-error classification (per dialect) ──────────────────────────────

/**
 * Is `error` a RETRYABLE transaction failure — a deadlock, a serialization failure, or a broken
 * connection — for which re-running the whole transaction can succeed? Classification is by the
 * driver's stable error CODE first (PG `SQLSTATE`, MySQL `errno`), with the v1 message substrings as
 * a fallback (mirrors v1 `DBModel.isRetryableError` :2865). A data conflict (unique/FK/check) is NOT
 * retryable — re-running would fail identically.
 *
 * Codes (per dialect):
 *   - **postgres** SQLSTATE: `40001` serialization_failure, `40P01` deadlock_detected.
 *   - **mysql** errno: `1213` ER_LOCK_DEADLOCK, `1205` ER_LOCK_WAIT_TIMEOUT.
 *   - **connection errors** (either dialect): via {@link isConnectionError} — a dropped/reset/refused
 *     connection is retryable (reconnect on the next attempt).
 */
export function isRetryableTxError(error: unknown, isConnectionError: (e: Error) => boolean): boolean {
  if (!(error instanceof Error)) return false;
  if (isConnectionError(error)) return true;

  // PG: SQLSTATE on `error.code` (5-char string). MySQL: errno on `error.errno` (number) or `code`.
  const e = error as Error & { code?: unknown; errno?: unknown; sqlState?: unknown };
  const pgState = typeof e.code === 'string' ? e.code : typeof e.sqlState === 'string' ? e.sqlState : '';
  if (pgState === '40001' || pgState === '40P01') return true; // serialization_failure / deadlock_detected
  const myErrno = typeof e.errno === 'number' ? e.errno : typeof e.code === 'number' ? e.code : undefined;
  if (myErrno === 1213 || myErrno === 1205) return true; // ER_LOCK_DEADLOCK / ER_LOCK_WAIT_TIMEOUT

  // Fallback: the v1 message substrings (driver-version-independent phrasing).
  const message = error.message || '';
  return (
    message.includes('The transaction might succeed if retried') ||
    message.includes('try restarting transaction') ||
    message.includes('could not serialize access due to concurrent update') ||
    message.includes('could not serialize access') ||
    message.includes('Deadlock found') ||
    message.includes('deadlock detected') ||
    message.includes('Lock wait timeout exceeded')
  );
}

/** Sleep `ms` (the retry backoff). */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ── write=tx guards (mirror v1 `checkWriteAllowed`, DBModel.ts:886) ────────────

import { AsyncLocalStorage } from 'node:async_hooks';

/**
 * Ambient "inside an active transaction" marker (§ write=tx guard). Set by the tx runtime while a
 * transaction body runs — {@link isInTransaction} reads it, {@link checkWriteAllowed} enforces it.
 * This is the guard-facing mirror of the exec-context ALS connection pin: the pin proves per-
 * execution ownership; THIS marker proves the write is inside a caller-opened tx scope. The native
 * ports mark the SAME scope in their task-local / contextvar / `context.Context`.
 */
const txActiveStore = new AsyncLocalStorage<true>();

/** Ambient "read-only context" marker (mirror v1 `withWriter`): a write here is REJECTED. */
const readOnlyStore = new AsyncLocalStorage<true>();

/** Run `fn` with the "inside a transaction" marker set (called by the tx runtime around the body). */
export function runInTransactionScope<R>(fn: () => R): R {
  return txActiveStore.run(true, fn);
}

/**
 * Run `fn` in a READ-ONLY context (mirror v1 `DBModel.withWriter`): reads are allowed, but ANY write
 * inside `fn` throws {@link WriteInReadOnlyContextError}. Use for a read-your-writes / writer-pinned
 * read scope that must never accidentally mutate.
 */
export function withReadOnly<R>(fn: () => R): R {
  return readOnlyStore.run(true, fn);
}

/** True if the current async scope is inside an active transaction (guard/nested detection). */
export function isInTransaction(): boolean {
  return txActiveStore.getStore() === true;
}

/** True if the current async scope is a read-only context ({@link withReadOnly}). */
export function isReadOnly(): boolean {
  return readOnlyStore.getStore() === true;
}

/**
 * Enforce the write=tx guard (mirror v1 `DBModel._checkWriteAllowed`, DBModel.ts:886-896): a write
 * inside a {@link withReadOnly} scope throws {@link WriteInReadOnlyContextError}; a write with NO
 * active transaction throws {@link WriteOutsideTransactionError}. Called at every write ENTRY
 * (create/update/delete/upsert/batch) BEFORE any SQL is issued. The order matches v1: read-only is
 * checked first (a read-only scope is the more specific rejection).
 *
 * @param operation the write op name (INSERT/UPDATE/DELETE/UPSERT/BATCH) — carried in the error.
 * @param modelName optional model/table name — carried in the error for diagnosis.
 */
export function checkWriteAllowed(operation: string, modelName?: string): void {
  if (isReadOnly()) throw new WriteInReadOnlyContextError(operation, modelName);
  if (!isInTransaction()) throw new WriteOutsideTransactionError(operation, modelName);
}
