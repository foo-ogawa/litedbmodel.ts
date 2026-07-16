/**
 * Phase B-1 (#81) — tx-completeness CONTRACT unit tests (the API-reference behavior the ports mirror).
 *
 * These pin the DIALECT-NEUTRAL, driver-agnostic pieces of the tx-completeness API defined in
 * `src/scp/tx-options.ts` + `src/scp/exec-context.ts`, plus the guard + nested + rollbackOnly + retry
 * behavior on an in-memory fake async pool (no live DB). The LIVE isolation + LIVE real-contention
 * retry proofs are in `test/integration/TxCompleteness.test.ts`.
 */

import { describe, it, expect } from 'vitest';
import {
  beginStatements,
  isolationPhrase,
  resolveTxOptions,
  isRetryableTxError,
  checkWriteAllowed,
  withReadOnly,
  runInTransactionScope,
  isInTransaction,
  WriteOutsideTransactionError,
  WriteInReadOnlyContextError,
  withTransactionAsync,
  runGuardedAsync,
  runAsync,
  PooledAsyncContext,
  type AsyncConnection,
  type AsyncConnectionPool,
} from '../../src/scp';
import { isConnectionError } from '../../src/connection-errors';

// ── isolation-level → SQL (per dialect) ───────────────────────────────────────

describe('#81 isolation level → SQL mapping', () => {
  it('maps the three levels to canonical phrases', () => {
    expect(isolationPhrase('read committed')).toBe('READ COMMITTED');
    expect(isolationPhrase('repeatable read')).toBe('REPEATABLE READ');
    expect(isolationPhrase('serializable')).toBe('SERIALIZABLE');
  });

  it('postgres: the level rides BEGIN (one statement)', () => {
    expect(beginStatements('postgres')).toEqual(['BEGIN']);
    expect(beginStatements('postgres', 'serializable')).toEqual(['BEGIN ISOLATION LEVEL SERIALIZABLE']);
    expect(beginStatements('postgres', 'repeatable read')).toEqual(['BEGIN ISOLATION LEVEL REPEATABLE READ']);
  });

  it('mysql: a SET precedes a bare BEGIN (two statements — SET scopes only the next tx)', () => {
    expect(beginStatements('mysql')).toEqual(['BEGIN']);
    expect(beginStatements('mysql', 'repeatable read')).toEqual(['SET TRANSACTION ISOLATION LEVEL REPEATABLE READ', 'BEGIN']);
  });

  it('sqlite: an isolation request is a HARD ERROR (no per-tx level — not silently dropped)', () => {
    expect(beginStatements('sqlite')).toEqual(['BEGIN']); // no isolation ⇒ bare BEGIN, fine
    expect(() => beginStatements('sqlite', 'serializable')).toThrow(/SQLite does not support a per-transaction isolation level/);
  });
});

// ── option defaults (v1 parity) ───────────────────────────────────────────────

describe('#81 TransactionOptions defaults (v1 parity)', () => {
  it('applies retryOnError=true, retryLimit=3, retryDuration=200, rollbackOnly=false', () => {
    expect(resolveTxOptions()).toEqual({ isolation: undefined, retryOnError: true, retryLimit: 3, retryDuration: 200, rollbackOnly: false });
  });
  it('honors explicit overrides', () => {
    expect(resolveTxOptions({ retryLimit: 5, retryDuration: 50, rollbackOnly: true, isolation: 'serializable' })).toEqual({
      isolation: 'serializable',
      retryOnError: true,
      retryLimit: 5,
      retryDuration: 50,
      rollbackOnly: true,
    });
  });
});

// ── retryable-error classification (per dialect codes + fallbacks) ─────────────

describe('#81 isRetryableTxError classification', () => {
  const mkErr = (msg: string, extra: Record<string, unknown> = {}): Error => Object.assign(new Error(msg), extra);

  it('PG serialization_failure (40001) + deadlock_detected (40P01) are retryable', () => {
    expect(isRetryableTxError(mkErr('could not serialize access', { code: '40001' }), isConnectionError)).toBe(true);
    expect(isRetryableTxError(mkErr('deadlock detected', { code: '40P01' }), isConnectionError)).toBe(true);
  });
  it('MySQL deadlock (1213) + lock-wait-timeout (1205) are retryable', () => {
    expect(isRetryableTxError(mkErr('Deadlock found', { errno: 1213 }), isConnectionError)).toBe(true);
    expect(isRetryableTxError(mkErr('Lock wait timeout exceeded', { errno: 1205 }), isConnectionError)).toBe(true);
  });
  it('connection errors are retryable', () => {
    expect(isRetryableTxError(mkErr('read ECONNRESET', { code: 'ECONNRESET' }), isConnectionError)).toBe(true);
  });
  it('a unique/constraint conflict is NOT retryable', () => {
    expect(isRetryableTxError(mkErr('duplicate key value violates unique constraint', { code: '23505' }), isConnectionError)).toBe(false);
  });
  it('a non-Error is not retryable', () => {
    expect(isRetryableTxError('nope', isConnectionError)).toBe(false);
  });
});

// ── write=tx guard (mirror v1 checkWriteAllowed) ──────────────────────────────

describe('#81 write=tx guard (checkWriteAllowed)', () => {
  it('a write OUTSIDE a transaction throws WriteOutsideTransactionError', () => {
    expect(() => checkWriteAllowed('INSERT', 'User')).toThrow(WriteOutsideTransactionError);
    try {
      checkWriteAllowed('INSERT', 'User');
    } catch (e) {
      expect((e as WriteOutsideTransactionError).operation).toBe('INSERT');
      expect((e as WriteOutsideTransactionError).modelName).toBe('User');
    }
  });

  it('a write INSIDE a transaction scope is allowed', () => {
    runInTransactionScope(() => {
      expect(isInTransaction()).toBe(true);
      expect(() => checkWriteAllowed('UPDATE')).not.toThrow();
    });
  });

  it('a write in a read-only (withReadOnly) scope throws WriteInReadOnlyContextError — even inside a tx', () => {
    runInTransactionScope(() => {
      withReadOnly(() => {
        expect(() => checkWriteAllowed('DELETE', 'Post')).toThrow(WriteInReadOnlyContextError);
      });
    });
    // read-only is checked BEFORE the tx check (the more specific rejection wins)
    withReadOnly(() => {
      expect(() => checkWriteAllowed('DELETE')).toThrow(WriteInReadOnlyContextError);
    });
  });
});

// ── An in-memory fake async pool to exercise nested / rollbackOnly / retry without a live DB ──

interface Recorded {
  readonly sql: string[];
  connections: number; // how many distinct connections were acquired
}

function fakePool(opts: { failFirstN?: number; failWith?: () => Error } = {}): { pool: AsyncConnectionPool; rec: Recorded } {
  const rec: Recorded = { sql: [], connections: 0 };
  let attemptOfFailingStmt = 0;
  const conn: AsyncConnection = {
    async execute(sql) {
      rec.sql.push(sql);
      return [];
    },
    async run(sql) {
      rec.sql.push(sql);
      // Fail the BODY write on the first N attempts (the retry proof).
      if (opts.failFirstN !== undefined && /insert|update|delete/i.test(sql)) {
        attemptOfFailingStmt++;
        if (attemptOfFailingStmt <= opts.failFirstN) throw (opts.failWith ?? (() => Object.assign(new Error('deadlock detected'), { code: '40P01' })))();
      }
      return { changes: 1, lastInsertRowid: 0 };
    },
  };
  const pool: AsyncConnectionPool = {
    async acquire() {
      rec.connections++;
      return conn;
    },
    async release() {
      /* no-op */
    },
  };
  return { pool, rec };
}

describe('#81 rollbackOnly (dry-run)', () => {
  it('runs the body, returns its result, but ROLLBACKs instead of COMMIT', async () => {
    const { pool, rec } = fakePool();
    const ctx = new PooledAsyncContext(pool);
    const result = await withTransactionAsync(
      ctx,
      async (txCtx) => {
        await runAsync(txCtx, 'INSERT INTO t VALUES (1)', []);
        return 'body-result';
      },
      { rollbackOnly: true },
      'postgres',
    );
    expect(result).toBe('body-result'); // body ran, result returned
    expect(rec.sql).toEqual(['BEGIN', 'INSERT INTO t VALUES (1)', 'ROLLBACK']); // ROLLBACK, no COMMIT
  });
});

describe('#81 nested transaction (join the outer — no new BEGIN/COMMIT)', () => {
  it('an inner withTransactionAsync joins the outer: ONE BEGIN/COMMIT, ONE connection', async () => {
    const { pool, rec } = fakePool();
    const ctx = new PooledAsyncContext(pool);
    await withTransactionAsync(
      ctx,
      async (txCtx) => {
        await runAsync(txCtx, 'INSERT INTO outer_t VALUES (1)', []);
        // Inner tx: must JOIN the outer (no BEGIN/COMMIT/new acquire).
        await withTransactionAsync(
          ctx,
          async (innerCtx) => {
            await runAsync(innerCtx, 'INSERT INTO inner_t VALUES (2)', []);
          },
          {},
          'postgres',
        );
      },
      {},
      'postgres',
    );
    // Exactly ONE BEGIN and ONE COMMIT; both inserts between them; ONE connection acquired.
    expect(rec.sql).toEqual(['BEGIN', 'INSERT INTO outer_t VALUES (1)', 'INSERT INTO inner_t VALUES (2)', 'COMMIT']);
    expect(rec.connections).toBe(1);
  });

  it('an inner-body error rolls back the WHOLE tx (nested join = one physical tx)', async () => {
    const { pool, rec } = fakePool();
    const ctx = new PooledAsyncContext(pool);
    await expect(
      withTransactionAsync(
        ctx,
        async (txCtx) => {
          await runAsync(txCtx, 'INSERT INTO outer_t VALUES (1)', []);
          await withTransactionAsync(ctx, async () => {
            throw new Error('inner boom');
          }, {}, 'postgres');
        },
        { retryOnError: false },
        'postgres',
      ),
    ).rejects.toThrow('inner boom');
    // The single physical tx ROLLBACKs (no COMMIT).
    expect(rec.sql).toEqual(['BEGIN', 'INSERT INTO outer_t VALUES (1)', 'ROLLBACK']);
    expect(rec.connections).toBe(1);
  });
});

describe('#81 tx retry (whole-tx re-run on a retryable error; fresh connection per attempt)', () => {
  it('retries a deadlock and eventually succeeds, on a NEW connection each attempt', async () => {
    // Body write fails with a deadlock on attempt 1, succeeds on attempt 2.
    const { pool, rec } = fakePool({ failFirstN: 1 });
    const ctx = new PooledAsyncContext(pool);
    const result = await withTransactionAsync(
      ctx,
      async (txCtx) => {
        await runAsync(txCtx, 'INSERT INTO t VALUES (1)', []);
        return 'ok';
      },
      { retryLimit: 3, retryDuration: 1 },
      'postgres',
    );
    expect(result).toBe('ok');
    expect(rec.connections).toBe(2); // attempt 1 (deadlock) + attempt 2 (success) = 2 distinct acquires
    // BEGIN, failing INSERT, ROLLBACK (attempt 1); BEGIN, INSERT, COMMIT (attempt 2).
    expect(rec.sql).toEqual(['BEGIN', 'INSERT INTO t VALUES (1)', 'ROLLBACK', 'BEGIN', 'INSERT INTO t VALUES (1)', 'COMMIT']);
  });

  it('a NON-retryable error does NOT retry (one attempt, then throw)', async () => {
    const { pool, rec } = fakePool({ failFirstN: 99, failWith: () => Object.assign(new Error('unique violation'), { code: '23505' }) });
    const ctx = new PooledAsyncContext(pool);
    await expect(
      withTransactionAsync(ctx, async (txCtx) => { await runAsync(txCtx, 'INSERT INTO t VALUES (1)', []); }, { retryLimit: 5, retryDuration: 1 }, 'postgres'),
    ).rejects.toThrow('unique violation');
    expect(rec.connections).toBe(1); // NO retry
  });

  it('a retryable error that never clears exhausts retryLimit then throws', async () => {
    const { pool, rec } = fakePool({ failFirstN: 99 }); // deadlock every attempt
    const ctx = new PooledAsyncContext(pool);
    await expect(
      withTransactionAsync(ctx, async (txCtx) => { await runAsync(txCtx, 'INSERT INTO t VALUES (1)', []); }, { retryLimit: 3, retryDuration: 1 }, 'postgres'),
    ).rejects.toThrow('deadlock');
    expect(rec.connections).toBe(3); // retryLimit attempts, all failed
  });
});

describe('#81 runGuardedAsync (guarded write seam)', () => {
  it('rejects a guarded write outside a tx, allows it inside', async () => {
    const { pool } = fakePool();
    const ctx = new PooledAsyncContext(pool);
    // Outside a tx → WriteOutsideTransactionError (before any SQL).
    await expect(runGuardedAsync(ctx, 'INSERT INTO t VALUES (1)', [], 'INSERT', 'T')).rejects.toThrow(WriteOutsideTransactionError);
    // Inside a tx → allowed.
    await withTransactionAsync(ctx, async (txCtx) => {
      await expect(runGuardedAsync(txCtx, 'INSERT INTO t VALUES (1)', [], 'INSERT', 'T')).resolves.toEqual({ changes: 1, lastInsertRowid: 0 });
    }, {}, 'postgres');
  });
});
