/**
 * Phase B-core (#86) — the PUBLIC user-controlled `transaction(fn)` boundary on LIVE PG + MySQL.
 *
 * This is the REAL transaction feature v2 was missing: `transaction(ctx, fn)` lets a caller wrap
 * MULTIPLE arbitrary operations so they commit / roll back TOGETHER. The proofs (real DB state, not
 * string checks):
 *
 *   (A) MULTI-OPERATION ATOMICITY — `transaction(() => { A.insert; B.insert })` commits BOTH; make
 *       B fail (PK collision) and A's row is ALSO rolled back (all-or-nothing across two SEPARATE
 *       operations). A SQL-capturing pool asserts exactly ONE BEGIN + ONE COMMIT/ROLLBACK on exactly
 *       ONE acquired connection for the whole boundary (the ops JOIN the ambient tx — they do NOT
 *       open their own envelopes).
 *   (B) WRITE=TX GUARD — a write OUTSIDE any `transaction(fn)` throws `WriteOutsideTransactionError`;
 *       a write inside `withReadOnly` throws `WriteInReadOnlyContextError`; a write inside
 *       `transaction(fn)` SUCCEEDS.
 *   (C) NESTED — `transaction()` inside `transaction()` is ONE physical BEGIN/COMMIT; an inner error
 *       rolls back the WHOLE tx.
 *
 * Builds on Phase A ownership (`PooledAsyncContext` / `withTransactionAsync`) + #81 tx primitives.
 * Requires live PG (:5433) + MySQL (:3307): `npm run docker:livedb:up`.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import mysql from 'mysql2/promise';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  entityWrites,
  compileWriteBundle,
  executeTransactionAsync,
  transaction,
  withReadOnly,
  PooledAsyncContext,
  pgConnectionPool,
  mysqlConnectionPool,
  WriteOutsideTransactionError,
  WriteInReadOnlyContextError,
  type AsyncConnection,
  type AsyncConnectionPool,
  type EntityWritesDefinition,
  type In,
} from '../../src/scp';

const PG = {
  host: process.env.TEST_DB_HOST || 'localhost',
  port: parseInt(process.env.TEST_DB_PORT || '5433', 10),
  database: process.env.TEST_DB_NAME || 'testdb',
  user: process.env.TEST_DB_USER || 'testuser',
  password: process.env.TEST_DB_PASSWORD || 'testpass',
  max: 16,
};
const MY = {
  host: process.env.TEST_MYSQL_HOST || '127.0.0.1',
  port: parseInt(process.env.TEST_MYSQL_PORT || '3307', 10),
  database: process.env.TEST_MYSQL_DB || 'testdb',
  user: process.env.TEST_MYSQL_USER || 'testuser',
  password: process.env.TEST_MYSQL_PASSWORD || 'testpass',
  connectionLimit: 16,
};

const TBL = 'scp_tx_boundary';

// ── The write behavior under test: a bare INSERT command (id, worker, seq) ──────
const L = components();
class BoundaryCommands extends SemanticBehavior {
  Insert($: In<{ id: number; worker: number; seq: number }>) {
    return L.Insert({
      table: TBL,
      'values.id': $.id,
      'values.worker': $.worker,
      'values.seq': $.seq,
      returning: 'id, worker, seq',
    });
  }
}
const contract = publishBehaviors(BoundaryCommands);
const insertWrites: EntityWritesDefinition = entityWrites<BoundaryCommands>((w) => ({ create: w.lifecycle({}) }));

let pgPool: Pool | undefined;
let myPool: mysql.Pool | undefined;

beforeAll(async () => {
  try {
    pgPool = new Pool(PG);
    await pgPool.query('SELECT 1');
  } catch (e) {
    throw new Error(`Postgres required for #86 tx-boundary at ${PG.host}:${PG.port} — ${(e as Error).message}. npm run docker:livedb:up`);
  }
  try {
    myPool = mysql.createPool(MY);
    await myPool.query('SELECT 1');
  } catch (e) {
    throw new Error(`MySQL required for #86 tx-boundary at ${MY.host}:${MY.port} — ${(e as Error).message}. npm run docker:livedb:up`);
  }
});

afterAll(async () => {
  await pgPool?.end().catch(() => undefined);
  await myPool?.end().catch(() => undefined);
}, 20000);

async function resetPg(): Promise<void> {
  await pgPool!.query(`DROP TABLE IF EXISTS ${TBL}`);
  await pgPool!.query(`CREATE TABLE ${TBL} (id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, seq INTEGER NOT NULL)`);
}
async function resetMy(): Promise<void> {
  await myPool!.query(`DROP TABLE IF EXISTS ${TBL}`);
  await myPool!.query(`CREATE TABLE ${TBL} (id INT PRIMARY KEY, worker INT NOT NULL, seq INT NOT NULL) ENGINE=InnoDB`);
}
async function readAllPg(): Promise<number[]> {
  return (await pgPool!.query(`SELECT id FROM ${TBL} ORDER BY id`)).rows.map((r) => Number(r.id));
}
async function readAllMy(): Promise<number[]> {
  const [r] = await myPool!.query(`SELECT id FROM ${TBL} ORDER BY id`);
  return (r as Record<string, unknown>[]).map((x) => Number(x.id));
}

/**
 * A SQL-CAPTURING pool wrapper: forwards every acquire/statement to the real pool but records each
 * SQL string (per owned connection) and counts how many connections were acquired. The whole-boundary
 * proof reads `acquires` (⇒ ONE connection) and greps the captured SQL for exactly ONE BEGIN + ONE
 * COMMIT/ROLLBACK — the ambient-join guarantee (ops do NOT open their own envelopes).
 */
function capturingPool(real: AsyncConnectionPool): AsyncConnectionPool & { sql: string[]; acquires: number } {
  const state = { sql: [] as string[], acquires: 0 };
  const wrapper: AsyncConnectionPool & { sql: string[]; acquires: number } = {
    sql: state.sql,
    get acquires() {
      return state.acquires;
    },
    async acquire() {
      state.acquires++;
      const conn = await real.acquire();
      const capturing: AsyncConnection = {
        async execute(s, p) {
          state.sql.push(s);
          return conn.execute(s, p);
        },
        async run(s, p) {
          state.sql.push(s);
          return conn.run(s, p);
        },
      };
      wraps.set(capturing, conn);
      return capturing;
    },
    async release(conn, destroy) {
      const inner = wraps.get(conn);
      if (inner !== undefined) await real.release(inner, destroy);
    },
  };
  const wraps = new WeakMap<AsyncConnection, AsyncConnection>();
  return wrapper;
}

const countBegins = (sql: string[]): number => sql.filter((s) => /^\s*(BEGIN|START TRANSACTION)/i.test(s)).length;
const countCommits = (sql: string[]): number => sql.filter((s) => /^\s*COMMIT/i.test(s)).length;
const countRollbacks = (sql: string[]): number => sql.filter((s) => /^\s*ROLLBACK/i.test(s)).length;

// ── (A) MULTI-OPERATION ATOMICITY — one BEGIN/COMMIT, one connection, A rolls back when B fails ──

describe('#86 transaction(fn) boundary — multi-operation atomicity (LIVE)', () => {
  async function multiOpCommits(
    reset: () => Promise<void>,
    rawPool: AsyncConnectionPool,
    dialect: 'postgres' | 'mysql',
    readAll: () => Promise<number[]>,
  ): Promise<void> {
    await reset();
    const cap = capturingPool(rawPool);
    const ctx = new PooledAsyncContext(cap);
    const bundle = compileWriteBundle(contract, 'Insert', insertWrites, 'create', dialect);

    // TWO separate operations inside ONE boundary — they must JOIN the ambient tx.
    await transaction(
      ctx,
      async () => {
        const a = await executeTransactionAsync(ctx, bundle.transaction!, { id: 1, worker: 1, seq: 0 }, dialect);
        expect(a.committed).toBe(true);
        const b = await executeTransactionAsync(ctx, bundle.transaction!, { id: 2, worker: 2, seq: 0 }, dialect);
        expect(b.committed).toBe(true);
      },
      { retryOnError: false },
      dialect,
    );

    // Both rows committed.
    expect(await readAll()).toEqual([1, 2]);
    // Exactly ONE connection, ONE BEGIN, ONE COMMIT for the WHOLE boundary (the ops did NOT open
    // their own envelopes — they joined the ambient tx).
    expect(cap.acquires).toBe(1);
    expect(countBegins(cap.sql)).toBe(1);
    expect(countCommits(cap.sql)).toBe(1);
    expect(countRollbacks(cap.sql)).toBe(0);
  }

  async function multiOpRollsBackWhenBFails(
    reset: () => Promise<void>,
    rawPool: AsyncConnectionPool,
    dialect: 'postgres' | 'mysql',
    readAll: () => Promise<number[]>,
  ): Promise<void> {
    await reset();
    const cap = capturingPool(rawPool);
    const ctx = new PooledAsyncContext(cap);
    const bundle = compileWriteBundle(contract, 'Insert', insertWrites, 'create', dialect);

    // op A inserts id=1; op B collides on id=1 (PK) → B throws → the WHOLE boundary rolls back.
    await expect(
      transaction(
        ctx,
        async () => {
          const a = await executeTransactionAsync(ctx, bundle.transaction!, { id: 1, worker: 1, seq: 0 }, dialect);
          expect(a.committed).toBe(true); // A "succeeded" WITHIN the tx…
          // …but B's PK collision aborts the whole boundary, so A must NOT survive.
          await executeTransactionAsync(ctx, bundle.transaction!, { id: 1, worker: 2, seq: 0 }, dialect);
        },
        { retryOnError: false },
        dialect,
      ),
    ).rejects.toThrow();

    // A's row is ALSO gone — real all-or-nothing across two SEPARATE operations on ONE connection.
    expect(await readAll()).toEqual([]);
    // ONE connection, ONE BEGIN, ONE ROLLBACK, ZERO COMMIT for the whole boundary.
    expect(cap.acquires).toBe(1);
    expect(countBegins(cap.sql)).toBe(1);
    expect(countCommits(cap.sql)).toBe(0);
    expect(countRollbacks(cap.sql)).toBe(1);
  }

  it('PG: two ops commit atomically — ONE BEGIN + ONE COMMIT on ONE connection', async () => {
    await multiOpCommits(resetPg, pgConnectionPool(pgPool as never), 'postgres', readAllPg);
  });
  it('MySQL: two ops commit atomically — ONE BEGIN + ONE COMMIT on ONE connection', async () => {
    await multiOpCommits(resetMy, mysqlConnectionPool(myPool as never), 'mysql', readAllMy);
  });
  it('PG: op B fails → op A is ALSO rolled back (ONE BEGIN + ONE ROLLBACK, ONE connection)', async () => {
    await multiOpRollsBackWhenBFails(resetPg, pgConnectionPool(pgPool as never), 'postgres', readAllPg);
  });
  it('MySQL: op B fails → op A is ALSO rolled back (ONE BEGIN + ONE ROLLBACK, ONE connection)', async () => {
    await multiOpRollsBackWhenBFails(resetMy, mysqlConnectionPool(myPool as never), 'mysql', readAllMy);
  });
});

// ── (B) WRITE=TX GUARD ─────────────────────────────────────────────────────────

describe('#86 write=tx guard (LIVE) — writes require an explicit transaction', () => {
  async function guardOutside(reset: () => Promise<void>, rawPool: AsyncConnectionPool, dialect: 'postgres' | 'mysql'): Promise<void> {
    await reset();
    const ctx = new PooledAsyncContext(rawPool);
    const bundle = compileWriteBundle(contract, 'Insert', insertWrites, 'create', dialect);
    // A write OUTSIDE any transaction(fn) is REJECTED.
    await expect(executeTransactionAsync(ctx, bundle.transaction!, { id: 5, worker: 1, seq: 0 }, dialect)).rejects.toBeInstanceOf(
      WriteOutsideTransactionError,
    );
  }
  async function guardReadOnly(reset: () => Promise<void>, rawPool: AsyncConnectionPool, dialect: 'postgres' | 'mysql'): Promise<void> {
    await reset();
    const ctx = new PooledAsyncContext(rawPool);
    const bundle = compileWriteBundle(contract, 'Insert', insertWrites, 'create', dialect);
    // A write inside a withReadOnly scope is REJECTED (read-only takes precedence).
    await expect(
      withReadOnly(() => executeTransactionAsync(ctx, bundle.transaction!, { id: 6, worker: 1, seq: 0 }, dialect)),
    ).rejects.toBeInstanceOf(WriteInReadOnlyContextError);
  }
  async function guardInsideOk(reset: () => Promise<void>, rawPool: AsyncConnectionPool, dialect: 'postgres' | 'mysql', readAll: () => Promise<number[]>): Promise<void> {
    await reset();
    const ctx = new PooledAsyncContext(rawPool);
    const bundle = compileWriteBundle(contract, 'Insert', insertWrites, 'create', dialect);
    await transaction(
      ctx,
      async () => {
        const r = await executeTransactionAsync(ctx, bundle.transaction!, { id: 7, worker: 1, seq: 0 }, dialect);
        expect(r.committed).toBe(true);
      },
      { retryOnError: false },
      dialect,
    );
    expect(await readAll()).toEqual([7]);
  }

  it('PG: write outside transaction → WriteOutsideTransactionError', async () => {
    await guardOutside(resetPg, pgConnectionPool(pgPool as never), 'postgres');
  });
  it('MySQL: write outside transaction → WriteOutsideTransactionError', async () => {
    await guardOutside(resetMy, mysqlConnectionPool(myPool as never), 'mysql');
  });
  it('PG: write in withReadOnly → WriteInReadOnlyContextError', async () => {
    await guardReadOnly(resetPg, pgConnectionPool(pgPool as never), 'postgres');
  });
  it('MySQL: write in withReadOnly → WriteInReadOnlyContextError', async () => {
    await guardReadOnly(resetMy, mysqlConnectionPool(myPool as never), 'mysql');
  });
  it('PG: write inside transaction(fn) SUCCEEDS', async () => {
    await guardInsideOk(resetPg, pgConnectionPool(pgPool as never), 'postgres', readAllPg);
  });
  it('MySQL: write inside transaction(fn) SUCCEEDS', async () => {
    await guardInsideOk(resetMy, mysqlConnectionPool(myPool as never), 'mysql', readAllMy);
  });
});

// ── (C) NESTED transaction(fn) — one BEGIN/COMMIT; inner error rolls back the whole ──

describe('#86 nested transaction(fn) (LIVE) — joins the outer (one physical BEGIN/COMMIT)', () => {
  async function nestedCommits(reset: () => Promise<void>, rawPool: AsyncConnectionPool, dialect: 'postgres' | 'mysql', readAll: () => Promise<number[]>): Promise<void> {
    await reset();
    const cap = capturingPool(rawPool);
    const ctx = new PooledAsyncContext(cap);
    const bundle = compileWriteBundle(contract, 'Insert', insertWrites, 'create', dialect);

    await transaction(
      ctx,
      async () => {
        await executeTransactionAsync(ctx, bundle.transaction!, { id: 10, worker: 1, seq: 0 }, dialect);
        // A NESTED transaction() joins the outer — NO new BEGIN/COMMIT/connection.
        await transaction(
          ctx,
          async () => {
            await executeTransactionAsync(ctx, bundle.transaction!, { id: 11, worker: 1, seq: 0 }, dialect);
          },
          {},
          dialect,
        );
      },
      { retryOnError: false },
      dialect,
    );

    expect(await readAll()).toEqual([10, 11]);
    expect(cap.acquires).toBe(1); // ONE connection for the whole nested tree
    expect(countBegins(cap.sql)).toBe(1); // ONE physical BEGIN
    expect(countCommits(cap.sql)).toBe(1); // ONE physical COMMIT
  }

  async function nestedInnerErrorRollsBackWhole(reset: () => Promise<void>, rawPool: AsyncConnectionPool, dialect: 'postgres' | 'mysql', readAll: () => Promise<number[]>): Promise<void> {
    await reset();
    const cap = capturingPool(rawPool);
    const ctx = new PooledAsyncContext(cap);
    const bundle = compileWriteBundle(contract, 'Insert', insertWrites, 'create', dialect);

    await expect(
      transaction(
        ctx,
        async () => {
          await executeTransactionAsync(ctx, bundle.transaction!, { id: 20, worker: 1, seq: 0 }, dialect); // outer op
          await transaction(
            ctx,
            async () => {
              // inner op collides with the outer op's id → throws → whole tx rolls back
              await executeTransactionAsync(ctx, bundle.transaction!, { id: 20, worker: 2, seq: 0 }, dialect);
            },
            {},
            dialect,
          );
        },
        { retryOnError: false },
        dialect,
      ),
    ).rejects.toThrow();

    // The outer op (id=20) is rolled back too — an inner error rolls back the WHOLE tx.
    expect(await readAll()).toEqual([]);
    expect(cap.acquires).toBe(1);
    expect(countBegins(cap.sql)).toBe(1);
    expect(countRollbacks(cap.sql)).toBe(1);
    expect(countCommits(cap.sql)).toBe(0);
  }

  it('PG: nested transaction() commits as ONE physical BEGIN/COMMIT', async () => {
    await nestedCommits(resetPg, pgConnectionPool(pgPool as never), 'postgres', readAllPg);
  });
  it('MySQL: nested transaction() commits as ONE physical BEGIN/COMMIT', async () => {
    await nestedCommits(resetMy, mysqlConnectionPool(myPool as never), 'mysql', readAllMy);
  });
  it('PG: an inner-tx error rolls back the WHOLE (outer op absent)', async () => {
    await nestedInnerErrorRollsBackWhole(resetPg, pgConnectionPool(pgPool as never), 'postgres', readAllPg);
  });
  it('MySQL: an inner-tx error rolls back the WHOLE (outer op absent)', async () => {
    await nestedInnerErrorRollsBackWhole(resetMy, mysqlConnectionPool(myPool as never), 'mysql', readAllMy);
  });
});
