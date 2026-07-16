/**
 * Phase B-1 (#81) — tx-completeness on LIVE PG + MySQL: REAL behavioral proofs (not string checks).
 *
 *   (A) ISOLATION (live) — under `repeatable read`, a re-read inside ONE tx is STABLE while a
 *       concurrent tx commits a change (the concurrent change is NOT visible); under `read committed`
 *       the same re-read DOES see the concurrent commit. Real snapshot behavior, driven through
 *       `withTransactionAsync` + `executeAsync` on the tx-owned connection.
 *
 *   (B) RETRY on REAL contention (live) — two concurrent transactions induce a genuine
 *       deadlock / serialization_failure (PG SERIALIZABLE write-skew → 40001; MySQL opposite-order
 *       row locks → 1213). The retry loop re-runs the loser and it eventually SUCCEEDS; a
 *       NON-retryable error (unique violation) is NOT retried.
 *
 * Builds on the Phase A ExecutionContext ownership (`PooledAsyncContext` / `withTransactionAsync`).
 * Requires live PG (:5433) + MySQL (:3307): `npm run docker:livedb:up`.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import mysql from 'mysql2/promise';
import {
  PooledAsyncContext,
  pgConnectionPool,
  mysqlConnectionPool,
  withTransactionAsync,
  executeAsync,
  runAsync,
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

const TBL = 'scp_tx_complete';

let pgPool: Pool | undefined;
let myPool: mysql.Pool | undefined;

beforeAll(async () => {
  try {
    pgPool = new Pool(PG);
    await pgPool.query('SELECT 1');
  } catch (e) {
    throw new Error(`Postgres required for #81 tx-completeness at ${PG.host}:${PG.port} — ${(e as Error).message}. npm run docker:livedb:up`);
  }
  try {
    myPool = mysql.createPool(MY);
    await myPool.query('SELECT 1');
  } catch (e) {
    throw new Error(`MySQL required for #81 tx-completeness at ${MY.host}:${MY.port} — ${(e as Error).message}. npm run docker:livedb:up`);
  }
});

afterAll(async () => {
  await pgPool?.end().catch(() => undefined);
  await myPool?.end().catch(() => undefined);
}, 20000);

const delay = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

// ── (A) ISOLATION — repeatable read is stable; read committed sees the concurrent commit ──

describe('#81 isolation level (LIVE) — repeatable read snapshot vs. read committed', () => {
  async function resetPg(): Promise<void> {
    await pgPool!.query(`DROP TABLE IF EXISTS ${TBL}`);
    await pgPool!.query(`CREATE TABLE ${TBL} (id INT PRIMARY KEY, val INT NOT NULL)`);
    await pgPool!.query(`INSERT INTO ${TBL} (id, val) VALUES (1, 100)`);
  }
  async function resetMy(): Promise<void> {
    await myPool!.query(`DROP TABLE IF EXISTS ${TBL}`);
    await myPool!.query(`CREATE TABLE ${TBL} (id INT PRIMARY KEY, val INT NOT NULL) ENGINE=InnoDB`);
    await myPool!.query(`INSERT INTO ${TBL} (id, val) VALUES (1, 100)`);
  }

  /**
   * Inside ONE tx at `level`: read val, then (after a concurrent committed UPDATE lands) re-read val.
   * Returns [firstRead, secondRead]. Under repeatable read both reads see 100; under read committed
   * the second read sees the concurrently-committed 200.
   */
  async function readReadWithConcurrentCommit(
    ctx: PooledAsyncContext,
    dialect: 'postgres' | 'mysql',
    level: 'repeatable read' | 'read committed',
    concurrentUpdate: () => Promise<void>,
  ): Promise<[number, number]> {
    const gateOpened = { v: false };
    const result = await withTransactionAsync(
      ctx,
      async (txCtx) => {
        const r1 = await executeAsync(txCtx, `SELECT val FROM ${TBL} WHERE id = 1`, [], { write: true });
        const first = Number(r1[0].val);
        // Now let a CONCURRENT tx UPDATE+COMMIT val=200 while THIS tx is still open.
        await concurrentUpdate();
        gateOpened.v = true;
        const r2 = await executeAsync(txCtx, `SELECT val FROM ${TBL} WHERE id = 1`, [], { write: true });
        const second = Number(r2[0].val);
        return [first, second] as [number, number];
      },
      { isolation: level, retryOnError: false },
      dialect,
    );
    expect(gateOpened.v).toBe(true);
    return result;
  }

  it('PG repeatable read: the re-read is STABLE (concurrent commit NOT visible)', async () => {
    await resetPg();
    const ctx = new PooledAsyncContext(pgConnectionPool(pgPool as never));
    const [first, second] = await readReadWithConcurrentCommit(ctx, 'postgres', 'repeatable read', async () => {
      await pgPool!.query(`UPDATE ${TBL} SET val = 200 WHERE id = 1`);
    });
    expect(first).toBe(100);
    expect(second).toBe(100); // snapshot held — the concurrent commit is INVISIBLE
    // ...but the committed value is really 200 now (proving the concurrent UPDATE truly committed).
    expect(Number((await pgPool!.query(`SELECT val FROM ${TBL} WHERE id=1`)).rows[0].val)).toBe(200);
  });

  it('PG read committed: the re-read SEES the concurrent commit', async () => {
    await resetPg();
    const ctx = new PooledAsyncContext(pgConnectionPool(pgPool as never));
    const [first, second] = await readReadWithConcurrentCommit(ctx, 'postgres', 'read committed', async () => {
      await pgPool!.query(`UPDATE ${TBL} SET val = 200 WHERE id = 1`);
    });
    expect(first).toBe(100);
    expect(second).toBe(200); // read committed — the concurrent commit is VISIBLE
  });

  it('MySQL repeatable read: the re-read is STABLE (concurrent commit NOT visible)', async () => {
    await resetMy();
    const ctx = new PooledAsyncContext(mysqlConnectionPool(myPool as never));
    const [first, second] = await readReadWithConcurrentCommit(ctx, 'mysql', 'repeatable read', async () => {
      await myPool!.query(`UPDATE ${TBL} SET val = 200 WHERE id = 1`);
    });
    expect(first).toBe(100);
    expect(second).toBe(100); // InnoDB repeatable-read consistent snapshot
    expect(Number(((await myPool!.query(`SELECT val FROM ${TBL} WHERE id=1`))[0] as Record<string, unknown>[])[0].val)).toBe(200);
  });

  it('MySQL read committed: the re-read SEES the concurrent commit', async () => {
    await resetMy();
    const ctx = new PooledAsyncContext(mysqlConnectionPool(myPool as never));
    const [first, second] = await readReadWithConcurrentCommit(ctx, 'mysql', 'read committed', async () => {
      await myPool!.query(`UPDATE ${TBL} SET val = 200 WHERE id = 1`);
    });
    expect(first).toBe(100);
    expect(second).toBe(200);
  });
});

// ── (B) RETRY on REAL contention ──────────────────────────────────────────────

describe('#81 tx retry (LIVE) — real induced contention retries and succeeds', () => {
  it('PG SERIALIZABLE write-skew (40001): the serialization-failure loser RETRIES and succeeds', async () => {
    // Two accounts; the classic write-skew: each tx reads the SUM of both, then decrements one, with
    // a business rule "sum must stay >= 0". Under SERIALIZABLE, concurrent execution triggers 40001
    // on one tx. The retry loop re-runs it (now seeing the other's commit) and it succeeds.
    await pgPool!.query(`DROP TABLE IF EXISTS ${TBL}`);
    await pgPool!.query(`CREATE TABLE ${TBL} (id INT PRIMARY KEY, val INT NOT NULL)`);
    await pgPool!.query(`INSERT INTO ${TBL} (id, val) VALUES (1, 100), (2, 100)`);
    const ctx = new PooledAsyncContext(pgConnectionPool(pgPool as never));

    let attemptsTx1 = 0;
    let attemptsTx2 = 0;
    const barrier = { firstRead: undefined as undefined | (() => void), gate: null as null | Promise<void> };
    barrier.gate = new Promise<void>((res) => (barrier.firstRead = res));

    // Both txs SELECT both rows (building the read-set), pause for overlap, then UPDATE their own row.
    // SERIALIZABLE detects the read/write dependency cycle and aborts one with 40001.
    async function transfer(which: 1 | 2, count: () => void): Promise<void> {
      await withTransactionAsync(
        ctx,
        async (txCtx) => {
          count();
          const rows = await executeAsync(txCtx, `SELECT id, val FROM ${TBL} ORDER BY id`, [], { write: true });
          const total = rows.reduce((s, r) => s + Number(r.val), 0);
          await delay(60); // hold the snapshot so BOTH txs read before EITHER writes → the cycle
          // decrement this tx's own row (write-skew: the decision used the shared read-set)
          await runAsync(txCtx, `UPDATE ${TBL} SET val = val - 10 WHERE id = ${which} AND ${total} >= 0`, []);
        },
        { isolation: 'serializable', retryLimit: 5, retryDuration: 20 },
        'postgres',
      );
    }

    // Run both concurrently — one WILL hit 40001 and must be retried by the loop.
    await Promise.all([transfer(1, () => attemptsTx1++), transfer(2, () => attemptsTx2++)]);

    // At least one tx ran more than once (it was retried after a 40001). Both ultimately committed.
    expect(attemptsTx1 + attemptsTx2).toBeGreaterThan(2);
    const rows = (await pgPool!.query(`SELECT id, val FROM ${TBL} ORDER BY id`)).rows;
    // Both decrements landed (each tx eventually committed its -10).
    expect(rows.map((r) => Number(r.val))).toEqual([90, 90]);
  }, 30000);

  it('MySQL deadlock (1213): opposite-order row locks deadlock, the loser RETRIES and succeeds', async () => {
    await myPool!.query(`DROP TABLE IF EXISTS ${TBL}`);
    await myPool!.query(`CREATE TABLE ${TBL} (id INT PRIMARY KEY, val INT NOT NULL) ENGINE=InnoDB`);
    await myPool!.query(`INSERT INTO ${TBL} (id, val) VALUES (1, 0), (2, 0)`);
    const ctx = new PooledAsyncContext(mysqlConnectionPool(myPool as never));

    let attemptsA = 0;
    let attemptsB = 0;

    // txA locks row 1 then row 2; txB locks row 2 then row 1 — the classic opposite-order deadlock.
    // InnoDB aborts ONE with errno 1213; the retry loop re-runs it (locks now free) → success.
    async function lockInOrder(first: number, second: number, count: () => void): Promise<void> {
      await withTransactionAsync(
        ctx,
        async (txCtx) => {
          count();
          await runAsync(txCtx, `UPDATE ${TBL} SET val = val + 1 WHERE id = ${first}`, []);
          await delay(60); // hold row `first` locked so the other tx grabs `second` first → deadlock
          await runAsync(txCtx, `UPDATE ${TBL} SET val = val + 1 WHERE id = ${second}`, []);
        },
        { isolation: 'repeatable read', retryLimit: 5, retryDuration: 20 },
        'mysql',
      );
    }

    await Promise.all([lockInOrder(1, 2, () => attemptsA++), lockInOrder(2, 1, () => attemptsB++)]);

    expect(attemptsA + attemptsB).toBeGreaterThan(2); // a deadlock forced a retry
    const [rows] = await myPool!.query(`SELECT id, val FROM ${TBL} ORDER BY id`);
    // Both txs eventually incremented both rows once each ⇒ every row = 2.
    expect((rows as Record<string, unknown>[]).map((r) => Number(r.val))).toEqual([2, 2]);
  }, 30000);

  it('PG: a NON-retryable unique violation does NOT retry (fails on the first attempt)', async () => {
    await pgPool!.query(`DROP TABLE IF EXISTS ${TBL}`);
    await pgPool!.query(`CREATE TABLE ${TBL} (id INT PRIMARY KEY, val INT NOT NULL)`);
    await pgPool!.query(`INSERT INTO ${TBL} (id, val) VALUES (1, 100)`);
    const ctx = new PooledAsyncContext(pgConnectionPool(pgPool as never));

    let attempts = 0;
    await expect(
      withTransactionAsync(
        ctx,
        async (txCtx) => {
          attempts++;
          await runAsync(txCtx, `INSERT INTO ${TBL} (id, val) VALUES (1, 999)`, []); // PK collision → 23505
        },
        { retryLimit: 5, retryDuration: 5 },
        'postgres',
      ),
    ).rejects.toThrow();
    expect(attempts).toBe(1); // NOT retried — a data conflict is not retryable
  }, 15000);
});
