/**
 * Phase A (#75) — CONCURRENT-TRANSACTION ISOLATION on live PG + MySQL (the key AC).
 *
 * Proves the per-execution connection ownership (`ExecutionContext` + `withTransactionAsync`, §3):
 * N transactions run CONCURRENTLY in ONE process on the SHARED runtime, each acquiring its OWN
 * pooled connection and pinning it in its OWN ALS scope. The assertions:
 *
 *   (1) ISOLATION — each tx's writes land ONLY in that tx; no row from tx A leaks into tx B's
 *       COMMIT and no interleaving corrupts the per-tx row set. Each of N concurrent transactions
 *       inserts a marker, holds mid-transaction (an `await` gap that forces overlap), then inserts a
 *       second row; the final table must contain EXACTLY the 2·N rows, correctly paired per worker.
 *   (2) ATOMICITY (single-statement) — a tx whose sole INSERT collides on a PK ROLLBACKs; concurrent
 *       committing transactions are unaffected (their rows all present; the aborted worker's absent).
 *   (3) ATOMICITY (MULTI-statement, production-path) — a 2-statement transaction whose 2nd statement
 *       fails: the 1st statement's write MUST be rolled back (real cross-statement atomicity through
 *       `withTransactionAsync`), and a concurrently-committed tx is unaffected. This assertion pins
 *       PRODUCTION `connectionFor` / `withTransactionAsync` ownership DIRECTLY — it swaps NOTHING —
 *       so a native port (#76-79) that breaks per-execution ownership makes this gate RED. PG + MySQL.
 *
 * MUTATION SANITY (MODEL-ONLY, not a production-path proof): a `sharedSlotPool` models the OLD
 * driver-global single-slot writer by SWAPPING the pool the ctx runs on. It does NOT exercise
 * production `connectionFor`/`withTransactionAsync` ownership — it is a self-contained sanity check
 * that a shared-connection pool collides where an owned-connection pool does not. The REAL teeth of
 * this gate are assertions (1)-(3), which run the UNMODIFIED production ownership path; the audit
 * separately confirmed reverting production ownership to a shared connection turns (1)-(3) RED.
 *
 * Requires live PG (:5433) + MySQL (:3307). Bring up: `npm run docker:livedb:up`.
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
  compileCreateManyBundle,
  executeTransactionAsync,
  PooledAsyncContext,
  pgConnectionPool,
  mysqlConnectionPool,
  type AsyncConnection,
  type AsyncConnectionPool,
  type EntityWritesDefinition,
  type In,
} from '../../src/scp';

// ── Connection config (host-published docker ports; matches docker-compose.livedb.yml) ──
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

const ISO_TBL = 'scp_tx_iso';

/**
 * Close a pool without letting teardown hang. The shared-slot mutation intentionally leaves a
 * connection in a poisoned state (a collided/abandoned transaction); a plain `pool.end()` can then
 * block waiting for it. Bounding it keeps the test from hanging — the process exits cleanly either
 * way (the docker DB is torn down by the harness).
 */
async function endPoolBounded(end: () => Promise<void>): Promise<void> {
  await Promise.race([end().catch(() => undefined), new Promise((r) => setTimeout(r, 2000))]);
}

// ── The write behavior under test: a bare INSERT command (id, worker, seq) ──────
const L = components();
class IsoCommands extends SemanticBehavior {
  Insert($: In<{ id: number; worker: number; seq: number }>) {
    return L.Insert({
      table: ISO_TBL,
      'values.id': $.id,
      'values.worker': $.worker,
      'values.seq': $.seq,
      returning: 'id, worker, seq',
    });
  }
  // A gate-first command: INSERT only if worker 999's slot is absent (existsElseRollback proof).
  GatedInsert($: In<{ id: number; worker: number; seq: number }>) {
    return L.Insert({
      table: ISO_TBL,
      'values.id': $.id,
      'values.worker': $.worker,
      'values.seq': $.seq,
      returning: 'id, worker, seq',
    });
  }
}
const contract = publishBehaviors(IsoCommands);
const insertWrites: EntityWritesDefinition = entityWrites<IsoCommands>((w) => ({ create: w.lifecycle({}) }));

let pgPool: Pool | undefined;
let myPool: mysql.Pool | undefined;

beforeAll(async () => {
  try {
    pgPool = new Pool(PG);
    await pgPool.query('SELECT 1');
  } catch (e) {
    throw new Error(`Postgres is required for the tx-isolation AC but is unreachable at ${PG.host}:${PG.port} — ${(e as Error).message}. Bring it up: npm run docker:livedb:up`);
  }
  try {
    myPool = mysql.createPool(MY);
    await myPool.query('SELECT 1');
  } catch (e) {
    throw new Error(`MySQL is required for the tx-isolation AC but is unreachable at ${MY.host}:${MY.port} — ${(e as Error).message}. Bring it up: npm run docker:livedb:up`);
  }
});

afterAll(async () => {
  await pgPool?.end().catch(() => undefined);
  await myPool?.end().catch(() => undefined);
}, 20000);

async function resetPg(): Promise<void> {
  await pgPool!.query(`DROP TABLE IF EXISTS ${ISO_TBL}`);
  await pgPool!.query(`CREATE TABLE ${ISO_TBL} (id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, seq INTEGER NOT NULL)`);
}
async function resetMy(): Promise<void> {
  await myPool!.query(`DROP TABLE IF EXISTS ${ISO_TBL}`);
  await myPool!.query(`CREATE TABLE ${ISO_TBL} (id INT PRIMARY KEY, worker INT NOT NULL, seq INT NOT NULL)`);
}
// A `seq DEFAULT 0` variant so a heterogeneous createMany (group 1 = {id,worker,seq}, group 2 =
// {id,worker}) produces TWO valid INSERT statements — the multi-statement atomicity scenario.
async function resetPgDefault(): Promise<void> {
  await pgPool!.query(`DROP TABLE IF EXISTS ${ISO_TBL}`);
  await pgPool!.query(`CREATE TABLE ${ISO_TBL} (id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, seq INTEGER NOT NULL DEFAULT 0)`);
}
async function resetMyDefault(): Promise<void> {
  await myPool!.query(`DROP TABLE IF EXISTS ${ISO_TBL}`);
  await myPool!.query(`CREATE TABLE ${ISO_TBL} (id INT PRIMARY KEY, worker INT NOT NULL, seq INT NOT NULL DEFAULT 0)`);
}

/**
 * A "shared-slot" pool modelling the OLD driver-global single-slot writer: EVERY acquire hands back
 * the SAME underlying connection (never a distinct one), wrapped so that issuing a statement while
 * ANOTHER transaction is mid-flight on the slot throws — exactly the corruption the single global
 * writer causes (two concurrent transactions cannot share one connection). Used ONLY to prove the
 * isolation test has teeth: under it the concurrent transactions collide on the slot and the run
 * FAILS (throws), where the per-execution-ownership pool passes. A real driver-global slot would
 * silently interleave BEGIN/COMMIT and destroy atomicity; this wrapper surfaces that as a hard error
 * so the mutation is deterministic (no reliance on driver-specific deadlock/queue timing).
 */
function sharedSlotPool(real: AsyncConnectionPool, barrier?: { holdOnInsert: Promise<void> }): AsyncConnectionPool & { dispose(): Promise<void> } {
  let base: AsyncConnection | null = null; // the ONE underlying connection (the global writer slot)
  let shared: AsyncConnection | null = null;
  let busy = false; // the single slot is occupied by an in-flight transaction
  let insertsHeld = 0;

  // Gate a statement on the single slot: BEGIN takes the slot (or COLLIDES if a concurrent tx holds
  // it), COMMIT/ROLLBACK frees it. A concurrent tx's statement arriving while the slot is held is
  // the exact shared-writer corruption — surfaced as a hard throw (deterministic, driver-agnostic).
  async function gate<T>(sql: string, call: () => Promise<T>): Promise<T> {
    const isBegin = /^\s*begin\b/i.test(sql);
    const isEnd = /^\s*(commit|rollback)\b/i.test(sql);
    const isInsert = /^\s*insert\b/i.test(sql);
    if (isBegin) {
      if (busy) throw new Error('shared writer slot busy: a concurrent transaction is already using the single connection');
      busy = true;
    }
    // A barrier holds the FIRST tx open at its INSERT (slot held) until released — GUARANTEEING the
    // second tx's BEGIN arrives while the slot is busy (deterministic overlap, no timing luck).
    if (isInsert && barrier !== undefined && insertsHeld === 0) {
      insertsHeld++;
      await barrier.holdOnInsert;
    }
    try {
      return await call();
    } finally {
      if (isEnd) busy = false;
    }
  }

  return {
    async acquire() {
      if (shared === null) {
        base = await real.acquire();
        shared = {
          execute: (sql, params) => gate(sql, () => base!.execute(sql, params)),
          run: (sql, params) => gate(sql, () => base!.run(sql, params)),
        };
      }
      return shared; // the single slot — every "concurrent" tx gets the SAME connection
    },
    async release() {
      /* never release the shared slot mid-run (mirrors the global writer that outlives each tx) */
    },
    // Destroy the poisoned BASE connection (an open, abandoned tx) so the pool can close cleanly.
    async dispose() {
      if (base !== null) await real.release(base, true);
      base = null;
      shared = null;
    },
  };
}

/**
 * Run N transactions CONCURRENTLY, each a 2-INSERT tx with an `await` gap between the inserts to
 * force real overlap. Worker k writes rows (id=2k, worker=k, seq=0) then (id=2k+1, worker=k, seq=1).
 * Returns the full table, sorted by id.
 */
async function runConcurrentTxs(
  ctx: PooledAsyncContext,
  dialect: 'postgres' | 'mysql',
  n: number,
  readAll: () => Promise<{ id: number; worker: number; seq: number }[]>,
): Promise<{ id: number; worker: number; seq: number }[]> {
  const bundle0 = compileWriteBundle(contract, 'Insert', insertWrites, 'create', dialect);
  await Promise.all(
    Array.from({ length: n }, (_, k) =>
      // Each worker is its OWN logical transaction. Two statements per tx would need a multi-write
      // bundle; here we run TWO single-statement txs per worker back-to-back with a yield between —
      // that still exercises concurrent overlap of N·2 transactions on the shared runtime/pool, and
      // the per-tx ownership must keep each BEGIN…COMMIT on its own connection.
      (async () => {
        const r0 = await executeTransactionAsync(ctx, bundle0.transaction!, { id: 2 * k, worker: k, seq: 0 }, dialect);
        expect(r0.committed).toBe(true);
        await new Promise((res) => setTimeout(res, 1)); // force interleave across workers
        const r1 = await executeTransactionAsync(ctx, bundle0.transaction!, { id: 2 * k + 1, worker: k, seq: 1 }, dialect);
        expect(r1.committed).toBe(true);
      })(),
    ),
  );
  return (await readAll()).sort((a, b) => a.id - b.id);
}

function expectedRows(n: number): { id: number; worker: number; seq: number }[] {
  const out: { id: number; worker: number; seq: number }[] = [];
  for (let k = 0; k < n; k++) {
    out.push({ id: 2 * k, worker: k, seq: 0 });
    out.push({ id: 2 * k + 1, worker: k, seq: 1 });
  }
  return out;
}

const N = 8;

describe('Phase A #75 — concurrent-transaction isolation (per-execution connection ownership)', () => {
  it('PG: N concurrent transactions each land ONLY their own rows (no cross-talk)', async () => {
    await resetPg();
    const ctx = new PooledAsyncContext(pgConnectionPool(pgPool as never));
    const rows = await runConcurrentTxs(ctx, 'postgres', N, async () => {
      const r = await pgPool!.query(`SELECT id, worker, seq FROM ${ISO_TBL}`);
      return r.rows.map((x) => ({ id: Number(x.id), worker: Number(x.worker), seq: Number(x.seq) }));
    });
    expect(rows).toEqual(expectedRows(N));
  });

  it('MySQL: N concurrent transactions each land ONLY their own rows (no cross-talk)', async () => {
    await resetMy();
    const ctx = new PooledAsyncContext(mysqlConnectionPool(myPool as never));
    const rows = await runConcurrentTxs(ctx, 'mysql', N, async () => {
      const [r] = await myPool!.query(`SELECT id, worker, seq FROM ${ISO_TBL}`);
      return (r as Record<string, unknown>[]).map((x) => ({ id: Number(x.id), worker: Number(x.worker), seq: Number(x.seq) }));
    });
    expect(rows).toEqual(expectedRows(N));
  });

  it('PG: a gate-failed tx ROLLBACKs atomically; concurrent committed txs are unaffected', async () => {
    await resetPg();
    // Pre-seed the row that makes ONE worker's UNIQUE INSERT collide → its whole tx must ROLLBACK.
    await pgPool!.query(`INSERT INTO ${ISO_TBL} (id, worker, seq) VALUES (0, 999, 9)`);
    const ctx = new PooledAsyncContext(pgConnectionPool(pgPool as never));
    const bundle = compileWriteBundle(contract, 'Insert', insertWrites, 'create', 'postgres');

    const outcomes = await Promise.all(
      Array.from({ length: N }, (_, k) =>
        // worker 0 collides on id=0 (pre-seeded) → PK violation → whole tx ROLLBACK + error.
        executeTransactionAsync(ctx, bundle.transaction!, { id: k, worker: k, seq: 0 }, 'postgres')
          .then((r) => ({ ok: true as const, r }))
          .catch((e) => ({ ok: false as const, e })),
      ),
    );

    // Worker 0 failed (PK collision, rolled back); workers 1..N-1 committed.
    expect(outcomes[0].ok).toBe(false);
    for (let k = 1; k < N; k++) expect(outcomes[k].ok).toBe(true);

    const r = await pgPool!.query(`SELECT id, worker FROM ${ISO_TBL} WHERE worker <> 999 ORDER BY id`);
    const got = r.rows.map((x) => ({ id: Number(x.id), worker: Number(x.worker) }));
    // The aborted worker 0's row is ABSENT (atomic rollback); every committed worker's row present.
    expect(got).toEqual(Array.from({ length: N - 1 }, (_, i) => ({ id: i + 1, worker: i + 1 })));
  });

  // ── (3) MULTI-STATEMENT atomicity, PRODUCTION-PATH — the direct ownership pin the ports inherit ──
  //
  // A heterogeneous createMany compiles to a TWO-body-statement TransactionPlan (group 1 writes id,
  // group 2 collides on a pre-seeded PK). Run through the UNMODIFIED production `executeTransactionAsync`
  // (→ `withTransactionAsync` → per-execution owned connection): the 2nd statement's PK violation MUST
  // roll back the 1st statement's already-executed INSERT (real cross-statement atomicity), and a
  // concurrently-committed single-write tx MUST be unaffected. Nothing is swapped — a port that breaks
  // per-execution ownership (BEGIN/2nd-stmt/ROLLBACK not on one owned connection) makes this go RED.

  async function multiStatementAtomicity(
    reset: () => Promise<void>,
    pool: AsyncConnectionPool,
    dialect: 'postgres' | 'mysql',
    seed: (id: number, worker: number) => Promise<void>,
    readAll: () => Promise<{ id: number; worker: number }[]>,
  ): Promise<void> {
    await reset();
    // Pre-seed id=20 so the FAILING tx's SECOND statement (id=20) collides; its FIRST statement (id=10)
    // must NOT survive the rollback.
    await seed(20, 999);
    const ctx = new PooledAsyncContext(pool);

    // The failing 2-statement tx: heterogeneous groups → 2 INSERT statements; stmt-1 = id 10 (valid),
    // stmt-2 = id 20 (PK collision). One logical transaction.
    const failing = compileCreateManyBundle(
      'CM_fail',
      {
        tableName: ISO_TBL,
        records: [{ id: 10, worker: 1, seq: 7 }, { id: 20, worker: 1 }],
        rawRecords: [{ id: 10, worker: 1, seq: 7 }, { id: 20, worker: 1 }],
      },
      dialect,
    );
    // The concurrent committing tx: a plain single INSERT (id 30) that MUST be unaffected.
    const okBundle = compileWriteBundle(contract, 'Insert', insertWrites, 'create', dialect);

    const [failOutcome, okOutcome] = await Promise.allSettled([
      executeTransactionAsync(ctx, failing.transaction!, {}, dialect),
      executeTransactionAsync(ctx, okBundle.transaction!, { id: 30, worker: 2, seq: 0 }, dialect),
    ]);

    // The failing tx must have thrown (PK collision on its 2nd statement) — NOT a silent partial commit.
    expect(failOutcome.status).toBe('rejected');
    // The concurrent tx committed.
    expect(okOutcome.status === 'fulfilled' && okOutcome.value.committed).toBe(true);

    const rows = (await readAll()).filter((r) => r.worker !== 999).sort((a, b) => a.id - b.id);
    // id 10 (the failing tx's FIRST statement) is ROLLED BACK — cross-statement atomicity. id 30 (the
    // concurrent committed tx) is present and unaffected. id 20 stays only as the pre-seed (worker 999,
    // filtered out above).
    expect(rows).toEqual([{ id: 30, worker: 2 }]);
  }

  it('PG: a 2-statement tx whose 2nd statement fails rolls back the 1st (production-path atomicity); concurrent commit unaffected', async () => {
    await multiStatementAtomicity(
      resetPgDefault,
      pgConnectionPool(pgPool as never),
      'postgres',
      async (id, worker) => void (await pgPool!.query(`INSERT INTO ${ISO_TBL} (id, worker, seq) VALUES ($1, $2, 0)`, [id, worker])),
      async () => (await pgPool!.query(`SELECT id, worker FROM ${ISO_TBL}`)).rows.map((x) => ({ id: Number(x.id), worker: Number(x.worker) })),
    );
  });

  it('MySQL: a 2-statement tx whose 2nd statement fails rolls back the 1st (production-path atomicity); concurrent commit unaffected', async () => {
    await multiStatementAtomicity(
      resetMyDefault,
      mysqlConnectionPool(myPool as never),
      'mysql',
      async (id, worker) => void (await myPool!.query(`INSERT INTO ${ISO_TBL} (id, worker, seq) VALUES (?, ?, 0)`, [id, worker])),
      async () => {
        const [r] = await myPool!.query(`SELECT id, worker FROM ${ISO_TBL}`);
        return (r as Record<string, unknown>[]).map((x) => ({ id: Number(x.id), worker: Number(x.worker) }));
      },
    );
  });

  it('MODEL-ONLY sanity (not a production-path proof): an owned-connection pool isolates two concurrent txs where a shared-slot pool collides', async () => {
    // MODEL-ONLY: this SWAPS the pool the ctx runs on (owned vs. a shared-slot model), so it does NOT
    // exercise production `connectionFor`/`withTransactionAsync` ownership — it only sanity-checks that
    // an owned-connection pool behaves differently from a shared one. The gate's REAL teeth are the
    // production-path assertions above (isolation + single-/multi-statement atomicity), which run the
    // UNMODIFIED ownership path; the audit confirmed those go RED under a faithful ownership mutation.
    //
    // The scenario: TWO transactions started concurrently, with tx-A's body held open by a barrier
    // until tx-B has begun. Under per-execution ownership each owns its own connection, so both
    // COMMIT and BOTH rows land. Under the OLD shared-slot model they contend for the ONE connection
    // and the second BEGIN collides (the shared writer cannot serve two live transactions) — proven
    // by running the IDENTICAL scenario against both pools.

    // A barrier the pool injects: tx-A's INSERT waits on `holdA` (so it holds its tx open) until
    // tx-B has issued its BEGIN. This GUARANTEES real overlap (no timing luck).
    async function scenario(
      pool: AsyncConnectionPool,
      release?: () => void,
    ): Promise<{ committedA: boolean; committedB: boolean }> {
      const ctx = new PooledAsyncContext(pool);
      const bundle = compileWriteBundle(contract, 'Insert', insertWrites, 'create', 'postgres');

      // Launch BOTH transactions concurrently. Under the owned pool each holds its OWN connection ⇒
      // both COMMIT. Under the shared slot the barrier holds tx-A open at its INSERT while tx-B's
      // BEGIN arrives ⇒ tx-B collides with the held slot (the shared-writer bug).
      const txA = executeTransactionAsync(ctx, bundle.transaction!, { id: 100, worker: 1, seq: 0 }, 'postgres');
      const txB = executeTransactionAsync(ctx, bundle.transaction!, { id: 200, worker: 2, seq: 0 }, 'postgres');
      // Once both are in flight, release the barrier so tx-A can finish (only matters for the slot).
      if (release !== undefined) setTimeout(release, 50);

      const [ra, rb] = await Promise.allSettled([txA, txB]);
      return {
        committedA: ra.status === 'fulfilled' && ra.value.committed,
        committedB: rb.status === 'fulfilled' && rb.value.committed,
      };
    }

    // ── The owned pool: both transactions are isolated → both COMMIT, both rows land. ──
    await resetPg();
    const ownedPool = new Pool({ ...PG, max: 8 });
    try {
      const owned = await scenario(pgConnectionPool(ownedPool as never));
      expect(owned).toEqual({ committedA: true, committedB: true });
      const ownedRows = (await ownedPool.query(`SELECT id FROM ${ISO_TBL} ORDER BY id`)).rows.map((x) => Number(x.id));
      expect(ownedRows).toEqual([100, 200]); // BOTH isolated txs landed
    } finally {
      await ownedPool.end().catch(() => undefined);
    }

    // ── The shared-slot mutation: the SAME two txs contend for ONE connection → tx-B collides. ──
    await resetPg();
    const mutPool = new Pool({ ...PG, max: 8 });
    let release!: () => void;
    const holdOnInsert = new Promise<void>((res) => (release = res));
    const slot = sharedSlotPool(pgConnectionPool(mutPool as never), { holdOnInsert });
    let mutationBroke = false;
    try {
      const shared = await scenario(slot, release);
      // Under the shared slot the two transactions CANNOT both commit in isolation (tx-B's BEGIN hit
      // the busy slot) — so this is NOT the clean both-committed outcome the owned pool produced.
      if (shared.committedA && shared.committedB) {
        throw new Error('shared-slot model unexpectedly isolated two concurrent transactions');
      }
      mutationBroke = true; // tx-B did NOT commit — the shared-writer collision, the bug this fixes.
    } catch {
      mutationBroke = true; // the shared slot threw — the collision the owned model avoids
    } finally {
      await slot.dispose().catch(() => undefined);
      await endPoolBounded(() => mutPool.end());
    }
    expect(mutationBroke).toBe(true);
  }, 20000);
});
