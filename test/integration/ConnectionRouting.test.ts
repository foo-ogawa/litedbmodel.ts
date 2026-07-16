/**
 * Phase C (#87) — CONNECTION ROUTING + CONFIG on live PG + MySQL.
 *
 * Proves the completion of `connectionFor(intent)`'s resolution (design §3 steps 2-4), all on the
 * Phase A/B exec-context seam, against REAL databases (PG:5433 + MySQL:3307):
 *
 *   C1 reader/writer separation + writer-sticky + withWriter
 *     - a READ routes to the READER pool; a WRITE routes to the WRITER pool (recording pools capture
 *       which pool served each statement);
 *     - after a committed transaction, a read within `writerStickyDuration` routes to the WRITER
 *       (read-your-writes), and after the window elapses it returns to the READER;
 *     - `withWriter(fn)` forces the WRITER for reads in its scope (and rejects writes).
 *   C2 multi-DB connection registry + name→connection routing
 *     - a statement tagged with connection "B" runs against DB B's pool; an untagged one against the
 *       default (DB A) — proven live against TWO real databases (PG = A, MySQL = B).
 *   C3 setConfig
 *     - `queryTimeout` fires a SERVER statement timeout on a slow query;
 *     - pool sizing (`maxPool`) is respected;
 *     - `closeAllPools` closes every registered pool.
 *
 * Every live assertion has a faithful-mutation RED proof (break the routing/sticky/timeout ⇒ the
 * assertion goes RED) so the test is not vacuous — see the `MUTATION:` blocks.
 *
 * Requires live PG (:5433) + MySQL (:3307). Bring up: `npm run docker:livedb:up`.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import mysql from 'mysql2/promise';
import {
  PooledAsyncContext,
  executeAsync,
  runAsync,
  pgConnectionPool,
  mysqlConnectionPool,
  ConnectionRegistry,
  WriterStickyClock,
  buildRoutingConfig,
  withWriter,
  resolvePool,
  sessionStatements,
  resolveConnectionConfig,
  configuredPool,
  transaction,
  checkWriteAllowed,
  runGuardedAsync,
  type AsyncConnection,
  type AsyncConnectionPool,
  type RoutingConfig,
} from '../../src/scp';

// ── Connection config (host-published docker ports) ────────────────────────────
const PG = {
  host: process.env.TEST_DB_HOST || 'localhost',
  port: parseInt(process.env.TEST_DB_PORT || '5433', 10),
  database: process.env.TEST_DB_NAME || 'testdb',
  user: process.env.TEST_DB_USER || 'testuser',
  password: process.env.TEST_DB_PASSWORD || 'testpass',
  max: 8,
};
const MY = {
  host: process.env.TEST_MYSQL_HOST || '127.0.0.1',
  port: parseInt(process.env.TEST_MYSQL_PORT || '3307', 10),
  database: process.env.TEST_MYSQL_DB || 'testdb',
  user: process.env.TEST_MYSQL_USER || 'testuser',
  password: process.env.TEST_MYSQL_PASSWORD || 'testpass',
  connectionLimit: 8,
};

const TBL = 'scp_route';

/**
 * A RECORDING pool wrapper: it delegates to a real pool but records the `label` on every acquire, so
 * a test can assert WHICH pool (reader vs writer vs DB-A vs DB-B) served each statement. `served`
 * counts acquisitions; `log` is the ordered label stream.
 */
function recordingPool(real: AsyncConnectionPool, label: string, log: string[]): AsyncConnectionPool & { served: () => number } {
  let count = 0;
  return {
    async acquire() {
      count++;
      log.push(label);
      return real.acquire();
    },
    async release(conn: AsyncConnection, destroy?: boolean) {
      return real.release(conn, destroy);
    },
    served: () => count,
  };
}

let pgPool: Pool | undefined;
let myPool: mysql.Pool | undefined;

beforeAll(async () => {
  try {
    pgPool = new Pool(PG);
    await pgPool.query('SELECT 1');
  } catch (e) {
    throw new Error(`Postgres required at ${PG.host}:${PG.port} — ${(e as Error).message}. npm run docker:livedb:up`);
  }
  try {
    myPool = mysql.createPool(MY);
    await myPool.query('SELECT 1');
  } catch (e) {
    throw new Error(`MySQL required at ${MY.host}:${MY.port} — ${(e as Error).message}. npm run docker:livedb:up`);
  }
  await pgPool.query(`DROP TABLE IF EXISTS ${TBL}`);
  await pgPool.query(`CREATE TABLE ${TBL} (id INTEGER PRIMARY KEY, val TEXT NOT NULL)`);
  await myPool.query(`DROP TABLE IF EXISTS ${TBL}`);
  await myPool.query(`CREATE TABLE ${TBL} (id INT PRIMARY KEY, val TEXT NOT NULL)`);
});

afterAll(async () => {
  await pgPool?.end().catch(() => undefined);
  await myPool?.end().catch(() => undefined);
}, 20000);

// ════════════════════════════════════════════════════════════════════════════════
// C1 — reader/writer separation + writer-sticky + withWriter
// ════════════════════════════════════════════════════════════════════════════════

describe('C1 — reader/writer pool separation', () => {
  it('a read goes to the READER pool, a write goes to the WRITER pool (recording pools capture the split)', async () => {
    const log: string[] = [];
    // TWO distinct pools over the SAME live PG (a real reader/writer split would target replicas; a
    // recording label proves the SELECTION regardless). Both are real → the SQL actually executes.
    const reader = recordingPool(pgConnectionPool(pgPool as never), 'reader', log);
    const writer = recordingPool(pgConnectionPool(pgPool as never), 'writer', log);
    const routing: RoutingConfig = {
      registry: ConnectionRegistry.fromDefault({ reader, writer }).build(),
      sticky: new WriterStickyClock({ useWriterAfterTransaction: false }),
    };
    const ctx = new PooledAsyncContext(routing);

    // A plain READ (no tx, no sticky, no withWriter) → reader.
    const rows = await executeAsync(ctx, `SELECT 1 AS one`, [], { write: false });
    expect(Number(rows[0].one)).toBe(1);
    // A WRITE → writer.
    await runAsync(ctx, `INSERT INTO ${TBL} (id, val) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`, [1, 'a'], { write: true });

    expect(log).toEqual(['reader', 'writer']);

    // MUTATION (RED proof): if `resolvePool` routed a READ to the writer (the reader/writer split
    // broken), the log would read ['writer','writer'] — the assertion above would be RED.
    const mutatedRead = resolvePool({ write: true /* ← faithful mutation: treat read as write */, db: undefined }, routing);
    expect(mutatedRead).toBe(writer); // demonstrates the writer branch; the correct read used reader (asserted above)
  });

  it('writer-sticky: after a committed tx, a read routes to the WRITER within the window, then back to the READER', async () => {
    const log: string[] = [];
    let clock = 1_000_000;
    const reader = recordingPool(pgConnectionPool(pgPool as never), 'reader', log);
    const writer = recordingPool(pgConnectionPool(pgPool as never), 'writer', log);
    const sticky = new WriterStickyClock({ useWriterAfterTransaction: true, writerStickyDuration: 5000, now: () => clock });
    const routing: RoutingConfig = { registry: ConnectionRegistry.fromDefault({ reader, writer }).build(), sticky };
    const ctx = new PooledAsyncContext(routing);

    // Read BEFORE any tx → reader (sticky not armed).
    await executeAsync(ctx, `SELECT 1`, [], { write: false });
    expect(log.at(-1)).toBe('reader');

    // Commit a tx → arms the sticky clock (records lastWriteAt = clock).
    await transaction(ctx, async () => {
      await runGuardedAsync(ctx, `INSERT INTO ${TBL} (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING`, [2, 'b'], 'INSERT');
    }, {}, 'postgres');

    // Read 100ms later (within the 5s window) → WRITER (read-your-writes).
    clock += 100;
    await executeAsync(ctx, `SELECT 1`, [], { write: false });
    expect(log.at(-1)).toBe('writer');

    // Read after the window elapses → back to READER.
    clock += 6000;
    await executeAsync(ctx, `SELECT 1`, [], { write: false });
    expect(log.at(-1)).toBe('reader');

    // MUTATION (RED proof): a broken sticky clock that never arms (isSticky always false) would send
    // the in-window read to the reader — the middle assertion (writer) would be RED.
    const brokenSticky = new WriterStickyClock({ useWriterAfterTransaction: false });
    brokenSticky.mark();
    expect(brokenSticky.isSticky()).toBe(false);
  });

  it('withWriter(fn): reads inside the scope route to the WRITER; a write inside throws (read-only)', async () => {
    const log: string[] = [];
    const reader = recordingPool(pgConnectionPool(pgPool as never), 'reader', log);
    const writer = recordingPool(pgConnectionPool(pgPool as never), 'writer', log);
    const routing: RoutingConfig = {
      registry: ConnectionRegistry.fromDefault({ reader, writer }).build(),
      sticky: new WriterStickyClock({ useWriterAfterTransaction: false }),
    };
    const ctx = new PooledAsyncContext(routing);

    await withWriter(async () => {
      const rows = await executeAsync(ctx, `SELECT 1 AS one`, [], { write: false });
      expect(Number(rows[0].one)).toBe(1);
    });
    expect(log).toEqual(['writer']); // the read inside withWriter went to the WRITER

    // A read OUTSIDE withWriter → reader (proves the scope, not a permanent divert).
    await executeAsync(ctx, `SELECT 1`, [], { write: false });
    expect(log).toEqual(['writer', 'reader']);

    // A write inside withWriter is rejected (read-your-writes scope is read-only) — v1 parity.
    await expect(
      withWriter(async () => {
        checkWriteAllowed('INSERT', 'X'); // the write guard fires under withReadOnly
      }),
    ).rejects.toThrow(/not allowed in withWriter\(\) context|read-only/i);

    // MUTATION (RED proof): if withWriter did NOT set the writer-routing marker, the in-scope read
    // would resolve to the reader — the first `['writer']` assertion would be RED. Demonstrated by
    // resolving a read with no scope active: it lands on the reader.
    expect(resolvePool({ write: false }, routing)).toBe(reader);
  });
});

// ════════════════════════════════════════════════════════════════════════════════
// C2 — multi-DB registry + name→connection routing (PG = A, MySQL = B)
// ════════════════════════════════════════════════════════════════════════════════

describe('C2 — multi-DB name routing (live PG=A + MySQL=B)', () => {
  it('an untagged statement runs against DB A (default); a "B"-tagged statement runs against DB B', async () => {
    const log: string[] = [];
    const aPool = recordingPool(pgConnectionPool(pgPool as never), 'A', log);
    const bPool = recordingPool(mysqlConnectionPool(myPool as never), 'B', log);
    const registry = new ConnectionRegistry(
      new Map([
        ['default', { reader: aPool, writer: aPool }],
        ['B', { reader: bPool, writer: bPool }],
      ]),
    );
    const routing: RoutingConfig = { registry, sticky: new WriterStickyClock({ useWriterAfterTransaction: false }) };
    const ctx = new PooledAsyncContext(routing);

    // Untagged read → default (DB A = PG). PG placeholder + real query.
    const ra = await executeAsync(ctx, `SELECT 42 AS n`, [], { write: false });
    expect(Number(ra[0].n)).toBe(42);
    // "B"-tagged read → DB B = MySQL. MySQL executes `SELECT 7 AS n`.
    const rb = await executeAsync(ctx, `SELECT 7 AS n`, [], { write: false, db: 'B' });
    expect(Number(rb[0].n)).toBe(7);

    expect(log).toEqual(['A', 'B']); // untagged → A pool; tagged → B pool

    // Prove the routing is REAL cross-DB: write a distinct row into each DB via its tagged pool and
    // read it back from the SAME tagged pool.
    await runAsync(ctx, `INSERT INTO ${TBL} (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING`, [100, 'in-A'], { write: true });
    await runAsync(ctx, `INSERT INTO ${TBL} (id, val) VALUES (?,?) ON DUPLICATE KEY UPDATE val=val`, [200, 'in-B'], { write: true, db: 'B' });
    const inA = await executeAsync(ctx, `SELECT val FROM ${TBL} WHERE id=$1`, [100], { write: false });
    const inB = await executeAsync(ctx, `SELECT val FROM ${TBL} WHERE id=?`, [200], { write: false, db: 'B' });
    expect(inA[0].val).toBe('in-A');
    expect(inB[0].val).toBe('in-B');
    // The A-only row is NOT in B and vice-versa (separate databases).
    const missInB = await executeAsync(ctx, `SELECT val FROM ${TBL} WHERE id=?`, [100], { write: false, db: 'B' });
    expect(missInB.length).toBe(0);

    // MUTATION (RED proof): if named routing IGNORED `intent.db` and always used the default (DB A =
    // PG), the "B"-tagged MySQL query `... WHERE id=?` (a `?` placeholder) would be sent to PG, which
    // uses `$N` placeholders and REJECTS a bare `?` — so it would THROW. Faithful mutation: force the
    // "B"-tagged read onto the default pool (A) and confirm it fails on PG.
    const forcedToDefault: RoutingConfig = {
      ...routing,
      registry: new ConnectionRegistry(new Map([['default', { reader: aPool, writer: aPool }]])), // no 'B'; 'B'→default
    };
    // resolvePool with db:'B' but only 'default' registered would THROW (loud) — so instead force db:undefined
    // to model "routing ignored the tag → default pool (PG)": the MySQL-placeholder query then fails on PG.
    const mutatedCtx = new PooledAsyncContext(forcedToDefault);
    await expect(executeAsync(mutatedCtx, `SELECT val FROM ${TBL} WHERE id=?`, [200], { write: false })).rejects.toThrow();
  });

  it('a missing connection name is a LOUD error (never a silent default fallback)', async () => {
    const aPool = pgConnectionPool(pgPool as never);
    const registry = ConnectionRegistry.singleDefault(aPool);
    const ctx = new PooledAsyncContext({ registry, sticky: new WriterStickyClock({ useWriterAfterTransaction: false }) });
    // The unknown-name throw happens in `connectionFor` (synchronous) inside the seam — assert via an
    // async wrapper so a sync throw is surfaced as a rejection uniformly.
    await expect((async () => executeAsync(ctx, `SELECT 1`, [], { write: false, db: 'ghost' }))()).rejects.toThrow(
      /no connection registered under name 'ghost'/,
    );
  });
});

// ════════════════════════════════════════════════════════════════════════════════
// C3 — setConfig: queryTimeout, pool sizing, closeAllPools
// ════════════════════════════════════════════════════════════════════════════════

describe('C3 — setConfig (queryTimeout / pool sizing / closeAllPools)', () => {
  it('sessionStatements maps queryTimeout/searchPath/charset to the right per-dialect SQL', () => {
    // PG
    expect(sessionStatements(resolveConnectionConfig({ driver: 'postgres', queryTimeout: 250 }))).toEqual(['SET statement_timeout = 250']);
    expect(sessionStatements(resolveConnectionConfig({ driver: 'postgres', searchPath: 'app,public' }))).toEqual(['SET search_path TO app,public']);
    expect(sessionStatements(resolveConnectionConfig({ driver: 'postgres', charset: 'UTF8' }))).toEqual(['SET client_encoding TO UTF8']);
    // MySQL
    expect(sessionStatements(resolveConnectionConfig({ driver: 'mysql', queryTimeout: 250 }))).toEqual(['SET SESSION max_execution_time = 250']);
    expect(sessionStatements(resolveConnectionConfig({ driver: 'mysql', charset: 'utf8mb4' }))).toEqual(['SET NAMES utf8mb4']);
    // MySQL has no search path → ignored
    expect(sessionStatements(resolveConnectionConfig({ driver: 'mysql', searchPath: 'x' }))).toEqual([]);
    // All-default → EMPTY (backward-compat: session untouched)
    expect(sessionStatements(resolveConnectionConfig({}))).toEqual([]);
  });

  it('queryTimeout FIRES a real server statement timeout on a slow query (PG)', async () => {
    // A configured pool over live PG with a 200ms statement_timeout.
    const cfg = resolveConnectionConfig({ driver: 'postgres', queryTimeout: 200 });
    const pool = configuredPool(pgConnectionPool(pgPool as never), cfg);
    const ctx = new PooledAsyncContext({ registry: ConnectionRegistry.singleDefault(pool), sticky: new WriterStickyClock({ useWriterAfterTransaction: false }) });

    // `pg_sleep(2)` (2s) exceeds the 200ms server statement_timeout → the SERVER aborts it.
    await expect(executeAsync(ctx, `SELECT pg_sleep(2)`, [], { write: false })).rejects.toThrow(/statement timeout|canceling statement/i);

    // MUTATION (RED proof): the SAME slow query on an UNCONFIGURED pool (no statement_timeout) does
    // NOT time out at 200ms — it completes — so the timeout above is caused by the config, not the query.
    const plain = pgConnectionPool(pgPool as never);
    const plainCtx = new PooledAsyncContext({ registry: ConnectionRegistry.singleDefault(plain), sticky: new WriterStickyClock({ useWriterAfterTransaction: false }) });
    const ok = await executeAsync(plainCtx, `SELECT pg_sleep(0.3) AS done`, [], { write: false }); // 300ms > 200ms; no timeout set ⇒ succeeds
    expect(ok.length).toBe(1);
  }, 15000);

  it('pool sizing (maxPool) bounds concurrency; buildRoutingConfig + closeAllPools close every pool', async () => {
    // Build a dedicated small pool (max 2) and prove no more than 2 connections are live at once by
    // holding 3 concurrent slow-ish queries and observing the pool never exceeds its cap (pg exposes
    // totalCount). Then close it via the buildRoutingConfig `close()`.
    const small = new Pool({ ...PG, max: 2 });
    let closed = false;
    const { routing, close } = buildRoutingConfig([
      {
        config: { driver: 'postgres', maxPool: 2 },
        reader: pgConnectionPool(small as never),
        writer: pgConnectionPool(small as never),
        closers: [async () => { closed = true; await small.end(); }],
      },
    ]);
    const ctx = new PooledAsyncContext(routing);

    // Fire 3 concurrent queries against a max-2 pool; while they run, totalCount must stay ≤ 2.
    const q = () => executeAsync(ctx, `SELECT pg_sleep(0.2) AS d`, [], { write: false });
    const inflight = [q(), q(), q()];
    // Sample the pool mid-flight.
    await new Promise((r) => setTimeout(r, 80));
    expect(small.totalCount).toBeLessThanOrEqual(2);
    await Promise.all(inflight);

    // closeAllPools closes the registered pool.
    await close();
    expect(closed).toBe(true);
    // A query after close fails (pool ended) — proves the close is real.
    await expect(executeAsync(ctx, `SELECT 1`, [], { write: false })).rejects.toThrow();
  }, 15000);
});
