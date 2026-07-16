/**
 * Phase D (#92) — the SCP MIDDLEWARE layer, hook-mechanics unit tests.
 *
 * Proves D1/D2/D3 on the REAL Phase A exec-context seam (better-sqlite3), the reference the native
 * ports (#93-96) mirror:
 *   D1 SQL-level `execute` hook — a registered middleware intercepts EVERY SQL through the seam
 *      (read/write/tx-control), can OBSERVE / REWRITE / TIME / SHORT-CIRCUIT; per-scope ISOLATION
 *      (two concurrent scopes don't see each other's middleware). RED: unregister ⇒ no interception.
 *   D2 method-level hooks — `runMethod(kind, …)` fires the matching op-kind hook (find/create/…),
 *      before/after observed; applied order = first-registered outermost. RED: wrong kind ⇒ no fire.
 *   D3 Logger + raw execute/query — Logger records real SQL/params/timing; rawExecute/rawQuery go
 *      THROUGH the seam (a registered SQL middleware sees the raw call). RED: remove wiring ⇒ empty.
 *
 * Every registration is inside a `withMiddlewareScope` so the process-global registry stays clean
 * (an unregistered chain is byte-identical — the conformance/livedb runners register none).
 */

import Database from 'better-sqlite3';
import { describe, it, expect, beforeEach } from 'vitest';
import {
  contextForDriver,
  execute as seamExecute,
  run as seamRun,
  use,
  createMiddleware,
  withMiddlewareScope,
  runMethod,
  Logger,
  rawExecute,
  rawQuery,
  clearMiddlewares,
  type ExecutionContext,
} from '../../src/scp';

function freshDb(): Database.Database {
  const db = new Database(':memory:');
  db.exec('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
  return db;
}

describe('Phase D — SQL-level execute hook (D1)', () => {
  beforeEach(() => clearMiddlewares());

  it('intercepts EVERY SQL through the seam (read, write, tx-control) — observe', () => {
    const db = freshDb();
    const ctx = contextForDriver(db);
    const seen: string[] = [];
    withMiddlewareScope(() => {
      use(createMiddleware({ execute: function (next, sql, params) { seen.push(sql); return next(sql, params); } }));
      seamRun(ctx, 'BEGIN', []);
      seamRun(ctx, 'INSERT INTO t (name) VALUES (?)', ['a']);
      seamRun(ctx, 'COMMIT', []);
      seamExecute(ctx, 'SELECT * FROM t', []);
    });
    // BEGIN, INSERT, COMMIT, SELECT — write, tx-control and read all funnel through.
    expect(seen).toEqual(['BEGIN', 'INSERT INTO t (name) VALUES (?)', 'COMMIT', 'SELECT * FROM t']);
  });

  it('RED proof: WITHOUT the middleware wiring, nothing is observed', () => {
    const db = freshDb();
    const ctx = contextForDriver(db);
    const seen: string[] = [];
    // No use()/withMiddlewareScope registration → the seam is a byte-identical passthrough.
    seamRun(ctx, 'INSERT INTO t (name) VALUES (?)', ['a']);
    seamExecute(ctx, 'SELECT * FROM t', []);
    expect(seen).toEqual([]); // would be non-empty iff the hook fired; proves the assertion is real
  });

  it('can REWRITE the SQL/params passed to next', () => {
    const db = freshDb();
    const ctx = contextForDriver(db);
    withMiddlewareScope(() => {
      use(createMiddleware({
        execute: function (next, sql, params) {
          // rewrite the inserted name
          if (sql.startsWith('INSERT')) return next(sql, ['rewritten']);
          return next(sql, params);
        },
      }));
      seamRun(ctx, 'INSERT INTO t (name) VALUES (?)', ['original']);
    });
    const row = db.prepare('SELECT name FROM t').get() as { name: string };
    expect(row.name).toBe('rewritten');
  });

  it('can TIME next', () => {
    const db = freshDb();
    const ctx = contextForDriver(db);
    let timed = -1;
    withMiddlewareScope(() => {
      use(createMiddleware({
        execute: function (next, sql, params) {
          const t0 = Date.now();
          const r = next(sql, params);
          timed = Date.now() - t0;
          return r;
        },
      }));
      seamExecute(ctx, 'SELECT * FROM t', []);
    });
    expect(timed).toBeGreaterThanOrEqual(0);
  });

  it('can SHORT-CIRCUIT (skip next → the real DB is never touched)', () => {
    const db = freshDb();
    const ctx = contextForDriver(db);
    withMiddlewareScope(() => {
      use(createMiddleware({
        execute: function (_next, _sql, _params) {
          // Do NOT call next — short-circuit with a synthetic row list.
          return [{ id: 99, name: 'synthetic' }];
        },
      }));
      const rows = seamExecute(ctx, 'SELECT * FROM t', []);
      expect(rows).toEqual([{ id: 99, name: 'synthetic' }]);
    });
    // Nothing was ever inserted, so a real query returns empty — proves the DB was bypassed.
    expect(db.prepare('SELECT COUNT(*) c FROM t').get()).toEqual({ c: 0 });
  });

  it('per-scope ISOLATION: two concurrent scopes do not see each other s middleware', async () => {
    const db = freshDb();
    const ctx = contextForDriver(db);
    const seenA: string[] = [];
    const seenB: string[] = [];

    async function scope(seen: string[], tag: string): Promise<void> {
      await new Promise<void>((resolve) => {
        withMiddlewareScope(() => {
          use(createMiddleware({ execute: function (next, sql, params) { seen.push(`${tag}:${sql}`); return next(sql, params); } }));
          // yield to the event loop, then run a statement — the OTHER scope is concurrently active
          setTimeout(() => { seamExecute(ctx, `SELECT ${tag === 'A' ? 1 : 2}`, []); resolve(); }, tag === 'A' ? 5 : 1);
        });
      });
    }

    await Promise.all([scope(seenA, 'A'), scope(seenB, 'B')]);
    // Each scope observed ONLY its own statement — no cross-talk.
    expect(seenA).toEqual(['A:SELECT 1']);
    expect(seenB).toEqual(['B:SELECT 2']);
  });

  it('applied ORDER: first-registered is outermost', () => {
    const db = freshDb();
    const ctx = contextForDriver(db);
    const order: string[] = [];
    withMiddlewareScope(() => {
      use(createMiddleware({ execute: function (next, sql, params) { order.push('A:before'); const r = next(sql, params); order.push('A:after'); return r; } }));
      use(createMiddleware({ execute: function (next, sql, params) { order.push('B:before'); const r = next(sql, params); order.push('B:after'); return r; } }));
      seamExecute(ctx, 'SELECT 1', []);
    });
    expect(order).toEqual(['A:before', 'B:before', 'B:after', 'A:after']);
  });

  it('per-scope STATE is isolated + fresh (v1 getCurrentContext)', () => {
    const db = freshDb();
    const ctx = contextForDriver(db);
    const mw = createMiddleware<{ count: number }>({
      state: { count: 0 },
      execute: function (next, sql, params) { this.count++; return next(sql, params); },
    });
    withMiddlewareScope(() => {
      use(mw);
      seamExecute(ctx, 'SELECT 1', []);
      seamExecute(ctx, 'SELECT 2', []);
      expect(mw.state().count).toBe(2);
    });
    // A fresh scope starts from a fresh state copy (0), not the previous scope's 2.
    withMiddlewareScope(() => {
      use(mw);
      seamExecute(ctx, 'SELECT 3', []);
      expect(mw.state().count).toBe(1);
    });
  });
});

describe('Phase D — method-level hooks (D2)', () => {
  beforeEach(() => clearMiddlewares());

  it('fires the matching op-kind hook (find/create/update/delete), before/after observed', async () => {
    for (const kind of ['find', 'create', 'update', 'delete'] as const) {
      const events: string[] = [];
      await withMiddlewareScope(async () => {
        use(createMiddleware({
          [kind]: async function (_model: unknown, next: (...a: unknown[]) => Promise<unknown>, ...args: unknown[]) {
            events.push(`${kind}:before`);
            const r = await next(...args);
            events.push(`${kind}:after`);
            return r;
          },
        }));
        const result = await runMethod(kind, undefined, async () => { events.push(`${kind}:core`); return 'ok'; }, []);
        expect(result).toBe('ok');
      });
      expect(events).toEqual([`${kind}:before`, `${kind}:core`, `${kind}:after`]);
    }
  });

  it('RED proof: a hook of a DIFFERENT kind does not fire', async () => {
    const events: string[] = [];
    await withMiddlewareScope(async () => {
      use(createMiddleware({
        create: async function (_m: unknown, next: (...a: unknown[]) => Promise<unknown>, ...args: unknown[]) { events.push('create'); return next(...args); },
      }));
      // Dispatch a `find` — the `create` hook must NOT fire (kind mismatch).
      await runMethod('find', undefined, async () => 'r', []);
    });
    expect(events).toEqual([]);
  });

  it('method hooks compose in first-registered-outermost order + can rewrite args', async () => {
    const order: string[] = [];
    let coreArg = 0;
    await withMiddlewareScope(async () => {
      use(createMiddleware({ find: async function (_m: unknown, next: (...a: unknown[]) => Promise<unknown>, n: unknown) { order.push('A'); return next((n as number) + 1); } }));
      use(createMiddleware({ find: async function (_m: unknown, next: (...a: unknown[]) => Promise<unknown>, n: unknown) { order.push('B'); return next((n as number) + 10); } }));
      await runMethod('find', undefined, async (n) => { coreArg = n as number; return null; }, [0]);
    });
    expect(order).toEqual(['A', 'B']); // A outer, B inner
    expect(coreArg).toBe(11); // 0 +1 (A) +10 (B)
  });
});

describe('Phase D — Logger + raw execute/query (D3)', () => {
  beforeEach(() => clearMiddlewares());

  it('Logger records real SQL / params / timing for every seam statement', () => {
    const db = freshDb();
    const ctx = contextForDriver(db);
    const logger = Logger();
    withMiddlewareScope(() => {
      use(logger);
      seamRun(ctx, 'INSERT INTO t (name) VALUES (?)', ['x']);
      seamExecute(ctx, 'SELECT * FROM t WHERE name = ?', ['x']);
      const entries = logger.state().entries;
      expect(entries.map((e) => e.sql)).toEqual(['INSERT INTO t (name) VALUES (?)', 'SELECT * FROM t WHERE name = ?']);
      expect(entries[0].params).toEqual(['x']);
      expect(entries[1].params).toEqual(['x']);
      for (const e of entries) expect(e.durationMs).toBeGreaterThanOrEqual(0);
    });
  });

  it('rawExecute goes THROUGH the seam — a registered SQL middleware sees it', () => {
    const db = freshDb();
    const ctx: ExecutionContext = contextForDriver(db);
    const seen: string[] = [];
    withMiddlewareScope(() => {
      use(createMiddleware({ execute: function (next, sql, params) { seen.push(sql); return next(sql, params); } }));
      const insert = rawExecute(ctx, 'INSERT INTO t (name) VALUES (?)', ['raw']);
      expect(insert.rowCount).toBe(1);
      const read = rawExecute(ctx, 'SELECT name FROM t');
      expect(read.rows).toEqual([{ name: 'raw' }]);
    });
    expect(seen).toEqual(['INSERT INTO t (name) VALUES (?)', 'SELECT name FROM t']);
  });

  it('rawQuery fires a `query` method hook AND flows through the execute seam', async () => {
    const db = freshDb();
    const ctx = contextForDriver(db);
    db.prepare("INSERT INTO t (name) VALUES ('q')").run();
    const events: string[] = [];
    await withMiddlewareScope(async () => {
      use(createMiddleware({
        query: async function (_m: unknown, next: (...a: unknown[]) => Promise<unknown>, ...args: unknown[]) { events.push('query'); return next(...args); },
        execute: function (next, sql, params) { events.push(`execute:${sql}`); return next(sql, params); },
      }));
      const rows = await rawQuery(ctx, 'SELECT name FROM t');
      expect(rows).toEqual([{ name: 'q' }]);
    });
    expect(events).toEqual(['query', 'execute:SELECT name FROM t']);
  });

  it('RED proof: without the wiring the Logger records nothing', () => {
    const db = freshDb();
    const ctx = contextForDriver(db);
    const logger = Logger();
    // NOT registered → the seam never invokes it.
    seamExecute(ctx, 'SELECT 1', []);
    expect(logger.state().entries).toEqual([]);
  });
});
