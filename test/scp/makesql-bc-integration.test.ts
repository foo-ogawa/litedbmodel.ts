/**
 * makeSQL × behavior-contracts integration + real better-sqlite3 execution.
 *
 * Proves the LOCKED model's bc claim (spec §11): the `makeSQL` catalog leaf rides bc's
 * `runBehavior` — bc does composition / value-eval / plan / envelope; litedbmodel
 * supplies ONLY the catalog entry + handler + compile. We:
 *
 *   1. build the single-`makeSQL` component IR and pass it through bc's Portability
 *      Guard (`assertPortableComponentGraph`) — the graph is portable;
 *   2. run it on the shared `runBehavior` with the `makeSQL` handler bound to a REAL
 *      better-sqlite3 driver, and assert the rows match a direct query (result parity);
 *   3. exercise nested-`makeSQL` subquery params (splice) and `skip` (drop) end-to-end.
 */

/* eslint-disable @typescript-eslint/no-explicit-any -- bc runtime/driver seams need casts */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import Database from 'better-sqlite3';
import { runBehavior, assertPortableComponentGraph, type Handlers } from 'behavior-contracts';
import {
  compileSelect,
  compileWhere,
  makeSqlComponentIR,
  makeSqlInput,
  makeSqlHandlerSync,
  assembleMakeSQL,
  renderPlaceholders,
  MAKESQL,
  type MakeSQL,
} from '../../src/scp/makesql';

let db: Database.Database;

beforeAll(() => {
  db = new Database(':memory:');
  db.exec(`
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, status TEXT);
    CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER);
    INSERT INTO users (id, name, status) VALUES (1, 'alice', 'active'), (2, 'bob', 'inactive'), (3, 'carol', 'active');
    INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100), (11, 1, 50), (12, 3, 200);
  `);
});
afterAll(() => db.close());

/** The sync better-sqlite3 executor seam. */
function sqliteExec(sql: string, params: unknown[]): Record<string, unknown>[] {
  return db.prepare(sql).all(...(params as any[])) as Record<string, unknown>[];
}

/** Run a compiled makeSQL bundle through bc's runBehavior with the makeSQL handler. */
function runViaBc(node: MakeSQL): Record<string, unknown>[] {
  const ir = makeSqlComponentIR('Query');
  assertPortableComponentGraph(ir); // bc Portability Guard passes on our graph.
  const handlers: Handlers = { [MAKESQL]: makeSqlHandlerSync(sqliteExec, 'sqlite') };
  const out = runBehavior(ir, handlers, makeSqlInput(node) as any, 'Query');
  return out as unknown as Record<string, unknown>[];
}

describe('makeSQL rides behavior-contracts runBehavior (SQLite real exec)', () => {
  it('SELECT + WHERE composes, value-evals, executes — rows match a direct query', () => {
    const node = compileSelect({
      dialect: 'sqlite',
      tableName: 'users',
      conditions: { status: 'active' },
      order: 'id ASC',
    });
    const viaBc = runViaBc(node);

    // Direct reference query (byte-identical SQL/params via assemble+render).
    const asm = assembleMakeSQL(node);
    const direct = sqliteExec(renderPlaceholders(asm.sql, 'sqlite'), asm.params);

    expect(viaBc).toEqual(direct);
    expect(viaBc.map((r) => r.name)).toEqual(['alice', 'carol']);
  });

  it('nested-makeSQL subquery param splices its SQL + params inline', () => {
    // Outer: SELECT * FROM users WHERE id IN (<subquery>) — the subquery is a NESTED
    // makeSQL in a param slot (the ONLY recursion). No IR "kind" for subqueries.
    const sub: MakeSQL = { sql: 'SELECT user_id FROM orders WHERE total >= ?', params: [100] };
    const outer: MakeSQL = { sql: 'SELECT * FROM users WHERE id IN (?) ORDER BY id ASC', params: [sub] };

    const asm = assembleMakeSQL(outer);
    expect(asm.sql).toBe('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total >= ?) ORDER BY id ASC');
    expect(asm.params).toEqual([100]);

    const rows = runViaBc(outer);
    expect(rows.map((r) => r.name)).toEqual(['alice', 'carol']); // users with an order >= 100
  });

  it('skip drops the component entirely (sql AND params) — handler contributes nothing', () => {
    const skipped: MakeSQL = { sql: 'SELECT * FROM users', params: [], skip: true };
    const rows = runViaBc(skipped);
    expect(rows).toEqual([]); // skipped ⇒ no execution, empty result

    // And an optional WHERE member that is skipped contributes no text/param:
    const optional = compileWhere({ status: 'active' }, 'sqlite');
    const asm = assembleMakeSQL({ ...optional, skip: true });
    expect(asm).toEqual({ sql: '', params: [] });
  });

  it('the makeSQL IR is a single catalog leaf referencing only the makeSQL component', () => {
    const ir = makeSqlComponentIR('Query');
    expect(ir.components).toHaveLength(1);
    const body = ir.components[0].body as any[];
    expect(body).toHaveLength(1);
    expect(body[0].component).toBe(MAKESQL); // the ONLY catalog vocabulary
  });
});
