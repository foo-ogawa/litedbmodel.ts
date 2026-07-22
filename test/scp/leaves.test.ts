/**
 * Unit coverage for the op-independent runtime leaves (`src/scp/leaves.ts`, #141): the
 * `executeSQL`/`pluck`/`group` transport+util leaves that replace the retired per-op catalog.
 *
 * Exercises the REAL module through bc `compileBehaviors`/`bindBehaviors` against a fake
 * {@link SyncConnection} (the central seam's driver contact) — no native driver, so it runs in any
 * env (better-sqlite3 is arch-gated here). It pins the load-bearing transport behavior:
 *   - a relation reads N+1-free (parents → pluck → children WHERE fk = ANY($1) → group) = 2 queries;
 *   - the key array binds as ONE param: MySQL/SQLite JSON-encoded, PostgreSQL a raw array;
 *   - the deferred PG cast resolves from the real keys (`::int[]`, v1 byte-parity) + `?`→`$N` render;
 *   - a write routes through the `run` seam and returns the `[{changes,lastInsertRowid}]` summary.
 */

import { test, expect } from 'vitest';
import { compileBehaviors, bindBehaviors, SemanticBehavior } from 'behavior-contracts';
import { leafComponents } from '../../src/scp/leaves';
import { contextForConnection, type SyncConnection, type Rows, type RunInfo } from '../../src/scp/exec-context';

const L = leafComponents as unknown as Record<string, (ports: Record<string, unknown>) => { as(t: unknown): unknown } & Record<string, unknown>>;

interface Call { kind: 'execute' | 'executeSafe' | 'run'; sql: string; params: unknown[] }

function recordingConn(calls: Call[]): SyncConnection {
  const rows = (sql: string): Rows => {
    if (sql.includes('FROM posts')) return [{ id: 1, author_id: 10 }, { id: 2, author_id: 10 }, { id: 3, author_id: 20 }];
    if (sql.includes('FROM users')) return [{ id: 10, name: 'A' }, { id: 20, name: 'B' }];
    return [];
  };
  return {
    execute(sql, params) { calls.push({ kind: 'execute', sql, params: [...params] }); return rows(sql); },
    executeSafeIntegers(sql, params) { calls.push({ kind: 'executeSafe', sql, params: [...params] }); return rows(sql); },
    run(sql, params) { calls.push({ kind: 'run', sql, params: [...params] }); return { changes: 1, lastInsertRowid: 42 } as RunInfo; },
  };
}

test('relation read is N+1-free (2 queries) with JSON key param on sqlite + belongsTo grouping', () => {
  const calls: Call[] = [];
  const ctx = { exec: contextForConnection(recordingConn(calls)), dialect: 'sqlite' as const };
  const cls = class extends SemanticBehavior {
    Find() {
      const posts = L.executeSQL({ sql: 'SELECT id, author_id FROM posts', params: [], write: false, returning: false, bigint: false });
      const ids = L.pluck({ rows: posts, col: 'author_id' }).as({ arr: 'float' });
      const authors = L.executeSQL({ sql: 'SELECT id, name FROM users WHERE id IN (SELECT value FROM json_each(?))', params: [ids], write: false, returning: false, bigint: false });
      return L.group({ parents: posts, children: authors, pk: 'author_id', fk: 'id', into: 'author', single: true });
    }
  };
  const out = bindBehaviors(compileBehaviors(cls as never), ctx).run('Find', {}) as Array<Record<string, unknown>>;
  // exactly two SELECTs — the child fetch is one batched `IN (json_each(?))`, never one-per-parent
  expect(calls.filter((c) => c.kind === 'execute').length).toBe(2);
  // the deduped key set binds as ONE JSON string param
  expect(calls[1].params).toEqual(['[10,20]']);
  expect(out.length).toBe(3);
  expect(out[0].author).toMatchObject({ name: 'A' });
  expect(out[2].author).toMatchObject({ name: 'B' });
});

test('postgres: key array binds raw, deferred cast resolves to ::int[], placeholders render $N', () => {
  const calls: Call[] = [];
  const ctx = { exec: contextForConnection(recordingConn(calls)), dialect: 'postgres' as const };
  const cls = class extends SemanticBehavior {
    Find() {
      const posts = L.executeSQL({ sql: 'SELECT id, author_id FROM posts', params: [], write: false, returning: false, bigint: false });
      const ids = L.pluck({ rows: posts, col: 'author_id' }).as({ arr: 'float' });
      const authors = L.executeSQL({ sql: 'SELECT id, name FROM users WHERE id = ANY(?::@@PG_ARRAY_CAST@@)', params: [ids], write: false, returning: false, bigint: false });
      return L.group({ parents: posts, children: authors, pk: 'author_id', fk: 'id', into: 'author', single: true });
    }
  };
  bindBehaviors(compileBehaviors(cls as never), ctx).run('Find', {});
  expect(calls[1].sql).toContain('$1');
  expect(calls[1].sql).toContain('::int[]');
  expect(Array.isArray(calls[1].params[0])).toBe(true);
});

test('write routes through the run seam and returns the affected summary', () => {
  const calls: Call[] = [];
  const ctx = { exec: contextForConnection(recordingConn(calls)), dialect: 'sqlite' as const };
  const cls = class extends SemanticBehavior {
    Ins() {
      return L.executeSQL({ sql: 'INSERT INTO users(id,name) VALUES (?,?)', params: [30, 'C'], write: true, returning: false, bigint: false });
    }
  };
  const out = bindBehaviors(compileBehaviors(cls as never), ctx).run('Ins', {}) as Array<Record<string, unknown>>;
  expect(calls[0].kind).toBe('run');
  expect(out[0].changes).toBe(1);
  expect(out[0].lastInsertRowid).toBe(42n);
});
