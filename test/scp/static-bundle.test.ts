/**
 * Static-symbolic makeSQL bundle (epic #43/#45 Phase B, design #45) — the SOLE makeSQL read
 * path. Proves the STATIC bundle (symbolic compile → deferred value-specs + skip expression,
 * bc-evaluated per-input at runtime) executes against real SQLite with RESULT PARITY to the
 * reduced `executeBehavior` runtime it replaces. Same behaviors, same seed, same rows.
 *
 * The compile step takes NO concrete input (symbolic); the runtime evaluates skip + params
 * per-input via bc `evaluateExpression`, assembles present fragments, renders, executes.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  executeBehavior,
  compileStaticBundle,
  executeStaticBundle,
  whereEq,
  whereGe,
  whereIn,
  inColumn,
  when,
  ne,
  opt,
  coalesce,
  type In,
} from '../../src/scp';

const L = components();

function freshDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE posts (
      id INTEGER PRIMARY KEY,
      author_id INTEGER NOT NULL,
      title TEXT NOT NULL,
      status TEXT,
      created_at TEXT NOT NULL
    );
  `);
  db.prepare('INSERT INTO posts VALUES (?,?,?,?,?)').run(1, 7, 'Hello', 'live', '2026-02-01');
  db.prepare('INSERT INTO posts VALUES (?,?,?,?,?)').run(2, 7, 'World', 'draft', '2026-03-01');
  db.prepare('INSERT INTO posts VALUES (?,?,?,?,?)').run(3, 8, 'Other', 'live', '2026-01-15');
  return db;
}

class PostQueries extends SemanticBehavior {
  Search($: In<{ author_id: number; status?: string; since: string; limit?: number }>) {
    return L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title', 'status'],
      where: [
        whereEq($.author_id, $.author_id),
        when(ne(opt($.status), null), () => whereEq($.status, $.status)),
        whereGe($.created_at, $.since),
      ],
      order: 'id ASC',
      limit: coalesce(opt($.limit), 20),
    });
  }

  ByIds($: In<{ ids: number[] }>) {
    return L.Select({
      table: 'posts',
      select: ['id', 'title'],
      where: [whereIn(inColumn($, 'id'), $.ids)],
      order: 'id ASC',
    });
  }
}

const contract = publishBehaviors(PostQueries);

/** Run BOTH the reduced runtime and the static bundle; assert identical rows. */
function bothAgree(entry: string, input: Record<string, unknown>): unknown {
  const dbA = freshDb();
  const reduced = executeBehavior(contract, input, { db: dbA, entry });
  dbA.close();

  const dbB = freshDb();
  const bundle = compileStaticBundle(contract, 'sqlite', entry);
  const staticRows = executeStaticBundle(bundle, input, dbB);
  dbB.close();

  expect(staticRows).toEqual(reduced);
  return staticRows;
}

describe('static-symbolic makeSQL bundle — Select read parity (real SQLite)', () => {
  it('compile is symbolic (no input) and reusable across inputs', () => {
    const bundle = compileStaticBundle(contract, 'sqlite', 'Search');
    expect(bundle.name).toBe('Search');
    expect(bundle.dialect).toBe('sqlite');
    // The SQL text is fixed (value-independent); params/skip are deferred bc Expression IR.
    expect(bundle.statements.length).toBeGreaterThan(0);
    // The SKIP-optional status fragment carries a skip presence expression.
    const guarded = bundle.statements.filter((s) => s.skip !== undefined);
    expect(guarded.length).toBe(1);
  });

  it('eq + SKIP present → status filter applied', () => {
    const rows = bothAgree('Search', { author_id: 7, status: 'live', since: '2026-01-01' });
    expect(rows).toEqual([{ id: 1, author_id: 7, title: 'Hello', status: 'live' }]);
  });

  it('SKIP null → status fragment dropped', () => {
    bothAgree('Search', { author_id: 7, status: null, since: '2026-01-01' });
  });

  it('SKIP absent-via-refOpt → fragment dropped end-to-end (optional head omitted)', () => {
    const rows = bothAgree('Search', { author_id: 7, since: '2026-01-01' });
    expect(rows).toEqual([
      { id: 1, author_id: 7, title: 'Hello', status: 'live' },
      { id: 2, author_id: 7, title: 'World', status: 'draft' },
    ]);
  });

  it('multi-AND + range narrows the set', () => {
    bothAgree('Search', { author_id: 7, status: null, since: '2026-02-15' });
  });

  it('LIMIT via coalesce default (omitted → 20)', () => {
    const rows = bothAgree('Search', { author_id: 7, status: null, since: '2026-01-01' }) as unknown[];
    expect(rows).toHaveLength(2);
  });

  it('LIMIT explicit caps the row count', () => {
    const rows = bothAgree('Search', { author_id: 7, status: null, since: '2026-01-01', limit: 1 });
    expect(rows).toEqual([{ id: 1, author_id: 7, title: 'Hello', status: 'live' }]);
  });

  it('IN-list (N elements) via single-JSON param matches', () => {
    const rows = bothAgree('ByIds', { ids: [1, 3] });
    expect(rows).toEqual([{ id: 1, title: 'Hello' }, { id: 3, title: 'Other' }]);
  });

  it('IN-list (empty) → no rows (single-JSON param, server-side empty)', () => {
    const rows = bothAgree('ByIds', { ids: [] });
    expect(rows).toEqual([]);
  });
});
