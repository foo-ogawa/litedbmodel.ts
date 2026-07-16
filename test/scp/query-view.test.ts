/**
 * Phase E-3 (#98) — QUERY view-model authoring.
 *
 * A v1 `DBModel` may declare `static QUERY = <SQL | SqlFragment>` instead of a `TABLE_NAME`;
 * reads then wrap it as `WITH <alias> AS (<QUERY>) SELECT … FROM <alias>` (v1
 * `DBModel._buildSelectSQL` view-model path, `:563-624`), prepending the QUERY's params.
 *
 * v2 lowers this onto the EXISTING Select `cte`/`cteParams` ports (the SAME ports the
 * LIVE-tested `CteLive` conformance vector exercises) via the thin {@link queryView}
 * authoring wrapper — NO new IR, NO native work. The lowering is the read-path readGraph
 * (`compileBundle` → `renderReadPrimary` / `executeBundle`, the SAME path conformance +
 * production reads take; the additive R4 cte/cteParams ports live on `compileSelectNode`).
 * This test proves, on that path + REAL better-sqlite3:
 *
 *   (a) STRING QUERY → `WITH <alias> AS (<sql>) SELECT <select> FROM <alias> …`.
 *   (b) STRING QUERY WITH params + a WHERE over the derived rows: the QUERY params bind
 *       FIRST (CTE slot), then the WHERE params — v1 param-prepend order.
 *   (c) SqlFragment QUERY (`{ sql, params }`): the fragment's own params ride the CTE slot.
 *   (d) RED PROOF: the derived alias is a VIRTUAL CTE, not a base table — running the SAME
 *       read WITHOUT the CTE (base-table form) throws `no such table: derived`, so the CTE
 *       being emitted is load-bearing (a stub that dropped it could not return the rows).
 */

/* eslint-disable @typescript-eslint/no-explicit-any -- capturing-model harness + driver seams need casts */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileBundle,
  executeBundle,
  renderReadPrimary,
  schemaColumnTypeResolver,
  queryView,
  whereGe,
  type In,
  type Recorded,
} from '../../src/scp';

const L = components();

// The derived CTE alias ('derived', v1 `getCTEAlias` default) is a VIRTUAL table projecting
// id/title. The read-path de-box resolver types it from this schema (the SAME schema the read
// executes against, sans the `derived` view — the CTE is emitted by the compile, not a table).
const SCHEMA: readonly string[] = [
  'CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT NOT NULL)',
  'CREATE TABLE derived (id INTEGER, title TEXT)', // typed-resolver SoT for the CTE projection
];
const resolver = schemaColumnTypeResolver(SCHEMA);

// A QUERY view-model over `posts`. Every read is a VIEW over the declared QUERY — the model
// has NO base table; each read selects FROM the `derived` CTE built from the QUERY.
class PostView extends SemanticBehavior {
  // The authoring-time typed-column SoT (bc 0.8.0 all-nodes-typed gate): the reads project from
  // the `derived` CTE virtual table (id/title). This is the SAME declaration a QUERY view-model
  // carries (the derived alias is v1 `getCTEAlias()` = 'derived' when there is no TABLE_NAME).
  static columns = {
    derived: { id: 'INTEGER', title: 'TEXT' },
  };

  /** (a) STRING QUERY, no params. */
  LivePosts(_$: In<Record<string, never>>) {
    return L.Select(queryView(
      "SELECT id, title FROM posts WHERE status = 'live'",
      ['id', 'title'],
      { order: 'id ASC' },
    ));
  }

  /** (b) STRING QUERY WITH a param + a WHERE-over-CTE (param prepend). */
  LivePostsMinId($: In<{ min_id: number }>) {
    const idCol = ($ as unknown as Record<string, Recorded>).id;
    return L.Select(queryView(
      'SELECT id, title FROM posts WHERE status = ?',
      ['id', 'title'],
      { params: ['live'], where: [whereGe(idCol, $.min_id)], order: 'id ASC' },
    ));
  }

  /** (c) SqlFragment QUERY (`{ sql, params }`) — the fragment carries its own param. */
  ByAuthor(_$: In<Record<string, never>>) {
    return L.Select(queryView(
      { sql: 'SELECT id, title FROM posts WHERE author_id = ?', params: [7] },
      ['id', 'title'],
      { order: 'id ASC' },
    ));
  }
}

const contract = publishBehaviors(PostView);
const graphFor = (method: string) => compileBundle(contract, method, [], 'sqlite', undefined, resolver).readGraph!;
const sqlFor = (method: string, input: Record<string, unknown>) => renderReadPrimary(graphFor(method), input as any);

describe('#98 QUERY view-model authoring — lowers onto the Select cte/cteParams ports', () => {
  it('(a) STRING QUERY → WITH derived AS (<sql>) SELECT … FROM derived (v1 WITH-wrap shape)', () => {
    const { sql, params } = sqlFor('LivePosts', {});
    expect(sql).toBe(
      "WITH derived AS (SELECT id, title FROM posts WHERE status = 'live') " +
        'SELECT id, title FROM derived ORDER BY id ASC',
    );
    expect(params).toEqual([]);
  });

  it('(b) STRING QUERY param binds FIRST, then the WHERE-over-CTE param (v1 param prepend)', () => {
    const { sql, params } = sqlFor('LivePostsMinId', { min_id: 2 });
    expect(sql).toBe(
      'WITH derived AS (SELECT id, title FROM posts WHERE status = ?) ' +
        'SELECT id, title FROM derived WHERE id >= ? ORDER BY id ASC',
    );
    // QUERY param ('live') is the CTE param → binds before the WHERE param (min_id=2).
    expect(params).toEqual(['live', 2]);
  });

  it('(c) SqlFragment QUERY prepends the fragment’s own params (v1 _resolveQuery)', () => {
    const { sql, params } = sqlFor('ByAuthor', {});
    expect(sql).toBe(
      'WITH derived AS (SELECT id, title FROM posts WHERE author_id = ?) ' +
        'SELECT id, title FROM derived ORDER BY id ASC',
    );
    expect(params).toEqual([7]);
  });
});

describe('#98 QUERY view-model — REAL better-sqlite3 execution + RED proof', () => {
  let db: Database.Database;
  beforeAll(() => {
    db = new Database(':memory:');
    db.exec(
      'CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT NOT NULL)',
    );
    db.exec(
      "INSERT INTO posts VALUES (1,7,'Hello','live'),(2,7,'World','draft'),(3,8,'Other','live')",
    );
  });
  afterAll(() => db.close());

  const run = (method: string, input: Record<string, unknown>) =>
    executeBundle(compileBundle(contract, method, [], 'sqlite', undefined, resolver), input as any, { db });

  it('(a) STRING QUERY view returns the derived rows (live posts 1,3)', () => {
    expect(run('LivePosts', {})).toEqual([
      { id: 1, title: 'Hello' },
      { id: 3, title: 'Other' },
    ]);
  });

  it('(b) STRING QUERY + WHERE over the CTE returns only post 3 (live & id>=2)', () => {
    expect(run('LivePostsMinId', { min_id: 2 })).toEqual([{ id: 3, title: 'Other' }]);
  });

  it('(c) SqlFragment QUERY view (author_id=7) returns posts 1,2', () => {
    expect(run('ByAuthor', {})).toEqual([
      { id: 1, title: 'Hello' },
      { id: 2, title: 'World' },
    ]);
  });

  it('(d) RED PROOF: the derived alias is a VIRTUAL CTE — the base-table form (no CTE) has no such table', () => {
    // The QUERY view read selects FROM `derived`, which exists ONLY because the CTE is emitted.
    // Strip the `WITH derived AS (…) ` prefix (what a stub that ignored QUERY would do) and the
    // SAME tail hits a non-existent base table → the rows are unreachable. (A real `derived`
    // table is NOT seeded — only `posts` is; the successful read below proves the CTE supplies it.)
    const { sql } = sqlFor('LivePosts', {});
    const withoutCte = sql.replace(/^WITH derived AS \([^)]*\) /, '');
    expect(withoutCte).toBe('SELECT id, title FROM derived ORDER BY id ASC');
    expect(() => db.prepare(withoutCte).all()).toThrow(/no such table: derived/);
    // …whereas the real (CTE-carrying) read succeeds and returns the derived rows.
    expect((run('LivePosts', {}) as unknown[]).length).toBe(2);
  });
});
