/**
 * Phase E-3 (#98) — QUERY view-model authoring.
 *
 * A v1 `DBModel` may declare `static QUERY = <SQL | SqlFragment>` instead of a `TABLE_NAME`;
 * reads then wrap it as `WITH <alias> AS (<QUERY>) SELECT … FROM <alias>` (v1
 * `DBModel._buildSelectSQL` view-model path, `:563-624`), prepending the QUERY's params.
 *
 * v2 lowers this onto the EXISTING Select `cte`/`cteParams` ports via the thin {@link queryView}
 * authoring wrapper — NO new IR, NO native work. #141: the read authoring lowers through the
 * op-independent `executeSQL` leaf ({@link emitRead}), which assembles the head/CTE/WHERE/tail into
 * ONE literal `sql` (native-lowerable) via the SAME `compileSelectNode` SSoT. This test asserts, on
 * that path + REAL better-sqlite3:
 *
 *   (a) STRING QUERY → `WITH <alias> AS (<sql>) SELECT <select> FROM <alias> …` (v1 SQL-text golden).
 *   (b) STRING QUERY WITH params + a WHERE over the derived rows: the QUERY params bind FIRST (CTE
 *       slot), then the WHERE params — proven by the executed rows (v1 param-prepend order).
 *   (c) SqlFragment QUERY (`{ sql, params }`): the fragment's own params ride the CTE slot.
 *   (d) RED PROOF: the derived alias is a VIRTUAL CTE, not a base table — stripping the CTE prefix
 *       from the SAME assembled SQL hits a non-existent base table.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  emitRead,
  executeBehavior,
  queryView,
  whereGe,
  type In,
  type Recorded,
  type BehaviorModelContract,
} from '../../src/scp';

const L = components();

// The derived CTE alias ('derived', v1 `getCTEAlias` default) is a VIRTUAL table projecting
// id/title. The read-path de-box resolver types it from `static columns` (the SAME schema the read
// executes against, sans the `derived` view — the CTE is emitted by the compile, not a table).

// A QUERY view-model over `posts`. Every read is a VIEW over the declared QUERY — the model has NO
// base table; each read selects FROM the `derived` CTE built from the QUERY. #141: the read authors
// through `emitRead` (the op-independent `executeSQL` transport), NOT the retired `L.Select` catalog.
class PostView extends SemanticBehavior {
  static columns = {
    derived: { id: 'INTEGER', title: 'TEXT' },
  };

  /** (a) STRING QUERY, no params. */
  LivePosts(_$: In<Record<string, never>>) {
    return emitRead(L, 'Select', queryView(
      "SELECT id, title FROM posts WHERE status = 'live'",
      ['id', 'title'],
      { order: 'id ASC' },
      'derived',
    ), 'sqlite');
  }

  /** (b) STRING QUERY WITH a param + a WHERE-over-CTE (param prepend). */
  LivePostsMinId($: In<{ min_id: number }>) {
    const idCol = ($ as unknown as Record<string, Recorded>).id;
    return emitRead(L, 'Select', queryView(
      'SELECT id, title FROM posts WHERE status = ?',
      ['id', 'title'],
      { params: ['live'], where: [whereGe(idCol, $.min_id)], order: 'id ASC' },
      'derived',
    ), 'sqlite');
  }

  /** (c) SqlFragment QUERY (`{ sql, params }`) — the fragment carries its own param. */
  ByAuthor(_$: In<Record<string, never>>) {
    return emitRead(L, 'Select', queryView(
      { sql: 'SELECT id, title FROM posts WHERE author_id = ?', params: [7] },
      ['id', 'title'],
      { order: 'id ASC' },
      'derived',
    ), 'sqlite');
  }
}

const contract = publishBehaviors(PostView);

/** The assembled literal SQL of a read method's SOLE `executeSQL` transport node (the v1-shaped text). */
function sqlFor(c: BehaviorModelContract, method: string): string {
  const node = c.methods[method].component.body.find(
    (n) => 'component' in n && (n as { component: string }).component === 'executeSQL',
  ) as { ports: { sql: string } };
  return node.ports.sql;
}

describe('#98 QUERY view-model authoring — lowers onto the Select cte/cteParams ports (executeSQL transport)', () => {
  it('(a) STRING QUERY → WITH derived AS (<sql>) SELECT … FROM derived (v1 WITH-wrap SQL-text golden)', () => {
    expect(sqlFor(contract, 'LivePosts')).toBe(
      "WITH derived AS (SELECT id, title FROM posts WHERE status = 'live') " +
        'SELECT id, title FROM derived ORDER BY id ASC',
    );
  });

  it('(b) STRING QUERY param + WHERE-over-CTE assemble in v1 order (CTE `?` before the WHERE `?`)', () => {
    expect(sqlFor(contract, 'LivePostsMinId')).toBe(
      'WITH derived AS (SELECT id, title FROM posts WHERE status = ?) ' +
        'SELECT id, title FROM derived WHERE id >= ? ORDER BY id ASC',
    );
  });

  it('(c) SqlFragment QUERY assembles the fragment’s own `?` in the CTE slot (v1 _resolveQuery)', () => {
    expect(sqlFor(contract, 'ByAuthor')).toBe(
      'WITH derived AS (SELECT id, title FROM posts WHERE author_id = ?) ' +
        'SELECT id, title FROM derived ORDER BY id ASC',
    );
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
    executeBehavior(contract, input as never, { db: db as never, entry: method, dialect: 'sqlite' });

  it('(a) STRING QUERY view returns the derived rows (live posts 1,3)', () => {
    expect(run('LivePosts', {})).toEqual([
      { id: 1, title: 'Hello' },
      { id: 3, title: 'Other' },
    ]);
  });

  it('(b) STRING QUERY + WHERE over the CTE returns only post 3 (live & id>=2 — proves CTE param binds first)', () => {
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
    // SAME tail hits a non-existent base table → the rows are unreachable (only `posts` is seeded).
    const sql = sqlFor(contract, 'LivePosts');
    const withoutCte = sql.replace(/^WITH derived AS \([^)]*\) /, '');
    expect(withoutCte).toBe('SELECT id, title FROM derived ORDER BY id ASC');
    expect(() => db.prepare(withoutCte).all()).toThrow(/no such table: derived/);
    // …whereas the real (CTE-carrying) read succeeds and returns the derived rows.
    expect((run('LivePosts', {}) as unknown[]).length).toBe(2);
  });
});
