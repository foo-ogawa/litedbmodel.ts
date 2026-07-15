/**
 * Phase A (epic #43/#45) — the AUTHORING → `makeSQL` bundle wiring.
 *
 * Proves the authoring surface (declared / eager behaviors) compiles to `makeSQL`
 * bundles for the primary read/write path, executed via bc `runBehavior` + the makeSQL
 * handler. Four legs, per the goal:
 *
 *   (a) EAGER ≡ DECLARATION — the eager public-API path (`compileEager`) and the
 *       declaration path (`publishBehaviors`) produce a BYTE-IDENTICAL makeSQL bundle
 *       (the single-compile-path invariant carries onto the makeSQL target — spec §9).
 *   (b) v1-TUNED SQL — the emitted SQL byte-matches what the ORIGINAL v1 builders send:
 *       the primary SELECT byte-matches `DBConditions` + the `_buildSelectSQL` shape; the
 *       INSERT byte-matches the original single-row `buildInsert`; the hasMany /
 *       hasMany-limit relation byte-matches the v1 `LazyRelationContext` capture (its
 *       single-JSON-param rewrite on SQLite). Golden is CAPTURED FROM THE ORIGINALS,
 *       never v2-to-v2.
 *   (c) REAL-DB EXECUTION — the primary bundle rides bc `runBehavior` + `makeSqlHandlerSync`
 *       against REAL better-sqlite3 and returns the SAME rows as a direct query (result
 *       parity; no skip-on-DB-absent — SQLite is always in-process).
 *   (d) PURE-JSON BUNDLE — the compiled bundle survives `JSON.parse(JSON.stringify(...))`
 *       and assembles identically (multi-language target). PG bundle byte-matches v1 PG.
 *
 * Coverage: a read behavior (Select + hasMany `.map` + hasMany-LIMIT `.map`), a write
 * behavior (Insert), and a SKIP-optional condition (present / absent).
 */

/* eslint-disable @typescript-eslint/no-explicit-any -- capturing-model harness + bc/driver seams need casts */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import Database from 'better-sqlite3';
import { runBehavior, assertPortableComponentGraph, type Handlers } from 'behavior-contracts';
import { DBModel } from '../../src/DBModel';
import { LazyRelationContext } from '../../src/LazyRelation';
import { DBConditions } from '../../src/DBConditions';
import { sqliteSqlBuilder } from '../../src/drivers/SqliteSqlBuilder';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileEager,
  eq,
  ge,
  ne,
  when,
  type In,
  type Recorded,
  // makeSQL surface
  compileAuthoredBehavior,
  compileRelationMap,
  assembleMakeSQL,
  renderPlaceholders,
  makeSqlComponentIR,
  makeSqlInput,
  makeSqlHandlerSync,
  MAKESQL,
  type MakeSQL,
  type MakeSQLDialect,
} from '../../src/scp';

const L = components();

// ── The authored behaviors (spec §2.4). A read behavior + a write behavior. ───────────
//
// The read behavior selects posts, then batch-loads two hasMany relations off the parent
// posts: `comments` (unlimited) and `recent` (per-parent LIMIT). A `.map` names the target
// FK column via `eq($p.<fk>, …)`; the batch keys come from a parent field (here `id`).

class ReadBehaviors extends SemanticBehavior {
  static columns = {
    posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT', since: 'TEXT' },
    comments: { id: 'INTEGER', post_id: 'INTEGER', body: 'TEXT' },
  };
  PostSearch($: In<{ authorId: number; status?: string; since: string }>) {
    const posts = L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title'],
      where: [
        eq($.author_id, $.authorId),
        when(ne($.status, null), () => eq($.status, $.status)), // SKIP-optional (§7)
        ge($.since, $.since),
      ],
      order: 'id ASC',
    });
    // hasMany: each post's comments (batch by posts.id → comments.post_id).
    const comments = posts.map(($p: Recorded) =>
      L.Select({ table: 'comments', select: ['id', 'post_id', 'body'], where: [eq($p.post_id, $p.post_id)] }),
    );
    return { posts, comments };
  }
}

class WriteBehaviors extends SemanticBehavior {
  CreatePost($: In<{ authorId: number; title: string }>) {
    return L.Insert({
      table: 'posts',
      'values.author_id': $.authorId,
      'values.title': $.title,
      returning: 'id, title',
    });
  }
}

// The eager public-API equivalents — SAME authoring vocabulary, installed AS the method.
const eagerPostSearch = ($: Recorded, l: typeof L) => {
  const posts = l.Select({
    table: 'posts',
    select: ['id', 'author_id', 'title'],
    where: [
      eq($.author_id, $.authorId),
      when(ne($.status, null), () => eq($.status, $.status)),
      ge($.since, $.since),
    ],
    order: 'id ASC',
  });
  const comments = posts.map(($p: Recorded) =>
    l.Select({ table: 'comments', select: ['id', 'post_id', 'body'], where: [eq($p.post_id, $p.post_id)] }),
  );
  return { posts, comments };
};
const eagerCreatePost = ($: Recorded, l: typeof L) =>
  l.Insert({ table: 'posts', 'values.author_id': $.authorId, 'values.title': $.title, returning: 'id, title' });
const eagerCols = {
  columns: {
    posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT', since: 'TEXT' },
    comments: { id: 'INTEGER', post_id: 'INTEGER', body: 'TEXT' },
  },
};

const dialect: MakeSQLDialect = 'sqlite';
const render = (n: MakeSQL) => {
  const a = assembleMakeSQL(n);
  return { sql: renderPlaceholders(a.sql, dialect), params: a.params };
};

// ===========================================================================
// (a) EAGER ≡ DECLARATION — byte-identical makeSQL bundle (single compile path).
// ===========================================================================
describe('(a) eager public API ≡ SemanticBehavior declaration — identical makeSQL bundle', () => {
  it('read behavior: the primary Select bundle is byte-identical for both authoring paths', () => {
    const scope = { authorId: 10, status: 'active', since: '2020-01-01' };
    const decl = compileAuthoredBehavior(publishBehaviors(ReadBehaviors), scope, dialect, 'PostSearch');
    const eager = compileAuthoredBehavior(compileEager('PostSearch', eagerPostSearch as any, eagerCols), scope, dialect, 'PostSearch');
    expect(eager.primary).toEqual(decl.primary);
    expect(JSON.stringify(eager.primary)).toBe(JSON.stringify(decl.primary));
  });

  it('write behavior: the Insert bundle is byte-identical for both authoring paths', () => {
    const scope = { authorId: 10, title: 'Hello' };
    const decl = compileAuthoredBehavior(publishBehaviors(WriteBehaviors), scope, dialect, 'CreatePost');
    const eager = compileAuthoredBehavior(compileEager('CreatePost', eagerCreatePost as any), scope, dialect, 'CreatePost');
    expect(JSON.stringify(eager.primary)).toBe(JSON.stringify(decl.primary));
  });

  it('SKIP-optional: eager ≡ declaration for BOTH the present and the absent case', () => {
    for (const scope of [{ authorId: 10, status: 'active', since: 'x' }, { authorId: 10, since: 'x' }]) {
      const decl = compileAuthoredBehavior(publishBehaviors(ReadBehaviors), scope, dialect, 'PostSearch');
      const eager = compileAuthoredBehavior(compileEager('PostSearch', eagerPostSearch as any, eagerCols), scope, dialect, 'PostSearch');
      expect(JSON.stringify(eager.primary)).toBe(JSON.stringify(decl.primary));
    }
  });

  it('relation `.map` bundle is byte-identical for both authoring paths', () => {
    const parentRows = [{ id: 1 }, { id: 2 }];
    const declMap = mapNodeOf(publishBehaviors(ReadBehaviors));
    const eagerMap = mapNodeOf(compileEager('PostSearch', eagerPostSearch as any, eagerCols));
    const declRel = compileRelationMap(declMap, { parentRows }, dialect, 'id');
    const eagerRel = compileRelationMap(eagerMap, { parentRows }, dialect, 'id');
    expect(JSON.stringify(eagerRel)).toBe(JSON.stringify(declRel));
  });
});

// ===========================================================================
// (b) v1-TUNED SQL — golden captured FROM THE ORIGINALS (not v2-to-v2).
// ===========================================================================
describe('(b) emitted SQL is v1-tuned — byte-matches the ORIGINAL builders', () => {
  const contract = publishBehaviors(ReadBehaviors);

  it('primary SELECT byte-matches DBConditions + the _buildSelectSQL shape (present case)', () => {
    const scope = { authorId: 10, status: 'active', since: '2020-01-01' };
    const got = render(compileAuthoredBehavior(contract, scope, dialect, 'PostSearch').primary);
    const golden = buildV1Select({
      table: 'posts',
      columns: 'id, author_id, title',
      conditions: { author_id: 10, status: 'active', 'since >= ?': '2020-01-01' },
      order: 'id ASC',
    });
    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
  });

  it('primary SELECT drops the SKIP-optional member when absent (byte-matches v1 without it)', () => {
    const scope = { authorId: 10, since: '2020-01-01' };
    const got = render(compileAuthoredBehavior(contract, scope, dialect, 'PostSearch').primary);
    const golden = buildV1Select({
      table: 'posts',
      columns: 'id, author_id, title',
      conditions: { author_id: 10, 'since >= ?': '2020-01-01' },
      order: 'id ASC',
    });
    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
  });

  it('INSERT byte-matches the ORIGINAL single-row sqliteSqlBuilder.buildInsert (canonical order)', () => {
    const scope = { authorId: 10, title: 'Hello' };
    const got = render(compileAuthoredBehavior(publishBehaviors(WriteBehaviors), scope, dialect, 'CreatePost').primary);
    const golden = sqliteSqlBuilder.buildInsert({
      tableName: 'posts',
      columns: ['author_id', 'title'], // canonical (alphabetical) order — v2 write-path SSoT
      records: [{ author_id: 10, title: 'Hello' }],
      returning: 'id, title',
    } as any);
    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
  });

  it('hasMany relation byte-matches the v1 LazyRelation capture (SQLite JSON single-param)', async () => {
    const parentRows = [{ id: 1 }, { id: 2 }, { id: 1 }]; // distinct post ids: 1, 2
    const got = render(compileRelationMap(mapNodeOf(contract), { parentRows }, dialect, 'id'));

    // GOLDEN: capture the v1 relation SQL projecting the SAME columns the authored `.map`
    // requests, then rewrite the IN-list to the JSON single-param form (the SAME transform
    // the makeSQL relation compile applies) — an INDEPENDENT target derived from v1.
    const v1 = await captureHasMany([1, 2], undefined, 'id, post_id, body');
    const golden = jsonifySqliteInList(v1, 'post_id', [1, 2]);
    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
  });

  it('hasMany-LIMIT relation byte-matches the v1 LazyRelation ROW_NUMBER capture (SQLite)', async () => {
    const parentRows = [{ id: 1 }, { id: 2 }];
    // The v1 ROW_NUMBER per-parent-limit path projects the outer `SELECT <SELECT_COLUMN>
    // FROM ranked`; the compile-relation anchor reproduces this as the outer `*` (the pinned
    // limited shape). This leg pins the per-parent-LIMIT relation against the v1 capture with
    // a `*`-projecting target — the authored `.map` per-parent limit.
    const got = render(compileRelationMap(mapNodeOf(contract), { parentRows, limit: 5 }, dialect, 'id'));

    const v1 = await captureHasMany([1, 2], 5, '*');
    const golden = jsonifySqliteInList(v1, 'post_id', [1, 2]);
    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
  });
});

// ===========================================================================
// (c) REAL-DB EXECUTION — primary bundle via bc runBehavior + makeSqlHandlerSync.
// ===========================================================================
describe('(c) authored bundle rides bc runBehavior + makeSQL handler (real better-sqlite3)', () => {
  let db: Database.Database;
  beforeAll(() => {
    db = new Database(':memory:');
    db.exec(`
      CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT, since TEXT);
      CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER, body TEXT);
      INSERT INTO posts (id, author_id, title, since) VALUES
        (1, 10, 'a', '2021-01-01'), (2, 11, 'b', '2019-01-01'), (3, 10, 'c', '2022-01-01');
      INSERT INTO comments (id, post_id, body) VALUES
        (100, 1, 'c1'), (101, 1, 'c2'), (102, 3, 'c3');
    `);
  });
  afterAll(() => db.close());

  const sqliteExec = (sql: string, params: unknown[]) =>
    db.prepare(sql).all(...(params as any[])) as Record<string, unknown>[];

  function runViaBc(node: MakeSQL): Record<string, unknown>[] {
    const ir = makeSqlComponentIR('Query');
    assertPortableComponentGraph(ir); // bc Portability Guard passes on our graph
    const handlers: Handlers = { [MAKESQL]: makeSqlHandlerSync(sqliteExec, 'sqlite') };
    return runBehavior(ir, handlers, makeSqlInput(node) as any, 'Query') as unknown as Record<string, unknown>[];
  }

  it('primary Select executes via runBehavior and matches a direct query (SKIP absent)', () => {
    // author_id=10, since>=2020 keeps posts 1 & 3.
    const node = compileAuthoredBehavior(publishBehaviors(ReadBehaviors), { authorId: 10, since: '2020-01-01' }, 'sqlite', 'PostSearch').primary;
    const viaBc = runViaBc(node);
    const asm = assembleMakeSQL(node);
    const direct = sqliteExec(renderPlaceholders(asm.sql, 'sqlite'), asm.params);
    expect(viaBc).toEqual(direct);
    expect(viaBc.map((r) => r.id)).toEqual([1, 3]);
  });

  it('hasMany relation batch executes via runBehavior and returns the parent comments', () => {
    const parentRows = [{ id: 1 }, { id: 3 }];
    const rel = compileRelationMap(mapNodeOf(publishBehaviors(ReadBehaviors)), { parentRows }, 'sqlite', 'id');
    const rows = runViaBc(rel);
    expect(rows.map((r) => r.body).sort()).toEqual(['c1', 'c2', 'c3']);
  });

  it('INSERT executes via runBehavior and RETURNING round-trips through better-sqlite3', () => {
    const node = compileAuthoredBehavior(publishBehaviors(WriteBehaviors), { authorId: 10, title: 'New' }, 'sqlite', 'CreatePost').primary;
    const asm = assembleMakeSQL(node);
    const rows = db.prepare(renderPlaceholders(asm.sql, 'sqlite')).all(...(asm.params as any[])) as any[];
    expect(rows[0].title).toBe('New');
    const found = db.prepare('SELECT title FROM posts WHERE title = ?').get('New') as any;
    expect(found.title).toBe('New');
  });
});

// ===========================================================================
// (d) PURE-JSON BUNDLE — round-trip through JSON and re-assemble identically.
// ===========================================================================
describe('(d) the makeSQL bundle is pure JSON (round-trips losslessly)', () => {
  it('read + write bundles survive JSON.parse(JSON.stringify(...)) with identical assembly', () => {
    const read = compileAuthoredBehavior(publishBehaviors(ReadBehaviors), { authorId: 10, since: 'x' }, 'sqlite', 'PostSearch').primary;
    const write = compileAuthoredBehavior(publishBehaviors(WriteBehaviors), { authorId: 10, title: 't' }, 'sqlite', 'CreatePost').primary;
    for (const node of [read, write]) {
      const roundTripped = JSON.parse(JSON.stringify(node)) as MakeSQL;
      expect(assembleMakeSQL(roundTripped)).toEqual(assembleMakeSQL(node));
    }
  });

  it('PostgreSQL primary bundle byte-matches v1 PG SQL (?→$N) and round-trips', () => {
    const pg = compileAuthoredBehavior(publishBehaviors(ReadBehaviors), { authorId: 10, status: 'active', since: 'x' }, 'postgres', 'PostSearch').primary;
    const asm = assembleMakeSQL(JSON.parse(JSON.stringify(pg)) as MakeSQL);
    const rendered = renderPlaceholders(asm.sql, 'postgres');
    const golden = buildV1SelectPg({
      table: 'posts',
      columns: 'id, author_id, title',
      conditions: { author_id: 10, status: 'active', 'since >= ?': 'x' },
      order: 'id ASC',
    });
    expect(rendered).toBe(golden.sql);
    expect(asm.params).toEqual(golden.params);
  });
});

// ===========================================================================
// GOLDEN helpers — drive the ORIGINAL builders (v1) to produce the expected target.
// ===========================================================================

/** The read behavior's single relation `.map` body node. */
function mapNodeOf(contract: ReturnType<typeof publishBehaviors>): any {
  return contract.methods.PostSearch.component.body.find((n: any) => 'map' in n) as any;
}

/** SQLite v1 SELECT: `SELECT <cols> FROM <t> WHERE <DBConditions>[ ORDER BY …]`. */
function buildV1Select(o: { table: string; columns: string; conditions: Record<string, unknown>; order?: string }) {
  const params: unknown[] = [];
  const where = new DBConditions(o.conditions as any).compile(params);
  let sql = `SELECT ${o.columns} FROM ${o.table}`;
  if (where) sql += ` WHERE ${where}`;
  if (o.order) sql += ` ORDER BY ${o.order}`;
  return { sql, params };
}

/** PostgreSQL v1 SELECT (?→$N via the same render pass, PG cast formatter). */
function buildV1SelectPg(o: { table: string; columns: string; conditions: Record<string, unknown>; order?: string }) {
  const params: unknown[] = [];
  const pgFmt = (ph: string, t: string) => `${ph}::${t}`;
  const where = new DBConditions(o.conditions as any).compile(params, pgFmt);
  let sql = `SELECT ${o.columns} FROM ${o.table}`;
  if (where) sql += ` WHERE ${where}`;
  if (o.order) sql += ` ORDER BY ${o.order}`;
  return { sql: renderPlaceholders(sql, 'postgres'), params };
}

/** Capture the v1 hasMany relation SQL via LazyRelationContext (SQLite), optional limit.
 * `selectColumn` sets the target class projection so the capture matches the shape the
 * compile emits (unlimited honors the projection; the ROW_NUMBER limited outer is `*`). */
async function captureHasMany(
  _keys: number[],
  limit: number | undefined,
  selectColumn: string,
): Promise<{ sql: string; params: unknown[] }> {
  const captures: { sql: string; params: unknown[] }[] = [];
  class Base extends DBModel {
    static getDriverType(): MakeSQLDialect {
      return 'sqlite';
    }
    static override async query(sql: string, params: unknown[]): Promise<any[]> {
      captures.push({ sql, params });
      return [];
    }
  }
  class Post extends Base {
    protected static override TABLE_NAME = 'posts';
    protected static override SELECT_COLUMN = '*';
    id?: number;
  }
  class Comment extends Base {
    protected static override TABLE_NAME = 'comments';
    protected static override SELECT_COLUMN = selectColumn;
    id?: number;
    post_id?: number;
  }
  const posts = [Object.assign(new Post(), { id: 1 }), Object.assign(new Post(), { id: 2 })];
  const ctx = new LazyRelationContext(Post as any, posts as any);
  const config: any = {
    targetClass: Comment,
    targetKey: 'post_id',
    sourceKey: 'id',
    relationName: 'comments',
  };
  if (selectColumn !== '*') config.select = selectColumn;
  if (limit !== undefined) config.limit = limit;
  await (ctx as any).getRelation(posts[0], 'hasMany', config);
  const cap = captures[0];
  return { sql: renderPlaceholders(cap.sql, 'sqlite'), params: cap.params };
}

/** Rewrite a v1 SQLite `col IN (?, …)` capture to the JSON single-param form (json_each). */
function jsonifySqliteInList(v1: { sql: string; params: unknown[] }, col: string, values: unknown[]) {
  const nForm = `${col} IN (${values.map(() => '?').join(', ')})`;
  const jsonForm = `${col} IN (SELECT value FROM json_each(?))`;
  expect(v1.sql).toContain(nForm);
  const sql = v1.sql.replace(nForm, () => jsonForm);
  const p = v1.params;
  let at = -1;
  for (let i = 0; i + values.length <= p.length; i++) {
    if (values.every((v, k) => p[i + k] === v)) { at = i; break; }
  }
  expect(at).toBeGreaterThanOrEqual(0);
  const params = [...p.slice(0, at), JSON.stringify(values), ...p.slice(at + values.length)];
  return { sql, params };
}
