/**
 * Golden byte-parity tests for the MINIMAL fragment model (epic #43).
 *
 * The GOLDEN is the ORIGINAL tuned builders' ACTUAL output — we drive
 * `DBConditions` / `LazyRelationContext` / `buildSelectSQL` to emit the expected
 * `{ sql, params }` for each fixture, freeze THAT, and assert the fragment model's
 * `assemble` + `toPostgres` reproduces it byte-for-byte. Nothing is compared against
 * v2's own render.
 */

/* eslint-disable @typescript-eslint/no-explicit-any -- capturing-model test harness needs casts */
import { describe, it, expect } from 'vitest';
import { DBModel } from '../../src/DBModel';
import { LazyRelationContext } from '../../src/LazyRelation';
import { DBConditions } from '../../src/DBConditions';
import {
  assemble,
  toPostgres,
  compileBelongsTo,
  compileHasManyLimited,
  compileHasManyAny,
  compileBaseSelect,
  type Node,
  type Scope,
} from '../../src/scp/fragment';

// ---------------------------------------------------------------------------
// Capturing models: drive the ORIGINAL builders WITHOUT a live DB.
// query() records (sql, params) instead of executing.
// ---------------------------------------------------------------------------
const captures: { sql: string; params: unknown[] }[] = [];

class Base extends DBModel {
  static getDriverType(): 'postgres' | 'mysql' | 'sqlite' {
    return 'postgres';
  }
  static async query(sql: string, params: unknown[]): Promise<any[]> {
    captures.push({ sql, params });
    return [];
  }
}
class User extends Base {
  protected static TABLE_NAME = 'users';
  protected static SELECT_COLUMN = '*';
  id?: number;
}
class Comment extends Base {
  protected static TABLE_NAME = 'comments';
  protected static SELECT_COLUMN = '*';
  id?: number;
  post_id?: number;
}
class Post extends Base {
  protected static TABLE_NAME = 'posts';
  protected static SELECT_COLUMN = '*';
  id?: number;
  author_id?: number;
}

/** Drive the ORIGINAL LazyRelation to capture its exact SQL+params. */
async function originalRelation(
  sourceRecords: DBModel[],
  relationType: 'belongsTo' | 'hasMany' | 'hasOne',
  config: any
): Promise<{ sql: string; params: unknown[] }> {
  const ctx = new LazyRelationContext(Post as any, sourceRecords as any);
  captures.length = 0;
  await (ctx as any).getRelation(sourceRecords[0], relationType, config);
  expect(captures.length).toBe(1);
  // toPostgres to match what the real pg driver sends over the wire.
  return { sql: toPostgres(captures[0].sql), params: captures[0].params };
}

/** Drive the ORIGINAL DBConditions + buildSelectSQL-style prefix for a base SELECT. */
function originalBaseSelect(
  table: string,
  conds: Record<string, unknown>
): { sql: string; params: unknown[] } {
  const params: unknown[] = [];
  const where = new DBConditions(conds).compile(params);
  let sql = `SELECT * FROM ${table}`;
  if (where) sql += ` WHERE ${where}`;
  return { sql: toPostgres(sql), params };
}

const posts = [
  Object.assign(new Post(), { id: 1, author_id: 10 }),
  Object.assign(new Post(), { id: 2, author_id: 11 }),
  Object.assign(new Post(), { id: 3, author_id: 10 }),
];
// dedup(author_id) preserving first-seen order (as LazyRelation does): [10, 11]
const authorKeys = [10, 11];
const postKeys = [1, 2, 3];

describe('fragment model — PG byte parity vs ORIGINAL builders', () => {
  // -----------------------------------------------------------------------
  // (b) belongsTo  — = ANY(?::int[])
  // -----------------------------------------------------------------------
  it('belongsTo: = ANY(?::int[]) byte-matches LazyRelation', async () => {
    const golden = await originalRelation(posts, 'belongsTo', {
      targetClass: User,
      targetKey: 'id',
      sourceKey: 'author_id',
      relationName: 'author',
    });

    const frags = compileBelongsTo({
      tableName: 'users',
      targetKey: 'id',
      sampleKeys: authorKeys,
      keys: { literal: authorKeys },
    });
    const scope: Scope = { input: {} };
    const got = assemble(frags, scope);
    got.sql = toPostgres(got.sql);

    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
    expect(golden.sql).toBe('SELECT * FROM users WHERE users.id = ANY($1::int[])');
    expect(golden.params).toEqual([[10, 11]]);
  });

  // -----------------------------------------------------------------------
  // (c) hasMany + limit — CROSS JOIN LATERAL
  // -----------------------------------------------------------------------
  it('hasMany+limit: CROSS JOIN LATERAL byte-matches LazyRelation', async () => {
    const golden = await originalRelation(posts, 'hasMany', {
      targetClass: Comment,
      targetKey: 'post_id',
      sourceKey: 'id',
      limit: 5,
      order: 'created_at DESC',
      relationName: 'comments',
    });

    const frags = compileHasManyLimited({
      tableName: 'comments',
      targetKey: 'post_id',
      sampleKeys: postKeys,
      keys: { literal: postKeys },
      order: 'created_at DESC',
      limit: 5,
    });
    const got = assemble(frags, { input: {} });
    got.sql = toPostgres(got.sql);

    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
    expect(golden.sql).toBe(
      'SELECT comments.* FROM unnest($1::int[]) AS _keys(key) CROSS JOIN LATERAL (SELECT * FROM comments WHERE comments.post_id = _keys.key ORDER BY created_at DESC LIMIT 5) comments'
    );
    expect(golden.params).toEqual([[1, 2, 3]]);
  });

  // -----------------------------------------------------------------------
  // hasMany no limit — = ANY + ORDER BY (used to prove ANY+order shape)
  // -----------------------------------------------------------------------
  it('hasMany no-limit: = ANY + ORDER BY byte-matches LazyRelation', async () => {
    const golden = await originalRelation(posts, 'hasMany', {
      targetClass: Comment,
      targetKey: 'post_id',
      sourceKey: 'id',
      order: 'created_at DESC',
      relationName: 'comments2',
    });

    const frags = compileHasManyAny({
      tableName: 'comments',
      targetKey: 'post_id',
      sampleKeys: postKeys,
      keys: { literal: postKeys },
      order: 'created_at DESC',
    });
    const got = assemble(frags, { input: {} });
    got.sql = toPostgres(got.sql);

    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
    expect(golden.sql).toBe(
      'SELECT * FROM comments WHERE comments.post_id = ANY($1::int[]) ORDER BY created_at DESC'
    );
  });

  // -----------------------------------------------------------------------
  // hasMany + limit + optional filter INSIDE the LATERAL — present AND absent.
  // -----------------------------------------------------------------------
  it('hasMany+limit + LATERAL filter PRESENT byte-matches original', async () => {
    const golden = await originalRelation(posts, 'hasMany', {
      targetClass: Comment,
      targetKey: 'post_id',
      sourceKey: 'id',
      limit: 5,
      order: 'created_at DESC',
      conditions: { status: 'published' },
      relationName: 'cPub',
    });

    const frags = compileHasManyLimited({
      tableName: 'comments',
      targetKey: 'post_id',
      sampleKeys: postKeys,
      keys: { literal: postKeys },
      order: 'created_at DESC',
      limit: 5,
      filters: [{ column: 'status', inputPath: ['status'] }],
    });
    const got = assemble(frags, { input: { status: 'published' } });
    got.sql = toPostgres(got.sql);

    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
    expect(golden.sql).toBe(
      'SELECT comments.* FROM unnest($1::int[]) AS _keys(key) CROSS JOIN LATERAL (SELECT * FROM comments WHERE comments.post_id = _keys.key AND status = $2 ORDER BY created_at DESC LIMIT 5) comments'
    );
    expect(golden.params).toEqual([[1, 2, 3], 'published']);
  });

  it('hasMany+limit + LATERAL filter ABSENT (SKIP) byte-matches original w/o filter', async () => {
    const golden = await originalRelation(posts, 'hasMany', {
      targetClass: Comment,
      targetKey: 'post_id',
      sourceKey: 'id',
      limit: 5,
      order: 'created_at DESC',
      relationName: 'cNoPub',
    });

    const frags = compileHasManyLimited({
      tableName: 'comments',
      targetKey: 'post_id',
      sampleKeys: postKeys,
      keys: { literal: postKeys },
      order: 'created_at DESC',
      limit: 5,
      filters: [{ column: 'status', inputPath: ['status'] }],
    });
    const got = assemble(frags, { input: {} }); // status absent → SKIP inside LATERAL
    got.sql = toPostgres(got.sql);

    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
    expect(golden.sql).toBe(
      'SELECT comments.* FROM unnest($1::int[]) AS _keys(key) CROSS JOIN LATERAL (SELECT * FROM comments WHERE comments.post_id = _keys.key ORDER BY created_at DESC LIMIT 5) comments'
    );
  });

  // -----------------------------------------------------------------------
  // (a) optional / SKIP WHERE condition — present AND absent
  //     Anchored after the mandatory = ANY key predicate (relation where-filter).
  // -----------------------------------------------------------------------
  it('belongsTo + optional filter PRESENT byte-matches original', async () => {
    const golden = await originalRelation(posts, 'belongsTo', {
      targetClass: User,
      targetKey: 'id',
      sourceKey: 'author_id',
      relationName: 'authorActive',
      conditions: { status: 'active' },
    });

    const frags = compileBelongsTo({
      tableName: 'users',
      targetKey: 'id',
      sampleKeys: authorKeys,
      keys: { literal: authorKeys },
      filters: [{ column: 'status', inputPath: ['status'] }],
    });
    const got = assemble(frags, { input: { status: 'active' } });
    got.sql = toPostgres(got.sql);

    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
    expect(golden.sql).toBe(
      'SELECT * FROM users WHERE users.id = ANY($1::int[]) AND status = $2'
    );
    expect(golden.params).toEqual([[10, 11], 'active']);
  });

  it('belongsTo + optional filter ABSENT (SKIP) byte-matches original with no filter', async () => {
    // Golden with the filter ABSENT = the original called WITHOUT that condition.
    const golden = await originalRelation(posts, 'belongsTo', {
      targetClass: User,
      targetKey: 'id',
      sourceKey: 'author_id',
      relationName: 'authorNoFilter',
    });

    // Same compiled fragments as the PRESENT case, but the input lacks `status`,
    // so the skip fires and BOTH its sql and its param are omitted.
    const frags = compileBelongsTo({
      tableName: 'users',
      targetKey: 'id',
      sampleKeys: authorKeys,
      keys: { literal: authorKeys },
      filters: [{ column: 'status', inputPath: ['status'] }],
    });
    const got = assemble(frags, { input: {} }); // status absent → SKIP
    got.sql = toPostgres(got.sql);

    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
    expect(golden.sql).toBe('SELECT * FROM users WHERE users.id = ANY($1::int[])');
    expect(golden.params).toEqual([[10, 11]]);
  });

  // -----------------------------------------------------------------------
  // Pure base SELECT + all-optional WHERE — every present/absent SUBSET
  // byte-matches the ORIGINAL DBConditions output for that subset.
  // -----------------------------------------------------------------------
  const baseConds = [
    { column: 'status', inputPath: ['status'] },
    { column: 'author_id', inputPath: ['authorId'] },
  ];

  const subsets: Array<{ name: string; input: Record<string, unknown>; origConds: Record<string, unknown> }> = [
    { name: 'both present', input: { status: 'active', authorId: 7 }, origConds: { status: 'active', author_id: 7 } },
    { name: 'only status', input: { status: 'active' }, origConds: { status: 'active' } },
    { name: 'only author_id', input: { authorId: 7 }, origConds: { author_id: 7 } },
    { name: 'none present', input: {}, origConds: {} },
  ];

  for (const s of subsets) {
    it(`base SELECT + optional WHERE [${s.name}] byte-matches original`, () => {
      const golden = originalBaseSelect('posts', s.origConds);
      const frags: Node[] = compileBaseSelect({ tableName: 'posts', conditions: baseConds });
      const got = assemble(frags, { input: s.input });
      got.sql = toPostgres(got.sql);
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
    });
  }
});
