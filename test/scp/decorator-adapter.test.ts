/**
 * Phase F-1 (#104) — the decorator → SCP authoring ADAPTER. For representative decorator models
 * covering every README shape, the adapter-generated bundle is BYTE-IDENTICAL to the hand-written SCP
 * behavior for the same model (authoring.ts guarantees eager↔declaration byte-identity — this leans on
 * that). Proves the decorator surface lowers to the SAME SCP the native runtimes already execute.
 *
 * Note: vitest (esbuild) does NOT support `emitDecoratorMetadata`, so `design:type` auto-inference is
 * unavailable; the models here use the EXPLICIT `@column.*` variants (which set the `sqlCast` family),
 * and bare `@column()` id/number columns take the documented `DEFAULT_UNCAST_SQL_TYPE` (INTEGER) or a
 * `columnTypes` pin — exactly the adapter's column-type mapping under test.
 */

import 'reflect-metadata';
import { describe, it, expect } from 'vitest';
import { model, column, hasMany, belongsTo, hasOne } from '../../src/decorators';
import {
  deriveModelColumns,
  columnSqlType,
  tableNameOf,
  findAuthoring,
  countAuthoring,
  createAuthoring,
  updateAuthoring,
  deleteAuthoring,
  compileReadBundle,
  compileCommandBundle,
  compileCreateBundle,
  compileUpdateBundle,
  compileDeleteBundle,
  deriveRelationDecls,
  compileRelationOps,
  modelColumnResolver,
  compileEager,
  compileBundle,
  compileWriteBundle,
  compileCreateManyBundle,
  compileUpdateManyBundle,
  compileDeleteManyBundle,
  compileRelationOp,
  entityWrites,
  whereEq,
  when,
  ne,
  inSubquery,
  col,
  parentRef,
  queryView,
  type Recorded,
  type ComponentFns,
} from '../../src/scp';

// A tiny save-contract with an empty lifecycle for every phase, so the single-write Command path
// (`compileWriteBundle`) has a lifecycle to lower around the base write for create/update/remove — the
// adapter is not testing write-time relations, so all effect arrays are empty.
const NO_EFFECTS = entityWrites((w) => ({ create: w.lifecycle(), update: w.lifecycle(), remove: w.lifecycle() }));

// ── Representative README models ─────────────────────────────────────────────────────────────────

@model('users')
class User {
  @column() id?: number;
  @column() name?: string;
  @column.boolean() is_active?: boolean;
  @column.datetime() created_at?: Date;
  @column.bigint() big_id?: bigint;
  @column.uuid() ext_id?: string;
  @column.json() metadata?: Record<string, unknown>;
  @column.date() birth_date?: string;
  @column.stringArray() tags?: string[];

  @hasMany(() => [User.id, Post.author_id], {
    order: () => Post.created_at.desc(),
    limit: 10,
    hardLimit: 500,
  })
  declare recentPosts: Promise<Post[]>;

  @hasOne(() => [User.id, Profile.user_id])
  declare profile: Promise<Profile | null>;
}

@model('posts')
class Post {
  @column() id?: number;
  @column() author_id?: number;
  @column() title?: string;
  @column.datetime() created_at?: Date;

  @belongsTo(() => [Post.author_id, User.id])
  declare author: Promise<User | null>;
}

@model('profiles')
class Profile {
  @column() id?: number;
  @column() user_id?: number;
  @column() bio?: string;
}

// Composite-key tenant models
@model('tenant_users')
class TenantUser {
  @column() tenant_id?: number;
  @column() id?: number;
  @column() name?: string;

  @hasMany(() => [
    [TenantUser.tenant_id, TenantPost.tenant_id],
    [TenantUser.id, TenantPost.author_id],
  ])
  declare posts: Promise<TenantPost[]>;
}

@model('tenant_posts')
class TenantPost {
  @column() tenant_id?: number;
  @column() author_id?: number;
  @column() title?: string;
}

const registry: Record<string, unknown> = { User, Post, Profile, TenantUser, TenantPost };
const resolve = (name: string) => registry[name] as never;

// ── 1. Column-type mapping (every README @column.* type family) ──────────────────────────────────

describe('F1 columns — @column.* → SCP static columns SQL-type token', () => {
  it('maps each decorator type family to its §4.1 SQL-type token (uuid/json/date/array pinned; bare→INTEGER)', () => {
    const cols = deriveModelColumns(User as never, { columnTypes: { name: 'TEXT' } });
    expect(cols).toEqual({
      users: {
        id: 'INTEGER', // bare @column() number → DEFAULT_UNCAST_SQL_TYPE
        name: 'TEXT', // pinned override (bare string, no sqlCast)
        is_active: 'BOOLEAN',
        created_at: 'TIMESTAMP',
        big_id: 'BIGINT',
        ext_id: 'UUID',
        metadata: 'JSONB',
        birth_date: 'DATE',
        tags: 'TEXT[]',
      },
    });
  });

  it('columnSqlType honors the override, else the family, else the uncast default', () => {
    expect(columnSqlType('x', { columnName: 'x', sqlCast: 'boolean' })).toBe('BOOLEAN');
    expect(columnSqlType('x', { columnName: 'x' })).toBe('INTEGER'); // no sqlCast → default
    expect(columnSqlType('x', { columnName: 'x' }, 'REAL')).toBe('REAL'); // override wins
  });

  it('every produced token is accepted by coltype (fail-closed): a bogus family throws', () => {
    expect(() => columnSqlType('x', { columnName: 'x', sqlCast: 'geometry' })).toThrow(/no SCP SQL-type mapping/);
  });

  it('tableNameOf uses TABLE_NAME, else the model name lowercased', () => {
    expect(tableNameOf(User as never)).toBe('users');
    expect(tableNameOf({ name: 'FooBar' })).toBe('foobar');
  });
});

// ── 2. Reads: find / findOne / findById / count with conditions/order/limit/subquery/QUERY ────────

const usersColumns = { columnTypes: { name: 'TEXT' } };

/** Hand-write the equivalent read declaration bundle for a User read shape (the byte-identity oracle). */
function handRead(name: string, fn: ($: Recorded, L: ComponentFns) => unknown, cols: Record<string, Record<string, string>>) {
  const contract = compileEager(name, fn, { columns: cols });
  return compileBundle(contract, name, [], 'sqlite', undefined, contract.resolveColumnType);
}
const USERS_COLS = { users: { id: 'INTEGER', name: 'TEXT', is_active: 'BOOLEAN', created_at: 'TIMESTAMP', big_id: 'BIGINT', ext_id: 'UUID', metadata: 'JSONB', birth_date: 'DATE', tags: 'TEXT[]' } };

describe('F1 reads — find/findOne/findById/count byte-identical to hand-written SCP', () => {
  it('find with a WHERE condition + order', () => {
    const spec = {
      select: ['id', 'name'],
      where: ($: Recorded) => [whereEq($.id, $.id)],
      order: 'created_at DESC',
    };
    const adapter = compileReadBundle(User as never, 'find', findAuthoring('users', spec), 'sqlite', usersColumns);
    const hand = handRead('find', ($: Recorded, L) => L.Select({ table: 'users', select: ['id', 'name'], where: [whereEq($.id, $.id)], order: 'created_at DESC' }), USERS_COLS);
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
  });

  it('findById (identity WHERE) and findOne (identity WHERE) share the find authoring', () => {
    const byId = compileReadBundle(User as never, 'findById', findAuthoring('users', { select: ['id', 'name'], where: ($: Recorded) => [whereEq($.id, $.id)] }), 'sqlite', usersColumns);
    const hand = handRead('findById', ($: Recorded, L) => L.Select({ table: 'users', select: ['id', 'name'], where: [whereEq($.id, $.id)] }), USERS_COLS);
    expect(JSON.stringify(byId)).toBe(JSON.stringify(hand));
  });

  it('find with a SKIP-optional condition (when(ne(...), ...))', () => {
    const spec = {
      select: ['id', 'name'],
      where: ($: Recorded) => [whereEq($.id, $.id), when(ne($.name, null), () => whereEq($.name, $.name))],
    };
    const adapter = compileReadBundle(User as never, 'find', findAuthoring('users', spec), 'sqlite', usersColumns);
    const hand = handRead('find', ($: Recorded, L) => L.Select({ table: 'users', select: ['id', 'name'], where: [whereEq($.id, $.id), when(ne($.name, null), () => whereEq($.name, $.name))] }), USERS_COLS);
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
  });

  it('find with limit + offset', () => {
    const spec = { select: ['id', 'name'], limit: ($: Recorded) => $.lim, offset: 5 };
    const adapter = compileReadBundle(User as never, 'find', findAuthoring('users', spec), 'sqlite', usersColumns);
    const hand = handRead('find', ($: Recorded, L) => L.Select({ table: 'users', select: ['id', 'name'], limit: $.lim, offset: 5 }), USERS_COLS);
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
  });

  it('find with an IN-subquery condition (Phase E-1 sugar)', () => {
    const spec = {
      select: ['id', 'name'],
      where: ($: Recorded) => [inSubquery($, [col('users', 'id'), col('posts', 'author_id')], [[col('posts', 'author_id'), parentRef(col('users', 'id'))]])],
    };
    const adapter = compileReadBundle(User as never, 'find', findAuthoring('users', spec), 'sqlite', usersColumns);
    const hand = handRead('find', ($: Recorded, L) => L.Select({ table: 'users', select: ['id', 'name'], where: [inSubquery($, [col('users', 'id'), col('posts', 'author_id')], [[col('posts', 'author_id'), parentRef(col('users', 'id'))]])] }), USERS_COLS);
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
  });

  it('count with a WHERE condition', () => {
    const adapter = compileReadBundle(User as never, 'count', countAuthoring('users', ($: Recorded) => [whereEq($.is_active, $.is_active)]), 'sqlite', usersColumns);
    const hand = handRead('count', ($: Recorded, L) => L.Count({ table: 'users', where: [whereEq($.is_active, $.is_active)] }), USERS_COLS);
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
  });

  it('a QUERY view-model read (queryView → Select cte/cteParams) via the adapter', () => {
    // A view model: no base table; the SELECT reads from the declared QUERY as a CTE. The adapter's
    // `compileReadBundle` accepts ANY eager fn, so a queryView-composed read lowers through it
    // byte-identically to a hand-written declaration.
    @model('user_stats')
    class UserStats {
      @column() id?: number;
      @column() post_count?: number;
    }
    const cols = { user_stats: { id: 'INTEGER', post_count: 'INTEGER' } };
    const q = 'SELECT users.id, COUNT(posts.id) as post_count FROM users LEFT JOIN posts ON posts.author_id = users.id GROUP BY users.id';
    const eager = ($: Recorded, L: ComponentFns) => L.Select(queryView(q, ['id', 'post_count'], {}, 'user_stats'));
    const adapter = compileReadBundle(UserStats as never, 'find', eager, 'sqlite', { columnTypes: { id: 'INTEGER', post_count: 'INTEGER' } });
    const hand = handRead('find', eager, cols);
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
    // The QUERY lowered onto the Select cte/cteParams ports (no base table).
    expect(adapter.readGraph).toBeDefined();
  });
});

// ── 3. Writes: create / createMany / update / updateMany / delete / upsert ────────────────────────

describe('F1 writes — create/update/delete byte-identical to hand-written SCP', () => {
  it('create (single INSERT via compileWriteBundle)', () => {
    const spec = {
      values: ($: Recorded) => ({ author_id: $.author_id, title: $.title }),
      returning: 'id, author_id, title',
    };
    const adapter = compileCommandBundle(Post as never, 'create', createAuthoring('posts', spec), NO_EFFECTS, 'create', 'sqlite');
    const contract = compileEager('create', ($: Recorded, L) => L.Insert({ table: 'posts', 'values.author_id': $.author_id, 'values.title': $.title, returning: 'id, author_id, title' }), { columns: { posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'INTEGER', created_at: 'TIMESTAMP' } } });
    const hand = compileWriteBundle(contract, 'create', NO_EFFECTS, 'create', 'sqlite', contract.resolveColumnType);
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
  });

  it('update byte-identical', () => {
    const adapter = compileCommandBundle(Post as never, 'update', updateAuthoring('posts', ($: Recorded) => ({ title: $.title }), ($: Recorded) => [whereEq($.id, $.id)], { returning: 'id, title' }), NO_EFFECTS, 'update', 'sqlite');
    const contract = compileEager('update', ($: Recorded, L) => L.Update({ table: 'posts', where: [whereEq($.id, $.id)], 'set.title': $.title, returning: 'id, title' }), { columns: { posts: { id: 'INTEGER', title: 'INTEGER' } } });
    const hand = compileWriteBundle(contract, 'update', NO_EFFECTS, 'update', 'sqlite', contract.resolveColumnType);
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
  });

  it('delete byte-identical', () => {
    const adapter = compileCommandBundle(Post as never, 'delete', deleteAuthoring('posts', ($: Recorded) => [whereEq($.id, $.id)], 'id'), NO_EFFECTS, 'remove', 'sqlite');
    const contract = compileEager('delete', ($: Recorded, L) => L.Delete({ table: 'posts', where: [whereEq($.id, $.id)], returning: 'id' }), { columns: { posts: { id: 'INTEGER' } } });
    const hand = compileWriteBundle(contract, 'delete', NO_EFFECTS, 'remove', 'sqlite', contract.resolveColumnType);
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
  });

  it('createMany byte-identical (compileCreateManyBundle)', () => {
    const opts = { tableName: 'posts', records: [{ author_id: 1, title: 'a' }, { author_id: 2, title: 'b' }], returning: 'id' };
    const adapter = compileCreateBundle(Post as never, 'createMany', opts, 'sqlite');
    const hand = compileCreateManyBundle('createMany', opts, 'sqlite', modelColumnResolver(Post as never));
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
  });

  it('upsert (create WITH onConflict) carries end-to-end via the createMany path — NO SCP authoring addition', () => {
    const opts = { tableName: 'posts', records: [{ author_id: 1, title: 'a' }], onConflict: ['id'], onConflictUpdate: 'all' as const, returning: 'id' };
    const adapter = compileCreateBundle(Post as never, 'create', opts, 'sqlite');
    const hand = compileCreateManyBundle('create', opts, 'sqlite', modelColumnResolver(Post as never));
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
    // The ON CONFLICT verb reached the SQL text (proves the upsert carried).
    expect(adapter.transaction?.statements?.[0]?.op?.sql ?? adapter.statement?.sql).toMatch(/ON CONFLICT/i);
  });

  it('upsert onConflictIgnore (DO NOTHING) carries', () => {
    const opts = { tableName: 'posts', records: [{ author_id: 1, title: 'a' }], onConflict: ['id'], onConflictIgnore: true, returning: 'id' };
    const adapter = compileCreateBundle(Post as never, 'create', opts, 'sqlite');
    // SQLite's DO-NOTHING verb is `INSERT OR IGNORE` (v1 sqliteSqlBuilder); the ignore carried end-to-end
    // AND the typed-model outType annotation succeeded (writeouttype now recognizes the OR IGNORE verb).
    expect(adapter.statement?.sql).toMatch(/INSERT OR IGNORE/i);
    expect(adapter.outputType).toBeDefined();
  });

  it('updateMany (keyColumns) byte-identical', () => {
    const opts = { tableName: 'posts', keyColumns: ['id'], updateColumns: ['title'], records: [{ id: 1, title: 'x' }, { id: 2, title: 'y' }], returning: 'id' };
    const adapter = compileUpdateBundle('updateMany', opts, 'sqlite');
    const hand = compileUpdateManyBundle('updateMany', opts, 'sqlite');
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
    // keyColumns reached the WHERE/JOIN.
    expect(adapter.statement?.sql).toMatch(/id/);
  });

  it('deleteMany (keyColumns + returning) byte-identical', () => {
    const opts = { tableName: 'posts', keyColumns: ['id'], keys: [{ id: 1 }, { id: 2 }], returning: 'id' };
    const adapter = compileDeleteBundle('deleteMany', opts, 'sqlite');
    const hand = compileDeleteManyBundle('deleteMany', opts, 'sqlite');
    expect(JSON.stringify(adapter)).toBe(JSON.stringify(hand));
  });
});

// ── 4. Relations: hasMany / belongsTo / hasOne (single + composite + limit + hardLimit) ──────────

describe('F1 relations — @hasMany/@belongsTo/@hasOne → RelationDecl → RelationOp byte-identical', () => {
  it('derives a single-key hasMany with per-parent limit + hardLimit + order', () => {
    const decls = deriveRelationDecls(User as never, resolve, 'sqlite');
    const recentPosts = decls.find((d) => d.name === 'recentPosts')!;
    expect(recentPosts).toMatchObject({
      name: 'recentPosts',
      kind: 'hasMany',
      targetTable: 'posts',
      parentKey: 'id',
      targetKey: 'author_id',
      limit: 10,
      hardLimit: 500,
      order: 'created_at DESC',
    });
    expect(recentPosts.select).toEqual(['id', 'author_id', 'title', 'created_at']);
  });

  it('derives a hasOne (single, single-cardinality)', () => {
    const decls = deriveRelationDecls(User as never, resolve, 'sqlite');
    const profile = decls.find((d) => d.name === 'profile')!;
    expect(profile).toMatchObject({ name: 'profile', kind: 'hasOne', targetTable: 'profiles', parentKey: 'id', targetKey: 'user_id' });
  });

  it('derives a belongsTo', () => {
    const decls = deriveRelationDecls(Post as never, resolve, 'sqlite');
    const author = decls.find((d) => d.name === 'author')!;
    expect(author).toMatchObject({ name: 'author', kind: 'belongsTo', targetTable: 'users', parentKey: 'author_id', targetKey: 'id' });
  });

  it('derives a COMPOSITE-key hasMany', () => {
    const decls = deriveRelationDecls(TenantUser as never, resolve, 'sqlite');
    const posts = decls.find((d) => d.name === 'posts')!;
    expect(posts).toMatchObject({ name: 'posts', kind: 'hasMany', targetTable: 'tenant_posts', parentKeys: ['tenant_id', 'id'], targetKeys: ['tenant_id', 'author_id'] });
    expect(posts.parentKey).toBeUndefined();
  });

  it('compileRelationOps is byte-identical to compileRelationOp over the derived decl', () => {
    const ops = compileRelationOps(User as never, resolve, 'sqlite');
    const decls = deriveRelationDecls(User as never, resolve, 'sqlite');
    for (const decl of decls) {
      // The op is compiled with the TARGET model's column resolver (child de-box materializers).
      const targetName = decl.targetTable === 'posts' ? 'Post' : decl.targetTable === 'profiles' ? 'Profile' : 'User';
      const hand = compileRelationOp(decl, modelColumnResolver(registry[targetName] as never));
      expect(JSON.stringify(ops[decl.name])).toBe(JSON.stringify(hand));
    }
  });
});

// ── RED proof: a WRONG column-type mapping diverges the generated bundle ──────────────────────────

describe('F1 RED proof — a wrong column-type mapping makes the generated bundle diverge', () => {
  it('mis-mapping id INTEGER→BIGINT changes the read bundle bytes (a byte-identity test would RED)', () => {
    // Correct: id INTEGER → bc scalar `int`. Wrong: BIGINT → bc scalar `string` (exact-decimal de-box).
    // The read projects `id`, so its outType annotation differs — OBSERVABLE in the bundle bytes.
    const spec = { select: ['id', 'name'], where: ($: Recorded) => [whereEq($.id, $.id)] };
    const correct = compileReadBundle(User as never, 'find', findAuthoring('users', spec), 'sqlite', usersColumns);
    const wrong = compileReadBundle(User as never, 'find', findAuthoring('users', spec), 'sqlite', { columnTypes: { name: 'TEXT', id: 'BIGINT' } });
    // A byte-identity assertion against the correct hand-written bundle would go RED under the wrong map.
    expect(JSON.stringify(wrong)).not.toBe(JSON.stringify(correct));
    // And confirm the CORRECT mapping IS byte-identical to the hand-written oracle (the GREEN direction).
    const hand = handRead('find', ($: Recorded, L) => L.Select({ table: 'users', select: ['id', 'name'], where: [whereEq($.id, $.id)] }), USERS_COLS);
    expect(JSON.stringify(correct)).toBe(JSON.stringify(hand));
  });

  it('a wrong SQL family for a column throws at derive (fail-closed, never a silent wrong bundle)', () => {
    expect(() => columnSqlType('c', { columnName: 'c', sqlCast: 'not-a-family' })).toThrow();
  });
});
