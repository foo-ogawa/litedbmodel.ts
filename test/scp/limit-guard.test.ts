/**
 * Hard-limit runaway prevention (Phase E-2, epic #74; #99 TS reference).
 *
 * Proves the TS SCP runtime throws {@link LimitExceededError} when a top-level read or a hasMany
 * relation batch exceeds its configured hard cap, and SKIPS the check for the exempt cases
 * (`null` disable / explicit author limit / intrinsic per-parent LIMIT window). These are the SAME
 * semantics the conformance `guard.json` expect-error vectors assert, and the reference the
 * rust/go/py/php ports (#100-103) mirror.
 *
 * The guard is authoring-side (`LIMIT hardLimit + 1` injected at compile when no author limit) +
 * a post-fetch count check. It runs against REAL over-cap data (seeded SQLite), so the throw is
 * genuine, not a stub.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileBundle,
  executeBundle,
  readBundle,
  compileReadGraph,
  schemaColumnTypeResolver,
  setLimitConfig,
  resetLimitConfig,
  LimitExceededError,
  whereEq,
  type In,
  type RelationDecl,
} from '../../src/scp';

const L = components();

/** A read whose primary Select carries NO author limit (the find-guard target). */
class Feed extends SemanticBehavior {
  static columns = {
    posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT' },
    tags: { id: 'INTEGER', post_id: 'INTEGER', label: 'TEXT' },
  };
  /** Bare row list, no `limit` port → capped by findHardLimit. */
  Posts($: In<{ author_id: number }>) {
    return L.Select({ table: 'posts', select: ['id', 'author_id', 'title'], where: [whereEq($.author_id, $.author_id)], order: 'id ASC' });
  }
  /** Author explicitly sets a limit → the find-guard is SKIPPED (v1 `!opts?.limit`). */
  PostsLimited($: In<{ author_id: number }>) {
    return L.Select({ table: 'posts', select: ['id', 'author_id', 'title'], where: [whereEq($.author_id, $.author_id)], order: 'id ASC', limit: 100 });
  }
}

const SCHEMA = (n: number): string[] => {
  const ddl = [
    `CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL)`,
    `CREATE TABLE tags (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, label TEXT NOT NULL)`,
  ];
  // n posts for author 7; each post gets 3 tags → the tags batch total is 3n.
  for (let i = 1; i <= n; i++) ddl.push(`INSERT INTO posts VALUES (${i}, 7, 'p${i}')`);
  for (let i = 1; i <= n; i++) for (let t = 0; t < 3; t++) ddl.push(`INSERT INTO tags VALUES (${i * 10 + t}, ${i}, 'l${i}_${t}')`);
  return ddl;
};

function seed(n: number): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  for (const stmt of SCHEMA(n)) db.exec(stmt);
  return db;
}

const resolver = schemaColumnTypeResolver(SCHEMA(1));

// hasMany tags (batch-total capped) + hasMany tags with an intrinsic per-parent LIMIT window (skip).
const tagsUnlimited: RelationDecl = { name: 'tags', kind: 'hasMany', targetTable: 'tags', select: ['id', 'post_id', 'label'], parentKey: 'id', targetKey: 'post_id', order: 'id ASC' };
const tagsWindowed: RelationDecl = { name: 'tags', kind: 'hasMany', targetTable: 'tags', select: ['id', 'post_id', 'label'], parentKey: 'id', targetKey: 'post_id', order: 'id ASC', limit: 1 };

beforeEach(() => resetLimitConfig());
afterEach(() => resetLimitConfig());

describe('find hard-limit (top-level read)', () => {
  it('throws LimitExceededError (context=find) when the read exceeds findHardLimit', () => {
    setLimitConfig({ findHardLimit: 5 });
    const bundle = compileBundle(publishBehaviors(Feed), 'Posts', [], 'sqlite', undefined, resolver);
    const db = seed(10); // 10 rows > cap 5
    try {
      expect(() => executeBundle(bundle, { author_id: 7 }, { db })).toThrowError(LimitExceededError);
      let caught: LimitExceededError | undefined;
      try { executeBundle(bundle, { author_id: 7 }, { db }); } catch (e) { caught = e as LimitExceededError; }
      expect(caught).toBeInstanceOf(LimitExceededError);
      expect(caught!.limit).toBe(5);
      expect(caught!.count).toBe(6); // LIMIT hardLimit+1 = 6 fetched (N+1)
      expect(caught!.context).toBe('find');
      expect(caught!.model).toBe('Posts');
      expect(caught!.relation).toBeUndefined();
      expect(caught!.message).toContain('more than 5');
    } finally { db.close(); }
  });

  it('does NOT throw when the row count is at/under the cap', () => {
    setLimitConfig({ findHardLimit: 20 });
    const bundle = compileBundle(publishBehaviors(Feed), 'Posts', [], 'sqlite', undefined, resolver);
    const db = seed(10);
    try {
      const rows = executeBundle(bundle, { author_id: 7 }, { db }) as unknown[];
      expect(rows.length).toBe(10);
    } finally { db.close(); }
  });

  it('SKIPS the check when findHardLimit is null (disabled)', () => {
    setLimitConfig({ findHardLimit: null });
    const bundle = compileBundle(publishBehaviors(Feed), 'Posts', [], 'sqlite', undefined, resolver);
    const db = seed(10);
    try {
      const rows = executeBundle(bundle, { author_id: 7 }, { db }) as unknown[];
      expect(rows.length).toBe(10); // no throw, no cap
    } finally { db.close(); }
  });

  it('SKIPS the check when the author set an explicit limit', () => {
    setLimitConfig({ findHardLimit: 5 });
    const bundle = compileBundle(publishBehaviors(Feed), 'PostsLimited', [], 'sqlite', undefined, resolver);
    const db = seed(10);
    try {
      const rows = executeBundle(bundle, { author_id: 7 }, { db }) as unknown[];
      expect(rows.length).toBe(10); // author limit 100 governs; no find-guard
    } finally { db.close(); }
  });

  it('does not inject LIMIT / findGuard when disabled (byte-identical SQL)', () => {
    // no config → no guard, no injection
    const off = compileReadGraph(publishBehaviors(Feed), 'sqlite', 'Posts', undefined, resolver);
    expect(off.findGuard).toBeUndefined();
    setLimitConfig({ findHardLimit: 5 });
    const on = compileReadGraph(publishBehaviors(Feed), 'sqlite', 'Posts', undefined, resolver);
    expect(on.findGuard).toEqual({ hardLimit: 5, nodeId: on.findGuard!.nodeId, model: 'Posts' });
    // the injected LIMIT statement is present ONLY when capped
    const stmtsOff = JSON.stringify(off.statementsById);
    const stmtsOn = JSON.stringify(on.statementsById);
    expect(stmtsOff).not.toContain('LIMIT');
    expect(stmtsOn).toContain('LIMIT');
  });
});

describe('hasMany relation hard-limit (batch total)', () => {
  it('throws LimitExceededError (context=relation) with the EXACT count when a batch exceeds hasManyHardLimit', () => {
    setLimitConfig({ hasManyHardLimit: 10 });
    const bundle = compileBundle(publishBehaviors(Feed), 'Posts', [tagsUnlimited], 'sqlite', undefined, resolver);
    const db = seed(5); // 5 posts × 3 tags = 15 child rows > cap 10
    try {
      let caught: LimitExceededError | undefined;
      try { readBundle(bundle, { author_id: 7 }, { db, with: { tags: true } }); } catch (e) { caught = e as LimitExceededError; }
      expect(caught).toBeInstanceOf(LimitExceededError);
      expect(caught!.limit).toBe(10);
      expect(caught!.count).toBe(15); // EXACT batch total (no N+1 for relations)
      expect(caught!.context).toBe('relation');
      expect(caught!.relation).toBe('tags');
      expect(caught!.message).toContain('returned 15 records');
    } finally { db.close(); }
  });

  it('does NOT throw when the batch total is under the cap', () => {
    setLimitConfig({ hasManyHardLimit: 100 });
    const bundle = compileBundle(publishBehaviors(Feed), 'Posts', [tagsUnlimited], 'sqlite', undefined, resolver);
    const db = seed(5);
    try {
      const rows = readBundle(bundle, { author_id: 7 }, { db, with: { tags: true } }) as Record<string, unknown>[];
      expect(rows.length).toBe(5);
      expect((rows[0].tags as unknown[]).length).toBe(3);
    } finally { db.close(); }
  });

  it('SKIPS the check when a per-relation hardLimit override is null (disabled)', () => {
    setLimitConfig({ hasManyHardLimit: 5 });
    const decl: RelationDecl = { ...tagsUnlimited, hardLimit: null };
    const bundle = compileBundle(publishBehaviors(Feed), 'Posts', [decl], 'sqlite', undefined, resolver);
    const db = seed(5); // 15 child rows > global 5, but the relation disables it
    try {
      const rows = readBundle(bundle, { author_id: 7 }, { db, with: { tags: true } }) as Record<string, unknown>[];
      expect(rows.length).toBe(5); // no throw
    } finally { db.close(); }
  });

  it('per-relation hardLimit override WINS over the global', () => {
    setLimitConfig({ hasManyHardLimit: 1000 });
    const decl: RelationDecl = { ...tagsUnlimited, hardLimit: 10 };
    const bundle = compileBundle(publishBehaviors(Feed), 'Posts', [decl], 'sqlite', undefined, resolver);
    const db = seed(5); // 15 > per-relation 10
    try {
      let caught: LimitExceededError | undefined;
      try { readBundle(bundle, { author_id: 7 }, { db, with: { tags: true } }); } catch (e) { caught = e as LimitExceededError; }
      expect(caught).toBeInstanceOf(LimitExceededError);
      expect(caught!.limit).toBe(10);
      expect(caught!.count).toBe(15);
    } finally { db.close(); }
  });

  it('SKIPS the batch-total check for a relation with an intrinsic per-parent LIMIT window', () => {
    setLimitConfig({ hasManyHardLimit: 5 });
    const bundle = compileBundle(publishBehaviors(Feed), 'Posts', [tagsWindowed], 'sqlite', undefined, resolver);
    // The op carries no hardLimit (intrinsic per-parent limit ⇒ bounded fanout, skip).
    expect(bundle.relations.tags.hardLimit).toBeUndefined();
    const db = seed(5); // each parent keeps 1 tag → 5 child rows; total under 5 anyway, but the point is no cap baked
    try {
      const rows = readBundle(bundle, { author_id: 7 }, { db, with: { tags: true } }) as Record<string, unknown>[];
      expect(rows.length).toBe(5);
      expect((rows[0].tags as unknown[]).length).toBe(1);
    } finally { db.close(); }
  });
});
