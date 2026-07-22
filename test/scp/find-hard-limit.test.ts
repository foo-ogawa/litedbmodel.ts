/**
 * Phase E-2 find-hard-limit runaway guard on the op-independent leaf read path (#143).
 *
 * The find-hard-limit was a shipped safety feature enforced by the retired `resolveFindGuard` (the
 * read-graph path). #143 relocated it to the SINGLE compile path (`lowerFindGuard`, which bakes
 * `LIMIT hardLimit+1` into the capped primary read leaf) + the read boundary (`executeBehavior` /
 * `read`, which throws `LimitExceededError{context:'find'}` post-fetch). This proves the relocation:
 * a bare row-list read over-cap THROWS with the exact fields; an authored LIMIT / a null cap does NOT.
 * It mirrors the relation twin (`runRelationOp` at src/scp/relation.ts) — a TS-runtime read-boundary
 * guard, off the `executeSQL` node contract (native stays byte-unchanged).
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  read,
  emitRead,
  whereEq,
  setLimitConfig,
  resetLimitConfig,
  LimitExceededError,
  type In,
} from '../../src/scp';

const L = components();

class Posts extends SemanticBehavior {
  static columns = { posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT' } };
  // A bare row-list read with NO authored LIMIT — the find-guard TARGET.
  Feed($: In<{ author_id: number }>) {
    return emitRead(L, 'Select', { table: 'posts', select: ['id', 'author_id', 'title'], where: [whereEq($.author_id, $.author_id)], order: 'id ASC' }, 'sqlite');
  }
  // The SAME read but WITH an authored LIMIT — the find-guard must NOT apply (author governs).
  FeedLimited($: In<{ author_id: number }>) {
    return emitRead(L, 'Select', { table: 'posts', select: ['id', 'author_id', 'title'], where: [whereEq($.author_id, $.author_id)], order: 'id ASC', limit: 100 }, 'sqlite');
  }
}

function freshDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL);`);
  for (let i = 1; i <= 5; i++) db.prepare('INSERT INTO posts VALUES (?, ?, ?)').run(i, 7, `t${i}`);
  return db;
}

describe('Phase E-2 find-hard-limit — leaf read boundary (relocated from resolveFindGuard)', () => {
  let db: InstanceType<typeof Database>;
  beforeEach(() => { db = freshDb(); });
  afterEach(() => { resetLimitConfig(); db.close(); });

  it('THROWS LimitExceededError{context:find} when a bare read exceeds the cap', () => {
    setLimitConfig({ findHardLimit: 2 }); // 5 posts > cap 2
    const contract = publishBehaviors(Posts); // cap baked at publish
    let caught: unknown;
    try {
      read(contract, { author_id: 7 }, { db, entry: 'Feed' });
    } catch (e) { caught = e; }
    expect(caught).toBeInstanceOf(LimitExceededError);
    const err = caught as LimitExceededError;
    expect(err.context).toBe('find');
    expect(err.limit).toBe(2);
    // Bounded fetch: `LIMIT cap+1` means exactly cap+1 rows are read (the total is only known to EXCEED).
    expect(err.count).toBe(3);
    expect(err.model).toBe('Feed');
  });

  it('does NOT throw when the cap is disabled (findHardLimit: null)', () => {
    setLimitConfig({ findHardLimit: null });
    const rows = read(publishBehaviors(Posts), { author_id: 7 }, { db, entry: 'Feed' });
    expect(rows).toHaveLength(5);
  });

  it('does NOT throw when the read is under the cap', () => {
    setLimitConfig({ findHardLimit: 10 }); // 5 posts < cap 10
    const rows = read(publishBehaviors(Posts), { author_id: 7 }, { db, entry: 'Feed' });
    expect(rows).toHaveLength(5);
  });

  it('does NOT throw when an authored LIMIT governs (the guard skips)', () => {
    setLimitConfig({ findHardLimit: 2 });
    const rows = read(publishBehaviors(Posts), { author_id: 7 }, { db, entry: 'FeedLimited' });
    expect(rows).toHaveLength(5); // author LIMIT 100 → no cap injection, all 5 returned
  });

  it('no config ⇒ no guard baked (native/back-compat: the contract carries no findGuards)', () => {
    const contract = publishBehaviors(Posts);
    expect(contract.findGuards).toBeUndefined();
  });
});
