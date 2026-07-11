/**
 * V0 R1 — CROSS-DB relations: a relation whose TARGET model lives in a DIFFERENT database
 * (v1 `LazyRelation.ts:236` runs a relation on `TargetClass.getDriverType()`'s driver) is
 * batch-loaded against its OWN tagged connection, not the parent's.
 *
 * The proof is a GENUINE two-database run: the parent rows live in DB-A, and the target/child
 * table exists ONLY in DB-B. The relation resolves to real rows IFF the compiled op's `connection`
 * tag routes the batch SELECT to DB-B. Routing to DB-A (the same-DB default) would throw
 * "no such table" — so a green result is unforgeable evidence the tag is honored end-to-end.
 *
 * (This is the TS reference leg of R1. The 4 language runtimes carry the SAME bundle shape; their
 * live cross-DB routing is the escalation item — see docs/proposal/v0-coverage-matrix.md R1.)
 */
import { describe, it, expect } from 'vitest';
import Database from 'better-sqlite3';
import { compileRelationOp } from '../../src/scp/relation';
import { buildResultSet } from '../../src/scp/typed-object';

describe('R1 cross-DB relation routing (TS reference, two real SQLite DBs)', () => {
  it('belongsTo whose target lives in a DIFFERENT db resolves via the connection tag', async () => {
    // DB-A: the parent page (posts). DB-B: the target model (users) — NOT present in DB-A.
    const dbA = new Database(':memory:');
    dbA.exec('CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL)');
    dbA.exec('INSERT INTO posts VALUES (1, 7), (2, 8)');
    const dbB = new Database(':memory:');
    dbB.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    dbB.exec("INSERT INTO users VALUES (7, 'Ada'), (8, 'Alan')");

    // The cross-DB belongsTo: target `users` lives on the 'analytics' connection (DB-B).
    const op = compileRelationOp({
      name: 'author', kind: 'belongsTo', targetTable: 'users', select: ['id', 'name'],
      parentKey: 'author_id', targetKey: 'id', dialect: 'sqlite', connection: 'analytics',
    });
    expect(op.connection).toBe('analytics');

    const parents = dbA.prepare('SELECT id, author_id FROM posts ORDER BY id').all() as Record<string, unknown>[];
    // Primary db = DB-A (parents); the tagged relation routes to DB-B via the registry.
    const rows = buildResultSet(parents, { author: op }, dbA, {
      with: { author: true },
      connections: { analytics: dbB },
    });
    expect(rows).toEqual([
      { id: 1, author_id: 7, author: { id: 7, name: 'Ada' } },
      { id: 2, author_id: 8, author: { id: 8, name: 'Alan' } },
    ]);

    // NEGATIVE: without the registry, the tagged relation has nowhere to route → loud failure
    // (never a silent same-DB fallback that would hit the missing table on DB-A).
    expect(() => buildResultSet(parents, { author: op }, dbA, { with: { author: true } }))
      .toThrow(/no driver registered for connection 'analytics'/);

    dbA.close();
    dbB.close();
  });

  it('an UNTAGGED (same-DB) relation ignores the registry and uses the primary db', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL)');
    db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
    db.exec('INSERT INTO posts VALUES (1, 7)');
    db.exec("INSERT INTO users VALUES (7, 'Ada')");
    const op = compileRelationOp({
      name: 'author', kind: 'belongsTo', targetTable: 'users', select: ['id', 'name'],
      parentKey: 'author_id', targetKey: 'id', dialect: 'sqlite',
    });
    expect('connection' in op).toBe(false);
    const parents = db.prepare('SELECT id, author_id FROM posts').all() as Record<string, unknown>[];
    // A stray registry entry is IGNORED by the untagged relation (routes to the primary db).
    const rows = buildResultSet(parents, { author: op }, db, { with: { author: true }, connections: {} });
    expect(rows).toEqual([{ id: 1, author_id: 7, author: { id: 7, name: 'Ada' } }]);
    db.close();
  });
});
