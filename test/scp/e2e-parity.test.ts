/**
 * WS3 (#23) end-to-end α parity — the REAL vertical slice on the makeSQL path (epic #43/#45):
 *
 *   authoring (public API AND SemanticBehavior) → static makeSQL bundle → bc runBehavior +
 *   makeSQL handler → REAL better-sqlite3 execution → assembled result
 *
 * Each case asserts the assembled rows the SCP runtime returns EQUAL litedbmodel v1's DIRECT
 * execution of the equivalent v1 SQL on the same schema + data (result parity — the v2 bar for
 * MySQL/SQLite array surfaces, which intentionally deviate from v1's placeholder-expansion TEXT
 * but reproduce the SAME rows; byte-level golden-from-originals is pinned in makesql-golden). A
 * drift in the compiler or runtime fails the test. Real DB, no mocks (SQLite is in-process).
 */

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import { DBConditions } from '../../src/DBConditions';
import { sqliteSqlBuilder } from '../../src/drivers/SqliteSqlBuilder';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileEager,
  executeBehavior,
  whereEq,
  whereGe,
  whereIn,
  inColumn,
  when,
  ne,
  opt,
  type In,
  type Recorded,
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
  db.prepare('INSERT INTO posts VALUES (?,?,?,?,?)').run(4, 7, 'Later', 'live', '2026-04-01');
  return db;
}

// ── The authored behavior (declaration surface) ───────────────────────────────

class PostSearch extends SemanticBehavior {
  Find($: In<{ author_id: number; status?: string; since: string }>) {
    return L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title', 'status', 'created_at'],
      where: [
        whereEq($.author_id, $.author_id),
        when(ne(opt($.status), null), () => whereEq($.status, $.status)),
        whereGe($.created_at, $.since),
      ],
      order: 'id ASC',
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

describe('WS3 α parity — SELECT (assembled-row parity with v1 direct execution)', () => {
  let db: InstanceType<typeof Database>;
  const contract = publishBehaviors(PostSearch);
  beforeEach(() => {
    db = freshDb();
  });

  it('eq + SKIP present + range: rows == v1 direct execution', () => {
    const input = { author_id: 7, status: 'live', since: '2026-01-01' };
    // v1 golden SQL (the equivalence bar): execute it directly and compare rows.
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({
      author_id: input.author_id,
      status: input.status,
      'created_at >= ?': input.since,
    }).compile(v1Params);
    const v1Sql = `SELECT id, author_id, title, status, created_at FROM posts WHERE ${v1Where} ORDER BY id ASC`;
    const v1Rows = db.prepare(v1Sql).all(...v1Params);

    const scpRows = executeBehavior(contract, input, { db, entry: 'Find' });
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows).toEqual([
      { id: 1, author_id: 7, title: 'Hello', status: 'live', created_at: '2026-02-01' },
      { id: 4, author_id: 7, title: 'Later', status: 'live', created_at: '2026-04-01' },
    ]);
  });

  it('SKIP absent-via-refOpt: status fragment dropped; rows == v1 without it', () => {
    const input = { author_id: 7, since: '2026-01-01' }; // status OMITTED
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({
      author_id: input.author_id,
      'created_at >= ?': input.since,
    }).compile(v1Params);
    expect(v1Where).toBe('author_id = ? AND created_at >= ?');
    const v1Sql = `SELECT id, author_id, title, status, created_at FROM posts WHERE ${v1Where} ORDER BY id ASC`;
    const v1Rows = db.prepare(v1Sql).all(...v1Params);

    const scpRows = executeBehavior(contract, input, { db, entry: 'Find' });
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows).toEqual([
      { id: 1, author_id: 7, title: 'Hello', status: 'live', created_at: '2026-02-01' },
      { id: 2, author_id: 7, title: 'World', status: 'draft', created_at: '2026-03-01' },
      { id: 4, author_id: 7, title: 'Later', status: 'live', created_at: '2026-04-01' },
    ]);
  });

  it('IN-list: rows == v1 direct execution (single-JSON param, result parity)', () => {
    const input = { ids: [1, 4] };
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ id: input.ids }).compile(v1Params);
    expect(v1Where).toBe('id IN (?, ?)');
    const v1Rows = db.prepare(`SELECT id, title FROM posts WHERE ${v1Where} ORDER BY id ASC`).all(...v1Params);

    const scpRows = executeBehavior(contract, input, { db, entry: 'ByIds' });
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows).toEqual([{ id: 1, title: 'Hello' }, { id: 4, title: 'Later' }]);
  });

  it('IN-list empty: rows == v1 (empty), single-JSON param over no keys', () => {
    const v1Where = new DBConditions({ id: [] }).compile([]);
    expect(v1Where).toBe('1 = 0');
    const v1Rows = db.prepare(`SELECT id, title FROM posts WHERE ${v1Where} ORDER BY id ASC`).all();

    const scpRows = executeBehavior(contract, { ids: [] }, { db, entry: 'ByIds' });
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows).toEqual([]);
  });
});

// ── Single compile path: eager public-API ≡ declaration, both execute to v1 parity ──

describe('WS3 α parity — eager path executes identically to the declaration path', () => {
  it('eager compileEager and the declaration method produce identical IR and rows', () => {
    const db = freshDb();
    const decl = publishBehaviors(PostSearch);
    const eager = compileEager('ByIds', ($: Recorded, l) =>
      l.Select({ table: 'posts', select: ['id', 'title'], where: [whereIn(inColumn($, 'id'), $.ids)], order: 'id ASC' }),
    );

    // Byte-identical component IR (spec §9 single-compile-path invariant).
    expect(JSON.stringify(eager.methods.ByIds.component)).toBe(JSON.stringify(decl.methods.ByIds.component));

    const declRows = executeBehavior(decl, { ids: [1, 4] }, { db, entry: 'ByIds' });
    const eagerRows = executeBehavior(eager, { ids: [1, 4] }, { db, entry: 'ByIds' });
    expect(eagerRows).toEqual(declRows);
    expect(eagerRows).toEqual([{ id: 1, title: 'Hello' }, { id: 4, title: 'Later' }]);
  });
});

// ── INSERT parity (canonical column order == v1 SqliteSqlBuilder, persisted row) ──

describe('WS3 α parity — INSERT (real persistence, canonical column order)', () => {
  class CreatePost extends SemanticBehavior {
    Create($: In<{ author_id: number; title: string; created_at: string }>) {
      return L.Insert({
        table: 'posts',
        'values.author_id': $.author_id,
        'values.title': $.title,
        'values.created_at': $.created_at,
        returning: 'id, author_id, title',
      });
    }
  }

  it('INSERT persists + returns the new row; column order is canonical like DBModel', () => {
    const db = freshDb();
    const contract = publishBehaviors(CreatePost);

    // v1 canonical order (DBModel._insert derives Object.keys then .sort()).
    const authorRecord = { title: 'Brand New', author_id: 8, created_at: '2026-05-01' };
    const canonicalColumns = Object.keys(authorRecord).sort();
    const v2 = sqliteSqlBuilder.buildInsert({
      tableName: 'posts',
      columns: canonicalColumns,
      records: [authorRecord],
      returning: 'id, author_id, title',
    });
    expect(v2.sql).toBe(
      'INSERT INTO posts (author_id, created_at, title) VALUES (?, ?, ?) RETURNING id, author_id, title',
    );

    const scpRows = executeBehavior(contract, authorRecord, { db, entry: 'Create' });
    expect(scpRows).toEqual([{ id: 5, author_id: 8, title: 'Brand New' }]);
    // Direct execution of the same v1 INSERT SQL yields the same RETURNING shape.
    const v2Row = db.prepare(v2.sql).get(...v2.params);
    expect(v2Row).toEqual({ id: 6, author_id: 8, title: 'Brand New' });
  });
});
