/**
 * WS3 (#23) end-to-end α parity — the REAL vertical slice:
 *
 *   authoring (public API AND SemanticBehavior) → bc ComponentGraphIR → Backend-Compile →
 *   thin runtime → REAL better-sqlite3 execution → assembled result
 *
 * This is α's real bar (behavior-contracts#1's body = the SQLite+TS vertical slice). Each
 * case asserts BOTH:
 *   (a) the golden SQL TEXT the Backend-Compile bridge produces equals litedbmodel v1's SQL
 *       generation (`DBConditions` / `sqliteSqlBuilder`), and
 *   (b) the assembled rows the SCP runtime returns equal litedbmodel v1's DIRECT execution
 *       of that same SQL on the same schema + data.
 *
 * Real DB, no mocks (docker N/A — SQLite is in-process; see runtime.test.ts). A drift in the
 * compiler, renderer, bridge, or runtime fails the test.
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
  compileNode,
  renderOperation,
  whereEq,
  whereGe,
  whereIn,
  inColumn,
  when,
  ne,
  opt,
  type In,
  type Recorded,
  type BehaviorModelContract,
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

/** The Backend-Compiled Select op of a single-Select behavior (for golden text asserts). */
function selectOpOf(contract: BehaviorModelContract, method: string) {
  const node = contract.methods[method].component.body.find(
    (n) => 'component' in n && n.component === 'Select',
  );
  return compileNode(node as never);
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

describe('WS3 α parity — SELECT (golden SQL + assembled-row parity with v1 direct execution)', () => {
  let db: InstanceType<typeof Database>;
  const contract = publishBehaviors(PostSearch);
  beforeEach(() => {
    db = freshDb();
  });

  it('eq + SKIP present + range: golden SQL == v1, rows == v1 direct execution', () => {
    const input = { author_id: 7, status: 'live', since: '2026-01-01' };

    // (a) Golden SQL text: the bridge/renderer output equals v1's SQL for this query.
    const op = selectOpOf(contract, 'Find');
    const rendered = renderOperation(op, input);
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({
      author_id: input.author_id,
      status: input.status,
      'created_at >= ?': input.since,
    }).compile(v1Params);
    const v1Sql = `SELECT id, author_id, title, status, created_at FROM posts WHERE ${v1Where} ORDER BY id ASC`;
    expect(rendered.sql).toBe(v1Sql);
    // Rendered params bind input refs as-is (v1 and SCP both bind the same JS values).
    expect(rendered.params).toEqual(v1Params);

    // (b) Assembled-row parity: SCP runtime rows == v1 direct execution of v1's SQL.
    const v1Rows = db.prepare(v1Sql).all(...v1Params);
    const scpRows = executeBehavior(contract, input, { db, entry: 'Find' });
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows).toEqual([
      { id: 1, author_id: 7, title: 'Hello', status: 'live', created_at: '2026-02-01' },
      { id: 4, author_id: 7, title: 'Later', status: 'live', created_at: '2026-04-01' },
    ]);
  });

  it('SKIP absent-via-refOpt: golden SQL drops the status fragment; rows == v1 without it', () => {
    const input = { author_id: 7, since: '2026-01-01' }; // status OMITTED

    const op = selectOpOf(contract, 'Find');
    const rendered = renderOperation(op, { ...input, status: null });
    // v1 golden with NO status condition (the SKIP fragment is absent).
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({
      author_id: input.author_id,
      'created_at >= ?': input.since,
    }).compile(v1Params);
    const v1Sql = `SELECT id, author_id, title, status, created_at FROM posts WHERE ${v1Where} ORDER BY id ASC`;
    expect(rendered.sql).toBe(v1Sql);
    expect(v1Where).toBe('author_id = ? AND created_at >= ?');

    const v1Rows = db.prepare(v1Sql).all(...v1Params);
    const scpRows = executeBehavior(contract, input, { db, entry: 'Find' });
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows).toEqual([
      { id: 1, author_id: 7, title: 'Hello', status: 'live', created_at: '2026-02-01' },
      { id: 2, author_id: 7, title: 'World', status: 'draft', created_at: '2026-03-01' },
      { id: 4, author_id: 7, title: 'Later', status: 'live', created_at: '2026-04-01' },
    ]);
  });

  it('IN-list: golden SQL == v1 DBConditions, rows == v1 direct execution', () => {
    const input = { ids: [1, 4] };

    const op = selectOpOf(contract, 'ByIds');
    const rendered = renderOperation(op, { ids: input.ids });
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ id: input.ids }).compile(v1Params);
    const v1Sql = `SELECT id, title FROM posts WHERE ${v1Where} ORDER BY id ASC`;
    expect(rendered.sql).toBe(v1Sql);
    expect(rendered.params).toEqual(v1Params);
    expect(v1Where).toBe('id IN (?, ?)');

    const v1Rows = db.prepare(v1Sql).all(...v1Params);
    const scpRows = executeBehavior(contract, input, { db, entry: 'ByIds' });
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows).toEqual([{ id: 1, title: 'Hello' }, { id: 4, title: 'Later' }]);
  });

  it('IN-list empty: golden "1 = 0" == v1, rows == v1 direct execution (empty)', () => {
    const op = selectOpOf(contract, 'ByIds');
    const rendered = renderOperation(op, { ids: [] });
    const v1Where = new DBConditions({ id: [] }).compile([]);
    expect(v1Where).toBe('1 = 0');
    const v1Sql = `SELECT id, title FROM posts WHERE ${v1Where} ORDER BY id ASC`;
    expect(rendered.sql).toBe(v1Sql);

    const v1Rows = db.prepare(v1Sql).all();
    const scpRows = executeBehavior(contract, { ids: [] }, { db, entry: 'ByIds' });
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows).toEqual([]);
  });
});

// ── Single compile path: eager public-API ≡ declaration, both execute to v1 parity ──

describe('WS3 α parity — eager path executes identically to the declaration path', () => {
  it('eager compileEager and the declaration method produce identical SQL and rows', () => {
    const db = freshDb();
    const decl = publishBehaviors(PostSearch);
    const eager = compileEager('ByIds', ($: Recorded, l) =>
      l.Select({ table: 'posts', select: ['id', 'title'], where: [whereIn(inColumn($, 'id'), $.ids)], order: 'id ASC' }),
    );

    // Byte-identical component IR (spec §9 single-compile-path invariant): the eager
    // `ByIds` component equals the declaration `ByIds` component.
    expect(JSON.stringify(eager.methods.ByIds.component)).toBe(JSON.stringify(decl.methods.ByIds.component));

    // Both execute to the same real rows.
    const declRows = executeBehavior(decl, { ids: [1, 4] }, { db, entry: 'ByIds' });
    const eagerRows = executeBehavior(eager, { ids: [1, 4] }, { db, entry: 'ByIds' });
    expect(eagerRows).toEqual(declRows);
    expect(eagerRows).toEqual([{ id: 1, title: 'Hello' }, { id: 4, title: 'Later' }]);
  });
});

// ── INSERT parity (golden SQL == v1 SqliteSqlBuilder, persisted row == direct execution) ──

describe('WS3 α parity — INSERT (golden SQL + real persistence)', () => {
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

  it('golden INSERT SQL uses canonical column order derived like DBModel (non-alpha record)', () => {
    // Un-faked: derive the v2 columns the SAME way `DBModel._insert` does — Object.keys of a
    // NON-alphabetical author record, then `.sort()` (the canonical-order alignment, FIX 1).
    // This exercises the sort rather than hand-pre-sorting the columns. (The full byte-for-
    // byte parity against the REAL DBModel.create public path is in bundle-roundtrip.test.ts.)
    const db = freshDb();
    const contract = publishBehaviors(CreatePost);
    const op = compileNode(
      contract.methods.Create.component.body.find((n) => 'component' in n && n.component === 'Insert') as never,
    );
    const rendered = renderOperation(op, { author_id: 8, title: 'Brand New', created_at: '2026-05-01' });

    // Author key order is NON-alphabetical; DBModel._insert derives + sorts it canonically.
    const authorRecord = { title: 'Brand New', author_id: 8, created_at: '2026-05-01' };
    const canonicalColumns = Object.keys(authorRecord).sort();
    const v2 = sqliteSqlBuilder.buildInsert({
      tableName: 'posts',
      columns: canonicalColumns,
      records: [authorRecord],
      returning: 'id, author_id, title',
    });
    expect(rendered.sql).toBe(v2.sql);
    expect(rendered.sql).toBe(
      'INSERT INTO posts (author_id, created_at, title) VALUES (?, ?, ?) RETURNING id, author_id, title',
    );

    // Real execution: the SCP runtime inserts and returns the new row.
    const scpRows = executeBehavior(contract, authorRecord, { db, entry: 'Create' });
    expect(scpRows).toEqual([{ id: 5, author_id: 8, title: 'Brand New' }]);
    // Direct execution of the same INSERT SQL yields the same RETURNING shape.
    const v2Row = db.prepare(v2.sql).get(...v2.params);
    expect(v2Row).toEqual({ id: 6, author_id: 8, title: 'Brand New' });
  });
});
