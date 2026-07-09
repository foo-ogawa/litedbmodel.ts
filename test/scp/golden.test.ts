/**
 * WS1 (#21) golden tests — deterministic IR → SQLite SQL compilation.
 *
 * AC: 同一 IR + 入力 → 同一 SQL テキスト. Each test compiles a representative operation to
 * SQL IR (`src/scp/compile-sqlite.ts`), renders it against a bound input scope
 * (`src/scp/render.ts`), and asserts the EXACT SQL text + params. The expected text is the
 * litedbmodel v1 golden bar (verified against `DBConditions` / `SqliteSqlBuilder` /
 * `_buildSelectSQL` in the equivalence test below), so a drift in either the compiler or
 * the renderer fails the test.
 */

import { describe, it, expect } from 'vitest';
import {
  compileSelect,
  compileInsert,
  compileUpdate,
  compileDelete,
  renderOperation,
  assertOperationPortable,
  type Condition,
} from '../../src/scp';

// Expression IR helpers (bc closed set only). bc uses a FLAT scope (input ports are
// top-level roots — see runBehavior's `{...input, ...results}`), so an input reference is
// `{ref:["authorId"]}` and the render scope is `{ authorId: … }` (no `input` wrapper).
const inref = (name: string) => ({ ref: [name] });

describe('WS1 golden: SELECT', () => {
  it('simple equality WHERE', () => {
    const op = compileSelect({
      table: 'posts',
      select: ['id', 'title'],
      where: [{ kind: 'eq', column: 'author_id', value: inref('authorId') }],
    });
    const r = renderOperation(op, { authorId: 7n });
    expect(r.sql).toBe('SELECT id, title FROM posts WHERE author_id = ?');
    expect(r.params).toEqual([7n]);
  });

  it('multi-condition AND + ORDER BY + LIMIT', () => {
    const op = compileSelect({
      table: 'posts',
      select: ['id', 'author_id', 'title', 'created_at'],
      where: [
        { kind: 'eq', column: 'author_id', value: inref('authorId') },
        { kind: 'cmp', column: 'created_at', op: '>=', value: inref('since') },
      ],
      order: 'created_at DESC',
      // limit default lives in the Expression IR (coalesce), NOT an ad-hoc `?? 20` in code.
      limit: { coalesce: [{ refOpt: ['limit'] }, 20] },
    });
    const r = renderOperation(op, { authorId: 7n, since: '2026-01-01', limit: null });
    expect(r.sql).toBe(
      'SELECT id, author_id, title, created_at FROM posts WHERE author_id = ? AND created_at >= ? ORDER BY created_at DESC LIMIT ?',
    );
    expect(r.params).toEqual([7n, '2026-01-01', 20n]);
  });

  it('SKIP optional condition — present vs absent', () => {
    const op = compileSelect({
      table: 'posts',
      select: ['id'],
      where: [
        { kind: 'eq', column: 'author_id', value: inref('authorId') },
        // SKIP: present only when status is non-null.
        {
          kind: 'eq',
          column: 'status',
          value: inref('status'),
          skipWhen: { ne: [{ refOpt: ['status'] }, null] },
        },
      ],
    });

    const present = renderOperation(op, { authorId: 7n, status: 'live' });
    expect(present.sql).toBe('SELECT id FROM posts WHERE author_id = ? AND status = ?');
    expect(present.params).toEqual([7n, 'live']);

    const absent = renderOperation(op, { authorId: 7n, status: null });
    expect(absent.sql).toBe('SELECT id FROM posts WHERE author_id = ?');
    expect(absent.params).toEqual([7n]);
  });

  it('IN-list array expansion (N and empty)', () => {
    const op = compileSelect({
      table: 'posts',
      select: ['id'],
      where: [{ kind: 'in', column: 'id', value: inref('ids') }],
    });

    const three = renderOperation(op, { ids: [1n, 2n, 3n] });
    expect(three.sql).toBe('SELECT id FROM posts WHERE id IN (?, ?, ?)');
    expect(three.params).toEqual([1n, 2n, 3n]);

    // empty array degenerates to the always-false sentinel (v1 parity), no params.
    const empty = renderOperation(op, { ids: [] });
    expect(empty.sql).toBe('SELECT id FROM posts WHERE 1 = 0');
    expect(empty.params).toEqual([]);
  });

  it('empty-WHERE degeneration (all conditions SKIP absent)', () => {
    const op = compileSelect({
      table: 'posts',
      select: ['*'],
      where: [
        {
          kind: 'eq',
          column: 'status',
          value: inref('status'),
          skipWhen: { ne: [{ refOpt: ['status'] }, null] },
        },
      ],
      order: 'id ASC',
    });
    const r = renderOperation(op, { status: null });
    // WHERE keyword dropped entirely.
    expect(r.sql).toBe('SELECT * FROM posts ORDER BY id ASC');
    expect(r.params).toEqual([]);
  });

  it('NULL condition + nested OR group with parenthesization', () => {
    const where: Condition[] = [
      { kind: 'isNull', column: 'deleted_at' },
      {
        kind: 'group',
        connector: 'OR',
        conditions: [
          { kind: 'eq', column: 'a', value: inref('a') },
          { kind: 'eq', column: 'b', value: inref('b') },
        ],
      },
    ];
    const op = compileSelect({ table: 't', select: ['*'], where });
    const r = renderOperation(op, { a: 1n, b: 2n });
    expect(r.sql).toBe('SELECT * FROM t WHERE deleted_at IS NULL AND (a = ? OR b = ?)');
    expect(r.params).toEqual([1n, 2n]);
  });
});

describe('WS1 golden: INSERT', () => {
  it('basic insert with RETURNING', () => {
    const op = compileInsert({
      table: 'posts',
      values: { author_id: inref('authorId'), title: inref('title') },
      returning: ['id', 'title'],
    });
    const r = renderOperation(op, { authorId: 7n, title: 'Hello' });
    expect(r.sql).toBe('INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, title');
    expect(r.params).toEqual([7n, 'Hello']);
  });

  it('insert OR IGNORE', () => {
    const op = compileInsert({
      table: 'idem',
      values: { token: inref('token') },
      onConflict: ['token'],
      onConflictAction: 'ignore',
    });
    const r = renderOperation(op, { token: 'r-123' });
    expect(r.sql).toBe('INSERT OR IGNORE INTO idem (token) VALUES (?)');
    expect(r.params).toEqual(['r-123']);
  });

  it('insert ON CONFLICT DO UPDATE', () => {
    const op = compileInsert({
      table: 'counters',
      values: { id: inref('id'), n: inref('n') },
      onConflict: ['id'],
      onConflictAction: { updateColumns: ['n'] },
    });
    const r = renderOperation(op, { id: 1n, n: 5n });
    expect(r.sql).toBe(
      'INSERT INTO counters (id, n) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET n = excluded.n',
    );
    expect(r.params).toEqual([1n, 5n]);
  });
});

describe('WS1 golden: UPDATE', () => {
  it('SET + WHERE with RETURNING', () => {
    const op = compileUpdate({
      table: 'users',
      set: { post_count: { add: [{ ref: ['cur'] }, 1] } },
      where: [{ kind: 'eq', column: 'id', value: inref('id') }],
      returning: ['id', 'post_count'],
    });
    const r = renderOperation(op, { cur: 4n, id: 7n });
    expect(r.sql).toBe('UPDATE users SET post_count = ? WHERE id = ? RETURNING id, post_count');
    // SET param first (before {where}), then WHERE param.
    expect(r.params).toEqual([5n, 7n]);
  });

  it('multi-column SET, param order = SET then WHERE', () => {
    const op = compileUpdate({
      table: 'posts',
      set: { title: inref('title'), body: inref('body') },
      where: [{ kind: 'eq', column: 'id', value: inref('id') }],
    });
    const r = renderOperation(op, { title: 'T', body: 'B', id: 9n });
    expect(r.sql).toBe('UPDATE posts SET title = ?, body = ? WHERE id = ?');
    expect(r.params).toEqual(['T', 'B', 9n]);
  });
});

describe('WS1 golden: DELETE', () => {
  it('WHERE equality', () => {
    const op = compileDelete({
      table: 'posts',
      where: [{ kind: 'eq', column: 'id', value: inref('id') }],
    });
    const r = renderOperation(op, { id: 42n });
    expect(r.sql).toBe('DELETE FROM posts WHERE id = ?');
    expect(r.params).toEqual([42n]);
  });

  it('WHERE AND with RETURNING', () => {
    const op = compileDelete({
      table: 'posts',
      where: [
        { kind: 'eq', column: 'author_id', value: inref('authorId') },
        { kind: 'in', column: 'id', value: inref('ids') },
      ],
      returning: ['id'],
    });
    const r = renderOperation(op, { authorId: 7n, ids: [1n, 2n] });
    expect(r.sql).toBe('DELETE FROM posts WHERE author_id = ? AND id IN (?, ?) RETURNING id');
    expect(r.params).toEqual([7n, 1n, 2n]);
  });
});

describe('WS1 golden: determinism (same IR + input → same SQL text)', () => {
  it('re-rendering the same op with the same input is byte-identical', () => {
    const op = compileSelect({
      table: 'posts',
      select: ['id', 'title'],
      where: [
        { kind: 'eq', column: 'author_id', value: inref('authorId') },
        {
          kind: 'eq',
          column: 'status',
          value: inref('status'),
          skipWhen: { ne: [{ refOpt: ['status'] }, null] },
        },
      ],
      order: 'created_at DESC',
      limit: { coalesce: [{ refOpt: ['limit'] }, 20] },
    });
    const input = { authorId: 7n, status: 'live', limit: 10n };
    const a = renderOperation(op, input);
    const b = renderOperation(op, input);
    expect(a.sql).toBe(b.sql);
    expect(a.params).toEqual(b.params);
    expect(a.sql).toBe(
      'SELECT id, title FROM posts WHERE author_id = ? AND status = ? ORDER BY created_at DESC LIMIT ?',
    );
    expect(a.params).toEqual([7n, 'live', 10n]);
  });

  it('every emitted param slot is portable (closed Expression IR only)', () => {
    const ops = [
      compileSelect({
        table: 'posts',
        select: ['id'],
        where: [{ kind: 'eq', column: 'author_id', value: inref('authorId') }],
        limit: { coalesce: [{ refOpt: ['limit'] }, 20] },
      }),
      compileInsert({ table: 'posts', values: { title: inref('title') } }),
      compileUpdate({
        table: 'users',
        set: { post_count: { add: [{ ref: ['cur'] }, 1] } },
        where: [{ kind: 'eq', column: 'id', value: inref('id') }],
      }),
      compileDelete({ table: 'posts', where: [{ kind: 'in', column: 'id', value: inref('ids') }] }),
    ];
    for (const op of ops) expect(() => assertOperationPortable(op)).not.toThrow();
  });
});
