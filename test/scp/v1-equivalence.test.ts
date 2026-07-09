/**
 * WS1 (#21) golden equivalence — the SCP compiler's SQLite output is byte-identical to
 * litedbmodel v1's SQL generation for equivalent queries.
 *
 * This is the golden bar the spec sets (§10 / §14 v2α: "既存 SqlBuilder 資産 + golden SQL").
 * It drives BOTH paths from the same inputs and asserts the exact same SQL text + params,
 * so a drift in the SCP compiler that diverges from the maintained v1 line fails loudly.
 */

import { describe, it, expect } from 'vitest';
import { DBConditions } from '../../src/DBConditions';
import { sqliteSqlBuilder } from '../../src/drivers/SqliteSqlBuilder';
import { compileSelect, compileInsert, renderOperation } from '../../src/scp';

const inref = (name: string) => ({ ref: [name] });

describe('WS1 SCP ≡ litedbmodel v1 (SQLite)', () => {
  it('WHERE: equality + AND + IN matches DBConditions.compile', () => {
    // v1 golden.
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({
      author_id: 7,
      'created_at >= ?': '2026-01-01',
      id: [1, 2, 3],
    }).compile(v1Params);

    // SCP path.
    const op = compileSelect({
      table: 'posts',
      select: ['*'],
      where: [
        { kind: 'eq', column: 'author_id', value: inref('authorId') },
        { kind: 'cmp', column: 'created_at', op: '>=', value: inref('since') },
        { kind: 'in', column: 'id', value: inref('ids') },
      ],
    });
    const scp = renderOperation(op, { authorId: 7, since: '2026-01-01', ids: [1, 2, 3] });

    // v1 WHERE body appears verbatim inside the SCP SELECT.
    expect(scp.sql).toBe(`SELECT * FROM posts WHERE ${v1Where}`);
    expect(scp.params).toEqual(v1Params);
    expect(v1Where).toBe('author_id = ? AND created_at >= ? AND id IN (?, ?, ?)');
  });

  it('empty WHERE degeneration matches DBConditions ("")', () => {
    const v1Where = new DBConditions({}).compile([]);
    expect(v1Where).toBe('');
    // SCP: a Select with no conditions has no WHERE splice at all.
    const op = compileSelect({ table: 'posts', select: ['*'] });
    const scp = renderOperation(op, {});
    expect(scp.sql).toBe('SELECT * FROM posts');
  });

  it('empty IN-list "1 = 0" sentinel matches DBConditions', () => {
    const v1Params: unknown[] = [];
    const v1 = new DBConditions({ id: [] }).compile(v1Params);
    expect(v1).toBe('1 = 0');
    expect(v1Params).toEqual([]);

    const op = compileSelect({
      table: 'posts',
      select: ['id'],
      where: [{ kind: 'in', column: 'id', value: inref('ids') }],
    });
    const scp = renderOperation(op, { ids: [] });
    expect(scp.sql).toBe(`SELECT id FROM posts WHERE ${v1}`);
    expect(scp.params).toEqual([]);
  });

  it('INSERT matches SqliteSqlBuilder.buildInsert', () => {
    const v1 = sqliteSqlBuilder.buildInsert({
      tableName: 'posts',
      columns: ['author_id', 'title'],
      records: [{ author_id: 7, title: 'Hello' }],
      returning: 'id, title',
    });
    expect(v1.sql).toBe('INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, title');

    const op = compileInsert({
      table: 'posts',
      values: { author_id: inref('authorId'), title: inref('title') },
      returning: ['id', 'title'],
    });
    const scp = renderOperation(op, { authorId: 7, title: 'Hello' });
    expect(scp.sql).toBe(v1.sql);
    expect(scp.params).toEqual(v1.params);
  });

  it('INSERT OR IGNORE matches SqliteSqlBuilder', () => {
    const v1 = sqliteSqlBuilder.buildInsert({
      tableName: 'idem',
      columns: ['token'],
      records: [{ token: 'r-123' }],
      onConflict: ['token'],
      onConflictIgnore: true,
    });
    expect(v1.sql).toBe('INSERT OR IGNORE INTO idem (token) VALUES (?)');

    const op = compileInsert({
      table: 'idem',
      values: { token: inref('token') },
      onConflict: ['token'],
      onConflictAction: 'ignore',
    });
    const scp = renderOperation(op, { token: 'r-123' });
    expect(scp.sql).toBe(v1.sql);
    expect(scp.params).toEqual(v1.params);
  });

  it('INSERT ON CONFLICT DO UPDATE matches SqliteSqlBuilder', () => {
    const v1 = sqliteSqlBuilder.buildInsert({
      tableName: 'counters',
      columns: ['id', 'n'],
      records: [{ id: 1, n: 5 }],
      onConflict: ['id'],
      onConflictUpdate: ['n'],
    });
    expect(v1.sql).toBe(
      'INSERT INTO counters (id, n) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET n = excluded.n',
    );

    const op = compileInsert({
      table: 'counters',
      values: { id: inref('id'), n: inref('n') },
      onConflict: ['id'],
      onConflictAction: { updateColumns: ['n'] },
    });
    const scp = renderOperation(op, { id: 1, n: 5 });
    expect(scp.sql).toBe(v1.sql);
    expect(scp.params).toEqual(v1.params);
  });
});
