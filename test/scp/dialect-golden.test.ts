/**
 * WS6 (#26) dialect golden — same Component-graph IR + input → dialect-specific SQL text,
 * byte-identical PER DIALECT (SQLite / Postgres / MySQL), validated TS-side (no live DB).
 *
 * AC (#26):
 *   - SQLite/PG/MySQL で同一 Component-graph IR → 方言別 SQL
 *   - 方言 golden（IR+入力→方言別 SQL テキスト）green
 *   - `?`→`$N` は PG のみ最終1パスで機械変換
 *
 * The golden bar is the REAL v1 SqlBuilder output (`sqliteSqlBuilder` / `postgresSqlBuilder` /
 * `mysqlSqlBuilder` `buildInsert`) for equivalent single-record operations — NOT hand-written
 * expectations (avoids the WS3 faked-parity pattern). For SELECT/UPDATE/DELETE the dialect text
 * is identical across dialects except the PG `?`→`$N` conversion, so we assert the SCP output of
 * the SAME compiled op equals the SQLite text with the dialect's placeholder finalization
 * applied. The `?`→`$N` pass is asserted to be byte-identical to v1's documented one-pass
 * (`sql.replace(/\?/g, () => '$'+(++i))`, src/drivers/postgres.ts convertPlaceholders).
 */

import { describe, it, expect } from 'vitest';
import { sqliteSqlBuilder } from '../../src/drivers/SqliteSqlBuilder';
import { postgresSqlBuilder } from '../../src/drivers/PostgresSqlBuilder';
import { mysqlSqlBuilder } from '../../src/drivers/MysqlSqlBuilder';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileBundle,
  compileWriteBundle,
  entityWrites,
  whereEq,
  compileSelect,
  compileUpdate,
  compileDelete,
  compileInsertFor,
  renderOperation,
  toDollarPlaceholders,
  dialectFor,
  SQLITE,
  POSTGRES,
  MYSQL,
  type In,
  type DialectName,
  type SqlBundle,
} from '../../src/scp';

const inref = (name: string) => ({ ref: [name] });

/** v1's documented `?`→`$N` one-pass (src/drivers/postgres.ts convertPlaceholders) — pinned. */
function v1ConvertPlaceholders(sql: string): string {
  let index = 0;
  return sql.replace(/\?/g, () => `$${++index}`);
}

const ALL_DIALECTS: DialectName[] = ['sqlite', 'postgres', 'mysql'];

describe('WS6 dialect golden — `?`→`$N` is a PG-only final one-pass', () => {
  it('toDollarPlaceholders is byte-identical to v1 convertPlaceholders (left-to-right, once)', () => {
    const samples = [
      'SELECT id FROM posts WHERE author_id = ? AND status = ?',
      'INSERT INTO posts (a, b, c) VALUES (?, ?, ?) RETURNING id',
      'UPDATE users SET post_count = post_count + ? WHERE id = ?',
      'SELECT id FROM posts WHERE id IN (?, ?, ?, ?, ?)',
      'DELETE FROM posts WHERE author_id = ? AND id IN (?, ?) RETURNING id',
      'no placeholders here',
    ];
    for (const s of samples) {
      expect(toDollarPlaceholders(s)).toBe(v1ConvertPlaceholders(s));
    }
  });

  it('numbering never reassigns — 12 placeholders map to $1..$12 in order', () => {
    const many = Array.from({ length: 12 }, () => '?').join(', ');
    const sql = `INSERT INTO t (${Array.from({ length: 12 }, (_, i) => `c${i}`).join(', ')}) VALUES (${many})`;
    const got = POSTGRES.finalizePlaceholders(sql);
    const expectedTail = Array.from({ length: 12 }, (_, i) => `$${i + 1}`).join(', ');
    expect(got.endsWith(`VALUES (${expectedTail})`)).toBe(true);
    // exactly one $N per original ?, in order, no gaps/dupes
    expect((got.match(/\$\d+/g) ?? []).map((m) => m.slice(1)).map(Number)).toEqual(
      Array.from({ length: 12 }, (_, i) => i + 1),
    );
  });

  it('SQLite and MySQL keep `?` (identity finalization); only PG converts', () => {
    const sql = 'SELECT id FROM t WHERE a = ? AND b = ?';
    expect(SQLITE.finalizePlaceholders(sql)).toBe(sql);
    expect(MYSQL.finalizePlaceholders(sql)).toBe(sql);
    expect(POSTGRES.finalizePlaceholders(sql)).toBe('SELECT id FROM t WHERE a = $1 AND b = $2');
  });
});

describe('WS6 dialect golden — INSERT (byte-identical to the REAL v1 SqlBuilders per dialect)', () => {
  // The SAME logical Insert description compiled per-dialect.
  const desc = {
    table: 'posts',
    values: { author_id: inref('authorId'), title: inref('title') },
    returning: ['id', 'title'],
  } as const;
  const input = { authorId: 7, title: 'Hello' };

  it('plain INSERT + RETURNING — SQLite ≡ v1 sqliteSqlBuilder', () => {
    const scp = renderOperation(compileInsertFor(SQLITE, desc), input, SQLITE);
    const v1 = sqliteSqlBuilder.buildInsert({
      tableName: 'posts',
      columns: ['author_id', 'title'],
      records: [{ author_id: 7, title: 'Hello' }],
      returning: 'id, title',
    });
    expect(scp.sql).toBe(v1.sql);
    expect(scp.sql).toBe('INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, title');
    expect(scp.params).toEqual(v1.params);
  });

  it('plain INSERT + RETURNING — MySQL ≡ v1 mysqlSqlBuilder', () => {
    const scp = renderOperation(compileInsertFor(MYSQL, desc), input, MYSQL);
    const v1 = mysqlSqlBuilder.buildInsert({
      tableName: 'posts',
      columns: ['author_id', 'title'],
      records: [{ author_id: 7, title: 'Hello' }],
      returning: 'id, title',
    });
    expect(scp.sql).toBe(v1.sql);
    // MySQL keeps `?` and the v1 builder emits the RETURNING (driver simulates it).
    expect(scp.sql).toBe('INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, title');
    expect(scp.params).toEqual(v1.params);
  });

  it('plain INSERT + RETURNING — Postgres ≡ v1 postgresSqlBuilder with `?`→`$N`', () => {
    const scp = renderOperation(compileInsertFor(POSTGRES, desc), input, POSTGRES);
    const v1 = postgresSqlBuilder.buildInsert({
      tableName: 'posts',
      columns: ['author_id', 'title'],
      records: [{ author_id: 7, title: 'Hello' }],
      returning: 'id, title',
    });
    // The v1 PG builder emits `?`; the driver converts to `$N`. SCP applies the SAME conversion
    // in its render finalization pass. Assert against the REAL v1 builder output converted.
    expect(scp.sql).toBe(v1ConvertPlaceholders(v1.sql));
    expect(scp.sql).toBe('INSERT INTO posts (author_id, title) VALUES ($1, $2) RETURNING id, title');
    expect(scp.params).toEqual(v1.params);
  });

  it('INSERT ... ignore — verb/clause differs per dialect (v1 SqlBuilder parity)', () => {
    const ignoreDesc = {
      table: 'idem',
      values: { token: inref('token') },
      onConflict: ['token'],
      onConflictAction: 'ignore',
    } as const;
    const ins = { token: 'r-1' };

    const sqlite = renderOperation(compileInsertFor(SQLITE, ignoreDesc), ins, SQLITE);
    const v1sqlite = sqliteSqlBuilder.buildInsert({
      tableName: 'idem', columns: ['token'], records: [{ token: 'r-1' }], onConflict: ['token'], onConflictIgnore: true,
    });
    expect(sqlite.sql).toBe(v1sqlite.sql);
    expect(sqlite.sql).toBe('INSERT OR IGNORE INTO idem (token) VALUES (?)');

    const mysql = renderOperation(compileInsertFor(MYSQL, ignoreDesc), ins, MYSQL);
    const v1mysql = mysqlSqlBuilder.buildInsert({
      tableName: 'idem', columns: ['token'], records: [{ token: 'r-1' }], onConflict: ['token'], onConflictIgnore: true,
    });
    expect(mysql.sql).toBe(v1mysql.sql);
    expect(mysql.sql).toBe('INSERT IGNORE INTO idem (token) VALUES (?)');

    const pg = renderOperation(compileInsertFor(POSTGRES, ignoreDesc), ins, POSTGRES);
    const v1pg = postgresSqlBuilder.buildInsert({
      tableName: 'idem', columns: ['token'], records: [{ token: 'r-1' }], onConflict: ['token'], onConflictIgnore: true,
    });
    expect(pg.sql).toBe(v1ConvertPlaceholders(v1pg.sql));
    expect(pg.sql).toBe('INSERT INTO idem (token) VALUES ($1) ON CONFLICT (token) DO NOTHING');
  });

  it('INSERT ... ON CONFLICT DO UPDATE — upsert SET form differs per dialect (v1 parity)', () => {
    const upsertDesc = {
      table: 'counters',
      values: { id: inref('id'), n: inref('n') },
      onConflict: ['id'],
      onConflictAction: { updateColumns: ['n'] },
    } as const;
    const ins = { id: 1, n: 5 };

    const sqlite = renderOperation(compileInsertFor(SQLITE, upsertDesc), ins, SQLITE);
    const v1sqlite = sqliteSqlBuilder.buildInsert({
      tableName: 'counters', columns: ['id', 'n'], records: [{ id: 1, n: 5 }], onConflict: ['id'], onConflictUpdate: ['n'],
    });
    expect(sqlite.sql).toBe(v1sqlite.sql);
    expect(sqlite.sql).toBe('INSERT INTO counters (id, n) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET n = excluded.n');

    const mysql = renderOperation(compileInsertFor(MYSQL, upsertDesc), ins, MYSQL);
    const v1mysql = mysqlSqlBuilder.buildInsert({
      tableName: 'counters', columns: ['id', 'n'], records: [{ id: 1, n: 5 }], onConflict: ['id'], onConflictUpdate: ['n'],
    });
    expect(mysql.sql).toBe(v1mysql.sql);
    expect(mysql.sql).toBe('INSERT INTO counters (id, n) VALUES (?, ?) ON DUPLICATE KEY UPDATE n = VALUES(n)');

    const pg = renderOperation(compileInsertFor(POSTGRES, upsertDesc), ins, POSTGRES);
    const v1pg = postgresSqlBuilder.buildInsert({
      tableName: 'counters', columns: ['id', 'n'], records: [{ id: 1, n: 5 }], onConflict: ['id'], onConflictUpdate: ['n'],
    });
    expect(pg.sql).toBe(v1ConvertPlaceholders(v1pg.sql));
    expect(pg.sql).toBe('INSERT INTO counters (id, n) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n');
  });
});

describe('WS6 dialect golden — SELECT / UPDATE / DELETE (same op, per-dialect placeholders)', () => {
  it('SELECT: identical text across SQLite/MySQL, `$N` only for PG', () => {
    const op = compileSelect({
      table: 'posts',
      select: ['id', 'author_id', 'title'],
      where: [
        { kind: 'eq', column: 'author_id', value: inref('authorId') },
        { kind: 'in', column: 'id', value: inref('ids') },
      ],
      order: 'id ASC',
    });
    const input = { authorId: 7, ids: [1, 2, 3] };

    const sqlite = renderOperation(op, input, SQLITE);
    const mysql = renderOperation(op, input, MYSQL);
    const pg = renderOperation(op, input, POSTGRES);

    // SQLite ≡ MySQL (both `?`); byte-identical.
    expect(sqlite.sql).toBe('SELECT id, author_id, title FROM posts WHERE author_id = ? AND id IN (?, ?, ?) ORDER BY id ASC');
    expect(mysql.sql).toBe(sqlite.sql);
    // PG: the SAME text with `?`→`$N` applied AFTER IN-list expansion (no renumbering bug).
    expect(pg.sql).toBe('SELECT id, author_id, title FROM posts WHERE author_id = $1 AND id IN ($2, $3, $4) ORDER BY id ASC');
    expect(pg.sql).toBe(v1ConvertPlaceholders(sqlite.sql));
    // Params identical across dialects (values are dialect-neutral).
    expect(pg.params).toEqual(sqlite.params);
    expect(mysql.params).toEqual(sqlite.params);
  });

  it('UPDATE: SET before WHERE, PG numbers SET then WHERE left-to-right', () => {
    const op = compileUpdate({
      table: 'users',
      set: { post_count: { add: [{ ref: ['cur'] }, 1] } },
      where: [{ kind: 'eq', column: 'id', value: inref('id') }],
      returning: ['id', 'post_count'],
    });
    const input = { cur: 4n, id: 7 };
    const sqlite = renderOperation(op, input, SQLITE);
    const pg = renderOperation(op, input, POSTGRES);
    expect(sqlite.sql).toBe('UPDATE users SET post_count = ? WHERE id = ? RETURNING id, post_count');
    expect(pg.sql).toBe('UPDATE users SET post_count = $1 WHERE id = $2 RETURNING id, post_count');
    expect(pg.sql).toBe(v1ConvertPlaceholders(sqlite.sql));
  });

  it('DELETE: WHERE AND + IN, PG `$N` after expansion', () => {
    const op = compileDelete({
      table: 'posts',
      where: [
        { kind: 'eq', column: 'author_id', value: inref('authorId') },
        { kind: 'in', column: 'id', value: inref('ids') },
      ],
      returning: ['id'],
    });
    const input = { authorId: 7, ids: [1, 2] };
    const sqlite = renderOperation(op, input, SQLITE);
    const mysql = renderOperation(op, input, MYSQL);
    const pg = renderOperation(op, input, POSTGRES);
    expect(sqlite.sql).toBe('DELETE FROM posts WHERE author_id = ? AND id IN (?, ?) RETURNING id');
    expect(mysql.sql).toBe(sqlite.sql);
    expect(pg.sql).toBe('DELETE FROM posts WHERE author_id = $1 AND id IN ($2, $3) RETURNING id');
  });

  it('empty IN-list "1 = 0" degeneration is identical across all dialects', () => {
    const op = compileSelect({
      table: 'posts',
      select: ['id'],
      where: [{ kind: 'in', column: 'id', value: inref('ids') }],
    });
    for (const name of ALL_DIALECTS) {
      const r = renderOperation(op, { ids: [] }, dialectFor(name));
      expect(r.sql).toBe('SELECT id FROM posts WHERE 1 = 0');
      expect(r.params).toEqual([]);
    }
  });
});

// ── Bundle stays pure JSON + dialect-tagged; round-trip identity holds per dialect (WS7) ──

const LC = components();

class PostSearch extends SemanticBehavior {
  Find($: In<{ author_id: number }>) {
    return LC.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title'],
      where: [whereEq($.author_id, $.author_id)],
      order: 'id ASC',
    });
  }
}

class CreatePost extends SemanticBehavior {
  Create($: In<{ author_id: number; title: string }>) {
    return LC.Insert({
      table: 'posts',
      'values.author_id': $.author_id,
      'values.title': $.title,
      returning: 'id, author_id, title',
    });
  }
}

const createWrites = entityWrites<CreatePost>((w) => ({
  create: w.lifecycle({
    idempotency: w.idempotentBy('idem', 'token', '$.input.author_id'),
    derive: [w.increment('users', { id: '$.input.author_id' }, 'post_count', +1)],
  }),
}));

/** Assert a bundle is pure JSON (round-trips losslessly) and dialect-tagged. */
function assertPureJsonBundle(bundle: SqlBundle, dialect: DialectName): void {
  expect(bundle.dialect).toBe(dialect);
  const json = JSON.stringify(bundle);
  const parsed = JSON.parse(json) as SqlBundle;
  // Lossless: re-serializing the parsed bundle is byte-identical (no functions/Date residue).
  expect(JSON.stringify(parsed)).toBe(json);
  expect(parsed.dialect).toBe(dialect);
}

describe('WS6 — bundle stays pure JSON + dialect-tagged; round-trip identity per dialect', () => {
  const readContract = publishBehaviors(PostSearch);
  const writeContract = publishBehaviors(CreatePost);

  it('read bundle is pure JSON + dialect-tagged for every dialect', () => {
    for (const name of ALL_DIALECTS) {
      const bundle = compileBundle(readContract, 'Find', [], name);
      assertPureJsonBundle(bundle, name);
    }
  });

  it('write-tx bundle is pure JSON + dialect-tagged; guard INSERT verb/clause differs per dialect', () => {
    const sqlite = compileWriteBundle(writeContract, 'Create', createWrites, 'create', 'sqlite');
    const pg = compileWriteBundle(writeContract, 'Create', createWrites, 'create', 'postgres');
    const mysql = compileWriteBundle(writeContract, 'Create', createWrites, 'create', 'mysql');
    assertPureJsonBundle(sqlite, 'sqlite');
    assertPureJsonBundle(pg, 'postgres');
    assertPureJsonBundle(mysql, 'mysql');

    // The idempotency guard INSERT is dialect-specific (SSoT-routed), byte-visible in the plan.
    const idemSql = (b: SqlBundle) => b.transaction!.statements.find((s) => s.role === 'gate:idempotency')!.op.sql;
    expect(idemSql(sqlite)).toBe('INSERT INTO idem (token) VALUES (?) ON CONFLICT DO NOTHING');
    expect(idemSql(pg)).toBe('INSERT INTO idem (token) VALUES (?) ON CONFLICT DO NOTHING');
    expect(idemSql(mysql)).toBe('INSERT IGNORE INTO idem (token) VALUES (?)');

    // Rendered PG statement converts `?`→`$N`; MySQL keeps `?`.
    const body = (b: SqlBundle) => b.transaction!.statements.find((s) => s.role === 'body')!.op;
    const scope = { author_id: 7, title: 'Hello' };
    expect(renderOperation(body(pg), scope, POSTGRES).sql).toBe(
      'INSERT INTO posts (author_id, title) VALUES ($1, $2) RETURNING id, author_id, title',
    );
    expect(renderOperation(body(mysql), scope, MYSQL).sql).toBe(
      'INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, author_id, title',
    );
  });
});

describe('WS6 dialect golden — deterministic (same IR + input → same per-dialect SQL)', () => {
  it('re-rendering per dialect is byte-identical', () => {
    const op = compileSelect({
      table: 'posts',
      select: ['id'],
      where: [{ kind: 'eq', column: 'author_id', value: inref('authorId') }],
      limit: { coalesce: [{ refOpt: ['limit'] }, 20] },
    });
    const input = { authorId: 7, limit: null };
    for (const name of ALL_DIALECTS) {
      const d = dialectFor(name);
      const a = renderOperation(op, input, d);
      const b = renderOperation(op, input, d);
      expect(a.sql).toBe(b.sql);
      expect(a.params).toEqual(b.params);
    }
  });
});
