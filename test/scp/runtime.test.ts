/**
 * WS3 (#23) thin-runtime tests — the SCP TS runtime consumes bc's runtime-core and adds
 * ONLY the SQL-backend concerns (Backend-Compile bridge, Handlers, Error Mapping). Every
 * test runs REAL better-sqlite3 (create table → insert → run the SCP-compiled SQL → read
 * rows). No mock driver.
 *
 * ## docker N/A justification (#23 AC)
 *
 * litedbmodel's SQLite backend is better-sqlite3 — an IN-PROCESS, synchronous engine with
 * no server. There is nothing to containerize (unlike the PG/MySQL integration path). This
 * suite is therefore the sanctioned substitute for a docker integration test: it exercises
 * the FULL execution pipeline against a real SQLite database, in-process. Mocking the driver
 * would defeat the purpose and is a failed audit; these tests bind and execute real SQL.
 *
 * Covers: Select (eq, multi-AND, IN-list N + empty, SKIP present/null/absent-via-refOpt,
 * ORDER BY + LIMIT), Insert, Update, Delete, relation `.map`, and error mapping (UNIQUE
 * violation → constraint_violation SqlFailure).
 */

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  executeBehavior,
  mapSqliteError,
  SqlFailure,
  whereEq,
  whereGe,
  whereIn,
  inColumn,
  when,
  ne,
  opt,
  coalesce,
  add,
  type In,
  type Recorded,
} from '../../src/scp';

const L = components();

/** Fresh in-memory DB with the α schema + seed rows for each test. */
function freshDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  db.exec(`
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE posts (
      id INTEGER PRIMARY KEY,
      author_id INTEGER NOT NULL REFERENCES users(id),
      title TEXT NOT NULL,
      status TEXT,
      created_at TEXT NOT NULL
    );
    CREATE UNIQUE INDEX posts_title_unique ON posts(title);
    CREATE TABLE idem (token TEXT PRIMARY KEY);
    CREATE TABLE counters (id INTEGER PRIMARY KEY, n INTEGER NOT NULL);
  `);
  db.prepare('INSERT INTO counters VALUES (?, ?)').run(1, 5);
  db.prepare('INSERT INTO users VALUES (?, ?)').run(7, 'Ada');
  db.prepare('INSERT INTO users VALUES (?, ?)').run(8, 'Alan');
  db.prepare('INSERT INTO posts VALUES (?,?,?,?,?)').run(1, 7, 'Hello', 'live', '2026-02-01');
  db.prepare('INSERT INTO posts VALUES (?,?,?,?,?)').run(2, 7, 'World', 'draft', '2026-03-01');
  db.prepare('INSERT INTO posts VALUES (?,?,?,?,?)').run(3, 8, 'Other', 'live', '2026-01-15');
  return db;
}

// ── Behaviors (authoring surface) ─────────────────────────────────────────────

class PostQueries extends SemanticBehavior {
  static columns = {
    posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT', status: 'TEXT', created_at: 'TEXT' },
  };
  // eq + SKIP-optional (absent-via-refOpt) + range + ORDER BY + LIMIT (coalesce default).
  Search($: In<{ author_id: number; status?: string; since: string; limit?: number }>) {
    return L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title', 'status'],
      where: [
        whereEq($.author_id, $.author_id),
        when(ne(opt($.status), null), () => whereEq($.status, $.status)),
        whereGe($.created_at, $.since),
      ],
      order: 'id ASC',
      limit: coalesce(opt($.limit), 20),
    });
  }

  // IN-list expansion.
  ByIds($: In<{ ids: number[] }>) {
    return L.Select({
      table: 'posts',
      select: ['id', 'title'],
      where: [whereIn(inColumn($, 'id'), $.ids)],
      order: 'id ASC',
    });
  }
}

class PostCommands extends SemanticBehavior {
  Create($: In<{ author_id: number; title: string; created_at: string }>) {
    return L.Insert({
      table: 'posts',
      'values.author_id': $.author_id,
      'values.title': $.title,
      'values.created_at': $.created_at,
      returning: 'id, author_id, title',
    });
  }

  Rename($: In<{ id: number; title: string }>) {
    return L.Update({
      table: 'posts',
      'set.title': $.title,
      where: [whereEq($.id, $.id)],
      returning: 'id, title',
    });
  }

  Remove($: In<{ id: number }>) {
    return L.Delete({ table: 'posts', where: [whereEq($.id, $.id)], returning: 'id' });
  }

  // Bumps a counter using a SET expression (add over the current value) — closed-set
  // Expression IR computed server-side (the SET slot binds `n + bump`).
  Bump($: In<{ id: number; n: number; bump: number }>) {
    return L.Update({
      table: 'counters',
      'set.n': add($.n, $.bump),
      where: [whereEq($.id, $.id)],
      returning: 'id, n',
    });
  }
}

// ── Select ─────────────────────────────────────────────────────────────────────

describe('WS3 runtime — Select (real SQLite)', () => {
  let db: InstanceType<typeof Database>;
  const q = publishBehaviors(PostQueries);
  beforeEach(() => {
    db = freshDb();
  });

  it('eq + SKIP present → status filter applied', () => {
    const out = executeBehavior(q, { author_id: 7, status: 'live', since: '2026-01-01' }, { db, entry: 'Search' });
    expect(out).toEqual([{ id: 1, author_id: 7, title: 'Hello', status: 'live' }]);
  });

  it('SKIP null → fragment dropped (only author + since apply)', () => {
    const out = executeBehavior(q, { author_id: 7, status: null, since: '2026-01-01' }, { db, entry: 'Search' });
    expect(out).toEqual([
      { id: 1, author_id: 7, title: 'Hello', status: 'live' },
      { id: 2, author_id: 7, title: 'World', status: 'draft' },
    ]);
  });

  it('SKIP absent-via-refOpt → fragment dropped end-to-end (undefined optional input)', () => {
    // `status` is entirely OMITTED from the input. The SKIP guard's refOpt sees a
    // present-as-null normalization (schema/structure-driven), evaluates false, and DROPS
    // the fragment — proving absent-key SKIP, which WS2 never exercised.
    const out = executeBehavior(q, { author_id: 7, since: '2026-01-01' }, { db, entry: 'Search' });
    expect(out).toEqual([
      { id: 1, author_id: 7, title: 'Hello', status: 'live' },
      { id: 2, author_id: 7, title: 'World', status: 'draft' },
    ]);
  });

  it('multi-AND + range narrows the set', () => {
    const out = executeBehavior(q, { author_id: 7, status: null, since: '2026-02-15' }, { db, entry: 'Search' });
    expect(out).toEqual([{ id: 2, author_id: 7, title: 'World', status: 'draft' }]);
  });

  it('LIMIT via coalesce default (limit omitted → 20) returns all matches', () => {
    const out = executeBehavior(q, { author_id: 7, status: null, since: '2026-01-01' }, { db, entry: 'Search' }) as unknown[];
    expect(out).toHaveLength(2);
  });

  it('LIMIT explicit caps the row count', () => {
    const out = executeBehavior(q, { author_id: 7, status: null, since: '2026-01-01', limit: 1 }, { db, entry: 'Search' }) as unknown[];
    expect(out).toEqual([{ id: 1, author_id: 7, title: 'Hello', status: 'live' }]);
  });

  it('IN-list (N elements) expands and matches', () => {
    const out = executeBehavior(q, { ids: [1, 3] }, { db, entry: 'ByIds' });
    expect(out).toEqual([{ id: 1, title: 'Hello' }, { id: 3, title: 'Other' }]);
  });

  it('IN-list (empty) degenerates to 1 = 0 → no rows', () => {
    const out = executeBehavior(q, { ids: [] }, { db, entry: 'ByIds' });
    expect(out).toEqual([]);
  });
});

// ── Insert / Update / Delete ────────────────────────────────────────────────────

describe('WS3 runtime — Insert / Update / Delete (real SQLite)', () => {
  let db: InstanceType<typeof Database>;
  const cmd = publishBehaviors(PostCommands);
  beforeEach(() => {
    db = freshDb();
  });

  it('Insert RETURNING yields the new row and persists it', () => {
    const out = executeBehavior(
      cmd,
      { author_id: 8, title: 'Fresh', created_at: '2026-04-01' },
      { db, entry: 'Create' },
    );
    expect(out).toEqual([{ id: 4, author_id: 8, title: 'Fresh' }]);
    const row = db.prepare('SELECT title FROM posts WHERE id = 4').get();
    expect(row).toEqual({ title: 'Fresh' });
  });

  it('Update RETURNING mutates the row', () => {
    const out = executeBehavior(cmd, { id: 1, title: 'Renamed' }, { db, entry: 'Rename' });
    expect(out).toEqual([{ id: 1, title: 'Renamed' }]);
    expect(db.prepare('SELECT title FROM posts WHERE id = 1').get()).toEqual({ title: 'Renamed' });
  });

  it('Update with a closed-set SET expression (add) computes server-side', () => {
    // counters(1).n = 5; bump by 3 → n becomes 5 + 3 = 8 (the add slot binds `n + bump`).
    const out = executeBehavior(cmd, { id: 1, n: 5, bump: 3 }, { db, entry: 'Bump' });
    expect(out).toEqual([{ id: 1, n: 8 }]);
    expect(db.prepare('SELECT n FROM counters WHERE id = 1').get()).toEqual({ n: 8 });
  });

  it('Delete RETURNING removes the row', () => {
    const out = executeBehavior(cmd, { id: 2 }, { db, entry: 'Remove' });
    expect(out).toEqual([{ id: 2 }]);
    expect(db.prepare('SELECT COUNT(*) c FROM posts WHERE id = 2').get()).toEqual({ c: 0 });
  });
});

// ── Relation .map ────────────────────────────────────────────────────────────────

describe('WS3 runtime — relation .map (bc drives iteration; runtime executes SQL)', () => {
  class WithAuthor extends SemanticBehavior {
    static columns = {
      posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT' },
      users: { id: 'INTEGER', name: 'TEXT' },
    };
    Feed($: In<{ author_id: number }>) {
      const posts = L.Select({
        table: 'posts',
        select: ['id', 'author_id', 'title'],
        where: [whereEq($.author_id, $.author_id)],
        order: 'id ASC',
      });
      const authors = posts.map(($p: Recorded) =>
        L.Select({ table: 'users', select: ['id', 'name'], where: [whereEq($p.id, $p.author_id)] }),
      );
      return { posts, authors };
    }
  }

  it('runs the per-parent author query and Φ-merges the output', () => {
    const db = freshDb();
    const out = executeBehavior(publishBehaviors(WithAuthor), { author_id: 7 }, { db, entry: 'Feed' }) as {
      posts: unknown[];
      authors: unknown[];
    };
    expect(out.posts).toEqual([
      { id: 1, author_id: 7, title: 'Hello' },
      { id: 2, author_id: 7, title: 'World' },
    ]);
    // Each post resolves its author (both by author 7 = Ada).
    expect(out.authors).toEqual([[{ id: 7, name: 'Ada' }], [{ id: 7, name: 'Ada' }]]);
  });
});

// ── Error Mapping ────────────────────────────────────────────────────────────────

describe('WS3 runtime — Error Mapping (driver error → SCP Failure)', () => {
  const cmd = publishBehaviors(PostCommands);

  it('UNIQUE violation surfaces as a constraint_violation SqlFailure (Policy fail)', () => {
    const db = freshDb();
    // 'Hello' already exists (unique title index) → INSERT collides.
    let caught: unknown;
    try {
      executeBehavior(cmd, { author_id: 8, title: 'Hello', created_at: '2026-04-02' }, { db, entry: 'Create' });
    } catch (e) {
      caught = e;
    }
    expect(caught).toBeInstanceOf(SqlFailure);
    const f = caught as SqlFailure;
    expect(f.kind).toBe('constraint_violation');
    expect(f.policy).toBe('fail');
    expect(f.sqliteCode).toBe('SQLITE_CONSTRAINT_UNIQUE');
  });

  it('mapSqliteError classifies the SQLITE_* families', () => {
    expect(mapSqliteError({ code: 'SQLITE_CONSTRAINT_UNIQUE', message: 'x' }).kind).toBe('constraint_violation');
    expect(mapSqliteError({ code: 'SQLITE_CONSTRAINT_FOREIGNKEY', message: 'x' }).kind).toBe('foreign_key_violation');
    expect(mapSqliteError({ code: 'SQLITE_CONSTRAINT_FOREIGNKEY', message: 'x' }).policy).toBe('fail');
    expect(mapSqliteError({ code: 'SQLITE_BUSY', message: 'x' }).kind).toBe('retryable');
    expect(mapSqliteError({ code: 'SQLITE_BUSY', message: 'x' }).policy).toBe('retry');
    expect(mapSqliteError({ code: 'SQLITE_IOERR', message: 'x' }).kind).toBe('driver_error');
    expect(mapSqliteError(new Error('boom')).kind).toBe('driver_error');
  });

  it('FK violation surfaces as foreign_key_violation end-to-end', () => {
    const db = freshDb();
    // author_id 999 has no users row → FK fails (foreign_keys pragma is ON).
    let caught: unknown;
    try {
      executeBehavior(cmd, { author_id: 999, title: 'Orphan', created_at: '2026-04-03' }, { db, entry: 'Create' });
    } catch (e) {
      caught = e;
    }
    expect(caught).toBeInstanceOf(SqlFailure);
    expect((caught as SqlFailure).kind).toBe('foreign_key_violation');
  });
});

// ── Backend-Compile bridge (real IR → CompiledOperation) ──────────────────────────
