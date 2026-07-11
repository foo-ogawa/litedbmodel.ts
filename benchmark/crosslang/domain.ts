// ════════════════════════════════════════════════════════════════════════════
// Shared benchmark domain (epic #44) — schema, seed data, authored behaviors,
// fixed inputs, and the hand-optimized raw-SQL baseline.
// ════════════════════════════════════════════════════════════════════════════
//
// EVERY adapter (TS sql/codegen/ir/dynamic/prepared, and Python/Rust/PHP/Go
// sql/codegen/ir) runs the SAME access patterns against the SAME dataset, so
// only the runtime-layer overhead differs. The behaviors are authored ONCE here
// with the litedbmodel public authoring API and compiled to SqlBundles; those
// bundles are what the ir/codegen cells (all languages) consume. The raw-SQL
// baseline strings are the hand-written N+1-avoided, projection-tight SQL.

import Database from 'better-sqlite3';
import * as lm from '../../dist/scp/index.mjs';
import { CROSSLANG_CASE_IDS } from './contract.js';

// Re-export the canonical case-id order for the generator/adapters.
export const CROSSLANG_CASE_IDS_LOCAL: readonly string[] = CROSSLANG_CASE_IDS;

const L = lm.components();

// ── Schema (shared by every cell) ────────────────────────────────────────────
export const SCHEMA: readonly string[] = [
  `CREATE TABLE users (
     id INTEGER PRIMARY KEY,
     name TEXT NOT NULL,
     post_count INTEGER NOT NULL DEFAULT 0
   );`,
  `CREATE TABLE posts (
     id INTEGER PRIMARY KEY,
     author_id INTEGER NOT NULL,
     title TEXT NOT NULL,
     status TEXT,
     views INTEGER NOT NULL DEFAULT 0,
     created_at TEXT NOT NULL
   );`,
  `CREATE TABLE comments (
     id INTEGER PRIMARY KEY,
     post_id INTEGER NOT NULL,
     body TEXT NOT NULL,
     created_at TEXT NOT NULL
   );`,
  // Gate guard tables for the write-tx case (spec §6 shape).
  `CREATE TABLE uniq (name TEXT NOT NULL, s0 TEXT, f0 TEXT);`,
];

// Deterministic seed: 8 users, 40 posts (5 per user), 200 comments (5 per post).
export function seed(db: InstanceType<typeof Database>): void {
  const insUser = db.prepare('INSERT INTO users (id, name, post_count) VALUES (?,?,?)');
  const insPost = db.prepare(
    'INSERT INTO posts (id, author_id, title, status, views, created_at) VALUES (?,?,?,?,?,?)',
  );
  const insComment = db.prepare(
    'INSERT INTO comments (id, post_id, body, created_at) VALUES (?,?,?,?)',
  );
  const tx = db.transaction(() => {
    for (let u = 1; u <= 8; u++) insUser.run(u, `user-${u}`, 5);
    let pid = 0;
    let cid = 0;
    for (let u = 1; u <= 8; u++) {
      for (let k = 0; k < 5; k++) {
        pid++;
        const status = k % 3 === 0 ? 'live' : k % 3 === 1 ? 'draft' : 'live';
        const day = String(k + 1).padStart(2, '0');
        insPost.run(pid, u, `post-${pid}`, status, pid * 10, `2026-02-${day}`);
        for (let c = 0; c < 5; c++) {
          cid++;
          insComment.run(cid, pid, `comment-${cid}`, `2026-03-01`);
        }
      }
    }
  });
  tx();
}

export function freshDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  for (const s of SCHEMA) db.exec(s);
  seed(db);
  return db;
}

// ── Fixed inputs (identical logical work for every cell) ─────────────────────
export const INPUTS = {
  find: { author_id: 1, status: 'live', since: '2026-02-01' },
  complexWhere: { author_id: 1, since: '2026-02-01', titleLike: 'post-%', ids: [1, 2, 3, 4, 5] },
  inList: { ids: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] },
  belongsTo: { author_id: 1 },
  hasMany: { author_id: 1 },
  hasManyLimit: { author_id: 1 },
  batchInsert: {
    rows: Array.from({ length: 10 }, (_, i) => ({
      author_id: 2,
      title: `bulk-${i}`,
      status: 'live',
      views: 0,
      created_at: '2026-05-01',
    })),
  },
  writeTxGate: { author_id: 1, title: 'txn-post', created_at: '2026-05-01' },
} as const;

// ── Authored read behaviors (compiled to SqlBundles the ir/codegen cells consume) ──
export class Reads extends lm.SemanticBehavior {
  // find: eq + SKIP-optional status + range, ORDER BY.
  Find($: any) {
    return L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title', 'status', 'views', 'created_at'],
      where: [
        lm.whereEq($.author_id, $.author_id),
        lm.when(lm.ne(lm.opt($.status), null), () => lm.whereEq($.status, $.status)),
        lm.whereGe($.created_at, $.since),
      ],
      order: 'id ASC',
    });
  }

  // complexWhere: eq + range + LIKE + IN — multiple predicate kinds in one WHERE.
  ComplexWhere($: any) {
    return L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title', 'status', 'views'],
      where: [
        lm.whereEq($.author_id, $.author_id),
        lm.whereGe($.created_at, $.since),
        lm.whereLike($, 'title', $.titleLike),
        lm.whereIn(lm.inColumn($, 'id'), $.ids),
      ],
      order: 'id ASC',
    });
  }

  // inList: IN-list, single-JSON param.
  ByIds($: any) {
    return L.Select({
      table: 'posts',
      select: ['id', 'title'],
      where: [lm.whereIn(lm.inColumn($, 'id'), $.ids)],
      order: 'id ASC',
    });
  }

  // The relation cases (belongsTo / hasMany / hasMany-limit) share ONE parent Select
  // (posts by author). The child load is a DECLARED relation batch op (RelationDecl),
  // resolved by the runtime as ONE batched IN query per relation (N+1 avoided) — the
  // fair, hand-baseline-matching relation surface (NOT the per-parent `.map`).
  Posts($: any) {
    return L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title'],
      where: [lm.whereEq($.author_id, $.author_id)],
      order: 'id ASC',
    });
  }
}

// Declarative, N+1-avoided relation ops (one batched IN query each). The `with`
// name selects which relation the read prefetches for that case.
export const REL_BELONGS_TO: any = {
  name: 'author', kind: 'belongsTo', targetTable: 'users', select: ['id', 'name'],
  parentKey: 'author_id', targetKey: 'id', dialect: 'sqlite',
};
export const REL_HAS_MANY: any = {
  name: 'comments', kind: 'hasMany', targetTable: 'comments', select: ['id', 'post_id', 'body'],
  parentKey: 'id', targetKey: 'post_id', dialect: 'sqlite',
};
export const REL_HAS_MANY_LIMIT: any = {
  name: 'recent', kind: 'hasMany', targetTable: 'comments', select: ['id', 'post_id', 'body'],
  parentKey: 'id', targetKey: 'post_id', order: 'id DESC', limit: 3, dialect: 'sqlite',
};

// ── Authored write behavior (single base write; gate contract for the tx case) ──
export class Writes extends lm.SemanticBehavior {
  Create($: any) {
    return L.Insert({
      table: 'posts',
      'values.author_id': $.author_id,
      'values.title': $.title,
      'values.created_at': $.created_at,
      returning: 'id, author_id, title',
    });
  }
}

// The `create` gate contract: referential integrity (author exists) + uniqueness
// (title per author), then the cascade counter — one gate-first tx (spec §6).
export const writeGateContract = lm.entityWrites<Writes>((w: any) => ({
  create: w.lifecycle({
    requires: [w.exists('users', { id: '$.input.author_id' })],
    unique: [
      w.unique({ name: 'title_per_author', guardTable: 'uniq', scope: ['$.input.author_id'], fields: ['$.input.title'] }),
    ],
    derive: [w.increment('users', { id: '$.input.author_id' }, 'post_count', +1)],
  }),
}));

export const readsContract = lm.publishBehaviors(Reads);
export const writesContract = lm.publishBehaviors(Writes);

// ── Hand-optimized raw-SQL baseline (the 1.0× denominator; NOT a strawman) ──────
// Each entry is the tight, N+1-avoided, projection-only SQL a careful engineer
// would hand-write. Relations use ONE batched IN query for the children (no N+1).
// `run(db)` executes it exactly as the ir/codegen path would and returns the same
// logical result; `queries`/`rows` are the fairness counters.
export interface SqlBaselineCase {
  queries: number;
  rows: number;
  run(db: InstanceType<typeof Database>): void;
}

export const SQL_BASELINE: Record<string, SqlBaselineCase> = {
  find: {
    queries: 1,
    rows: 3,
    run(db) {
      db.prepare(
        'SELECT id, author_id, title, status, views, created_at FROM posts WHERE author_id = ? AND status = ? AND created_at >= ? ORDER BY id ASC',
      ).all(INPUTS.find.author_id, INPUTS.find.status, INPUTS.find.since);
    },
  },
  complexWhere: {
    queries: 1,
    rows: 5,
    run(db) {
      db.prepare(
        'SELECT id, author_id, title, status, views FROM posts WHERE author_id = ? AND created_at >= ? AND title LIKE ? AND id IN (?, ?, ?, ?, ?) ORDER BY id ASC',
      ).all(INPUTS.complexWhere.author_id, INPUTS.complexWhere.since, INPUTS.complexWhere.titleLike, ...INPUTS.complexWhere.ids);
    },
  },
  inList: {
    queries: 1,
    rows: 10,
    run(db) {
      const ids = INPUTS.inList.ids;
      db.prepare(`SELECT id, title FROM posts WHERE id IN (${ids.map(() => '?').join(', ')}) ORDER BY id ASC`).all(...ids);
    },
  },
  belongsTo: {
    queries: 2,
    rows: 6, // 5 posts + 1 distinct author (batched, N+1 avoided)
    run(db) {
      const posts = db
        .prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC')
        .all(INPUTS.belongsTo.author_id) as { author_id: number }[];
      const authorIds = [...new Set(posts.map((p) => p.author_id))];
      db.prepare(`SELECT id, name FROM users WHERE id IN (${authorIds.map(() => '?').join(', ')})`).all(...authorIds);
    },
  },
  hasMany: {
    queries: 2,
    rows: 30, // 5 posts + 25 comments (one batched IN query, N+1 avoided)
    run(db) {
      const posts = db
        .prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC')
        .all(INPUTS.hasMany.author_id) as { id: number }[];
      const ids = posts.map((p) => p.id);
      db.prepare(`SELECT id, post_id, body FROM comments WHERE post_id IN (${ids.map(() => '?').join(', ')})`).all(...ids);
    },
  },
  hasManyLimit: {
    queries: 2,
    rows: 20, // 5 posts + 3 comments/parent (per-parent LIMIT 3) = 15 children
    run(db) {
      const posts = db
        .prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC')
        .all(INPUTS.hasManyLimit.author_id) as { id: number }[];
      const ids = posts.map((p) => p.id);
      // Per-parent LIMIT 3 via ROW_NUMBER (the hand-optimized batched form; N+1 avoided).
      db.prepare(
        `SELECT id, post_id, body FROM (
           SELECT id, post_id, body, ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY id DESC) rn
           FROM comments WHERE post_id IN (${ids.map(() => '?').join(', ')})
         ) WHERE rn <= 3`,
      ).all(...ids);
    },
  },
  batchInsert: {
    queries: 1,
    rows: 0,
    run(db) {
      const rows = INPUTS.batchInsert.rows;
      const cols = ['author_id', 'title', 'status', 'views', 'created_at'];
      const placeholders = rows.map(() => `(${cols.map(() => '?').join(',')})`).join(',');
      const flat: unknown[] = [];
      for (const r of rows) flat.push(r.author_id, r.title, r.status, r.views, r.created_at);
      db.prepare(`INSERT INTO posts (${cols.join(',')}) VALUES ${placeholders}`).run(...flat);
    },
  },
  writeTxGate: {
    queries: 4, // gate:requires + gate:unique + body INSERT(RETURNING) + derive UPDATE (one tx)
    rows: 2, // requires SELECT (1) + INSERT RETURNING (1)
    run(db) {
      const inp = INPUTS.writeTxGate;
      const tx = db.transaction(() => {
        const author = db.prepare('SELECT 1 FROM users WHERE id = ?').get(inp.author_id);
        if (!author) throw new Error('requires_absent');
        db.prepare('INSERT INTO uniq (name, s0, f0) VALUES (?, ?, ?) ON CONFLICT DO NOTHING').run(
          'title_per_author',
          String(inp.author_id),
          inp.title,
        );
        // Body write returns the new row (RETURNING parity with the lm write path).
        db.prepare('INSERT INTO posts (author_id, title, created_at) VALUES (?, ?, ?) RETURNING id, author_id, title').get(
          inp.author_id,
          inp.title,
          inp.created_at,
        );
        db.prepare('UPDATE users SET post_count = post_count + ? WHERE id = ?').run(1, inp.author_id);
      });
      tx();
    },
  },
};

// The entry component name + relations for each read case, so every cell resolves
// the SAME bundle. Write cases route through the write/batch bundle helpers.
export const READ_ENTRY: Record<string, string> = {
  find: 'Find',
  complexWhere: 'ComplexWhere',
  inList: 'ByIds',
  belongsTo: 'Posts',
  hasMany: 'Posts',
  hasManyLimit: 'Posts',
};

// For the relation cases: which declared relation the read prefetches (`with`).
export const READ_RELATION: Record<string, { decl: any; withName: string } | undefined> = {
  belongsTo: { decl: REL_BELONGS_TO, withName: 'author' },
  hasMany: { decl: REL_HAS_MANY, withName: 'comments' },
  hasManyLimit: { decl: REL_HAS_MANY_LIMIT, withName: 'recent' },
};

// ── Real-DB (PG / MySQL) schema + seed for the DB-backed axis (#44 gap #2) ─────
// Every language's DB-backed cell creates its OWN bench tables (drop-then-create) in
// an ISOLATED namespace, seeds the SAME 8-user/40-post/200-comment dataset the SQLite
// cells use, and runs the 8 cases against the REAL dockerized DB. The table names are
// the SAME (`users`/`posts`/`comments`/`uniq`) as the compiled bundles reference, so
// the SAME bundle SQL executes unchanged — only the connection + dialect differ.
// `posts.id` must AUTO-GENERATE: the batchInsert / writeTxGate cases INSERT posts
// with no id (matching SQLite's implicit rowid). PG uses SERIAL, MySQL AUTO_INCREMENT.
// After seeding explicit ids the PG sequence is bumped past the seeded max (see
// `pgSeqResetStatements`) so the write cases' new rows don't collide.
export const PG_SCHEMA: readonly string[] = [
  `DROP TABLE IF EXISTS comments CASCADE`,
  `DROP TABLE IF EXISTS posts CASCADE`,
  `DROP TABLE IF EXISTS users CASCADE`,
  `DROP TABLE IF EXISTS uniq CASCADE`,
  `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, post_count INTEGER NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id SERIAL PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, views INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL)`,
  `CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL)`,
  `CREATE TABLE uniq (name TEXT NOT NULL, s0 TEXT, f0 TEXT)`,
];
export const MYSQL_SCHEMA: readonly string[] = [
  `SET FOREIGN_KEY_CHECKS = 0`,
  `DROP TABLE IF EXISTS comments`,
  `DROP TABLE IF EXISTS posts`,
  `DROP TABLE IF EXISTS users`,
  `DROP TABLE IF EXISTS uniq`,
  `SET FOREIGN_KEY_CHECKS = 1`,
  `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, post_count INT NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, status VARCHAR(255), views INT NOT NULL DEFAULT 0, created_at VARCHAR(255) NOT NULL)`,
  `CREATE TABLE comments (id INT PRIMARY KEY, post_id INT NOT NULL, body VARCHAR(255) NOT NULL, created_at VARCHAR(255) NOT NULL)`,
  `CREATE TABLE uniq (name VARCHAR(255) NOT NULL, s0 VARCHAR(255), f0 VARCHAR(255))`,
];

// After seeding 40 posts with explicit ids 1..40, advance the PG SERIAL sequence so a
// subsequent no-id INSERT gets id 41+ (MySQL AUTO_INCREMENT self-advances past the max).
export const PG_SEQ_RESET: readonly string[] = [`SELECT setval('posts_id_seq', (SELECT MAX(id) FROM posts))`];

// The seed as language-neutral INSERT statements (the SAME dataset as `seed()` above),
// portable across PG/MySQL. Deterministic: 8 users, 40 posts, 200 comments.
export function seedStatementsShared(): string[] {
  const stmts: string[] = [];
  for (let u = 1; u <= 8; u++) stmts.push(`INSERT INTO users (id, name, post_count) VALUES (${u}, 'user-${u}', 5)`);
  let pid = 0;
  let cid = 0;
  for (let u = 1; u <= 8; u++) {
    for (let k = 0; k < 5; k++) {
      pid++;
      const status = k % 3 === 0 ? 'live' : k % 3 === 1 ? 'draft' : 'live';
      const day = String(k + 1).padStart(2, '0');
      stmts.push(`INSERT INTO posts (id, author_id, title, status, views, created_at) VALUES (${pid}, ${u}, 'post-${pid}', '${status}', ${pid * 10}, '2026-02-${day}')`);
      for (let c = 0; c < 5; c++) {
        cid++;
        stmts.push(`INSERT INTO comments (id, post_id, body, created_at) VALUES (${cid}, ${pid}, 'comment-${cid}', '2026-03-01')`);
      }
    }
  }
  return stmts;
}

// Env-driven connection config (matches docker-compose.test.yml + WS6 host defaults:
// PG 5433, MySQL 3307 when the livedb override republishes the ports to the host).
export const PG_CONN = {
  host: process.env.TEST_DB_HOST || 'localhost',
  port: parseInt(process.env.TEST_DB_PORT || '5433', 10),
  database: process.env.TEST_DB_NAME || 'testdb',
  user: process.env.TEST_DB_USER || 'testuser',
  password: process.env.TEST_DB_PASSWORD || 'testpass',
};
export const MYSQL_CONN = {
  host: process.env.TEST_MYSQL_HOST || 'localhost',
  port: parseInt(process.env.TEST_MYSQL_PORT || '3307', 10),
  database: process.env.TEST_MYSQL_DB || 'testdb',
  user: process.env.TEST_MYSQL_USER || 'testuser',
  password: process.env.TEST_MYSQL_PASSWORD || 'testpass',
};
