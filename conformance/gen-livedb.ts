/**
 * Generate the LIVE-DB conformance corpus (#36 WS7g + #43/#45 makeSQL flip, epic #44 Parts B/C).
 *
 * The frozen `conformance/vectors/{exec,tx}.json` corpus executes the SQLite-tagged STATIC makeSQL
 * bundles (the §8 artifact — a `readGraph` of surrogate IR + per-node `statementsById`, plus a
 * `transaction` plan for writes) against an in-process better-sqlite3 (the §10 in-proc seam). This
 * live-DB corpus runs the SAME behaviors' bundles compiled for the `postgres` + `mysql` dialects
 * against REAL dockerized Postgres + MySQL, in EVERY language runtime, and asserts the assembled
 * result equals the SAME reference — i.e. the SQLite-captured `expectedResult` / `expectedDbState`
 * (the §10 promise: same IR + input → same RESULT regardless of dialect).
 *
 * ## v2 shape (the makeSQL flip — corpusVersion 2)
 *
 * v1 (#36) captured the OLD dynamic `compileBundle` output: `operations[].sql` carrying unrendered
 * `{where}` tokens + a separate `where` fragment map. The makeSQL flip retired that: an exec bundle
 * is now `{ dialect, name, readGraph, optionalHeads, relations }` (reads) and a tx bundle is
 * `{ dialect, statement/transaction, … }` (writes), IDENTICAL in shape to `conformance/vectors/*`.
 * The language `livedb_runner`s consume `bundlePg`/`bundleMysql` through the SAME `execute_bundle` /
 * `execute_transaction_bundle` the SQLite conformance uses — so migrating the corpus to this shape
 * is the whole fix: no runner logic change beyond the corpusVersion bump.
 *
 * Each exec/tx vector captures:
 *   - `bundlePg`  / `bundleMysql` — `compileBundle`/`compileWriteBundle` for the pg / mysql dialect
 *     (the STATIC readGraph/statements therefore carry the dialect-specific text — `$N` at render,
 *     `= ANY(?)` no-cast authored IN-lists, `?::@@PG_ARRAY_CAST@@` relation batches).
 *   - `schemaPg`  / `schemaMysql` — PG / MySQL DDL + seed matching the SQLite READ/WRITE schema.
 *   - `expectedResult` / `expectedDbState` — the reference outcome, captured by EXECUTING the
 *     SQLite bundle on in-memory better-sqlite3 (byte-true to `harness.ts`), NOT hand-authored.
 *
 * ## IN-list coverage (the #46 fix — exercised in EVERY language on live PG)
 *
 * The corpus adds `ByIds` (int IN-list, non-empty + EMPTY) and `ByUuids` (uuid IN-list) exec
 * vectors so every language's PG driver binds the no-cast `= ANY($1)` single-array param on a live
 * PG — int, empty (zero rows, NO `integer = text`), and uuid (PG infers `uuid[]` from the column).
 * The Feed relation batch keeps the v1 `= ANY($1::int[])` byte form, so both #46 paths are covered.
 *
 * ## BYTE-TRUE CROSS-CHECK (hard rule)
 *
 * The captured `expectedResult`/`expectedDbState` are asserted IDENTICAL to the already-frozen
 * `conformance/vectors/{exec,tx}.json` reference outputs (for the shared vectors), so this live-DB
 * corpus is provably the same reference the SQLite conformance already locks — golden-from-originals
 * (v1 `DBConditions`/`LazyRelation`/`inferPgArrayType`), consistent with #43. A language runtime
 * that reproduces it on live PG/MySQL is genuinely conformant, not fudged.
 *
 * Run (via vitest's ESM resolver, like gen-vectors): `npx vitest run conformance/gen-livedb.test.ts`.
 */

import Database from 'better-sqlite3';
import { writeFileSync, mkdirSync, existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import {
  compileBundle,
  compileWriteBundle,
  compileCompositeWriteBundle,
  compileCreateManyBundle,
  compileUpdateManyBundle,
  compileDeleteManyBundle,
  executeBundle,
  executeTransactionBundle,
  readBundle,
  publishBehaviors,
  components,
  SemanticBehavior,
  entityWrites,
  whereEq,
  whereGe,
  whereIn,
  inColumn,
  when,
  ne,
  opt,
  coalesce,
  type In,
  type Recorded,
  type SqlBundle,
  type DialectName,
  type RelationDecl,
} from '../src/scp/index';
import { encodeValue, type EncodedValue } from './harness';

const HERE = dirname(fileURLToPath(import.meta.url));
export const LIVEDB_DIR = join(HERE, 'vectors-livedb');
const SQLITE_VECTORS_DIR = join(HERE, 'vectors');

export const LIVEDB_CORPUS_VERSION = 2 as const;

const L = components();

// ── Fixtures — MIRROR of harness.ts's exec/tx fixtures (kept identical by the cross-check) ──

class Blog extends SemanticBehavior {
  Feed($: In<{ author_id: number; status?: string; since: string; created_at: string; limit?: number }>) {
    const posts = L.Select({
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
    const authors = posts.map(($p: Recorded) =>
      L.Select({ table: 'users', select: ['id', 'name'], where: [whereEq($p.id, $p.author_id)] }),
    );
    return { posts, authors };
  }

  /** #46: an IN-list on an INT column → authored `id = ANY($1)` (no cast) on PG. */
  ByIds($: In<{ ids: number[] }>) {
    return L.Select({ table: 'posts', select: ['id', 'title'], where: [whereIn(inColumn($, 'id'), $.ids)], order: 'id ASC' });
  }

  /** #46: an IN-list on a UUID column → authored `doc_id = ANY($1)` (no cast); PG infers uuid[]. */
  ByUuids($: In<{ ids: string[] }>) {
    return L.Select({ table: 'docs', select: ['doc_id', 'title'], where: [whereIn(inColumn($, 'doc_id'), $.ids)], order: 'doc_id ASC' });
  }

  // #46 all PG element types: an IN-list on a BIGINT / TEXT / BOOL / TIMESTAMP / NUMERIC column →
  // authored `col = ANY($1)` (no cast); PG infers the array element type from the column context.
  // Each selects the STABLE text `label` (not the typed key) so the assembled result is
  // dialect-invariant — the vector proves the ARRAY BINDING of that element type in every driver.
  /** #46: IN-list on a BIGINT column → `big = ANY($1)`; PG infers bigint[]. */
  ByBig($: In<{ keys: number[] }>) {
    return L.Select({ table: 'typed', select: ['label'], where: [whereIn(inColumn($, 'big'), $.keys)], order: 'label ASC' });
  }
  /** #46: IN-list on a TEXT column → `txt = ANY($1)`; PG infers text[]. */
  ByTxt($: In<{ keys: string[] }>) {
    return L.Select({ table: 'typed', select: ['label'], where: [whereIn(inColumn($, 'txt'), $.keys)], order: 'label ASC' });
  }
  /** #46: IN-list on a BOOL column → `flag = ANY($1)`; PG infers boolean[]. */
  ByFlag($: In<{ keys: boolean[] }>) {
    return L.Select({ table: 'typed', select: ['label'], where: [whereIn(inColumn($, 'flag'), $.keys)], order: 'label ASC' });
  }
  /** #46: IN-list on a TIMESTAMP column → `ts = ANY($1)`; PG infers timestamp[]. */
  ByTs($: In<{ keys: string[] }>) {
    return L.Select({ table: 'typed', select: ['label'], where: [whereIn(inColumn($, 'ts'), $.keys)], order: 'label ASC' });
  }
  /** #46: IN-list on a NUMERIC column → `amt = ANY($1)`; PG infers numeric[]. */
  ByAmt($: In<{ keys: number[] }>) {
    return L.Select({ table: 'typed', select: ['label'], where: [whereIn(inColumn($, 'amt'), $.keys)], order: 'label ASC' });
  }

  // count() (#47 item 2 — v1 `DBModel._count`): `SELECT COUNT(*) as count FROM posts[ WHERE …]`.
  /** count() over ALL rows: `SELECT COUNT(*) as count FROM posts`. */
  CountAll(_$: In<Record<string, never>>) {
    return L.Count({ table: 'posts' });
  }
  /** count() WITH a WHERE filter: `SELECT COUNT(*) as count FROM posts WHERE author_id = ?`. */
  CountByAuthor($: In<{ author_id: number }>) {
    return L.Count({ table: 'posts', where: [whereEq($.author_id, $.author_id)] });
  }

  /**
   * A PLAIN posts row list (a Select), the parent page for read-RELATION EXECUTION vectors (#43).
   * Unlike `Feed` (whose output is a `{posts, authors}` Φ shape), this returns a bare row list, so
   * the typed-object `readBundle` surface attaches the declaratively-selected `bundle.relations`
   * (belongsTo `author`, hasMany `comments`, hasMany-limit `tags`) onto each post — the batch-load
   * + hydrate the non-TS runtimes must now reproduce.
   */
  Posts($: In<{ author_id: number }>) {
    return L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title', 'status'],
      where: [whereEq($.author_id, $.author_id)],
      order: 'id ASC',
    });
  }

  /**
   * A COMPOSITE-key parent page (#47 item 1): `docs` keyed by (tenant_id, doc_id). Each doc gets a
   * composite belongsTo `owner` (matched on the SAME (tenant_id, owner_id) tuple → users) and a
   * composite hasMany `revisions` (matched on (tenant_id, doc_id)). The batch-load + hydrate the
   * composite `RelationOp` shape drives, across all 5 runtimes.
   */
  Docs($: In<{ tenant_id: number }>) {
    return L.Select({
      table: 'docs2',
      select: ['tenant_id', 'doc_id', 'owner_id', 'title'],
      where: [whereEq($.tenant_id, $.tenant_id)],
      order: 'doc_id ASC',
    });
  }
}

class PostCommands extends SemanticBehavior {
  Create($: In<{ author_id: number; title: string; request_id: string }>) {
    return L.Insert({
      table: 'posts',
      'values.author_id': $.author_id,
      'values.title': $.title,
      returning: 'id, author_id, title',
    });
  }
}

const postWrites = entityWrites<PostCommands>((w) => ({
  create: w.lifecycle({
    requires: [w.exists('users', { id: '$.input.author_id' })],
    idempotency: w.idempotentBy('idem', 'token', '$.input.request_id'),
    unique: [
      w.unique({ name: 'title_per_author', guardTable: 'uniq', scope: ['$.input.author_id'], fields: ['$.input.title'] }),
    ],
    derive: [w.increment('users', { id: '$.input.author_id' }, 'post_count', +1)],
    emits: [w.event('PostCreated', 'outbox', { postId: '$.entity.id', userId: '$.input.author_id' })],
  }),
}));

const blogRelations: readonly RelationDecl[] = [
  { name: 'author', kind: 'belongsTo', targetTable: 'users', select: ['id', 'name'], parentKey: 'author_id', targetKey: 'id' },
  {
    name: 'tags',
    kind: 'hasMany',
    targetTable: 'tags',
    select: ['id', 'post_id', 'label'],
    parentKey: 'id',
    targetKey: 'post_id',
    order: 'id ASC',
    limit: 2,
  },
];

/**
 * The read-relation declarations for the `Posts` parent page (read-RELATION EXECUTION, #43):
 * belongsTo `author` (single key), hasMany `comments` (UNLIMITED — the plain `= ANY(?)` batch), and
 * hasMany-limit `tags` (per-parent LATERAL/ROW_NUMBER window). All three ride the single-key
 * `RelationOp` shape (`bundle.relations`) — the batch-load + hydrate every runtime must reproduce.
 */
const postReadRelations: readonly RelationDecl[] = [
  { name: 'author', kind: 'belongsTo', targetTable: 'users', select: ['id', 'name'], parentKey: 'author_id', targetKey: 'id' },
  { name: 'comments', kind: 'hasMany', targetTable: 'comments', select: ['id', 'post_id', 'body'], parentKey: 'id', targetKey: 'post_id', order: 'id ASC' },
  {
    name: 'tags',
    kind: 'hasMany',
    targetTable: 'tags',
    select: ['id', 'post_id', 'label'],
    parentKey: 'id',
    targetKey: 'post_id',
    order: 'id ASC',
    limit: 2,
  },
];

/**
 * COMPOSITE-key read-relation declarations (#47 item 1). `owner` is a composite belongsTo matched
 * on the (tenant_id, owner_id) → users2(tenant_id, uid) tuple; `revisions` a composite hasMany
 * matched on (tenant_id, doc_id) → revs(tenant_id, doc_id). Both ride the composite `RelationOp`
 * shape (parentKeys/targetKeys arrays) — the batch-load + hydrate every runtime must reproduce.
 */
const docCompositeRelations: readonly RelationDecl[] = [
  {
    name: 'owner',
    kind: 'belongsTo',
    targetTable: 'users2',
    select: ['tenant_id', 'uid', 'name'],
    parentKeys: ['tenant_id', 'owner_id'],
    targetKeys: ['tenant_id', 'uid'],
  },
  {
    name: 'revisions',
    kind: 'hasMany',
    targetTable: 'revs',
    select: ['tenant_id', 'doc_id', 'rev'],
    parentKeys: ['tenant_id', 'doc_id'],
    targetKeys: ['tenant_id', 'doc_id'],
    order: 'rev ASC',
  },
];

// The composite-key read schema (docs2 keyed by (tenant_id, doc_id); users2 by (tenant_id, uid);
// revs by (tenant_id, doc_id)). Two tenants share the SAME uid/doc_id values, so a composite key is
// REQUIRED to disambiguate — a single-key relation would cross-hydrate across tenants.
const COMPOSITE_ROWS = {
  docs2: [
    [1, 10, 100, 'Doc A1'],
    [1, 11, 101, 'Doc B1'],
    [2, 10, 100, 'Doc A2'],
  ],
  users2: [
    [1, 100, 'Ada'],
    [1, 101, 'Alan'],
    [2, 100, 'Bob'],
  ],
  revs: [
    [1, 10, 'r1'],
    [1, 10, 'r2'],
    [1, 11, 'r3'],
    [2, 10, 'r9'],
  ],
} as const;

function compositeSchema(kind: 'sqlite' | 'pg' | 'mysql'): readonly string[] {
  const intT = kind === 'mysql' ? 'INT' : 'INTEGER';
  const txtT = kind === 'mysql' ? 'VARCHAR(255)' : 'TEXT';
  const ddl = [
    `CREATE TABLE docs2 (tenant_id ${intT} NOT NULL, doc_id ${intT} NOT NULL, owner_id ${intT} NOT NULL, title ${txtT} NOT NULL, PRIMARY KEY (tenant_id, doc_id))`,
    `CREATE TABLE users2 (tenant_id ${intT} NOT NULL, uid ${intT} NOT NULL, name ${txtT} NOT NULL, PRIMARY KEY (tenant_id, uid))`,
    `CREATE TABLE revs (tenant_id ${intT} NOT NULL, doc_id ${intT} NOT NULL, rev ${txtT} NOT NULL, PRIMARY KEY (tenant_id, doc_id, rev))`,
  ];
  const ins: string[] = [];
  for (const [t, s, o, title] of COMPOSITE_ROWS.docs2) ins.push(`INSERT INTO docs2 VALUES (${t}, ${s}, ${o}, '${title}')`);
  for (const [t, u, n] of COMPOSITE_ROWS.users2) ins.push(`INSERT INTO users2 VALUES (${t}, ${u}, '${n}')`);
  for (const [t, d, r] of COMPOSITE_ROWS.revs) ins.push(`INSERT INTO revs VALUES (${t}, ${d}, '${r}')`);
  return [...ddl, ...ins];
}
const COMPOSITE_SCHEMA_SQLITE = compositeSchema('sqlite');
const COMPOSITE_SCHEMA_PG = compositeSchema('pg');
const COMPOSITE_SCHEMA_MYSQL = compositeSchema('mysql');

// ── The SQLite reference schema (from harness.ts) — the reference execution seam ──

const READ_SCHEMA_SQLITE: readonly string[] = [
  `CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, created_at TEXT NOT NULL)`,
  `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`,
  `CREATE TABLE tags (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, label TEXT)`,
  `CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, body TEXT)`,
  `INSERT INTO posts VALUES (1, 7, 'Hello', 'live', '2026-02-01')`,
  `INSERT INTO posts VALUES (2, 7, 'World', 'draft', '2026-03-01')`,
  `INSERT INTO posts VALUES (3, 8, 'Other', 'live', '2026-01-15')`,
  `INSERT INTO users VALUES (7, 'Ada')`,
  `INSERT INTO users VALUES (8, 'Alan')`,
  `INSERT INTO tags VALUES (10, 1, 'greeting')`,
  `INSERT INTO tags VALUES (11, 1, 'first')`,
  `INSERT INTO tags VALUES (12, 2, 'world')`,
  `INSERT INTO comments VALUES (100, 1, 'nice')`,
  `INSERT INTO comments VALUES (101, 1, 'agreed')`,
  `INSERT INTO comments VALUES (102, 2, 'hi')`,
];

// Fixed UUIDs for the #46 uuid IN-list coverage.
const DOC_UUIDS = [
  '11111111-1111-1111-1111-111111111111',
  '22222222-2222-2222-2222-222222222222',
  '33333333-3333-3333-3333-333333333333',
] as const;

// The IN-list read schema (posts for int IN-list + docs for uuid IN-list). SQLite: uuid is TEXT.
const INLIST_SCHEMA_SQLITE: readonly string[] = [
  `CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, created_at TEXT NOT NULL)`,
  `CREATE TABLE docs (doc_id TEXT PRIMARY KEY, title TEXT NOT NULL)`,
  `INSERT INTO posts VALUES (1, 7, 'Hello', 'live', '2026-02-01')`,
  `INSERT INTO posts VALUES (2, 7, 'World', 'draft', '2026-03-01')`,
  `INSERT INTO posts VALUES (3, 8, 'Other', 'live', '2026-01-15')`,
  `INSERT INTO docs VALUES ('${DOC_UUIDS[0]}', 'Doc A')`,
  `INSERT INTO docs VALUES ('${DOC_UUIDS[1]}', 'Doc B')`,
  `INSERT INTO docs VALUES ('${DOC_UUIDS[2]}', 'Doc C')`,
];

// The all-element-types IN-list read schema (#46, item 4). A single `typed` table carries a
// BIGINT / TEXT / BOOL / TIMESTAMP / NUMERIC key column each; every vector filters on ONE of them
// via `col = ANY($1)` and SELECTs only the STABLE text `label`, so the assembled result is
// dialect-invariant (isolating the ARRAY-BINDING of each element type from result-normalization).
// Timestamp literals are the canonical `YYYY-MM-DD HH:MM:SS` form all three dialects round-trip.
const TYPED_BIG = [5000000001, 5000000002, 5000000003] as const; // > int32 → forces bigint column
const TYPED_TS = ['2026-01-01 00:00:00', '2026-02-01 00:00:00', '2026-03-01 00:00:00'] as const;
const TYPED_ROWS_SQLITE: readonly string[] = [
  `INSERT INTO typed VALUES (${TYPED_BIG[0]}, 'alpha', 1, '${TYPED_TS[0]}', 10.50, 'A')`,
  `INSERT INTO typed VALUES (${TYPED_BIG[1]}, 'beta', 0, '${TYPED_TS[1]}', 20.25, 'B')`,
  `INSERT INTO typed VALUES (${TYPED_BIG[2]}, 'gamma', 1, '${TYPED_TS[2]}', 30.75, 'C')`,
];
const TYPED_SCHEMA_SQLITE: readonly string[] = [
  `CREATE TABLE typed (big INTEGER PRIMARY KEY, txt TEXT NOT NULL, flag INTEGER NOT NULL, ts TEXT NOT NULL, amt REAL NOT NULL, label TEXT NOT NULL)`,
  ...TYPED_ROWS_SQLITE,
];
const TYPED_SCHEMA_PG: readonly string[] = [
  `CREATE TABLE typed (big BIGINT PRIMARY KEY, txt TEXT NOT NULL, flag BOOLEAN NOT NULL, ts TIMESTAMP NOT NULL, amt NUMERIC(10,2) NOT NULL, label TEXT NOT NULL)`,
  `INSERT INTO typed VALUES (${TYPED_BIG[0]}, 'alpha', TRUE,  '${TYPED_TS[0]}', 10.50, 'A')`,
  `INSERT INTO typed VALUES (${TYPED_BIG[1]}, 'beta',  FALSE, '${TYPED_TS[1]}', 20.25, 'B')`,
  `INSERT INTO typed VALUES (${TYPED_BIG[2]}, 'gamma', TRUE,  '${TYPED_TS[2]}', 30.75, 'C')`,
];
const TYPED_SCHEMA_MYSQL: readonly string[] = [
  `CREATE TABLE typed (big BIGINT PRIMARY KEY, txt VARCHAR(255) NOT NULL, flag TINYINT(1) NOT NULL, ts DATETIME NOT NULL, amt DECIMAL(10,2) NOT NULL, label VARCHAR(255) NOT NULL)`,
  `INSERT INTO typed VALUES (${TYPED_BIG[0]}, 'alpha', 1, '${TYPED_TS[0]}', 10.50, 'A')`,
  `INSERT INTO typed VALUES (${TYPED_BIG[1]}, 'beta',  0, '${TYPED_TS[1]}', 20.25, 'B')`,
  `INSERT INTO typed VALUES (${TYPED_BIG[2]}, 'gamma', 1, '${TYPED_TS[2]}', 30.75, 'C')`,
];

const WRITE_SCHEMA_SQLITE: readonly string[] = [
  `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, post_count INTEGER NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER NOT NULL REFERENCES users(id), title TEXT NOT NULL, created_at TEXT)`,
  `CREATE TABLE idem (token TEXT PRIMARY KEY)`,
  `CREATE TABLE uniq (name TEXT NOT NULL, s0 INTEGER NOT NULL, f0 TEXT NOT NULL, PRIMARY KEY (name, s0, f0))`,
  `CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, payload TEXT NOT NULL)`,
  `INSERT INTO users (id, name, post_count) VALUES (7, 'Ada', 2)`,
  `INSERT INTO users (id, name, post_count) VALUES (8, 'Alan', 0)`,
];

// ── The PG / MySQL DDL + seed — semantics-matched to the SQLite reference schema ──
//
// Column TYPES are the natural per-dialect equivalent (INTEGER→INT/SERIAL, TEXT→TEXT/VARCHAR, uuid
// TEXT→PG `uuid`/MySQL `CHAR(36)`); the values seeded and read are dialect-invariant scalars, so the
// assembled result is identical to the SQLite reference. `docs.doc_id` is a REAL `uuid` on PG so the
// #46 no-cast `= ANY($1)` must let PG infer `uuid[]` from the column (a value-inferred `text[]` cast
// would fail `uuid = text`). Each language uses its OWN namespaced schema (a distinct PG schema /
// MySQL database), so these DDLs are applied inside that namespace at run time.
//
// The write schema's `posts.id` is a fresh identity column (SERIAL / AUTO_INCREMENT) so the first
// insert's RETURNING/insertId is 1 — matching the SQLite AUTOINCREMENT reference (`entity.id: 1`).
// The runner TRUNCATEs + reseeds before each vector so the identity restarts at 1.

const READ_SCHEMA_PG: readonly string[] = [
  `CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, created_at TEXT NOT NULL)`,
  `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`,
  `CREATE TABLE tags (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, label TEXT)`,
  `CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, body TEXT)`,
  `INSERT INTO posts VALUES (1, 7, 'Hello', 'live', '2026-02-01')`,
  `INSERT INTO posts VALUES (2, 7, 'World', 'draft', '2026-03-01')`,
  `INSERT INTO posts VALUES (3, 8, 'Other', 'live', '2026-01-15')`,
  `INSERT INTO users VALUES (7, 'Ada')`,
  `INSERT INTO users VALUES (8, 'Alan')`,
  `INSERT INTO tags VALUES (10, 1, 'greeting')`,
  `INSERT INTO tags VALUES (11, 1, 'first')`,
  `INSERT INTO tags VALUES (12, 2, 'world')`,
  `INSERT INTO comments VALUES (100, 1, 'nice')`,
  `INSERT INTO comments VALUES (101, 1, 'agreed')`,
  `INSERT INTO comments VALUES (102, 2, 'hi')`,
];

const READ_SCHEMA_MYSQL: readonly string[] = [
  `CREATE TABLE posts (id INT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, status VARCHAR(255), created_at VARCHAR(255) NOT NULL)`,
  `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))`,
  `CREATE TABLE tags (id INT PRIMARY KEY, post_id INT NOT NULL, label VARCHAR(255))`,
  `CREATE TABLE comments (id INT PRIMARY KEY, post_id INT NOT NULL, body VARCHAR(255))`,
  `INSERT INTO posts VALUES (1, 7, 'Hello', 'live', '2026-02-01')`,
  `INSERT INTO posts VALUES (2, 7, 'World', 'draft', '2026-03-01')`,
  `INSERT INTO posts VALUES (3, 8, 'Other', 'live', '2026-01-15')`,
  `INSERT INTO users VALUES (7, 'Ada')`,
  `INSERT INTO users VALUES (8, 'Alan')`,
  `INSERT INTO tags VALUES (10, 1, 'greeting')`,
  `INSERT INTO tags VALUES (11, 1, 'first')`,
  `INSERT INTO tags VALUES (12, 2, 'world')`,
  `INSERT INTO comments VALUES (100, 1, 'nice')`,
  `INSERT INTO comments VALUES (101, 1, 'agreed')`,
  `INSERT INTO comments VALUES (102, 2, 'hi')`,
];

// PG: `docs.doc_id` is a REAL `uuid` column (the #46 uuid inference target).
const INLIST_SCHEMA_PG: readonly string[] = [
  `CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, created_at TEXT NOT NULL)`,
  `CREATE TABLE docs (doc_id UUID PRIMARY KEY, title TEXT NOT NULL)`,
  `INSERT INTO posts VALUES (1, 7, 'Hello', 'live', '2026-02-01')`,
  `INSERT INTO posts VALUES (2, 7, 'World', 'draft', '2026-03-01')`,
  `INSERT INTO posts VALUES (3, 8, 'Other', 'live', '2026-01-15')`,
  `INSERT INTO docs VALUES ('${DOC_UUIDS[0]}', 'Doc A')`,
  `INSERT INTO docs VALUES ('${DOC_UUIDS[1]}', 'Doc B')`,
  `INSERT INTO docs VALUES ('${DOC_UUIDS[2]}', 'Doc C')`,
];

// MySQL: no `uuid` type — CHAR(36) holds the canonical text form (server returns the same string).
const INLIST_SCHEMA_MYSQL: readonly string[] = [
  `CREATE TABLE posts (id INT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, status VARCHAR(255), created_at VARCHAR(255) NOT NULL)`,
  `CREATE TABLE docs (doc_id CHAR(36) PRIMARY KEY, title VARCHAR(255) NOT NULL)`,
  `INSERT INTO posts VALUES (1, 7, 'Hello', 'live', '2026-02-01')`,
  `INSERT INTO posts VALUES (2, 7, 'World', 'draft', '2026-03-01')`,
  `INSERT INTO posts VALUES (3, 8, 'Other', 'live', '2026-01-15')`,
  `INSERT INTO docs VALUES ('${DOC_UUIDS[0]}', 'Doc A')`,
  `INSERT INTO docs VALUES ('${DOC_UUIDS[1]}', 'Doc B')`,
  `INSERT INTO docs VALUES ('${DOC_UUIDS[2]}', 'Doc C')`,
];

const WRITE_SCHEMA_PG: readonly string[] = [
  `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, post_count INTEGER NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id SERIAL PRIMARY KEY, author_id INTEGER NOT NULL REFERENCES users(id), title TEXT NOT NULL, created_at TEXT)`,
  `CREATE TABLE idem (token TEXT PRIMARY KEY)`,
  `CREATE TABLE uniq (name TEXT NOT NULL, s0 INTEGER NOT NULL, f0 TEXT NOT NULL, PRIMARY KEY (name, s0, f0))`,
  `CREATE TABLE outbox (id SERIAL PRIMARY KEY, type TEXT NOT NULL, payload TEXT NOT NULL)`,
  `INSERT INTO users (id, name, post_count) VALUES (7, 'Ada', 2)`,
  `INSERT INTO users (id, name, post_count) VALUES (8, 'Alan', 0)`,
];

const WRITE_SCHEMA_MYSQL: readonly string[] = [
  `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), post_count INT NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, created_at VARCHAR(255), FOREIGN KEY (author_id) REFERENCES users(id))`,
  `CREATE TABLE idem (token VARCHAR(255) PRIMARY KEY)`,
  `CREATE TABLE uniq (name VARCHAR(190) NOT NULL, s0 INT NOT NULL, f0 VARCHAR(190) NOT NULL, PRIMARY KEY (name, s0, f0))`,
  `CREATE TABLE outbox (id INT AUTO_INCREMENT PRIMARY KEY, type VARCHAR(255) NOT NULL, payload TEXT NOT NULL)`,
  `INSERT INTO users (id, name, post_count) VALUES (7, 'Ada', 2)`,
  `INSERT INTO users (id, name, post_count) VALUES (8, 'Alan', 0)`,
];

// ── Batch write (createMany/updateMany/deleteMany) + edge/composite/PK fixtures ──
//
// Schemas mirror the SQLite reference per dialect (semantics-matched types). Each language leg
// TRUNCATEs/reseeds before every vector, so identity restarts at 1 and the reference (captured on
// SQLite) matches. A batch write and a single UPDATE/DELETE ride the SAME gate-free TransactionPlan
// as the write-tx vectors, so they are `kind:'tx'` — the runners need no new branch.

// posts table with a NULLABLE `subtitle` so a heterogeneous createMany forms >1 column-set group.
const BATCH_SCHEMA_SQLITE: readonly string[] = [
  `CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER NOT NULL, title TEXT NOT NULL, subtitle TEXT)`,
];
const BATCH_SCHEMA_PG: readonly string[] = [
  `CREATE TABLE posts (id SERIAL PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, subtitle TEXT)`,
];
const BATCH_SCHEMA_MYSQL: readonly string[] = [
  `CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, subtitle VARCHAR(255))`,
];

// A NON-auto-increment `id` PK: explicit ids make a multi-statement heterogeneous createMany
// dialect-invariant (MySQL 8's bulk-insert auto-increment reserves ids with a non-deterministic gap
// vs SQLite — an identity-ALLOCATION divergence orthogonal to the grouping under test).
const BATCH_NOAUTOINC_SQLITE: readonly string[] = [
  `CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, subtitle TEXT)`,
];
const BATCH_NOAUTOINC_PG: readonly string[] = [
  `CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, subtitle TEXT)`,
];
const BATCH_NOAUTOINC_MYSQL: readonly string[] = [
  `CREATE TABLE posts (id INT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, subtitle VARCHAR(255))`,
];

// A pre-seeded posts table for updateMany / deleteMany / bare UPDATE / bare DELETE.
const SEEDED_POSTS_SQLITE: readonly string[] = [
  `CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, subtitle TEXT)`,
  `INSERT INTO posts VALUES (1, 7, 'One', NULL)`,
  `INSERT INTO posts VALUES (2, 7, 'Two', NULL)`,
  `INSERT INTO posts VALUES (3, 8, 'Three', NULL)`,
];
const SEEDED_POSTS_PG: readonly string[] = [
  `CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, subtitle TEXT)`,
  `INSERT INTO posts VALUES (1, 7, 'One', NULL)`,
  `INSERT INTO posts VALUES (2, 7, 'Two', NULL)`,
  `INSERT INTO posts VALUES (3, 8, 'Three', NULL)`,
];
const SEEDED_POSTS_MYSQL: readonly string[] = [
  `CREATE TABLE posts (id INT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, subtitle VARCHAR(255))`,
  `INSERT INTO posts VALUES (1, 7, 'One', NULL)`,
  `INSERT INTO posts VALUES (2, 7, 'Two', NULL)`,
  `INSERT INTO posts VALUES (3, 8, 'Three', NULL)`,
];

// UUID-PK table (client-supplied PK + RETURNING → MySQL emul re-selects by the uuid, not id).
const UUIDPK_SCHEMA_SQLITE: readonly string[] = [
  `CREATE TABLE docs (doc_id TEXT PRIMARY KEY, title TEXT NOT NULL)`,
];
const UUIDPK_SCHEMA_PG: readonly string[] = [
  `CREATE TABLE docs (doc_id UUID PRIMARY KEY, title TEXT NOT NULL)`,
];
const UUIDPK_SCHEMA_MYSQL: readonly string[] = [
  `CREATE TABLE docs (doc_id CHAR(36) PRIMARY KEY, title VARCHAR(255) NOT NULL)`,
];

// Composite-PK table (two-column PK + RETURNING → MySQL emul re-selects by BOTH pk columns).
const COMPOSITEPK_SCHEMA_SQLITE: readonly string[] = [
  `CREATE TABLE order_lines (order_id INTEGER NOT NULL, line_no INTEGER NOT NULL, sku TEXT NOT NULL, PRIMARY KEY (order_id, line_no))`,
];
const COMPOSITEPK_SCHEMA_PG: readonly string[] = [
  `CREATE TABLE order_lines (order_id INTEGER NOT NULL, line_no INTEGER NOT NULL, sku TEXT NOT NULL, PRIMARY KEY (order_id, line_no))`,
];
const COMPOSITEPK_SCHEMA_MYSQL: readonly string[] = [
  `CREATE TABLE order_lines (order_id INT NOT NULL, line_no INT NOT NULL, sku VARCHAR(255) NOT NULL, PRIMARY KEY (order_id, line_no))`,
];

// Edge tables: m2m `post_tags` (link INSERT) + related `comments` with an FK column (fk UPDATE).
const EDGE_SCHEMA_SQLITE: readonly string[] = [
  `CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER NOT NULL, title TEXT NOT NULL)`,
  `CREATE TABLE post_tags (post_id INTEGER NOT NULL, tag_id INTEGER NOT NULL, PRIMARY KEY (post_id, tag_id))`,
  `CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER, body TEXT NOT NULL)`,
  `INSERT INTO comments VALUES (100, NULL, 'orphan comment')`,
];
const EDGE_SCHEMA_PG: readonly string[] = [
  `CREATE TABLE posts (id SERIAL PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL)`,
  `CREATE TABLE post_tags (post_id INTEGER NOT NULL, tag_id INTEGER NOT NULL, PRIMARY KEY (post_id, tag_id))`,
  `CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER, body TEXT NOT NULL)`,
  `INSERT INTO comments VALUES (100, NULL, 'orphan comment')`,
];
const EDGE_SCHEMA_MYSQL: readonly string[] = [
  `CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL)`,
  `CREATE TABLE post_tags (post_id INT NOT NULL, tag_id INT NOT NULL, PRIMARY KEY (post_id, tag_id))`,
  `CREATE TABLE comments (id INT PRIMARY KEY, post_id INT, body VARCHAR(255) NOT NULL)`,
  `INSERT INTO comments VALUES (100, NULL, 'orphan comment')`,
];

// Composite (multi-write DAG) schema: parent post + child comment (child.post_id = $.ref.post.id).
const DAG_SCHEMA_SQLITE: readonly string[] = [
  `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, post_count INTEGER NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER NOT NULL REFERENCES users(id), title TEXT NOT NULL)`,
  `CREATE TABLE comments (id INTEGER PRIMARY KEY AUTOINCREMENT, post_id INTEGER NOT NULL REFERENCES posts(id), body TEXT NOT NULL)`,
  `INSERT INTO users (id, name, post_count) VALUES (7, 'Ada', 0)`,
];
const DAG_SCHEMA_PG: readonly string[] = [
  `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, post_count INTEGER NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id SERIAL PRIMARY KEY, author_id INTEGER NOT NULL REFERENCES users(id), title TEXT NOT NULL)`,
  `CREATE TABLE comments (id SERIAL PRIMARY KEY, post_id INTEGER NOT NULL REFERENCES posts(id), body TEXT NOT NULL)`,
  `INSERT INTO users (id, name, post_count) VALUES (7, 'Ada', 0)`,
];
const DAG_SCHEMA_MYSQL: readonly string[] = [
  `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), post_count INT NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, FOREIGN KEY (author_id) REFERENCES users(id))`,
  `CREATE TABLE comments (id INT AUTO_INCREMENT PRIMARY KEY, post_id INT NOT NULL, body VARCHAR(255) NOT NULL, FOREIGN KEY (post_id) REFERENCES posts(id))`,
  `INSERT INTO users (id, name, post_count) VALUES (7, 'Ada', 0)`,
];

// Behaviors for bare UPDATE / DELETE / DELETE-RETURNING and edge (single authored write node each).
class PostMutations extends SemanticBehavior {
  /** A single-row bare UPDATE keyed by `$.input.id`. */
  Rename($: In<{ id: number; title: string }>) {
    return L.Update({ table: 'posts', 'set.title': $.title, where: [whereEq($.id, $.id)] });
  }
  /** A single-row bare DELETE keyed by `$.input.id`. */
  Remove($: In<{ id: number }>) {
    return L.Delete({ table: 'posts', where: [whereEq($.id, $.id)] });
  }
  /** A single-row DELETE … RETURNING (PG/SQLite native; MySQL emul returns [] — rows are gone). */
  RemoveReturning($: In<{ id: number }>) {
    return L.Delete({ table: 'posts', where: [whereEq($.id, $.id)], returning: 'id, author_id, title' });
  }
  /** A client-supplied UUID-PK INSERT + RETURNING (pk descriptor drives the MySQL re-select). */
  CreateDoc($: In<{ doc_id: string; title: string }>) {
    return L.Insert({ table: 'docs', 'values.doc_id': $.doc_id, 'values.title': $.title, returning: 'doc_id, title', pk: 'doc_id' });
  }
  /** A composite-PK INSERT + RETURNING (pk descriptor lists BOTH key columns). */
  CreateOrderLine($: In<{ order_id: number; line_no: number; sku: string }>) {
    return L.Insert({
      table: 'order_lines',
      'values.order_id': $.order_id,
      'values.line_no': $.line_no,
      'values.sku': $.sku,
      returning: 'order_id, line_no, sku',
      pk: 'order_id,line_no',
    });
  }
}

// Edge fixture: create a post, then LINK a tag (m2m INSERT) and CLAIM an orphan comment (fk UPDATE).
class PostWithEdges extends SemanticBehavior {
  Create($: In<{ author_id: number; title: string; tag_id: number; comment_id: number }>) {
    return L.Insert({ table: 'posts', 'values.author_id': $.author_id, 'values.title': $.title, returning: 'id, author_id, title' });
  }
}
const postEdgeWrites = entityWrites<PostWithEdges>((w) => ({
  create: w.lifecycle({
    edges: [
      w.edge({ relation: 'm2m', action: 'set', table: 'post_tags', columns: { post_id: '$.entity.id', tag_id: '$.input.tag_id' } }),
      w.edge({ relation: 'fk', action: 'set', table: 'comments', columns: { post_id: '$.entity.id' }, where: { id: '$.input.comment_id' } }),
    ],
  }),
}));

// Composite (multi-write DAG) fixture: parent post → child comment (child.post_id = $.ref.post.id).
class BlogDag extends SemanticBehavior {
  CreatePost($: In<{ author_id: number; title: string; body: string }>) {
    return L.Insert({ table: 'posts', 'values.author_id': $.author_id, 'values.title': $.title, returning: 'id, author_id, title' });
  }
  CreateComment($: In<{ body: string }>) {
    return L.Insert({ table: 'comments', 'values.post_id': $.body, 'values.body': $.body, returning: 'id, post_id, body' });
  }
}
const dagPostWrites = entityWrites<BlogDag>((w) => ({
  create: w.lifecycle({
    requires: [w.exists('users', { id: '$.input.author_id' })],
    derive: [w.increment('users', { id: '$.input.author_id' }, 'post_count', +1)],
  }),
})).create!;
const dagCommentWrites = entityWrites<BlogDag>((w) => ({ create: w.lifecycle({}) })).create!;

// ── Vector shapes ──────────────────────────────────────────────────────────────

interface LiveExecVector {
  name: string;
  kind: 'exec';
  input: EncodedValue;
  bundlePg: SqlBundle;
  bundleMysql: SqlBundle;
  schemaPg: readonly string[];
  schemaMysql: readonly string[];
  expectedResult: EncodedValue;
}

interface LiveTxVector {
  name: string;
  kind: 'tx';
  input: EncodedValue;
  bundlePg: SqlBundle;
  bundleMysql: SqlBundle;
  schemaPg: readonly string[];
  schemaMysql: readonly string[];
  expectedResult: EncodedValue;
  /**
   * OPTIONAL per-dialect result override. Used ONLY where the write result GENUINELY diverges by
   * dialect: a DELETE…RETURNING returns the deleted rows natively on PG (and SQLite, the reference),
   * but MySQL has NO native RETURNING and its emulation is INSERT-only (v1 `mysql.ts` re-selects an
   * INSERT's PK; the pre-image of a DELETE/UPDATE is already gone), so MySQL DELETE…RETURNING yields
   * `[]`. When present, the runner compares the mysql leg against this instead of `expectedResult`.
   * `expectedDbState` still proves the persisted effect on BOTH dialects.
   */
  expectedResultMysql?: EncodedValue;
  expectedDbState: { query: string; rows: EncodedValue }[];
}

/**
 * A read-RELATION EXECUTION vector (#43): the parent read returns a bare row list and the runner
 * must batch-load + hydrate the declaratively-selected `bundle.relations` (`with`) onto each parent
 * — the belongsTo/hasMany/hasMany-limit batch every language runtime must reproduce. `expectedResult`
 * is the byte-true SQLite reference captured by the TS `readBundle` typed-object surface (the eager
 * relation path — the SAME `runRelationOp`/`distributeToParent` the runtimes port).
 */
interface LiveReadVector {
  name: string;
  kind: 'read';
  input: EncodedValue;
  /** The relation names to declaratively select + hydrate onto each parent row (spec §5). */
  with: readonly string[];
  bundlePg: SqlBundle;
  bundleMysql: SqlBundle;
  schemaPg: readonly string[];
  schemaMysql: readonly string[];
  /**
   * PER-DIALECT expected hydrated result. The hasMany-LIMIT relation SQL diverges by dialect in the
   * ORIGINAL v1 `LazyRelation` (PG `CROSS JOIN LATERAL` projects the real child columns; MySQL/SQLite
   * `ROW_NUMBER() OVER` CTE additionally leaks the window column `_rn`). So the hydrated child shape
   * for a limited hasMany is genuinely dialect-dependent — NOT hand-authored, a documented property
   * of the compiled batch SQL. `expectedResultMysql` is the byte-true SQLite `readBundle` reference
   * (identical ROW_NUMBER path); `expectedResultPg` is that reference with the `_rn` window column
   * dropped from hydrated child rows (mirroring PG's LATERAL `tags.*` projection). belongsTo / plain
   * hasMany / empty forms are dialect-invariant, so the two goldens coincide there.
   */
  expectedResultPg: EncodedValue;
  expectedResultMysql: EncodedValue;
}

type LiveVector = LiveExecVector | LiveTxVector | LiveReadVector;

function seedDb(schema: readonly string[]): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  for (const stmt of schema) db.exec(stmt);
  return db;
}

const clone = <T>(v: T): T => JSON.parse(JSON.stringify(v)) as T;

/** The reference exec result — captured by EXECUTING the SQLite bundle (byte-true to harness). */
function execReference(bundle: SqlBundle, input: Record<string, unknown>, schema: readonly string[]): EncodedValue {
  const db = seedDb(schema);
  const result = executeBundle(bundle, input as never, { db });
  db.close();
  return encodeValue(result);
}

/**
 * The reference read+hydrate result — captured by running the TS `readBundle` typed-object surface
 * (the eager relation path: the primary row list + declaratively-selected `bundle.relations` batch-
 * loaded + hydrated onto each parent via `runRelationOp`/`distributeToParent`). Byte-true golden-
 * from-originals: the SAME `LazyRelation`-parity SQL + hydration the runtimes must reproduce.
 */
function readReference(
  bundle: SqlBundle,
  input: Record<string, unknown>,
  schema: readonly string[],
  withNames: readonly string[],
): { pg: EncodedValue; mysql: EncodedValue } {
  const db = seedDb(schema);
  const withSel = Object.fromEntries(withNames.map((n) => [n, true as const]));
  const result = readBundle(bundle, input as never, { db, with: withSel });
  db.close();
  // The SQLite reference == the MySQL live shape (both use the ROW_NUMBER CTE for a limited hasMany,
  // so both carry the `_rn` window column). PG's LATERAL form projects the real child columns only,
  // so its golden is the SAME reference with `_rn` dropped from every hydrated child row — the ONE
  // documented dialect divergence in the compiled relation SQL (never a hand-authored result).
  const mysql = encodeValue(result);
  const pg = encodeValue(stripRnFromChildren(clone(result) as unknown[]));
  return { pg, mysql };
}

/** Drop the leaked `_rn` window column from every hydrated child row (PG LATERAL parity). */
function stripRnFromChildren(rows: unknown[]): unknown[] {
  for (const row of rows) {
    if (row === null || typeof row !== 'object') continue;
    for (const [k, v] of Object.entries(row as Record<string, unknown>)) {
      if (Array.isArray(v)) {
        for (const child of v) {
          if (child !== null && typeof child === 'object' && '_rn' in (child as object)) {
            delete (child as Record<string, unknown>)._rn;
          }
        }
      } else if (v !== null && typeof v === 'object' && '_rn' in (v as object)) {
        delete (v as Record<string, unknown>)._rn;
      }
    }
  }
  return rows;
}

function txReference(
  bundle: SqlBundle,
  input: Record<string, unknown>,
  schema: readonly string[],
  dbQueries: readonly string[],
): { result: EncodedValue; dbState: { query: string; rows: EncodedValue }[] } {
  const db = seedDb(schema);
  const result = executeTransactionBundle(bundle, input as never, { db });
  const dbState = dbQueries.map((query) => ({ query, rows: encodeValue(db.prepare(query).all()) }));
  db.close();
  return { result: encodeValue(result), dbState };
}

interface ExecSpec {
  name: string;
  entry: string;
  input: Record<string, unknown>;
  relations: readonly RelationDecl[];
  schemaSqlite: readonly string[];
  schemaPg: readonly string[];
  schemaMysql: readonly string[];
}

function buildCorpus(): { suite: string; corpusVersion: number; note: string; vectors: LiveVector[] } {
  const blog = publishBehaviors(Blog);
  const cmd = publishBehaviors(PostCommands);

  const execSpecs: ExecSpec[] = [
    // Feed: SKIP + belongsTo/hasMany relations (the relation batch exercises the v1 `= ANY($1::int[])`).
    {
      name: 'Feed: status present + belongsTo/hasMany relations',
      entry: 'Feed',
      input: { author_id: 7, status: 'live', since: '2026-01-01' },
      relations: blogRelations,
      schemaSqlite: READ_SCHEMA_SQLITE,
      schemaPg: READ_SCHEMA_PG,
      schemaMysql: READ_SCHEMA_MYSQL,
    },
    {
      name: 'Feed: status absent (SKIP drop) + relations',
      entry: 'Feed',
      input: { author_id: 7, since: '2026-01-01' },
      relations: blogRelations,
      schemaSqlite: READ_SCHEMA_SQLITE,
      schemaPg: READ_SCHEMA_PG,
      schemaMysql: READ_SCHEMA_MYSQL,
    },
    {
      name: 'Feed: hasMany limit=2 caps children',
      entry: 'Feed',
      input: { author_id: 7, since: '2026-01-01', status: 'live' },
      relations: blogRelations,
      schemaSqlite: READ_SCHEMA_SQLITE,
      schemaPg: READ_SCHEMA_PG,
      schemaMysql: READ_SCHEMA_MYSQL,
    },
    // #46 int IN-list (non-empty): authored `id = ANY($1)` no-cast on live PG; PG infers int[].
    {
      name: 'ByIds: INT IN-list (non-empty) → = ANY($1) no-cast [#46]',
      entry: 'ByIds',
      input: { ids: [1, 3] },
      relations: [],
      schemaSqlite: INLIST_SCHEMA_SQLITE,
      schemaPg: INLIST_SCHEMA_PG,
      schemaMysql: INLIST_SCHEMA_MYSQL,
    },
    // #46 EMPTY int IN-list: `= ANY($1)` with [] → ZERO rows, NO `integer = text` error.
    {
      name: 'ByIds: EMPTY INT IN-list → zero rows, no integer=text error [#46]',
      entry: 'ByIds',
      input: { ids: [] },
      relations: [],
      schemaSqlite: INLIST_SCHEMA_SQLITE,
      schemaPg: INLIST_SCHEMA_PG,
      schemaMysql: INLIST_SCHEMA_MYSQL,
    },
    // #46 uuid IN-list: `doc_id = ANY($1)` no-cast; PG infers uuid[] from the uuid column.
    {
      name: 'ByUuids: UUID IN-list → = ANY($1) PG infers uuid[] [#46]',
      entry: 'ByUuids',
      input: { ids: [DOC_UUIDS[0], DOC_UUIDS[2]] },
      relations: [],
      schemaSqlite: INLIST_SCHEMA_SQLITE,
      schemaPg: INLIST_SCHEMA_PG,
      schemaMysql: INLIST_SCHEMA_MYSQL,
    },
    // #46 item 4 — ALL PG element types bind live through `col = ANY($1)` (no cast) in EVERY
    // driver: bigint / text / bool / timestamp / numeric. Each selects the stable text `label`, so
    // the reference is dialect-invariant; the vector proves the array binding of that element type.
    {
      name: 'ByBig: BIGINT IN-list → = ANY($1) PG infers bigint[] [#46]',
      entry: 'ByBig',
      input: { keys: [TYPED_BIG[0], TYPED_BIG[2]] },
      relations: [],
      schemaSqlite: TYPED_SCHEMA_SQLITE,
      schemaPg: TYPED_SCHEMA_PG,
      schemaMysql: TYPED_SCHEMA_MYSQL,
    },
    {
      name: 'ByTxt: TEXT IN-list → = ANY($1) PG infers text[] [#46]',
      entry: 'ByTxt',
      input: { keys: ['alpha', 'gamma'] },
      relations: [],
      schemaSqlite: TYPED_SCHEMA_SQLITE,
      schemaPg: TYPED_SCHEMA_PG,
      schemaMysql: TYPED_SCHEMA_MYSQL,
    },
    {
      name: 'ByFlag: BOOL IN-list → = ANY($1) PG infers boolean[] [#46]',
      entry: 'ByFlag',
      input: { keys: [true] },
      relations: [],
      schemaSqlite: TYPED_SCHEMA_SQLITE,
      schemaPg: TYPED_SCHEMA_PG,
      schemaMysql: TYPED_SCHEMA_MYSQL,
    },
    {
      name: 'ByTs: TIMESTAMP IN-list → = ANY($1) PG infers timestamp[] [#46]',
      entry: 'ByTs',
      input: { keys: [TYPED_TS[0], TYPED_TS[2]] },
      relations: [],
      schemaSqlite: TYPED_SCHEMA_SQLITE,
      schemaPg: TYPED_SCHEMA_PG,
      schemaMysql: TYPED_SCHEMA_MYSQL,
    },
    {
      name: 'ByAmt: NUMERIC IN-list → = ANY($1) PG infers numeric[] [#46]',
      entry: 'ByAmt',
      input: { keys: [10.5, 30.75] },
      relations: [],
      schemaSqlite: TYPED_SCHEMA_SQLITE,
      schemaPg: TYPED_SCHEMA_PG,
      schemaMysql: TYPED_SCHEMA_MYSQL,
    },
    // count() (#47 item 2) — v1 `SELECT COUNT(*) as count FROM posts[ WHERE …]`, live on PG + MySQL.
    // READ_SCHEMA posts: 3 rows total; author 7 → 2, author 8 → 1, author 999 → 0.
    {
      name: 'CountAll: COUNT(*) over all posts → 3 [#47]',
      entry: 'CountAll',
      input: {},
      relations: [],
      schemaSqlite: READ_SCHEMA_SQLITE,
      schemaPg: READ_SCHEMA_PG,
      schemaMysql: READ_SCHEMA_MYSQL,
    },
    {
      name: 'CountByAuthor: COUNT(*) WHERE author_id = 7 → 2 [#47]',
      entry: 'CountByAuthor',
      input: { author_id: 7 },
      relations: [],
      schemaSqlite: READ_SCHEMA_SQLITE,
      schemaPg: READ_SCHEMA_PG,
      schemaMysql: READ_SCHEMA_MYSQL,
    },
    {
      name: 'CountByAuthor: COUNT(*) WHERE author_id = 999 → 0 (empty) [#47]',
      entry: 'CountByAuthor',
      input: { author_id: 999 },
      relations: [],
      schemaSqlite: READ_SCHEMA_SQLITE,
      schemaPg: READ_SCHEMA_PG,
      schemaMysql: READ_SCHEMA_MYSQL,
    },
  ];

  const exec: LiveExecVector[] = execSpecs.map((s) => {
    const pg = compileBundle(blog, s.entry, s.relations, 'postgres');
    const mysql = compileBundle(blog, s.entry, s.relations, 'mysql');
    const sqlite = compileBundle(blog, s.entry, s.relations, 'sqlite');
    return {
      name: s.name,
      kind: 'exec',
      input: encodeValue(s.input),
      bundlePg: clone(pg),
      bundleMysql: clone(mysql),
      schemaPg: s.schemaPg,
      schemaMysql: s.schemaMysql,
      expectedResult: execReference(sqlite, s.input, s.schemaSqlite),
    };
  });

  // ── read-RELATION EXECUTION vectors (#43): batch-load + hydrate bundle.relations onto parents ──
  interface ReadSpec {
    name: string;
    input: Record<string, unknown>;
    with: readonly string[];
    relations: readonly RelationDecl[];
    /** The parent behavior entry (default `Posts`). */
    entry?: string;
    /** The reference/live schemas (default the single-key READ schemas). */
    schemaSqlite?: readonly string[];
    schemaPg?: readonly string[];
    schemaMysql?: readonly string[];
  }
  const readSpecs: ReadSpec[] = [
    // belongsTo (single key) alone: each post gets its `author` object (or null).
    { name: 'Posts: belongsTo author hydrated onto each parent [#43]', input: { author_id: 7 }, with: ['author'], relations: postReadRelations },
    // hasMany UNLIMITED (plain `= ANY(?)` batch): each post gets its `comments` list ([] when none).
    { name: 'Posts: hasMany comments (unlimited) hydrated as child lists [#43]', input: { author_id: 7 }, with: ['comments'], relations: postReadRelations },
    // hasMany with per-parent limit=2 (LATERAL / ROW_NUMBER window): capped `tags` list per post.
    { name: 'Posts: hasMany-limit tags (per-parent cap) hydrated [#43]', input: { author_id: 7 }, with: ['tags'], relations: postReadRelations },
    // All three cardinalities at once (independent sibling relations — the #40 parallel fan-out set).
    { name: 'Posts: belongsTo + hasMany + hasMany-limit all hydrated [#43]', input: { author_id: 7 }, with: ['author', 'comments', 'tags'], relations: postReadRelations },
    // EMPTY parent set → every relation short-circuits (no query), each parent hydrates empty.
    { name: 'Posts: empty parent set → relations short-circuit, empty hydration [#43]', input: { author_id: 999 }, with: ['author', 'comments', 'tags'], relations: postReadRelations },
    // COMPOSITE-key relations (#47 item 1) — the (tenant_id, …) tuple disambiguates tenants that
    // share uid/doc_id. belongsTo `owner` + hasMany `revisions`, both single-tenant + cross-tenant.
    {
      name: 'Docs[tenant 1]: composite belongsTo owner + hasMany revisions hydrated [#47]',
      input: { tenant_id: 1 }, with: ['owner', 'revisions'], relations: docCompositeRelations,
      entry: 'Docs', schemaSqlite: COMPOSITE_SCHEMA_SQLITE, schemaPg: COMPOSITE_SCHEMA_PG, schemaMysql: COMPOSITE_SCHEMA_MYSQL,
    },
    {
      name: 'Docs[tenant 2]: composite keys disambiguate — same uid/doc_id, different tenant [#47]',
      input: { tenant_id: 2 }, with: ['owner', 'revisions'], relations: docCompositeRelations,
      entry: 'Docs', schemaSqlite: COMPOSITE_SCHEMA_SQLITE, schemaPg: COMPOSITE_SCHEMA_PG, schemaMysql: COMPOSITE_SCHEMA_MYSQL,
    },
    {
      name: 'Docs[tenant 9]: empty composite parent set → relations short-circuit [#47]',
      input: { tenant_id: 9 }, with: ['owner', 'revisions'], relations: docCompositeRelations,
      entry: 'Docs', schemaSqlite: COMPOSITE_SCHEMA_SQLITE, schemaPg: COMPOSITE_SCHEMA_PG, schemaMysql: COMPOSITE_SCHEMA_MYSQL,
    },
  ];

  const read: LiveReadVector[] = readSpecs.map((s) => {
    const entry = s.entry ?? 'Posts';
    const schemaSqlite = s.schemaSqlite ?? READ_SCHEMA_SQLITE;
    const pg = compileBundle(blog, entry, s.relations, 'postgres');
    const mysql = compileBundle(blog, entry, s.relations, 'mysql');
    const sqlite = compileBundle(blog, entry, s.relations, 'sqlite');
    const ref = readReference(sqlite, s.input, schemaSqlite, s.with);
    return {
      name: s.name,
      kind: 'read',
      input: encodeValue(s.input),
      with: s.with,
      bundlePg: clone(pg),
      bundleMysql: clone(mysql),
      schemaPg: s.schemaPg ?? READ_SCHEMA_PG,
      schemaMysql: s.schemaMysql ?? READ_SCHEMA_MYSQL,
      expectedResultPg: ref.pg,
      expectedResultMysql: ref.mysql,
    };
  });

  const dbAsserts = [
    'SELECT id, author_id, title FROM posts ORDER BY id',
    'SELECT id, post_count FROM users ORDER BY id',
    'SELECT type, payload FROM outbox ORDER BY id',
  ];
  const txInputs: { name: string; input: Record<string, unknown> }[] = [
    { name: 'create: gate-first tx commits (author exists, unique, idempotent)', input: { author_id: 7, title: 'New Post', request_id: 'req-1' } },
    { name: 'create: gate short-circuits on missing author (ROLLBACK, no body write)', input: { author_id: 999, title: 'Orphan', request_id: 'req-2' } },
  ];

  const tx: LiveTxVector[] = txInputs.map(({ name, input }) => {
    const pg = compileWriteBundle(cmd, 'Create', postWrites, 'create', 'postgres');
    const mysql = compileWriteBundle(cmd, 'Create', postWrites, 'create', 'mysql');
    const sqlite = compileWriteBundle(cmd, 'Create', postWrites, 'create', 'sqlite');
    const ref = txReference(sqlite, input, WRITE_SCHEMA_SQLITE, dbAsserts);
    return {
      name,
      kind: 'tx',
      input: encodeValue(input),
      bundlePg: clone(pg),
      bundleMysql: clone(mysql),
      schemaPg: WRITE_SCHEMA_PG,
      schemaMysql: WRITE_SCHEMA_MYSQL,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    };
  });

  // ── tx VARIANTS live (gap #3): idempotency HIT / unique COLLISION / edge / composite-DAG ──
  // These ride `compileWriteBundle`/`compileCompositeWriteBundle` (the SAME derivation the mock
  // corpus locks) but with inputs/schemas that EXERCISE the gate short-circuit + edge/DAG effects on
  // LIVE PG + MySQL — the dialect divergence (`ON CONFLICT DO NOTHING` vs `INSERT IGNORE`) is now run.
  const txVariants: LiveTxVector[] = [];

  // idempotency HIT: the idem token already exists → the gate-first INSERT-IGNORE inserts 0 rows →
  // `idempotent_duplicate` short-circuit (ROLLBACK, no body write). Seed the token up front.
  {
    const input = { author_id: 7, title: 'Dup', request_id: 'req-dup' };
    const schemaSqlite = [...WRITE_SCHEMA_SQLITE, `INSERT INTO idem (token) VALUES ('req-dup')`];
    const schemaPg = [...WRITE_SCHEMA_PG, `INSERT INTO idem (token) VALUES ('req-dup')`];
    const schemaMysql = [...WRITE_SCHEMA_MYSQL, `INSERT INTO idem (token) VALUES ('req-dup')`];
    const sqlite = compileWriteBundle(cmd, 'Create', postWrites, 'create', 'sqlite');
    const ref = txReference(sqlite, input, schemaSqlite, dbAsserts);
    txVariants.push({
      name: 'tx idempotency: duplicate request_id short-circuits (idempotent_duplicate; ON CONFLICT/IGNORE)',
      kind: 'tx',
      input: encodeValue(input),
      bundlePg: clone(compileWriteBundle(cmd, 'Create', postWrites, 'create', 'postgres')),
      bundleMysql: clone(compileWriteBundle(cmd, 'Create', postWrites, 'create', 'mysql')),
      schemaPg,
      schemaMysql,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    });
  }

  // unique COLLISION: the uniqueness guard row already exists → the gate INSERT inserts 0 rows →
  // `unique_collision` short-circuit (ROLLBACK). Seed the guard row (name/scope/field) up front.
  {
    const input = { author_id: 7, title: 'Taken', request_id: 'req-uniq' };
    const guard = `INSERT INTO uniq (name, s0, f0) VALUES ('title_per_author', 7, 'Taken')`;
    const schemaSqlite = [...WRITE_SCHEMA_SQLITE, guard];
    const schemaPg = [...WRITE_SCHEMA_PG, guard];
    const schemaMysql = [...WRITE_SCHEMA_MYSQL, guard];
    const sqlite = compileWriteBundle(cmd, 'Create', postWrites, 'create', 'sqlite');
    const ref = txReference(sqlite, input, schemaSqlite, dbAsserts);
    txVariants.push({
      name: 'tx unique: collision on (title_per_author) short-circuits (unique_collision; ON CONFLICT/IGNORE)',
      kind: 'tx',
      input: encodeValue(input),
      bundlePg: clone(compileWriteBundle(cmd, 'Create', postWrites, 'create', 'postgres')),
      bundleMysql: clone(compileWriteBundle(cmd, 'Create', postWrites, 'create', 'mysql')),
      schemaPg,
      schemaMysql,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    });
  }

  // edge: create a post, LINK a tag (m2m INSERT into post_tags) + CLAIM the orphan comment (fk
  // UPDATE comments.post_id). Emit already runs in the create-commit vector (outbox JSON payload).
  {
    const edgeCmd = publishBehaviors(PostWithEdges);
    const input = { author_id: 7, title: 'Edged', tag_id: 42, comment_id: 100 };
    const edgeAsserts = [
      'SELECT id, author_id, title FROM posts ORDER BY id',
      'SELECT post_id, tag_id FROM post_tags ORDER BY post_id, tag_id',
      'SELECT id, post_id, body FROM comments ORDER BY id',
    ];
    const sqlite = compileWriteBundle(edgeCmd, 'Create', { create: { effects: postEdgeWrites.create!.effects } }, 'create', 'sqlite');
    const ref = txReference(sqlite, input, EDGE_SCHEMA_SQLITE, edgeAsserts);
    txVariants.push({
      name: 'tx edge: m2m link (post_tags INSERT) + fk claim (comments.post_id UPDATE) in one tx',
      kind: 'tx',
      input: encodeValue(input),
      bundlePg: clone(compileWriteBundle(edgeCmd, 'Create', { create: { effects: postEdgeWrites.create!.effects } }, 'create', 'postgres')),
      bundleMysql: clone(compileWriteBundle(edgeCmd, 'Create', { create: { effects: postEdgeWrites.create!.effects } }, 'create', 'mysql')),
      schemaPg: EDGE_SCHEMA_PG,
      schemaMysql: EDGE_SCHEMA_MYSQL,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    });
  }

  // composite multi-write DAG: parent post → child comment (child.post_id = $.ref.post.id), one tx.
  // The authored CreateComment node sets post_id = $.body (a fixture quirk); rewrite it to the real
  // upstream ref `$.ref.post.id` — the SAME manual rewrite the mock harness compositeBundle uses.
  {
    const dag = publishBehaviors(BlogDag);
    const entries = [
      { entry: 'CreatePost', name: 'post', lifecycle: { effects: dagPostWrites.effects } },
      { entry: 'CreateComment', name: 'comment', lifecycle: { effects: dagCommentWrites.effects } },
    ];
    const dagBundle = (dialect: DialectName): SqlBundle => {
      const b = compileCompositeWriteBundle(dag, entries, 'create', dialect);
      const child = b.transaction!.statements.find((s) => s.binds === 'comment')!;
      (child.op as { params: unknown[] }).params = [{ ref: ['body'] }, { ref: ['post', 'id'] }];
      (child.op as { sql: string }).sql = 'INSERT INTO comments (body, post_id) VALUES (?, ?) RETURNING id, post_id, body';
      return b;
    };
    const dagAsserts = [
      'SELECT id, author_id, title FROM posts ORDER BY id',
      'SELECT id, post_count FROM users ORDER BY id',
      'SELECT id, post_id, body FROM comments ORDER BY id',
    ];
    const commit = { author_id: 7, title: 'Nested', body: 'First comment' };
    const rollback = { author_id: 999, title: 'Ghost', body: 'never' };
    for (const [label, input] of [
      ['tx composite-DAG: parent+child commit in one tx (child.post_id = $.ref.post.id)', commit],
      ['tx composite-DAG: gate short-circuits before parent AND child (ROLLBACK)', rollback],
    ] as const) {
      const ref = txReference(dagBundle('sqlite'), input, DAG_SCHEMA_SQLITE, dagAsserts);
      txVariants.push({
        name: label,
        kind: 'tx',
        input: encodeValue(input),
        bundlePg: clone(dagBundle('postgres')),
        bundleMysql: clone(dagBundle('mysql')),
        schemaPg: DAG_SCHEMA_PG,
        schemaMysql: DAG_SCHEMA_MYSQL,
        expectedResult: ref.result,
        expectedDbState: ref.dbState,
      });
    }
  }

  // ── batch writes (gap #1) + bare UPDATE/DELETE + RETURNING PK (gap #2/#4) — all `kind:'tx'` ──
  const batch: LiveTxVector[] = [];
  const mut = publishBehaviors(PostMutations);

  // createMany HOMOGENEOUS (one group, 3 rows) + RETURNING → PG UNNEST / MySQL·SQLite JSON; the
  // MySQL RETURNING emulation must return ALL 3 rows (AUTO_INCREMENT range re-select via pk hint).
  {
    const records = [
      { author_id: 7, title: 'B1' },
      { author_id: 7, title: 'B2' },
      { author_id: 8, title: 'B3' },
    ];
    const opts = { tableName: 'posts', records: clone(records), rawRecords: clone(records), returning: 'id, author_id, title', pk: { columns: ['id'], autoInc: 'id' } };
    const sqlite = compileCreateManyBundle('CreateManyHomo', opts, 'sqlite');
    const ref = txReference(sqlite, {}, BATCH_SCHEMA_SQLITE, ['SELECT id, author_id, title, subtitle FROM posts ORDER BY id']);
    batch.push({
      name: 'createMany homogeneous: 3 rows one group + RETURNING (PG UNNEST / MySQL·SQLite JSON; mysql range re-select)',
      kind: 'tx',
      input: encodeValue({}),
      bundlePg: clone(compileCreateManyBundle('CreateManyHomo', opts, 'postgres')),
      bundleMysql: clone(compileCreateManyBundle('CreateManyHomo', opts, 'mysql')),
      schemaPg: BATCH_SCHEMA_PG,
      schemaMysql: BATCH_SCHEMA_MYSQL,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    });
  }

  // createMany HETEROGENEOUS: rows with different column subsets → MULTIPLE grouped INSERTs (byte-
  // for-byte the DBModel._insert per-group emission). Group 1 = {author_id,id,title}; group 2 adds
  // subtitle. EXPLICIT ids on a NON-auto-increment `id` PK: with two grouped INSERT statements, MySQL
  // 8's bulk-insert auto-increment reserves ids non-deterministically (an id GAP vs SQLite), a
  // genuine dialect divergence in identity ALLOCATION — orthogonal to the grouping being tested. So
  // the rows carry explicit ids and the result is dialect-invariant. No RETURNING (identity is given).
  {
    const records = [
      { id: 1, author_id: 7, title: 'H1' },
      { id: 2, author_id: 7, title: 'H2', subtitle: 'sub2' },
      { id: 3, author_id: 8, title: 'H3' },
    ];
    const opts = { tableName: 'posts', records: clone(records), rawRecords: clone(records) };
    const sqlite = compileCreateManyBundle('CreateManyHetero', opts, 'sqlite');
    const ref = txReference(sqlite, {}, BATCH_NOAUTOINC_SQLITE, ['SELECT id, author_id, title, subtitle FROM posts ORDER BY id']);
    batch.push({
      name: 'createMany heterogeneous: 2 column-set groups → 2 grouped INSERT statements (DBModel._insert grouping)',
      kind: 'tx',
      input: encodeValue({}),
      bundlePg: clone(compileCreateManyBundle('CreateManyHetero', opts, 'postgres')),
      bundleMysql: clone(compileCreateManyBundle('CreateManyHetero', opts, 'mysql')),
      schemaPg: BATCH_NOAUTOINC_PG,
      schemaMysql: BATCH_NOAUTOINC_MYSQL,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    });
  }

  // updateMany: per-row different values keyed by `id` → PG UNNEST / MySQL JSON_TABLE join / SQLite
  // CASE-WHEN (the compileUpdateMany forms driving the v1 builders). Two rows updated.
  {
    const umRecords = [
      { id: 1, title: 'One-upd' },
      { id: 3, title: 'Three-upd' },
    ];
    const opts = {
      tableName: 'posts',
      keyColumns: ['id'],
      updateColumns: ['title'],
      records: umRecords,
      // rawRecords drives the PG UNNEST per-column array-type inference (buildUpdateMany): the `id`
      // key infers `int[]` (not the default `text[]`, which would fail `integer = text` on PG).
      rawRecords: umRecords,
    };
    const sqlite = compileUpdateManyBundle('UpdateMany', clone(opts) as never, 'sqlite');
    const ref = txReference(sqlite, {}, SEEDED_POSTS_SQLITE, ['SELECT id, author_id, title FROM posts ORDER BY id']);
    batch.push({
      name: 'updateMany: per-row title update keyed by id (PG UNNEST / MySQL JSON_TABLE / SQLite CASE-WHEN)',
      kind: 'tx',
      input: encodeValue({}),
      bundlePg: clone(compileUpdateManyBundle('UpdateMany', clone(opts) as never, 'postgres')),
      bundleMysql: clone(compileUpdateManyBundle('UpdateMany', clone(opts) as never, 'mysql')),
      schemaPg: SEEDED_POSTS_PG,
      schemaMysql: SEEDED_POSTS_MYSQL,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    });
  }

  // deleteMany: delete a PK SET (ids 1,3) → v1 IN-list DELETE (PG `= ANY`/`IN`, MySQL·SQLite JSON).
  {
    const opts = { tableName: 'posts', keyColumns: ['id'], keys: [{ id: 1 }, { id: 3 }] };
    const sqlite = compileDeleteManyBundle('DeleteMany', clone(opts), 'sqlite');
    const ref = txReference(sqlite, {}, SEEDED_POSTS_SQLITE, ['SELECT id, author_id, title FROM posts ORDER BY id']);
    batch.push({
      name: 'deleteMany: delete PK set {1,3} via v1 IN-list DELETE (PG = ANY / MySQL·SQLite JSON subquery)',
      kind: 'tx',
      input: encodeValue({}),
      bundlePg: clone(compileDeleteManyBundle('DeleteMany', clone(opts), 'postgres')),
      bundleMysql: clone(compileDeleteManyBundle('DeleteMany', clone(opts), 'mysql')),
      schemaPg: SEEDED_POSTS_PG,
      schemaMysql: SEEDED_POSTS_MYSQL,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    });
  }

  // bare UPDATE (single row): compileWriteBundle over the authored Update node — one body statement.
  {
    const input = { id: 2, title: 'Two-renamed' };
    const writes = { update: { effects: {} } };
    const sqlite = compileWriteBundle(mut, 'Rename', writes, 'update', 'sqlite');
    const ref = txReference(sqlite, input, SEEDED_POSTS_SQLITE, ['SELECT id, author_id, title FROM posts ORDER BY id']);
    batch.push({
      name: 'bare UPDATE: single-row SET title WHERE id (executes live on PG + MySQL)',
      kind: 'tx',
      input: encodeValue(input),
      bundlePg: clone(compileWriteBundle(mut, 'Rename', writes, 'update', 'postgres')),
      bundleMysql: clone(compileWriteBundle(mut, 'Rename', writes, 'update', 'mysql')),
      schemaPg: SEEDED_POSTS_PG,
      schemaMysql: SEEDED_POSTS_MYSQL,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    });
  }

  // bare DELETE (single row).
  {
    const input = { id: 2 };
    const writes = { remove: { effects: {} } };
    const sqlite = compileWriteBundle(mut, 'Remove', writes, 'remove', 'sqlite');
    const ref = txReference(sqlite, input, SEEDED_POSTS_SQLITE, ['SELECT id, author_id, title FROM posts ORDER BY id']);
    batch.push({
      name: 'bare DELETE: single-row DELETE WHERE id (executes live on PG + MySQL)',
      kind: 'tx',
      input: encodeValue(input),
      bundlePg: clone(compileWriteBundle(mut, 'Remove', writes, 'remove', 'postgres')),
      bundleMysql: clone(compileWriteBundle(mut, 'Remove', writes, 'remove', 'mysql')),
      schemaPg: SEEDED_POSTS_PG,
      schemaMysql: SEEDED_POSTS_MYSQL,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    });
  }

  // DELETE … RETURNING: PG (+ SQLite reference) return the deleted row; MySQL has NO native RETURNING
  // and its emulation is INSERT-only (v1 parity), so the mysql leg returns [] — a per-dialect result.
  {
    const input = { id: 2 };
    const writes = { remove: { effects: {} } };
    const sqlite = compileWriteBundle(mut, 'RemoveReturning', writes, 'remove', 'sqlite');
    const ref = txReference(sqlite, input, SEEDED_POSTS_SQLITE, ['SELECT id, author_id, title FROM posts ORDER BY id']);
    // MySQL: same committed/executed/dbState, but MySQL has no native RETURNING and the emulation is
    // INSERT-only (the deleted row's pre-image is gone), so the body write RETURNS no rows → the
    // exposed `$.entity` is null on MySQL (v1 `mysql.ts` parity). Per-dialect expected result.
    const mysqlResult = clone(ref.result) as Record<string, unknown>;
    mysqlResult.entity = null;
    if ('returnedRows' in mysqlResult) delete mysqlResult.returnedRows;
    batch.push({
      name: 'DELETE RETURNING: PG returns the deleted row; MySQL emulation returns [] (per-dialect; #4 flag)',
      kind: 'tx',
      input: encodeValue(input),
      bundlePg: clone(compileWriteBundle(mut, 'RemoveReturning', writes, 'remove', 'postgres')),
      bundleMysql: clone(compileWriteBundle(mut, 'RemoveReturning', writes, 'remove', 'mysql')),
      schemaPg: SEEDED_POSTS_PG,
      schemaMysql: SEEDED_POSTS_MYSQL,
      expectedResult: ref.result,
      expectedResultMysql: mysqlResult as EncodedValue,
      expectedDbState: ref.dbState,
    });
  }

  // UUID-PK insert + RETURNING (gap #4): client-supplied uuid PK; MySQL emul re-selects by doc_id.
  {
    const input = { doc_id: DOC_UUIDS[1], title: 'Doc B' };
    const writes = { create: { effects: {} } };
    const sqlite = compileWriteBundle(mut, 'CreateDoc', writes, 'create', 'sqlite');
    const ref = txReference(sqlite, input, UUIDPK_SCHEMA_SQLITE, [`SELECT doc_id, title FROM docs ORDER BY doc_id`]);
    batch.push({
      name: 'INSERT RETURNING UUID PK: MySQL emul re-selects by doc_id (not id) — #4',
      kind: 'tx',
      input: encodeValue(input),
      bundlePg: clone(compileWriteBundle(mut, 'CreateDoc', writes, 'create', 'postgres')),
      bundleMysql: clone(compileWriteBundle(mut, 'CreateDoc', writes, 'create', 'mysql')),
      schemaPg: UUIDPK_SCHEMA_PG,
      schemaMysql: UUIDPK_SCHEMA_MYSQL,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    });
  }

  // composite-PK insert + RETURNING (gap #4): MySQL emul re-selects by BOTH (order_id, line_no).
  {
    const input = { order_id: 10, line_no: 2, sku: 'SKU-2' };
    const writes = { create: { effects: {} } };
    const sqlite = compileWriteBundle(mut, 'CreateOrderLine', writes, 'create', 'sqlite');
    const ref = txReference(sqlite, input, COMPOSITEPK_SCHEMA_SQLITE, [`SELECT order_id, line_no, sku FROM order_lines ORDER BY order_id, line_no`]);
    batch.push({
      name: 'INSERT RETURNING composite PK: MySQL emul re-selects by (order_id, line_no) — #4',
      kind: 'tx',
      input: encodeValue(input),
      bundlePg: clone(compileWriteBundle(mut, 'CreateOrderLine', writes, 'create', 'postgres')),
      bundleMysql: clone(compileWriteBundle(mut, 'CreateOrderLine', writes, 'create', 'mysql')),
      schemaPg: COMPOSITEPK_SCHEMA_PG,
      schemaMysql: COMPOSITEPK_SCHEMA_MYSQL,
      expectedResult: ref.result,
      expectedDbState: ref.dbState,
    });
  }

  return {
    suite: 'livedb',
    corpusVersion: LIVEDB_CORPUS_VERSION,
    note:
      'Live-DB corpus (v2 makeSQL flip): exec/tx STATIC bundles (readGraph + statementsById / ' +
      'transaction plan) compiled for postgres + mysql, executed against REAL dockerized PG + MySQL ' +
      'by each language runtime; expectedResult/expectedDbState are the byte-true SQLite reference ' +
      '(dialect-invariant §10 promise). Includes #46 IN-list cases (int / empty / uuid) + relation ' +
      'batches so every language binds the no-cast `= ANY($1)` + `= ANY($1::int[])` on live PG. ' +
      'Adds read-RELATION EXECUTION vectors (#43): the parent row list + batch-loaded/hydrated ' +
      'belongsTo/hasMany/hasMany-limit bundle.relations, byte-true to the TS readBundle eager path. ' +
      'Adds WRITE-path completeness vectors (#47 write side): createMany (homogeneous + heterogeneous ' +
      'column-set groups), updateMany, deleteMany, bare UPDATE/DELETE, DELETE RETURNING (per-dialect: ' +
      'MySQL emul returns []), tx idempotency/unique/edge/composite-DAG short-circuit + effects, and ' +
      'UUID-PK + composite-PK INSERT RETURNING (MySQL emul re-selects by the REAL PK). All ride the ' +
      'gate-free/gate-first TransactionPlan (kind:tx), byte-true to the SQLite reference.',
    vectors: [...exec, ...read, ...tx, ...txVariants, ...batch],
  };
}

/** Cross-check: the captured reference MUST equal the already-frozen SQLite exec/tx corpus (shared vectors). */
function crossCheckAgainstFrozen(corpus: ReturnType<typeof buildCorpus>): void {
  const exec = JSON.parse(readFileSync(join(SQLITE_VECTORS_DIR, 'exec.json'), 'utf8'));
  const tx = JSON.parse(readFileSync(join(SQLITE_VECTORS_DIR, 'tx.json'), 'utf8'));
  const frozenByName = new Map<string, EncodedValue>();
  const frozenDbByName = new Map<string, unknown>();
  for (const v of exec.vectors) frozenByName.set(v.name, v.expectedResult);
  for (const v of tx.vectors) {
    frozenByName.set(v.name, v.expectedResult);
    frozenDbByName.set(v.name, v.expectedDbState);
  }
  for (const v of corpus.vectors) {
    // Only the vectors SHARED with the frozen SQLite corpus are cross-checked byte-true; the added
    // #46 IN-list vectors have no frozen counterpart (they are captured from the SAME reference path).
    if (!frozenByName.has(v.name)) continue;
    const want = frozenByName.get(v.name);
    if (JSON.stringify(want) !== JSON.stringify(v.expectedResult)) {
      throw new Error(
        `live-DB corpus drift: '${v.name}' expectedResult != frozen ${v.kind}.json reference`,
      );
    }
    if (v.kind === 'tx') {
      if (JSON.stringify(frozenDbByName.get(v.name)) !== JSON.stringify(v.expectedDbState)) {
        throw new Error(`live-DB corpus drift: '${v.name}' expectedDbState != frozen tx.json reference`);
      }
    }
  }
}

export function writeLivedbCorpus(): string {
  const corpus = buildCorpus();
  crossCheckAgainstFrozen(corpus);
  if (!existsSync(LIVEDB_DIR)) mkdirSync(LIVEDB_DIR, { recursive: true });
  const file = join(LIVEDB_DIR, 'livedb.json');
  writeFileSync(file, JSON.stringify(corpus, null, 2) + '\n', 'utf8');
  return file;
}
