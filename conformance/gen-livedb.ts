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
  executeBundle,
  executeTransactionBundle,
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

// ── The SQLite reference schema (from harness.ts) — the reference execution seam ──

const READ_SCHEMA_SQLITE: readonly string[] = [
  `CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, created_at TEXT NOT NULL)`,
  `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`,
  `CREATE TABLE tags (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, label TEXT)`,
  `INSERT INTO posts VALUES (1, 7, 'Hello', 'live', '2026-02-01')`,
  `INSERT INTO posts VALUES (2, 7, 'World', 'draft', '2026-03-01')`,
  `INSERT INTO posts VALUES (3, 8, 'Other', 'live', '2026-01-15')`,
  `INSERT INTO users VALUES (7, 'Ada')`,
  `INSERT INTO users VALUES (8, 'Alan')`,
  `INSERT INTO tags VALUES (10, 1, 'greeting')`,
  `INSERT INTO tags VALUES (11, 1, 'first')`,
  `INSERT INTO tags VALUES (12, 2, 'world')`,
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
  `INSERT INTO posts VALUES (1, 7, 'Hello', 'live', '2026-02-01')`,
  `INSERT INTO posts VALUES (2, 7, 'World', 'draft', '2026-03-01')`,
  `INSERT INTO posts VALUES (3, 8, 'Other', 'live', '2026-01-15')`,
  `INSERT INTO users VALUES (7, 'Ada')`,
  `INSERT INTO users VALUES (8, 'Alan')`,
  `INSERT INTO tags VALUES (10, 1, 'greeting')`,
  `INSERT INTO tags VALUES (11, 1, 'first')`,
  `INSERT INTO tags VALUES (12, 2, 'world')`,
];

const READ_SCHEMA_MYSQL: readonly string[] = [
  `CREATE TABLE posts (id INT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, status VARCHAR(255), created_at VARCHAR(255) NOT NULL)`,
  `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))`,
  `CREATE TABLE tags (id INT PRIMARY KEY, post_id INT NOT NULL, label VARCHAR(255))`,
  `INSERT INTO posts VALUES (1, 7, 'Hello', 'live', '2026-02-01')`,
  `INSERT INTO posts VALUES (2, 7, 'World', 'draft', '2026-03-01')`,
  `INSERT INTO posts VALUES (3, 8, 'Other', 'live', '2026-01-15')`,
  `INSERT INTO users VALUES (7, 'Ada')`,
  `INSERT INTO users VALUES (8, 'Alan')`,
  `INSERT INTO tags VALUES (10, 1, 'greeting')`,
  `INSERT INTO tags VALUES (11, 1, 'first')`,
  `INSERT INTO tags VALUES (12, 2, 'world')`,
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
  expectedDbState: { query: string; rows: EncodedValue }[];
}

type LiveVector = LiveExecVector | LiveTxVector;

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

  return {
    suite: 'livedb',
    corpusVersion: LIVEDB_CORPUS_VERSION,
    note:
      'Live-DB corpus (v2 makeSQL flip): exec/tx STATIC bundles (readGraph + statementsById / ' +
      'transaction plan) compiled for postgres + mysql, executed against REAL dockerized PG + MySQL ' +
      'by each language runtime; expectedResult/expectedDbState are the byte-true SQLite reference ' +
      '(dialect-invariant §10 promise). Includes #46 IN-list cases (int / empty / uuid) + relation ' +
      'batches so every language binds the no-cast `= ANY($1)` + `= ANY($1::int[])` on live PG.',
    vectors: [...exec, ...tx],
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
