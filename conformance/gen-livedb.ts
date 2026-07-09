/**
 * Generate the LIVE-DB conformance corpus for WS7g (#36) — the coordinated cross-language
 * live-PostgreSQL/MySQL validation pass.
 *
 * The frozen `conformance/vectors/{exec,tx}.json` corpus executes the SQLite-tagged bundles
 * against an in-process SQLite (the sanctioned in-proc conformance seam; §10). WS7g runs the
 * SAME behaviors' bundles compiled for the `postgres` + `mysql` dialects against REAL dockerized
 * Postgres + MySQL, in every language runtime, and asserts the assembled result equals the
 * SAME reference — i.e. the SQLite-captured `expectedResult` / `expectedDbState` (the §10 promise:
 * same IR + input → same RESULT regardless of dialect).
 *
 * This script builds one `conformance/vectors-livedb/livedb.json` from the REAL TS reference,
 * mirroring `harness.ts`'s exec/tx fixtures EXACTLY. For each exec/tx vector it captures:
 *   - `bundlePg`  / `bundleMysql` — `compileBundle`/`compileWriteBundle` for the pg / mysql dialect
 *     (the `operations[].sql` therefore carries the dialect-specific text — `$N` for PG).
 *   - `schemaPg`  / `schemaMysql` — PG / MySQL DDL + seed matching the SQLite READ/WRITE schema.
 *   - `expectedResult` / `expectedDbState` — the reference outcome, captured by EXECUTING the
 *     SQLite bundle on in-memory better-sqlite3 (byte-true to `harness.ts`), NOT hand-authored.
 *
 * BYTE-TRUE CROSS-CHECK (hard rule): the captured `expectedResult`/`expectedDbState` are asserted
 * IDENTICAL to the already-frozen `conformance/vectors/{exec,tx}.json` reference outputs, so this
 * live-DB corpus is provably the same reference the SQLite conformance already locks — a language
 * runtime that reproduces it on live PG/MySQL is genuinely conformant, not fudged.
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

export const LIVEDB_CORPUS_VERSION = 1 as const;

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

// The SQLite schema (from harness.ts) — the reference execution seam.
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
// Column TYPES are the natural per-dialect equivalent (INTEGER→INT/SERIAL, TEXT→TEXT/VARCHAR); the
// values seeded and read are dialect-invariant scalars (ints, ascii text), so the assembled result
// is identical to the SQLite reference. Each language uses its OWN namespaced schema (a distinct
// PG schema / MySQL database), so these DDLs are applied inside that namespace at run time.
//
// The write schema's `posts.id` is a fresh identity column (SERIAL / AUTO_INCREMENT) so the first
// insert's RETURNING/insertId is 1 — matching the SQLite AUTOINCREMENT reference (`entity.id: 1`).
// The runner TRUNCATEs + reseeds before each tx vector so the identity restarts at 1.

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

// MySQL: TEXT columns cannot have inline DEFAULT and `status` needs to be nullable TEXT; identical
// data. INTEGER is INT. (backtick-free — the rendered SQL uses bare identifiers.)
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
function execReference(bundle: SqlBundle, input: Record<string, unknown>): EncodedValue {
  const db = seedDb(READ_SCHEMA_SQLITE);
  const result = executeBundle(bundle, input as never, { db });
  db.close();
  return encodeValue(result);
}

function txReference(
  bundle: SqlBundle,
  input: Record<string, unknown>,
  dbQueries: readonly string[],
): { result: EncodedValue; dbState: { query: string; rows: EncodedValue }[] } {
  const db = seedDb(WRITE_SCHEMA_SQLITE);
  const result = executeTransactionBundle(bundle, input as never, { db });
  const dbState = dbQueries.map((query) => ({ query, rows: encodeValue(db.prepare(query).all()) }));
  db.close();
  return { result: encodeValue(result), dbState };
}

function buildCorpus(): { suite: string; corpusVersion: number; note: string; vectors: LiveVector[] } {
  const blog = publishBehaviors(Blog);
  const cmd = publishBehaviors(PostCommands);
  const compileExec = (input: Record<string, unknown>) => ({
    pg: compileBundle(blog, 'Feed', blogRelations, 'postgres'),
    mysql: compileBundle(blog, 'Feed', blogRelations, 'mysql'),
    sqlite: compileBundle(blog, 'Feed', blogRelations, 'sqlite'),
    input,
  });

  const execInputs: { name: string; input: Record<string, unknown> }[] = [
    { name: 'Feed: status present + belongsTo/hasMany relations', input: { author_id: 7, status: 'live', since: '2026-01-01' } },
    { name: 'Feed: status absent (SKIP drop) + relations', input: { author_id: 7, since: '2026-01-01' } },
    { name: 'Feed: hasMany limit=2 caps children', input: { author_id: 7, since: '2026-01-01', status: 'live' } },
  ];

  const exec: LiveExecVector[] = execInputs.map(({ name, input }) => {
    const c = compileExec(input);
    return {
      name,
      kind: 'exec',
      input: encodeValue(input),
      bundlePg: clone(c.pg),
      bundleMysql: clone(c.mysql),
      schemaPg: READ_SCHEMA_PG,
      schemaMysql: READ_SCHEMA_MYSQL,
      expectedResult: execReference(c.sqlite, input),
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
    const ref = txReference(sqlite, input, dbAsserts);
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
      'WS7g (#36) live-DB corpus: exec/tx bundles compiled for postgres + mysql, executed against ' +
      'REAL dockerized PG + MySQL by each language runtime; expectedResult/expectedDbState are the ' +
      'byte-true SQLite reference (dialect-invariant §10 promise).',
    vectors: [...exec, ...tx],
  };
}

/** Cross-check: the captured reference MUST equal the already-frozen SQLite exec/tx corpus. */
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
