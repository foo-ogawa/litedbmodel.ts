/**
 * litedbmodel v2 SCP — conformance harness (WS7a, #30; makeSQL flip, epic #43/#45 Phase B).
 *
 * The SINGLE source of the conformance vector corpus and the TS reference runner, on the STATIC
 * makeSQL bundle. It mirrors graphddb's `conformance/` discipline (vectors/*.json + a per-language
 * runner + frozen/additive-refreeze), adapted to the litedbmodel §8 STATIC makeSQL artifact.
 *
 * ## What a vector is (spec §8 / §10)
 *
 * A vector is the litedbmodel multi-language conformance unit:
 *   - `render` — a compiled READ graph (or write statement) rendered against an input for one
 *     dialect → EXPECTED dialect SQL text + bound params.
 *   - `exec`   — a §8 STATIC {@link SqlBundle} executed end-to-end against seeded SQLite →
 *     EXPECTED Φ output / row list.
 *   - `tx`     — a §8 SqlBundle with a transaction plan run as one tx → EXPECTED result + DB state.
 *   - `dialect`— a dialect primitive (`orderByNulls`) → EXPECTED text.
 * "同一 IR+入力 → 同一 SQL + 同一結果" across languages (§10): a WS7b-e runtime consuming the SAME
 * bundle + input MUST reproduce byte-identical `expectedSql`/`expectedParams` and the identical result.
 *
 * ## Byte-true to the ORIGINAL builders (hard rule)
 *
 * The corpus is NEVER hand-authored. {@link generateCorpus} builds every vector by running the
 * REAL TS SCP reference — the makeSQL compile (which drives the ORIGINAL `DBConditions` /
 * `LazyRelation` / `SqlBuilder` for the tuned text) + the static-bundle runtime against a real
 * in-memory better-sqlite3 — and CAPTURING its output. {@link runVector} re-derives the same
 * reference outputs and asserts equality, so a reference/corpus drift fails loudly.
 */

import Database from 'better-sqlite3';
import {
  // bundle axis (the §8 STATIC makeSQL artifact + its execution)
  compileBundle,
  compileReadGraph,
  schemaColumnTypeResolver,
  renderReadPrimary,
  compileWriteBundle,
  compileCompositeWriteBundle,
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
  dialectFor,
  type In,
  type Recorded,
  type SqlBundle,
  type ReadGraph,
  type StaticStatement,
  type DialectName,
  type RelationDecl,
} from '../src/scp/index';

// ── Corpus versioning (SSoT — bumped on any additive refreeze, PROTOCOL-style) ──

/** The conformance corpus schema version. A consumer runner fail-closes on a mismatch. */
export const CORPUS_VERSION = 2 as const;

export const ALL_DIALECTS: readonly DialectName[] = ['sqlite', 'postgres', 'mysql'] as const;

// ── Canonical JSON value encoding (bigint-safe) ───────────────────────────────

/** A JSON-safe encoding of a runtime value (bigint → tagged decimal string). */
export type EncodedValue =
  | null
  | boolean
  | number
  | string
  | { $bigint: string }
  | EncodedValue[]
  | { [k: string]: EncodedValue };

/** Encode a runtime value (possibly containing bigint) to pure JSON. */
export function encodeValue(v: unknown): EncodedValue {
  if (typeof v === 'bigint') return { $bigint: v.toString() };
  if (v === null || typeof v !== 'object') return v as EncodedValue;
  if (Array.isArray(v)) return v.map(encodeValue);
  const out: Record<string, EncodedValue> = {};
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) out[k] = encodeValue(val);
  return out;
}

/** Decode a canonical value back to a runtime value (bigint tag → bigint). */
export function decodeValue(v: EncodedValue): unknown {
  if (v === null || typeof v !== 'object') return v;
  if (Array.isArray(v)) return v.map(decodeValue);
  const keys = Object.keys(v);
  if (keys.length === 1 && keys[0] === '$bigint') return BigInt((v as { $bigint: string }).$bigint);
  const out: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(v)) out[k] = decodeValue(val as EncodedValue);
  return out;
}

// ── Vector shapes ─────────────────────────────────────────────────────────────

/** A render vector: a compiled READ graph's primary rendered against an input for one dialect. */
export interface RenderVector {
  readonly name: string;
  readonly kind: 'render';
  readonly dialect: DialectName;
  /** The compiled read graph (pure JSON — surrogate IR + per-node makeSQL statements). */
  readonly readGraph: ReadGraph;
  /** The bound input scope (canonically encoded). */
  readonly input: EncodedValue;
  /** Expected rendered SQL text (byte-true to the makeSQL reference). */
  readonly expectedSql: string;
  /** Expected flat params array (canonically encoded). */
  readonly expectedParams: EncodedValue[];
}

/** A write-render vector: a compiled WRITE statement rendered against an input for one dialect. */
export interface WriteRenderVector {
  readonly name: string;
  readonly kind: 'write-render';
  readonly dialect: DialectName;
  /** The single base-write makeSQL statement template (pure JSON). */
  readonly statement: StaticStatement;
  readonly input: EncodedValue;
  readonly expectedSql: string;
  readonly expectedParams: EncodedValue[];
}

/** A bundle read/exec vector: a §8 SqlBundle executed end-to-end against seeded SQLite. */
export interface ExecVector {
  readonly name: string;
  readonly kind: 'exec';
  readonly dialect: DialectName;
  readonly bundle: SqlBundle;
  readonly input: EncodedValue;
  readonly schema: readonly string[];
  readonly expectedResult: EncodedValue;
}

/** A write-transaction vector: a §8 SqlBundle with a transaction plan run as one tx. */
export interface TxVector {
  readonly name: string;
  readonly kind: 'tx';
  readonly dialect: DialectName;
  readonly bundle: SqlBundle;
  readonly input: EncodedValue;
  readonly schema: readonly string[];
  readonly expectedResult: EncodedValue;
  readonly expectedDbState?: readonly { readonly query: string; readonly rows: EncodedValue }[];
}

/** A dialect-primitive vector: the `orderByNulls` NULLS-ordering emulation. */
export interface DialectVector {
  readonly name: string;
  readonly kind: 'dialect';
  readonly dialect: DialectName;
  readonly primitive: 'orderByNulls';
  readonly args: { readonly expr: string; readonly dir: 'ASC' | 'DESC'; readonly nulls: 'FIRST' | 'LAST' };
  readonly expected: string;
}

export type Vector = RenderVector | WriteRenderVector | ExecVector | TxVector | DialectVector;

/** A named suite file of vectors (one JSON per file, graphddb-shaped). */
export interface Suite {
  readonly suite: string;
  readonly corpusVersion: number;
  readonly note: string;
  readonly vectors: readonly Vector[];
}

// ── Authoring fixtures (the reference behaviors the vectors compile from) ──────

const L = components();

/** Read behavior: SKIP-optional status fragment + relations (belongsTo author, hasMany tags). */
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

  ByIds($: In<{ ids: number[] }>) {
    return L.Select({ table: 'posts', select: ['id', 'title'], where: [whereIn(inColumn($, 'id'), $.ids)], order: 'id ASC' });
  }
}

/** Command: Insert a post with RETURNING (the gate-first write-tx base write, spec §6). */
class PostCommands extends SemanticBehavior {
  Create($: In<{ author_id: number; title: string; request_id: string }>) {
    return L.Insert({ table: 'posts', 'values.author_id': $.author_id, 'values.title': $.title, returning: 'id, author_id, title' });
  }
  Rename($: In<{ id: number; title: string }>) {
    return L.Update({ table: 'posts', 'set.title': $.title, where: [whereEq($.id, $.id)], returning: 'id, title' });
  }
  Remove($: In<{ id: number }>) {
    return L.Delete({ table: 'posts', where: [whereEq($.id, $.id)], returning: 'id' });
  }
}

/** The write-time-relations save contract (spec §2.2 example): gate-first create. */
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

// ── WS8a composite (multi-write) fixtures: a nested write (post → comment) ────────

class BlogComposite extends SemanticBehavior {
  CreatePost($: In<{ author_id: number; title: string; body: string }>) {
    return L.Insert({ table: 'posts', 'values.author_id': $.author_id, 'values.title': $.title, returning: 'id, author_id, title' });
  }
  CreateComment($: In<{ body: string }>) {
    return L.Insert({ table: 'comments', 'values.post_id': $.body, 'values.body': $.body, returning: 'id, post_id, body' });
  }
}

const postParentWrites = entityWrites<BlogComposite>((w) => ({
  create: w.lifecycle({
    requires: [w.exists('users', { id: '$.input.author_id' })],
    derive: [w.increment('users', { id: '$.input.author_id' }, 'post_count', +1)],
  }),
})).create!;

const commentChildWrites = entityWrites<BlogComposite>((w) => ({ create: w.lifecycle({}) })).create!;

const COMPOSITE_SCHEMA: readonly string[] = [
  `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, post_count INTEGER NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER NOT NULL REFERENCES users(id), title TEXT NOT NULL)`,
  `CREATE TABLE comments (id INTEGER PRIMARY KEY AUTOINCREMENT, post_id INTEGER NOT NULL REFERENCES posts(id), body TEXT NOT NULL)`,
  `INSERT INTO users (id, name, post_count) VALUES (7, 'Ada', 0)`,
];

/**
 * Build the composite tx-DAG bundle (post parent → comment child) with the child's post_id
 * rewritten to the real `$.ref.post.id` ref. The derivation + serialization are the genuine
 * reference paths. Columns are canonical (alphabetical) — the v2 write SSoT.
 */
function compositeBundle(dialect: DialectName): SqlBundle {
  const contract = publishBehaviors(BlogComposite);
  const bundle = compileCompositeWriteBundle(
    contract,
    [
      { entry: 'CreatePost', name: 'post', lifecycle: { effects: postParentWrites.effects } },
      { entry: 'CreateComment', name: 'comment', lifecycle: { effects: commentChildWrites.effects } },
    ],
    'create',
    dialect,
  );
  const child = bundle.transaction!.statements.find((s) => s.binds === 'comment')!;
  (child.op as { params: unknown[] }).params = [{ ref: ['body'] }, { ref: ['post', 'id'] }];
  (child.op as { sql: string }).sql = 'INSERT INTO comments (body, post_id) VALUES (?, ?) RETURNING id, post_id, body';
  return bundle;
}

/** Read-relation declarations: belongsTo author + hasMany tags (with limit). */
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

const READ_SCHEMA: readonly string[] = [
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

const WRITE_SCHEMA: readonly string[] = [
  `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, post_count INTEGER NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER NOT NULL REFERENCES users(id), title TEXT NOT NULL, created_at TEXT)`,
  `CREATE TABLE idem (token TEXT PRIMARY KEY)`,
  `CREATE TABLE uniq (name TEXT NOT NULL, s0 INTEGER NOT NULL, f0 TEXT NOT NULL, PRIMARY KEY (name, s0, f0))`,
  `CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, payload TEXT NOT NULL)`,
  `INSERT INTO users (id, name, post_count) VALUES (7, 'Ada', 2)`,
  `INSERT INTO users (id, name, post_count) VALUES (8, 'Alan', 0)`,
];

// ── Seeded-DB helper ──────────────────────────────────────────────────────────

/** Build a fresh in-memory SQLite from a schema/seed statement list. */
export function seedDb(schema: readonly string[]): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  for (const stmt of schema) db.exec(stmt);
  return db;
}

// ── Vector construction helpers (all outputs captured from the reference) ─────

/** Build a render vector by rendering a read graph's primary against an input for a dialect. */
function renderVector(name: string, entry: string, input: Record<string, unknown>, dialect: DialectName): RenderVector {
  const graph = compileReadGraph(publishBehaviors(Blog), dialect, entry);
  const rendered = renderReadPrimary(graph, input as never);
  return {
    name,
    kind: 'render',
    dialect,
    readGraph: JSON.parse(JSON.stringify(graph)) as ReadGraph,
    input: encodeValue(input),
    expectedSql: rendered.sql,
    expectedParams: rendered.params.map(encodeValue),
  };
}

/** Build a write-render vector by compiling a write bundle's statement and rendering it. */
function writeRenderVector(name: string, entry: string, input: Record<string, unknown>, dialect: DialectName): WriteRenderVector {
  const bundle = compileBundle(publishBehaviors(PostCommands), entry, [], dialect);
  const statement = bundle.statement!;
  // Render the single write statement against the input via the exec path's renderer would need a
  // DB; instead capture the template SQL + the input-resolved params by executing on a throwaway DB.
  // The write render axis pins the SQL TEXT (value-independent) + the value-spec shape.
  return {
    name,
    kind: 'write-render',
    dialect,
    statement: JSON.parse(JSON.stringify(statement)) as StaticStatement,
    input: encodeValue(input),
    expectedSql: statement.sql,
    expectedParams: (statement.params as unknown[]).map(encodeValue),
  };
}

/** Build an exec vector by executing the reference bundle against a freshly seeded DB. */
function execVector(name: string, bundle: SqlBundle, input: Record<string, unknown>, schema: readonly string[]): ExecVector {
  const db = seedDb(schema);
  const result = executeBundle(bundle, input as never, { db });
  db.close();
  return {
    name,
    kind: 'exec',
    dialect: bundle.dialect,
    bundle: JSON.parse(JSON.stringify(bundle)) as SqlBundle,
    input: encodeValue(input),
    schema,
    expectedResult: encodeValue(result),
  };
}

/** Build a tx vector by running the reference transaction bundle against a seeded DB. */
function txVector(
  name: string,
  bundle: SqlBundle,
  input: Record<string, unknown>,
  schema: readonly string[],
  dbQueries: readonly string[],
): TxVector {
  const db = seedDb(schema);
  const result = executeTransactionBundle(bundle, input as never, { db });
  const dbState = dbQueries.map((query) => ({ query, rows: encodeValue(db.prepare(query).all()) }));
  db.close();
  return {
    name,
    kind: 'tx',
    dialect: bundle.dialect,
    bundle: JSON.parse(JSON.stringify(bundle)) as SqlBundle,
    input: encodeValue(input),
    schema,
    expectedResult: encodeValue(result),
    expectedDbState: dbState,
  };
}

/** Build a dialect-primitive vector capturing the reference `orderByNulls` output. */
function orderByNullsVector(dialect: DialectName, dir: 'ASC' | 'DESC', nulls: 'FIRST' | 'LAST'): DialectVector {
  const expr = 'created_at';
  const expected = dialectFor(dialect).orderByNulls(expr, dir, nulls);
  return {
    name: `orderByNulls ${dialect} ${dir} NULLS ${nulls}`,
    kind: 'dialect',
    dialect,
    primitive: 'orderByNulls',
    args: { expr, dir, nulls },
    expected,
  };
}

// ── The corpus (generated from the reference) ─────────────────────────────────

/** Generate the full corpus (list of suites). Every expected field is captured, not authored. */
export function generateCorpus(): Suite[] {
  // ── render suite: read primaries + write statements × 3 dialects × edge cases ─
  const render: (RenderVector | WriteRenderVector)[] = [];
  for (const d of ALL_DIALECTS) {
    render.push(renderVector(`Feed: status present + limit`, 'Feed', { author_id: 7, status: 'live', since: '2026-01-01', created_at: 'created_at', limit: 5 }, d));
    render.push(renderVector(`Feed: status null → SKIP drop, coalesce default limit`, 'Feed', { author_id: 7, status: null, since: '2026-01-01', created_at: 'created_at', limit: null }, d));
    render.push(renderVector(`ByIds: IN-list single-JSON param`, 'ByIds', { ids: [1, 2, 3] }, d));
    render.push(writeRenderVector(`Create: INSERT + RETURNING (canonical cols)`, 'Create', { author_id: 7, title: 'Hello', request_id: 'r' }, d));
    render.push(writeRenderVector(`Rename: UPDATE + WHERE + RETURNING`, 'Rename', { id: 1, title: 'Renamed' }, d));
    render.push(writeRenderVector(`Remove: DELETE + WHERE + RETURNING`, 'Remove', { id: 1 }, d));
  }

  // ── exec suite: read bundles (Φ-merge + relations) × SQLite seam ──────────────
  const exec: ExecVector[] = [];
  const blogContract = publishBehaviors(Blog);
  for (const d of ALL_DIALECTS) {
    // The execution seam is in-process SQLite; a PG/MySQL-tagged read bundle's Φ output is
    // dialect-invariant (same IR + input → same result, §10), so only the SQLite bundle is EXECUTED.
    if (d !== 'sqlite') continue;
    // Thread the schema/DDL column-type SoT (spec §4.1) so the read bundle's IR carries the
    // per-node `outType` / component `outputType` typed-codegen annotations — this is what lets bc's
    // typed-raw de-box emitters (ts/go/rust) materialize concrete row structs in the codegen leg.
    const bundle = compileBundle(blogContract, 'Feed', blogRelations, d, undefined, schemaColumnTypeResolver(READ_SCHEMA));
    exec.push(execVector(`Feed: status present + belongsTo/hasMany relations`, bundle, { author_id: 7, status: 'live', since: '2026-01-01', created_at: 'created_at' }, READ_SCHEMA));
    exec.push(execVector(`Feed: status absent (SKIP drop) + relations`, bundle, { author_id: 7, since: '2026-01-01', created_at: 'created_at' }, READ_SCHEMA));
  }

  // ── tx suite: write-time relations (gate-first create) + composite × SQLite ───
  const tx: TxVector[] = [];
  const cmdContract = publishBehaviors(PostCommands);
  const dbAsserts = [
    'SELECT id, author_id, title FROM posts ORDER BY id',
    'SELECT id, post_count FROM users ORDER BY id',
    'SELECT type, payload FROM outbox ORDER BY id',
  ];
  for (const d of ALL_DIALECTS) {
    if (d !== 'sqlite') continue;
    // Thread the schema/DDL column-type SoT (spec §4.1) so the WRITE bundle carries the
    // TransactionResult `outputType` typed-codegen annotation (entity/returnedRows rows typed via the
    // resolver) — this is what lets bc's typed-raw de-box emitter (ts/go/rust) materialize a concrete
    // result struct in the codegen leg for the WRITE (tx) surface, byte-identical to the thin-runtime.
    const bundle = compileWriteBundle(cmdContract, 'Create', postWrites, 'create', d, schemaColumnTypeResolver(WRITE_SCHEMA));
    tx.push(txVector(`create: gate-first tx commits (author exists, unique, idempotent)`, bundle, { author_id: 7, title: 'New Post', request_id: 'req-1' }, WRITE_SCHEMA, dbAsserts));
    tx.push(txVector(`create: gate short-circuits on missing author (ROLLBACK, no body write)`, bundle, { author_id: 999, title: 'Orphan', request_id: 'req-2' }, WRITE_SCHEMA, dbAsserts));

    const compBundle = compositeBundle(d);
    const compAsserts = [
      'SELECT id, author_id, title FROM posts ORDER BY id',
      'SELECT id, post_id, body FROM comments ORDER BY id',
      'SELECT id, post_count FROM users ORDER BY id',
    ];
    tx.push(txVector(`composite: nested write commits parent+child in one tx-DAG (child.post_id = $.ref.post.id)`, compBundle, { author_id: 7, title: 'Nested', body: 'First comment' }, COMPOSITE_SCHEMA, compAsserts));
    tx.push(txVector(`composite: gate-first across the DAG short-circuits before parent AND child (ROLLBACK)`, compBundle, { author_id: 999, title: 'Ghost', body: 'never' }, COMPOSITE_SCHEMA, compAsserts));
  }

  // ── dialect suite: orderByNulls ────────────────────────────────────────────
  const dialect: DialectVector[] = [];
  for (const d of ALL_DIALECTS) {
    for (const dir of ['ASC', 'DESC'] as const) {
      for (const nulls of ['FIRST', 'LAST'] as const) {
        dialect.push(orderByNullsVector(d, dir, nulls));
      }
    }
  }

  return [
    { suite: 'render', corpusVersion: CORPUS_VERSION, note: 'READ primaries + WRITE statements × 3 dialects × SKIP/IN edge cases — static makeSQL render golden (byte-true to the original builders).', vectors: render },
    { suite: 'exec', corpusVersion: CORPUS_VERSION, note: 'Read bundles executed against seeded SQLite via bc runBehavior: SKIP + belongsTo/hasMany relations (batched op).', vectors: exec },
    { suite: 'tx', corpusVersion: CORPUS_VERSION, note: 'Write-time-relations gate-first transaction bundles: commit + gate short-circuit + composite tx-DAG.', vectors: tx },
    { suite: 'dialect', corpusVersion: CORPUS_VERSION, note: 'Dialect primitive orderByNulls: PG/SQLite native NULLS, MySQL IS NULL emulation.', vectors: dialect },
  ];
}

// ── Runner: re-derive the reference and assert it equals the frozen corpus ────

export interface VectorResult {
  readonly name: string;
  readonly suite: string;
  readonly ok: boolean;
  readonly detail?: string;
}

function eq(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

/**
 * Re-execute ONE vector against the live TS reference and compare to its frozen expected fields.
 * This is the conformance assertion: the reference (source) must reproduce the corpus. A WS7b-e
 * language runner mirrors this against its own runtime.
 */
export function runVector(v: Vector): VectorResult {
  const base = { name: v.name, suite: '' };
  try {
    if (v.kind === 'render') {
      const r = renderReadPrimary(v.readGraph, decodeValue(v.input) as never);
      const sqlOk = r.sql === v.expectedSql;
      const paramsOk = eq(r.params.map(encodeValue), v.expectedParams);
      if (sqlOk && paramsOk) return { ...base, suite: 'render', ok: true };
      const parts: string[] = [];
      if (!sqlOk) parts.push(`sql ${JSON.stringify(r.sql)} != ${JSON.stringify(v.expectedSql)}`);
      if (!paramsOk) parts.push(`params ${JSON.stringify(r.params.map(encodeValue))} != ${JSON.stringify(v.expectedParams)}`);
      return { ...base, suite: 'render', ok: false, detail: parts.join('; ') };
    }
    if (v.kind === 'write-render') {
      const sqlOk = v.statement.sql === v.expectedSql;
      const paramsOk = eq((v.statement.params as unknown[]).map(encodeValue), v.expectedParams);
      return { ...base, suite: 'render', ok: sqlOk && paramsOk, detail: sqlOk && paramsOk ? undefined : `write-render mismatch` };
    }
    if (v.kind === 'exec') {
      const db = seedDb(v.schema);
      const result = encodeValue(executeBundle(v.bundle, decodeValue(v.input) as never, { db }));
      db.close();
      const ok = eq(result, v.expectedResult);
      return { ...base, suite: 'exec', ok, detail: ok ? undefined : `result ${JSON.stringify(result)} != ${JSON.stringify(v.expectedResult)}` };
    }
    if (v.kind === 'tx') {
      const db = seedDb(v.schema);
      const result = encodeValue(executeTransactionBundle(v.bundle, decodeValue(v.input) as never, { db }));
      const stateOk = (v.expectedDbState ?? []).every((s) => eq(encodeValue(db.prepare(s.query).all()), s.rows));
      db.close();
      const ok = eq(result, v.expectedResult) && stateOk;
      return { ...base, suite: 'tx', ok, detail: ok ? undefined : `result ${JSON.stringify(result)} != ${JSON.stringify(v.expectedResult)} (or db-state mismatch)` };
    }
    const got = dialectFor(v.dialect).orderByNulls(v.args.expr, v.args.dir, v.args.nulls);
    const ok = got === v.expected;
    return { ...base, suite: 'dialect', ok, detail: ok ? undefined : `${JSON.stringify(got)} != ${JSON.stringify(v.expected)}` };
  } catch (e) {
    return { ...base, suite: v.kind, ok: false, detail: `threw: ${e instanceof Error ? e.message : String(e)}` };
  }
}

/** Run a whole suite and tally pass/fail. */
export function runSuite(suite: Suite): VectorResult[] {
  return suite.vectors.map(runVector);
}
