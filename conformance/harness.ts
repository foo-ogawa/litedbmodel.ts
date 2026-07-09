/**
 * litedbmodel v2 SCP — conformance harness (WS7a, #30).
 *
 * The SINGLE source of the conformance vector corpus and the TS reference runner. It mirrors
 * graphddb's `conformance/` discipline (vectors/*.json + a per-language runner + an
 * orchestrator + frozen/additive-refreeze), adapted to the litedbmodel SCP §8 artifact.
 *
 * ## What a vector is (spec §8 / §10)
 *
 * A vector is the litedbmodel multi-language conformance unit: a §8 pure-JSON `SqlBundle`
 * (or a bare {@link CompiledOperation} for pure render vectors), an `input` scope, a target
 * `dialect`, and the EXPECTED render output (`expectedSql` + `expectedParams`) and/or the
 * EXPECTED execution result (`expectedResult`, the rows/state after running against a seeded
 * SQLite). "同一 IR+入力 → 同一 SQL + 同一結果" across languages (§10): a WS7b-e runtime that
 * consumes the SAME bundle + input MUST reproduce byte-identical `expectedSql`/`expectedParams`
 * and the identical `expectedResult`.
 *
 * ## Byte-true to the reference (hard rule)
 *
 * The corpus is NEVER hand-authored. {@link generateCorpus} builds every vector by running the
 * REAL TS SCP reference — `compileSelect`/`compileInsertFor`/… + `renderOperation` for the
 * render axis, and `compileBundle`/`compileWriteBundle` + `executeBundle`/
 * `executeTransactionBundle` against a real in-memory better-sqlite3 for the execution axis —
 * and CAPTURING its output. The expected fields are therefore, by construction, byte-identical
 * to the reference. {@link runCorpus} re-derives the same reference outputs and asserts they
 * equal the frozen corpus, so a reference drift (or a corrupt corpus) fails loudly.
 *
 * ## Canonical value encoding
 *
 * bc evaluates integers to `bigint`, which JSON cannot represent. Rendered param slots and any
 * captured value therefore pass through {@link encodeValue} (`bigint` → `{ "$bigint": "<dec>" }`,
 * everything else structural JSON) so the corpus is pure JSON and round-trips losslessly.
 */

import Database from 'better-sqlite3';
import {
  // render axis (the normative dynamic-expansion reference)
  renderOperation,
  dialectFor,
  compileSelect,
  compileUpdate,
  compileDelete,
  compileInsertFor,
  SQLITE,
  POSTGRES,
  MYSQL,
  // bundle axis (the §8 published artifact + its execution)
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
  type CompiledOperation,
  type DialectName,
  type RelationDecl,
} from '../src/scp/index';

// ── Corpus versioning (SSoT — bumped on any additive refreeze, PROTOCOL-style) ──

/** The conformance corpus schema version. A consumer runner fail-closes on a mismatch. */
export const CORPUS_VERSION = 1 as const;

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

// ── Vector shapes ─────────────────────────────────────────────────────────────

/** A render-only vector: a §8 CompiledOperation rendered against an input for one dialect. */
export interface RenderVector {
  readonly name: string;
  readonly kind: 'render';
  readonly dialect: DialectName;
  /** The §8 compiled operation (pure JSON). */
  readonly operation: CompiledOperation;
  /** The bound input scope (canonically encoded). */
  readonly input: EncodedValue;
  /** Expected rendered SQL text (byte-true to `renderOperation`). */
  readonly expectedSql: string;
  /** Expected flat params array (canonically encoded, 1:1 with `?`/`$N`). */
  readonly expectedParams: EncodedValue[];
}

/** A bundle read/exec vector: a §8 SqlBundle executed end-to-end against seeded SQLite. */
export interface ExecVector {
  readonly name: string;
  readonly kind: 'exec';
  readonly dialect: DialectName;
  /** The §8 published bundle (pure JSON). */
  readonly bundle: SqlBundle;
  /** The bound input scope (canonically encoded). */
  readonly input: EncodedValue;
  /** DDL + seed statements applied to a fresh in-memory SQLite before executing. */
  readonly schema: readonly string[];
  /** Expected behavior output (Φ merge / row list), canonically encoded. */
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
  /** Expected {@link TransactionResult} (committed / shortCircuit / entity / executed), encoded. */
  readonly expectedResult: EncodedValue;
  /** Optional post-tx DB assertions: `{ query, expectRows }` (rows canonically encoded). */
  readonly expectedDbState?: readonly { readonly query: string; readonly rows: EncodedValue }[];
}

/** A dialect-primitive vector: e.g. the `orderByNulls` NULLS-ordering emulation (WS6-flagged). */
export interface DialectVector {
  readonly name: string;
  readonly kind: 'dialect';
  readonly dialect: DialectName;
  /** The dialect primitive under test. */
  readonly primitive: 'orderByNulls';
  readonly args: { readonly expr: string; readonly dir: 'ASC' | 'DESC'; readonly nulls: 'FIRST' | 'LAST' };
  readonly expected: string;
}

export type Vector = RenderVector | ExecVector | TxVector | DialectVector;

/** A named suite file of vectors (one JSON per file, graphddb-shaped). */
export interface Suite {
  readonly suite: string;
  readonly corpusVersion: number;
  readonly note: string;
  readonly vectors: readonly Vector[];
}

// ── Authoring fixtures (the reference behaviors the exec/tx vectors compile from) ──

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
}

/** Command: Insert a post with RETURNING (the gate-first write-tx base write, spec §6). */
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

/** Read-schema DDL + seed (posts/users/tags), shared by the read/exec vectors. */
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

/** Write-schema DDL + seed (the WS5 write vertical-slice tables). */
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

function dialectOf(name: DialectName) {
  switch (name) {
    case 'sqlite': return SQLITE;
    case 'postgres': return POSTGRES;
    case 'mysql': return MYSQL;
  }
}

/** Build a render vector by running `renderOperation` on the reference-compiled op. */
function renderVector(name: string, op: CompiledOperation, input: Record<string, unknown>, dialect: DialectName): RenderVector {
  const rendered = renderOperation(op, input as never, dialectOf(dialect));
  return {
    name,
    kind: 'render',
    dialect,
    operation: JSON.parse(JSON.stringify(op)) as CompiledOperation,
    input: encodeValue(input),
    expectedSql: rendered.sql,
    expectedParams: rendered.params.map(encodeValue),
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
  const expected = dialectOf(dialect).orderByNulls(expr, dir, nulls);
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

/** Reference-compiled ops for the render axis (CRUD × edge cases), compiled ONCE. */
function crudOps() {
  const inref = (name: string) => ({ ref: [name] });
  return {
    select: compileSelect({
      table: 'posts',
      select: ['id', 'author_id', 'title'],
      where: [
        { kind: 'eq', column: 'author_id', value: inref('authorId') },
        { kind: 'in', column: 'id', value: inref('ids') },
      ],
      order: 'id ASC',
    }),
    // SKIP-optional fragment (status) + coalesce LIMIT — the present/null/absent edge axis.
    selectSkip: compileSelect({
      table: 'posts',
      select: ['id', 'status'],
      where: [
        { kind: 'eq', column: 'author_id', value: inref('authorId') },
        { kind: 'eq', column: 'status', value: inref('status'), skipWhen: { ne: [{ refOpt: ['status'] }, null] } },
      ],
      limit: { coalesce: [{ refOpt: ['limit'] }, 20] },
    }),
    // WHERE with only a SKIP fragment → empty-WHERE degeneration when absent.
    selectEmptyWhere: compileSelect({
      table: 'posts',
      select: ['id'],
      where: [{ kind: 'eq', column: 'status', value: inref('status'), skipWhen: { ne: [{ refOpt: ['status'] }, null] } }],
    }),
    // IN-list only (N / empty degeneration to `1 = 0`).
    selectIn: compileSelect({ table: 'posts', select: ['id'], where: [{ kind: 'in', column: 'id', value: inref('ids') }] }),
    insert: compileInsertFor(SQLITE, {
      table: 'posts',
      values: { author_id: inref('authorId'), title: inref('title') },
      returning: ['id', 'title'],
    }),
    update: compileUpdate({
      table: 'users',
      set: { post_count: { add: [{ ref: ['cur'] }, 1] } },
      where: [{ kind: 'eq', column: 'id', value: inref('id') }],
      returning: ['id', 'post_count'],
    }),
    delete: compileDelete({
      table: 'posts',
      where: [
        { kind: 'eq', column: 'author_id', value: inref('authorId') },
        { kind: 'in', column: 'id', value: inref('ids') },
      ],
      returning: ['id'],
    }),
  };
}

/** Generate the full corpus (list of suites). Every expected field is captured, not authored. */
export function generateCorpus(): Suite[] {
  const ops = crudOps();

  // ── render suite: CRUD × 3 dialects × edge cases ──────────────────────────
  const render: RenderVector[] = [];
  for (const d of ALL_DIALECTS) {
    render.push(renderVector(`select eq+IN`, ops.select, { authorId: 7, ids: [1, 2, 3] }, d));
    render.push(renderVector(`insert +RETURNING`, ops.insert, { authorId: 7, title: 'Hello' }, d));
    render.push(renderVector(`update SET+WHERE`, ops.update, { cur: 4n, id: 7 }, d));
    render.push(renderVector(`delete eq+IN`, ops.delete, { authorId: 7, ids: [1, 2] }, d));
    // Edge: SKIP present / null (the two raw-render inputs). The genuine ABSENT-key case is a
    // runtime/bundle concern — the runtime normalizes an absent OPTIONAL head to present-as-null
    // (from the bundle's optionalHeads) BEFORE rendering, so at the render boundary "absent" is
    // indistinguishable from `null`. Absent-key normalization is covered on the exec axis (the
    // Feed-status-omitted vector), which exercises normalizeInput end-to-end.
    render.push(renderVector(`skip present (status='live', limit=5)`, ops.selectSkip, { authorId: 7, status: 'live', limit: 5 }, d));
    render.push(renderVector(`skip null (status=null → drop, coalesce default limit)`, ops.selectSkip, { authorId: 7, status: null, limit: null }, d));
    // Edge: empty-WHERE degeneration (the sole fragment is SKIP-dropped → no ` WHERE ` at all).
    // `status: null` is the runtime-normalized form of an absent optional head (see note above).
    render.push(renderVector(`empty-WHERE degeneration (status=null → whole WHERE collapses)`, ops.selectEmptyWhere, { status: null }, d));
    render.push(renderVector(`empty-WHERE present (status='live')`, ops.selectEmptyWhere, { status: 'live' }, d));
    // Edge: IN-list N and empty.
    render.push(renderVector(`IN-list N=3`, ops.selectIn, { ids: [1, 2, 3] }, d));
    render.push(renderVector(`IN-list empty → 1 = 0`, ops.selectIn, { ids: [] }, d));
  }

  // ── exec suite: read bundles (relations) × 3 dialects ─────────────────────
  const exec: ExecVector[] = [];
  const blogContract = publishBehaviors(Blog);
  for (const d of ALL_DIALECTS) {
    const bundle = compileBundle(blogContract, 'Feed', blogRelations, d);
    // The execution seam is in-process SQLite regardless of the tagged dialect; the bundle's
    // dialect axis governs the rendered SQL text (PG `$N`), while the seeded DB is SQLite.
    // For a PG/MySQL-tagged bundle the rendered `$N`/`?` still binds positionally on SQLite,
    // so the EXECUTED result is dialect-invariant — this is exactly the §10 promise (same IR +
    // input → same result across dialects/languages). We therefore only EXECUTE the SQLite
    // bundle (the runnable seam) and keep PG/MySQL as render-text vectors above.
    if (d !== 'sqlite') continue;
    exec.push(execVector(`Feed: status present + belongsTo/hasMany relations`, bundle, { author_id: 7, status: 'live', since: '2026-01-01' }, READ_SCHEMA));
    exec.push(execVector(`Feed: status absent (SKIP drop) + relations`, bundle, { author_id: 7, since: '2026-01-01' }, READ_SCHEMA));
    exec.push(execVector(`Feed: hasMany limit=2 caps children`, bundle, { author_id: 7, since: '2026-01-01', status: 'live' }, READ_SCHEMA));
  }

  // ── tx suite: write-time relations (gate-first create) × 3 dialects ───────
  const tx: TxVector[] = [];
  const cmdContract = publishBehaviors(PostCommands);
  const dbAsserts = [
    'SELECT id, author_id, title FROM posts ORDER BY id',
    'SELECT id, post_count FROM users ORDER BY id',
    'SELECT type, payload FROM outbox ORDER BY id',
  ];
  for (const d of ALL_DIALECTS) {
    if (d !== 'sqlite') continue; // executed against the SQLite seam (dialect text covered by render)
    const bundle = compileWriteBundle(cmdContract, 'Create', postWrites, 'create', d);
    tx.push(
      txVector(
        `create: gate-first tx commits (author exists, unique, idempotent)`,
        bundle,
        { author_id: 7, title: 'New Post', request_id: 'req-1' },
        WRITE_SCHEMA,
        dbAsserts,
      ),
    );
    // Gate short-circuit: author 999 does not exist → requires gate fails, tx rolls back.
    tx.push(
      txVector(
        `create: gate short-circuits on missing author (ROLLBACK, no body write)`,
        bundle,
        { author_id: 999, title: 'Orphan', request_id: 'req-2' },
        WRITE_SCHEMA,
        dbAsserts,
      ),
    );
  }

  // ── dialect suite: orderByNulls (WS6-flagged: had NO test) ─────────────────
  const dialect: DialectVector[] = [];
  for (const d of ALL_DIALECTS) {
    for (const dir of ['ASC', 'DESC'] as const) {
      for (const nulls of ['FIRST', 'LAST'] as const) {
        dialect.push(orderByNullsVector(d, dir, nulls));
      }
    }
  }

  return [
    { suite: 'render', corpusVersion: CORPUS_VERSION, note: 'CRUD × 3 dialects × SKIP/IN/empty-WHERE edge cases — renderOperation golden (byte-true).', vectors: render },
    { suite: 'exec', corpusVersion: CORPUS_VERSION, note: 'Read bundles executed against seeded SQLite: SKIP + belongsTo/hasMany relations, hasMany limit.', vectors: exec },
    { suite: 'tx', corpusVersion: CORPUS_VERSION, note: 'Write-time-relations gate-first transaction bundles: commit + gate short-circuit.', vectors: tx },
    { suite: 'dialect', corpusVersion: CORPUS_VERSION, note: 'Dialect primitive orderByNulls (WS6-flagged untested): PG/SQLite native NULLS, MySQL IS NULL emulation.', vectors: dialect },
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
 * Re-execute ONE vector against the live TS reference and compare to its frozen expected
 * fields. This is the conformance assertion: the reference (source) must reproduce the corpus
 * byte-for-byte. A WS7b-e language runner mirrors this against its own runtime.
 */
export function runVector(v: Vector): VectorResult {
  const base = { name: v.name, suite: '' };
  try {
    if (v.kind === 'render') {
      const r = renderOperation(v.operation, decodeValue(v.input) as never, dialectOf(v.dialect));
      const sqlOk = r.sql === v.expectedSql;
      const paramsOk = eq(r.params.map(encodeValue), v.expectedParams);
      if (sqlOk && paramsOk) return { ...base, suite: 'render', ok: true };
      const parts: string[] = [];
      if (!sqlOk) parts.push(`sql ${JSON.stringify(r.sql)} != ${JSON.stringify(v.expectedSql)}`);
      if (!paramsOk) parts.push(`params ${JSON.stringify(r.params.map(encodeValue))} != ${JSON.stringify(v.expectedParams)}`);
      return { ...base, suite: 'render', ok: false, detail: parts.join('; ') };
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
    // dialect
    const got = dialectOf(v.dialect).orderByNulls(v.args.expr, v.args.dir, v.args.nulls);
    const ok = got === v.expected;
    return { ...base, suite: 'dialect', ok, detail: ok ? undefined : `${JSON.stringify(got)} != ${JSON.stringify(v.expected)}` };
  } catch (e) {
    return { ...base, suite: v.kind, ok: false, detail: `threw: ${e instanceof Error ? e.message : String(e)}` };
  }
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

/** Run a whole suite and tally pass/fail. */
export function runSuite(suite: Suite): VectorResult[] {
  return suite.vectors.map(runVector);
}
