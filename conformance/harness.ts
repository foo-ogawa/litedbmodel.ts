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
  // #143 leaf axis: reads run the op-independent leaf graph (`executeBehavior`/`read`); the WRITE-tx
  // bundle (`compileWriteBundle`) is the ONLY surviving §8 SqlBundle. The retired read-bundle surface
  // (`compileBundle`/`compileReadGraph`/`renderReadPrimary`/`executeBundle`/`readBundle`) is gone.
  emitRead,
  emitWrite,
  executeBehavior,
  read,
  compileWriteBundle,
  compileCompositeWriteBundle,
  executeTransactionBundle,
  LimitExceededError,
  setLimitConfig,
  resetLimitConfig,
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
  assembleDynamicWhere,
  prepareSql,
  type In,
  type Recorded,
  type SqlBundle,
  type BehaviorModelContract,
  type DynamicWhereFrag,
  type DialectName,
  type RelationDecl,
} from '../src/scp/index';
import { evaluateExpression } from 'behavior-contracts';

// ── Corpus versioning (SSoT — bumped on any additive refreeze, PROTOCOL-style) ──

/** The conformance corpus schema version. A consumer runner fail-closes on a mismatch. Bumped to 4 for
 * the #143 leaf-path schema: reads re-derive from the fixture `entry` + `config` (no serialized
 * `readGraph`/`bundle` artifact); the WRITE tx vector keeps its `bundle`. */
export const CORPUS_VERSION = 4 as const;

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

// ── Leaf-path render helper (#143 — the SSoT read-leaf → {sql, params} renderer) ──

/**
 * Render a published contract's PRIMARY read leaf to `{sql, params}` — the leaf-path replacement for the
 * retired `compileReadGraph`→`renderReadPrimary`. The WHERE + LIMIT are lowered into the read
 * `executeSQL` node's static `sql` at publish (via the SAME `lowerWherePort`/emitRead builders); render
 * `?`→`$N` for the dialect and evaluate each deferred value-spec param against the input (bc
 * `evaluateExpression`), normalizing a bc `int` BigInt back to a JS number at the driver boundary.
 * SHARED by `test/scp/makesql-golden.test.ts` (one renderer, no duplication).
 */
export function renderPrimaryRead(contract: BehaviorModelContract, entry: string, input: Record<string, unknown>, dialect: DialectName): { sql: string; params: unknown[] } {
  const comp = contract.methods[entry].component;
  const node = comp.body.find(
    (n) => !('cond' in n) && !('map' in n) && (n as { component?: string }).component === 'executeSQL' && (n as { ports?: { write?: unknown } }).ports?.write !== true,
  );
  if (node === undefined) throw new Error(`harness: no read leaf for '${entry}'`);
  const ports = (node as { ports: { sql?: unknown; params?: { arr?: unknown[] }; whereDynamic?: unknown } }).ports;
  const scope = input as Parameters<typeof evaluateExpression>[1];
  let sql = String(ports.sql);
  let params = (ports.params?.arr ?? []).map((spec) => normalizeParam(evaluateExpression(spec, scope)));
  // A SKIP/dynamic WHERE rides the `whereDynamic` plan (not the static sql) — evaluate it per-input and
  // assemble the surviving fragments through the SAME leaf assembler the runtime uses ({@link assembleDynamicWhere}).
  if (ports.whereDynamic != null) {
    const evaluated = evaluateExpression(ports.whereDynamic, scope) as { frags?: (DynamicWhereFrag | null)[] };
    const asm = assembleDynamicWhere({ sql, params, whereDynamic: evaluated });
    sql = asm.sql;
    params = asm.params.map(normalizeParam);
  }
  // Render exactly as the `executeSQL` transport does (`?`→`$N`, PG array cast, param encode) — the render
  // golden IS the driver-bound form a per-language runtime reproduces (SSoT {@link prepareSql}).
  const prepared = prepareSql({ sql, params, write: false }, dialect as Parameters<typeof prepareSql>[1]);
  return { sql: prepared.sql, params: prepared.bound };
}

/** Normalize an evaluated param at the driver boundary: a safe-range bc-int BigInt → JS number, recursing arrays. */
export function normalizeParam(v: unknown): unknown {
  if (typeof v === 'bigint') return v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v;
  if (Array.isArray(v)) return v.map(normalizeParam);
  return v;
}

/** The single base-write `executeSQL` leaf's `{sql, params}` template of a WRITE contract method. */
export function writeLeafOf(contract: BehaviorModelContract, entry: string): { sql: string; params: unknown[] } {
  const node = contract.methods[entry].component.body.find(
    (n) => !('cond' in n) && !('map' in n) && (n as { component?: string }).component === 'executeSQL' && (n as { ports?: { write?: unknown } }).ports?.write === true,
  );
  if (node === undefined) throw new Error(`harness: no write leaf for '${entry}'`);
  const ports = (node as { ports: { sql?: unknown; params?: { arr?: unknown[] } } }).ports;
  return { sql: String(ports.sql), params: (ports.params?.arr ?? []) as unknown[] };
}

// ── Vector shapes ─────────────────────────────────────────────────────────────

/** The hard-limit config a guard vector re-applies before re-publishing (the cap bakes at publish on the
 * leaf path — `lowerFindGuard` / `compileRelationOp` — so the vector CARRIES it, replacing the retired
 * bundle-baked cap). Absent ⇒ no config (a plain exec vector). */
export interface LimitConfigSpec {
  readonly findHardLimit?: number | null;
  readonly hasManyHardLimit?: number | null;
}

/** A render vector: the PRIMARY read leaf's lowered sql rendered against an input for one dialect (#143 —
 * re-derived from the `Blog` fixture's `entry` method; the retired `readGraph` artifact is gone). */
export interface RenderVector {
  readonly name: string;
  readonly kind: 'render';
  readonly dialect: DialectName;
  /** The `Blog` read method the vector renders (`Feed`/`ByIds`). */
  readonly entry: string;
  /** The bound input scope (canonically encoded). */
  readonly input: EncodedValue;
  /** Expected rendered SQL text (byte-true to the makeSQL reference). */
  readonly expectedSql: string;
  /** Expected flat params array (canonically encoded). */
  readonly expectedParams: EncodedValue[];
}

/** A write-render vector: the base-write `executeSQL` leaf's sql template of a `PostCommands` `entry`. */
export interface WriteRenderVector {
  readonly name: string;
  readonly kind: 'write-render';
  readonly dialect: DialectName;
  /** The `PostCommands` write method (`Create`/`Rename`/`Remove`). */
  readonly entry: string;
  readonly input: EncodedValue;
  readonly expectedSql: string;
  readonly expectedParams: EncodedValue[];
}

/** A read/exec vector: a `Blog` read method executed end-to-end against seeded SQLite via the leaf path
 * (#143 — re-derived from the fixture; the retired `bundle` artifact is gone). */
export interface ExecVector {
  readonly name: string;
  readonly kind: 'exec';
  readonly dialect: DialectName;
  /** The `Blog` read method (`Feed`/`Posts`). */
  readonly entry: string;
  readonly input: EncodedValue;
  readonly schema: readonly string[];
  readonly expectedResult: EncodedValue;
  /** A hard-limit config the SKIP guard vectors re-apply before publish (null-disable / intrinsic-LIMIT). */
  readonly config?: LimitConfigSpec;
  /** Model read-relation declarations to attach (the typed-object `read()` surface). */
  readonly relations?: readonly RelationDecl[];
  /** When set, run through `read()` with this relation eagerly selected (batch fires); else bare `executeBehavior`. */
  readonly withRelation?: string;
}

/** A write-transaction vector: a §8 SqlBundle with a transaction plan run as one tx (the SOLE surviving
 * SqlBundle — `compileWriteBundle` output, executed via `executeTransactionBundle`). */
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

/**
 * An EXPECT-ERROR vector (Phase E-2, epic #74): a `Blog` read executed under a hard-limit `config` over
 * seeded OVER-CAP SQLite, asserting the runtime THROWS {@link import('../src/scp').LimitExceededError}
 * with the exact fields. #143: the cap bakes at PUBLISH on the leaf path (`lowerFindGuard` for a find
 * cap; `compileRelationOp` for a relation cap), so the vector CARRIES the `config` a runner re-applies
 * before re-publishing — replacing the retired bundle-baked cap. `withRelation`/`relations` present ⇒
 * run `read()` so the relation batch fires (relation cap); absent ⇒ a bare `executeBehavior` (find cap).
 */
export interface ExpectErrorVector {
  readonly name: string;
  readonly kind: 'expect-error';
  readonly dialect: DialectName;
  readonly entry: string;
  readonly input: EncodedValue;
  readonly schema: readonly string[];
  /** The hard-limit config re-applied before publish (bakes the cap). */
  readonly config: LimitConfigSpec;
  readonly relations?: readonly RelationDecl[];
  readonly withRelation?: string;
  /** The exact {@link LimitExceededError} fields the throw must carry (the contract). */
  readonly expectedError: {
    readonly name: 'LimitExceededError';
    readonly limit: number;
    readonly count: number;
    readonly context: 'find' | 'relation';
    readonly model?: string;
    readonly relation?: string;
  };
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

export type Vector = RenderVector | WriteRenderVector | ExecVector | TxVector | DialectVector | ExpectErrorVector;

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
  // Inline typed-column declaration (issue #59): the reads project from these declared types (matches
  // READ_SCHEMA). Required for a typed read — registration fails closed on an undeclared column.
  static columns = {
    posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT', status: 'TEXT', created_at: 'TEXT' },
    users: { id: 'INTEGER', name: 'TEXT' },
    tags: { id: 'INTEGER', post_id: 'INTEGER', label: 'TEXT' },
  };

  Feed($: In<{ author_id: number; status?: string; since: string; created_at: string; limit?: number }>) {
    const posts = emitRead(L, 'Select', {
      table: 'posts',
      select: ['id', 'author_id', 'title', 'status'],
      where: [
        whereEq($.author_id, $.author_id),
        when(ne(opt($.status), null), () => whereEq($.status, $.status)),
        whereGe($.created_at, $.since),
      ],
      order: 'id ASC',
      limit: coalesce(opt($.limit), 20),
    }, 'sqlite');
    const authors = posts.map(($p: Recorded) =>
      emitRead(L, 'Select', { table: 'users', select: ['id', 'name'], where: [whereEq($p.id, $p.author_id)] }, 'sqlite'),
    );
    return { posts, authors };
  }

  ByIds($: In<{ ids: number[] }>) {
    return emitRead(L, 'Select', { table: 'posts', select: ['id', 'title'], where: [whereIn(inColumn($, 'id'), $.ids)], order: 'id ASC' }, 'sqlite');
  }

  /**
   * A bare posts row list with NO author `limit` — the find hard-limit guard TARGET (Phase E-2).
   * The parent page for the relation-batch guard vectors too (hasMany tags attach onto each post).
   */
  Posts($: In<{ author_id: number }>) {
    return emitRead(L, 'Select', { table: 'posts', select: ['id', 'author_id', 'title', 'status'], where: [whereEq($.author_id, $.author_id)], order: 'id ASC' }, 'sqlite');
  }
}

/** Command: Insert a post with RETURNING (the gate-first write-tx base write, spec §6). */
class PostCommands extends SemanticBehavior {
  static columns = { posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT' } };
  Create($: In<{ author_id: number; title: string; request_id: string }>) {
    return emitWrite(L, 'Insert', { table: 'posts', 'values.author_id': $.author_id, 'values.title': $.title, returning: 'id, author_id, title' }, 'sqlite');
  }
  Rename($: In<{ id: number; title: string }>) {
    return emitWrite(L, 'Update', { table: 'posts', 'set.title': $.title, where: [whereEq($.id, $.id)], returning: 'id, title' }, 'sqlite');
  }
  Remove($: In<{ id: number }>) {
    return emitWrite(L, 'Delete', { table: 'posts', where: [whereEq($.id, $.id)], returning: 'id' }, 'sqlite');
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
  static columns = {
    posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT' },
    comments: { id: 'INTEGER', post_id: 'INTEGER', body: 'TEXT' },
  };
  CreatePost($: In<{ author_id: number; title: string; body: string }>) {
    return emitWrite(L, 'Insert', { table: 'posts', 'values.author_id': $.author_id, 'values.title': $.title, returning: 'id, author_id, title' }, 'sqlite');
  }
  CreateComment($: In<{ body: string }>) {
    return emitWrite(L, 'Insert', { table: 'comments', 'values.post_id': $.body, 'values.body': $.body, returning: 'id, post_id, body' }, 'sqlite');
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
  (child.op as unknown as { params: unknown[]; sql: string }).params = [{ ref: ['body'] }, { ref: ['post', 'id'] }];
  (child.op as unknown as { params: unknown[]; sql: string }).sql = 'INSERT INTO comments (body, post_id) VALUES (?, ?) RETURNING id, post_id, body';
  return bundle;
}

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

/**
 * The SSoT leaf read executor for the `Blog` fixture — SHARED by the vector GENERATORS (capture the
 * golden) and {@link runVector} (re-derive + compare), so capture and replay run the IDENTICAL path.
 * Publishes `Blog` under `config` (the find cap bakes at publish via `lowerFindGuard`; the relation cap
 * resolves at read via `compileRelationOp`), executes `entry` over seeded SQLite — `read()` when
 * relations/`withRelation` are supplied (the batch fires + attaches), else bare `executeBehavior` (the Φ
 * output). Resets the config after (test isolation). Returns the raw runtime output (or throws the guard).
 */
function runBlogRead(spec: { entry: string; input: Record<string, unknown>; schema: readonly string[]; config?: LimitConfigSpec; relations?: readonly RelationDecl[]; withRelation?: string }): unknown {
  resetLimitConfig();
  if (spec.config !== undefined) setLimitConfig(spec.config);
  const db = seedDb(spec.schema);
  try {
    const contract = publishBehaviors(Blog); // the find cap (if any) bakes into the primary read leaf here
    return spec.relations !== undefined || spec.withRelation !== undefined
      ? read(contract, spec.input as never, { db, entry: spec.entry, relations: spec.relations ?? [], ...(spec.withRelation !== undefined ? { with: { [spec.withRelation]: true } } : {}) })
      : executeBehavior(contract, spec.input as never, { db, entry: spec.entry });
  } finally {
    db.close();
    resetLimitConfig();
  }
}

/** Build a render vector: render the `Blog` `entry` read leaf against an input for a dialect (the WHERE/IN
 *  form is lowered per-dialect at publish; the base is dialect-invariant). */
function renderVector(name: string, entry: string, input: Record<string, unknown>, dialect: DialectName): RenderVector {
  const rendered = renderPrimaryRead(publishBehaviors(Blog, { dialect }), entry, input, dialect);
  return { name, kind: 'render', dialect, entry, input: encodeValue(input), expectedSql: rendered.sql, expectedParams: rendered.params.map(encodeValue) };
}

/** Build a write-render vector: the `PostCommands` `entry` base-write leaf's sql template (dialect-invariant
 *  for these RETURNING writes — no per-column cast). */
function writeRenderVector(name: string, entry: string, input: Record<string, unknown>, dialect: DialectName): WriteRenderVector {
  const w = writeLeafOf(publishBehaviors(PostCommands), entry);
  return { name, kind: 'write-render', dialect, entry, input: encodeValue(input), expectedSql: w.sql, expectedParams: w.params.map(encodeValue) };
}

/** Build an exec vector: run the `Blog` `entry` read (Φ output) against a freshly seeded DB via the leaf. */
function execVector(name: string, entry: string, input: Record<string, unknown>, schema: readonly string[]): ExecVector {
  const result = runBlogRead({ entry, input, schema });
  return { name, kind: 'exec', dialect: 'sqlite', entry, input: encodeValue(input), schema, expectedResult: encodeValue(result) };
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

// ── Expect-error (hard-limit) vector construction (Phase E-2, epic #74) ────────
//
// Each guard vector runs the `Blog` read UNDER a hard-limit `config` (the find cap bakes into the read
// leaf at publish via `lowerFindGuard`; the relation cap resolves at read via `compileRelationOp`) over
// seeded OVER-CAP SQLite and CAPTURES the LimitExceededError fields it throws (via the SHARED
// {@link runBlogRead}). #143: the cap is no longer bundle-baked, so the vector CARRIES the `config` a
// runner re-applies before re-publishing. If the reference does NOT throw, generation fails loudly.

/** Build a find-cap expect-error vector: a bare read over-cap → throw (context=find). */
function findGuardVector(name: string, findHardLimit: number, schema: readonly string[], input: Record<string, unknown>): ExpectErrorVector {
  const config: LimitConfigSpec = { findHardLimit };
  const err = captureLimitError(() => runBlogRead({ entry: 'Posts', input, schema, config }), name);
  return {
    name, kind: 'expect-error', dialect: 'sqlite', entry: 'Posts', input: encodeValue(input), schema, config,
    expectedError: { name: 'LimitExceededError', limit: err.limit, count: err.count, context: err.context, ...(err.model !== undefined ? { model: err.model } : {}) },
  };
}

/** Build a relation-cap expect-error vector: an over-cap hasMany batch → throw (context=relation, exact count). */
function relationGuardVector(name: string, relations: readonly RelationDecl[], hasManyHardLimit: number | undefined, schema: readonly string[], input: Record<string, unknown>): ExpectErrorVector {
  const config: LimitConfigSpec = hasManyHardLimit !== undefined ? { hasManyHardLimit } : {};
  const err = captureLimitError(() => runBlogRead({ entry: 'Posts', input, schema, config, relations, withRelation: 'tags' }), name);
  return {
    name, kind: 'expect-error', dialect: 'sqlite', entry: 'Posts', input: encodeValue(input), schema, config, relations, withRelation: 'tags',
    expectedError: { name: 'LimitExceededError', limit: err.limit, count: err.count, context: err.context, ...(err.model !== undefined ? { model: err.model } : {}), ...(err.relation !== undefined ? { relation: err.relation } : {}) },
  };
}

/**
 * Build a SKIP exec vector: run `Posts` UNDER a config that must NOT throw (null-disable /
 * per-relation-disable / intrinsic-LIMIT window) over the SAME over-cap data, capturing the NORMAL
 * result. If the reference throws, generation fails. `withRelation` set ⇒ the typed-object `read()`
 * surface (batch fires but is not capped); else bare `executeBehavior`.
 */
function guardSkipExec(name: string, config: LimitConfigSpec, relations: readonly RelationDecl[], withRelation: string | undefined, input: Record<string, unknown>): ExecVector {
  const result = runBlogRead({ entry: 'Posts', input, schema: READ_SCHEMA, config, relations, ...(withRelation !== undefined ? { withRelation } : {}) });
  return { name, kind: 'exec', dialect: 'sqlite', entry: 'Posts', input: encodeValue(input), schema: READ_SCHEMA, config, relations, expectedResult: encodeValue(result), ...(withRelation !== undefined ? { withRelation } : {}) };
}

/** Build a SKIP exec vector on `Feed` (author-limit present ⇒ find-guard skipped): run under a find cap
 *  that would over-cap, capture the normal Φ result (no throw). */
function guardSkipFeedExec(name: string, config: LimitConfigSpec, input: Record<string, unknown>): ExecVector {
  const result = runBlogRead({ entry: 'Feed', input, schema: READ_SCHEMA, config });
  return { name, kind: 'exec', dialect: 'sqlite', entry: 'Feed', input: encodeValue(input), schema: READ_SCHEMA, config, expectedResult: encodeValue(result) };
}

/** Run `fn`, assert it threw a `LimitExceededError`, return its fields (generation fails if it did not). */
function captureLimitError(fn: () => unknown, name: string): LimitExceededError {
  try {
    fn();
  } catch (e) {
    if (e instanceof LimitExceededError) return e;
    throw new Error(`guard vector '${name}': expected LimitExceededError, got ${e instanceof Error ? e.name + ': ' + e.message : String(e)}`);
  }
  throw new Error(`guard vector '${name}': the reference did NOT throw (an expect-error vector must error)`);
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

  // ── exec suite: read Φ-merge (Feed = posts + per-parent authors map) × SQLite seam ────────────
  const exec: ExecVector[] = [];
  // The execution seam is in-process SQLite; a PG/MySQL read's Φ output is dialect-invariant (same IR +
  // input → same result, §10), so only the SQLite run is EXECUTED. `executeBehavior(Feed)` runs the full
  // leaf graph (posts + the `.map` authors relation) → the Φ output `{posts, authors}`.
  exec.push(execVector(`Feed: status present + belongsTo/hasMany relations`, 'Feed', { author_id: 7, status: 'live', since: '2026-01-01', created_at: 'created_at' }, READ_SCHEMA));
  exec.push(execVector(`Feed: status absent (SKIP drop) + relations`, 'Feed', { author_id: 7, since: '2026-01-01', created_at: 'created_at' }, READ_SCHEMA));

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
    // The WRITE bundle carries the TransactionResult `outputType` typed-codegen annotation (entity /
    // returnedRows typed via the model's inline `static columns` — the leaf SoT `cmdContract.resolveColumnType`)
    // so bc's typed-raw de-box emitter materializes a concrete result struct for the WRITE (tx) surface.
    const bundle = compileWriteBundle(cmdContract, 'Create', postWrites, 'create', d, cmdContract.resolveColumnType);
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

  // ── guard suite: hard-limit runaway prevention (Phase E-2, epic #74; #99 reference) ─────────────
  // Author 7 has posts (1,2) with tags (10,11 on post 1; 12 on post 2) = 3 tags total. So a
  // findHardLimit of 1 over-caps the 2-post read, and a hasManyHardLimit of 2 over-caps the 3-tag
  // batch. `Posts` carries no author `limit` → the find-guard applies. The expect-error vectors
  // assert the THROW; the skip vectors (null / explicit-limit / intrinsic-LIMIT) are exec vectors
  // that must NOT throw — the cross-language contract the ports (#100-103) reproduce.
  const relTagsUnlimited: readonly RelationDecl[] = [
    { name: 'author', kind: 'belongsTo', targetTable: 'users', select: ['id', 'name'], parentKey: 'author_id', targetKey: 'id' },
    { name: 'tags', kind: 'hasMany', targetTable: 'tags', select: ['id', 'post_id', 'label'], parentKey: 'id', targetKey: 'post_id', order: 'id ASC' },
  ];
  const relTagsWindowed: readonly RelationDecl[] = [
    { name: 'tags', kind: 'hasMany', targetTable: 'tags', select: ['id', 'post_id', 'label'], parentKey: 'id', targetKey: 'post_id', order: 'id ASC', limit: 1 },
  ];
  const relTagsOverride: readonly RelationDecl[] = [
    { name: 'tags', kind: 'hasMany', targetTable: 'tags', select: ['id', 'post_id', 'label'], parentKey: 'id', targetKey: 'post_id', order: 'id ASC', hardLimit: 2 },
  ];
  const relTagsDisabled: readonly RelationDecl[] = [
    { name: 'tags', kind: 'hasMany', targetTable: 'tags', select: ['id', 'post_id', 'label'], parentKey: 'id', targetKey: 'post_id', order: 'id ASC', hardLimit: null },
  ];
  const guard: Vector[] = [
    // THROW: find cap exceeded (2 posts > cap 1) — reported count is the N+1 fetch (hardLimit+1=2).
    findGuardVector(`find: read exceeds findHardLimit → throw (context=find)`, 1, READ_SCHEMA, { author_id: 7 }),
    // THROW: relation batch total exceeded (3 tags > global cap 2) — reported EXACT count 3.
    relationGuardVector(`relation: hasMany batch exceeds hasManyHardLimit → throw (exact count)`, relTagsUnlimited, 2, READ_SCHEMA, { author_id: 7 }),
    // THROW: relation per-relation hardLimit override (2) wins over an absent/high global.
    relationGuardVector(`relation: per-relation hardLimit override → throw`, relTagsOverride, undefined, READ_SCHEMA, { author_id: 7 }),
    // SKIP (no throw): findHardLimit=null disables the read cap → normal rows.
    guardSkipExec(`find: findHardLimit null → no throw (disabled)`, { findHardLimit: null }, [], undefined, { author_id: 7 }),
    // SKIP: an explicit author limit governs (Feed's limit=20 branch), find-guard not applied.
    guardSkipFeedExec(`find: explicit author limit → no throw (skip)`, { findHardLimit: 1 }, { author_id: 7, status: 'live', since: '2026-01-01', created_at: 'created_at' }),
    // SKIP: per-relation hardLimit null disables the relation cap even when the global is set.
    guardSkipExec(`relation: per-relation hardLimit null → no throw (disabled)`, { hasManyHardLimit: 1 }, relTagsDisabled, 'tags', { author_id: 7 }),
    // SKIP: an intrinsic per-parent LIMIT window relation skips the batch-total check.
    guardSkipExec(`relation: intrinsic per-parent LIMIT window → no throw (skip batch check)`, { hasManyHardLimit: 1 }, relTagsWindowed, 'tags', { author_id: 7 }),
  ];

  return [
    { suite: 'render', corpusVersion: CORPUS_VERSION, note: 'READ primaries + WRITE statements × 3 dialects × SKIP/IN edge cases — static makeSQL render golden (byte-true to the original builders).', vectors: render },
    { suite: 'exec', corpusVersion: CORPUS_VERSION, note: 'Read bundles executed against seeded SQLite via the native read-graph walker: SKIP + belongsTo/hasMany relations (batched op).', vectors: exec },
    { suite: 'tx', corpusVersion: CORPUS_VERSION, note: 'Write-time-relations gate-first transaction bundles: commit + gate short-circuit + composite tx-DAG.', vectors: tx },
    { suite: 'dialect', corpusVersion: CORPUS_VERSION, note: 'Dialect primitive orderByNulls: PG/SQLite native NULLS, MySQL IS NULL emulation.', vectors: dialect },
    { suite: 'guard', corpusVersion: CORPUS_VERSION, note: 'Hard-limit runaway prevention (Phase E-2, epic #74): expect-error vectors assert LimitExceededError (find N+1 / relation exact-count) with baked caps; exec skip vectors assert null-disable / explicit-limit / intrinsic-LIMIT-window do NOT throw. The cross-language contract #100-103 mirror.', vectors: guard },
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
      // Re-publish `Blog` for the dialect (the WHERE/IN form lowers per-dialect) and render the read leaf.
      const r = renderPrimaryRead(publishBehaviors(Blog, { dialect: v.dialect }), v.entry, decodeValue(v.input) as Record<string, unknown>, v.dialect);
      const sqlOk = r.sql === v.expectedSql;
      const paramsOk = eq(r.params.map(encodeValue), v.expectedParams);
      if (sqlOk && paramsOk) return { ...base, suite: 'render', ok: true };
      const parts: string[] = [];
      if (!sqlOk) parts.push(`sql ${JSON.stringify(r.sql)} != ${JSON.stringify(v.expectedSql)}`);
      if (!paramsOk) parts.push(`params ${JSON.stringify(r.params.map(encodeValue))} != ${JSON.stringify(v.expectedParams)}`);
      return { ...base, suite: 'render', ok: false, detail: parts.join('; ') };
    }
    if (v.kind === 'write-render') {
      const w = writeLeafOf(publishBehaviors(PostCommands), v.entry);
      const sqlOk = w.sql === v.expectedSql;
      const paramsOk = eq(w.params.map(encodeValue), v.expectedParams);
      return { ...base, suite: 'render', ok: sqlOk && paramsOk, detail: sqlOk && paramsOk ? undefined : `write-render mismatch: sql ${JSON.stringify(w.sql)} vs ${JSON.stringify(v.expectedSql)}` };
    }
    if (v.kind === 'exec') {
      // Re-run through the SHARED leaf executor (the SAME path the generator captured with).
      const result = encodeValue(runBlogRead({ entry: v.entry, input: decodeValue(v.input) as Record<string, unknown>, schema: v.schema, ...(v.config !== undefined ? { config: v.config } : {}), ...(v.relations !== undefined ? { relations: v.relations } : {}), ...(v.withRelation !== undefined ? { withRelation: v.withRelation } : {}) }));
      const ok = eq(result, v.expectedResult);
      return { ...base, suite: v.config !== undefined ? 'guard' : 'exec', ok, detail: ok ? undefined : `result ${JSON.stringify(result)} != ${JSON.stringify(v.expectedResult)}` };
    }
    if (v.kind === 'expect-error') {
      // Re-apply the config + re-publish (the cap bakes at publish / read via the SHARED `runBlogRead`),
      // run over-cap, and assert the SAME typed LimitExceededError fields. A port mirrors this: apply the
      // config, run the model read → assert the throw.
      let thrown: unknown;
      try {
        runBlogRead({ entry: v.entry, input: decodeValue(v.input) as Record<string, unknown>, schema: v.schema, config: v.config, ...(v.relations !== undefined ? { relations: v.relations } : {}), ...(v.withRelation !== undefined ? { withRelation: v.withRelation } : {}) });
      } catch (e) { thrown = e; }
      if (!(thrown instanceof LimitExceededError)) {
        return { ...base, suite: 'guard', ok: false, detail: `expected LimitExceededError, got ${thrown === undefined ? 'no throw' : thrown instanceof Error ? thrown.name + ': ' + thrown.message : String(thrown)}` };
      }
      const got = { name: thrown.name, limit: thrown.limit, count: thrown.count, context: thrown.context, ...(thrown.model !== undefined ? { model: thrown.model } : {}), ...(thrown.relation !== undefined ? { relation: thrown.relation } : {}) };
      const ok = eq(got, v.expectedError);
      return { ...base, suite: 'guard', ok, detail: ok ? undefined : `error ${JSON.stringify(got)} != ${JSON.stringify(v.expectedError)}` };
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
