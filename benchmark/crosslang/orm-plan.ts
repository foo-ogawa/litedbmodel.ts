// ════════════════════════════════════════════════════════════════════════════
// Unified ORM-op statement PLAN — the language-neutral SSoT.
// ════════════════════════════════════════════════════════════════════════════
//
// The cross-language bench measures each language's THIN GENERIC RUNTIME executing
// the SAME 19 ORM-comparison ops (benchmark/benchmark.ts → the litedbmodel column,
// captured as the v1 SQL golden and byte-parity with the v2 SCP path)
// DB-backed on all three real dialects (sqlite in-proc / mysql :3307 / postgres :5433),
// driver-included.
//
// This module builds, for each op × dialect, an ORDERED STATEMENT PLAN — the exact
// `{ sql, params }` the v2 SCP makeSQL compile path emits (compileSelect / compileInsert
// / compileInsertMany / compileUpdateSingle / compileUpdateMany / compileDelete /
// compileSingleKeyUnlimited / compileCompositeKeyUnlimited). That SQL is byte-identical
// to what the ORM-bench litedbmodel column runs (the parity gate asserts it), so the
// TS cross-lang numbers are consistent with the ORM-bench litedbmodel column BY
// CONSTRUCTION (same op, same v2 path, same SQL).
//
// Each language executes the plan through its shipped runtime's GENERIC driver seam
// (`prepare(sql).all(params)` / `.run(params)`, a BEGIN/…/COMMIT for writes, and the
// relation batch-load stitch for nested reads) — NO hand-mirror that re-implements SQL
// generation: the SQL text + params are pre-rendered here by the proven SCP path and the
// runtime just binds + executes them against the real driver. This is the "thin generic
// statement executor" every runtime already is (execute_bundle / execute_transaction_bundle
// render the same statements internally; here the render is the shared SCP path).
//
// 🔒 CONSUME-ONLY: uses the litedbmodel public compile API (src/scp/makesql) to PRODUCE the
// plan artifact; it does NOT modify src.

// Import the compile API from the esbuild bundle (dist/scp/index.mjs) — the ONLY tsx-importable
// entry (behavior-contracts is ESM-only with no CJS resolution, so the raw src/ submodule cannot be
// imported under tsx; the bundled .mjs resolves bc as a true external ESM import). The top-level
// entry re-exports the makeSQL compilers under `*MakeSQL` aliases (compileSelect→compileSelectMakeSQL,
// compileInsert→compileInsertMakeSQL, compileDelete→compileDeleteMakeSQL); the others keep their name.
import * as lm from '../../dist/scp/index.mjs';

// Local type aliases (the bundle does not re-export the compile-arg types by name; the shapes below
// match src/scp/makesql exactly — asserted byte-for-byte by the parity test).
type Dialect = 'sqlite' | 'mysql' | 'postgres';
interface MakeSQL {
  readonly [k: string]: unknown;
}

const compileSelect = lm.compileSelectMakeSQL as (desc: Record<string, unknown>) => MakeSQL;
const compileInsert = lm.compileInsertMakeSQL as (d: Dialect, o: Record<string, unknown>) => MakeSQL;
const compileInsertMany = lm.compileInsertMany as (d: Dialect, o: Record<string, unknown>) => MakeSQL[];
const compileUpdateMany = lm.compileUpdateMany as (d: Dialect, o: Record<string, unknown>) => MakeSQL;
const compileUpdateSingle = lm.compileUpdateSingle as (o: Record<string, unknown>) => MakeSQL;
const compileDelete = lm.compileDeleteMakeSQL as (o: Record<string, unknown>) => MakeSQL;
const compileSingleKeyUnlimited = lm.compileSingleKeyUnlimited as (o: Record<string, unknown>) => MakeSQL;
const compileCompositeKeyUnlimited = lm.compileCompositeKeyUnlimited as (o: Record<string, unknown>) => MakeSQL;
const assembleMakeSQL = lm.assembleMakeSQL as (n: MakeSQL) => { sql: string; params: unknown[] };
const renderPlaceholders = lm.renderPlaceholders as (sql: string, d: Dialect) => string;
// dbCast MUST come from the SAME bundle as the compilers (shared DBCast class identity).
const dbCast = lm.dbCast as (v: unknown, cast: string) => unknown;

// ── Dialects (the three real targets) ─────────────────────────────────────────
export const ORM_DIALECTS = ['sqlite', 'mysql', 'postgres'] as const;
export type OrmDialect = (typeof ORM_DIALECTS)[number];

// ── The 19 ORM ops (== benchmark.ts testCategories) ─
// Order + labels mirror benchmark/benchmark.ts exactly. `write` marks the ten ops whose
// logical op mutates (they run inside a transaction: BEGIN … COMMIT).
export interface OrmOpMeta {
  readonly id: string; // stable slug (protocol id)
  readonly label: string; // the human label (== benchmark.ts / golden op key)
  readonly write: boolean;
}

export const ORM_OPS: readonly OrmOpMeta[] = [
  { id: 'findAll', label: 'Find all (limit 100)', write: false },
  { id: 'filterPaginateSort', label: 'Filter, paginate & sort', write: false },
  { id: 'nestedFindAll', label: 'Nested find all (include posts)', write: false },
  { id: 'findFirst', label: 'Find first', write: false },
  { id: 'nestedFindFirst', label: 'Nested find first (include posts)', write: false },
  { id: 'findUnique', label: 'Find unique (by email)', write: false },
  { id: 'nestedFindUnique', label: 'Nested find unique (include posts)', write: false },
  { id: 'create', label: 'Create', write: true },
  { id: 'nestedCreate', label: 'Nested create (with post)', write: true },
  { id: 'update', label: 'Update', write: true },
  { id: 'nestedUpdate', label: 'Nested update (update user + post)', write: true },
  { id: 'upsert', label: 'Upsert', write: true },
  { id: 'nestedUpsert', label: 'Nested upsert (user + post)', write: true },
  { id: 'delete', label: 'Delete', write: true },
  { id: 'createMany', label: 'Create Many (10 records)', write: true },
  { id: 'upsertMany', label: 'Upsert Many (10 records)', write: true },
  { id: 'updateMany', label: 'Update Many (10 different values)', write: true },
  { id: 'nestedRelations', label: 'Nested relations (100->1000->10000)', write: false },
  { id: 'compositeRelations', label: 'Nested relations (composite key, 5 tenants)', write: false },
] as const;

export const ORM_OP_IDS: readonly string[] = ORM_OPS.map((o) => o.id);
export const ORM_WRITE_OP_IDS: ReadonlySet<string> = new Set(ORM_OPS.filter((o) => o.write).map((o) => o.id));
export const ORM_OP_LABEL: Record<string, string> = Object.fromEntries(ORM_OPS.map((o) => [o.id, o.label]));

// ── A rendered statement + its role in the plan ───────────────────────────────
// role:
//   'stmt'         — a plain read/write statement executed as-is against the driver. In a read plan,
//                    reads[0] is the PRIMARY select (its rows feed the relation stages by array position).
//   'insertReturn' — a write that RETURNs a generated id used by the NEXT statement's param.
//   'useReturn'    — a write whose Nth param (useReturnAt) is filled from the prior insertReturn id.
// (Relation batches are NOT PlanStmts — they are RelationStage entries with baked per-dialect SQL.)
export type StmtRole = 'stmt' | 'insertReturn' | 'useReturn';

export interface PlanStmt {
  readonly role: StmtRole;
  readonly sql: string;
  readonly params: readonly unknown[];
  // For 'useReturn': index into params to replace with the prior insertReturn id.
  readonly useReturnAt?: number;
}

// How a relation batch binds its resolved parent keys (the baked SQL's ONE param slot / groups):
//   'jsonParam'      — sqlite/mysql single-key: bind ONE param = JSON.stringify(distinct keys)
//                      (the SQL is `… IN (SELECT … FROM json_each(?)/JSON_TABLE(?))`, key-count-independent).
//   'pgArraySingle'  — pg single-key: bind ONE array param = distinct keys (`= ANY($1::type[])`).
//   'pgArrayComposite' — pg composite: bind TWO array params = the two key columns
//                      (`JOIN unnest($1::int[], $2::int[])`), key-count-independent.
//   'tupleExpand'    — sqlite/mysql composite: the SQL's `(col0, col1) IN (` prefix + a `(?, ?)` group
//                      REPEATED per distinct tuple (joined by ', ') + `)`, params = flattened tuples.
//                      `groupTemplate` + `prefix`/`suffix` let a language rebuild it without a compiler.
export type RelationBindKind = 'jsonParam' | 'pgArraySingle' | 'pgArrayComposite' | 'tupleExpand';

// A relation stage: how to load children for a prior read's rows. The SQL is BAKED per dialect (from
// the SAME SCP relation compiler the ORM path uses) so a non-TS runtime binds it without a compiler.
export interface RelationStage {
  // Which read's rows this relation loads children for (0 = primary; N = relation stage N-1's rows).
  readonly parentStmt: number;
  readonly tableName: string;
  readonly select: string;
  // Single-key relation: parent rows' `parentKey` → child `targetKey`.
  readonly single?: { parentKey: string; targetKey: string };
  // Composite-key relation: parent rows' (pk0,pk1) → child (tk0,tk1).
  readonly composite?: { parentKeys: [string, string]; targetKeys: [string, string] };
  // Baked, per-dialect batch SQL + bind protocol (this stage belongs to ONE dialect's plan).
  readonly bindKind: RelationBindKind;
  // 'jsonParam'/'pgArray*': the full rendered SQL (key-count-independent). 'tupleExpand': the prefix
  // (`… WHERE (col0, col1) IN (`), with `groupTemplate`/`suffix` completing it.
  readonly sql: string;
  readonly groupTemplate?: string; // 'tupleExpand' only, e.g. '(?, ?)'
  readonly suffix?: string; // 'tupleExpand' only, e.g. ')'
}

// A read op plan: ordered primary/relation reads.
export interface ReadPlan {
  readonly kind: 'read';
  readonly reads: readonly PlanStmt[]; // index 0 is the primary; later entries are extra reads
  readonly relations: readonly RelationStage[]; // resolved against `reads` at exec time
}

// A write op plan: a transaction (BEGIN … statements … COMMIT).
export interface WritePlan {
  readonly kind: 'write';
  readonly statements: readonly PlanStmt[];
}

export type OpPlan = ReadPlan | WritePlan;

// The full artifact: op id → dialect → plan.
export type OrmPlanArtifact = Record<string, Record<OrmDialect, OpPlan>>;

// ── SCP render helpers (identical to the parity test) ─────────────────────
function render(node: MakeSQL, dialect: Dialect): { sql: string; params: unknown[] } {
  const asm = assembleMakeSQL(node);
  return { sql: renderPlaceholders(asm.sql, dialect), params: asm.params as unknown[] };
}
function stmt(node: MakeSQL, dialect: Dialect, role: StmtRole = 'stmt', useReturnAt?: number): PlanStmt {
  const r = render(node, dialect);
  return { role, sql: r.sql, params: r.params, useReturnAt };
}

// ── Relation SQL bakers (the SAME SCP relation compilers the ORM path + parity test use) ──
// Bake the per-dialect relation batch SQL + bind protocol so a non-TS runtime executes it WITHOUT
// a compiler. Single-key + pg-composite SQL is key-count-independent (one/two JSON/array params);
// sqlite/mysql composite expands a `(?, ?)` group per tuple (tupleExpand).

// Two representative single keys (SQL is identical for any count — asserted by the executor).
const SAMPLE_KEYS = [1, 2];
const SAMPLE_TUPLES: number[][] = [[1, 1], [1, 2]];

function bakeSingle(
  d: OrmDialect,
  parentStmt: number,
  rel: { tableName: string; select: string; parentKey: string; targetKey: string },
): RelationStage {
  const node = compileSingleKeyUnlimited({ dialect: d, tableName: rel.tableName, select: rel.select, targetKey: rel.targetKey, values: SAMPLE_KEYS });
  const asm = assembleMakeSQL(node);
  const sql = renderPlaceholders(asm.sql, d);
  return {
    parentStmt,
    tableName: rel.tableName,
    select: rel.select,
    single: { parentKey: rel.parentKey, targetKey: rel.targetKey },
    bindKind: d === 'postgres' ? 'pgArraySingle' : 'jsonParam',
    sql,
  };
}

function bakeComposite(
  d: OrmDialect,
  parentStmt: number,
  rel: { tableName: string; select: string; parentKeys: [string, string]; targetKeys: [string, string] },
): RelationStage {
  const node = compileCompositeKeyUnlimited({ dialect: d, tableName: rel.tableName, select: rel.select, targetKeys: rel.targetKeys, tuples: SAMPLE_TUPLES });
  const asm = assembleMakeSQL(node);
  const sql = renderPlaceholders(asm.sql, d);
  const base = { parentStmt, tableName: rel.tableName, select: rel.select, composite: { parentKeys: rel.parentKeys, targetKeys: rel.targetKeys } };
  if (d === 'postgres') {
    return { ...base, bindKind: 'pgArrayComposite', sql };
  }
  // sqlite/mysql: `… WHERE (c0, c1) IN ((?, ?), (?, ?))` for the 2 SAMPLE_TUPLES → split into
  // prefix `… IN (`, group `(?, ?)`, suffix `)` so the executor repeats the group per real tuple.
  const inOpen = sql.indexOf('IN (') + 'IN ('.length;
  const prefix = sql.slice(0, inOpen);
  const suffix = ')';
  return { ...base, bindKind: 'tupleExpand', sql: prefix, groupTemplate: '(?, ?)', suffix };
}

// ── Per-op × per-dialect plan builders ────────────────────────────────────────
function buildReadOnly(node: (d: Dialect) => MakeSQL): (d: OrmDialect) => OpPlan {
  return (d) => ({ kind: 'read', reads: [stmt(node(d), d, 'stmt')], relations: [] });
}

// A nested single-key read: primary select + one single-key relation batch (children by author_id).
function buildNestedSingle(
  primary: (d: Dialect) => MakeSQL,
  rel: { tableName: string; select: string; parentKey: string; targetKey: string },
): (d: OrmDialect) => OpPlan {
  return (d) => ({
    kind: 'read',
    reads: [stmt(primary(d), d, 'stmt')],
    relations: [bakeSingle(d, 0, rel)],
  });
}

const orm: Record<string, (d: OrmDialect) => OpPlan> = {
  // 1. Find all (limit 100)
  findAll: buildReadOnly((d) => compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', limit: 100 })),

  // 2. Filter, paginate & sort: published boolean (pg ::boolean cast), ORDER BY created_at DESC, LIMIT/OFFSET.
  filterPaginateSort: buildReadOnly((d) =>
    compileSelect({
      dialect: d,
      tableName: 'benchmark_posts',
      select: '*',
      conditions: { published: dbCast(true, 'boolean') },
      order: 'created_at DESC',
      limit: 20,
      offset: 10,
    }),
  ),

  // 3. Nested find all (include posts): users LIMIT 100 + posts-by-author_id relation.
  nestedFindAll: buildNestedSingle(
    (d) => compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', limit: 100 }),
    { tableName: 'benchmark_posts', select: '*', parentKey: 'id', targetKey: 'author_id' },
  ),

  // 4. Find first: name LIKE ? LIMIT 1
  findFirst: buildReadOnly((d) =>
    compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', conditions: { 'name LIKE ?': 'User%' }, limit: 1 }),
  ),

  // 5. Nested find first (include posts): findOne + posts relation.
  nestedFindFirst: buildNestedSingle(
    (d) => compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', conditions: { 'name LIKE ?': 'User%' }, limit: 1 }),
    { tableName: 'benchmark_posts', select: '*', parentKey: 'id', targetKey: 'author_id' },
  ),

  // 6. Find unique (by email)
  findUnique: buildReadOnly((d) =>
    compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', conditions: { email: 'user500@example.com' }, limit: 1 }),
  ),

  // 7. Nested find unique (include posts)
  nestedFindUnique: buildNestedSingle(
    (d) => compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', conditions: { email: 'user500@example.com' }, limit: 1 }),
    { tableName: 'benchmark_posts', select: '*', parentKey: 'id', targetKey: 'author_id' },
  ),

  // 8. Create: INSERT user (email, name)
  create: (d) => ({
    kind: 'write',
    statements: [
      stmt(
        compileInsert(d, {
          tableName: 'benchmark_users',
          columns: ['email', 'name'],
          records: [{ email: uniqueEmail('bench'), name: 'Benchmark User' }],
          rawRecords: [{ email: uniqueEmail('bench'), name: 'Benchmark User' }],
        }),
        d,
        'stmt',
      ),
    ],
  }),

  // 9. Nested create (with post): INSERT user RETURNING id → INSERT post (author_id = returned id).
  nestedCreate: (d) => ({
    kind: 'write',
    statements: [
      stmt(
        compileInsert(d, {
          tableName: 'benchmark_users',
          columns: ['email', 'name'],
          records: [{ email: uniqueEmail('nested'), name: 'Nested User' }],
          rawRecords: [{ email: uniqueEmail('nested'), name: 'Nested User' }],
          returning: 'id',
        }),
        d,
        'insertReturn',
      ),
      // author_id is filled from the returned id at exec time (useReturnAt: 0).
      insertPostUsingReturn(d, 'Nested Post', ['author_id', 'content', 'title'], { content: 'Content', title: 'Nested Post' }),
    ],
  }),

  // 10. Update: UPDATE users SET name WHERE id = 100
  update: (d) => ({
    kind: 'write',
    statements: [stmt(compileUpdateSingle({ dialect: d, tableName: 'benchmark_users', serializedValues: { name: 'Updated User' }, conditions: { id: 100 } }), d, 'stmt')],
  }),

  // 11. Nested update: UPDATE user + UPDATE posts by author_id.
  nestedUpdate: (d) => ({
    kind: 'write',
    statements: [
      stmt(compileUpdateSingle({ dialect: d, tableName: 'benchmark_users', serializedValues: { name: 'Nested Updated' }, conditions: { id: 100 } }), d, 'stmt'),
      stmt(compileUpdateSingle({ dialect: d, tableName: 'benchmark_posts', serializedValues: { title: 'Updated Post' }, conditions: { author_id: 100 } }), d, 'stmt'),
    ],
  }),

  // 12. Upsert: INSERT … ON CONFLICT (email) DO UPDATE SET name RETURNING id
  upsert: (d) => ({
    kind: 'write',
    statements: [
      stmt(
        compileInsert(d, {
          tableName: 'benchmark_users',
          columns: ['email', 'name'],
          records: [{ email: uniqueEmail('upsert'), name: 'Upsert User' }],
          rawRecords: [{ email: uniqueEmail('upsert'), name: 'Upsert User' }],
          onConflict: ['email'],
          onConflictUpdate: ['name'],
          returning: 'id',
        }),
        d,
        'stmt',
      ),
    ],
  }),

  // 13. Nested upsert (user + post): upsert user RETURNING id → INSERT post.
  nestedUpsert: (d) => ({
    kind: 'write',
    statements: [
      stmt(
        compileInsert(d, {
          tableName: 'benchmark_users',
          columns: ['email', 'name'],
          records: [{ email: uniqueEmail('nupsert'), name: 'Nested Upsert' }],
          rawRecords: [{ email: uniqueEmail('nupsert'), name: 'Nested Upsert' }],
          onConflict: ['email'],
          onConflictUpdate: ['name'],
          returning: 'id',
        }),
        d,
        'insertReturn',
      ),
      insertPostUsingReturn(d, 'Upsert Post', ['author_id', 'title'], { title: 'Upsert Post' }),
    ],
  }),

  // 14. Delete: INSERT user RETURNING id → DELETE by that id.
  delete: (d) => ({
    kind: 'write',
    statements: [
      stmt(
        compileInsert(d, {
          tableName: 'benchmark_users',
          columns: ['email', 'name'],
          records: [{ email: uniqueEmail('del'), name: 'Delete User' }],
          rawRecords: [{ email: uniqueEmail('del'), name: 'Delete User' }],
          returning: 'id',
        }),
        d,
        'insertReturn',
      ),
      // DELETE WHERE id = <returned id>.
      deleteUsingReturn(d),
    ],
  }),

  // 15. Create Many (10)
  createMany: (d) => {
    const records = Array.from({ length: 10 }, (_, i) => ({ email: uniqueEmail(`bulk${i}`), name: `Bulk User ${i}` }));
    const nodes = compileInsertMany(d, { tableName: 'benchmark_users', records, rawRecords: records });
    return { kind: 'write', statements: nodes.map((n) => stmt(n, d, 'stmt')) };
  },

  // 16. Upsert Many (10)
  upsertMany: (d) => {
    const records = Array.from({ length: 10 }, (_, i) => ({ email: uniqueEmail(`ubulk${i}`), name: `Upsert Bulk ${i}` }));
    const nodes = compileInsertMany(d, { tableName: 'benchmark_users', records, rawRecords: records, onConflict: ['email'], onConflictUpdate: ['name'] });
    return { kind: 'write', statements: nodes.map((n) => stmt(n, d, 'stmt')) };
  },

  // 17. Update Many (10 different values)
  updateMany: (d) => {
    const records = Array.from({ length: 10 }, (_, i) => ({ id: 100 + i, name: `Updated Different ${i}` }));
    return {
      kind: 'write',
      statements: [
        stmt(
          compileUpdateMany(d, {
            tableName: 'benchmark_users',
            keyColumns: ['id'],
            updateColumns: ['name'],
            records,
            rawRecords: records,
          } as Parameters<typeof compileUpdateMany>[1]),
          d,
          'stmt',
        ),
      ],
    };
  },

  // 18. Nested relations (100->1000->10000): users + posts(author_id) + comments(post_id).
  nestedRelations: (d) => ({
    kind: 'read',
    reads: [stmt(compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', order: 'id ASC', limit: 100 }), d, 'stmt')],
    relations: [
      bakeSingle(d, 0, { tableName: 'benchmark_posts', select: '*', parentKey: 'id', targetKey: 'author_id' }),
      bakeSingle(d, 1, { tableName: 'benchmark_comments', select: '*', parentKey: 'id', targetKey: 'post_id' }),
    ],
  }),

  // 19. Nested relations (composite key, 5 tenants): tenant_users (IN tenants) + composite posts + composite comments.
  compositeRelations: (d) => ({
    kind: 'read',
    reads: [
      stmt(
        compileSelect({ dialect: d, tableName: 'benchmark_tenant_users', select: '*', conditions: { tenant_id: [1, 2, 3, 4, 5] }, limit: 100 }),
        d,
        'stmt',
      ),
    ],
    relations: [
      bakeComposite(d, 0, { tableName: 'benchmark_tenant_posts', select: '*', parentKeys: ['tenant_id', 'user_id'], targetKeys: ['tenant_id', 'user_id'] }),
      bakeComposite(d, 1, { tableName: 'benchmark_tenant_comments', select: '*', parentKeys: ['tenant_id', 'post_id'], targetKeys: ['tenant_id', 'post_id'] }),
    ],
  }),
};

// The relation stages above load children keyed off the PRIOR read's rows. `parentStmt: 1`
// means "the rows produced by relation stage 0" — the executor treats reads[0] as the primary,
// and each relation stage appends its loaded rows as the next indexable read result.

// ── Deterministic unique email (avoids UNIQUE collisions across the measured loop) ──
let uniqueCounter = 0;
function uniqueEmail(prefix: string): string {
  // A fixed seed per BUILD (the plan is rendered once); the executor re-parametrizes writes
  // per iteration to keep them collision-free (see the adapter's per-iteration email rewrite).
  return `${prefix}-{{SEQ}}@example.com`;
}
void uniqueCounter;

// The post insert whose author_id (param 0) is the prior insert's RETURNING id.
function insertPostUsingReturn(d: Dialect, title: string, columns: string[], extra: Record<string, unknown>): PlanStmt {
  const record = { author_id: 0, ...extra } as Record<string, unknown>;
  const node = compileInsert(d, { tableName: 'benchmark_posts', columns, records: [record], rawRecords: [record] });
  void title;
  // author_id is the FIRST param (columns are alphabetized by the compiler → 'author_id' first).
  return { ...stmt(node, d, 'useReturn'), useReturnAt: 0 };
}

function deleteUsingReturn(d: Dialect): PlanStmt {
  const node = compileDelete({ dialect: d, tableName: 'benchmark_users', conditions: { id: 0 } });
  return { ...stmt(node, d, 'useReturn'), useReturnAt: 0 };
}

// ── Build the full artifact ───────────────────────────────────────────────────
export function buildOrmPlanArtifact(): OrmPlanArtifact {
  const out: OrmPlanArtifact = {};
  for (const op of ORM_OPS) {
    const byDialect = {} as Record<OrmDialect, OpPlan>;
    for (const d of ORM_DIALECTS) {
      byDialect[d] = orm[op.id](d);
    }
    out[op.id] = byDialect;
  }
  return out;
}
