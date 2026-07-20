// ════════════════════════════════════════════════════════════════════════════
// The 19 ORM ops declared via SCP (epic #107 P1 / #109) — the cross-lang bench SSoT.
// ════════════════════════════════════════════════════════════════════════════
//
// Every op in `contract.ts` ORM_OPS is authored ONCE here on the litedbmodel public SCP surface, over
// the `benchmark_*` schema + seed of `orm-domain.ts`, and compiled to a SqlBundle (read/write) or a
// TransactionPlan bundle (the nested-write tx ops). From these bundles:
//   • BC consumes the authored TypeScript graph and owns native module generation;
//   • the SDK-baseline cell runs the hand-SQL against the raw driver;
//   • the mode-2 oracle (executeBundle / executeTransactionBundle) is the byte-equal reference.
// The operator surface is the completed E1–E5 (reads / writes / upsert / batch / relation / tx).
//
// Imported from the BUILT bundle (dist/scp/index.cjs, which inlines the ESM-only behavior-contracts) so
// this module runs standalone under tsx as well as under vitest.
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileBundle,
  compileWriteNode,
  deriveTransactionPlan,
  whereEq,
  whereLike,
  whereIn,
  inColumn,
  schemaColumnTypeResolver,
} from '../../dist/scp/index.cjs';
import type { ColumnTypeResolver } from '../../src/scp/coltype';
import type { RelationDecl } from '../../src/scp/relation';
import type { SqlBundle } from '../../src/scp/runtime';
import type { TransactionPlan } from '../../src/scp/makesql/tx';
import { ddl, type OrmDialect } from './orm-domain';

const L = components();
/** The §4.1 column-type resolver for a dialect (drives native port typing + read materialize). */
export function resolveFor(dialect: OrmDialect): ColumnTypeResolver {
  return schemaColumnTypeResolver(ddl(dialect));
}
export const RESOLVE: ColumnTypeResolver = resolveFor('sqlite'); // back-compat default

const USER_COLUMNS = {
  benchmark_users: { id: 'INTEGER', email: 'TEXT', name: 'TEXT', created_at: 'TEXT', updated_at: 'TEXT' },
} as const;
const POST_COLUMNS = {
  benchmark_posts: { id: 'INTEGER', title: 'TEXT', content: 'TEXT', published: 'INTEGER', author_id: 'INTEGER', created_at: 'TEXT' },
} as const;
const REL_COLUMNS = {
  benchmark_posts: POST_COLUMNS.benchmark_posts,
  benchmark_comments: { id: 'INTEGER', body: 'TEXT', post_id: 'INTEGER', created_at: 'TEXT' },
  benchmark_users: USER_COLUMNS.benchmark_users,
} as const;
const TENANT_COLUMNS = {
  benchmark_tenant_users: { tenant_id: 'INTEGER', user_id: 'INTEGER', name: 'TEXT' },
  benchmark_tenant_posts: { tenant_id: 'INTEGER', post_id: 'INTEGER', user_id: 'INTEGER', title: 'TEXT' },
} as const;

// ── READ behaviors (single Select) — bodies match benchmark.ts (the ORM-column label SoT) ───────────
class UserReads extends SemanticBehavior {
  static columns = USER_COLUMNS;
  // findAll: LiteUser.find([], {limit:100}) — order id ASC added for deterministic LIMIT across dialects.
  FindAll() {
    return L.Select({ table: 'benchmark_users', select: ['id', 'email', 'name'], order: 'id ASC', limit: 100 });
  }
  // findFirst: LiteUser.findOne([[name LIKE 'User%']]).
  FindFirst($: { name: unknown }) {
    return L.Select({ table: 'benchmark_users', select: ['id', 'email', 'name'], where: [whereLike($ as never, 'name', $.name)], limit: 1 });
  }
  // findUnique: LiteUser.findOne([[email, 'user500@example.com']]) — also the nestedFindUnique parent.
  FindUnique($: { email: unknown }) {
    return L.Select({ table: 'benchmark_users', select: ['id', 'email', 'name'], where: [whereEq(($ as never)['email'], $.email)], limit: 1 });
  }
}
class PostReads extends SemanticBehavior {
  static columns = POST_COLUMNS;
  // filterPaginateSort: LitePost.find([[published,true]], {order: created_at desc, limit:20, offset:10}).
  FilterPaginateSort($: { published: unknown }) {
    return L.Select({ table: 'benchmark_posts', select: ['id', 'title', 'content', 'published', 'author_id', 'created_at'], where: [whereEq(($ as never)['published'], $.published)], order: 'created_at DESC', limit: 20, offset: 10 });
  }
}
class TenantReads extends SemanticBehavior {
  static columns = TENANT_COLUMNS;
  ByTenant($: { tenant_id: unknown }) {
    return L.Select({ table: 'benchmark_tenant_users', select: ['tenant_id', 'user_id', 'name'], where: [whereEq(($ as never)['tenant_id'], $.tenant_id)], order: 'user_id ASC' });
  }
}

// ── WRITE behaviors (single + batch) ──────────────────────────────────────────
class UserWrites extends SemanticBehavior {
  static columns = USER_COLUMNS;
  // v1 returning semantics (test/parity/v1-sql-golden.test.ts + DBModel): a write returns NULL by
  // default, and `{returning:true}` returns the PRIMARY KEY only (`PkeyResult` = `id`), never all
  // columns. v1's bench ops: single create/update = NO returning; single upsert = returning (id);
  // batch createMany/upsertMany/updateMany = NO returning. (A no-returning write's result is null —
  // dialect-independent, unlike an engine-assigned id or affected-count: mysql ON DUPLICATE KEY
  // reports affected_rows = inserts + 2·updates, so upsertMany's count is 12 on mysql vs 10 elsewhere.)
  Create($: { email: unknown; name: unknown }) {
    return L.Insert({ table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name });
  }
  Update($: { id: unknown; name: unknown }) {
    return L.Update({ table: 'benchmark_users', 'set.name': $.name, where: [whereEq(($ as never)['id'], $.id)] });
  }
  Delete($: { id: unknown }) {
    return L.Delete({ table: 'benchmark_users', where: [whereEq(($ as never)['id'], $.id)] });
  }
  Upsert($: { email: unknown; name: unknown }) {
    // `pk`/`autoInc` declare the table's primary key (the model is the SSoT) so the mysql RETURNING
    // re-select recovers the row by the REAL pk — never an engine-hardcoded 'id' default.
    return L.Insert({ table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name, onConflict: 'email', onConflictAction: 'update', returning: 'id', pk: 'id', autoInc: 'id' });
  }
  CreateMany($: { emails: unknown; names: unknown }) {
    return L.Insert({ table: 'benchmark_users', batch: 'true', 'values.email': $.emails, 'values.name': $.names });
  }
  UpsertMany($: { emails: unknown; names: unknown }) {
    return L.Insert({ table: 'benchmark_users', batch: 'true', 'values.email': $.emails, 'values.name': $.names, onConflict: 'email', onConflictAction: 'update' });
  }
  UpdateMany($: { ids: unknown; names: unknown }) {
    return L.Update({ table: 'benchmark_users', batch: 'true', 'key.id': $.ids, 'set.name': $.names });
  }
}

const USERS = publishBehaviors(UserReads);
const POSTS = publishBehaviors(PostReads);
const TENANTS = publishBehaviors(TenantReads);
const WRITES = publishBehaviors(UserWrites);

// ── Relations (declared batch ops, N+1-avoided) ───────────────────────────────
const postsOfUser = (dialect: OrmDialect): RelationDecl => ({
  name: 'posts', kind: 'hasMany', targetTable: 'benchmark_posts', select: ['id', 'title', 'author_id'],
  parentKey: 'id', targetKey: 'author_id', order: 'id ASC', dialect,
} as unknown as RelationDecl);
const commentsOfPost = (dialect: OrmDialect): RelationDecl => ({
  name: 'comments', kind: 'hasMany', targetTable: 'benchmark_comments', select: ['id', 'body', 'post_id'],
  parentKey: 'id', targetKey: 'post_id', order: 'id ASC', dialect,
} as unknown as RelationDecl);
const postsComposite = (dialect: OrmDialect): RelationDecl => ({
  name: 'posts', kind: 'hasMany', targetTable: 'benchmark_tenant_posts', select: ['tenant_id', 'post_id', 'user_id', 'title'],
  parentKeys: ['tenant_id', 'user_id'], targetKeys: ['tenant_id', 'user_id'], order: 'post_id ASC', dialect,
} as unknown as RelationDecl);
// ── FULL 3-level chains (#119): users→posts→comments and tenants→posts→comments (composite key). Each
// nested `childRelations` is a batched relation off the LEVEL-2 relation's rows (level-3 keyed by
// level-2's results), lowered as a batched-map chain (N+1-free per level). ─────────────────────────
const postsWithComments = (dialect: OrmDialect): RelationDecl => ({
  ...postsOfUser(dialect),
  childRelations: [commentsOfPost(dialect)],
} as unknown as RelationDecl);
const commentsOfTenantPost = (dialect: OrmDialect): RelationDecl => ({
  name: 'comments', kind: 'hasMany', targetTable: 'benchmark_tenant_comments', select: ['tenant_id', 'comment_id', 'post_id', 'body'],
  parentKeys: ['tenant_id', 'post_id'], targetKeys: ['tenant_id', 'post_id'], order: 'comment_id ASC', dialect,
} as unknown as RelationDecl);
const postsCompositeWithComments = (dialect: OrmDialect): RelationDecl => ({
  ...postsComposite(dialect),
  childRelations: [commentsOfTenantPost(dialect)],
} as unknown as RelationDecl);

// ── TX ops (RETURNING-chained; E5) — built from the shared compiler + deriveTransactionPlan ──
const txNode = (dialect: OrmDialect, id: string, component: string, ports: Record<string, unknown>) => compileWriteNode({ id, component, ports } as never, dialect);
function nestedCreatePlan(dialect: OrmDialect): TransactionPlan {
  return deriveTransactionPlan('create', [
    { op: txNode(dialect, 'u', 'Insert', { table: 'benchmark_users', 'values.email': { ref: ['email'] }, 'values.name': { ref: ['name'] }, returning: 'id', pk: 'id', autoInc: 'id' }), label: 'Insert user', name: 'user', effects: {} },
    { op: txNode(dialect, 'p', 'Insert', { table: 'benchmark_posts', 'values.author_id': { ref: ['user', 'id'] }, 'values.title': { ref: ['title'] } }), label: 'Insert post', name: 'post', effects: {} },
  ], { effects: {} }) as unknown as TransactionPlan;
}
function nestedUpdatePlan(dialect: OrmDialect): TransactionPlan {
  return deriveTransactionPlan('update', [
    { op: txNode(dialect, 'u', 'Update', { table: 'benchmark_users', 'set.name': { ref: ['name'] }, where: { arr: [{ eq: [{ ref: ['id'] }, { ref: ['user_id'] }] }] } }), label: 'Update user', name: 'user', effects: {} },
    { op: txNode(dialect, 'p', 'Update', { table: 'benchmark_posts', 'set.title': { ref: ['title'] }, where: { arr: [{ eq: [{ ref: ['author_id'] }, { ref: ['user_id'] }] }] } }), label: 'Update post', name: 'post', effects: {} },
  ], { effects: {} }) as unknown as TransactionPlan;
}
function nestedUpsertPlan(dialect: OrmDialect): TransactionPlan {
  return deriveTransactionPlan('create', [
    { op: txNode(dialect, 'u', 'Insert', { table: 'benchmark_users', 'values.email': { ref: ['email'] }, 'values.name': { ref: ['name'] }, onConflict: 'email', onConflictAction: 'update', returning: 'id', pk: 'id', autoInc: 'id' }), label: 'Upsert user', name: 'user', effects: {} },
    { op: txNode(dialect, 'p', 'Insert', { table: 'benchmark_posts', 'values.author_id': { ref: ['user', 'id'] }, 'values.title': { ref: ['title'] } }), label: 'Insert post', name: 'post', effects: {} },
  ], { effects: {} }) as unknown as TransactionPlan;
}
// delete: benchmark.ts's op is create-then-delete (a 2-statement tx — insert a fresh user, delete it by
// the RETURNING id), NOT a single DELETE. Matches the E5 txdelete shape.
function deletePlan(dialect: OrmDialect): TransactionPlan {
  return deriveTransactionPlan('create', [
    { op: txNode(dialect, 'u', 'Insert', { table: 'benchmark_users', 'values.email': { ref: ['email'] }, 'values.name': { ref: ['name'] }, returning: 'id', pk: 'id', autoInc: 'id' }), label: 'Insert user', name: 'user', effects: {} },
    { op: txNode(dialect, 'd', 'Delete', { table: 'benchmark_users', where: { arr: [{ eq: [{ ref: ['id'] }, { ref: ['user', 'id'] }] }] } }), label: 'Delete user', name: 'deleted', effects: {} },
  ], { effects: {} }) as unknown as TransactionPlan;
}
function txBundle(dialect: OrmDialect, name: string, plan: TransactionPlan): SqlBundle {
  const first = plan.statements[0].op;
  return { dialect, name, statement: { sql: first.sql, params: first.params }, optionalHeads: [], relations: {}, transaction: plan } as unknown as SqlBundle;
}

// ── The 19-op registry ────────────────────────────────────────────────────────
export interface BenchOp {
  readonly id: string; // matches contract.ts ORM_OPS id
  readonly kind: 'read' | 'read+rel' | 'write' | 'batch' | 'tx';
  readonly bundle: SqlBundle;
  readonly resolve: ColumnTypeResolver;
  readonly input: Record<string, unknown>;
  readonly withRel?: string; // relation name to prefetch (read+rel ops)
}

const BATCH_EMAILS = Array.from({ length: 10 }, (_, i) => `many${i}@bench.com`);
const BATCH_NAMES = Array.from({ length: 10 }, (_, i) => `Many ${i}`);

/**
 * Build the 19-op registry for a dialect. All per-dialect SQL is GENERATED here
 * (compileBundle/compileWriteNode/relation `dialect`) — pg=v1 byte-golden,
 * mysql/sqlite=v2 JSON-array single-param (per litedbmodel-v1-v2-sql-parity-rule).
 */
export function buildOps(dialect: OrmDialect = 'sqlite'): BenchOp[] {
  const d = dialect;
  const rv = resolveFor(d);
  const POSTS_OF_USER = postsOfUser(d);
  const POSTS_WITH_COMMENTS = postsWithComments(d);
  const POSTS_COMPOSITE_WITH_COMMENTS = postsCompositeWithComments(d);
  const B = (id: string, kind: BenchOp['kind'], bundle: SqlBundle, input: Record<string, unknown>, withRel?: string): BenchOp =>
    ({ id, kind, bundle, resolve: rv, input, ...(withRel ? { withRel } : {}) });
  return [
    // ── reads (single Select) ──
    B('findAll', 'read', compileBundle(USERS, 'FindAll', [], d, undefined, rv), {}),
    B('filterPaginateSort', 'read', compileBundle(POSTS, 'FilterPaginateSort', [], d, undefined, rv), { published: 1 }),
    B('findFirst', 'read', compileBundle(USERS, 'FindFirst', [], d, undefined, rv), { name: 'User%' }),
    B('findUnique', 'read', compileBundle(USERS, 'FindUnique', [], d, undefined, rv), { email: 'user500@example.com' }),
    // ── reads + relation (parent Select + ONE batched relation level) ──
    B('nestedFindAll', 'read+rel', compileBundle(USERS, 'FindAll', [POSTS_OF_USER], d, undefined, rv), {}, 'posts'),
    B('nestedFindFirst', 'read+rel', compileBundle(USERS, 'FindFirst', [POSTS_OF_USER], d, undefined, rv), { name: 'User%' }, 'posts'),
    B('nestedFindUnique', 'read+rel', compileBundle(USERS, 'FindUnique', [POSTS_OF_USER], d, undefined, rv), { email: 'user1@example.com' }, 'posts'),
    // nestedRelations / compositeRelations: the FULL 3-level native chains benchmark.ts defines
    // (users→posts→comments / tenant_users→tenant_posts→tenant_comments, composite key). The level-2
    // relation carries a `childRelations` level-3 batch keyed by its own result rows; injectBatchedRelations
    // lowers the chain as batched-map-over-batched-map (one query per level, N+1-free).
    B('nestedRelations', 'read+rel', compileBundle(USERS, 'FindAll', [POSTS_WITH_COMMENTS], d, undefined, rv), {}, 'posts'),
    B('compositeRelations', 'read+rel', compileBundle(TENANTS, 'ByTenant', [POSTS_COMPOSITE_WITH_COMMENTS], d, undefined, rv), { tenant_id: 1 }, 'posts'),
    // ── single writes ──
    B('create', 'write', compileBundle(WRITES, 'Create', [], d, undefined, rv), { email: 'new@bench.com', name: 'New' }),
    B('update', 'write', compileBundle(WRITES, 'Update', [], d, undefined, rv), { id: 100, name: 'Updated 100' }),
    // delete = create-then-delete tx (benchmark.ts) — see deletePlan.
    B('delete', 'tx', txBundle(d, 'Delete', deletePlan(d)), { email: 'del0@bench.com', name: 'Del' }),
    B('upsert', 'write', compileBundle(WRITES, 'Upsert', [], d, undefined, rv), { email: 'user1@example.com', name: 'Upserted One' }),
    // ── batch writes ──
    B('createMany', 'batch', compileBundle(WRITES, 'CreateMany', [], d, undefined, rv), { emails: BATCH_EMAILS, names: BATCH_NAMES }),
    B('upsertMany', 'batch', compileBundle(WRITES, 'UpsertMany', [], d, undefined, rv), { emails: ['user1@example.com', 'user2@example.com', ...BATCH_EMAILS.slice(0, 8)], names: BATCH_NAMES }),
    B('updateMany', 'batch', compileBundle(WRITES, 'UpdateMany', [], d, undefined, rv), { ids: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], names: BATCH_NAMES }),
    // ── nested-write transactions (RETURNING-chained) ──
    B('nestedCreate', 'tx', txBundle(d, 'NestedCreate', nestedCreatePlan(d)), { email: 'nc@bench.com', name: 'NC', title: 'NC Post' }),
    B('nestedUpdate', 'tx', txBundle(d, 'NestedUpdate', nestedUpdatePlan(d)), { user_id: 7, name: 'NU', title: 'NU Post' }),
    B('nestedUpsert', 'tx', txBundle(d, 'NestedUpsert', nestedUpsertPlan(d)), { email: 'user1@example.com', name: 'NUp', title: 'NUp Post' }),
  ];
}

/** Canonical SCP/BC source for the relation hard-limit oracle fixture. */
export function buildRelationLimitOracle(dialect: OrmDialect): BenchOp {
  const resolve = resolveFor(dialect);
  const relation = { ...postsOfUser(dialect), hardLimit: 1 } as RelationDecl;
  return {
    id: 'relationLimit',
    kind: 'read+rel',
    bundle: compileBundle(USERS, 'FindAll', [relation], dialect, undefined, resolve),
    resolve,
    input: {},
    withRel: 'posts',
  };
}
