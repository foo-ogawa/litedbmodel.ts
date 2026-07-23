// ════════════════════════════════════════════════════════════════════════════
// ORM-bench NATIVE model authoring (#141) — the litedbmodel SCP surface for the cross-lang bench.
//
// This module ONLY authors the ORM ops on litedbmodel's public SCP surface (`emitRead` / `emitWrite`
// / relation `pluck`/`group` graph), publishes them through bc's `publishBehaviors`, and DUMPS the
// resulting `contract.ir` VERBATIM to `.ir/native.ir.json`. NOTHING transforms the IR after publish:
//   • #164 output-passthrough (intermediate nodes stay opaque `WireValue`) is expressed at the
//     PUBLISH layer via `publishBehaviors({ nativePassthrough:true })` (src/scp/authoring.ts) — NOT
//     here, and NOT after publish.
//   • the TS-only `materializers` port is removed at the SAME publish layer (native-clean contract).
//
// The native rust module is generated from the dumped IR by the bc CLI (`bc generate`, see
// gen-native.sh) — litedbmodel writes NO native code and runs NO custom generator. The bc drift gate
// (`bc check`) verifies the committed `behaviors_generated.rs` is verbatim bc output.
//
// The 19-op SSoT semantics live in `ops.ts`; the SQL bodies below match it (same schema/seed as
// `orm-domain.ts`). Ops that are NOT yet expressible on the bc 0.8.16 native surface after exhausting
// the authoring surface are listed in NATIVE_RELATION_PLAN.md with the precise blocker.
// ════════════════════════════════════════════════════════════════════════════
import { writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as lm from '../../dist/scp/index.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const IR_DIR = join(HERE, '.ir');
const IR_PATH = join(IR_DIR, 'native.ir.json');
const L = lm.components();

// The merged column SoT — ONE class, ONE outType universe (every table the ops project).
const COLUMNS = {
  // `id` is `INTEGER NOT NULL` (it IS the PK): the RETURNING-chained tx ops need a non-optional RETURNING
  // scalar (`$u.id` consumable in the dependent statement's value position, no coalesce). Non-tx ops are
  // unaffected (id is always present). This lets ALL 19 ops live in ONE class → ONE publish (no hand-merge).
  benchmark_users: { id: 'INTEGER NOT NULL', email: 'TEXT', name: 'TEXT', created_at: 'TEXT', updated_at: 'TEXT' },
  benchmark_posts: { id: 'INTEGER NOT NULL', title: 'TEXT', content: 'TEXT', published: 'INTEGER', author_id: 'INTEGER', created_at: 'TEXT' },
  benchmark_comments: { id: 'INTEGER', body: 'TEXT', post_id: 'INTEGER', created_at: 'TEXT' },
  // composite-key (multi-column PK) tables — the compositeRelations 3-level chain projects these.
  benchmark_tenant_users: { tenant_id: 'INTEGER', user_id: 'INTEGER', name: 'TEXT' },
  benchmark_tenant_posts: { tenant_id: 'INTEGER', post_id: 'INTEGER', user_id: 'INTEGER', title: 'TEXT' },
  benchmark_tenant_comments: { tenant_id: 'INTEGER', comment_id: 'INTEGER', post_id: 'INTEGER', body: 'TEXT' },
} as const;

// The single-key relation declarations (2- and 3-level chains) — the ops.ts SoT. The COMPOSITE-key
// relations (compositeRelations) are authored below via the SAME graph: the `pluck`/`group` leaves take
// the key-column TUPLE (`col`/`pk`/`fk` : `{arr:'string'}`), so a composite parent/target key set groups
// by the whole tuple identity (NATIVE_RELATION_PLAN.md B-2 — leaf-port widening; NOT a bc gap).
const postsOfUser = { name: 'posts', kind: 'hasMany', targetTable: 'benchmark_posts', select: ['id', 'title', 'author_id'], parentKey: 'id', targetKey: 'author_id', order: 'id ASC', dialect: 'sqlite' } as const;
const commentsOfPost = { name: 'comments', kind: 'hasMany', targetTable: 'benchmark_comments', select: ['id', 'body', 'post_id'], parentKey: 'id', targetKey: 'post_id', order: 'id ASC', dialect: 'sqlite' } as const;
const postsWithComments = { ...postsOfUser, childRelations: [commentsOfPost] };

// COMPOSITE-key (multi-column) relation chain (#141 B-2): tenant_users → tenant_posts → tenant_comments,
// keyed on 2-column tuples. The pluck/group leaves now take the key-column TUPLE (widened `{arr:'string'}`
// ports), so the composite key groups by the WHOLE tuple identity (no `''`-scalar-collapse cartesian).
const postsComposite = { name: 'posts', kind: 'hasMany', targetTable: 'benchmark_tenant_posts', select: ['tenant_id', 'post_id', 'user_id', 'title'], parentKeys: ['tenant_id', 'user_id'], targetKeys: ['tenant_id', 'user_id'], order: 'post_id ASC', dialect: 'sqlite' } as const;
const commentsOfTenantPost = { name: 'comments', kind: 'hasMany', targetTable: 'benchmark_tenant_comments', select: ['tenant_id', 'comment_id', 'post_id', 'body'], parentKeys: ['tenant_id', 'post_id'], targetKeys: ['tenant_id', 'post_id'], order: 'comment_id ASC', dialect: 'sqlite' } as const;
const postsCompositeWithComments = { ...postsComposite, childRelations: [commentsOfTenantPost] };

// ── The ONE covered behavior class — each method authors one op via emitRead/emitWrite + relations. ──
// Every method body is a bare op-builder call (no native control syntax); the port assembly + op→sql
// lowering are EXTERNAL helpers (bc's native-control scan reads only the method frame). Read WHEREs are
// deferred to the transient `where` port (lowered post-compile); relations author the pluck/group graph.
class Bench extends lm.SemanticBehavior {
  static columns = COLUMNS;

  // ── reads (single Select) ──
  findAll() { return lm.emitRead(L, 'Select', { table: 'benchmark_users', select: ['id', 'email', 'name'], order: 'id ASC', limit: 100 }, 'sqlite'); }
  filterPaginateSort($: { published: unknown }) { return lm.emitRead(L, 'Select', { table: 'benchmark_posts', select: ['id', 'title', 'content', 'published', 'author_id', 'created_at'], where: [lm.whereEq($.published, $.published)], order: 'created_at DESC', limit: 20, offset: 10 }, 'sqlite'); }
  findFirst($: { name: unknown }) { return lm.emitRead(L, 'Select', { table: 'benchmark_users', select: ['id', 'email', 'name'], where: [lm.whereLike($ as never, 'name', $.name)], limit: 1 }, 'sqlite'); }
  findUnique($: { email: unknown }) { return lm.emitRead(L, 'Select', { table: 'benchmark_users', select: ['id', 'email', 'name'], where: [lm.whereEq($.email, $.email)], limit: 1 }, 'sqlite'); }

  // ── reads + relation (parent Select + one batched relation level, N+1-free) ──
  nestedFindAll($: unknown) { return lm.relationReadAuthoring('benchmark_users', { select: ['id', 'email', 'name'], limit: 100 }, [postsOfUser], 'sqlite')($ as never, L); }
  nestedFindFirst($: { name: unknown }) { return lm.relationReadAuthoring('benchmark_users', { select: ['id', 'email', 'name'], where: (r: never) => [lm.whereLike(r, 'name', ($ as { name: unknown }).name)], limit: 1 }, [postsOfUser], 'sqlite')($ as never, L); }
  nestedFindUnique($: { email: unknown }) { return lm.relationReadAuthoring('benchmark_users', { select: ['id', 'email', 'name'], where: (r: never) => [lm.whereEq(($ as { email: unknown }).email, ($ as { email: unknown }).email)], limit: 1 }, [postsOfUser], 'sqlite')($ as never, L); }
  // FULL 3-level chain (#119): users→posts→comments (single-key, one batched query per level).
  nestedRelations($: unknown) { return lm.relationReadAuthoring('benchmark_users', { select: ['id', 'email', 'name'], limit: 100 }, [postsWithComments], 'sqlite')($ as never, L); }
  // COMPOSITE-key 3-level chain: tenant_users → tenant_posts → tenant_comments (2-column keys). One
  // batched query per level (N+1-free): compositeRelations = 3 queries; grouped by the full key tuple.
  compositeRelations($: unknown) { return lm.relationReadAuthoring('benchmark_tenant_users', { select: ['tenant_id', 'user_id', 'name'], order: 'user_id ASC', limit: 100 }, [postsCompositeWithComments], 'sqlite')($ as never, L); }

  // ── single writes ──
  create($: { email: unknown; name: unknown }) { return lm.emitWrite(L, 'Insert', { table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name }, 'sqlite'); }
  update($: { id: unknown; name: unknown }) { return lm.emitWrite(L, 'Update', { table: 'benchmark_users', 'set.name': $.name, where: [lm.whereEq($.id, $.id)] }, 'sqlite'); }
  upsert($: { email: unknown; name: unknown }) { return lm.emitWrite(L, 'Insert', { table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name, onConflict: 'email', onConflictAction: 'update', returning: 'id', pk: 'id', autoInc: 'id' }, 'sqlite'); }

  // ── batch writes (ONE statement per op — the json_each/JSON_TABLE batch form binds the whole record
  //    set as ONE opaque `rows` array value; N+1-free by construction: 1 query). ──
  createMany($: { rows: unknown }) { return lm.emitBatchWrite(L, 'Insert', { table: 'benchmark_users', columns: ['email', 'name'], rows: $.rows as never }, 'sqlite'); }
  upsertMany($: { rows: unknown }) { return lm.emitBatchWrite(L, 'Insert', { table: 'benchmark_users', columns: ['email', 'name'], rows: $.rows as never, onConflict: 'email', onConflictAction: 'update' }, 'sqlite'); }
  updateMany($: { rows: unknown }) { return lm.emitBatchWrite(L, 'Update', { table: 'benchmark_users', columns: ['name'], keyColumns: ['id'], rows: $.rows as never }, 'sqlite'); }

  // ── nested-write TRANSACTIONS (E5, RETURNING-chained; #142) ──────────────────────────────────────
  // The DB transaction boundary (BEGIN/COMMIT/ROLLBACK + atomicity) is the CONSUMER's (litedbmodel's)
  // responsibility, owned by the runtime `with_ambient_transaction` wrapper (begin_tx → runner →
  // COMMIT/ROLLBACK) — NOT a bc feature and NOT emitted into the generated runner (it runs the body
  // statements via `execute_sql` and returns `Result`). Each op is a COVERED typed-source `.map`:
  // `write RETURNING id → id.map(id => dependent write binding that id)`. `emitWrite` stamps the RETURNING
  // projection as the #59 `readColumns` port, so the source node carries a typed outType and the `.map`
  // de-boxes the row (`$u.id`); `id` is `NOT NULL` (COLUMNS) so the RETURNING cell is a non-optional
  // scalar consumable in the dependent statement's value position (no coalesce). Each tx = 2 statements.
  // nestedCreate: INSERT user RETURNING id → INSERT post with author_id = the new user's id.
  nestedCreate($: { email: unknown; name: unknown; title: unknown }) {
    const user = lm.emitWrite(L, 'Insert', { table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name, returning: 'id' }, 'sqlite');
    return user.map(($u: never) => lm.emitWrite(L, 'Insert', { table: 'benchmark_posts', 'values.author_id': ($u as { id: unknown }).id, 'values.title': $.title }, 'sqlite')).as(TX_DEP);
  }
  // nestedUpsert: INSERT user ON CONFLICT DO UPDATE RETURNING id → INSERT post with author_id = that id.
  nestedUpsert($: { email: unknown; name: unknown; title: unknown }) {
    const user = lm.emitWrite(L, 'Insert', { table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name, onConflict: 'email', onConflictAction: 'update', returning: 'id' }, 'sqlite');
    return user.map(($u: never) => lm.emitWrite(L, 'Insert', { table: 'benchmark_posts', 'values.author_id': ($u as { id: unknown }).id, 'values.title': $.title }, 'sqlite')).as(TX_DEP);
  }
  // nestedUpdate: UPDATE user SET name WHERE id RETURNING id → UPDATE that user's posts SET title.
  nestedUpdate($: { id: unknown; name: unknown; title: unknown }) {
    const user = lm.emitWrite(L, 'Update', { table: 'benchmark_users', 'set.name': $.name, where: [lm.whereEq($.id, $.id)], returning: 'id' }, 'sqlite');
    return user.map(($u: never) => lm.emitWrite(L, 'Update', { table: 'benchmark_posts', 'set.title': $.title, where: [lm.whereEq(($u as { author_id: unknown }).author_id, ($u as { id: unknown }).id)] }, 'sqlite')).as(TX_DEP);
  }
  // delete: create-then-delete tx — INSERT a fresh user RETURNING id → DELETE the created row by its
  // id ALONE (`DELETE WHERE id = user.id`). Under bc 0.9.0 the port boundary honors the declared
  // portSchema: the `params` transport port is declared `{arr:'value'}` so the bound RETURNING scalar
  // materializes to `WireValue` in the transport value-position regardless of a `value` input port
  // (bc#172 resolved). The map runs the dependent DELETE once per RETURNING row (exactly one).
  delete($: { email: unknown; name: unknown }) {
    const user = lm.emitWrite(L, 'Insert', { table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name, returning: 'id' }, 'sqlite');
    return user.map(($u: never) => lm.emitWrite(L, 'Delete', { table: 'benchmark_users', where: [lm.whereEq(($u as { id: unknown }).id, ($u as { id: unknown }).id)] }, 'sqlite')).as(TX_DEP);
  }
}

// The `.map` element type for a RETURNING-chained tx: each dependent write is the `executeSQL` leaf,
// whose result is the `many`-cardinality write-summary LIST (`[{…}]`). bc's map records the element
// outType from the leaf's element OUTPUT (`{obj:{}}`), not its cardinality-adjusted list result — so the
// map is annotated with the correct element type (`{arr:{obj:{}}}` — the dependent write returns a list,
// the map result is a list-of-lists). The tx result is unused (the tx runs for its effects).
const TX_DEP = { arr: { obj: {} } } as const;

// Input Port type declarations (bc records the port NAMES from `$` access; the TS `In<>` type is erased
// at runtime, so the DECLARED type is carried into the IR here — the input side's SSoT). Scalar inputs
// declare their real type (`string`/`int`, per the `COLUMNS` SQL types), so bc emits NATIVE-typed input
// structs (`InNRCreate { email: String, name: String }`) and the bench harness passes native values —
// no hand-boxing. `type` follows the column's SQL type: TEXT→`string`, INTEGER→`int`.
type PortDecl = Record<string, { type: string; required: true; elemType?: unknown }>;
// Scalar ports: the SQL type (TEXT→string, INTEGER→int).
const p = (spec: Record<string, 'string' | 'int'>): PortDecl =>
  Object.fromEntries(Object.entries(spec).map(([k, t]) => [k, { type: t, required: true as const }]));
// The batch record-set port `rows`: the bc-canonical PortableType shape (`type:'array'` + `elemType` the
// row `obj`). bc emits a native `Vec<Row>` input and boxes typed→wire at the leaf-param boundary (bc#178).
const rows = (elem: Record<string, 'string' | 'int'>): PortDecl =>
  ({ rows: { required: true, type: 'array', elemType: { obj: elem } } });
const inputPorts: Record<string, PortDecl> = {
  filterPaginateSort: p({ published: 'int' }),
  findFirst: p({ name: 'string' }), findUnique: p({ email: 'string' }),
  nestedFindFirst: p({ name: 'string' }), nestedFindUnique: p({ email: 'string' }),
  create: p({ email: 'string', name: 'string' }), update: p({ id: 'int', name: 'string' }), upsert: p({ email: 'string', name: 'string' }),
  // batch record-set input: the bc-canonical PortableType shape — `type:'array'` + `elemType` the row
  // `obj`. bc emits a native `Vec<Row>` input (Row = the elemType struct) and generates the typed→wire
  // box at the leaf-param boundary itself (bc#178). The harness passes native rows — no wire hand-build.
  createMany: rows({ email: 'string', name: 'string' }), upsertMany: rows({ email: 'string', name: 'string' }),
  updateMany: rows({ id: 'int', name: 'string' }),
  nestedCreate: p({ email: 'string', name: 'string', title: 'string' }), nestedUpsert: p({ email: 'string', name: 'string', title: 'string' }),
  nestedUpdate: p({ id: 'int', name: 'string', title: 'string' }), delete: p({ email: 'string', name: 'string' }),
};

// PUBLISH → the native-clean contract (nativePassthrough expresses #164 + strips `materializers` at the
// publish layer). ALL 19 ops are ONE class → ONE `publishBehaviors`; the IR is dumped VERBATIM (no
// hand-merge, no post-publish transform — nothing runs between publish and JSON.stringify). The IR is
// exactly what bc's `compileBehaviors` produced; `bc generate` consumes it via the CLI.
const contract = lm.publishBehaviors(Bench, { dialect: 'sqlite', nativePassthrough: true, inputPorts });
const ir = contract.ir;
mkdirSync(IR_DIR, { recursive: true });
writeFileSync(IR_PATH, JSON.stringify(ir));
console.log(`native.ir.json: ${ir.components.length} components → ${IR_PATH}`);
console.log('components:', ir.components.map((c) => c.name).join(', '));
