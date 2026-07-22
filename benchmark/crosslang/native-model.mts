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
  benchmark_users: { id: 'INTEGER', email: 'TEXT', name: 'TEXT', created_at: 'TEXT', updated_at: 'TEXT' },
  benchmark_posts: { id: 'INTEGER', title: 'TEXT', content: 'TEXT', published: 'INTEGER', author_id: 'INTEGER', created_at: 'TEXT' },
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
}

// ── nested-write TRANSACTIONS (E5, RETURNING-chained; #142) ────────────────────────────────────────
// The DB transaction boundary (BEGIN/COMMIT/ROLLBACK + atomicity) is the CONSUMER's (litedbmodel's)
// responsibility, owned by the runtime `with_ambient_transaction` wrapper (begin_tx → runner →
// COMMIT/ROLLBACK) — NOT a bc feature and NOT emitted into the generated runner (it runs the body
// statements via `execute_sql` and returns `Result`). Each op is a COVERED typed-source `.map`:
// `write RETURNING id → id.map(id => dependent write binding that id)`. `emitWrite` stamps the RETURNING
// projection as the #59 `readColumns` port, so the source node carries a typed outType and the `.map`
// de-boxes the row (`$u.id`). `id` is declared `NOT NULL` (it IS the primary key) so the RETURNING cell
// is a non-optional scalar — consumable in the dependent statement's value position (no coalesce). The
// map runs the dependent write ONCE per RETURNING row (exactly one), so each tx is exactly 2 statements.
class BenchTx extends lm.SemanticBehavior {
  static columns = {
    benchmark_users: { id: 'INTEGER NOT NULL', email: 'TEXT', name: 'TEXT' },
    benchmark_posts: { id: 'INTEGER NOT NULL', title: 'TEXT', author_id: 'INTEGER' },
  } as const;

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
  // delete: create-then-delete tx — INSERT a fresh user RETURNING id → DELETE the created row.
  // TODO(bc#172): the TRUE semantic is id-ONLY (`DELETE WHERE id = user.id`). Today a bound scalar
  // coerces to `WireValue` (transport `&[WireValue]`) only when the node has a `value` input port
  // (bc#156); an id-only predicate has none, so it lowers to `Vec<f64>` and mismatches the transport.
  // bc#172 is being FIXED on the BC side (the emitter will coerce a bound scalar in a transport
  // value-position to `WireValue` regardless) — once that lands, revert to the pure id-only predicate.
  // Until then, INTERIM: the extra `email = $.email` (a `value` input param) keeps the bound params a
  // wire list. Faithful (deletes the exact created row) but NOT the pure id-only semantic.
  delete($: { email: unknown; name: unknown }) {
    const user = lm.emitWrite(L, 'Insert', { table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name, returning: 'id' }, 'sqlite');
    return user.map(($u: never) => lm.emitWrite(L, 'Delete', { table: 'benchmark_users', where: [lm.whereEq(($u as { id: unknown }).id, ($u as { id: unknown }).id), lm.whereEq($.email, $.email)] }, 'sqlite')).as(TX_DEP);
  }
}

// The `.map` element type for a RETURNING-chained tx: each dependent write is the `executeSQL` leaf,
// whose result is the `many`-cardinality write-summary LIST (`[{…}]`). bc's map records the element
// outType from the leaf's element OUTPUT (`{obj:{}}`), not its cardinality-adjusted list result — so the
// map is annotated with the correct element type (`{arr:{obj:{}}}` — the dependent write returns a list,
// the map result is a list-of-lists). The tx result is unused (the tx runs for its effects).
const TX_DEP = { arr: { obj: {} } } as const;

// Input Port type strings (erased at runtime; bc records the port NAMES from `$` access — these carry
// the declared type into the IR). All bound values are opaque wire (`value`) — the native leaf binds them.
const v = (...k: string[]): Record<string, { type: 'value'; required: true }> => Object.fromEntries(k.map((x) => [x, { type: 'value' as const, required: true }]));
const inputPorts: Record<string, Record<string, { type: 'value'; required: true }>> = {
  filterPaginateSort: v('published'),
  findFirst: v('name'), findUnique: v('email'),
  nestedFindFirst: v('name'), nestedFindUnique: v('email'),
  create: v('email', 'name'), update: v('id', 'name'), upsert: v('email', 'name'),
  createMany: v('rows'), upsertMany: v('rows'), updateMany: v('rows'),
};
const inputPortsTx: Record<string, Record<string, { type: 'value'; required: true }>> = {
  nestedCreate: v('email', 'name', 'title'), nestedUpsert: v('email', 'name', 'title'),
  nestedUpdate: v('id', 'name', 'title'), delete: v('email', 'name'),
};

// PUBLISH → the native-clean contract (nativePassthrough expresses #164 + strips `materializers` at the
// publish layer). The DUMP is verbatim: nothing runs between publish and JSON.stringify. The reads/single
// writes/batches (Bench) and the RETURNING-chained transactions (BenchTx, NOT NULL id) publish as two
// contracts and their component lists concat into the ONE dumped IR (byte-identical to a single publish;
// the two classes only differ in their `static columns` nullability declaration).
const contract = lm.publishBehaviors(Bench, { dialect: 'sqlite', nativePassthrough: true, inputPorts });
const contractTx = lm.publishBehaviors(BenchTx, { dialect: 'sqlite', nativePassthrough: true, inputPorts: inputPortsTx });
const ir = { ...contract.ir, components: [...contract.ir.components, ...contractTx.ir.components] };
mkdirSync(IR_DIR, { recursive: true });
writeFileSync(IR_PATH, JSON.stringify(ir));
console.log(`native.ir.json: ${ir.components.length} components → ${IR_PATH}`);
console.log('components:', ir.components.map((c) => c.name).join(', '));
