// ════════════════════════════════════════════════════════════════════════════
// ORM-bench NATIVE generation (#141 step6/7) — bc-DELEGATED ONLY (no litedbmodel generator).
// Authors the ops on the SCP surface (emitRead / emitWrite / relation pluck-group graph) as methods of
// ONE behavior class, so bc `generateModule({language:'rust-typed-native', leafTransport})` emits ONE
// module with ONE shared `WireValue` + one op-agnostic transport (execute_sql/pluck_keys/group_children).
// Emits `rust/orm_bench/src/gen/behaviors_generated.rs` (+ generated_setup.rs from the ddl SSoT).
// ════════════════════════════════════════════════════════════════════════════
import { writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as lm from '../../dist/scp/index.mjs';
import { ddl } from './orm-domain.ts';

const GEN = join(dirname(fileURLToPath(import.meta.url)), '..', '..', 'rust', 'orm_bench', 'src', 'gen');
const RT = 'litedbmodel_runtime';
const LT = { symbols: { executeSQL: 'execute_sql', pluck: 'pluck_keys', group: 'group_children' }, import: RT };
const L = lm.components();

// Merged column SoT (all tables the ops project) — ONE class, ONE outType universe.
const COLUMNS = {
  benchmark_users: { id: 'INTEGER', email: 'TEXT', name: 'TEXT', created_at: 'TEXT', updated_at: 'TEXT' },
  benchmark_posts: { id: 'INTEGER', title: 'TEXT', content: 'TEXT', published: 'INTEGER', author_id: 'INTEGER', created_at: 'TEXT' },
  benchmark_comments: { id: 'INTEGER', body: 'TEXT', post_id: 'INTEGER', created_at: 'TEXT' },
  benchmark_tenant_users: { tenant_id: 'INTEGER', user_id: 'INTEGER', name: 'TEXT' },
  benchmark_tenant_posts: { tenant_id: 'INTEGER', post_id: 'INTEGER', user_id: 'INTEGER', title: 'TEXT' },
  benchmark_tenant_comments: { tenant_id: 'INTEGER', comment_id: 'INTEGER', post_id: 'INTEGER', body: 'TEXT' },
};
const postsOfUser = { name: 'posts', kind: 'hasMany', targetTable: 'benchmark_posts', select: ['id', 'title', 'author_id'], parentKey: 'id', targetKey: 'author_id', order: 'id ASC', dialect: 'sqlite' };
const commentsOfPost = { name: 'comments', kind: 'hasMany', targetTable: 'benchmark_comments', select: ['id', 'body', 'post_id'], parentKey: 'id', targetKey: 'post_id', order: 'id ASC', dialect: 'sqlite' };
const postsWithComments = { ...postsOfUser, childRelations: [commentsOfPost] };
const postsComposite = { name: 'posts', kind: 'hasMany', targetTable: 'benchmark_tenant_posts', select: ['tenant_id', 'post_id', 'user_id', 'title'], parentKeys: ['tenant_id', 'user_id'], targetKeys: ['tenant_id', 'user_id'], order: 'post_id ASC', dialect: 'sqlite' };
const v = (...k: string[]) => Object.fromEntries(k.map((x) => [x, { type: 'value', required: true }]));

// ONE behavior class: each method = one covered op. (batch createMany/upsertMany/updateMany and the
// RETURNING-chained tx ops are NOT native-covered on rust-typed-native — see report; omitted here so
// the generated module stays fully-covered/runtime-free.)
class Bench extends lm.SemanticBehavior {
  static columns = COLUMNS;
  findAll() { return lm.emitRead(L, 'Select', { table: 'benchmark_users', select: ['id', 'email', 'name'], order: 'id ASC', limit: 100 }, 'sqlite'); }
  filterPaginateSort($: any) { return lm.emitRead(L, 'Select', { table: 'benchmark_posts', select: ['id', 'title', 'content', 'published', 'author_id', 'created_at'], where: [lm.whereEq($.published, $.published)], order: 'created_at DESC', limit: 20, offset: 10 }, 'sqlite'); }
  findFirst($: any) { return lm.emitRead(L, 'Select', { table: 'benchmark_users', select: ['id', 'email', 'name'], where: [lm.whereLike($, 'name', $.name)], limit: 1 }, 'sqlite'); }
  findUnique($: any) { return lm.emitRead(L, 'Select', { table: 'benchmark_users', select: ['id', 'email', 'name'], where: [lm.whereEq($.email, $.email)], limit: 1 }, 'sqlite'); }
  nestedFindAll($: any) { return lm.relationReadAuthoring('benchmark_users', { select: ['id', 'email', 'name'], limit: 100 }, [postsOfUser], 'sqlite')($, L); }
  nestedFindFirst($: any) { return lm.relationReadAuthoring('benchmark_users', { select: ['id', 'email', 'name'], where: (r: any) => [lm.whereLike(r, 'name', r.name)], limit: 1 }, [postsOfUser], 'sqlite')($, L); }
  nestedFindUnique($: any) { return lm.relationReadAuthoring('benchmark_users', { select: ['id', 'email', 'name'], where: (r: any) => [lm.whereEq(r.email, r.email)], limit: 1 }, [postsOfUser], 'sqlite')($, L); }
  nestedRelations($: any) { return lm.relationReadAuthoring('benchmark_users', { select: ['id', 'email', 'name'], limit: 100 }, [postsWithComments], 'sqlite')($, L); }
  compositeRelations($: any) { return lm.relationReadAuthoring('benchmark_tenant_users', { select: ['tenant_id', 'user_id', 'name'], where: (r: any) => [lm.whereEq(r.tenant_id, r.tenant_id)], order: 'user_id ASC' }, [postsComposite], 'sqlite')($, L); }
  create($: any) { return lm.emitWrite(L, 'Insert', { table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name }, 'sqlite'); }
  update($: any) { return lm.emitWrite(L, 'Update', { table: 'benchmark_users', 'set.name': $.name, where: [lm.whereEq($.id, $.id)] }, 'sqlite'); }
  upsert($: any) { return lm.emitWrite(L, 'Insert', { table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name, onConflict: 'email', onConflictAction: 'update', returning: 'id', pk: 'id', autoInc: 'id' }, 'sqlite'); }
}

const inputPorts = {
  filterPaginateSort: v('published'), findFirst: v('name'), findUnique: v('email'),
  nestedFindFirst: v('name'), nestedFindUnique: v('email'), compositeRelations: v('tenant_id'),
  create: v('email', 'name'), update: v('id', 'name'), upsert: v('email', 'name'),
};

const contract = lm.publishBehaviors(Bench, { dialect: 'sqlite', inputPorts });
// Strip the TS-only `materializers` port (native output de-box is BC-generated from outType, #154).
for (const c of contract.components) for (const n of c.body) if (n.ports && n.ports.materializers) delete n.ports.materializers;
const mod = lm.generateModule(contract.ir, { language: 'rust-typed-native', runtimeImport: RT, leafTransport: LT });
writeFileSync(join(GEN, 'behaviors_generated.rs'), '#![allow(non_snake_case, unused_imports, dead_code, clippy::all)]\n' + mod.code);

// setup module from the ddl SSoT.
const setup = '// GENERATED native setup; no serialized sidecar.\npub const STATEMENTS: &[&str] = &[\n' +
  ddl('sqlite').map((s) => `    ${JSON.stringify(s)},`).join('\n') + '\n];\n';
writeFileSync(join(GEN, 'generated_setup.rs'), setup);
writeFileSync(join(GEN, 'mod.rs'), '#![allow(non_snake_case, unused_imports, clippy::all)]\n// GENERATED by gen-native.mts (bc-delegated).\npub mod behaviors_generated;\npub mod generated_setup;\n');

console.log('components:', contract.components.map((c) => c.name).join(', '));
console.log('WireValue defs:', (mod.code.match(/pub enum WireValue/g) || []).length);
console.log('run fns:', (mod.code.match(/pub fn run_native_raw_struct_\w+/g) || []).length);
console.log('transport imports:', (mod.code.match(/use litedbmodel_runtime::\{[^}]*\}/) || ['?'])[0]);
