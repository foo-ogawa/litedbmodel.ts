// Main-bench TS cell — the CLI-generated typescript-typed modules (baked STRAIGHT-LINE: no runBehavior
// tree-walk, no IR walk, no JSON.parse — only bc's codegen-primitive value helpers) + a thin exec seam
// (the Select/Insert/Update/Delete component handlers) + the SDK baseline. This round: the 10 flat ops
// (reads / single writes / batch). read+rel + tx are the next ts slice (per-op child-key + tx envelope).
//
//   tsx main.ts run <op> <db> <native|sdk>   → print the canonical result (node driver byte-compares)
import Database from 'better-sqlite3';
import { copyFileSync, rmSync, writeFileSync } from 'node:fs';

import { bind as bindFindAll } from './generated/gen_findall.ts';
import { bind as bindFilterPaginateSort } from './generated/gen_filterpaginatesort.ts';
import { bind as bindFindFirst } from './generated/gen_findfirst.ts';
import { bind as bindFindUnique } from './generated/gen_findunique.ts';
import { bind as bindCreate } from './generated/gen_create.ts';
import { bind as bindUpdate } from './generated/gen_update.ts';
import { bind as bindUpsert } from './generated/gen_upsert.ts';
import { bind as bindCreateMany } from './generated/gen_createmany.ts';
import { bind as bindUpsertMany } from './generated/gen_upsertmany.ts';
import { bind as bindUpdateMany } from './generated/gen_updatemany.ts';
import { bind as bindNestedFindAll } from './generated/gen_nestedfindall.ts';
import { bind as bindNestedFindFirst } from './generated/gen_nestedfindfirst.ts';
import { bind as bindNestedFindUnique } from './generated/gen_nestedfindunique.ts';
import { bind as bindNestedRelations } from './generated/gen_nestedrelations.ts';
import { bind as bindCompositeRelations } from './generated/gen_compositerelations.ts';
import { bind as bindDelete } from './generated/gen_delete.ts';
import { bind as bindNestedCreate } from './generated/gen_nestedcreate.ts';
import { bind as bindNestedUpdate } from './generated/gen_nestedupdate.ts';
import { bind as bindNestedUpsert } from './generated/gen_nestedupsert.ts';

// ── canonical serialization (matches oracle.ts canonVal/canonRow) ──
function canonVal(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'number' || typeof v === 'bigint') return String(v);
  if (typeof v === 'boolean') return v ? '1' : '0';
  return JSON.stringify(String(v));
}
function canonRow(row: Record<string, unknown>, fields: readonly string[]): string {
  return '{' + fields.map((f) => `${JSON.stringify(f)}:${canonVal(row[f])}`).join(',') + '}';
}
function canonRows(rows: Record<string, unknown>[], fields: readonly string[]): string {
  return '[' + rows.map((r) => canonRow(r, fields)).join(',') + ']';
}
const FIELDS: Record<string, string[]> = {
  findAll: ['id', 'email', 'name'], filterPaginateSort: ['id', 'title', 'content', 'published', 'author_id', 'created_at'],
  findFirst: ['id', 'email', 'name'], findUnique: ['id', 'email', 'name'],
  create: ['id', 'email', 'name'], update: ['id', 'email', 'name'], upsert: ['id', 'email', 'name'],
  createMany: ['id', 'email', 'name'], upsertMany: ['id', 'email', 'name'], updateMany: ['id', 'email', 'name'],
};
// batch column order per op (v0,v1 → columns for the json_each records the baked SQL reads).
const BATCH_COLS: Record<string, [string, string]> = { createMany: ['email', 'name'], upsertMany: ['email', 'name'], updateMany: ['id', 'name'] };

// ── the thin exec seam: component handlers over better-sqlite3 (baked SQL from the ports) ──
type Handler = (ports: Record<string, unknown>, ctx: unknown) => { ok: unknown } | { error: string };

function scalarParams(ports: Record<string, unknown>): unknown[] {
  return Object.keys(ports).filter((k) => /^p\d+$/.test(k)).sort((a, b) => Number(a.slice(1)) - Number(b.slice(1))).map((k) => ports[k]);
}
/** Build the `[{col:val,…},…]` json (one string) the baked json_each(?) expands, from parallel arrays. */
function batchJson(ports: Record<string, unknown>, cols: [string, string]): string {
  const v0 = ports['v0'] as unknown[];
  const v1 = ports['v1'] as unknown[];
  return JSON.stringify(v0.map((_, i) => ({ [cols[0]]: v0[i], [cols[1]]: v1[i] })));
}
function makeHandlers(db: InstanceType<typeof Database>, batchCols?: [string, string]): Record<string, Handler> {
  const rowsOrRun = (ports: Record<string, unknown>): { ok: unknown } => {
    const sql = ports['sql'] as string;
    if ('v0' in ports) {
      // batch write: bind the SAME records-json to every `?`.
      const json = batchJson(ports, batchCols!);
      const n = (sql.match(/\?/g) ?? []).length;
      return { ok: db.prepare(sql).all(...Array.from({ length: n }, () => json)) };
    }
    const params = scalarParams(ports);
    if (/\breturning\b/i.test(sql) || /^\s*select\b/i.test(sql)) return { ok: db.prepare(sql).all(...params) };
    const info = db.prepare(sql).run(...params);
    return { ok: { changes: info.changes, lastInsertRowid: Number(info.lastInsertRowid) } };
  };
  const h: Handler = (ports) => rowsOrRun(ports);
  return { Select: h, Insert: h, Update: h, Delete: h };
}

// ── SDK baseline — raw better-sqlite3 + the SAME hand-SQL as rust/go ──
const SDK_SQL: Record<string, { sql: string; params: unknown[] }> = {
  findAll: { sql: 'SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100', params: [] },
  filterPaginateSort: { sql: 'SELECT id, title, content, published, author_id, created_at FROM benchmark_posts WHERE published = ? ORDER BY created_at DESC LIMIT 20 OFFSET 10', params: [1] },
  findFirst: { sql: 'SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1', params: ['User%'] },
  findUnique: { sql: 'SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1', params: ['user500@example.com'] },
  create: { sql: 'INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id, email, name', params: ['new@bench.com', 'New'] },
  update: { sql: 'UPDATE benchmark_users SET name = ? WHERE id = ? RETURNING id, email, name', params: ['Updated 100', 100] },
  upsert: { sql: 'INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id, email, name', params: ['user1@example.com', 'Upserted One'] },
};
const BATCH_EMAILS = Array.from({ length: 10 }, (_, i) => `many${i}@bench.com`);
const BATCH_NAMES = Array.from({ length: 10 }, (_, i) => `Many ${i}`);
const UPSERTMANY_EMAILS = ['user1@example.com', 'user2@example.com', ...BATCH_EMAILS.slice(0, 8)];

function sdkRows(db: InstanceType<typeof Database>, op: string): Record<string, unknown>[] {
  if (op in SDK_SQL) { const { sql, params } = SDK_SQL[op]; return db.prepare(sql).all(...params) as Record<string, unknown>[]; }
  if (op === 'createMany' || op === 'upsertMany') {
    const emails = op === 'createMany' ? BATCH_EMAILS : UPSERTMANY_EMAILS;
    const ph = emails.map(() => '(?, ?)').join(', ');
    const conflict = op === 'upsertMany' ? ' ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name' : '';
    const params: unknown[] = [];
    emails.forEach((e, i) => params.push(e, BATCH_NAMES[i]));
    return db.prepare(`INSERT INTO benchmark_users (email, name) VALUES ${ph}${conflict} RETURNING id, email, name`).all(...params) as Record<string, unknown>[];
  }
  if (op === 'updateMany') {
    const cases = Array.from({ length: 10 }, (_, i) => `WHEN ${i + 1} THEN ?`).join(' ');
    return db.prepare(`UPDATE benchmark_users SET name = CASE id ${cases} END WHERE id IN (1,2,3,4,5,6,7,8,9,10) RETURNING id, email, name`).all(...BATCH_NAMES) as Record<string, unknown>[];
  }
  throw new Error(`sdk: op '${op}' not in this cell`);
}

// ══ read+rel (2-level slice; 3-level is #119) — {rows,rel} + the batched-map handler ══════════════════
interface RelCfg {
  rel: string; comp: string; parentF: string[]; childF: string[];
  input: Record<string, unknown>; bind: (h: never) => Record<string, (i?: unknown) => unknown>;
  childKey: (c: Record<string, unknown>) => string; itemKey: (it: Record<string, unknown>) => string; itemVal: (it: Record<string, unknown>) => unknown;
  // SDK: parent query + IN child query + group/align (N+1-avoided). parentKey extracts the IN key.
  parentSql: string; parentParams: unknown[]; childSqlFmt: string; parentKey: (r: Record<string, unknown>) => unknown;
}
const singleKeyPost = { childF: ['id', 'title', 'author_id'], childSqlFmt: 'SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN ({IN}) ORDER BY id ASC', childKey: (c: Record<string, unknown>) => String(c.author_id), itemKey: (it: Record<string, unknown>) => String(it.k0), itemVal: (it: Record<string, unknown>) => it.k0, parentKey: (r: Record<string, unknown>) => r.id };
const REL: Record<string, RelCfg> = {
  nestedFindAll: { rel: 'posts', comp: 'FindAll', parentF: ['id', 'email', 'name'], input: {}, bind: bindNestedFindAll as never, parentSql: 'SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100', parentParams: [], ...singleKeyPost },
  nestedFindFirst: { rel: 'posts', comp: 'FindFirst', parentF: ['id', 'email', 'name'], input: { name: 'User%' }, bind: bindNestedFindFirst as never, parentSql: 'SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1', parentParams: ['User%'], ...singleKeyPost },
  nestedFindUnique: { rel: 'posts', comp: 'FindUnique', parentF: ['id', 'email', 'name'], input: { email: 'user1@example.com' }, bind: bindNestedFindUnique as never, parentSql: 'SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1', parentParams: ['user1@example.com'], ...singleKeyPost },
  nestedRelations: { rel: 'comments', comp: 'ByAuthor', parentF: ['id', 'title', 'author_id'], childF: ['id', 'body', 'post_id'], input: { author_id: 7 }, bind: bindNestedRelations as never, childKey: (c) => String(c.post_id), itemKey: (it) => String(it.k0), itemVal: (it) => it.k0, parentSql: 'SELECT id, title, author_id FROM benchmark_posts WHERE author_id = ? ORDER BY id ASC', parentParams: [7], childSqlFmt: 'SELECT id, body, post_id FROM benchmark_comments WHERE post_id IN ({IN}) ORDER BY id ASC', parentKey: (r) => r.id },
  compositeRelations: { rel: 'posts', comp: 'ByTenant', parentF: ['tenant_id', 'user_id', 'name'], childF: ['tenant_id', 'post_id', 'user_id', 'title'], input: { tenant_id: 1 }, bind: bindCompositeRelations as never, childKey: (c) => `${c.tenant_id},${c.user_id}`, itemKey: (it) => `${it.k0},${it.k1}`, itemVal: (it) => [it.k0, it.k1], parentSql: 'SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = ? ORDER BY user_id ASC', parentParams: [1], childSqlFmt: '', parentKey: (r) => r.user_id },
};

function makeRelHandlers(db: InstanceType<typeof Database>, cfg: RelCfg): Record<string, Handler> {
  const select: Handler = (ports) => {
    if ('items' in ports) {
      const items = ports.items as Record<string, unknown>[];
      if (items.length === 0) return { ok: [] };
      const itemKeys = items.map(cfg.itemKey);
      const seen = new Set<string>();
      const distinct: unknown[] = [];
      items.forEach((it, i) => { if (!seen.has(itemKeys[i])) { seen.add(itemKeys[i]); distinct.push(cfg.itemVal(it)); } });
      const children = db.prepare(items[0].sql as string).all(JSON.stringify(distinct)) as Record<string, unknown>[];
      const groups = new Map<string, Record<string, unknown>[]>();
      for (const c of children) { const k = cfg.childKey(c); const g = groups.get(k); if (g) g.push(c); else groups.set(k, [c]); }
      return { ok: itemKeys.map((k) => groups.get(k) ?? []) };
    }
    return { ok: db.prepare(ports.sql as string).all(...scalarParams(ports)) };
  };
  return { Select: select };
}
function canonRel(res: { rows: Record<string, unknown>[]; [k: string]: unknown }, cfg: RelCfg): string {
  const parents = canonRows(res.rows, cfg.parentF);
  const childLists = '[' + (res[cfg.rel] as Record<string, unknown>[][]).map((cl) => canonRows(cl, cfg.childF)).join(',') + ']';
  return `{"rows":${parents},"${cfg.rel}":${childLists}}`;
}
function nativeRel(db: InstanceType<typeof Database>, op: string): string {
  const cfg = REL[op];
  const callable = (cfg.bind as (h: unknown) => Record<string, (i?: unknown) => unknown>)(makeRelHandlers(db, cfg));
  return canonRel(callable[cfg.comp](cfg.input) as never, cfg);
}
function sdkRel(db: InstanceType<typeof Database>, op: string): string {
  const cfg = REL[op];
  const parents = db.prepare(cfg.parentSql).all(...cfg.parentParams) as Record<string, unknown>[];
  let children: Record<string, unknown>[];
  let childKeyOf: (c: Record<string, unknown>) => string;
  let parentKeyStr: (p: Record<string, unknown>) => string;
  if (op === 'compositeRelations') {
    children = db.prepare('SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE tenant_id = ? ORDER BY post_id ASC').all(1) as Record<string, unknown>[];
    childKeyOf = (c) => `${c.tenant_id},${c.user_id}`;
    parentKeyStr = (p) => `${p.tenant_id},${p.user_id}`;
  } else {
    const keys = parents.map(cfg.parentKey);
    const inlist = keys.map(() => '?').join(',');
    children = db.prepare(cfg.childSqlFmt.replace('{IN}', inlist)).all(...keys) as Record<string, unknown>[];
    childKeyOf = cfg.childKey;
    parentKeyStr = (p) => String(cfg.parentKey(p));
  }
  const groups = new Map<string, Record<string, unknown>[]>();
  for (const c of children) { const k = childKeyOf(c); const g = groups.get(k); if (g) g.push(c); else groups.set(k, [c]); }
  const parentJson = canonRows(parents, cfg.parentF);
  const childLists = '[' + parents.map((p) => canonRows(groups.get(parentKeyStr(p)) ?? [], cfg.childF)).join(',') + ']';
  return `{"rows":${parentJson},"${cfg.rel}":${childLists}}`;
}

// ══ tx — the transaction-envelope wrap (better-sqlite3 db.transaction: rollback on throw) + {committed,state} ══
const TX: Record<string, { comp: string; input: Record<string, unknown>; bind: (h: never) => Record<string, (i?: unknown) => unknown> }> = {
  delete: { comp: 'Delete', input: { email: 'del0@bench.com', name: 'Del' }, bind: bindDelete as never },
  nestedCreate: { comp: 'NestedCreate', input: { email: 'nc@bench.com', name: 'NC', title: 'NC Post' }, bind: bindNestedCreate as never },
  nestedUpdate: { comp: 'NestedUpdate', input: { name: 'NU', user_id: 7, title: 'NU Post' }, bind: bindNestedUpdate as never },
  nestedUpsert: { comp: 'NestedUpsert', input: { email: 'user1@example.com', name: 'NUp', title: 'NUp Post' }, bind: bindNestedUpsert as never },
};
function txHandler(db: InstanceType<typeof Database>): Record<string, Handler> {
  // a tx statement produces a SINGLE obj (the RETURNING row — .get) or the {changes,lastInsertRowid} summary.
  const h: Handler = (ports) => {
    const sql = ports.sql as string;
    const params = scalarParams(ports);
    if (/\breturning\b/i.test(sql)) return { ok: (db.prepare(sql).get(...params) as unknown) ?? {} };
    const info = db.prepare(sql).run(...params);
    return { ok: { changes: info.changes, lastInsertRowid: Number(info.lastInsertRowid) } };
  };
  return { Insert: h, Update: h, Delete: h };
}
function stateSnapshot(db: InstanceType<typeof Database>): string {
  const users = db.prepare('SELECT id, email, name FROM benchmark_users ORDER BY id').all() as Record<string, unknown>[];
  const posts = db.prepare('SELECT id, title, author_id FROM benchmark_posts ORDER BY id').all() as Record<string, unknown>[];
  return `{"users":${canonRows(users, ['id', 'email', 'name'])},"posts":${canonRows(posts, ['id', 'title', 'author_id'])}}`;
}
function nativeTx(db: InstanceType<typeof Database>, op: string): string {
  const cfg = TX[op];
  const callable = (cfg.bind as (h: unknown) => Record<string, (i?: unknown) => unknown>)(txHandler(db));
  let committed = true;
  try { db.transaction(() => callable[cfg.comp](cfg.input))(); } catch { committed = false; }
  return `{"committed":${committed},"state":${stateSnapshot(db)}}`;
}
function sdkTx(db: InstanceType<typeof Database>, op: string): string {
  let committed = true;
  try {
    db.transaction(() => {
      if (op === 'delete') {
        const id = (db.prepare('INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id').get('del0@bench.com', 'Del') as { id: number }).id;
        db.prepare('DELETE FROM benchmark_users WHERE id = ?').run(id);
      } else if (op === 'nestedCreate') {
        const id = (db.prepare('INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id').get('nc@bench.com', 'NC') as { id: number }).id;
        db.prepare('INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)').run(id, 'NC Post');
      } else if (op === 'nestedUpdate') {
        db.prepare('UPDATE benchmark_users SET name = ? WHERE id = ?').run('NU', 7);
        db.prepare('UPDATE benchmark_posts SET title = ? WHERE author_id = ?').run('NU Post', 7);
      } else {
        const id = (db.prepare('INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id').get('user1@example.com', 'NUp') as { id: number }).id;
        db.prepare('INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)').run(id, 'NUp Post');
      }
    })();
  } catch { committed = false; }
  return `{"committed":${committed},"state":${stateSnapshot(db)}}`;
}

// ── op inputs (match ops.ts / oracle.ts) ──
const INPUTS: Record<string, Record<string, unknown>> = {
  findAll: {}, filterPaginateSort: { published: 1 }, findFirst: { name: 'User%' }, findUnique: { email: 'user500@example.com' },
  create: { email: 'new@bench.com', name: 'New' }, update: { id: 100, name: 'Updated 100' }, upsert: { email: 'user1@example.com', name: 'Upserted One' },
  createMany: { emails: BATCH_EMAILS, names: BATCH_NAMES }, upsertMany: { emails: UPSERTMANY_EMAILS, names: BATCH_NAMES }, updateMany: { ids: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], names: BATCH_NAMES },
};
const BINDS: Record<string, (h: never) => Record<string, (i?: unknown) => unknown>> = {
  findAll: bindFindAll as never, filterPaginateSort: bindFilterPaginateSort as never, findFirst: bindFindFirst as never, findUnique: bindFindUnique as never,
  create: bindCreate as never, update: bindUpdate as never, upsert: bindUpsert as never, createMany: bindCreateMany as never, upsertMany: bindUpsertMany as never, updateMany: bindUpdateMany as never,
};
const COMP: Record<string, string> = {
  findAll: 'FindAll', filterPaginateSort: 'FilterPaginateSort', findFirst: 'FindFirst', findUnique: 'FindUnique',
  create: 'Create', update: 'Update', upsert: 'Upsert', createMany: 'CreateMany', upsertMany: 'UpsertMany', updateMany: 'UpdateMany',
};

function nativeResult(db: InstanceType<typeof Database>, op: string): string {
  if (op in REL) return nativeRel(db, op);
  if (op in TX) return nativeTx(db, op);
  const handlers = makeHandlers(db, BATCH_COLS[op]);
  const callable = (BINDS[op] as (h: unknown) => Record<string, (i?: unknown) => unknown>)(handlers);
  const out = callable[COMP[op]](INPUTS[op]) as Record<string, unknown>[];
  return canonRows(out, FIELDS[op]);
}
function sdkStr(db: InstanceType<typeof Database>, op: string): string {
  if (op in REL) return sdkRel(db, op);
  if (op in TX) return sdkTx(db, op);
  return canonRows(sdkRows(db, op), FIELDS[op]);
}

const READ_OPS = new Set(['findAll', 'filterPaginateSort', 'findFirst', 'findUnique', 'nestedFindAll', 'nestedFindFirst', 'nestedFindUnique', 'nestedRelations', 'compositeRelations']);
const ALL_OPS = ['findAll', 'filterPaginateSort', 'findFirst', 'findUnique', 'nestedFindAll', 'nestedFindFirst', 'nestedFindUnique', 'nestedRelations', 'compositeRelations', 'create', 'update', 'upsert', 'createMany', 'upsertMany', 'updateMany', 'delete', 'nestedCreate', 'nestedUpdate', 'nestedUpsert'];
function usFrom(t0: bigint): number { return Number(process.hrtime.bigint() - t0) / 1000; }

function main(): void {
  const argv = process.argv.slice(2);
  const mode = argv[0];
  if (mode === 'run') {
    const [, op, dbPath, cell] = argv;
    const db = new Database(dbPath);
    process.stdout.write((cell === 'native' ? nativeResult(db, op) : sdkStr(db, op)) + '\n');
    db.close();
    return;
  }
  if (mode === 'bench') {
    // bench <seed_db> <warmup> <iters> <out_csv> — reads on the seed; mutating ops reset per iter (untimed).
    const [, seed, warmupS, itersS, outCsv] = argv;
    const warmup = Number(warmupS);
    const iters = Number(itersS);
    const lines: string[] = ['op,cell,us'];
    for (const op of ALL_OPS) {
      const mutating = !READ_OPS.has(op);
      const n = mutating ? Math.min(iters, 500) : iters;
      for (const cell of ['native', 'sdk'] as const) {
        const runOne = (db: InstanceType<typeof Database>) => (cell === 'native' ? nativeResult(db, op) : sdkStr(db, op));
        if (!mutating) {
          const db = new Database(seed, { readonly: true });
          for (let i = 0; i < warmup; i++) runOne(db);
          for (let i = 0; i < n; i++) { const t0 = process.hrtime.bigint(); runOne(db); lines.push(`${op},${cell},${usFrom(t0).toFixed(3)}`); }
          db.close();
        } else {
          const wu = Math.min(warmup, 50);
          const tmp = `${seed}.${op}.${cell}.work`;
          for (let i = 0; i < wu + n; i++) {
            copyFileSync(seed, tmp);
            const db = new Database(tmp);
            const t0 = process.hrtime.bigint();
            runOne(db);
            const us = usFrom(t0);
            db.close();
            rmSync(tmp, { force: true });
            if (i >= wu) lines.push(`${op},${cell},${us.toFixed(3)}`);
          }
        }
      }
    }
    writeFileSync(outCsv, lines.join('\n') + '\n');
    process.stderr.write(`ts bench done: ${ALL_OPS.length} ops × (native, sdk)\n`);
    return;
  }
  process.stderr.write('usage: tsx main.ts run <op> <db> <native|sdk> | bench <seed> <w> <n> <csv>\n');
  process.exit(2);
}
main();
