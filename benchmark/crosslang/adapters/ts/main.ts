// Main-bench TS cell — the CLI-generated typescript-typed modules (baked STRAIGHT-LINE: no runBehavior
// tree-walk, no IR walk, no JSON.parse — only bc's codegen-primitive value helpers) + a thin exec seam
// (the Select/Insert/Update/Delete component handlers) + the SDK baseline. This round: the 10 flat ops
// (reads / single writes / batch). read+rel + tx are the next ts slice (per-op child-key + tx envelope).
//
//   tsx main.ts run <op> <db> <native|sdk>   → print the canonical result (node driver byte-compares)
import Database from 'better-sqlite3';

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

function sdkResult(db: InstanceType<typeof Database>, op: string): Record<string, unknown>[] {
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
  const handlers = makeHandlers(db, BATCH_COLS[op]);
  const callable = (BINDS[op] as (h: unknown) => Record<string, (i?: unknown) => unknown>)(handlers);
  const out = callable[COMP[op]](INPUTS[op]) as Record<string, unknown>[];
  return canonRows(out, FIELDS[op]);
}

function main(): void {
  const [mode, op, dbPath, cell] = process.argv.slice(2);
  if (mode !== 'run') { process.stderr.write('usage: tsx main.ts run <op> <db> <native|sdk>\n'); process.exit(2); }
  const db = new Database(dbPath);
  const out = cell === 'native' ? nativeResult(db, op) : canonRows(sdkResult(db, op), FIELDS[op]);
  db.close();
  process.stdout.write(out + '\n');
}
main();
