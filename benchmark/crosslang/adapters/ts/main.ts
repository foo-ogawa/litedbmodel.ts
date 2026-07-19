// Main-bench TS cell — the CLI-generated typescript-typed modules (baked STRAIGHT-LINE; they import the
// bc RUNTIME value-helpers + conformResultToOutType from the LOCAL 0.8.10 dist via --runtime-import) + a
// thin ASYNC exec seam over the built dialect's driver + the SDK baseline. ONE dialect per run, selected
// at RUNTIME from the <target> shape (sqlite path / pg conn / mysql url) — the generated module for that
// dialect is dynamically imported. mysql has no RETURNING → the `/*scp-reselect:*/` marker is stripped,
// the write run, and the row re-selected (the SAME one-path mechanic as rust/go). v1-faithful returning.
//
//   tsx main.ts run <op> <target> <native|sdk>   → print the canonical result (node driver byte-compares)
//   tsx main.ts bench <seed_db> <w> <n> <csv>     → latency CSV (sqlite only)
import { copyFileSync, rmSync, writeFileSync } from 'node:fs';

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

// ── the async DRIVER abstraction (the ts twin of the rust/go seam) ──
type Row = Record<string, unknown>;
// bc's conform requires INT fields to be bigint (a JS number classifies as float). So the seam returns
// integer columns + the write summary as bigint (the ts twin of rust/go's typed-int cells); canonVal
// stringifies bigint identically. A pg BOOLEAN `published` stays boolean (its outType is bool).
type Summary = { changes: bigint; lastInsertRowid: bigint };
type Dialect = 'sqlite' | 'postgres' | 'mysql';
interface Db {
  dialect: Dialect;
  query(sql: string, params: unknown[]): Promise<Row[]>; // marker-aware read / RETURNING
  run(sql: string, params: unknown[]): Promise<Summary>; // non-returning write
  transaction(fn: () => Promise<void>): Promise<boolean>;
  close(): Promise<void>;
}

function dialectOf(target: string): Dialect {
  if (target.startsWith('mysql://')) return 'mysql';
  if (/(^|\s)host=/.test(target)) return 'postgres';
  return 'sqlite';
}

// ── mysql RETURNING emulation marker (dialect-independent; emitted into the SQL ONLY for mysql) ──
const MARK_OPEN = ' /*scp-reselect: ';
function parseReselect(sql: string): { write: string; select: string; binds: string[] } | undefined {
  const open = sql.indexOf(MARK_OPEN);
  if (open < 0) return undefined;
  const write = sql.slice(0, open);
  const rest = sql.slice(open + MARK_OPEN.length);
  const close = rest.lastIndexOf('*/');
  if (close < 0) return undefined;
  const body = rest.slice(0, close).trimEnd();
  const sep = body.indexOf(' ::binds:: ');
  if (sep < 0) return undefined;
  return { write: write, select: body.slice(0, sep), binds: body.slice(sep + ' ::binds:: '.length).split(',').filter(Boolean) };
}
function reselectParams(binds: string[], writeParams: unknown[], s: Summary): unknown[] {
  return binds.map((t) => {
    if (t === 'L') return s.lastInsertRowid;
    if (t === 'H') return s.lastInsertRowid + s.changes;
    if (t.startsWith('p')) return writeParams[Number(t.slice(1))];
    throw new Error(`unknown reselect bind token '${t}'`);
  });
}
function renumber(sql: string): string {
  let n = 0;
  return sql.replace(/\?/g, () => `$${++n}`);
}

async function openDb(target: string): Promise<Db> {
  const dialect = dialectOf(target);
  if (dialect === 'sqlite') {
    const Database = (await import('better-sqlite3')).default;
    const db = new Database(target);
    db.defaultSafeIntegers(true); // INTEGER columns + lastInsertRowid → bigint (conform int)
    const runSummary = (sql: string, params: unknown[]): Summary => {
      const info = db.prepare(sql).run(...params);
      return { changes: BigInt(info.changes), lastInsertRowid: info.lastInsertRowid as bigint };
    };
    const doMarker = async (sql: string, params: unknown[]): Promise<Row[] | undefined> => {
      const r = parseReselect(sql);
      if (!r) return undefined;
      const s = runSummary(r.write, params);
      return db.prepare(r.select).all(...reselectParams(r.binds, params, s)) as Row[];
    };
    return {
      dialect,
      async query(sql, params) {
        return (await doMarker(sql, params)) ?? (db.prepare(sql).all(...params) as Row[]);
      },
      async run(sql, params) {
        return runSummary(sql, params);
      },
      async transaction(fn) {
        db.exec('BEGIN');
        try {
          await fn();
          db.exec('COMMIT');
          return true;
        } catch {
          try { db.exec('ROLLBACK'); } catch { /* ignore */ }
          return false;
        }
      },
      async close() { db.close(); },
    };
  }
  if (dialect === 'postgres') {
    const pg = (await import('pg')).default;
    pg.types.setTypeParser(1114, (v: string) => v); // TIMESTAMP → the raw 'YYYY-MM-DD HH:MM:SS' text (oracle form)
    for (const oid of [20, 21, 23]) pg.types.setTypeParser(oid, (v: string) => BigInt(v)); // int8/int2/int4 → bigint (conform int)
    // The shared target is a libpq keyword string (`host=… port=… dbname=…`) — accepted by rust's
    // postgres crate + go's lib/pq, but node-postgres needs an options object, so parse it here.
    const kv: Record<string, string> = {};
    for (const pair of target.trim().split(/\s+/)) { const eq = pair.indexOf('='); if (eq > 0) kv[pair.slice(0, eq)] = pair.slice(eq + 1); }
    const client = new pg.Client({ host: kv.host, port: Number(kv.port), user: kv.user, password: kv.password, database: kv.dbname, ssl: false });
    await client.connect();
    return {
      dialect,
      async query(sql, params) { return (await client.query(sql, params)).rows as Row[]; }, // pg has native RETURNING (no marker)
      async run(sql, params) { const r = await client.query(sql, params); return { changes: BigInt(r.rowCount ?? 0), lastInsertRowid: 0n }; },
      async transaction(fn) {
        await client.query('BEGIN');
        try { await fn(); await client.query('COMMIT'); return true; } catch { try { await client.query('ROLLBACK'); } catch { /* ignore */ } return false; }
      },
      async close() { await client.end(); },
    };
  }
  // mysql
  const mysql = (await import('mysql2/promise')).default;
  const url = new URL(target);
  // typeCast: integer columns → bigint (conform int); dateStrings keeps TIMESTAMP/DATETIME as the string.
  const intTypes = new Set(['TINY', 'SHORT', 'INT24', 'LONG', 'LONGLONG']);
  const conn = await mysql.createConnection({
    host: url.hostname, port: Number(url.port || 3306), user: decodeURIComponent(url.username), password: decodeURIComponent(url.password),
    database: url.pathname.replace(/^\//, ''), dateStrings: true, multipleStatements: false,
    typeCast: (field, next) => {
      if (intTypes.has(field.type)) { const s = field.string(); return s === null ? null : BigInt(s); }
      return next();
    },
  });
  const runSummary = (res: unknown): Summary => {
    const h = res as { affectedRows?: number; insertId?: number };
    return { changes: BigInt(h.affectedRows ?? 0), lastInsertRowid: BigInt(h.insertId ?? 0) };
  };
  const doMarker = async (sql: string, params: unknown[]): Promise<Row[] | undefined> => {
    const r = parseReselect(sql);
    if (!r) return undefined;
    const [res] = await conn.query(r.write, params);
    const [rows] = await conn.query(r.select, reselectParams(r.binds, params, runSummary(res)));
    return rows as Row[];
  };
  return {
    dialect,
    async query(sql, params) {
      const m = await doMarker(sql, params);
      if (m) return m;
      const [rows] = await conn.query(sql, params);
      return rows as Row[];
    },
    async run(sql, params) {
      const [res] = await conn.query(sql, params);
      return runSummary(res);
    },
    async transaction(fn) {
      await conn.query('BEGIN');
      try { await fn(); await conn.query('COMMIT'); return true; } catch { try { await conn.query('ROLLBACK'); } catch { /* ignore */ } return false; }
    },
    async close() { await conn.end(); },
  };
}

// ── handler param binding (dialect-aware; the ports name the shape) ──
type Handler = (ports: Record<string, unknown>, ctx: unknown) => Promise<{ ok: unknown } | { error: string }>;
function scalarParams(ports: Record<string, unknown>): unknown[] {
  return Object.keys(ports).filter((k) => /^p\d+$/.test(k)).sort((a, b) => Number(a.slice(1)) - Number(b.slice(1))).map((k) => ports[k]);
}
function hasMarker(sql: string): boolean { return sql.includes(MARK_OPEN); }
function isRowsSql(sql: string): boolean { return /^\s*select\b/i.test(sql) || /\breturning\b/i.test(sql) || hasMarker(sql); }
/** Batch (v0/v1) params: sqlite/mysql bind ONE records-JSON to every `?`; pg binds each column array. */
function batchParams(db: Db, sql: string, cols: [string, string], ports: Record<string, unknown>): unknown[] {
  const v0 = ports['v0'] as unknown[];
  const v1 = ports['v1'] as unknown[];
  if (db.dialect === 'postgres') return [v0, v1];
  const json = JSON.stringify(v0.map((_, i) => ({ [cols[0]]: v0[i], [cols[1]]: v1[i] })));
  const n = (sql.match(/\?/g) ?? []).length;
  return Array.from({ length: n }, () => json);
}

// ── flat / batch native cells ──
const BATCH_COLS: Record<string, [string, string]> = { createMany: ['email', 'name'], upsertMany: ['email', 'name'], updateMany: ['id', 'name'] };
function flatHandlers(db: Db, batchCols?: [string, string]): Record<string, Handler> {
  const h: Handler = async (ports) => {
    const sql = ports['sql'] as string;
    if ('v0' in ports) { // batch write — v1-faithful NO returning → summary (list); the cell emits null
      const s = await db.run(sql, batchParams(db, sql, batchCols!, ports));
      return { ok: [s] };
    }
    const params = scalarParams(ports);
    if (isRowsSql(sql)) return { ok: await db.query(sql, params) };
    return { ok: [await db.run(sql, params)] }; // no-returning single write → summary (list); cell emits null
  };
  return { Select: h, Insert: h, Update: h, Delete: h };
}

// ── read+rel (2-level slice) — the batched-map handler ──
interface RelCfg {
  rel: string; comp: string; parentF: string[]; childF: string[]; input: Record<string, unknown>;
  file: string; composite: boolean;
  childKey: (c: Row) => string; itemKey: (it: Record<string, unknown>) => string;
  parentSql: string; parentParams: unknown[]; childSqlFmt: string; parentKey: (r: Row) => string;
  parentSer: string[]; childSer: string[];
}
const REL: Record<string, RelCfg> = {
  nestedFindAll: { rel: 'posts', comp: 'FindAll', parentF: ['id', 'email', 'name'], childF: ['id', 'title', 'author_id'], input: {}, file: 'gen_nestedfindall', composite: false, childKey: (c) => String(c.author_id), itemKey: (it) => String(it.k0), parentSql: 'SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100', parentParams: [], childSqlFmt: 'SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC', parentKey: (r) => String(r.id), parentSer: ['id', 'email', 'name'], childSer: ['id', 'title', 'author_id'] },
  nestedFindFirst: { rel: 'posts', comp: 'FindFirst', parentF: ['id', 'email', 'name'], childF: ['id', 'title', 'author_id'], input: { name: 'User%' }, file: 'gen_nestedfindfirst', composite: false, childKey: (c) => String(c.author_id), itemKey: (it) => String(it.k0), parentSql: 'SELECT id, email, name FROM benchmark_users WHERE name LIKE {PH1} LIMIT 1', parentParams: ['User%'], childSqlFmt: 'SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC', parentKey: (r) => String(r.id), parentSer: ['id', 'email', 'name'], childSer: ['id', 'title', 'author_id'] },
  nestedFindUnique: { rel: 'posts', comp: 'FindUnique', parentF: ['id', 'email', 'name'], childF: ['id', 'title', 'author_id'], input: { email: 'user1@example.com' }, file: 'gen_nestedfindunique', composite: false, childKey: (c) => String(c.author_id), itemKey: (it) => String(it.k0), parentSql: 'SELECT id, email, name FROM benchmark_users WHERE email = {PH1} LIMIT 1', parentParams: ['user1@example.com'], childSqlFmt: 'SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC', parentKey: (r) => String(r.id), parentSer: ['id', 'email', 'name'], childSer: ['id', 'title', 'author_id'] },
  nestedRelations: { rel: 'comments', comp: 'ByAuthor', parentF: ['id', 'title', 'author_id'], childF: ['id', 'body', 'post_id'], input: { author_id: 7 }, file: 'gen_nestedrelations', composite: false, childKey: (c) => String(c.post_id), itemKey: (it) => String(it.k0), parentSql: 'SELECT id, title, author_id FROM benchmark_posts WHERE author_id = {PH1} ORDER BY id ASC', parentParams: [7], childSqlFmt: 'SELECT id, body, post_id FROM benchmark_comments WHERE post_id {IN} ORDER BY id ASC', parentKey: (r) => String(r.id), parentSer: ['id', 'title', 'author_id'], childSer: ['id', 'body', 'post_id'] },
  compositeRelations: { rel: 'posts', comp: 'ByTenant', parentF: ['tenant_id', 'user_id', 'name'], childF: ['tenant_id', 'post_id', 'user_id', 'title'], input: { tenant_id: 1 }, file: 'gen_compositerelations', composite: true, childKey: (c) => `${c.tenant_id},${c.user_id}`, itemKey: (it) => `${it.k0},${it.k1}`, parentSql: 'SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = {PH1} ORDER BY user_id ASC', parentParams: [1], childSqlFmt: 'SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE tenant_id = {PH1} ORDER BY post_id ASC', parentKey: (r) => `${r.tenant_id},${r.user_id}`, parentSer: ['tenant_id', 'user_id', 'name'], childSer: ['tenant_id', 'post_id', 'user_id', 'title'] },
};
/** The child-query params for the batched relation: sqlite/mysql ONE keys-JSON; pg native array(s). */
function relChildQuery(db: Db, sql: string, distinct: Array<number | [number, number]>, composite: boolean): { sql: string; params: unknown[] } {
  if (db.dialect === 'postgres') {
    const resolved = renumber(sql.replace(/@@PG_ARRAY_CAST@@/g, 'int[]'));
    if (composite) return { sql: resolved, params: [(distinct as [number, number][]).map((k) => k[0]), (distinct as [number, number][]).map((k) => k[1])] };
    return { sql: resolved, params: [distinct as number[]] };
  }
  return { sql, params: [JSON.stringify(distinct)] };
}
function relHandlers(db: Db, cfg: RelCfg): Record<string, Handler> {
  const select: Handler = async (ports) => {
    if ('items' in ports) {
      const items = ports.items as Record<string, unknown>[];
      if (items.length === 0) return { ok: [] };
      const itemKeys = items.map(cfg.itemKey);
      const seen = new Set<string>();
      const distinct: Array<number | [number, number]> = [];
      items.forEach((it, i) => {
        if (!seen.has(itemKeys[i])) {
          seen.add(itemKeys[i]);
          distinct.push(cfg.composite ? [Number(it.k0), Number(it.k1)] : Number(it.k0));
        }
      });
      const q = relChildQuery(db, items[0].sql as string, distinct, cfg.composite);
      const children = await db.query(q.sql, q.params);
      const groups = new Map<string, Row[]>();
      for (const c of children) { const k = cfg.childKey(c); (groups.get(k) ?? groups.set(k, []).get(k)!).push(c); }
      return { ok: itemKeys.map((k) => groups.get(k) ?? []) };
    }
    return { ok: await db.query(ports.sql as string, scalarParams(ports)) };
  };
  return { Select: select };
}
function canonRel(res: { rows: Row[]; [k: string]: unknown }, cfg: RelCfg): string {
  const parents = canonRows(res.rows, cfg.parentSer);
  const childLists = '[' + (res[cfg.rel] as Row[][]).map((cl) => canonRows(cl, cfg.childSer)).join(',') + ']';
  return `{"rows":${parents},"${cfg.rel}":${childLists}}`;
}

// ── tx (transaction envelope + chain; {committed, state}) ──
const TX: Record<string, { comp: string; input: Record<string, unknown>; file: string }> = {
  delete: { comp: 'Delete', input: { email: 'del0@bench.com', name: 'Del' }, file: 'gen_delete' },
  nestedCreate: { comp: 'NestedCreate', input: { email: 'nc@bench.com', name: 'NC', title: 'NC Post' }, file: 'gen_nestedcreate' },
  nestedUpdate: { comp: 'NestedUpdate', input: { name: 'NU', user_id: 7, title: 'NU Post' }, file: 'gen_nestedupdate' },
  nestedUpsert: { comp: 'NestedUpsert', input: { email: 'user1@example.com', name: 'NUp', title: 'NUp Post' }, file: 'gen_nestedupsert' },
};
function txHandlers(db: Db): Record<string, Handler> {
  // a tx body produces a SINGLE obj: the RETURNING/re-select row, or the {changes,lastInsertRowid} summary.
  const h: Handler = async (ports) => {
    const sql = ports.sql as string;
    const params = scalarParams(ports);
    if (isRowsSql(sql)) { const rows = await db.query(sql, params); return { ok: rows[0] ?? {} }; }
    return { ok: await db.run(sql, params) };
  };
  return { Insert: h, Update: h, Delete: h };
}
async function stateSnapshot(db: Db): Promise<string> {
  const users = await db.query('SELECT id, email, name FROM benchmark_users ORDER BY id', []);
  const posts = await db.query('SELECT id, title, author_id FROM benchmark_posts ORDER BY id', []);
  return `{"users":${canonRows(users, ['id', 'email', 'name'])},"posts":${canonRows(posts, ['id', 'title', 'author_id'])}}`;
}

// ── op registry ──
const NULL_OPS = new Set(['create', 'update', 'createMany', 'upsertMany', 'updateMany']); // v1: no returning → null
const READ_FIELDS: Record<string, string[]> = {
  findAll: ['id', 'email', 'name'], filterPaginateSort: ['id', 'title', 'content', 'published', 'author_id', 'created_at'],
  findFirst: ['id', 'email', 'name'], findUnique: ['id', 'email', 'name'],
};
const FLAT_INPUT: Record<string, Record<string, unknown>> = {
  findAll: {}, filterPaginateSort: { published: 1 }, findFirst: { name: 'User%' }, findUnique: { email: 'user500@example.com' },
  create: { email: 'new@bench.com', name: 'New' }, update: { id: 100, name: 'Updated 100' }, upsert: { email: 'user1@example.com', name: 'Upserted One' },
  createMany: { emails: batchEmails(), names: batchNames() }, upsertMany: { emails: upsertManyEmails(), names: batchNames() }, updateMany: { ids: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], names: batchNames() },
};
const FLAT_COMP: Record<string, string> = {
  findAll: 'FindAll', filterPaginateSort: 'FilterPaginateSort', findFirst: 'FindFirst', findUnique: 'FindUnique',
  create: 'Create', update: 'Update', upsert: 'Upsert', createMany: 'CreateMany', upsertMany: 'UpsertMany', updateMany: 'UpdateMany',
};
function batchEmails(): string[] { return Array.from({ length: 10 }, (_, i) => `many${i}@bench.com`); }
function batchNames(): string[] { return Array.from({ length: 10 }, (_, i) => `Many ${i}`); }
function upsertManyEmails(): string[] { return ['user1@example.com', 'user2@example.com', ...Array.from({ length: 8 }, (_, i) => `many${i}@bench.com`)]; }

// The dialect-specific input value for filterPaginateSort's `published` head (pg BOOLEAN vs int).
function fpsInput(db: Db): Record<string, unknown> { return { published: db.dialect === 'postgres' ? true : 1 }; }

async function bindAsyncOf(db: Db, file: string): Promise<(h: unknown) => Record<string, (i?: unknown) => Promise<unknown>>> {
  const mod = await import(`./generated/${db.dialect}/${file}.ts`);
  return mod.bindAsync as (h: unknown) => Record<string, (i?: unknown) => Promise<unknown>>;
}

async function nativeResult(db: Db, op: string): Promise<string> {
  if (op in REL) {
    const cfg = REL[op];
    const bindAsync = await bindAsyncOf(db, cfg.file);
    const callable = bindAsync(relHandlers(db, cfg));
    return canonRel((await callable[cfg.comp](cfg.input)) as never, cfg);
  }
  if (op in TX) {
    const cfg = TX[op];
    const bindAsync = await bindAsyncOf(db, cfg.file);
    const callable = bindAsync(txHandlers(db));
    const committed = await db.transaction(async () => { await callable[cfg.comp](cfg.input); });
    return `{"committed":${committed},"state":${await stateSnapshot(db)}}`;
  }
  const bindAsync = await bindAsyncOf(db, 'gen_' + op.toLowerCase());
  const callable = bindAsync(flatHandlers(db, BATCH_COLS[op]));
  const input = op === 'filterPaginateSort' ? fpsInput(db) : FLAT_INPUT[op];
  const out = (await callable[FLAT_COMP[op]](input)) as Row[];
  if (NULL_OPS.has(op)) return 'null';
  if (op === 'upsert') return canonRows(out, ['id']);
  return canonRows(out, READ_FIELDS[op]);
}

// ── SDK baseline — raw driver + hand-SQL (dialect-aware; the ts twin of rust/go SDK) ──
function ph(db: Db, n: number): string { return db.dialect === 'postgres' ? `$${n}` : '?'; }
function upsertReturningIdSql(db: Db): string {
  if (db.dialect === 'mysql') return 'INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name) /*scp-reselect: SELECT id FROM benchmark_users WHERE email = ? ORDER BY id ::binds:: p0*/';
  return `INSERT INTO benchmark_users (email, name) VALUES (${ph(db, 1)}, ${ph(db, 2)}) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id`;
}
function insertUserIdSql(db: Db): string {
  if (db.dialect === 'mysql') return 'INSERT INTO benchmark_users (email, name) VALUES (?, ?) /*scp-reselect: SELECT id FROM benchmark_users WHERE id >= ? AND id < ? ORDER BY id ::binds:: L,H*/';
  return `INSERT INTO benchmark_users (email, name) VALUES (${ph(db, 1)}, ${ph(db, 2)}) RETURNING id`;
}
function upsertManyTail(db: Db): string {
  return db.dialect === 'mysql' ? ' ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name)' : ' ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name';
}
async function sdkStr(db: Db, op: string): Promise<string> {
  switch (op) {
    case 'findAll': return canonRows(await db.query('SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100', []), READ_FIELDS.findAll);
    case 'findFirst': return canonRows(await db.query(`SELECT id, email, name FROM benchmark_users WHERE name LIKE ${ph(db, 1)} LIMIT 1`, ['User%']), READ_FIELDS.findFirst);
    case 'findUnique': return canonRows(await db.query(`SELECT id, email, name FROM benchmark_users WHERE email = ${ph(db, 1)} LIMIT 1`, ['user500@example.com']), READ_FIELDS.findUnique);
    case 'filterPaginateSort': return canonRows(await db.query(`SELECT id, title, content, published, author_id, created_at FROM benchmark_posts WHERE published = ${ph(db, 1)} ORDER BY created_at DESC LIMIT 20 OFFSET 10`, [1]), READ_FIELDS.filterPaginateSort);
    case 'create': await db.run(`INSERT INTO benchmark_users (email, name) VALUES (${ph(db, 1)}, ${ph(db, 2)})`, ['new@bench.com', 'New']); return 'null';
    case 'update': await db.run(`UPDATE benchmark_users SET name = ${ph(db, 1)} WHERE id = ${ph(db, 2)}`, ['Updated 100', 100]); return 'null';
    case 'upsert': return canonRows(await db.query(upsertReturningIdSql(db), ['user1@example.com', 'Upserted One']), ['id']);
    case 'createMany': return sdkInsertMany(db, batchEmails(), batchNames(), '');
    case 'upsertMany': return sdkInsertMany(db, upsertManyEmails(), batchNames(), upsertManyTail(db));
    case 'updateMany': return sdkUpdateMany(db);
    case 'nestedFindAll': case 'nestedFindFirst': case 'nestedFindUnique': case 'nestedRelations': case 'compositeRelations': return sdkRel(db, op);
    case 'delete': case 'nestedCreate': case 'nestedUpdate': case 'nestedUpsert': return sdkTx(db, op);
  }
  throw new Error(`sdk: unknown op '${op}'`);
}
async function sdkInsertMany(db: Db, emails: string[], names: string[], tail: string): Promise<string> {
  const tuples: string[] = [];
  const params: unknown[] = [];
  emails.forEach((e, i) => { tuples.push(`(${ph(db, 2 * i + 1)}, ${ph(db, 2 * i + 2)})`); params.push(e, names[i]); });
  await db.run(`INSERT INTO benchmark_users (email, name) VALUES ${tuples.join(', ')}${tail}`, params);
  return 'null';
}
async function sdkUpdateMany(db: Db): Promise<string> {
  const names = batchNames();
  const cases = Array.from({ length: 10 }, (_, i) => `WHEN ${i + 1} THEN ${ph(db, i + 1)}`).join(' ');
  await db.run(`UPDATE benchmark_users SET name = CASE id ${cases} END WHERE id IN (1,2,3,4,5,6,7,8,9,10)`, names);
  return 'null';
}
function sdkChildIn(db: Db, keys: number[]): { clause: string; params: unknown[] } {
  if (db.dialect === 'postgres') return { clause: '= ANY($1::int[])', params: [keys] };
  const marks = keys.map(() => '?').join(',');
  return { clause: `IN (${marks})`, params: keys };
}
async function sdkRel(db: Db, op: string): Promise<string> {
  const cfg = REL[op];
  const parentSql = cfg.parentSql.replace('{PH1}', ph(db, 1));
  const parents = await db.query(parentSql, cfg.parentParams);
  const groups = new Map<string, Row[]>();
  let childLists: string;
  if (cfg.composite) {
    const children = await db.query(cfg.childSqlFmt.replace('{PH1}', ph(db, 1)), [1]);
    for (const c of children) { const k = cfg.childKey(c); (groups.get(k) ?? groups.set(k, []).get(k)!).push(c); }
    childLists = '[' + parents.map((p) => canonRows(groups.get(cfg.parentKey(p)) ?? [], cfg.childSer)).join(',') + ']';
  } else {
    const keys = parents.map((r) => Number(r[cfg.parentF[0]]));
    const { clause, params } = sdkChildIn(db, keys);
    const children = await db.query(cfg.childSqlFmt.replace('{IN}', clause), params);
    for (const c of children) { const k = cfg.childKey(c); (groups.get(k) ?? groups.set(k, []).get(k)!).push(c); }
    childLists = '[' + parents.map((p) => canonRows(groups.get(cfg.parentKey(p)) ?? [], cfg.childSer)).join(',') + ']';
  }
  return `{"rows":${canonRows(parents, cfg.parentSer)},"${cfg.rel}":${childLists}}`;
}
async function sdkTx(db: Db, op: string): Promise<string> {
  const committed = await db.transaction(async () => {
    if (op === 'delete') {
      const rows = await db.query(insertUserIdSql(db), ['del0@bench.com', 'Del']);
      await db.run(`DELETE FROM benchmark_users WHERE id = ${ph(db, 1)}`, [Number(rows[0].id)]);
    } else if (op === 'nestedCreate') {
      const rows = await db.query(insertUserIdSql(db), ['nc@bench.com', 'NC']);
      await db.run(`INSERT INTO benchmark_posts (author_id, title) VALUES (${ph(db, 1)}, ${ph(db, 2)})`, [Number(rows[0].id), 'NC Post']);
    } else if (op === 'nestedUpdate') {
      await db.run(`UPDATE benchmark_users SET name = ${ph(db, 1)} WHERE id = ${ph(db, 2)}`, ['NU', 7]);
      await db.run(`UPDATE benchmark_posts SET title = ${ph(db, 1)} WHERE author_id = ${ph(db, 2)}`, ['NU Post', 7]);
    } else {
      const rows = await db.query(upsertReturningIdSql(db), ['user1@example.com', 'NUp']);
      await db.run(`INSERT INTO benchmark_posts (author_id, title) VALUES (${ph(db, 1)}, ${ph(db, 2)})`, [Number(rows[0].id), 'NUp Post']);
    }
  });
  return `{"committed":${committed},"state":${await stateSnapshot(db)}}`;
}

const READ_OPS = new Set(['findAll', 'filterPaginateSort', 'findFirst', 'findUnique', 'nestedFindAll', 'nestedFindFirst', 'nestedFindUnique', 'nestedRelations', 'compositeRelations']);
const ALL_OPS = ['findAll', 'filterPaginateSort', 'findFirst', 'findUnique', 'nestedFindAll', 'nestedFindFirst', 'nestedFindUnique', 'nestedRelations', 'compositeRelations', 'create', 'update', 'upsert', 'createMany', 'upsertMany', 'updateMany', 'delete', 'nestedCreate', 'nestedUpdate', 'nestedUpsert'];

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const mode = argv[0];
  if (mode === 'run') {
    const [, op, target, cell] = argv;
    const db = await openDb(target);
    process.stdout.write((cell === 'native' ? await nativeResult(db, op) : await sdkStr(db, op)) + '\n');
    await db.close();
    return;
  }
  if (mode === 'bench') {
    // sqlite-only latency (file-copy reset per mutating iter).
    const [, seed, warmupS, itersS, outCsv] = argv;
    const warmup = Number(warmupS);
    const iters = Number(itersS);
    const lines: string[] = ['op,cell,us'];
    const usFrom = (t0: bigint): number => Number(process.hrtime.bigint() - t0) / 1000;
    for (const op of ALL_OPS) {
      const mutating = !READ_OPS.has(op);
      const n = mutating ? Math.min(iters, 500) : iters;
      for (const cell of ['native', 'sdk'] as const) {
        if (!mutating) {
          const db = await openDb(seed);
          for (let i = 0; i < warmup; i++) await (cell === 'native' ? nativeResult(db, op) : sdkStr(db, op));
          for (let i = 0; i < n; i++) { const t0 = process.hrtime.bigint(); await (cell === 'native' ? nativeResult(db, op) : sdkStr(db, op)); lines.push(`${op},${cell},${usFrom(t0).toFixed(3)}`); }
          await db.close();
        } else {
          const wu = Math.min(warmup, 50);
          const tmp = `${seed}.${op}.${cell}.work`;
          for (let i = 0; i < wu + n; i++) {
            copyFileSync(seed, tmp);
            const db = await openDb(tmp);
            const t0 = process.hrtime.bigint();
            await (cell === 'native' ? nativeResult(db, op) : sdkStr(db, op));
            const us = usFrom(t0);
            await db.close();
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
  process.stderr.write('usage: tsx main.ts run <op> <target> <native|sdk> | bench <seed> <w> <n> <csv>\n');
  process.exit(2);
}
main().catch((e) => {
  process.stderr.write(String((e && e.stack) || e) + '\n');
  process.exit(1);
});
