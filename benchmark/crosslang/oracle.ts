// Emit the byte-equal ORACLE: per op, the mode-2 result canonicalized to a stable JSON string, + a
// shared seed DB every cell copies. The native (rust/go/ts) + SDK cells must reproduce this exact string.
// Run: npx tsx benchmark/crosslang/oracle.ts
import Database from 'better-sqlite3';
import { writeFileSync, mkdirSync, rmSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { executeBundle, readBundle, executeTransactionBundle } from '../../dist/scp/index.cjs';
import { buildOps, type BenchOp } from './ops';
import { ddl, seedStatements } from './orm-domain';

const HERE = dirname(fileURLToPath(import.meta.url));
const ART = join(HERE, '.artifacts');

/** Canonical value serialization — matches the rust/go seams (int bare, string JSON-quoted, null). */
function canonVal(v: unknown): string {
  if (v === null || v === undefined) return 'null';
  if (typeof v === 'number' || typeof v === 'bigint') return String(v);
  if (typeof v === 'boolean') return v ? '1' : '0';
  return JSON.stringify(String(v));
}
/** A row → `{"k":v,…}` in the given field order (the projection order the native structs use). */
function canonRow(row: Record<string, unknown>, fields: readonly string[]): string {
  return '{' + fields.map((f) => `${JSON.stringify(f)}:${canonVal(row[f])}`).join(',') + '}';
}
function canonRows(rows: Record<string, unknown>[], fields: readonly string[]): string {
  return '[' + rows.map((r) => canonRow(r, fields)).join(',') + ']';
}

/** The projected field order per op (== the native module's row struct field order == the SELECT/RETURNING). */
const FIELDS: Record<string, string[]> = {
  findAll: ['id', 'email', 'name'],
  filterPaginateSort: ['id', 'title', 'content', 'published', 'author_id', 'created_at'],
  findFirst: ['id', 'email', 'name'],
  findUnique: ['id', 'email', 'name'],
  // v1-faithful returning: `upsert` returns the PK only; create/update/createMany/upsertMany/updateMany
  // are NO-RETURNING (v1 returns null) — canonicalized to `null` below (a no-returning write's mode-2
  // summary {changes,lastInsertRowid} is dialect-dependent: mysql ON DUPLICATE KEY reports affected=12
  // vs 10, and lastInsertRowid differs per engine — so only `null` is dialect-independent).
  upsert: ['id'],
};
// read+rel: parent fields + the child (relation) fields.
const REL_FIELDS: Record<string, { parent: string[]; child: string[] }> = {
  nestedFindAll: { parent: ['id', 'email', 'name'], child: ['id', 'title', 'author_id'] },
  nestedFindFirst: { parent: ['id', 'email', 'name'], child: ['id', 'title', 'author_id'] },
  nestedFindUnique: { parent: ['id', 'email', 'name'], child: ['id', 'title', 'author_id'] },
  nestedRelations: { parent: ['id', 'title', 'author_id'], child: ['id', 'body', 'post_id'] },
  compositeRelations: { parent: ['tenant_id', 'user_id', 'name'], child: ['tenant_id', 'post_id', 'user_id', 'title'] },
};

function freshDb() {
  const db = new Database(':memory:');
  for (const s of ddl('sqlite')) db.exec(s);
  const tx = db.transaction(() => {
    for (const s of seedStatements('sqlite')) db.prepare(s.sql).run(...(s.params as never[]));
  });
  tx();
  return db;
}

/** users+posts state after a mutating op — the affected-tables snapshot the write/tx cells also emit. */
function stateSnapshot(db: InstanceType<typeof Database>): string {
  const users = db.prepare('SELECT id, email, name FROM benchmark_users ORDER BY id').all() as Record<string, unknown>[];
  const posts = db.prepare('SELECT id, title, author_id FROM benchmark_posts ORDER BY id').all() as Record<string, unknown>[];
  return `{"users":${canonRows(users, ['id', 'email', 'name'])},"posts":${canonRows(posts, ['id', 'title', 'author_id'])}}`;
}

/** Canonicalize one op's mode-2 result to the shared string. */
function oracleFor(op: BenchOp, db: InstanceType<typeof Database>): string {
  if (op.kind === 'tx') {
    let committed: boolean;
    try {
      committed = (executeTransactionBundle(op.bundle, op.input as never, { db: db as never }) as { committed: boolean }).committed;
    } catch {
      committed = false;
    }
    return `{"committed":${committed},"state":${stateSnapshot(db)}}`;
  }
  if (op.withRel) {
    const rel = REL_FIELDS[op.id];
    const rows = readBundle(op.bundle, op.input as never, { db: db as never, with: { [op.withRel]: true } as never }) as Record<string, unknown>[];
    // Normalize to {rows:[parent…], <rel>:[[child…]…]} — the native T2 {rows, <rel>} shape.
    const parents = canonRows(rows, rel.parent);
    const children = '[' + rows.map((r) => canonRows((r[op.withRel!] as Record<string, unknown>[]) ?? [], rel.child)).join(',') + ']';
    return `{"rows":${parents},"${op.withRel}":${children}}`;
  }
  const out = executeBundle(op.bundle, op.input as never, { db: db as never }) as Record<string, unknown>[];
  // A NO-RETURNING write (v1 default) hands back the mode-2 summary row [{changes,lastInsertRowid}];
  // v1's actual return is null. Canonicalize to `null` — the ONLY dialect-independent representation
  // (the summary's fields diverge across engines). A returning write (upsert) canonicalizes its PK rows.
  if (out.length > 0 && out[0] !== null && typeof out[0] === 'object' && 'changes' in (out[0] as object) && 'lastInsertRowid' in (out[0] as object)) {
    return 'null';
  }
  return canonRows(out, FIELDS[op.id]);
}

function main(): void {
  mkdirSync(ART, { recursive: true });
  // Shared read seed (the read ops query it; the write/tx cells copy it fresh + mutate).
  const seedPath = join(ART, 'bench.db');
  if (existsSync(seedPath)) rmSync(seedPath);
  const sdb = new Database(seedPath);
  for (const s of ddl('sqlite')) sdb.exec(s);
  const tx = sdb.transaction(() => { for (const s of seedStatements('sqlite')) sdb.prepare(s.sql).run(...(s.params as never[])); });
  tx();
  sdb.close();

  const oracle: Record<string, { kind: string; input: Record<string, unknown>; result: string }> = {};
  for (const op of buildOps()) {
    const db = freshDb();
    oracle[op.id] = { kind: op.kind, input: op.input, result: oracleFor(op, db) };
    db.close();
  }
  writeFileSync(join(ART, 'oracle.json'), JSON.stringify(oracle, null, 2));
  process.stderr.write(`oracle: ${Object.keys(oracle).length} ops → .artifacts/oracle.json + bench.db\n`);
}

main();
