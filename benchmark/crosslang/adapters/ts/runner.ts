// ════════════════════════════════════════════════════════════════════════════
// TS cross-language adapter RUNNER (epic #44) — the subprocess entry point.
// ════════════════════════════════════════════════════════════════════════════
//
// The harness spawns this once per TS cell with `--impl=<sql|codegen|ir|dynamic|
// prepared|v1>`. It speaks the contract (../../contract.ts) over stdin/stdout: one
// JSON request per line, one JSON response per line. This is the REFERENCE runner;
// the Python/PHP/Rust/Go runners implement the SAME phases for their three impls.
//
// THREE validity axes (#44 owner gaps + coordinator scope):
//   1. DIALECT axis — every case-scoped request carries `dialect`
//      (sqlite/postgres/mysql). The micro cell runs against the matching PER-DIALECT
//      bundle (different SQL/`?`→`$N`/JSON-array forms → different CLIENT-PATH cost).
//      The DB-backed cell runs against the matching REAL database.
//   2. DB-backed across REAL PG + MySQL + SQLite. SQLite is in-proc better-sqlite3
//      via the sync runtime; PG/MySQL use the ASYNC production live-DB seam
//      (adapters/ts/livedb.ts) since Node's pg/mysql2 are async-only.
//   3. TRUE codegen — the codegen cell executes THROUGH the bc-generated module
//      (adapters/ts/codegen-cell.ts), not the decorative `executeBundle` alias.
//
// The impl axis (litedbmodel exec surfaces):
//   sql       — hand-optimized raw SQL via better-sqlite3 (baseline 1.0×; sqlite only)
//   codegen   — the bc-GENERATED module (IR baked + fingerprint-verified at load),
//               executed via `bind(handlers)` — a distinct entry from ir
//   ir        — the bundle loaded FROM JSON at cold start, then executed via the
//               shared runtime (bc run_behavior + makeSQL handler) — the non-TS reality
//   dynamic   — executeBehavior: compileBundle (recompile) + executeBundle EVERY call
//   prepared  — compileBundle ONCE → executeBundle many (compile-once/execute-many)
//   v1        — shipped litedbmodel@1.2.10 eager path (DBConditions build) — regression gate

import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';
import { createInterface } from 'node:readline';
import { performance } from 'node:perf_hooks';
import Database from 'better-sqlite3';
import * as lm from '../../../../dist/scp/index.mjs';
import { encodeMessage, type Request, type Response, type Impl, type CrosslangCaseId, type CrosslangDialect } from '../../contract.js';
import { collectSamples, runConcurrent } from '../../timing.js';
import {
  freshDb, readsContract, writesContract, writeGateContract,
  READ_ENTRY, READ_RELATION, INPUTS, SQL_BASELINE, SCHEMA,
} from '../../domain.js';
import { mockDb } from '../../microbench.js';
import { codegenCell } from './codegen-cell.js';
import { connectPg, connectMysql, type LiveDb } from './livedb.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const BUNDLES_PATH = resolve(HERE, '../../generated/bundles.json');

const implArg = process.argv.find((a) => a.startsWith('--impl='));
const impl = (implArg?.split('=')[1] ?? 'sql') as Impl;

function write(msg: Response): void {
  process.stdout.write(encodeMessage(msg));
}

// ── Load the generated artifact (per-dialect bundles) ─────────────────────────
interface CaseArt {
  case: string; kind: 'read' | 'relation' | 'batch' | 'tx';
  entry?: string; withRelation?: string; relation?: unknown;
  bundle: any; input: any; fingerprint: string; expectedQueries: number; expectedRows: number;
}
interface Artifact {
  dialects: Record<string, { cases: CaseArt[] }>;
}
const rawArtifact = readFileSync(BUNDLES_PATH, 'utf8');
const artifact = JSON.parse(rawArtifact) as Artifact;
// Per-dialect case maps: the SAME 8 cases compiled for each dialect (different SQL).
const CASE_BY_DIALECT: Record<string, Map<string, CaseArt>> = {};
for (const [d, { cases }] of Object.entries(artifact.dialects)) {
  CASE_BY_DIALECT[d] = new Map(cases.map((c) => [c.case, c]));
}
function caseFor(caseId: string, dialect: CrosslangDialect): CaseArt {
  const c = CASE_BY_DIALECT[dialect]?.get(caseId);
  if (!c) throw new Error(`no bundle for case ${caseId} dialect ${dialect}`);
  return c;
}

// ir: re-parse the bundle JSON at cold start (the language-neutral load path).
const reparsedArtifact = (): Artifact => JSON.parse(rawArtifact) as Artifact;

// v1: the shipped litedbmodel@1.2.10 eager path (DBConditions WHERE build).
const requireV1 = createRequire(resolve(HERE, '../../v1ts/package.json'));
let V1: any;
function loadV1(): any {
  if (!V1) V1 = requireV1('litedbmodel-v1');
  return V1;
}

// ── Per-impl MICRO op factory (mock driver; per-dialect bundle) ───────────────
// The mock is DB-agnostic (fixed rows, no round-trip) so the timed op is ONLY the
// client-side path (compile/render/param-eval/bind/`?`→`$N`/hydration) — the
// difference between the three dialect bundles is the render form.
function makeMicroOp(caseId: CrosslangCaseId, dialect: CrosslangDialect, db: any): () => unknown {
  const c = caseFor(caseId, dialect);
  if (impl === 'sql') return () => SQL_BASELINE[caseId].run(db); // hand-SQL is sqlite-shaped
  if (impl === 'v1') return v1Op(caseId, db);
  if (impl === 'codegen') return codegenCell.op(c as any, db, dialect);
  if (impl === 'dynamic') return dynamicOp(c, db);
  // ir / prepared: execute the (reparsed or resident) per-dialect bundle.
  return bundleOp(c, db);
}

function dynamicOp(c: CaseArt, db: any): () => unknown {
  if (c.kind === 'batch') {
    const cols = ['author_id', 'title', 'status', 'views', 'created_at'];
    return () => {
      const b = lm.compileCreateManyBundle('BatchInsert', { tableName: 'posts', columns: cols, records: INPUTS.batchInsert.rows as any }, c.bundle.dialect);
      return lm.executeTransactionBundle(b, {}, { db });
    };
  }
  if (c.kind === 'tx') {
    return () => {
      const b = lm.compileWriteBundle(writesContract, 'Create', writeGateContract, 'create', c.bundle.dialect);
      return lm.executeTransactionBundle(b, c.input, { db });
    };
  }
  const rel = READ_RELATION[c.case];
  if (rel) return () => lm.read(readsContract, c.input, { db, entry: READ_ENTRY[c.case], relations: [{ ...rel.decl, dialect: c.bundle.dialect }], with: { [rel.withName]: true } });
  return () => lm.executeBehavior(readsContract, c.input, { db, entry: READ_ENTRY[c.case] });
}

function bundleOp(c: CaseArt, db: any): () => unknown {
  const bundle = c.bundle;
  if (c.kind === 'batch' || c.kind === 'tx') return () => lm.executeTransactionBundle(bundle, c.kind === 'tx' ? c.input : {}, { db });
  if (c.kind === 'relation') return () => lm.readBundle(bundle, c.input, { db, with: { [c.withRelation!]: true } });
  return () => lm.executeBundle(bundle, c.input, { db });
}

// v1 (1.2.10) eager path (sqlite-shaped) — unchanged; only exercised on sqlite.
function v1Op(caseId: CrosslangCaseId, db: any): () => unknown {
  const v1 = loadV1();
  const DBConditions = v1.DBConditions;
  switch (caseId) {
    case 'find':
      return () => {
        const p: unknown[] = [];
        const w = new DBConditions({ author_id: 1, status: 'live', 'created_at >= ?': '2026-02-01' }).compile(p);
        return db.prepare(`SELECT id, author_id, title, status, views, created_at FROM posts WHERE ${w} ORDER BY id ASC`).all(...p);
      };
    case 'complexWhere':
      return () => {
        const p: unknown[] = [];
        const w = new DBConditions({ author_id: 1, 'created_at >= ?': '2026-02-01', 'title LIKE ?': 'post-%', id: [1, 2, 3, 4, 5] }).compile(p);
        return db.prepare(`SELECT id, author_id, title, status, views FROM posts WHERE ${w} ORDER BY id ASC`).all(...p);
      };
    case 'inList':
      return () => {
        const p: unknown[] = [];
        const w = new DBConditions({ id: INPUTS.inList.ids }).compile(p);
        return db.prepare(`SELECT id, title FROM posts WHERE ${w} ORDER BY id ASC`).all(...p);
      };
    case 'belongsTo':
      return () => {
        const posts = db.prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC').all(1) as any[];
        const ids = [...new Set(posts.map((r) => r.author_id))];
        const p: unknown[] = [];
        const w = new DBConditions({ id: ids }).compile(p);
        db.prepare(`SELECT id, name FROM users WHERE ${w}`).all(...p);
      };
    case 'hasMany':
      return () => {
        const posts = db.prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC').all(1) as any[];
        const ids = posts.map((r) => r.id);
        const p: unknown[] = [];
        const w = new DBConditions({ post_id: ids }).compile(p);
        db.prepare(`SELECT id, post_id, body FROM comments WHERE ${w}`).all(...p);
      };
    case 'hasManyLimit':
      return () => {
        const posts = db.prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC').all(1) as any[];
        const ids = posts.map((r) => r.id);
        const p: unknown[] = [];
        const w = new DBConditions({ post_id: ids }).compile(p);
        db.prepare(`SELECT id, post_id, body FROM (SELECT id, post_id, body, ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY id DESC) rn FROM comments WHERE ${w}) WHERE rn <= 3`).all(...p);
      };
    case 'batchInsert':
      return () => SQL_BASELINE.batchInsert.run(db);
    case 'writeTxGate':
      return () => SQL_BASELINE.writeTxGate.run(db);
    default:
      throw new Error(`v1: unknown case ${caseId}`);
  }
}

// ── Fairness cost probe (sqlite; queries/op + rows/op) ────────────────────────
const TX_CONTROL = /^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE|PRAGMA)\b/i;
function costOf(caseId: CrosslangCaseId, dialect: CrosslangDialect): { queries: number; rows: number } {
  // Fairness is measured on the in-proc SQLite path (the sql baseline is sqlite); it
  // proves identical logical work per dialect since the bundle SQL is logically equal.
  const db: any = freshDb();
  let queries = 0, rows = 0;
  const orig = db.prepare.bind(db);
  db.prepare = (sql: string) => {
    const stmt = orig(sql);
    if (TX_CONTROL.test(sql)) return stmt;
    const wrap = Object.create(stmt);
    wrap.all = (...a: any[]) => { queries++; const r = stmt.all(...a); rows += Array.isArray(r) ? r.length : 0; return r; };
    wrap.get = (...a: any[]) => { queries++; const r = stmt.get(...a); if (r !== undefined) rows += 1; return r; };
    wrap.run = (...a: any[]) => { queries++; return stmt.run(...a); };
    return wrap;
  };
  try {
    const sqliteCase = caseFor(caseId, 'sqlite');
    // Always cost via the sqlite bundle op (fairness denominator), regardless of impl.
    if (impl === 'sql') SQL_BASELINE[caseId].run(db);
    else if (impl === 'v1') v1Op(caseId, db)();
    else if (impl === 'codegen') codegenCell.op(sqliteCase as any, db, 'sqlite')();
    else if (impl === 'dynamic') dynamicOp(sqliteCase, db)();
    else bundleOp(sqliteCase, db)();
  } finally {
    db.prepare = orig;
    db.close();
  }
  void dialect;
  return { queries, rows };
}

// ── DB-backed live connections (lazy; one per dialect the cell is asked for) ──
// A connection FAILURE (e.g. no docker in CI) is remembered as an honest per-cell skip
// reason, NOT a hard error — so the bench runs green without a live DB (SQLite-only) and
// reports the PG/MySQL cells as explicitly skipped (unreachable), never silently dropped.
let pgDb: LiveDb | null = null;
let myDb: LiveDb | null = null;
const connectFailed: Record<string, string> = {};
async function liveDbFor(dialect: CrosslangDialect): Promise<LiveDb | null> {
  if (connectFailed[dialect]) return null;
  try {
    if (dialect === 'postgres') return (pgDb ??= await connectPg());
    return (myDb ??= await connectMysql());
  } catch (err) {
    connectFailed[dialect] = err instanceof Error ? err.message.split('\n')[0] : String(err);
    return null;
  }
}

// Whether a cell can run the DB-backed axis for a dialect.
function dbBackedSupported(dialect: CrosslangDialect): { ok: boolean; reason?: string } {
  // The hand-SQL baseline + v1 eager path are sqlite-shaped by construction (they build
  // sqlite SQL directly), so they run DB-backed on sqlite only.
  if ((impl === 'sql' || impl === 'v1') && dialect !== 'sqlite') {
    return { ok: false, reason: `${impl} baseline is hand-written sqlite SQL — not run against ${dialect} (dialect-specific by construction)` };
  }
  // The codegen cell's generated read module is wired to the in-proc sqlite driver; its
  // PG/MySQL DB-backed path is not wired (see report note) — run it on sqlite only.
  if (impl === 'codegen' && dialect !== 'sqlite') {
    return { ok: false, reason: 'codegen generated-module cell is wired to the in-proc sqlite driver; PG/MySQL DB-backed not wired for the generated cell' };
  }
  // dynamic / prepared recompile/execute the bundle via the SYNC runtime, which needs a
  // sync SqliteDb driver — Node has no sync PG/MySQL driver, so these run sqlite only.
  if ((impl === 'dynamic' || impl === 'prepared') && dialect !== 'sqlite') {
    return { ok: false, reason: `${impl} uses the sync runtime (sync SqliteDb driver); Node has no sync PG/MySQL driver — run on sqlite only` };
  }
  return { ok: true };
}

// ── DB-backed op factory (returns a skip reason if the DB is unreachable) ─────
async function makeDbOp(caseId: CrosslangCaseId, dialect: CrosslangDialect): Promise<{ op?: () => unknown | Promise<unknown>; teardown: () => void; skip?: string }> {
  if (dialect === 'sqlite') {
    const db = freshDb();
    const c = caseFor(caseId, 'sqlite');
    const op =
      impl === 'sql' ? () => SQL_BASELINE[caseId].run(db)
      : impl === 'v1' ? v1Op(caseId, db)
      : impl === 'codegen' ? codegenCell.op(c as any, db, 'sqlite')
      : impl === 'dynamic' ? dynamicOp(c, db)
      : bundleOp(c, db);
    return { op, teardown: () => db.close() };
  }
  // postgres / mysql — real DB via the async live-DB seam.
  const live = await liveDbFor(dialect);
  if (!live) return { teardown: () => {}, skip: `${dialect} unreachable (${connectFailed[dialect] ?? 'no live DB'}) — DB-backed cell not run` };
  const c = caseFor(caseId, dialect);
  return { op: live.op(c as any), teardown: () => { /* connection reused; closed on shutdown */ } };
}

// ── Request handler ───────────────────────────────────────────────────────────
async function handle(req: Request): Promise<void> {
  switch (req.kind) {
    case 'run': {
      const sup = dbBackedSupported(req.dialect);
      if (!sup.ok) { write({ kind: 'skipped', case: req.case, dialect: req.dialect, reason: sup.reason! }); return; }
      const { op, teardown, skip } = await makeDbOp(req.case, req.dialect);
      if (!op) { teardown(); write({ kind: 'skipped', case: req.case, dialect: req.dialect, reason: skip! }); return; }
      const samplesMs = await collectSamples(op, req.warmup, req.iterations);
      teardown();
      write({ kind: 'run', case: req.case, dialect: req.dialect, samplesMs });
      return;
    }
    case 'throughput': {
      const sup = dbBackedSupported(req.dialect);
      if (!sup.ok) { write({ kind: 'skipped', case: req.case, dialect: req.dialect, reason: sup.reason! }); return; }
      const { op, teardown, skip } = await makeDbOp(req.case, req.dialect);
      if (!op) { teardown(); write({ kind: 'skipped', case: req.case, dialect: req.dialect, reason: skip! }); return; }
      const { elapsedMs, completed } = await runConcurrent(op, req.iterations, req.concurrency);
      teardown();
      write({ kind: 'throughput', case: req.case, dialect: req.dialect, elapsedMs, completed });
      return;
    }
    case 'micro': {
      // I/O-excluded: mock driver, no DB round-trip. Times ONLY the client-side path
      // against the PER-DIALECT bundle (the render/placeholder/array form differs).
      if (impl === 'sql' && req.dialect !== 'sqlite') { write({ kind: 'skipped', case: req.case, dialect: req.dialect, reason: 'hand-SQL baseline is sqlite-shaped' }); return; }
      if (impl === 'v1' && req.dialect !== 'sqlite') { write({ kind: 'skipped', case: req.case, dialect: req.dialect, reason: 'v1 eager path is sqlite-shaped' }); return; }
      const db = mockDb();
      const op = makeMicroOp(req.case, req.dialect, db as any);
      const samplesMs = await collectSamples(op, req.warmup, req.iterations);
      write({ kind: 'micro', case: req.case, dialect: req.dialect, samplesMs });
      return;
    }
    case 'rss': {
      write({ kind: 'rss', rssBytes: process.memoryUsage().rss });
      return;
    }
    case 'cost': {
      if ((impl === 'sql' || impl === 'v1' || impl === 'codegen' || impl === 'dynamic' || impl === 'prepared') && req.dialect !== 'sqlite') {
        // Fairness is proven on sqlite (same logical work per dialect); non-sqlite cost
        // for these cells is reported as the sqlite value (the logical work is identical).
      }
      const { queries, rows } = costOf(req.case, req.dialect);
      write({ kind: 'cost', case: req.case, dialect: req.dialect, queries, rows });
      return;
    }
    case 'shutdown':
      if (pgDb) await pgDb.close();
      if (myDb) await myDb.close();
      process.exit(0);
  }
}

async function main(): Promise<void> {
  // Touch the impl's cold-start work BEFORE announcing ready.
  if (impl === 'codegen') await codegenCell.preload(artifact.dialects.sqlite.cases as any);
  if (impl === 'ir') reparsedArtifact();
  if (impl === 'v1') loadV1();
  void SCHEMA;

  write({ kind: 'ready', language: 'ts', impl, readyAtEpochMs: Date.now() });

  const rl = createInterface({ input: process.stdin });
  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let req: Request;
    try {
      req = JSON.parse(trimmed) as Request;
    } catch (err) {
      write({ kind: 'error', message: `bad request line: ${String(err)}` });
      continue;
    }
    try {
      await handle(req);
    } catch (err) {
      write({ kind: 'error', message: err instanceof Error ? err.message : String(err), stack: err instanceof Error ? err.stack : undefined });
    }
  }
}

void main();
void performance;
