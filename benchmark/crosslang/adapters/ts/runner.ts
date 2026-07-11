// ════════════════════════════════════════════════════════════════════════════
// TS cross-language adapter RUNNER (epic #44) — the subprocess entry point.
// ════════════════════════════════════════════════════════════════════════════
//
// The harness spawns this once per TS cell with `--impl=<sql|codegen|ir|dynamic|
// prepared|v1>`. It speaks the contract (../../contract.ts) over stdin/stdout:
// one JSON request per line, one JSON response per line. This is the REFERENCE
// runner; the Python/PHP/Rust/Go runners implement the SAME phases for their
// three impls (sql/codegen/ir).
//
// The impl axis (litedbmodel exec surfaces):
//   sql       — hand-optimized raw SQL via better-sqlite3 (baseline 1.0×)
//   codegen   — bundle IR resident as a native literal + fingerprint-verified once
//               at load (NO JSON parse per run), then executed via the makeSQL catalog
//   ir        — the bundle is loaded FROM JSON at cold start, then executed via the
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
import { fingerprintComponentGraph } from 'behavior-contracts';
import * as lm from '../../../../dist/scp/index.mjs';
import { encodeMessage, type Request, type Response, type Impl, type CrosslangCaseId } from '../../contract.js';
import { collectSamples, runConcurrent } from '../../timing.js';
import {
  freshDb, readsContract, writesContract, writeGateContract,
  READ_ENTRY, READ_RELATION, INPUTS, SQL_BASELINE, SCHEMA,
} from '../../domain.js';
import { mockDb } from '../../microbench.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const BUNDLES_PATH = resolve(HERE, '../../generated/bundles.json');

const implArg = process.argv.find((a) => a.startsWith('--impl='));
const impl = (implArg?.split('=')[1] ?? 'sql') as Impl;

function write(msg: Response): void {
  process.stdout.write(encodeMessage(msg));
}

// ── Load the generated artifact (the ir/codegen cells consume this) ───────────
interface CaseArt {
  case: string; kind: 'read' | 'relation' | 'batch' | 'tx';
  entry?: string; withRelation?: string; relation?: unknown;
  bundle: any; input: any; fingerprint: string; expectedQueries: number; expectedRows: number;
}
const rawArtifact = readFileSync(BUNDLES_PATH, 'utf8');
const artifact = JSON.parse(rawArtifact) as { cases: CaseArt[] };
const CASE = new Map(artifact.cases.map((c) => [c.case, c]));

// codegen: the bundle IR is resident as a native literal (already parsed at module
// load) and its fingerprint is verified ONCE here (the codegen fail-closed load
// check). No per-run JSON parse. Compare against `ir`, which re-parses from JSON.
function bakedBundles(): Map<string, CaseArt> {
  for (const c of artifact.cases) {
    const recomputed = fingerprintComponentGraph(lm.bundleToPortableIR(c.bundle));
    if (recomputed !== c.fingerprint) throw new Error(`codegen: fingerprint mismatch for ${c.case} (${recomputed} != ${c.fingerprint})`);
  }
  return CASE;
}
// ir: parse the bundle JSON at cold start (the language-neutral load path).
function parsedBundles(): Map<string, CaseArt> {
  const reparsed = JSON.parse(rawArtifact) as { cases: CaseArt[] };
  return new Map(reparsed.cases.map((c) => [c.case, c]));
}

// v1: the shipped litedbmodel@1.2.10 eager path (DBConditions WHERE build).
const requireV1 = createRequire(resolve(HERE, '../../v1ts/package.json'));
let V1: any;
function loadV1(): any {
  if (!V1) V1 = requireV1('litedbmodel-v1');
  return V1;
}

// ── Per-impl op factory: returns a zero-arg op that does ONE logical case op ──
// against the given db (a real freshDb for DB-backed, a mockDb for micro).
function makeOp(caseId: CrosslangCaseId, db: any, bundles?: Map<string, CaseArt>): () => unknown {
  const c = CASE.get(caseId)!;
  const input = c.input;

  if (impl === 'sql') {
    return () => SQL_BASELINE[caseId].run(db);
  }

  if (impl === 'v1') return v1Op(caseId, db);

  // codegen / ir / prepared all execute a PRE-COMPILED bundle (compile-once).
  // dynamic recompiles per call via executeBehavior.
  if (impl === 'dynamic') {
    if (c.kind === 'batch') {
      const cols = ['author_id', 'title', 'status', 'views', 'created_at'];
      return () => {
        const b = lm.compileCreateManyBundle('BatchInsert', { tableName: 'posts', columns: cols, records: INPUTS.batchInsert.rows as any }, 'sqlite');
        return lm.executeTransactionBundle(b, {}, { db });
      };
    }
    if (c.kind === 'tx') {
      return () => {
        const b = lm.compileWriteBundle(writesContract, 'Create', writeGateContract, 'create', 'sqlite');
        return lm.executeTransactionBundle(b, input, { db });
      };
    }
    const rel = READ_RELATION[caseId];
    if (rel) return () => lm.read(readsContract, input, { db, entry: READ_ENTRY[caseId], relations: [rel.decl], with: { [rel.withName]: true } });
    return () => lm.executeBehavior(readsContract, input, { db, entry: READ_ENTRY[caseId] });
  }

  // codegen / ir / prepared: use the resolved bundle (baked, parsed, or the same).
  const src = bundles ?? CASE;
  const bundle = src.get(caseId)!.bundle;
  if (c.kind === 'batch' || c.kind === 'tx') {
    return () => lm.executeTransactionBundle(bundle, c.kind === 'tx' ? input : {}, { db });
  }
  if (c.kind === 'relation') {
    const withName = c.withRelation!;
    return () => lm.readBundle(bundle, input, { db, with: { [withName]: true } });
  }
  return () => lm.executeBundle(bundle, input, { db });
}

// v1 (1.2.10) eager path: reproduce each case via DBConditions WHERE build + raw
// better-sqlite3 execute (v1 has no SCP bundle; this IS the v1 client-side path).
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
      return () => SQL_BASELINE.batchInsert.run(db); // v1 createMany == the same multi-VALUES insert
    case 'writeTxGate':
      return () => SQL_BASELINE.writeTxGate.run(db); // v1 hand-tx of the same gate group
    default:
      throw new Error(`v1: unknown case ${caseId}`);
  }
}

// The bundle source for the current impl (codegen bakes+verifies, ir reparses).
function bundleSource(): Map<string, CaseArt> | undefined {
  if (impl === 'codegen') return bakedBundles();
  if (impl === 'ir') return parsedBundles();
  if (impl === 'prepared') return CASE;
  return undefined; // sql / dynamic / v1 don't use a shared bundle map
}

// ── Fairness cost probe: DML statements + DB rows read (excl. tx-control) ──────
const TX_CONTROL = /^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE|PRAGMA)\b/i;
function costOf(caseId: CrosslangCaseId): { queries: number; rows: number } {
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
    makeOp(caseId, db, bundleSource())();
  } finally {
    db.prepare = orig;
    db.close();
  }
  return { queries, rows };
}

// ── Request handler ───────────────────────────────────────────────────────────
async function handle(req: Request): Promise<void> {
  switch (req.kind) {
    case 'run': {
      const db = freshDb();
      const op = makeOp(req.case, db, bundleSource());
      const samplesMs = await collectSamples(op, req.warmup, req.iterations);
      db.close();
      write({ kind: 'run', case: req.case, samplesMs });
      return;
    }
    case 'throughput': {
      const db = freshDb();
      const op = makeOp(req.case, db, bundleSource());
      const { elapsedMs, completed } = await runConcurrent(op, req.iterations, req.concurrency);
      db.close();
      write({ kind: 'throughput', case: req.case, elapsedMs, completed });
      return;
    }
    case 'micro': {
      // I/O-excluded: mock driver, no DB round-trip. Times ONLY the client-side path.
      const db = mockDb();
      const op = makeOp(req.case, db as any, bundleSource());
      const samplesMs = await collectSamples(op, req.warmup, req.iterations);
      write({ kind: 'micro', case: req.case, samplesMs });
      return;
    }
    case 'rss': {
      write({ kind: 'rss', rssBytes: process.memoryUsage().rss });
      return;
    }
    case 'cost': {
      const { queries, rows } = costOf(req.case);
      write({ kind: 'cost', case: req.case, queries, rows });
      return;
    }
    case 'shutdown':
      process.exit(0);
  }
}

async function main(): Promise<void> {
  // Touch the impl's cold-start work (load bundles / verify fingerprint / require v1)
  // BEFORE announcing ready, so cold start reflects the impl's real init cost.
  if (impl === 'codegen') bakedBundles();
  if (impl === 'ir') parsedBundles();
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
void performance; // (perf_hooks imported for parity with other adapters)
