// ════════════════════════════════════════════════════════════════════════════
// TS ORM-plan adapter RUNNER (epic #63) — the NDJSON contract entry (reference).
// ════════════════════════════════════════════════════════════════════════════
//
// The production TS cell: the harness spawns this once; it loads the shared 19-op plan
// artifact + connects the three real drivers, then speaks contract.ts over stdin/stdout.
// It executes the SAME 19 ORM ops the ORM-bench litedbmodel column runs (the executor is
// orm-exec-ts.ts, verified 57/57 on real DBs), so TS numbers == the ORM column.
//
// The other languages (Python/PHP/Rust/Go) implement the SAME NDJSON phases over their
// shipped driver seam — this is the reference. Run: npx tsx …/adapters/ts/orm-runner.ts

import { createInterface } from 'node:readline';
import { performance } from 'node:perf_hooks';
import { buildOrmPlanArtifact, type OrmDialect, type OpPlan } from '../../orm-plan.js';
import { sqliteDriver, pgDriver, mysqlDriver, type OrmDriver } from '../../orm-exec-ts.js';
import {
  PG_SCHEMA_NAME, MYSQL_DB_NAME, PG_CONN, PG_BOOT_CONN, MYSQL_CONN, MYSQL_BOOT_CONN,
} from '../../domain.js';
import { encodeMessage, type Request, type Response } from '../../contract.js';

const art = buildOrmPlanArtifact();

function write(msg: Response): void {
  process.stdout.write(encodeMessage(msg));
}

// Lazy per-dialect drivers; a connection FAILURE is an honest per-cell skip reason (never a stall).
const drivers: Partial<Record<OrmDialect, OrmDriver>> = {};
const connectFailed: Partial<Record<OrmDialect, string>> = {};
async function driverFor(dialect: OrmDialect): Promise<OrmDriver | null> {
  if (drivers[dialect]) return drivers[dialect]!;
  if (connectFailed[dialect]) return null;
  try {
    if (dialect === 'sqlite') return (drivers.sqlite = sqliteDriver());
    if (dialect === 'postgres') return (drivers.postgres = await pgDriver(PG_SCHEMA_NAME, PG_CONN as never, PG_BOOT_CONN as never));
    return (drivers.mysql = await mysqlDriver(MYSQL_DB_NAME, MYSQL_CONN as never, MYSQL_BOOT_CONN as never));
  } catch (err) {
    connectFailed[dialect] = err instanceof Error ? err.message.split('\n')[0] : String(err);
    return null;
  }
}

function planFor(caseId: string, dialect: OrmDialect): OpPlan {
  const byD = art[caseId];
  if (!byD) throw new Error(`unknown op '${caseId}'`);
  return byD[dialect];
}

async function collect(drv: OrmDriver, plan: OpPlan, warmup: number, iterations: number): Promise<number[]> {
  for (let i = 0; i < warmup; i++) await drv.run(plan);
  const samples: number[] = new Array(iterations);
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    await drv.run(plan);
    samples[i] = performance.now() - t0;
  }
  return samples;
}

async function handle(req: Request): Promise<void> {
  switch (req.kind) {
    case 'run': {
      const drv = await driverFor(req.dialect);
      if (!drv) { write({ kind: 'skipped', case: req.case, dialect: req.dialect, reason: `${req.dialect} unreachable (${connectFailed[req.dialect]})` }); return; }
      const samplesMs = await collect(drv, planFor(req.case, req.dialect), req.warmup, req.iterations);
      write({ kind: 'run', case: req.case, dialect: req.dialect, samplesMs });
      return;
    }
    case 'throughput': {
      const drv = await driverFor(req.dialect);
      if (!drv) { write({ kind: 'skipped', case: req.case, dialect: req.dialect, reason: `${req.dialect} unreachable` }); return; }
      const plan = planFor(req.case, req.dialect);
      const t0 = performance.now();
      for (let i = 0; i < req.iterations; i++) await drv.run(plan);
      write({ kind: 'throughput', case: req.case, dialect: req.dialect, elapsedMs: performance.now() - t0, completed: req.iterations });
      return;
    }
    case 'cost': {
      // queries/op·rows/op fairness: rows/op is the executor's returned count (reads = rows read,
      // writes = statements executed). queries/op is derived from the plan shape (statements +
      // relation stages), matching the logical work every language does for the SAME SQL.
      const plan = planFor(req.case, req.dialect);
      const queries = plan.kind === 'read' ? plan.reads.length + plan.relations.length : plan.statements.length;
      const drv = await driverFor('sqlite');
      let rows = 0;
      if (drv) { try { rows = await drv.run(planFor(req.case, 'sqlite')); } catch { rows = 0; } }
      write({ kind: 'cost', case: req.case, dialect: req.dialect, queries, rows });
      return;
    }
    case 'rss':
      write({ kind: 'rss', rssBytes: process.memoryUsage().rss });
      return;
    case 'shutdown':
      for (const d of Object.values(drivers)) if (d) await d.close();
      process.exit(0);
  }
}

async function main(): Promise<void> {
  write({ kind: 'ready', language: 'ts', impl: 'runtime', readyAtEpochMs: Date.now() });
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
