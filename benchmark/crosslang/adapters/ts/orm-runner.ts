// ════════════════════════════════════════════════════════════════════════════
// TS ORM-plan bench — STANDALONE CSV writer (epic #63) — the reference.
// ════════════════════════════════════════════════════════════════════════════
//
// ONE standalone process (no stdin/stdout protocol): loads the shared 19-op plan
// artifact + connects the three real drivers, self-measures ALL 19 ops × 3 dialects
// (cold at startup, warmup + timed iterations for latency, throughput, cost
// queries/rows, rss at end), and writes a FLAT CSV to
// benchmark/crosslang/.results/ts.csv. The bench NEVER reports — the collector
// (collect.ts) reads the CSVs and renders CROSS-LANG.md.
//
// It executes the SAME 19 ORM ops the ORM-bench litedbmodel column runs (the
// executor is orm-exec-ts.ts, verified 57/57 on real DBs), so TS numbers == the
// ORM column. The other languages (Python/PHP/Rust/Go) are the SAME shape.
//
// CSV schema (RAW values only — the collector owns all percentile/ratio math):
//   language,case,dialect,metric,value
// metrics: latency_ms (one row PER timed iteration), throughput_elapsed_ms,
//   throughput_completed, cost_queries, cost_rows, cold_ms, rss_bytes,
//   skipped (value = reason). cold_ms / rss_bytes are process-level (case+dialect empty).
//
// Budgets come from env (BENCH_WARMUP / BENCH_ITER / BENCH_TP_ITER) — no protocol.
//   npx tsx benchmark/crosslang/adapters/ts/orm-runner.ts

import { performance } from 'node:perf_hooks';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildOrmPlanArtifact, ORM_OP_IDS, ORM_DIALECTS, type OrmDialect, type OpPlan } from '../../orm-plan.js';
import { sqliteDriver, pgDriver, mysqlDriver, type OrmDriver } from '../../orm-exec-ts.js';
import {
  PG_SCHEMA_NAME, MYSQL_DB_NAME, PG_CONN, PG_BOOT_CONN, MYSQL_CONN, MYSQL_BOOT_CONN,
} from '../../domain.js';

const LANGUAGE = 'ts';
const SPAWNED_AT = Date.now();
const HERE = dirname(fileURLToPath(import.meta.url));
const OUT_CSV = resolve(HERE, '../../.results', `${LANGUAGE}.csv`);

const WARMUP = Number(process.env.BENCH_WARMUP ?? 50);
const ITER = Number(process.env.BENCH_ITER ?? 300);
const TP_ITER = Number(process.env.BENCH_TP_ITER ?? Math.min(ITER, 2000));

const art = buildOrmPlanArtifact();

// ── flat CSV row buffer ───────────────────────────────────────────────────────
const rows: string[] = ['language,case,dialect,metric,value'];
function emit(caseId: string, dialect: string, metric: string, value: string | number): void {
  rows.push(`${LANGUAGE},${caseId},${dialect},${metric},${csvField(value)}`);
}
function csvField(v: string | number): string {
  const s = String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
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

async function benchCell(drv: OrmDriver, caseId: string, dialect: OrmDialect): Promise<void> {
  const plan = planFor(caseId, dialect);

  // cost (fairness): queries/op from the plan shape; rows/op = the executor's returned count.
  const queries = plan.kind === 'read' ? plan.reads.length + plan.relations.length : plan.statements.length;
  const rowsCount = await drv.run(plan);
  emit(caseId, dialect, 'cost_queries', queries);
  emit(caseId, dialect, 'cost_rows', rowsCount);

  // latency: warmup, then one row PER timed iteration.
  for (let i = 0; i < WARMUP; i++) await drv.run(plan);
  for (let i = 0; i < ITER; i++) {
    const t0 = performance.now();
    await drv.run(plan);
    emit(caseId, dialect, 'latency_ms', performance.now() - t0);
  }

  // throughput: a tight loop of TP_ITER ops, raw elapsed + completed.
  const t0 = performance.now();
  for (let i = 0; i < TP_ITER; i++) await drv.run(plan);
  emit(caseId, dialect, 'throughput_elapsed_ms', performance.now() - t0);
  emit(caseId, dialect, 'throughput_completed', TP_ITER);
}

async function main(): Promise<void> {
  // cold = process start → runtime ready (interpreter + module/artifact load), measured BEFORE any
  // driver connect — the same point the old harness read as the `ready` line (drivers were lazy).
  const coldMs = Math.max(0, Date.now() - SPAWNED_AT);

  for (const dialect of ORM_DIALECTS) {
    const drv = await driverFor(dialect);
    if (!drv) {
      const reason = `${dialect} unreachable (${connectFailed[dialect]})`;
      for (const caseId of ORM_OP_IDS) emit(caseId, dialect, 'skipped', reason);
      continue;
    }
    for (const caseId of ORM_OP_IDS) {
      try {
        await benchCell(drv, caseId, dialect);
      } catch (err) {
        emit(caseId, dialect, 'skipped', err instanceof Error ? err.message.split('\n')[0] : String(err));
      }
    }
  }

  emit('', '', 'cold_ms', coldMs);
  emit('', '', 'rss_bytes', process.memoryUsage().rss);
  emit('', '', 'warmup', WARMUP);

  for (const d of Object.values(drivers)) if (d) await d.close();

  mkdirSync(dirname(OUT_CSV), { recursive: true });
  writeFileSync(OUT_CSV, rows.join('\n') + '\n');
  process.stderr.write(`[${LANGUAGE}] wrote ${OUT_CSV} (${rows.length - 1} rows)\n`);
}

void main();
