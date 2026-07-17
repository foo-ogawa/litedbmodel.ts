// ════════════════════════════════════════════════════════════════════════════
// TS ORM-plan bench — STANDALONE CSV writer — the reference.
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
import { sqliteDriver, pgDriver, mysqlDriver, type OrmDriver, type ExecImpl } from '../../orm-exec-ts.js';
import {
  PG_SCHEMA_NAME, MYSQL_DB_NAME, PG_CONN, PG_BOOT_CONN, MYSQL_CONN, MYSQL_BOOT_CONN,
} from '../../domain.js';

// Raw-driver BASELINE gets its OWN isolated PG schema / MySQL db so the two impls never clobber
// each other's tables (and could run concurrently). Same real driver, so the runtime÷baseline ratio
// isolates litedbmodel's over-driver cost, not a driver difference.
const PG_BASELINE_SCHEMA = `${PG_SCHEMA_NAME}_baseline`;
const MYSQL_BASELINE_DB = `${MYSQL_DB_NAME}_baseline`;
const PG_BASELINE_CONN = { ...PG_BOOT_CONN, options: `-c search_path=${PG_BASELINE_SCHEMA}` };
const MYSQL_BASELINE_CONN = { ...MYSQL_BOOT_CONN, database: MYSQL_BASELINE_DB };

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

// Lazy per-dialect drivers PER IMPL; a connection FAILURE is an honest per-cell skip (never a stall).
// `runtime` = the shipped litedbmodel path; `raw` = the bare-driver baseline (same real driver, same
// SQL, no litedbmodel de-box) → runtime÷baseline = litedbmodel's over-driver overhead.
const drivers: Record<ExecImpl, Partial<Record<OrmDialect, OrmDriver>>> = { runtime: {}, raw: {} };
const connectFailed: Record<ExecImpl, Partial<Record<OrmDialect, string>>> = { runtime: {}, raw: {} };
async function driverFor(dialect: OrmDialect, impl: ExecImpl): Promise<OrmDriver | null> {
  if (drivers[impl][dialect]) return drivers[impl][dialect]!;
  if (connectFailed[impl][dialect]) return null;
  const [pgSchema, pgConn] = impl === 'raw' ? [PG_BASELINE_SCHEMA, PG_BASELINE_CONN] : [PG_SCHEMA_NAME, PG_CONN];
  const [myDb, myConn] = impl === 'raw' ? [MYSQL_BASELINE_DB, MYSQL_BASELINE_CONN] : [MYSQL_DB_NAME, MYSQL_CONN];
  try {
    if (dialect === 'sqlite') return (drivers[impl].sqlite = sqliteDriver(impl));
    if (dialect === 'postgres') return (drivers[impl].postgres = await pgDriver(pgSchema, pgConn as never, PG_BOOT_CONN as never, impl));
    return (drivers[impl].mysql = await mysqlDriver(myDb, myConn as never, MYSQL_BOOT_CONN as never, impl));
  } catch (err) {
    connectFailed[impl][dialect] = err instanceof Error ? err.message.split('\n')[0] : String(err);
    return null;
  }
}

function planFor(caseId: string, dialect: OrmDialect): OpPlan {
  const byD = art[caseId];
  if (!byD) throw new Error(`unknown op '${caseId}'`);
  return byD[dialect];
}

async function benchCell(drv: OrmDriver, baseline: OrmDriver | null, caseId: string, dialect: OrmDialect): Promise<void> {
  const plan = planFor(caseId, dialect);

  // cost (fairness): queries/op from the plan shape; rows/op = the executor's returned count.
  const queries = plan.kind === 'read' ? plan.reads.length + plan.relations.length : plan.statements.length;
  const rowsCount = await drv.run(plan);
  emit(caseId, dialect, 'cost_queries', queries);
  emit(caseId, dialect, 'cost_rows', rowsCount);

  // latency (RUNTIME): warmup, then one row PER timed iteration.
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

  // latency (BASELINE): the IDENTICAL SQL/params through the bare driver (no litedbmodel de-box),
  // SAME warmup + timed iterations → runtime÷baseline = litedbmodel's over-driver overhead. Emitted
  // as `baseline_latency_ms`; the collector splits it into the `impl: baseline` cell for §②.
  if (baseline) {
    for (let i = 0; i < WARMUP; i++) await baseline.run(plan);
    for (let i = 0; i < ITER; i++) {
      const b0 = performance.now();
      await baseline.run(plan);
      emit(caseId, dialect, 'baseline_latency_ms', performance.now() - b0);
    }
  }
}

async function main(): Promise<void> {
  // cold = process start → runtime ready (interpreter + module/artifact load), measured BEFORE any
  // driver connect — the first real connection (drivers connect lazily).
  const coldMs = Math.max(0, Date.now() - SPAWNED_AT);

  for (const dialect of ORM_DIALECTS) {
    const drv = await driverFor(dialect, 'runtime');
    if (!drv) {
      const reason = `${dialect} unreachable (${connectFailed.runtime[dialect]})`;
      for (const caseId of ORM_OP_IDS) emit(caseId, dialect, 'skipped', reason);
      continue;
    }
    // The bare-driver baseline (same real driver, same SQL). A baseline connect failure is NOT a
    // whole-cell skip — the runtime numbers still stand; only the ÷sql ratio for that dialect drops.
    const baseline = await driverFor(dialect, 'raw');
    for (const caseId of ORM_OP_IDS) {
      try {
        await benchCell(drv, baseline, caseId, dialect);
      } catch (err) {
        emit(caseId, dialect, 'skipped', err instanceof Error ? err.message.split('\n')[0] : String(err));
      }
    }
  }

  emit('', '', 'cold_ms', coldMs);
  emit('', '', 'rss_bytes', process.memoryUsage().rss);
  emit('', '', 'warmup', WARMUP);

  for (const byImpl of Object.values(drivers)) for (const d of Object.values(byImpl)) if (d) await d.close();

  mkdirSync(dirname(OUT_CSV), { recursive: true });
  writeFileSync(OUT_CSV, rows.join('\n') + '\n');
  process.stderr.write(`[${LANGUAGE}] wrote ${OUT_CSV} (${rows.length - 1} rows)\n`);
}

void main();
