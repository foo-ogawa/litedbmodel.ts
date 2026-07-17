// ════════════════════════════════════════════════════════════════════════════
// Cross-language bench COLLECTOR — .results/*.csv → CROSS-LANG.md.
// ════════════════════════════════════════════════════════════════════════════
//
// The ONLY program that produces the report (strict responsibility separation: the
// per-language bench programs write RAW CSV and NEVER report). This globs
// benchmark/crosslang/.results/*.csv, parses the flat rows, computes the
// percentiles / throughput / ratios (REUSING metrics.ts), assembles a MatrixResult,
// and renders CROSS-LANG.md (REUSING report.ts).
//
// CSV schema (RAW values only) — one row per fact:
//   language,case,dialect,metric,value
// metrics: latency_ms (one row PER timed iteration), throughput_elapsed_ms,
//   throughput_completed, cost_queries, cost_rows, cold_ms, rss_bytes,
//   skipped (value = reason). cold_ms / rss_bytes are process-level (case+dialect empty).
//
//   npx tsx benchmark/crosslang/collect.ts

import { readdirSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  summarizeLatency, throughputOpsPerSec,
  type CellResult, type CaseResult, type DialectResult, type MatrixResult,
} from './metrics.js';
import { renderReport } from './report.js';
import { CROSSLANG_DIALECTS } from './contract.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const RESULTS_DIR = resolve(HERE, '.results');
const OUT_MD = resolve(HERE, '../CROSS-LANG.md');

interface CsvRow { language: string; case: string; dialect: string; metric: string; value: string }

// ── minimal CSV parse (our own writer's dialect: `"`-quoted fields, `""` escape) ─
function parseCsv(text: string): CsvRow[] {
  const lines = text.split('\n').filter((l) => l.length > 0);
  const out: CsvRow[] = [];
  for (let i = 1; i < lines.length; i++) {
    // header is line 0
    const fields = splitCsvLine(lines[i]);
    if (fields.length < 5) continue;
    out.push({ language: fields[0], case: fields[1], dialect: fields[2], metric: fields[3], value: fields[4] });
  }
  return out;
}
function splitCsvLine(line: string): string[] {
  const fields: string[] = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQ) {
      if (ch === '"') {
        if (line[i + 1] === '"') { cur += '"'; i++; } else inQ = false;
      } else cur += ch;
    } else if (ch === '"') inQ = true;
    else if (ch === ',') { fields.push(cur); cur = ''; }
    else cur += ch;
  }
  fields.push(cur);
  return fields;
}

// ── per-language accumulator ────────────────────────────────────────────────────
interface CellAccum {
  latency: Record<string, Record<string, number[]>>; // [dialect][case] -> raw ms samples (RUNTIME)
  // The raw-driver BASELINE latency samples (same SQL, no litedbmodel de-box). The collector emits
  // a SEPARATE `impl: baseline` cell from these so report.ts's §② computes runtime÷baseline.
  baselineLatency: Record<string, Record<string, number[]>>;
  tpElapsed: Record<string, Record<string, number>>;
  tpCompleted: Record<string, Record<string, number>>;
  costQueries: Record<string, Record<string, number>>;
  costRows: Record<string, Record<string, number>>;
  skipped: Record<string, Record<string, string>>;
  coldMs?: number;
  rssBytes?: number;
  warmup?: number;
  artifactBytes?: number;
}

function emptyAccum(): CellAccum {
  return { latency: {}, baselineLatency: {}, tpElapsed: {}, tpCompleted: {}, costQueries: {}, costRows: {}, skipped: {} };
}
function nest<T>(m: Record<string, Record<string, T>>, dialect: string, caseId: string, v: T): void {
  (m[dialect] ??= {})[caseId] = v;
}

function accumulate(rows: CsvRow[]): CellAccum {
  const a = emptyAccum();
  for (const r of rows) {
    const num = Number(r.value);
    switch (r.metric) {
      case 'latency_ms':
        ((a.latency[r.dialect] ??= {})[r.case] ??= []).push(num);
        break;
      case 'baseline_latency_ms':
        ((a.baselineLatency[r.dialect] ??= {})[r.case] ??= []).push(num);
        break;
      case 'throughput_elapsed_ms': nest(a.tpElapsed, r.dialect, r.case, num); break;
      case 'throughput_completed': nest(a.tpCompleted, r.dialect, r.case, num); break;
      case 'cost_queries': nest(a.costQueries, r.dialect, r.case, num); break;
      case 'cost_rows': nest(a.costRows, r.dialect, r.case, num); break;
      case 'skipped': nest(a.skipped, r.dialect, r.case, r.value); break;
      case 'cold_ms': a.coldMs = num; break;
      case 'rss_bytes': a.rssBytes = num; break;
      case 'warmup': a.warmup = num; break;
      case 'artifact_bytes': a.artifactBytes = num; break;
    }
  }
  return a;
}

// The RUNTIME cell (production path) — carries cost/throughput/skip + the resource metrics.
function toCell(language: string, a: CellAccum): CellResult {
  const dialects: Record<string, DialectResult> = {};
  // Union of every dialect that carried any signal (latency OR skip).
  const dialectSet = new Set<string>([...Object.keys(a.latency), ...Object.keys(a.skipped)]);
  for (const dialect of dialectSet) {
    const cases: Record<string, CaseResult> = {};
    const caseSet = new Set<string>([
      ...Object.keys(a.latency[dialect] ?? {}),
      ...Object.keys(a.skipped[dialect] ?? {}),
      ...Object.keys(a.costQueries[dialect] ?? {}),
    ]);
    for (const caseId of caseSet) {
      const skip = a.skipped[dialect]?.[caseId];
      if (skip) {
        cases[caseId] = { case: caseId, latency: summarizeLatency([]), skipped: skip };
        continue;
      }
      const samples = a.latency[dialect]?.[caseId] ?? [];
      const elapsed = a.tpElapsed[dialect]?.[caseId];
      const completed = a.tpCompleted[dialect]?.[caseId];
      cases[caseId] = {
        case: caseId,
        latency: summarizeLatency(samples),
        throughputOpsPerSec: elapsed !== undefined && completed !== undefined ? throughputOpsPerSec(completed, elapsed) : undefined,
        queries: a.costQueries[dialect]?.[caseId],
        rows: a.costRows[dialect]?.[caseId],
      };
    }
    dialects[dialect] = { cases };
  }
  return {
    language, impl: 'runtime',
    coldStartMs: a.coldMs, rssBytes: a.rssBytes,
    artifactSizeBytes: a.artifactBytes, // native cells only; interpreted cells omit it → report shows `—`
    dialects,
  };
}

// The raw-driver BASELINE cell (identical SQL, bare driver) — latency only; report.ts §② divides
// the runtime p50 by this baseline p50 per op×dialect. Returns undefined when NO baseline was
// measured (so the report falls back to its "omitted, ≈1.0× by construction" rationale).
function toBaselineCell(language: string, a: CellAccum): CellResult | undefined {
  const dialectSet = new Set<string>(Object.keys(a.baselineLatency));
  if (dialectSet.size === 0) return undefined;
  const dialects: Record<string, DialectResult> = {};
  for (const dialect of dialectSet) {
    const cases: Record<string, CaseResult> = {};
    for (const caseId of Object.keys(a.baselineLatency[dialect] ?? {})) {
      const samples = a.baselineLatency[dialect][caseId];
      if (!samples || samples.length === 0) continue;
      cases[caseId] = { case: caseId, latency: summarizeLatency(samples) };
    }
    if (Object.keys(cases).length) dialects[dialect] = { cases };
  }
  if (Object.keys(dialects).length === 0) return undefined;
  return { language, impl: 'baseline', dialects };
}

function iterationsOf(a: CellAccum): number {
  for (const byCase of Object.values(a.latency)) {
    for (const samples of Object.values(byCase)) if (samples.length) return samples.length;
  }
  return 0;
}

function main(): void {
  let files: string[];
  try {
    files = readdirSync(RESULTS_DIR).filter((f) => f.endsWith('.csv')).sort();
  } catch {
    console.error(`✗ no results dir at ${RESULTS_DIR} — run the per-language bench(es) first.`);
    process.exit(1);
  }
  if (files.length === 0) {
    console.error(`✗ no *.csv in ${RESULTS_DIR} — run the per-language bench(es) first.`);
    process.exit(1);
  }

  const cells: CellResult[] = [];
  let iterations = 0;
  let warmup = 0;
  for (const f of files) {
    const rows = parseCsv(readFileSync(join(RESULTS_DIR, f), 'utf8'));
    if (rows.length === 0) continue;
    const language = rows[0].language;
    const accum = accumulate(rows);
    iterations = Math.max(iterations, iterationsOf(accum));
    if (accum.warmup !== undefined) warmup = accum.warmup;
    cells.push(toCell(language, accum));
    const baseline = toBaselineCell(language, accum);
    if (baseline) cells.push(baseline);
    console.error(`  ✓ ${f} → ${language} (${rows.length} rows${baseline ? ', +baseline' : ''})`);
  }

  const result: MatrixResult = {
    generatedAt: new Date().toISOString(),
    iterations,
    warmup,
    dialects: [...CROSSLANG_DIALECTS],
    cells,
  };

  mkdirSync(dirname(OUT_MD), { recursive: true });
  writeFileSync(OUT_MD, renderReport(result));
  console.error(`\nWrote ${OUT_MD} (${cells.length} language cell(s)).`);
}

main();
