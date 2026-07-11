// ════════════════════════════════════════════════════════════════════════════
// Cross-language benchmark ENTRY (epic #44) — generate → run matrix → write report.
// ════════════════════════════════════════════════════════════════════════════
//
//   npx tsx benchmark/crosslang/run.ts                     # full run, writes CROSS-LANG.md
//   BENCH_ITER=500 BENCH_MICRO_ITER=5000 npx tsx …/run.ts  # quick
//   CROSSLANG_CASES=find,inList npx tsx …/run.ts           # a case subset
//   CROSSLANG_ONLY=ts npx tsx …/run.ts                     # a language subset
//
// SQLite is in-process (better-sqlite3) — no docker needed for the primary run.
// The generated bundle artifact (generated/bundles.json) must exist first
// (`npx tsx benchmark/crosslang/generate.ts`); this entry regenerates it.

import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { runMatrix, DEFAULT_CONFIG, type HarnessConfig } from './harness.js';
import { renderReport } from './report.js';
import { CROSSLANG_CASE_IDS, type CrosslangCaseId } from './contract.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const OUT_MD = resolve(HERE, '../CROSS-LANG.md');
const OUT_JSON = resolve(HERE, '.crosslang-results.json');
const REPO = resolve(HERE, '../..');

function config(): HarnessConfig {
  const iter = Number(process.env.BENCH_ITER ?? DEFAULT_CONFIG.iterations);
  const warmup = Number(process.env.BENCH_WARMUP ?? DEFAULT_CONFIG.warmup);
  const casesEnv = process.env.CROSSLANG_CASES;
  const cases = casesEnv
    ? (casesEnv.split(',').map((s) => s.trim()).filter(Boolean) as CrosslangCaseId[])
    : ([...CROSSLANG_CASE_IDS] as CrosslangCaseId[]);
  return {
    ...DEFAULT_CONFIG,
    iterations: iter,
    warmup,
    throughputIterations: Number(process.env.BENCH_TP_ITER ?? Math.min(iter, 2000)),
    microIterations: Number(process.env.BENCH_MICRO_ITER ?? DEFAULT_CONFIG.microIterations),
    microWarmup: Number(process.env.BENCH_MICRO_WARMUP ?? DEFAULT_CONFIG.microWarmup),
    cases,
  };
}

async function main(): Promise<void> {
  const cfg = config();
  console.error(`Cross-language bench — iterations=${cfg.iterations} warmup=${cfg.warmup} micro=${cfg.microIterations} cases=${cfg.cases?.length}\n`);

  // Regenerate the shared bundle artifact (consume-only compile — src untouched).
  console.error('Generating makeSQL bundles (generated/bundles.json)…');
  execFileSync('npx', ['tsx', 'benchmark/crosslang/generate.ts'], { cwd: REPO, stdio: 'inherit' });

  const result = await runMatrix(cfg);
  mkdirSync(dirname(OUT_JSON), { recursive: true });
  writeFileSync(OUT_JSON, JSON.stringify(result, null, 2));
  writeFileSync(OUT_MD, renderReport(result));
  console.error(`\nWrote ${OUT_MD}`);
  console.error(`Wrote ${OUT_JSON}`);
}

void main();
