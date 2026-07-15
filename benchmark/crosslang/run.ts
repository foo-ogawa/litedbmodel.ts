// ════════════════════════════════════════════════════════════════════════════
// Cross-language benchmark ENTRY (epic #63) — generate → run matrix → write report.
// ════════════════════════════════════════════════════════════════════════════
//
//   npx tsx benchmark/crosslang/run.ts                     # full run, writes CROSS-LANG.md
//   BENCH_ITER=100 npx tsx …/run.ts                        # quick
//   CROSSLANG_CASES=findAll,create npx tsx …/run.ts        # an op subset
//   CROSSLANG_ONLY=ts npx tsx …/run.ts                     # a language subset
//
// SQLite is in-process; PG :5433 + MySQL :3307 are the dockerized live DBs. The shared
// orm-plan.json artifact (the 19 ORM ops × 3 dialects) is regenerated first.

import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { runMatrix, DEFAULT_CONFIG, type HarnessConfig } from './harness.js';
import { renderReport } from './report.js';
import { CROSSLANG_CASE_IDS, CROSSLANG_DIALECTS, type CrosslangCaseId, type CrosslangDialect } from './contract.js';

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
  const dialectsEnv = process.env.CROSSLANG_DIALECTS;
  const dialects = dialectsEnv
    ? (dialectsEnv.split(',').map((s) => s.trim()).filter(Boolean) as CrosslangDialect[])
    : ([...CROSSLANG_DIALECTS] as CrosslangDialect[]);
  return {
    ...DEFAULT_CONFIG,
    iterations: iter,
    warmup,
    throughputIterations: Number(process.env.BENCH_TP_ITER ?? Math.min(iter, 2000)),
    cases,
    dialects,
  };
}

async function main(): Promise<void> {
  const cfg = config();
  console.error(`Cross-language bench (#63) — iterations=${cfg.iterations} warmup=${cfg.warmup} ops=${cfg.cases?.length} dialects=${cfg.dialects?.join(',')}\n`);

  // Regenerate the shared 19-op plan artifact (consume-only compile — src untouched).
  console.error('Generating the ORM-plan artifact (generated/orm-plan.json)…');
  execFileSync('npx', ['tsx', 'benchmark/crosslang/gen-orm-plan.ts'], { cwd: REPO, stdio: 'inherit' });

  const result = await runMatrix(cfg);
  mkdirSync(dirname(OUT_JSON), { recursive: true });
  writeFileSync(OUT_JSON, JSON.stringify(result, null, 2));
  writeFileSync(OUT_MD, renderReport(result));
  console.error(`\nWrote ${OUT_MD}`);
  console.error(`Wrote ${OUT_JSON}`);
}

void main();
