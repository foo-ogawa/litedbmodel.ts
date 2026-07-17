// ════════════════════════════════════════════════════════════════════════════
// Cross-language benchmark ORCHESTRATOR — build → run each standalone
// bench (each writes its CSV) → collect (CSVs → CROSS-LANG.md).
// ════════════════════════════════════════════════════════════════════════════
//
//   npx tsx benchmark/crosslang/run.ts                     # full run
//   BENCH_ITER=5 BENCH_WARMUP=2 npx tsx …/run.ts           # quick smoke
//   CROSSLANG_ONLY=ts,python npx tsx …/run.ts              # a language subset
//
// This orchestrator does NOT drive the languages over a protocol. It only: (1)
// regenerates the shared orm-plan.json artifact, (2) builds the
// cells that must be pre-compiled — rust/go native binaries AND the TS runner (esbuild →
// plain JS, launched with bare `node` not `tsx`, so no transpiler is resident in the
// measured process inflating RSS; see the note below), (3) SPAWNS each language's STANDALONE bench as its own
// process (each self-measures ALL 19 ops × 3 dialects and writes .results/<lang>.csv),
// then (4) runs the collector (collect.ts), the ONLY program that reads the CSVs and
// renders CROSS-LANG.md. Budgets flow to each bench purely via env (BENCH_*).
//
// SQLite is in-process; PG :5433 + MySQL :3307 are the dockerized live DBs.

import { rmSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(HERE, '../..');
const RESULTS_DIR = resolve(HERE, '.results');

// ── The standalone bench entry per language (each writes .results/<lang>.csv) ────
interface LangBench { language: string; build?: () => void; run: () => void }

function sh(command: string, args: string[], cwd = REPO): void {
  execFileSync(command, args, { cwd, stdio: 'inherit', env: process.env });
}

const RUST_DIR = resolve(HERE, 'adapters/rust');
const RUST_BIN = process.env.RUST_ORM_BIN ?? resolve(RUST_DIR, 'target/release/lm_orm');
const GO_DIR = resolve(REPO, 'go/lm_bench/lm_orm');
const GO_BIN = process.env.GO_ORM_BIN ?? resolve(GO_DIR, 'lm_orm');
const PY = process.env.PYTHON_BIN ?? 'python3';
const PHP = process.env.PHP_BIN ?? 'php';

// The TS cell is PRE-COMPILED to plain JS and launched with bare `node` (like rust/go are pre-built
// native binaries) — NOT run through `tsx`. tsx is an esbuild-based on-the-fly transpiler that stays
// RESIDENT in the measured process, inflating the reported RSS with a build tool that has nothing to
// do with the litedbmodel runtime (measured: same-budget A/B on this bench, tsx ≈ 230–250 MB vs the
// compiled node path ≈ 160–190 MB — a ~50–70 MB esbuild tax the other languages never pay). Bundling
// the runner to JS and running it under node keeps the MEASURED process esbuild-free, so TS RSS
// reflects node + the litedbmodel runtime + drivers, the same class of footprint the other languages
// report. NB: an end-of-run RSS is a V8 high-water mark that never shrinks (post-GC live heap here is
// only ~16 MB); the residual over the ~74 MB import-time floor is legitimate workload memory (pg Pool
// + mysql2 buffers + V8 heap growth), NOT esbuild — do not chase the floor by forcing GC or reading
// RSS before the workload, that would change WHAT is measured. The native/node drivers are EXTERNAL —
// they load from node_modules at runtime (never bundle the `.node` addons). Wired here (not run-bench.sh)
// so `run.ts` alone produces a fair TS cell.
const TS_SRC = 'benchmark/crosslang/adapters/ts/orm-runner.ts';
const TS_BUILT = resolve(HERE, 'adapters/ts/orm-runner.built.mjs');
function buildTsCell(): void {
  sh('npx', [
    'esbuild', TS_SRC,
    '--bundle', '--format=esm', '--platform=node',
    '--external:better-sqlite3', '--external:pg', '--external:mysql2',
    `--outfile=${TS_BUILT}`,
  ]);
}

const BENCHES: LangBench[] = [
  { language: 'ts', build: buildTsCell, run: () => sh('node', [TS_BUILT]) },
  { language: 'python', run: () => sh(PY, ['benchmark/crosslang/adapters/python/orm_runner.py']) },
  { language: 'php', run: () => sh(PHP, ['benchmark/crosslang/adapters/php/orm-runner.php']) },
  {
    language: 'rust',
    build: () => sh('cargo', ['build', '--release'], RUST_DIR),
    run: () => sh(RUST_BIN, []),
  },
  {
    language: 'go',
    build: () => sh('go', ['build', '-o', 'lm_orm', '.'], GO_DIR),
    run: () => sh(GO_BIN, []),
  },
];

function selected(): LangBench[] {
  const only = process.env.CROSSLANG_ONLY;
  if (!only) return BENCHES;
  const set = new Set(only.split(',').map((s) => s.trim()).filter(Boolean));
  return BENCHES.filter((b) => set.has(b.language));
}

function main(): void {
  const benches = selected();
  console.error(`Cross-language bench — standalone-CSV flow. Languages: ${benches.map((b) => b.language).join(', ')}\n`);

  // Fresh results dir (a stale CSV would poison the collector's report).
  rmSync(RESULTS_DIR, { recursive: true, force: true });

  // (1) Regenerate the shared 19-op plan artifact (consume-only compile — src untouched).
  console.error('Generating the ORM-plan artifact (generated/orm-plan.json)…');
  sh('npx', ['tsx', 'benchmark/crosslang/gen-orm-plan.ts']);

  // (2) + (3) Build native cells, then run each standalone bench (each writes its CSV).
  for (const b of benches) {
    if (b.build) {
      console.error(`\n▶ build ${b.language} …`);
      try { b.build(); } catch (err) { console.error(`  ✗ build ${b.language} failed: ${String(err)}`); continue; }
    }
    console.error(`\n▶ run ${b.language} bench …`);
    try { b.run(); } catch (err) { console.error(`  ✗ ${b.language} bench failed: ${String(err)}`); }
  }

  // (4) Collect: the ONLY program that reads the CSVs and renders CROSS-LANG.md.
  console.error('\n▶ collect (.results/*.csv → CROSS-LANG.md) …');
  sh('npx', ['tsx', 'benchmark/crosslang/collect.ts']);
}

main();
