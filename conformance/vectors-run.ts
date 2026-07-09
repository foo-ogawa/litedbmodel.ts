/**
 * litedbmodel SCP conformance orchestrator (WS7a, #30) — the cross-language LOCK.
 *
 * Mirrors graphddb's `conformance/vectors-run.ts`: it runs the SAME frozen vector corpus
 * (`conformance/vectors/*.json`) through the litedbmodel SCP runtime in EACH language and
 * asserts every runtime produces BYTE-IDENTICAL pass/fail per suite — the §10 promise
 * ("同一 IR+入力 → 同一 SQL + 同一結果" across languages).
 *
 * Each language runner emits a machine-readable JSON summary as its LAST stdout line:
 *   {"lang","suites":{<suite>:{"pass","fail"}},"total_pass","total_fail","version_mismatch"}
 * and exits 0 (all pass) / 1 (any fail) / 2 (corpus-version fail-closed).
 *
 * ## WS7a status: TS reference only
 *
 * WS7a delivers the TS reference runner + the corpus + the scaffold. The language RUNTIMES are
 * WS7b-e — until each is implemented, its runner entry is a not-yet-implemented stub. This
 * orchestrator DISCOVERS which language runners are runnable and runs those; a language whose
 * runner is still a WS7b-e stub is reported as PENDING (not a failure) so WS7a is green while
 * WS7b-e fill in. Once a language runner is real, it joins the cross-language agreement check
 * automatically. Set STRICT_ALL_LANGS=1 to require every language (the post-WS7e CI gate).
 *
 * Prerequisite: `npm run build:scp` (TS consumes the built dist artifact).
 */
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = join(HERE, '..');
const VECTORS_DIR = join(HERE, 'vectors');

interface SuiteTally {
  pass: number;
  fail: number;
}
interface Summary {
  lang: string;
  suites: Record<string, SuiteTally>;
  total_pass: number;
  total_fail: number;
  version_mismatch: boolean;
}
interface RunnerResult {
  lang: string;
  status: number | null;
  summary: Summary | null;
  stderr: string;
  error?: string;
  pending?: boolean;
}

/** Parse the LAST non-empty stdout line as the JSON summary. */
function parseSummary(stdout: string): Summary | null {
  const lines = stdout.trimEnd().split('\n');
  for (let i = lines.length - 1; i >= 0; i--) {
    const l = lines[i].trim();
    if (!l) continue;
    try {
      const j = JSON.parse(l);
      if (j && typeof j === 'object' && 'lang' in j && 'total_pass' in j) return j as Summary;
    } catch {
      // not JSON — keep scanning upward
    }
    break;
  }
  return null;
}

const env = { ...process.env, LITEDBMODEL_VECTORS: VECTORS_DIR };

/** A language runner entry: how to launch it + a predicate for whether it is scaffolded yet. */
interface LangRunner {
  lang: string;
  /** Absolute path whose ABSENCE means "WS7b-e not implemented yet" → PENDING, not FAIL. */
  readyPath: string;
  cmd: string;
  args: string[];
  cwd?: string;
}

const RUNNERS: LangRunner[] = [
  { lang: 'ts', readyPath: join(REPO, 'dist', 'scp', 'index.mjs'), cmd: 'npx', args: ['tsx', join(HERE, 'vectors-runner.ts')] },
  { lang: 'py', readyPath: join(REPO, 'python', 'litedbmodel_runtime', 'vectors_runner.py'), cmd: 'python3', args: [join(REPO, 'python', 'litedbmodel_runtime', 'vectors_runner.py')] },
  { lang: 'php', readyPath: join(REPO, 'php', 'conformance', 'vectors_runner.php'), cmd: 'php', args: [join(REPO, 'php', 'conformance', 'vectors_runner.php')] },
  { lang: 'go', readyPath: join(REPO, 'go', 'vectors_runner', 'main.go'), cmd: 'go', args: ['run', './vectors_runner'], cwd: join(REPO, 'go') },
  { lang: 'rust', readyPath: join(REPO, 'rust', 'vectors_runner', 'src', 'main.rs'), cmd: 'cargo', args: ['run', '--quiet', '--bin', 'vectors_runner'], cwd: join(REPO, 'rust') },
];

/** A runner is "implemented" only if its entry file has real body (not the WS7b-e stub marker). */
function isImplemented(r: LangRunner): boolean {
  if (!existsSync(r.readyPath)) return false;
  // TS is the reference — always implemented (the dist artifact IS its readyPath).
  if (r.lang === 'ts') return true;
  // A WS7b-e stub carries the sentinel; treat as pending until the runtime body lands.
  try {
    return !readFileSync(r.readyPath, 'utf8').includes('WS7B_E_RUNTIME_STUB');
  } catch {
    return false;
  }
}

function runProc(r: LangRunner): RunnerResult {
  if (!isImplemented(r)) return { lang: r.lang, status: null, summary: null, stderr: '', pending: true };
  const proc = spawnSync(r.cmd, r.args, {
    cwd: r.cwd ?? REPO,
    env,
    encoding: 'utf-8',
    maxBuffer: 32 * 1024 * 1024,
  });
  if (proc.error) return { lang: r.lang, status: null, summary: null, stderr: '', error: String(proc.error) };
  return { lang: r.lang, status: proc.status, summary: parseSummary(proc.stdout ?? ''), stderr: proc.stderr ?? '' };
}

function main(): void {
  const strict = process.env.STRICT_ALL_LANGS === '1';
  console.log('conformance(vectors): litedbmodel SCP corpus × language runtimes');
  console.log(`conformance(vectors): vectors dir ${VECTORS_DIR}\n`);

  const results = RUNNERS.map(runProc);
  let problems = 0;

  for (const r of results) {
    if (r.pending) {
      const msg = `  [PEND] ${r.lang.padEnd(4)} runtime not implemented yet (WS7b-e)`;
      if (strict) {
        console.error(msg + ' — STRICT_ALL_LANGS=1');
        problems++;
      } else {
        console.log(msg);
      }
      continue;
    }
    if (r.error) {
      console.error(`  [FAIL] ${r.lang.padEnd(4)} could not launch: ${r.error}`);
      problems++;
      continue;
    }
    if (!r.summary) {
      console.error(`  [FAIL] ${r.lang.padEnd(4)} no JSON summary (exit ${r.status})`);
      if (r.stderr) console.error(r.stderr.split('\n').slice(-8).join('\n'));
      problems++;
      continue;
    }
    const s = r.summary;
    const suiteStr = Object.entries(s.suites).map(([k, t]) => `${k} ${t.pass}/${t.pass + t.fail}`).join(', ');
    const bad = s.total_fail > 0 || s.version_mismatch || r.status !== 0;
    console.log(
      `  [${bad ? 'FAIL' : 'OK  '}] ${r.lang.padEnd(4)} ${s.total_pass}/${s.total_pass + s.total_fail} ` +
        `(${suiteStr})${s.version_mismatch ? ' VERSION-MISMATCH' : ''} [exit ${r.status}]`,
    );
    if (bad) problems++;
  }

  // Cross-language agreement: for each suite, every RUNNABLE language must report the identical
  // pass/fail split (byte-identical runtime behavior). With only TS runnable in WS7a this is a
  // 1-language no-op; it engages automatically as WS7b-e runners come online.
  const runnable = results.filter((r) => r.summary);
  const suites = new Set<string>();
  for (const r of runnable) for (const k of Object.keys(r.summary!.suites)) suites.add(k);
  console.log('\nconformance(vectors): cross-language agreement per suite');
  for (const suite of [...suites].sort()) {
    const perLang = runnable
      .filter((r) => r.summary!.suites[suite])
      .map((r) => ({ lang: r.lang, t: r.summary!.suites[suite] }));
    const distinct = new Set(perLang.map((p) => `${p.t.pass}/${p.t.fail}`));
    const langs = perLang.map((p) => p.lang).join(',');
    if (distinct.size <= 1) {
      const t = perLang[0]?.t;
      console.log(`  [OK  ] ${suite.padEnd(9)} ${t ? `${t.pass}/${t.pass + t.fail}` : '-'} agreed across [${langs}]`);
    } else {
      console.error(`  [FAIL] ${suite.padEnd(9)} DISAGREEMENT across [${langs}]:`);
      for (const p of perLang) console.error(`           ${p.lang}: pass=${p.t.pass} fail=${p.t.fail}`);
      problems++;
    }
  }

  // ── Mode-3 codegen leg (WS7f, #35) ──────────────────────────────────────────
  // A SEPARATE conformance concern (static codegen vs the mode-2 IR-reference runtimes above), so
  // it is NOT folded into the per-language agreement matrix (its per-vector check count differs by
  // design). It proves the bc-generator output is byte-identical to the thin-runtime: the emitted
  // TS + Python modules are EXECUTED, and the go/rust/php emitted source is compiled/parsed.
  const codegenRunner = join(HERE, 'codegen', 'codegen-runner.ts');
  if (existsSync(codegenRunner) && existsSync(join(REPO, 'dist', 'scp', 'index.mjs'))) {
    console.log('\nconformance(vectors): mode-3 codegen leg (bc shared generator)');
    const cg = spawnSync('npx', ['tsx', codegenRunner], { cwd: REPO, env, encoding: 'utf-8', maxBuffer: 32 * 1024 * 1024 });
    const summary = parseSummary(cg.stdout ?? '');
    if (!summary) {
      console.error(`  [FAIL] codegen no JSON summary (exit ${cg.status})`);
      if (cg.stderr) console.error(cg.stderr.split('\n').slice(-8).join('\n'));
      problems++;
    } else {
      const suiteStr = Object.entries(summary.suites).map(([k, t]) => `${k} ${t.pass}/${t.pass + t.fail}`).join(', ');
      const bad = summary.total_fail > 0 || summary.version_mismatch || cg.status !== 0;
      console.log(
        `  [${bad ? 'FAIL' : 'OK  '}] codegen ${summary.total_pass}/${summary.total_pass + summary.total_fail} (${suiteStr}) [exit ${cg.status}]`,
      );
      if (bad) problems++;
    }
  } else {
    console.log('\nconformance(vectors): mode-3 codegen leg SKIPPED (run `npm run build:scp` first)');
  }

  if (problems > 0) {
    console.error(`\nconformance(vectors): ${problems} problem(s).`);
    process.exit(1);
  }
  console.log(`\nconformance(vectors): PASS (${runnable.length} runtime(s) clean + codegen leg)`);
}

main();
