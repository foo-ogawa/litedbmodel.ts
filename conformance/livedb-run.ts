/**
 * litedbmodel SCP LIVE-DB orchestrator (WS7g, #36) — the coordinated cross-language live-DB pass.
 *
 * Runs the WS7g live-DB corpus (`conformance/vectors-livedb/livedb.json` — the exec/tx bundles
 * compiled for `postgres` + `mysql`) through EACH language runtime's live-DB leg (Python / PHP /
 * Go / Rust) against ONE shared dockerized Postgres + MySQL stack, sequentially, and asserts every
 * runtime reproduces the frozen SQLite reference on BOTH live dialects (the §10 promise). Each
 * language leg isolates its tables in its OWN namespace (Postgres schema / MySQL database
 * scp_py / scp_php / scp_go / scp_rust) so the shared stack has no cross-contamination.
 *
 * Prerequisite: the docker stack is UP with host-published ports (docker-compose.livedb.yml) and
 * the live-DB corpus is generated (`npm run conformance:gen:livedb`). Typical driver:
 *
 *   npm run docker:livedb:up          # postgres+mysql on host ports 5433/3307
 *   npm run conformance:gen:livedb    # (re)write conformance/vectors-livedb/livedb.json
 *   npx tsx conformance/livedb-run.ts # run all 4 language legs
 *   npm run docker:livedb:down
 *
 * Each language leg emits a machine-readable JSON summary as its LAST stdout line:
 *   {"lang":"<x>-livedb","suites":{"livedb-pg":{pass,fail},"livedb-mysql":{pass,fail}},...}
 * and exits 0 (all pass) / 1 (any fail) / 2 (corpus mismatch) / 3 (DB unreachable — LOUD, never
 * a silent skip). This orchestrator fails if ANY leg is not all-pass, or any leg is unrunnable.
 *
 * The Python venv / driver install is environment-specific; set LIVEDB_PY to point at the Python
 * interpreter that has psycopg + pymysql + behavior_contracts (defaults to `python3`).
 */
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = join(HERE, '..');
const CORPUS = join(HERE, 'vectors-livedb', 'livedb.json');

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

function parseSummary(stdout: string): Summary | null {
  const lines = stdout.trimEnd().split('\n');
  for (let i = lines.length - 1; i >= 0; i--) {
    const l = lines[i].trim();
    if (!l) continue;
    try {
      const j = JSON.parse(l);
      if (j && typeof j === 'object' && 'lang' in j && 'total_pass' in j) return j as Summary;
    } catch {
      // keep scanning upward
    }
    break;
  }
  return null;
}

interface LangLeg {
  lang: string;
  cmd: string;
  args: string[];
  cwd?: string;
}

const LEGS: LangLeg[] = [
  { lang: 'py', cmd: process.env.LIVEDB_PY || 'python3', args: [join(REPO, 'python', 'litedbmodel_runtime', 'livedb_runner.py')] },
  { lang: 'php', cmd: 'php', args: [join(REPO, 'php', 'conformance', 'livedb_runner.php')] },
  { lang: 'go', cmd: 'go', args: ['run', './livedb_runner'], cwd: join(REPO, 'go') },
  { lang: 'rust', cmd: 'cargo', args: ['run', '--quiet', '-p', 'livedb_runner'], cwd: join(REPO, 'rust') },
];

// The env each leg inherits (host-published docker ports; matches docker-compose.livedb.yml).
const env = {
  ...process.env,
  LITEDBMODEL_LIVEDB_VECTORS: CORPUS,
  TEST_DB_HOST: process.env.TEST_DB_HOST || 'localhost',
  TEST_DB_PORT: process.env.TEST_DB_PORT || '5433',
  TEST_MYSQL_HOST: process.env.TEST_MYSQL_HOST || '127.0.0.1',
  TEST_MYSQL_PORT: process.env.TEST_MYSQL_PORT || '3307',
  GOPRIVATE: process.env.GOPRIVATE || 'github.com/foo-ogawa/*',
};

function main(): void {
  console.log('conformance(livedb): litedbmodel SCP live-DB corpus × language runtimes (real PG + MySQL)');
  console.log(`conformance(livedb): corpus ${CORPUS}\n`);
  if (!existsSync(CORPUS)) {
    console.error(`conformance(livedb): FAIL — live-DB corpus missing (run: npm run conformance:gen:livedb)`);
    process.exit(2);
  }

  let anyFail = false;
  for (const leg of LEGS) {
    const proc = spawnSync(leg.cmd, leg.args, {
      cwd: leg.cwd ?? REPO,
      env,
      encoding: 'utf-8',
      maxBuffer: 32 * 1024 * 1024,
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    if (proc.error) {
      console.error(`  [ERR ] ${leg.lang.padEnd(4)} could not launch: ${proc.error}`);
      anyFail = true;
      continue;
    }
    const summary = parseSummary(proc.stdout ?? '');
    if (proc.status === 3) {
      console.error(`  [FAIL] ${leg.lang.padEnd(4)} DB UNREACHABLE (exit 3) — start the docker stack first`);
      anyFail = true;
      continue;
    }
    if (!summary) {
      console.error(`  [FAIL] ${leg.lang.padEnd(4)} no JSON summary (exit ${proc.status})`);
      anyFail = true;
      continue;
    }
    const pg = summary.suites['livedb-pg'] ?? { pass: 0, fail: 0 };
    const my = summary.suites['livedb-mysql'] ?? { pass: 0, fail: 0 };
    const ok = summary.total_fail === 0 && proc.status === 0;
    const tag = ok ? 'OK  ' : 'FAIL';
    console.log(
      `  [${tag}] ${leg.lang.padEnd(4)} pg ${pg.pass}/${pg.pass + pg.fail}, mysql ${my.pass}/${my.pass + my.fail} (total ${summary.total_pass}/${summary.total_pass + summary.total_fail}) [exit ${proc.status}]`,
    );
    if (!ok) anyFail = true;
  }

  console.log('');
  if (anyFail) {
    console.error('conformance(livedb): FAIL — a language leg did not pass all live-DB vectors');
    process.exit(1);
  }
  console.log('conformance(livedb): PASS (all 4 language runtimes green on live PG + MySQL)');
}

main();
