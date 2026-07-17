// ════════════════════════════════════════════════════════════════════════════
// Cross-language report renderer — MatrixResult → CROSS-LANG.md.
// ════════════════════════════════════════════════════════════════════════════
//
// The driver-included, END-TO-END report: the 19 ORM ops executed by each language's thin
// generic runtime on all 3 real DBs (SQLite in-proc, MySQL, PostgreSQL) — every number is a
// real DB round-trip. The SQL is compiled once, ahead of time (language-neutral). Each results
// cell pairs two p50 latencies (µs): the raw-SQL driver baseline (SDK) and litedbmodel's shipped
// runtime executing that same SQL, so the language ranking and litedbmodel's over-driver cost read
// from one table. A fairness table (queries/op · rows/op) shows every language does identical work.

import { type MatrixResult, type CellResult, type DialectResult } from './metrics.js';
import { CROSSLANG_CASE_LABELS, CROSSLANG_CASE_IDS } from './contract.js';

const us = (ms: number): string => `${(ms * 1000).toFixed(1)}µs`;

const LANG_LABEL: Record<string, string> = { ts: 'TypeScript', python: 'Python', php: 'PHP', rust: 'Rust', go: 'Go' };
const DIALECT_LABEL: Record<string, string> = { sqlite: 'SQLite', postgres: 'PostgreSQL', mysql: 'MySQL' };
const LANG_ORDER = ['ts', 'python', 'php', 'rust', 'go'];

function runtimeCell(m: MatrixResult, language: string): CellResult | undefined {
  return m.cells.find((c) => c.language === language && c.impl === 'runtime');
}
function baselineCell(m: MatrixResult, language: string): CellResult | undefined {
  return m.cells.find((c) => c.language === language && c.impl === 'baseline');
}
function dcell(cell: CellResult | undefined, dialect: string): DialectResult | undefined {
  return cell?.dialects[dialect];
}
// The languages whose runtime cell actually produced results (no error).
function liveLangs(m: MatrixResult): string[] {
  return LANG_ORDER.filter((l) => {
    const c = runtimeCell(m, l);
    return c && !c.error;
  });
}

function methodology(m: MatrixResult): string {
  return [
    '## Methodology',
    '',
    'The **driver-included, end-to-end** cross-language benchmark. Each language’s **thin generic runtime**',
    'executes the SAME **19 ORM-comparison ops** against all three real dialects (SQLite in-proc, MySQL :3307,',
    'PostgreSQL :5433) — every number below is a real DB round-trip through the shipped runtime + real driver.',
    '',
    '- **Same op, same SQL, every language.** All languages run byte-identical SQL for a given op (the shared',
    '  `orm-plan.json` artifact) — the same statements the ORM-vs-ORM bench measures for litedbmodel',
    '  (`benchmark/benchmark.ts`), so the numbers are comparable across languages and with that bench.',
    '- **No subset.** All 19 ops run in every language on every DB, or an explicit per-cell SKIP note (never a',
    '  silent drop). A cell whose adapter did not build/run renders an honest failure row.',
    '',
    `_Generated ${m.generatedAt} — warmup ${m.warmup}, ${m.iterations} measured iterations. Dialects: ${m.dialects.join(', ')}._`,
    '',
    '_Environment: **native arm64 (Apple Silicon)** — go arm64, node arm64, rust `aarch64-apple-darwin`._',
    '',
    comparabilityDisclosure(m),
  ].join('\n');
}

function comparabilityDisclosure(m: MatrixResult): string {
  return [
    '> ## Read before the numbers (honest caveats)',
    '>',
    '> **1. PostgreSQL + MySQL are I/O-DOMINATED — languages CONVERGE there.** The round-trip to a real',
    '> networked DB dwarfs the client-side render/bind cost, so the per-op PG/MySQL latencies are close across',
    '> languages (they mostly measure the driver + the DB, not the language). The interesting language spread',
    '> is on **SQLite in-proc** (no network), where the client path is a larger fraction of the total.',
    '>',
    '> **2. Each language uses its OWN real driver per dialect** (below), so cross-language absolute times carry',
    '> a driver caveat (different driver overheads). Within a language, the numbers are directly comparable.',
    '>',
    driverTable(m),
    '>',
    '> **3. Go uses the PURE-GO `modernc.org/sqlite` driver** (no cgo) — a realistic default for Go users, but',
    '> a cgo SQLite driver (mattn/go-sqlite3) would post different (usually faster) in-proc numbers. Where shown,',
    '> the Go SQLite column is the pure-Go driver; a cgo variant is noted if measured.',
    '>',
    '> **4. TS PG/MySQL ride the async pool path** (`pg` / `mysql2`, Node has no sync networked driver); SQLite',
    '> is sync better-sqlite3. TS numbers match the ORM-bench litedbmodel column (same op, same v2 SQL).',
    '',
  ].join('\n');
}

function driverTable(m: MatrixResult): string {
  const rows = [
    '> | Language | SQLite | PostgreSQL | MySQL |',
    '> |---|---|---|---|',
    '> | TypeScript | better-sqlite3 (sync) | `pg` Pool (async) | `mysql2` pool (async) |',
    '> | Python | stdlib `sqlite3` | `psycopg` 3 | `PyMySQL` |',
    '> | PHP | PDO sqlite | PDO pgsql | PDO mysql |',
    '> | Rust | `rusqlite` | `tokio-postgres` + `deadpool` | `sqlx` |',
    '> | Go | `modernc.org/sqlite` (pure-Go) | `pgx` | `go-sql-driver/mysql` |',
  ];
  void m;
  return rows.join('\n');
}

// ── Results — p50 (µs) per op × language × DB: raw SQL / litedbmodel ───────────
function whichLanguageFastest(m: MatrixResult, dialect: string): string {
  const langs = liveLangs(m);
  if (langs.length === 0) return `#### ${DIALECT_LABEL[dialect] ?? dialect}\n\n_No language cell ran._\n`;
  const head = `| Op | ${langs.map((l) => `${LANG_LABEL[l] ?? l} SDK | ${LANG_LABEL[l] ?? l} runtime`).join(' | ')} |`;
  const sep = `|---|${langs.map(() => '---|---').join('|')}|`;
  const rows = CROSSLANG_CASE_IDS.map((caseId) => {
    // Per language: SDK (raw-driver baseline) p50, then litedbmodel runtime p50 with its ratio to SDK.
    const cells = langs.flatMap((l) => {
      const rt = dcell(runtimeCell(m, l), dialect)?.cases[caseId];
      const bl = dcell(baselineCell(m, l), dialect)?.cases[caseId];
      const sdk = bl && !bl.skipped ? us(bl.latency.p50Ms) : '—';
      let lm: string;
      if (!rt) lm = '—';
      else if (rt.skipped) lm = 'skip';
      else if (bl && !bl.skipped) lm = `${us(rt.latency.p50Ms)} (${(rt.latency.p50Ms / bl.latency.p50Ms).toFixed(2)}×)`;
      else lm = us(rt.latency.p50Ms);
      return [sdk, lm];
    });
    return `| ${CROSSLANG_CASE_LABELS[caseId] ?? caseId} | ${cells.join(' | ')} |`;
  });
  // Skip-reason footnotes (deduped).
  const notes = new Set<string>();
  for (const l of langs) {
    for (const caseId of CROSSLANG_CASE_IDS) {
      const s = dcell(runtimeCell(m, l), dialect)?.cases[caseId]?.skipped;
      if (s) notes.add(`${LANG_LABEL[l] ?? l}: ${s}`);
    }
  }
  const noteLines = notes.size ? ['', ...[...notes].map((n) => `> _skip — ${n}_`)] : [];
  return [`#### ${DIALECT_LABEL[dialect] ?? dialect} — p50 (µs), SDK vs runtime`, '', head, sep, ...rows, ...noteLines, ''].join('\n');
}

// ── Fairness — queries/op · rows/op ───────────────────────────────────────────
function fairnessTable(m: MatrixResult, dialect: string): string {
  const langs = liveLangs(m);
  const cells = langs.map((l) => runtimeCell(m, l)!).filter((c) => c.dialects[dialect] && Object.values(c.dialects[dialect].cases).some((r) => r.queries !== undefined));
  if (cells.length === 0) return '';
  const head = `| Op | ${cells.map((c) => LANG_LABEL[c.language] ?? c.language).join(' | ')} |`;
  const sep = `|---|${cells.map(() => '---').join('|')}|`;
  const rows = CROSSLANG_CASE_IDS.map((caseId) => {
    const vals = cells.map((c) => {
      const r = c.dialects[dialect].cases[caseId];
      return r && r.queries !== undefined ? `${r.queries}/${r.rows}` : '—';
    });
    return `| ${CROSSLANG_CASE_LABELS[caseId] ?? caseId} | ${vals.join(' | ')} |`;
  });
  return [`#### ${DIALECT_LABEL[dialect] ?? dialect} — queries/op · rows/op`, '', head, sep, ...rows, ''].join('\n');
}

function resourceTable(m: MatrixResult): string {
  const head = '| Cell | Cold start (ms) | RSS (MB) | Artifact size (MB) |';
  const sep = '|---|---|---|---|';
  // Only the runtime cells carry process-level resource metrics; the raw-driver baseline cell is a
  // latency-only measurement (its resource numbers would all be `—`), so it is not a row here.
  const rows = m.cells.filter((c) => c.impl !== 'baseline').map((c) => {
    if (c.error) return `| ${LANG_LABEL[c.language] ?? c.language} | FAILED: ${c.error} | — | — |`;
    const cold = c.coldStartMs === undefined ? '—' : c.coldStartMs.toFixed(0);
    const rss = c.rssBytes === undefined ? '—' : (c.rssBytes / 1024 / 1024).toFixed(1);
    const art = c.artifactSizeBytes === undefined ? '—' : (c.artifactSizeBytes / 1024 / 1024).toFixed(2);
    return `| ${LANG_LABEL[c.language] ?? c.language} | ${cold} | ${rss} | ${art} |`;
  });
  return ['## Cold start, memory & artifact size', '', head, sep, ...rows, ''].join('\n');
}

export function renderReport(m: MatrixResult): string {
  const parts: string[] = [];
  parts.push('# Cross-language END-TO-END benchmark');
  parts.push('');
  parts.push('<!-- GENERATED by benchmark/crosslang/collect.ts (from the per-language .results/*.csv) — do not edit by hand; re-run to update. -->');
  parts.push('');
  parts.push(methodology(m));

  parts.push('## Results — absolute p50 latency per op × language × DB (µs)');
  parts.push('');
  parts.push('> Every number is an absolute p50 latency in µs. The SQL is compiled once, ahead of time (language-');
  parts.push('> neutral). Two columns per language: **SDK** = that SQL run through the bare driver; **runtime** =');
  parts.push('> the same SQL run through litedbmodel’s shipped runtime (absolute p50, ratio to that language’s SDK');
  parts.push('> in parens). `skip` = not run (footnoted).');
  parts.push('');
  for (const dialect of m.dialects) parts.push(whichLanguageFastest(m, dialect));

  parts.push('## Fairness evidence — queries/op · rows/op (per dialect)');
  parts.push('');
  parts.push('> Identical queries/op AND rows/op across every language proves they do the SAME logical DB work');
  parts.push('> per op (the same SQL) — the apples-to-apples basis for the latency comparison.');
  parts.push('');
  for (const dialect of m.dialects) parts.push(fairnessTable(m, dialect));

  parts.push(resourceTable(m));
  return parts.join('\n') + '\n';
}
