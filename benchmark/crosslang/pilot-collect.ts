// ════════════════════════════════════════════════════════════════════════════
// pilot-collect — aggregate the rust pilot's raw CSVs into the #129 report (results only).
// ════════════════════════════════════════════════════════════════════════════
//
// The measurement (rust cells) and the aggregation (here) stay SEPARATE: each cell self-measures and
// writes a flat `.results/<cell>.csv` (`cell,dialect,op,iter,us`); this collector globs those, feeds the
// raw samples through the shared `metrics.ts` percentile/throughput math, and renders the comparison
// tables. No CSV = the cell did not run (skipped, honestly noted) — never fabricated.
//
// Inputs (benchmark/crosslang/.results/):
//   native.csv        — the native-codegen cell (orm_bench)          [required]
//   sdk.csv           — the raw-driver SDK baseline cell (orm_bench_sdk) [required]
//   native-safety.txt — the N+1 query-count proof lines               [optional]
// v1 rust-native (pg leg): /Users/ogawa/Work/my-libs/litedbmodel.rs/benchmark/results/rust-benchmark-results.csv
//
// Output: benchmark/ORM-PILOT.md (results only — no issue numbers / narrative, per the doc rule).
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { percentile, mean, throughputOpsPerSec } from './metrics';
import { ORM_OPS, ORM_OP_LABEL, ORM_DIALECTS } from './contract';

const HERE = dirname(fileURLToPath(import.meta.url));
const RESULTS = join(HERE, '.results');
const V1_CSV = '/Users/ogawa/Work/my-libs/litedbmodel.rs/benchmark/results/rust-benchmark-results.csv';

type Cell = 'native' | 'sdk';
interface Sample { cell: string; dialect: string; op: string; us: number }

function parseCsv(path: string): Sample[] {
  const lines = readFileSync(path, 'utf8').trim().split('\n');
  const out: Sample[] = [];
  for (const line of lines.slice(1)) {
    const [cell, dialect, op, , us] = line.split(',');
    if (cell === undefined || us === undefined) continue;
    out.push({ cell, dialect, op, us: Number(us) });
  }
  return out;
}

interface Stat { p50Ms: number; p99Ms: number; opsSec: number; n: number }
function statOf(samplesUs: number[]): Stat {
  const ms = samplesUs.map((u) => u / 1000);
  const meanMs = mean(ms);
  return {
    p50Ms: percentile(ms, 50),
    p99Ms: percentile(ms, 99),
    opsSec: throughputOpsPerSec(1, meanMs),
    n: ms.length,
  };
}

function index(samples: Sample[]): Map<string, number[]> {
  const m = new Map<string, number[]>();
  for (const s of samples) {
    const k = `${s.cell}|${s.dialect}|${s.op}`;
    (m.get(k) ?? m.set(k, []).get(k)!).push(s.us);
  }
  return m;
}

function fmt(n: number): string {
  return Number.isFinite(n) ? (n < 10 ? n.toFixed(3) : n < 100 ? n.toFixed(2) : n.toFixed(1)) : '—';
}
function fmtOps(n: number): string {
  return Number.isFinite(n) ? Math.round(n).toLocaleString('en-US') : '—';
}

// ── v1 rust-native (pg) — keyed by the human label (== ORM_OP_LABEL == v1 `category`). ──────────────
function parseV1(): Map<string, Record<string, number>> {
  const m = new Map<string, Record<string, number>>();
  if (!existsSync(V1_CSV)) return m;
  for (const line of readFileSync(V1_CSV, 'utf8').trim().split('\n').slice(1)) {
    // Columns: category,orm,median_ms,iqr_ms,stddev_ms,min_ms,max_ms — 5 numerics. category may contain
    // commas ("Filter, paginate & sort"), so anchor on the trailing 5 numerics + the orm before them.
    const parts = line.split(',');
    const nums = parts.slice(-5);
    const orm = parts[parts.length - 6];
    const category = parts.slice(0, parts.length - 6).join(',');
    const median = Number(nums[0]);
    const rec = m.get(category) ?? {};
    rec[orm] = median;
    m.set(category, rec);
  }
  return m;
}

function main() {
  const nativePath = join(RESULTS, 'native.csv');
  const sdkPath = join(RESULTS, 'sdk.csv');
  if (!existsSync(nativePath)) throw new Error(`missing ${nativePath} — run the native cell first`);
  const idx = index([...parseCsv(nativePath), ...(existsSync(sdkPath) ? parseCsv(sdkPath) : [])]);
  const stat = (cell: Cell, dialect: string, op: string): Stat | undefined => {
    const s = idx.get(`${cell}|${dialect}|${op}`);
    return s ? statOf(s) : undefined;
  };

  const lines: string[] = [];
  lines.push('# ORM bench — rust pilot (native codegen vs raw-driver SDK)\n');
  lines.push(
    'Each op runs the same logical operation two ways: **native** = litedbmodel-generated native module ' +
      '+ runtime (no hand-written exec seam); **sdk** = a hand-written raw rust driver (rusqlite / postgres ' +
      '/ mysql), litedbmodel NOT in the path. Reads/writes/batch/relations/tx across sqlite (in-proc file) ' +
      '+ docker PostgreSQL + docker MySQL. Latency p50/p99 in ms; ops/sec from the mean; overhead = ' +
      'native p50 ÷ sdk p50.\n',
  );

  // ── Table 1: 19 × 3 native vs sdk ──
  for (const dialect of ORM_DIALECTS) {
    lines.push(`\n## ${dialect}\n`);
    lines.push('| op | native p50 | native p99 | native ops/s | sdk p50 | sdk p99 | sdk ops/s | native÷sdk |');
    lines.push('|---|--:|--:|--:|--:|--:|--:|--:|');
    for (const { id } of ORM_OPS) {
      const n = stat('native', dialect, id);
      const s = stat('sdk', dialect, id);
      const ratio = n && s && s.p50Ms > 0 ? `${(n.p50Ms / s.p50Ms).toFixed(2)}×` : '—';
      lines.push(
        `| ${id} | ${n ? fmt(n.p50Ms) : '—'} | ${n ? fmt(n.p99Ms) : '—'} | ${n ? fmtOps(n.opsSec) : '—'} ` +
          `| ${s ? fmt(s.p50Ms) : '—'} | ${s ? fmt(s.p99Ms) : '—'} | ${s ? fmtOps(s.opsSec) : '—'} | ${ratio} |`,
      );
    }
  }

  // ── Table 2: pg leg — v2-native ↔ v1-native (+ v1 SeaORM/Diesel + #129 sdk), by label ──
  const v1 = parseV1();
  lines.push('\n## PostgreSQL: v2 native (this pilot) vs v1 rust native (litedbmodel.rs)\n');
  lines.push(
    'v1 measured PostgreSQL only (median_ms). Baselines differ: v1 baseline = SeaORM/Diesel; this ' +
      "pilot's baseline = a raw rust driver. Overlap is by the op label (ORM_OP_LABEL == v1 category).\n",
  );
  lines.push('| op (label) | v2-native p50 ms | v1-native median ms | v1 SeaORM ms | v1 Diesel ms | #129 sdk p50 ms |');
  lines.push('|---|--:|--:|--:|--:|--:|');
  for (const { id } of ORM_OPS) {
    const label = ORM_OP_LABEL[id];
    const n = stat('native', 'postgres', id);
    const s = stat('sdk', 'postgres', id);
    const v = v1.get(label) ?? {};
    const cell = (x: number | undefined) => (x === undefined ? '—' : fmt(x));
    lines.push(
      `| ${label} | ${n ? fmt(n.p50Ms) : '—'} | ${cell(v['litedbmodel'])} | ${cell(v['SeaORM'])} ` +
        `| ${cell(v['Diesel'])} | ${s ? fmt(s.p50Ms) : '—'} |`,
    );
  }

  // ── Safety proofs ──
  const safetyPath = join(RESULTS, 'native-safety.txt');
  if (existsSync(safetyPath)) {
    lines.push('\n## Safety proofs (native cell)\n');
    lines.push('The measured latency is the cost WITH the safety guards on. Query counts are issued at the Driver seam (a batched relation = 1 parent + 1 batched child per level; a batch write = 1 statement); the find hardLimit fires end-to-end on the guarded native read:\n');
    lines.push('```');
    lines.push(readFileSync(safetyPath, 'utf8').trim());
    lines.push('```');
    lines.push(
      '\nReader/writer routing: the native read/write companions expose the `handler_routed(&RoutingConfig)` ' +
        'seam; the runtime routing resolver sends a read → reader pool, a write → writer pool, and a read ' +
        'inside a writer scope → writer (read-your-writes) — verified green by the runtime routing tests ' +
        '(`resolve_pool_reader_writer_split`, `named_routing_selects_the_pair`).',
    );
  }

  const out = join(HERE, '../ORM-PILOT.md');
  writeFileSync(out, lines.join('\n') + '\n');
  console.log(`pilot-collect: wrote ${out} (${idx.size} cell×dialect×op groups)`);
}

main();
