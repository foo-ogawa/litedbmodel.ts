// COLLECTOR — aggregate the three cells' raw per-iteration CSVs (.results/<cell>.csv, columns `op,us`)
// into the payoff table: op × cell → p50 / p99 (µs) + ops/sec. Measurement (the cells) and aggregation
// (here) stay separate; the percentile is computed identically for every cell (nearest-rank, the same
// method as benchmark/crosslang/metrics.ts). Verbatim numbers — no massaging, no rigging.
import { readFileSync, writeFileSync, readdirSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const RESULTS = join(HERE, '.results');

// Nearest-rank percentile (== metrics.ts percentile). samples need not be sorted.
function percentile(samples, p) {
  if (samples.length === 0) return NaN;
  const s = [...samples].sort((a, b) => a - b);
  const rank = Math.ceil((p / 100) * s.length);
  return s[Math.min(s.length - 1, Math.max(0, rank - 1))];
}
const mean = (a) => (a.length ? a.reduce((s, v) => s + v, 0) / a.length : NaN);

const CELL_ORDER = ['ts_ir', 'rust', 'go'];
const CELL_LABEL = { ts_ir: 'ts-IR (interp)', rust: 'rust-native', go: 'go-native' };
const OP_ORDER = ['findunique', 'relsingle', 'createuser', 'createmany'];
const OP_LABEL = {
  findunique: 'findUnique (point read)',
  relsingle: 'relComments (batched relation)',
  createuser: 'createUser (single write)',
  createmany: 'createMany (batch write ×10)',
};

// cell → op → samples[]
const data = {};
for (const f of readdirSync(RESULTS).filter((f) => f.endsWith('.csv'))) {
  const cell = f.replace(/\.csv$/, '');
  const lines = readFileSync(join(RESULTS, f), 'utf8').trim().split('\n');
  data[cell] = {};
  for (const line of lines.slice(1)) {
    const [op, us] = line.split(',');
    if (us === undefined) continue;
    (data[cell][op] ??= []).push(Number(us));
  }
}

const cells = CELL_ORDER.filter((c) => data[c]);
const rows = [];
for (const op of OP_ORDER) {
  for (const cell of cells) {
    const s = data[cell]?.[op] ?? [];
    if (s.length === 0) continue;
    const p50 = percentile(s, 50);
    const p99 = percentile(s, 99);
    const m = mean(s);
    rows.push({ op, cell, n: s.length, p50, p99, mean: m, opsSec: 1e6 / m });
  }
}
writeFileSync(join(RESULTS, 'summary.csv'), 'op,cell,n,p50_us,p99_us,mean_us,ops_per_sec\n' +
  rows.map((r) => `${r.op},${r.cell},${r.n},${r.p50.toFixed(3)},${r.p99.toFixed(3)},${r.mean.toFixed(3)},${r.opsSec.toFixed(0)}`).join('\n') + '\n');

// ── render the markdown table (op × cell) + the native-vs-interpreter speedup ──
const pad = (s, w) => String(s).padEnd(w);
const num = (x, w) => String(typeof x === 'number' ? x.toFixed(2) : x).padStart(w);
let out = '';
out += `# Latency: ts-IR-interpreter vs rust-native vs go-native (sqlite in-proc)\n\n`;
out += `Same 4 ops, same seed sqlite (C engine in every cell: better-sqlite3 / rusqlite-bundled /\n`;
out += `mattn-go-sqlite3), same iteration count. Whole hot path timed (bind + exec + decode into the\n`;
out += `typed result). p50/p99 in µs; ops/sec = 1e6 / mean latency (single-thread serial). Verbatim.\n\n`;
out += `| op | cell | p50 µs | p99 µs | ops/sec | vs ts-IR (p50) |\n`;
out += `|---|---|--:|--:|--:|--:|\n`;
for (const op of OP_ORDER) {
  const opRows = rows.filter((r) => r.op === op);
  const base = opRows.find((r) => r.cell === 'ts_ir');
  for (const r of opRows) {
    let speedup = '';
    if (r.cell === 'ts_ir') speedup = '— (baseline)';
    else if (base) {
      const ratio = base.p50 / r.p50; // >1 ⇒ native faster than the interpreter
      speedup = ratio >= 1 ? `${ratio.toFixed(2)}× faster` : `${(1 / ratio).toFixed(2)}× slower`;
    }
    out += `| ${op === opRows[0].op && r === opRows[0] ? OP_LABEL[op] : ''} | ${CELL_LABEL[r.cell]} | ${r.p50.toFixed(2)} | ${r.p99.toFixed(2)} | ${r.opsSec.toFixed(0)} | ${speedup} |\n`;
  }
}
out += `\nRaw per-iteration samples: \`.results/<cell>.csv\`; per-op summary: \`.results/summary.csv\`.\n`;

console.log(out);
writeFileSync(join(HERE, 'LATENCY.md'), out);
