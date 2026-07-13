// ════════════════════════════════════════════════════════════════════════════
// Cross-language cell REGISTRY (epic #44) — the (language × impl) matrix.
// ════════════════════════════════════════════════════════════════════════════
//
//   TS   : sql / codegen / ir / dynamic / prepared        (5 cells)
//   Python/Rust/PHP/Go : sql / codegen / ir               (3 each = 12 cells)
//   → 17 cells, + v1 comparison rows (v1-ts; v1-rs if the old .rs builds).
//
// A cell says how to SPAWN its adapter subprocess. Compiled cells (Rust/Go) are
// built first; if the build fails the cell carries a `buildError` and the harness
// renders an honest failure row (never silently drops it).

export interface CellSpec {
  language: string; // 'ts' | 'python' | 'php' | 'rust' | 'go' | 'v1-ts' | 'v1-rs'
  impl: string; // 'sql' | 'codegen' | 'ir' | 'dynamic' | 'prepared' | 'v1'
  spawn?: { command: string; args: string[]; cwd?: string; env?: Record<string, string> };
  // A note when a cell can't be built/run (rendered in the report).
  note?: string;
}

const TS_RUNNER = 'benchmark/crosslang/adapters/ts/runner.ts';
const PY = process.env.PYTHON_BIN ?? 'python3';
const PY_RUNNER = 'benchmark/crosslang/adapters/python/runner.py';
const PHP = process.env.PHP_BIN ?? 'php';
const PHP_RUNNER = 'benchmark/crosslang/adapters/php/runner.php';
const RUST_BIN = process.env.RUST_BENCH_BIN ?? 'benchmark/crosslang/adapters/rust/target/release/lm_bench';
// The codegen cell is a SEPARATE binary (owner order): its crate carries NO serde_json / no
// litedbmodel_runtime — generated modules + the generated native companion only. It never
// parses JSON or touches IR data at execution time.
const RUST_CODEGEN_BIN = process.env.RUST_CODEGEN_BENCH_BIN ?? 'benchmark/crosslang/adapters/rust-codegen/target/release/lm_codegen';
const GO_BIN = process.env.GO_BENCH_BIN ?? 'benchmark/crosslang/adapters/go/go_bench';
const V1RS_BIN = process.env.V1RS_BENCH_BIN ?? 'benchmark/crosslang/adapters/v1rs/target/release/v1rs_bench';

function ts(impl: string): CellSpec {
  return { language: 'ts', impl, spawn: { command: 'npx', args: ['tsx', TS_RUNNER, `--impl=${impl}`] } };
}
function py(impl: string): CellSpec {
  return { language: 'python', impl, spawn: { command: PY, args: [PY_RUNNER, `--impl=${impl}`] } };
}
function php(impl: string): CellSpec {
  return { language: 'php', impl, spawn: { command: PHP, args: [PHP_RUNNER, `--impl=${impl}`] } };
}
function rust(impl: string): CellSpec {
  // codegen rides the dedicated JSON-free binary; sql/ir ride the shared lm_bench adapter.
  const command = impl === 'codegen' ? RUST_CODEGEN_BIN : RUST_BIN;
  return { language: 'rust', impl, spawn: { command, args: [`--impl=${impl}`] } };
}
function go(impl: string): CellSpec {
  return { language: 'go', impl, spawn: { command: GO_BIN, args: [`--impl=${impl}`] } };
}

export const MATRIX: CellSpec[] = [
  // TS — the 5-mode reference.
  ts('sql'), ts('codegen'), ts('ir'), ts('dynamic'), ts('prepared'),
  // Python / PHP / Rust / Go — sql / codegen / ir.
  py('sql'), py('codegen'), py('ir'),
  php('sql'), php('codegen'), php('ir'),
  rust('sql'), rust('codegen'), rust('ir'),
  go('sql'), go('codegen'), go('ir'),
  // v1 regression baselines.
  { language: 'v1-ts', impl: 'v1', spawn: { command: 'npx', args: ['tsx', TS_RUNNER, '--impl=v1'] } },
  { language: 'v1-rs', impl: 'ir', spawn: { command: V1RS_BIN, args: ['--impl=ir'] }, note: 'old litedbmodel.rs (async+deadpool) — built separately' },
];

export function liveCells(): CellSpec[] {
  let cells = MATRIX.filter((c) => c.spawn);
  // CROSSLANG_ONLY=ts,python — restrict to a subset of languages (comma list).
  const only = process.env.CROSSLANG_ONLY;
  if (only) {
    const set = new Set(only.split(',').map((s) => s.trim()).filter(Boolean));
    cells = cells.filter((c) => set.has(c.language));
  }
  return cells;
}
