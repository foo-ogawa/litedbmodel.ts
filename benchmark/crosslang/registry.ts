// ════════════════════════════════════════════════════════════════════════════
// Cross-language cell REGISTRY (epic #44) — the (language × impl) matrix.
// ════════════════════════════════════════════════════════════════════════════
//
//   TS      : sql / codegen / ir / dynamic / prepared     (5 cells)
//   Python/PHP : sql / ir     (NO codegen — Python/PHP are     (2 each = 4 cells)
//             NOT codegen-MODULE languages: generate.ts's CODEGEN_LANGS is
//             {ts,go,rust}, so a py/php "codegen" cell was only ir + a
//             one-time fingerprint check — not a distinct exec surface. Dropped
//             per the owner-corrected #44 matrix.)
//   Rust/Go : sql / codegen  (NATIVE-ONLY — no ir cell,    (2 each = 4 cells)
//             the IR interpreter is deleted from their runtimes, #8)
//   → 13 cells, + v1 comparison rows (v1-ts; v1-rs if the old .rs builds).
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
// The go codegen cell is a SEPARATE binary (owner order, mirroring rust-codegen): its build links
// NEITHER litedbmodel_runtime NOR (directly) encoding/json — the generated typed-native modules + the
// generated native companion (cgplans, incl. SCHEMA/SEED) + database/sql only. It never parses the
// JSON artifact or touches IR data at execution time. (bc-go transitively imports encoding/json for
// its parser; that is unavoidable for any bc consumer and is not litedbmodel's codegen path.)
const GO_CODEGEN_BIN = process.env.GO_CODEGEN_BENCH_BIN ?? 'benchmark/crosslang/adapters/go-codegen/lm_codegen';
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
  // codegen rides the dedicated JSON-free/rt-free binary (lm_codegen, no --impl arg — it IS codegen);
  // sql rides the shared go_bench adapter.
  if (impl === 'codegen') {
    return { language: 'go', impl, spawn: { command: GO_CODEGEN_BIN, args: [] } };
  }
  return { language: 'go', impl, spawn: { command: GO_BIN, args: [`--impl=${impl}`] } };
}

export const MATRIX: CellSpec[] = [
  // TS — the 5-mode reference.
  ts('sql'), ts('codegen'), ts('ir'), ts('dynamic'), ts('prepared'),
  // Python / PHP — sql / ir ONLY (the ir/interpret exec surface is their by-design mode).
  // No codegen cell: Python/PHP are NOT codegen-MODULE languages (generate.ts's CODEGEN_LANGS
  // is {ts,go,rust}), so a py/php "codegen" cell was only `ir` + a one-time fingerprint check —
  // not a distinct exec surface. Dropped per the owner-corrected #44 matrix.
  py('sql'), py('ir'),
  php('sql'), php('ir'),
  // Rust / Go — NATIVE-ONLY: { sql, codegen } (NO `ir` cell). The IR interpreter is DELETED from
  // the rust/go runtimes (epic #44 native-only, #8) — every read/write runs generated native code
  // (static SQL text + typed param binding), so there is no interpreter path to bench.
  rust('sql'), rust('codegen'),
  go('sql'), go('codegen'),
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
