// ════════════════════════════════════════════════════════════════════════════
// Cross-language cell REGISTRY (epic #63) — ONE production cell per language.
// ════════════════════════════════════════════════════════════════════════════
//
// The unified bench measures each language's THIN GENERIC RUNTIME executing the 19
// ORM ops (from the shared orm-plan.json artifact) DB-backed on all 3 real dialects.
// There is ONE production cell per language — no impl axis, no codegen-module cells,
// no micro/mock. Each cell spawns its language's 19-op plan-executor subprocess, which
// speaks the contract (contract.ts) over stdin/stdout.
//
// A cell says how to SPAWN its adapter subprocess. Compiled cells (Rust/Go) are built
// first; if the build/spawn fails the harness renders an honest failure row (never a
// silent drop), so a not-yet-built cell is visible, never faked.

export interface CellSpec {
  language: string; // 'ts' | 'python' | 'php' | 'rust' | 'go'
  impl: string; // 'runtime' (the shipped thin-runtime production path) | 'baseline' (hand-SQL, sqlite)
  spawn?: { command: string; args: string[]; cwd?: string; env?: Record<string, string> };
  note?: string;
}

// The 19-op plan executor entry per language. Each reads the committed orm-plan.json and speaks the
// NDJSON contract (default; `--smoke` runs the standalone 57-cell matrix). Overridable via env.
const TS_RUNNER = process.env.TS_ORM_RUNNER ?? 'benchmark/crosslang/adapters/ts/orm-runner.ts';
const PY = process.env.PYTHON_BIN ?? 'python3';
const PY_RUNNER = process.env.PY_ORM_RUNNER ?? 'benchmark/crosslang/adapters/python/orm_exec.py';
const PHP = process.env.PHP_BIN ?? 'php';
const PHP_RUNNER = process.env.PHP_ORM_RUNNER ?? 'benchmark/crosslang/adapters/php/orm_exec.php';
const RUST_BIN = process.env.RUST_ORM_BIN ?? 'benchmark/crosslang/adapters/rust/target/release/lm_orm';
const GO_BIN = process.env.GO_ORM_BIN ?? 'go/lm_bench/lm_orm/lm_orm';

function ts(): CellSpec {
  return { language: 'ts', impl: 'runtime', spawn: { command: 'npx', args: ['tsx', TS_RUNNER] } };
}
function py(): CellSpec {
  return { language: 'python', impl: 'runtime', spawn: { command: PY, args: [PY_RUNNER, '--orm-plan'] } };
}
function php(): CellSpec {
  return { language: 'php', impl: 'runtime', spawn: { command: PHP, args: [PHP_RUNNER, '--orm-plan'] } };
}
function rust(): CellSpec {
  return { language: 'rust', impl: 'runtime', spawn: { command: RUST_BIN, args: ['--orm-plan'] } };
}
function go(): CellSpec {
  return { language: 'go', impl: 'runtime', spawn: { command: GO_BIN, args: ['--orm-plan'] } };
}

// ONE production cell per language: the shipped thin runtime executing the 19 ORM ops on
// all 3 real DBs, driver-included. (The retired #44 codegen-module cells + impl axis are gone.)
export const MATRIX: CellSpec[] = [ts(), py(), php(), rust(), go()];

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
