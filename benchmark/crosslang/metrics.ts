// ════════════════════════════════════════════════════════════════════════════
// Cross-language metric computation (epic #44) — PURE functions, no I/O.
// ════════════════════════════════════════════════════════════════════════════
//
// All percentile / throughput / aggregation math lives here so it is computed
// IDENTICALLY for every language and every impl. Language subprocesses return
// RAW samples over the contract; the harness feeds them through these functions.

// Nearest-rank percentile. `samples` need not be pre-sorted. NaN for empty.
export function percentile(samples: readonly number[], p: number): number {
  if (samples.length === 0) return NaN;
  if (p <= 0) return Math.min(...samples);
  const sorted = [...samples].sort((a, b) => a - b);
  const rank = Math.ceil((p / 100) * sorted.length);
  const index = Math.min(sorted.length - 1, Math.max(0, rank - 1));
  return sorted[index];
}

export function mean(samples: readonly number[]): number {
  if (samples.length === 0) return NaN;
  return samples.reduce((s, v) => s + v, 0) / samples.length;
}
export function min(samples: readonly number[]): number {
  if (samples.length === 0) return NaN;
  return samples.reduce((m, v) => (v < m ? v : m), Infinity);
}
export function max(samples: readonly number[]): number {
  if (samples.length === 0) return NaN;
  return samples.reduce((m, v) => (v > m ? v : m), -Infinity);
}

export interface LatencyStats {
  count: number;
  meanMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  minMs: number;
  maxMs: number;
}

export function summarizeLatency(samplesMs: readonly number[]): LatencyStats {
  return {
    count: samplesMs.length,
    meanMs: mean(samplesMs),
    p50Ms: percentile(samplesMs, 50),
    p95Ms: percentile(samplesMs, 95),
    p99Ms: percentile(samplesMs, 99),
    minMs: min(samplesMs),
    maxMs: max(samplesMs),
  };
}

export function throughputOpsPerSec(completed: number, elapsedMs: number): number {
  if (elapsedMs <= 0) return NaN;
  return completed / (elapsedMs / 1000);
}

export function coldStartMs(spawnedAtEpochMs: number, readyAtEpochMs: number): number {
  return Math.max(0, readyAtEpochMs - spawnedAtEpochMs);
}

// Relative overhead of an impl vs its same-language `sql` baseline, a MULTIPLE of
// baseline latency (impl ÷ sql). This is the MAIN signal of #44: the abstraction
// cost of each exec surface over hand-written raw SQL. >1 = slower than baseline.
export function relativeOverhead(implMs: number, baselineMs: number): number {
  if (!(baselineMs > 0)) return NaN;
  return implMs / baselineMs;
}

// ── Aggregated result shapes (what the harness assembles + report renders) ────
export interface CaseResult {
  case: string;
  latency: LatencyStats;
  throughputOpsPerSec?: number;
  // Fairness evidence (from the `cost` probe): queries + rows per op.
  queries?: number;
  rows?: number;
  // An HONEST per-cell "did not run" reason (e.g. no live PG driver, or the sql
  // baseline on a non-sqlite DB-backed cell) — rendered as a note, never dropped.
  skipped?: string;
}

// Per-dialect results for one cell: the DB-backed cases + the micro cases compiled
// for THAT dialect (the render/placeholder/array form differs per dialect).
export interface DialectResult {
  cases: Record<string, CaseResult>;
  micro: Record<string, LatencyStats>;
  microSkipped: Record<string, string>;
}

export interface CellResult {
  language: string;
  impl: string;
  coldStartMs?: number;
  rssBytes?: number;
  // Compiled-artifact size in bytes (Rust/Go binaries). Undefined for interpreted cells.
  artifactSizeBytes?: number;
  // Per-dialect (sqlite/postgres/mysql) case + micro results.
  dialects: Record<string, DialectResult>;
  pending?: boolean;
  // A cell that failed to run (toolchain/build error) carries an error note so the
  // report renders an honest failure row rather than silently dropping the cell.
  error?: string;
}

export interface MatrixResult {
  generatedAt: string;
  iterations: number;
  warmup: number;
  microIterations: number;
  dialects: readonly string[];
  cells: CellResult[];
}
