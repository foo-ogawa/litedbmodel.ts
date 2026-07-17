// Shared timing helpers. Warmup + measured per-iteration latency
// sampling and a bounded-concurrency throughput probe. Sync-op friendly: the
// litedbmodel exec surfaces are synchronous (in-proc better-sqlite3), so `op`
// may return a value or a Promise; both are awaited.

import { performance } from 'node:perf_hooks';

export async function collectSamples(
  op: () => unknown | Promise<unknown>,
  warmup: number,
  iterations: number,
): Promise<number[]> {
  for (let i = 0; i < warmup; i++) await op();
  const samples = new Array<number>(iterations);
  for (let i = 0; i < iterations; i++) {
    const t0 = performance.now();
    await op();
    samples[i] = performance.now() - t0;
  }
  return samples;
}

// Single-worker sequential throughput (the write cases hold create/tx state that
// must stay serial for key monotonicity). Honest ops/s under a tight loop.
export async function runConcurrent(
  op: () => unknown | Promise<unknown>,
  iterations: number,
  _concurrency: number,
): Promise<{ elapsedMs: number; completed: number }> {
  const t0 = performance.now();
  let completed = 0;
  for (let i = 0; i < iterations; i++) {
    await op();
    completed++;
  }
  return { elapsedMs: performance.now() - t0, completed };
}
