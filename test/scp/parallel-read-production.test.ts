/**
 * #40 (gap fix) — the PRODUCTION PG/MySQL read path fans out independent sibling relations.
 *
 * `test/scp/parallel-read-async.test.ts` proves the STANDALONE primitive `executeReadGraphAsync`.
 * This test drives the PRODUCTION entry point `executeBundleAsync` (runtime.ts — the async twin of
 * `executeBundle` the live PG/MySQL path uses) through a multi-sibling read GRAPH so the fan-out is
 * proven where it actually ships, not only in the test-only primitive:
 *
 *   1. FAN-OUT: a read bundle whose read graph has N independent sibling nodes in ONE plan stage
 *      (concurrency 16) → peak simultaneous in-flight = N (capped at 16), wall ≈ ONE op's latency.
 *   2. SERIAL PERTURBATION (negative check): the SAME bundle with plan concurrency forced to 1 →
 *      peak in-flight = 1 and wall ≈ N × latency (proves the parallelism is real, not incidental).
 *   3. DETERMINISM: under a SHUFFLED-completion mock where later-dispatched siblings finish FIRST,
 *      the Φ-merged result is BYTE-IDENTICAL to the serial `executeBundle` output.
 *   4. SINGLE-SIBLING IDENTITY: a one-relation read graph is byte-identical serial vs async.
 */

import { describe, it, expect } from 'vitest';
import {
  executeBundle,
  executeBundleAsync,
  type SqlBundle,
  type SqliteDb,
  type SqlExecutorAsync,
} from '../../src/scp';

const SCOPE_PORT = '__scope';
const NODE_COMPONENT = '__makeSqlNode';

/** A READ SqlBundle whose read graph has n independent sibling nodes in ONE plan stage. */
function siblingBundle(n: number, concurrency = 16): SqlBundle {
  const statementsById: Record<string, { sql: string; params: unknown[] }[]> = {};
  const body: unknown[] = [];
  const outputObj: Record<string, unknown> = {};
  for (let i = 0; i < n; i++) {
    const id = `rel${i}`;
    statementsById[id] = [{ sql: `SELECT ${i}`, params: [] }];
    body.push({ id, component: NODE_COMPONENT, ports: { [SCOPE_PORT]: { obj: {} } } });
    outputObj[id] = { ref: [id] };
  }
  const component = {
    name: 'Siblings',
    inputPorts: {},
    body,
    output: { obj: outputObj },
    // One stage carrying every sibling → the bounded-parallel dispatch shape.
    plan: { concurrency, groups: [Array.from({ length: n }, (_, i) => i)] },
  };
  return {
    dialect: 'postgres',
    name: 'Siblings',
    readGraph: {
      dialect: 'postgres',
      name: 'Siblings',
      ir: { irVersion: 1, exprVersion: 2, components: [component] },
      statementsById,
      optionalHeads: [],
    },
    optionalHeads: [],
    relations: {},
  } as unknown as SqlBundle;
}

/** A synchronous SQLite driver stub echoing the SQL — the serial-path oracle. */
const echoDb: SqliteDb = {
  prepare: (sql: string) => ({
    all: () => [{ sql }],
    run: () => ({ changes: 0, lastInsertRowid: 0 }),
  }),
};

describe('#40 production async read path (executeBundleAsync) fans out sibling relations', () => {
  it('N sibling queries overlap (wall ≈ latency, peak in-flight = N), result = serial', async () => {
    const N = 8;
    const LATENCY_MS = 60;
    let inFlight = 0;
    let peak = 0;
    let calls = 0;

    const latencyExec: SqlExecutorAsync = async (sql) => {
      calls++;
      inFlight++;
      peak = Math.max(peak, inFlight);
      await new Promise((r) => setTimeout(r, LATENCY_MS));
      inFlight--;
      return [{ sql }];
    };

    const bundle = siblingBundle(N);
    const t0 = Date.now();
    const result = await executeBundleAsync(bundle, {}, { exec: latencyExec });
    const elapsed = Date.now() - t0;

    // 1. Overlap: N=8 × 60ms serial = 480ms; concurrent ≈ 60ms. Well under half proves overlap.
    expect(elapsed).toBeLessThan((LATENCY_MS * N) / 2);
    expect(calls).toBe(N);
    expect(peak).toBe(N); // = sibling count, under the cap 16

    // 3. Determinism (basic): the Φ-merged result equals the SERIAL sync path byte-for-byte.
    const serial = executeBundle(bundle, {}, { db: echoDb });
    expect(result).toEqual(serial);

    // eslint-disable-next-line no-console
    console.log(
      `TS PRODUCTION PARALLEL PROOF: ${N} sibling queries @ ${LATENCY_MS}ms via executeBundleAsync → ` +
        `wall ${elapsed}ms (serial would be ${LATENCY_MS * N}ms), peak in-flight = ${peak}`,
    );
  });

  it('SERIAL PERTURBATION: plan concurrency=1 → peak in-flight = 1, wall ≈ N × latency', async () => {
    const N = 4;
    const LATENCY_MS = 30;
    let inFlight = 0;
    let peak = 0;

    const latencyExec: SqlExecutorAsync = async (sql) => {
      inFlight++;
      peak = Math.max(peak, inFlight);
      await new Promise((r) => setTimeout(r, LATENCY_MS));
      inFlight--;
      return [{ sql }];
    };

    const bundle = siblingBundle(N, 1); // concurrency forced to 1
    const t0 = Date.now();
    await executeBundleAsync(bundle, {}, { exec: latencyExec });
    const elapsed = Date.now() - t0;

    expect(peak).toBe(1); // serial: never more than one in flight
    // Wall must be ≈ N × latency (serial), well over the concurrent bound.
    expect(elapsed).toBeGreaterThan(LATENCY_MS * (N - 1));
  });

  it('DETERMINISM: later-dispatched siblings finish FIRST → result still byte-identical to serial', async () => {
    const N = 6;
    // Shuffled completion: rel0 sleeps longest, rel{N-1} shortest → reverse finish order.
    const shuffledExec: SqlExecutorAsync = async (sql) => {
      const i = Number(/SELECT (\d+)/.exec(sql)?.[1] ?? 0);
      const delay = (N - i) * 10; // rel0 → 60ms, rel5 → 10ms: later siblings finish first
      await new Promise((r) => setTimeout(r, delay));
      return [{ sql }];
    };

    const bundle = siblingBundle(N);
    const result = await executeBundleAsync(bundle, {}, { exec: shuffledExec });
    const serial = executeBundle(bundle, {}, { db: echoDb });

    // Byte-identical to the serial path despite reverse completion order.
    expect(JSON.stringify(result)).toBe(JSON.stringify(serial));
  });

  it('SINGLE-SIBLING IDENTITY: a one-relation read graph is identical serial vs async', async () => {
    const bundle = siblingBundle(1);
    const exec: SqlExecutorAsync = async (sql) => [{ sql }];
    const result = await executeBundleAsync(bundle, {}, { exec });
    const serial = executeBundle(bundle, {}, { db: echoDb });
    expect(JSON.stringify(result)).toBe(JSON.stringify(serial));
  });
});
