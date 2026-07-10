/**
 * #40 — TS proof that independent sibling-relation read nodes are dispatched CONCURRENTLY.
 *
 * The conformance bar runs the SYNC in-proc better-sqlite3 path (`executeReadGraph` → `runBehavior`,
 * serial). #40 adds the PG/MySQL execution model: `executeReadGraphAsync` → bc `runBehaviorAsync`,
 * whose `runPlanAsync` stage exec dispatches the INDEPENDENT sibling nodes of a plan stage in
 * bounded parallel (bc#23), bounded by `plan.concurrency` (default 16). Against a pooled async
 * driver (`pg`/`mysql2`) each `exec` resolves on its own pooled connection → REAL parallel DB I/O.
 *
 * Proven here with a LATENCY-INJECTING, in-flight-counting async executor over a hand-built read
 * graph of N independent sibling `__makeSqlNode`s in ONE plan stage: N queries each sleeping `D`
 * overlap (wall ≈ D, not N·D), the peak simultaneous in-flight count reaches N, and the Φ-merged
 * result is byte-identical to the serial `executeReadGraph` output (determinism preserved).
 */

import { describe, it, expect } from 'vitest';
import { executeReadGraph, executeReadGraphAsync, type ReadGraph } from '../../src/scp/makesql';

const SCOPE_PORT = '__scope';
const NODE_COMPONENT = '__makeSqlNode';

/** A read graph of N independent sibling nodes in ONE plan stage, each a trivial `SELECT <i>`. */
function siblingGraph(n: number): ReadGraph {
  const statementsById: Record<string, { sql: string; params: unknown[] }[]> = {};
  const body = [];
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
    // One stage carrying every sibling → the bounded-parallel dispatch shape (default concurrency).
    plan: { concurrency: 16, groups: [Array.from({ length: n }, (_, i) => i)] },
  };
  return {
    dialect: 'sqlite',
    name: 'Siblings',
    ir: { irVersion: 1, exprVersion: 2, components: [component] } as unknown as ReadGraph['ir'],
    statementsById: statementsById as unknown as ReadGraph['statementsById'],
    optionalHeads: [],
  };
}

describe('#40 async read-graph dispatches sibling relations concurrently', () => {
  it('N sibling queries overlap (wall ≈ latency, peak in-flight = N), result deterministic', async () => {
    const N = 8;
    const LATENCY_MS = 60;
    let inFlight = 0;
    let peak = 0;
    let calls = 0;

    const latencyExec = async (sql: string): Promise<Record<string, unknown>[]> => {
      calls++;
      inFlight++;
      peak = Math.max(peak, inFlight);
      await new Promise((r) => setTimeout(r, LATENCY_MS));
      inFlight--;
      return [{ sql }];
    };

    const graph = siblingGraph(N);
    const t0 = Date.now();
    const result = await executeReadGraphAsync(graph, {}, latencyExec);
    const elapsed = Date.now() - t0;

    // 1. Overlap: N=8 × 60ms serial = 480ms; concurrent ≈ 60ms. Well under half proves overlap.
    expect(elapsed).toBeLessThan((LATENCY_MS * N) / 2);
    // 2. All N ran and all N were simultaneously in flight.
    expect(calls).toBe(N);
    expect(peak).toBe(N);

    // 3. Determinism: the Φ-merged result equals the SERIAL sync path byte-for-byte.
    const serial = executeReadGraph(graph, {}, {
      prepare: (sql: string) => ({
        all: () => [{ sql }],
        run: () => ({ changes: 0, lastInsertRowid: 0 }),
      }),
    });
    expect(result).toEqual(serial);

    // eslint-disable-next-line no-console
    console.log(
      `TS PARALLEL PROOF: ${N} sibling queries @ ${LATENCY_MS}ms each → wall ${elapsed}ms ` +
        `(serial would be ${LATENCY_MS * N}ms), peak in-flight = ${peak}`,
    );
  });
});
