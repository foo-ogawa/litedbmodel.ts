/**
 * WS7a (#30) — the TS reference conformance runner (the multi-language baseline).
 *
 * This is the "TS runner green" bar of the conformance harness: it loads the FROZEN vector
 * corpus (`conformance/vectors/*.json`, generated from the TS reference by
 * `conformance/gen-vectors.ts`) and re-executes the live TS SCP reference against every vector,
 * asserting byte-identical SQL + params (render axis), identical executed rows/state (exec + tx
 * axes) and identical dialect-primitive output (orderByNulls). It is the reference leg of the
 * §10 promise — "同一 IR+入力 → 同一 SQL + 同一結果"; WS7b-e language runners mirror this exact
 * corpus against their own runtimes.
 *
 * ## Not faked
 *
 * The corpus is captured from the reference, and this runner RE-DERIVES the reference and
 * asserts equality — a genuine round-trip, not a stubbed pass. The drift gate below also proves
 * the on-disk corpus is exactly what the current reference produces (an additive-refreeze
 * discipline: change the reference → regenerate → the diff is reviewable).
 */

import { describe, it, expect } from 'vitest';
import { readFileSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { CORPUS_VERSION, runVector, type Suite } from '../../conformance/harness';
import { checkCorpus } from '../../conformance/gen-vectors';

const VECTORS_DIR = join(dirname(fileURLToPath(import.meta.url)), '..', '..', 'conformance', 'vectors');

function loadSuites(): Suite[] {
  return readdirSync(VECTORS_DIR)
    .filter((f) => f.endsWith('.json'))
    .sort()
    .map((f) => JSON.parse(readFileSync(join(VECTORS_DIR, f), 'utf8')) as Suite);
}

describe('WS7a conformance — TS reference runner over the frozen vector corpus', () => {
  const suites = loadSuites();

  it('the on-disk corpus is byte-true to the current TS reference (drift gate)', () => {
    // If this fails: the reference changed. Regenerate with
    //   npx vitest run --config conformance/vitest.config.ts
    // review the diff, and re-commit (additive-refreeze).
    expect(checkCorpus()).toEqual([]);
  });

  it('every suite declares the supported corpus version (fail-closed)', () => {
    expect(suites.length).toBeGreaterThan(0);
    for (const s of suites) expect(s.corpusVersion).toBe(CORPUS_VERSION);
  });

  for (const suite of loadSuites()) {
    describe(`suite: ${suite.suite} (${suite.vectors.length} vectors)`, () => {
      for (const v of suite.vectors) {
        it(`${v.kind}: ${v.name}`, () => {
          const r = runVector(v);
          expect(r.ok, r.detail).toBe(true);
        });
      }
    });
  }
});
