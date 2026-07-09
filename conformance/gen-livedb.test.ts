/**
 * Live-DB corpus (re)generation entry, run under vitest's ESM resolver (WS7g, #36).
 *
 *   npx vitest run conformance/gen-livedb.test.ts   # writes conformance/vectors-livedb/livedb.json
 *
 * Like gen-vectors.test.ts, this is the SSoT generator wrapped so vitest resolves the ESM-only
 * behavior-contracts from source. It writes the pg+mysql live-DB bundles and asserts (inside
 * writeLivedbCorpus → crossCheckAgainstFrozen) that the captured expectedResult/expectedDbState
 * equal the already-frozen SQLite exec/tx reference — so the live-DB corpus is provably the same
 * reference the SQLite conformance locks.
 */
import { describe, it, expect } from 'vitest';
import { writeLivedbCorpus } from './gen-livedb';

describe('live-DB conformance corpus generation (WS7g)', () => {
  it('writes the pg+mysql live-DB corpus from the TS reference (cross-checked vs frozen)', () => {
    const file = writeLivedbCorpus();
    expect(file).toContain('livedb.json');
    // eslint-disable-next-line no-console
    console.log(`wrote live-DB corpus: ${file}`);
  });
});
