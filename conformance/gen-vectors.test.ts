/**
 * Corpus (re)generation entry, run under vitest's resolver (WS7a, #30).
 *
 *   npx vitest run conformance/gen-vectors.test.ts   # writes conformance/vectors/*.json
 *
 * This is NOT part of the assertion suite (excluded from `test/**`); it is the SSoT generator
 * wrapped so vitest resolves the ESM-only behavior-contracts from source. The frozen corpus it
 * writes is then asserted byte-true + green by `test/scp/conformance-vectors.test.ts`.
 */
import { describe, it, expect } from 'vitest';
import { writeCorpus } from './gen-vectors';

describe('conformance corpus generation (WS7a)', () => {
  it('writes the vector corpus from the TS reference', () => {
    const written = writeCorpus();
    expect(written.length).toBeGreaterThan(0);
    // eslint-disable-next-line no-console
    console.log(`wrote ${written.length} suite files:\n  ${written.join('\n  ')}`);
  });
});
