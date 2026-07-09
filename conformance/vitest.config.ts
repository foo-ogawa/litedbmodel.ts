import { defineConfig } from 'vitest/config';
import path from 'path';

/**
 * Vitest config for the conformance CORPUS GENERATOR (WS7a, #30).
 *
 * The generator (`gen-vectors.test.ts`) imports the SCP reference from source, which pulls in
 * ESM-only behavior-contracts — vitest's node resolver handles that seam (the same way the main
 * test suite does). It is kept OUT of the main `test/**` include so a normal `vitest run` never
 * (re)writes the frozen corpus; regeneration is an explicit, opt-in step.
 */
export default defineConfig({
  root: path.resolve(__dirname, '..'),
  test: {
    globals: true,
    environment: 'node',
    include: ['conformance/gen-vectors.test.ts'],
    pool: 'forks',
    fileParallelism: false,
  },
  resolve: {
    conditions: ['node'],
  },
});
