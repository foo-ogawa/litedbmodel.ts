import { defineConfig } from 'vitest/config';
import path from 'path';

/**
 * Vitest config for the WS7g (#36) LIVE-DB corpus generator (`gen-livedb.test.ts`).
 *
 * Mirrors `vitest.config.ts` (the exec/tx corpus generator): it imports the SCP reference from
 * source (ESM-only behavior-contracts resolved by vitest's node resolver) and is kept OUT of the
 * main `test/**` include, so regeneration of the pg+mysql live-DB bundles is an explicit opt-in
 * step (npm run conformance:gen:livedb) — a normal `vitest run` never rewrites it.
 */
export default defineConfig({
  root: path.resolve(__dirname, '..'),
  test: {
    globals: true,
    environment: 'node',
    include: ['conformance/gen-livedb.test.ts'],
    pool: 'forks',
    fileParallelism: false,
  },
  resolve: {
    conditions: ['node'],
  },
});
