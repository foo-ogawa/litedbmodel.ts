/**
 * build-scp — dual ESM + CJS build of the litedbmodel v2 SCP subsystem (WS3, #23).
 *
 * ## The ESM/CJS seam
 *
 * behavior-contracts@0.2.0 is **ESM-only** (`"type":"module"`, exports only `import`), while
 * litedbmodel ships CommonJS (`main: dist/index.js`, `tsconfig module: CommonJS`). WS1/WS2
 * were compile-only (vitest's ESM loader resolved bc), so the seam was inert. WS3 EXECUTES
 * bc at runtime (`runBehavior` / `evaluateExpression`), so a plain `tsc` CJS build emitting
 * `require('behavior-contracts')` fails at runtime with `ERR_PACKAGE_PATH_NOT_EXPORTED`.
 *
 * ## Chosen fix (clean, not a dynamic-import bodge)
 *
 * Build the SCP subsystem with esbuild (already a devDep) to **both** formats from the one
 * TS source of truth:
 *   - `dist/scp/index.mjs` — ESM, bc left EXTERNAL (a native ESM consumer imports bc directly).
 *   - `dist/scp/index.cjs` — CJS, bc **bundled IN** (esbuild transpiles bc's ESM into the CJS
 *     output), so `require('litedbmodel/scp')` works with zero runtime ESM/CJS friction.
 * `better-sqlite3` stays external in both (a native addon; the consumer supplies it).
 *
 * The `litedbmodel/scp` subpath export (package.json `exports`) points `import` → the .mjs
 * and `require` → the .cjs. v1's CommonJS main (`dist/index.js`, built by `tsc`) is untouched
 * — v1 consumers keep working; only the new SCP surface gains the dual output. Types come
 * from `tsc` (`dist/scp/index.d.ts`).
 */

import { build } from 'esbuild';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const entry = resolve(root, 'src/scp/index.ts');

/** External in every build: the native SQLite addon (consumer-supplied). */
const alwaysExternal = ['better-sqlite3'];

async function run() {
  // ESM: bc stays external (a native ESM consumer imports it directly).
  await build({
    entryPoints: [entry],
    outfile: resolve(root, 'dist/scp/index.mjs'),
    bundle: true,
    format: 'esm',
    platform: 'node',
    target: 'node18',
    external: [...alwaysExternal, 'behavior-contracts'],
    logLevel: 'info',
  });

  // CJS: bc is bundled IN so `require` works without touching bc's ESM-only exports.
  await build({
    entryPoints: [entry],
    outfile: resolve(root, 'dist/scp/index.cjs'),
    bundle: true,
    format: 'cjs',
    platform: 'node',
    target: 'node18',
    external: alwaysExternal,
    logLevel: 'info',
  });
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
