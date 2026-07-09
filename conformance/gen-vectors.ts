/**
 * Generate the conformance vector corpus from the TS SCP reference (WS7a, #30).
 *
 * Writes one `conformance/vectors/<suite>.json` per suite from {@link generateCorpus}, which
 * captures every expected field by running the REAL reference (compile + render + execute
 * against in-memory better-sqlite3). The corpus is therefore byte-true to the reference by
 * construction — never hand-authored.
 *
 * Run:
 *   npx tsx conformance/gen-vectors.ts            # (re)generate + write the corpus
 *   npx tsx conformance/gen-vectors.ts --check    # fail (exit 1) if the on-disk corpus drifts
 *
 * The `--check` mode is the frozen/additive-refreeze gate (mirrors graphddb): regenerate in
 * memory and compare to the committed files; any difference means the reference changed without
 * a re-freeze (or a corpus was hand-edited). It never writes.
 *
 * NOTE ON EXECUTION: this script imports the reference from SOURCE (`../src/scp/index`). Because
 * the repo is CommonJS and behavior-contracts is ESM-only, run it via vitest's resolver, not raw
 * node/tsx — the npm script `conformance:gen` wires `vitest run conformance/gen-vectors.test.ts`,
 * and the standalone cross-language orchestrator consumes the BUILT `dist/scp/index.mjs`. This
 * module exports {@link writeCorpus}/{@link checkCorpus} so a vitest wrapper can drive it.
 */

import { writeFileSync, readFileSync, existsSync, mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { generateCorpus, type Suite } from './harness';

const HERE = dirname(fileURLToPath(import.meta.url));
export const VECTORS_DIR = join(HERE, 'vectors');

/** Stable, pretty JSON (2-space) so the corpus diffs cleanly and freezes deterministically. */
function serialize(suite: Suite): string {
  return JSON.stringify(suite, null, 2) + '\n';
}

/** Write every suite to `conformance/vectors/<suite>.json`. Returns the files written. */
export function writeCorpus(): string[] {
  if (!existsSync(VECTORS_DIR)) mkdirSync(VECTORS_DIR, { recursive: true });
  const written: string[] = [];
  for (const suite of generateCorpus()) {
    const file = join(VECTORS_DIR, `${suite.suite}.json`);
    writeFileSync(file, serialize(suite), 'utf8');
    written.push(file);
  }
  return written;
}

/** Compare the freshly generated corpus to the on-disk files. Returns the drifted suite names. */
export function checkCorpus(): string[] {
  const drift: string[] = [];
  for (const suite of generateCorpus()) {
    const file = join(VECTORS_DIR, `${suite.suite}.json`);
    if (!existsSync(file)) {
      drift.push(`${suite.suite} (missing on disk)`);
      continue;
    }
    if (readFileSync(file, 'utf8') !== serialize(suite)) drift.push(suite.suite);
  }
  return drift;
}
