// ════════════════════════════════════════════════════════════════════════════
// Artifact generation (epic #44) — compile the 8 shared-domain behaviors into
// makeSQL bundles + codegen fingerprints, emit the ONE generated artifact every
// language's ir / codegen cells consume.
// ════════════════════════════════════════════════════════════════════════════
//
// 🔒 CONSUME-ONLY: this uses the litedbmodel public compile API (dist/scp) to
// PRODUCE artifacts; it does NOT modify src. The output `generated/bundles.json`
// is the language-neutral §8 published artifact (pure JSON) that Python / Rust /
// PHP / Go load and execute via their thin runtimes — exactly the ir path.

import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as lm from '../../dist/scp/index.mjs';
import { fingerprintComponentGraph } from 'behavior-contracts';
import {
  SCHEMA,
  INPUTS,
  SQL_BASELINE,
  READ_ENTRY,
  READ_RELATION,
  readsContract,
  writesContract,
  writeGateContract,
  CROSSLANG_CASE_IDS_LOCAL,
} from './domain.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(HERE, 'generated', 'bundles.json');

// Seed spec as language-neutral INSERT statements, so every language's ir cell
// seeds an IDENTICAL in-memory SQLite (same dataset the TS cells use).
function seedStatements(): string[] {
  const stmts: string[] = [];
  for (let u = 1; u <= 8; u++) stmts.push(`INSERT INTO users (id, name, post_count) VALUES (${u}, 'user-${u}', 5)`);
  let pid = 0;
  let cid = 0;
  for (let u = 1; u <= 8; u++) {
    for (let k = 0; k < 5; k++) {
      pid++;
      const status = k % 3 === 0 ? 'live' : k % 3 === 1 ? 'draft' : 'live';
      const day = String(k + 1).padStart(2, '0');
      stmts.push(
        `INSERT INTO posts (id, author_id, title, status, views, created_at) VALUES (${pid}, ${u}, 'post-${pid}', '${status}', ${pid * 10}, '2026-02-${day}')`,
      );
      for (let c = 0; c < 5; c++) {
        cid++;
        stmts.push(`INSERT INTO comments (id, post_id, body, created_at) VALUES (${cid}, ${pid}, 'comment-${cid}', '2026-03-01')`);
      }
    }
  }
  return stmts;
}

interface CaseArtifact {
  case: string;
  kind: 'read' | 'relation' | 'batch' | 'tx';
  entry?: string;
  withRelation?: string;
  relation?: unknown;
  bundle: unknown;
  input: unknown;
  fingerprint: string; // codegen: baked-IR fingerprint (fail-closed verified at load)
  expectedQueries: number;
  expectedRows: number;
}

function buildBundle(caseId: string): { bundle: any; kind: CaseArtifact['kind']; entry?: string; withRelation?: string; relation?: unknown } {
  if (caseId === 'batchInsert') {
    const cols = ['author_id', 'title', 'status', 'views', 'created_at'];
    const bundle = lm.compileCreateManyBundle('BatchInsert', { tableName: 'posts', columns: cols, records: INPUTS.batchInsert.rows as any }, 'sqlite');
    return { bundle, kind: 'batch' };
  }
  if (caseId === 'writeTxGate') {
    const bundle = lm.compileWriteBundle(writesContract, 'Create', writeGateContract, 'create', 'sqlite');
    return { bundle, kind: 'tx' };
  }
  const rel = READ_RELATION[caseId];
  if (rel) {
    const bundle = lm.compileBundle(readsContract, READ_ENTRY[caseId], [rel.decl], 'sqlite');
    return { bundle, kind: 'relation', entry: READ_ENTRY[caseId], withRelation: rel.withName, relation: rel.decl };
  }
  const bundle = lm.compileBundle(readsContract, READ_ENTRY[caseId], [], 'sqlite');
  return { bundle, kind: 'read', entry: READ_ENTRY[caseId] };
}

function main(): void {
  const cases: CaseArtifact[] = [];
  for (const caseId of CROSSLANG_CASE_IDS_LOCAL) {
    const { bundle, kind, entry, withRelation, relation } = buildBundle(caseId);
    // Portable IR fingerprint = the codegen fail-closed identity the codegen cell verifies at load.
    const ir = lm.bundleToPortableIR(bundle);
    const fingerprint = fingerprintComponentGraph(ir);
    const base = SQL_BASELINE[caseId];
    cases.push({
      case: caseId,
      kind,
      ...(entry ? { entry } : {}),
      ...(withRelation ? { withRelation } : {}),
      ...(relation ? { relation } : {}),
      bundle,
      input: (INPUTS as any)[caseId] ?? {},
      fingerprint,
      expectedQueries: base.queries,
      expectedRows: base.rows,
    });
  }

  const artifact = {
    generatedAt: new Date().toISOString(),
    corpusVersion: 2,
    schema: [...SCHEMA],
    seed: seedStatements(),
    cases,
  };

  mkdirSync(dirname(OUT), { recursive: true });
  writeFileSync(OUT, JSON.stringify(artifact, null, 2));
  console.error(`Wrote ${OUT} (${cases.length} case bundles)`);
  for (const c of cases) console.error(`  ${c.case.padEnd(14)} kind=${c.kind} fp=${c.fingerprint.slice(0, 12)}… Q=${c.expectedQueries} R=${c.expectedRows}`);
}

main();
