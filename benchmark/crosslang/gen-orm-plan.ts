// ════════════════════════════════════════════════════════════════════════════
// ORM-plan ARTIFACT generator (epic #63) — the language-neutral JSON every runtime reads.
// ════════════════════════════════════════════════════════════════════════════
//
// Emits benchmark/crosslang/generated/orm-plan.json: the 19 ORM ops' per-dialect statement
// plans (baked {sql,params} + baked relation SQL + bind protocol) PLUS the per-dialect DDL +
// seed. The TS executor and the Python/PHP/Rust/Go ports all consume THIS one artifact (the
// non-TS langs cannot call the TS SCP compilers), so every language runs byte-identical SQL.
//
//   generate:  tsx benchmark/crosslang/gen-orm-plan.ts
//   drift-check: tsx benchmark/crosslang/gen-orm-plan.ts --check   (fails if the committed
//               artifact differs from a fresh build — CI gate; regen only with justification)

import { writeFileSync, readFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildOrmPlanArtifact, ORM_OPS, ORM_DIALECTS } from './orm-plan.js';
import { ddl, dropStatements, seedStatements, pgSeqResetStatements, SEED } from './orm-domain.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(HERE, 'generated', 'orm-plan.json');

function buildArtifact(): unknown {
  const plans = buildOrmPlanArtifact();
  const schema: Record<string, { ddl: string[]; drop: string[]; seed: { sql: string; params: unknown[] }[]; seqReset?: string[] }> = {};
  for (const d of ORM_DIALECTS) {
    schema[d] = {
      ddl: ddl(d),
      drop: dropStatements(d),
      seed: seedStatements(d).map((s) => ({ sql: s.sql, params: [...s.params] })),
      ...(d === 'postgres' ? { seqReset: pgSeqResetStatements() } : {}),
    };
  }
  return {
    $schema: 'litedbmodel cross-lang ORM-plan artifact (epic #63)',
    note:
      'The 19 ORM-comparison ops (== #64 v1 SQL golden == #65 v2 SCP parity == benchmark.ts litedbmodel ' +
      'column), rendered per dialect via the v2 SCP makeSQL compile path. Every language executes THESE ' +
      'statements through its shipped runtime driver seam (no compiler at exec time). Relation stages ' +
      'carry baked SQL + a bind protocol (bindKind). Regenerate: tsx benchmark/crosslang/gen-orm-plan.ts.',
    dialects: [...ORM_DIALECTS],
    seed: SEED,
    ops: ORM_OPS.map((o) => ({ id: o.id, label: o.label, write: o.write })),
    schema,
    plans,
  };
}

function serialize(a: unknown): string {
  return JSON.stringify(a, null, 2) + '\n';
}

function main(): void {
  const check = process.argv.includes('--check');
  const fresh = serialize(buildArtifact());
  if (check) {
    let current = '';
    try {
      current = readFileSync(OUT, 'utf8');
    } catch {
      console.error(`✗ orm-plan.json missing at ${OUT} — run: tsx benchmark/crosslang/gen-orm-plan.ts`);
      process.exit(1);
    }
    if (current !== fresh) {
      console.error('✗ orm-plan.json DRIFT — the committed artifact differs from a fresh build.');
      console.error('  Regenerate (with justification): tsx benchmark/crosslang/gen-orm-plan.ts');
      process.exit(1);
    }
    console.log('✓ orm-plan.json is up to date (no drift).');
    return;
  }
  mkdirSync(dirname(OUT), { recursive: true });
  writeFileSync(OUT, fresh);
  console.log(`✓ wrote ${OUT} (${ORM_OPS.length} ops × ${ORM_DIALECTS.length} dialects).`);
}

main();
