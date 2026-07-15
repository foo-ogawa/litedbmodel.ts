// ════════════════════════════════════════════════════════════════════════════
// Cross-lang bench SELF-CHECK (epic #63) — anti-sham gates for the 19-op model.
// ════════════════════════════════════════════════════════════════════════════
//
// Fast, fail-closed gates that make the unified bench honest (no subset, no fabricated
// numbers, same logical work across languages):
//   1. COVERAGE — the plan artifact + the op axis cover EXACTLY the 19 ORM ops.
//   2. EXECUTE  — every op runs DB-backed on sqlite (in-proc, always available) with the
//      EXPECTED rows/op (the ORM-bench logical work). A parser-invalid / wrong-work op fails.
//   3. FAIRNESS — the rows/op the executor observes matches the golden expectation, i.e. the
//      SAME v2 SCP SQL the ORM-bench litedbmodel column runs (the TS==ORM consistency anchor).
//
// (The old #44 codegen-module anti-sham gates are gone — there is no codegen-module cell.)

import { buildOrmPlanArtifact, ORM_OPS, ORM_OP_IDS } from './orm-plan.js';
import { sqliteDriver } from './orm-exec-ts.js';

// Expected rows/op (reads) / statements (writes) — the ORM-bench logical work against the shared
// seed. Identical across dialects; the selfcheck proves it on the always-available sqlite path.
const EXPECTED: Record<string, number> = {
  findAll: 100,
  filterPaginateSort: 20,
  nestedFindAll: 300,
  findFirst: 1,
  nestedFindFirst: 3,
  findUnique: 1,
  nestedFindUnique: 1,
  create: 1,
  nestedCreate: 2,
  update: 1,
  nestedUpdate: 2,
  upsert: 1,
  nestedUpsert: 2,
  delete: 2,
  createMany: 1,
  upsertMany: 1,
  updateMany: 1,
  nestedRelations: 700,
  compositeRelations: 140,
};

async function main(): Promise<void> {
  console.log('=== litedbmodel #63 cross-lang self-check (coverage + execute + rows/op fairness) ===');
  let failures = 0;

  // 1. COVERAGE — exactly the 19 ORM ops, no subset, no stray.
  if (ORM_OP_IDS.length !== 19) {
    console.error(`✗ coverage: expected 19 ops, got ${ORM_OP_IDS.length}`);
    failures++;
  }
  const art = buildOrmPlanArtifact();
  for (const op of ORM_OPS) {
    if (!art[op.id]) {
      console.error(`✗ coverage: no plan for op "${op.id}"`);
      failures++;
    }
    if (!(op.id in EXPECTED)) {
      console.error(`✗ coverage: no expected rows/op for "${op.id}"`);
      failures++;
    }
  }
  for (const id of Object.keys(EXPECTED)) {
    if (!ORM_OP_IDS.includes(id)) {
      console.error(`✗ coverage: expected-table op "${id}" is not in the op axis`);
      failures++;
    }
  }

  // 2/3. EXECUTE + FAIRNESS — run every op on real in-proc sqlite; rows/op MUST equal EXPECTED.
  const drv = sqliteDriver();
  for (const op of ORM_OPS) {
    try {
      const n = await drv.run(art[op.id].sqlite);
      if (n !== EXPECTED[op.id]) {
        console.error(`✗ ${op.id}: rows/op ${n} != expected ${EXPECTED[op.id]}`);
        failures++;
      }
    } catch (e) {
      console.error(`✗ ${op.id}: execute FAILED — ${e instanceof Error ? e.message.split('\n')[0] : String(e)}`);
      failures++;
    }
  }
  await drv.close();

  if (failures > 0) {
    console.error(`\nSELF-CHECK: FAIL (${failures} problem(s)).`);
    process.exit(1);
  }
  console.log(`\nSELF-CHECK: PASS — 19 ops covered + execute DB-backed on sqlite with expected rows/op.`);
}

void main();
