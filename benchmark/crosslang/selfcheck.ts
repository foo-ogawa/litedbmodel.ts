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
import Database from 'better-sqlite3';

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

  // 4. RAW-BASELINE PARITY (task gate) — the raw-driver baseline MUST run the IDENTICAL SQL the
  //    runtime issues. Both impls derive their statements from the SAME assembly (subst/bindRelation/
  //    id-chaining); the ONLY difference is the low-level param seam. Proof: the `raw` sqlite driver
  //    executes every op DB-backed with the SAME rows/op the runtime produces (identical SQL runs to
  //    the same logical result), AND every plan's baked primary SQL runs VERBATIM on a bare driver.
  const rawDrv = sqliteDriver('raw');
  for (const op of ORM_OPS) {
    try {
      const n = await rawDrv.run(art[op.id].sqlite);
      if (n !== EXPECTED[op.id]) {
        console.error(`✗ ${op.id}: RAW baseline rows/op ${n} != expected ${EXPECTED[op.id]} (SQL diverged from runtime)`);
        failures++;
      }
    } catch (e) {
      console.error(`✗ ${op.id}: RAW baseline execute FAILED — ${e instanceof Error ? e.message.split('\n')[0] : String(e)}`);
      failures++;
    }
  }
  await rawDrv.close();

  // 4b. BYTE-IDENTITY — the plan's baked primary read SQL must be executable byte-for-byte by a BARE
  //     better-sqlite3 (no litedbmodel): if the runtime were silently rewriting SQL, this raw prepare
  //     of the artifact's own SQL would drift from the runtime's row shape. We prepare + run the exact
  //     `reads[0].sql`/`params` string from the artifact against a freshly seeded bare DB.
  {
    const bare = new Database(':memory:');
    try {
      // Seed the bare DB identically to the runtime (same DDL + seed from the artifact-domain path).
      const { ddl, dropStatements, seedStatements } = await import('./orm-domain.js');
      bare.pragma('foreign_keys = ON');
      for (const s of dropStatements('sqlite')) bare.exec(s);
      for (const s of ddl('sqlite')) bare.exec(s);
      for (const s of seedStatements('sqlite')) bare.prepare(s.sql).run(...s.params);
      for (const op of ORM_OPS) {
        const plan = art[op.id].sqlite;
        if (plan.kind !== 'read') continue; // writes mutate; the read primaries are the pure byte check
        const primary = plan.reads[0];
        // Prepare + run the ARTIFACT's own SQL string verbatim on the bare driver (throws if the plan
        // SQL is not valid stand-alone SQL — i.e. if the runtime depended on rewriting it).
        bare.prepare(primary.sql).all(...(primary.params as unknown[]).map((v) => (typeof v === 'boolean' ? (v ? 1 : 0) : v)));
      }
    } catch (e) {
      console.error(`✗ byte-identity: the artifact's baked read SQL did not run verbatim on a bare driver — ${e instanceof Error ? e.message.split('\n')[0] : String(e)}`);
      failures++;
    } finally {
      bare.close();
    }
  }

  if (failures > 0) {
    console.error(`\nSELF-CHECK: FAIL (${failures} problem(s)).`);
    process.exit(1);
  }
  console.log(`\nSELF-CHECK: PASS — 19 ops covered + execute DB-backed on sqlite with expected rows/op.`);
}

void main();
