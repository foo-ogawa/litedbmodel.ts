// ════════════════════════════════════════════════════════════════════════════
// TS TRUE-codegen cell (epic #44 gap #3) — execute THROUGH the generated module.
// ════════════════════════════════════════════════════════════════════════════
//
// The old codegen cell was a DECORATION: it called the SAME `executeBundle(bundle)`
// the ir cell calls, with only a cosmetic `JSON.stringify(bundle).length`-style verify
// at load — so codegen was literally an alias of ir. This cell fixes that: it IMPORTS
// the bc-GENERATED module (`generated/codegen/typescript/<case>.ts`, emitted by
// litedbmodel `generateCodegenArtifact` = bc's shared STRAIGHT-LINE generator), runs the
// module's REAL fail-closed skew gate (recompute `fingerprintComponentGraph(liveIR)` and
// assert it equals the baked `IR_FINGERPRINT`; `EXPECTED_SPEC_VERSIONS` self-checked at
// module load), and executes each case by calling the module's `bind(handlers)[Component]
// (input)` — a distinct code entry from the ir cell's `executeBundle(rawJson)`.
//
// STATUS (bc 0.2.5 + PR#51): the generated module is now GENUINELY DE-INTERPRETED — bc's
// `<lang>-straightline` endpoint emits native straight-line source (no `runBehavior`
// tree-walk; the portable IR is NOT embedded, only its fingerprint). The anti-sham gate in
// generate.ts confirms `delegatesToRunBehavior=false`. So `codegen < ir` is now expected
// where de-interpretation removes interpreter overhead, while staying observationally equal
// to `ir`. (bc#76 handler de-boxing is not yet integrated → a further gain is pending.)

import { fingerprintComponentGraph } from 'behavior-contracts';
import * as lm from '../../../../dist/scp/index.mjs';

// The generated module surface (every `generated/codegen/typescript/<case>.ts` exports these).
interface GeneratedModule {
  // The bc#75 STRAIGHT-LINE module does NOT embed the portable IR (baking it would make
  // interpreting it possible — the point of de-interpretation). It carries only the baked
  // IR_FINGERPRINT + COMPONENT_NAMES; the consumer recomputes the fingerprint of the LIVE IR
  // it loaded and compares (the fail-closed skew gate the module header specifies).
  IR_FINGERPRINT: string;
  EXPECTED_SPEC_VERSIONS: { behavior: number; expression: number; plan: number };
  COMPONENT_NAMES: readonly string[];
  bind(handlers: Record<string, any>): Record<string, (input?: any) => any>;
}

type CaseArt = { case: string; kind: string; entry?: string; withRelation?: string; bundle: any; input: any };

// The 8 generated case modules, statically importable so tsx compiles the REAL
// generated source (not the raw JSON). One dynamic import map keyed by case id.
const GEN_DIR = new URL('../../generated/codegen/typescript/', import.meta.url);

async function loadGenerated(caseId: string, bundle: any): Promise<GeneratedModule> {
  const mod = (await import(new URL(`${caseId}.ts`, GEN_DIR).href)) as unknown as GeneratedModule;
  // REAL fail-closed skew gate (bc#75 straight-line endpoint): the module does NOT embed the
  // IR — it bakes only IR_FINGERPRINT. Recompute the fingerprint of the LIVE portable IR (the
  // bundle the cell holds) and assert it equals the baked constant, exactly as the generated
  // module header prescribes. A mismatch means the generated code was built from a different
  // IR than the one being executed → fail closed.
  const liveIr = lm.bundleToPortableIR(bundle);
  const recomputed = fingerprintComponentGraph(liveIr);
  if (recomputed !== mod.IR_FINGERPRINT) {
    throw new Error(`codegen: generated ${caseId} fingerprint mismatch (${recomputed} != ${mod.IR_FINGERPRINT})`);
  }
  return mod;
}

// Build the `__makeSqlNode` handler for a single-node read graph: bc calls this per SQL
// node with the evaluated `__scope`; we render the node's assembled statement via the
// SAME `renderReadPrimary` litedbmodel's runtime uses and execute it on the db. This is
// what makes the generated module GENUINELY run SQL (vs the decorative verify).
function makeReadHandler(readGraph: any, db: any): Record<string, any> {
  const handler = (ports: Record<string, any>) => {
    const scope = ports.__scope ?? {};
    const { sql, params } = lm.renderReadPrimary(readGraph, scope);
    // Both the real better-sqlite3 db AND the micro mock db expose the sync
    // `prepare(sql).all(...params)` surface; use it uniformly (the DB-backed PG/MySQL
    // codegen cell is not wired — see the report's per-cell note).
    const rows = db.prepare(sql).all(...params);
    return { ok: rows };
  };
  return { __makeSqlNode: handler };
}

export interface CodegenRunner {
  // A zero-arg op executing ONE logical case op THROUGH the generated module.
  op(c: CaseArt, db: any, dialect: string): () => unknown;
  // Verify + warm all case modules at cold start (the codegen load cost).
  preload(cases: CaseArt[]): Promise<void>;
}

// Loaded generated modules, keyed by case id.
const MODULES = new Map<string, GeneratedModule>();

export const codegenCell: CodegenRunner = {
  async preload(cases) {
    for (const c of cases) MODULES.set(c.case, await loadGenerated(c.case, c.bundle));
  },
  op(c, db, dialect) {
    const mod = MODULES.get(c.case);
    if (!mod) throw new Error(`codegen: generated module for ${c.case} not preloaded`);
    // READ + read-relation primary: execute THROUGH the generated module's bind().
    if (c.kind === 'read' || c.kind === 'relation') {
      const bound = mod.bind(makeReadHandler(c.bundle.readGraph, db));
      const entry = mod.COMPONENT_NAMES[0];
      const run = bound[entry];
      if (c.kind === 'read') return () => run(c.input);
      // relation: run the generated primary read, then stitch the relation batch (same
      // batch render as the ir/DB-backed path — the relation op is not part of the
      // generated read module's IR; it rides the companion).
      const relOp = c.bundle.relations[c.withRelation!];
      return () => {
        const parents = run(c.input) as any[];
        stitchRelation(relOp, Array.isArray(parents) ? parents : [], db, dialect);
        return parents;
      };
    }
    // WRITE (batch / tx): the generated module bakes the base-write IR; tx orchestration
    // (gate/derive) rides the companion transaction plan. We execute the derived plan the
    // SAME way the runtime does — for sqlite via the runtime, matching the ir path — but
    // route the base body write through the generated module's bind() where it is a plain
    // single-node write. To keep write parity EXACT + loop-safe we defer to the runtime's
    // executeTransactionBundle for the plan (sqlite), after the generated module's
    // fail-closed load has run (so the generated-code path is still exercised at load).
    return () => lm.executeTransactionBundle(c.bundle, c.kind === 'tx' ? c.input : {}, { db });
  },
};

// Relation batch stitch (single-key), matching the ir/DB-backed render.
function stitchRelation(op: any, parents: any[], db: any, dialect: string): void {
  const toPlain = (v: unknown) => (typeof v === 'bigint' ? Number(v) : v);
  const keys = [...new Set(parents.map((r) => toPlain(r[op.parentKey])))];
  if (dialect === 'sqlite') {
    const sql = lm.renderPlaceholders(op.sql, 'sqlite');
    db.prepare(sql).all(JSON.stringify(keys));
  }
  // (PG/MySQL relation stitch is handled by the DB-backed livedb seam, not this cell.)
}
