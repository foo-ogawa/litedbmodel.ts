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
    // WRITE (batch / tx): the generated module's single `makeSQL` node IS the whole write; its
    // outType is the TransactionResult typed shape (obj{committed,executed,shortCircuit,entity,
    // returnedRows}). We route the write THROUGH the generated module's bind(): the `makeSQL`
    // handler drives the derived transaction plan via the runtime's executeTransactionBundle
    // (gate-first, byte-parity with the thin runtime) and returns the TransactionResult, which the
    // generated de-interpreted `run_<Component>` returns as the module output (the typed-view de-box
    // is the typescript-typed endpoint's row materialization). This is a DISTINCT code entry from the
    // ir cell's executeBundle — the write executes through the generated module, not around it.
    const bound = mod.bind(makeWriteHandler(c.bundle, c.kind === 'tx' ? c.input : {}, db));
    const entry = mod.COMPONENT_NAMES[0];
    const run = bound[entry];
    return () => run(WRITE_MODULE_INPUT);
  },
};

// The input scope for a WRITE module's generated function: bc's makeSqlComponentIR node reads
// `__sql`/`__sqlParams`/`__skip` from the input (the boxed read path's convention), so those heads
// MUST be present or slBind fail-closes (UNKNOWN_BINDING). The generated function passes them to the
// `makeSQL` handler as ports, but our write handler ignores them (it drives the plan from the bundle),
// so present-as-empty is exact — the values are never read. Not a fabricated default: the surrogate input.
const WRITE_MODULE_INPUT = { __sql: '', __sqlParams: [], __skip: false };

// Build the `makeSQL` handler for a generated WRITE module: run the derived transaction plan (the
// SAME executeTransactionBundle the ir path uses — gate-first, exact parity) and return the
// TransactionResult, NORMALIZED to the full-5-key present-as-null shape the typed outType declares
// (the runtime omits an absent optional key; the seam presents the typed contract's wire shape).
function makeWriteHandler(bundle: any, input: any, db: any): Record<string, any> {
  const handler = () => {
    const result = lm.executeTransactionBundle(bundle, input, { db });
    return { ok: normalizeTxResult(result) };
  };
  return { makeSQL: handler };
}

/** Present a TransactionResult as the canonical full-5-key shape (absent optional → present-as-null). */
function normalizeTxResult(result: any): any {
  if (result === null || typeof result !== 'object' || Array.isArray(result)) return result;
  return {
    committed: result.committed ?? false,
    executed: result.executed ?? [],
    shortCircuit: result.shortCircuit ?? null,
    entity: result.entity ?? null,
    returnedRows: result.returnedRows ?? null,
  };
}

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
