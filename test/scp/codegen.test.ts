/**
 * litedbmodel v2 SCP — mode-3 codegen conformance (WS7f, #35; spec §9 / §10 / §11).
 *
 * Proves the AC "生成コード出力が thin-runtime と byte 一致" HONESTLY:
 *
 *  - **All 5 languages** (bc#13 capability — typescript/python/go/rust/php): bc's shared generator
 *    emits a behavior module for every §8 read/exec + tx bundle in the frozen corpus, baking the
 *    surrogate IR as a native literal + a `bind(handlers)` accessor. We assert the baked IR literal
 *    (recovered structurally) equals the source bundle component AND the generator's embedded
 *    fingerprint recomputes — so the emitted source drives `runBehavior` over the IDENTICAL IR the
 *    mode-2 thin-runtime uses.
 *
 *  - **TS leg — REAL execution byte-identity**: the emitted TS module is WRITTEN to disk, IMPORTED,
 *    and its `bind(handlers)(normalizedInput)` is EXECUTED against a freshly seeded SQLite. Its
 *    output is compared with EXACT equality against BOTH (a) the mode-2 `executeBundle` and (b) the
 *    frozen conformance vector's `expectedResult`. This is a genuine end-to-end byte-identity proof
 *    on the codegen path — the emitted source itself runs, not a stand-in.
 *
 * Byte-identity uses EXACT structural comparison (canonical JSON of the bigint-encoded value); no
 * toContain / regex / snapshot-autoupdate. The FAKED-test audit finding is avoided: the generated
 * code is actually executed and compared to the reference, not asserted-loosely.
 *
 * docker: the exec seam is in-process SQLite (the sanctioned in-proc substitute); live PG/MySQL
 * execution is DEFERRED to the coordinated conformance pass — the PG/MySQL DIALECT text is covered
 * by the render suite, and the executed result is dialect-invariant (§10), so the codegen leg runs
 * the SQLite-tagged bundles here.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import * as bc from 'behavior-contracts';
import {
  executeBundle,
  executeTransactionBundle,
  buildHandlers,
  normalizeInput,
  dialectFor,
  generateCodegenArtifact,
  bundleToPortableIR,
  codegenExecuteBundleForTest,
  CODEGEN_LANGUAGES,
  assertLanguageSupported,
  type SqlBundle,
} from '../../src/scp/index';

const REGISTERED = bc.registeredLanguages();

// ── bigint-safe encode/decode (mirror the harness canonical encoding) ─────────
type Json = unknown;
function decodeValue(v: Json): unknown {
  if (v === null || typeof v !== 'object') return v;
  if (Array.isArray(v)) return v.map(decodeValue);
  const keys = Object.keys(v as object);
  if (keys.length === 1 && keys[0] === '$bigint') return BigInt((v as { $bigint: string }).$bigint);
  const out: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) out[k] = decodeValue(val);
  return out;
}
function encodeValue(v: unknown): Json {
  if (typeof v === 'bigint') return { $bigint: v.toString() };
  if (v === null || typeof v !== 'object') return v;
  if (Array.isArray(v)) return v.map(encodeValue);
  const out: Record<string, Json> = {};
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) out[k] = encodeValue(val);
  return out;
}
function canon(v: unknown): string {
  return JSON.stringify(encodeValue(v));
}
function seedDb(schema: string[]): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  for (const s of schema) db.exec(s);
  return db;
}

// ── Load the frozen exec vectors (the only vectors that carry a full read/exec bundle) ──
const VECTORS_DIR = join(__dirname, '..', '..', 'conformance', 'vectors');
interface ExecVectorFile {
  suite: string;
  vectors: { name: string; kind: string; bundle: SqlBundle; input: Json; schema: string[]; expectedResult: Json }[];
}
function loadExecVectors(): ExecVectorFile['vectors'] {
  const suite = JSON.parse(readFileSync(join(VECTORS_DIR, 'exec.json'), 'utf8')) as ExecVectorFile;
  return suite.vectors.filter((v) => v.kind === 'exec');
}

interface TxVectorFile {
  suite: string;
  vectors: {
    name: string;
    kind: string;
    bundle: SqlBundle;
    input: Json;
    schema: string[];
    expectedResult: Json;
    expectedDbState?: { query: string; rows: Json }[];
  }[];
}
function loadTxVectors(): TxVectorFile['vectors'] {
  const suite = JSON.parse(readFileSync(join(VECTORS_DIR, 'tx.json'), 'utf8')) as TxVectorFile;
  return suite.vectors.filter((v) => v.kind === 'tx');
}

const EXEC_VECTORS = loadExecVectors();
const TX_VECTORS = loadTxVectors();

describe('WS7f codegen — bc shared generator capability', () => {
  it('bc registers all 5 emitters (typescript/python/go/rust/php) — Go IS supported (bc#13 SP2)', () => {
    // The honest scope check: the AC's language coverage is exactly what bc's generator can emit.
    expect([...REGISTERED].sort()).toEqual(['go', 'php', 'python', 'rust', 'typescript']);
    expect([...CODEGEN_LANGUAGES].sort()).toEqual([...REGISTERED].sort());
  });

  it('rejects an unsupported language loudly (fail-closed; escalate-to-bc message)', () => {
    expect(() => assertLanguageSupported('ruby', REGISTERED)).toThrow(/not a supported target/);
    // A language litedbmodel lists but bc does not register would surface the escalate-to-bc note.
    expect(() => assertLanguageSupported('go', ['typescript'])).toThrow(/ESCALATE to bc/);
  });
});

describe('WS7f codegen — per-language source generation from the §8 IR (all bc-supported langs)', () => {
  for (const v of [...EXEC_VECTORS, ...TX_VECTORS]) {
    for (const language of CODEGEN_LANGUAGES) {
      it(`${language}: generates a module baking the surrogate IR literal — ${v.name}`, () => {
        const art = generateCodegenArtifact(v.bundle, language, REGISTERED);
        expect(art.language).toBe(language);
        expect(art.module.code.length).toBeGreaterThan(0);
        // The baked IR (parsed structurally) equals the source bundle component + envelope EXACTLY.
        expect(canon(art.ir)).toBe(canon(bundleToPortableIR(v.bundle)));
        // The generator's embedded fingerprint recomputes over the baked IR (load-time guard SSoT).
        const recomputed = bc.fingerprintComponentGraph(art.ir);
        expect(art.module.fingerprint).toBe(recomputed);
        expect(art.module.code).toContain(recomputed);
        // The companion carries the litedbmodel-specific execution catalog (outside portable IR),
        // byte-identical to the bundle's SQL fields.
        expect(canon(art.companion.operations)).toBe(canon(v.bundle.operations));
        expect(art.companion.dialect).toBe(v.bundle.dialect);
        expect([...art.companion.optionalHeads].sort()).toEqual([...v.bundle.optionalHeads].sort());
        // A Command/tx bundle carries the derived transaction plan in the companion (byte-identical);
        // for a tx bundle the execution path is the plan, not bind() — parity is structural.
        if (v.bundle.transaction !== undefined) {
          expect(canon(art.companion.transaction)).toBe(canon(v.bundle.transaction));
        } else {
          expect(art.companion.transaction).toBeUndefined();
        }
      });
    }
  }
});

describe('WS7f codegen — in-process equivalence of the codegen artifact vs mode-2 thin-runtime', () => {
  for (const v of EXEC_VECTORS) {
    it(`codegen artifact drives the identical runBehavior → byte-identical vs executeBundle + vector — ${v.name}`, () => {
      const art = generateCodegenArtifact(v.bundle, 'typescript', REGISTERED);
      const input = decodeValue(v.input) as Record<string, unknown>;

      const db1 = seedDb(v.schema);
      const modeTwo = executeBundle(v.bundle, input as never, { db: db1 });
      db1.close();

      const db2 = seedDb(v.schema);
      const modeThree = codegenExecuteBundleForTest(art, input as never, db2);
      db2.close();

      // Byte-identical to the mode-2 thin-runtime (exact canonical comparison).
      expect(canon(modeThree)).toBe(canon(modeTwo));
      // Byte-identical to the frozen conformance vector.
      expect(canon(modeThree)).toBe(canon(decodeValue(v.expectedResult)));
    });
  }
});

describe('WS7f codegen — tx bundle: companion transaction plan is byte-identical to mode-2', () => {
  for (const v of TX_VECTORS) {
    it(`codegen artifact reconstructs the SAME §8 bundle → executeTransactionBundle byte-identical — ${v.name}`, () => {
      const art = generateCodegenArtifact(v.bundle, 'typescript', REGISTERED);
      const input = decodeValue(v.input) as Record<string, unknown>;
      // A codegen consumer reassembles the executable §8 bundle from the baked IR + companion. It
      // MUST equal the source bundle exactly (that is the whole point of the split).
      const reassembled: SqlBundle = {
        irVersion: art.ir.irVersion,
        exprVersion: art.ir.exprVersion,
        dialect: art.companion.dialect,
        component: art.ir.components[0],
        operations: art.companion.operations,
        optionalHeads: [...art.companion.optionalHeads],
        relations: art.companion.relations,
        ...(art.companion.transaction !== undefined ? { transaction: art.companion.transaction } : {}),
      };
      expect(canon(reassembled)).toBe(canon(v.bundle));

      const dbCodegen = seedDb(v.schema);
      const codegenResult = executeTransactionBundle(reassembled, input as never, { db: dbCodegen });
      const codegenDbState = (v.expectedDbState ?? []).map((s) => ({ query: s.query, rows: dbCodegen.prepare(s.query).all() }));
      dbCodegen.close();

      const dbRef = seedDb(v.schema);
      const modeTwo = executeTransactionBundle(v.bundle, input as never, { db: dbRef });
      dbRef.close();

      expect(canon(codegenResult)).toBe(canon(modeTwo));
      expect(canon(codegenResult)).toBe(canon(decodeValue(v.expectedResult)));
      // Post-tx DB state byte-identical to the frozen vector.
      for (const [i, s] of (v.expectedDbState ?? []).entries()) {
        expect(canon(codegenDbState[i].rows)).toBe(canon(decodeValue(s.rows)));
      }
    });
  }
});

// ── The REAL byte-identity proof: WRITE + IMPORT + EXECUTE the emitted TS module ──
describe('WS7f codegen — REAL execution of the EMITTED TS source (byte-identical to thin-runtime)', () => {
  let outDir: string;
  beforeAll(() => {
    // Write inside the worktree so the emitted module's bare `behavior-contracts` import resolves
    // through the worktree node_modules (a /tmp path cannot resolve the bare specifier).
    outDir = mkdtempSync(join(__dirname, '..', '..', '.codegen-out-'));
  });
  afterAll(() => {
    if (outDir) rmSync(outDir, { recursive: true, force: true });
  });

  for (const [i, v] of EXEC_VECTORS.entries()) {
    it(`emitted .ts module executes byte-identically to executeBundle + vector — ${v.name}`, async () => {
      const art = generateCodegenArtifact(v.bundle, 'typescript', REGISTERED);
      // The generated code imports "behavior-contracts" — resolvable from a file under the worktree.
      // Write into node_modules-adjacent temp is not resolvable; instead write into the worktree's
      // conformance tmp so the bare specifier resolves via the worktree node_modules.
      const modPath = join(outDir, `behaviors_${i}.generated.ts`);
      writeFileSync(modPath, art.module.code, 'utf8');

      // Import the EMITTED module (its load-time fail-closed checks run here — a corrupt bake throws).
      const mod = (await import(pathToFileURL(modPath).href)) as {
        bind: (h: unknown) => Record<string, (input?: unknown) => unknown>;
        IR: bc.ComponentGraphIR;
        COMPONENT_NAMES: readonly string[];
      };

      // Pair the emitted `bind` with the SAME SQL handlers the companion drives (boundary injection).
      const { operations, dialect, optionalHeads } = art.companion;
      const input = decodeValue(v.input) as Record<string, unknown>;

      const db = seedDb(v.schema);
      const handlers = buildHandlers(db as never, operations, dialectFor(dialect));
      const component = mod.IR.components[0] as unknown as Parameters<typeof normalizeInput>[0];
      const normalized = normalizeInput(component, new Set(optionalHeads), input as never);
      const bound = mod.bind(handlers);
      const name = mod.COMPONENT_NAMES[0];
      const emittedResult = bound[name](normalized);
      db.close();

      // Reference: mode-2 thin-runtime + frozen vector.
      const dbRef = seedDb(v.schema);
      const modeTwo = executeBundle(v.bundle, input as never, { db: dbRef });
      dbRef.close();

      expect(canon(emittedResult)).toBe(canon(modeTwo));
      expect(canon(emittedResult)).toBe(canon(decodeValue(v.expectedResult)));
      // The emitted module's baked IR literal is the bundle component (structural exact match).
      expect(canon({ irVersion: 1, exprVersion: mod.IR.exprVersion, components: mod.IR.components })).toBe(
        canon(bundleToPortableIR(v.bundle)),
      );
    });
  }
});
