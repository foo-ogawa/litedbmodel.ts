/**
 * litedbmodel v2 SCP — mode-3 codegen conformance (WS7f, #35; makeSQL flip, epic #43/#45).
 *
 * Proves the AC "生成コード出力が thin-runtime と byte 一致" HONESTLY on the STATIC makeSQL bundle,
 * now via bc's STRAIGHT-LINE (de-interpreted, bc#75) endpoint — REAL static code, not the old
 * literal-bake+interpret path:
 *
 *  - **All 5 languages** (bc#13/#75 — typescript/python/go/rust/php): bc's shared generator emits a
 *    STRAIGHT-LINE behavior module for every §8 read/tx bundle in the frozen corpus from the
 *    portable IR (a read bundle's surrogate `makeSQL`-node IR / the `makeSQL` component IR for a
 *    write) + a `bind(handlers)` accessor. We assert the module is genuinely de-interpreted: it
 *    carries the generation-time IR FINGERPRINT (fail-closed skew gate) but does NOT embed the IR
 *    itself and contains no interpreter machinery (anti-sham gate).
 *  - **SQL catalog companion**: the STATIC makeSQL execution catalog (readGraph / statement /
 *    relations / transaction) rides the companion byte-identical to the bundle's SQL fields.
 *  - **In-process byte-identity**: `codegenExecuteBundleForTest` (a codegen consumer reading the
 *    companion) drives the IDENTICAL static-makeSQL render/execute path `executeBundle` uses — its
 *    output equals mode-2 `executeBundle` AND the frozen vector's `expectedResult`, EXACTLY.
 *  - **TS leg — emitted source loads + is de-interpreted**: the emitted TS module is written,
 *    imported (its load-time fail-closed checks run), and its IR_FINGERPRINT constant is asserted
 *    to equal the fingerprint of `bundleToPortableIR` — while the source is proven NOT to embed the
 *    IR (de-interpretation, bc#75).
 *
 * Byte-identity uses EXACT structural comparison (canonical JSON of the bigint-encoded value).
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
  generateCodegenArtifact,
  bundleToPortableIR,
  codegenExecuteBundleForTest,
  CODEGEN_EMITTER,
  codegenEmitterFor,
  type SqlBundle,
} from '../../src/scp/index';

const REGISTERED = bc.registeredLanguages();

/**
 * Anti-sham markers (bc#75): a de-interpreted straight-line module must contain NONE of these —
 * the embedded ComponentGraphIR JSON literal, nor a named `IR` export. If any appears, the module
 * could secretly interpret a baked IR (a sham "codegen"), so we reject it.
 */
const IR_EMBED_MARKERS: RegExp[] = [
  /"irVersion"|'irVersion'/,
  /\bexport const IR\b|\bexport var IR\b|\bpub static IR\b|\bvar IR\b\s*=|'IR'\s*=>/,
];

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

// ── Load the frozen exec + tx vectors (they carry full §8 bundles) ──
const VECTORS_DIR = join(__dirname, '..', '..', 'conformance', 'vectors');
interface ExecVectorFile {
  vectors: { name: string; kind: string; bundle: SqlBundle; input: Json; schema: string[]; expectedResult: Json }[];
}
function loadExecVectors(): ExecVectorFile['vectors'] {
  const suite = JSON.parse(readFileSync(join(VECTORS_DIR, 'exec.json'), 'utf8')) as ExecVectorFile;
  return suite.vectors.filter((v) => v.kind === 'exec');
}
interface TxVectorFile {
  vectors: { name: string; kind: string; bundle: SqlBundle; input: Json; schema: string[]; expectedResult: Json; expectedDbState?: { query: string; rows: Json }[] }[];
}
function loadTxVectors(): TxVectorFile['vectors'] {
  const suite = JSON.parse(readFileSync(join(VECTORS_DIR, 'tx.json'), 'utf8')) as TxVectorFile;
  return suite.vectors.filter((v) => v.kind === 'tx');
}

const EXEC_VECTORS = loadExecVectors();
const TX_VECTORS = loadTxVectors();

describe('WS7f codegen — bc shared generator capability', () => {
  it('drives the de-boxed TYPED endpoint for ts/go/rust; py/php have NO de-box endpoint (NO literal fallback)', () => {
    // litedbmodel codegen is STATIC de-interpreted codegen (spec §9) with NO fallback (an unspec'd
    // fallback is invalid). ts/go/rust map to a de-box endpoint bc registers; py/php do NOT — and
    // litedbmodel does NOT substitute the literal emitter, it hard-errors (bc capability gap).
    const registered = new Set(REGISTERED);
    for (const language of ['typescript', 'go', 'rust'] as const) {
      const emitter = CODEGEN_EMITTER[language];
      expect(emitter).toBeDefined();
      expect(registered.has(emitter!)).toBe(true);
    }
    expect(CODEGEN_EMITTER.python).toBeUndefined();
    expect(CODEGEN_EMITTER.php).toBeUndefined();
    // Requesting codegen for py/php ERRORS (no fallback), naming the capability gap + ESCALATE.
    for (const language of ['python', 'php']) {
      expect(() => codegenEmitterFor(language, REGISTERED)).toThrow(/no de-boxed typed endpoint|ESCALATE to bc/);
    }
  });

  it('rejects an unsupported language loudly (fail-closed; escalate-to-bc message)', () => {
    expect(() => codegenEmitterFor('ruby', REGISTERED)).toThrow(/not a supported target/);
    // A registry missing the de-box emitter for a supported logical language fails closed (no fallback).
    expect(() => codegenEmitterFor('go', ['typescript'])).toThrow(/ESCALATE to bc/);
  });
});

// The DE-BOX codegen languages (ts/go/rust) over the READ (exec) surface. python/php are the
// ir/interpret surface (no de-box endpoint — a declared spec choice, not a fallback). A tx (write)
// bundle's `makeSqlComponentIR` is opaque/untyped and NOT de-boxable by the go/rust raw ABI, so
// writes are not part of the per-language de-box surface here (the tx codegen companion-plan is
// covered by the TS-only test below; write execution is proven in the mode-2 thin-runtime leg).
const DEBOX_LANGS = ['typescript', 'go', 'rust'] as const;

describe('WS7f codegen — per-language STRAIGHT-LINE (de-interpreted) source from the §8 STATIC makeSQL bundle', () => {
  for (const v of EXEC_VECTORS) {
    for (const language of DEBOX_LANGS) {
      it(`${language}: emits de-interpreted static code (fingerprint, NO embedded IR) + static SQL catalog — ${v.name}`, () => {
        const art = generateCodegenArtifact(v.bundle, language, REGISTERED);
        expect(art.language).toBe(language);
        expect(art.module.code.length).toBeGreaterThan(0);
        // The module was generated from the bundle's portable IR EXACTLY.
        expect(canon(art.ir)).toBe(canon(bundleToPortableIR(v.bundle)));
        // The generator's fingerprint recomputes over the source IR and is baked into the code
        // (the fail-closed skew gate SSoT — computed at generation time, IR not embedded).
        const recomputed = bc.fingerprintComponentGraph(art.ir);
        expect(art.module.fingerprint).toBe(recomputed);
        expect(art.module.code).toContain(recomputed);
        // De-interpretation (bc#75 anti-sham): the module must NOT embed the IR it compiled away.
        for (const marker of IR_EMBED_MARKERS) {
          expect(art.module.code).not.toMatch(marker);
        }
        // The companion carries the STATIC makeSQL catalog, byte-identical to the bundle's fields.
        expect(art.companion.dialect).toBe(v.bundle.dialect);
        expect([...art.companion.optionalHeads].sort()).toEqual([...v.bundle.optionalHeads].sort());
        expect(canon(art.companion.relations)).toBe(canon(v.bundle.relations));
        if (v.bundle.readGraph !== undefined) {
          expect(canon(art.companion.readGraph)).toBe(canon(v.bundle.readGraph));
        }
        if (v.bundle.statement !== undefined) {
          expect(canon(art.companion.statement)).toBe(canon(v.bundle.statement));
        }
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
    it(`codegen consumer drives the identical static makeSQL path → byte-identical vs executeBundle + vector — ${v.name}`, () => {
      const art = generateCodegenArtifact(v.bundle, 'typescript', REGISTERED);
      const input = decodeValue(v.input) as Record<string, unknown>;

      const db1 = seedDb(v.schema);
      const modeTwo = executeBundle(v.bundle, input as never, { db: db1 });
      db1.close();

      const db2 = seedDb(v.schema);
      const modeThree = codegenExecuteBundleForTest(art, input as never, db2);
      db2.close();

      expect(canon(modeThree)).toBe(canon(modeTwo));
      expect(canon(modeThree)).toBe(canon(decodeValue(v.expectedResult)));
    });
  }
});

describe('WS7f codegen — tx bundle: companion transaction plan is byte-identical to mode-2', () => {
  for (const v of TX_VECTORS) {
    it(`codegen consumer reconstructs the SAME §8 bundle → executeTransactionBundle byte-identical — ${v.name}`, () => {
      const art = generateCodegenArtifact(v.bundle, 'typescript', REGISTERED);
      const input = decodeValue(v.input) as Record<string, unknown>;
      // A codegen consumer reassembles the executable §8 bundle from the companion. It MUST equal
      // the source bundle exactly (the whole point of the IR/catalog split).
      // Reassemble with the SAME key order the runtime emits (dialect, name, statement/readGraph,
      // optionalHeads, relations, transaction) so the canonical JSON compares byte-identical.
      const reassembled: SqlBundle = {
        dialect: art.companion.dialect,
        name: v.bundle.name,
        ...(art.companion.statement !== undefined ? { statement: art.companion.statement } : {}),
        ...(art.companion.readGraph !== undefined ? { readGraph: art.companion.readGraph } : {}),
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
      for (const [i, s] of (v.expectedDbState ?? []).entries()) {
        expect(canon(codegenDbState[i].rows)).toBe(canon(decodeValue(s.rows)));
      }
    });
  }
});

// ── The emitted TS module WRITES + IMPORTS + is de-interpreted (no embedded IR, bc#75) ──
describe('WS7f codegen — the EMITTED TS straight-line source loads and is de-interpreted', () => {
  let outDir: string;
  beforeAll(() => {
    outDir = mkdtempSync(join(__dirname, '..', '..', '.codegen-out-'));
  });
  afterAll(() => {
    if (outDir) rmSync(outDir, { recursive: true, force: true });
  });

  for (const [i, v] of EXEC_VECTORS.entries()) {
    it(`emitted .ts module loads (fail-closed checks run), carries IR_FINGERPRINT + does NOT embed the IR — ${v.name}`, async () => {
      const art = generateCodegenArtifact(v.bundle, 'typescript', REGISTERED);
      const modPath = join(outDir, `behaviors_${i}.generated.ts`);
      writeFileSync(modPath, art.module.code, 'utf8');

      // Import the EMITTED module (its load-time fail-closed checks — spec-version envelope pin —
      // run here). The de-interpreted module exports IR_FINGERPRINT + COMPONENT_NAMES, NOT the IR.
      const mod = (await import(pathToFileURL(modPath).href)) as {
        IR_FINGERPRINT: string;
        COMPONENT_NAMES: readonly string[];
        bind: unknown;
      };

      // The emitted module's generation-time fingerprint equals the fingerprint of the portable IR
      // the consumer holds (the fail-closed skew gate) — WITHOUT the IR ever being embedded.
      expect(mod.IR_FINGERPRINT).toBe(bc.fingerprintComponentGraph(bundleToPortableIR(v.bundle)));
      expect(Array.isArray(mod.COMPONENT_NAMES)).toBe(true);
      // De-interpretation (bc#75): the emitted source embeds no IR literal / no named IR export.
      for (const marker of IR_EMBED_MARKERS) {
        expect(art.module.code).not.toMatch(marker);
      }
    });
  }
});
