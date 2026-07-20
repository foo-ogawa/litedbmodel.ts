/**
 * litedbmodel v2 SCP — mode-3 codegen conformance (WS7f, #35; makeSQL flip, epic #43/#45;
 * #60 milestone 1 — typed-NATIVE READ codegen, bc#77/#90).
 *
 * Proves the AC "生成コード出力が thin-runtime と byte 一致" for the READ codegen surface:
 *
 *  - **go/rust** drive bc's typed-NATIVE endpoint (bc#77/#90, RUNTIME-FREE): litedbmodel's
 *    codegen-only lowering ({@link lowerReadGraphForTypedNative}) rebuilds each real Select-node's
 *    ports from its compiled `statementsById` genuine bound heads into individual scalar ref ports
 *    (#12: no surrogate `__scope` obj anymore) and types the component's
 *    `inputPorts` from the schema (spec §4.1), so a COVERED read shape produces a module with
 *    ZERO boxing markers (`obj_native`/`ser_T*`/`run_plan`/`RawValue`) and NO embedded IR. An
 *    IN-list / array-bound WHERE head is now COVERED via bc#110 (native array/list port → a
 *    `Vec<ElemT>`/`[]ElemT` input port, no serde_json/encoding-json boxing). A relation expressed via
 *    a `.map` node with per-element field-access ports (`{ref:['$e0','author_id']}`) is now COVERED via
 *    bc 0.7.3's map/fanout support + the lowering's map element-FIELD ports + LIMIT-clause head typing.
 *    A genuinely uncovered shape still THROWS `TypedNativeCoverageError` — reported explicitly, never
 *    silently regenerated on a boxed fallback.
 *  - **typescript** stays on the boxed `typescript-typed` endpoint (bc has not registered a
 *    `typescript-typed-native` endpoint yet), but is now fed the SAME genuine-bound-head lowering
 *    ({@link lowerReadGraphForTypedNative}) go/rust consume (#12 regression fix): post-#12 the real
 *    Select-node read graph carries a fragment for every authored `whereX($.col, …)`, including a
 *    WHERE COLUMN-NAME MARKER head (`whereGe($.created_at, $.since)` — `$.created_at` names the
 *    column, is never a bound value). Handing that raw IR to the emitter emitted a stray
 *    `created_at` input binding → `unknown binding: created_at` at execution. The lowering rebuilds
 *    each node's ports from the GENUINE bound heads, dropping the marker, so the emitted TS module
 *    runs (the array-typed heads lower cleanly via bc#110).
 *  - **In-process byte-identity**: `codegenExecuteBundleForTest` (a codegen consumer reading the
 *    companion) drives the IDENTICAL static-makeSQL render/execute path `executeBundle` uses — its
 *    output equals mode-2 `executeBundle` AND the frozen vector's `expectedResult`, EXACTLY.
 *  - **WRITE bundles are NOT codegen-module cases** (#60 m1): `generateCodegenArtifact` throws if
 *    given a bundle with no `readGraph` — writes stay on the existing write/tx execution path
 *    (`executeTransactionBundle`).
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
  generateRustExecutable,
  lowerReadGraphForNativeSql,
  codegenExecuteBundleForTest,
  CODEGEN_EMITTER,
  codegenEmitterFor,
  TypedNativeCoverageError,
  schemaColumnTypeResolver,
  compileBundle,
  publishBehaviors,
  SemanticBehavior,
  components,
  whereEq,
  whereIn,
  inColumn,
  type SqlBundle,
  type ColumnTypeResolver,
} from '../../src/scp/index';

const REGISTERED = bc.registeredLanguages();

/**
 * Anti-sham markers (bc#75/#90): a de-interpreted module must contain NONE of these — the
 * embedded ComponentGraphIR JSON literal, nor a named `IR` export, nor an interpreter CALL
 * (checked against comment-stripped source — bc's straight-line modules' explanatory COMMENTS
 * legitimately mention "no runBehavior tree-walk" prose, which must not false-positive here).
 */
const IR_EMBED_MARKERS: RegExp[] = [
  /"irVersion"|'irVersion'/,
  /\bexport const IR\b|\bexport var IR\b|\bpub static IR\b|\bvar IR\b\s*=|'IR'\s*=>/,
];
// A genuine interpreter delegation is a CALL — `run_behavior(`/`RunBehavior(`/`runBehavior(` with an
// immediate open paren. Explanatory prose ("byte-equal to run_behavior", "converge with run_behavior
// (fixes …)") names the interpreter without invoking it and must NOT false-positive.
const INTERPRETER_CALL_MARKER = /\brun_behavior\(|\bRunBehavior\(|\brunBehavior\(/;

/** Strip line/block comments while preserving string/template literals (mirrors
 * `conformance/codegen/codegen-runner.ts`'s `stripComments`), so a marker matches genuine
 * code/data — explanatory prose in a comment never false-positives. */
function stripComments(src: string): string {
  let out = '';
  let i = 0;
  const n = src.length;
  while (i < n) {
    const c = src[i];
    if (c === '"' || c === "'" || c === '`') {
      out += c;
      i++;
      while (i < n && src[i] !== c) {
        if (src[i] === '\\') {
          out += src[i];
          i++;
        }
        if (i < n) {
          out += src[i];
          i++;
        }
      }
      if (i < n) {
        out += src[i];
        i++;
      }
      continue;
    }
    if (c === '/' && src[i + 1] === '/') {
      while (i < n && src[i] !== '\n') i++;
      continue;
    }
    if (c === '/' && src[i + 1] === '*') {
      i += 2;
      while (i < n && !(src[i] === '*' && src[i + 1] === '/')) i++;
      i += 2;
      continue;
    }
    out += c;
    i++;
  }
  return out;
}

/** typed-native purity markers (#60 m1 owner order): a COVERED go/rust read carries ZERO of
 * these — the whole point of the migration off `-typed-raw`/`-typed` is zero-boxing. Real boxing
 * crosses the boxed `Value`/`RawValue` ENUM (`Value::`/`RawValue::`); the bc#146 structured-error
 * plane's `RawValue`/`raw_value` struct FIELD of `ErrorDetail` (a diagnostic string, not a hot-path
 * value box) is NOT boxing and must not false-positive — so match the enum crossing, not the word. */
const NATIVE_BOXING_MARKERS: RegExp[] = [/\bobj_native\b/, /\bser_T\d/, /\brun_plan\b/, /\bRawValue::/, /\bValue::/];

/** Assert a generated module embeds no IR literal/export and calls no interpreter (comment-stripped). */
function assertDeInterpreted(code: string): void {
  for (const marker of IR_EMBED_MARKERS) expect(code).not.toMatch(marker);
  expect(stripComments(code)).not.toMatch(INTERPRETER_CALL_MARKER);
}

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

describe('WS7f codegen — bc READ emitter capability (#60 m1: typed-native go/rust, typed ts)', () => {
  it('drives typed-NATIVE for go/rust, typed(boxed) for ts; py/php have NO endpoint (NO literal fallback)', () => {
    const registered = new Set(REGISTERED);
    expect(CODEGEN_EMITTER.go).toBe('go-typed-native');
    expect(CODEGEN_EMITTER.rust).toBe('rust-typed-native');
    expect(CODEGEN_EMITTER.typescript).toBe('typescript-typed');
    for (const language of ['typescript', 'go', 'rust'] as const) {
      const emitter = CODEGEN_EMITTER[language];
      expect(emitter).toBeDefined();
      expect(registered.has(emitter!)).toBe(true);
    }
    expect(CODEGEN_EMITTER.python).toBeUndefined();
    expect(CODEGEN_EMITTER.php).toBeUndefined();
    // Requesting codegen for py/php ERRORS (no fallback), naming the capability gap + ESCALATE.
    for (const language of ['python', 'php']) {
      expect(() => codegenEmitterFor(language, REGISTERED)).toThrow(/no READ codegen endpoint|ESCALATE to bc/);
    }
  });

  it('rejects an unsupported language loudly (fail-closed; escalate-to-bc message)', () => {
    expect(() => codegenEmitterFor('ruby', REGISTERED)).toThrow(/not a supported target/);
    // A registry missing the endpoint for a supported logical language fails closed (no fallback).
    expect(() => codegenEmitterFor('go', ['typescript'])).toThrow(/ESCALATE to bc/);
  });

  it('generateCodegenArtifact refuses a bundle carrying NEITHER a graph nor a statement (nothing to generate)', () => {
    // Read/write are ONE flow now (owner): a single-write bundle carries a component graph (compileBundle
    // keeps it alongside the statement) and DOES codegen — see e1-native-sql-port.test.ts. Only a bundle
    // with no graph at all (e.g. a bare tx/DAG fixture) is refused.
    expect(() => generateCodegenArtifact({ dialect: 'sqlite', name: 'Create', optionalHeads: [], relations: {} } as SqlBundle, 'typescript', REGISTERED, () => 'INTEGER')).toThrow(
      /carries no component graph/,
    );
  });
});

// ts is exec-checked directly (boxed endpoint, unaffected by the typed-native lowering — it
// covers every read shape below). go/rust are checked via `structuralResult`, which distinguishes
// a genuine failure from an EXPECTED typed-native coverage gap (never silently swallowed).
const NATIVE_LANGS = ['go', 'rust'] as const;

type StructuralResult = { kind: 'ok'; module: bc.GeneratedModule } | { kind: 'uncovered'; error: InstanceType<typeof TypedNativeCoverageError> };

function structuralResult(bundle: SqlBundle, language: string, resolveColumnType: ColumnTypeResolver): StructuralResult {
  try {
    const art = generateCodegenArtifact(bundle, language, REGISTERED, resolveColumnType);
    return { kind: 'ok', module: art.module };
  } catch (e) {
    if (e instanceof TypedNativeCoverageError) return { kind: 'uncovered', error: e };
    throw e;
  }
}

describe('WS7f codegen — the FROZEN exec.json vector: ts (boxed) AND go/rust (typed-native) all cover it (bc 0.7.3 map)', () => {
  for (const v of EXEC_VECTORS) {
    const resolveColumnType = schemaColumnTypeResolver(v.schema);

    it(`typescript: emits de-interpreted code (NO embedded IR) + byte-identical companion — ${v.name}`, () => {
      const art = generateCodegenArtifact(v.bundle, 'typescript', REGISTERED, resolveColumnType);
      expect(art.language).toBe('typescript');
      expect(art.module.code.length).toBeGreaterThan(0);
      assertDeInterpreted(art.module.code);
      expect(art.companion.dialect).toBe(v.bundle.dialect);
      expect([...art.companion.optionalHeads].sort()).toEqual([...v.bundle.optionalHeads].sort());
      expect(canon(art.companion.relations)).toBe(canon(v.bundle.relations));
      // The read SQL is now BAKED into the module — the companion is retired for reads (carries no
      // readGraph). The module holds the query text; the companion is the runtime-stitch sidecar only.
      expect((art.companion as { readGraph?: unknown }).readGraph).toBeUndefined();
      expect(art.module.code).toContain('SELECT');
    });

    for (const language of NATIVE_LANGS) {
      // bc 0.7.3 + the litedbmodel codegen lowering (LIMIT-clause head typing + map-element FIELD
      // ports, `{ref:['$e0','author_id']}`) now cover this vector's `.map`-relation shape as a
      // RUNTIME-FREE typed-native module (the prior bc#86 gap is closed). The read primary (n0) + the
      // per-element map child (n1, `authors`) both lower to concrete `HandlerNR<Feed>` node methods
      // returning typed rows — zero boxed Value, no interpreter delegation.
      it(`${language}: the '.map'-relation shape IS typed-native-covered (bc 0.7.3 map) — zero-boxing, de-interpreted — ${v.name}`, () => {
        const r = structuralResult(v.bundle, language, resolveColumnType);
        expect(r.kind).toBe('ok');
        if (r.kind === 'ok') {
          assertDeInterpreted(r.module.code);
          const stripped = stripComments(r.module.code);
          for (const marker of NATIVE_BOXING_MARKERS) expect(stripped).not.toMatch(marker);
        }
      });
    }
  }
});

describe('WS7f codegen — in-process equivalence of the TS codegen artifact vs mode-2 thin-runtime', () => {
  for (const v of EXEC_VECTORS) {
    it(`codegen consumer drives the identical static makeSQL path → byte-identical vs executeBundle + vector — ${v.name}`, () => {
      const resolveColumnType = schemaColumnTypeResolver(v.schema);
      const art = generateCodegenArtifact(v.bundle, 'typescript', REGISTERED, resolveColumnType);
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

describe('WS7f codegen — GATED tx-DAG bundles stay on the interpreter tx path (gate coverage gap)', () => {
  for (const v of TX_VECTORS) {
    it(`generateCodegenArtifact refuses this GATED tx bundle; executeTransactionBundle reproduces the vector — ${v.name}`, () => {
      // A SINGLE write and a gate-free RETURNING-chained transaction are now codegen cases (E5/#120:
      // `lowerTransactionForNativeChain` bakes the chain — see e1-native-sql-port.test.ts). These
      // vectors are GATED multi-write DAGs (requires/unique/idempotency/derive/emit) — a gate statement
      // short-circuits with a {committed:false, shortCircuit} result the native struct chain does not
      // model, so the lowering fails closed (naming the gate) and execution stays on the interpreter tx.
      expect(() => generateCodegenArtifact(v.bundle, 'typescript', REGISTERED, () => 'INTEGER')).toThrow(/gate\/non-body statement/);

      const input = decodeValue(v.input) as Record<string, unknown>;
      const db = seedDb(v.schema);
      const result = executeTransactionBundle(v.bundle, input as never, { db });
      db.close();

      expect(canon(result)).toBe(canon(decodeValue(v.expectedResult)));
    });
  }
});

// ── The emitted TS module WRITES + IMPORTS + is de-interpreted (no embedded IR, bc#75) ──
describe('WS7f codegen — the EMITTED TS source loads and is de-interpreted', () => {
  let outDir: string;
  beforeAll(() => {
    outDir = mkdtempSync(join(__dirname, '..', '..', '.codegen-out-'));
  });
  afterAll(() => {
    if (outDir) rmSync(outDir, { recursive: true, force: true });
  });

  for (const [i, v] of EXEC_VECTORS.entries()) {
    it(`emitted .ts module loads (fail-closed spec-version checks run) + does NOT embed the IR — ${v.name}`, async () => {
      const resolveColumnType = schemaColumnTypeResolver(v.schema);
      const art = generateCodegenArtifact(v.bundle, 'typescript', REGISTERED, resolveColumnType);
      const modPath = join(outDir, `behaviors_${i}.generated.ts`);
      writeFileSync(modPath, art.module.code, 'utf8');

      // Import the EMITTED module (its load-time fail-closed checks — spec-version envelope pin —
      // run here). bc 0.5.0 does NOT embed a fingerprint in the module (GenerateResult.fingerprint
      // is a build-time-only return, compared on the consumer/build side) — assert THAT instead.
      const mod = (await import(pathToFileURL(modPath).href)) as {
        COMPONENT_NAMES: readonly string[];
        bind: unknown;
      };
      expect(Array.isArray(mod.COMPONENT_NAMES)).toBe(true);
      // #12 regression fix: the TS `typescript-typed` codegen input is now the SAME genuine-bound-head
      // lowering go/rust consume ({@link lowerReadGraphForTypedNative}) — dropping the WHERE-fragment
      // column-name marker heads that broke the emitted module (`unknown binding: created_at`). The
      // module fingerprint therefore covers the LOWERED IR, not the raw portable IR, so assert against
      // that (the SAME input generateCodegenArtifact feeds the emitter).
      // bc 0.8.0 (SA3/SA7): the derived codegen IR is un-tokened, so `fingerprintComponentGraph`
      // (provenance-gated) requires re-adopting it via `loadCompiledIR` first — the SAME seam
      // `generateCodegenArtifact` uses before `generateModule`. The token is invisible to the
      // fingerprint (non-enumerable symbol), so the value equals the artifact's fingerprint.
      expect(art.module.fingerprint).toBe(
        bc.fingerprintComponentGraph(bc.loadCompiledIR(lowerReadGraphForNativeSql(v.bundle.readGraph!, resolveColumnType))),
      );
      assertDeInterpreted(art.module.code);
    });
  }
});

// ── A COVERED typed-native shape (#60 m1 positive path): a plain single-componentRef Select with
// only scalar WHERE heads. The frozen exec.json corpus's `.map`-relation vector is ALSO covered now
// (bc 0.7.3 map + lowering); this block additionally proves the plain single-node positive path. ──
describe('WS7f codegen — a COVERED go/rust typed-native read: zero-boxing + byte-identity', () => {
  const SCHEMA = [`CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT)`];
  const resolveColumnType = schemaColumnTypeResolver(SCHEMA);
  const L = components();
  class Reads extends SemanticBehavior {
    static columns = { posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT', status: 'TEXT' } };
    Find($: any) {
      return L.Select({
        table: 'posts',
        select: ['id', 'author_id', 'title', 'status'],
        where: [whereEq($.author_id, $.author_id)],
        order: 'id ASC',
      });
    }
  }
  const contract = publishBehaviors(Reads);
  const bundle = compileBundle(contract, 'Find', [], 'sqlite', undefined, resolveColumnType);

  for (const language of NATIVE_LANGS) {
    it(`${language}: covered shape produces a ZERO-boxing typed-native module (no obj_native/ser_T*/run_plan/RawValue)`, () => {
      const art = generateCodegenArtifact(bundle, language, REGISTERED, resolveColumnType);
      expect(art.module.code.length).toBeGreaterThan(0);
      assertDeInterpreted(art.module.code);
      const stripped = stripComments(art.module.code);
      for (const marker of NATIVE_BOXING_MARKERS) expect(stripped).not.toMatch(marker);
    });
  }

  it('typescript: same covered shape still generates on the boxed endpoint (unaffected by the lowering)', () => {
    const art = generateCodegenArtifact(bundle, 'typescript', REGISTERED, resolveColumnType);
    expect(art.module.code.length).toBeGreaterThan(0);
  });

  it('rust companion: emits the wire_impls! bridge + a HandlerNR impl delegating to the runtime executor (no rusqlite, no concrete Driver)', () => {
    const companion = generateRustExecutable(bundle, 'generated_find', resolveColumnType, REGISTERED);
    // the orphan-rule wire bridge + the runtime import + the module glob
    expect(companion).toContain('litedbmodel static runtime adapter for `generated_find`');
    expect(companion).toContain('litedbmodel_runtime::wire_impls!();');
    // a per-component HandlerNR impl whose node_* resolves the ctx from the ConnSource (#135 — the
    // reader/writer routing seam) then delegates UNIFORMLY to the ONE runtime executor.
    expect(companion).toContain('impl<\'a> HandlerNRFind for Rt<\'a>');
    expect(companion).toContain('src: litedbmodel_runtime::ConnSource<\'a>');
    expect(companion).toMatch(/let ctx = self\.src\.ctx\(\)\.map_err\(cvt\)\?;/);
    expect(companion).toMatch(/litedbmodel_runtime::exec\(&ctx,/);
    // the litedbmodel-consumer entry points: a SINGLE-driver handler (byte-identical single-pool) and
    // the ROUTED handler (read→reader / write→writer) — both supply no node_*.
    expect(companion).toContain('pub fn run(driver: &dyn Driver');
    expect(companion).toContain('pub fn handler_routed(routing: &litedbmodel_runtime::RoutingConfig) -> Rt<');
    // it is a Driver-backed delegation — NO rusqlite, NO concrete driver type, NO hand-written exec
    for (const banned of ['rusqlite', 'SqliteDriver', 'MysqlDriver', 'PostgresDriver', '.prepare(']) {
      expect(companion).not.toContain(banned);
    }
  });

  it('an IN-list read (array-typed head) IS typed-native-covered for go/rust via bc#110 (native array port; no serde_json/encoding-json, no boxing)', () => {
    class InListReads extends SemanticBehavior {
      static columns = { posts: { id: 'INTEGER', title: 'TEXT' } };
      ByIds($: any) {
        return L.Select({
          table: 'posts',
          select: ['id', 'title'],
          where: [whereIn(inColumn($, 'id'), $.ids)],
          order: 'id ASC',
        });
      }
    }
    const inListContract = publishBehaviors(InListReads);
    const inListBundle = compileBundle(inListContract, 'ByIds', [], 'sqlite', undefined, resolveColumnType);
    // The native array-bind mechanism must be a native Vec<ElemT>/[]ElemT port — NEVER a
    // serde_json/encoding-json marshal of the array on the hot path (that is the boxing this closes).
    const NATIVE_ARRAY_PORT: Record<string, RegExp> = { go: /\[\]int64\b/, rust: /Vec<i64>/ };
    const JSON_HOTPATH_MARKERS: RegExp[] = [/serde_json/, /encoding\/json/, /json\.Marshal/, /json\.Unmarshal/];
    for (const language of NATIVE_LANGS) {
      const art = generateCodegenArtifact(inListBundle, language, REGISTERED, resolveColumnType);
      expect(art.module.code.length).toBeGreaterThan(0);
      assertDeInterpreted(art.module.code);
      const stripped = stripComments(art.module.code);
      // Zero boxing on the hot path (same gate as the covered scalar reads).
      for (const marker of NATIVE_BOXING_MARKERS) expect(stripped).not.toMatch(marker);
      // The IN-list array head lowers to a CONCRETE native array port fed natively (bc#110)…
      expect(art.module.code).toMatch(NATIVE_ARRAY_PORT[language]);
      // …and NO JSON-marshal of the array appears on the generated read hot path (proves the port
      // feeds the driver's native array bind, not a serde_json/json.Marshal boxing).
      for (const marker of JSON_HOTPATH_MARKERS) expect(art.module.code).not.toMatch(marker);
    }
    // ts (boxed) still covers it too.
    const art = generateCodegenArtifact(inListBundle, 'typescript', REGISTERED, resolveColumnType);
    expect(art.module.code.length).toBeGreaterThan(0);
  });
});
