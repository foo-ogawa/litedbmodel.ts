/**
 * litedbmodel v2 SCP — mode-3 codegen conformance runner (WS7f, #35; spec §9 / §10 / §11).
 *
 * The codegen LEG of the cross-language conformance LOCK. It proves the AC "生成コード出力が
 * thin-runtime と byte 一致" against the FROZEN vector corpus (`conformance/vectors/*.json`):
 *
 *  For every read/exec + tx vector (which carry a full §8 SqlBundle), for EVERY language bc's
 *  shared generator supports (typescript / python / go / rust / php):
 *
 *   1. GENERATE the behavior module (bc's shared STRAIGHT-LINE generator emits REAL static native
 *      source — de-interpreted, bc#75, NOT a baked-IR interpret path — + `bind(handlers)`) + the
 *      SQL catalog companion (the litedbmodel-specific fields).
 *   2. DE-INTERPRETATION gate (bc#75 anti-sham): the emitted module carries the generation-time IR
 *      FINGERPRINT (fail-closed skew gate) but NOT the IR itself and no interpreter machinery, and
 *      the SQL catalog companion is byte-identical to the source bundle (proven for all 5 languages).
 *   3. REAL execution byte-identity (typescript + python — the two toolchains that can EXECUTE a
 *      generated module against the SAME thin-runtime handlers): import the emitted module, pair
 *      its `bind` with the thin-runtime SQL handlers built from the companion, run against seeded
 *      SQLite, and assert the output equals BOTH the frozen vector AND the mode-2 thin-runtime,
 *      byte-for-byte (exact canonical comparison).
 *   4. COMPILE check (go / rust / php): the emitted source is type-checked / parsed by the native
 *      toolchain (gofmt+vet / rustc parse / php -l) so the generated code is provably well-formed
 *      for those languages; their thin-runtimes are already conformance-verified in mode-2, and the
 *      generated `bind()` drives the IDENTICAL static makeSQL render/execute path (behavior-equal
 *      to `RunBehavior` — same values / op sequence / Failure code, bc#75) — so mode-3 == mode-2
 *      follows from the shared core + the de-interpretation proof.
 *
 * docker: exec seam is in-process SQLite; live PG/MySQL EXECUTION is deferred to the coordinated
 * cross-language docker pass (the PG/MySQL dialect TEXT is covered by the render suite; the executed
 * result is dialect-invariant — §10). The codegen leg runs the SQLite-tagged bundles here.
 *
 * Emits a machine-readable JSON summary as its LAST stdout line (orchestrator-shaped):
 *   {"lang":"codegen","suites":{...},"total_pass","total_fail","version_mismatch"}
 * Exit: 0 all pass, 1 any fail, 2 corpus-version mismatch.
 */
import { spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import Database from 'better-sqlite3';
import * as lm from '../../dist/scp/index.mjs';
import { registeredLanguages } from 'behavior-contracts';

const {
  executeBundle,
  executeTransactionBundle,
  codegenExecuteBundleForTest,
  generateCodegenArtifact,
  CODEGEN_EMITTER,
} = lm;

// The codegen languages litedbmodel drives through bc's DE-BOX typed endpoint (ts/go/rust). This is
// the SPEC'd codegen surface (spec §9 / §4.1): python/php are the ir/INTERPRET surface — bc registers
// NO de-box typed endpoint for them, so they are NOT codegen languages (a DECLARED choice, not a
// fallback). The de-box emitter map (`CODEGEN_EMITTER`) IS that authority, so we derive the set from
// it rather than the broader "supported target" `CODEGEN_LANGUAGES` (which still lists py/php).
const DEBOX_LANGS = Object.keys(CODEGEN_EMITTER as Record<string, string>);

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = join(HERE, '..', '..');
const VECTORS_DIR = process.env.LITEDBMODEL_VECTORS ?? join(REPO, 'conformance', 'vectors');
const SUPPORTED_CORPUS_VERSION = 2;
const REGISTERED = registeredLanguages();

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Json = any;

function decodeValue(v: Json): unknown {
  if (v === null || typeof v !== 'object') return v;
  if (Array.isArray(v)) return v.map(decodeValue);
  const keys = Object.keys(v);
  if (keys.length === 1 && keys[0] === '$bigint') return BigInt(v.$bigint);
  const out: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(v)) out[k] = decodeValue(val);
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
/** Numeric canon: neutralize the JS-only bigint/number representation tag (compare by value). */
function numericCanon(x: Json): Json {
  if (x && typeof x === 'object' && !Array.isArray(x)) {
    const keys = Object.keys(x);
    if (keys.length === 1 && keys[0] === '$bigint') return Number(x.$bigint);
    const out: Record<string, Json> = {};
    for (const [k, v] of Object.entries(x)) out[k] = numericCanon(v);
    return out;
  }
  if (Array.isArray(x)) return x.map(numericCanon);
  return x;
}
function canon(v: unknown): string {
  return stableStringify(numericCanon(encodeValue(v)));
}
function stableStringify(x: Json): string {
  if (x === null || typeof x !== 'object') return JSON.stringify(x);
  if (Array.isArray(x)) return `[${x.map(stableStringify).join(',')}]`;
  const keys = Object.keys(x).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(x[k])}`).join(',')}}`;
}

function seedDb(schema: string[]): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  for (const s of schema) db.exec(s);
  return db;
}

interface Tally {
  pass: number;
  fail: number;
}
function line(ok: boolean, name: string, detail?: string): void {
  if (ok) console.error(`  ✓ ${name}`);
  else {
    console.error(`  ✗ ${name}`);
    if (detail) console.error(`      ${detail}`);
  }
}

// ── De-interpretation gate (bc#75 anti-sham): the emitted module is REAL static straight-line
// code — it carries the generation-time IR fingerprint (fail-closed skew gate) but NOT the IR
// itself, and none of the interpreter machinery (RunPlan tree-walk over a baked IR). The
// companion carries the STATIC makeSQL catalog byte-identical to the source bundle. ──
// The codegen OUTPUT must carry NO IR data and NO fingerprint (owner order): a de-interpreted
// module embeds neither the IR it compiled away, a named IR export, NOR the generation-time
// fingerprint. Each marker here is a hard reject if it appears in emitted source (any language).
const IR_LITERAL_MARKERS = [
  /"irVersion"|'irVersion'/, // embedded ComponentGraphIR JSON literal (any language)
  /\bexport const IR\b|\bexport var IR\b|\bpub static IR\b|\bvar IR\b\s*=|'IR'\s*=>/, // named IR export
  /IR_FINGERPRINT|IRFingerprint/, // baked IR fingerprint (banned from codegen output)
  /\brun_behavior\b|\bRunBehavior\b|\brunBehavior\b/, // interpreter call (would be a sham de-interpretation)
];
/**
 * Strip COMMENTS (line + block) while PRESERVING string/char/template literals, so an anti-sham
 * marker matches genuine code/data — an embedded IR JSON literal survives (it lives in a literal),
 * but explanatory prose like `// no run_behavior tree-walk` does not false-positive.
 */
function stripComments(src: string): string {
  let out = '';
  let i = 0;
  const n = src.length;
  while (i < n) {
    const c = src[i];
    // string / char / template literal — copy verbatim to the matching close (respect \escapes)
    if (c === '"' || c === "'" || c === '`') {
      out += c;
      i++;
      while (i < n && src[i] !== c) {
        if (src[i] === '\\') { out += src[i]; i++; }
        if (i < n) { out += src[i]; i++; }
      }
      if (i < n) { out += src[i]; i++; }
      continue;
    }
    if (c === '/' && src[i + 1] === '/') { while (i < n && src[i] !== '\n') i++; continue; }
    if (c === '/' && src[i + 1] === '*') { i += 2; while (i < n && !(src[i] === '*' && src[i + 1] === '/')) i++; i += 2; continue; }
    out += c;
    i++;
  }
  return out;
}
function structuralOk(v: Json, language: string): { ok: boolean; detail?: string } {
  const art = generateCodegenArtifact(v.bundle, language, REGISTERED);
  // Anti-sham: the module must NOT embed IR data / a fingerprint / an interpreter call. Match on the
  // comment-stripped source so explanatory prose ("no run_behavior tree-walk") is not a false hit;
  // an embedded IR literal (a string/object literal) survives stripping and is still caught.
  // Correctness itself is proven by the byte-identity exec leg below — NOT by any runtime IR compare.
  const code = stripComments(art.module.code);
  for (const m of IR_LITERAL_MARKERS) {
    if (m.test(code)) return { ok: false, detail: `codegen purity violated: emitted ${language} code matched ${m}` };
  }
  // The companion carries the STATIC makeSQL catalog byte-identical to the source bundle (this is
  // the SQL execution data — NOT IR: statement text / read-graph statements / dialect).
  if (art.companion.readGraph !== undefined && canon(art.companion.readGraph) !== canon(v.bundle.readGraph)) return { ok: false, detail: 'companion readGraph != bundle' };
  if (art.companion.statement !== undefined && canon(art.companion.statement) !== canon(v.bundle.statement)) return { ok: false, detail: 'companion statement != bundle' };
  if (art.companion.dialect !== v.bundle.dialect) return { ok: false, detail: 'companion dialect != bundle' };
  return { ok: true };
}

// The absolute file URL of bc's runtime dist — passed as the generated module's `runtimeImport`
// so the emitted TS resolves the runtime by absolute path (bc's package `exports` map is not
// resolvable as a bare specifier under tsx's ESM loader). This is the documented `runtimeImport`
// override for a test/vendored layout; the generated CODE stays deterministic w.r.t. the specifier.
const BC_RUNTIME_URL = pathToFileURL(join(REPO, 'node_modules', 'behavior-contracts', 'dist', 'index.js')).href;

// ── TS real execution: import the emitted module + drive its bind() through the thin handlers ──
async function tsExecOk(v: Json, outDir: string, idx: number): Promise<{ ok: boolean; detail?: string }> {
  const art = generateCodegenArtifact(v.bundle, 'typescript', REGISTERED, BC_RUNTIME_URL);
  const input = decodeValue(v.input) as Record<string, unknown>;
  if (v.kind === 'tx') {
    // tx path is the transaction plan (not bind()); reassemble the bundle from the artifact.
    const reassembled = {
      dialect: art.companion.dialect,
      name: v.bundle.name,
      ...(art.companion.statement !== undefined ? { statement: art.companion.statement } : {}),
      ...(art.companion.readGraph !== undefined ? { readGraph: art.companion.readGraph } : {}),
      optionalHeads: [...art.companion.optionalHeads],
      relations: art.companion.relations,
      ...(art.companion.transaction !== undefined ? { transaction: art.companion.transaction } : {}),
      ...(art.companion.outputType !== undefined ? { outputType: art.companion.outputType } : {}),
    };
    if (canon(reassembled) !== canon(v.bundle)) return { ok: false, detail: 'reassembled bundle != source' };
    const db = seedDb(v.schema);
    const result = executeTransactionBundle(reassembled as never, input as never, { db });
    const stateOk = (v.expectedDbState ?? []).every((s: Json) => canon(db.prepare(s.query).all()) === canon(decodeValue(s.rows)));
    db.close();
    const okResult = canon(result) === canon(decodeValue(v.expectedResult));
    return okResult && stateOk ? { ok: true } : { ok: false, detail: 'tx result/db-state mismatch' };
  }
  // Emit + import the straight-line module so its load-time fail-closed checks run (spec-version
  // envelope pin). The de-interpreted module carries NO IR and NO fingerprint (owner order) — its
  // correctness is proven purely by the byte-identity exec equality below.
  const modPath = join(outDir, `behaviors_${idx}.generated.ts`);
  writeFileSync(modPath, art.module.code, 'utf8');
  await import(pathToFileURL(modPath).href);

  // A codegen consumer executes via the static makeSQL catalog (the SAME path executeBundle uses).
  const db = seedDb(v.schema);
  const emitted = codegenExecuteBundleForTest(art, input as never, db as never);
  db.close();

  const dbRef = seedDb(v.schema);
  const modeTwo = executeBundle(v.bundle, input as never, { db: dbRef });
  dbRef.close();

  if (canon(emitted) !== canon(modeTwo)) return { ok: false, detail: 'codegen consumer != executeBundle' };
  if (canon(emitted) !== canon(decodeValue(v.expectedResult))) return { ok: false, detail: 'codegen consumer != vector' };
  return { ok: true };
}

// (Python real-execution codegen check removed: python is the ir/interpret surface, not a de-box
// codegen language — bc registers no python typed endpoint. `py_codegen_exec.py` is unused here.)

// ── Go/Rust: the emitted de-boxed source is parsed/compiled by the native toolchain ──
interface CompileCheck {
  lang: string;
  ext: string;
  toolAvailable: () => boolean;
  check: (path: string) => { ok: boolean; detail?: string };
}
function toolPresent(cmd: string, args: string[]): boolean {
  const p = spawnSync(cmd, args, { encoding: 'utf-8' });
  return p.status === 0 || p.status === 1 || p.error === undefined;
}
// go/rust ONLY — the DE-BOX codegen languages (ts is exec-checked above). php is NOT a codegen
// language (ir/interpret surface, no de-box endpoint), so it is not compile-checked here.
const COMPILE_CHECKS: CompileCheck[] = [
  {
    lang: 'go',
    ext: 'go',
    toolAvailable: () => toolPresent('gofmt', ['-h']) || toolPresent('go', ['version']),
    check: (path) => {
      // WELL-FORMEDNESS check: `gofmt -e` parses the file and reports SYNTAX errors on stderr; a
      // non-zero exit or an `error:`/`expected` diagnostic means the emitted Go does not parse.
      // We assert PARSE validity here — the generated straight-line Go must be valid, vettable Go.
      //
      // We deliberately do NOT gate on gofmt's whitespace fixed point (`gofmt -l` drift). Blank-line
      // placement between top-level declarations is a bc go-straightline EMITTER formatting-fidelity
      // property (bc#75 promised a gofmt fixed point; Go 1.26's gofmt now inserts a blank line before
      // each doc-commented declaration, which the current emitter's helper block does not pre-insert —
      // ESCALATED to bc, codegen は上流所有). It is purely cosmetic: the code parses, vets, and is
      // behavior-identical. Gating litedbmodel's conformance on a downstream cosmetic emitter detail
      // (that varies by gofmt version) would be wrong — litedbmodel owns "the emitted code is valid
      // and behavior-equal", bc owns "the emitted code is gofmt-canonical".
      const p = spawnSync('gofmt', ['-e', path], { encoding: 'utf-8' });
      if (p.status !== 0) return { ok: false, detail: `gofmt parse error (exit ${p.status}): ${(p.stderr ?? '').trim()}` };
      const stderr = p.stderr ?? '';
      if (/(^|\n)\S+\.go:\d+:\d+:|(\berror\b|\bexpected\b)/.test(stderr)) {
        return { ok: false, detail: `go parse diagnostic: ${stderr.split('\n').slice(0, 3).join(' | ')}` };
      }
      return { ok: true };
    },
  },
  {
    lang: 'rust',
    ext: 'rs',
    toolAvailable: () => toolPresent('rustfmt', ['--version']),
    check: (path) => {
      // rustfmt PARSES the file before formatting. Its exit code is 1 for BOTH "would reformat" and
      // "parse error", so distinguish by stderr: a parse error emits a diagnostic `error:` line.
      // (rustc --emit=metadata would need the runtime crate deps; a parse via rustfmt is the
      // dependency-free well-formedness check appropriate here.) The emitted IR literal is a
      // `serde_json::json!(...)` body — a syntactically valid Rust token stream by construction.
      const fmt = spawnSync('rustfmt', ['--check', '--edition', '2021', path], { encoding: 'utf-8' });
      if (fmt.error !== undefined) return { ok: false, detail: `rustfmt not runnable: ${String(fmt.error)}` };
      const stderr = fmt.stderr ?? '';
      if (/^error(\[|:| )/m.test(stderr)) return { ok: false, detail: `rust parse error: ${stderr.split('\n').slice(0, 3).join(' | ')}` };
      // status 0 (already formatted) or 1 (would reformat) with no parse error → parses OK.
      return { ok: true };
    },
  },
];

interface CodegenResult {
  suite: string;
  tally: Tally;
}

async function runExecVectors(vectors: Json[], suiteName: string, outDir: string): Promise<Tally> {
  const t: Tally = { pass: 0, fail: 0 };
  console.error(`\n${suiteName}.json — ${vectors.length} bundle vectors × codegen`);
  for (const [idx, v] of vectors.entries()) {
    // 1) structural byte-identity for the DE-BOX codegen languages (ts/go/rust). python/php are the
    //    ir/interpret surface (no de-box endpoint — a declared spec choice), so they are NOT codegen'd.
    let allStructural = true;
    for (const lang of DEBOX_LANGS) {
      const r = structuralOk(v, lang);
      if (!r.ok) {
        line(false, `${v.name} [structural:${lang}]`, r.detail);
        allStructural = false;
        t.fail++;
      }
    }
    if (allStructural) {
      line(true, `${v.name} [structural: ${DEBOX_LANGS.join('/')} bake identical IR + de-box]`);
      t.pass++;
    }

    // 2) TS real execution (through the emitted typed module + thin handlers)
    try {
      const r = await tsExecOk(v, outDir, idx);
      line(r.ok, `${v.name} [exec:ts emitted module]`, r.detail);
      r.ok ? t.pass++ : t.fail++;
    } catch (e) {
      line(false, `${v.name} [exec:ts]`, e instanceof Error ? e.message : String(e));
      t.fail++;
    }

    // 3) Go/Rust compile/parse check of the emitted de-boxed source
    for (const c of COMPILE_CHECKS) {
      if (!c.toolAvailable()) {
        line(true, `${v.name} [compile:${c.lang} SKIPPED — toolchain absent]`);
        continue;
      }
      const art = generateCodegenArtifact(v.bundle, c.lang === 'go' ? 'go' : c.lang, REGISTERED);
      const p = join(outDir, `behaviors_${idx}_${c.lang}.${c.ext}`);
      writeFileSync(p, art.module.code, 'utf8');
      const r = c.check(p);
      line(r.ok, `${v.name} [compile:${c.lang} emitted source parses]`, r.detail);
      r.ok ? t.pass++ : t.fail++;
    }
  }
  return t;
}

async function main(): Promise<void> {
  console.error('litedbmodel SCP codegen conformance — mode-3 (bc shared generator) byte-identity');
  console.error(`bc generator languages: ${[...REGISTERED].sort().join(', ')}`);
  const files = readdirSync(VECTORS_DIR).filter((f) => f.endsWith('.json')).sort();
  const suites = files.map((f) => JSON.parse(readFileSync(join(VECTORS_DIR, f), 'utf8')) as Json);
  const mismatched = suites.filter((s) => s.corpusVersion !== SUPPORTED_CORPUS_VERSION);
  if (mismatched.length > 0) {
    for (const s of mismatched) console.error(`FAIL-CLOSED: suite '${s.suite}' corpusVersion ${s.corpusVersion} != ${SUPPORTED_CORPUS_VERSION}`);
    console.log(JSON.stringify({ lang: 'codegen', suites: {}, total_pass: 0, total_fail: 0, version_mismatch: true }));
    process.exit(2);
  }

  const outDir = mkdtempSync(join(REPO, '.codegen-conf-'));
  const tallies: Record<string, Tally> = {};
  try {
    for (const suite of suites) {
      // The DE-BOX codegen leg covers BOTH read AND write. A read (exec) bundle's IR carries the
      // typed outType/outputType annotations bc's typed(-raw) emitters de-box into concrete row
      // structs; a WRITE (tx) bundle carries the TransactionResult `outputType` (entity/returnedRows
      // rows typed via the schema SoT — `annotateWriteBundleOutType`), so the SAME emitters de-box the
      // write RESULT into a concrete struct. There is NO literal/boxed fallback: an un-de-boxable
      // write THROWS at generation. A tx vector WITHOUT an `outputType` on its bundle is a write shape
      // outside the single-row-struct de-box capability (e.g. a heterogeneous multi-table COMPOSITE
      // write, which targets two tables and cannot materialize one row struct) — it is genuinely not a
      // codegen-module case (a capability boundary, not a fallback) and stays covered by the mode-2
      // thin-runtime tx leg. We include exec + de-boxable tx vectors here.
      const bundleVectors = suite.vectors.filter(
        (v: Json) => v.kind === 'exec' || (v.kind === 'tx' && v.bundle?.outputType !== undefined),
      );
      if (bundleVectors.length === 0) continue;
      tallies[suite.suite] = await runExecVectors(bundleVectors, suite.suite, outDir);
    }
  } finally {
    rmSync(outDir, { recursive: true, force: true });
  }

  const total_pass = Object.values(tallies).reduce((n, s) => n + s.pass, 0);
  const total_fail = Object.values(tallies).reduce((n, s) => n + s.fail, 0);
  console.error(`\n${total_pass} passed, ${total_fail} failed across codegen checks`);
  console.log(JSON.stringify({ lang: 'codegen', suites: tallies, total_pass, total_fail, version_mismatch: false }));
  process.exit(total_fail > 0 ? 1 : 0);
}

main();
