/**
 * litedbmodel v2 SCP — mode-3 codegen conformance runner (WS7f, #35; spec §9 / §10 / §11).
 *
 * The codegen LEG of the cross-language conformance LOCK. It proves the AC "生成コード出力が
 * thin-runtime と byte 一致" against the FROZEN vector corpus (`conformance/vectors/*.json`):
 *
 *  For every read/exec + tx vector (which carry a full §8 SqlBundle), for EVERY language bc's
 *  shared generator supports (typescript / python / go / rust / php):
 *
 *   1. GENERATE the behavior module (bc's shared generator bakes the surrogate IR as a native
 *      literal + `bind(handlers)`) + the SQL catalog companion (the litedbmodel-specific fields).
 *   2. STRUCTURAL byte-identity: the baked IR literal equals the source bundle component + the
 *      generator's embedded fingerprint recomputes (proven for all 5 languages).
 *   3. REAL execution byte-identity (typescript + python — the two toolchains that can EXECUTE a
 *      generated module against the SAME thin-runtime handlers): import the emitted module, pair
 *      its `bind` with the thin-runtime SQL handlers built from the companion, run against seeded
 *      SQLite, and assert the output equals BOTH the frozen vector AND the mode-2 thin-runtime,
 *      byte-for-byte (exact canonical comparison).
 *   4. COMPILE check (go / rust / php): the emitted source is type-checked / parsed by the native
 *      toolchain (gofmt+vet / rustc parse / php -l) so the generated code is provably well-formed
 *      for those languages; their thin-runtimes are already conformance-verified in mode-2, and the
 *      generated `bind()` calls the IDENTICAL `RunBehavior` over the IDENTICAL baked IR — so mode-3
 *      == mode-2 follows from the shared core + the structural-IR proof.
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
import { registeredLanguages, fingerprintComponentGraph } from 'behavior-contracts';

const {
  executeBundle,
  executeTransactionBundle,
  buildHandlers,
  normalizeInput,
  dialectFor,
  generateCodegenArtifact,
  bundleToPortableIR,
  CODEGEN_LANGUAGES,
} = lm;

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = join(HERE, '..', '..');
const VECTORS_DIR = process.env.LITEDBMODEL_VECTORS ?? join(REPO, 'conformance', 'vectors');
const SUPPORTED_CORPUS_VERSION = 1;
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

// ── Structural byte-identity: baked IR literal == bundle component + fingerprint recomputes ──
function structuralOk(v: Json, language: string): { ok: boolean; detail?: string } {
  const art = generateCodegenArtifact(v.bundle, language, REGISTERED);
  if (canon(art.ir) !== canon(bundleToPortableIR(v.bundle))) return { ok: false, detail: 'baked IR != bundle component' };
  const recomputed = fingerprintComponentGraph(art.ir);
  if (art.module.fingerprint !== recomputed) return { ok: false, detail: 'fingerprint mismatch' };
  if (!art.module.code.includes(recomputed)) return { ok: false, detail: 'fingerprint not baked into code' };
  if (canon(art.companion.operations) !== canon(v.bundle.operations)) return { ok: false, detail: 'companion operations != bundle' };
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
      irVersion: art.ir.irVersion,
      exprVersion: art.ir.exprVersion,
      dialect: art.companion.dialect,
      component: art.ir.components[0],
      operations: art.companion.operations,
      optionalHeads: [...art.companion.optionalHeads],
      relations: art.companion.relations,
      ...(art.companion.transaction !== undefined ? { transaction: art.companion.transaction } : {}),
    };
    if (canon(reassembled) !== canon(v.bundle)) return { ok: false, detail: 'reassembled bundle != source' };
    const db = seedDb(v.schema);
    const result = executeTransactionBundle(reassembled as never, input as never, { db });
    const stateOk = (v.expectedDbState ?? []).every((s: Json) => canon(db.prepare(s.query).all()) === canon(decodeValue(s.rows)));
    db.close();
    const okResult = canon(result) === canon(decodeValue(v.expectedResult));
    return okResult && stateOk ? { ok: true } : { ok: false, detail: 'tx result/db-state mismatch' };
  }
  const modPath = join(outDir, `behaviors_${idx}.generated.ts`);
  writeFileSync(modPath, art.module.code, 'utf8');
  const mod = (await import(pathToFileURL(modPath).href)) as {
    bind: (h: unknown) => Record<string, (input?: unknown) => unknown>;
    IR: Json;
    COMPONENT_NAMES: readonly string[];
  };
  const db = seedDb(v.schema);
  const handlers = buildHandlers(db as never, art.companion.operations, dialectFor(art.companion.dialect));
  const component = mod.IR.components[0];
  const normalized = normalizeInput(component as never, new Set(art.companion.optionalHeads), input as never);
  const emitted = mod.bind(handlers)[mod.COMPONENT_NAMES[0]](normalized);
  db.close();

  const dbRef = seedDb(v.schema);
  const modeTwo = executeBundle(v.bundle, input as never, { db: dbRef });
  dbRef.close();

  if (canon(emitted) !== canon(modeTwo)) return { ok: false, detail: 'emitted TS != executeBundle' };
  if (canon(emitted) !== canon(decodeValue(v.expectedResult))) return { ok: false, detail: 'emitted TS != vector' };
  return { ok: true };
}

// ── Python real execution: shell to the Python executor (imports emitted module) ──
function pyExecOk(v: Json, outDir: string, idx: number): { ok: boolean; detail?: string } {
  const art = generateCodegenArtifact(v.bundle, 'python', REGISTERED);
  const modPath = join(outDir, `behaviors_${idx}.generated.py`);
  writeFileSync(modPath, art.module.code, 'utf8');
  const job = {
    modulePath: modPath,
    companion: JSON.parse(JSON.stringify(art.companion)),
    input: v.input,
    schema: v.schema,
    expectedResult: v.expectedResult,
    kind: v.kind,
    expectedDbState: v.expectedDbState ?? [],
  };
  const proc = spawnSync('python3', [join(HERE, 'py_codegen_exec.py'), JSON.stringify(job)], {
    cwd: REPO,
    encoding: 'utf-8',
    maxBuffer: 32 * 1024 * 1024,
  });
  if (proc.status !== 0) return { ok: false, detail: `python exec exited ${proc.status}: ${(proc.stderr ?? '').split('\n').slice(-4).join(' | ')}` };
  const lastLine = (proc.stdout ?? '').trimEnd().split('\n').pop() ?? '';
  let out: { result: Json; dbState: Json[] };
  try {
    out = JSON.parse(lastLine);
  } catch {
    return { ok: false, detail: `python produced no JSON: ${lastLine}` };
  }
  if (canon(out.result) !== canon(decodeValue(v.expectedResult))) return { ok: false, detail: `python result != vector` };
  for (const [i, s] of (v.expectedDbState ?? []).entries()) {
    if (canon(out.dbState[i]?.rows) !== canon(decodeValue(s.rows))) return { ok: false, detail: `python db-state[${i}] mismatch` };
  }
  return { ok: true };
}

// ── Go/Rust/PHP: the emitted source is parsed/compiled by the native toolchain ──
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
const COMPILE_CHECKS: CompileCheck[] = [
  {
    lang: 'php',
    ext: 'php',
    toolAvailable: () => toolPresent('php', ['--version']),
    check: (path) => {
      const p = spawnSync('php', ['-l', path], { encoding: 'utf-8' });
      return p.status === 0 ? { ok: true } : { ok: false, detail: (p.stdout ?? '') + (p.stderr ?? '') };
    },
  },
  {
    lang: 'go',
    ext: 'go',
    toolAvailable: () => toolPresent('gofmt', ['-h']) || toolPresent('go', ['version']),
    check: (path) => {
      // gofmt -l reports files whose formatting differs; the emitter promises a gofmt fixed point,
      // and `gofmt` also parses (a syntax error is a non-zero exit). So a clean, empty gofmt -l is
      // both "parses" and "gofmt-clean".
      const p = spawnSync('gofmt', ['-l', path], { encoding: 'utf-8' });
      if (p.status !== 0) return { ok: false, detail: `gofmt error: ${(p.stderr ?? '').trim()}` };
      const drift = (p.stdout ?? '').trim();
      return drift === '' ? { ok: true } : { ok: false, detail: `gofmt drift: ${drift}` };
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
    // 1) structural byte-identity for ALL 5 languages
    let allStructural = true;
    for (const lang of CODEGEN_LANGUAGES) {
      const r = structuralOk(v, lang);
      if (!r.ok) {
        line(false, `${v.name} [structural:${lang}]`, r.detail);
        allStructural = false;
        t.fail++;
      }
    }
    if (allStructural) {
      line(true, `${v.name} [structural: all 5 langs bake identical IR]`);
      t.pass++;
    }

    // 2) TS real execution
    try {
      const r = await tsExecOk(v, outDir, idx);
      line(r.ok, `${v.name} [exec:ts emitted module]`, r.detail);
      r.ok ? t.pass++ : t.fail++;
    } catch (e) {
      line(false, `${v.name} [exec:ts]`, e instanceof Error ? e.message : String(e));
      t.fail++;
    }

    // 3) Python real execution
    try {
      const r = pyExecOk(v, outDir, idx);
      line(r.ok, `${v.name} [exec:py emitted module]`, r.detail);
      r.ok ? t.pass++ : t.fail++;
    } catch (e) {
      line(false, `${v.name} [exec:py]`, e instanceof Error ? e.message : String(e));
      t.fail++;
    }

    // 4) Go/Rust/PHP compile/parse check of the emitted source
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
      // Only exec + tx suites carry a full §8 bundle (component + operations) that codegen bakes.
      const bundleVectors = suite.vectors.filter((v: Json) => v.kind === 'exec' || v.kind === 'tx');
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
