#!/usr/bin/env node
// native-codegen-purity — the #113 purity gate. Proves the GENERATED crosslang codegen modules are
// what each tier claims to be. It is COMPILER-MEASURED for the native tiers (not a text/regex scan of
// the hot path), so it is definitive:
//
//   • rust (NATIVE) — compile every generated/<dialect>/gen_*.rs STANDALONE with
//       `rustc --crate-type lib --emit metadata` and NO `--extern behavior_contracts`. If a module
//       names the bc runtime crate (or serde / serde_json / any non-std crate), rustc fails with an
//       unresolved-crate error ⇒ FAIL. If it compiles, it provably uses only std ⇒ runtime-free by
//       construction (the boxed Value/RawValue enum + interpreter are unreachable).
//   • go   (NATIVE) — `go list -deps ./generated/<dialect>/...` yields the full transitive import set
//       (the compiler's own resolution). A behavior-contracts runtime path OR `encoding/json` in that
//       set ⇒ FAIL (the covered plane must carry no bc runtime and no JSON marshal on the hot path).
//   • ts   (TYPED) — the boxed-typed endpoint is still de-interpreted: the generated modules import
//       ONLY bc VALUE helpers (codegenPrimitives / conformResultToOutType / baked constants + failure
//       types) — never a runtime IR-doc load — and the baked straight-line body calls no interpreter
//       (`runBehavior(`), parses no IR (`JSON.parse(` / `loadCompiledIR(`), and embeds no IR literal
//       (`"irVersion"`). Checked as CALLS (immediate `(`) / quoted keys so prose comments never
//       false-positive.
//   • py / php (IR tier) — the interpreter tier: they load the portable IR doc and run it through the
//       bc runtime. That is their DESIGN (labeled `ir`), so they are REPORTED, not native-purity-gated.
//
// Usage:  node benchmark/crosslang/native-codegen-purity.mjs   (from repo root)
// Exit:   0 = every native/typed module is pure for its tier, 1 = a leak.

import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, resolve } from 'node:path';
import { tmpdir } from 'node:os';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const ADAPTERS = join(ROOT, 'benchmark', 'crosslang', 'adapters');
const DIALECTS = ['sqlite', 'postgres', 'mysql'];
let failed = 0;
let passed = 0;

// ── rust (NATIVE): standalone rustc, no bc extern in scope. ──────────────────────────────────
console.log('── rust (native) — standalone rustc, no behavior_contracts extern ──');
for (const dialect of DIALECTS) {
  const dir = join(ADAPTERS, 'rust', 'generated', dialect);
  if (!existsSync(dir)) { console.error(`ERROR [rust] ${dialect}: ${dir} not found`); failed++; continue; }
  const files = readdirSync(dir).filter((f) => f.startsWith('gen_') && f.endsWith('.rs'));
  for (const f of files) {
    const abs = join(dir, f);
    const out = join(mkdtempSync(join(tmpdir(), 'ncp-rust-')), 'purity.rmeta');
    try {
      execFileSync('rustc', ['--edition', '2021', '--crate-type', 'lib', '--emit', 'metadata', '-o', out, abs],
        { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
      passed++;
    } catch (e) {
      const msg = (e.stderr || e.message).toString();
      console.error(`FAIL  [rust] ${dialect}/${f} does not compile runtime-free (rustc):`);
      console.error(msg.split('\n').slice(0, 10).map((l) => '        ' + l).join('\n'));
      failed++;
    }
  }
  console.log(`  ${dialect}: ${files.length} generated .rs compiled standalone (std-only)`);
}

// ── go (NATIVE): go list -deps, no bc runtime / no encoding/json. ─────────────────────────────
console.log('── go (native) — go list -deps, no bc-runtime / no encoding/json ──');
const GO_RUNTIME = /behavior[-_.]?contracts/i;
for (const dialect of DIALECTS) {
  const pkg = `./generated/${dialect}/...`;
  try {
    const deps = execFileSync('go', ['list', '-deps', pkg], {
      cwd: join(ADAPTERS, 'go'), encoding: 'utf8',
      env: { ...process.env, GOPRIVATE: 'github.com/foo-ogawa/*' },
    }).split('\n').filter(Boolean);
    const leaks = deps.filter((d) => GO_RUNTIME.test(d) || d === 'encoding/json');
    if (leaks.length) {
      console.error(`FAIL  [go] ${pkg} transitively imports a forbidden dependency:`);
      leaks.forEach((l) => console.error(`        ${l}`));
      failed++;
    } else {
      console.log(`  ${dialect}: ${deps.length} transitive deps, 0 bc-runtime, 0 encoding/json (compiler-enforced)`);
      passed++;
    }
  } catch (e) {
    console.error(`ERROR [go] go list -deps ${pkg} failed:\n${(e.stderr || e.message).toString().slice(0, 500)}`);
    failed++;
  }
}

// ── ts (TYPED): value-helper-only imports + baked straight-line (no interpreter call / IR load). ─
console.log('── ts (typed) — value-helper imports only, baked straight-line (no IR interpreter) ──');
// The value helpers a de-interpreted typed module may import: baked constants + failure types +
// the codegen VALUE primitives. NOT the IR interpreter / IR-doc loader.
const TS_ALLOWED_VALUE = new Set([
  'SPEC_VERSIONS', 'BehaviorFailure', 'PlanFailure', 'ExprFailure',
  'codegenPrimitives', 'conformResultToOutType',
]);
// Forbidden RUNTIME signals — matched as CALLS / quoted keys so a prose comment can never false-positive.
const TS_FORBIDDEN = [/\brunBehavior\s*\(/, /\bJSON\.parse\s*\(/, /\bloadCompiledIR\s*\(/, /\bloadIrDoc\s*\(/, /\bloadCompiledIrDoc\s*\(/, /["']irVersion["']/];
for (const dialect of DIALECTS) {
  const dir = join(ADAPTERS, 'ts', 'generated', dialect);
  if (!existsSync(dir)) { console.error(`ERROR [ts] ${dialect}: ${dir} not found`); failed++; continue; }
  const files = readdirSync(dir).filter((f) => f.startsWith('gen_') && f.endsWith('.ts'));
  let dialectOk = true;
  for (const f of files) {
    const src = readFileSync(join(dir, f), 'utf8');
    // Value (non-type) imports from behavior-contracts must be a SUBSET of the allowed value helpers.
    const valueImports = [...src.matchAll(/import\s+(?!type\b)\{([^}]*)\}\s+from\s+["']behavior-contracts["']/g)];
    for (const m of valueImports) {
      const specs = m[1].split(',').map((s) => s.trim().split(/\s+as\s+/)[0].trim()).filter(Boolean);
      const bad = specs.filter((s) => !TS_ALLOWED_VALUE.has(s));
      if (bad.length) {
        console.error(`FAIL  [ts] ${dialect}/${f} imports non-value-helper bc symbol(s): ${bad.join(', ')}`);
        failed++; dialectOk = false;
      }
    }
    // A bc import from any runtime SUBPATH (e.g. behavior-contracts/dist/…) is a runtime reach — forbid.
    if (/from\s+["']behavior-contracts\/[^"']+["']/.test(src)) {
      console.error(`FAIL  [ts] ${dialect}/${f} imports a behavior-contracts runtime subpath`);
      failed++; dialectOk = false;
    }
    for (const re of TS_FORBIDDEN) {
      if (re.test(src)) {
        console.error(`FAIL  [ts] ${dialect}/${f} contains a runtime signal ${re}`);
        failed++; dialectOk = false;
      }
    }
  }
  if (dialectOk) { console.log(`  ${dialect}: ${files.length} generated .ts import only value helpers, no IR interpreter (baked straight-line)`); passed++; }
}

// ── py / php (IR tier): interpreter by design — reported, not native-gated. ───────────────────
console.log('── py / php (ir tier) — interpreter over the portable IR doc; bc runtime allowed (labeled `ir`, NOT native-gated) ──');
for (const lang of ['py', 'php']) {
  const dir = join(ADAPTERS, lang);
  console.log(`  ${lang}: ir tier${existsSync(dir) ? '' : ' (adapter dir missing)'} — bc runtime interpretation is the intended execution model`);
}

console.log('');
if (failed) {
  console.error(`✗ native-codegen-purity: ${failed} check(s) failed — a runtime/boxing leak reached a native/typed codegen module.`);
  process.exit(1);
}
console.log(`✓ native-codegen-purity: all ${passed} native/typed codegen checks pure (rust std-only, go bc/json-free, ts value-helper baked); py/php are the ir tier.`);
process.exit(0);
