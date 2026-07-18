// ════════════════════════════════════════════════════════════════════════════
// codegen-build (epic #107 P3 / #111) — the ONE codegen path every native cell reuses + a drift gate.
// ════════════════════════════════════════════════════════════════════════════
//
// litedbmodel has no `generate` CLI (unlike graphddb); its codegen is the IN-PROCESS
// `generateCodegenArtifact(bundle, lang, …)` → bc. So this wrapper compiles the 19 ORM ops (ops.ts,
// the SCP SSoT), runs `generateCodegenArtifact` for each op × lang, and writes the generated module into
// `adapters/<lang>/generated/`. `check` re-generates in memory and diffs against the committed files —
// a model/emitter change that was not re-baked fails LOUDLY (the graphddb `checkCodegenDrift` pattern).
//
// Run (via tsx — bc is ESM-only, resolved through the bundled dist/scp/index.cjs):
//   npx tsx benchmark/crosslang/codegen-build.ts generate --lang rust
//   npx tsx benchmark/crosslang/codegen-build.ts check    --lang rust
//   (…--lang go | ts ; omit --lang to do all three)
import { writeFileSync, readFileSync, existsSync, mkdirSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
// The bundled instance (inlines the ESM-only behavior-contracts) so this runs standalone under tsx.
import { generateCodegenArtifact } from '../../dist/scp/index.cjs';
import { buildOps, type BenchOp } from './ops';

const HERE = dirname(fileURLToPath(import.meta.url));
const ADAPTERS = join(HERE, 'adapters');

// bc's registered emitters (the cjs bundle registers these on load; passed for the validate-only check
// inside generateCodegenArtifact — hardcoded because the ESM-only bc can't be imported under tsx).
const REGISTERED = ['go-typed-native', 'php', 'python', 'rust-typed-native', 'typescript', 'typescript-native', 'typescript-typed'];

const LANGS = ['rust', 'go', 'ts'] as const;
type Lang = (typeof LANGS)[number];
const LANG_ARG: Record<Lang, string> = { rust: 'rust', go: 'go', ts: 'typescript' };
const EXT: Record<Lang, string> = { rust: 'rs', go: 'go', ts: 'ts' };

/** Generate one op's module source for one language (the string the drift gate freezes). */
function genModule(op: BenchOp, lang: Lang): string {
  const art = generateCodegenArtifact(op.bundle, LANG_ARG[lang], REGISTERED, op.resolve);
  let code = art.module.code as string;
  // Each generated go module is standalone (own package boilerplate) — put each in its OWN package
  // (adapters/go/generated/<op>/) to avoid symbol collisions, the go twin of rust's per-op `mod`.
  if (lang === 'go') code = code.replace(/^package behaviors$/m, `package ${op.id.toLowerCase()}`);
  return code;
}

/** The committed path for an op's generated module (go gets a per-op subdir). */
function modulePath(op: BenchOp, lang: Lang): string {
  const base = join(ADAPTERS, lang, 'generated');
  if (lang === 'go') return join(base, op.id.toLowerCase(), 'gen.go');
  return join(base, `gen_${op.id.toLowerCase()}.${EXT[lang]}`);
}

function generate(lang: Lang): { count: number } {
  const ops = buildOps();
  for (const op of ops) {
    const p = modulePath(op, lang);
    mkdirSync(dirname(p), { recursive: true });
    writeFileSync(p, genModule(op, lang));
  }
  return { count: ops.length };
}

/** Drift gate: regenerate in memory, diff against the committed module. Returns the drifted op ids. */
function check(lang: Lang): { ok: boolean; drifted: string[] } {
  const drifted: string[] = [];
  for (const op of buildOps()) {
    const p = modulePath(op, lang);
    const fresh = genModule(op, lang);
    if (!existsSync(p)) {
      drifted.push(`${op.id} (missing)`);
      continue;
    }
    if (readFileSync(p, 'utf8') !== fresh) drifted.push(op.id);
  }
  return { ok: drifted.length === 0, drifted };
}

// ── CLI ───────────────────────────────────────────────────────────────────────
function langsFromArgs(argv: string[]): Lang[] {
  const i = argv.indexOf('--lang');
  if (i === -1 || !argv[i + 1]) return [...LANGS];
  const l = argv[i + 1] as Lang;
  if (!LANGS.includes(l)) throw new Error(`unknown --lang '${l}' (rust|go|ts)`);
  return [l];
}

function main(): void {
  const [cmd, ...rest] = process.argv.slice(2);
  const langs = langsFromArgs(rest);
  if (cmd === 'generate') {
    for (const lang of langs) {
      const { count } = generate(lang);
      process.stdout.write(`generated ${count} ${lang} modules → adapters/${lang}/generated/\n`);
    }
  } else if (cmd === 'check') {
    let bad = false;
    for (const lang of langs) {
      const { ok, drifted } = check(lang);
      if (ok) process.stdout.write(`codegen drift gate: ${lang} up to date\n`);
      else {
        bad = true;
        process.stderr.write(`codegen DRIFT (${lang}): ${drifted.join(', ')}\n  re-bake: npx tsx benchmark/crosslang/codegen-build.ts generate --lang ${lang}\n`);
      }
    }
    process.exit(bad ? 1 : 0);
  } else {
    process.stderr.write('usage: codegen-build.ts <generate|check> [--lang rust|go|ts]\n');
    process.exit(2);
  }
}

main();
