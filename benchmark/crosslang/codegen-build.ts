// ════════════════════════════════════════════════════════════════════════════
// codegen-build (epic #107 P3 / #111) — the ONE codegen path every native cell reuses + a drift gate.
// ════════════════════════════════════════════════════════════════════════════
//
// The bench's codegen is driven through bc's codegen CLI (`bc generate`), NOT the in-process
// `generateCodegenArtifact`. litedbmodel lowers each op to its portable IR DOC (the exact
// `ComponentGraphIR` `generateModule` consumes — via `lowerBundleToPortableIrDoc`), writes it to
// `ir-docs/<op>.json` (a BUILD-TIME codegen INPUT, committed — like graphddb's operations.json; NEVER read
// at runtime), and shells out to bc's CLI to emit the native module into `adapters/<lang>/generated/`.
// `check` re-lowers + re-generates via the CLI and byte-diffs the committed modules (drift gate).
//
// The bc CLI is the LOCAL, unpublished build (behavior-contracts/ts/dist/cli.js) until bc releases —
// resolved via $BC_CLI or the sibling-repo default. Run via tsx (the lowering uses the bundled cjs):
//   npx tsx benchmark/crosslang/codegen-build.ts generate [--lang rust|go|ts]
//   npx tsx benchmark/crosslang/codegen-build.ts check    [--lang rust|go|ts]
import { writeFileSync, readFileSync, existsSync, mkdirSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
// The bundled instance (inlines the ESM-only behavior-contracts) so this runs standalone under tsx.
import { lowerBundleToPortableIrDoc } from '../../dist/scp/index.cjs';
import { buildOps, type BenchOp } from './ops';

const HERE = dirname(fileURLToPath(import.meta.url));
const ADAPTERS = join(HERE, 'adapters');
const IR_DOCS = join(HERE, 'ir-docs');
// The LOCAL bc codegen CLI (unpublished until release). Override with $BC_CLI; default = sibling repo.
const BC_CLI = process.env.BC_CLI ?? resolve(HERE, '../../../behavior-contracts/ts/dist/cli.js');

const LANGS = ['rust', 'go', 'ts'] as const;
type Lang = (typeof LANGS)[number];
const EMITTER: Record<Lang, string> = { rust: 'rust-typed-native', go: 'go-typed-native', ts: 'typescript-typed' };
const EXT: Record<Lang, string> = { rust: 'rs', go: 'go', ts: 'ts' };

/** Write an op's portable IR doc (the build-time codegen input the CLI consumes). Returns its path. */
function writeIrDoc(op: BenchOp): string {
  mkdirSync(IR_DOCS, { recursive: true });
  const p = join(IR_DOCS, `${op.id}.json`);
  writeFileSync(p, JSON.stringify(lowerBundleToPortableIrDoc(op.bundle, op.resolve), null, 2) + '\n');
  return p;
}

/** Generate one op's module for one language by shelling out to `bc generate` over the op's IR doc. */
function genModule(op: BenchOp, lang: Lang): string {
  const docPath = writeIrDoc(op);
  const res = spawnSync('node', [BC_CLI, 'generate', '--lang', EMITTER[lang], '--in', docPath], {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
  if (res.status !== 0) {
    throw new Error(`bc generate (${lang} ${op.id}) failed (exit ${res.status}): ${(res.stderr ?? '').trim() || 'no stderr'}`);
  }
  let code = res.stdout as string;
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
