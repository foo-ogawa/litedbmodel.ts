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
import { join, dirname, resolve, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
// The bundled instance (inlines the ESM-only behavior-contracts) so this runs standalone under tsx.
import { lowerBundleToPortableIrDoc } from '../../dist/scp/index.cjs';
import { buildOps, type BenchOp } from './ops';
import type { OrmDialect } from './orm-domain';
import { goCompanion, goDispatch } from './go-companions';

const HERE = dirname(fileURLToPath(import.meta.url));
const ADAPTERS = join(HERE, 'adapters');
const IR_DOCS = join(HERE, 'ir-docs');
// The LOCAL bc codegen CLI (unpublished until release). Override with $BC_CLI; default = sibling repo.
const BC_CLI = process.env.BC_CLI ?? resolve(HERE, '../../../behavior-contracts/ts/dist/cli.js');

const LANGS = ['rust', 'go', 'ts'] as const;
type Lang = (typeof LANGS)[number];
const DIALECTS = ['sqlite', 'postgres', 'mysql'] as const;
const EMITTER: Record<Lang, string> = { rust: 'rust-typed-native', go: 'go-typed-native', ts: 'typescript-typed' };
const EXT: Record<Lang, string> = { rust: 'rs', go: 'go', ts: 'ts' };

/** Write an op's per-dialect portable IR doc (the build-time codegen input the CLI consumes). Returns its path. */
function writeIrDoc(op: BenchOp, dialect: OrmDialect): string {
  const dir = join(IR_DOCS, dialect);
  mkdirSync(dir, { recursive: true });
  const p = join(dir, `${op.id}.json`);
  writeFileSync(p, JSON.stringify(lowerBundleToPortableIrDoc(op.bundle, op.resolve), null, 2) + '\n');
  return p;
}

/** Generate one op's module for one language by shelling out to `bc generate` over the op's IR doc. */
function genModule(op: BenchOp, lang: Lang, dialect: OrmDialect): string {
  const docPath = writeIrDoc(op, dialect);
  // go-typed-native: the WireValue/WireRow/WireList seam types are CONSUMER-supplied (bc 0.8.10 contract,
  // #152/#153). `--go-wire-import` qualifies them to the bench's shared `wire` package (one seam for all
  // ops); the per-module probe structs + the in-package `Handler_<comp>` (consumer-defined) stay local.
  // typescript-typed imports the bc RUNTIME (codegenPrimitives / conformResultToOutType / Failure types +
  // Handlers). node_modules bc is 0.8.5 (litedbmodel's pinned dep) but the generated module needs the
  // LOCAL 0.8.10 (spec skew) → point the runtime import at the local bc dist (the SAME build $BC_CLI drives)
  // via a stable relative path from generated/<dialect>/ (deterministic → drift-clean).
  const tsRuntime = relative(join(ADAPTERS, 'ts', 'generated', dialect), join(dirname(BC_CLI), 'index.js'));
  const extra =
    lang === 'go' ? ['--go-wire-import', 'orm_bench_go/wire'] : lang === 'ts' ? ['--runtime-import', tsRuntime] : [];
  const res = spawnSync('node', [BC_CLI, 'generate', '--lang', EMITTER[lang], '--in', docPath, ...extra], {
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

/** The committed path for an op's generated per-dialect module (go gets a per-op subdir). */
function modulePath(op: BenchOp, lang: Lang, dialect: OrmDialect): string {
  const base = join(ADAPTERS, lang, 'generated', dialect);
  if (lang === 'go') return join(base, op.id.toLowerCase(), 'gen.go');
  return join(base, `gen_${op.id.toLowerCase()}.${EXT[lang]}`);
}

/** The CONSUMER-supplied go artifacts (bc 0.8.10): per-op `handler.go` (Handler_<comp> + Node_* leaf +
 * Native) + the per-dialect `dispatch.go` (op → package.Native). ONE source of their path+content, so
 * generate() writes them and check() byte-diffs them identically (drift gate over the machine-generated
 * leaves — no hand-edited companions). */
function goArtifacts(dialect: OrmDialect): { path: string; content: string }[] {
  const ops = buildOps(dialect);
  const base = join(ADAPTERS, 'go', 'generated', dialect);
  const arts = ops.map((op) => ({ path: join(base, op.id.toLowerCase(), 'handler.go'), content: goCompanion(op, dialect) }));
  arts.push({ path: join(base, 'dispatch.go'), content: goDispatch(ops, dialect) });
  return arts;
}

function generate(lang: Lang, dialect: OrmDialect): { count: number } {
  const ops = buildOps(dialect);
  for (const op of ops) {
    const p = modulePath(op, lang, dialect);
    mkdirSync(dirname(p), { recursive: true });
    writeFileSync(p, genModule(op, lang, dialect));
  }
  // Go: the consumer-supplied companions (leaf handlers + Native) + the per-dialect dispatch.
  if (lang === 'go') {
    for (const a of goArtifacts(dialect)) {
      mkdirSync(dirname(a.path), { recursive: true });
      writeFileSync(a.path, a.content);
    }
  }
  // Rust: emit the per-dialect module tree (mod.rs) so main.rs cfg-selects one `use gen::*` line.
  if (lang === 'rust') {
    const dir = join(ADAPTERS, 'rust', 'generated', dialect);
    const mods = ops.map((op) => `pub mod gen_${op.id.toLowerCase()};`).sort().join('\n');
    writeFileSync(join(dir, 'mod.rs'),
      `// Generated module tree for one dialect — declares the ${ops.length} CLI-generated op modules. main.rs\n` +
      `// cfg-selects generated/<dialect>/mod.rs by the build feature; \`use gen::*\` brings the op modules\n` +
      `// into scope so the dialect-agnostic handlers reference them unqualified.\n${mods}\n`);
  }
  return { count: ops.length };
}

/** Drift gate: regenerate in memory, diff against the committed module. Returns the drifted op ids. */
function check(lang: Lang, dialect: OrmDialect): { ok: boolean; drifted: string[] } {
  const drifted: string[] = [];
  for (const op of buildOps(dialect)) {
    const p = modulePath(op, lang, dialect);
    const fresh = genModule(op, lang, dialect);
    if (!existsSync(p)) {
      drifted.push(`${op.id} (missing)`);
      continue;
    }
    if (readFileSync(p, 'utf8') !== fresh) drifted.push(op.id);
  }
  // Go: the consumer companions + dispatch are ALSO machine-generated → byte-diff them (no hand-edits).
  if (lang === 'go') {
    for (const a of goArtifacts(dialect)) {
      if (!existsSync(a.path)) drifted.push(`${a.path} (missing)`);
      else if (readFileSync(a.path, 'utf8') !== a.content) drifted.push(a.path);
    }
  }
  return { ok: drifted.length === 0, drifted };
}

// ── CLI ───────────────────────────────────────────────────────────────────────
function pickFromArgs<T extends string>(argv: string[], flag: string, all: readonly T[]): T[] {
  const i = argv.indexOf(flag);
  if (i === -1 || !argv[i + 1]) return [...all];
  const v = argv[i + 1] as T;
  if (!all.includes(v)) throw new Error(`unknown ${flag} '${v}' (${all.join('|')})`);
  return [v];
}

function main(): void {
  const [cmd, ...rest] = process.argv.slice(2);
  const langs = pickFromArgs(rest, '--lang', LANGS);
  const dialects = pickFromArgs(rest, '--dialect', DIALECTS);
  if (cmd === 'generate') {
    for (const dialect of dialects)
      for (const lang of langs) {
        const { count } = generate(lang, dialect);
        process.stdout.write(`generated ${count} ${lang} modules → adapters/${lang}/generated/${dialect}/\n`);
      }
  } else if (cmd === 'check') {
    let bad = false;
    for (const dialect of dialects)
      for (const lang of langs) {
        const { ok, drifted } = check(lang, dialect);
        if (ok) process.stdout.write(`codegen drift gate: ${lang}/${dialect} up to date\n`);
        else {
          bad = true;
          process.stderr.write(`codegen DRIFT (${lang}/${dialect}): ${drifted.join(', ')}\n  re-bake: npx tsx benchmark/crosslang/codegen-build.ts generate --lang ${lang} --dialect ${dialect}\n`);
        }
      }
    process.exit(bad ? 1 : 0);
  } else {
    process.stderr.write('usage: codegen-build.ts <generate|check> [--lang rust|go|ts] [--dialect sqlite|postgres|mysql]\n');
    process.exit(2);
  }
}

main();
