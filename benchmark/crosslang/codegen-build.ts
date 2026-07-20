// ════════════════════════════════════════════════════════════════════════════
// codegen-build — emit one NATIVE Rust module per ORM op and dialect (#129).
// ════════════════════════════════════════════════════════════════════════════
//
// SSoT = ops.ts `buildOps(dialect)` (the 19 ORM ops declared once on the SCP surface). For each
// dialect × op this drives litedbmodel's REAL codegen:
//   • bc emits the runtime-free native module (baked SQL literal + typed ports + de-box runner);
//   • litedbmodel co-locates boundary handlers and statically-expanded relation hydrators in it.
// There is NO hand-written exec seam and NO plan artifact walked at runtime: the module carries its
// own SQL as a native literal and the only runtime is litedbmodel_runtime's op-agnostic `exec` seam.
//
// Output layout (mirrors run-proof-livedb.sh's per-dialect swap model):
//   /tmp/ormbench/<dialect>/generated_<id>.rs   (all 3 dialects, regenerated)
//   rust/orm_bench/src/gen/generated_<id>.rs    (COMMITTED sqlite set)
//
// The native bench binary compiles the COMMITTED sqlite `gen/`; the pg/mysql legs swap the matching
// /tmp/ormbench/<dialect>/ files in before building (like the e1 livedb harness).
import { writeFileSync, readFileSync, mkdirSync, readdirSync, rmSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
// Import the codegen entrypoints from the BUILT bundle (dist/scp/index.cjs, which inlines the ESM-only
// behavior-contracts) — the SAME module ops.ts compiles its bundles through, so bundle + emitter share
// one bc instance. The registered-EMITTER list (`rust-typed-native`, …) that `typedEmitterFor` validates
// against comes from bc's own `registeredLanguages()` — loaded once in `main` (bc is ESM-only).
import {
  generateCodegenArtifact,
  generateRustExecutable,
  SemanticBehavior,
  components,
  publishBehaviors,
  compileBundle,
  schemaColumnTypeResolver,
  setLimitConfig,
  resetLimitConfig,
} from '../../dist/scp/index.cjs';
import { buildOps, type BenchOp } from './ops';
import { ORM_DIALECTS, type OrmDialect } from './contract';
import { ddl, dropStatements, seedStatements, pgSeqResetStatements } from './orm-domain';

let REGISTERED: readonly string[] = [];
const TMP = '/tmp/ormbench';
const CRATE_GEN = join(dirname(fileURLToPath(import.meta.url)), '../../rust/orm_bench/src/gen');
const SDK_SETUP = join(dirname(fileURLToPath(import.meta.url)), '../../rust/orm_bench_sdk/src/generated_setup.rs');
function formatRust(path: string): void {
  const result = spawnSync('rustfmt', ['--edition', '2021', path], { encoding: 'utf8' });
  if (result.status !== 0) throw new Error(`rustfmt failed for ${path}: ${result.stderr}`);
}
/** The generated native entry fn `run_native_raw_struct_<Component>` — parsed from the module code so
 * the manifest never re-derives the component name by hand (the emitter is the SSoT). */
function entryOf(moduleCode: string, id: string): string {
  const m = moduleCode.match(/pub fn (run_native_raw_struct_\w+)/);
  if (!m) throw new Error(`no run_native_raw_struct entry in generated module for '${id}'`);
  return m[1];
}

interface ManifestOp {
  readonly id: string;
  readonly kind: BenchOp['kind'];
  readonly entry: string; // '' for tx ops (they call the adapter run_on, not a native raw entry)
  readonly component: string; // the InNR<Component> struct suffix
  readonly withRel?: string;
}

function componentOf(entry: string): string {
  return entry.replace('run_native_raw_struct_', '');
}

function emitDialect(dialect: OrmDialect): ManifestOp[] {
  const dir = join(TMP, dialect);
  rmSync(dir, { recursive: true, force: true });
  mkdirSync(dir, { recursive: true });
  const manifest: ManifestOp[] = [];
  for (const op of buildOps(dialect)) {
    const art = generateCodegenArtifact(op.bundle as never, 'rust', REGISTERED, op.resolve);
    const moduleCode = art.module.code;
    const generated = join(dir, `generated_${op.id}.rs`);
    writeFileSync(generated, generateRustExecutable(op.bundle as never, `generated_${op.id}`, op.resolve, REGISTERED));
    formatRust(generated);
    // tx ops: the native chain runs through the adapter `run_on` (BEGIN…COMMIT envelope), not a
    // bare native raw entry — so the entry name is only meaningful for read/write/batch ops.
    let entry = '';
    try {
      entry = entryOf(moduleCode, op.id);
    } catch {
      if (op.kind !== 'tx') throw new Error(`missing native entry for non-tx op '${op.id}'`);
    }
    manifest.push({ id: op.id, kind: op.kind, entry, component: entry ? componentOf(entry) : op.id, ...(op.withRel ? { withRel: op.withRel } : {}) });
  }
  return manifest;
}

/** Inline a seed param as a SQL literal (numbers/booleans bare, strings single-quoted, `''`-escaped) —
 * so the emitted Rust module is a native static statement list any driver execs verbatim. The seed SSoT is
 * orm-domain.ts `seedStatements`; this only renders its `?` params into literals for the rust bench. */
function inlineLiteral(v: unknown): string {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'number') return String(v);
  if (typeof v === 'boolean') return v ? 'true' : 'false';
  return `'${String(v).replace(/'/g, "''")}'`;
}
function inlineSeed(sql: string, params: readonly unknown[]): string {
  let i = 0;
  return sql.replace(/\?/g, () => inlineLiteral(params[i++]));
}

function rustString(value: string): string {
  return `"${value
    .replaceAll('\\', '\\\\')
    .replaceAll('"', '\\"')
    .replaceAll('\n', '\\n')
    .replaceAll('\r', '\\r')
    .replaceAll('\t', '\\t')}"`;
}

/** The param-free setup statement list (drops → ddl → inlined seed → pg SERIAL fixup) the rust bench
 * cells exec at startup — emitted from the orm-domain SSoT so native + SDK seed byte-identically. */
function emitSetup(dialect: OrmDialect): void {
  const stmts = [
    ...dropStatements(dialect),
    ...ddl(dialect),
    ...seedStatements(dialect).map((s) => inlineSeed(s.sql, s.params)),
    ...(dialect === 'postgres' ? pgSeqResetStatements() : []),
  ];
  const rust = stmts.map(rustString).join(',\n    ');
  const path = join(TMP, dialect, 'generated_setup.rs');
  writeFileSync(path, `// GENERATED native setup; no serialized sidecar.\npub const STATEMENTS: &[&str] = &[\n    ${rust}\n];\n`);
  formatRust(path);
}

/** The find-hardLimit SAFETY fixture (#135/#136): a BARE find on benchmark_users compiled under
 * `setLimitConfig({findHardLimit})` — the compile bakes `LIMIT hardLimit+1` + a findGuard, so the
 * generated adapter emits the GUARDED `run` (calls the shared `check_find_hard_limit`). The seed has
 * >2 users, so `run` trips `RuntimeError::Limit` — the hardLimit proof, on the SAME native path. NOT one
 * of the 19 ops; a sqlite-only safety fixture. */
function emitCappedFixture(): void {
  const L = components();
  class CappedReads extends SemanticBehavior {
    static columns = { benchmark_users: { id: 'INTEGER', email: 'TEXT', name: 'TEXT' } } as const;
    CappedFind(_$: Record<string, never>) {
      return L.Select({ table: 'benchmark_users', select: ['id', 'email', 'name'], order: 'id ASC' });
    }
  }
  const contract = publishBehaviors(CappedReads);
  const resolve = schemaColumnTypeResolver(ddl('sqlite'));
  setLimitConfig({ findHardLimit: 2 });
  const bundle = compileBundle(contract, 'CappedFind', [], 'sqlite', undefined, resolve);
  resetLimitConfig();
  const path = join(CRATE_GEN, 'generated_cappedFindAll.rs');
  writeFileSync(path, generateRustExecutable(bundle as never, 'generated_cappedFindAll', resolve, REGISTERED));
  formatRust(path);
}

async function main() {
  const arg = process.argv[2];
  const before = arg === 'check'
    ? new Map(readdirSync(CRATE_GEN).filter((name) => name.startsWith('generated_') || name === 'mod.rs').map((name) => [name, readFileSync(join(CRATE_GEN, name), 'utf8')]))
    : new Map<string, string>();
  // bc self-registers its typed-native emitters on load; take the authoritative registered-emitter list
  // from bc itself (ESM-only → dynamic import). Same bc version dist inlines, so the ids match.
  const bc = await import('behavior-contracts');
  REGISTERED = bc.registeredLanguages();
  mkdirSync(CRATE_GEN, { recursive: true });
  let manifest: ManifestOp[] = [];
  for (const dialect of ORM_DIALECTS) {
    const m = emitDialect(dialect);
    emitSetup(dialect);
    if (dialect === 'sqlite') manifest = m;
  }
  // The committed set the bench crate compiles by default = sqlite (primary module + adapter + every
  // #140 typed-child de-box module + adapter for the op's relations).
  for (const op of manifest) {
    const src = join(TMP, 'sqlite');
    for (const f of [`generated_${op.id}.rs`]) {
      const from = join(src, f);
      const to = join(CRATE_GEN, f);
      writeFileSync(to, readFileSync(from));
    }
  }
  writeFileSync(join(CRATE_GEN, 'generated_setup.rs'), readFileSync(join(TMP, 'sqlite', 'generated_setup.rs')));
  const sdkSetupBefore = arg === 'check' ? readFileSync(SDK_SETUP, 'utf8') : undefined;
  writeFileSync(SDK_SETUP, readFileSync(join(TMP, 'sqlite', 'generated_setup.rs')));
  emitCappedFixture();
  // The gen module tree: one `mod` line per committed generated/adapter file (op ids are camelCase →
  // allow(non_snake_case)) + each op's #140 typed-child de-box modules/adapters + the cappedFindAll
  // safety fixture. Emitted so the crate never hand-maintains the module list (drift-free).
  const modLines = [
    ...manifest.map((op) => `pub mod generated_${op.id};`),
    `pub mod generated_setup;`,
    `pub mod generated_cappedFindAll;`,
  ];
  const modPath = join(CRATE_GEN, 'mod.rs');
  writeFileSync(modPath, `#![allow(non_snake_case, unused_imports, clippy::all)]\n// GENERATED by codegen-build.ts — the native ORM-bench module tree (do not edit).\n${modLines.join('\n')}\n`);
  formatRust(modPath);
  console.log(`codegen-build: emitted ${manifest.length} ops × ${ORM_DIALECTS.length} dialects → ${TMP}/<dialect>, committed sqlite → ${CRATE_GEN}`);
  if (arg === 'check') {
    const after = new Map(readdirSync(CRATE_GEN).filter((name) => name.startsWith('generated_') || name === 'mod.rs').map((name) => [name, readFileSync(join(CRATE_GEN, name), 'utf8')]));
    const names = new Set([...before.keys(), ...after.keys()]);
    const drift = [...names].filter((name) => before.get(name) !== after.get(name));
    if (sdkSetupBefore !== readFileSync(SDK_SETUP, 'utf8')) drift.push('orm_bench_sdk/src/generated_setup.rs');
    if (drift.length > 0) {
      console.error(`codegen-build: DRIFT — ${drift.join(', ')}`);
      process.exitCode = 1;
    } else {
      console.log('codegen-build: check PASS — committed native output is current');
    }
  }
}

void main();
