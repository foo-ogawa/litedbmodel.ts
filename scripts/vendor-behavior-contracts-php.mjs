#!/usr/bin/env node
/**
 * Vendor the behavior-contracts PHP port into litedbmodel (WS7d, #33).
 *
 * The behavior-contracts PHP port (`behavior-contracts/php/src/*.php`) is the SSoT for
 * litedbmodel's PHP runtime-core (the Expression-IR evaluator + the component-graph
 * `runBehavior` execution engine the render/exec/tx paths sit on). Unlike the
 * TS/Python/Rust/Go runtimes — which consume behavior-contracts from
 * npm/PyPI/crates/VCS-tag — PHP is NOT published to any registry (owner decision,
 * foo-ogawa/behavior-contracts#7). litedbmodel therefore consumes it by VENDORING a
 * mechanical copy into `php/src/BehaviorContracts/` behind this sync script + a CI drift
 * gate (the established graphddb pattern — see graphddb/scripts/vendor-behavior-contracts-php.mjs).
 *
 * Discipline (mirrors graphddb):
 *   - SSoT is behavior-contracts/php/src (never hand-edit the vendored copy).
 *   - This script copies the needed files, rewriting the PHP namespace
 *     `BehaviorContracts` -> `LiteDbModel\Runtime\BehaviorContracts` so the vendored
 *     classes autoload under litedbmodel's PSR-4 root (`LiteDbModel\Runtime\` -> src/),
 *     and stamps a "generated — do not edit" provenance header.
 *   - It writes `php/src/BehaviorContracts/VENDOR_MANIFEST.json` (per-file SHA256 of the
 *     TRANSFORMED output + upstream provenance). The drift gate recomputes the transform
 *     in-memory and compares, so a diverged vendored copy — or an upstream change that was
 *     not re-vendored — fails CI.
 *
 * Usage:
 *   node scripts/vendor-behavior-contracts-php.mjs           # (re)vendor from source
 *   node scripts/vendor-behavior-contracts-php.mjs --check   # CI: exit non-zero on drift, no writes
 *
 * Source location (SSoT) resolution order:
 *   $BEHAVIOR_CONTRACTS_PHP_SRC  ->  <litedbmodel>/../behavior-contracts/php/src
 */

import { readFileSync, writeFileSync, mkdirSync, readdirSync, existsSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dirname, resolve, join } from 'node:path';

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(scriptDir, '..');

// The bc port modules litedbmodel's PHP runtime needs. litedbmodel's thin runtime consumes
// bc runtime-core for BOTH:
//   - Expression-IR evaluation (`ExprEval::evaluate`) — the render pipeline's param-slot /
//     SKIP-`when` value evaluation (spec §8 closed operator set), AND
//   - the unified component-graph execution (`Behavior::runBehavior` + `Plan::runPlan` — stage
//     execution / Skip propagation / Policy Kind / map iteration / Φ output assembly), the SAME
//     shared engine the TS/Python/Rust/Go runtimes use.
// `Codec`/`Envelope`/`SpecVersions`/`Fingerprint`/`template`/`canonical` are NOT in
// litedbmodel's runtime closure (verified: no `Codec::`/`Envelope::`/… call sites in the
// vendored set) and are intentionally not vendored.
const VENDORED_FILES = [
  // Namespace-level shared constants (bc 0.2.3, foo-ogawa/behavior-contracts#… split
  // FORBIDDEN_OBJECT_KEY out of ExprEval.php into its own file-scope const so ExprEval and
  // Codec share one SSoT regardless of class-autoload order). ExprEval.php below references
  // the bare `FORBIDDEN_OBJECT_KEY` constant, which a file-scope const in a PSR-4-only
  // package would NOT auto-define — so this file is also wired into composer's `files`
  // autoload (php/composer.json) exactly as upstream bc wires its own.
  'Constants.php',
  'ExprFailure.php',
  'ExprEval.php',
  'PlanFailure.php',
  'Plan.php',
  'BehaviorFailure.php',
  'Behavior.php',
];

const SRC_NAMESPACE = 'BehaviorContracts';
const DST_NAMESPACE = 'LiteDbModel\\Runtime\\BehaviorContracts';

const destDir = resolve(repoRoot, 'php/src/BehaviorContracts');
const manifestName = 'VENDOR_MANIFEST.json';

function sourceDir() {
  const env = process.env.BEHAVIOR_CONTRACTS_PHP_SRC;
  if (env) return resolve(env);
  return resolve(repoRoot, '..', 'behavior-contracts', 'php', 'src');
}

function sha256(s) {
  return createHash('sha256').update(s, 'utf8').digest('hex');
}

/** Best-effort upstream provenance (git describe of the behavior-contracts repo). */
function upstreamProvenance(srcDir) {
  const repo = resolve(srcDir, '..', '..'); // php/src -> repo root
  const info = { path: srcDir };
  try {
    info.version = readFileSync(resolve(repo, 'VERSION'), 'utf8').trim();
  } catch {
    // VERSION optional
  }
  try {
    info.commit = execFileSync('git', ['-C', repo, 'rev-parse', 'HEAD'], {
      encoding: 'utf8',
    }).trim();
  } catch {
    // not a git repo / git unavailable — provenance is advisory only
  }
  return info;
}

/**
 * The deterministic vendoring transform applied to each source file. MUST be pure and stable
 * so the drift gate reproduces byte-identical output.
 */
function transform(file, raw) {
  // 1. Rewrite the file's own `namespace BehaviorContracts;` declaration.
  let out = raw.replace(/^namespace\s+BehaviorContracts;/m, `namespace ${DST_NAMESPACE};`);
  // 2. Rewrite fully-qualified `\BehaviorContracts\...` references (none expected today — the
  //    port uses same-namespace short names — but keep it robust).
  out = out.replace(/\\BehaviorContracts\\/g, `\\${DST_NAMESPACE}\\`);

  // 3. Stamp a generated-file provenance header right after the opening <?php.
  const banner =
    '\n/**\n' +
    ' * !!! VENDORED — DO NOT EDIT !!!\n' +
    ' *\n' +
    ` * Mechanically vendored from behavior-contracts/php/src/${file} by\n` +
    ' * scripts/vendor-behavior-contracts-php.mjs (litedbmodel#33). The SSoT is the\n' +
    ' * behavior-contracts repo; edit there and re-run the vendoring script. A CI drift\n' +
    ' * gate (npm run vendor:bc-php:check) fails if this copy diverges.\n' +
    ' */\n';
  out = out.replace(/^<\?php\n/, `<?php\n${banner}`);
  return out;
}

function readSourceFiles(srcDir) {
  if (!existsSync(srcDir)) {
    console.error(
      `Source not found: ${srcDir}\n` +
        'Set $BEHAVIOR_CONTRACTS_PHP_SRC or place the behavior-contracts repo as a sibling.',
    );
    process.exit(2);
  }
  const present = new Set(readdirSync(srcDir));
  const files = {};
  for (const f of VENDORED_FILES) {
    if (!present.has(f)) {
      console.error(`Source is missing required port file: ${f} (in ${srcDir})`);
      process.exit(2);
    }
    files[f] = transform(f, readFileSync(join(srcDir, f), 'utf8'));
  }
  return files;
}

function buildManifest(transformed, provenance) {
  const filesHashes = {};
  for (const f of VENDORED_FILES) filesHashes[f] = sha256(transformed[f]);
  return {
    generator: 'scripts/vendor-behavior-contracts-php.mjs',
    upstream: provenance,
    namespace: DST_NAMESPACE,
    files: filesHashes,
  };
}

function manifestBody(manifest) {
  return JSON.stringify(manifest, null, 2) + '\n';
}

const args = process.argv.slice(2);
const check = args.includes('--check');

const srcDir = sourceDir();
const transformed = readSourceFiles(srcDir);
const provenance = upstreamProvenance(srcDir);
const manifest = buildManifest(transformed, provenance);

if (check) {
  // Drift gate: the vendored files + their manifest hashes must equal what a fresh vendoring
  // from source would produce. Compares CONTENT (authoritative) and the manifest hash table.
  const problems = [];
  for (const f of VENDORED_FILES) {
    const dst = join(destDir, f);
    if (!existsSync(dst)) {
      problems.push(`missing vendored file: php/src/BehaviorContracts/${f}`);
      continue;
    }
    const have = readFileSync(dst, 'utf8');
    if (have !== transformed[f]) {
      problems.push(
        `drift: php/src/BehaviorContracts/${f} differs from behavior-contracts/php/src/${f} (after vendoring transform)`,
      );
    }
  }
  const manPath = join(destDir, manifestName);
  if (!existsSync(manPath)) {
    problems.push(`missing manifest: php/src/BehaviorContracts/${manifestName}`);
  } else {
    const haveMan = JSON.parse(readFileSync(manPath, 'utf8'));
    for (const f of VENDORED_FILES) {
      if (haveMan.files?.[f] !== manifest.files[f]) {
        problems.push(`manifest hash drift for ${f}`);
      }
    }
  }

  if (problems.length) {
    console.error(`❌ behavior-contracts PHP vendor drift — ${problems.length} problem(s):`);
    for (const p of problems) console.error('  - ' + p);
    console.error(
      '\nThe vendored PHP copy is out of sync with the behavior-contracts SSoT.\n' +
        'Re-vendor with `npm run vendor:bc-php` and commit the result (never hand-edit\n' +
        'php/src/BehaviorContracts/).',
    );
    process.exit(1);
  }
  console.log(
    '✅ behavior-contracts PHP vendor in sync (' +
      VENDORED_FILES.length +
      ' files hash-match the upstream SSoT).',
  );
  process.exit(0);
}

// Write mode: (re)generate the vendored copy + manifest.
mkdirSync(destDir, { recursive: true });
for (const f of VENDORED_FILES) {
  writeFileSync(join(destDir, f), transformed[f], 'utf8');
  console.log(`vendored php/src/BehaviorContracts/${f}`);
}
writeFileSync(join(destDir, manifestName), manifestBody(manifest), 'utf8');
console.log(`wrote php/src/BehaviorContracts/${manifestName}`);
console.log(
  `\nDone. Upstream: ${provenance.version ?? '(no VERSION)'} @ ${
    provenance.commit ? provenance.commit.slice(0, 12) : '(no commit)'
  }`,
);
