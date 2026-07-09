#!/usr/bin/env node
// Guard: no dependency may reference a filesystem path OUTSIDE this repository (WS7a, #30).
//
// Why: during pre-publish development we sometimes link a sibling repo (behavior-contracts) via a
// local path — `file:../behavior-contracts/ts` (npm), `path = "../../behavior-contracts/rust"`
// (cargo), `replace => ../` (go), `@ file://…` / `-e ../` (pip), `path` repository (composer).
// Those resolve locally (the sibling checkout exists) but BREAK in CI and in any published
// artifact, where no sibling checkout is present. This class of bug is invisible to normal local
// builds.
//
// The rule is precise: a path dependency that stays INSIDE the repo (a cargo workspace member
// like `path = "../litedbmodel_runtime"`, a package `exports` self-reference) is fine; only paths
// that ESCAPE the repo root are flagged.
//
// The five language runtimes consume behavior-contracts from PUBLISHED coordinates only:
//   npm  -> behavior-contracts@^0.2.0            (published)
//   PyPI -> behavior-contracts==0.2.0            (published)
//   crates.io -> behavior-contracts = "0.2.0"    (published)
//   Go   -> github.com/foo-ogawa/behavior-contracts/go v0.2.0  (VCS tag; private repo, GOPRIVATE)
//   PHP  -> VENDORED into php/src/BehaviorContracts (not a dep at all; drift-gated copy)
//
// Runs as a CI gate and a git pre-push hook. To intentionally keep a local sibling link during
// development, set ALLOW_LOCAL_DEPS=1 (loud notice, exit 0).
//
// Usage: node scripts/check-no-local-deps.mjs   (exit 1 on any escaping ref)

import { readFileSync, existsSync, globSync } from 'node:fs';
import { dirname, join, resolve, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(join(dirname(fileURLToPath(import.meta.url)), '..'));
const findings = [];

/** True if resolving `p` relative to `baseAbs` lands outside the repo root. */
function escapesRepo(baseAbs, p) {
  const abs = resolve(baseAbs, p);
  const rel = relative(ROOT, abs);
  return rel === '..' || rel.startsWith('..' + sep);
}

/** Strip a dependency-spec scheme (file:/link:) → the bare path, or null. */
function pathOf(spec) {
  const m = /^(?:file:|link:)?(\.\.?\/.*|\/.*|\.\.?)$/.exec(spec.trim());
  return m ? m[1] : null;
}

function flag(relFile, lineNo, text, why) {
  findings.push({ file: relFile, line: lineNo, text: text.trim(), why });
}

/** Find the 1-indexed line of `needle` in a file's text (for JSON findings). */
function lineOf(text, needle) {
  const idx = text.indexOf(needle);
  if (idx < 0) return 0;
  return text.slice(0, idx).split('\n').length;
}

// ── npm: package.json dependency sections (NOT exports/main/types) ──────────
{
  const rel = 'package.json';
  const abs = join(ROOT, rel);
  if (existsSync(abs)) {
    const raw = readFileSync(abs, 'utf8');
    const pkg = JSON.parse(raw);
    for (const section of ['dependencies', 'devDependencies', 'peerDependencies', 'optionalDependencies']) {
      for (const [name, spec] of Object.entries(pkg[section] ?? {})) {
        const p = typeof spec === 'string' ? pathOf(spec) : null;
        if (p && escapesRepo(ROOT, p)) {
          flag(rel, lineOf(raw, `"${name}"`), `"${name}": "${spec}"`, `npm ${section} escapes the repo`);
        }
      }
    }
  }
}

// ── npm: package-lock.json (link deps + resolved paths that escape) ─────────
{
  const rel = 'package-lock.json';
  const abs = join(ROOT, rel);
  if (existsSync(abs)) {
    const raw = readFileSync(abs, 'utf8');
    const lock = JSON.parse(raw);
    for (const [key, node] of Object.entries(lock.packages ?? {})) {
      const res = typeof node?.resolved === 'string' ? node.resolved : null;
      const p = res ? pathOf(res) : null;
      if ((p && escapesRepo(ROOT, p)) || (node?.link && key && escapesRepo(ROOT, key))) {
        flag(rel, lineOf(raw, res ?? key), `${key} → ${res ?? '(link)'}`, 'npm lock resolves a dependency outside the repo');
      }
    }
  }
}

// ── cargo: every Cargo.toml `path = "…"` that escapes the repo ──────────────
for (const toml of globSync('rust/**/Cargo.toml', { cwd: ROOT }).filter((p) => !p.includes(`${sep}target${sep}`) && !p.includes('/target/'))) {
  const abs = join(ROOT, toml);
  const lines = readFileSync(abs, 'utf8').split('\n');
  lines.forEach((line, i) => {
    if (/^\s*#/.test(line)) return;
    const m = /\bpath\s*=\s*"([^"]+)"/.exec(line);
    if (m && escapesRepo(dirname(abs), m[1])) flag(toml, i + 1, line, 'cargo path dependency escapes the repo');
  });
}

// ── go: replace directives whose target escapes the repo ───────────────────
{
  const rel = 'go/go.mod';
  const abs = join(ROOT, rel);
  if (existsSync(abs)) {
    readFileSync(abs, 'utf8').split('\n').forEach((line, i) => {
      if (/^\s*\/\//.test(line)) return;
      const m = /^\s*replace\s+\S+\s+(?:\S+\s+)?=>\s+(\S+)/.exec(line);
      if (m && /^(\.\.?\/|\/)/.test(m[1]) && escapesRepo(join(ROOT, 'go'), m[1])) {
        flag(rel, i + 1, line, 'go.mod replace targets a path outside the repo');
      }
    });
  }
}

// ── composer: a `repositories` path entry that escapes the repo ─────────────
{
  const rel = 'php/composer.json';
  const abs = join(ROOT, rel);
  if (existsSync(abs)) {
    const raw = readFileSync(abs, 'utf8');
    const composer = JSON.parse(raw);
    for (const r of composer.repositories ?? []) {
      if (r && r.type === 'path' && typeof r.url === 'string' && escapesRepo(join(ROOT, 'php'), r.url)) {
        flag(rel, lineOf(raw, r.url), `repository path "${r.url}"`, 'composer path repository escapes the repo');
      }
    }
  }
}

// ── python + CI workflows: local editable / file installs ──────────────────
scanRegexEscaping('python/pyproject.toml', join(ROOT, 'python'), /@\s*file:\/\/(\S+)|path\s*=\s*"([^"]+)"/);
for (const wf of globSync('.github/workflows/*.yml', { cwd: ROOT })) {
  scanRegexEscaping(wf, ROOT, /pip install[^\n]*?\s(?:-e\s+)?((?:\.\.?)\/\S+|file:\/\/\S+)/);
}

function scanRegexEscaping(relFile, baseAbs, re) {
  const abs = join(ROOT, relFile);
  if (!existsSync(abs)) return;
  readFileSync(abs, 'utf8').split('\n').forEach((line, i) => {
    if (/^\s*(#|\/\/)/.test(line)) return;
    const m = re.exec(line);
    const p = m && (m[1] ?? m[2]);
    if (p && escapesRepo(baseAbs, p.replace(/^file:\/\//, ''))) flag(relFile, i + 1, line, 'dependency installs from a path outside the repo');
  });
}

// ── report ──────────────────────────────────────────────────────────────
if (findings.length === 0) {
  console.log('✓ no-local-deps: no dependency references escape the repository.');
  process.exit(0);
}
const allow = process.env.ALLOW_LOCAL_DEPS === '1';
console.error(`\n${allow ? '⚠️ ' : '✗ '}no-local-deps: ${findings.length} dependency reference(s) escape the repository:\n`);
for (const f of findings) {
  console.error(`  ${f.file}:${f.line}  — ${f.why}`);
  console.error(`      ${f.text}`);
}
if (allow) {
  console.error('\nALLOW_LOCAL_DEPS=1 — permitting local sibling links (dev mode). Do NOT push/release in this state.');
  process.exit(0);
}
console.error('\nThese resolve locally but break in CI and in published artifacts.');
console.error('Repin to published coordinates before pushing (or ALLOW_LOCAL_DEPS=1 for intentional local dev linking).');
process.exit(1);
