#!/usr/bin/env node
/**
 * Single source of truth for the release version is package.json (WS7a, #30). Every language
 * runtime that ships on its own registry must track that version so the generated §8 bundle and
 * the runtime that interprets it stay in lockstep across all 5 registries:
 *
 *   - python/pyproject.toml                        -> PyPI       (litedbmodel-runtime)
 *   - rust/litedbmodel_runtime/Cargo.toml          -> crates.io  (litedbmodel_runtime)
 *   - rust/vectors_runner/Cargo.toml               -> (workspace member; version-locked)
 *   - go/litedbmodel_runtime/runtime.go `Version`  -> Go module VCS tag `go/v<version>`
 *   - php/src/Runtime.php `VERSION`                -> Packagist  (litedbmodel/runtime)
 *
 * (npm itself is package.json, the SSoT — nothing to sync.)
 *
 * This script copies package.json's `version` into each target. It runs in the publish workflows
 * before building and can be run by hand after bumping package.json:
 *
 *   npm run sync:versions
 *
 * With `--check` it exits non-zero if ANY target is out of sync instead of rewriting (CI drift
 * gate). NO ad-hoc default: the version is read from the SSoT (package.json) and every target
 * MUST already carry a recognizable version marker, else the script fails loudly.
 */

import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const version = JSON.parse(readFileSync(resolve(root, 'package.json'), 'utf8')).version;
const check = process.argv.includes('--check');

/**
 * Each target: a file + the regex whose FIRST capture group is the version to replace, and a
 * `render(v)` producing the full replacement line. The regex must match exactly one place (the
 * package/manifest version marker), never a dependency spec.
 */
const targets = [
  {
    label: 'python/pyproject.toml',
    path: resolve(root, 'python/pyproject.toml'),
    // `[project]` version is a bare `version = "..."` at line start (dependency versions are
    // inside `dependencies = [...]`, never at line start).
    re: /^version = "([^"]*)"$/m,
    render: (v) => `version = "${v}"`,
  },
  {
    label: 'rust/litedbmodel_runtime/Cargo.toml',
    path: resolve(root, 'rust/litedbmodel_runtime/Cargo.toml'),
    // `[package]` version at line start (dependency versions are inline `{ version = "..." }`).
    re: /^version = "([^"]*)"$/m,
    render: (v) => `version = "${v}"`,
  },
  {
    label: 'rust/vectors_runner/Cargo.toml',
    path: resolve(root, 'rust/vectors_runner/Cargo.toml'),
    re: /^version = "([^"]*)"$/m,
    render: (v) => `version = "${v}"`,
  },
  {
    label: 'go/litedbmodel_runtime/runtime.go',
    path: resolve(root, 'go/litedbmodel_runtime/runtime.go'),
    // `const Version = "..."` — Go publishes by tag, so the constant is the in-source mirror.
    re: /const Version = "([^"]*)"/,
    render: (v) => `const Version = "${v}"`,
  },
  {
    label: 'php/src/Runtime.php',
    path: resolve(root, 'php/src/Runtime.php'),
    // `public const VERSION = '...'` (Packagist reads the git tag; this is the in-source mirror).
    re: /public const VERSION = '([^']*)'/,
    render: (v) => `public const VERSION = '${v}'`,
  },
];

let drift = false;

for (const { label, path, re, render } of targets) {
  const contents = readFileSync(path, 'utf8');
  const m = re.exec(contents);
  if (!m) {
    console.error(`Could not find a version marker in ${label} (pattern ${re})`);
    process.exit(1);
  }
  const current = m[1];
  if (current === version) {
    console.log(`${label} already at ${version}`);
    continue;
  }
  if (check) {
    console.error(`Version drift: package.json=${version}, ${label} has ${current}`);
    drift = true;
    continue;
  }
  writeFileSync(path, contents.replace(re, render(version)), 'utf8');
  console.log(`Synced ${label} to ${version}`);
}

if (drift) {
  console.error('Run `npm run sync:versions` and commit the result.');
  process.exit(1);
}
