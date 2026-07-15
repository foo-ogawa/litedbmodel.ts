// ════════════════════════════════════════════════════════════════════════════
// Artifact generation (epic #63) — the shared ORM-plan artifact.
// ════════════════════════════════════════════════════════════════════════════
//
// The unified bench (#63) consumes ONE generated artifact: generated/orm-plan.json (the
// 19 ORM ops' per-dialect statement plans, built from the v2 SCP compile path). The old
// #44 8-behavior makeSQL bundles + bc typed-native codegen modules are retired (the
// codegen-module cells covered none of the 19 ORM ops).
//
// This module is a thin compatibility shim that delegates to gen-orm-plan.ts (the real
// generator + --check drift gate), so existing callers (`generate.ts` / `generate.ts --check`)
// keep working against the new artifact.
//
//   generate:     npx tsx benchmark/crosslang/generate.ts
//   drift-check:  npx tsx benchmark/crosslang/generate.ts --check

import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const GEN = resolve(HERE, 'gen-orm-plan.ts');
const REPO = resolve(HERE, '..', '..');

const args = process.argv.slice(2).filter((a) => a === '--check');
execFileSync('npx', ['tsx', GEN, ...args], { cwd: REPO, stdio: 'inherit' });
