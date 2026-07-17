// ════════════════════════════════════════════════════════════════════════════
// Artifact generation — the shared ORM-plan artifact.
// ════════════════════════════════════════════════════════════════════════════
//
// The unified bench consumes ONE generated artifact: generated/orm-plan.json (the
// 19 ORM ops' per-dialect statement plans, built from the v2 SCP compile path).
//
// This module is a thin shim that delegates to gen-orm-plan.ts (the real
// generator + --check drift gate).
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
