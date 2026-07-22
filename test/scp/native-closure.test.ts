import { mkdtempSync, mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { describe, expect, it } from 'vitest';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '../..');
const RUNTIME = join(ROOT, 'rust/litedbmodel_runtime');

function crate(source: string) {
  const dir = mkdtempSync(join(tmpdir(), 'native-closure-'));
  mkdirSync(join(dir, 'src'));
  writeFileSync(join(dir, 'Cargo.toml'), `[package]\nname = "native_closure_probe"\nversion = "0.0.0"\nedition = "2021"\n\n[dependencies]\nlitedbmodel_runtime = { path = ${JSON.stringify(RUNTIME)} }\n`);
  writeFileSync(join(dir, 'src/lib.rs'), source);
  const env = {
    ...process.env,
    CARGO_NET_OFFLINE: 'true',
    CARGO_TARGET_DIR: join(tmpdir(), 'litedbmodel-native-closure-target'),
  };
  const lock = spawnSync('cargo', ['generate-lockfile', '--offline'], { cwd: dir, encoding: 'utf8', env });
  if (lock.status !== 0) return lock;
  return spawnSync('cargo', ['check', '--quiet', '--offline', '--locked'], {
    cwd: dir,
    encoding: 'utf8',
    env,
  });
}

describe('native runtime dependency closure', () => {
  it('compiles as a standalone native dependency', () => {
    // #141 retired the ir-exec interpreter's `ExecMode` enum (removed in commit 026037c together with
    // codegen_exec.rs). `StatementIntent` is the current public exec-surface type the runtime exposes;
    // referencing it proves the crate is still a usable standalone native dependency.
    expect(crate('pub fn intent() -> litedbmodel_runtime::StatementIntent { litedbmodel_runtime::StatementIntent::read() }\n').status).toBe(0);
  }, 60_000);

  it('does not expose interpreter Node', () => {
    const result = crate('pub fn node() -> litedbmodel_runtime::Node { litedbmodel_runtime::Node::Null }\n');
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain('cannot find type `Node`');
  }, 60_000);
});
