import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { describe, expect, it } from 'vitest';

describe('bench structure guard fail-closed behavior', () => {
  const rejects = (source: string, expected: string) => {
    const dir = mkdtempSync(join(tmpdir(), 'relation-guard-negative-'));
    const injected = join(dir, 'generated_injected.rs');
    writeFileSync(injected, source);
    const result = spawnSync(
      process.execPath,
      ['--import', 'tsx', 'benchmark/crosslang/bench-structure-guard.ts'],
      { cwd: process.cwd(), encoding: 'utf8', env: { ...process.env, LITEDB_GUARD_EXTRA: injected } },
    );
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain(expected);
  };

  it('rejects serialized relation metadata', () => {
    rejects('fn bad() { let _ = serde_json::from_str::<Value>("{}"); }\n', 'runtime serialized parser');
  });

  it('rejects a serialized SDK setup input', () => {
    rejects('fn bad() { let _ = std::fs::read_to_string("/tmp/setup.json"); }\n', 'serialized setup file input');
  });

  it('rejects a generated interpreter dependency', () => {
    rejects('fn bad() { let _ = litedbmodel_interpreter::Node::Null; }\n', 'crosses interpreter boundary');
  });

  it('rejects a dummy reference for any generated variable', () => {
    rejects('fn bad() { let oel_n1 = 1; let _ = &oel_n1; }\n', 'dummy generated-variable reference');
  });

  it('rejects an unused generated produced flag', () => {
    rejects('fn bad() { let produced_n0 = std::cell::Cell::new(false); produced_n0.set(true); }\n', 'unused produced flag');
  });

  it('rejects retired sidecar metadata', () => {
    rejects(`fn bad() { com${'panion'}Of("op"); }\n`, 'retired sidecar metadata API');
  });
});
