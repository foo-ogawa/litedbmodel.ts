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

  it('rejects a generated interpreter dependency', () => {
    rejects('fn bad() { let _ = litedbmodel_interpreter::Node::Null; }\n', 'crosses interpreter boundary');
  });

  it('rejects retired companion metadata', () => {
    rejects('fn bad() { companionOf("op"); }\n', 'retired companion metadata API');
  });
});
