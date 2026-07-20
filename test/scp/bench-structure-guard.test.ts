import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { describe, expect, it } from 'vitest';

describe('bench structure guard fail-closed behavior', () => {
  it('fails when a copied generated source is injected with serialized relation metadata', () => {
    const dir = mkdtempSync(join(tmpdir(), 'relation-guard-negative-'));
    const injected = join(dir, 'generated_injected.rs');
    writeFileSync(injected, 'fn bad() { let _ = serde_json::from_str::<Value>("{}"); }\n');
    const result = spawnSync(
      process.execPath,
      ['--import', 'tsx', 'benchmark/crosslang/bench-structure-guard.ts'],
      { cwd: process.cwd(), encoding: 'utf8', env: { ...process.env, LITEDB_GUARD_EXTRA: injected } },
    );
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain('runtime serialized parser');
  });
});
