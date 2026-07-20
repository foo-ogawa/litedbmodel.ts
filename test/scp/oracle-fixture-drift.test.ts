import { existsSync, unlinkSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { describe, expect, it } from 'vitest';

const stale = 'rust/litedbmodel_oracle/src/generated/stale_extra.rs';
const run = (...args: string[]) => spawnSync(
  process.execPath,
  ['--import', 'tsx', 'benchmark/crosslang/oracle-fixture-build.ts', ...args],
  { cwd: process.cwd(), encoding: 'utf8' },
);

describe('oracle fixture artifact-set drift', () => {
  it('rejects a stale Rust artifact and canonical regeneration removes it', () => {
    try {
      writeFileSync(stale, '// stale generated artifact\n');
      const rejected = run('check');
      expect(rejected.status).not.toBe(0);
      expect(rejected.stderr).toContain('unexpected: stale_extra.rs');

      expect(run().status).toBe(0);
      expect(existsSync(stale)).toBe(false);
      expect(run('check').status).toBe(0);
    } finally {
      if (existsSync(stale)) unlinkSync(stale);
    }
  }, 60_000);
});
