import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { describe, expect, it } from 'vitest';

describe('codegen-build check', () => {
  it('fails on generated drift and restores the canonical output', () => {
    const file = 'rust/orm_bench/src/gen/generated_setup.rs';
    const canonical = readFileSync(file, 'utf8');
    appendFileSync(file, '\n// injected drift\n');
    const result = spawnSync(process.execPath, ['--import', 'tsx', 'benchmark/crosslang/codegen-build.ts', 'check'], {
      cwd: process.cwd(), encoding: 'utf8',
    });
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain('DRIFT');
    expect(readFileSync(file, 'utf8')).toBe(canonical);
    writeFileSync(file, canonical);
  });
});
