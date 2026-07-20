import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { describe, expect, it } from 'vitest';

describe('native pilot fails closed on oracle failure', () => {
  it('preserves the oracle exit status and restores sqlite generated modules', () => {
    const generated = 'rust/orm_bench/src/gen/generated_nestedFindAll.rs';
    const before = readFileSync(generated, 'utf8');
    const postgres = '/tmp/ormbench/postgres';
    const sqlite = '/tmp/ormbench/sqlite';
    mkdirSync(postgres, { recursive: true });
    mkdirSync(sqlite, { recursive: true });
    writeFileSync(join(postgres, 'generated_nestedFindAll.rs'), `${before}\n// failure-probe dialect overlay\n`);
    writeFileSync(join(sqlite, 'generated_nestedFindAll.rs'), before);

    const result = spawnSync('bash', ['rust/orm_bench/run-pilot.sh'], {
      cwd: process.cwd(),
      encoding: 'utf8',
      env: { ...process.env, LITEDB_ORACLE_FORCE_FAIL: '1' },
    });

    expect(result.status).toBe(97);
    expect(readFileSync(generated, 'utf8')).toBe(before);
  });
});
