/**
 * Phase F-2 (#105) — the option-B LIVE read-contract proof.
 *
 * The README `User` shape uses BARE `@column()` for its columns (`name`/`email`: string, `id`: number,
 * `is_active`: boolean, `created_at`: Date). F1's blanket-INTEGER default typed a bare string column as
 * INTEGER, so the SCP typed-read de-box threw `materialize int32` on a live string value. Option B
 * (decorator `baseSqlType` from `design:type`, + the DBModel path's passthrough pin for ambiguous
 * numbers) fixes that: name/email read back as STRINGS, id as a NUMBER, is_active as a BOOLEAN — the
 * v1 read contract, unchanged — through the SCP compile + Phase A-E runtime (no `materialize int32`
 * throw). This test drives the PUBLIC `User.find`/`findOne`/`count` API (README code unchanged) on
 * live PG, proving the decorated ActiveRecord surface now runs on SCP end-to-end.
 *
 * Requires live PG (:5433). Bring up: `npm run docker:livedb:up`.
 */

import 'reflect-metadata';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { DBModel, model, column } from '../../src';
import type { ColumnsOf } from '../../src';
import { skipIntegrationTests, pgConfig } from '../helpers/setup';

// The README `User` shape — every column is a BARE `@column()` (the shape option B must handle).
@model('f2_users')
class F2UserModel extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column() email?: string;
  @column.boolean() is_active?: boolean;
  @column.datetime() created_at?: Date;
}
const F2User = F2UserModel as typeof F2UserModel & ColumnsOf<F2UserModel>;

describe.skipIf(skipIntegrationTests)('Phase F-2 option-B read contract (LIVE PG)', () => {
  beforeAll(async () => {
    DBModel.setConfig(pgConfig);
    await DBModel.execute('DROP TABLE IF EXISTS f2_users');
    await DBModel.execute(
      `CREATE TABLE f2_users (id SERIAL PRIMARY KEY, name TEXT, email TEXT, is_active BOOLEAN, created_at TIMESTAMP DEFAULT NOW())`,
    );
    await DBModel.transaction(async () => {
      await F2User.create([[F2User.name, 'Alice'], [F2User.email, 'alice@example.com'], [F2User.is_active, true]]);
      await F2User.create([[F2User.name, 'Bob'], [F2User.email, 'bob@example.com'], [F2User.is_active, false]]);
    });
  });

  afterAll(async () => {
    await DBModel.execute('DROP TABLE IF EXISTS f2_users');
  });

  it('find returns bare string columns as STRINGS and the id as a NUMBER (no materialize int32 throw)', async () => {
    const users = await F2User.find([[F2User.is_active, true]]);
    expect(users.length).toBe(1);
    const u = users[0];
    expect(typeof u.name).toBe('string');
    expect(u.name).toBe('Alice');
    expect(typeof u.email).toBe('string');
    expect(u.email).toBe('alice@example.com');
    expect(typeof u.id).toBe('number');
    expect(typeof u.is_active).toBe('boolean');
    expect(u.is_active).toBe(true);
    // v2 read contract: a TIMESTAMP column materializes to a TZ-attached STRING (not a JS Date) —
    // the SCP `@column.datetime()` de-box (the deliberate v2 change; the value is the driver's textual
    // form). The point here is it is well-typed and does not throw, not the Date-vs-string shape.
    expect(typeof u.created_at).toBe('string');
    expect(String(u.created_at)).toMatch(/^\d{4}-\d{2}-\d{2}/);
  });

  it('findOne + a raw-condition + IN-list read through SCP, string columns preserved', async () => {
    const alice = await F2User.findOne([[F2User.email, 'alice@example.com']]);
    expect(alice).not.toBeNull();
    expect(alice!.name).toBe('Alice');

    const both = await F2User.find([[F2User.name, ['Alice', 'Bob']] as never]);
    expect(both.length).toBe(2);
    expect(both.every((u) => typeof u.name === 'string')).toBe(true);

    // A raw-SQL custom-op condition still routes through the SCP where bridge (byte-true v1 WHERE).
    const active = await F2User.find([[`${F2User.id} >= ?`, 1] as never]);
    expect(active.length).toBe(2);
  });

  it('count returns a NUMBER through the SCP L.Count path', async () => {
    const n = await F2User.count([[F2User.is_active, false]]);
    expect(typeof n).toBe('number');
    expect(n).toBe(1);
    const all = await F2User.count([]);
    expect(all).toBe(2);
  });

  it('update + delete + findById round-trip through SCP (string columns preserved)', async () => {
    const bob = await F2User.findOne([[F2User.email, 'bob@example.com']]);
    const result = await DBModel.transaction(async () => {
      return F2User.update([[F2User.id, bob!.id!]], [[F2User.name, 'Bobby']], { returning: true });
    });
    const [updated] = await F2User.findById(result!);
    expect(updated.name).toBe('Bobby');
    expect(typeof updated.email).toBe('string');

    await DBModel.transaction(async () => {
      await F2User.delete([[F2User.email, 'bob@example.com']]);
    });
    const gone = await F2User.findOne([[F2User.email, 'bob@example.com']]);
    expect(gone).toBeNull();
  });
});
