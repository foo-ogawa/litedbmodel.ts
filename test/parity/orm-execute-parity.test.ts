/**
 * #67 / #63 — EXECUTE-parity gate.
 *
 * The #65 v2-SCP↔golden parity test only STRING-COMPARES the emitted SQL. That let a
 * runtime-invalid statement pass: the SQLite batch-upsert (`INSERT … SELECT … FROM
 * json_each(?) ON CONFLICT …`) was byte-correct against the intended form yet REJECTED
 * by SQLite's parser at execution time (`near "DO": syntax error`, missing `WHERE true`).
 *
 * This gate RUNS every one of the 19 ORM-comparison ops through the shipped compile path
 * (benchmark/crosslang/orm-plan.ts → the SAME statements the ORM-bench litedbmodel column
 * emits) against a REAL database for ALL THREE dialects (sqlite in-proc, mysql :3307,
 * postgres :5433). A parser-rejected or runtime-failing statement now FAILS the suite —
 * string-compare (#65) + real-execution (here) together.
 *
 * PARITY PROOF ⇒ NO SILENT SKIP: sqlite always runs; if mysql/postgres are unreachable the
 * suite FAILS (bring the DBs up: `npm run docker:livedb:up`), matching test/scp/
 * json-array-parity.test.ts's requireX convention.
 *
 * Also asserts rows/op per read op is IDENTICAL across the three dialects (same logical
 * work — the TS-vs-ORM consistency anchor: same op, same v2 SQL, same rows).
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';

import { buildOrmPlanArtifact, ORM_OPS, type OrmDialect } from '../../benchmark/crosslang/orm-plan.js';
import { sqliteDriver, pgDriver, mysqlDriver, type OrmDriver } from '../../benchmark/crosslang/orm-exec-ts.js';
import {
  PG_SCHEMA_NAME, MYSQL_DB_NAME, PG_CONN, PG_BOOT_CONN, MYSQL_CONN, MYSQL_BOOT_CONN,
} from '../../benchmark/crosslang/domain.js';

const art = buildOrmPlanArtifact();

// Expected rows/op (reads) — the logical work the ORM-bench litedbmodel column does against
// the shared seed (110 users + user500, 2 posts/user, 2 comments/post, 5 tenants ×4 ×2 ×2).
// Writes report statements executed. Identical across all three dialects.
const EXPECTED: Record<string, number> = {
  findAll: 100,
  filterPaginateSort: 20,
  nestedFindAll: 300,
  findFirst: 1,
  nestedFindFirst: 3,
  findUnique: 1,
  nestedFindUnique: 1,
  create: 1,
  nestedCreate: 2,
  update: 1,
  nestedUpdate: 2,
  upsert: 1,
  nestedUpsert: 2,
  delete: 2,
  createMany: 1,
  upsertMany: 1,
  updateMany: 1,
  nestedRelations: 700,
  compositeRelations: 140,
};

const drivers: Partial<Record<OrmDialect, OrmDriver>> = {};
const connectErr: Partial<Record<OrmDialect, string>> = {};

beforeAll(async () => {
  drivers.sqlite = sqliteDriver();
  try {
    drivers.postgres = await pgDriver(PG_SCHEMA_NAME, PG_CONN as never, PG_BOOT_CONN as never);
  } catch (e) {
    connectErr.postgres = e instanceof Error ? e.message.split('\n')[0] : String(e);
  }
  try {
    drivers.mysql = await mysqlDriver(MYSQL_DB_NAME, MYSQL_CONN as never, MYSQL_BOOT_CONN as never);
  } catch (e) {
    connectErr.mysql = e instanceof Error ? e.message.split('\n')[0] : String(e);
  }
}, 60_000);

afterAll(async () => {
  for (const d of Object.values(drivers)) if (d) await d.close();
});

const DIALECTS: OrmDialect[] = ['sqlite', 'mysql', 'postgres'];

describe('#67/#63 EXECUTE-parity — all 19 ORM ops run DB-backed on all 3 dialects', () => {
  it('the plan artifact covers exactly the 19 ORM ops (no subset)', () => {
    expect(ORM_OPS.length).toBe(19);
    expect(Object.keys(art).length).toBe(19);
  });

  for (const dialect of DIALECTS) {
    describe(`[${dialect}]`, () => {
      // A parity PROOF: an unreachable required DB FAILS (never a silent skip).
      it(`${dialect} is reachable`, () => {
        if (dialect === 'sqlite') return;
        if (!drivers[dialect]) {
          throw new Error(
            `[orm-execute-parity] ${dialect} is REQUIRED for this execute-parity proof but is unreachable ` +
              `(${connectErr[dialect]}). Bring it up: npm run docker:livedb:up`,
          );
        }
      });

      for (const op of ORM_OPS) {
        it(`${op.id} — ${op.label} executes + expected rows/op`, async () => {
          const drv = drivers[dialect];
          if (!drv) throw new Error(`${dialect} driver not connected — cannot run ${op.id}`);
          // A parser-rejected / runtime-failing statement throws here → the gate FAILS.
          const n = await drv.run(art[op.id][dialect]);
          expect(n, `${op.id} rows/op (${dialect})`).toBe(EXPECTED[op.id]);
        });
      }
    });
  }
});
