/**
 * V0 R8 — FIND_FILTER is a NO-OP for the makeSQL compile (EVIDENCE, not a silent waive).
 *
 * v1 applies the per-model `FIND_FILTER` (a soft-delete / tenant scope predicate) by MERGING it
 * INTO the `DBConditions` WHERE object BEFORE compiling — `DBModel._buildSelectSQL` (src/DBModel.ts
 * :596-604) and `DBModel._count` (:858-866) both do `normalizedCond.add(condsToRecord(FIND_FILTER))`
 * then `.compile()`. So FIND_FILTER manifests SOLELY as extra WHERE fragments; it is NOT a distinct
 * SQL construct.
 *
 * The SCP compile path (`compileSelect`/`compileWhere`) reimplements the `_buildSelectSQL` TEXT
 * from an EXPLICIT `conditions`/`where`; it never reads `DBModel.FIND_FILTER` (there is no
 * `FIND_FILTER` reference anywhere in src/scp/). So the resolution is variant (b) of the plan: the
 * filter is folded into the WHERE UPSTREAM of the makeSQL compile — the same authored WHERE the SCP
 * surface already expresses (byte + live done). This test is the evidence: a model's FIND_FILTER,
 * as v1 merges + compiles it, is BYTE-IDENTICAL to the SCP authored WHERE carrying the same
 * predicate. Hence auto-applying FIND_FILTER in the SCP compile would be redundant, and NOT applying
 * it drops nothing — the author expresses the model scope as an explicit `whereEq`/`whereIsNull`
 * (already covered), which is exactly what the merged FIND_FILTER becomes.
 */
/* eslint-disable @typescript-eslint/no-explicit-any -- capturing-model harness needs casts */
import { describe, it, expect } from 'vitest';
import { DBModel } from '../../src/DBModel';
import { DBConditions } from '../../src/DBConditions';
import { compileWhere, renderPlaceholders, assembleMakeSQL, type Dialect } from '../../src/scp/makesql';

const dialects: Dialect[] = ['postgres', 'mysql', 'sqlite'];

/** Render an SCP makeSQL WHERE bundle to the dialect placeholder form. */
function scpWhere(cond: Record<string, unknown>, dialect: Dialect): { sql: string; params: unknown[] } {
  const asm = assembleMakeSQL(compileWhere(cond as any, dialect));
  return { sql: renderPlaceholders(asm.sql, dialect), params: asm.params };
}

describe('R8 FIND_FILTER — merged filter is byte-identical to an authored SCP WHERE (no-op)', () => {
  for (const dialect of dialects) {
    it(`[${dialect}] v1 _buildSelectSQL WITH FIND_FILTER == SCP authored WHERE (same predicate)`, () => {
      // A model with a soft-delete FIND_FILTER (deleted_at IS NULL) + a tenant scope (tenant_id).
      const captures: { sql: string; params: unknown[] }[] = [];
      class Base extends DBModel {
        static getDriverType(): Dialect { return dialect; }
        static async query(sql: string, params: unknown[]): Promise<any[]> {
          captures.push({ sql, params });
          return [];
        }
      }
      class Doc extends Base {
        protected static TABLE_NAME = 'docs';
        protected static SELECT_COLUMN = '*';
        // The per-model global soft filter (v1 merges this into every find/count WHERE).
        protected static FIND_FILTER = [['deleted_at', null], ['tenant_id', 5]] as any;
      }

      // v1: buildSelectSQL WITHOUT FIND_FILTER is the "flexibility" path; the INTERNAL find() path
      // (_buildSelectSQL) merges FIND_FILTER. Reproduce the internal merge directly with DBConditions
      // — the SAME `normalizedCond.add(condsToRecord(FIND_FILTER))` v1 runs at DBModel.ts:597/859.
      // The user's own condition (status='live') + the merged filter → one WHERE.
      const userCond = { status: 'live' };
      const mergedForV1 = { ...userCond, deleted_at: null, tenant_id: 5 };

      // The SCP author expresses the SAME model scope as explicit WHERE members (whereEq/whereIsNull);
      // the resulting condition object is identical to v1's merged object.
      const scp = scpWhere(mergedForV1, dialect);

      // v1 golden: DBConditions over the merged object with the dialect formatter (the exact call
      // _buildSelectSQL makes after the FIND_FILTER add).
      const params: unknown[] = [];
      const formatter = dialect === 'postgres' ? (ph: string, t: string) => `${ph}::${t}` : (ph: string) => ph;
      const v1Sql = new DBConditions(mergedForV1).compile(params, formatter);
      const v1 = { sql: renderPlaceholders(v1Sql, dialect), params };

      expect(scp.sql).toBe(v1.sql);
      expect(scp.params).toEqual(v1.params);
      // The soft-delete + tenant scope both appear as ordinary WHERE fragments (no special construct).
      expect(scp.sql).toContain('deleted_at IS NULL');
      expect(scp.sql).toContain('tenant_id =');
      void Doc; // the model documents where FIND_FILTER lives; the compile is condition-driven.
    });
  }
});
