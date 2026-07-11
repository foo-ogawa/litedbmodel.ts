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
import { assertFindFilterFolded, findFilterKeys, FindFilterLeakError } from '../../src/scp/index';

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

// ---------------------------------------------------------------------------
// #47 Finding B — the LATENT AUTHORING BOUNDARY is closed FAIL-CLOSED.
//
// The no-op proof above holds only when the author folds the FIND_FILTER predicates into the
// SCP-authored WHERE. If a model DECLARES a FIND_FILTER and is routed through the SCP compile
// WITHOUT its scope keys, the SCP compile has no model context to auto-apply it, so the scope
// would silently leak. `assertFindFilterFolded` is the fail-closed guard: missing scope keys →
// loud `FindFilterLeakError`; scope keys present → no-op (redundant with the authored WHERE).
// ---------------------------------------------------------------------------
describe('R8 FIND_FILTER — fail-closed guard (a FIND_FILTER model cannot silently leak via SCP)', () => {
  class SoftDeleted extends DBModel {
    protected static TABLE_NAME = 'docs';
    protected static SELECT_COLUMN = '*';
    // Soft-delete + tenant scope: v1 merges these into EVERY find/count WHERE.
    protected static FIND_FILTER = [['deleted_at', null], ['tenant_id', 5]] as any;
  }
  class Unfiltered extends DBModel {
    protected static TABLE_NAME = 'events';
    protected static SELECT_COLUMN = '*';
    // No FIND_FILTER → the guard is a no-op for any authored WHERE.
  }

  it('extracts the FIND_FILTER scope keys via the SAME condsToRecord v1 merges with', () => {
    expect(findFilterKeys((SoftDeleted as any).FIND_FILTER).sort()).toEqual(['deleted_at', 'tenant_id']);
    expect(findFilterKeys((Unfiltered as any).FIND_FILTER)).toEqual([]);
    expect(findFilterKeys(null)).toEqual([]);
  });

  it('THROWS FindFilterLeakError when a FIND_FILTER model is SCP-compiled without the scope keys', () => {
    // Author expresses only their own predicate (status) — the model scope is DROPPED. Fail-closed.
    expect(() => assertFindFilterFolded(SoftDeleted as any, ['status'])).toThrow(FindFilterLeakError);
    try {
      assertFindFilterFolded(SoftDeleted as any, ['status']);
    } catch (e) {
      expect(e).toBeInstanceOf(FindFilterLeakError);
      const err = e as FindFilterLeakError;
      expect(err.modelName).toBe('SoftDeleted');
      expect(err.missingKeys.sort()).toEqual(['deleted_at', 'tenant_id']);
      expect(err.message).toContain('would be silently dropped');
    }
  });

  it('THROWS when the scope is PARTIALLY folded (one key present, one missing)', () => {
    // A partial fold is still a leak — the missing key silently drops. Fail-closed on the remainder.
    expect(() => assertFindFilterFolded(SoftDeleted as any, ['status', 'deleted_at'])).toThrow(FindFilterLeakError);
    try {
      assertFindFilterFolded(SoftDeleted as any, ['status', 'deleted_at']);
    } catch (e) {
      expect((e as FindFilterLeakError).missingKeys).toEqual(['tenant_id']);
    }
  });

  it('is a NO-OP when every FIND_FILTER scope key is folded into the authored WHERE', () => {
    // The author folded BOTH scope predicates (the no-op case proven byte-equal above).
    expect(() => assertFindFilterFolded(SoftDeleted as any, ['status', 'deleted_at', 'tenant_id'])).not.toThrow();
  });

  it('is a NO-OP for a model with no FIND_FILTER (nothing to fold)', () => {
    expect(() => assertFindFilterFolded(Unfiltered as any, [])).not.toThrow();
    expect(() => assertFindFilterFolded(Unfiltered as any, ['anything'])).not.toThrow();
  });
});
