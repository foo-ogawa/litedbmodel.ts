/**
 * ALL-TYPE coverage — typed de-box outType DERIVATION (issue #59).
 *
 * The dialect-independent core of #59 item 4: `sqlTypeToBcScalar` (the §4.1 SQL-type → bc
 * scalar table) and `schemaColumnTypeResolver` must map EVERY coverage column — across all
 * three dialects' DDL tokens — to the CORRECT bc scalar, i.e. the CONCRETE native struct field
 * type the typed-native codegen path materializes. This is the derivation that the live-DB
 * round-trip verifier (`benchmark/crosslang/coverage-roundtrip.ts`) builds on.
 *
 * date → 'string' and decimal → 'string' are the OWNER-APPROVED re-scope (#59): bc 0.6.0 has NO
 * date/decimal portable scalar (behavior-contracts#84 deferred), so these are value-preserving
 * string representations — asserted here so the compromise is pinned in a test, never silent.
 */

import { describe, it, expect } from 'vitest';
import { sqlTypeToBcScalar, schemaColumnTypeResolver } from '../../src/scp';

// The per-dialect coverage DDL tokens (mirrors benchmark/crosslang/domain.ts's coverage table).
// SQLite / Postgres / MySQL each spell the same logical type differently; the §4.1 mapping must
// resolve every spelling to the SAME bc scalar.
const COVERAGE_DIALECT_TYPES: Record<string, { sqlite: string; postgres: string; mysql: string; scalar: 'int' | 'float' | 'string' | 'bool' }> = {
  int_val: { sqlite: 'INTEGER', postgres: 'BIGINT', mysql: 'BIGINT', scalar: 'int' },
  real_val: { sqlite: 'REAL', postgres: 'DOUBLE PRECISION', mysql: 'DOUBLE', scalar: 'float' },
  dec_val: { sqlite: 'DECIMAL(20,4)', postgres: 'NUMERIC(20,4)', mysql: 'DECIMAL(20,4)', scalar: 'string' }, // bc#84 gap
  text_val: { sqlite: 'TEXT', postgres: 'TEXT', mysql: 'TEXT', scalar: 'string' },
  bool_val: { sqlite: 'BOOLEAN', postgres: 'BOOLEAN', mysql: 'BOOLEAN', scalar: 'bool' },
  date_val: { sqlite: 'DATE', postgres: 'DATE', mysql: 'DATE', scalar: 'string' }, // bc#84 gap
  json_val: { sqlite: 'JSON', postgres: 'JSONB', mysql: 'JSON', scalar: 'string' },
};

describe('#59 coverage outType derivation — sqlTypeToBcScalar (§4.1, all types × all dialects)', () => {
  for (const [col, spec] of Object.entries(COVERAGE_DIALECT_TYPES)) {
    for (const dialect of ['sqlite', 'postgres', 'mysql'] as const) {
      it(`${col} (${dialect} ${spec[dialect]}) → bc '${spec.scalar}'`, () => {
        expect(sqlTypeToBcScalar(spec[dialect])).toBe(spec.scalar);
      });
    }
  }

  it('decimal/date map to string, NOT a (nonexistent) bc decimal/date scalar (bc#84 deferred)', () => {
    for (const t of ['DECIMAL(20,4)', 'NUMERIC', 'MONEY', 'DATE', 'TIMESTAMP', 'TIMESTAMPTZ', 'DATETIME', 'TIME']) {
      expect(sqlTypeToBcScalar(t)).toBe('string');
    }
  });

  it('int stays int for every int width incl. BIGINT (no silent widen/narrow at derivation)', () => {
    for (const t of ['INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'INT8']) {
      expect(sqlTypeToBcScalar(t)).toBe('int');
    }
  });

  it('int and real are SEPARATED (int never derives to float, and vice-versa)', () => {
    expect(sqlTypeToBcScalar('BIGINT')).toBe('int');
    expect(sqlTypeToBcScalar('DOUBLE PRECISION')).toBe('float');
    expect(sqlTypeToBcScalar('REAL')).toBe('float');
  });

  it('an unknown SQL type is a HARD error (no-assume, no-fallback) — never defaulted', () => {
    expect(() => sqlTypeToBcScalar('GEOMETRY')).toThrow(/no bc outType mapping/i);
  });
});

describe('#59 coverage — schemaColumnTypeResolver over the coverage DDL resolves every column', () => {
  // Mirrors the ACTUAL SQLite coverage DDL (benchmark/crosslang/domain.ts): the decimal
  // columns are TEXT (not DECIMAL) on SQLite — a string-represented decimal stored in a
  // TEXT-affinity column so precision round-trips exactly. Both TEXT and DECIMAL derive to
  // bc `string` (asserted in the dialect-token table above), so the derivation is identical.
  const COVERAGE_DDL = `CREATE TABLE coverage (
     id INTEGER PRIMARY KEY,
     int_val INTEGER NOT NULL,
     real_val REAL NOT NULL,
     dec_val TEXT NOT NULL,
     text_val TEXT NOT NULL,
     bool_val BOOLEAN NOT NULL,
     date_val DATE NOT NULL,
     json_val JSON NOT NULL,
     intn_val INTEGER,
     realn_val REAL,
     decn_val TEXT,
     textn_val TEXT,
     booln_val BOOLEAN,
     daten_val DATE,
     jsonn_val JSON
   );`;

  const EXPECTED: Record<string, 'int' | 'float' | 'string' | 'bool'> = {
    id: 'int', int_val: 'int', real_val: 'float', dec_val: 'string', text_val: 'string',
    bool_val: 'bool', date_val: 'string', json_val: 'string',
    intn_val: 'int', realn_val: 'float', decn_val: 'string', textn_val: 'string',
    booln_val: 'bool', daten_val: 'string', jsonn_val: 'string',
  };

  it('resolves each of the 15 columns and derives the expected bc scalar', () => {
    const resolve = schemaColumnTypeResolver([COVERAGE_DDL]);
    for (const [col, scalar] of Object.entries(EXPECTED)) {
      expect(sqlTypeToBcScalar(resolve('coverage', col)), `${col}`).toBe(scalar);
    }
  });

  it('an undeclared column is a HARD error (no silent boxed fallback)', () => {
    const resolve = schemaColumnTypeResolver([COVERAGE_DDL]);
    expect(() => resolve('coverage', 'nonexistent')).toThrow(/not declared/i);
  });
});
