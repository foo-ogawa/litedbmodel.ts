/**
 * ALL-TYPE coverage — typed de-box outType DERIVATION + TS read-path materialization (issue #59).
 *
 * Two dialect-independent cores of #59:
 *  1. `sqlTypeToBcScalar` (§4.1 SQL-type → bc scalar) maps every coverage column to the correct bc
 *     scalar — the native struct field type.
 *  2. `sqlTypeToMaterializeClass` + `materializeCell` (the TS read-path de-box, owner-approved
 *     contract): INT32→number, BIGINT→string (exact, JSON-safe), DATE→string (TZ-attached),
 *     BOOLEAN→boolean, float/decimal/text/json→passthrough.
 *
 * date → 'string', decimal → 'string', and BIGINT → string are value-preserving representations
 * (bc 0.6.0 has no date/decimal scalar — behavior-contracts#84 deferred; a JS bigint is not
 * JSON-safe). Pinned here so the compromises live in a test, never silent.
 */

import { describe, it, expect } from 'vitest';
import { sqlTypeToBcScalar, sqlTypeToMaterializeClass, materializeCell, schemaColumnTypeResolver } from '../../src/scp';

const COVERAGE_DIALECT_TYPES: Record<string, { sqlite: string; postgres: string; mysql: string; scalar: 'int' | 'float' | 'string' | 'bool'; mat: 'int32' | 'int64' | 'date' | 'bool' | 'passthrough' }> = {
  int32_val: { sqlite: 'INT', postgres: 'INTEGER', mysql: 'INT', scalar: 'int', mat: 'int32' },
  int64_val: { sqlite: 'BIGINT', postgres: 'BIGINT', mysql: 'BIGINT', scalar: 'int', mat: 'int64' },
  real_val: { sqlite: 'REAL', postgres: 'DOUBLE PRECISION', mysql: 'DOUBLE', scalar: 'float', mat: 'passthrough' },
  dec_val: { sqlite: 'TEXT', postgres: 'NUMERIC(20,4)', mysql: 'DECIMAL(20,4)', scalar: 'string', mat: 'passthrough' }, // decimal→string
  text_val: { sqlite: 'TEXT', postgres: 'TEXT', mysql: 'TEXT', scalar: 'string', mat: 'passthrough' },
  bool_val: { sqlite: 'BOOLEAN', postgres: 'BOOLEAN', mysql: 'BOOLEAN', scalar: 'bool', mat: 'bool' },
  date_val: { sqlite: 'DATE', postgres: 'DATE', mysql: 'DATE', scalar: 'string', mat: 'date' },
  json_val: { sqlite: 'JSON', postgres: 'JSONB', mysql: 'JSON', scalar: 'string', mat: 'passthrough' },
};

describe('#59 coverage — sqlTypeToBcScalar (§4.1, all types × all dialects)', () => {
  for (const [col, spec] of Object.entries(COVERAGE_DIALECT_TYPES)) {
    for (const dialect of ['sqlite', 'postgres', 'mysql'] as const) {
      it(`${col} (${dialect} ${spec[dialect]}) → bc '${spec.scalar}'`, () => {
        expect(sqlTypeToBcScalar(spec[dialect])).toBe(spec.scalar);
      });
    }
  }

  it('int stays int for every int width incl. BIGINT (bc scalar does not split by width)', () => {
    for (const t of ['INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'INT8']) {
      expect(sqlTypeToBcScalar(t)).toBe('int');
    }
  });

  it('an unknown SQL type is a HARD error (no-assume, no-fallback)', () => {
    expect(() => sqlTypeToBcScalar('GEOMETRY')).toThrow(/no bc outType mapping/i);
  });
});

describe('#59 coverage — sqlTypeToMaterializeClass (TS read-path int32/int64/date/bool split)', () => {
  for (const [col, spec] of Object.entries(COVERAGE_DIALECT_TYPES)) {
    for (const dialect of ['sqlite', 'postgres', 'mysql'] as const) {
      it(`${col} (${dialect} ${spec[dialect]}) → materialize '${spec.mat}'`, () => {
        expect(sqlTypeToMaterializeClass(spec[dialect])).toBe(spec.mat);
      });
    }
  }

  it('32-bit int family → int32; 64-bit → int64 (the width split)', () => {
    for (const t of ['INT', 'INTEGER', 'SMALLINT', 'TINYINT', 'MEDIUMINT', 'INT2', 'INT4']) {
      expect(sqlTypeToMaterializeClass(t)).toBe('int32');
    }
    for (const t of ['BIGINT', 'INT8']) {
      expect(sqlTypeToMaterializeClass(t)).toBe('int64');
    }
  });

  it('date family → date; decimal/float/text/json → passthrough', () => {
    for (const t of ['DATE', 'TIMESTAMP', 'TIMESTAMPTZ', 'DATETIME', 'TIME']) expect(sqlTypeToMaterializeClass(t)).toBe('date');
    for (const t of ['DECIMAL(20,4)', 'NUMERIC', 'REAL', 'DOUBLE', 'TEXT', 'VARCHAR(9)', 'JSON', 'JSONB', 'UUID']) {
      expect(sqlTypeToMaterializeClass(t)).toBe('passthrough');
    }
  });

  it('unknown SQL type is a HARD error', () => {
    expect(() => sqlTypeToMaterializeClass('GEOMETRY')).toThrow(/no materialization class/i);
  });
});

describe('#59 coverage — materializeCell (per-cell JS coercion)', () => {
  it('int64: bigint/string/safe-number → exact decimal STRING (JSON-safe)', () => {
    expect(materializeCell(9223372036854775807n, 'int64')).toBe('9223372036854775807');
    expect(materializeCell('9223372036854775807', 'int64')).toBe('9223372036854775807');
    expect(materializeCell('-9223372036854775808', 'int64')).toBe('-9223372036854775808');
    expect(materializeCell(42, 'int64')).toBe('42');
    // A string result JSON.stringify-es without throwing (a bigint would throw).
    expect(() => JSON.stringify({ v: materializeCell(9223372036854775807n, 'int64') })).not.toThrow();
  });

  it('int64: an ALREADY-ROUNDED unsafe JS number is a HARD error (precision lost upstream)', () => {
    expect(() => materializeCell(9223372036854775807 /* rounds to ...776000 */, 'int64')).toThrow(/UNSAFE JS number|precision/i);
  });

  it('int32: number stays; bigint/string → number', () => {
    expect(materializeCell(2147483647, 'int32')).toBe(2147483647);
    expect(materializeCell(42n, 'int32')).toBe(42);
    expect(materializeCell('7', 'int32')).toBe(7);
    expect(typeof materializeCell(42n, 'int32')).toBe('number');
  });

  it('date: a JS Date → its ISO string; a string stays', () => {
    expect(materializeCell('2026-07-14', 'date')).toBe('2026-07-14');
    expect(materializeCell(new Date('2026-07-14T00:00:00.000Z'), 'date')).toBe('2026-07-14T00:00:00.000Z');
    expect(typeof materializeCell(new Date(), 'date')).toBe('string');
  });

  it('bool: 0/1/0n/1n → boolean; boolean stays', () => {
    expect(materializeCell(0, 'bool')).toBe(false);
    expect(materializeCell(1, 'bool')).toBe(true);
    expect(materializeCell(0n, 'bool')).toBe(false);
    expect(materializeCell(true, 'bool')).toBe(true);
  });

  it('NULL passes through for every class', () => {
    for (const k of ['int32', 'int64', 'date', 'bool', 'passthrough'] as const) {
      expect(materializeCell(null, k)).toBeNull();
    }
  });

  it('passthrough leaves the value unchanged', () => {
    expect(materializeCell('12345678901234.5678', 'passthrough')).toBe('12345678901234.5678');
    expect(materializeCell(3.14, 'passthrough')).toBe(3.14);
  });
});

describe('#59 coverage — schemaColumnTypeResolver over the coverage DDL', () => {
  // Mirrors the ACTUAL SQLite coverage DDL (benchmark/crosslang/domain.ts): split int32/int64,
  // decimal columns as TEXT.
  const COVERAGE_DDL = `CREATE TABLE coverage (
     id INTEGER PRIMARY KEY,
     int32_val INT NOT NULL,
     int64_val BIGINT NOT NULL,
     real_val REAL NOT NULL,
     dec_val TEXT NOT NULL,
     text_val TEXT NOT NULL,
     bool_val BOOLEAN NOT NULL,
     date_val DATE NOT NULL,
     json_val JSON NOT NULL,
     int32n_val INT,
     int64n_val BIGINT,
     realn_val REAL,
     decn_val TEXT,
     textn_val TEXT,
     booln_val BOOLEAN,
     daten_val DATE,
     jsonn_val JSON
   );`;

  const EXPECTED_SCALAR: Record<string, 'int' | 'float' | 'string' | 'bool'> = {
    id: 'int', int32_val: 'int', int64_val: 'int', real_val: 'float', dec_val: 'string', text_val: 'string',
    bool_val: 'bool', date_val: 'string', json_val: 'string',
    int32n_val: 'int', int64n_val: 'int', realn_val: 'float', decn_val: 'string', textn_val: 'string',
    booln_val: 'bool', daten_val: 'string', jsonn_val: 'string',
  };
  const EXPECTED_MAT: Record<string, 'int32' | 'int64' | 'date' | 'bool' | 'passthrough'> = {
    id: 'int32', int32_val: 'int32', int64_val: 'int64', real_val: 'passthrough', dec_val: 'passthrough',
    text_val: 'passthrough', bool_val: 'bool', date_val: 'date', json_val: 'passthrough',
    int32n_val: 'int32', int64n_val: 'int64', realn_val: 'passthrough', decn_val: 'passthrough',
    textn_val: 'passthrough', booln_val: 'bool', daten_val: 'date', jsonn_val: 'passthrough',
  };

  it('resolves each of the 17 columns and derives the expected bc scalar + materialize class', () => {
    const resolve = schemaColumnTypeResolver([COVERAGE_DDL]);
    for (const col of Object.keys(EXPECTED_SCALAR)) {
      expect(sqlTypeToBcScalar(resolve('coverage', col)), `${col} scalar`).toBe(EXPECTED_SCALAR[col]);
      expect(sqlTypeToMaterializeClass(resolve('coverage', col)), `${col} mat`).toBe(EXPECTED_MAT[col]);
    }
  });

  it('an undeclared column is a HARD error (no silent boxed fallback)', () => {
    const resolve = schemaColumnTypeResolver([COVERAGE_DDL]);
    expect(() => resolve('coverage', 'nonexistent')).toThrow(/not declared/i);
  });
});
