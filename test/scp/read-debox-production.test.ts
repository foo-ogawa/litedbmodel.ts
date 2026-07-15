/**
 * ALWAYS-ON production read-path de-box (issue #59) — the fix must fire on the SHIPPED API with NO
 * caller-supplied column-type resolver and NO schema argument. Column types are declared INLINE on
 * the model (`static columns`), so `executeBehavior` / `read` (the SQLite production entry points)
 * de-box every read: INT→number, BIGINT→string (exact + JSON-safe), DATE→string, BOOLEAN→boolean.
 * Guards against the fix regressing to opt-in (materialization only when a resolver/schema is passed)
 * or reintroducing per-read DB introspection.
 */

import { describe, it, expect } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior, components, publishBehaviors, executeBehavior, read, compileBundle,
  whereEq, whereGe, materializeResolverFromColumnMap, columnTypeResolverFromColumnMap,
  failClosedMaterializeResolverFromColumnMap,
} from '../../src/scp';

const L = components();

// The INLINE typed-column declaration — `dec` is TEXT (decimal→string; a SQLite NUMERIC-affinity
// column would drop precision at STORAGE). Declared as `static columns` on the model; the contract
// precomputes the resolvers ONCE (no per-read DB introspection, no schema arg).
const COV_COLUMNS = {
  cov: { id: 'INTEGER', i32: 'INT', i64: 'BIGINT', flag: 'BOOLEAN', day: 'DATE', dec: 'TEXT', note: 'TEXT' },
} as const;
const REL_COLUMNS = {
  parent: { id: 'INTEGER', name: 'TEXT' },
  child: { id: 'INTEGER', parent_id: 'INTEGER', big: 'BIGINT', day: 'DATE', flag: 'BOOLEAN' },
} as const;

class Reads extends SemanticBehavior {
  static columns = COV_COLUMNS;
  All($: { min_id: unknown }) {
    return L.Select({
      table: 'cov',
      select: ['id', 'i32', 'i64', 'flag', 'day', 'dec', 'note'],
      where: [whereGe(($ as never)['id'], $.min_id)],
      order: 'id ASC',
    });
  }
}

class RelReads extends SemanticBehavior {
  static columns = REL_COLUMNS;
  Parents($: { pid: unknown }) {
    return L.Select({ table: 'parent', select: ['id', 'name'], where: [whereEq(($ as never)['id'], $.pid)], order: 'id ASC' });
  }
}

function freshDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.exec(`CREATE TABLE cov (id INTEGER PRIMARY KEY, i32 INT, i64 BIGINT, flag BOOLEAN, day DATE, dec TEXT, note TEXT);`);
  // i64 max bound as a string so SQLite stores the exact 64-bit value (a JS number would round it).
  db.prepare(`INSERT INTO cov VALUES (?,?,?,?,?,?,?)`).run(1, 2147483647, '9223372036854775807', 1, '2026-07-14', '12345678901234.5678', 'hi');
  db.prepare(`INSERT INTO cov VALUES (?,?,?,?,?,?,?)`).run(2, 0, '-9223372036854775808', 0, '2000-02-29', '-0.5', null);
  return db;
}

describe('#59 ALWAYS-ON de-box — executeBehavior (sqlite), types declared INLINE (no schema arg, no resolver)', () => {
  // The model declares `static columns`; the resolver is precomputed on the contract. The caller
  // passes NOTHING extra to executeBehavior.
  const contract = publishBehaviors(Reads);

  it('materializes INT→number, BIGINT→string(exact), BOOLEAN→boolean, DATE→string', () => {
    const db = freshDb();
    const rows = executeBehavior(contract, { min_id: 1 }, { db, entry: 'All' }) as Record<string, unknown>[];
    db.close();
    const r1 = rows.find((r) => Number(r.id) === 1)!;
    expect(typeof r1.i32).toBe('number');
    expect(r1.i32).toBe(2147483647);
    expect(typeof r1.i64).toBe('string');
    expect(r1.i64).toBe('9223372036854775807'); // exact — NOT 9223372036854776000
    expect(typeof r1.flag).toBe('boolean');
    expect(r1.flag).toBe(true);
    expect(typeof r1.day).toBe('string');
    expect(r1.day).toBe('2026-07-14');
    expect(r1.dec).toBe('12345678901234.5678'); // decimal precision preserved (TEXT-affinity)
    // The whole row is JSON-safe (a bigint would throw here).
    expect(() => JSON.stringify(rows)).not.toThrow();
  });

  it('i64 min boundary is exact; NULLs pass through', () => {
    const db = freshDb();
    const rows = executeBehavior(contract, { min_id: 1 }, { db, entry: 'All' }) as Record<string, unknown>[];
    db.close();
    const r2 = rows.find((r) => Number(r.id) === 2)!;
    expect(r2.i64).toBe('-9223372036854775808');
    expect(r2.flag).toBe(false);
    expect(r2.note).toBeNull();
  });

  it('the `read` typed-object surface de-boxes identically (no resolver)', () => {
    const db = freshDb();
    const rows = read(contract, { min_id: 1 }, { db, entry: 'All' }) as Record<string, unknown>[];
    db.close();
    const r1 = rows.find((r) => Number(r.id) === 1)!;
    expect(r1.i64).toBe('9223372036854775807');
    expect(r1.flag).toBe(true);
    expect(r1.day).toBe('2026-07-14');
  });
});

describe('#59 ALWAYS-ON de-box — relation child rows materialize (inline columns, no resolver)', () => {
  const contract = publishBehaviors(RelReads);

  it('a hasMany relation over a BIGINT/DATE/BOOLEAN child de-boxes the child rows', () => {
    const db = new Database(':memory:');
    db.exec(`CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);`);
    db.exec(`CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, big BIGINT, day DATE, flag BOOLEAN);`);
    db.exec(`INSERT INTO parent VALUES (1,'p');`);
    db.prepare(`INSERT INTO child VALUES (?,?,?,?,?)`).run(10, 1, '9223372036854775807', '2026-07-14', 1);
    const REL = {
      name: 'kids', kind: 'hasMany', targetTable: 'child',
      select: ['id', 'parent_id', 'big', 'day', 'flag'], parentKey: 'id', targetKey: 'parent_id', dialect: 'sqlite',
    } as never;
    const rows = read(contract, { pid: 1 }, { db, entry: 'Parents', relations: [REL], with: { kids: true } }) as Record<string, unknown>[];
    db.close();
    const kid = (rows[0].kids as Record<string, unknown>[])[0];
    expect(typeof kid.big).toBe('string');
    expect(kid.big).toBe('9223372036854775807');
    expect(kid.day).toBe('2026-07-14');
    expect(kid.flag).toBe(true);
  });
});

describe('#59 STATIC resolvers from an inline column map (pure in-memory, zero DB)', () => {
  const colMap = new Map<string, Map<string, string>>([
    ['t', new Map(Object.entries({ a: 'INTEGER', b: 'BIGINT', c: 'BOOLEAN', d: 'DATE', e: 'DECIMAL(10,2)', f: 'TEXT' }))],
  ]);

  it('materializeResolverFromColumnMap derives each column class (tolerant on unknown)', () => {
    const resolve = materializeResolverFromColumnMap(colMap);
    expect(resolve('t', 'a')).toBe('int32');
    expect(resolve('t', 'b')).toBe('int64');
    expect(resolve('t', 'c')).toBe('bool');
    expect(resolve('t', 'd')).toBe('date');
    expect(resolve('t', 'e')).toBe('passthrough'); // decimal → string, no coercion
    expect(resolve('t', 'f')).toBe('passthrough');
    expect(resolve('t', 'nonexistent')).toBeUndefined(); // tolerant: unknown → undefined (kept raw)
    expect(resolve('nosuchtable', 'x')).toBeUndefined();
  });

  it('columnTypeResolverFromColumnMap returns the declared SQL type; THROWS on undeclared (codegen fail-closed)', () => {
    const resolve = columnTypeResolverFromColumnMap(colMap);
    expect(resolve('t', 'b')).toBe('BIGINT');
    expect(() => resolve('t', 'zzz')).toThrow(/not declared/i);
    expect(() => resolve('nosuchtable', 'x')).toThrow(/no inline column-type declaration/i);
  });

  it('failClosedMaterializeResolverFromColumnMap resolves declared classes and THROWS on undeclared', () => {
    const resolve = failClosedMaterializeResolverFromColumnMap(colMap);
    expect(resolve('t', 'b')).toBe('int64');
    expect(resolve('t', 'a')).toBe('int32');
    expect(resolve('t', 'e')).toBe('passthrough'); // declared decimal → passthrough (no-op) BUT type-checked
    expect(() => resolve('t', 'zzz')).toThrow(/not declared/i); // undeclared → THROW (no silent skip)
    expect(() => resolve('nosuchtable', 'x')).toThrow(/no inline column-type declaration/i);
  });

  it('the contract carries BOTH resolvers precomputed from the inline `static columns` (ONCE)', () => {
    const contract = publishBehaviors(Reads);
    expect(contract.materializeResolver).toBeDefined();
    expect(contract.materializeResolver!('cov', 'i64')).toBe('int64');
    expect(contract.materializeResolver!('cov', 'i32')).toBe('int32');
    expect(contract.materializeResolver!('cov', 'flag')).toBe('bool');
    expect(contract.resolveColumnType).toBeDefined();
    expect(contract.resolveColumnType!('cov', 'i64')).toBe('BIGINT');
  });
});

describe('#59 FAIL-CLOSED at registration — a typed read whose projected columns are undeclared THROWS', () => {
  it('a model with NO `static columns` but a read projecting columns THROWS (columns REQUIRED)', () => {
    class NoDecl extends SemanticBehavior {
      Q($: { pid: unknown }) {
        return L.Select({ table: 'parent', select: ['id', 'name'], where: [whereEq(($ as never)['id'], $.pid)], order: 'id ASC' });
      }
    }
    expect(() => publishBehaviors(NoDecl)).toThrow(/REQUIRES an inline `static columns`|declares\s+NO `columns`/i);
  });

  it('a model WITH `static columns` but a projected BIGINT column OMITTED from the map THROWS', () => {
    class MissingBig extends SemanticBehavior {
      // `big` (a BIGINT the read projects) is intentionally OMITTED — must fail closed at registration
      // (never a silent skip that would return a rounded i64 at read time).
      static columns = { t: { id: 'INTEGER', name: 'TEXT' } };
      Q($: { pid: unknown }) {
        return L.Select({ table: 't', select: ['id', 'big', 'name'], where: [whereEq(($ as never)['id'], $.pid)], order: 'id ASC' });
      }
    }
    expect(() => publishBehaviors(MissingBig)).toThrow(/'big' not declared|not declared on table 't'/i);
  });

  it('a WRITE-only model needs no `columns` (writes are exempt)', () => {
    class WriteOnly extends SemanticBehavior {
      Make($: { title: unknown }) {
        return (L as never as { Insert(x: unknown): unknown }).Insert({ table: 'posts', 'values.title': $.title, returning: 'id' });
      }
    }
    expect(() => publishBehaviors(WriteOnly)).not.toThrow();
  });

  it('a fully-declared read registers fine and its production reads de-box (regression guard)', () => {
    const contract = publishBehaviors(Reads); // Reads declares all its projected cov columns
    const db = freshDb();
    const rows = executeBehavior(contract, { min_id: 1 }, { db, entry: 'All' }) as Record<string, unknown>[];
    db.close();
    const r1 = rows.find((r) => Number(r.id) === 1)!;
    expect(r1.i64).toBe('9223372036854775807'); // exact, de-boxed — NOT a rounded number
    expect(typeof r1.i64).toBe('string');
  });
});

describe('#59 ALL projection shapes — bare/qualified/aliased de-box; * hard-errors; undeclared THROWS (no bare-only escape hatch)', () => {
  function bigDb(): InstanceType<typeof Database> {
    const db = new Database(':memory:');
    db.exec(`CREATE TABLE t (id INTEGER PRIMARY KEY, big BIGINT);`);
    db.prepare(`INSERT INTO t VALUES (?,?)`).run(1, '9223372036854775807');
    return db;
  }

  it('QUALIFIED `t.big` (declared BIGINT) → materializes to EXACT string, NOT a rounded number', () => {
    class Qual extends SemanticBehavior {
      static columns = { t: { id: 'INTEGER', big: 'BIGINT' } };
      Q($: { min: unknown }) { return L.Select({ table: 't', select: ['id', 't.big'], where: [whereGe(($ as never)['id'], $.min)], order: 'id ASC' }); }
    }
    const db = bigDb();
    const r = (executeBehavior(publishBehaviors(Qual), { min: 1 }, { db, entry: 'Q' }) as Record<string, unknown>[])[0];
    db.close();
    expect(typeof r.big).toBe('string');
    expect(r.big).toBe('9223372036854775807'); // exact — the qualified shape is NOT a silent-raw hole
  });

  it('ALIASED `big AS b` (declared BIGINT) → row key `b` materializes to EXACT string', () => {
    class Alias extends SemanticBehavior {
      static columns = { t: { id: 'INTEGER', big: 'BIGINT' } };
      Q($: { min: unknown }) { return L.Select({ table: 't', select: ['id', 'big AS b'], where: [whereGe(($ as never)['id'], $.min)], order: 'id ASC' }); }
    }
    const db = bigDb();
    const r = (executeBehavior(publishBehaviors(Alias), { min: 1 }, { db, entry: 'Q' }) as Record<string, unknown>[])[0];
    db.close();
    expect(typeof r.b).toBe('string');
    expect(r.b).toBe('9223372036854775807');
    expect('big' in r).toBe(false); // the driver returns the row under the ALIAS, not the underlying name
  });

  it('`SELECT *` HARD-ERRORS at registration (a typed read must project explicit columns)', () => {
    class Star extends SemanticBehavior {
      static columns = { t: { id: 'INTEGER', big: 'BIGINT' } };
      Q($: { min: unknown }) { return L.Select({ table: 't', select: ['*'], where: [whereGe(($ as never)['id'], $.min)], order: 'id ASC' }); }
    }
    expect(() => publishBehaviors(Star)).toThrow(/wildcard|projects '\*'/i);
  });

  it('UNDECLARED column in QUALIFIED form (`t.big`, big undeclared) THROWS', () => {
    class QualUndecl extends SemanticBehavior {
      static columns = { t: { id: 'INTEGER' } };
      Q($: { min: unknown }) { return L.Select({ table: 't', select: ['id', 't.big'], where: [whereGe(($ as never)['id'], $.min)], order: 'id ASC' }); }
    }
    expect(() => publishBehaviors(QualUndecl)).toThrow(/'big' not declared/i);
  });

  it('UNDECLARED column in ALIASED form (`big AS b`, big undeclared) THROWS', () => {
    class AliasUndecl extends SemanticBehavior {
      static columns = { t: { id: 'INTEGER' } };
      Q($: { min: unknown }) { return L.Select({ table: 't', select: ['id', 'big AS b'], where: [whereGe(($ as never)['id'], $.min)], order: 'id ASC' }); }
    }
    expect(() => publishBehaviors(AliasUndecl)).toThrow(/'big' not declared/i);
  });

  it('a QUALIFIED JOIN column resolves against ITS OWN table (qualifier), not the base table', () => {
    class Jn extends SemanticBehavior {
      static columns = { posts: { id: 'INTEGER', big: 'BIGINT' }, users: { id: 'INTEGER', name: 'TEXT' } };
      Q($: { min: unknown }) { return L.Select({ table: 'posts', select: ['posts.id', 'posts.big AS pb', 'users.name'], where: [whereGe(($ as never)['id'], $.min)], order: 'posts.id ASC' }); }
    }
    // Registers fine (users.name resolves against `users`, not `posts`). The materializer keys the
    // aliased BIGINT under `pb` and leaves users.name passthrough.
    const contract = publishBehaviors(Jn);
    expect(contract.materializeResolver!('posts', 'big')).toBe('int64');
    expect(contract.materializeResolver!('users', 'name')).toBe('passthrough');
  });

  it('a COMPUTED projection (`COUNT(*) as n`) is allowed (no schema column to round) — left raw', () => {
    class Agg extends SemanticBehavior {
      static columns = { t: { id: 'INTEGER', big: 'BIGINT' } };
      Q(_$: Record<string, never>) { return L.Select({ table: 't', select: ['id', 'COUNT(*) as n'], group: 'id' }); }
    }
    // A computed/aggregate projection has no underlying schema column → not the i64 hole; registers fine.
    expect(() => publishBehaviors(Agg)).not.toThrow();
  });

  it('SINGLE SOURCE: the read materializers are the SAME resolution as the codegen outType (not a second pass)', () => {
    class R extends SemanticBehavior {
      static columns = { t: { id: 'INTEGER', big: 'BIGINT', flag: 'BOOLEAN' } };
      Q($: { min: unknown }) { return L.Select({ table: 't', select: ['id', 'big AS b', 'flag'], where: [whereGe(($ as never)['id'], $.min)], order: 'id ASC' }); }
    }
    const contract = publishBehaviors(R);
    // ONE compile with the contract's resolveColumnType → the bundle carries BOTH the codegen outType
    // IR annotations AND the read-path materializersByNode, derived from the SAME deriveReadOutTypes.
    const bundle = compileBundle(contract, 'Q', [], 'sqlite', undefined, contract.resolveColumnType) as {
      readGraph?: { ir: unknown; materializersByNode?: Record<string, Record<string, string>> };
    };
    const rg = bundle.readGraph!;
    // The materializer map (read path) is keyed by OUTPUT column and typed identically to the outType.
    const mat = rg.materializersByNode!;
    const node = Object.values(mat)[0];
    // Keyed by OUTPUT column: id→int32, big AS b→int64 (under alias `b`), flag→bool.
    expect(node).toEqual({ id: 'int32', b: 'int64', flag: 'bool' });
    // Walk the outType row obj (codegen) and confirm it types the SAME output keys.
    let rowObj: Record<string, unknown> | undefined;
    const visit = (n: unknown): void => {
      if (n === null || typeof n !== 'object') return;
      const o = n as Record<string, unknown>;
      if ('outType' in o) { let t: unknown = o.outType; while (t && typeof t === 'object' && !('obj' in (t as object))) t = (t as { arr?: unknown; opt?: unknown }).arr ?? (t as { opt?: unknown }).opt; if (t && typeof t === 'object' && 'obj' in (t as object)) rowObj = (t as { obj: Record<string, unknown> }).obj; }
      for (const v of Object.values(o)) visit(v);
    };
    visit(rg.ir);
    // Same output keys in both (codegen outType + read materializers come from one resolution).
    expect(Object.keys(rowObj!).sort()).toEqual(['b', 'flag', 'id']); // big AS b → key 'b'
    expect(rowObj!.id).toBe('int'); expect(rowObj!.b).toBe('int'); expect(rowObj!.flag).toBe('bool');
  });
});
