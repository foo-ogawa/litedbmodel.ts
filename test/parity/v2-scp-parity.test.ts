/**
 * #65 — v2 SCP ↔ v1 golden SQL parity (the deliverable that makes "litedbmodel vs
 * other ORMs" honest).
 *
 * For every op in the ORM-comparison bench (benchmark/benchmark.ts → testCategories,
 * captured in `benchmark/parity/v1-sql.golden.json` by #64), this asserts that the
 * litedbmodel **v2 SCP** makeSQL compile path emits the ordered `{ sql, params }`
 * statement set that satisfies the owner's DIALECT-SPECIFIC parity rule (issue #65
 * comment, 2026-07-16):
 *
 *   - postgres  → BYTE-IDENTICAL to the v1 golden (`= ANY(?::int[])`, `UNNEST(…)`,
 *                 `JOIN unnest(…)`, `ON CONFLICT`, etc). The golden captures the portable
 *                 `?` form ABOVE the driver; the pg driver rewrites `?`→`$N` internally
 *                 (documented in benchmark/parity/README.md). We reconcile that ONE
 *                 mechanical driver detail EXPLICITLY by applying `renderPlaceholders(…,
 *                 'postgres')` to BOTH sides, then assert byte-equality. Nothing else is
 *                 normalized.
 *   - mysql / sqlite → the v2 "multiple IDs bundled into ONE non-array JSON-array
 *                 parameter" form is AUTHORITATIVE (sqlite `json_each(?)`, mysql
 *                 `JSON_TABLE(?, …)`). We assert THAT form (single JSON param, no
 *                 per-id placeholder explosion). The v1 golden's tuple/IN expansion is
 *                 reference-only here — we do NOT byte-match it. For the non-bundling ops
 *                 (plain SELECT / single INSERT / UPDATE / DELETE / composite tuple-IN,
 *                 which v1 and v2 emit identically) we DO byte-match the golden.
 *
 * The op→SCP mapping drives the SAME public makeSQL compile functions the language
 * runtimes replay (compileSelect / compileInsert(Many) / compileUpdateSingle /
 * compileUpdateMany / compileDelete / compileSingleKeyUnlimited /
 * compileCompositeKeyUnlimited) — the declare-via-BC / static-columns compile surface,
 * NOT a hand-built IR. All 19 ops are expressible; none is stubbed or skipped.
 *
 * This test FAILS on golden drift (a changed golden statement) AND on op coverage loss
 * (an op that no longer compiles / a missing statement).
 */

import { describe, it, expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

import {
  assembleMakeSQL,
  renderPlaceholders,
  compileSelect,
  compileInsert,
  compileInsertMany,
  compileUpdateMany,
  compileUpdateSingle,
  compileDelete,
  compileSingleKeyUnlimited,
  compileCompositeKeyUnlimited,
  type Dialect,
  type MakeSQL,
} from '../../src/scp/makesql';
import { dbCast } from '../../src/DBValues';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ── Golden (ground truth — #64) ───────────────────────────────────────────────

interface GoldenStmt {
  sql: string;
  params: unknown[];
}
type GoldenOp = { statements: GoldenStmt[] } | { error: string };
interface Golden {
  ops: Record<string, Record<Dialect, GoldenOp>>;
}

const golden: Golden = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '..', '..', 'benchmark', 'parity', 'v1-sql.golden.json'), 'utf8')
);

const dialects: Dialect[] = ['postgres', 'mysql', 'sqlite'];

// ── SCP render helpers ────────────────────────────────────────────────────────

interface Rendered {
  sql: string;
  params: unknown[];
}
function render(node: MakeSQL, dialect: Dialect): Rendered {
  const asm = assembleMakeSQL(node);
  return { sql: renderPlaceholders(asm.sql, dialect), params: asm.params as unknown[] };
}
function renderMany(nodes: MakeSQL[], dialect: Dialect): Rendered[] {
  return nodes.map((n) => render(n, dialect));
}

/** The golden statement, with the pg `?`→`$N` driver rewrite applied (the ONE reconciliation). */
function goldenStmts(op: string, dialect: Dialect): Rendered[] {
  const cell = golden.ops[op]?.[dialect];
  if (!cell || 'error' in cell) {
    throw new Error(`golden missing/errored for op="${op}" dialect="${dialect}": ${cell && 'error' in cell ? cell.error : 'absent'}`);
  }
  return cell.statements.map((s) => ({ sql: renderPlaceholders(s.sql, dialect), params: s.params }));
}

/** The plain integer id list a bundling relation carries in the golden (its JSON-param values). */
function relIdsFromGolden(op: string, stmtIdx: number): number[] {
  // PG golden bundles the ids as ONE array param on `= ANY(?::int[])` / the first unnest col.
  const pg = golden.ops[op].postgres as { statements: GoldenStmt[] };
  const p0 = pg.statements[stmtIdx].params[0];
  return p0 as number[];
}

// ── Per-op v2 SCP statement builders (declare-via-BC compile surface) ─────────
//
// Each returns the ordered statement set for a given dialect. Column values / id lists
// come from the golden's own captured params so the SCP path is anchored to the SAME
// data the v1 op executed (not re-invented).

type OpBuilder = (dialect: Dialect) => Rendered[];

const opBuilders: Record<string, OpBuilder> = {
  // 1. Find all (limit 100): SELECT * FROM benchmark_users LIMIT 100
  'Find all (limit 100)': (d) => [render(compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', limit: 100 }), d)],

  // 2. Filter, paginate & sort: WHERE published = ? [::boolean on pg] ORDER BY created_at DESC LIMIT 20 OFFSET 10
  // The `published` column is a decorated boolean → v1 auto-infers sqlCast 'boolean'
  // (decorators.ts `Boolean` → `sqlCast: 'boolean'`), so the value is a BOUND param with a
  // `::boolean` pg cast — NOT the `= TRUE` boolean-literal path. The v2 SCP threads the same
  // column type via `dbCast(value, 'boolean')` (the makesql-golden dbCast construct), so the
  // pg cast + parameter binding are byte-identical to v1; mysql/sqlite drop the cast (identity
  // formatter) → bare `?`, matching the golden.
  'Filter, paginate & sort': (d) => [
    render(
      compileSelect({
        dialect: d,
        tableName: 'benchmark_posts',
        select: '*',
        conditions: { published: dbCast(true, 'boolean') },
        order: 'created_at DESC',
        limit: 20,
        offset: 10,
      }),
      d
    ),
  ],

  // 3. Nested find all (include posts): primary users SELECT + posts-by-author_id relation.
  'Nested find all (include posts)': (d) => [
    render(compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', limit: 100 }), d),
    render(
      compileSingleKeyUnlimited({
        dialect: d,
        tableName: 'benchmark_posts',
        select: '*',
        targetKey: 'author_id',
        values: relIdsFromGolden('Nested find all (include posts)', 1),
      }),
      d
    ),
  ],

  // 4. Find first: WHERE name LIKE ? LIMIT 1
  'Find first': (d) => [
    render(
      compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', conditions: { 'name LIKE ?': 'User%' }, limit: 1 }),
      d
    ),
  ],

  // 5. Nested find first (include posts): primary findOne + posts relation (1 author).
  'Nested find first (include posts)': (d) => [
    render(
      compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', conditions: { 'name LIKE ?': 'User%' }, limit: 1 }),
      d
    ),
    render(
      compileSingleKeyUnlimited({
        dialect: d,
        tableName: 'benchmark_posts',
        select: '*',
        targetKey: 'author_id',
        values: relIdsFromGolden('Nested find first (include posts)', 1),
      }),
      d
    ),
  ],

  // 6. Find unique (by email): WHERE email = ? LIMIT 1
  'Find unique (by email)': (d) => [
    render(
      compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', conditions: { email: 'user500@example.com' }, limit: 1 }),
      d
    ),
  ],

  // 7. Nested find unique (include posts): primary findOne + posts relation.
  'Nested find unique (include posts)': (d) => [
    render(
      compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', conditions: { email: 'user500@example.com' }, limit: 1 }),
      d
    ),
    render(
      compileSingleKeyUnlimited({
        dialect: d,
        tableName: 'benchmark_posts',
        select: '*',
        targetKey: 'author_id',
        values: relIdsFromGolden('Nested find unique (include posts)', 1),
      }),
      d
    ),
  ],

  // 8. Create: INSERT INTO benchmark_users (email, name) VALUES (?, ?)
  Create: (d) => [
    render(
      compileInsert(d, {
        tableName: 'benchmark_users',
        columns: ['email', 'name'],
        records: [{ email: 'bench10000@example.com', name: 'Benchmark User' }],
        rawRecords: [{ email: 'bench10000@example.com', name: 'Benchmark User' }],
      }),
      d
    ),
  ],

  // 9. Nested create (with post): INSERT user RETURNING id, then INSERT post.
  'Nested create (with post)': (d) => [
    render(
      compileInsert(d, {
        tableName: 'benchmark_users',
        columns: ['email', 'name'],
        records: [{ email: 'nested10001@example.com', name: 'Nested User' }],
        rawRecords: [{ email: 'nested10001@example.com', name: 'Nested User' }],
        returning: 'id',
      }),
      d
    ),
    render(
      compileInsert(d, {
        tableName: 'benchmark_posts',
        columns: ['author_id', 'content', 'title'],
        records: [{ author_id: 502, content: 'Content', title: 'Nested Post' }],
        rawRecords: [{ author_id: 502, content: 'Content', title: 'Nested Post' }],
      }),
      d
    ),
  ],

  // 10. Update: UPDATE benchmark_users SET name = ? WHERE id = ?
  Update: (d) => [
    render(
      compileUpdateSingle({
        dialect: d,
        tableName: 'benchmark_users',
        serializedValues: { name: 'Updated User' },
        conditions: { id: 100 },
      }),
      d
    ),
  ],

  // 11. Nested update (update user + post): UPDATE user, then UPDATE posts by author_id.
  'Nested update (update user + post)': (d) => [
    render(
      compileUpdateSingle({
        dialect: d,
        tableName: 'benchmark_users',
        serializedValues: { name: 'Nested Updated' },
        conditions: { id: 100 },
      }),
      d
    ),
    render(
      compileUpdateSingle({
        dialect: d,
        tableName: 'benchmark_posts',
        serializedValues: { title: 'Updated Post' },
        conditions: { author_id: 100 },
      }),
      d
    ),
  ],

  // 12. Upsert: INSERT … ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name RETURNING id
  Upsert: (d) => [
    render(
      compileInsert(d, {
        tableName: 'benchmark_users',
        columns: ['email', 'name'],
        records: [{ email: 'upsert20000@example.com', name: 'Upsert User' }],
        rawRecords: [{ email: 'upsert20000@example.com', name: 'Upsert User' }],
        onConflict: ['email'],
        onConflictUpdate: ['name'],
        returning: 'id',
      }),
      d
    ),
  ],

  // 13. Nested upsert (user + post): upsert user RETURNING id, then INSERT post.
  'Nested upsert (user + post)': (d) => [
    render(
      compileInsert(d, {
        tableName: 'benchmark_users',
        columns: ['email', 'name'],
        records: [{ email: 'nupsert20001@example.com', name: 'Nested Upsert' }],
        rawRecords: [{ email: 'nupsert20001@example.com', name: 'Nested Upsert' }],
        onConflict: ['email'],
        onConflictUpdate: ['name'],
        returning: 'id',
      }),
      d
    ),
    render(
      compileInsert(d, {
        tableName: 'benchmark_posts',
        columns: ['author_id', 'title'],
        records: [{ author_id: 504, title: 'Upsert Post' }],
        rawRecords: [{ author_id: 504, title: 'Upsert Post' }],
      }),
      d
    ),
  ],

  // 14. Delete: INSERT user RETURNING id, then DELETE FROM benchmark_users WHERE id = ?
  Delete: (d) => [
    render(
      compileInsert(d, {
        tableName: 'benchmark_users',
        columns: ['email', 'name'],
        records: [{ email: 'del10002@example.com', name: 'Delete User' }],
        rawRecords: [{ email: 'del10002@example.com', name: 'Delete User' }],
        returning: 'id',
      }),
      d
    ),
    render(compileDelete({ dialect: d, tableName: 'benchmark_users', conditions: { id: 505 } }), d),
  ],

  // 15. Create Many (10): PG UNNEST batch / MySQL+SQLite ONE JSON-array param.
  'Create Many (10 records)': (d) => {
    const records = Array.from({ length: 10 }, (_, i) => ({
      email: `bulk${10003 + i}@example.com`,
      name: `Bulk User ${i}`,
    }));
    return renderMany(compileInsertMany(d, { tableName: 'benchmark_users', records, rawRecords: records }), d);
  },

  // 16. Upsert Many (10): PG UNNEST + ON CONFLICT / MySQL+SQLite ONE JSON param + upsert verb.
  'Upsert Many (10 records)': (d) => {
    const records = Array.from({ length: 10 }, (_, i) => ({
      email: `upsertbulk${20002 + i}@example.com`,
      name: `Upsert Bulk ${i}`,
    }));
    return renderMany(
      compileInsertMany(d, {
        tableName: 'benchmark_users',
        records,
        rawRecords: records,
        onConflict: ['email'],
        onConflictUpdate: ['name'],
      }),
      d
    );
  },

  // 17. Update Many (10 different values): PG UNNEST-join / MySQL+SQLite ONE JSON param.
  'Update Many (10 different values)': (d) => {
    const records = Array.from({ length: 10 }, (_, i) => ({ id: 100 + i, name: `Updated Different ${i}` }));
    return [
      render(
        compileUpdateMany(d, {
          tableName: 'benchmark_users',
          keyColumns: ['id'],
          updateColumns: ['name'],
          records,
          rawRecords: records,
        } as Parameters<typeof compileUpdateMany>[1]),
        d
      ),
    ];
  },

  // 18. Nested relations (100->1000->10000): users SELECT + posts relation + comments relation.
  'Nested relations (100->1000->10000)': (d) => [
    render(compileSelect({ dialect: d, tableName: 'benchmark_users', select: '*', order: 'id ASC', limit: 100 }), d),
    render(
      compileSingleKeyUnlimited({
        dialect: d,
        tableName: 'benchmark_posts',
        select: '*',
        targetKey: 'author_id',
        values: relIdsFromGolden('Nested relations (100->1000->10000)', 1),
      }),
      d
    ),
    render(
      compileSingleKeyUnlimited({
        dialect: d,
        tableName: 'benchmark_comments',
        select: '*',
        targetKey: 'post_id',
        values: relIdsFromGolden('Nested relations (100->1000->10000)', 2),
      }),
      d
    ),
  ],

  // 19. Nested relations (composite key, 5 tenants): tenant_users SELECT (IN tenants) +
  //     composite (tenant_id,user_id) posts relation + composite (tenant_id,post_id) comments relation.
  'Nested relations (composite key, 5 tenants)': (d) => {
    // The composite-tuple lists are read from the golden pg first array param per stmt.
    const postTuples = compositeTuplesFromGolden('Nested relations (composite key, 5 tenants)', 1);
    const commentTuples = compositeTuplesFromGolden('Nested relations (composite key, 5 tenants)', 2);
    return [
      render(
        compileSelect({
          dialect: d,
          tableName: 'benchmark_tenant_users',
          select: '*',
          conditions: { tenant_id: [1, 2, 3, 4, 5] },
          limit: 100,
        }),
        d
      ),
      render(
        compileCompositeKeyUnlimited({
          dialect: d,
          tableName: 'benchmark_tenant_posts',
          select: '*',
          targetKeys: ['tenant_id', 'user_id'],
          tuples: postTuples,
        }),
        d
      ),
      render(
        compileCompositeKeyUnlimited({
          dialect: d,
          tableName: 'benchmark_tenant_comments',
          select: '*',
          targetKeys: ['tenant_id', 'post_id'],
          tuples: commentTuples,
        }),
        d
      ),
    ];
  },
};

/** Rebuild the composite (col0,col1) tuple list from the golden pg two array params. */
function compositeTuplesFromGolden(op: string, stmtIdx: number): number[][] {
  const pg = golden.ops[op].postgres as { statements: GoldenStmt[] };
  const stmt = pg.statements[stmtIdx];
  const col0 = stmt.params[0] as number[];
  const col1 = stmt.params[1] as number[];
  return col0.map((v, i) => [v, col1[i]]);
}

const ALL_OPS = Object.keys(golden.ops);

// A v2 MySQL/SQLite statement is in the "bundled JSON" form (the owner-authoritative
// deviation) iff it carries the server-side JSON-expansion marker. Detected structurally
// (not by hardcoded op/index) so a new bundling site can't silently escape the check.
function isBundledJson(dialect: 'mysql' | 'sqlite', sql: string): boolean {
  return dialect === 'sqlite' ? sql.includes('json_each(?)') : sql.includes('JSON_TABLE(?');
}
/** The count of `?` placeholders in a rendered SQL (mysql/sqlite keep `?`). */
function placeholderCount(sql: string): number {
  return (sql.match(/\?/g) ?? []).length;
}

describe('#65 v2 SCP ↔ v1 golden parity — all 19 ORM-bench ops × 3 dialects', () => {
  it('golden has exactly the 19 bench ops (coverage guard — no op silently dropped)', () => {
    expect(ALL_OPS.length).toBe(19);
    // Every golden op MUST have a v2 SCP builder (no subset / skip).
    for (const op of ALL_OPS) {
      expect(opBuilders[op], `v2 SCP builder missing for op "${op}"`).toBeTypeOf('function');
    }
    // And no stray builder for a non-existent op.
    for (const op of Object.keys(opBuilders)) {
      expect(ALL_OPS, `builder for unknown op "${op}"`).toContain(op);
    }
  });

  // ── #67 regression guard: the SQLite bundled-JSON upsert (INSERT … SELECT … FROM json_each(?)
  //    … ON CONFLICT … DO UPDATE) MUST carry a `WHERE true` terminating the SELECT source, or
  //    SQLite's parser rejects `ON CONFLICT` (`near "DO": syntax error`). String-compare alone let
  //    this slip; test/parity/orm-execute-parity.test.ts additionally RUNS it on real SQLite. ──
  describe('[sqlite] #67 — bundled-JSON upsert carries `WHERE true` before ON CONFLICT', () => {
    // The ops whose SQLite compile takes the json_each ON CONFLICT DO UPDATE path.
    for (const op of ['Upsert Many (10 records)']) {
      it(op, () => {
        const stmts = opBuilders[op]('sqlite');
        const upsert = stmts.find((s) => /FROM json_each\(\?\)/i.test(s.sql) && /ON CONFLICT/i.test(s.sql));
        expect(upsert, `${op}: expected a json_each ON CONFLICT statement`).toBeDefined();
        expect(upsert!.sql, `${op}: json_each upsert must have WHERE true before ON CONFLICT (#67)`).toMatch(
          /FROM json_each\(\?\)\s+WHERE true\s+ON CONFLICT/i,
        );
      });
    }
  });

  // ── postgres: BYTE-IDENTICAL to the v1 golden (after the documented ?→$N rewrite) ──
  describe('[postgres] v2 SCP SQL is BYTE-IDENTICAL to the v1 golden', () => {
    for (const op of ALL_OPS) {
      it(op, () => {
        const got = opBuilders[op]('postgres');
        const want = goldenStmts(op, 'postgres');
        expect(got.length, `stmt count for "${op}"`).toBe(want.length);
        for (let i = 0; i < want.length; i++) {
          expect(got[i].sql, `${op} stmt#${i} sql`).toBe(want[i].sql);
          expect(got[i].params, `${op} stmt#${i} params`).toEqual(want[i].params);
        }
      });
    }
  });

  // ── mysql / sqlite: non-bundling ops byte-match golden; bundling ops use the v2 JSON form ──
  for (const dialect of ['mysql', 'sqlite'] as const) {
    describe(`[${dialect}] v2 SCP SQL — non-bundling byte-matches golden; bundling ops = v2 JSON-array (authoritative)`, () => {
      for (const op of ALL_OPS) {
        it(op, () => {
          const got = opBuilders[op](dialect);
          const want = goldenStmts(op, dialect);
          expect(got.length, `stmt count for "${op}"`).toBe(want.length);
          for (let i = 0; i < got.length; i++) {
            if (isBundledJson(dialect, got[i].sql)) {
              // AUTHORITATIVE v2 form: the ids/rows are bundled into ONE non-array JSON string
              // parameter, expanded server-side (sqlite json_each / mysql JSON_TABLE). Assert
              // THAT shape — do NOT byte-match the golden's tuple/IN placeholder expansion.
              //  - every bound param is a JSON string (no scalar-per-id explosion);
              //  - each distinct JSON param round-trips as an array;
              //  - the whole statement carries exactly ONE `?` per bundled group (the JSON
              //    handle), so the v2 placeholder count is far below v1's N-placeholder golden
              //    whenever the group holds 2+ ids (single-id groups legitimately match at 1).
              for (let k = 0; k < got[i].params.length; k++) {
                const p = got[i].params[k];
                expect(typeof p, `${op} stmt#${i} param#${k}: JSON string`).toBe('string');
                expect(Array.isArray(JSON.parse(p as string)), `${op} stmt#${i} param#${k}: JSON array`).toBe(true);
              }
              // Placeholder count == number of bundled JSON handles (one `?` per json_each /
              // JSON_TABLE occurrence), NOT the golden's per-id expansion.
              const handles = dialect === 'sqlite'
                ? (got[i].sql.match(/json_each\(\?\)/g) ?? []).length
                : (got[i].sql.match(/JSON_TABLE\(\?/g) ?? []).length;
              expect(placeholderCount(got[i].sql), `${op} stmt#${i}: one ? per JSON handle`).toBe(handles);
              // Guard the DEVIATION is real: v1's golden for this statement uses MORE placeholders
              // (its per-id tuple/IN expansion) whenever the bundle holds >1 id.
              const goldenPh = placeholderCount(want[i].sql);
              if (goldenPh > 1) {
                expect(placeholderCount(got[i].sql), `${op} stmt#${i}: v2 collapses v1's ${goldenPh}-placeholder expansion`).toBeLessThan(goldenPh);
              }
            } else {
              // Non-bundling statement (plain SELECT / single write / composite tuple-IN, which
              // v1 and v2 emit identically): byte-match the golden.
              expect(got[i].sql, `${op} stmt#${i} sql`).toBe(want[i].sql);
              expect(got[i].params, `${op} stmt#${i} params`).toEqual(want[i].params);
            }
          }
        });
      }
    });
  }
});
