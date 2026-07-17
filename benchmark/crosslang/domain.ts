// ════════════════════════════════════════════════════════════════════════════
// Shared benchmark domain — schema, seed data, authored behaviors,
// fixed inputs, and the hand-optimized raw-SQL baseline.
// ════════════════════════════════════════════════════════════════════════════
//
// EVERY language's thin generic runtime runs the SAME 19 access patterns against the
// SAME dataset — ONE production path per language, so only the runtime-layer overhead differs. The
// behaviors are authored ONCE here with the litedbmodel public authoring API and compiled
// to SqlBundles; those bundles (baked into the shared orm-plan.json artifact) are what each
// language's standalone runtime consumes. The raw-SQL baseline strings are the hand-written
// N+1-avoided, projection-tight SQL each op's runtime÷raw overhead is measured against.

import Database from 'better-sqlite3';
import * as lm from '../../dist/scp/index.mjs';
import { CROSSLANG_CASE_IDS } from './contract.js';

// Re-export the canonical case-id order for the generator/adapters.
export const CROSSLANG_CASE_IDS_LOCAL: readonly string[] = CROSSLANG_CASE_IDS;

const L = lm.components();

// ── Schema (shared by every cell) ────────────────────────────────────────────
export const SCHEMA: readonly string[] = [
  `CREATE TABLE users (
     id INTEGER PRIMARY KEY,
     name TEXT NOT NULL,
     post_count INTEGER NOT NULL DEFAULT 0
   );`,
  `CREATE TABLE posts (
     id INTEGER PRIMARY KEY,
     author_id INTEGER NOT NULL,
     title TEXT NOT NULL,
     status TEXT,
     views INTEGER NOT NULL DEFAULT 0,
     created_at TEXT NOT NULL
   );`,
  `CREATE TABLE comments (
     id INTEGER PRIMARY KEY,
     post_id INTEGER NOT NULL,
     body TEXT NOT NULL,
     created_at TEXT NOT NULL
   );`,
  // Gate guard tables for the write-tx case (spec §6 shape).
  `CREATE TABLE uniq (name TEXT NOT NULL, s0 TEXT, f0 TEXT);`,
  // ── ALL-TYPE coverage table ────────────────────────────────────────────────
  // Every §4.1 SQL type + a NULLABLE variant of each, so the typed de-box round-trip
  // (DB value → wire → concrete struct → expected) is exercised for the FULL type set,
  // not just int/text/json. The `deriveReadOutTypes` derivation types each column's bc
  // scalar from THIS DDL (the §4.1 SoT); the round-trip verifier (`coverage-roundtrip.ts`)
  // asserts each column materializes to the correct bc scalar AND the value survives.
  //
  // date → string and decimal → string are INTENTIONAL:
  // bc has NO date/decimal portable scalar (PORTABLE_SCALAR_TYPES = string|int|
  // float|bool|null), so these two columns are
  // VALUE-PRESERVING string round-trips — the compromise is marked in code, never silent.
  //
  // int32 vs int64 SPLIT (TS read-path de-box): the coverage table has BOTH a
  // 32-bit `int32_val` (INT → JS number, exact + JSON-safe) AND a 64-bit `int64_val` (BIGINT
  // → value-preserving decimal STRING, exact + JSON-safe; a JS number rounds past 2^53 and a
  // JS bigint throws in JSON.stringify — so string, mirroring decimal/date→string). The read
  // path materializes each by its SQL column type, consistent across all three drivers.
  //
  // SQLite decimal storage: `dec_val`/`decn_val` map to bc scalar `string` (§4.1 decimal→
  // string), so — matching that representation — the SQLite column is declared **TEXT**,
  // NOT DECIMAL. A DECIMAL/NUMERIC column has SQLite NUMERIC affinity, which coerces the
  // stored value to REAL and DROPS precision on an 18-digit decimal. A TEXT-affinity column
  // stores the exact digit string, so decimal round-
  // trips EXACTLY on SQLite too — the correct SQLite DDL for a string-represented decimal.
  // PG/MySQL keep real DECIMAL/NUMERIC (their drivers already return it as an exact string).
  // The `int64_val` SQLite column stays BIGINT (integer affinity is fine; the read path puts
  // the statement in safeIntegers mode so i64 max arrives as an exact bigint, then stringifies).
  `CREATE TABLE coverage (
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
   );`,
];

// ── Coverage seed: representative + BOUNDARY values for every type ──
// One "full" row (id=1) with every column non-null at a boundary value, and one
// "null" row (id=2) whose nullable variants are all NULL (non-null columns still carry
// their own boundary values). The verifier reads BOTH rows and checks the typed de-box
// against `COVERAGE_EXPECTED` per dialect. Boundary values (§59 checklist): i64 max,
// negative, 0, NULL, decimal precision edge, a real date, true/false, json object AND
// json array.
export const I64_MAX = '9223372036854775807'; // 2^63-1 — the i64 upper boundary (as string; JS Number can't hold it exactly)
export const I64_MIN = '-9223372036854775808'; // -2^63 — the i64 lower boundary
export const I32_MAX = 2147483647; // 2^31-1 — the INT4 upper boundary (fits a JS number exactly)
export const COVERAGE_JSON_OBJECT = { k: 'v', n: 42, nested: { a: [1, 2, 3] } };
export const COVERAGE_JSON_ARRAY = [1, 'two', { three: 3 }, null];

// Canonical JSON text (stable key order) — the value-preserving wire form the verifier
// compares. DBs may reformat JSON whitespace, so the verifier parses+re-stringifies both
// sides through JSON.parse before comparing (structural equality), catching any drift.
export interface CoverageRow {
  id: number;
  int32_val: number; // INT → JS number (exact, JSON-safe): the 32-bit class stays a number
  int64_val: string; // BIGINT → value-preserving decimal STRING (exact + JSON-safe; i64 max rounds as a number)
  real_val: number;
  dec_val: string; // decimal → string (no bc decimal scalar — value/precision-preserving)
  text_val: string;
  bool_val: boolean;
  date_val: string; // date → string (no bc date scalar — value-preserving string)
  json_val: unknown;
  int32n_val: number | null;
  int64n_val: string | null;
  realn_val: number | null;
  decn_val: string | null;
  textn_val: string | null;
  booln_val: boolean | null;
  daten_val: string | null;
  jsonn_val: unknown | null;
}

// Row 1: every column at a boundary/representative value; nullable variants NON-null.
// Row 2: id=2 with 0 / negative / false boundaries and ALL nullable variants = NULL.
export const COVERAGE_EXPECTED: readonly CoverageRow[] = [
  {
    id: 1,
    int32_val: I32_MAX, // INT4 max — stays an exact JS number
    int64_val: I64_MAX, // i64 max boundary — exact value-preserving string
    real_val: 3.141592653589793,
    dec_val: '12345678901234.5678', // 18 significant digits — precision edge (would lose digits if boxed to float64)
    text_val: "coverage-text: 'quotes' & symbols ✓",
    bool_val: true,
    date_val: '2026-07-14',
    json_val: COVERAGE_JSON_OBJECT, // json OBJECT
    int32n_val: -2147483648, // INT4 min (negative) — exact JS number
    int64n_val: I64_MIN, // i64 min boundary (negative) — exact string
    realn_val: -2.5,
    decn_val: '0.0001', // smallest scale-4 decimal
    textn_val: '',
    booln_val: false,
    daten_val: '1970-01-01',
    jsonn_val: COVERAGE_JSON_ARRAY, // json ARRAY
  },
  {
    id: 2,
    int32_val: 0, // zero boundary (number)
    int64_val: '0', // zero boundary (string)
    real_val: 0,
    dec_val: '-98765432109876.5432', // negative decimal precision edge
    text_val: 'row2',
    bool_val: false,
    date_val: '2000-02-29', // leap-day date
    json_val: [], // empty json array
    int32n_val: null,
    int64n_val: null,
    realn_val: null,
    decn_val: null,
    textn_val: null,
    booln_val: null,
    daten_val: null,
    jsonn_val: null,
  },
];

// The projected column list for the coverage `find` (every column — full de-box surface).
export const COVERAGE_COLUMNS = [
  'id', 'int32_val', 'int64_val', 'real_val', 'dec_val', 'text_val', 'bool_val', 'date_val', 'json_val',
  'int32n_val', 'int64n_val', 'realn_val', 'decn_val', 'textn_val', 'booln_val', 'daten_val', 'jsonn_val',
] as const;

// The expected bc scalar per coverage column (the §4.1 mapping — what `deriveReadOutTypes`
// MUST produce, i.e. the concrete native struct field type the codegen path materializes).
// date/decimal → 'string' are value-preserving representations (no bc date/decimal scalar).
// The expected bc portable scalar per coverage column. Both int32 and int64 are the bc `int`
// scalar (the width split is a TS READ-PATH materialization concern, not a portable-type one;
// see COVERAGE_EXPECTED_MATERIALIZE). date/decimal/json → 'string' are the value-preserving /
// JSON-text representations.
export const COVERAGE_EXPECTED_SCALAR: Record<string, 'int' | 'float' | 'string' | 'bool'> = {
  id: 'int',
  int32_val: 'int',
  int64_val: 'int',
  real_val: 'float',
  dec_val: 'string', // no bc decimal scalar — precision-preserving string
  text_val: 'string',
  bool_val: 'bool',
  date_val: 'string', // no bc date scalar — value-preserving string
  json_val: 'string', // JSON column → JSON text (string); TS convenience de/serializes
  int32n_val: 'int',
  int64n_val: 'int',
  realn_val: 'float',
  decn_val: 'string', // no bc decimal scalar
  textn_val: 'string',
  booln_val: 'bool',
  daten_val: 'string', // no bc date scalar
  jsonn_val: 'string',
};

// The expected TS READ-PATH materialized JS form per column (de-box): the split of the
// `int` scalar into number (INT32) vs string (INT64), plus date→string and bool→boolean. Used by
// the round-trip verifier to assert each column materializes to the RIGHT JS type on the read path.
export const COVERAGE_EXPECTED_MATERIALIZE: Record<string, 'number' | 'bigint-string' | 'float' | 'string' | 'bool' | 'json'> = {
  id: 'number', // INTEGER PK → number (32-bit)
  int32_val: 'number',
  int64_val: 'bigint-string', // BIGINT → value-preserving decimal string
  real_val: 'float',
  dec_val: 'string',
  text_val: 'string',
  bool_val: 'bool',
  date_val: 'string',
  json_val: 'json',
  int32n_val: 'number',
  int64n_val: 'bigint-string',
  realn_val: 'float',
  decn_val: 'string',
  textn_val: 'string',
  booln_val: 'bool',
  daten_val: 'string',
  jsonn_val: 'json',
};

// Bind a coverage row to positional params for an INSERT. `bool` is written as 0/1 (SQLite has no
// native boolean; PG/MySQL accept 0/1 too), `json` as its canonical text, decimal + int64 as their
// exact string forms (so the DB stores the exact digits, not a rounded float). int32 binds as a
// plain number (its range is JS-number-exact).
function coverageRowValues(r: CoverageRow): unknown[] {
  const j = (v: unknown): string | null => (v === null ? null : JSON.stringify(v));
  const b = (v: boolean | null): number | null => (v === null ? null : v ? 1 : 0);
  return [
    r.id, r.int32_val, r.int64_val, r.real_val, r.dec_val, r.text_val, b(r.bool_val), r.date_val, j(r.json_val),
    r.int32n_val, r.int64n_val, r.realn_val, r.decn_val, r.textn_val, b(r.booln_val), r.daten_val, j(r.jsonn_val),
  ];
}

export function seedCoverage(db: InstanceType<typeof Database>): void {
  const cols = COVERAGE_COLUMNS.join(', ');
  const ph = COVERAGE_COLUMNS.map(() => '?').join(', ');
  const ins = db.prepare(`INSERT INTO coverage (${cols}) VALUES (${ph})`);
  const tx = db.transaction(() => {
    for (const r of COVERAGE_EXPECTED) ins.run(...coverageRowValues(r));
  });
  tx();
}

// Coverage seed as language-neutral INSERTs (for the PG / MySQL live-DB axis). Decimals /
// dates / i64 / json are single-quoted literals; NULLs are SQL NULL. Booleans are dialect-
// aware: PG's BOOLEAN column rejects an integer literal, so PG gets `TRUE`/`FALSE` while
// MySQL's TINYINT(1)-backed BOOLEAN takes `1`/`0`. The bool columns are the 6th (bool_val)
// and 13th (booln_val) in COVERAGE_COLUMNS.
export function seedCoverageStatements(dialect: 'postgres' | 'mysql' = 'mysql'): string[] {
  const cols = COVERAGE_COLUMNS.join(', ');
  const boolIdx = new Set([COVERAGE_COLUMNS.indexOf('bool_val'), COVERAGE_COLUMNS.indexOf('booln_val')]);
  const lit = (v: unknown, i: number): string => {
    if (v === null) return 'NULL';
    // coverageRowValues already lowered booleans to 0/1; re-lift to a proper boolean literal
    // for PG (it will not accept `1`/`0` for a BOOLEAN column).
    if (boolIdx.has(i) && dialect === 'postgres') return v === 1 || v === true ? 'TRUE' : 'FALSE';
    if (typeof v === 'number') return String(v);
    return `'${String(v).replace(/'/g, "''")}'`;
  };
  return COVERAGE_EXPECTED.map((r) => {
    const vals = coverageRowValues(r).map((v, i) => lit(v, i)).join(', ');
    return `INSERT INTO coverage (${cols}) VALUES (${vals})`;
  });
}

// Deterministic seed: 8 users, 40 posts (5 per user), 200 comments (5 per post).
export function seed(db: InstanceType<typeof Database>): void {
  const insUser = db.prepare('INSERT INTO users (id, name, post_count) VALUES (?,?,?)');
  const insPost = db.prepare(
    'INSERT INTO posts (id, author_id, title, status, views, created_at) VALUES (?,?,?,?,?,?)',
  );
  const insComment = db.prepare(
    'INSERT INTO comments (id, post_id, body, created_at) VALUES (?,?,?,?)',
  );
  const tx = db.transaction(() => {
    for (let u = 1; u <= 8; u++) insUser.run(u, `user-${u}`, 5);
    let pid = 0;
    let cid = 0;
    for (let u = 1; u <= 8; u++) {
      for (let k = 0; k < 5; k++) {
        pid++;
        const status = k % 3 === 0 ? 'live' : k % 3 === 1 ? 'draft' : 'live';
        const day = String(k + 1).padStart(2, '0');
        insPost.run(pid, u, `post-${pid}`, status, pid * 10, `2026-02-${day}`);
        for (let c = 0; c < 5; c++) {
          cid++;
          insComment.run(cid, pid, `comment-${cid}`, `2026-03-01`);
        }
      }
    }
  });
  tx();
}

export function freshDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  for (const s of SCHEMA) db.exec(s);
  seed(db);
  seedCoverage(db); // the ALL-TYPE coverage table + boundary-value rows
  return db;
}

// ── Fixed inputs (identical logical work for every cell) ─────────────────────
export const INPUTS = {
  find: { author_id: 1, status: 'live', since: '2026-02-01' },
  complexWhere: { author_id: 1, since: '2026-02-01', titleLike: 'post-%', ids: [1, 2, 3, 4, 5] },
  inList: { ids: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] },
  belongsTo: { author_id: 1 },
  hasMany: { author_id: 1 },
  hasManyLimit: { author_id: 1 },
  batchInsert: {
    rows: Array.from({ length: 10 }, (_, i) => ({
      author_id: 2,
      title: `bulk-${i}`,
      status: 'live',
      views: 0,
      created_at: '2026-05-01',
    })),
  },
  writeTxGate: { author_id: 1, title: 'txn-post', created_at: '2026-05-01' },
} as const;

// ── Authored read behaviors (compiled to SqlBundles each language's runtime consumes) ──
// ── Inline typed-column declaration — the BC-native column-type SoT ────
// Each read/write projects from THESE declared types (bc never infers types; the consumer inline-
// annotates them, exactly as graphddb declares its typed entity columns). Declared ONCE per table
// here; the read projections + relation projections + write RETURNING columns resolve their types
// from this map. The registration precomputes the codegen `outType` resolver AND the TS read-path
// materialize resolver from it, so de-box is ALWAYS-ON with zero external DDL / zero introspection.
// The coverage columns exercise the full §4.1 type set; the tokens map to the same class on every
// dialect (INT→int32, BIGINT→int64, DATE→date, BOOLEAN→bool, TEXT/DECIMAL→string).
export const MODEL_COLUMNS = {
  posts: {
    id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT', status: 'TEXT', views: 'INTEGER', created_at: 'TEXT',
  },
  users: { id: 'INTEGER', name: 'TEXT', post_count: 'INTEGER' },
  comments: { id: 'INTEGER', post_id: 'INTEGER', body: 'TEXT', created_at: 'TEXT' },
  coverage: {
    id: 'INTEGER', int32_val: 'INT', int64_val: 'BIGINT', real_val: 'REAL', dec_val: 'DECIMAL(20,4)',
    text_val: 'TEXT', bool_val: 'BOOLEAN', date_val: 'DATE', json_val: 'JSON',
    int32n_val: 'INT', int64n_val: 'BIGINT', realn_val: 'REAL', decn_val: 'DECIMAL(20,4)',
    textn_val: 'TEXT', booln_val: 'BOOLEAN', daten_val: 'DATE', jsonn_val: 'JSON',
  },
} as const;

export class Reads extends lm.SemanticBehavior {
  // Inline typed-column declaration: the reads project from these declared types.
  static columns = MODEL_COLUMNS;

  // find: eq + SKIP-optional status + range, ORDER BY.
  Find($: any) {
    return L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title', 'status', 'views', 'created_at'],
      where: [
        lm.whereEq($.author_id, $.author_id),
        lm.when(lm.ne(lm.opt($.status), null), () => lm.whereEq($.status, $.status)),
        lm.whereGe($.created_at, $.since),
      ],
      order: 'id ASC',
    });
  }

  // complexWhere: eq + range + LIKE + IN — multiple predicate kinds in one WHERE.
  ComplexWhere($: any) {
    return L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title', 'status', 'views'],
      where: [
        lm.whereEq($.author_id, $.author_id),
        lm.whereGe($.created_at, $.since),
        lm.whereLike($, 'title', $.titleLike),
        lm.whereIn(lm.inColumn($, 'id'), $.ids),
      ],
      order: 'id ASC',
    });
  }

  // coverage: a find over the ALL-TYPE coverage table binding a scalar eq
  // param (`id`). Projects EVERY column so the typed de-box materializes the full type set
  // (int/real/decimal/text/bool/date/json + nullable variants) into a concrete row struct.
  // Scalar-eq only ⇒ typed-native-coverable (no array/IN-list head).
  CoverageFind($: any) {
    return L.Select({
      table: 'coverage',
      select: [...COVERAGE_COLUMNS],
      where: [lm.whereGe($.id, $.min_id)],
      order: 'id ASC',
    });
  }

  // inList: IN-list, single-JSON param.
  ByIds($: any) {
    return L.Select({
      table: 'posts',
      select: ['id', 'title'],
      where: [lm.whereIn(lm.inColumn($, 'id'), $.ids)],
      order: 'id ASC',
    });
  }

  // The relation cases (belongsTo / hasMany / hasMany-limit) share ONE parent Select
  // (posts by author). The child load is a DECLARED relation batch op (RelationDecl),
  // resolved by the runtime as ONE batched IN query per relation (N+1 avoided) — the
  // fair, hand-baseline-matching relation surface (NOT the per-parent `.map`).
  Posts($: any) {
    return L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title'],
      where: [lm.whereEq($.author_id, $.author_id)],
      order: 'id ASC',
    });
  }
}

// Declarative, N+1-avoided relation ops (one batched IN query each). The `with`
// name selects which relation the read prefetches for that case.
export const REL_BELONGS_TO: any = {
  name: 'author', kind: 'belongsTo', targetTable: 'users', select: ['id', 'name'],
  parentKey: 'author_id', targetKey: 'id', dialect: 'sqlite',
};
export const REL_HAS_MANY: any = {
  name: 'comments', kind: 'hasMany', targetTable: 'comments', select: ['id', 'post_id', 'body'],
  parentKey: 'id', targetKey: 'post_id', dialect: 'sqlite',
};
export const REL_HAS_MANY_LIMIT: any = {
  name: 'recent', kind: 'hasMany', targetTable: 'comments', select: ['id', 'post_id', 'body'],
  parentKey: 'id', targetKey: 'post_id', order: 'id DESC', limit: 3, dialect: 'sqlite',
};

// ── Authored write behavior (single base write; gate contract for the tx case) ──
export class Writes extends lm.SemanticBehavior {
  // Inline typed-column declaration: the write RETURNING columns type from these.
  static columns = MODEL_COLUMNS;

  Create($: any) {
    return L.Insert({
      table: 'posts',
      'values.author_id': $.author_id,
      'values.title': $.title,
      'values.created_at': $.created_at,
      returning: 'id, author_id, title',
    });
  }
}

// The `create` gate contract: referential integrity (author exists) + uniqueness
// (title per author), then the cascade counter — one gate-first tx (spec §6).
export const writeGateContract = lm.entityWrites<Writes>((w: any) => ({
  create: w.lifecycle({
    requires: [w.exists('users', { id: '$.input.author_id' })],
    unique: [
      w.unique({ name: 'title_per_author', guardTable: 'uniq', scope: ['$.input.author_id'], fields: ['$.input.title'] }),
    ],
    derive: [w.increment('users', { id: '$.input.author_id' }, 'post_count', +1)],
  }),
}));

// Register the models. Column types come from the INLINE `static columns` declaration on each class
// — the contract precomputes BOTH the codegen outType resolver and the TS read-path
// materialize resolver from it ONCE, so every read de-boxes (INT→number / BIGINT→string /
// DATE→string / BOOLEAN→boolean) with ZERO external DDL / ZERO per-read DB introspection.
export const readsContract = lm.publishBehaviors(Reads);
export const writesContract = lm.publishBehaviors(Writes);

// ── Hand-optimized raw-SQL baseline (the 1.0× denominator; NOT a strawman) ──────
// Each entry is the tight, N+1-avoided, projection-only SQL a careful engineer
// would hand-write. Relations use ONE batched IN query for the children (no N+1).
// `run(db)` executes it exactly as the ir/codegen path would and returns the same
// logical result; `queries`/`rows` are the fairness counters.
export interface SqlBaselineCase {
  queries: number;
  rows: number;
  run(db: InstanceType<typeof Database>): void;
}

export const SQL_BASELINE: Record<string, SqlBaselineCase> = {
  find: {
    queries: 1,
    rows: 3,
    run(db) {
      db.prepare(
        'SELECT id, author_id, title, status, views, created_at FROM posts WHERE author_id = ? AND status = ? AND created_at >= ? ORDER BY id ASC',
      ).all(INPUTS.find.author_id, INPUTS.find.status, INPUTS.find.since);
    },
  },
  complexWhere: {
    queries: 1,
    rows: 5,
    run(db) {
      db.prepare(
        'SELECT id, author_id, title, status, views FROM posts WHERE author_id = ? AND created_at >= ? AND title LIKE ? AND id IN (?, ?, ?, ?, ?) ORDER BY id ASC',
      ).all(INPUTS.complexWhere.author_id, INPUTS.complexWhere.since, INPUTS.complexWhere.titleLike, ...INPUTS.complexWhere.ids);
    },
  },
  inList: {
    queries: 1,
    rows: 10,
    run(db) {
      const ids = INPUTS.inList.ids;
      db.prepare(`SELECT id, title FROM posts WHERE id IN (${ids.map(() => '?').join(', ')}) ORDER BY id ASC`).all(...ids);
    },
  },
  belongsTo: {
    queries: 2,
    rows: 6, // 5 posts + 1 distinct author (batched, N+1 avoided)
    run(db) {
      const posts = db
        .prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC')
        .all(INPUTS.belongsTo.author_id) as { author_id: number }[];
      const authorIds = [...new Set(posts.map((p) => p.author_id))];
      db.prepare(`SELECT id, name FROM users WHERE id IN (${authorIds.map(() => '?').join(', ')})`).all(...authorIds);
    },
  },
  hasMany: {
    queries: 2,
    rows: 30, // 5 posts + 25 comments (one batched IN query, N+1 avoided)
    run(db) {
      const posts = db
        .prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC')
        .all(INPUTS.hasMany.author_id) as { id: number }[];
      const ids = posts.map((p) => p.id);
      db.prepare(`SELECT id, post_id, body FROM comments WHERE post_id IN (${ids.map(() => '?').join(', ')})`).all(...ids);
    },
  },
  hasManyLimit: {
    queries: 2,
    rows: 20, // 5 posts + 3 comments/parent (per-parent LIMIT 3) = 15 children
    run(db) {
      const posts = db
        .prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC')
        .all(INPUTS.hasManyLimit.author_id) as { id: number }[];
      const ids = posts.map((p) => p.id);
      // Per-parent LIMIT 3 via ROW_NUMBER (the hand-optimized batched form; N+1 avoided).
      db.prepare(
        `SELECT id, post_id, body FROM (
           SELECT id, post_id, body, ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY id DESC) rn
           FROM comments WHERE post_id IN (${ids.map(() => '?').join(', ')})
         ) WHERE rn <= 3`,
      ).all(...ids);
    },
  },
  batchInsert: {
    queries: 1,
    rows: 0,
    run(db) {
      const rows = INPUTS.batchInsert.rows;
      const cols = ['author_id', 'title', 'status', 'views', 'created_at'];
      const placeholders = rows.map(() => `(${cols.map(() => '?').join(',')})`).join(',');
      const flat: unknown[] = [];
      for (const r of rows) flat.push(r.author_id, r.title, r.status, r.views, r.created_at);
      db.prepare(`INSERT INTO posts (${cols.join(',')}) VALUES ${placeholders}`).run(...flat);
    },
  },
  writeTxGate: {
    queries: 4, // gate:requires + gate:unique + body INSERT(RETURNING) + derive UPDATE (one tx)
    rows: 2, // requires SELECT (1) + INSERT RETURNING (1)
    run(db) {
      const inp = INPUTS.writeTxGate;
      const tx = db.transaction(() => {
        const author = db.prepare('SELECT 1 FROM users WHERE id = ?').get(inp.author_id);
        if (!author) throw new Error('requires_absent');
        db.prepare('INSERT INTO uniq (name, s0, f0) VALUES (?, ?, ?) ON CONFLICT DO NOTHING').run(
          'title_per_author',
          String(inp.author_id),
          inp.title,
        );
        // Body write returns the new row (RETURNING parity with the lm write path).
        db.prepare('INSERT INTO posts (author_id, title, created_at) VALUES (?, ?, ?) RETURNING id, author_id, title').get(
          inp.author_id,
          inp.title,
          inp.created_at,
        );
        db.prepare('UPDATE users SET post_count = post_count + ? WHERE id = ?').run(1, inp.author_id);
      });
      tx();
    },
  },
};

// The entry component name + relations for each read case, so every cell resolves
// the SAME bundle. Write cases route through the write/batch bundle helpers.
export const READ_ENTRY: Record<string, string> = {
  find: 'Find',
  complexWhere: 'ComplexWhere',
  inList: 'ByIds',
  belongsTo: 'Posts',
  hasMany: 'Posts',
  hasManyLimit: 'Posts',
};

// The coverage read, kept SEPARATE from the perf matrix
// (CROSSLANG_CASE_IDS) — it is a correctness/round-trip case, wired here so the
// `coverage-roundtrip` verifier resolves the SAME entry + input.
export const COVERAGE_ENTRY = 'CoverageFind';
export const COVERAGE_INPUT = { min_id: 1 } as const; // id >= 1 ⇒ both seeded rows

// For the relation cases: which declared relation the read prefetches (`with`).
export const READ_RELATION: Record<string, { decl: any; withName: string } | undefined> = {
  belongsTo: { decl: REL_BELONGS_TO, withName: 'author' },
  hasMany: { decl: REL_HAS_MANY, withName: 'comments' },
  hasManyLimit: { decl: REL_HAS_MANY_LIMIT, withName: 'recent' },
};

// ── Real-DB (PG / MySQL) schema + seed for the DB-backed axis ─────
// Every language's DB-backed cell creates its OWN bench tables (drop-then-create) in
// an ISOLATED namespace, seeds the SAME 8-user/40-post/200-comment dataset the SQLite
// cells use, and runs the 8 cases against the REAL dockerized DB. The table names are
// the SAME (`users`/`posts`/`comments`/`uniq`) as the compiled bundles reference, so
// the SAME bundle SQL executes unchanged — only the connection + dialect differ.
// `posts.id` must AUTO-GENERATE: the batchInsert / writeTxGate cases INSERT posts
// with no id (matching SQLite's implicit rowid). PG uses SERIAL, MySQL AUTO_INCREMENT.
// After seeding explicit ids the PG sequence is bumped past the seeded max (see
// `pgSeqResetStatements`) so the write cases' new rows don't collide.
//
// This bench MUST NOT touch the shared `testdb`
// fixture tables that `test/fixtures/init.sql` seeds for the integration suite. Every
// language isolates into its OWN `scp_<lang>_bench` schema (PG, via search_path) /
// database (MySQL) — Rust/Go/PHP already do this (see their adapters' `PG_SCHEMA_NAME`/
// `MYSQL_DB_NAME` = `scp_rust_bench`/`scp_go_bench`/`scp_php_bench`); TS uses
// `scp_ts_bench` (below, `PG_CONN`/`MYSQL_CONN`).
export const PG_SCHEMA: readonly string[] = [
  `DROP TABLE IF EXISTS comments CASCADE`,
  `DROP TABLE IF EXISTS posts CASCADE`,
  `DROP TABLE IF EXISTS users CASCADE`,
  `DROP TABLE IF EXISTS uniq CASCADE`,
  `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, post_count INTEGER NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id SERIAL PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, views INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL)`,
  `CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL)`,
  // `s0` binds `author_id` (always numeric) — INTEGER, matching the conformance corpus's
  // proven-working `uniq` schema (gen-livedb.ts). A TEXT s0 works under the permissive
  // text-protocol drivers (pg / psycopg) but pgx's strict binary protocol (Go) rejects an
  // int64 arg for a text column ("unable to encode into text format") — INTEGER is correct for
  // every driver since the column only ever stores a numeric author_id.
  `CREATE TABLE uniq (name TEXT NOT NULL, s0 INTEGER, f0 TEXT)`,
  // Coverage table, PG dialect. `NUMERIC` → bc string (precision-preserving),
  // `DOUBLE PRECISION` → bc float, `BOOLEAN` → bc bool, `DATE` → bc string, `JSONB` → bc
  // string (JSON text). The DDL token drives the §4.1 scalar; the round-trip verifier
  // checks each column materializes to the correct bc scalar on the LIVE PG driver.
  `DROP TABLE IF EXISTS coverage CASCADE`,
  `CREATE TABLE coverage (
     id INTEGER PRIMARY KEY,
     int32_val INTEGER NOT NULL,
     int64_val BIGINT NOT NULL,
     real_val DOUBLE PRECISION NOT NULL,
     dec_val NUMERIC(20,4) NOT NULL,
     text_val TEXT NOT NULL,
     bool_val BOOLEAN NOT NULL,
     date_val DATE NOT NULL,
     json_val JSONB NOT NULL,
     int32n_val INTEGER,
     int64n_val BIGINT,
     realn_val DOUBLE PRECISION,
     decn_val NUMERIC(20,4),
     textn_val TEXT,
     booln_val BOOLEAN,
     daten_val DATE,
     jsonn_val JSONB
   )`,
];
export const MYSQL_SCHEMA: readonly string[] = [
  `SET FOREIGN_KEY_CHECKS = 0`,
  `DROP TABLE IF EXISTS comments`,
  `DROP TABLE IF EXISTS posts`,
  `DROP TABLE IF EXISTS users`,
  `DROP TABLE IF EXISTS uniq`,
  `SET FOREIGN_KEY_CHECKS = 1`,
  `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, post_count INT NOT NULL DEFAULT 0)`,
  `CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, status VARCHAR(255), views INT NOT NULL DEFAULT 0, created_at VARCHAR(255) NOT NULL)`,
  `CREATE TABLE comments (id INT PRIMARY KEY, post_id INT NOT NULL, body VARCHAR(255) NOT NULL, created_at VARCHAR(255) NOT NULL)`,
  // `s0` binds `author_id` (always numeric) — INT, matching the PG schema's fix above.
  `CREATE TABLE uniq (name VARCHAR(255) NOT NULL, s0 INT, f0 VARCHAR(255))`,
  // Coverage table, MySQL dialect. `DECIMAL` → bc string (precision), `DOUBLE`
  // → bc float, `BOOLEAN` (TINYINT(1) alias) → bc bool, `DATE` → bc string, `JSON` → bc
  // string. MySQL DECIMAL is the classic single-driver precision hole (mysql2 returns it as
  // a STRING already, PG as string, SQLite as REAL/text) — the verifier compares the string
  // form so any drift shows up on the offending driver only.
  `DROP TABLE IF EXISTS coverage`,
  `CREATE TABLE coverage (
     id INT PRIMARY KEY,
     int32_val INT NOT NULL,
     int64_val BIGINT NOT NULL,
     real_val DOUBLE NOT NULL,
     dec_val DECIMAL(20,4) NOT NULL,
     text_val TEXT NOT NULL,
     bool_val BOOLEAN NOT NULL,
     date_val DATE NOT NULL,
     json_val JSON NOT NULL,
     int32n_val INT,
     int64n_val BIGINT,
     realn_val DOUBLE,
     decn_val DECIMAL(20,4),
     textn_val TEXT,
     booln_val BOOLEAN,
     daten_val DATE,
     jsonn_val JSON
   )`,
];

// After seeding 40 posts with explicit ids 1..40, advance the PG SERIAL sequence so a
// subsequent no-id INSERT gets id 41+ (MySQL AUTO_INCREMENT self-advances past the max).
export const PG_SEQ_RESET: readonly string[] = [`SELECT setval('posts_id_seq', (SELECT MAX(id) FROM posts))`];

// The seed as language-neutral INSERT statements (the SAME dataset as `seed()` above),
// portable across PG/MySQL. Deterministic: 8 users, 40 posts, 200 comments.
export function seedStatementsShared(): string[] {
  const stmts: string[] = [];
  for (let u = 1; u <= 8; u++) stmts.push(`INSERT INTO users (id, name, post_count) VALUES (${u}, 'user-${u}', 5)`);
  let pid = 0;
  let cid = 0;
  for (let u = 1; u <= 8; u++) {
    for (let k = 0; k < 5; k++) {
      pid++;
      const status = k % 3 === 0 ? 'live' : k % 3 === 1 ? 'draft' : 'live';
      const day = String(k + 1).padStart(2, '0');
      stmts.push(`INSERT INTO posts (id, author_id, title, status, views, created_at) VALUES (${pid}, ${u}, 'post-${pid}', '${status}', ${pid * 10}, '2026-02-${day}')`);
      for (let c = 0; c < 5; c++) {
        cid++;
        stmts.push(`INSERT INTO comments (id, post_id, body, created_at) VALUES (${cid}, ${pid}, 'comment-${cid}', '2026-03-01')`);
      }
    }
  }
  return stmts;
}

// Env-driven connection config (matches docker-compose.test.yml + WS6 host defaults:
// PG 5433, MySQL 3307 when the livedb override republishes the ports to the host).
//
// TS is isolated into its OWN `scp_ts_bench` namespace (mirroring the
// Rust/Go/PHP adapters' `scp_<lang>_bench`), never the shared `testdb` fixture tables
// `test/fixtures/init.sql` seeds for the integration suite.
//   - PG: `PG_BOOT_CONN` connects to the base `testdb` (bootstrap-only, to CREATE SCHEMA
//     IF NOT EXISTS); `PG_CONN` is the actual bench connection and sets `search_path` via
//     the libpq startup `options` param — the pool-safe way to pin a schema, since `pg.Pool`
//     multiplexes many physical connections and a runtime `SET search_path` on one borrowed
//     connection would NOT apply to the others (unlike a single-connection driver).
//   - MySQL: `MYSQL_BOOT_CONN` connects to the base `testdb` (bootstrap-only, to CREATE
//     DATABASE IF NOT EXISTS); `MYSQL_CONN` points `database` directly at `scp_ts_bench` —
//     MySQL has no cross-database "current schema" ambiguity, so this is the direct analog of
//     Rust/Go/PHP's approach.
export const PG_SCHEMA_NAME = 'scp_ts_bench';
export const MYSQL_DB_NAME = 'scp_ts_bench';

export const PG_BOOT_CONN = {
  host: process.env.TEST_DB_HOST || 'localhost',
  port: parseInt(process.env.TEST_DB_PORT || '5433', 10),
  database: process.env.TEST_DB_NAME || 'testdb',
  user: process.env.TEST_DB_USER || 'testuser',
  password: process.env.TEST_DB_PASSWORD || 'testpass',
};
export const PG_CONN = {
  ...PG_BOOT_CONN,
  options: `-c search_path=${PG_SCHEMA_NAME}`,
};
export const MYSQL_BOOT_CONN = {
  host: process.env.TEST_MYSQL_HOST || 'localhost',
  port: parseInt(process.env.TEST_MYSQL_PORT || '3307', 10),
  database: process.env.TEST_MYSQL_DB || 'testdb',
  user: process.env.TEST_MYSQL_USER || 'testuser',
  password: process.env.TEST_MYSQL_PASSWORD || 'testpass',
};
export const MYSQL_CONN = {
  ...MYSQL_BOOT_CONN,
  database: MYSQL_DB_NAME,
};
