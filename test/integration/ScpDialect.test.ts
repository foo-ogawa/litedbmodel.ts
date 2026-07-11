/**
 * WS6 (#26) SCP dialect integration — the compiled Postgres + MySQL SQL executes correctly
 * against REAL dockerized Postgres + MySQL, and yields RESULT PARITY with v1 direct execution
 * on the same dialect.
 *
 * This is the WS where docker genuinely applies (the SCP TS runtime seam is synchronous SQLite;
 * PG/MySQL are async). It proves BOTH #26 AC clauses that need a live DB:
 *   (a) the SCP-compiled dialect SQL (Backend Compile → render → `?`→`$N` for PG) executes on the
 *       real DB across CRUD + relations + write-tx;
 *   (b) result parity — the SCP path returns the same rows / leaves the same DB state as v1
 *       direct execution of the equivalent v1-SqlBuilder / v1-condition SQL on the SAME dialect.
 *
 * The SCP path renders + executes through the CURRENT shipped makeSQL runtime — reads via
 * `compileBundle` + `executeBundleAsync` + `pgPoolExecutor`/`mysqlPoolExecutor`, writes/tx via the
 * derived `TransactionPlan` + `renderTxStatement`, relation batches via `compileRelationOp` +
 * `renderReadPrimary`-style render (resolving the #46 deferred PG array cast from the real keys).
 * No mock, no hand-written SQL for the SCP side. The v1 side calls the REAL v1 SqlBuilders /
 * DBConditions / `inferPgArrayType` (not hand-written expectations — avoids the WS3 faked-parity
 * pattern).
 *
 * Run in-container via the compose `test-integration` service (TEST_DB_HOST=postgres /
 * TEST_MYSQL_HOST=mysql on the internal network), or locally with published ports + env.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import mysql from 'mysql2/promise';
import { DBConditions } from '../../src/DBConditions';
import { postgresSqlBuilder } from '../../src/drivers/PostgresSqlBuilder';
import { mysqlSqlBuilder } from '../../src/drivers/MysqlSqlBuilder';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileBundle,
  compileWriteBundle,
  compileCreateManyBundle,
  compileUpdateManyBundle,
  compileDeleteManyBundle,
  compileRelationOp,
  executeBundleAsync,
  pgPoolExecutor,
  mysqlPoolExecutor,
  renderTxStatement,
  renderReadPrimary,
  renderPlaceholders,
  resolvePgArrayCast,
  inferPgArrayType,
  stripMysqlPkHint,
  whereEq,
  whereGe,
  whereIn,
  or,
  coalesce,
  opt,
  inColumn,
  entityWrites,
  type SqlBundle,
  type In,
  type BehaviorModelContract,
  type RelationOp,
} from '../../src/scp';

// ── Connection config (env-driven; matches docker-compose.test.yml) ────────────

const PG = {
  host: process.env.TEST_DB_HOST || 'localhost',
  port: parseInt(process.env.TEST_DB_PORT || '5433', 10),
  database: process.env.TEST_DB_NAME || 'testdb',
  user: process.env.TEST_DB_USER || 'testuser',
  password: process.env.TEST_DB_PASSWORD || 'testpass',
};
const MY = {
  host: process.env.TEST_MYSQL_HOST || 'localhost',
  port: parseInt(process.env.TEST_MYSQL_PORT || '3307', 10),
  database: process.env.TEST_MYSQL_DB || 'testdb',
  user: process.env.TEST_MYSQL_USER || 'testuser',
  password: process.env.TEST_MYSQL_PASSWORD || 'testpass',
};

const L = components();

// ── Isolated table namespace (fix #37) ─────────────────────────────────────────
// This file owns dedicated `scp_posts` / `scp_users` tables that it seeds and tears
// down itself (see beforeAll/afterAll). This is TRUE data isolation from the shared
// `posts`/`users` seed that other integration files (e.g. Mysql.test.ts) wipe and
// reseed in their own beforeEach — under the single shared MySQL `testdb`, running
// all integration files together previously let that reseed clobber the base seed
// this file depended on, tripping the "rows exist" assertions. Mirrors WS7g's
// per-namespace live-DB isolation (scp_* databases). Same normative render/compile
// path and the same v1 builders/DBConditions — only the table identifiers change.
const T_POSTS = 'scp_posts';
const T_USERS = 'scp_users';
// #46 item 4: a `typed` table with a BIGINT / TEXT / BOOL / TIMESTAMP / NUMERIC key column each, for
// the all-element-types no-cast `= ANY($1)` IN-list live coverage on real PG + MySQL (TS leg).
const T_TYPED = 'scp_typed';
const TYPED_BIG_TS = [5000000001, 5000000002, 5000000003] as const;
const TYPED_TS_TS = ['2026-01-01 00:00:00', '2026-02-01 00:00:00', '2026-03-01 00:00:00'] as const;
// #47 item 1: composite-key relation tables — (tenant_id, doc_id) docs, (tenant_id, uid) users.
const T_DOCS2 = 'scp_docs2';
const T_USERS2 = 'scp_users2';
const T_REVS = 'scp_revs';
// Fixed UUIDs for the #46 uuid IN-list coverage (posts 1/2/3).
const POST_GUIDS = [
  '11111111-1111-1111-1111-111111111111',
  '22222222-2222-2222-2222-222222222222',
  '33333333-3333-3333-3333-333333333333',
];

// ── The authored behaviors (declaration surface — dialect-neutral IR) ──────────

class PostQueries extends SemanticBehavior {
  ByUser($: In<{ user_id: number }>) {
    return L.Select({
      table: T_POSTS,
      select: ['id', 'user_id', 'title', 'view_count'],
      where: [whereEq($.user_id, $.user_id)],
      order: 'id ASC',
    });
  }

  ByIds($: In<{ ids: number[] }>) {
    return L.Select({
      table: T_POSTS,
      select: ['id', 'title'],
      where: [whereIn(inColumn($, 'id'), $.ids)],
      order: 'id ASC',
    });
  }

  // #46: an IN-list on a UUID column. Value-inference cannot tell a uuid from text by value, so
  // the authored surface emits `= ANY($1)` with NO cast and lets PG infer `uuid[]` from the column.
  ByGuids($: In<{ guids: string[] }>) {
    return L.Select({
      table: T_POSTS,
      select: ['id', 'guid'],
      where: [whereIn(inColumn($, 'guid'), $.guids)],
      order: 'id ASC',
    });
  }

  // #46 item 4: no-cast `= ANY($1)` IN-list on each PG element type — bigint / text / bool /
  // timestamp / numeric. Each selects the stable text `label` so the assertion is dialect-invariant;
  // PG infers the array element type from the column, MySQL uses the single-JSON form.
  ByBig($: In<{ keys: number[] }>) {
    return L.Select({ table: T_TYPED, select: ['label'], where: [whereIn(inColumn($, 'big'), $.keys)], order: 'label ASC' });
  }
  ByTxt($: In<{ keys: string[] }>) {
    return L.Select({ table: T_TYPED, select: ['label'], where: [whereIn(inColumn($, 'txt'), $.keys)], order: 'label ASC' });
  }
  ByFlag($: In<{ keys: boolean[] }>) {
    return L.Select({ table: T_TYPED, select: ['label'], where: [whereIn(inColumn($, 'flag'), $.keys)], order: 'label ASC' });
  }
  ByTs($: In<{ keys: string[] }>) {
    return L.Select({ table: T_TYPED, select: ['label'], where: [whereIn(inColumn($, 'ts'), $.keys)], order: 'label ASC' });
  }
  ByAmt($: In<{ keys: number[] }>) {
    return L.Select({ table: T_TYPED, select: ['label'], where: [whereIn(inColumn($, 'amt'), $.keys)], order: 'label ASC' });
  }

  // #47 item 5 — the WHERE assembly (AND/OR group) + LIMIT/OFFSET tail are now driven from v1's
  // DBConditions/compileSelect (not a v2 hand-roll); this behavior exercises both live for parity.
  // An OR group over two eq members + a LIMIT + OFFSET.
  Page($: In<{ user_id: number; other_id: number; offset: number }>) {
    return L.Select({
      table: T_POSTS,
      select: ['id', 'user_id', 'title'],
      where: [or(whereEq($.user_id, $.user_id), whereEq($.user_id, $.other_id))],
      order: 'id ASC',
      limit: 2,
      offset: coalesce(opt($.offset), 0),
    });
  }

  // count() (#47 item 2 — v1 `DBModel._count`): `SELECT COUNT(*) as count FROM t[ WHERE …]`.
  CountAll(_$: In<Record<string, never>>) {
    return L.Count({ table: T_POSTS });
  }
  CountByUser($: In<{ user_id: number }>) {
    return L.Count({ table: T_POSTS, where: [whereEq($.user_id, $.user_id)] });
  }
}

class PostWrites extends SemanticBehavior {
  Create($: In<{ user_id: number; title: string; content: string }>) {
    return L.Insert({
      table: T_POSTS,
      'values.user_id': $.user_id,
      'values.title': $.title,
      'values.content': $.content,
      returning: 'id, user_id, title, view_count',
    });
  }
}

// A write-time-relations Command: Insert a post + derive the author's post count in ONE tx.
class CreatePostWithCount extends SemanticBehavior {
  Create($: In<{ user_id: number; title: string; content: string }>) {
    return L.Insert({
      table: T_POSTS,
      'values.user_id': $.user_id,
      'values.title': $.title,
      'values.content': $.content,
      returning: 'id, user_id, title',
    });
  }
}

const postCountWrites = entityWrites<CreatePostWithCount>((w) => ({
  create: w.lifecycle({
    requires: [w.exists(T_USERS, { id: '$.input.user_id' })],
    derive: [w.increment(T_USERS, { id: '$.input.user_id' }, 'post_count_scp', +1)],
  }),
}));

// ── Async driver seams: render a compiled op (with dialect) + execute on the real DB ──

type Row = Record<string, unknown>;

async function pgQuery(pool: Pool, sql: string, params: unknown[]): Promise<Row[]> {
  const res = await pool.query(sql, params);
  return res.rows as Row[];
}

async function myQuery(conn: mysql.Connection, sql: string, params: unknown[]): Promise<Row[]> {
  const [rows] = await conn.query(sql, params);
  return Array.isArray(rows) ? (rows as Row[]) : [];
}

/** bc evaluates ints to bigint; convert to a driver-bindable JS value (numbers for i32 range). */
function toPlain(v: unknown): unknown {
  if (typeof v === 'bigint') return Number(v);
  return v;
}

/**
 * Render a relation batch op's PG/MySQL SQL for a bound key set — the SAME render `runRelationOp`
 * performs: resolve the deferred PG array cast (#46) from the real keys, then `?`→`$N`. (The
 * MySQL/SQLite JSON single-param form carries no cast token, so it is a straight placeholder render.)
 */
function renderRelationSql(op: RelationOp, keys: unknown[]): string {
  const cast = op.dialect === 'postgres' ? resolvePgArrayCast(op.sql, keys) : op.sql;
  return renderPlaceholders(cast, op.dialect);
}

/**
 * Render + bind a COMPOSITE relation op for a set of parent key tuples (#47 item 1) — the SAME work
 * the composite `runRelationOp` does: resolve ONE deferred PG cast per key column, `?`→`$N`, then
 * bind ONE array param PER column (PG, transposed) / ONE JSON array-of-tuples param (MySQL). Returns
 * `{ sql, params }` for direct pool execution.
 */
function renderCompositeRelation(
  op: RelationOp,
  cols: readonly string[],
  tuples: readonly unknown[][],
): { sql: string; params: unknown[] } {
  let cast = op.sql;
  if (op.dialect === 'postgres') {
    for (let col = 0; col < cols.length; col++) cast = resolvePgArrayCast(cast, tuples.map((t) => t[col]));
    return { sql: renderPlaceholders(cast, op.dialect), params: cols.map((_, col) => tuples.map((t) => t[col])) };
  }
  const sql = renderPlaceholders(op.sql, op.dialect);
  return { sql, params: [JSON.stringify(tuples.map((t) => [...t]))] };
}

// ── Test lifecycle: connect, add the derive column, clean state ────────────────

let pgPool: Pool | null = null;
let myConn: mysql.Connection | null = null;

beforeAll(async () => {
  try {
    pgPool = new Pool(PG);
    await pgPool.query('SELECT 1');
    // Own, isolated tables (fix #37) — drop-then-create so the seed is deterministic
    // regardless of what other integration files did to the shared `posts`/`users`.
    // `post_count_scp` on scp_users is the write-tx derive target.
    await pgPool.query(`DROP TABLE IF EXISTS ${T_POSTS} CASCADE`);
    await pgPool.query(`DROP TABLE IF EXISTS ${T_USERS} CASCADE`);
    await pgPool.query(`
      CREATE TABLE ${T_USERS} (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        post_count_scp INTEGER NOT NULL DEFAULT 0
      )`);
    await pgPool.query(`
      CREATE TABLE ${T_POSTS} (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES ${T_USERS}(id),
        title VARCHAR(255) NOT NULL,
        content TEXT,
        view_count INTEGER NOT NULL DEFAULT 0,
        guid UUID
      )`);
    // Deterministic seed: users id 1,2; posts id 1,2 (user 1) and id 3 (user 2)
    // — matches the parity fixtures (user_id=1 present, id IN (1,3) present). `guid` is a UUID
    // column (#46 uuid IN-list coverage): posts 1/2/3 carry the three POST_GUIDS values.
    await pgPool.query(`INSERT INTO ${T_USERS} (id, name) VALUES (1, 'Alice'), (2, 'Bob')`);
    await pgPool.query(`SELECT setval('${T_USERS}_id_seq', 2)`);
    await pgPool.query(`INSERT INTO ${T_POSTS} (id, user_id, title, content, view_count, guid) VALUES
      (1, 1, 'First Post', 'Hello World!', 100, '${POST_GUIDS[0]}'),
      (2, 1, 'Second Post', 'Another post', 0, '${POST_GUIDS[1]}'),
      (3, 2, 'Bob''s Post', 'Content here', 50, '${POST_GUIDS[2]}')`);
    await pgPool.query(`SELECT setval('${T_POSTS}_id_seq', 3)`);
    // #46 item 4: the typed IN-list table (bigint/text/bool/timestamp/numeric key columns).
    await pgPool.query(`DROP TABLE IF EXISTS ${T_TYPED} CASCADE`);
    await pgPool.query(`
      CREATE TABLE ${T_TYPED} (
        big BIGINT PRIMARY KEY, txt TEXT NOT NULL, flag BOOLEAN NOT NULL,
        ts TIMESTAMP NOT NULL, amt NUMERIC(10,2) NOT NULL, label TEXT NOT NULL
      )`);
    await pgPool.query(`INSERT INTO ${T_TYPED} VALUES
      (${TYPED_BIG_TS[0]}, 'alpha', TRUE,  '${TYPED_TS_TS[0]}', 10.50, 'A'),
      (${TYPED_BIG_TS[1]}, 'beta',  FALSE, '${TYPED_TS_TS[1]}', 20.25, 'B'),
      (${TYPED_BIG_TS[2]}, 'gamma', TRUE,  '${TYPED_TS_TS[2]}', 30.75, 'C')`);
    // #47 item 1: composite-key relation tables — two tenants share uid/doc_id (100 / 10).
    await pgPool.query(`DROP TABLE IF EXISTS ${T_DOCS2}`);
    await pgPool.query(`DROP TABLE IF EXISTS ${T_USERS2}`);
    await pgPool.query(`DROP TABLE IF EXISTS ${T_REVS}`);
    await pgPool.query(`CREATE TABLE ${T_USERS2} (tenant_id INT, uid INT, name TEXT, PRIMARY KEY (tenant_id, uid))`);
    await pgPool.query(`CREATE TABLE ${T_DOCS2} (tenant_id INT, doc_id INT, owner_id INT, title TEXT, PRIMARY KEY (tenant_id, doc_id))`);
    await pgPool.query(`CREATE TABLE ${T_REVS} (tenant_id INT, doc_id INT, rev TEXT, PRIMARY KEY (tenant_id, doc_id, rev))`);
    await pgPool.query(`INSERT INTO ${T_USERS2} VALUES (1,100,'Ada'),(1,101,'Alan'),(2,100,'Bob')`);
    await pgPool.query(`INSERT INTO ${T_DOCS2} VALUES (1,10,100,'Doc A1'),(1,11,101,'Doc B1'),(2,10,100,'Doc A2')`);
    await pgPool.query(`INSERT INTO ${T_REVS} VALUES (1,10,'r1'),(1,10,'r2'),(1,11,'r3'),(2,10,'r9')`);
  } catch (e) {
    throw new Error(`Postgres is required for WS6 integration but is not reachable at ${PG.host}:${PG.port} — ${(e as Error).message}`);
  }
  try {
    myConn = await mysql.createConnection({ ...MY, multipleStatements: false });
    await myConn.query('SELECT 1');
    await myConn.query(`DROP TABLE IF EXISTS ${T_POSTS}`);
    await myConn.query(`DROP TABLE IF EXISTS ${T_USERS}`);
    await myConn.query(`
      CREATE TABLE ${T_USERS} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        post_count_scp INT NOT NULL DEFAULT 0
      )`);
    await myConn.query(`
      CREATE TABLE ${T_POSTS} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        title VARCHAR(255) NOT NULL,
        content TEXT,
        view_count INT NOT NULL DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES ${T_USERS}(id)
      )`);
    await myConn.query(`INSERT INTO ${T_USERS} (id, name) VALUES (1, 'Alice'), (2, 'Bob')`);
    await myConn.query(`INSERT INTO ${T_POSTS} (id, user_id, title, content, view_count) VALUES
      (1, 1, 'First Post', 'Hello World!', 100),
      (2, 1, 'Second Post', 'Another post', 0),
      (3, 2, 'Bob''s Post', 'Content here', 50)`);
    // #46 item 4: the typed IN-list table (MySQL types; flag as TINYINT(1), ts DATETIME, amt DECIMAL).
    await myConn.query(`DROP TABLE IF EXISTS ${T_TYPED}`);
    await myConn.query(`
      CREATE TABLE ${T_TYPED} (
        big BIGINT PRIMARY KEY, txt VARCHAR(255) NOT NULL, flag TINYINT(1) NOT NULL,
        ts DATETIME NOT NULL, amt DECIMAL(10,2) NOT NULL, label VARCHAR(255) NOT NULL
      )`);
    await myConn.query(`INSERT INTO ${T_TYPED} VALUES
      (${TYPED_BIG_TS[0]}, 'alpha', 1, '${TYPED_TS_TS[0]}', 10.50, 'A'),
      (${TYPED_BIG_TS[1]}, 'beta',  0, '${TYPED_TS_TS[1]}', 20.25, 'B'),
      (${TYPED_BIG_TS[2]}, 'gamma', 1, '${TYPED_TS_TS[2]}', 30.75, 'C')`);
    // #47 item 1: composite-key relation tables (MySQL types).
    await myConn.query(`DROP TABLE IF EXISTS ${T_DOCS2}`);
    await myConn.query(`DROP TABLE IF EXISTS ${T_USERS2}`);
    await myConn.query(`DROP TABLE IF EXISTS ${T_REVS}`);
    await myConn.query(`CREATE TABLE ${T_USERS2} (tenant_id INT, uid INT, name VARCHAR(255), PRIMARY KEY (tenant_id, uid))`);
    await myConn.query(`CREATE TABLE ${T_DOCS2} (tenant_id INT, doc_id INT, owner_id INT, title VARCHAR(255), PRIMARY KEY (tenant_id, doc_id))`);
    await myConn.query(`CREATE TABLE ${T_REVS} (tenant_id INT, doc_id INT, rev VARCHAR(255), PRIMARY KEY (tenant_id, doc_id, rev))`);
    await myConn.query(`INSERT INTO ${T_USERS2} VALUES (1,100,'Ada'),(1,101,'Alan'),(2,100,'Bob')`);
    await myConn.query(`INSERT INTO ${T_DOCS2} VALUES (1,10,100,'Doc A1'),(1,11,101,'Doc B1'),(2,10,100,'Doc A2')`);
    await myConn.query(`INSERT INTO ${T_REVS} VALUES (1,10,'r1'),(1,10,'r2'),(1,11,'r3'),(2,10,'r9')`);
  } catch (e) {
    throw new Error(`MySQL is required for WS6 integration but is not reachable at ${MY.host}:${MY.port} — ${(e as Error).message}`);
  }
});

afterAll(async () => {
  // Tear down our isolated tables so no residue leaks to other files/runs.
  try {
    if (pgPool) {
      await pgPool.query(`DROP TABLE IF EXISTS ${T_TYPED} CASCADE`);
      await pgPool.query(`DROP TABLE IF EXISTS ${T_DOCS2} CASCADE`);
      await pgPool.query(`DROP TABLE IF EXISTS ${T_USERS2} CASCADE`);
      await pgPool.query(`DROP TABLE IF EXISTS ${T_REVS} CASCADE`);
      await pgPool.query(`DROP TABLE IF EXISTS ${T_POSTS} CASCADE`);
      await pgPool.query(`DROP TABLE IF EXISTS ${T_USERS} CASCADE`);
    }
  } catch {
    /* best-effort cleanup */
  }
  try {
    if (myConn) {
      await myConn.query(`DROP TABLE IF EXISTS ${T_TYPED}`);
      await myConn.query(`DROP TABLE IF EXISTS ${T_DOCS2}`);
      await myConn.query(`DROP TABLE IF EXISTS ${T_USERS2}`);
      await myConn.query(`DROP TABLE IF EXISTS ${T_REVS}`);
      await myConn.query(`DROP TABLE IF EXISTS ${T_POSTS}`);
      await myConn.query(`DROP TABLE IF EXISTS ${T_USERS}`);
    }
  } catch {
    /* best-effort cleanup */
  }
  if (pgPool) await pgPool.end();
  if (myConn) await myConn.end();
});

// ── Postgres ───────────────────────────────────────────────────────────────────

describe('WS6 integration — Postgres: SCP-compiled SQL executes + parity with v1 direct execution', () => {
  const contract: BehaviorModelContract = publishBehaviors(PostQueries);

  it('SELECT by user_id: SCP rows == v1 direct execution (`$N` placeholders on real PG)', async () => {
    const bundle = compileBundle(contract, 'ByUser', [], 'postgres');
    // Assert the emitted PG SQL (`$N`) via the shipped render axis, then execute via the shipped
    // async runtime (compileBundle + executeBundleAsync + pgPoolExecutor).
    const rendered = renderReadPrimary(bundle.readGraph!, { user_id: 1 } as never);
    expect(rendered.sql).toBe(`SELECT id, user_id, title, view_count FROM ${T_POSTS} WHERE user_id = $1 ORDER BY id ASC`);
    const scpRows = (await executeBundleAsync(bundle, { user_id: 1 } as never, {
      exec: pgPoolExecutor(pgPool!),
      entry: 'ByUser',
      dialect: 'postgres',
    })) as unknown as Row[];

    // v1 direct execution: build the equivalent WHERE via v1 DBConditions, convert `?`→`$N`.
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ user_id: 1 }).compile(v1Params);
    let i = 0;
    const v1Sql = `SELECT id, user_id, title, view_count FROM ${T_POSTS} WHERE ${v1Where} ORDER BY id ASC`.replace(/\?/g, () => `$${++i}`);
    const v1Rows = await pgQuery(pgPool!, v1Sql, v1Params);

    expect(scpRows).toEqual(v1Rows);
    expect(scpRows.length).toBeGreaterThan(0);
    for (const r of scpRows) expect(r.user_id).toBe(1);
  });

  it('SELECT IN-list on an INT column: no-cast `= ANY($1)` — #46; PG infers int[]; SCP rows == v1', async () => {
    const bundle = compileBundle(contract, 'ByIds', [], 'postgres');
    // #46: the authored IN-list emits `= ANY($1)` with NO element-type cast — PG infers `int[]` from
    // the `id` column. A value-inferred `::text[]` cast threw `operator does not exist: integer =
    // text` on real PG (the #43/#46 regression). No-cast is v1-RESULT-parity (same rows as v1's
    // `IN (1, 3)`), and is the ONE form that also survives the empty + uuid cases below.
    const rendered = renderReadPrimary(bundle.readGraph!, { ids: [1, 3] } as never);
    expect(rendered.sql).toBe(`SELECT id, title FROM ${T_POSTS} WHERE id = ANY($1) ORDER BY id ASC`);
    const scpRows = (await executeBundleAsync(bundle, { ids: [1, 3] } as never, {
      exec: pgPoolExecutor(pgPool!),
      entry: 'ByIds',
      dialect: 'postgres',
    })) as unknown as Row[];
    expect(scpRows.map((r) => Number(r.id))).toEqual([1, 3]);

    // v1 RESULT parity: v1's authored IN-list expanded to `id IN ($1, $2)` (DBConditions). Same rows.
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ id: [1, 3] }).compile(v1Params);
    let i = 0;
    const v1Sql = `SELECT id, title FROM ${T_POSTS} WHERE ${v1Where} ORDER BY id ASC`.replace(/\?/g, () => `$${++i}`);
    const v1Rows = await pgQuery(pgPool!, v1Sql, v1Params);
    expect(scpRows).toEqual(v1Rows);
  });

  it('SELECT IN-list EMPTY int array: `= ANY($1)` with [] → ZERO rows, no error — #46; == v1 `1 = 0`', async () => {
    const bundle = compileBundle(contract, 'ByIds', [], 'postgres');
    // The blocker: an empty int IN-list. `inferPgArrayType([])` = `text[]` → `integer = text` at PLAN
    // time. No-cast `= ANY($1)` with an empty array binds fine → PG infers int[] from the column and
    // selects zero rows. v1 short-circuited `[]` to `1 = 0` (DBConditions) → the SAME zero rows.
    const rendered = renderReadPrimary(bundle.readGraph!, { ids: [] } as never);
    expect(rendered.sql).toBe(`SELECT id, title FROM ${T_POSTS} WHERE id = ANY($1) ORDER BY id ASC`);
    const scpRows = (await executeBundleAsync(bundle, { ids: [] } as never, {
      exec: pgPoolExecutor(pgPool!),
      entry: 'ByIds',
      dialect: 'postgres',
    })) as unknown as Row[];
    expect(scpRows).toEqual([]);
  });

  it('SELECT IN-list on a UUID column (non-empty): `= ANY($1)` → PG infers uuid[]; correct rows — #46', async () => {
    const bundle = compileBundle(contract, 'ByGuids', [], 'postgres');
    // uuid values are indistinguishable-from-text by value, so a value-inferred cast is `text[]` →
    // `uuid = text` error. No-cast lets PG infer `uuid[]` from the column.
    const rendered = renderReadPrimary(bundle.readGraph!, { guids: [POST_GUIDS[0], POST_GUIDS[2]] } as never);
    expect(rendered.sql).toBe(`SELECT id, guid FROM ${T_POSTS} WHERE guid = ANY($1) ORDER BY id ASC`);
    const scpRows = (await executeBundleAsync(bundle, { guids: [POST_GUIDS[0], POST_GUIDS[2]] } as never, {
      exec: pgPoolExecutor(pgPool!),
      entry: 'ByGuids',
      dialect: 'postgres',
    })) as unknown as Row[];
    expect(scpRows.map((r) => Number(r.id))).toEqual([1, 3]);
    expect(scpRows.map((r) => String(r.guid))).toEqual([POST_GUIDS[0], POST_GUIDS[2]]);
  });

  it('SELECT IN-list EMPTY uuid array: `= ANY($1)` with [] → ZERO rows, no error — #46', async () => {
    const bundle = compileBundle(contract, 'ByGuids', [], 'postgres');
    const scpRows = (await executeBundleAsync(bundle, { guids: [] } as never, {
      exec: pgPoolExecutor(pgPool!),
      entry: 'ByGuids',
      dialect: 'postgres',
    })) as unknown as Row[];
    expect(scpRows).toEqual([]);
  });

  // #46 item 4 — every PG element type binds live through the no-cast `= ANY($1)` IN-list. Each
  // case selects the stable `label`, so `[A,C]` is the dialect-invariant expected result; the test
  // proves the ARRAY BINDING of bigint / text / bool / timestamp / numeric through `pg`.
  const pgTypeCases: { entry: string; keys: unknown[]; col: string }[] = [
    { entry: 'ByBig', keys: [TYPED_BIG_TS[0], TYPED_BIG_TS[2]], col: 'big' },
    { entry: 'ByTxt', keys: ['alpha', 'gamma'], col: 'txt' },
    { entry: 'ByFlag', keys: [true], col: 'flag' },
    { entry: 'ByTs', keys: [TYPED_TS_TS[0], TYPED_TS_TS[2]], col: 'ts' },
    { entry: 'ByAmt', keys: [10.5, 30.75], col: 'amt' },
  ];
  for (const c of pgTypeCases) {
    it(`SELECT IN-list on a ${c.col} column: no-cast \`= ANY($1)\` binds live — #46 item 4`, async () => {
      const bundle = compileBundle(contract, c.entry, [], 'postgres');
      const rendered = renderReadPrimary(bundle.readGraph!, { keys: c.keys } as never);
      expect(rendered.sql).toBe(`SELECT label FROM ${T_TYPED} WHERE ${c.col} = ANY($1) ORDER BY label ASC`);
      const scpRows = (await executeBundleAsync(bundle, { keys: c.keys } as never, {
        exec: pgPoolExecutor(pgPool!),
        entry: c.entry,
        dialect: 'postgres',
      })) as unknown as Row[];
      expect(scpRows.map((r) => String(r.label))).toEqual(['A', 'C']);
    });
  }

  // count() (#47 item 2) — `SELECT COUNT(*) as count FROM t[ WHERE …]` executes live + matches v1.
  it('COUNT(*) all rows: SCP `SELECT COUNT(*) as count` == v1 _count (real PG)', async () => {
    const bundle = compileBundle(contract, 'CountAll', [], 'postgres');
    const rendered = renderReadPrimary(bundle.readGraph!, {} as never);
    expect(rendered.sql).toBe(`SELECT COUNT(*) as count FROM ${T_POSTS}`);
    const scpRows = (await executeBundleAsync(bundle, {} as never, {
      exec: pgPoolExecutor(pgPool!),
      entry: 'CountAll',
      dialect: 'postgres',
    })) as unknown as Row[];
    // v1 _count returns parseInt(rows[0].count); the same one-row [{count}] shape is asserted.
    const v1Rows = await pgQuery(pgPool!, `SELECT COUNT(*) as count FROM ${T_POSTS}`, []);
    expect(Number(scpRows[0].count)).toBe(Number(v1Rows[0].count));
    expect(Number(scpRows[0].count)).toBe(3);
  });

  it('COUNT(*) WHERE user_id: SCP == v1 _count with condition (real PG)', async () => {
    const bundle = compileBundle(contract, 'CountByUser', [], 'postgres');
    const rendered = renderReadPrimary(bundle.readGraph!, { user_id: 1 } as never);
    expect(rendered.sql).toBe(`SELECT COUNT(*) as count FROM ${T_POSTS} WHERE user_id = $1`);
    const scpRows = (await executeBundleAsync(bundle, { user_id: 1 } as never, {
      exec: pgPoolExecutor(pgPool!),
      entry: 'CountByUser',
      dialect: 'postgres',
    })) as unknown as Row[];
    expect(Number(scpRows[0].count)).toBe(2);
    // Empty result → 0 (not null): a real DB always returns one COUNT row.
    const empty = (await executeBundleAsync(bundle, { user_id: 999 } as never, {
      exec: pgPoolExecutor(pgPool!),
      entry: 'CountByUser',
      dialect: 'postgres',
    })) as unknown as Row[];
    expect(Number(empty[0].count)).toBe(0);
  });

  // #47 item 5 — the OR-group WHERE + LIMIT/OFFSET tail (now v1-sourced) execute live on PG and
  // return the SAME rows as v1 direct execution of the equivalent DBConditions OR + inline LIMIT.
  it('OR-group WHERE + LIMIT/OFFSET: SCP rows == v1 direct execution (real PG)', async () => {
    const bundle = compileBundle(contract, 'Page', [], 'postgres');
    const input = { user_id: 1, other_id: 2, offset: 0 };
    const scpRows = (await executeBundleAsync(bundle, input as never, {
      exec: pgPoolExecutor(pgPool!),
      entry: 'Page',
      dialect: 'postgres',
    })) as unknown as Row[];
    // v1 direct: an OR over the two user_ids + inline LIMIT/OFFSET (v1 inlines the count as a literal).
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ __or__: [{ user_id: 1 }, { user_id: 2 }] }).compile(v1Params);
    let i = 0;
    const v1Sql = `SELECT id, user_id, title FROM ${T_POSTS} WHERE ${v1Where} ORDER BY id ASC LIMIT 2 OFFSET 0`.replace(/\?/g, () => `$${++i}`);
    const v1Rows = await pgQuery(pgPool!, v1Sql, v1Params);
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows.length).toBe(2); // LIMIT 2 caps the page (all 3 posts belong to user 1 or 2)
  });

  it('INSERT + RETURNING: SCP persists + returns; parity with v1 postgresSqlBuilder', async () => {
    const wc = publishBehaviors(PostWrites);
    const bundle = compileBundle(wc, 'Create', [], 'postgres');
    // A single-statement write bundle carries the compiled INSERT (canonical column order).
    const input = { user_id: 2, title: 'SCP PG Post', content: 'from scp' };
    const { sql, params } = renderTxStatement(bundle.statement!, input as never, 'postgres');
    // The SCP Insert compiles columns in the canonical (alphabetical) order (WS3 SSoT), so the
    // column list is `content, title, user_id` regardless of declaration order.
    expect(sql).toBe(`INSERT INTO ${T_POSTS} (content, title, user_id) VALUES ($1, $2, $3) RETURNING id, user_id, title, view_count`);

    const scpRows = await pgQuery(pgPool!, sql, params.map(toPlain));
    expect(scpRows.length).toBe(1);
    expect(scpRows[0]).toMatchObject({ user_id: 2, title: 'SCP PG Post', view_count: 0 });

    // v1 parity: the REAL v1 builder produces the equivalent INSERT (canonical column order,
    // matching DBModel._insert's Object.keys().sort()), converted `?`→`$N`.
    const v1 = postgresSqlBuilder.buildInsert({
      tableName: T_POSTS,
      columns: ['content', 'title', 'user_id'],
      records: [{ user_id: 2, title: 'v1 PG Post', content: 'from v1' }],
      returning: 'id, user_id, title, view_count',
    });
    let i = 0;
    const v1Sql = v1.sql.replace(/\?/g, () => `$${++i}`);
    const v1Rows = await pgQuery(pgPool!, v1Sql, v1.params as unknown[]);
    expect(v1Rows.length).toBe(1);
    // Same shape + same non-id column values (ids differ by sequence).
    expect(Object.keys(scpRows[0]).sort()).toEqual(Object.keys(v1Rows[0]).sort());
    expect(v1Rows[0]).toMatchObject({ user_id: 2, title: 'v1 PG Post', view_count: 0 });

    // Cleanup the two inserted rows.
    await pgPool!.query(`DELETE FROM ${T_POSTS} WHERE id = ANY($1::int[])`, [[scpRows[0].id, v1Rows[0].id]]);
  });

  it('read-relation batch (belongsTo author, INT key): `$1::int[]` (NOT text[]) — #46; SCP == v1', async () => {
    // scp_posts.user_id → scp_users.id (belongsTo). Compile the relation op, render its batch SELECT.
    const op: RelationOp = compileRelationOp({
      name: 'author',
      kind: 'belongsTo',
      targetTable: T_USERS,
      select: ['id', 'name'],
      parentKey: 'user_id',
      targetKey: 'id',
      dialect: 'postgres',
    });
    const parentRows = await pgQuery(pgPool!, `SELECT id, user_id FROM ${T_POSTS} ORDER BY id`, []);
    const keys = [...new Set(parentRows.map((r) => Number(r.user_id)))];
    // #46: the deferred cast resolves to `int[]` from the real int keys — v1's live-correct form.
    const scpSql = renderRelationSql(op, keys);
    expect(scpSql).toBe(`SELECT id, name FROM ${T_USERS} WHERE ${T_USERS}.id = ANY($1::int[])`);
    const scpChildren = await pgQuery(pgPool!, scpSql, [keys]);

    // v1 parity: the REAL v1 LazyRelation `= ANY(?::type[])` form over the same keys (`?`→`$N`).
    const v1Type = inferPgArrayType(keys);
    const v1Sql = `SELECT id, name FROM ${T_USERS} WHERE ${T_USERS}.id = ANY($1::${v1Type})`;
    const v1Children = await pgQuery(pgPool!, v1Sql, [keys]);
    expect(scpChildren).toEqual(v1Children);
    expect(scpChildren.length).toBe(keys.length);
  });

  // #47 item 1 — COMPOSITE-key relation batch binds + executes live on PG (unnest per-column arrays),
  // and the (tenant_id, …) tuple correctly disambiguates the two tenants sharing uid/doc_id.
  it('composite belongsTo (tenant_id, owner_id) → users2: `unnest(?::int[], ?::int[])` binds live on PG', async () => {
    const op: RelationOp = compileRelationOp({
      name: 'owner', kind: 'belongsTo', targetTable: T_USERS2, select: ['tenant_id', 'uid', 'name'],
      parentKeys: ['tenant_id', 'owner_id'], targetKeys: ['tenant_id', 'uid'], dialect: 'postgres',
    });
    const docs = await pgQuery(pgPool!, `SELECT tenant_id, doc_id, owner_id FROM ${T_DOCS2} ORDER BY tenant_id, doc_id`, []);
    const tuples = docs.map((d) => [Number(d.tenant_id), Number(d.owner_id)]);
    const { sql, params } = renderCompositeRelation(op, ['tenant_id', 'owner_id'], tuples);
    expect(sql).toContain('unnest($1::int[], $2::int[])');
    const children = await pgQuery(pgPool!, sql, params);
    // (2,100) must resolve to Bob (tenant 2), NOT Ada (tenant 1) — the composite key disambiguates.
    const bob = children.find((c) => Number(c.tenant_id) === 2 && Number(c.uid) === 100);
    expect(bob?.name).toBe('Bob');
    const ada = children.find((c) => Number(c.tenant_id) === 1 && Number(c.uid) === 100);
    expect(ada?.name).toBe('Ada');
  });

  it('composite hasMany (tenant_id, doc_id) → revs: per-tenant revisions bind live on PG', async () => {
    const op: RelationOp = compileRelationOp({
      name: 'revisions', kind: 'hasMany', targetTable: T_REVS, select: ['tenant_id', 'doc_id', 'rev'],
      parentKeys: ['tenant_id', 'doc_id'], targetKeys: ['tenant_id', 'doc_id'], order: 'rev ASC', dialect: 'postgres',
    });
    const tuples = [[1, 10], [2, 10]]; // same doc_id 10 across two tenants
    const { sql, params } = renderCompositeRelation(op, ['tenant_id', 'doc_id'], tuples);
    const rows = await pgQuery(pgPool!, sql, params);
    const t1 = rows.filter((r) => Number(r.tenant_id) === 1).map((r) => String(r.rev)).sort();
    const t2 = rows.filter((r) => Number(r.tenant_id) === 2).map((r) => String(r.rev)).sort();
    expect(t1).toEqual(['r1', 'r2']); // tenant 1 doc 10 → r1,r2 (NOT r9)
    expect(t2).toEqual(['r9']); // tenant 2 doc 10 → r9 only
  });

  it('composite hasMany + per-parent LIMIT (tenant_id, doc_id) → revs: STATIC LATERAL caps live on PG (#47 last gap)', async () => {
    const op: RelationOp = compileRelationOp({
      name: 'latestRev', kind: 'hasMany', targetTable: T_REVS, select: ['tenant_id', 'doc_id', 'rev'],
      parentKeys: ['tenant_id', 'doc_id'], targetKeys: ['tenant_id', 'doc_id'], order: 'rev DESC', limit: 1, dialect: 'postgres',
    });
    const tuples = [[1, 10], [1, 11], [2, 10]];
    const { sql, params } = renderCompositeRelation(op, ['tenant_id', 'doc_id'], tuples);
    // STATIC composite-LIMITED = v1 LATERAL over per-column unnest (length-independent, deferred cast).
    expect(sql).toContain('unnest($1::int[], $2::int[])');
    expect(sql).toContain('CROSS JOIN LATERAL');
    expect(sql).toContain('ORDER BY rev DESC LIMIT 1');
    const rows = await pgQuery(pgPool!, sql, params);
    // Each parent keeps exactly its highest rev: (1,10)→r2 [capped from r1,r2], (1,11)→r3, (2,10)→r9.
    const got = rows
      .map((r) => `${Number(r.tenant_id)}/${Number(r.doc_id)}=${String(r.rev)}`)
      .sort();
    expect(got).toEqual(['1/10=r2', '1/11=r3', '2/10=r9']);
  });

  it('write-tx Command (Insert + derive counter) commits atomically as ONE tx on real PG', async () => {
    const contract = publishBehaviors(CreatePostWithCount);
    const bundle = compileWriteBundle(contract, 'Create', postCountWrites, 'create', 'postgres');
    expect(bundle.dialect).toBe('postgres');
    const plan = bundle.transaction!;
    const input = { user_id: 1, title: 'TX PG', content: 'c' };

    const before = await pgQuery(pgPool!, `SELECT post_count_scp FROM ${T_USERS} WHERE id = $1`, [1]);
    const beforeCount = Number(before[0].post_count_scp);

    // Execute the derived plan's statements in ONE real transaction, rendering each with PG.
    const client = await pgPool!.connect();
    let entityId: number | null = null;
    try {
      await client.query('BEGIN');
      const scope: Record<string, unknown> = { ...input };
      for (const stmt of plan.statements) {
        const r = renderTxStatement(stmt.op, scope as never, 'postgres');
        const res = await client.query(r.sql, r.params.map(toPlain));
        const rows = res.rows as Row[];
        // Gate-first: a `requires` existence probe returning zero rows would ROLLBACK.
        if (stmt.gate === 'existsElseRollback' && rows.length === 0) {
          throw new Error('gate requires failed');
        }
        if (stmt.id === plan.entityFrom && rows.length > 0) {
          scope.__entity = rows[0];
          entityId = Number(rows[0].id);
        }
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    // The post was inserted and the counter derived (+1) in the SAME tx.
    const after = await pgQuery(pgPool!, `SELECT post_count_scp FROM ${T_USERS} WHERE id = $1`, [1]);
    expect(Number(after[0].post_count_scp)).toBe(beforeCount + 1);
    const post = await pgQuery(pgPool!, `SELECT id, user_id, title FROM ${T_POSTS} WHERE id = $1`, [entityId]);
    expect(post[0]).toMatchObject({ user_id: 1, title: 'TX PG' });

    // Cleanup.
    await pgPool!.query(`DELETE FROM ${T_POSTS} WHERE id = $1`, [entityId]);
    await pgPool!.query(`UPDATE ${T_USERS} SET post_count_scp = $1 WHERE id = $2`, [beforeCount, 1]);
  });
});

// ── MySQL ───────────────────────────────────────────────────────────────────────

describe('WS6 integration — MySQL: SCP-compiled SQL executes + parity with v1 direct execution', () => {
  const contract: BehaviorModelContract = publishBehaviors(PostQueries);

  it('SELECT by user_id: SCP rows == v1 direct execution (`?` placeholders on real MySQL)', async () => {
    const bundle = compileBundle(contract, 'ByUser', [], 'mysql');
    const rendered = renderReadPrimary(bundle.readGraph!, { user_id: 1 } as never);
    expect(rendered.sql).toBe(`SELECT id, user_id, title, view_count FROM ${T_POSTS} WHERE user_id = ? ORDER BY id ASC`);
    const scpRows = (await executeBundleAsync(bundle, { user_id: 1 } as never, {
      exec: mysqlPoolExecutor(myConn! as never),
      entry: 'ByUser',
      dialect: 'mysql',
    })) as unknown as Row[];

    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ user_id: 1 }).compile(v1Params);
    const v1Sql = `SELECT id, user_id, title, view_count FROM ${T_POSTS} WHERE ${v1Where} ORDER BY id ASC`;
    const v1Rows = await myQuery(myConn!, v1Sql, v1Params);
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows.length).toBeGreaterThan(0);
  });

  it('SELECT IN-list: single-JSON-param form (no cast token — MySQL); SCP rows == v1', async () => {
    const bundle = compileBundle(contract, 'ByIds', [], 'mysql');
    const rendered = renderReadPrimary(bundle.readGraph!, { ids: [1, 3] } as never);
    // MySQL uses the single-JSON-param IN-list (epic #43/#45), NOT `IN (?, ?)`; NO PG cast token.
    expect(rendered.sql).toContain('JSON_TABLE');
    expect(rendered.sql).not.toContain('PG_ARRAY_CAST');
    const scpRows = (await executeBundleAsync(bundle, { ids: [1, 3] } as never, {
      exec: mysqlPoolExecutor(myConn! as never),
      entry: 'ByIds',
      dialect: 'mysql',
    })) as unknown as Row[];
    expect(scpRows.map((r) => Number(r.id))).toEqual([1, 3]);
  });

  // #46 item 4 — every element type binds live through the single-JSON MySQL IN-list form. The
  // BOOLEAN element is encoded `1`/`0` in the JSON param (MySQL's JSON_UNQUOTE would stringify a
  // JSON `true` to `'true'` → coerce to 0 against TINYINT). Each selects the stable `label` → [A,C].
  const myTypeCases: { entry: string; keys: unknown[] }[] = [
    { entry: 'ByBig', keys: [TYPED_BIG_TS[0], TYPED_BIG_TS[2]] },
    { entry: 'ByTxt', keys: ['alpha', 'gamma'] },
    { entry: 'ByFlag', keys: [true] },
    { entry: 'ByTs', keys: [TYPED_TS_TS[0], TYPED_TS_TS[2]] },
    { entry: 'ByAmt', keys: [10.5, 30.75] },
  ];
  for (const c of myTypeCases) {
    it(`SELECT IN-list ${c.entry}: single-JSON form binds live on MySQL — #46 item 4`, async () => {
      const bundle = compileBundle(contract, c.entry, [], 'mysql');
      const scpRows = (await executeBundleAsync(bundle, { keys: c.keys } as never, {
        exec: mysqlPoolExecutor(myConn! as never),
        entry: c.entry,
        dialect: 'mysql',
      })) as unknown as Row[];
      expect(scpRows.map((r) => String(r.label))).toEqual(['A', 'C']);
    });
  }

  // count() (#47 item 2) — `SELECT COUNT(*) as count FROM t[ WHERE …]` executes live on MySQL.
  it('COUNT(*) all rows + WHERE: SCP `SELECT COUNT(*) as count` on real MySQL', async () => {
    const all = compileBundle(contract, 'CountAll', [], 'mysql');
    expect(renderReadPrimary(all.readGraph!, {} as never).sql).toBe(`SELECT COUNT(*) as count FROM ${T_POSTS}`);
    const allRows = (await executeBundleAsync(all, {} as never, {
      exec: mysqlPoolExecutor(myConn! as never),
      entry: 'CountAll',
      dialect: 'mysql',
    })) as unknown as Row[];
    expect(Number(allRows[0].count)).toBe(3);
    const byUser = compileBundle(contract, 'CountByUser', [], 'mysql');
    const byRows = (await executeBundleAsync(byUser, { user_id: 1 } as never, {
      exec: mysqlPoolExecutor(myConn! as never),
      entry: 'CountByUser',
      dialect: 'mysql',
    })) as unknown as Row[];
    expect(Number(byRows[0].count)).toBe(2);
  });

  // #47 item 5 — OR-group WHERE + LIMIT/OFFSET tail (v1-sourced) execute live on MySQL, rows == v1.
  it('OR-group WHERE + LIMIT/OFFSET: SCP rows == v1 direct execution (real MySQL)', async () => {
    const bundle = compileBundle(contract, 'Page', [], 'mysql');
    const input = { user_id: 1, other_id: 2, offset: 0 };
    const scpRows = (await executeBundleAsync(bundle, input as never, {
      exec: mysqlPoolExecutor(myConn! as never),
      entry: 'Page',
      dialect: 'mysql',
    })) as unknown as Row[];
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ __or__: [{ user_id: 1 }, { user_id: 2 }] }).compile(v1Params);
    const v1Sql = `SELECT id, user_id, title FROM ${T_POSTS} WHERE ${v1Where} ORDER BY id ASC LIMIT 2 OFFSET 0`;
    const v1Rows = await myQuery(myConn!, v1Sql, v1Params);
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows.length).toBe(2);
  });

  it('INSERT: SCP persists (MySQL keeps `?`, RETURNING stripped by re-select) + parity with v1', async () => {
    const wc = publishBehaviors(PostWrites);
    const bundle = compileBundle(wc, 'Create', [], 'mysql');
    const input = { user_id: 2, title: 'SCP MY Post', content: 'from scp' };
    const { sql, params } = renderTxStatement(bundle.statement!, input as never, 'mysql');
    // MySQL has no native RETURNING; the compiled text carries it (driver simulates via re-select).
    // For the raw mysql2 seam we execute the INSERT sans RETURNING, then re-select — the v1
    // MysqlSqlBuilder + mysql.ts do the same (RETURNING stripped, re-select the inserted PK).
    const insertSql = sql.replace(/\s+RETURNING\s+.+$/i, '');
    // Canonical (alphabetical) column order (WS3 SSoT): `content, title, user_id`.
    expect(insertSql).toBe(`INSERT INTO ${T_POSTS} (content, title, user_id) VALUES (?, ?, ?)`);
    const res = await myConn!.query(insertSql, params.map(toPlain));
    const scpId = (res[0] as mysql.ResultSetHeader).insertId;
    const scpRows = await myQuery(myConn!, `SELECT id, user_id, title, view_count FROM ${T_POSTS} WHERE id = ?`, [scpId]);
    expect(scpRows[0]).toMatchObject({ user_id: 2, title: 'SCP MY Post', view_count: 0 });

    // v1 parity: the REAL v1 builder produces the equivalent INSERT (canonical column order).
    const v1 = mysqlSqlBuilder.buildInsert({
      tableName: T_POSTS,
      columns: ['content', 'title', 'user_id'],
      records: [{ user_id: 2, title: 'v1 MY Post', content: 'from v1' }],
    });
    const v1res = await myConn!.query(v1.sql, v1.params as unknown[]);
    const v1Id = (v1res[0] as mysql.ResultSetHeader).insertId;
    const v1Rows = await myQuery(myConn!, `SELECT id, user_id, title, view_count FROM ${T_POSTS} WHERE id = ?`, [v1Id]);
    expect(Object.keys(scpRows[0]).sort()).toEqual(Object.keys(v1Rows[0]).sort());
    expect(v1Rows[0]).toMatchObject({ user_id: 2, title: 'v1 MY Post', view_count: 0 });

    await myConn!.query(`DELETE FROM ${T_POSTS} WHERE id IN (?, ?)`, [scpId, v1Id]);
  });

  it('read-relation batch (belongsTo author): SCP single-JSON-param batch == v1 on real MySQL', async () => {
    const op: RelationOp = compileRelationOp({
      name: 'author',
      kind: 'belongsTo',
      targetTable: T_USERS,
      select: ['id', 'name'],
      parentKey: 'user_id',
      targetKey: 'id',
      dialect: 'mysql',
    });
    const parentRows = await myQuery(myConn!, `SELECT id, user_id FROM ${T_POSTS} ORDER BY id`, []);
    const keys = [...new Set(parentRows.map((r) => Number(r.user_id)))];
    // MySQL binds the deduped key set as ONE JSON param (server-side expansion); NO PG cast token.
    const scpSql = renderRelationSql(op, keys);
    expect(scpSql).not.toContain('PG_ARRAY_CAST');
    const scpChildren = await myQuery(myConn!, scpSql, [JSON.stringify(keys)]);

    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ id: keys }).compile(v1Params);
    const v1Sql = `SELECT id, name FROM ${T_USERS} WHERE ${v1Where}`;
    const v1Children = await myQuery(myConn!, v1Sql, v1Params);
    // Same rows (order-independent — the JSON form does not impose the IN-list order).
    expect([...scpChildren].sort((a, b) => Number(a.id) - Number(b.id))).toEqual(v1Children);
    expect(scpChildren.length).toBe(keys.length);
  });

  // #47 item 1 — COMPOSITE-key relation batch binds + executes live on MySQL (single-JSON tuple
  // param, `(k1,k2) IN (SELECT … JSON_TABLE …)`), disambiguating tenants sharing uid/doc_id.
  it('composite belongsTo (tenant_id, owner_id): single-JSON tuple form binds live on MySQL', async () => {
    const op: RelationOp = compileRelationOp({
      name: 'owner', kind: 'belongsTo', targetTable: T_USERS2, select: ['tenant_id', 'uid', 'name'],
      parentKeys: ['tenant_id', 'owner_id'], targetKeys: ['tenant_id', 'uid'], dialect: 'mysql',
    });
    expect(op.sql).toContain('JSON_TABLE');
    const tuples = [[1, 100], [2, 100]]; // same uid 100 across two tenants
    const { sql, params } = renderCompositeRelation(op, ['tenant_id', 'owner_id'], tuples);
    const children = await myQuery(myConn!, sql, params);
    const bob = children.find((c) => Number(c.tenant_id) === 2 && Number(c.uid) === 100);
    const ada = children.find((c) => Number(c.tenant_id) === 1 && Number(c.uid) === 100);
    expect(bob?.name).toBe('Bob');
    expect(ada?.name).toBe('Ada');
    expect(children.length).toBe(2); // exactly the two composite matches, no cross-tenant bleed
  });

  it('composite hasMany (tenant_id, doc_id) → revs: per-tenant revisions bind live on MySQL', async () => {
    const op: RelationOp = compileRelationOp({
      name: 'revisions', kind: 'hasMany', targetTable: T_REVS, select: ['tenant_id', 'doc_id', 'rev'],
      parentKeys: ['tenant_id', 'doc_id'], targetKeys: ['tenant_id', 'doc_id'], order: 'rev ASC', dialect: 'mysql',
    });
    const { sql, params } = renderCompositeRelation(op, ['tenant_id', 'doc_id'], [[1, 10], [2, 10]]);
    const rows = await myQuery(myConn!, sql, params);
    const t1 = rows.filter((r) => Number(r.tenant_id) === 1).map((r) => String(r.rev)).sort();
    const t2 = rows.filter((r) => Number(r.tenant_id) === 2).map((r) => String(r.rev)).sort();
    expect(t1).toEqual(['r1', 'r2']);
    expect(t2).toEqual(['r9']);
  });

  it('composite hasMany + per-parent LIMIT (tenant_id, doc_id) → revs: STATIC ROW_NUMBER caps live on MySQL (#47 last gap)', async () => {
    const op: RelationOp = compileRelationOp({
      name: 'latestRev', kind: 'hasMany', targetTable: T_REVS, select: ['tenant_id', 'doc_id', 'rev'],
      parentKeys: ['tenant_id', 'doc_id'], targetKeys: ['tenant_id', 'doc_id'], order: 'rev DESC', limit: 1, dialect: 'mysql',
    });
    const { sql, params } = renderCompositeRelation(op, ['tenant_id', 'doc_id'], [[1, 10], [1, 11], [2, 10]]);
    // STATIC composite-LIMITED = v1 ROW_NUMBER window + static JSON key-set predicate (no tuple-IN).
    expect(sql).toContain('ROW_NUMBER() OVER (PARTITION BY tenant_id, doc_id ORDER BY rev DESC)');
    expect(sql).toContain('JSON_TABLE');
    expect(sql).not.toContain('IN ((?, ?)');
    const rows = await myQuery(myConn!, sql, params);
    const got = rows
      .map((r) => `${Number(r.tenant_id)}/${Number(r.doc_id)}=${String(r.rev)}`)
      .sort();
    // Each parent keeps exactly its highest rev: (1,10)→r2 [capped], (1,11)→r3, (2,10)→r9.
    expect(got).toEqual(['1/10=r2', '1/11=r3', '2/10=r9']);
  });

  it('write-tx Command (Insert + derive counter) commits atomically as ONE tx on real MySQL', async () => {
    const contract = publishBehaviors(CreatePostWithCount);
    const bundle = compileWriteBundle(contract, 'Create', postCountWrites, 'create', 'mysql');
    expect(bundle.dialect).toBe('mysql');
    const plan = bundle.transaction!;
    const input = { user_id: 1, title: 'TX MY', content: 'c' };

    const before = await myQuery(myConn!, `SELECT post_count_scp FROM ${T_USERS} WHERE id = ?`, [1]);
    const beforeCount = Number(before[0].post_count_scp);

    await myConn!.beginTransaction();
    let entityId: number | null = null;
    try {
      const scope: Record<string, unknown> = { ...input };
      for (const stmt of plan.statements) {
        const r = renderTxStatement(stmt.op, scope as never, 'mysql');
        // MySQL has no RETURNING: for the body Insert, strip RETURNING, run, then re-select the PK.
        const isBody = stmt.id === plan.entityFrom;
        if (isBody && /\bRETURNING\b/i.test(r.sql)) {
          const insertSql = r.sql.replace(/\s+RETURNING\s+.+$/i, '');
          const res = await myConn!.query(insertSql, r.params.map(toPlain));
          entityId = (res[0] as mysql.ResultSetHeader).insertId;
          const sel = await myQuery(myConn!, `SELECT id, user_id, title FROM ${T_POSTS} WHERE id = ?`, [entityId]);
          scope.__entity = sel[0];
        } else {
          const res = await myConn!.query(r.sql, r.params.map(toPlain));
          const rows = Array.isArray(res[0]) ? (res[0] as Row[]) : [];
          if (stmt.gate === 'existsElseRollback' && rows.length === 0) throw new Error('gate requires failed');
        }
      }
      await myConn!.commit();
    } catch (e) {
      await myConn!.rollback();
      throw e;
    }

    const after = await myQuery(myConn!, `SELECT post_count_scp FROM ${T_USERS} WHERE id = ?`, [1]);
    expect(Number(after[0].post_count_scp)).toBe(beforeCount + 1);
    const post = await myQuery(myConn!, `SELECT id, user_id, title FROM ${T_POSTS} WHERE id = ?`, [entityId]);
    expect(post[0]).toMatchObject({ user_id: 1, title: 'TX MY' });

    await myConn!.query(`DELETE FROM ${T_POSTS} WHERE id = ?`, [entityId]);
    await myConn!.query(`UPDATE ${T_USERS} SET post_count_scp = ? WHERE id = ?`, [beforeCount, 1]);
  });
});

// ── Write-path completeness (#47 write side): the TS 5th-language live leg ─────
//
// createMany / updateMany / deleteMany / bare UPDATE·DELETE / UUID-PK + composite-PK INSERT
// RETURNING — the SAME batch/write bundles the corpus ships, executed live on PG + MySQL through the
// TS runtime's tx path (`renderTxStatement`). MySQL has no native RETURNING, so an INSERT…RETURNING
// is emulated PK-aware (strip → INSERT → re-select by the REAL PK via the `/*scp:pk=…*/` hint), and a
// non-INSERT RETURNING returns [] — the SAME contract the four non-TS runtimes implement. Dedicated
// `wc_*` tables keep this isolated from the seed above.

const WC_POSTS = 'scp_wc_posts';
const WC_DOCS = 'scp_wc_docs';
const WC_LINES = 'scp_wc_lines';
const WC_UUID = '44444444-4444-4444-4444-444444444444';

class WcMutations extends SemanticBehavior {
  Rename($: In<{ id: number; title: string }>) {
    return L.Update({ table: WC_POSTS, 'set.title': $.title, where: [whereEq($.id, $.id)] });
  }
  Remove($: In<{ id: number }>) {
    return L.Delete({ table: WC_POSTS, where: [whereEq($.id, $.id)] });
  }
  CreateDoc($: In<{ doc_id: string; title: string }>) {
    return L.Insert({ table: WC_DOCS, 'values.doc_id': $.doc_id, 'values.title': $.title, returning: 'doc_id, title', pk: 'doc_id' });
  }
  CreateLine($: In<{ order_id: number; line_no: number; sku: string }>) {
    return L.Insert({
      table: WC_LINES,
      'values.order_id': $.order_id,
      'values.line_no': $.line_no,
      'values.sku': $.sku,
      returning: 'order_id, line_no, sku',
      pk: 'order_id,line_no',
    });
  }
}

/** A minimal per-dialect live client seam (parameterized query → rows / affected count). */
interface LiveClient {
  query(sql: string, params: unknown[]): Promise<{ rows: Row[]; affected: number; insertId: number }>;
  begin(): Promise<void>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
}

function pgClient(pool: Pool): LiveClient {
  return {
    async query(sql, params) {
      const res = await pool.query(sql, params);
      return { rows: (res.rows ?? []) as Row[], affected: res.rowCount ?? 0, insertId: 0 };
    },
    begin: async () => void (await pool.query('BEGIN')),
    commit: async () => void (await pool.query('COMMIT')),
    rollback: async () => void (await pool.query('ROLLBACK')),
  };
}

function myClient(conn: mysql.Connection): LiveClient {
  return {
    async query(sql, params) {
      const [r] = await conn.query(sql, params);
      if (Array.isArray(r)) return { rows: r as Row[], affected: (r as Row[]).length, insertId: 0 };
      const h = r as mysql.ResultSetHeader;
      return { rows: [], affected: h.affectedRows ?? 0, insertId: h.insertId ?? 0 };
    },
    begin: async () => void (await conn.beginTransaction()),
    commit: async () => void (await conn.commit()),
    rollback: async () => void (await conn.rollback()),
  };
}

/** Parse the ` /*scp:pk=cols;ai=col*​/` hint (mirrors the 4 runtime emulations). */
function parsePkHint(sql: string): { cols: string[]; autoInc: string } | null {
  const m = /\/\*scp:pk=([^;*]*);ai=([^*]*)\*\//i.exec(sql);
  if (!m) return null;
  return { cols: m[1].split(',').map((c) => c.trim()).filter(Boolean), autoInc: m[2].trim() };
}

/**
 * Execute a tx/batch bundle live through the TS runtime's render + a per-dialect client, with the
 * PK-aware MySQL RETURNING emulation. Returns the collected body RETURNING rows (batch "all created
 * rows" / single entity). This is the TS twin of the 4 runtimes' execute_transaction_bundle.
 */
async function execTxLive(bundle: SqlBundle, input: Record<string, unknown>, client: LiveClient, isMysql: boolean): Promise<Row[][]> {
  const plan = bundle.transaction!;
  const scope: Record<string, unknown> = { ...input };
  const returned: Row[][] = [];
  await client.begin();
  try {
    for (const stmt of plan.statements) {
      const r = renderTxStatement(stmt.op, scope as never, bundle.dialect);
      const params = r.params.map(toPlain);
      const hasReturn = /^\s*select\b/i.test(r.sql) || /\breturning\b/i.test(r.sql);
      let rows: Row[] = [];
      if (isMysql && /\breturning\b/i.test(r.sql)) {
        // MySQL RETURNING emulation (PK-aware): strip RETURNING (+ hint), run, re-select by real PK.
        const retMatch = /\s+RETURNING\s+(.+?)\s*$/is.exec(r.sql)!;
        const cols = stripMysqlPkHint(retMatch[1]).trim();
        const writeSql = stripMysqlPkHint(r.sql.slice(0, retMatch.index));
        const pk = parsePkHint(r.sql);
        const isInsert = /^\s*INSERT\b/i.test(writeSql);
        const res = await client.query(writeSql, params);
        if (!isInsert) {
          rows = []; // non-INSERT RETURNING: no rows (v1 parity)
        } else if (pk === null) {
          rows = (await client.query(`SELECT ${cols} FROM ${insertTable(writeSql)} WHERE id = ?`, [res.insertId])).rows;
        } else if (pk.autoInc && pk.cols.length === 1 && pk.cols[0] === pk.autoInc) {
          rows = (await client.query(`SELECT ${cols} FROM ${insertTable(writeSql)} WHERE ${pk.autoInc} >= ? AND ${pk.autoInc} < ?`, [res.insertId, res.insertId + Math.max(1, res.affected)])).rows;
        } else {
          const insCols = insertCols(writeSql);
          const where = pk.cols.map((c) => `${c} = ?`).join(' AND ');
          const vals = pk.cols.map((c) => params[insCols.indexOf(c)]);
          rows = (await client.query(`SELECT ${cols} FROM ${insertTable(writeSql)} WHERE ${where}`, vals)).rows;
        }
      } else {
        const res = await client.query(r.sql, params);
        rows = hasReturn ? res.rows : [];
      }
      if (stmt.role === 'body' && rows.length > 0) returned.push(rows);
      if (stmt.id === plan.entityFrom && rows.length > 0) scope.__entity = rows[0];
      if (stmt.binds !== undefined && rows.length > 0) scope[stmt.binds] = rows[0];
    }
    await client.commit();
  } catch (e) {
    await client.rollback();
    throw e;
  }
  return returned;
}

function insertTable(sql: string): string {
  return /INSERT\s+(?:IGNORE\s+)?INTO\s+([A-Za-z_][A-Za-z0-9_]*)/i.exec(sql)![1];
}
function insertCols(sql: string): string[] {
  const m = /INSERT\s+(?:IGNORE\s+)?INTO\s+[A-Za-z_][A-Za-z0-9_]*\s*\(([^)]*)\)/i.exec(sql);
  return m ? m[1].split(',').map((c) => c.trim()) : [];
}

describe('WS#47 write-path completeness — batch + bare + PK RETURNING execute live (TS leg, PG + MySQL)', () => {
  const mut = () => publishBehaviors(WcMutations);

  async function setupPg(): Promise<void> {
    await pgPool!.query(`DROP TABLE IF EXISTS ${WC_POSTS}`);
    await pgPool!.query(`DROP TABLE IF EXISTS ${WC_DOCS}`);
    await pgPool!.query(`DROP TABLE IF EXISTS ${WC_LINES}`);
    await pgPool!.query(`CREATE TABLE ${WC_POSTS} (id SERIAL PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, subtitle TEXT)`);
    await pgPool!.query(`CREATE TABLE ${WC_DOCS} (doc_id UUID PRIMARY KEY, title TEXT NOT NULL)`);
    await pgPool!.query(`CREATE TABLE ${WC_LINES} (order_id INTEGER NOT NULL, line_no INTEGER NOT NULL, sku TEXT NOT NULL, PRIMARY KEY (order_id, line_no))`);
  }
  async function setupMy(): Promise<void> {
    await myConn!.query(`DROP TABLE IF EXISTS ${WC_POSTS}`);
    await myConn!.query(`DROP TABLE IF EXISTS ${WC_DOCS}`);
    await myConn!.query(`DROP TABLE IF EXISTS ${WC_LINES}`);
    await myConn!.query(`CREATE TABLE ${WC_POSTS} (id INT AUTO_INCREMENT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, subtitle VARCHAR(255))`);
    await myConn!.query(`CREATE TABLE ${WC_DOCS} (doc_id CHAR(36) PRIMARY KEY, title VARCHAR(255) NOT NULL)`);
    await myConn!.query(`CREATE TABLE ${WC_LINES} (order_id INT NOT NULL, line_no INT NOT NULL, sku VARCHAR(255) NOT NULL, PRIMARY KEY (order_id, line_no))`);
  }

  it('createMany (homogeneous, RETURNING) + updateMany + deleteMany execute on PG and MySQL', async () => {
    const records = [
      { author_id: 7, title: 'B1' },
      { author_id: 7, title: 'B2' },
      { author_id: 8, title: 'B3' },
    ];
    const cmOpts = { tableName: WC_POSTS, records, rawRecords: records, returning: 'id, author_id, title', pk: { columns: ['id'], autoInc: 'id' } };

    // ── PG ──
    await setupPg();
    const cmPg = await execTxLive(compileCreateManyBundle('CM', cmOpts, 'postgres'), {}, pgClient(pgPool!), false);
    expect(cmPg.flat().map((r) => r.title).sort()).toEqual(['B1', 'B2', 'B3']);
    const umPg = compileUpdateManyBundle('UM', { tableName: WC_POSTS, keyColumns: ['id'], updateColumns: ['title'], records: [{ id: 1, title: 'B1x' }, { id: 3, title: 'B3x' }], rawRecords: [{ id: 1, title: 'B1x' }, { id: 3, title: 'B3x' }] }, 'postgres');
    await execTxLive(umPg, {}, pgClient(pgPool!), false);
    const afterUmPg = await pgQuery(pgPool!, `SELECT id, title FROM ${WC_POSTS} ORDER BY id`, []);
    expect(afterUmPg.map((r) => r.title)).toEqual(['B1x', 'B2', 'B3x']);
    await execTxLive(compileDeleteManyBundle('DM', { tableName: WC_POSTS, keyColumns: ['id'], keys: [{ id: 1 }, { id: 3 }] }, 'postgres'), {}, pgClient(pgPool!), false);
    const afterDmPg = await pgQuery(pgPool!, `SELECT id FROM ${WC_POSTS} ORDER BY id`, []);
    expect(afterDmPg.map((r) => Number(r.id))).toEqual([2]);

    // ── MySQL ──
    await setupMy();
    const cmMy = await execTxLive(compileCreateManyBundle('CM', cmOpts, 'mysql'), {}, myClient(myConn!), true);
    expect(cmMy.flat().map((r) => r.title).sort()).toEqual(['B1', 'B2', 'B3']); // multi-row RETURNING range re-select
    const umMy = compileUpdateManyBundle('UM', { tableName: WC_POSTS, keyColumns: ['id'], updateColumns: ['title'], records: [{ id: 1, title: 'B1x' }, { id: 3, title: 'B3x' }] }, 'mysql');
    await execTxLive(umMy, {}, myClient(myConn!), true);
    const afterUmMy = await myQuery(myConn!, `SELECT id, title FROM ${WC_POSTS} ORDER BY id`, []);
    expect(afterUmMy.map((r) => r.title)).toEqual(['B1x', 'B2', 'B3x']);
    await execTxLive(compileDeleteManyBundle('DM', { tableName: WC_POSTS, keyColumns: ['id'], keys: [{ id: 1 }, { id: 3 }] }, 'mysql'), {}, myClient(myConn!), true);
    const afterDmMy = await myQuery(myConn!, `SELECT id FROM ${WC_POSTS} ORDER BY id`, []);
    expect(afterDmMy.map((r) => Number(r.id))).toEqual([2]);
  });

  it('bare UPDATE + bare DELETE execute on PG and MySQL', async () => {
    const contract = mut();
    const upd = { update: { effects: {} } };
    const rem = { remove: { effects: {} } };

    await setupPg();
    await pgPool!.query(`INSERT INTO ${WC_POSTS} (id, author_id, title) VALUES (1,7,'One'),(2,7,'Two')`);
    await execTxLive(compileWriteBundle(contract, 'Rename', upd, 'update', 'postgres'), { id: 2, title: 'Two-x' }, pgClient(pgPool!), false);
    expect((await pgQuery(pgPool!, `SELECT title FROM ${WC_POSTS} WHERE id=2`, []))[0].title).toBe('Two-x');
    await execTxLive(compileWriteBundle(contract, 'Remove', rem, 'remove', 'postgres'), { id: 1 }, pgClient(pgPool!), false);
    expect((await pgQuery(pgPool!, `SELECT id FROM ${WC_POSTS} ORDER BY id`, [])).map((r) => Number(r.id))).toEqual([2]);

    await setupMy();
    await myConn!.query(`INSERT INTO ${WC_POSTS} (id, author_id, title) VALUES (1,7,'One'),(2,7,'Two')`);
    await execTxLive(compileWriteBundle(contract, 'Rename', upd, 'update', 'mysql'), { id: 2, title: 'Two-x' }, myClient(myConn!), true);
    expect((await myQuery(myConn!, `SELECT title FROM ${WC_POSTS} WHERE id=2`, []))[0].title).toBe('Two-x');
    await execTxLive(compileWriteBundle(contract, 'Remove', rem, 'remove', 'mysql'), { id: 1 }, myClient(myConn!), true);
    expect((await myQuery(myConn!, `SELECT id FROM ${WC_POSTS} ORDER BY id`, [])).map((r) => Number(r.id))).toEqual([2]);
  });

  it('UUID-PK + composite-PK INSERT RETURNING: MySQL emul re-selects by the REAL PK (#4)', async () => {
    const contract = mut();
    const cr = { create: { effects: {} } };

    // ── UUID PK ──
    await setupPg();
    const docPg = await execTxLive(compileWriteBundle(contract, 'CreateDoc', cr, 'create', 'postgres'), { doc_id: WC_UUID, title: 'Doc' }, pgClient(pgPool!), false);
    expect(String(docPg[0][0].doc_id)).toBe(WC_UUID);
    await setupMy();
    const docMy = await execTxLive(compileWriteBundle(contract, 'CreateDoc', cr, 'create', 'mysql'), { doc_id: WC_UUID, title: 'Doc' }, myClient(myConn!), true);
    expect(String(docMy[0][0].doc_id)).toBe(WC_UUID); // re-selected by doc_id, NOT id
    expect(docMy[0][0].title).toBe('Doc');

    // ── Composite PK ──
    await setupPg();
    const linePg = await execTxLive(compileWriteBundle(contract, 'CreateLine', cr, 'create', 'postgres'), { order_id: 10, line_no: 2, sku: 'SKU-2' }, pgClient(pgPool!), false);
    expect(linePg[0][0]).toMatchObject({ order_id: 10, line_no: 2, sku: 'SKU-2' });
    await setupMy();
    const lineMy = await execTxLive(compileWriteBundle(contract, 'CreateLine', cr, 'create', 'mysql'), { order_id: 10, line_no: 2, sku: 'SKU-2' }, myClient(myConn!), true);
    expect(lineMy[0][0]).toMatchObject({ order_id: 10, line_no: 2, sku: 'SKU-2' }); // re-selected by (order_id, line_no)

    // cleanup
    await pgPool!.query(`DROP TABLE IF EXISTS ${WC_POSTS}`);
    await pgPool!.query(`DROP TABLE IF EXISTS ${WC_DOCS}`);
    await pgPool!.query(`DROP TABLE IF EXISTS ${WC_LINES}`);
    await myConn!.query(`DROP TABLE IF EXISTS ${WC_POSTS}`);
    await myConn!.query(`DROP TABLE IF EXISTS ${WC_DOCS}`);
    await myConn!.query(`DROP TABLE IF EXISTS ${WC_LINES}`);
  });
});
