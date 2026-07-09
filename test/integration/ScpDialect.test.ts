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
 * The SCP path renders through the SAME normative `renderOperation` the golden pins (with the
 * per-dialect strategy), and executes via the real `pg` Pool / `mysql2` connection — no mock, no
 * hand-written SQL for the SCP side. The v1 side calls the REAL v1 SqlBuilders / DBConditions
 * (not hand-written expectations — avoids the WS3 faked-parity pattern).
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
  compileRelationOp,
  renderOperation,
  whereEq,
  whereIn,
  inColumn,
  entityWrites,
  dialectFor,
  RELATION_KEYS_HEAD,
  type In,
  type BehaviorModelContract,
  type SqlBundle,
  type Dialect,
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

/** Render a bundle's single Select/write op for a bound input scope + dialect → {sql, params}. */
function renderBundleOp(bundle: SqlBundle, input: Record<string, unknown>, dialect: Dialect): { sql: string; params: unknown[] } {
  const opIds = Object.keys(bundle.operations);
  expect(opIds.length).toBe(1);
  const op = bundle.operations[opIds[0]];
  const r = renderOperation(op, input as never, dialect);
  return { sql: r.sql, params: r.params.map(toPlain) };
}

/** bc evaluates ints to bigint; convert to a driver-bindable JS value (numbers for i32 range). */
function toPlain(v: unknown): unknown {
  if (typeof v === 'bigint') return Number(v);
  return v;
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
        view_count INTEGER NOT NULL DEFAULT 0
      )`);
    // Deterministic seed: users id 1,2; posts id 1,2 (user 1) and id 3 (user 2)
    // — matches the parity fixtures (user_id=1 present, id IN (1,3) present).
    await pgPool.query(`INSERT INTO ${T_USERS} (id, name) VALUES (1, 'Alice'), (2, 'Bob')`);
    await pgPool.query(`SELECT setval('${T_USERS}_id_seq', 2)`);
    await pgPool.query(`INSERT INTO ${T_POSTS} (id, user_id, title, content, view_count) VALUES
      (1, 1, 'First Post', 'Hello World!', 100),
      (2, 1, 'Second Post', 'Another post', 0),
      (3, 2, 'Bob''s Post', 'Content here', 50)`);
    await pgPool.query(`SELECT setval('${T_POSTS}_id_seq', 3)`);
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
  } catch (e) {
    throw new Error(`MySQL is required for WS6 integration but is not reachable at ${MY.host}:${MY.port} — ${(e as Error).message}`);
  }
});

afterAll(async () => {
  // Tear down our isolated tables so no residue leaks to other files/runs.
  try {
    if (pgPool) {
      await pgPool.query(`DROP TABLE IF EXISTS ${T_POSTS} CASCADE`);
      await pgPool.query(`DROP TABLE IF EXISTS ${T_USERS} CASCADE`);
    }
  } catch {
    /* best-effort cleanup */
  }
  try {
    if (myConn) {
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
  const dialect = dialectFor('postgres');

  it('SELECT by user_id: SCP rows == v1 direct execution (`$N` placeholders on real PG)', async () => {
    const bundle = compileBundle(contract, 'ByUser', [], 'postgres');
    const { sql, params } = renderBundleOp(bundle, { user_id: 1 }, dialect);
    // The compiled PG SQL uses `$N` placeholders.
    expect(sql).toBe(`SELECT id, user_id, title, view_count FROM ${T_POSTS} WHERE user_id = $1 ORDER BY id ASC`);
    const scpRows = await pgQuery(pgPool!, sql, params);

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

  it('SELECT IN-list: `$N` numbered after IN expansion; SCP rows == v1', async () => {
    const bundle = compileBundle(contract, 'ByIds', [], 'postgres');
    const { sql, params } = renderBundleOp(bundle, { ids: [1, 3] }, dialect);
    expect(sql).toBe(`SELECT id, title FROM ${T_POSTS} WHERE id IN ($1, $2) ORDER BY id ASC`);
    const scpRows = await pgQuery(pgPool!, sql, params);

    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ id: [1, 3] }).compile(v1Params);
    let i = 0;
    const v1Sql = `SELECT id, title FROM ${T_POSTS} WHERE ${v1Where} ORDER BY id ASC`.replace(/\?/g, () => `$${++i}`);
    const v1Rows = await pgQuery(pgPool!, v1Sql, v1Params);
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows.map((r) => r.id)).toEqual([1, 3]);
  });

  it('INSERT + RETURNING: SCP persists + returns; parity with v1 postgresSqlBuilder', async () => {
    const wc = publishBehaviors(PostWrites);
    const bundle = compileBundle(wc, 'Create', [], 'postgres');
    const input = { user_id: 2, title: 'SCP PG Post', content: 'from scp' };
    const { sql, params } = renderBundleOp(bundle, input, dialect);
    // The SCP Insert compiles columns in the canonical (alphabetical) order (WS3 SSoT), so the
    // column list is `content, title, user_id` regardless of declaration order.
    expect(sql).toBe(`INSERT INTO ${T_POSTS} (content, title, user_id) VALUES ($1, $2, $3) RETURNING id, user_id, title, view_count`);

    const scpRows = await pgQuery(pgPool!, sql, params);
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

  it('read-relation batch (belongsTo author): SCP `$N` IN-batch == v1 IN-list on real PG', async () => {
    // scp_posts.user_id → scp_users.id (belongsTo). Compile the relation op, render its batch SELECT.
    const op: RelationOp = compileRelationOp({
      name: 'author',
      kind: 'belongsTo',
      targetTable: T_USERS,
      select: ['id', 'name'],
      parentKey: 'user_id',
      targetKey: 'id',
    });
    const parentRows = await pgQuery(pgPool!, `SELECT id, user_id FROM ${T_POSTS} ORDER BY id`, []);
    const keys = [...new Set(parentRows.map((r) => Number(r.user_id)))];
    const rendered = renderOperation(op.query, { [RELATION_KEYS_HEAD]: keys }, dialect);
    const scpSql = rendered.sql;
    const scpParams = rendered.params.map(toPlain);
    expect(scpSql).toMatch(new RegExp(`SELECT id, name FROM ${T_USERS} WHERE id IN \\(\\$1(, \\$\\d+)*\\)`));
    const scpChildren = await pgQuery(pgPool!, scpSql, scpParams);

    // v1 parity: DBConditions IN-list over the same keys, converted to `$N`.
    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ id: keys }).compile(v1Params);
    let i = 0;
    const v1Sql = `SELECT id, name FROM ${T_USERS} WHERE ${v1Where}`.replace(/\?/g, () => `$${++i}`);
    const v1Children = await pgQuery(pgPool!, v1Sql, v1Params);
    expect(scpChildren).toEqual(v1Children);
    expect(scpChildren.length).toBe(keys.length);
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
        const r = renderOperation(stmt.op, scope as never, dialect);
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
  const dialect = dialectFor('mysql');

  it('SELECT by user_id: SCP rows == v1 direct execution (`?` placeholders on real MySQL)', async () => {
    const bundle = compileBundle(contract, 'ByUser', [], 'mysql');
    const { sql, params } = renderBundleOp(bundle, { user_id: 1 }, dialect);
    expect(sql).toBe(`SELECT id, user_id, title, view_count FROM ${T_POSTS} WHERE user_id = ? ORDER BY id ASC`);
    const scpRows = await myQuery(myConn!, sql, params);

    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ user_id: 1 }).compile(v1Params);
    const v1Sql = `SELECT id, user_id, title, view_count FROM ${T_POSTS} WHERE ${v1Where} ORDER BY id ASC`;
    const v1Rows = await myQuery(myConn!, v1Sql, v1Params);
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows.length).toBeGreaterThan(0);
  });

  it('SELECT IN-list: `?` expansion; SCP rows == v1', async () => {
    const bundle = compileBundle(contract, 'ByIds', [], 'mysql');
    const { sql, params } = renderBundleOp(bundle, { ids: [1, 3] }, dialect);
    expect(sql).toBe(`SELECT id, title FROM ${T_POSTS} WHERE id IN (?, ?) ORDER BY id ASC`);
    const scpRows = await myQuery(myConn!, sql, params);

    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ id: [1, 3] }).compile(v1Params);
    const v1Sql = `SELECT id, title FROM ${T_POSTS} WHERE ${v1Where} ORDER BY id ASC`;
    const v1Rows = await myQuery(myConn!, v1Sql, v1Params);
    expect(scpRows).toEqual(v1Rows);
    expect(scpRows.map((r) => Number(r.id))).toEqual([1, 3]);
  });

  it('INSERT: SCP persists (MySQL keeps `?`, RETURNING stripped by re-select) + parity with v1', async () => {
    const wc = publishBehaviors(PostWrites);
    const bundle = compileBundle(wc, 'Create', [], 'mysql');
    const input = { user_id: 2, title: 'SCP MY Post', content: 'from scp' };
    const { sql, params } = renderBundleOp(bundle, input, dialect);
    // MySQL has no native RETURNING; the compiled text carries it (driver simulates via re-select).
    // For the raw mysql2 seam we execute the INSERT sans RETURNING, then re-select — the v1
    // MysqlSqlBuilder + mysql.ts do the same (RETURNING stripped, re-select the inserted PK).
    const insertSql = sql.replace(/\s+RETURNING\s+.+$/i, '');
    // Canonical (alphabetical) column order (WS3 SSoT): `content, title, user_id`.
    expect(insertSql).toBe(`INSERT INTO ${T_POSTS} (content, title, user_id) VALUES (?, ?, ?)`);
    const res = await myConn!.query(insertSql, params);
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

  it('read-relation batch (belongsTo author): SCP `?` IN-batch == v1 IN-list on real MySQL', async () => {
    const op: RelationOp = compileRelationOp({
      name: 'author',
      kind: 'belongsTo',
      targetTable: T_USERS,
      select: ['id', 'name'],
      parentKey: 'user_id',
      targetKey: 'id',
    });
    const parentRows = await myQuery(myConn!, `SELECT id, user_id FROM ${T_POSTS} ORDER BY id`, []);
    const keys = [...new Set(parentRows.map((r) => Number(r.user_id)))];
    const rendered = renderOperation(op.query, { [RELATION_KEYS_HEAD]: keys }, dialect);
    expect(rendered.sql).toMatch(new RegExp(`SELECT id, name FROM ${T_USERS} WHERE id IN \\(\\?(, \\?)*\\)`));
    const scpChildren = await myQuery(myConn!, rendered.sql, rendered.params.map(toPlain));

    const v1Params: unknown[] = [];
    const v1Where = new DBConditions({ id: keys }).compile(v1Params);
    const v1Sql = `SELECT id, name FROM ${T_USERS} WHERE ${v1Where}`;
    const v1Children = await myQuery(myConn!, v1Sql, v1Params);
    expect(scpChildren).toEqual(v1Children);
    expect(scpChildren.length).toBe(keys.length);
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
        const r = renderOperation(stmt.op, scope as never, dialect);
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
