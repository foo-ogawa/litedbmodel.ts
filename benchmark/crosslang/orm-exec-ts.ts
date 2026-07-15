// ════════════════════════════════════════════════════════════════════════════
// TS ORM-plan EXECUTOR (epic #63) — the thin generic statement executor + the 3 drivers.
// ════════════════════════════════════════════════════════════════════════════
//
// Executes an OpPlan (from orm-plan.ts) DB-backed against a real driver. It binds the
// PRE-RENDERED {sql, params} statements (from the proven v2 SCP path) and drives:
//   - reads: primary select → per-relation batch-load (distinct parent keys → the SAME
//     SCP relation compiler → bind) → stitch children → count rows;
//   - writes: BEGIN → per-stmt {{SEQ}} rewrite + RETURNING-id chaining → COMMIT.
// This is the shipped runtime's execute_bundle / execute_transaction_bundle semantics,
// driven over statements the SCP compile path already rendered (no re-render, no hand
// mirror of SQL generation). The row/param binding is the ONLY per-language surface.
//
// TS is the REFERENCE + the ORM-consistency anchor: the SQL is byte-identical to the
// benchmark.ts litedbmodel column (#65 parity), so TS cross-lang numbers == ORM column.

import Database from 'better-sqlite3';
import { Pool } from 'pg';
import mysql from 'mysql2/promise';
import * as lm from '../../dist/scp/index.mjs';
import type { OpPlan, ReadPlan, WritePlan, RelationStage, OrmDialect } from './orm-plan.js';
import { ddl, dropStatements, seedStatements, pgSeqResetStatements } from './orm-domain.js';

const resolvePgArrayCast = lm.resolvePgArrayCast as (sql: string, values: unknown[]) => string;
// pg binds `$N`; the portable `?` seed SQL is rewritten once at seed time.
const renderPlaceholders = lm.renderPlaceholders as (sql: string, d: OrmDialect) => string;

type Row = Record<string, unknown>;

// A per-run monotonic counter for unique-email writes ({{SEQ}} substitution).
let SEQ = 0;
function nextSeq(): number {
  return SEQ++;
}
// Replace the `{{SEQ}}` unique-email marker with the per-invocation counter — recursively, because a
// batch write (createMany/upsertMany) carries its records as an ARRAY param (pg UNNEST) or a JSON
// STRING param (sqlite json_each / mysql JSON_TABLE), and the marker lives INSIDE those. Without
// recursion the batch emails stay literal `{{SEQ}}` and collide on the second invocation.
function substOne(p: unknown, seq: number): unknown {
  if (typeof p === 'string') return p.includes('{{SEQ}}') ? p.replace(/\{\{SEQ\}\}/g, String(seq)) : p;
  if (Array.isArray(p)) return p.map((e) => substOne(e, seq));
  return p;
}
function subst(params: readonly unknown[], seq: number): unknown[] {
  return params.map((p) => substOne(p, seq));
}

// bc/driver value coercion: bigint→Number (safe for the bench's int ids).
function toPlain(v: unknown): unknown {
  return typeof v === 'bigint' ? Number(v) : v;
}

// MySQL has no RETURNING. Strip a trailing `RETURNING …` clause; the caller uses LAST_INSERT_ID()
// when it needs the generated id.
function stripReturning(sql: string): { sql: string; hadReturning: boolean } {
  const m = /\s+RETURNING\s+.+$/i.exec(sql);
  return m ? { sql: sql.slice(0, m.index), hadReturning: true } : { sql, hadReturning: false };
}

// ── Relation batch bind (consumes the BAKED per-dialect SQL from orm-plan.ts) ──
function distinctSingleKeys(stage: RelationStage, parents: Row[]): unknown[] {
  const seen = new Set<string>();
  const out: unknown[] = [];
  for (const r of parents) {
    const k = toPlain(r[stage.single!.parentKey]);
    if (k === null || k === undefined) continue;
    const s = String(k);
    if (!seen.has(s)) {
      seen.add(s);
      out.push(k);
    }
  }
  return out;
}
function distinctTuples(stage: RelationStage, parents: Row[]): unknown[][] {
  const seen = new Set<string>();
  const out: unknown[][] = [];
  const [p0, p1] = stage.composite!.parentKeys;
  for (const r of parents) {
    const k0 = toPlain(r[p0]);
    const k1 = toPlain(r[p1]);
    if (k0 === null || k0 === undefined || k1 === null || k1 === undefined) continue;
    const s = String(k0) + ' ' + String(k1);
    if (!seen.has(s)) {
      seen.add(s);
      out.push([k0, k1]);
    }
  }
  return out;
}

// Bind resolved keys onto the BAKED per-dialect relation SQL (from orm-plan.ts) per bindKind — the
// SAME protocol every language port implements (no compiler at exec time). null = no parent keys.
function bindRelation(stage: RelationStage, parents: Row[]): { sql: string; params: unknown[]; kind: RelationStage['bindKind'] } | null {
  if (stage.single) {
    const keys = distinctSingleKeys(stage, parents);
    if (keys.length === 0) return null;
    if (stage.bindKind === 'pgArraySingle') return { sql: stage.sql, params: [keys], kind: stage.bindKind };
    return { sql: stage.sql, params: [JSON.stringify(keys)], kind: stage.bindKind }; // jsonParam
  }
  const tuples = distinctTuples(stage, parents);
  if (tuples.length === 0) return null;
  if (stage.bindKind === 'pgArrayComposite') {
    return { sql: stage.sql, params: [tuples.map((t) => t[0]), tuples.map((t) => t[1])], kind: stage.bindKind };
  }
  // tupleExpand (sqlite/mysql composite): repeat the group per tuple, flatten params.
  const groups = tuples.map(() => stage.groupTemplate!).join(', ');
  return { sql: stage.sql + groups + stage.suffix, params: tuples.flat(), kind: stage.bindKind };
}

// ── Driver abstraction (sync sqlite + async pg/mysql behind one exec-op interface) ──
export interface OrmDriver {
  readonly dialect: OrmDialect;
  // Execute one op plan; returns rows read (reads) or rows written (writes).
  run(plan: OpPlan): Promise<number>;
  close(): Promise<void>;
}

// ── SQLite (better-sqlite3, sync) ─────────────────────────────────────────────
export function sqliteDriver(): OrmDriver {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  for (const s of dropStatements('sqlite')) db.exec(s);
  for (const s of ddl('sqlite')) db.exec(s);
  for (const s of seedStatements('sqlite')) db.prepare(s.sql).run(...s.params);

  // better-sqlite3 binds only number/string/bigint/buffer/null — coerce booleans to 1/0.
  const sq = (v: unknown): unknown => (typeof v === 'boolean' ? (v ? 1 : 0) : toPlain(v));
  function all(sql: string, params: unknown[]): Row[] {
    return db.prepare(sql).all(...params.map(sq)) as Row[];
  }
  function runStmt(sql: string, params: unknown[]): { lastId: number } {
    const info = db.prepare(sql).run(...params.map(sq));
    return { lastId: Number(info.lastInsertRowid) };
  }

  function readPlan(plan: ReadPlan): number {
    let rows = all(plan.reads[0].sql, plan.reads[0].params as unknown[]);
    let total = rows.length;
    const stageRows: Row[][] = [rows];
    for (const stage of plan.relations) {
      const parents = stageRows[stage.parentStmt];
      const rel = bindRelation(stage, parents);
      const children = rel ? all(rel.sql, rel.params) : [];
      total += children.length;
      stageRows.push(children);
      rows = children;
    }
    return total;
  }

  function writePlan(plan: WritePlan): number {
    const seq = nextSeq();
    const tx = db.transaction(() => {
      let returnedId = 0;
      let n = 0;
      for (const st of plan.statements) {
        let params = subst(st.params, seq);
        if (st.role === 'useReturn' && st.useReturnAt !== undefined) params = params.map((p, i) => (i === st.useReturnAt ? returnedId : p));
        if (st.role === 'insertReturn') {
          // sqlite: strip RETURNING, run, use lastInsertRowid.
          const sql = st.sql.replace(/\s+RETURNING\s+.+$/i, '');
          const { lastId } = runStmt(sql, params);
          returnedId = lastId;
          n += 1;
        } else {
          runStmt(st.sql, params);
          n += 1;
        }
      }
      return n;
    });
    return tx();
  }

  return {
    dialect: 'sqlite',
    async run(plan) {
      return plan.kind === 'read' ? readPlan(plan) : writePlan(plan);
    },
    async close() {
      db.close();
    },
  };
}

// ── Postgres (pg Pool, async) ─────────────────────────────────────────────────
export async function pgDriver(schemaName: string, conn: Record<string, unknown>, bootConn: Record<string, unknown>): Promise<OrmDriver> {
  const boot = new Pool({ ...bootConn, max: 1 });
  await boot.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`);
  await boot.end();
  const pool = new Pool({ ...conn, max: 8 });
  await pool.query('SELECT 1');
  for (const s of dropStatements('postgres')) await pool.query(s);
  for (const s of ddl('postgres')) await pool.query(s);
  for (const s of seedStatements('postgres')) await pool.query(renderPlaceholders(s.sql, 'postgres'), s.params.map(toPlain));
  for (const s of pgSeqResetStatements()) await pool.query(s);

  async function all(sql: string, params: unknown[], arrayParams = false): Promise<Row[]> {
    // pg binds a JS array as one array param natively.
    const res = await pool.query(sql, params.map(toPlain));
    void arrayParams;
    return res.rows as Row[];
  }

  async function readPlan(plan: ReadPlan): Promise<number> {
    const first = await all(plan.reads[0].sql, plan.reads[0].params as unknown[]);
    let total = first.length;
    const stageRows: Row[][] = [first];
    for (const stage of plan.relations) {
      const parents = stageRows[stage.parentStmt];
      const rel = bindRelation(stage, parents);
      let children: Row[] = [];
      if (rel) {
        // pg single-key: resolve the deferred array cast from the real keys (rel.params[0] is the array).
        const sql = rel.kind === 'pgArraySingle' ? resolvePgArrayCast(rel.sql, rel.params[0] as unknown[]) : rel.sql;
        children = await all(sql, rel.params);
      }
      total += children.length;
      stageRows.push(children);
    }
    return total;
  }

  async function writePlan(plan: WritePlan): Promise<number> {
    const seq = nextSeq();
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      let returnedId = 0;
      let n = 0;
      for (const st of plan.statements) {
        let params = subst(st.params, seq);
        if (st.role === 'useReturn' && st.useReturnAt !== undefined) params = params.map((p, i) => (i === st.useReturnAt ? returnedId : p));
        const res = await client.query(st.sql, params.map(toPlain));
        if (st.role === 'insertReturn' && res.rows.length > 0) returnedId = Number((res.rows[0] as Row).id);
        n += 1;
      }
      await client.query('COMMIT');
      return n;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  return {
    dialect: 'postgres',
    async run(plan) {
      return plan.kind === 'read' ? readPlan(plan) : writePlan(plan);
    },
    async close() {
      await pool.end();
    },
  };
}

// ── MySQL (mysql2/promise pool, async) ────────────────────────────────────────
export async function mysqlDriver(dbName: string, conn: Record<string, unknown>, bootConn: Record<string, unknown>): Promise<OrmDriver> {
  const boot = mysql.createPool({ ...bootConn, connectionLimit: 1 });
  await boot.query(`CREATE DATABASE IF NOT EXISTS ${dbName}`);
  await boot.end();
  const pool = mysql.createPool({ ...conn, connectionLimit: 8, multipleStatements: false });
  await pool.query('SELECT 1');
  for (const s of dropStatements('mysql')) await pool.query(s);
  for (const s of ddl('mysql')) await pool.query(s);
  for (const s of seedStatements('mysql')) await pool.query(s.sql, s.params as unknown[]);

  async function all(sql: string, params: unknown[]): Promise<Row[]> {
    const [rows] = await pool.query(sql, params.map(toPlain));
    return Array.isArray(rows) ? (rows as Row[]) : [];
  }

  async function readPlan(plan: ReadPlan): Promise<number> {
    const first = await all(plan.reads[0].sql, plan.reads[0].params as unknown[]);
    let total = first.length;
    const stageRows: Row[][] = [first];
    for (const stage of plan.relations) {
      const parents = stageRows[stage.parentStmt];
      const rel = bindRelation(stage, parents);
      const children = rel ? await all(rel.sql, rel.params) : [];
      total += children.length;
      stageRows.push(children);
    }
    return total;
  }

  async function writePlan(plan: WritePlan): Promise<number> {
    const seq = nextSeq();
    const c = await pool.getConnection();
    try {
      await c.query('START TRANSACTION');
      let returnedId = 0;
      let n = 0;
      for (const st of plan.statements) {
        let params = subst(st.params, seq);
        if (st.role === 'useReturn' && st.useReturnAt !== undefined) params = params.map((p, i) => (i === st.useReturnAt ? returnedId : p));
        // MySQL has no native RETURNING: strip it on ANY statement (insertReturn AND a
        // plain upsert that carries `RETURNING id`); capture LAST_INSERT_ID for chaining.
        const { sql } = stripReturning(st.sql);
        const [res]: [mysql.ResultSetHeader, unknown] = (await c.query(sql, params.map(toPlain))) as never;
        if (st.role === 'insertReturn') returnedId = Number(res.insertId);
        n += 1;
      }
      await c.query('COMMIT');
      return n;
    } catch (e) {
      await c.query('ROLLBACK');
      throw e;
    } finally {
      c.release();
    }
  }

  return {
    dialect: 'mysql',
    async run(plan) {
      return plan.kind === 'read' ? readPlan(plan) : writePlan(plan);
    },
    async close() {
      await pool.end();
    },
  };
}
