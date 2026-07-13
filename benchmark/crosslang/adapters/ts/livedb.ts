// ════════════════════════════════════════════════════════════════════════════
// TS DB-backed live-DB seam (epic #44 gap #2) — real PG / MySQL execution.
// ════════════════════════════════════════════════════════════════════════════
//
// Node's `pg` / `mysql2` drivers are ASYNC-only (no synchronous `.all()`), so the TS
// cell CANNOT drive PG/MySQL through the shipped SYNCHRONOUS `executeBundle` /
// `executeTransactionBundle` path (which needs a sync `SqliteDb`-shaped driver — what
// better-sqlite3 provides for SQLite). Instead the TS cell uses the litedbmodel
// PRODUCTION ASYNC live-DB entry points, exactly as the WS6 `ScpDialect.test.ts`
// integration does:
//
//   - reads (find/complexWhere/inList): `executeBundleAsync` + `pgPoolExecutor` /
//     `mysqlPoolExecutor` — the async production read path.
//   - read-relations (belongsTo/hasMany/hasManyLimit): async primary read, then the
//     relation batch rendered via `renderPlaceholders` (+ PG deferred array cast
//     resolved from the real keys) and executed on the pool — the SAME render
//     `runRelationOp` performs.
//   - writes (batchInsert/writeTxGate): the derived `TransactionPlan` executed in ONE
//     real transaction, each statement rendered via `renderTxStatement`. MySQL has no
//     native RETURNING, so the body INSERT's RETURNING is emulated by a re-select on
//     `LAST_INSERT_ID()` (the standard MySQL emulation).
//
// This is disclosed in CROSS-LANG.md: the TS PG/MySQL DB-backed path is the ASYNC
// production model (the non-TS legs use their shipped SYNC `PostgresDriver`/
// `MysqlDriver` through the standard runtime), so TS DB-backed PG/MySQL numbers carry
// the async-driver caveat and are comparable WITHIN TS across surfaces.
//
// #53 follow-up (independent audit): this seam is isolated into its OWN `scp_ts_bench`
// namespace (PG schema via search_path, MySQL database), mirroring the Rust/Go/PHP
// adapters' `scp_<lang>_bench` — it never creates/seeds/drops tables in the shared
// `testdb` that `test/fixtures/init.sql` seeds for the integration suite.

import { Pool } from 'pg';
import mysql from 'mysql2/promise';
import * as lm from '../../../../dist/scp/index.mjs';
import {
  PG_SCHEMA, PG_SEQ_RESET, MYSQL_SCHEMA, seedStatementsShared, PG_CONN, MYSQL_CONN,
  PG_BOOT_CONN, MYSQL_BOOT_CONN, PG_SCHEMA_NAME, MYSQL_DB_NAME,
} from '../../domain.js';

type Row = Record<string, unknown>;
type CaseArt = { case: string; kind: string; entry?: string; withRelation?: string; bundle: any; input: any };

/** bc evaluates ints to bigint; convert to a driver-bindable JS value. */
function toPlain(v: unknown): unknown {
  return typeof v === 'bigint' ? Number(v) : v;
}

// ── Postgres ──────────────────────────────────────────────────────────────────

export interface LiveDb {
  /** A zero-arg op for a case (one logical case op against the real DB). */
  op(c: CaseArt): () => Promise<unknown>;
  close(): Promise<void>;
}

export async function connectPg(): Promise<LiveDb> {
  // Bootstrap: CREATE SCHEMA IF NOT EXISTS on the base `testdb` connection (isolated
  // `scp_ts_bench` namespace — mirrors the Rust/Go/PHP adapters; never touches the
  // integration-test fixture tables in `testdb.public`).
  const boot = new Pool({ ...PG_BOOT_CONN, max: 1 });
  await boot.query(`CREATE SCHEMA IF NOT EXISTS ${PG_SCHEMA_NAME}`);
  await boot.end();
  // The bench pool's every physical connection pins `search_path` via the libpq startup
  // `options` param (set in `PG_CONN`) — a runtime `SET search_path` on one borrowed
  // connection would NOT apply to the pool's other connections.
  const pool = new Pool({ ...PG_CONN, max: 8 });
  await pool.query('SELECT 1');
  for (const s of PG_SCHEMA) await pool.query(s);
  for (const s of seedStatementsShared()) await pool.query(s);
  for (const s of PG_SEQ_RESET) await pool.query(s);
  const exec = lm.pgPoolExecutor(pool);

  async function readPrimary(c: CaseArt): Promise<Row[]> {
    const r = await lm.executeBundleAsync(c.bundle, c.input, { exec, entry: c.entry, dialect: 'postgres' });
    return Array.isArray(r) ? (r as Row[]) : [];
  }
  async function readRelation(c: CaseArt): Promise<void> {
    const rows = await readPrimary(c);
    const op = c.bundle.relations[c.withRelation!];
    const keys = [...new Set(rows.map((r) => toPlain(r[op.parentKey])))];
    const cast = lm.resolvePgArrayCast(op.sql, keys);
    const sql = lm.renderPlaceholders(cast, 'postgres');
    await pool.query(sql, [keys]);
  }
  async function runPlan(c: CaseArt): Promise<void> {
    const plan = c.bundle.transaction;
    const input = c.kind === 'tx' ? c.input : {};
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const scope: Record<string, unknown> = { ...input };
      for (const stmt of plan.statements) {
        const r = lm.renderTxStatement(stmt.op, scope as never, 'postgres');
        const res = await client.query(r.sql, r.params.map(toPlain));
        if (stmt.gate === 'existsElseRollback' && res.rows.length === 0) throw new Error('gate requires failed');
        if (stmt.id === plan.entityFrom && res.rows.length > 0) scope.__entity = res.rows[0];
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }
  return {
    op(c) {
      if (c.kind === 'read') return () => readPrimary(c);
      if (c.kind === 'relation') return () => readRelation(c);
      return () => runPlan(c);
    },
    async close() { await pool.end(); },
  };
}

// ── MySQL ───────────────────────────────────────────────────────────────────────

export async function connectMysql(): Promise<LiveDb> {
  // Bootstrap: CREATE DATABASE IF NOT EXISTS on the base `testdb` connection (isolated
  // `scp_ts_bench` database — mirrors the Rust/Go/PHP adapters; never touches the
  // integration-test fixture tables in `testdb`).
  const boot = mysql.createPool({ ...MYSQL_BOOT_CONN, connectionLimit: 1 });
  await boot.query(`CREATE DATABASE IF NOT EXISTS ${MYSQL_DB_NAME}`);
  await boot.end();
  const pool = mysql.createPool({ ...MYSQL_CONN, connectionLimit: 8, multipleStatements: false });
  await pool.query('SELECT 1');
  for (const s of MYSQL_SCHEMA) await pool.query(s);
  for (const s of seedStatementsShared()) await pool.query(s);
  const exec = lm.mysqlPoolExecutor(pool as never);

  async function readPrimary(c: CaseArt): Promise<Row[]> {
    const r = await lm.executeBundleAsync(c.bundle, c.input, { exec, entry: c.entry, dialect: 'mysql' });
    return Array.isArray(r) ? (r as Row[]) : [];
  }
  async function readRelation(c: CaseArt): Promise<void> {
    const rows = await readPrimary(c);
    const op = c.bundle.relations[c.withRelation!];
    const keys = [...new Set(rows.map((r) => toPlain(r[op.parentKey])))];
    const sql = lm.renderPlaceholders(op.sql, 'mysql');
    // MySQL IN-list is the single-JSON-array param form.
    await pool.query(sql, [JSON.stringify(keys)]);
  }
  async function runPlan(c: CaseArt): Promise<void> {
    const plan = c.bundle.transaction;
    const input = c.kind === 'tx' ? c.input : {};
    const conn = await pool.getConnection();
    try {
      await conn.query('START TRANSACTION');
      const scope: Record<string, unknown> = { ...input };
      for (const stmt of plan.statements) {
        const r = lm.renderTxStatement(stmt.op, scope as never, 'mysql');
        // MySQL has no native RETURNING: strip it, run the INSERT, then re-select the
        // new row by LAST_INSERT_ID() (the standard emulation the async live path uses).
        const returningMatch = /\s+RETURNING\s+(.+)$/i.exec(r.sql);
        if (returningMatch && /^\s*INSERT/i.test(r.sql)) {
          const insertSql = lm.stripMysqlPkHint(r.sql).replace(/\s+RETURNING\s+.+$/i, '');
          const [res]: any = await conn.query(insertSql, r.params.map(toPlain));
          const insertId = res?.insertId;
          const cols = returningMatch[1];
          // The body INSERT targets `posts`; re-select the just-inserted row.
          const [rows]: any = await conn.query(`SELECT ${cols} FROM posts WHERE id = ?`, [insertId]);
          const rr = Array.isArray(rows) ? rows : [];
          if (stmt.id === plan.entityFrom && rr.length > 0) scope.__entity = rr[0];
          continue;
        }
        const [rows]: any = await conn.query(r.sql, r.params.map(toPlain));
        const rr = Array.isArray(rows) ? rows : [];
        if (stmt.gate === 'existsElseRollback' && rr.length === 0) throw new Error('gate requires failed');
        if (stmt.id === plan.entityFrom && rr.length > 0) scope.__entity = rr[0];
      }
      await conn.query('COMMIT');
    } catch (e) {
      await conn.query('ROLLBACK');
      throw e;
    } finally {
      conn.release();
    }
  }
  return {
    op(c) {
      if (c.kind === 'read') return () => readPrimary(c);
      if (c.kind === 'relation') return () => readRelation(c);
      return () => runPlan(c);
    },
    async close() { await pool.end(); },
  };
}
