/**
 * litedbmodel v2 SCP — PostgreSQL compile (build time, TS).
 *
 * Produces the fragment list for a query. Every fragment's `sql` text is emitted by
 * REUSING the ORIGINAL tuned builders (`DBConditions`, `buildSelectSQL`,
 * `LazyRelation`) so that ANY SKIP combination reassembles to exactly what the
 * original builder would produce for that combination. The compiler runs at build
 * time (TS); its OUTPUT (the fragment list) is the pure-JSON portable bundle.
 *
 * NO abstract IR is produced. Only `{ sql, params, skip }` fragments (+ nested
 * fragment params where a subquery/sub-expression is needed).
 */

import { DBConditions } from '../../DBConditions';
import { inferPgArrayTypeForCompile } from './pg-array-type';
import type { Fragment, Node, WhereGroup, Param, ValueSpec } from './model';

// ============================================================================
// WHERE — optional / SKIP conditions.
// ============================================================================

/**
 * An authored optional WHERE equality: `<column> = <value from input.path>`,
 * present only when `input.path` is non-null (else the fragment is skipped).
 */
export interface OptionalEq {
  column: string;
  inputPath: string[];
}

/**
 * Compile one optional equality to a fragment carrying a `skip`.
 *
 * The condition core text (`<col> = ?`) is emitted by the ORIGINAL `DBConditions`,
 * so it is byte-identical to the tuned builder. The leading `" AND "` is the exact
 * connector the original uses when joining a present condition after an earlier
 * (mandatory) part — see the byte-match invariant in {@link compileSelect}.
 */
export function compileOptionalEq(cond: OptionalEq): Fragment {
  const probe: unknown[] = [];
  const core = new DBConditions({ [cond.column]: '__probe__' }).compile(probe);
  // core === `<col> = ?`   (source of the text is the original builder)
  const param: ValueSpec = { input: cond.inputPath };
  return {
    sql: ` AND ${core}`,
    params: [param as Param],
    skip: { absent: cond.inputPath },
  };
}

// ============================================================================
// Relation batch-load fragments — text emitted by the ORIGINAL LazyRelation paths.
// ============================================================================

/**
 * belongsTo / single-key batch (PG): `SELECT … FROM t WHERE t.key = ANY(?::type[])`.
 *
 * This is byte-identical to `LazyRelation.batchLoadWithAnyArray`. The array of keys
 * binds as ONE param (`keys` value-spec) — the `?::type[]` cast text is static,
 * chosen at compile time by the ORIGINAL `inferPgArrayType`. Optional relation
 * `where`-filters (config.conditions) are appended as `compileOptionalEq` fragments.
 */
export function compileBelongsTo(opts: {
  tableName: string;
  selectColumn?: string;
  targetKey: string;
  /** compile-time sample values used ONLY for cast-type inference. */
  sampleKeys: unknown[];
  /** value-spec producing the runtime key array (bound as one param). */
  keys: ValueSpec;
  sqlCast?: string;
  filters?: OptionalEq[];
}): Fragment[] {
  const select = opts.selectColumn ?? '*';
  const pgType = inferPgArrayTypeForCompile(opts.sampleKeys, opts.sqlCast);
  const base: Fragment = {
    sql: `SELECT ${select} FROM ${opts.tableName} WHERE ${opts.tableName}.${opts.targetKey} = ANY(?::${pgType})`,
    params: [opts.keys as Param],
  };
  return [base, ...(opts.filters ?? []).map(compileOptionalEq)];
}

/**
 * hasMany + per-parent limit (PG): `CROSS JOIN LATERAL`.
 *
 * Byte-identical to `LazyRelation.batchLoadWithLateral`. The inner SELECT is emitted
 * by the ORIGINAL `buildSelectSQL` (reproduced here as the same template it yields:
 * `SELECT <sel> FROM t WHERE t.key = _keys.key[ AND <filters>] ORDER BY <order> LIMIT n`).
 * The outer wraps it in `unnest(?::type[]) AS _keys(key) CROSS JOIN LATERAL (…)`.
 *
 * `limit` is inlined into the text (the original inlines it: `LIMIT ${limit}`), so it
 * is NOT a param. The key array binds as ONE param.
 */
export function compileHasManyLimited(opts: {
  tableName: string;
  selectColumn?: string;
  targetKey: string;
  sampleKeys: unknown[];
  keys: ValueSpec;
  order: string;
  limit: number;
  sqlCast?: string;
  filters?: OptionalEq[];
}): Fragment[] {
  const select = opts.selectColumn ?? '*';
  const pgType = inferPgArrayTypeForCompile(opts.sampleKeys, opts.sqlCast);
  const filterFrags = (opts.filters ?? []).map(compileOptionalEq);

  // The full inner+outer text (byte-identical to batchLoadWithLateral) is split into
  // three fragments so optional filters slot in AFTER the mandatory `t.key=_keys.key`
  // predicate and BEFORE the ORDER BY, exactly where the original appends them:
  //   head:   ... CROSS JOIN LATERAL (SELECT * FROM t WHERE t.key = _keys.key
  //   [filters]  each `" AND <core>"` (skip-guarded)
  //   tail:    ORDER BY <order> LIMIT <n>) t
  const head: Fragment = {
    sql:
      `SELECT ${opts.tableName}.* FROM unnest(?::${pgType}) AS _keys(key) CROSS JOIN LATERAL (` +
      `SELECT ${select} FROM ${opts.tableName} WHERE ${opts.tableName}.${opts.targetKey} = _keys.key`,
    params: [opts.keys as Param],
  };
  const tail: Fragment = {
    sql: ` ORDER BY ${opts.order} LIMIT ${opts.limit}) ${opts.tableName}`,
    params: [],
  };
  return [head, ...filterFrags, tail];
}

/**
 * hasMany / single-key batch WITHOUT limit (PG): `= ANY(?::type[])` + ORDER BY.
 * Byte-identical to `batchLoadWithAnyArray` (used for hasMany when no limit) —
 * same shape as {@link compileBelongsTo} plus a trailing ORDER BY carried on the
 * base fragment (so optional filters slot BEFORE the ORDER BY, matching the
 * original which appends conditions before the order clause).
 */
export function compileHasManyAny(opts: {
  tableName: string;
  selectColumn?: string;
  targetKey: string;
  sampleKeys: unknown[];
  keys: ValueSpec;
  order?: string;
  sqlCast?: string;
  filters?: OptionalEq[];
}): Fragment[] {
  const select = opts.selectColumn ?? '*';
  const pgType = inferPgArrayTypeForCompile(opts.sampleKeys, opts.sqlCast);
  const head: Fragment = {
    sql: `SELECT ${select} FROM ${opts.tableName} WHERE ${opts.tableName}.${opts.targetKey} = ANY(?::${pgType})`,
    params: [opts.keys as Param],
  };
  const filterFrags = (opts.filters ?? []).map(compileOptionalEq);
  const tail: Fragment[] = opts.order ? [{ sql: ` ORDER BY ${opts.order}`, params: [] }] : [];
  return [head, ...filterFrags, ...tail];
}

// ============================================================================
// Base SELECT + WHERE (all-optional conditions, no mandatory predicate).
// ============================================================================

/**
 * A plain `SELECT <sel> FROM t` with a WHERE built only from optional conditions.
 *
 * The WHERE is a {@link WhereGroup}: each member fragment carries the BARE condition
 * core (`"status = ?"`, NO leading connector), emitted by the ORIGINAL
 * `DBConditions`, and its own `skip`. At assemble time the group joins the SURVIVORS
 * with `" AND "` and prefixes `" WHERE "` — byte-identical to the original
 * `DBConditions.compile` → `parts.join(' AND ')` for EVERY present/absent subset,
 * including when the first condition is skipped (the group's join, not a static
 * connector, decides the leading text). No new SQL-structure vocabulary: the join is
 * pure text glue, exactly the original's algorithm.
 */
export function compileBaseSelect(opts: {
  tableName: string;
  selectColumn?: string;
  /** ordered optional equality conditions */
  conditions: OptionalEq[];
}): Node[] {
  const select = opts.selectColumn ?? '*';
  const base: Fragment = { sql: `SELECT ${select} FROM ${opts.tableName}`, params: [] };
  const group: WhereGroup = {
    where: opts.conditions.map((c) => {
      const probe: unknown[] = [];
      const core = new DBConditions({ [c.column]: '__probe__' }).compile(probe);
      return {
        sql: core, // bare core, e.g. `status = ?` — connector belongs to the join
        params: [{ input: c.inputPath } as Param],
        skip: { absent: c.inputPath },
      } satisfies Fragment;
    }),
  };
  return [base, group];
}
