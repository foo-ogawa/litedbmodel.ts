/**
 * litedbmodel v2 SCP — the LOCKED minimal bc integration: ONE catalog component
 * **`makeSQL(sql, params, skip?)`** (a behavior-contracts catalog leaf, exactly like
 * graphddb's `GetItem`/`Query`).
 *
 * ## The locked model (owner-specified — nothing else exists)
 *
 * - bc integration = ONE catalog component `makeSQL`. Ports:
 *     - `sql`    — tuned dialect SQL text (a string; `?` placeholders).
 *     - `params` — value list. Each element is EITHER a bound value (an opaque SQL
 *                  value: number/string/bool/null/Date/array/…) OR a **nested
 *                  `makeSQL`** (a `{ sql, params, skip? }` object) whose `sql` is
 *                  spliced at the corresponding `?` and whose params flow inline.
 *                  A subquery / sub-expression is a nested `makeSQL` in a param slot.
 *     - `skip?`  — presence condition. When satisfied, the component contributes
 *                  NOTHING (its `sql` and its `params` both drop).
 * - A query = a composition of `makeSQL` components. bc composes; present ones
 *   concatenate. There is NO relation-op / operator / "kind" for SQL structure.
 * - `= ANY`, `CROSS JOIN LATERAL`, `UNNEST`, cast, subquery, batch shapes are ALL
 *   TEXT inside `sql`. Never modeled.
 * - An array/rows param binds as ONE value with STATIC text on EVERY dialect
 *   (epic #43/#45): PostgreSQL `= ANY(?::t[])` / `UNNEST`, MySQL/SQLite a single JSON
 *   param expanded server-side (`MEMBER OF` / `JSON_TABLE` / `json_each`). There is no
 *   array placeholder-count-expansion anywhere anymore.
 *
 * litedbmodel supplies (spec §11): the `makeSQL` catalog entry, its **handler**
 * (bind params → execute SQL), and the **compile** step that emits the tuned dialect
 * SQL text (by REUSING the original tuned builders — see `compile-*.ts`). bc supplies
 * composition / value-eval / envelope / plan (it comes free once queries are `makeSQL`
 * components). The bundle stays pure JSON (multi-language ready).
 */

import type { CatalogEntry } from 'behavior-contracts';

/** The single catalog leaf name. Everything SQL is a `makeSQL`. */
export const MAKESQL = 'makeSQL' as const;

/**
 * A bound SQL param value OR a nested `makeSQL` (subquery / sub-expression).
 *
 * A nested `makeSQL` in a param slot is how subqueries are represented: its `sql`
 * splices into the parent's `?` position and its params flow into the parent's param
 * stream in order. This is the ONLY recursion; there is no other SQL-structure
 * vocabulary.
 */
export type SqlParam = unknown | MakeSQL;

/**
 * The `makeSQL` port bundle — the ONLY structural type.
 *
 * `sql` is complete tuned SQL text with `?` placeholders (leading connector included
 * where the composition needs it, exactly as the original builder emits). `params`
 * are 1:1 with the top-level `?` (a bound value fills one `?`; a nested `makeSQL`
 * param splices its own `sql`, which may contain further `?`). `skip`, when `true`,
 * omits the whole component (sql AND params).
 */
export interface MakeSQL {
  sql: string;
  params: SqlParam[];
  /** Presence condition already resolved to a boolean at bind/eval time. */
  skip?: boolean;
}

/** Structural guard: a nested-`makeSQL` param has a string `sql`; a bound value does not. */
export function isMakeSQL(p: SqlParam): p is MakeSQL {
  return (
    typeof p === 'object' &&
    p !== null &&
    typeof (p as MakeSQL).sql === 'string' &&
    Array.isArray((p as MakeSQL).params)
  );
}

/**
 * The `makeSQL` catalog entry (bc `CatalogEntry`). A leaf Specialty Component: name +
 * Port contract + output shape. The handler implementation is separate (see
 * `handler.ts`) — the IR carries only the reference name + port wiring (C4).
 *
 * `params` is a dynamic-arity port family (the value list length is query-specific),
 * so `additionalPorts` is not needed: `params` is a single arr-typed port, `sql` a
 * string, `skip` an optional bool. `output.shape = 'items'` (a SELECT yields rows; a
 * write yields RETURNING rows or `[]`).
 */
export const makeSqlCatalogEntry: CatalogEntry = {
  name: MAKESQL,
  inputPorts: {
    sql: { type: 'string', required: true },
    params: { type: 'arr' },
    skip: { type: 'bool' },
  },
  output: { shape: 'items' },
  portableToIR: true,
};

/** The litedbmodel catalog — a single leaf, `makeSQL`. */
export const LITEDBMODEL_MAKESQL_CATALOG: Record<string, CatalogEntry> = {
  [MAKESQL]: makeSqlCatalogEntry,
};

// ============================================================================
// Assembly — flatten a MakeSQL (with nested-makeSQL params + skip) to `{ sql, params }`.
//
// This is the pure, language-portable core the handler and the thin runtime share.
// It does exactly what the locked model says: drop skipped components, splice nested
// `makeSQL` text at its `?`, concatenate params in order. No SQL is generated here —
// the text is already complete; assembly only splices and fills.
// ============================================================================

/** A fully-assembled statement: complete SQL text (`?`) + a flat ordered value list. */
export interface AssembledSql {
  sql: string;
  params: unknown[];
}

/**
 * Assemble one `makeSQL` into `{ sql, params }`.
 *
 * - If `skip` is true, this component contributes nothing: returns empty text + no
 *   params (the caller composes over the survivors).
 * - Otherwise interleave the literal `sql` (split on `?`) with each param: a bound
 *   value emits a single `?` and contributes its value; a nested `makeSQL` splices
 *   its assembled `sql` and contributes its assembled params (recursively). A nested
 *   `makeSQL` that is itself skipped contributes empty text + no params.
 */
export function assembleMakeSQL(node: MakeSQL): AssembledSql {
  if (node.skip === true) return { sql: '', params: [] };

  const chunks = node.sql.split('?');
  if (chunks.length - 1 !== node.params.length) {
    throw new Error(
      `makeSQL placeholder/param mismatch: ${chunks.length - 1} '?' vs ${node.params.length} params in ${JSON.stringify(node.sql)}`
    );
  }

  let sql = chunks[0];
  const params: unknown[] = [];
  for (let i = 0; i < node.params.length; i++) {
    const p = node.params[i];
    if (isMakeSQL(p)) {
      const inner = assembleMakeSQL(p);
      sql += inner.sql + chunks[i + 1];
      params.push(...inner.params);
    } else {
      sql += '?' + chunks[i + 1];
      params.push(p);
    }
  }
  return { sql, params };
}

/**
 * Compose an ordered list of `makeSQL` components into one statement: concatenate the
 * assembled `sql` of every PRESENT component and their params in order (the locked
 * model's "present ones concatenate"). Skipped components drop entirely.
 *
 * Each component's `sql` already carries whatever connector text the tuned builder
 * emits for that position, so concatenation is byte-faithful for every skip subset.
 */
export function composeMakeSQL(nodes: MakeSQL[]): AssembledSql {
  let sql = '';
  const params: unknown[] = [];
  for (const node of nodes) {
    const r = assembleMakeSQL(node);
    sql += r.sql;
    params.push(...r.params);
  }
  return { sql, params };
}
