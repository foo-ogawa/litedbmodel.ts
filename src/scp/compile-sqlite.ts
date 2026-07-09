/**
 * litedbmodel v2 SCP — SQLite Backend Compile (WS1, #21).
 *
 * Lowers a structured CRUD operation description (a Component-graph Select/Insert/Update/
 * Delete node with ports) into a {@link CompiledOperation}: SQLite dialect SQL text,
 * a fragment tree (dynamic WHERE/SET with existence rules), and static param slots — the
 * IR shape of spec §8. Placeholders are unified as `?` (the `?`→`$N` pass is PG-only and
 * OUT OF SCOPE for WS1).
 *
 * The generated SQLite SQL is byte-identical to what litedbmodel v1's SqliteSqlBuilder /
 * `DBModel._buildSelectSQL` produce for equivalent queries (the golden bar). In
 * particular:
 *   - SELECT: `SELECT <cols> FROM <t>[ WHERE <c>][ GROUP BY <g>][ ORDER BY <o>][ LIMIT ?][ OFFSET ?]`
 *   - INSERT: `INSERT INTO <t> (<cols>) VALUES (<vals>)[ RETURNING <r>]`
 *   - UPDATE: `UPDATE <t> SET <a = ?, …> WHERE <c>[ RETURNING <r>]`
 *   - DELETE: `DELETE FROM <t> WHERE <c>[ RETURNING <r>]`
 *   - WHERE parts joined by ` AND ` (or ` OR ` for an OR group), equality `col = ?`,
 *     IN-list `col IN (?, …)`, empty array → `1 = 0`, NULL → `col IS NULL`.
 *
 * ## Hard rule: closed Expression IR only
 *
 * Every param slot this compiler emits is an Expression IR node from bc's closed set
 * (`PORTABLE_EXPR_OPERATORS`). NO litedbmodel-local opcode is ever invented. An
 * un-lowerable predicate is the Raw SQL escape hatch's concern (spec §13), not a fake
 * opcode. {@link compileSelect} et al. only construct `ref` / `coalesce` / etc.
 */

import type { AssemblySpec, CompiledOperation, ExprNode, Fragment, FragmentTree } from './ir';
import { WHERE_SLOT } from './ir';

// ── Authoring-adjacent operation description (Backend Compile input) ──────────
//
// This is the litedbmodel-side lowering INPUT: a Component-graph CRUD node's resolved
// ports, expressed as a structured value. WS2 (authoring parse) produces this from
// `SemanticBehavior` method bodies / public-API calls; WS1 compiles it to SQL IR.

/** A reference into the input scope or a sibling wire result (Expression IR `ref`). */
export type Ref = { ref: string[] };

/** A single WHERE condition. `skipWhen` (absent → always present) drives SKIP (§8). */
export type Condition =
  | { kind: 'eq'; column: string; value: ExprNode; skipWhen?: ExprNode }
  | { kind: 'cmp'; column: string; op: '<' | '<=' | '>' | '>=' | '<>'; value: ExprNode; skipWhen?: ExprNode }
  | { kind: 'isNull'; column: string; skipWhen?: ExprNode }
  | { kind: 'in'; column: string; value: ExprNode; skipWhen?: ExprNode }
  /** A nested AND/OR group (parenthesized, dynamic-expansion spec §4). */
  | { kind: 'group'; connector: 'AND' | 'OR'; conditions: Condition[]; skipWhen?: ExprNode };

export interface SelectDesc {
  table: string;
  /** Projection columns; `['*']` renders `*`. */
  select: string[];
  where?: Condition[];
  /** Raw ORDER BY body (dialect-neutral text, e.g. `"created_at DESC"`). */
  order?: string;
  /** GROUP BY body. */
  group?: string;
  /** LIMIT value as an Expression IR node (e.g. a `coalesce` default). */
  limit?: ExprNode;
  /** OFFSET value as an Expression IR node. */
  offset?: ExprNode;
}

export interface InsertDesc {
  table: string;
  /** Column → value Expression IR (insertion order = SQL column order). */
  values: Record<string, ExprNode>;
  returning?: string[];
  onConflict?: string[];
  onConflictAction?: 'ignore' | { updateColumns: 'all' | string[] };
}

export interface UpdateDesc {
  table: string;
  /** Column → value Expression IR (insertion order = SET clause order). */
  set: Record<string, ExprNode>;
  where: Condition[];
  returning?: string[];
}

export interface DeleteDesc {
  table: string;
  where: Condition[];
  returning?: string[];
}

// ── WHERE fragment-tree lowering (spec §8) ────────────────────────────────────

/** Lower one condition to a fragment (leaf) or nested tree, without a leading connector. */
function lowerCondition(c: Condition): Fragment | FragmentTree {
  if (c.kind === 'group') {
    const tree: FragmentTree = {
      connector: c.connector,
      fragments: c.conditions.map(lowerCondition),
    };
    // A group's own SKIP guard is carried by wrapping it — but a FragmentTree has no
    // `when`; represent a conditional group as a single-fragment tree is not expressible,
    // so a guarded group is lowered as a leaf-less tree whose presence is the OR/AND of
    // its members' presence (renderTree already drops it when empty). A group with its
    // own skipWhen is not part of WS1's representative surface; reject loudly if used.
    if (c.skipWhen !== undefined) {
      throw new Error('a conditional (SKIP) group is not supported in WS1; guard the member conditions individually');
    }
    return tree;
  }

  let f: Fragment;
  switch (c.kind) {
    case 'eq':
      f = { always: true, sql: `${c.column} = ?`, params: [c.value] };
      break;
    case 'cmp':
      f = { always: true, sql: `${c.column} ${c.op} ?`, params: [c.value] };
      break;
    case 'isNull':
      f = { always: true, sql: `${c.column} IS NULL`, params: [] };
      break;
    case 'in':
      // IN-list: single `(?)` slot expands at render time (spec §5). golden text:
      // `col IN (?, ?, …)` with `, ` separators; empty array degenerates to `(NULL)`.
      f = { always: true, sql: `${c.column} IN (?)`, params: [c.value], expand: 0 };
      break;
  }
  if (c.skipWhen !== undefined) {
    delete f.always;
    f.when = c.skipWhen;
  }
  return f;
}

/** Build a top-level AND fragment tree from a condition list (null when empty). */
function lowerWhere(conditions: Condition[] | undefined): FragmentTree | null {
  if (conditions === undefined || conditions.length === 0) return null;
  return { connector: 'AND', fragments: conditions.map(lowerCondition) };
}

// ── CRUD compilers ────────────────────────────────────────────────────────────

const assembly = (shape: string): AssemblySpec => ({ shape });

/**
 * Compile a SELECT. Static params (LIMIT / OFFSET) sit AFTER the `{where}` splice, so
 * they are emitted after the fragment params (dynamic-expansion spec §6).
 */
export function compileSelect(desc: SelectDesc): CompiledOperation {
  const cols = desc.select.length === 1 && desc.select[0] === '*' ? '*' : desc.select.join(', ');
  let sql = `SELECT ${cols} FROM ${desc.table}`;
  const where = lowerWhere(desc.where);
  // The WHERE splice marker is emitted whenever a fragment tree exists (even if all
  // fragments may skip — degeneration is a render-time decision, spec §3).
  if (where !== null) sql += WHERE_SLOT;
  if (desc.group !== undefined) sql += ` GROUP BY ${desc.group}`;
  if (desc.order !== undefined) sql += ` ORDER BY ${desc.order}`;

  const params: ExprNode[] = [];
  if (desc.limit !== undefined) {
    sql += ` LIMIT ?`;
    params.push(desc.limit);
  }
  if (desc.offset !== undefined) {
    sql += ` OFFSET ?`;
    params.push(desc.offset);
  }

  return { component: 'Select', sql, where, params, assembly: assembly('items') };
}

/** Compile an INSERT. All params are static (no WHERE); order = column order. */
export function compileInsert(desc: InsertDesc): CompiledOperation {
  const columns = Object.keys(desc.values);
  const placeholders = columns.map(() => '?').join(', ');
  const params: ExprNode[] = columns.map((c) => desc.values[c]);

  let sql: string;
  if (desc.onConflict !== undefined && desc.onConflictAction !== undefined) {
    if (desc.onConflictAction === 'ignore') {
      sql = `INSERT OR IGNORE INTO ${desc.table} (${columns.join(', ')}) VALUES (${placeholders})`;
    } else {
      const updateCols = desc.onConflictAction.updateColumns === 'all' ? columns : desc.onConflictAction.updateColumns;
      const updateClauses = updateCols.map((c) => `${c} = excluded.${c}`);
      sql = `INSERT INTO ${desc.table} (${columns.join(', ')}) VALUES (${placeholders}) ON CONFLICT (${desc.onConflict.join(', ')}) DO UPDATE SET ${updateClauses.join(', ')}`;
    }
  } else {
    sql = `INSERT INTO ${desc.table} (${columns.join(', ')}) VALUES (${placeholders})`;
  }

  if (desc.returning !== undefined) sql += ` RETURNING ${desc.returning.join(', ')}`;

  return { component: 'Insert', sql, where: null, params, assembly: assembly('items') };
}

/** Compile an UPDATE. SET params (static) precede the `{where}` splice (spec §6). */
export function compileUpdate(desc: UpdateDesc): CompiledOperation {
  const setCols = Object.keys(desc.set);
  if (setCols.length === 0) throw new Error('UPDATE requires at least one SET column');
  const setClauses = setCols.map((c) => `${c} = ?`);
  const params: ExprNode[] = setCols.map((c) => desc.set[c]);

  const where = lowerWhere(desc.where);
  if (where === null) throw new Error('UPDATE requires conditions');

  // The ` WHERE ` keyword is emitted by render() (it prepends ` WHERE ` when the fragment
  // tree renders non-empty). Splice the marker directly after the SET clause.
  let sql = `UPDATE ${desc.table} SET ${setClauses.join(', ')}${WHERE_SLOT}`;
  if (desc.returning !== undefined) sql += ` RETURNING ${desc.returning.join(', ')}`;

  return { component: 'Update', sql, where, params, assembly: assembly('items') };
}

/** Compile a DELETE. No static params before the WHERE (all params are conditions). */
export function compileDelete(desc: DeleteDesc): CompiledOperation {
  const where = lowerWhere(desc.where);
  if (where === null) throw new Error('DELETE requires conditions');

  let sql = `DELETE FROM ${desc.table}${WHERE_SLOT}`;
  if (desc.returning !== undefined) sql += ` RETURNING ${desc.returning.join(', ')}`;

  return { component: 'Delete', sql, where, params: [], assembly: assembly('items') };
}
