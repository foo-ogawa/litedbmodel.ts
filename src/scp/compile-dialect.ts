/**
 * litedbmodel v2 SCP — dialect-parameterized Backend Compile (WS6, #26; spec §4/§5/§8/§10).
 *
 * The SAME Component-graph IR compiles to SQLite, Postgres and MySQL SQL. The IR→structure
 * lowering (SELECT/INSERT/UPDATE/DELETE skeletons, the WHERE fragment tree, `col = ?`, IN-list
 * `(?)` expansion, param slots) is SHARED across dialects; only the genuine dialect divergences
 * (INSERT conflict clause + INSERT verb) are delegated to the {@link Dialect} strategy (the SSoT
 * — no scattered `?:`). Placeholders stay `?` through compilation; the PG `?`→`$N` conversion is
 * a final render-time one-pass ({@link import('./dialect').Dialect.finalizePlaceholders}), NOT a
 * compile-time concern — so the compiled `CompiledOperation.sql` is dialect-tagged but still
 * `?`-placeheld, keeping the bundle uniform.
 *
 * `compile-sqlite.ts` delegates its INSERT compile here with the SQLite dialect, so the pinned
 * SQLite golden (WS1) is preserved byte-for-byte and the PG/MySQL variants diverge ONLY where the
 * v1 SqlBuilders diverge (proven byte-identical in the WS6 dialect-golden test).
 */

import type { CompiledOperation, ExprNode } from './ir';
import type { Dialect, ConflictAction } from './dialect';

// The write-value description (shared with compile-sqlite's InsertDesc, re-declared minimally so
// this module has no import cycle with compile-sqlite).

/** Column → value Expression IR (insertion order = SQL column order). */
export interface InsertShape {
  readonly table: string;
  readonly values: Record<string, ExprNode>;
  readonly returning?: readonly string[];
  readonly onConflict?: readonly string[];
  readonly onConflictAction?: 'ignore' | { readonly updateColumns: 'all' | readonly string[] };
}

/** Row → logical model assembly (shape only). */
const assembly = (shape: string) => ({ shape });

/**
 * Compile an INSERT for a given dialect. Byte-identical to the corresponding v1 SqlBuilder's
 * single-record `buildInsert` output (the golden bar):
 *   - SQLite: `INSERT [OR IGNORE] INTO t (c) VALUES (?)[ ON CONFLICT (k) DO UPDATE SET c = excluded.c][ RETURNING r]`
 *   - Postgres: `INSERT INTO t (c) VALUES (?)[ ON CONFLICT (k) DO NOTHING | DO UPDATE SET c = EXCLUDED.c][ RETURNING r]`
 *   - MySQL: `INSERT [IGNORE] INTO t (c) VALUES (?)[ ON DUPLICATE KEY UPDATE c = VALUES(c)]`
 *
 * Placeholders remain `?`; PG's `?`→`$N` is applied at render time.
 */
export function compileInsertFor(dialect: Dialect, desc: InsertShape): CompiledOperation {
  const columns = Object.keys(desc.values);
  if (columns.length === 0) throw new Error('INSERT requires at least one value column');
  const placeholders = columns.map(() => '?').join(', ');
  const params: ExprNode[] = columns.map((c) => desc.values[c]);

  const conflictAction = resolveConflictAction(desc, columns);
  const verb = dialect.insertVerb(conflictAction);
  let sql = `${verb} ${desc.table} (${columns.join(', ')}) VALUES (${placeholders})`;

  if (conflictAction !== undefined) {
    sql += dialect.insertConflictClause(desc.onConflict ?? [], conflictAction);
  }

  if (desc.returning !== undefined) sql += ` RETURNING ${desc.returning.join(', ')}`;

  return { component: 'Insert', sql, where: null, params, assembly: assembly('items') };
}

/**
 * Resolve the dialect-neutral {@link ConflictAction} from the INSERT description. An `'all'`
 * update-column list is expanded to the concrete inserted columns EXCEPT the conflict keys
 * (matching the v1 SqlBuilders' `onConflictUpdate === 'all' ? columns : …`, which include all
 * columns; the v1 builders use the full `columns` list for `'all'`, so we mirror that exactly).
 * Returns undefined when the INSERT declares no conflict handling.
 */
function resolveConflictAction(desc: InsertShape, columns: readonly string[]): ConflictAction | undefined {
  if (desc.onConflict === undefined || desc.onConflictAction === undefined) return undefined;
  if (desc.onConflictAction === 'ignore') return 'ignore';
  const cols = desc.onConflictAction.updateColumns === 'all' ? columns : desc.onConflictAction.updateColumns;
  return { updateColumns: cols };
}
