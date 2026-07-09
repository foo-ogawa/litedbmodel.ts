/**
 * litedbmodel v2 SCP — dialect strategy table (WS6, #26; spec §4/§5/§8/§10).
 *
 * The SINGLE SOURCE OF TRUTH for every SQL-dialect difference the Backend Compile + render
 * pipeline needs. The dialect axis is compiled ONCE, TS-side (spec §10: "方言軸はコンパイル時
 * に TS 側で1回検証"). This module encodes the differences as a closed strategy record — NO
 * scattered `?:` in the engine code (the hard rule). A backend compile / renderer asks the
 * {@link Dialect} for the dialect-specific text and never branches on the dialect name inline.
 *
 * ## What actually differs (verified against the v1 SqlBuilders — the golden bar)
 *
 * For the CRUD + write-tx + relation SQL the SCP Backend Compile emits (single-row VALUES,
 * `?` placeholders, `col = ?` WHERE, `col IN (?, …)`), the dialect surface is small and
 * closed:
 *
 *   - **INSERT conflict clause**: SQLite `INSERT OR IGNORE … ` / `… ON CONFLICT (cols) DO
 *     UPDATE SET c = excluded.c`; Postgres `… ON CONFLICT (cols) DO NOTHING` / `… DO UPDATE
 *     SET c = EXCLUDED.c`; MySQL `INSERT IGNORE …` / `… ON DUPLICATE KEY UPDATE c = VALUES(c)`.
 *     These are byte-identical to `SqliteSqlBuilder` / `PostgresSqlBuilder` / `MysqlSqlBuilder`
 *     `buildInsert` for the single-record case (see the WS6 dialect-golden test).
 *   - **Bare `ON CONFLICT DO NOTHING`** (no column list — the gate-first idempotency/unique
 *     guard INSERTs of the write-tx plan, spec §6): SQLite/Postgres emit `ON CONFLICT DO
 *     NOTHING`; MySQL emits `INSERT IGNORE`.
 *   - **Placeholder style**: all dialects author with `?`; Postgres converts `?`→`$1,$2,…` in a
 *     final left-to-right one-pass AFTER the SQL text is fully assembled (spec §8 — the
 *     number-reassignment problem is designed out because the pass runs once over the final
 *     flat text). SQLite/MySQL keep `?`.
 *
 * Everything else (SELECT/UPDATE/DELETE skeletons, `col = ?`, `col IN (?, …)` expansion, the
 * fragment tree, RETURNING for SQLite/Postgres) is identical text across dialects, so it is
 * NOT part of the strategy — only the genuine divergences live here.
 *
 * ## DB behavior differences beyond SQL text (spec §13) — closeable-by-convention line
 *
 * SQL-text identity does not guarantee identical DB behavior (NULL ordering, collation,
 * timezone). Per spec §13, those are closed "by dialect-compile-time convention" only within
 * the closeable line. WS6 encodes the ONE convention that is mechanically closeable in the
 * compiled SQL text: deterministic NULL ordering for an `ORDER BY … NULLS` requirement
 * ({@link Dialect.orderByNulls}). Collation / timezone / float semantics are OUT OF SCOPE
 * (documented, not silently defaulted).
 */

/** The SQL dialects the Backend Compile targets (spec §4 breadth: PG/MySQL/SQLite). */
export type DialectName = 'sqlite' | 'postgres' | 'mysql';

/** An INSERT conflict action (dialect-neutral intent; the dialect renders the clause). */
export type ConflictAction = 'ignore' | { readonly updateColumns: readonly string[] };

/**
 * A dialect strategy: the closed set of dialect-specific renderings the Backend Compile +
 * render pipeline consumes. One frozen record per dialect (the SSoT). Every method is a pure
 * text producer — no side effects, no hidden fallback.
 */
export interface Dialect {
  readonly name: DialectName;

  /**
   * Render an INSERT's optional conflict clause, appended after `… VALUES (…)`.
   *
   * @param conflictColumns the conflict-target columns (`ON CONFLICT (a, b)`), or an empty
   *   array for a bare conflict target (the write-tx guard INSERTs — spec §6).
   * @param action `'ignore'` (do-nothing) or an upsert with the update-column list.
   * @param insertColumns the INSERT's column list (needed for the `VALUES(col)` / `EXCLUDED.col`
   *   SET-clause rendering when `updateColumns` is `'all'` at the call site — the caller passes
   *   the resolved list, so this only needs the resolved update columns).
   */
  insertConflictClause(conflictColumns: readonly string[], action: ConflictAction): string;

  /**
   * For MySQL, an `ignore` conflict is expressed on the INSERT VERB (`INSERT IGNORE INTO`),
   * not as a trailing clause; SQLite uses `INSERT OR IGNORE INTO`; Postgres uses a plain
   * `INSERT INTO` + a trailing `ON CONFLICT … DO NOTHING`. This returns the INSERT verb+prefix
   * up to and including `INTO` for the `ignore` case, and the plain `INSERT INTO` otherwise.
   */
  insertVerb(action: ConflictAction | undefined): string;

  /**
   * Whether this dialect's conflict `ignore` is a trailing clause (Postgres) rather than an
   * INSERT-verb modifier (SQLite `OR IGNORE`, MySQL `IGNORE`). Drives whether
   * {@link insertConflictClause} emits text for the `ignore` case.
   */
  readonly ignoreIsTrailingClause: boolean;

  /**
   * Render a bare do-nothing GUARD INSERT — the gate-first idempotency/unique guard of the
   * write-tx plan (spec §6): `INSERT INTO <t> (<cols>) VALUES (<ph>)` + a do-nothing conflict
   * on ANY unique constraint (no column target). SQLite/Postgres emit a trailing
   * `… ON CONFLICT DO NOTHING`; MySQL emits `INSERT IGNORE INTO … VALUES (…)`.
   *
   * This is DISTINCT from the CRUD `ignore` path ({@link insertVerb} + {@link insertConflictClause}),
   * which mirrors the v1 SqlBuilders (SQLite uses the `INSERT OR IGNORE` verb there). The guard
   * form is SCP-internal and its SQLite golden is the trailing `ON CONFLICT DO NOTHING` (WS5),
   * so the two contexts render differently ON SQLITE and are kept as separate strategy methods
   * rather than one branch.
   *
   * @param table the guard table.
   * @param columns the guard row's columns (in order).
   * @param placeholders the pre-joined placeholder list (`"?, ?, ?"`).
   */
  guardInsert(table: string, columns: readonly string[], placeholders: string): string;

  /**
   * Convert the fully-assembled, param-flattened SQL text's `?` placeholders to this dialect's
   * final placeholder style. SQLite/MySQL: identity (keep `?`). Postgres: a single left-to-right
   * pass replacing the Nth `?` with `$N` (spec §8 final one-pass). This runs ONCE over the final
   * text, so there is no number-reassignment problem.
   */
  finalizePlaceholders(sql: string): string;

  /**
   * Deterministic NULL ordering for an `ORDER BY <expr> <dir>` term when the model requires it
   * (spec §13 "`ORDER BY … NULLS` 強制"). Postgres supports `NULLS FIRST/LAST` natively; SQLite
   * (3.30+) also supports it; MySQL does NOT, so the convention is emulated with a leading
   * `<expr> IS NULL` sort key. Returns the full ORDER BY term text.
   */
  orderByNulls(expr: string, dir: 'ASC' | 'DESC', nulls: 'FIRST' | 'LAST'): string;
}

// ── `?`→`$N` final one-pass (Postgres only) ───────────────────────────────────

/**
 * Replace each `?` placeholder in a fully-assembled SQL string with `$1, $2, …` left-to-right
 * (Postgres). This is the spec §8 "最終1パス" conversion: it runs ONCE over the final flat SQL
 * text, so placeholder numbering is a plain running counter — the number-reassignment problem
 * cannot reappear (there is no per-fragment renumbering). Every `?` is a bound param position
 * (the render pipeline never emits a literal `?` inside a string literal — values are always
 * parameterized), so a naive scan is correct for the compiled surface.
 */
export function toDollarPlaceholders(sql: string): string {
  let n = 0;
  let out = '';
  for (const ch of sql) {
    if (ch === '?') {
      n += 1;
      out += `$${n}`;
    } else {
      out += ch;
    }
  }
  return out;
}

// ── Shared conflict-clause rendering (parameterized by the SET-assignment form) ─

/**
 * Render the trailing conflict clause for the "has a column target" upsert/ignore forms shared
 * by SQLite and Postgres (`ON CONFLICT (cols) DO NOTHING | DO UPDATE SET …`). MySQL overrides
 * with its `ON DUPLICATE KEY UPDATE` form. `excludedRef` produces the per-column right-hand side
 * (`excluded.c` for SQLite, `EXCLUDED.c` for Postgres).
 */
function onConflictClause(
  conflictColumns: readonly string[],
  action: ConflictAction,
  excludedRef: (col: string) => string,
): string {
  const target = conflictColumns.length > 0 ? ` (${conflictColumns.join(', ')})` : '';
  if (action === 'ignore') {
    return ` ON CONFLICT${target} DO NOTHING`;
  }
  const sets = action.updateColumns.map((c) => `${c} = ${excludedRef(c)}`);
  return ` ON CONFLICT${target} DO UPDATE SET ${sets.join(', ')}`;
}

/**
 * Render a bare do-nothing guard INSERT. `trailingDoNothing` dialects (SQLite/Postgres) emit
 * `INSERT INTO … VALUES (…) ON CONFLICT DO NOTHING`; MySQL emits `INSERT IGNORE INTO … VALUES (…)`.
 */
function guardInsertText(
  table: string,
  columns: readonly string[],
  placeholders: string,
  form: 'trailingDoNothing' | 'insertIgnore',
): string {
  const head = form === 'insertIgnore' ? 'INSERT IGNORE INTO' : 'INSERT INTO';
  const tail = form === 'trailingDoNothing' ? ' ON CONFLICT DO NOTHING' : '';
  return `${head} ${table} (${columns.join(', ')}) VALUES (${placeholders})${tail}`;
}

// ── The three frozen dialect strategies (SSoT) ─────────────────────────────────

/**
 * SQLite. `ignore` is an INSERT-verb modifier (`INSERT OR IGNORE INTO`); upsert uses
 * `ON CONFLICT (cols) DO UPDATE SET c = excluded.c`. Placeholders stay `?`. NULL ordering uses
 * the native `NULLS FIRST/LAST` (SQLite 3.30+).
 */
export const SQLITE: Dialect = Object.freeze({
  name: 'sqlite',
  ignoreIsTrailingClause: false,
  insertVerb(action: ConflictAction | undefined): string {
    return action === 'ignore' ? 'INSERT OR IGNORE INTO' : 'INSERT INTO';
  },
  insertConflictClause(conflictColumns: readonly string[], action: ConflictAction): string {
    // `ignore` is handled by the verb (`INSERT OR IGNORE`), so no trailing clause for it.
    if (action === 'ignore') return '';
    return onConflictClause(conflictColumns, action, (c) => `excluded.${c}`);
  },
  guardInsert(table: string, columns: readonly string[], placeholders: string): string {
    return guardInsertText(table, columns, placeholders, 'trailingDoNothing');
  },
  finalizePlaceholders(sql: string): string {
    return sql;
  },
  orderByNulls(expr: string, dir: 'ASC' | 'DESC', nulls: 'FIRST' | 'LAST'): string {
    return `${expr} ${dir} NULLS ${nulls}`;
  },
});

/**
 * Postgres. `ignore` is a TRAILING clause (`… ON CONFLICT (cols) DO NOTHING`); upsert uses
 * `ON CONFLICT (cols) DO UPDATE SET c = EXCLUDED.c`. Placeholders are converted `?`→`$N` in the
 * final one-pass. NULL ordering uses native `NULLS FIRST/LAST`.
 */
export const POSTGRES: Dialect = Object.freeze({
  name: 'postgres',
  ignoreIsTrailingClause: true,
  insertVerb(_action: ConflictAction | undefined): string {
    return 'INSERT INTO';
  },
  insertConflictClause(conflictColumns: readonly string[], action: ConflictAction): string {
    return onConflictClause(conflictColumns, action, (c) => `EXCLUDED.${c}`);
  },
  guardInsert(table: string, columns: readonly string[], placeholders: string): string {
    return guardInsertText(table, columns, placeholders, 'trailingDoNothing');
  },
  finalizePlaceholders(sql: string): string {
    return toDollarPlaceholders(sql);
  },
  orderByNulls(expr: string, dir: 'ASC' | 'DESC', nulls: 'FIRST' | 'LAST'): string {
    return `${expr} ${dir} NULLS ${nulls}`;
  },
});

/**
 * MySQL. `ignore` is an INSERT-verb modifier (`INSERT IGNORE INTO`); upsert uses
 * `… ON DUPLICATE KEY UPDATE c = VALUES(c)` (no conflict-column target — MySQL keys off any
 * unique index). Placeholders stay `?`. NULL ordering is emulated with a leading
 * `<expr> IS NULL` sort key (MySQL has no `NULLS FIRST/LAST`).
 */
export const MYSQL: Dialect = Object.freeze({
  name: 'mysql',
  ignoreIsTrailingClause: false,
  insertVerb(action: ConflictAction | undefined): string {
    return action === 'ignore' ? 'INSERT IGNORE INTO' : 'INSERT INTO';
  },
  insertConflictClause(_conflictColumns: readonly string[], action: ConflictAction): string {
    if (action === 'ignore') return ''; // handled by the `INSERT IGNORE` verb
    const sets = action.updateColumns.map((c) => `${c} = VALUES(${c})`);
    return ` ON DUPLICATE KEY UPDATE ${sets.join(', ')}`;
  },
  guardInsert(table: string, columns: readonly string[], placeholders: string): string {
    return guardInsertText(table, columns, placeholders, 'insertIgnore');
  },
  finalizePlaceholders(sql: string): string {
    return sql;
  },
  orderByNulls(expr: string, dir: 'ASC' | 'DESC', nulls: 'FIRST' | 'LAST'): string {
    // MySQL: emulate NULLS FIRST/LAST with a leading `IS NULL` key.
    //   NULLS FIRST → `expr IS NULL DESC, expr <dir>` (nulls sort as 1, before 0... wait)
    // In MySQL, NULL sorts LOWEST by default. `expr IS NULL` is 1 for null, 0 otherwise.
    //   NULLS FIRST: nulls must come first → order the IS-NULL flag DESC (1 before 0).
    //   NULLS LAST:  nulls must come last  → order the IS-NULL flag ASC  (0 before 1).
    const flagDir = nulls === 'FIRST' ? 'DESC' : 'ASC';
    return `${expr} IS NULL ${flagDir}, ${expr} ${dir}`;
  },
});

/** Resolve a {@link DialectName} to its frozen {@link Dialect} strategy (fail-closed). */
const DIALECTS: Record<DialectName, Dialect> = Object.freeze({
  sqlite: SQLITE,
  postgres: POSTGRES,
  mysql: MYSQL,
});

/** Look up a dialect strategy by name (throws on an unknown dialect — no silent default). */
export function dialectFor(name: DialectName): Dialect {
  const d = DIALECTS[name];
  if (d === undefined) {
    throw new Error(`scp dialect: unknown dialect '${String(name)}' (known: ${Object.keys(DIALECTS).join(', ')})`);
  }
  return d;
}
