/**
 * litedbmodel v2 型システム（SCP アーキ spec §4.1）: SQL 型 → bc outType スカラの正規変換。
 *
 * litedbmodel は SQL バックエンド consumer なので **SQL 型を型の SoT** とする（TS の `number` は
 * INTEGER/REAL を区別できないため型の権威にしない）。typed codegen（bc typed-raw 脱box）の `outType`
 * 注記はこの変換で決まる。**曖昧/未知は error（no-assume・no-fallback）** — 既定値へ潰さない。
 *
 * 返すのは bc の scalar 型名（`typed.ts` の scalar union `"string"|"int"|"float"|"bool"|"null"`）。
 * 行/リレーションの構造型（`obj`/`arr`/`opt`）は列型ではなくクエリ由来なので本変換の対象外。
 */

/** bc typed outType のスカラ型（bc 0.3.0 の portable notation と一致；date 型は無い）。 */
export type BcScalar = 'string' | 'int' | 'float' | 'bool' | 'null';

/**
 * SQL 型（DDL のトークン。`DECIMAL(10,2)` 等の括弧やサイズは無視）→ bc outType スカラ。
 * spec §4.1 の正規表に一致。未知/曖昧は throw（fail-closed）。
 */
export function sqlTypeToBcScalar(sqlType: string): BcScalar {
  // 括弧のサイズ/精度・`UNSIGNED` 等の修飾を落として基底型名で判定。
  const t = sqlType
    .trim()
    .toUpperCase()
    .replace(/\(.*\)/, '')
    .replace(/\b(UNSIGNED|ZEROFILL|PRECISION)\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  switch (t) {
    // int は既定 64bit(i64)。INTEGER も BIGINT も int（bigint を別型にしない — サイズ制限は列制約で表現）。
    case 'INTEGER':
    case 'INT':
    case 'BIGINT':
    case 'SMALLINT':
    case 'TINYINT':
    case 'MEDIUMINT':
    case 'INT2':
    case 'INT4':
    case 'INT8':
      return 'int';
    // int と real は明確に分離。
    case 'REAL':
    case 'FLOAT':
    case 'DOUBLE':
    case 'FLOAT4':
    case 'FLOAT8':
      return 'float';
    // decimal は精度保持のため文字列表現。
    case 'DECIMAL':
    case 'NUMERIC':
    case 'MONEY':
      return 'string';
    case 'TEXT':
    case 'VARCHAR':
    case 'CHAR':
    case 'CHARACTER':
    case 'CHARACTER VARYING':
    case 'CLOB':
    case 'UUID':
      return 'string';
    case 'BOOLEAN':
    case 'BOOL':
      return 'bool';
    // date/timestamp は一旦 string（spec §4.1 owner 決定）。bc に date scalar が無く（behavior-contracts#84
    // は defer）、DB は行ごとに別 TZ を保存しないので、文字列表現で往復させる。将来 bc date 型導入時に最適化。
    case 'DATE':
    case 'TIMESTAMP':
    case 'TIMESTAMPTZ':
    case 'DATETIME':
    case 'TIME':
      return 'string';
    // JSON 列の表現は JSON テキスト＝文字列（TS のみ利便で object へ de/serialize。列を typed obj 化しない）。
    case 'JSON':
    case 'JSONB':
      return 'string';
    default:
      throw new Error(
        `litedbmodel type system (spec §4.1): no bc outType mapping for SQL type '${sqlType}' ` +
          `(normalized '${t}'). Add it to the §4.1 table or fix the column — ambiguous/unknown types ` +
          `are a hard error (no-assume, no-fallback), never defaulted.`,
      );
  }
}

/**
 * The TS read-path MATERIALIZATION class for a SQL column (issue #59, owner-approved
 * type-honoring de-box). The bc `outType` scalar (`int`/`float`/`string`/`bool`) is the PORTABLE
 * type the native codegen materializes; but on the TS/driver read path we additionally split the
 * `int` scalar by SQL WIDTH and coerce `date`/`bool` to their exact JS form, because a JS `number`
 * cannot hold an i64 and the drivers otherwise return a JS `Date` / `0|1` that violate the
 * declared `string`/`bool` outType. A materialization class is:
 *
 *   - `'int32'`  — 32-bit int family (INT/INTEGER/SMALLINT/TINYINT/MEDIUMINT/INT2/INT4). Its full
 *                  range fits in a JS `number` exactly, so it materializes to `number` (fast,
 *                  arith/JSON-friendly). This is the UNCHANGED, already-correct behavior.
 *   - `'int64'`  — 64-bit int (BIGINT/INT8). Exceeds `Number.MAX_SAFE_INTEGER`, so a JS `number`
 *                  would round it. Materializes to a value-preserving decimal STRING (e.g.
 *                  `"9223372036854775807"`) — EXACT and JSON-safe (a JS `bigint` throws in
 *                  `JSON.stringify`, so string is the JSON-friendly exact form, mirroring the
 *                  decimal→string / date→string mappings). This is the ONLY int class whose JS
 *                  type changes (#59 fix).
 *   - `'date'`   — DATE/TIMESTAMP/TIMESTAMPTZ/DATETIME/TIME. The bc outType is `string`; drivers
 *                  return a JS `Date` (TZ-shifted). Materializes to a TZ-attached STRING (the
 *                  driver's textual form), honoring the `string` outType (#59 fix; overrides the
 *                  old v2 "date→JS Date on TS" mapping).
 *   - `'bool'`   — BOOLEAN/BOOL. Drivers may return `0`/`1`; materializes to a JS `boolean`.
 *   - `'passthrough'` — every other class (float, text, decimal-as-string, json, uuid): the driver
 *                  value already matches the outType, so no coercion.
 */
export type MaterializeClass = 'int32' | 'int64' | 'date' | 'bool' | 'passthrough';

/**
 * Derive the TS read-path {@link MaterializeClass} for a SQL type (spec §4.1 owner de-box). Uses
 * the SAME normalization + closed-set discipline as {@link sqlTypeToBcScalar} (unknown ⇒ throw),
 * so a column that types for the bc scalar also types for materialization (and vice-versa). The
 * `int` scalar splits int32/int64 by width; `date`/`bool` get their own class; everything else is
 * `passthrough`.
 */
export function sqlTypeToMaterializeClass(sqlType: string): MaterializeClass {
  const t = sqlType
    .trim()
    .toUpperCase()
    .replace(/\(.*\)/, '')
    .replace(/\b(UNSIGNED|ZEROFILL|PRECISION)\b/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  switch (t) {
    // 32-bit int family: JS number holds the full range exactly.
    case 'INTEGER':
    case 'INT':
    case 'SMALLINT':
    case 'TINYINT':
    case 'MEDIUMINT':
    case 'INT2':
    case 'INT4':
      return 'int32';
    // 64-bit int: needs JS bigint for exactness.
    case 'BIGINT':
    case 'INT8':
      return 'int64';
    case 'BOOLEAN':
    case 'BOOL':
      return 'bool';
    case 'DATE':
    case 'TIMESTAMP':
    case 'TIMESTAMPTZ':
    case 'DATETIME':
    case 'TIME':
      return 'date';
    // float / decimal(→string) / text / json / uuid — driver value already matches the outType.
    case 'REAL':
    case 'FLOAT':
    case 'DOUBLE':
    case 'FLOAT4':
    case 'FLOAT8':
    case 'DECIMAL':
    case 'NUMERIC':
    case 'MONEY':
    case 'TEXT':
    case 'VARCHAR':
    case 'CHAR':
    case 'CHARACTER':
    case 'CHARACTER VARYING':
    case 'CLOB':
    case 'UUID':
    case 'JSON':
    case 'JSONB':
      return 'passthrough';
    default:
      throw new Error(
        `litedbmodel type system (spec §4.1): no materialization class for SQL type '${sqlType}' ` +
          `(normalized '${t}'). Add it to the §4.1 table or fix the column — ambiguous/unknown types ` +
          `are a hard error (no-assume, no-fallback), never defaulted.`,
      );
  }
}

/**
 * Coerce ONE raw driver cell to the JS form its {@link MaterializeClass} declares (issue #59 TS
 * read-path de-box). NULL passes through (nullable columns). This is driver-agnostic: it accepts
 * whatever form each driver returned for the class and normalizes it —
 *   - `int64`: a string (pg int8 / mysql2 bigNumberStrings), a bigint (better-sqlite3 safeIntegers),
 *              or a safe JS number → an EXACT decimal STRING (JSON-safe, value-preserving). An
 *              already-rounded unsafe JS number is a hard error (precision was lost upstream).
 *   - `int32`: a bigint (a driver in safe-integer mode) or string → `number`; a number stays.
 *   - `date` : a JS Date → its textual form; a string stays (already the textual form).
 *   - `bool` : `0`/`1`/`0n`/`1n` → boolean; a boolean stays.
 *   - `passthrough`: unchanged.
 */
export function materializeCell(value: unknown, klass: MaterializeClass): unknown {
  if (value === null || value === undefined) return value;
  switch (klass) {
    case 'int64': {
      // BIGINT → a value-preserving decimal STRING (exact + JSON-safe; a JS bigint throws in
      // JSON.stringify, a JS number rounds past 2^53). Accept each driver's exact form.
      if (typeof value === 'bigint') return value.toString();
      if (typeof value === 'string') {
        if (!/^-?\d+$/.test(value)) throw new Error(`materialize int64: driver returned a non-integer string '${value}'`);
        return value; // already the exact decimal string (pg int8 / mysql2 bigNumberStrings)
      }
      if (typeof value === 'number') {
        if (!Number.isInteger(value)) throw new Error(`materialize int64: driver returned a non-integer number ${value} for a 64-bit int column`);
        // A JS number past 2^53 already lost precision at the driver boundary; that is exactly the
        // hole this de-box closes, so a number here means the driver was NOT put in exact mode.
        if (!Number.isSafeInteger(value)) {
          throw new Error(
            `materialize int64: driver returned an UNSAFE JS number ${value} for a 64-bit int column — ` +
              `precision was already lost before materialization (the driver must return int8 as string/bigint, ` +
              `not a rounded double). Configure the driver (better-sqlite3 safeIntegers / mysql2 supportBigNumbers+bigNumberStrings / pg int8-as-string).`,
          );
        }
        return value.toString(); // a small BIGINT value that fit safely — stringify for a uniform exact string
      }
      throw new Error(`materialize int64: unexpected driver JS type ${typeof value}`);
    }
    case 'int32': {
      if (typeof value === 'number') return value;
      if (typeof value === 'bigint') return Number(value);
      if (typeof value === 'string' && /^-?\d+$/.test(value)) return Number(value);
      throw new Error(`materialize int32: unexpected driver JS type ${typeof value} (${String(value)})`);
    }
    case 'date': {
      if (typeof value === 'string') return value;
      if (value instanceof Date) return dateToTzString(value);
      throw new Error(`materialize date: unexpected driver JS type ${typeof value}`);
    }
    case 'bool': {
      if (typeof value === 'boolean') return value;
      if (typeof value === 'number') return value !== 0;
      if (typeof value === 'bigint') return value !== 0n;
      throw new Error(`materialize bool: unexpected driver JS type ${typeof value} (${String(value)})`);
    }
    case 'passthrough':
      return value;
  }
}

/**
 * Render a JS `Date` (a driver that was NOT configured for date-as-string) to a TZ-attached
 * string. A `Date` has no calendar-vs-timestamp distinction, so we emit the full ISO-8601 instant
 * (UTC, `Z`-suffixed) — a lossless, TZ-carrying textual form. The PREFERRED path is to configure
 * the driver to return the native textual form directly (pg `setTypeParser`, mysql2 `dateStrings`);
 * this fallback guarantees a STRING is returned even if a driver slips a Date through.
 */
function dateToTzString(d: Date): string {
  return d.toISOString();
}

/**
 * A column-type resolver: `(table, column) → SQL type token`. It is the SoT (spec §4.1: the
 * `schema.sql` DDL) a codegen `outType` derivation consults to type each SELECT projection column.
 * MUST throw (never return a default) when a `table.column` is unknown — an unmappable column is a
 * hard error (no-assume, no-fallback), never a silent boxed fallback.
 */
export type ColumnTypeResolver = (table: string, column: string) => string;

/**
 * A resolver that yields the TS read-path {@link MaterializeClass} of a `(table, column)`, or
 * `undefined` when the column's type is unknown/untypeable. Unlike {@link ColumnTypeResolver} this
 * is TOLERANT (never throws for an unknown column) — it drives the ALWAYS-ON production read-path
 * materialization (issue #59), which must degrade to leaving an untyped column as the raw driver
 * value rather than failing a real read. It is derived automatically from the DB's own schema (the
 * live column-type SoT), so it fires for EVERY production read with no caller-supplied resolver.
 */
export type MaterializeResolver = (table: string, column: string) => MaterializeClass | undefined;

/**
 * The minimal SQLite introspection surface: `PRAGMA table_info(<t>)` → `{name, type}[]`. Satisfied
 * by better-sqlite3's `Database` (`prepare(sql).all()`), so the production read entry points derive
 * the materialize resolver straight from the live connection — no caller-supplied schema.
 */
export interface SqliteSchemaIntrospector {
  prepare(sql: string): { all(...params: unknown[]): unknown[] };
}

/**
 * Build a DB-introspecting {@link MaterializeResolver} for SQLite from a live connection (issue
 * #59, ALWAYS-ON read de-box). It reads each table's column types via `PRAGMA table_info` (memoized
 * per table) — the DB IS the column-type SoT, so materialization fires for every real read with no
 * caller-supplied resolver. A column whose SQL type does not map to a known class (or a table that
 * cannot be introspected) resolves to `undefined` (tolerant → the raw driver value is kept, never a
 * thrown error on a production read).
 */
export function sqliteMaterializeResolver(db: SqliteSchemaIntrospector): MaterializeResolver {
  const cache = new Map<string, Map<string, MaterializeClass | undefined>>();
  const columnsOf = (table: string): Map<string, MaterializeClass | undefined> => {
    let cols = cache.get(table);
    if (cols !== undefined) return cols;
    cols = new Map();
    try {
      // `PRAGMA table_info(?)` does not bind a param; the table name is a bare identifier. It comes
      // from the compiled read graph (a literal projection table), not user input — quote-escape it.
      const rows = db.prepare(`PRAGMA table_info("${table.replace(/"/g, '""')}")`).all() as { name?: unknown; type?: unknown }[];
      for (const r of rows) {
        if (typeof r.name !== 'string' || typeof r.type !== 'string') continue;
        cols.set(r.name, materializeClassOrUndefined(r.type));
      }
    } catch {
      // Un-introspectable (e.g. a mock driver / a view): leave the map empty → all passthrough.
    }
    cache.set(table, cols);
    return cols;
  };
  return (table, column) => columnsOf(table).get(column);
}

/**
 * A `table → (column → SQL type)` map (e.g. from `information_schema.columns`, the async PG/MySQL
 * introspection) → a {@link MaterializeResolver}. Tolerant: an unknown `(table, column)` or an
 * un-mappable SQL type resolves to `undefined` (the raw driver value is kept). The async production
 * read path introspects the live DB once, builds this map, and passes it in — so materialization
 * fires with no caller-supplied resolver.
 */
export function materializeResolverFromColumnMap(
  columnTypes: ReadonlyMap<string, ReadonlyMap<string, string>>,
): MaterializeResolver {
  return (table, column) => {
    const sqlType = columnTypes.get(table)?.get(column);
    return sqlType === undefined ? undefined : materializeClassOrUndefined(sqlType);
  };
}

/** {@link sqlTypeToMaterializeClass} but TOLERANT: returns `undefined` for an unknown SQL type. */
export function materializeClassOrUndefined(sqlType: string): MaterializeClass | undefined {
  try {
    return sqlTypeToMaterializeClass(sqlType);
  } catch {
    return undefined;
  }
}

/** An async `(sql, params) → rows` executor (the pooled PG/MySQL seam) — for async introspection. */
export type AsyncQuery = (sql: string, params: unknown[]) => Promise<Record<string, unknown>[]>;

/**
 * Introspect the LIVE PG / MySQL schema via `information_schema.columns` (the async production
 * column-type SoT) for the given tables, and build a DB-derived {@link MaterializeResolver} (issue
 * #59, ALWAYS-ON read de-box). Runs ONE query bounded to `tables`, so `executeBehaviorAsync`
 * materializes with NO caller-supplied resolver. Tolerant: a table/column not found, or a type that
 * does not map, resolves to `undefined` (raw driver value kept — a real read never fails on it).
 *
 * `information_schema.columns.data_type` gives the canonical base type (`bigint`, `integer`,
 * `boolean`, `date`, `timestamp without time zone`, `numeric`, `double precision`, `character
 * varying`, `json`/`jsonb`, …) on BOTH PG and MySQL. We normalize it to the token
 * {@link sqlTypeToMaterializeClass} understands (strip the PG `without/with time zone` suffix →
 * `TIMESTAMP`/`TIMESTAMPTZ`; `character varying` → `VARCHAR`; `double precision` handled already).
 */
export async function asyncMaterializeResolver(
  query: AsyncQuery,
  tables: readonly string[],
): Promise<MaterializeResolver> {
  const columnTypes = new Map<string, Map<string, MaterializeClass | undefined>>();
  const uniqueTables = [...new Set(tables)];
  if (uniqueTables.length > 0) {
    const placeholders = uniqueTables.map((_, i) => `$${i + 1}`).join(', ');
    // `information_schema` is ANSI on PG; MySQL supports it too but uses `?` placeholders, so the
    // caller's async executor renders the dialect placeholder form — we pass a `$N` template and let
    // the pooled executor bind positionally (both drivers accept a positional param list). To stay
    // driver-neutral we instead fetch ALL columns of the named tables with an IN list.
    // Select `column_type` too: MySQL reports a BOOLEAN as `data_type=tinyint` but
    // `column_type=tinyint(1)` — the ONLY way to tell a boolean from a real 1-byte int. PG has no
    // `column_type` column, so we tolerate its absence (COALESCE-free — just read whichever is present).
    // Try, in order: MySQL-form (with `column_type`, `?` placeholders) → PG-form (with
    // `column_type`, `$N`) → PG-form WITHOUT `column_type` (PG's information_schema has no such
    // column) with `$N`. The MySQL `column_type` distinguishes `tinyint(1)`=BOOLEAN. Each form is
    // tried independently so a placeholder/column mismatch on one driver falls through to the next
    // rather than silently dropping `column_type` (the bug that let MySQL BOOLEAN read as int).
    const ph$ = placeholders;
    const phQ = uniqueTables.map(() => '?').join(', ');
    const withCt = (p: string): string => `SELECT table_name AS t, column_name AS c, data_type AS dt, column_type AS ct FROM information_schema.columns WHERE table_name IN (${p})`;
    const noCt = (p: string): string => `SELECT table_name AS t, column_name AS c, data_type AS dt FROM information_schema.columns WHERE table_name IN (${p})`;
    let rows: Record<string, unknown>[] = [];
    for (const candidate of [withCt(phQ), withCt(ph$), noCt(ph$), noCt(phQ)]) {
      try { rows = await query(candidate, uniqueTables); break; } catch { /* try next form */ }
    }
    for (const r of rows) {
      const t = String(r.t ?? r.table_name ?? '');
      const c = String(r.c ?? r.column_name ?? '');
      const dt = String(r.dt ?? r.data_type ?? '');
      const ct = r.ct ?? r.column_type;
      if (t === '' || c === '') continue;
      let cols = columnTypes.get(t);
      if (cols === undefined) { cols = new Map(); columnTypes.set(t, cols); }
      // MySQL `tinyint(1)` = BOOLEAN → bool. Otherwise map from the base data_type.
      const klass = typeof ct === 'string' && ct.trim().toLowerCase() === 'tinyint(1)'
        ? 'bool'
        : materializeClassOrUndefined(normalizeInfoSchemaType(dt));
      cols.set(c, klass as MaterializeClass | undefined);
    }
  }
  return (table, column) => columnTypes.get(table)?.get(column);
}

/** Normalize an `information_schema.data_type` token to the form {@link sqlTypeToMaterializeClass} maps. */
function normalizeInfoSchemaType(dataType: string): string {
  const t = dataType.trim().toLowerCase();
  if (t === 'timestamp without time zone' || t === 'timestamp') return 'TIMESTAMP';
  if (t === 'timestamp with time zone') return 'TIMESTAMPTZ';
  if (t === 'time without time zone' || t === 'time with time zone') return 'TIME';
  if (t === 'character varying') return 'VARCHAR';
  if (t === 'character') return 'CHAR';
  if (t === 'double precision') return 'DOUBLE';
  return t; // bigint / integer / smallint / boolean / date / numeric / decimal / json / jsonb / text / real …
}

/**
 * Parse a list of `CREATE TABLE` DDL statements into a `table → (column → SQL type)` map. This is
 * the spec §4.1 SoT: the physical SQL type of each column, from which `sqlTypeToBcScalar` derives
 * the bc `outType` scalar for typed codegen. Only the base column definitions are read; table-level
 * constraints (`PRIMARY KEY (…)`, `FOREIGN KEY …`, `UNIQUE (…)`, `CHECK …`) are skipped. A column's
 * type is its first token after the name (e.g. `id INTEGER PRIMARY KEY` → `INTEGER`). Column names
 * quoted with `"`/`` ` ``/`[]` are unquoted. Non-`CREATE TABLE` statements (INSERT seed rows, etc.)
 * are ignored, so a mixed schema+seed list is accepted as-is.
 */
export function parseSchemaColumnTypes(ddl: readonly string[]): Map<string, Map<string, string>> {
  const tables = new Map<string, Map<string, string>>();
  const CREATE = /^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_."`[\]]+)\s*\(([\s\S]*)\)\s*;?\s*$/i;
  const unquote = (s: string): string => s.replace(/^["`[]|["`\]]$/g, '');
  // Column/constraint list keywords that start a TABLE-LEVEL constraint (not a column definition).
  const TABLE_CONSTRAINT = /^(PRIMARY|FOREIGN|UNIQUE|CHECK|CONSTRAINT|KEY|INDEX)\b/i;
  for (const stmt of ddl) {
    const m = CREATE.exec(stmt);
    if (m === null) continue; // not a CREATE TABLE (seed INSERT / pragma / etc.) — skip
    const table = unquote(m[1].trim());
    const cols = new Map<string, string>();
    for (const entry of splitTopLevel(m[2])) {
      const line = entry.trim();
      if (line.length === 0) continue;
      if (TABLE_CONSTRAINT.test(line)) continue; // table-level constraint, not a column
      // `<name> <TYPE...> [constraints]` — the type is the token(s) up to the first constraint
      // keyword or the end. `parseColumnDef` returns `{ name, sqlType }` or undefined (unparseable).
      const parsed = parseColumnDef(line);
      if (parsed === undefined) continue;
      cols.set(parsed.name, parsed.sqlType);
    }
    tables.set(table, cols);
  }
  return tables;
}

/** Split a CREATE TABLE column list on TOP-LEVEL commas (commas inside `(…)`, e.g. `DECIMAL(10,2)`, stay). */
function splitTopLevel(body: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let cur = '';
  for (const ch of body) {
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    if (ch === ',' && depth === 0) {
      parts.push(cur);
      cur = '';
    } else cur += ch;
  }
  if (cur.trim().length > 0) parts.push(cur);
  return parts;
}

/** The SQL-type keywords that may form a multi-word type (so `CHARACTER VARYING`, `DOUBLE PRECISION` stay whole). */
const TYPE_CONTINUATION = /^(VARYING|PRECISION)$/i;

/** Parse one `<name> <TYPE...>` column definition into `{ name, sqlType }`. Undefined when unparseable. */
function parseColumnDef(line: string): { name: string; sqlType: string } | undefined {
  const unquote = (s: string): string => s.replace(/^["`[]|["`\]]$/g, '');
  const nameMatch = /^([A-Za-z0-9_."`[\]]+)\s+(.*)$/.exec(line);
  if (nameMatch === null) return undefined;
  const name = unquote(nameMatch[1]);
  const rest = nameMatch[2].trim();
  // The type is the leading token plus any `(...)` size/precision, plus a continuation word
  // (`CHARACTER VARYING`, `DOUBLE PRECISION`). Everything after is column constraints.
  const typeMatch = /^([A-Za-z0-9_]+(?:\s*\([^)]*\))?)\s*(.*)$/.exec(rest);
  if (typeMatch === null) return undefined;
  let sqlType = typeMatch[1].trim();
  const after = typeMatch[2].trim();
  const nextWord = /^([A-Za-z]+)/.exec(after);
  if (nextWord !== null && TYPE_CONTINUATION.test(nextWord[1])) sqlType = `${sqlType} ${nextWord[1]}`;
  if (sqlType.length === 0) return undefined;
  return { name, sqlType };
}

/**
 * Build a {@link ColumnTypeResolver} from CREATE TABLE DDL (spec §4.1 SoT). The resolver throws
 * (no default) for any `table.column` the DDL does not declare — an unknown column is a hard error
 * (no-assume, no-fallback), so a codegen `outType` derivation can never silently box an untyped row.
 */
export function schemaColumnTypeResolver(ddl: readonly string[]): ColumnTypeResolver {
  const tables = parseSchemaColumnTypes(ddl);
  return (table: string, column: string): string => {
    const cols = tables.get(table);
    if (cols === undefined) {
      throw new Error(
        `litedbmodel type system (spec §4.1): no schema for table '${table}' — cannot type its ` +
          `SELECT projection for typed codegen. The DDL (schema.sql SoT) must declare the table ` +
          `(known: ${[...tables.keys()].join(', ') || '<none>'}). No-assume, no-fallback.`,
      );
    }
    const sqlType = cols.get(column);
    if (sqlType === undefined) {
      throw new Error(
        `litedbmodel type system (spec §4.1): column '${column}' not declared on table '${table}' ` +
          `(known columns: ${[...cols.keys()].join(', ')}). A projected column with no schema type ` +
          `cannot be typed for de-boxed codegen. No-assume, no-fallback.`,
      );
    }
    return sqlType;
  };
}
