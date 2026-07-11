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
 * A column-type resolver: `(table, column) → SQL type token`. It is the SoT (spec §4.1: the
 * `schema.sql` DDL) a codegen `outType` derivation consults to type each SELECT projection column.
 * MUST throw (never return a default) when a `table.column` is unknown — an unmappable column is a
 * hard error (no-assume, no-fallback), never a silent boxed fallback.
 */
export type ColumnTypeResolver = (table: string, column: string) => string;

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
