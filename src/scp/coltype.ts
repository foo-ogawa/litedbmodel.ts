/**
 * litedbmodel v2 型システム（SCP アーキ spec §4.1）: SQL 型 → bc outType スカラの正規変換。
 *
 * litedbmodel は SQL バックエンド consumer なので **SQL 型を型の SoT** とする（TS の `number` は
 * INTEGER/REAL を区別できないため型の権威にしない）。typed codegen（bc typed-raw 脱box）の `outType`
 * 注記はこの変換で決まる。**曖昧/未知は error（no-assume・no-fallback）** — 既定値へ潰さない。
 *
 * 返すのは bc の scalar 型名（`typed.ts` の scalar union `"string"|"int"|"float"|"bool"|"null"|"date"`）。
 * 行/リレーションの構造型（`obj`/`arr`/`opt`）は列型ではなくクエリ由来なので本変換の対象外。
 */

/** bc typed outType のスカラ型（`date` は behavior-contracts#84 で新設）。 */
export type BcScalar = 'string' | 'int' | 'float' | 'bool' | 'null' | 'date';

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
    // date は bc の date scalar（behavior-contracts#84）。string に潰さない。
    case 'DATE':
    case 'TIMESTAMP':
    case 'TIMESTAMPTZ':
    case 'DATETIME':
    case 'TIME':
      return 'date';
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
