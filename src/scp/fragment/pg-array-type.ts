/**
 * PostgreSQL array-type inference for compile-time cast text.
 *
 * VERBATIM reuse of the ORIGINAL `LazyRelation.inferPgArrayType` logic so the
 * `?::<type>[]` cast text a fragment carries is byte-identical to the tuned
 * builder. sqlCast (from the target column's metadata) wins; otherwise the element
 * type is inferred from the compile-time sample values.
 *
 * (This is TEXT chosen at build time; it is NOT an IR "kind". The array itself is
 * bound as ONE param on PostgreSQL.)
 */
export function inferPgArrayTypeForCompile(values: unknown[], sqlCast?: string): string {
  if (sqlCast) return `${sqlCast}[]`;
  if (values.length === 0) return 'text[]';
  const sample = values[0];
  if (typeof sample === 'number') {
    if (values.every((v) => Number.isInteger(v))) return 'int[]';
    return 'numeric[]';
  }
  if (typeof sample === 'bigint') return 'bigint[]';
  if (typeof sample === 'boolean') return 'boolean[]';
  if (sample instanceof Date) return 'timestamp[]';
  return 'text[]';
}
