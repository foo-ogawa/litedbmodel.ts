/**
 * litedbmodel v2 SCP — the MINIMAL fragment model (epic #43 reset).
 *
 * Public surface: the fragment types, the compile helpers (build-time TS, reuse the
 * original tuned builders), and the runtime assembler (thin, language-portable).
 *
 * There is NO abstract IR here. The portable artifact is ONLY a combination of
 * `{ sql, params, skip }` fragments, with params being either value-specs or nested
 * fragments. SQL structure (`= ANY`, `CROSS JOIN LATERAL`, `UNNEST`, subquery, cast)
 * is TEXT inside `sql`, never a modeled construct.
 */

export * from './model';
export * from './assemble';
export * from './compile-pg';
export { inferPgArrayTypeForCompile } from './pg-array-type';
