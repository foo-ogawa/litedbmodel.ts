/**
 * litedbmodel v2 SCP — codegen `outType` derivation for WRITE bundles (spec §4.1 / §9; issues
 * #58/#56 extended to writes). The READ de-box ({@link ./outtype}) types a SELECT projection into a
 * concrete row struct; this module types the WRITE bundle's OUTPUT — a {@link TransactionResult} —
 * into the SAME de-boxable typed shape so bc's typed(-raw) emitters materialize a concrete struct
 * for the write result instead of a dynamic `Value::Obj` tree.
 *
 * ## The write output type (the TransactionResult typed shape — spec §6 / tx.ts)
 *
 * `executeTransaction` returns:
 * ```
 * { committed: boolean
 *   shortCircuit?: { statementId: string; reason: ShortCircuitReason }   // reason is a string enum
 *   entity: Record<string,unknown> | null      // the written entity ROW (RETURNING row)
 *   executed: readonly string[]
 *   returnedRows?: readonly (readonly Record<string,unknown>[])[]  // batch RETURNING rows }
 * ```
 * which de-boxes to the bc portable type
 * ```
 * obj{ committed:bool, executed:arr<string>,
 *      shortCircuit:opt<obj{statementId:string, reason:string}>,
 *      entity:opt<ROW>, returnedRows:opt<arr<arr<ROW>>> }
 * ```
 * where ROW is the written table's RETURNING columns typed via the SAME
 * {@link ColumnTypeResolver} the read de-box uses (spec §4.1 column SoT).
 *
 * ## Fail-closed (spec §4.1 discipline — no-assume, no-fallback)
 *
 * The ROW type is derived from the write's target table + its RETURNING column list, both extracted
 * STRUCTURALLY from the write bundle's transaction plan (the `body`-role statements). An un-typeable
 * write — a target table the DDL SoT does not declare, a RETURNING column with no schema type, a
 * `RETURNING *`, a computed/aliased RETURNING column, or divergent RETURNING shapes across batch
 * groups — THROWS. It is NEVER degraded to a boxed/untyped output; that would silently defeat the
 * de-box. A write with NO RETURNING anywhere types `entity`/`returnedRows` rows as an EMPTY row obj
 * (`obj{}`) — there is genuinely no row shape to de-box, and the runtime never populates those
 * fields, so the empty-obj element type is exact (not an assumed default).
 */

import type { PortableType, PortableScalarType } from 'behavior-contracts';
import { sqlTypeToBcScalar, type BcScalar, type ColumnTypeResolver } from '../coltype';
import type { SqlBundle } from '../runtime';
import type { TransactionPlan, TxStatement } from './tx';

/** Identity narrow (BcScalar is exactly bc's PortableScalarType); DATE→string happens in coltype. */
function toPortableScalar(scalar: BcScalar): PortableScalarType {
  return scalar;
}

/** The reserved ShortCircuit sub-object type — reason is a string enum, statementId a string. */
const SHORT_CIRCUIT_TYPE: PortableType = { obj: { statementId: 'string', reason: 'string' } };

/**
 * The target table + RETURNING columns of ONE `body`-role write statement, extracted STRUCTURALLY
 * from its complete tuned SQL text (the makeSQL `body` op — `INSERT INTO <t> … [RETURNING …]` /
 * `UPDATE <t> …` / `DELETE FROM <t> …`). The MySQL PK-hint comment (the `scp:pk=…` marker) is ignored
 * (it trails the RETURNING and carries no column). Returns `undefined` for a non-body / gate
 * statement's SQL that is not a base write (the caller only inspects `body`-role ops).
 */
function targetAndReturning(sql: string): { table: string; returning: string[] | null } {
  const s = sql.trim();
  const ins = /^INSERT\s+(?:IGNORE\s+)?INTO\s+([A-Za-z_][A-Za-z0-9_]*)/i.exec(s);
  const upd = /^UPDATE\s+([A-Za-z_][A-Za-z0-9_]*)/i.exec(s);
  const del = /^DELETE\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)/i.exec(s);
  const m = ins ?? upd ?? del;
  if (m === null) {
    throw new Error(
      `writeouttype: a write body statement's SQL is not a recognized INSERT/UPDATE/DELETE ` +
        `(${JSON.stringify(sql)}). The typed de-box must read the target table from the base write. No-assume, no-fallback.`,
    );
  }
  const table = m[1];
  // RETURNING columns (strip a trailing MySQL PK-hint comment first). No RETURNING → null.
  const clean = s.replace(/\s*\/\*scp:pk=[^*]*\*\//i, '');
  const ret = /\bRETURNING\s+(.+?)\s*$/i.exec(clean);
  if (ret === null) return { table, returning: null };
  const cols = ret[1].split(',').map((c) => c.trim()).filter((c) => c.length > 0);
  return { table, returning: cols };
}

/**
 * Build the `{obj:{col:<bcScalar>,…}}` ROW type for a write's RETURNING columns, typed via the
 * schema SoT resolver. A `*`, computed, aliased, or qualified RETURNING column is a HARD ERROR
 * (it cannot map to a single schema column). An unknown table/column throws inside the resolver.
 */
function rowObjType(table: string, returning: readonly string[], resolve: ColumnTypeResolver): PortableType {
  const obj: Record<string, PortableType> = {};
  for (const col of returning) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(col)) {
      throw new Error(
        `writeouttype: RETURNING column '${col}' on '${table}' is not a bare column name (alias / ` +
          `qualified / computed). The typed de-box needs bare schema columns so each maps to a SQL ` +
          `type. No-assume, no-fallback.`,
      );
    }
    if (col in obj) throw new Error(`writeouttype: duplicate RETURNING column '${col}' on '${table}'`);
    obj[col] = toPortableScalar(sqlTypeToBcScalar(resolve(table, col)));
  }
  return { obj };
}

/** The `body`-role statements of a transaction plan (the base writes whose RETURNING rows surface). */
function bodyStatements(plan: TransactionPlan): readonly TxStatement[] {
  return plan.statements.filter((s) => s.role === 'body');
}

/**
 * Is this plan a BATCH (createMany/updateMany/deleteMany)? Mirrors the runtime's batch test
 * (`executeTransaction`): a gate-free, ref-free plan (`entityFrom` null, every statement a plain
 * `body` with no gate / no `binds`). A batch is the ONLY shape that populates `returnedRows`; a
 * gate-first single/composite Command exposes its written row via `entity` instead.
 */
function isBatchPlan(plan: TransactionPlan): boolean {
  return (
    plan.entityFrom === null &&
    plan.statements.length > 0 &&
    plan.statements.every((s) => s.gate === undefined && s.binds === undefined && s.role === 'body')
  );
}

/**
 * The write's `(table, RETURNING)` — every `body`-role statement must agree (a batch's groups all
 * target the SAME table with the SAME RETURNING projection; a single/composite Command's body write
 * defines the entity row). A divergent table/RETURNING across a batch is a HARD ERROR.
 */
function writeTargetReturning(plan: TransactionPlan): { table: string; returning: string[] | null } {
  const bodies = bodyStatements(plan);
  if (bodies.length === 0) {
    throw new Error(
      `writeouttype: the transaction plan has no 'body' write statement — a write bundle must carry ` +
        `at least one base write. Cannot derive its output row type. No-assume, no-fallback.`,
    );
  }
  let table: string | undefined;
  let returning: string[] | null | undefined;
  for (const b of bodies) {
    const { table: t, returning: r } = targetAndReturning(b.op.sql);
    if (table === undefined) {
      table = t;
      returning = r;
      continue;
    }
    if (t !== table) {
      throw new Error(
        `writeouttype: batch write body statements target DIFFERENT tables ('${table}' vs '${t}'); ` +
          `the typed de-box requires a homogeneous RETURNING row shape across a batch. No-assume, no-fallback.`,
      );
    }
    if (JSON.stringify(returning) !== JSON.stringify(r)) {
      throw new Error(
        `writeouttype: batch write body statements on '${table}' have DIFFERENT RETURNING projections ` +
          `(${JSON.stringify(returning)} vs ${JSON.stringify(r)}); the de-boxed row struct must be identical ` +
          `across a batch. No-assume, no-fallback.`,
      );
    }
  }
  return { table: table!, returning: returning ?? null };
}

/**
 * The full write-bundle output type: the {@link TransactionResult} typed shape. `committed`,
 * `executed`, `shortCircuit` are always present. `entity` and `returnedRows` are included ONLY when
 * the plan shape can actually POPULATE them — precise typing, not a fallback:
 *  - `entity` (`opt<ROW>`) — present when the body write RETURNs (a gate-first single Command exposes
 *    its written row here). A no-RETURNING write leaves `entity` always null → typed `opt<obj{}>`.
 *  - `returnedRows` (`opt<arr<arr<ROW>>>`) — present ONLY for a BATCH plan whose body RETURNs (the
 *    sole shape the runtime accumulates it in: `executeTransaction` pushes a body's RETURNING rows to
 *    `returnedRows` iff batch mode AND the statement returned rows). A non-batch Command, or a batch
 *    with no RETURNING, never emits `returnedRows`, so it is provably absent from the output and
 *    omitted from the type (the de-box marshaller then never references a field the runtime does not
 *    produce). ROW is typed via the schema SoT + the write's target/RETURNING.
 * Fail-closed throughout (any un-typeable table/column/shape throws — never a boxed fallback).
 */
export function deriveWriteOutputType(plan: TransactionPlan, resolve: ColumnTypeResolver): PortableType {
  const { table, returning } = writeTargetReturning(plan);
  // A write WITHOUT a RETURNING clause NEVER produces a row (the runtime leaves `entity` null and
  // never accumulates `returnedRows`), so the row type is the bc `null` scalar — EXACT (the field is
  // provably always null), not an empty `obj{}` (which would type a zero-field row struct). A write
  // WITH a RETURNING types the row struct from the projected columns via the schema SoT.
  const hasReturning = returning !== null;
  // A no-RETURNING write's row is provably always null → the bc `null` scalar (EXACT, not an empty
  // `obj{}` zero-field struct). `entity` stays `opt<row>` uniformly: opt<null> types a provably-null
  // field (the runtime always returns entity=null there) and, unlike a bare `null` field, keeps the
  // de-box marshaller reading the wire slot (bc's go null-scalar-field emit leaves it unread otherwise).
  const row: PortableType = hasReturning ? rowObjType(table, returning as string[], resolve) : 'null';
  const fields: Record<string, PortableType> = {
    committed: 'bool',
    executed: { arr: 'string' },
    shortCircuit: { opt: SHORT_CIRCUIT_TYPE },
    entity: { opt: row },
  };
  // returnedRows is populated ONLY by a batch plan whose body RETURNs rows; include it in the type
  // only then (precise typing — the field is provably absent otherwise, so omitting it is exact).
  if (isBatchPlan(plan) && hasReturning) fields.returnedRows = { opt: { arr: { arr: row } } };
  return { obj: fields };
}

/**
 * Attach the derived write output type to a write {@link SqlBundle} as `outType` (on the surrogate
 * write IR's single `makeSQL` node) + `outputType` (on the component), matching how the read de-box
 * annotates its surrogate IR. Returns a NEW bundle carrying an `outputType` field the codegen path
 * reads (the makeSqlComponentIR surrogate node picks it up in {@link ../codegen.bundleToPortableIR}).
 * A bundle with no transaction plan (a plain single-statement write with no tx) throws — this de-box
 * is for the write-Command / batch bundles the bench/conformance codegen surface covers.
 */
export function annotateWriteBundleOutType(bundle: SqlBundle, resolve: ColumnTypeResolver): SqlBundle {
  if (bundle.transaction === undefined) {
    throw new Error(
      `writeouttype: bundle '${bundle.name}' carries no transaction plan — the write de-box types the ` +
        `TransactionResult of a write-Command / batch bundle. No-assume, no-fallback.`,
    );
  }
  const outputType = deriveWriteOutputType(bundle.transaction, resolve);
  return { ...bundle, outputType } as SqlBundle;
}
