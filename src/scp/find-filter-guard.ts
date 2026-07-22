/**
 * litedbmodel v2 SCP — FIND_FILTER fail-closed authoring guard (#47 Finding B / plan R8).
 *
 * `FIND_FILTER` is a v1 PER-MODEL implicit predicate (a soft-delete / tenant scope) that
 * v1 MERGES into the WHERE of every internal `find()`/`count()` before compiling
 * (`DBModel._buildSelectSQL` / `_count`: `normalizedCond.add(condsToRecord(FIND_FILTER))`).
 * It is not a distinct SQL construct — it manifests purely as extra WHERE fragments.
 *
 * The SCP compile path (`emitRead(L, 'Select', { where })` → the `executeSQL` leaf)
 * is AUTHORED from an explicit `where`; it has NO `DBModel` class context, so it CANNOT
 * auto-apply a model's `FIND_FILTER` (there is no model reference at compile time, and no
 * production seam routes a `DBModel` into the SCP compile — grep: `FIND_FILTER` appears only
 * in `DBModel`/`decorators`/`types`, never under `src/scp/`). Auto-apply is therefore
 * impossible without model context the SCP compile does not have.
 *
 * Resolution = the plan's variant chosen fail-closed: a model that DECLARES a `FIND_FILTER`
 * MUST have its scope predicates folded into the authored `emitRead(L, 'Select', { where })`; if the
 * author routes such a model through SCP WITHOUT the scope keys, this guard throws loudly
 * ({@link FindFilterLeakError}) rather than silently dropping the filter (which would leak
 * soft-deleted / cross-tenant rows — a correctness bug, not a cosmetic one). When the scope
 * keys ARE present the guard is a no-op (byte-identical to v1's merged WHERE — proven in
 * `test/scp/find-filter-noop.test.ts`).
 */

import { condsToRecord, type Conds } from '../Column';

/** A `DBModel`-shaped source: the only field this guard reads is the optional `FIND_FILTER`. */
export interface FindFilterSource {
  /** The per-model implicit scope predicate (v1 `DBModel.FIND_FILTER`), or null/undefined. */
  readonly FIND_FILTER?: Conds | null;
  /** Class name for diagnostics (present on every `DBModel` subclass). */
  readonly name?: string;
}

/**
 * A model declaring a `FIND_FILTER` was compiled via the SCP path without its scope
 * predicates present in the authored WHERE — the filter would silently leak. Thrown
 * fail-closed at compile time; never swallowed.
 */
export class FindFilterLeakError extends Error {
  constructor(
    /** The declaring model's class name (diagnostics). */
    readonly modelName: string,
    /** The FIND_FILTER scope keys that are MISSING from the authored WHERE. */
    readonly missingKeys: readonly string[],
  ) {
    super(
      `FIND_FILTER leak: model '${modelName}' declares an implicit scope predicate on ` +
        `[${missingKeys.join(', ')}] that is NOT present in the SCP-authored WHERE. The SCP ` +
        `compile cannot auto-apply FIND_FILTER (no model context at compile time), so the ` +
        `scope predicate would be silently dropped (soft-delete / tenant scope leak). Fold ` +
        `the FIND_FILTER predicates into the authored emitRead(L, 'Select', { where }) explicitly ` +
        `(e.g. whereEq/whereIsNull on ${missingKeys.join(', ')}).`,
    );
    this.name = 'FindFilterLeakError';
  }
}

/**
 * Extract the top-level predicate KEYS a `FIND_FILTER` folds into a WHERE. Uses the SAME
 * `condsToRecord` v1 runs when merging (`DBModel._buildSelectSQL`:597) so the key set is
 * exactly what v1 would add to `normalizedCond`. `__or__` (grouped OR) is kept as a key so a
 * scoped OR-group also demands explicit authoring (fail-closed; never silently satisfied).
 */
export function findFilterKeys(filter: Conds | null | undefined): string[] {
  if (!filter || filter.length === 0) return [];
  return Object.keys(condsToRecord(filter));
}

/**
 * Fail-closed guard: assert a model's `FIND_FILTER` scope keys are all present in the
 * authored WHERE key set BEFORE an SCP compile of that model. No FIND_FILTER → no-op. When
 * present, EVERY scope key must appear in `authoredWhereKeys`; any missing key throws
 * {@link FindFilterLeakError}. The caller passes the top-level keys of the object it hands to
 * `emitRead(L, 'Select', { where })` (the same shape `condsToRecord` yields), so the check compares
 * like-for-like against v1's merged predicate.
 *
 * @param model  a `DBModel`-shaped source that MAY declare `FIND_FILTER`.
 * @param authoredWhereKeys  the top-level WHERE keys the SCP author expressed for this model.
 * @throws FindFilterLeakError when a declared scope key is absent from the authored WHERE.
 */
export function assertFindFilterFolded(
  model: FindFilterSource,
  authoredWhereKeys: Iterable<string>,
): void {
  const required = findFilterKeys(model.FIND_FILTER);
  if (required.length === 0) return; // No implicit scope → nothing to fold → no-op.
  const present = new Set(authoredWhereKeys);
  const missing = required.filter((k) => !present.has(k));
  if (missing.length > 0) {
    throw new FindFilterLeakError(model.name ?? '<anonymous model>', missing);
  }
}
