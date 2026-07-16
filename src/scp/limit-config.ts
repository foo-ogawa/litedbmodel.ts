/**
 * litedbmodel v2 SCP — hard-limit runaway prevention config (Phase E-2, epic #74; v1 parity —
 * `DBModel.setLimitConfig` / `_limitConfig`).
 *
 * A read that forgot its WHERE clause, or a hasMany relation batch fanning out over an accidental
 * cross-join, can load an unbounded result set. v1 guarded this with a global `findHardLimit` /
 * `hasManyHardLimit` (plus a per-relation override); this module is the v2 SCP equivalent — the
 * SHARED config surface the TS runtime reads at COMPILE time to bake the effective caps onto the
 * portable artifacts (the {@link import('./makesql/static-bundle').ReadGraph} find-guard and each
 * {@link import('./relation').RelationOp}). Because the caps are baked at compile, the native ports
 * (#100-103) need NO config surface of their own: they read the resolved `hardLimit` off the JSON
 * artifact and run the SAME post-fetch throw.
 *
 * ## Semantics (v1 parity — match precisely)
 *
 *  - `findHardLimit`     — a top-level read (find/read) exceeding this THROWS {@link
 *    import('./errors').LimitExceededError} (`context: 'find'`). The read injects `LIMIT hardLimit +
 *    1` when the author set no explicit limit, so the throw only asserts the total EXCEEDS the cap.
 *  - `hasManyHardLimit`  — a hasMany relation batch whose TOTAL fetched rows exceed this THROWS
 *    (`context: 'relation'`, EXACT count). A per-relation `hardLimit` override wins over the global.
 *  - `null` DISABLES the check (a per-relation `hardLimit: null` disables it for that relation even
 *    when the global is set); `undefined` on a relation means "use the global".
 *  - A raw-SQL relation with an INTRINSIC per-parent `LIMIT` (a `hasMany` `RelationDecl.limit`) skips
 *    the batch-total check — its fanout is already bounded per parent by the window.
 *
 * The config is a process-global singleton (v1 `DBModel._limitConfig`). `setLimitConfig` MERGES
 * (v1 parity), `resetLimitConfig` clears it (test isolation). The defaults are BOTH disabled
 * (`undefined` ⇒ no cap) so nothing changes for a caller who never opts in.
 */

/**
 * Global hard-limit config (spec §E-2; v1 `LimitConfig`). Both caps default to disabled.
 * This is the AUTHORING/config surface — the resolved caps are baked onto the compiled artifacts.
 */
export interface LimitConfig {
  /**
   * Hard cap for a top-level read (find/read). A read returning more than this THROWS
   * {@link import('./errors').LimitExceededError}. `null` / `undefined` ⇒ disabled (no cap).
   */
  readonly findHardLimit?: number | null;
  /**
   * Hard cap for a hasMany relation batch TOTAL. A batch fetching more than this THROWS. A
   * per-relation `hardLimit` override wins. `null` / `undefined` ⇒ disabled (no cap).
   */
  readonly hasManyHardLimit?: number | null;
}

/** The process-global config singleton (v1 `DBModel._limitConfig`). Both caps disabled by default. */
let globalLimitConfig: LimitConfig = {};

/**
 * MERGE `config` into the global hard-limit config (v1 `DBModel.setLimitConfig` parity — a partial
 * update, not a replace). Call once at app start:
 *   `setLimitConfig({ findHardLimit: 5000, hasManyHardLimit: 500 })`.
 * Pass `null` for a key to explicitly DISABLE that check.
 */
export function setLimitConfig(config: LimitConfig): void {
  globalLimitConfig = { ...globalLimitConfig, ...config };
}

/** Read a shallow copy of the current global hard-limit config. */
export function getLimitConfig(): LimitConfig {
  return { ...globalLimitConfig };
}

/** Clear the global hard-limit config back to the disabled default (test isolation). */
export function resetLimitConfig(): void {
  globalLimitConfig = {};
}

/**
 * Resolve the effective `findHardLimit`: the value passed in `override` (when the caller supplies a
 * config explicitly, e.g. at compile) else the global. Returns `null` (disabled) or a non-negative
 * integer cap. `undefined` normalizes to the global; an explicit `null` disables.
 */
export function resolveFindHardLimit(override?: number | null): number | null {
  const v = override !== undefined ? override : globalLimitConfig.findHardLimit;
  return normalizeCap(v, 'findHardLimit');
}

/**
 * Resolve the effective hasMany cap for ONE relation (v1 `_selectForRelation` precedence):
 *   per-relation `perRelation` (`null` disables, a number overrides) wins over the `global`
 *   `hasManyHardLimit`; `undefined` on the relation falls through to the global. Returns `null`
 *   (disabled) or a non-negative integer cap.
 */
export function resolveHasManyHardLimit(perRelation?: number | null, global?: number | null): number | null {
  // v1: `perRelationHardLimit === null ? null : perRelationHardLimit ?? globalHasManyHardLimit`.
  const g = global !== undefined ? global : globalLimitConfig.hasManyHardLimit;
  const raw = perRelation === null ? null : perRelation !== undefined ? perRelation : g;
  return normalizeCap(raw, 'hasManyHardLimit');
}

/** Validate + normalize a cap: `null`/`undefined` ⇒ null (disabled); else a non-negative integer. */
function normalizeCap(v: number | null | undefined, field: string): number | null {
  if (v === null || v === undefined) return null;
  if (!Number.isInteger(v) || v < 0) {
    throw new Error(`limit-config: ${field} must be a non-negative integer or null (got ${String(v)})`);
  }
  return v;
}
