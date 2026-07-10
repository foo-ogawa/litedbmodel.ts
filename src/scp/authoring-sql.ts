/**
 * litedbmodel v2 SCP — SQL WHERE authoring helpers (WS3, #23).
 *
 * These are thin, closed-set wrappers over behavior-contracts' expression builders that
 * make a WHERE fragment tree authorable in the `SemanticBehavior` / eager surface while
 * emitting ONLY bc's closed-set Expression IR (no invented opcode — the hard rule). The
 * {@link import('./bridge').compileNode} Backend-Compile bridge decodes exactly these
 * closed-set encodings back into WS1's fragment `Condition[]`:
 *
 *   - {@link whereEq}   → `{eq:[<colRef>, <value>]}`                       → `col = ?`
 *   - {@link whereNe}/{@link whereLt}/… → `{ne|lt|le|gt|ge:[<colRef>, v]}` → `col <op> ?`
 *   - {@link whereIsNull} → `{eq:[<colRef>, null]}`                        → `col IS NULL`
 *   - {@link whereIn}   → `{eq:[{ref:[IN_SENTINEL, col]}, <arr>]}`         → `col IN (?, …)`
 *
 * A SKIP-optional condition is authored with bc's `when(cond, () => <whereX(...)>)`, which
 * bc lowers to `{cond:[cond, <member>, null]}` — the bridge reads that as a `skipWhen`
 * guard (dynamic-expansion spec §2). Nested AND/OR groups are authored with bc's `and` /
 * `or` over these members.
 *
 * The column argument is a RECORDED reference (`$.author_id`, or a wire field `$p.foo`):
 * its ref-path last segment is the physical column name (spec §7). The value argument is a
 * recorded reference, a literal, or any closed-set builder expression (`coalesce`, `add`, …).
 */

import { eq, ne, lt, le, gt, ge, type Recorded } from 'behavior-contracts';
import { IN_SENTINEL } from './makesql';

/** A recorded `$`-reference (column or value) or a literal value usable in a builder. */
type Operand = Recorded | unknown;

/** `col = value` — equality fragment (`{eq:[colRef, value]}`). */
export function whereEq(col: Recorded, value: Operand): Recorded {
  return eq(col, value) as unknown as Recorded;
}

/** `col <> value` — inequality fragment. */
export function whereNe(col: Recorded, value: Operand): Recorded {
  return ne(col, value) as unknown as Recorded;
}

/** `col < value`. */
export function whereLt(col: Recorded, value: Operand): Recorded {
  return lt(col, value) as unknown as Recorded;
}

/** `col <= value`. */
export function whereLe(col: Recorded, value: Operand): Recorded {
  return le(col, value) as unknown as Recorded;
}

/** `col > value`. */
export function whereGt(col: Recorded, value: Operand): Recorded {
  return gt(col, value) as unknown as Recorded;
}

/** `col >= value`. */
export function whereGe(col: Recorded, value: Operand): Recorded {
  return ge(col, value) as unknown as Recorded;
}

/** `col IS NULL` — encoded as `eq(col, null)` (v1 parity; bridge maps null-RHS → IS NULL). */
export function whereIsNull(col: Recorded): Recorded {
  return eq(col, null) as unknown as Recorded;
}

/**
 * `col IN (?, …)` — encoded as `eq(<IN-sentinel col>, value)`. `col` MUST be the IN-sentinel
 * reference built by {@link inColumn} (its ref-path head is {@link IN_SENTINEL}); `value` is
 * the array-valued reference/expression whose `?` expands to `(?, ?, …)` at render time
 * (dynamic-expansion spec §5). Using the sentinel keeps IN expressible with only the
 * closed-set `eq` + `ref` operators.
 */
export function whereIn(col: Recorded, value: Operand): Recorded {
  return eq(col, value) as unknown as Recorded;
}

/**
 * Build the IN-list column reference for {@link whereIn}: `$[IN_SENTINEL][name]` — a
 * recorded ref whose path is `[IN_SENTINEL, name]`. The bridge strips the sentinel head and
 * treats `name` as the physical column (a membership fragment, not an equality).
 */
export function inColumn($: Recorded, name: string): Recorded {
  return ($ as unknown as Record<string, Record<string, Recorded>>)[IN_SENTINEL][name];
}
