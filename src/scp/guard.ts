/**
 * litedbmodel v2 SCP — param/fragment portability guard (WS1, #21).
 *
 * Fail-closed enforcement of the hard rule: every Expression IR node this backend emits
 * (param slots, fragment `when` guards) uses ONLY bc's closed operator set
 * (`PORTABLE_EXPR_OPERATORS`). A single-key object whose key is not a known operator is
 * an un-lowerable construct — it must go through the Raw SQL escape hatch (spec §13), NOT
 * a fake opcode. This wraps bc's `assertPortable` (no functions/Date/Promise residue)
 * plus the operator-set check.
 */

import { assertPortable, PORTABLE_EXPR_OPERATORS, PortabilityError } from 'behavior-contracts';
import type { CompiledOperation, ExprNode, Fragment, FragmentTree } from './ir';

/**
 * Assert one Expression IR node is portable: no residue (functions/Date/…) and every
 * operator-node key is in bc's closed set. Literals (number/string/bool/null) and
 * arrays/objects pass structurally; a single-key object is treated as an operator node.
 */
export function assertExprPortable(node: ExprNode, path = '$'): void {
  assertPortable(node, path);
  walkExpr(node, path);
}

function walkExpr(node: unknown, path: string): void {
  if (node === null || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    node.forEach((el, i) => walkExpr(el, `${path}[${i}]`));
    return;
  }
  const keys = Object.keys(node);
  if (keys.length === 1) {
    const op = keys[0];
    if (!PORTABLE_EXPR_OPERATORS.has(op)) {
      throw new PortabilityError(path, `Portability Guard: '${op}' is not a portable Expression IR operator (closed set: ${[...PORTABLE_EXPR_OPERATORS].join(', ')})`);
    }
  }
  for (const k of keys) walkExpr((node as Record<string, unknown>)[k], `${path}.${k}`);
}

/** Assert a fragment (and its slots + `when`) emits only portable Expression IR. */
function assertFragmentPortable(node: Fragment | FragmentTree, path: string): void {
  if ('connector' in node) {
    node.fragments.forEach((f, i) => assertFragmentPortable(f, `${path}.fragments[${i}]`));
    return;
  }
  if (node.when !== undefined) assertExprPortable(node.when, `${path}.when`);
  node.params.forEach((p, i) => assertExprPortable(p, `${path}.params[${i}]`));
}

/**
 * Assert a whole compiled operation emits only portable Expression IR (static params +
 * fragment tree). Throws {@link PortabilityError} on the first non-portable construct.
 */
export function assertOperationPortable(op: CompiledOperation): void {
  op.params.forEach((p, i) => assertExprPortable(p, `$.params[${i}]`));
  if (op.where !== null) assertFragmentPortable(op.where, '$.where');
}
