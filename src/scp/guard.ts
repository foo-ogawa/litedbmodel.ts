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

import { assertPortable, FORBIDDEN_OBJECT_KEY, PORTABLE_EXPR_OPERATORS, PortabilityError, type Component, type ComponentGraphIR } from 'behavior-contracts';
import type { CompiledOperation, ExprNode, Fragment, FragmentTree } from './ir';

/**
 * Assert one Expression IR node is portable: no residue (functions/Date/…) and every
 * operator-node key is in bc's closed set. Literals (number/string/bool/null) and arrays
 * pass structurally.
 *
 * A single-key object is an OPERATOR node only when its key is a known operator
 * (∈ {@link PORTABLE_EXPR_OPERATORS}); its value is then recursed as operator args. A
 * single-key object whose key is NOT a known operator is a real portability violation and
 * is rejected fail-closed (an un-lowerable pseudo-opcode → must use the Raw SQL escape
 * hatch, spec §13). This mirrors bc's `assertPortableComponentGraph` operator-position
 * semantics exactly — see {@link walkExpr} for the `obj` field-map special case.
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
    // Single-key object = OPERATOR-position node. The key must be a known opcode
    // (fail-closed on unknown opcodes — a real portability violation).
    const op = keys[0];
    if (!PORTABLE_EXPR_OPERATORS.has(op)) {
      throw new PortabilityError(path, `Portability Guard: '${op}' is not a portable Expression IR operator (closed set: ${[...PORTABLE_EXPR_OPERATORS].join(', ')})`);
    }
    const arg = (node as Record<string, unknown>)[op];
    // `obj` is the construction node: its arg is a FIELD MAP whose KEYS are data field
    // names (NOT opcodes) and whose VALUES are ExprNodes. Recurse into the values ONLY —
    // never apply the operator heuristic to the field map itself (aligns with bc's
    // assertPortableExpr / evaluate() so a single-field {obj:{one:…}} is not misread as an
    // unknown opcode). See behavior-contracts guard.ts `op === "obj"` branch.
    if (op === 'obj') {
      if (arg === null || typeof arg !== 'object' || Array.isArray(arg)) {
        throw new PortabilityError(`${path}.obj`, `Portability Guard: {obj: ...} expects an object (at ${path}.obj)`);
      }
      for (const [k, v] of Object.entries(arg as Record<string, unknown>)) {
        // Static fail-closed (defense-in-depth): own key "__proto__" is rejected here as it
        // is by bc's guard/evaluator (prototype pollution / cross-language divergence).
        if (k === FORBIDDEN_OBJECT_KEY) {
          throw new PortabilityError(`${path}.obj`, `Portability Guard: object key "${FORBIDDEN_OBJECT_KEY}" is forbidden (fail-closed) at ${path}.obj`);
        }
        walkExpr(v, `${path}.obj.${k}`);
      }
      return;
    }
    // Any other operator: recurse its arg (array of args or a nested expr node).
    walkExpr(arg, `${path}.${op}`);
    return;
  }
  // Multi-key objects are not operator nodes (operator nodes are single-key). Recurse each
  // value structurally so nested operator nodes are still checked (assertPortable above has
  // already rejected any function/Date/Promise residue across the whole subtree).
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

// ── Authoring→IR lower guard (WS2, #22 — auto-applied on the single compile path) ─────

/**
 * Assert every Expression IR node in ONE lowered component's port wiring is portable
 * (WS2 AC). Walks every `componentRef`/`map` node's `ports` (and `map.over` / `map.when`)
 * and every `cond` node's `if`/`then`/`else`, applying {@link assertExprPortable} — so any
 * operator outside bc's closed set is rejected fail-closed at compile time. This is the
 * litedbmodel-owned portability layer the authoring lower path auto-applies, over and
 * above bc's own `assertPortableComponentGraph` self-check.
 */
export function assertComponentPortable(component: Component): void {
  const at = `component '${component.name}'`;
  for (const n of component.body) {
    if ('map' in n) {
      assertExprPortable(n.map.over, `${at}/${n.id}.map.over`);
      assertExprPortable(n.map.ports, `${at}/${n.id}.map.ports`);
      if (n.map.when !== undefined) assertExprPortable(n.map.when, `${at}/${n.id}.map.when`);
    } else if ('cond' in n) {
      assertExprPortable(n.cond.if, `${at}/${n.id}.cond.if`);
      assertExprPortable(n.cond.then, `${at}/${n.id}.cond.then`);
      assertExprPortable(n.cond.else, `${at}/${n.id}.cond.else`);
    } else {
      assertExprPortable(n.ports, `${at}/${n.id}.ports`);
    }
  }
  assertExprPortable(component.output, `${at}.output`);
}

/**
 * Assert every component of a lowered Component-graph IR is portable (WS2 AC). The
 * authoring lower path ({@link publishBehaviors} / {@link compileEager}) invokes this
 * automatically so a non-portable opcode anywhere in the emitted IR is a fail-closed
 * compile error. Throws {@link PortabilityError} on the first violation.
 */
export function assertComponentGraphPortable(ir: ComponentGraphIR): void {
  for (const c of ir.components) assertComponentPortable(c);
}
