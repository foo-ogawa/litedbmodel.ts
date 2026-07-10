/**
 * litedbmodel v2 SCP — build a bc `ComponentGraphIR` whose single body node is a
 * `makeSQL` componentRef, so a query rides bc's `runBehavior` (composition / value-eval
 * / plan / envelope come free). The `makeSQL` ports are bc Expression IR: `sql` and
 * `skip` are simple literals/refs; `params` is an `{ arr: [...] }` of Expression IR
 * nodes (each a bound-value literal, an input ref, or a nested-`makeSQL` `{ obj: … }`).
 *
 * The bundle is pure JSON (multi-language ready). The handler (see `handler.ts`)
 * assembles + binds + executes. bc validates the envelope and evaluates the ports.
 */

import type { ComponentGraphIR, Component } from 'behavior-contracts';
import { MAKESQL, type MakeSQL, type SqlParam, isMakeSQL } from './makesql';

/** Wrap a bound SQL value as a bc Expression IR literal node.
 *
 * bc's Expression IR represents ints as `{int:"…"}` and rejects bare `Date`/arrays.
 * SQL bound values are opaque to bc, so we pass them as an inert `{lit}` marker that
 * the handler unwraps — but since bc has no `lit` op, we instead carry the raw params
 * OUT-OF-BAND: the IR references `input.params` by ref and the caller supplies the
 * assembled params in the input scope. This keeps values byte-exact (no bc coercion).
 */

/**
 * Build the single-`makeSQL` component IR + the input scope for a pre-assembled query.
 *
 * We keep SQL bound values OUT of bc's numeric/format coercion by binding them through
 * the input scope: the `params` port is `{ ref: ["__sqlParams"] }` and the caller
 * passes `{ __sqlParams: <the params array>, __sql: <text>, __skip: <bool> }` as input.
 * bc evaluates the refs (returning the values verbatim), then hands them to the
 * `makeSQL` handler, which assembles + binds + executes.
 */
export function makeSqlComponentIR(entry = 'Query'): ComponentGraphIR {
  const component: Component = {
    name: entry,
    inputPorts: {
      __sql: { type: 'string', required: true },
      __sqlParams: { type: 'arr' },
      __skip: { type: 'bool' },
    },
    body: [
      {
        id: 'q',
        component: MAKESQL,
        ports: {
          sql: { ref: ['__sql'] },
          params: { ref: ['__sqlParams'] },
          skip: { ref: ['__skip'] },
        },
      },
    ],
    output: { ref: ['q'] },
  };
  return { irVersion: 1, exprVersion: 2, components: [component] };
}

/** The input scope for {@link makeSqlComponentIR}, from a compiled `makeSQL` bundle. */
export function makeSqlInput(node: MakeSQL): Record<string, unknown> {
  return {
    __sql: node.sql,
    __sqlParams: node.params,
    __skip: node.skip === true,
  };
}

/**
 * bc's `evaluateExpression` rejects bare arrays / non-plain values as expressions, and
 * the input scope is validated loosely (refs just read it). Because our params ride the
 * scope as a plain `ref`, bc returns them verbatim (arrays, Dates, nested makeSQL
 * objects) with NO coercion — exactly what the handler needs. `isMakeSQL` /
 * `SqlParam` re-exported for callers building nested params.
 */
export { isMakeSQL };
export type { SqlParam };
