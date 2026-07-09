/**
 * #38 — portability guard `{obj: fieldMap}` classification.
 *
 * Regression + hardening for the pre-existing (WS5) guard bug: `assertExprPortable` treated
 * ANY single-key object as an operator node, so a single-field `{obj:{one:…}}` payload had its
 * lone FIELD name (e.g. `postId`) misread as an unknown opcode and rejected. `obj` is the
 * construction opcode; its arg is a FIELD MAP whose keys are data field names (NOT opcodes) and
 * whose values are ExprNodes. The guard must recurse into the field VALUES only — aligning with
 * bc's own `assertPortableExpr`/`evaluate` `obj` semantics.
 *
 * AC(2) (guard-not-weakened): an unknown opcode in OPERATOR position (a single-key object whose
 * key is NOT in the closed set, and NOT an `obj` field-map value) must still fail fail-closed.
 */

import { describe, it, expect } from 'vitest';
import { PortabilityError } from 'behavior-contracts';
import { assertExprPortable, type ExprNode } from '../../src/scp';

describe('#38 — {obj: fieldMap} is a field map, not an operator node', () => {
  it('single-field {obj:{one:{ref:[…]}}} passes the guard (the exact WS5-rejected shape)', () => {
    const node: ExprNode = { obj: { postId: { ref: ['$', 'entity', 'id'] } } };
    expect(() => assertExprPortable(node)).not.toThrow();
  });

  it('a field named like a real payload key (postId) is NOT misread as an opcode', () => {
    // This is the precise bug: `postId` is a data field name, not an operator.
    const node: ExprNode = { obj: { postId: { ref: ['$', 'entity', 'id'] } } };
    expect(() => assertExprPortable(node)).not.toThrow();
  });

  it('two-field {obj:{a,b}} still passes (the WS8a workaround shape) — no regression', () => {
    const node: ExprNode = {
      obj: { postId: { ref: ['$', 'entity', 'id'] }, userId: { ref: ['$', 'input', 'author_id'] } },
    };
    expect(() => assertExprPortable(node)).not.toThrow();
  });

  it('empty {obj:{}} passes (zero-field field map is structurally valid)', () => {
    expect(() => assertExprPortable({ obj: {} } as ExprNode)).not.toThrow();
  });

  it('a single-field obj whose VALUE is an unknown opcode still fails fail-closed', () => {
    // The field name (postId) is data; but its VALUE is in operator position, so an unknown
    // opcode there must still be rejected — the guard is not weakened inside obj values.
    const node: ExprNode = { obj: { postId: { bogusOp: [1, 2] } } };
    expect(() => assertExprPortable(node)).toThrow(PortabilityError);
    expect(() => assertExprPortable(node)).toThrow(/'bogusOp' is not a portable/);
  });

  it('a nested single-field obj as an operator arg passes (obj can appear anywhere a value can)', () => {
    // {eq:[{obj:{k:1}}, {obj:{k:1}}]} — each obj is a single-field field map, not an opcode.
    const node: ExprNode = { eq: [{ obj: { k: { int: '1' } } }, { obj: { k: { int: '1' } } }] };
    expect(() => assertExprPortable(node)).not.toThrow();
  });

  it('{obj: <non-object>} is rejected (obj arg must be a field-map object)', () => {
    expect(() => assertExprPortable({ obj: [1, 2] } as ExprNode)).toThrow(/expects an object/);
    expect(() => assertExprPortable({ obj: 42 } as ExprNode)).toThrow(/expects an object/);
  });

  it('__proto__ as an obj field key is rejected fail-closed (matches bc)', () => {
    const node = { obj: JSON.parse('{"__proto__": {"int": "1"}}') } as ExprNode;
    expect(() => assertExprPortable(node)).toThrow(/__proto__.*forbidden/);
  });
});

describe('#38 — guard NOT weakened: unknown opcodes in operator position still rejected', () => {
  it('a single-field {unknownOp:[…]} in operator position fails fail-closed', () => {
    // NOT inside an obj field map — this is a genuine operator-position pseudo-opcode.
    const node: ExprNode = { bogusOp: [{ int: '1' }, { int: '2' }] };
    expect(() => assertExprPortable(node)).toThrow(PortabilityError);
    expect(() => assertExprPortable(node)).toThrow(/'bogusOp' is not a portable Expression IR operator/);
  });

  it('a single-field {postId:…} at the ROOT (no obj wrapper) is still rejected', () => {
    // Without the {obj:…} wrapper, `postId` IS in operator position → unknown opcode.
    const node: ExprNode = { postId: { ref: ['$', 'entity', 'id'] } };
    expect(() => assertExprPortable(node)).toThrow(/'postId' is not a portable/);
  });

  it('an unknown opcode nested inside a known operator arg is still rejected', () => {
    const node: ExprNode = { and: [{ ge: [{ int: '1' }, { int: '0' }] }, { madeUpOp: [] }] };
    expect(() => assertExprPortable(node)).toThrow(/'madeUpOp' is not a portable/);
  });

  it('every closed-set operator is still accepted (no over-tightening)', () => {
    expect(() => assertExprPortable({ ref: ['$', 'x'] } as ExprNode)).not.toThrow();
    expect(() => assertExprPortable({ int: '5' } as ExprNode)).not.toThrow();
    expect(() => assertExprPortable({ arr: [{ int: '1' }] } as ExprNode)).not.toThrow();
    expect(() => assertExprPortable({ add: [{ int: '1' }, { int: '2' }] } as ExprNode)).not.toThrow();
  });
});
