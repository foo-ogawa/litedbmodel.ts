/**
 * WS1 (#21) catalog tests — LITEDBMODEL_CATALOG is a well-formed behavior-contracts
 * Catalog, its CRUD entries compile through the SCP-only authoring path (`SemanticBehavior` +
 * `compileBehaviors`; bc 0.8.0 removed the route-B `buildComponentDefinition` seam), and the
 * graph-derived CQRS effect classification is correct (spec §2.4 / §11 item 1).
 */

import { describe, it, expect } from 'vitest';
import { assertPortableComponentGraph } from 'behavior-contracts';
import {
  LITEDBMODEL_CATALOG,
  CATALOG_NAMES,
  WRITE_CATALOG_NAMES,
  catalogEntry,
  assertComponentsInCatalog,
  deriveContractEffect,
  SemanticBehavior,
  components,
  publishBehaviors,
  type In,
  type Component,
} from '../../src/scp';

const L = components();

describe('LITEDBMODEL_CATALOG shape', () => {
  it('declares exactly the WS1 catalog names', () => {
    expect(Object.keys(LITEDBMODEL_CATALOG).sort()).toEqual(
      ['Count', 'Delete', 'Fragment', 'Insert', 'RelationBatch', 'Select', 'Tx', 'Update'],
    );
    expect([...CATALOG_NAMES].sort()).toEqual(
      ['Count', 'Delete', 'Fragment', 'Insert', 'RelationBatch', 'Select', 'Tx', 'Update'],
    );
  });

  it('RelationBatch is the portable typed native relation-query component', () => {
    const relation = catalogEntry('RelationBatch');
    expect(relation?.portableToIR).toBe(true);
    expect(relation?.output.shape).toBe('items');
    expect(relation?.inputPorts).toEqual({
      table: { type: 'string', required: true },
      select: { type: 'string[]', required: true },
      sql: { type: 'string', required: true },
      keyShape: { type: 'string', required: true },
      targetKeys: { type: 'string[]', required: true },
    });
  });

  it('CRUD + Fragment are portable; Tx (the atomic envelope) is not', () => {
    for (const name of ['Select', 'Count', 'Insert', 'Update', 'Delete', 'Fragment']) {
      expect(catalogEntry(name)?.portableToIR).toBe(true);
    }
    expect(catalogEntry('Tx')?.portableToIR).toBe(false);
    // Tx keeps bc's fail-closed default (static single port); CRUD opt into dynamic ports.
    expect(catalogEntry('Tx')?.additionalPorts).toBe(false);
    expect(catalogEntry('Select')?.additionalPorts).toBe(true);
  });

  it('every entry has a required `table` port on CRUD', () => {
    for (const name of ['Select', 'Insert', 'Update', 'Delete']) {
      expect(catalogEntry(name)?.inputPorts.table).toEqual({ type: 'string', required: true });
    }
  });

  it('write catalog = Insert/Update/Delete/Tx (Select/Fragment are reads)', () => {
    expect([...WRITE_CATALOG_NAMES].sort()).toEqual(['Delete', 'Insert', 'Tx', 'Update']);
  });
});

describe('CQRS effect derivation (graph-derived, spec §2.4)', () => {
  // bc 0.8.0 (scp-only-authoring): route-B `buildComponentDefinition` is removed — the ONLY way to
  // produce a component is to author a `SemanticBehavior` and run it through `compileBehaviors`
  // (`publishBehaviors`). These fixtures do exactly that, then assert `deriveContractEffect` /
  // portability / catalog membership on the REAL compiled components (`static columns` supplies the
  // typed-row SoT bc 0.8.0's all-nodes-typed gate requires).
  class Effects extends SemanticBehavior {
    static columns = { posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT' } };
    CreatePost($: In<{ authorId: number }>) {
      return L.Insert({ table: 'posts', 'values.author_id': $.authorId, returning: 'id, author_id' });
    }
    ListPosts(_$: In<{ authorId: number }>) {
      return L.Select({ table: 'posts', select: ['id', 'title'] });
    }
  }
  const contract = publishBehaviors(Effects);
  const compOf = (name: string): Component => {
    const m = contract.methods[name];
    if (m === undefined) throw new Error(`no method '${name}'`);
    return m.component;
  };

  it('a component referencing Insert derives command', () => {
    const comp = compOf('CreatePost');
    expect(deriveContractEffect(comp)).toBe('command');
    assertComponentsInCatalog([comp]);
    assertPortableComponentGraph(contract.ir);
  });

  it('a Select component derives query and is portable', () => {
    const comp = compOf('ListPosts');
    expect(deriveContractEffect(comp)).toBe('query');
    assertComponentsInCatalog([comp]);
    assertPortableComponentGraph(contract.ir);
  });
});

describe('assertComponentsInCatalog fail-closed', () => {
  it('rejects an unknown component name', () => {
    const bad = [
      { name: 'X', inputPorts: {}, body: [{ id: 'n', component: 'Upsert', ports: { table: 't' } }], output: null },
    ] as unknown as Component[];
    expect(() => assertComponentsInCatalog(bad)).toThrow(/not a LITEDBMODEL_CATALOG entry/);
  });

  it('rejects a missing required port', () => {
    const bad = [
      { name: 'X', inputPorts: {}, body: [{ id: 'n', component: 'Select', ports: { select: { arr: ['id'] } } }], output: null },
    ] as unknown as Component[];
    expect(() => assertComponentsInCatalog(bad)).toThrow(/required port 'table' is not wired/);
  });

  it('rejects an undeclared non-family port', () => {
    const bad = [
      { name: 'X', inputPorts: {}, body: [{ id: 'n', component: 'Select', ports: { table: 't', bogus: 1 } }], output: null },
    ] as unknown as Component[];
    expect(() => assertComponentsInCatalog(bad)).toThrow(/port 'bogus' is not declared/);
  });
});
