/**
 * WS1 (#21) catalog tests — LITEDBMODEL_CATALOG is a well-formed behavior-contracts
 * Catalog, its CRUD entries build through bc's `buildComponentDefinition` seam, and the
 * graph-derived CQRS effect classification is correct (spec §2.4 / §11 item 1).
 */

import { describe, it, expect } from 'vitest';
import {
  assertPortableComponentGraph,
  buildComponentDefinition,
  type Component,
} from 'behavior-contracts';
import {
  LITEDBMODEL_CATALOG,
  CATALOG_NAMES,
  WRITE_CATALOG_NAMES,
  catalogEntry,
  assertComponentsInCatalog,
  deriveContractEffect,
} from '../../src/scp';

describe('LITEDBMODEL_CATALOG shape', () => {
  it('declares exactly the WS1 catalog names', () => {
    expect(Object.keys(LITEDBMODEL_CATALOG).sort()).toEqual(
      ['Count', 'Delete', 'Fragment', 'Insert', 'Select', 'Tx', 'Update'],
    );
    expect([...CATALOG_NAMES].sort()).toEqual(
      ['Count', 'Delete', 'Fragment', 'Insert', 'Select', 'Tx', 'Update'],
    );
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
  const build = (name: string, body: Component['body']): Component =>
    buildComponentDefinition(
      { name, inputPorts: { authorId: { type: 'int' } }, body, output: { ref: [body[0].id] } },
      { catalog: LITEDBMODEL_CATALOG },
    );

  it('a component referencing Insert derives command', () => {
    const comp = build('CreatePost', [
      {
        id: 'ins',
        component: 'Insert',
        // bc's builder uses a FLAT scope (input ports are top-level roots); a port value
        // references an input as `{ref:["authorId"]}`, not `{ref:["input","authorId"]}`.
        ports: { table: 'posts', 'values.author_id': { ref: ['authorId'] } },
      },
    ]);
    expect(deriveContractEffect(comp)).toBe('command');
    assertComponentsInCatalog([comp]);
    assertPortableComponentGraph({ irVersion: 1, exprVersion: 2, components: [comp] });
  });

  it('a Select component derives query and is portable', () => {
    const comp = buildComponentDefinition(
      {
        name: 'ListPosts',
        inputPorts: { authorId: { type: 'int' } },
        body: [
          {
            id: 'sel',
            component: 'Select',
            ports: { table: 'posts', select: { arr: ['id', 'title'] } },
          },
        ],
        output: { ref: ['sel'] },
      },
      { catalog: LITEDBMODEL_CATALOG },
    );
    expect(deriveContractEffect(comp)).toBe('query');
    assertComponentsInCatalog([comp]);
    assertPortableComponentGraph({ irVersion: 1, exprVersion: 2, components: [comp] });
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
