/**
 * litedbmodel v2 SCP — public surface (WS1, #21).
 *
 * The SQL-backend consumer layer over behavior-contracts (spec §1). WS1 delivers the
 * Catalog definition and the SQLite Backend Compile + dynamic-expansion render. WS2
 * (authoring parse), WS3 (runtime execution / handlers), WS4/5 (relations) and the other
 * dialects (PG/MySQL) are out of scope here.
 */

// Catalog (spec §11 item 1)
export {
  LITEDBMODEL_CATALOG,
  catalogEntry,
  CATALOG_NAMES,
  WRITE_CATALOG_NAMES,
  WRITE_PORT_FAMILIES,
  assertComponentsInCatalog,
  deriveContractEffect,
} from './catalog';
export type { CatalogName, ContractEffect } from './catalog';

// SQL IR shapes (spec §8)
export { WHERE_SLOT } from './ir';
export type {
  ExprNode,
  Fragment,
  FragmentTree,
  AssemblySpec,
  CompiledOperation,
} from './ir';

// SQLite Backend Compile (spec §11 item 3)
export {
  compileSelect,
  compileInsert,
  compileUpdate,
  compileDelete,
} from './compile-sqlite';
export type {
  Ref,
  Condition,
  SelectDesc,
  InsertDesc,
  UpdateDesc,
  DeleteDesc,
} from './compile-sqlite';

// Dynamic-expansion render (normative reference for byte-identical output)
export { renderOperation } from './render';
export type { RenderedSql } from './render';

// Portability guard (closed Expression IR set only)
export { assertExprPortable, assertOperationPortable } from './guard';
