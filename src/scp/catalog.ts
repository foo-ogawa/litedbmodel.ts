/**
 * litedbmodel v2 SCP catalog — WS1 (#21).
 *
 * `LITEDBMODEL_CATALOG` is the behavior-contracts–typed `Catalog` that IS the whole
 * per-DSL difference litedbmodel supplies as a SQL-backend consumer (bc C2: "the only
 * per-DSL difference is the catalog"). It mirrors the SHAPE of graphddb's
 * `GRAPHDDB_CATALOG` (`src/spec/catalog.ts`) exactly: compact `PortSchema` constructors,
 * an `entry()` helper, per-primitive `inputPorts`, an `output.shape`, and a
 * `portableToIR` flag. The effect implementation (SQL execution + row→model assembly) is
 * bound separately in the handler registry (WS3, C4 — catalog declares, registry binds).
 *
 * ## Catalog names (spec §1 / §11 item 1)
 *
 * `Select` / `Insert` / `Update` / `Delete` are the CRUD leaf components. `Fragment` is
 * the dynamic WHERE/SET fragment-tree component (SKIP → fragment existence rules, §8);
 * `Tx` is the multi-statement transaction envelope. As in graphddb, the transaction
 * envelope (`Tx`) is `portableToIR: false` — a transaction is emitted as a Component
 * whose body references the per-statement CRUD primitives; the atomic BEGIN/COMMIT
 * envelope is the runtime's execution concern (WS3), never a portable-IR node.
 *
 * ## Port schema conventions
 *
 * A `CatalogEntry.inputPorts` names the STATIC ports of the emitted wiring. Two families
 * are dynamic by construction and are validated by {@link assertComponentsInCatalog}
 * against documented patterns instead of being statically enumerable:
 *
 * - **`where`/`set`/`values` records**: one port per model field, flattened one-port-
 *   per-field under a `<group>.<field>` key ({@link WRITE_PORT_FAMILIES}). These carry
 *   Expression IR (the field's value/condition), from the closed operator set only.
 *
 * The CRUD entries carry `additionalPorts: true` (bc#26) so bc's shared port check
 * (`checkPortNames` / `buildComponentDefinition`) accepts the dynamic families; every
 * DECLARED port keeps bc's required/type checks regardless of the opt-in. `Tx`'s single
 * port is static, so it keeps bc's fail-closed default.
 */

import { classifyBehaviorEffect, type Catalog, type CatalogEntry, type PortSchema } from 'behavior-contracts';
// bc 0.8.0: the UNBRANDED structural component shape (matches `ir.components[number]` AND bc's
// `classifyBehaviorEffect`/`referencedComponents` params, both unbranded). See `./authoring`
// (type-only import — no runtime cycle).
import type { Component } from './authoring';

/**
 * The catalog-name literal union — one entry per SQL primitive litedbmodel declares:
 * the CRUD leaves (`Select` / `Insert` / `Update` / `Delete`), the dynamic WHERE/SET
 * fragment-tree component (`Fragment`), and the multi-statement transaction envelope
 * (`Tx`).
 */
export type CatalogName =
  | 'Select'
  | 'Count'
  | 'Insert'
  | 'Update'
  | 'Delete'
  | 'Fragment'
  | 'Tx';

// Compact schema constructors (the IR bundle is size-budgeted — graphddb convention).
const P = (type: string, required?: true): PortSchema => (required ? { type, required } : { type });

/**
 * The shared read-port surface for `Select`: `table` always; the optional
 * `where` (a fragment-tree reference — see `Fragment`), `order`, `limit`, `offset`,
 * `group` wiring, and the always-emitted `select` projection.
 *
 * `where` is typed `fragment` (not `expr`): a Select's dynamic WHERE is a fragment tree
 * (§8), NOT a single Expression — the SKIP existence rules live on the tree, outside the
 * Expression IR vocabulary (bc `expression-ir.md` §4: SKIP is OUTSIDE Expression IR).
 * `limit` / `offset` are `expr` (they may be an input-ref or a `coalesce` default, §8).
 */
const SELECT_PORTS: Record<string, PortSchema> = {
  table: P('string', true),
  select: P('string[]', true),
  where: P('fragment'),
  order: P('string'),
  limit: P('expr'),
  offset: P('expr'),
  group: P('string'),
  // Additive read-tail/head ports (V0 R3/R4/R5/R6). SQL is v1-sourced from the matching
  // `SelectDesc` fields via `compileSelect` (`_buildSelectSQL`), wired in `compileSelectNode`:
  //  - `forUpdate` (R3): a literal `'true'` string toggles the ` FOR UPDATE` tail.
  //  - `join`/`joinParams` (R5): a literal JOIN clause text (`JOIN t ON …`) + its bound
  //    value-specs (`expr[]`), spliced after `FROM <t>` (v1 `_buildSelectSQL` JOIN position).
  //  - `cte`/`cteParams` (R4): a WITH-CTE `{name, sql}` literal + its bound value-specs,
  //    prefixed as `WITH <name> AS (<sql>) ` (v1 WITH-wrap). `cteParams` bind FIRST (v1 param
  //    order: CTE → JOIN → WHERE).
  //  - `append` (R6): a raw trailing clause text (e.g. `HAVING …`) appended verbatim.
  forUpdate: P('string'),
  join: P('string'),
  joinParams: P('expr[]'),
  cte: P('map'),
  cteParams: P('expr[]'),
  append: P('string'),
};

/**
 * The shared write-port surface for `Insert`/`Update`/`Delete`: `table` always;
 * `where` (fragment tree — required existence enforced by the compiler, not here, since a
 * Select may legitimately omit it); `returning` projection; `onConflict` wiring for
 * Insert. The per-field value/set ports (`values.<field>` / `set.<field>`) are dynamic
 * ({@link WRITE_PORT_FAMILIES}) and each carries an Expression IR value.
 */
const WRITE_PORTS: Record<string, PortSchema> = {
  table: P('string', true),
  where: P('fragment'),
  returning: P('string'),
  onConflict: P('string'),
  onConflictAction: P('string'),
  // PRIMARY KEY descriptor for the MySQL RETURNING emulation (INSERT…RETURNING): `pk` is a
  // comma-separated PK column list, `autoInc` names the single AUTO_INCREMENT column (absent for a
  // client-supplied PK). Consumed by compileWriteNode → mysqlPkHint so the emulation re-selects by
  // the REAL PK. Inert on PG/SQLite (native RETURNING).
  pk: P('string'),
  autoInc: P('string'),
};

const entry = (name: CatalogName, inputPorts: Record<string, PortSchema>, shape: string, portableToIR: boolean): CatalogEntry => ({
  name,
  inputPorts,
  output: { shape },
  portableToIR,
  // Dynamic port families (where/set/values record ports) are validated by
  // assertComponentsInCatalog, not statically enumerable. Tx overrides back to
  // fail-closed (its one port is static).
  additionalPorts: true,
});

/**
 * litedbmodel's specific-component catalog (spec §11 item 1) — the behavior-contracts
 * `Catalog`-typed source of truth. Each entry declares the Port schema of one SQL
 * primitive AS THE BACKEND COMPILE WIRES IT; the effect implementation is bound
 * separately in the handler registry (WS3, C4). All entries except `Tx` may appear in
 * the portable IR.
 */
export const LITEDBMODEL_CATALOG: Catalog = {
  // Read: `items` (a Select root yields a row list; per-row collapse to `item` is the
  // consumer's cardinality concern, mirrored from graphddb Query's `cardinality` port).
  Select: entry('Select', { ...SELECT_PORTS, cardinality: P('string') }, 'items', true),
  // COUNT(*) aggregate read (v1 `DBModel._count`): `SELECT COUNT(*) as count FROM t[ WHERE …]`.
  // Only `table` (required) + the optional `where` fragment tree — v1's count carries no
  // projection/order/limit/offset (it counts the filtered rows). Output is a one-row `[{count}]`
  // list (the `items` shape, like every read); the consumer reads `count` — v1's `parseInt(count)`.
  Count: entry('Count', { table: P('string', true), where: P('fragment') }, 'items', true),
  // Writes yield the RETURNING row list (`items`); a single-row write collapses at the
  // consumer boundary, same as reads.
  Insert: entry('Insert', WRITE_PORTS, 'items', true),
  Update: entry('Update', WRITE_PORTS, 'items', true),
  Delete: entry('Delete', WRITE_PORTS, 'items', true),
  // The dynamic WHERE/SET fragment-tree component (§8). Its ports carry the fragment
  // tree; the compiler resolves SKIP/AND-OR structure to fragment existence rules.
  Fragment: entry('Fragment', { tree: P('fragment', true) }, 'item', true),
  // The runtime-side atomic envelope: never referenced by the portable IR (a transaction
  // IS a component over the CRUD primitives) — runtime-only. Its single port is static,
  // so it keeps bc's fail-closed default (no additionalPorts opt-in).
  Tx: {
    ...entry('Tx', { statements: P('map[]', true) }, 'item', false),
    additionalPorts: false,
  },
};

/** Look up a catalog entry by name (undefined for an unknown primitive). */
export function catalogEntry(name: string): CatalogEntry | undefined {
  return LITEDBMODEL_CATALOG[name];
}

/** The catalog names litedbmodel declares (the C2 per-DSL surface). */
export const CATALOG_NAMES: readonly CatalogName[] = [
  'Select',
  'Count',
  'Insert',
  'Update',
  'Delete',
  'Fragment',
  'Tx',
];

/**
 * The CQRS write catalog: the names whose presence in a component body makes the
 * component a COMMAND (spec §2.4 — effect is derived from the graph, not authored).
 * `Select`/`Fragment` are read-side; `Insert`/`Update`/`Delete`/`Tx` are write-side.
 */
export const WRITE_CATALOG_NAMES: readonly CatalogName[] = [
  'Insert',
  'Update',
  'Delete',
  'Tx',
];

const WRITE_SET: ReadonlySet<string> = new Set(WRITE_CATALOG_NAMES);

/**
 * The dynamic per-field write-port families (`<group>.<field>` — the flattened record
 * convention; longest-prefix wins). A `values.<field>` (Insert) or `set.<field>`
 * (Update) port carries the field's Expression IR value; a `sqlCast.<field>` port carries
 * the field's PostgreSQL cast type (a literal string, e.g. `jsonb`/`uuid`/`int[]`) that drives
 * the v1 per-column `?::<sqlCast>` on Postgres (`DBModel._insert`/`_update`). `sqlCast.` ports are
 * INERT on MySQL/SQLite (v1's dialect-aware cast formatter is identity there).
 */
export const WRITE_PORT_FAMILIES: readonly string[] = [
  'values.',
  'set.',
  'sqlCast.',
];

/**
 * The emitter's catalog validation (loud; a violation is a BUILD error), mirrored from
 * graphddb's `assertComponentsInCatalog`. Every `componentRef` / `map` body node of every
 * emitted component must:
 *
 * 1. reference a declared catalog name;
 * 2. wire every `required` port of that entry;
 * 3. wire ONLY declared ports — a port is declared if it is a static `inputPorts` name or
 *    a write record-family port ({@link WRITE_PORT_FAMILIES}, write entries only).
 *
 * All violations are collected and thrown as one error. `cond` nodes carry no component
 * reference and are skipped.
 */
export function assertComponentsInCatalog(components: readonly Component[]): void {
  const errs: string[] = [];
  for (const c of components) {
    for (const n of c.body) {
      if ('cond' in n) continue;
      if ('fanout' in n) {
        // bc 0.7.3+ `FanoutNode` (connection fan-out / batched BatchGet). litedbmodel authors
        // only Select/Count/map/cond via LITEDBMODEL_CATALOG and never emits fanout, so a
        // fanout node here means an unsupported graph reached the emitter — reject fail-closed
        // rather than mis-validate it as a component ref.
        errs.push(`${c.name}/${n.id}: fanout node is not supported by litedbmodel (bc FanoutNode)`);
        continue;
      }
      const ref = 'map' in n ? n.map : n;
      const entryDef = LITEDBMODEL_CATALOG[ref.component];
      if (entryDef === undefined) {
        errs.push(`${c.name}/${n.id}: references '${ref.component}', not a LITEDBMODEL_CATALOG entry (known: ${CATALOG_NAMES.join(', ')})`);
        continue;
      }
      const ports = ref.ports;
      for (const [p, s] of Object.entries(entryDef.inputPorts)) {
        if (s.required === true && !(p in ports)) {
          errs.push(`${c.name}/${n.id} (${ref.component}): required port '${p}' is not wired`);
        }
      }
      const isWriteItem = WRITE_SET.has(ref.component);
      for (const p of Object.keys(ports)) {
        if (p in entryDef.inputPorts) continue;
        if (isWriteItem && WRITE_PORT_FAMILIES.some((f) => p.startsWith(f))) continue;
        errs.push(`${c.name}/${n.id} (${ref.component}): port '${p}' is not declared by the catalog entry (nor a write record-family port)`);
      }
    }
  }
  if (errs.length > 0) {
    throw new Error(`litedbmodel catalog validation failed (${errs.length} violation${errs.length === 1 ? '' : 's'}):\n - ${errs.join('\n - ')}`);
  }
}

// ── CQRS effect classification (graph-derived — spec §2.4) ────────────────────
/** litedbmodel's CQRS effect vocabulary, mapped 1:1 from bc's `BehaviorEffect`. */
export type ContractEffect = 'query' | 'command';

/**
 * Derive one component's Query/Command classification FROM THE GRAPH: behavior-contracts'
 * `classifyBehaviorEffect` over {@link WRITE_CATALOG_NAMES} — a component is a `'command'`
 * iff its body references a write-catalog primitive, else a `'query'` (spec §2.4: effect
 * is derived, never authored).
 */
export function deriveContractEffect(component: Component): ContractEffect {
  return classifyBehaviorEffect(component, WRITE_CATALOG_NAMES) === 'writing' ? 'command' : 'query';
}
