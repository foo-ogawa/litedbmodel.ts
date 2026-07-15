/**
 * litedbmodel v2 SCP — Authoring Parse (WS2, #22).
 *
 * The litedbmodel authoring surface (spec §2.4 / §7) and the SINGLE compile path
 * (spec §9) that lowers BOTH the SCP declaration blocks and the eager public API into
 * ONE internal Component-graph IR (behavior-contracts' portable JSON). This module is a
 * thin SQL-backend consumer over bc's shared authoring layer (`SemanticBehavior` /
 * `compileBehaviors` / `catalogComponents` — bc#24): litedbmodel supplies ONLY the
 * Catalog ({@link LITEDBMODEL_CATALOG}) and the CQRS effect derivation; bc owns all
 * composition, `$`-wiring, structured control (`when` / `.map`), expression lowering,
 * plan derivation and the portability self-check (C2: "the difference is the catalog
 * only"). It mirrors graphddb's `publishBehaviors` precedent exactly.
 *
 * ## Authoring surface (spec §2.4)
 *
 * - **Root = class**: a `SemanticBehavior` subclass (default) or an `@behavior`-marked
 *   class; each PUBLIC method is one root Behavior (= one component; name = method name).
 *   `#`-private / `_`-prefixed members are helpers and are not published. The marker is
 *   one-per-class and effect-agnostic — there is NO query/command vocabulary to author.
 * - **Input Port** = the method's `$: In<...>` argument; **Output Port** = its return
 *   value; the internal DAG is derived from `$`-refs, `.map` and `when` wiring.
 * - **Leaf vocabulary** = the Catalog only ({@link components} = `catalogComponents(
 *   LITEDBMODEL_CATALOG)` — `Select` / `Insert` / `Update` / `Delete` / `Fragment`). There
 *   is NO litedbmodel-local authoring vocabulary beyond the Catalog. Conditions/values are
 *   written with bc's expression builders (`eq` / `ne` / `coalesce` / …); `cond ? [...] :
 *   SKIP` is written as `when(cond, () => <fragment>)` — in a `where` port it lowers to a
 *   pure `{cond:[c, <frag>, null]}` Expression IR node (SKIP is the ABSENCE of the
 *   fragment when `c` is false — spec §7, WS1 render), NOT a new opcode. Native `?:` / `??`
 *   / `&&` are rejected by bc's source scan; use `when` / `coalesce` / `and`.
 *
 * ## CQRS effect (spec §2.4 — derived, never authored)
 *
 * Query vs Command is DERIVED from whether a lowered component references a write-Catalog
 * primitive (`Insert` / `Update` / `Delete` / `Tx`), via {@link deriveContractEffect}
 * (bc `classifyBehaviorEffect` over {@link WRITE_CATALOG_NAMES}). Class names carry no
 * meaning.
 *
 * ## Single compile path (spec §9 — a real invariant)
 *
 * The eager public API ({@link compileEager}) does NOT interpret model metadata on a
 * separate path. It synthesizes the SAME authoring (a one-method `SemanticBehavior`
 * subclass) and runs it through the SAME {@link compileBehaviors}. `test/scp` pins that
 * the eager path and the declaration path for an equivalent query produce byte-identical
 * component IR.
 */

import {
  catalogComponents,
  compileBehaviors,
  SemanticBehavior,
  type BehaviorClass,
  type Component,
  type ComponentGraphIR,
  type MapNode,
  type ComponentRefNode,
  type FanoutNode,
  type PortSchema,
  type Recorded,
} from 'behavior-contracts';
import {
  assertComponentsInCatalog,
  deriveContractEffect,
  LITEDBMODEL_CATALOG,
  type CatalogName,
  type ContractEffect,
} from './catalog';
import { assertComponentGraphPortable } from './guard';
import { failClosedMaterializeResolverFromColumnMap, columnTypeResolverFromColumnMap, type MaterializeResolver, type ColumnTypeResolver } from './coltype';
import { parseProjectionColumn } from './makesql/outtype';

/**
 * The litedbmodel leaf-component functions for the shared authoring surface —
 * `catalogComponents(LITEDBMODEL_CATALOG)` bound once, lazily (so importing this module
 * does not pay for the binding until authoring actually runs). Invoke them inside
 * `SemanticBehavior` method bodies:
 *
 * ```ts
 * const L = components();
 * class PostBehaviors extends SemanticBehavior {
 *   PostSearch($: In<{ authorId: number; status?: string; since: string }>) {
 *     const posts = L.Select({ table: 'posts', select: ['id', 'author_id', 'title'],
 *       where: [ eq($.authorId, $.authorId),
 *                when(ne($.status, null), () => eq($.status, $.status)),   // SKIP-optional
 *                ge($.since, $.since) ] });
 *     const authors = posts.map(($p: Recorded) =>
 *       L.Select({ table: 'users', select: ['id', 'name'], where: [ eq($p.author_id, $p.author_id) ] }));
 *     return { posts, authors };
 *   }
 * }
 * ```
 *
 * `Tx` is in the map but is `portableToIR: false`, so wiring it is rejected at record
 * time — the atomic transaction envelope is a WS3 runtime concern, never authored.
 */
export type ComponentFns = Record<CatalogName, (ports: Record<string, unknown>) => Recorded>;

let boundComponents: ComponentFns | undefined;

/** The `catalogComponents(LITEDBMODEL_CATALOG)` leaf-function map (typed, memoized). */
export function components(): ComponentFns {
  if (boundComponents === undefined) {
    boundComponents = catalogComponents(LITEDBMODEL_CATALOG) as ComponentFns;
  }
  return boundComponents;
}

/**
 * One published behavior method: the bc-lowered component (name = the method name) plus
 * its graph-DERIVED CQRS effect (spec §2.4 — the classification source of truth is the
 * component graph; there is no query/command vocabulary for the author to declare).
 */
export interface BehaviorMethodSpec {
  /** The method (= root Behavior = component) name. */
  readonly name: string;
  /** Graph-derived Query/Command classification (never author-declared). */
  readonly effect: ContractEffect;
  /** The lowered portable component (bc `compileBehaviors` output). */
  readonly component: Component;
}

/**
 * A resolved behavior contract — {@link publishBehaviors} output. Carries the whole
 * bc-lowered {@link ComponentGraphIR} (directly consumable by WS3's runtime + WS1's
 * backend-compile) and the per-method specs with their derived effects.
 */
export interface BehaviorModelContract {
  /** The authored class name (diagnostics only). */
  readonly className: string;
  /** The bc-lowered portable IR (all methods; the shared internal IR — spec §9). */
  readonly ir: ComponentGraphIR;
  /** The lowered components (`ir.components`, one per public method). */
  readonly components: readonly Component[];
  /** Method name → lowered component + derived effect. */
  readonly methods: Readonly<Record<string, BehaviorMethodSpec>>;
  /**
   * The STATIC read-path materialize resolver (issue #59), derived ONCE at registration from the
   * model's INLINE typed-column declaration (`static columns` — see {@link ModelColumns}).
   * `(table, column) → MaterializeClass`, pure in-memory map lookups — ZERO per-read DB
   * introspection. The read entry points (`executeBehavior`/`executeBehaviorAsync`/`read`) consult
   * it so INT→number / BIGINT→string / DATE→string / BOOLEAN→boolean de-boxes for every read. Absent
   * only when the model declared no `columns` (a model with no typed reads → raw driver values).
   */
  readonly materializeResolver?: MaterializeResolver;
  /**
   * The STATIC codegen column-type resolver (issue #58/#59), derived from the SAME inline `columns`
   * declaration — `(table, column) → SQL type token`. Feeds `deriveReadOutTypes` so the typed-native
   * codegen `outType` annotations come from the declaration (not an external DDL). Absent when the
   * model declared no `columns`.
   */
  readonly resolveColumnType?: ColumnTypeResolver;
}

/**
 * The INLINE typed-column declaration (issue #59): `table → (column → SQL type token)`, declared
 * ONCE per table on the behavior model — the BC-native, consumer-inline column-type SoT (bc never
 * infers types; the author annotates them, exactly as graphddb declares its typed entity columns).
 * A read's projection resolves each column's type from THIS declaration (no external DDL string, no
 * DB introspection). The registration precomputes both the codegen `outType` resolver and the TS
 * read-path materialize resolver from it, so de-box is always-on for every registered model. SQL
 * type tokens are the §4.1 vocabulary (`INTEGER`/`INT`/`BIGINT`/`REAL`/`DOUBLE`/`DECIMAL(…)`/`TEXT`/
 * `VARCHAR(…)`/`BOOLEAN`/`DATE`/`TIMESTAMP`/`JSON`/`JSONB`/`UUID`/…).
 */
export type ModelColumns = Readonly<Record<string, Readonly<Record<string, string>>>>;

/**
 * A behavior model class may declare its columns inline via a `static columns` field — the typed
 * catalog the reads project from. `publishBehaviors` reads it at registration to build the de-box
 * resolvers. (Optional per class; a model with no typed reads may omit it.)
 */
export interface TypedModelClass {
  readonly columns?: ModelColumns;
}

/** Options passed through to bc `compileBehaviors` (additive; all optional). */
export interface PublishBehaviorsOptions {
  /**
   * Input Port schema overrides (method → port → schema): build-time evaluation derives
   * port NAMES from `$` accesses, but TS types (`In<...>`) are erased at runtime, so a
   * consumer that wants the IR to carry the declared type strings supplies them here.
   */
  readonly inputPorts?: Record<string, Record<string, PortSchema>>;
  /** Plan concurrency recorded on the lowered components (bc default: 16). */
  readonly concurrency?: number;
  /**
   * The model's inline typed-column declaration (issue #59) — see {@link ModelColumns}. Normally
   * declared as a `static columns` field ON the behavior class (the BC-native shape); this option is
   * an alternative for callers that build the class dynamically. When both are present the option
   * takes precedence. Absent from BOTH ⇒ the model has no declared types → reads stay un-materialized
   * (raw driver values; pre-#59 — a model with no typed reads).
   */
  readonly columns?: ModelColumns;
}

/**
 * The single lowering core (spec §9): bc `compileBehaviors` → litedbmodel catalog check →
 * litedbmodel portability guard → CQRS effect derivation. BOTH {@link publishBehaviors}
 * (declaration path) and {@link compileEager} (eager public-API path) call this so there
 * is exactly one compile path and one guard application.
 *
 * Pipeline (all loud, nothing dropped):
 *  1. bc `compileBehaviors(cls)` lowers every public method into one portable component
 *     (bc owns ALL composition/lowering and runs its own `assertPortableComponentGraph`);
 *  2. {@link assertComponentsInCatalog} validates every lowered component against
 *     `LITEDBMODEL_CATALOG` — the SAME check the WS1 emitter path uses (covers the
 *     dynamic `values.<field>` / `set.<field>` write families bc's static schema check
 *     cannot enumerate);
 *  3. {@link assertComponentGraphPortable} AUTO-APPLIES litedbmodel's portability guard
 *     over every port Expression IR node (WS2 AC — the lower path rejects any opcode
 *     outside bc's closed set fail-closed, independently of bc's own self-check);
 *  4. the CQRS effect of every method is DERIVED from its graph
 *     ({@link deriveContractEffect}); the author never declares it.
 */
function lowerBehaviorClass(cls: BehaviorClass, options: PublishBehaviorsOptions): BehaviorModelContract {
  const ir = compileBehaviors(cls, {
    ...(options.inputPorts !== undefined ? { inputPorts: options.inputPorts } : {}),
    ...(options.concurrency !== undefined ? { concurrency: options.concurrency } : {}),
  });

  assertComponentsInCatalog(ir.components);
  assertComponentGraphPortable(ir);

  const methods: Record<string, BehaviorMethodSpec> = {};
  for (const component of ir.components) {
    methods[component.name] = {
      name: component.name,
      effect: deriveContractEffect(component),
      component,
    };
  }

  // STATIC de-box resolvers (issue #59): the column types come from the model's INLINE declaration
  // (`static columns` on the class, or `options.columns`) — the BC-native, consumer-inline SoT (bc
  // never infers types). Build the `table → (col → SQL type)` map ONCE at registration into BOTH the
  // codegen outType resolver AND the FAIL-CLOSED TS read-path materialize resolver. A read then
  // de-boxes with ZERO DB introspection (litedbmodel's static-resolution core value).
  const className = (cls as { name?: string }).name ?? '<anonymous>';
  const declaredColumns = options.columns ?? (cls as unknown as TypedModelClass).columns;
  const columnMap = declaredColumns !== undefined ? toColumnMap(declaredColumns) : undefined;
  const materializeResolver: MaterializeResolver | undefined =
    columnMap !== undefined ? failClosedMaterializeResolverFromColumnMap(columnMap) : undefined;
  const resolveColumnType: ColumnTypeResolver | undefined =
    columnMap !== undefined ? columnTypeResolverFromColumnMap(columnMap) : undefined;

  // FAIL-CLOSED coverage-by-construction (issue #59 audit): every column a READ method projects MUST
  // have a declared type. Validate NOW, at registration — a read whose projected columns aren't fully
  // declared cannot be published (so no production read can silently skip de-box and leak a rounded
  // i64). A model with NO `columns` but a read that projects columns fails here too (columns are
  // REQUIRED for a typed read). A write / a read that projects no explicit columns is exempt.
  assertReadColumnsDeclared(className, ir.components, methods, resolveColumnType);

  return {
    className,
    ir,
    components: ir.components,
    methods,
    ...(materializeResolver !== undefined ? { materializeResolver } : {}),
    ...(resolveColumnType !== undefined ? { resolveColumnType } : {}),
  };
}

/** Normalize a declared {@link ModelColumns} into the `Map<table, Map<column, sqlType>>` the resolvers use. */
function toColumnMap(columns: ModelColumns): Map<string, Map<string, string>> {
  const map = new Map<string, Map<string, string>>();
  for (const [table, cols] of Object.entries(columns)) {
    const inner = new Map<string, string>();
    for (const [col, sqlType] of Object.entries(cols)) inner.set(col, sqlType);
    map.set(table, inner);
  }
  return map;
}

/** A read Select node's literal `table` + explicit `select` column list (issue #59 validation). */
interface SelectProjection { readonly table: string; readonly columns: readonly string[] }

/** Read a literal string port of a body node, or `undefined` (not a literal). */
function literalStringPort(ports: Record<string, unknown>, name: string): string | undefined {
  const v = ports[name];
  return typeof v === 'string' ? v : undefined;
}

/** Read a literal `{arr:[str,…]}` string-list port, or `undefined` (not a literal list). */
function literalStringArrayPort(ports: Record<string, unknown>, name: string): string[] | undefined {
  const v = ports[name];
  if (v !== null && typeof v === 'object' && 'arr' in v && Array.isArray((v as { arr: unknown }).arr)) {
    const arr = (v as { arr: unknown[] }).arr;
    if (arr.every((e) => typeof e === 'string')) return arr as string[];
  }
  return undefined;
}

/** Extract each `Select` body node's `{ table, columns }` projection (skips Count / non-literal). */
function selectProjectionsOf(component: Component): SelectProjection[] {
  const out: SelectProjection[] = [];
  for (const n of component.body) {
    if ('cond' in n) continue;
    if ('fanout' in n) {
      // bc 0.7.3+ `FanoutNode`. litedbmodel never emits fanout — reject fail-closed rather
      // than mis-read it as a component ref with no `Select` projection.
      throw new Error(`selectProjectionsOf: component '${component.name}' node '${n.id}' is a fanout node, not supported by litedbmodel (bc FanoutNode)`);
    }
    const ref = 'map' in n ? n.map : n;
    if (ref.component !== 'Select') continue; // Count → scalar; no projection
    const ports = ref.ports as Record<string, unknown>;
    const table = literalStringPort(ports, 'table');
    const columns = literalStringArrayPort(ports, 'select');
    if (table === undefined || columns === undefined || columns.length === 0) continue;
    out.push({ table, columns });
  }
  return out;
}

/**
 * FAIL-CLOSED registration guard (issue #59 audit): every column a READ (`query`) method PROJECTS —
 * in ANY shape (bare `col`, qualified `t.col`, aliased `col AS b`) — must resolve to a declared type
 * in the model's inline `columns`. Uses the SHARED {@link parseProjectionColumn} (the SAME projection
 * parser the read-path materializer + codegen outType derivations use), so no projection shape can
 * escape validation. A `*` / computed projection, an undeclared underlying column, or a read with NO
 * `columns` declared at all, THROWS here — a typed read whose projected columns aren't fully declared
 * cannot be registered, so no production read can silently skip de-box and return a rounded i64.
 * Writes are exempt.
 */
function assertReadColumnsDeclared(
  className: string,
  components: readonly Component[],
  methods: Readonly<Record<string, BehaviorMethodSpec>>,
  resolveColumnType: ColumnTypeResolver | undefined,
): void {
  for (const component of components) {
    if (methods[component.name]?.effect !== 'query') continue; // reads only (writes exempt)
    for (const proj of selectProjectionsOf(component)) {
      const at = `model '${className}' method '${component.name}'`;
      // SHARED parse (all shapes): `*` → throw; computed → no schema column (skip); bare/qualified/
      // aliased schema column → must be declared (against its OWNER table = qualifier ?? base table).
      const schemaCols = proj.columns
        .map((col) => parseProjectionColumn(col, proj.table, at))
        .filter((e): e is { kind: 'column'; underlying: string; outputKey: string; qualifier?: string } => e.kind === 'column')
        .map((e) => ({ table: e.qualifier ?? proj.table, column: e.underlying }));
      if (schemaCols.length === 0) continue; // only computed projections — nothing to type
      if (resolveColumnType === undefined) {
        // A read projecting schema columns with NO `columns` declared → fail closed (columns REQUIRED).
        throw new Error(
          `scp publishBehaviors: ${at} reads projecting typed columns ` +
            `[${schemaCols.map((s) => `${s.table}.${s.column}`).join(', ')}] but the model declares NO ` +
            `\`columns\`. A typed read REQUIRES an inline \`static columns\` declaration so the read-path ` +
            `de-box (INT→number / BIGINT→string / DATE→string / BOOLEAN→boolean) is always-on — never a ` +
            `silent raw (rounded i64) result. Declare the model's \`static columns\`.`,
        );
      }
      // resolveColumnType THROWS (naming the table/column) for any undeclared UNDERLYING column.
      for (const s of schemaCols) resolveColumnType(s.table, s.column);
    }
  }
}

/**
 * Publish an SCP declaration block: a `SemanticBehavior` (or `@behavior`) class whose
 * public methods are the root Behaviors (spec §2.4). Lowers through the single compile
 * path ({@link lowerBehaviorClass}).
 *
 * @throws bc `AuthoringFailure` for a non-declarative body (native control syntax,
 *   coercion, unknown/missing ports, …); an `Error` from the catalog validation for an
 *   undeclared dynamic port; a `PortabilityError` for a non-portable expression.
 */
export function publishBehaviors(
  cls: BehaviorClass,
  options: PublishBehaviorsOptions = {},
): BehaviorModelContract {
  return lowerBehaviorClass(cls, options);
}

// ── Eager public-API path (spec §2.3 / §9 — same compile path, no separate interpreter) ──

/**
 * An eager authoring body: the composition a public-API call (`find` / `create` / …)
 * would run, expressed in the SAME authoring vocabulary as a declaration-block method.
 * `$` is the recorder Input Port and `L` is the litedbmodel leaf-component map
 * ({@link components}); it returns the Output Port. Expression builders (`eq` /
 * `coalesce` / …) and `when` MUST be used over `$` (they only work inside the recording
 * frame) — never native `?:` / `??` / `&&`.
 *
 * The public API funnels here so its lowering shares one compiler with the declaration
 * path (spec §9). It is installed AS the class method (below), so bc's source scan covers
 * it in full — the eager path has the same fail-closed coverage as the declaration path.
 */
export type EagerBehavior = ($: Recorded, L: ComponentFns) => unknown;

/**
 * Compile an eager public-API call through the SINGLE compile path (spec §9). It does NOT
 * interpret model metadata on a separate path: it synthesizes a one-method
 * `SemanticBehavior` subclass whose single public method IS `fn` (with `L` bound), and
 * runs it through the SAME {@link compileBehaviors} the declaration path uses.
 * Consequently the eager path and an equivalent declaration method produce byte-identical
 * component IR (pinned by the single-compile-path equivalence test in `test/scp/authoring.test.ts`).
 *
 * Because `fn` is installed AS the class method (not delegated to via a closure), bc's
 * source scan runs over `fn`'s own source — native control syntax inside `fn` is rejected
 * fail-closed exactly as in a declaration block (no unscanned-thunk residual). The
 * synthesized method is named `name` so the emitted component name matches a
 * declaration-block method of the same name (byte-identity requires equal names).
 *
 * @throws the same failures as {@link publishBehaviors} (single path, single guard).
 */
export function compileEager(
  name: string,
  fn: EagerBehavior,
  options: PublishBehaviorsOptions = {},
): BehaviorModelContract {
  const cls = makeEagerClass(name, fn);
  return lowerBehaviorClass(cls, options);
}

/**
 * Build a one-method `SemanticBehavior` subclass whose PUBLIC method is named `name` and
 * IS `fn` bound to the litedbmodel leaf map. bc records the method with a recorder `$`;
 * `fn`'s source is what bc scans for native control syntax (full fail-closed coverage).
 */
function makeEagerClass(name: string, fn: EagerBehavior): BehaviorClass {
  const L = components();
  const cls = class extends SemanticBehavior {};
  // The method's source that bc scans is `fn.toString()` (via `method`), so native control
  // syntax inside `fn` is rejected. `L` is bound by partial application, NOT closed over
  // inside `fn`'s scanned body.
  const method = function (this: unknown, $: Recorded): unknown {
    return fn($, L);
  };
  // Make bc scan `fn`'s source (the authored body), not the trivial wrapper: expose `fn`'s
  // source as the method's `toString`. bc's scanNativeControlSyntax reads `method.toString()`.
  Object.defineProperty(method, 'toString', { value: () => fn.toString(), configurable: true });
  Object.defineProperty(cls.prototype, name, {
    value: method,
    writable: true,
    enumerable: false,
    configurable: true,
  });
  Object.defineProperty(cls, 'name', { value: name, configurable: true });
  return cls;
}

// Re-export used component-graph node types so consumers (WS3) can narrow the emitted IR
// without importing bc directly.
export type { Component, ComponentGraphIR, MapNode, ComponentRefNode, FanoutNode };
