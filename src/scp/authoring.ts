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

  return {
    className: (cls as { name?: string }).name ?? '<anonymous>',
    ir,
    components: ir.components,
    methods,
  };
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
export type { Component, ComponentGraphIR, MapNode, ComponentRefNode };
