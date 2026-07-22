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
  compileBehaviors,
  SemanticBehavior,
  type BehaviorClass,
  type ComponentGraphIRDoc,
  type PortSchema,
  type Recorded,
} from 'behavior-contracts';

// These aliases are inspection-only structural views over BC output. Native generation must consume
// the original `compileBehaviors` handle directly; litedbmodel must not rebuild or re-adopt IR.
/** Unbranded whole-IR structural shape (bc `ComponentGraphIRDoc`) — litedbmodel's carried/transformed IR. */
export type ComponentGraphIR = ComponentGraphIRDoc;
/** Unbranded component structural shape (`ir.components[number]`). */
export type Component = ComponentGraphIRDoc['components'][number];
/** Unbranded body-node structural shapes. */
export type BodyNode = Component['body'][number];
export type MapNode = Extract<BodyNode, { map: unknown }>;
export type FanoutNode = Extract<BodyNode, { fanout: unknown }>;
export type CondNode = Extract<BodyNode, { cond: unknown }>;
export type ComponentRefNode = Exclude<BodyNode, MapNode | FanoutNode | CondNode>;
import { leafComponents, LEAVES, spliceWhere } from './leaves';
import { lowerWherePort } from './makesql/static-bundle';
import { deriveReadRow, outputType as composeOutputType } from './makesql/outtype';
import type { PortableType } from 'behavior-contracts';
import type { Dialect } from './makesql/handler';
import type { DialectName } from './dialect';
import { assertComponentGraphPortable } from './guard';
import { failClosedMaterializeResolverFromColumnMap, columnTypeResolverFromColumnMap, type MaterializeResolver, type ColumnTypeResolver } from './coltype';

/**
 * The CQRS effect vocabulary (spec §2.4 — derived from the graph, never authored). A component is a
 * `'command'` iff it issues a mutating statement; else a `'query'`.
 */
export type ContractEffect = 'query' | 'command';

/**
 * Derive one component's Query/Command classification FROM THE GRAPH (spec §2.4): a component is a
 * `'command'` iff any body node is an `executeSQL` leaf whose `write` port is the literal `true`
 * (an INSERT/UPDATE/DELETE or a tx-control statement). Op is graph-derived, never author-declared —
 * the effect no longer comes from a per-op catalog name (the 8-leaf catalog is retired), but from
 * the op-independent transport's `write` intent.
 */
export function deriveContractEffect(component: Component): ContractEffect {
  for (const n of component.body) {
    if ('cond' in n) continue;
    const ref = 'map' in n ? n.map : 'fanout' in n ? n.fanout : n;
    if ((ref as { component?: string }).component !== 'executeSQL') continue;
    if ((ref as { ports?: Record<string, unknown> }).ports?.write === true) return 'command';
  }
  return 'query';
}

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
export type ComponentFns = typeof leafComponents;

/**
 * The op-independent leaf-function map — `behaviorComponents({executeSQL,pluck,group})` bound once in
 * `./leaves` (SA8 / bc#126). Authoring bodies call `L.executeSQL({sql,params,write,…})` /
 * `L.pluck({rows,col})` / `L.group({parents,children,pk,fk,into,single})`. The per-projection read-row
 * de-box type and the relation key-array element type are stamped by the op builders at the CALL site
 * (they know the projection / key column), via bc's `.as` — authoring cannot parse the opaque `sql`.
 */
export function components(): ComponentFns {
  return leafComponents;
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
  /**
   * The target SQL dialect for the post-compile WHERE lowering (#141) — the WHERE structure is mostly
   * dialect-agnostic (`col = ?`), but the IN-list form (PG `= ANY(?)` vs MySQL/SQLite `json_each(?)`)
   * and per-column casts differ. Default `'sqlite'`. The read/write op builders thread the model's dialect.
   */
  readonly dialect?: DialectName;
  /**
   * Native-codegen output-passthrough mode (#141 / bc#164). When set, the publish makes the contract
   * NATIVE-CLEAN for `generateModule({language:'rust-typed-native'|'go-typed-native'})`:
   *
   *  1. **Output-passthrough (#164)**: every INTERMEDIATE node (a node whose result feeds a downstream
   *     op-agnostic leaf — parent read → `pluck` → child read → `group`, at every relation level) is
   *     flagged `wirePassthrough:true` with an OPAQUE-WIRE `outType` (`{arr:'value'}` for a `many`
   *     leaf, `'value'` for a `one` leaf like `pluck`). bc then keeps its result as raw `WireValue` /
   *     `Vec<WireValue>` (NO de-box) so it feeds the next leaf transport with no per-op `typed→Value`
   *     re-box. ONLY the TERMINAL node (the component's output) de-boxes to its concrete typed outType.
   *  2. **TS-only ports stripped**: the `materializers` port (the TS ir-exec read-path de-box map) is
   *     removed from every node — the native emitter de-boxes from `outType`, never `materializers`,
   *     so a `materializers` port would leak a TS-only concern into the native contract.
   *
   * Off (default): the contract is the TS ir-exec shape (`materializers` stamped, every read node
   * typed) — unchanged for the litedbmodel runtime read path. This flag is a PUBLISH-layer transform,
   * not a codegen-path IR edit: the serialized contract is native-clean by construction.
   */
  readonly nativePassthrough?: boolean;
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
  // STATIC de-box resolvers (issue #59): the column types come from the model's INLINE declaration
  // (`static columns` on the class, or `options.columns`) — the BC-native, consumer-inline SoT (bc
  // never infers types). Build the `table → (col → SQL type)` map ONCE at registration into BOTH the
  // codegen outType resolver AND the FAIL-CLOSED TS read-path materialize resolver. A read then
  // de-boxes with ZERO DB introspection (litedbmodel's static-resolution core value).
  //
  // bc 0.8.0 (#12 / SA5): the resolver is built BEFORE `compileBehaviors` and bound as the AMBIENT
  // compile-time resolver (`withColumnResolver`) so the typed CRUD leaf wrappers can stamp each
  // authored node's determined output type via `.as` DURING recording — clearing bc's all-nodes-typed
  // gate. Same `static columns` SoT as the post-compile de-box, moved to authoring time.
  const className = (cls as { name?: string }).name ?? '<anonymous>';
  const declaredColumns = options.columns ?? (cls as unknown as TypedModelClass).columns;
  const columnMap = declaredColumns !== undefined ? toColumnMap(declaredColumns) : undefined;
  const materializeResolver: MaterializeResolver | undefined =
    columnMap !== undefined ? failClosedMaterializeResolverFromColumnMap(columnMap) : undefined;
  const resolveColumnType: ColumnTypeResolver | undefined =
    columnMap !== undefined ? columnTypeResolverFromColumnMap(columnMap) : undefined;

  const ir = compileBehaviors(cls, {
    ...(options.inputPorts !== undefined ? { inputPorts: options.inputPorts } : {}),
    ...(options.concurrency !== undefined ? { concurrency: options.concurrency } : {}),
  });

  // bc runs its own `assertPortableComponentGraph` inside `compileBehaviors`; litedbmodel's
  // portability guard is auto-applied over every port Expression IR node. There is no per-op catalog
  // to validate against (the 8-leaf catalog is retired — the op-independent leaves' ports are checked
  // by `behaviorComponents`/`compileBehaviors` at record time).
  assertComponentGraphPortable(ir);

  // #141 WHERE lowering: lower each `executeSQL` node's RECORDED `where` port (plain Expression IR —
  // bc turned the live sugar proxies into IR during `compileBehaviors`) into the node's static
  // `sql`/`params`. Lowering the RECORDED IR (not the live proxy) is what makes `$.col` cols,
  // `whereRawPredicate`, and sentinel sugar work (the authoring-time proxy walk was the NOT_RECORDABLE
  // cause). Bounded WHERE → literal ` WHERE …` merged (native-lowerable); SKIP/dynamic → fail-closed.
  lowerRecordedWhere(ir, (options.dialect ?? 'sqlite') as Dialect);

  // #59 read-column de-box + coverage guard (op-builder layer): resolve each read node's `readColumns`
  // (base table + explicit projection, carried by `emitRead`) against the model's `static columns`,
  // fail-close on a `*`/undeclared column AND on a read model that declared NO columns at all, and stamp
  // the TS de-box `materializers` map the `executeSQL` leaf applies at execute. A RETURNING WRITE carries
  // `readColumns` too (its returning projection returns typed rows — the SAME mechanism), so a `.map`
  // over it is COVERED. The transient `readColumns` port is stripped.
  lowerReadColumns(ir, resolveColumnType, className);

  // #141 / bc#164 native output-passthrough: make the contract native-clean for `generateModule`
  // (rust/go typed-native). Runs LAST (after the read outType + WHERE lowering it consumes): flags every
  // INTERMEDIATE node opaque-wire (`wirePassthrough` + `{arr:'value'}` / `'value'`) so bc keeps its
  // result raw `WireValue`/`Vec<WireValue>` for the op-agnostic leaves, de-boxing ONLY the terminal; and
  // strips the TS-only `materializers` port. Off ⇒ the TS ir-exec shape (unchanged). See the option doc.
  if (options.nativePassthrough === true) lowerNativePassthrough(ir);

  const methods: Record<string, BehaviorMethodSpec> = {};
  for (const component of ir.components) {
    methods[component.name] = {
      name: component.name,
      effect: deriveContractEffect(component),
      component,
    };
  }

  // NOTE (#141 cutover): the #59 FAIL-CLOSED read-column-coverage guard was Select-node-projection
  // based (it read the `select` port off catalog `Select` nodes). With the op-independent `executeSQL`
  // leaf the projection lives inside the opaque `sql` text, so the guard MUST move to the op builder
  // (`decorator-adapter`), which knows the projection and stamps each read row's de-box type via `.as`
  // + validates the columns against `resolveColumnType`. Reinstate there before merge (no silent de-box
  // hole in the final state).

  return {
    className,
    ir,
    components: ir.components,
    methods,
    ...(materializeResolver !== undefined ? { materializeResolver } : {}),
    ...(resolveColumnType !== undefined ? { resolveColumnType } : {}),
  };
}

/** An `executeSQL`-bearing node the transient-port lowering passes reach — see {@link executeSqlNodes}. */
interface ExecuteSqlNodeView {
  /** The node's `executeSQL` ports (a top-level node's `ports`, or a map/fanout's INNER `ports`). */
  readonly ports: Record<string, unknown>;
  /** The node id (for diagnostics). */
  readonly id: string;
  /** The node the result `outType` stamps onto (a map/fanout node's `outType` is its ELEMENT type). */
  readonly outHost: { outType?: PortableType };
}

/**
 * Every `executeSQL`-bearing node in a component body — a TOP-LEVEL node, OR the single inner op of a
 * `.map`/fanout (a RETURNING-chained transaction: `INSERT … RETURNING id → id.map(id => INSERT …)`).
 * The transient-port lowering passes ({@link lowerRecordedWhere} / {@link lowerReadColumns}) iterate
 * BOTH via THIS one SSoT so a WHERE / RETURNING projection authored INSIDE a `.map` lowers exactly as a
 * top-level one does — a `.map` is COVERED control flow (typed field access on the covered plane), so
 * its inner statement is a first-class `executeSQL` node the passes must reach, not an opaque-wire leaf.
 * The map/fanout node's `outType` is its ELEMENT type (the node RESULT is `{arr: element}`), so a
 * map-inner RETURNING write stamps the SAME row type onto `outHost` a top-level one would.
 */
function executeSqlNodes(component: { name: string; body: readonly unknown[] }): ExecuteSqlNodeView[] {
  const out: ExecuteSqlNodeView[] = [];
  for (const node of component.body) {
    if ('cond' in (node as object)) continue;
    if ('map' in (node as object) || 'fanout' in (node as object)) {
      const inner = 'map' in (node as object)
        ? (node as { map: { component?: string; ports?: Record<string, unknown> } }).map
        : (node as { fanout: { component?: string; ports?: Record<string, unknown> } }).fanout;
      if (inner.component !== 'executeSQL' || inner.ports === undefined) continue;
      out.push({ ports: inner.ports, id: (node as { id: string }).id, outHost: node as { outType?: PortableType } });
    } else {
      const ref = node as { id?: string; component?: string; ports?: Record<string, unknown> };
      if (ref.component !== 'executeSQL' || ref.ports === undefined) continue;
      out.push({ ports: ref.ports, id: ref.id ?? '?', outHost: ref as { outType?: PortableType } });
    }
  }
  return out;
}

/**
 * The node ids consumed as a `.map`/fanout `over` SOURCE (a bare node ref `{ref:[id]}`). Such a source
 * is iterated with typed field access on the COVERED plane, so the native-passthrough lowering keeps it
 * de-boxed (typed) rather than opaque-wire — see {@link lowerNativePassthrough}. Only a single-segment
 * node ref is a covered map source; a deeper path / expression `over` is not a plain node result.
 */
function mapOverSources(component: { body: readonly unknown[] }): Set<string> {
  const ids = new Set<string>();
  for (const node of component.body) {
    const spec = 'map' in (node as object)
      ? (node as { map: { over?: unknown } }).map
      : 'fanout' in (node as object)
        ? (node as { fanout: { over?: unknown } }).fanout
        : undefined;
    const over = spec?.over as { ref?: unknown } | undefined;
    if (over !== undefined && Array.isArray(over.ref) && over.ref.length === 1 && typeof over.ref[0] === 'string') {
      ids.add(over.ref[0]);
    }
  }
  return ids;
}

/**
 * Lower each `executeSQL` node's transient RECORDED `where` port into the node's static `sql`/`params`
 * and strip it (#141). `where` was recorded by bc from the where sugar, so it is plain Expression IR
 * here — {@link lowerWherePort} (the SSoT WHERE lowering, over recorded IR) decodes eq/ne/cmp + every
 * sentinel form (IN/BETWEEN/LIKE/subquery/rawPredicate) without walking a live proxy. Bounded WHERE
 * (no SKIP) assembles to a literal ` WHERE … AND …` (the `?`→`$N` render is the leaf's runtime job).
 * A SKIP/dynamic fragment fails closed — dynamic-WHERE runtime assembly is a separate (unwired) path;
 * the predicate is never silently dropped. Reaches map-inner statements too (see {@link executeSqlNodes}).
 */
function lowerRecordedWhere(ir: ComponentGraphIR, dialect: Dialect): void {
  for (const component of ir.components) {
    for (const { ports, id } of executeSqlNodes(component)) {
      if (ports.where === undefined) continue;
      const at = `${component.name}/${id}`;
      // Lower per the model's dialect: the IN-list form (PG `= ANY(?)` vs MySQL/SQLite `json_each(?)`)
      // and per-column casts are dialect-specific; the `?`→`$N` render stays the leaf's runtime job.
      const fragments = lowerWherePort(ports, dialect, at);
      if (fragments.some((f) => f.skip !== undefined)) {
        // DYNAMIC WHERE (#143 SKIP restore): a SKIP fragment's presence is per-INPUT, so the WHERE is
        // assembled at RUNTIME by the executeSQL leaf ({@link import('./leaves').assembleDynamicWhere}).
        // Carry EVERY fragment as a bc expr in the `whereDynamic` plan — a bounded fragment as
        // `{obj:{sql,params}}`, a SKIP fragment wrapped in `{cond:[present, …, null]}` so bc's `cond`
        // LAZILY yields `null` (and never evaluates its params) when the guard is false. The base
        // `sql`/`params` (head + tail + LIMIT) stay intact; the leaf splices the surviving fragments in.
        const frags = fragments.map((f) => {
          const obj = { obj: { sql: f.sql, params: { arr: [...f.params] } } };
          // `f.skip` is `{not:[guard]}` (fragment dropped when truthy); present ⇔ `{not:[f.skip]}`.
          return f.skip !== undefined ? { cond: [{ not: [f.skip] }, obj, null] } : obj;
        });
        ports.whereDynamic = { obj: { frags: { arr: frags } } };
        delete ports.where;
        continue;
      }
      // BOUNDED WHERE: lower into the static `sql`/`params` at compile (native-lowerable). Splice at the
      // SQL position — BEFORE the first tail clause — via the SSoT {@link import('./leaves').spliceWhere}
      // (shared with the runtime dynamic assembler), never appended after the tail.
      let whereSql = '';
      const whereParams: unknown[] = [];
      fragments.forEach((f, i) => {
        whereSql += (i === 0 ? ' WHERE ' : ' AND ') + f.sql;
        whereParams.push(...f.params);
      });
      ports.sql = spliceWhere(String(ports.sql), whereSql);
      // The base carries NO tail-param when the tail is present here (a bounded `SELECT … WHERE … LIMIT`
      // inlines the literal limit — see emitRead; a write's SET params precede the WHERE and RETURNING
      // has no param), so appending the WHERE params is 1:1 with the `?` order in every bounded op.
      const paramsArr = (ports.params as { arr?: unknown[] } | undefined)?.arr;
      if (Array.isArray(paramsArr)) paramsArr.push(...whereParams);
      else ports.params = { arr: whereParams };
      delete ports.where;
    }
  }
}

/**
 * Lower each read `executeSQL` node's transient RECORDED `readColumns` port (base table + explicit
 * projection, stamped by `emitRead`) into the TS read-path de-box `materializers` map and strip it (#59).
 * {@link deriveReadMaterializers} (the SSoT read-column resolution, shared with the codegen `outType`
 * path) resolves each projected column against the model's `static columns` — fail-closed on a `*`
 * wildcard / an undeclared column (the #59 read-column COVERAGE GUARD), computed columns left raw. The
 * resulting `outputKey → MaterializeClass` map is stamped as the `materializers` port (BIGINT→string /
 * DATE→string / BOOLEAN→boolean); the `executeSQL` leaf applies it to the fetched rows at execute.
 */
function lowerReadColumns(ir: ComponentGraphIR, resolveColumnType: ColumnTypeResolver | undefined, className: string): void {
  for (const component of ir.components) {
    let stampedAny = false;
    for (const { ports, id, outHost } of executeSqlNodes(component)) {
      if (ports.readColumns === undefined) continue;
      const at = `${component.name}/${id}`;
      // A typed read REQUIRES the model's inline `static columns` (spec §4.1: bc never infers types; the
      // author annotates them). A model that declares NO columns cannot de-box → fail closed at
      // registration (never a silent raw / rounded-i64 read). A non-RETURNING write carries no `readColumns`.
      if (resolveColumnType === undefined) {
        throw new Error(
          `scp read-columns (#59): read behavior '${className}' projects explicit columns at ${at} but the ` +
            `model declares NO \`static columns\`. A typed read REQUIRES an inline \`static columns\` ` +
            `declaration (spec §4.1 — no-assume, no-fallback).`,
        );
      }
      // bc records the `{obj:{}}`-typed port as `{obj:{table, cols:{arr:[…]}}}`; unwrap to the raw
      // table string + projection list (the SoT `emitRead` carried).
      const rc = ports.readColumns as { obj?: { table?: unknown; cols?: unknown } };
      const meta = rc.obj ?? (rc as { table?: unknown; cols?: unknown });
      const table = meta.table;
      const colsNode = meta.cols as { arr?: unknown[] } | unknown[] | undefined;
      const cols = Array.isArray(colsNode) ? colsNode : colsNode?.arr;
      if (typeof table !== 'string' || !Array.isArray(cols)) {
        throw new Error(`scp read-columns (#59): malformed 'readColumns' port at ${at} (expected {table, cols}); the emitRead stamp is broken.`);
      }
      // ONE column resolution → the read ROW: `outType` (the SSoT row type — bc reads it for the NATIVE
      // typed de-box #154, and it is the TS conform target) AND the TS-leaf `materializers` coercion map
      // (the SAME resolution, so the coerced JS form always equals `outType`). The #59 coverage guard
      // fires here (throws on `*` / undeclared). Stamp the row type as the node's `outType`.
      const row = deriveReadRow(table, cols as string[], resolveColumnType, at);
      outHost.outType = row.outType;
      if (Object.keys(row.materializers).length > 0) ports.materializers = { obj: row.materializers };
      // A projected 64-bit int column must be read in exact-integer mode so the driver returns the
      // exact i64 (not a rounded double) BEFORE `materializeCell` renders it to a decimal string — the
      // #59 BIGINT-exact read. Flip the node's `bigint` port (sync sqlite `safeIntegers`; the async
      // PG/MySQL path de-boxes via per-connection driver config, so the flag is inert there).
      if (Object.values(row.materializers).includes('int64')) ports.bigint = true;
      delete ports.readColumns;
      stampedAny = true;
    }
    // Recompute the component `outputType` from the (now read-typed) node outTypes so it stays CONSISTENT
    // with the stamped read rows — bc's native emitter (#154) fail-closes on a comp.outputType that
    // disagrees with the node-result type. bc normally derives this at record time from `.as`, but the
    // read outType is resolved post-compile (no ambient resolver at authoring), so it is recomposed here
    // over the SAME `output` Φ-expression via the shared {@link composeOutputType} SSoT.
    if (stampedAny) {
      const byNode = new Map<string, PortableType>();
      for (const n of component.body) {
        if ('cond' in n) continue;
        const t = (n as { outType?: PortableType }).outType;
        if (t === undefined) continue;
        // A map/fanout node's `outType` is its ELEMENT type; the node RESULT is the list `{arr: element}`
        // (bc's type-gate SSoT: `"map" in n ? {arr:ot} : ot`). Store the RESULT type so a `{ref:[mapId]}`
        // in the output Φ composes to the list, not the element.
        const isList = 'map' in n || 'fanout' in n;
        byNode.set((n as { id: string }).id, isList ? { arr: t } : t);
      }
      (component as { output: unknown; outputType?: PortableType }).outputType = composeOutputType(
        (component as { output: unknown }).output,
        byNode,
        `component '${component.name}' output`,
      );
    }
  }
}

/**
 * The op-agnostic leaf cardinality SoT (`one` → single value result, `many` → list result) read from
 * the {@link LEAVES} `defineLeaf` annotations — the SAME catalog `behaviorComponents` derives from. A
 * `one` leaf's passthrough carries a bare opaque `'value'`; a `many` leaf's carries `{arr:'value'}`.
 */
const LEAF_CARDINALITY: Readonly<Record<string, 'one' | 'many'>> = Object.fromEntries(
  Object.entries(LEAVES).map(([name, def]) => [name, (def as { meta: { cardinality: 'one' | 'many' } }).meta.cardinality]),
);

/** The single body-node id a component's `output` Φ resolves to (its TERMINAL node). Fail-closed: the
 * native passthrough pass only supports a straight-line graph whose output is ONE node ref (every #141
 * op — the terminal read / the outermost `group`). A non-ref / multi-node output is not native-lowerable. */
function terminalNodeId(component: Component): string {
  const out = (component as { output?: unknown }).output as { ref?: unknown } | undefined;
  const ref = out?.ref;
  const id = Array.isArray(ref) && ref.length >= 1 && typeof ref[0] === 'string' ? ref[0] : undefined;
  if (id === undefined || !component.body.some((n) => (n as { id?: string }).id === id)) {
    throw new Error(
      `scp native-passthrough (#164): component '${component.name}' output is not a single body-node ref ` +
        `(${JSON.stringify(out)}); the native output-passthrough pass requires a straight-line graph with ` +
        `one terminal node. No-assume, no-fallback.`,
    );
  }
  return id;
}

/**
 * Native output-passthrough (#141 / bc#164) — the PUBLISH-layer transform that makes a lowered contract
 * native-clean for `generateModule` (rust/go typed-native). See {@link PublishBehaviorsOptions.nativePassthrough}.
 *
 * For each component, an INTERMEDIATE node whose result feeds a downstream op-agnostic LEAF (parent
 * read → `pluck` → child read → `group`, at every relation level) is flagged `wirePassthrough:true`
 * with an opaque-wire `outType` (`{arr:'value'}` for a `many` leaf, `'value'` for a `one` leaf) so bc
 * keeps the raw `WireValue`/`Vec<WireValue>` (NO de-box) for the leaf transports. The TERMINAL node
 * keeps its concrete typed outType. Two intermediates ALSO keep it: a node consumed as a `.map`/fanout
 * `over` SOURCE (a RETURNING-chained transaction: `INSERT … RETURNING id → id.map(id => INSERT …)`) —
 * a `.map` iterates a TYPED list with typed field access on the COVERED plane (it is covered control
 * flow, not an opaque-wire leaf), so its source must de-box, exactly like the terminal. The TS-only
 * `materializers` port is stripped from every node (the native emitter de-boxes from `outType`).
 *
 * This is expressed HERE (the single publish lowering core, twin of {@link lowerReadColumns}) — NOT as a
 * post-`generateModule` IR edit — so the serialized contract is native-clean by construction (SSoT: the
 * one place #164 passthrough is stamped).
 */
function lowerNativePassthrough(ir: ComponentGraphIR): void {
  for (const component of ir.components) {
    // B-1: prune the `@`-prefixed sentinel Input Ports. litedbmodel's WHERE sugar (`whereLike`/`whereIn`
    // /… — `authoring-sql.ts`) encodes structural metadata as `$[@x]` refs; bc faithfully derives an
    // Input Port per sentinel (typed `unknown`). `lowerRecordedWhere` CONSUMES the sentinel into the
    // static `sql`/`params`, so the derived `@x` port is now DEAD — but it survives on the component's
    // `inputPorts`, and the native emitter lowers `unknown` to a boxed `Value` field in the input struct
    // (`_like: Value` — an untyped-escape leak that breaks the native compile). It is a TS-only artifact
    // (the ir-exec path ignores the dead port), so it is pruned HERE, in the native-clean transform.
    const ports = (component as { inputPorts?: Record<string, unknown> }).inputPorts;
    if (ports !== undefined) for (const k of Object.keys(ports)) if (k.startsWith('@')) delete ports[k];
    // The TS ir-exec read-path de-box map is a TS-only concern — strip it from EVERY executeSQL node
    // (top-level AND map/fanout inner — {@link executeSqlNodes}) so no `materializers` survives native.
    for (const { ports: np } of executeSqlNodes(component)) delete np.materializers;
    const terminalId = terminalNodeId(component);
    // The node ids consumed as a `.map`/fanout `over` SOURCE — they de-box (typed) like the terminal.
    const typedSources = mapOverSources(component);
    for (const node of component.body) {
      if ('cond' in node) continue;
      const ref = node as { id?: string; component?: string; outType?: PortableType; wirePassthrough?: boolean; ports?: Record<string, unknown> };
      // the terminal AND a `.map` source keep their typed outType (the covered-plane de-boxes).
      if (ref.id === terminalId || (ref.id !== undefined && typedSources.has(ref.id))) continue;
      const componentName = 'map' in node ? (node as { map: { component: string } }).map.component : 'fanout' in node ? (node as { fanout: { component: string } }).fanout.component : ref.component;
      const cardinality = componentName !== undefined ? LEAF_CARDINALITY[componentName] : undefined;
      if (cardinality === undefined) {
        throw new Error(
          `scp native-passthrough (#164): node '${ref.id}' in '${component.name}' references unknown leaf ` +
            `'${componentName}' (not in the op-agnostic leaf catalog). No-assume, no-fallback.`,
        );
      }
      // Opaque-wire outType per cardinality: `many` → `{arr:'value'}` (Vec<WireValue>); `one` → `'value'`
      // (bare WireValue, e.g. `pluck`'s key array bound as ONE `= ANY($1)` param). bc's guard admits
      // exactly these two shapes for a `wirePassthrough:true` node ({@link wirePassthroughOutTypeOk}).
      ref.outType = cardinality === 'many' ? { arr: 'value' } : 'value';
      ref.wirePassthrough = true;
    }
    // The component `outputType` composes from the TERMINAL node's (unchanged) outType via the SAME
    // `output` Φ + the shared {@link composeOutputType} SSoT — the intermediate opaque outTypes never
    // enter it (the Φ references only the terminal), so it stays the terminal's concrete result type.
    const byNode = new Map<string, PortableType>();
    for (const n of component.body) {
      if ('cond' in n) continue;
      const t = (n as { outType?: PortableType }).outType;
      if (t === undefined) continue;
      const isList = 'map' in n || 'fanout' in n;
      byNode.set((n as { id: string }).id, isList ? { arr: t } : t);
    }
    (component as { output: unknown; outputType?: PortableType }).outputType = composeOutputType(
      (component as { output: unknown }).output,
      byNode,
      `component '${component.name}' output`,
    );
  }
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

// The component-graph node/component/graph types used for inspection are declared above.
