/**
 * litedbmodel v2 SCP — typed-object result + hydrate factory + lazy relations (WS4, #24).
 *
 * v2's read result is a plain **typed-object** (spec §4 / §12): its OWN enumerable props are
 * DATA ONLY (the projected columns + any declaratively-selected relations). It is NOT a
 * DBModel instance — `JSON.stringify` / `Object.keys` / spread stay clean (feasibility §9).
 *
 * The v1 method-like UX is recovered via a graphddb-shaped {@link hydrate} factory
 * (`(raw) => HostObject`): host-object construction is a CONSUMER concern that stays in the
 * litedbmodel runtime and NEVER leaks into the portable IR/bundle (per the runtime boundary —
 * the bundle carries the relation op + assembly shape only). `hydrate` is applied AFTER
 * relation resolution and is NOT applied to `null` (a null single-relation), matching
 * graphddb's `options.hydrate`.
 *
 * ## Lazy relations: prototype getter + non-enumerable Symbol batch context
 *
 * A typed-object gets a per-result-set prototype carrying a lazy getter per relation
 * (feasibility §9 "prototype で良い"). The getter, on access, reads a NON-ENUMERABLE Symbol
 * batch context (graphddb's `GRAPHDDB_KEY` technique — invisible to spread/JSON) shared by
 * every sibling in the result set, and fires the SAME pre-compiled relation op ONCE over the
 * whole sibling set (batch — so no N+1 even though it can't prefetch). A declaratively-selected
 * relation is attached as an OWN property (`Post[]`/`Author`) and naturally SHADOWS the
 * prototype getter (own prop wins) — the exact "宣言済み → データ / 未宣言 → lazy" unification
 * from feasibility §9.
 *
 * Both surfaces resolve through {@link import('./relation').runRelationOp} — the identical
 * compiled op — so there is one relation-execution code path, not two.
 */

import {
  runRelationOp,
  distributeToParent,
  type RelationOp,
  type RelationBatch,
  type RelationDriver,
} from './relation';

/** The non-enumerable Symbol under which the shared lazy batch context is stashed. */
export const RELATION_CONTEXT: unique symbol = Symbol('litedbmodel:relation-context');

/** A hydrate factory (graphddb-shaped): raw typed-object → host object. */
export type HydrateFactory<R> = (raw: Record<string, unknown>) => R;

/**
 * The shared lazy batch context for one result set. Holds the sibling rows, the driver, the
 * compiled relation ops (keyed by name), the (optional) hydrate factory, and a per-relation
 * memo so a relation is batched at most ONCE across all siblings (structural no-N+1 for lazy).
 * Stored as a non-enumerable Symbol prop so spread/JSON/`Object.keys` never see it (a spread
 * or clone drops it — "designed degradation", feasibility §9).
 */
export class RelationContext {
  private readonly siblings: Record<string, unknown>[];
  private readonly ops: Readonly<Record<string, RelationOp>>;
  private readonly db: RelationDriver;
  private readonly hydrate?: HydrateFactory<unknown>;
  /** CROSS-DB (V0 R1): name → driver registry; a tagged relation routes to `connections[tag]`. */
  private readonly connections?: Readonly<Record<string, RelationDriver>>;
  /** Per-relation resolved batch (name → grouping), memoized: one batch query per relation. */
  private readonly resolved = new Map<string, RelationBatch>();

  constructor(
    siblings: Record<string, unknown>[],
    ops: Readonly<Record<string, RelationOp>>,
    db: RelationDriver,
    hydrate?: HydrateFactory<unknown>,
    connections?: Readonly<Record<string, RelationDriver>>,
  ) {
    this.siblings = siblings;
    this.ops = ops;
    this.db = db;
    this.hydrate = hydrate;
    this.connections = connections;
  }

  /** The driver a relation runs against: its tagged cross-DB connection, else the primary `db`. */
  private driverFor(op: RelationOp): RelationDriver {
    return op.connection === undefined ? this.db : driverForOp(op, this.connections);
  }

  /**
   * Resolve `relationName` for `parent` (lazy access). The relation is batch-loaded across
   * ALL siblings on first access and memoized, so N sibling accesses issue ONE query. Returns
   * the hydrated child(ren) per cardinality (`hasMany` → array, else single-or-null).
   */
  resolve(relationName: string, parent: Record<string, unknown>): unknown {
    const op = this.ops[relationName];
    if (op === undefined) {
      throw new Error(`relation '${relationName}' is not declared on this result set`);
    }
    let batch = this.resolved.get(relationName);
    if (batch === undefined) {
      batch = runRelationOp(op, this.siblings, this.driverFor(op)).batch;
      this.resolved.set(relationName, batch);
    }
    return applyHydrate(distributeToParent(op, parent, batch), this.hydrate);
  }
}

/**
 * The driver a CROSS-DB relation op (V0 R1) runs against: the registered `connections[tag]`. Loud
 * failure when the tag has no registered driver (a real wiring bug — never a silent same-DB
 * fallback, which would run the target's query on the wrong DB). Only called for a tagged op.
 */
function driverForOp(op: RelationOp, connections: Readonly<Record<string, RelationDriver>> | undefined): RelationDriver {
  const d = connections?.[op.connection as string];
  if (d === undefined) {
    throw new Error(`cross-DB relation '${op.name}': no driver registered for connection '${op.connection}' (pass it in ReadOptions.connections)`);
  }
  return d;
}

/** Read the non-enumerable batch context off a typed-object (undefined if spread/cloned away). */
export function readRelationContext(o: Record<string, unknown>): RelationContext | undefined {
  const v = (o as Record<symbol, unknown>)[RELATION_CONTEXT];
  return v instanceof RelationContext ? v : undefined;
}

/** Apply the hydrate factory to a resolved relation value (skip `null`; map arrays). */
function applyHydrate(value: unknown, hydrate?: HydrateFactory<unknown>): unknown {
  if (hydrate === undefined) return value;
  if (value === null) return null; // graphddb: hydrate is NOT applied to null
  if (Array.isArray(value)) return value.map((v) => (v === null ? null : hydrate(v as Record<string, unknown>)));
  return hydrate(value as Record<string, unknown>);
}

/**
 * Read options bag (graphddb-shaped): declaratively-select relations to prefetch and/or a
 * `hydrate` factory to recover host objects.
 */
export interface ReadOptions<R = Record<string, unknown>> {
  /**
   * Declarative select: relation name → `true`. Selected relations are batch-prefetched
   * (staged) and attached as OWN props (shadowing the lazy getter). Unselected relations stay
   * lazy on the prototype.
   */
  readonly with?: Readonly<Record<string, true>>;
  /** Host-object factory applied AFTER relation resolution (not applied to `null`). */
  readonly hydrate?: HydrateFactory<R>;
  /**
   * CROSS-DB relations (V0 R1): connection-name → driver registry. A relation whose compiled op
   * carries a `connection` tag (its target model lives in a DIFFERENT DB — v1
   * `TargetClass.getDriverType()`) is batch-loaded against `connections[tag]` instead of the
   * primary `db`. Untagged (same-DB) relations ignore this. Omit for a single-DB read.
   */
  readonly connections?: Readonly<Record<string, RelationDriver>>;
}

/**
 * Build the typed-object result set for a page of raw child rows plus the relation ops
 * declared for this model. Every raw row becomes a typed-object whose:
 *   - OWN enumerable props = the raw data columns (+ any declaratively-selected relation);
 *   - PROTOTYPE carries a lazy getter per UNSELECTED relation (fires the same op at access);
 *   - non-enumerable {@link RELATION_CONTEXT} Symbol = the shared lazy batch context.
 *
 * Declaratively-selected relations (`options.with`) are batch-prefetched HERE (one query per
 * relation over the whole page — staged, no N+1) via the SAME {@link runRelationOp}, and
 * attached as own props (so they shadow the prototype getter). `hydrate`, when supplied, is
 * applied to each parent row AND to resolved relations.
 *
 * Returns the typed-objects (hydrated if `options.hydrate` given). This is the litedbmodel
 * runtime concern; the bundle carries only the relation ops + assembly shape (no hydrate).
 */
export function buildResultSet<R = Record<string, unknown>>(
  rawRows: readonly Record<string, unknown>[],
  ops: Readonly<Record<string, RelationOp>>,
  db: RelationDriver,
  options: ReadOptions<R> = {},
): R[] {
  const selected = options.with ?? {};
  const hydrate = options.hydrate as HydrateFactory<unknown> | undefined;

  // The prototype carries a lazy getter for every relation NOT declaratively selected. A
  // selected relation is an own prop, so it shadows the getter (feasibility §9 unification).
  const proto: Record<string, unknown> = {};
  for (const name of Object.keys(ops)) {
    if (selected[name] === true) continue;
    Object.defineProperty(proto, name, {
      enumerable: false,
      configurable: true,
      get(this: Record<string, unknown>): Promise<unknown> {
        const ctx = readRelationContext(this);
        if (ctx === undefined) {
          return Promise.reject(
            new Error(
              `lazy relation '${name}': the batch context was dropped (spread/clone/JSON); ` +
                `re-read through the query API or declaratively select it via { with: { ${name}: true } }`,
            ),
          );
        }
        // v1 parity: `await post.author` yields a Promise (getter → relation-op launch,
        // feasibility §9). The batch itself is synchronous (better-sqlite3) but memoized
        // across siblings, so N accesses still issue ONE query (no N+1).
        return Promise.resolve(ctx.resolve(name, this));
      },
    });
  }

  // The raw typed-objects (own props = data only), sharing `proto`.
  const rows: Record<string, unknown>[] = rawRows.map((raw) => {
    const o: Record<string, unknown> = Object.create(proto);
    for (const k of Object.keys(raw)) o[k] = raw[k];
    return o;
  });

  // The shared lazy batch context (non-enumerable Symbol) — the whole page is the sibling set.
  const ctx = new RelationContext(rows, ops, db, hydrate, options.connections);
  for (const o of rows) {
    Object.defineProperty(o, RELATION_CONTEXT, {
      value: ctx,
      enumerable: false,
      writable: false,
      configurable: false,
    });
  }

  // Declarative select: batch-prefetch each selected relation ONCE (staged) and attach as an
  // own prop (shadowing the lazy getter). SAME runRelationOp — identical compiled op + SQL.
  for (const name of Object.keys(selected)) {
    const op = ops[name];
    if (op === undefined) throw new Error(`declarative select: relation '${name}' is not declared on this model`);
    // CROSS-DB (V0 R1): a tagged relation batches against its own connection; else the primary db.
    const relDb = op.connection === undefined ? db : driverForOp(op, options.connections);
    const { batch } = runRelationOp(op, rows, relDb);
    for (const o of rows) {
      o[name] = applyHydrate(distributeToParent(op, o, batch), hydrate);
    }
  }

  if (hydrate === undefined) return rows as unknown as R[];
  return rows.map((o) => hydrate(o)) as R[];
}
