/**
 * litedbmodel v2 SCP — write-time relations declaration vocabulary (WS5, #25; spec §6 / §2.2).
 *
 * `entityWrites` / `edgeWrites` declare the **write-side invariants / effects required for
 * an entity to stay consistent** when it is created / updated / removed — the SQL-backend
 * counterpart of graphddb's `entityWrites` save contract (`graphddb/src/define/entity-writes.ts`,
 * same shape: a per-lifecycle map of `w.lifecycle({requires,unique,derive,edges,emits,idempotency})`).
 * This module produces a PORTABLE declaration (pure data, no closures): every value that
 * references the write is a **path-rooted string** (`$.input.<field>` = the mutation input,
 * `$.entity.<field>` = the written row's RETURNING result). {@link ../write-plan} lowers the
 * declaration + a base write op into an ordered, gate-first SQL transaction plan (§6 table).
 *
 * ## Path roots (spec §6 example) — the SSoT for every derived value
 *
 * A leaf value binds to an explicit root so its source is unambiguous and no engine-code
 * fallback is ever needed (hard rule: defaults live in the declaration):
 *   - `$.input.<field>`  — the Command's input port value (bc flat scope: `{ref:['<field>']}`).
 *   - `$.entity.<field>` — the just-written row (the body write's RETURNING row), exposed to
 *     the derive/edges/emits stages under the reserved `{@link ENTITY_ROOT}` binding.
 *
 * The declaration is INITIAL-scope (spec §6 / §13): single-statement Command + fixed-order
 * write-time relations. It does NOT model the complex DAG derivation (WS8).
 */

/** The reserved scope binding under which the body write's RETURNING row is exposed (`$.entity.*`). */
export const ENTITY_ROOT = '__entity';

/**
 * The path root a write-relation value binds from:
 *   - `input`  — the Command input port (bc flat scope).
 *   - `entity` — the SOLE body write's RETURNING row (the WS5 single-base-write shorthand),
 *     bound under {@link ENTITY_ROOT}.
 *   - `ref`    — a NAMED upstream write's RETURNING row (`$.ref.<writeName>.<field>`, WS8a
 *     composite/DAG scope). `entity` is exactly `ref` targeting the sole base write; keeping
 *     it distinct preserves the WS5 vocabulary while composite writes address writes by name.
 */
export type PathRoot = 'input' | 'entity' | 'ref';

/** A parsed, path-rooted write-relation value (`$.input.x` / `$.entity.x` / `$.ref.w.x`). */
export interface EffectPath {
  readonly root: PathRoot;
  /**
   * For `ref`: the upstream write's NAME (the statement whose RETURNING row is referenced).
   * Absent for `input` / `entity` (those have a single implicit source).
   */
  readonly writeName?: string;
  /** The field name after the root (a single physical column; no nested paths in α). */
  readonly field: string;
}

const PATH_RE = /^\$\.(input|entity)\.([A-Za-z_][A-Za-z0-9_]*)$/;
const REF_PATH_RE = /^\$\.ref\.([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$/;

/**
 * Parse a path-rooted write-relation value string into an {@link EffectPath}. Fail-closed on
 * a malformed path — a write-relation value MUST be `$.input.<field>`, `$.entity.<field>`, or
 * `$.ref.<writeName>.<field>` (no free-form paths, no implicit root), so a typo is a loud build
 * error, never a silent default (hard rule).
 */
export function parseEffectPath(value: string): EffectPath {
  const m = PATH_RE.exec(value);
  if (m !== null) return { root: m[1] as PathRoot, field: m[2] };
  const r = REF_PATH_RE.exec(value);
  if (r !== null) return { root: 'ref', writeName: r[1], field: r[2] };
  throw new Error(
    `write-time relations: '${value}' is not a valid path-rooted value; use ` +
      `'$.input.<field>' (the Command input), '$.entity.<field>' (the sole written row), or ` +
      `'$.ref.<writeName>.<field>' (a named upstream write's RETURNING row).`,
  );
}

// ── The §6 effect vocabulary (path-rooted, pure data) ─────────────────────────

/**
 * A referential-integrity requirement (§6 `requires`): the named `table` must contain a row
 * whose columns equal the bound keys. Lowered to a gate-first `SELECT 1 FROM <table> WHERE …`
 * existence guard that ROLLBACKs when absent (spec §6 example line 1).
 */
export interface RequiresEffect {
  readonly kind: 'requires';
  /** The table whose row must exist. */
  readonly table: string;
  /** target column → path-rooted source value (the existence key). */
  readonly keys: Readonly<Record<string, string>>;
}

/**
 * A uniqueness guard (§6 `unique`): the combined `fields` must be unique within `scope`.
 * Lowered to a guard-row `INSERT INTO <guardTable> … ON CONFLICT DO NOTHING` whose affected-
 * row count is checked (0 rows ⇒ collision ⇒ ROLLBACK) — the SQL idiom of §6 (`ON CONFLICT
 * DO NOTHING` + affected 検査).
 */
export interface UniqueEffect {
  readonly kind: 'unique';
  /** The guard-row name (its discriminator column value in the guard table). */
  readonly name: string;
  /** The guard table that materializes the uniqueness rows. */
  readonly guardTable: string;
  /** The scope the uniqueness is partitioned by (path-rooted values). */
  readonly scope: readonly string[];
  /** The fields whose combined value must be unique within the scope (path-rooted values). */
  readonly fields: readonly string[];
}

/**
 * A derived / cascading counter update (§6 `derive`): `SET <attribute> = <attribute> ± amount`
 * on the `table` row keyed by `keys`. Lowered to `UPDATE <table> SET c = c + :amount WHERE …`
 * (spec §6 example line "derive"). `amount` is the SSoT for the increment — never a code default.
 */
export interface DeriveEffect {
  readonly kind: 'derive';
  /** The table whose counter attribute is derived-updated. */
  readonly table: string;
  /** target column → path-rooted source value (the updated row's key). */
  readonly keys: Readonly<Record<string, string>>;
  /** The counter column to increment. */
  readonly attribute: string;
  /** The signed amount added to the counter (the declaration IS the source of truth). */
  readonly amount: number;
}

/**
 * An edge write (§6 `edges`). A many-to-many edge materializes as an intermediate-table
 * `INSERT` (create) / `DELETE` (remove); a one-to-many edge as a foreign-key column `UPDATE`
 * on the related table. Lowered per {@link EdgeEffect.relation} (spec §6 table row `edges`).
 */
export interface EdgeEffect {
  readonly kind: 'edge';
  /** `m2m` → intermediate-table INSERT/DELETE; `fk` → related-table FK column UPDATE. */
  readonly relation: 'm2m' | 'fk';
  /** The write action for this lifecycle: `set` (create/link) or `unset` (remove/unlink). */
  readonly action: 'set' | 'unset';
  /**
   * For `m2m`: the intermediate (join) table. For `fk`: the related table whose FK column is set.
   */
  readonly table: string;
  /**
   * For `m2m`: the join-row columns → path-rooted source values (both endpoint keys).
   * For `fk`: the FK column → path-rooted source value AND the matched-row key columns.
   */
  readonly columns: Readonly<Record<string, string>>;
  /**
   * For `fk` only: the WHERE key columns (matched-row identity) → path-rooted source values.
   * `columns` then holds ONLY the FK column(s) to SET. For `m2m` this is absent.
   */
  readonly where?: Readonly<Record<string, string>>;
}

/**
 * A domain event (§6 `emits`): an outbox `INSERT INTO <outboxTable>(type, payload)` in the
 * SAME transaction (spec §6 example line "emits"). The payload is a path-rooted map serialized
 * to a JSON text column at render time.
 */
export interface EmitEffect {
  readonly kind: 'emit';
  /** The event type discriminator (e.g. `PostCreated`). */
  readonly name: string;
  /** The outbox table the event row is inserted into. */
  readonly outboxTable: string;
  /** The event payload: field → path-rooted source value. */
  readonly payload: Readonly<Record<string, string>>;
}

/**
 * An idempotency guard (§6 `idempotency`): a client-token guard `INSERT INTO <table>(token)`
 * whose UNIQUE violation detects a duplicate request (spec §6 example line "idempotency").
 * Runs gate-first: a duplicate token short-circuits the whole transaction (no double write).
 */
export interface IdempotencyEffect {
  readonly kind: 'idempotency';
  /** The idempotency-key table (a single UNIQUE token column). */
  readonly table: string;
  /** The token column name. */
  readonly column: string;
  /** The path-rooted client-token value (e.g. `$.input.request_id`). */
  readonly token: string;
}

/**
 * The §6 effect set a {@link LifecycleContract} carries. Every field is optional; an omitted
 * field is the empty effect set. Fixed derivation order (spec §6 example): gate-first
 * (requires → idempotency → unique), body, then derive → edges → emits.
 */
export interface LifecycleEffects {
  /** Referential-integrity requirements (→ gate-first existence guard). */
  readonly requires?: readonly RequiresEffect[];
  /** Idempotency guard (→ gate-first token INSERT; duplicate short-circuits). */
  readonly idempotency?: IdempotencyEffect;
  /** Uniqueness guards (→ gate-first guard-row INSERT ON CONFLICT DO NOTHING + affected check). */
  readonly unique?: readonly UniqueEffect[];
  /** Derived / cascading counter updates (→ UPDATE … SET c = c ± n). */
  readonly derive?: readonly DeriveEffect[];
  /** Edge writes (→ M:N intermediate INSERT/DELETE or 1:N FK UPDATE). */
  readonly edges?: readonly EdgeEffect[];
  /** Domain events (→ outbox INSERT, same tx). */
  readonly emits?: readonly EmitEffect[];
}

/** One lifecycle's save contract — the §6 effect set for a single phase. */
export interface LifecycleContract {
  readonly effects: LifecycleEffects;
}

/** The lifecycle phase a save-contract entry declares (the Command intent selects it). */
export type WriteLifecyclePhase = 'create' | 'update' | 'remove';

/** A model's per-lifecycle save contract (`entityWrites(...)` result). */
export interface EntityWritesDefinition {
  readonly create?: LifecycleContract;
  readonly update?: LifecycleContract;
  readonly remove?: LifecycleContract;
}

// ── The recorder (mirrors graphddb's WriteRecorder, SQL flavor) ────────────────

/**
 * The recorder handed to the {@link entityWrites} / {@link edgeWrites} builder. Each method
 * builds ONE §6 effect (pure data). `w.lifecycle({...})` gathers the effect arrays into a
 * {@link LifecycleContract}. Every path-rooted value is validated eagerly ({@link parseEffectPath}).
 */
export interface WriteRecorder {
  /** Build one lifecycle's save contract from its §6 effect arrays (each field optional). */
  lifecycle(effects?: LifecycleEffects): LifecycleContract;

  /** `requires` — a referential-integrity existence guard (§6). */
  exists(table: string, keys: Readonly<Record<string, string>>): RequiresEffect;

  /** `unique` — a composite-scope uniqueness guard (§6). */
  unique(spec: {
    readonly name: string;
    readonly guardTable: string;
    readonly scope: readonly string[];
    readonly fields: readonly string[];
  }): UniqueEffect;

  /** `derive` — a cascade counter increment (§6). `amount` is the SSoT (no code default). */
  increment(
    table: string,
    keys: Readonly<Record<string, string>>,
    attribute: string,
    amount: number,
  ): DeriveEffect;

  /** `edges` — a many-to-many intermediate-table link/unlink (§6). */
  edge(spec: {
    readonly relation: 'm2m';
    readonly action: 'set' | 'unset';
    readonly table: string;
    readonly columns: Readonly<Record<string, string>>;
  }): EdgeEffect;
  /** `edges` — a one-to-many foreign-key column set/unset on a related row (§6). */
  edge(spec: {
    readonly relation: 'fk';
    readonly action: 'set' | 'unset';
    readonly table: string;
    readonly columns: Readonly<Record<string, string>>;
    readonly where: Readonly<Record<string, string>>;
  }): EdgeEffect;

  /** `emits` — a domain event into an outbox table, same tx (§6). */
  event(name: string, outboxTable: string, payload: Readonly<Record<string, string>>): EmitEffect;

  /** `idempotency` — a client-token duplicate guard (§6). */
  idempotentBy(table: string, column: string, token: string): IdempotencyEffect;
}

/** Assert every value in a path-rooted map parses (fail-closed on a malformed path). */
function assertPaths(map: Readonly<Record<string, string>>): void {
  for (const v of Object.values(map)) parseEffectPath(v); // throws on malformed
}

const recorder: WriteRecorder = {
  lifecycle(effects: LifecycleEffects = {}): LifecycleContract {
    // Validate every declared effect's paths eagerly (build-time fail-closed).
    for (const r of effects.requires ?? []) assertPaths(r.keys);
    if (effects.idempotency !== undefined) parseEffectPath(effects.idempotency.token);
    for (const u of effects.unique ?? []) {
      u.scope.forEach((p) => parseEffectPath(p));
      u.fields.forEach((p) => parseEffectPath(p));
    }
    for (const d of effects.derive ?? []) assertPaths(d.keys);
    for (const e of effects.edges ?? []) {
      assertPaths(e.columns);
      if (e.where !== undefined) assertPaths(e.where);
    }
    for (const em of effects.emits ?? []) assertPaths(em.payload);
    return { effects: freezeEffects(effects) };
  },
  exists(table, keys): RequiresEffect {
    return { kind: 'requires', table, keys };
  },
  unique(spec): UniqueEffect {
    return { kind: 'unique', name: spec.name, guardTable: spec.guardTable, scope: spec.scope, fields: spec.fields };
  },
  increment(table, keys, attribute, amount): DeriveEffect {
    if (!Number.isFinite(amount)) {
      throw new Error(`entityWrites: increment amount for '${table}.${attribute}' must be a finite number (got ${String(amount)})`);
    }
    return { kind: 'derive', table, keys, attribute, amount };
  },
  edge(spec: {
    relation: 'm2m' | 'fk';
    action: 'set' | 'unset';
    table: string;
    columns: Readonly<Record<string, string>>;
    where?: Readonly<Record<string, string>>;
  }): EdgeEffect {
    if (spec.relation === 'fk' && spec.where === undefined) {
      throw new Error(`entityWrites: a 'fk' edge on '${spec.table}' requires a 'where' key map (the matched row's identity)`);
    }
    return {
      kind: 'edge',
      relation: spec.relation,
      action: spec.action,
      table: spec.table,
      columns: spec.columns,
      ...(spec.where !== undefined ? { where: spec.where } : {}),
    };
  },
  event(name, outboxTable, payload): EmitEffect {
    return { kind: 'emit', name, outboxTable, payload };
  },
  idempotentBy(table, column, token): IdempotencyEffect {
    return { kind: 'idempotency', table, column, token };
  },
};

/** Keep only the present effect fields (an empty `w.lifecycle()` carries an empty set). */
function freezeEffects(effects: LifecycleEffects): LifecycleEffects {
  const out: { -readonly [K in keyof LifecycleEffects]: LifecycleEffects[K] } = {};
  if (effects.requires !== undefined) out.requires = effects.requires;
  if (effects.idempotency !== undefined) out.idempotency = effects.idempotency;
  if (effects.unique !== undefined) out.unique = effects.unique;
  if (effects.derive !== undefined) out.derive = effects.derive;
  if (effects.edges !== undefined) out.edges = effects.edges;
  if (effects.emits !== undefined) out.emits = effects.emits;
  return out;
}

/** The per-lifecycle map an {@link entityWrites} builder returns. */
export interface EntityWritesShape {
  readonly create?: LifecycleContract;
  readonly update?: LifecycleContract;
  readonly remove?: LifecycleContract;
}

/**
 * Declare a model's reusable write-time-relations save contract (spec §2.2 / §6): a
 * per-lifecycle map `{ create?, update?, remove? }`, each value a `w.lifecycle({...})`
 * carrying the §6 effect arrays. Stored on the model as
 * `static readonly writes = entityWrites<Model>((w) => ({ … }))`.
 *
 * @typeParam M documentary model type (the recorder is structural).
 * @throws if no lifecycle is declared, or a lifecycle is not built with `w.lifecycle(...)`.
 */
export function entityWrites<M = unknown>(
  builder: (w: WriteRecorder) => EntityWritesShape,
): EntityWritesDefinition {
  void (undefined as M | undefined);
  const shape = builder(recorder);
  if (shape.create === undefined && shape.update === undefined && shape.remove === undefined) {
    throw new Error(
      'entityWrites(...) must declare at least one lifecycle (`create` / `update` / `remove`).',
    );
  }
  return {
    ...(shape.create !== undefined ? { create: shape.create } : {}),
    ...(shape.update !== undefined ? { update: shape.update } : {}),
    ...(shape.remove !== undefined ? { remove: shape.remove } : {}),
  };
}

/**
 * Declare an adjacency entity's edge-only write contract (spec §2.2 / §6 — the `edgeWrites`
 * counterpart of graphddb's #82). It is a thin specialization of {@link entityWrites} whose
 * lifecycles carry ONLY `edges` effects — the write side of a many-to-many / one-to-many
 * relation entity. Kept distinct so an author signals intent (an edge entity vs a full
 * save contract), matching graphddb's two coexisting `writes` forms.
 *
 * @throws if a lifecycle declares a non-edge effect (an edge entity has only edge writes).
 */
export function edgeWrites<M = unknown>(
  builder: (w: WriteRecorder) => EntityWritesShape,
): EntityWritesDefinition {
  const def = entityWrites<M>(builder);
  for (const phase of ['create', 'update', 'remove'] as const) {
    const c = def[phase];
    if (c === undefined) continue;
    const e = c.effects;
    if (e.requires !== undefined || e.unique !== undefined || e.derive !== undefined || e.emits !== undefined || e.idempotency !== undefined) {
      throw new Error(
        `edgeWrites: the '${phase}' lifecycle may declare ONLY 'edges' effects ` +
          `(an edge entity carries only edge writes; use entityWrites for a full save contract).`,
      );
    }
  }
  return def;
}

/** Resolve the {@link LifecycleContract} for a Command intent, or undefined if not declared. */
export function lifecycleFor(
  def: EntityWritesDefinition,
  phase: WriteLifecyclePhase,
): LifecycleContract | undefined {
  return def[phase];
}
