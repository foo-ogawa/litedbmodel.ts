/**
 * litedbmodel v2 SCP — mode-3 codegen (spec §9 exec-mode 3 / §10 / §11; makeSQL flip, epic #43/#45).
 *
 * The static-codegen execution mode ("Codegen・静的（全言語）— IR → 各言語ソース生成 runtime≈0" —
 * spec §9). It takes a §8 STATIC makeSQL {@link SqlBundle} and, per target language, emits:
 *
 *  1. **Behavior module** — bc's SHARED generator run over the ONE SQL-baking lowering
 *     ({@link lowerReadGraphForNativeSql}): each read/write node's rendered per-dialect SQL is baked
 *     into the module as a native string LITERAL (`f_sql`) with typed param ports (scalar / optional /
 *     coalesce-default / IN-list array / skip-fragment / map element-field). The module carries its
 *     own query — it needs NO runtime JSON read. Covered by bc's typed-NATIVE endpoint
 *     (`rust-typed-native` / `go-typed-native`) for go/rust and `typescript-typed` for ts; every
 *     language consumes the SAME baked-SQL ports. There is ONE lowering and NO opt-in flag.
 *  2. **Runtime-stitch sidecar** — the pure-JSON `relations` batch ops (a belongsTo/hasMany prefetch
 *     is ONE batched IN query the runtime stitches, not baked into the read module) + the
 *     write-Command `transaction` plan. The read/write PRIMARY SQL is NOT here (baked in the module);
 *     the JSON `SqlCatalogCompanion` is RETIRED for reads.
 *
 * ## Codegen-path only; behavior-identical to the thin-runtime (mode-2), by construction
 *
 * The lowering builds a NEW, separate `ComponentGraphIR` from the bundle's `readGraph`; it never
 * mutates `SqlBundle`/`ReadGraph` and never touches the shared `executeReadGraph` (or the frozen
 * conformance corpus). The baked SQL is rendered through the SAME `composeMakeSQL`/`renderPlaceholders`
 * assembly the mode-2 runtime uses, so the module's query text is byte-identical to what
 * {@link executeBundle} renders. The TS leg proves result-equality vs {@link executeBundle}.
 *
 * ## Fail-closed, no boxed fallback
 *
 * A shape the lowering cannot bake natively (a `cond` node, a SKIP-guarded `map` child) THROWS
 * {@link TypedNativeCoverageError}, naming the exact gap — never a silent boxed escape.
 */

import { generateModule, loadCompiledIR, type GeneratedModule, type Scope, type Value, type PortSchema } from 'behavior-contracts';
// bc 0.8.0: litedbmodel's codegen IR is DERIVED (lowered from a compiled read graph) — an UNBRANDED
// structural doc. It is re-adopted into the branded compile-seam handle via `loadCompiledIR` right
// before `generateModule` (which fail-closes on un-tokened IR, SA3/SA7). The node/component types
// here are the unbranded structural shapes (see `./authoring`).
import type { Component, ComponentGraphIR, ModelColumns } from './authoring';
import { compileEager } from './authoring';
import type { SqlBundle, SqliteDb } from './runtime';
import { compileBundle, executeBundle } from './runtime';
import type { DialectName } from './dialect';
import type { RelationOp } from './relation';
import type { ReadGraph, StaticStatement } from './makesql/static-bundle';
import type { TransactionPlan } from './makesql/tx';
import { IN_SENTINEL, mysqlPkHint, pkPort } from './makesql/tx';
import { composeMakeSQL, type MakeSQL, type SqlParam } from './makesql/makesql';
import { sqlTypeToBcScalar, type BcScalar, type ColumnTypeResolver } from './coltype';

/**
 * The languages litedbmodel mode-3 codegen supports — exactly the set bc's shared generator
 * registers (bc#13 SP1 typescript/python + SP2 go/rust/php). {@link codegenEmitterFor}
 * verifies against the LIVE registry so a bc capability drift is caught loudly.
 */
export const CODEGEN_LANGUAGES = ['typescript', 'python', 'go', 'rust', 'php'] as const;
export type CodegenLanguage = (typeof CODEGEN_LANGUAGES)[number];

/**
 * The bc READ emitter litedbmodel drives for each codegen target (#60 milestone 1): the
 * **de-interpreted, RUNTIME-FREE typed-NATIVE** endpoint (bc#77/#90) for go/rust — ZERO boxed
 * `Value`/`RawValue`, ZERO IR baked into the module, concrete `HandlerNR`/`InNR`/`PortsNR`/
 * `RawRowNR` structs. There is **NO literal / straight-line / boxed-typed fallback** for a read: an
 * unspec'd fallback is INVALID (it silently swallows the "this shape can't be natively codegen'd"
 * signal) — a read shape typed-native does not cover MUST fail loudly at generation, naming the gap
 * (report it as a bc#86 coverage gap), never silently regenerate on `-typed`/`-typed-raw`.
 *
 * `-typed-raw` (bc#76, the boxed-wire intermediate) and `-typed` (the boxed-Value intermediate) are
 * RETIRED from litedbmodel's codegen surface entirely (#60 milestone 1) — typed-native supersedes
 * both for every read shape it covers (bc#77/#90: single-componentRef reads AND the `map`
 * relationKind:single|connection shape, bc#86 part 2).
 *
 * - ts: **no typed-native endpoint registered yet** (bc has not shipped `typescript-typed-native`).
 *   TS stays on its existing `typescript-typed` (boxed) endpoint until bc adds one — a DECLARED,
 *   reported gap (not silently mirrored to go/rust's retirement), tracked as a bc#77 TS follow-up.
 * - **python/php: NOT present.** bc registers no de-boxed typed endpoint for them (capability limit).
 *   litedbmodel does NOT substitute the literal (≈ir) emitter — that would be an unspec'd fallback.
 *   Codegen for py/php is a bc capability gap that ERRORS (ESCALATE to bc), never a silent literal.
 *
 * WRITES (batchInsert / gate-first tx) are NOT reads — they are NOT routed through this table at
 * all (#60 milestone 1): a write bundle stays on the existing write/tx execution path
 * (`executeTransactionBundle` / the native adapter's hand-written tx mirror), never a codegen
 * module, boxed or typed-raw. {@link generateCodegenArtifact} throws if asked to codegen a write.
 */
export const CODEGEN_EMITTER: Partial<Record<CodegenLanguage, string>> = {
  typescript: 'typescript-typed',
  go: 'go-typed-native',
  rust: 'rust-typed-native',
};

/**
 * The SQL catalog companion sidecar. The read/write PRIMARY SQL is now BAKED into the generated
 * module (the single SQL-baking lowering), so the companion NO LONGER carries the read graph — the
 * read path is companion-free (owner: "retire the JSON SqlCatalogCompanion for reads"). What remains
 * is the runtime-stitched surface the module does NOT bake: the `relations` batch ops (a RelationDecl
 * belongsTo/hasMany prefetch is ONE batched IN query resolved by the runtime, not part of the read
 * module) and the write-Command `transaction` plan (the gate-first tx-DAG, not codegen'd). Pure JSON.
 */
export interface SqlCatalogCompanion {
  /** Target SQL dialect (`sqlite`/`postgres`/`mysql`) — compiled once, TS-side (spec §10). */
  readonly dialect: DialectName;
  /** Input heads normalized to present-as-null (absent-key SKIP) — mirrors the bundle. */
  readonly optionalHeads: readonly string[];
  /** Pre-compiled STATIC read-relation batch ops (spec §5/§8), keyed by relation name — the ONE
   * batched IN query per relation the runtime stitches (not baked into the read module). */
  readonly relations: Record<string, RelationOp>;
  /** Derived gate-first write-time-relations transaction plan (spec §6/§8), for a Command bundle. */
  readonly transaction?: TransactionPlan;
  /**
   * WRITE bundles: the codegen typed-de-box `outputType` (the TransactionResult typed shape). Rides
   * the companion so a codegen consumer that reassembles the bundle round-trips it losslessly.
   */
  readonly outputType?: unknown;
}

/** The full codegen artifact for one bundle × one language. */
export interface CodegenArtifact {
  /** The target language. */
  readonly language: CodegenLanguage;
  /** The generated behavior module (bc's SHARED STRAIGHT-LINE generator output — real static code). */
  readonly module: GeneratedModule;
  /** The SQL catalog companion sidecar (the STATIC makeSQL execution catalog). */
  readonly companion: SqlCatalogCompanion;
  /** The originating bundle (so the equivalence leg re-executes the SAME static bundle). */
  readonly bundle: SqlBundle;
}


/** The runtime-stitched sidecar carried alongside the generated module — NO read/write SQL (baked in
 * the module now), only the relation batch ops + the write-Command tx plan the module does not bake. */
function companionOf(bundle: SqlBundle): SqlCatalogCompanion {
  const base: SqlCatalogCompanion = {
    dialect: bundle.dialect,
    optionalHeads: bundle.optionalHeads,
    relations: bundle.relations,
    ...(bundle.outputType !== undefined ? { outputType: bundle.outputType } : {}),
  };
  return bundle.transaction === undefined ? base : { ...base, transaction: bundle.transaction };
}

// ═══════════════════════════════════════════════════════════════════════════════════════════
// #60 milestone 1 — CODEGEN-ONLY typed-native lowering (#12: from the real-node read graph).
//
// bc's typed-native coverage predicate (`coverageRejectReason`, bc#86/#89) requires EVERY
// componentRef port to be STATICALLY resolvable to a native scalar/string-array/number-literal.
// The de-surrogated read graph's real `Select`/`Count`/map nodes carry boxed authoring ports
// (`where:{arr:[…]}`, `table`, `select`) that are not that shape, so bc would hard-fail on them.
// This lowering rebuilds a NEW, CODEGEN-ONLY component whose body nodes carry ONLY individual
// native-scalar `{ref:[head]}` ports (one per input head the node's compiled statements bind,
// derived from `statementsById`) + typed `inputPorts` — the shape bc's typed-native emitter needs.
//
// This is a NEW, throwaway `ComponentGraphIR` built FROM the bundle's `readGraph` fields
// (`ir` topology + `statementsById`, both already computed by the EXISTING `compileReadGraph` —
// untouched here) — it is never written back onto `SqlBundle`/`ReadGraph`, never serialized, and
// the native exec surface (`executeReadGraph`) never sees it (it walks the real-node `readGraph.ir`
// directly). Purely a generation-time input to `generateModule`.
// ═══════════════════════════════════════════════════════════════════════════════════════════

/**
 * Build a component's typed `inputPorts` from the heads the lowering collected. Kept as one helper so
 * a bug fix / a new port kind lands in ONE place (a scalar, an array, and an optional-default head are
 * all built here).
 *
 *  - `scalars` — required native-scalar heads (`{required:true, type}`).
 *  - `elems` — IN-list / array heads (`{type:'array', elemType}` → `Vec<ElemT>`; bc#110).
 *  - `optionals` — coalesce-default heads (`{required:false, type}` → `Option<T>`; bc#139/#122). An
 *    optional entry WINS over a required-scalar entry for the same head (so `absent` is a real `None`).
 *
 * A head present ONLY as a WHERE COLUMN-NAME MARKER (never bound) is not in any set, so it gets no
 * `InNR` field — sound: the covered plane never reads it (see the note at the legacy call site).
 */
function buildInputPorts(
  c: { inputPorts?: Record<string, PortSchema> },
  scalars: Map<string, BcScalar>,
  elems: Map<string, BcScalar>,
  optionals: Map<string, BcScalar>,
): Record<string, PortSchema> {
  const inputPorts: Record<string, PortSchema> = {};
  for (const [name, scalar] of scalars) {
    if (optionals.has(name)) continue; // the optional entry below wins (absent must be a real None)
    const schema = (c.inputPorts ?? {})[name];
    inputPorts[name] = schema === undefined ? { required: true, type: scalar } : { ...schema, type: scalar };
  }
  for (const [name, elem] of elems) {
    const schema = (c.inputPorts ?? {})[name];
    inputPorts[name] = { ...(schema ?? { required: true }), type: 'array', elemType: elem };
  }
  for (const [name, scalar] of optionals) {
    const schema = (c.inputPorts ?? {})[name];
    inputPorts[name] = { ...(schema ?? {}), required: false, type: scalar };
  }
  return inputPorts;
}

/** A head could not be typed for typed-native codegen — the read is NOT typed-native-coverable. */
export class TypedNativeCoverageError extends Error {
  constructor(
    readonly component: string,
    readonly reasons: readonly string[],
  ) {
    super(
      `litedbmodel codegen (#60 m1): component '${component}' is not typed-native-coverable:\n` +
        reasons.map((r) => `  - ${r}`).join('\n') +
        `\n\nbc's typed-native endpoint (bc#77/#90) fails closed on an uncovered shape — this is NOT ` +
        `regenerated on the boxed '-typed'/'-typed-raw' emitter (retired from litedbmodel's codegen ` +
        `surface, #60 milestone 1). Report this as a bc#86 coverage gap (array-typed / non-scalar ` +
        `input heads have no native port type yet), or exclude this case from native-codegen benching.`,
    );
    this.name = 'TypedNativeCoverageError';
  }
}

/** Extract the `{table, column}` a where-fragment statement's leading SQL token names, for the
 * closed set of fragment shapes `compileSelectNode`/`lowerWhereMember` emit: `<col> <op> ?`
 * (eq/cmp/LIKE/ILIKE). Returns undefined for any other shape (head-count / SQL text mismatch) —
 * the caller treats that head as NOT resolvable (fail-closed), never guesses. */
const WHERE_FRAGMENT_COLUMN = /^([A-Za-z_][A-Za-z0-9_]*)\s+(?:=|<>|<=|>=|<|>|LIKE|ILIKE)\s*\?$/;

/** Extract the table name from the head `SELECT … FROM <table>` statement (dialect-tuned, but the
 * `FROM <table>` token is stable across dialects/CTEs — `compileSelectNode`'s head always ends
 * ` FROM <table>[ …]`; a JOIN/CTE-shaped head is out of scope for this narrow codegen deriver and
 * reported as unresolvable rather than mis-parsed). */
const SELECT_FROM_TABLE = /\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)\b/i;

/** Extract the LHS column of an IN-list / array-bound WHERE fragment — the closed set of texts
 * {@link inListFragment} emits across dialects, all of the form `<col> IN (…)` (sqlite `json_each`,
 * mysql `JSON_TABLE`) or `<col> = ANY(?)` (postgres). The single array param binds a `?` inside; the
 * column names the type to resolve the ELEMENT scalar from (spec §4.1 schema SoT). Any other shape
 * returns undefined (fail-closed). */
const IN_LIST_COLUMN = /^([A-Za-z_][A-Za-z0-9_]*)\s+(?:IN\s*\(|=\s*ANY\s*\()/i;

/**
 * Derive each input head's bc scalar type from a read node's compiled {@link StaticStatement}s
 * (spec §4.1 schema SoT), for bc's typed-native codegen (#60 m1). Works from the ALREADY-COMPILED
 * statement templates (available on `ReadGraph.statementsById`, which codegen already carries)
 * instead of re-walking the authored component — so this lowering needs NO extra input beyond
 * what `SqlBundle`/`ReadGraph` already expose (no change to `compileReadGraph`/`ReadGraph`).
 *
 * Returns three sets:
 *  - `byHead` — heads bound by a bare single-segment `{ref:[head]}` scalar param, typed to their
 *    column's bc scalar (`int`/`float`/`string`/`bool`).
 *  - `arrayHeads` — heads bound by an IN-list / array param (`whereIn`'s
 *    `{__jsonArray:{ref:[head]},dialect}`), typed to their column's ELEMENT scalar. bc#110 gives
 *    typed-native a native array port (`Vec<ElemT>`/`[]ElemT`) for such a head, so it is now
 *    COVERED (no longer fail-closed) — the caller emits it as a native array input port.
 *  - `unresolved` — heads whose owning fragment's SQL text isn't a shape this deriver covers (the
 *    closed `<col> <op> ?` scalar shape or the `<col> IN (…)`/`<col> = ANY(?)` array shape). A
 *    non-empty `unresolved` means the read is NOT typed-native-coverable (fail-closed at the caller).
 *
 * `elementVar` (map nodes only): the map's `as` binding (`$e0`). A param whose ref's FIRST segment
 * is the element var (`{ref:['$e0','author_id']}`) is a map-ELEMENT field access, NOT a component
 * input head — bc types it from the map's `over` element struct (the prior node's row), so this
 * deriver neither types nor rejects it (it is fully covered by bc's `coveredMapNode` element typing).
 */
function deriveHeadTypesFromStatements(
  statements: readonly StaticStatement[],
  resolveColumnType: ColumnTypeResolver,
  elementVar?: string,
  tableFromPort?: string,
): { byHead: Map<string, BcScalar>; arrayHeads: Map<string, BcScalar>; unresolved: Map<string, string> } {
  const byHead = new Map<string, BcScalar>();
  const arrayHeads = new Map<string, BcScalar>();
  const unresolved = new Map<string, string>();

  // The owning table resolves every WHERE fragment's column type. `tableFromPort` is the node's
  // authored `table` port — the real SoT, and the ONLY thing that works for a write (an
  // `INSERT INTO t (…) VALUES …` / `UPDATE t SET …` has no `FROM <t>` to match at all). When absent
  // (the pre-E1 read caller) it falls back to matching the `SELECT … FROM <table>` head statement,
  // which `compileSelectNode` always pushes first — behaviour unchanged for that caller.
  const head = statements[0];
  const tableMatch = head === undefined ? null : SELECT_FROM_TABLE.exec(head.sql);
  const table = tableFromPort ?? tableMatch?.[1];

  for (const stmt of statements) {
    // The LIMIT clause (` LIMIT ?`) is NOT a whereFragment — its param is `coalesce(refOpt(limit), N)`
    // (optional head + a static default). bc's typed-native emitter binds `limit` as a native scalar
    // port (its `portIsStatic` accepts the bare ref + number-literal default), so type the head from
    // the schema-less LIMIT contract: `limit`-family heads are always an integer row count. The
    // element-var guard applies here too (a map child never has a LIMIT-bound element ref today, but
    // stay uniform). Anything OTHER than the closed `coalesce([refOpt([head]), <int>])` shape is left
    // untyped here (the surrogate-scope reconciliation below fail-closes on an un-typed referenced head).
    if (stmt.whereFragment !== true) {
      for (const param of stmt.params) {
        const lh = limitHeadNameOf(param);
        if (lh === undefined) continue;
        if (elementVar !== undefined && lh === elementVar) continue;
        const prior = byHead.get(lh);
        if (prior !== undefined && prior !== 'int') {
          unresolved.set(lh, `input head '${lh}' resolves to conflicting scalar types ('${prior}' vs 'int' from a LIMIT clause) across statements`);
          continue;
        }
        byHead.set(lh, 'int');
      }
      continue;
    }
    for (const param of stmt.params) {
      const path = refPathOf(param);
      // A map-ELEMENT field access (`{ref:['$e0','author_id']}`, first segment = the map `as` var) is
      // NOT a component input head — bc types it from the map's `over` element struct. Neither type
      // nor reject it here (fully covered by bc's `coveredMapNode` element-field lowering).
      if (path !== undefined && elementVar !== undefined && path[0] === elementVar) continue;
      if (path === undefined || path.length !== 1) {
        // An IN-list / array-bound WHERE head — `whereIn`'s `{__jsonArray:{ref:[head]},dialect}`
        // param (single-array param, one `?` inside the `IN (…)`/`= ANY(?)` subquery). bc#110 gives
        // typed-native a native ARRAY port for this shape (`Vec<ElemT>`/`[]ElemT`), so resolve the
        // column's ELEMENT scalar (spec §4.1 SoT) and record it as an array head — no longer
        // fail-closed. Any other non-scalar/nested-literal param stays unresolved (fail-closed).
        const arrHead = arrayHeadNameOf(param);
        if (arrHead !== undefined) {
          if (table === undefined) {
            unresolved.set(arrHead, `input head '${arrHead}': could not resolve the owning table from the SELECT head SQL ('${head?.sql ?? '<none>'}')`);
            continue;
          }
          const cm = IN_LIST_COLUMN.exec(stmt.sql.trim());
          if (cm === null) {
            unresolved.set(
              arrHead,
              `input head '${arrHead}': IN-list WHERE fragment SQL '${stmt.sql}' is not the closed '<col> IN (…)'/'<col> = ANY(?)' shape this codegen-only deriver covers`,
            );
            continue;
          }
          const elemType = sqlTypeToBcScalar(resolveColumnType(table, cm[1]));
          const priorElem = arrayHeads.get(arrHead);
          if (priorElem !== undefined && priorElem !== elemType) {
            unresolved.set(arrHead, `input head '${arrHead}' resolves to conflicting array element scalar types ('${priorElem}' vs '${elemType}') across statements`);
            continue;
          }
          arrayHeads.set(arrHead, elemType);
          continue;
        }
        // Not a bare single-segment ref and not a recognized array head (e.g. a nested literal): if
        // it names a head at all, surface it; otherwise it simply isn't head-typable here.
        const named = headNameOf(param);
        if (named !== undefined) {
          unresolved.set(
            named,
            `input head '${named}' is bound by a non-scalar param shape (${JSON.stringify(param)}) — bc's ` +
              `typed-native emitter has no native port type for it (string/int/float/bool scalars only).`,
          );
        }
        continue;
      }
      const headName = path[0];
      if (table === undefined) {
        unresolved.set(headName, `input head '${headName}': could not resolve the owning table from the SELECT head SQL ('${head?.sql ?? '<none>'}')`);
        continue;
      }
      const m = WHERE_FRAGMENT_COLUMN.exec(stmt.sql.trim());
      if (m === null) {
        unresolved.set(
          headName,
          `input head '${headName}': WHERE fragment SQL '${stmt.sql}' is not the closed '<col> <op> ?' shape this codegen-only deriver covers`,
        );
        continue;
      }
      const column = m[1];
      const sqlType = resolveColumnType(table, column);
      const scalar = sqlTypeToBcScalar(sqlType);
      const prior = byHead.get(headName);
      if (prior !== undefined && prior !== scalar) {
        unresolved.set(headName, `input head '${headName}' resolves to conflicting scalar types ('${prior}' vs '${scalar}') across statements`);
        continue;
      }
      byHead.set(headName, scalar);
    }
  }
  // A head bound BOTH as a scalar and as an array across fragments is not a coherent native port —
  // fail-closed rather than silently pick one (never occurs for the covered shapes, defensive).
  for (const head of arrayHeads.keys()) {
    if (byHead.has(head)) {
      unresolved.set(head, `input head '${head}' is bound both as a scalar and as an array param — no single native port type`);
    }
  }
  return { byHead, arrayHeads, unresolved };
}

/** A statement param's bare ref path (`{ref:[…]}`/`{refOpt:[…]}`), or undefined if it isn't one. */
function refPathOf(param: unknown): readonly string[] | undefined {
  if (param === null || typeof param !== 'object' || Array.isArray(param)) return undefined;
  const keys = Object.keys(param as object);
  if (keys.length !== 1 || (keys[0] !== 'ref' && keys[0] !== 'refOpt')) return undefined;
  const path = (param as Record<string, unknown>)[keys[0]];
  if (!Array.isArray(path) || path.length === 0 || !path.every((s) => typeof s === 'string')) return undefined;
  return path as string[];
}

/** The head name a non-bare-ref param shape still names (e.g. `{__jsonArray:{ref:[head]},…}`), for
 * a clearer diagnostic — undefined when no head can be identified at all (a plain literal). */
function headNameOf(param: unknown): string | undefined {
  if (param === null || typeof param !== 'object' || Array.isArray(param)) return undefined;
  const obj = param as Record<string, unknown>;
  if ('__jsonArray' in obj) {
    const inner = refPathOf(obj.__jsonArray);
    return inner?.[0];
  }
  return undefined;
}


/** The head a LIMIT param (`coalesce([{refOpt:[head]}, <int-literal>])`) binds — the closed shape
 * `compileSelectNode` emits for an optional `limit` with a static default (`coalesce(opt($.limit), N)`).
 * Returns the head name for that exact shape, else undefined (any other shape is not a LIMIT head this
 * deriver types). bc's typed-native emitter binds it as a native scalar port (bare ref + number literal
 * default are both `portIsStatic`). */
function limitHeadNameOf(param: unknown): string | undefined {
  if (param === null || typeof param !== 'object' || Array.isArray(param)) return undefined;
  const obj = param as Record<string, unknown>;
  const args = obj.coalesce;
  if (!Array.isArray(args) || args.length !== 2) return undefined;
  const [refPart, dflt] = args;
  // The default must be a static integer literal (`coalesce(opt($.limit), 20)`).
  if (typeof dflt !== 'number' || !Number.isInteger(dflt)) return undefined;
  if (refPart === null || typeof refPart !== 'object' || Array.isArray(refPart)) return undefined;
  const rp = refPart as Record<string, unknown>;
  const keys = Object.keys(rp);
  if (keys.length !== 1 || (keys[0] !== 'ref' && keys[0] !== 'refOpt')) return undefined;
  const path = rp[keys[0]];
  if (!Array.isArray(path) || path.length !== 1 || typeof path[0] !== 'string') return undefined;
  return path[0];
}

/** The head an IN-list / array param (`{__jsonArray:{ref:[head]},dialect}`) binds — ONLY the covered
 * single-segment `{ref:[head]}` inner shape (bc#110's native array port requires a single-segment,
 * non-opt ref). A multi-segment / opt / non-ref inner is NOT this covered array shape → undefined
 * (the caller then treats it as an unresolved non-scalar param, fail-closed). */
function arrayHeadNameOf(param: unknown): string | undefined {
  if (param === null || typeof param !== 'object' || Array.isArray(param)) return undefined;
  const obj = param as Record<string, unknown>;
  if (!('__jsonArray' in obj)) return undefined;
  const inner = refPathOf(obj.__jsonArray);
  return inner !== undefined && inner.length === 1 ? inner[0] : undefined;
}

// ═══════════════════════════════════════════════════════════════════════════════════════════
// THE SINGLE LOWERING (#116, epic #115) — bake the per-op SQL as a NATIVE LITERAL.
//
// litedbmodel codegen has ONE lowering: it renders each node's per-dialect SQL STATICALLY (the SAME
// assembly `renderStatements` performs, minus the per-input value evaluation) and emits it as native
// ports bc bakes as literals — a STATIC STRING `sql` port + one typed port per bound `?`. So the
// generated module carries its own query and needs NO runtime JSON companion read.
//
// bc's typed-native emitter is SQL-AGNOSTIC — it bakes any port its `portIsStatic` predicate covers
// as a native literal (exactly how graphddb gets `f_table:"UserPermissions"`). Verified against bc
// 0.8.5's `rust-typed-native`/`go-typed-native`/`typescript-typed`: a static-string `sql` port bakes
// as `f_sql: "SELECT … WHERE email = ? LIMIT ?".to_string()`, a single-segment input ref bakes as
// `f_p0: in_.email.clone()`, a bare number literal as `f_p1: 1i64`, an optional-default as
// `in_.limit.unwrap_or(20i64)` (bc#139), an IN-list array as `Vec<ElemT>` (bc#110), and a map
// element-field ref as `oel.author_id` — all on CONCRETE structs, zero boxed `Value`, zero
// bc-runtime import.
//
// A thin, op-agnostic native seam (`exec(sql, params)` + a SKIP-args `query_skip` that assembles
// present fragments) consumes the baked ports and drives the driver — no IR walk, no JSON, no
// dispatch. This ONE lowering covers every read shape (scalar / optional / coalesce params, IN-list
// arrays, skip-optional WHERE fragments, single- AND composite-key map relations) AND writes
// (Insert/Update/Delete); read and write are one flow. There is NO second lowering and NO opt-in flag.
//
// A COMPOSITE-key `map` (a child binding TWO+ parent element fields, e.g. a `(tenant_id, user_id)`
// tuple join) needs NO special case: `paramPortFor` passes EACH element-field ref through, and bc
// bakes N scalar element-field ports (the SAME primitive as single-key, repeated) —
// `f_p0: oel.tenant_id, f_p1: oel.user_id` over a two-column baked SQL. Verified end-to-end
// (rust byte-equal to the mode-2 oracle). The only map shape that fails closed is a SKIP-guarded map
// child (single-key relations do not skip); a plain `cond` node also fails closed.
// ═══════════════════════════════════════════════════════════════════════════════════════════

/**
 * Render a read node's compiled {@link StaticStatement}s to ONE static per-dialect SQL string +
 * the ordered deferred value-specs bound to its `?` placeholders.
 *
 * This mirrors the mode-2 runtime assembly (`renderStatements` in `./makesql/static-bundle`) EXACTLY
 * — same ` WHERE `/` AND ` connector resolution, same {@link composeMakeSQL} concatenation, same
 * {@link renderPlaceholders} dialect pass — with the ONE difference that the params stay as
 * value-spec IR instead of being evaluated against a concrete input scope. The SQL TEXT is therefore
 * identical to what the thin runtime renders, by construction (not by a parallel hand-rolled
 * re-implementation).
 *
 * Fail-closed on a `skip`-carrying statement: a skip means the statement DROPS for some inputs, so
 * the node's SQL text is input-DEPENDENT and has no single static literal. That shape needs a
 * per-skip-subset lowering (one baked SQL per present-set) — NOT a silently-wrong single literal.
 */
function renderStaticSql(
  statements: readonly StaticStatement[],
  dialect: DialectName,
  nodeId: string,
  reasons: string[],
): { sql: string; params: readonly unknown[] } | undefined {
  const nodes: MakeSQL[] = [];
  let whereSeen = false;
  for (const stmt of statements) {
    if (stmt.skip !== undefined) {
      reasons.push(
        `node '${nodeId}': statement ${JSON.stringify(stmt.sql)} carries a 'skip' presence expression — its SQL text is ` +
          `INPUT-DEPENDENT (the fragment drops for some inputs), so the node has no single static SQL literal to bake. ` +
          `A skip-carrying read needs a per-present-set lowering (one baked SQL per skip subset); it is NOT lowered here ` +
          `rather than baking a silently-wrong always-present literal.`,
      );
      return undefined;
    }
    let sql = stmt.sql;
    if (stmt.whereFragment === true) {
      sql = (whereSeen ? ' AND ' : ' WHERE ') + stmt.sql;
      whereSeen = true;
    }
    nodes.push({ sql, params: [...stmt.params] as SqlParam[] });
  }
  // `composeMakeSQL` validates the `?`/param count 1:1 and concatenates — the SAME call the runtime
  // makes. A value-spec param is not a nested `MakeSQL`, so it re-emits its `?` and rides through.
  const assembled = composeMakeSQL(nodes);
  // Bake dialect-NEUTRAL `?` placeholders — the `?`→`$N` renumber is resolved ONCE at RUNTIME
  // (litedbmodel_runtime's native exec, from the connection's dialect), NOT at generation time. A
  // generation-time `$N` would desync when a mid-fragment skip drops (the numbers would skip); the
  // single runtime renumber over the FINAL assembled SQL cannot. `dialect` is retained for the
  // caller signature (dialect-specific SQL CONSTRUCTS — json_each/ANY, ON CONFLICT — are still baked
  // per dialect upstream); only the placeholder style is neutralized here.
  void dialect;
  return { sql: assembled.sql, params: assembled.params };
}

/**
 * Append the MySQL RETURNING pk-hint (` /*scp:pk=cols;ai=col* /`) to a baked mysql WRITE SQL — the
 * native analogue of mode-2's `runtime.ts:390`/`:500` (`mysqlPkHint(compileWriteNode(...))`). It bakes
 * the SAME lightweight hint the mode-2 path bakes, from the SAME SSoT ({@link pkPort} reads the pk
 * descriptor off the write node's authored ports; {@link mysqlPkHint} formats + appends the comment).
 * The MySQL driver emulation reads it to re-select the written row(s) by the REAL primary key /
 * AUTO_INCREMENT range (a batch INSERT recovers all N rows, not just `last_insert_id`). PG/SQLite keep
 * native RETURNING (no hint). This is a generation-time per-dialect SQL decoration (like `?`→`$N`) the
 * DRIVER consumes — a lightweight metadata hint, NOT a retired reselect-SQL marker and not a new apparatus.
 */
function bakeMysqlPkHint(sql: string, dialect: DialectName, nodePorts: Record<string, unknown>): string {
  if (dialect !== 'mysql') return sql;
  // An upsert node carries the conflict-target column list on its `onConflict` port — the driver
  // re-selects the upserted row by it (the AUTO_INCREMENT range is wrong on a conflict).
  const onConflict = typeof nodePorts.onConflict === 'string' ? nodePorts.onConflict : undefined;
  return mysqlPkHint({ sql, params: [], pk: pkPort(nodePorts) }, onConflict).sql;
}

/** Does any of a node's statements carry a `skip` presence expression? A skip fragment DROPS for
 * some inputs, so the node's SQL is assembled per-input — it takes the fragmented lowering (below)
 * instead of the single baked `sql` string. */
function hasSkipStatement(statements: readonly StaticStatement[]): boolean {
  return statements.some((s) => s.skip !== undefined);
}

/**
 * The optional HEAD a SKIP-`cond` guard is driven by: litedbmodel lowers `when(present($.h),
 * whereX($.col, $.h))` to a fragment whose `skip` is `{not:[{ne:[{refOpt:[h]}, null]}]}` — "drop the
 * fragment when `h` is absent". Extract `h` from that exact closed shape (else undefined → the guard
 * isn't the covered presence shape, fail-closed at the caller). `h` is the presence signal: the
 * fragment is PRESENT iff `h` is present, which the native module reads as `in_.<h>.is_some()`.
 */
function skipGuardHead(skip: unknown): string | undefined {
  if (skip === null || typeof skip !== 'object' || Array.isArray(skip)) return undefined;
  const notArgs = (skip as Record<string, unknown>).not;
  if (!Array.isArray(notArgs) || notArgs.length !== 1) return undefined;
  const ne = notArgs[0];
  if (ne === null || typeof ne !== 'object') return undefined;
  const neArgs = (ne as Record<string, unknown>).ne;
  if (!Array.isArray(neArgs) || neArgs.length !== 2 || neArgs[1] !== null) return undefined;
  const inner = neArgs[0];
  if (inner === null || typeof inner !== 'object') return undefined;
  const path = (inner as Record<string, unknown>).refOpt;
  if (!Array.isArray(path) || path.length !== 1 || typeof path[0] !== 'string') return undefined;
  return path[0];
}

/** ONE assembled WHERE fragment of a skip-carrying read: the bare predicate SQL (`?` placeholders,
 * NO connector — the seam prepends ` WHERE `/` AND `), its ordered value-specs, and the optional head
 * whose presence gates it (undefined ⇒ always present). */
interface RenderedFragment {
  readonly sql: string;
  readonly params: readonly unknown[];
  readonly skipHead?: string;
}

/** The fragmented form of a skip-carrying read node: a static head + ordered WHERE fragments (some
 * gated by an optional head's presence) + a static tail. The generic exec seam assembles the present
 * fragments over these baked literals — NATIVE string assembly, NO IR walk / JSON / dispatch. */
interface RenderedSkip {
  readonly head: string;
  readonly headParams: readonly unknown[];
  readonly tail: string;
  readonly tailParams: readonly unknown[];
  readonly fragments: readonly RenderedFragment[];
}

/**
 * Render a skip-carrying read node into its fragmented form (owner's SKIP-args model: "native runtime
 * = 「SQL・params・SKIP引数の汎用クエリ実行関数」だけ"). Classifies the SAME statements
 * {@link renderStaticSql} does — `statements[0]` is the head, `whereFragment` statements are the WHERE
 * fragments (a skip fragment carries `skipHead`), everything else is tail (ORDER BY / LIMIT / …). The
 * bare predicate text keeps its `?` placeholders (the seam resolves ` WHERE `/` AND ` at assembly, and
 * the pg `?`→`$N` renumber is a post-assembly dialect concern like the array encode — sqlite/mysql
 * keep `?`). Fail-closed (a reason) on a skip whose guard is not the covered presence shape.
 */
function renderSkipFragments(
  statements: readonly StaticStatement[],
  nodeId: string,
  reasons: string[],
): RenderedSkip | undefined {
  const head = statements[0];
  if (head === undefined || head.whereFragment === true) {
    reasons.push(`node '${nodeId}': skip-fragment lowering expects a leading SELECT head statement`);
    return undefined;
  }
  const fragments: RenderedFragment[] = [];
  const tailNodes: MakeSQL[] = [];
  for (const stmt of statements.slice(1)) {
    if (stmt.whereFragment === true) {
      let skipHead: string | undefined;
      if (stmt.skip !== undefined) {
        skipHead = skipGuardHead(stmt.skip);
        if (skipHead === undefined) {
          reasons.push(
            `node '${nodeId}': WHERE fragment ${JSON.stringify(stmt.sql)} has a 'skip' guard that is not the covered ` +
              `presence shape (\`not(ne(refOpt(head), null))\`) — cannot derive the presence head, fail-closed.`,
          );
          return undefined;
        }
      }
      fragments.push({ sql: stmt.sql, params: [...stmt.params], ...(skipHead !== undefined ? { skipHead } : {}) });
      continue;
    }
    // A tail statement (ORDER BY / GROUP BY / LIMIT / OFFSET) — assembled verbatim after the WHERE.
    tailNodes.push({ sql: stmt.sql, params: [...stmt.params] as SqlParam[] });
  }
  const tail = composeMakeSQL(tailNodes);
  return { head: head.sql, headParams: [...head.params], tail: tail.sql, tailParams: tail.params, fragments };
}

/** The catalog components the E1/E2 SQL-port lowering bakes. Read and write are ONE flow: both make
 * SQL and execute it, and both hand back rows — so they lower identically and are NOT split. */
const READ_COMPONENTS = new Set(['Select', 'Count', 'RelationBatch']);
const WRITE_COMPONENTS = new Set(['Insert', 'Update', 'Delete']);

/** A write node's `values.<col>` / `set.<col>` port names — the column each bound head writes. */
const WRITE_VALUE_PORT = /^(?:values|set)\.(.+)$/;

/**
 * The SQL row shape `executeStaticWrite` returns for a write with NO RETURNING clause: the single
 * summary row `[{changes, lastInsertRowid}]`. A write's authored/compile-time `outType` is the
 * determined empty-row list `{arr:{obj:{}}}` (it existed only to satisfy bc's all-nodes-typed gate,
 * since writes were never typed-codegen'd) — that is NOT what the op actually returns, so the
 * codegen IR must carry the REAL shape: the generated module's row struct has to match what the exec
 * seam hands back, or the module would be typed a lie.
 */
const WRITE_SUMMARY_OUT_TYPE = { arr: { obj: { changes: 'int', lastInsertRowid: 'int' } } };

/**
 * Type a WRITE node's bound heads from its AUTHORED PORTS — the real SoT — rather than by regexing
 * the compiled SQL.
 *
 * The read deriver ({@link deriveHeadTypesFromStatements}) recovers a fragment's column by matching
 * the statement text and the table by matching `SELECT … FROM <t>`. Neither works for a write: an
 * `INSERT INTO t (…) VALUES (?, ?)` / `UPDATE t SET …` carries no `FROM <t>` at all (only DELETE does,
 * and only by accident), and a value bind's column appears in the INSERT column list, not next to its
 * `?`. The authored ports already state both facts exactly — `table: 'benchmark_users'` and
 * `values.email: {ref:['email']}` — so the column each head writes is read straight off the port
 * NAME, and its type resolves from the schema SoT. WHERE-bound heads (Update/Delete) keep the shared
 * fragment deriver, with the table supplied from the `table` port.
 */
/**
 * The parallel-column-array binding of a BATCH write statement (createMany / upsertMany / updateMany),
 * as `{columns, refs}` where `refs[i]` is the WHOLE array for column `columns[i]` — or undefined for a
 * non-batch statement. ONE derivation for BOTH dialect marker shapes (the write twin of the relation
 * batch), so codegen types the SAME array-input head off either:
 *
 *  - sqlite/mysql (v2): the single json_each/JSON_TABLE statement whose `?`(s) all bind the SAME
 *    `{__batchRows:{columns, refs}}` marker (one `?` for createMany; one per SET clause + WHERE for
 *    updateMany — all carrying the same columns/refs).
 *  - postgres (v1): the UNNEST statement whose `?`s each bind a distinct per-column
 *    `{__batchArray:{column, ref}}` marker (byte-identical to v1; the parity rule keeps pg on UNNEST).
 */
function batchRowsMarkerOf(stmts: readonly StaticStatement[]): { columns: string[]; refs: unknown[]; perColumn: boolean } | undefined {
  if (stmts.length !== 1) return undefined;
  const p = stmts[0].params;
  if (p.length === 0) return undefined;
  const isObj = (m: unknown): m is Record<string, unknown> => m !== null && typeof m === 'object' && !Array.isArray(m);
  // v2 (sqlite/mysql): every `?` binds the same `__batchRows` marker → SingleJson param-shape.
  if (p.every((m) => isObj(m) && '__batchRows' in m)) {
    const br = (p[0] as { __batchRows: { columns?: unknown; refs?: unknown } }).__batchRows;
    if (!Array.isArray(br.columns) || !Array.isArray(br.refs) || br.columns.length !== br.refs.length) return undefined;
    return { columns: br.columns as string[], refs: br.refs as unknown[], perColumn: false };
  }
  // v1 (postgres): each `?` binds a distinct per-column `__batchArray` marker → PerColumn param-shape
  // (assemble the parallel columns/refs from them — same shape the v2 marker carries whole).
  if (p.every((m) => isObj(m) && '__batchArray' in m)) {
    const columns: string[] = [];
    const refs: unknown[] = [];
    for (const m of p) {
      const ba = (m as { __batchArray: { column?: unknown; ref?: unknown } }).__batchArray;
      if (typeof ba.column !== 'string' || ba.ref === undefined) return undefined;
      columns.push(ba.column);
      refs.push(ba.ref);
    }
    return { columns, refs, perColumn: true };
  }
  return undefined;
}

function deriveWriteHeadTypes(
  ports: Record<string, unknown>,
  table: string,
  resolveColumnType: ColumnTypeResolver,
  nodeId: string,
  reasons: string[],
): Map<string, BcScalar> {
  const byHead = new Map<string, BcScalar>();
  const record = (head: string, column: string): void => {
    const scalar = sqlTypeToBcScalar(resolveColumnType(table, column));
    const prior = byHead.get(head);
    if (prior !== undefined && prior !== scalar) {
      reasons.push(`node '${nodeId}': input head '${head}' resolves to conflicting scalar types ('${prior}' vs '${scalar}')`);
      return;
    }
    byHead.set(head, scalar);
  };
  // The written values (`values.<col>` / `set.<col>`): the port NAME states the column, the port
  // VALUE names the bound head. This is the write's SoT — the compiled SQL carries the column in the
  // INSERT column list, nowhere near its `?`, so the ports (not the text) are the correct source.
  for (const [port, value] of Object.entries(ports)) {
    const m = WRITE_VALUE_PORT.exec(port);
    if (m === null) continue;
    const path = refPathOf(value);
    if (path === undefined) continue; // a literal value binds no head (nothing to type)
    if (path.length !== 1) {
      reasons.push(
        `node '${nodeId}' port '${port}': a written value must bind a single-segment head ({ref:[head]}) to lower ` +
          `natively (got ${JSON.stringify(value)}).`,
      );
      continue;
    }
    record(path[0], m[1]);
  }
  // The WHERE-bound heads (Update/Delete): the authored `where` port is the SoT for their column too
  // (an `UPDATE t SET … WHERE id = ?` statement has no `FROM <t>` for the fragment deriver to key
  // off). Each `eq`/cmp member is `<colRef> <op> <valueRef>`: the LHS ref's last segment is the
  // column, the RHS single-segment ref is the bound head. This reuses the SAME closed WHERE shape the
  // read path constrains; anything else fails closed at the node's param lowering below, not here.
  const where = ports.where;
  if (typeof where === 'object' && where !== null && 'arr' in where && Array.isArray((where as { arr: unknown[] }).arr)) {
    for (const member of (where as { arr: unknown[] }).arr) {
      const parsed = writeWhereScalarBind(member);
      if (parsed !== undefined) record(parsed.head, parsed.column);
    }
  }
  return byHead;
}

/** A write WHERE member `<colRef> <eq|cmp> <valueRef>` → its `{head, column}`, or undefined when the
 * member is not that closed scalar shape (a group / sentinel / literal — left for the node's param
 * lowering to type or fail-close, never guessed here). */
function writeWhereScalarBind(member: unknown): { head: string; column: string } | undefined {
  if (member === null || typeof member !== 'object' || Array.isArray(member)) return undefined;
  const keys = Object.keys(member as object);
  if (keys.length !== 1) return undefined;
  const op = keys[0];
  if (op !== 'eq' && op !== 'ne' && op !== 'lt' && op !== 'le' && op !== 'gt' && op !== 'ge') return undefined;
  const args = (member as Record<string, unknown>)[op];
  if (!Array.isArray(args) || args.length !== 2) return undefined;
  const colPath = refPathOf(args[0]);
  const valPath = refPathOf(args[1]);
  if (colPath === undefined || valPath === undefined || valPath.length !== 1) return undefined;
  // The LHS names the column (its last segment); a sentinel-headed LHS (IN/immediate/…) is not a plain
  // column and is skipped (typed via the param path instead).
  const column = colPath[colPath.length - 1];
  if (colPath[0] === IN_SENTINEL) return undefined;
  return { head: valPath[0], column };
}

/**
 * Lower ONE bound `?` value-spec to the native port node bc will bake, or record why it cannot.
 *
 * The COVERED param shapes (verified against bc 0.8.5 `portIsStatic`/`inferPortType`/#139):
 *  - a bare INTEGER/FLOAT literal (e.g. the `LIMIT 1` count) → the literal itself; bc bakes `1i64`.
 *  - a single-segment, non-opt `{ref:[head]}` → the same ref; bc bakes `in_.<head>.clone()`.
 *  - a `coalesce([{refOpt:[head]}, <lit>])` (optional-LIMIT default, #122) → the coalesce node
 *    verbatim; bc 0.8.5 bakes `in_.<head>.unwrap_or(<lit>)` over an OPTIONAL input port (the caller
 *    types the head via {@link coalesceOptHead}). The default is PRESERVED — never dropped.
 *  - an IN-list `{__jsonArray:{ref:[head]}}` array bind → the single-segment ref; the head is typed
 *    as a native array port (bc#110) and the seam performs the dialect's array encode.
 *
 * Everything else fails CLOSED with the exact offending shape named — no boxed escape, no silent
 * default-dropping.
 */
/** A `coalesce([{refOpt:[head]}, <numeric literal>])` param (the optional-LIMIT default, #122) → its
 * `{head, scalar}`, or undefined. bc 0.8.5 (#139) bakes this natively as `in_.<head>.unwrap_or(<lit>)`
 * over an OPTIONAL input port — so the default is PRESERVED, not dropped. The head's scalar comes from
 * the literal default (an int literal ⇒ int, a fractional/exponent literal ⇒ float); the optional ref
 * inner and the default must agree, which the LIMIT contract guarantees (both are the row count). */
function coalesceOptHead(param: unknown): { head: string; scalar: BcScalar } | undefined {
  if (param === null || typeof param !== 'object' || Array.isArray(param)) return undefined;
  const args = (param as Record<string, unknown>).coalesce;
  if (!Array.isArray(args) || args.length !== 2) return undefined;
  const [refPart, dflt] = args;
  if (typeof dflt !== 'number' || !Number.isFinite(dflt)) return undefined;
  if (refPart === null || typeof refPart !== 'object' || Array.isArray(refPart)) return undefined;
  const keys = Object.keys(refPart as object);
  if (keys.length !== 1 || keys[0] !== 'refOpt') return undefined;
  const path = (refPart as Record<string, unknown>).refOpt;
  if (!Array.isArray(path) || path.length !== 1 || typeof path[0] !== 'string') return undefined;
  return { head: path[0], scalar: Number.isInteger(dflt) ? 'int' : 'float' };
}

function paramPortFor(param: unknown, index: number, nodeId: string, reasons: string[], elementVar?: string): unknown | undefined {
  if (typeof param === 'number' && Number.isFinite(param)) return param;
  // A `coalesce(opt(head), literal)` default (#122): bc 0.8.5 bakes it as `in_.<head>.unwrap_or(lit)`
  // over an OPTIONAL input port — the default is preserved natively (NOT dropped by rewriting to a
  // bare ref, as the pre-#139 lowering did). Emit the coalesce node verbatim; the caller types the
  // head as an optional input port (see `coalesceOptHead`).
  if (coalesceOptHead(param) !== undefined) return param;
  // An IN-list / array-bound param (`{__jsonArray:{ref:[head]},dialect}`, ONE `?` inside the
  // `IN (…)`/`= ANY(?)` subquery). bc#110 gives typed-native a native ARRAY port for this head, so
  // the port is the SAME single-segment ref — the caller types its `inputPorts` entry as
  // `{type:'array', elemType}` and bc bakes `f_pN: Vec<ElemT> = in_.<head>.clone()`. The dialect's
  // array-ENCODE (the single-JSON bind for sqlite/mysql, the array bind for pg) is the exec seam's
  // job — it is a driver-binding concern, not a SQL-text one (the text is already baked).
  const arrHead = arrayHeadNameOf(param);
  if (arrHead !== undefined) return { ref: [arrHead] };
  const path = refPathOf(param);
  // A map ELEMENT-FIELD ref (`{ref:[$e0, field]}`, first segment = the map's `as` var) — the mapped
  // parent row's field. bc types it from the map's `over` element struct (the parent node's outType),
  // so emit it verbatim; it is NOT a component input head (the caller excludes it from inputPorts).
  if (elementVar !== undefined && path !== undefined && path.length === 2 && path[0] === elementVar) {
    return { ref: path };
  }
  if (path !== undefined && path.length === 1) {
    const keys = Object.keys(param as object);
    if (keys[0] === 'refOpt') {
      reasons.push(
        `node '${nodeId}' param p${index}: an OPTIONAL head ref ({refOpt:${JSON.stringify(path)}}) has no native non-Option ` +
          `port type on bc's typed-native input struct — not lowered (E1 covers required scalar heads + literals).`,
      );
      return undefined;
    }
    return { ref: path };
  }
  reasons.push(
    `node '${nodeId}' param p${index}: value-spec ${JSON.stringify(param)} is not a shape bc's typed-native emitter bakes ` +
      `as a native port (covered: a bare number literal, a single-segment required {ref:[head]}, or an IN-list ` +
      `{__jsonArray:{ref:[head]}} array bind). Rewriting it to a bare ref would silently drop its semantics ` +
      `(e.g. a coalesce default), so it fails closed.`,
  );
  return undefined;
}

/**
 * THE SINGLE codegen lowering (#116): lower a read/write bundle's REAL `ComponentGraphIR` into a NEW,
 * CODEGEN-ONLY IR whose nodes carry the rendered per-dialect SQL as native ports bc bakes as literals
 * — a STATIC `sql` string port + one typed port per bound `?`. The generated module then CARRIES its
 * own SQL: no runtime JSON companion read.
 *
 * Coverage: `Select`/`Count`/`Insert`/`Update`/`Delete` nodes; required + optional-`coalesce` (#122)
 * + IN-list array (bc#110) params; `skip`-optional WHERE fragments (fragmented shape, seam-assembled);
 * single- AND composite-key `map` relations (one element-field port per parent key column). A `cond`
 * node and a SKIP-guarded `map` child fail CLOSED with a precise reason.
 *
 * Does NOT mutate `bundle`/`bundle.readGraph`: a fresh IR object is returned. The native
 * `executeReadGraph` keeps consuming the real `readGraph.ir`, and the frozen makeSQL conformance
 * corpus is untouched (this changes no compiled statement — it only RE-EXPRESSES already-compiled
 * statements as ports).
 */
export function lowerReadGraphForNativeSql(readGraph: ReadGraph, resolveColumnType: ColumnTypeResolver): ComponentGraphIR {
  const ir = readGraph.ir;
  const reasons: string[] = [];
  const components = ir.components.map((c) => {
    const inputPortTypes = new Map<string, BcScalar>();
    const inputPortElemTypes = new Map<string, BcScalar>();
    // Heads bound by a `coalesce(opt(head), default)` param (#122): they lower to an OPTIONAL input
    // port (`{required:false, type}` → `Option<T>`), which bc 0.8.5 reads via `.unwrap_or(default)`.
    const optionalHeadTypes = new Map<string, BcScalar>();
    // A node whose outType this lowering CORRECTED (a no-RETURNING write → the summary shape): the
    // component `outputType` must follow, since the output Φ is a bare `{ref:[nodeId]}`. Tracked so
    // the corrected type propagates to the runner's return type (else the node row and the runner
    // output disagree — a self-inconsistent module).
    const correctedOutType = new Map<string, unknown>();
    const body = c.body.map((n) => {
      if ('cond' in n) {
        reasons.push(`node '${(n as unknown as { id?: string }).id ?? '<cond>'}': a 'cond' node is not lowered by the E1 SQL-port lowering`);
        return n;
      }
      const stmts = readGraph.statementsById[n.id] ?? [];
      // A `map` (relation) node runs a per-parent-element child Select — its ports live on `n.map`
      // and its child statements bind the mapped parent row via `{ref:[$e0, field]}` element-field
      // refs (bc types them from the parent node's outType; NOT component input heads). Everything
      // else — the SQL baking, the head typing, the param ports — is the SAME as a plain read (this
      // is ONE lowering, branching only on where the ports live + the element-var exclusion).
      const isMap = 'map' in n;
      if ('cond' in n) {
        reasons.push(`node '${(n as unknown as { id?: string }).id ?? '<cond>'}': a 'cond' node is not lowered by the SQL-port lowering`);
        return n;
      }
      const mapObj = isMap ? (n as unknown as { map: { as?: string; component?: string; ports: Record<string, unknown> } }).map : undefined;
      const elementVar = isMap ? mapObj?.as : undefined;
      const component = isMap ? (mapObj?.component ?? '') : ((n as unknown as { component?: string }).component ?? '');
      const isWrite = WRITE_COMPONENTS.has(component);
      if (!isWrite && !READ_COMPONENTS.has(component)) {
        reasons.push(`node '${n.id}': component '${component}' is not a SQL CRUD node the SQL-port lowering bakes (Select/Count/Insert/Update/Delete)`);
        return n;
      }
      if (isMap && (isWrite || elementVar === undefined)) {
        reasons.push(`node '${n.id}': a map node must run a Select/Count child with an 'as' element var (got component '${component}', as '${elementVar ?? '<none>'}')`);
        return n;
      }
      if (isMap && hasSkipStatement(stmts)) {
        reasons.push(`node '${n.id}': a map node with a SKIP-guarded child fragment is not yet lowered (single-key relations do not skip)`);
        return n;
      }
      // The node's authored `table` port — the SoT for every column type below. Every CRUD node
      // (read AND write, plain AND map-child) carries it.
      const nodePorts = isMap ? mapObj!.ports : (n as unknown as { ports: Record<string, unknown> }).ports;
      const table = typeof nodePorts.table === 'string' ? nodePorts.table : undefined;
      if (table === undefined) {
        reasons.push(`node '${n.id}': ${component} node has no literal 'table' port — cannot resolve its column types`);
        return n;
      }
      if (component === 'RelationBatch') {
        const sql = typeof nodePorts.sql === 'string' ? nodePorts.sql : undefined;
        const shape = typeof nodePorts.keyShape === 'string' ? nodePorts.keyShape : undefined;
        const targetKeysNode = nodePorts.targetKeys as { arr?: unknown[] } | undefined;
        const targetKeys = targetKeysNode?.arr?.filter((v): v is string => typeof v === 'string') ?? [];
        if (sql === undefined || shape === undefined) {
          reasons.push(`node '${n.id}': RelationBatch requires static sql/keyShape ports`);
          return n;
        }
        const keys = Object.keys(nodePorts)
          .filter((p) => /^key\.\d+$/.test(p))
          .sort((a, b) => Number(a.slice(4)) - Number(b.slice(4)));
        const newPorts: Record<string, unknown> = { sql, relation_shape: shape };
        keys.forEach((key, i) => {
          const ref = refPathOf(nodePorts[key]);
          const col = targetKeys[i];
          if (ref === undefined || ref.length !== 1 || col === undefined) {
            reasons.push(`node '${n.id}': RelationBatch ${key} lacks a direct input ref/target key type`);
            return;
          }
          newPorts[`v${i}`] = { ref };
          inputPortElemTypes.set(ref[0], sqlTypeToBcScalar(resolveColumnType(table, col)));
        });
        return { ...n, ports: newPorts };
      }
      // E3 (#118) BATCH write (createMany / upsertMany): the single statement is the json_each batch
      // form whose ONE `?` binds a `__batchRows` marker. Bake the f_sql + one native ARRAY port per
      // column (Vec<scalar>, bc#110 — the records ride as parallel column arrays, since bc has no
      // Vec<struct>). The generic seam zips the parallel arrays into the `[{col:val,…},…]` JSON and
      // binds the ONE `?` — one statement for N records. This is the write twin of the relation batch.
      const batch = isWrite ? batchRowsMarkerOf(stmts) : undefined;
      if (batch !== undefined) {
        const rendered = renderStaticSql(stmts, readGraph.dialect, n.id, reasons);
        if (rendered === undefined) return n;
        const newPorts: Record<string, unknown> = { sql: bakeMysqlPkHint(rendered.sql, readGraph.dialect, nodePorts) };
        batch.columns.forEach((col, i) => {
          const ref = batch.refs[i];
          const path = refPathOf(ref);
          if (path === undefined || path.length !== 1) {
            reasons.push(`node '${n.id}' batch column '${col}': the value must bind a single-segment array ref ({ref:[head]}) (got ${JSON.stringify(ref)})`);
            return;
          }
          newPorts[`v${i}`] = { ref: path }; // the parallel column array (bc bakes Vec<ElemT>)
          const elem = sqlTypeToBcScalar(resolveColumnType(table, col));
          const prior = inputPortElemTypes.get(path[0]);
          if (prior !== undefined && prior !== elem) {
            reasons.push(`input head '${path[0]}' resolves to conflicting array element scalar types ('${prior}' vs '${elem}')`);
          } else {
            inputPortElemTypes.set(path[0], elem);
          }
        });
        // A batch with NO RETURNING hands back the summary row (like a single non-returning write).
        const lowered = { ...n, ports: newPorts };
        if (nodePorts.returning === undefined) {
          correctedOutType.set(n.id, WRITE_SUMMARY_OUT_TYPE);
          return { ...lowered, outType: WRITE_SUMMARY_OUT_TYPE } as typeof lowered;
        }
        return lowered;
      }
      // Type each genuine bound head from the schema SoT — the SAME derivation the pre-E1 lowering
      // uses (unchanged), so the input struct's field types are identical. A map node passes its
      // element var so element-field refs are excluded from head typing. A write additionally types
      // the heads its `values.*`/`set.*` ports bind (the ports name the column; the SQL does not).
      const { byHead, arrayHeads, unresolved } = deriveHeadTypesFromStatements(stmts, resolveColumnType, elementVar, table);
      for (const [, reason] of unresolved) reasons.push(`node '${n.id}': ${reason}`);
      if (isWrite) {
        for (const [head, scalar] of deriveWriteHeadTypes(nodePorts, table, resolveColumnType, n.id, reasons)) {
          byHead.set(head, scalar);
        }
      }
      // IN-list / array-bound heads (bc#110): a native ARRAY input port carrying the ELEMENT scalar.
      // bc does NOT infer element types (consumer-interface C3), so the elemType comes from the
      // schema SoT (the IN-list column's resolved scalar), exactly as the pre-E1 lowering supplies it.
      for (const [head, elem] of arrayHeads) {
        const prior = inputPortElemTypes.get(head);
        if (prior !== undefined && prior !== elem) {
          reasons.push(`input head '${head}' resolves to conflicting array element scalar types ('${prior}' vs '${elem}') across nodes`);
          continue;
        }
        inputPortElemTypes.set(head, elem);
      }
      for (const [head, scalar] of byHead) {
        const prior = inputPortTypes.get(head);
        if (prior !== undefined && prior !== scalar) {
          reasons.push(`input head '${head}' resolves to conflicting scalar types ('${prior}' vs '${scalar}') across nodes`);
          continue;
        }
        inputPortTypes.set(head, scalar);
      }
      // Record a coalesce-default head so it becomes an OPTIONAL input port (#122). The deriver above
      // also typed it (in `byHead`, as a LIMIT int) — the optional entry wins when building
      // `inputPorts` so `absent` is a real `None` the baked `.unwrap_or(default)` resolves.
      const recordCoalesce = (p: unknown): void => {
        const co = coalesceOptHead(p);
        if (co === undefined) return;
        const prior = optionalHeadTypes.get(co.head);
        if (prior !== undefined && prior !== co.scalar) {
          reasons.push(`input head '${co.head}' resolves to conflicting optional scalar types ('${prior}' vs '${co.scalar}')`);
        } else {
          optionalHeadTypes.set(co.head, co.scalar);
        }
      };
      // A skip-carrying read takes the FRAGMENTED port shape (owner SKIP-args model): a static head +
      // ordered baked WHERE fragments (a skip fragment gated by an optional head's presence) + a
      // static tail. The generic exec seam assembles the PRESENT fragments over these baked literals —
      // native string assembly, NO IR walk / JSON / dispatch. A non-skip read keeps the simple single
      // `sql` port. Both are this ONE lowering; only the emitted port shape differs by whether the
      // node has a skip fragment.
      if (hasSkipStatement(stmts)) {
        const frag = renderSkipFragments(stmts, n.id, reasons);
        if (frag === undefined) return n;
        const newPorts: Record<string, unknown> = { sql_head: frag.head, sql_tail: frag.tail };
        frag.headParams.forEach((p, i) => {
          const port = paramPortFor(p, i, n.id, reasons);
          if (port !== undefined) newPorts[`h${i}`] = port;
          recordCoalesce(p);
        });
        frag.tailParams.forEach((p, i) => {
          const port = paramPortFor(p, i, n.id, reasons);
          if (port !== undefined) newPorts[`t${i}`] = port;
          recordCoalesce(p);
        });
        frag.fragments.forEach((f, fi) => {
          newPorts[`w${fi}`] = f.sql; // the bare predicate, a baked static string bc renders as a literal
          // A skip fragment's presence head is OPTIONAL (`Option<T>`) — the module reads it as
          // `in_.<head>.is_some()` (the seam's SKIP arg). litedbmodel's `when(present(h), whereX(col, h))`
          // makes the presence head the fragment's own bound head, so the frag's optional param IS the
          // presence signal; assert that invariant rather than guess.
          if (f.skipHead !== undefined) {
            const scalar = byHead.get(f.skipHead);
            if (scalar === undefined) {
              reasons.push(`node '${n.id}': skip head '${f.skipHead}' is not a typed bound head of its fragment (unsupported skip shape)`);
            } else {
              optionalHeadTypes.set(f.skipHead, scalar);
            }
          }
          f.params.forEach((p, pi) => {
            const port = paramPortFor(p, pi, n.id, reasons);
            if (port !== undefined) newPorts[`w${fi}p${pi}`] = port;
            recordCoalesce(p);
          });
        });
        return { ...n, ports: newPorts };
      }
      const rendered = renderStaticSql(stmts, readGraph.dialect, n.id, reasons);
      if (rendered === undefined) return n;
      // The single-`sql` port shape: the rendered SQL as a STATIC string port bc bakes as a native
      // literal, plus one typed port per bound `?` in placeholder order. For a map node the params may
      // include element-field refs (`{ref:[$e0, field]}`), which `paramPortFor` passes through (bc
      // types them from the parent outType). A mysql write's RETURNING gets the driver's pk-hint here
      // (no-op for reads / pg / sqlite, and for a write without a declared pk).
      const newPorts: Record<string, unknown> = { sql: bakeMysqlPkHint(rendered.sql, readGraph.dialect, nodePorts) };
      rendered.params.forEach((p, i) => {
        const port = paramPortFor(p, i, n.id, reasons, elementVar);
        if (port !== undefined) newPorts[`p${i}`] = port;
        recordCoalesce(p);
      });
      // A map node's ports live on `n.map`; the runner drives its child per parent element (bc emits
      // the batch/per-element structs). A plain read/write carries `n.ports`.
      if (isMap) {
        // A bc map node's `outType` is the PER-ELEMENT (per-iteration child) type — the typed-native
        // emitter synthesizes the produced parent-aligned array `{arr: element}` itself. bc's `.map()` recording stamps the FULL
        // produced array (`arr(childOutType)`) on the node instead; left as-is the emitter re-wraps it
        // and the built value nests one `arr` deeper than the component `outputType` field declares
        // (bc#151 OUTPUT_TYPE_INCONSISTENT). Strip the outer `arr` so the node carries the element
        // type the emitter wraps back to the declared produced array.
        const recorded = (n as { outType?: unknown }).outType;
        const elemOutType =
          recorded !== null && typeof recorded === 'object' && 'arr' in recorded
            ? (recorded as { arr: unknown }).arr
            : undefined;
        if (elemOutType === undefined) {
          reasons.push(`node '${n.id}': a map node's recorded outType is not the expected produced-array {arr:…} shape (got ${JSON.stringify(recorded)})`);
          return n;
        }
        return { ...n, outType: elemOutType, map: { ...mapObj, ports: newPorts } } as typeof n;
      }
      // A write with NO RETURNING hands back the single summary row `[{changes, lastInsertRowid}]`,
      // NOT the determined empty-row list its compile-time outType carries (that stamp existed only
      // to satisfy bc's all-nodes-typed gate, back when writes were never typed-codegen'd). Correct it
      // HERE — the module's row struct must describe what the exec seam actually returns.
      const lowered = { ...n, ports: newPorts };
      if (isWrite && nodePorts.returning === undefined) {
        correctedOutType.set(n.id, WRITE_SUMMARY_OUT_TYPE);
        return { ...lowered, outType: WRITE_SUMMARY_OUT_TYPE } as typeof lowered;
      }
      return lowered;
    });
    if (reasons.length > 0) throw new TypedNativeCoverageError(c.name, reasons);
    // Propagate a corrected node outType to the component `outputType` when the output Φ is a bare
    // ref to that node (`{ref:[nodeId]}`) — the single-write shape. Without this the runner's return
    // type stays the stale empty-row list while the node produces the summary row: a type mismatch
    // inside the generated module.
    const outputRef = refPathOf((c as unknown as { output?: unknown }).output);
    const componentOutputType =
      outputRef !== undefined && outputRef.length === 1 && correctedOutType.has(outputRef[0])
        ? correctedOutType.get(outputRef[0])
        : (c as unknown as { outputType?: unknown }).outputType;
    // Only the heads the lowered ports genuinely reference get an `InNR` field (identical rule to
    // the pre-E1 lowering: a WHERE COLUMN-NAME MARKER head is never bound, so it carries no field).
    // The SHARED tail (see {@link buildInputPorts}): required scalars + bc#110 array ports + bc#139
    // optional-default ports (#122). The SAME builder the legacy lowering uses — one place only.
    const inputPorts = buildInputPorts(c, inputPortTypes, inputPortElemTypes, optionalHeadTypes);
    return { ...c, body, inputPorts, outputType: componentOutputType } as unknown as Component;
  });
  return { ...ir, components };
}

// ═══════════════════════════════════════════════════════════════════════════════════════════
// NATIVE RETURNING-CHAINED TRANSACTIONS (E5/#120) — bake a multi-statement tx as a componentRef CHAIN.
//
// A composite/nested Command derives (in tx.ts) to a `TransactionPlan`: an ordered list of statements
// where a later statement binds an earlier statement's RETURNING row (`$.ref.<name>.<field>` →
// `{ref:[name,field]}`) into its own params. That imperative plan is the SoT for ordering + the
// RETURNING→next wiring; here it is RE-EXPRESSED (not re-derived) as one bc componentRef chain so the
// SAME typed-native emitter that bakes reads/writes bakes the whole transaction:
//
//   • each statement → a componentRef node with its baked `f_sql` literal + typed param ports;
//   • a `{ref:[writeName,field]}` param rewrites to `{ref:[producerNodeId,field]}` + sets the node's
//     `parent` = the producer — the SAME sequential-componentRef data-flow a single-key `.map` uses
//     (bc bakes the consumer's port as a NATIVE read of the producer's row: `cell_<producer>.<field>`);
//   • a PRODUCING statement's outType is a SINGLE `{obj:…}` (the RETURNING row) — REQUIRED: bc resolves
//     `{ref:[node,field]}` only against a single-row struct, not a row LIST (empirically verified).
//
// The generated runner chains the statements; the ONE generic exec seam wraps the whole runner in the
// transaction envelope (BEGIN … COMMIT, ROLLBACK on failure) — the envelope is the seam's concern, the
// statements + wiring are the SAME as any other write. NOT a parallel tx model: the ordering + binds
// come from the plan `deriveTransactionPlan` already produced.
//
// COVERED: gate-free, pure-`body` RETURNING-chained chains (the E5 ops delete / nestedCreate /
// nestedUpdate / nestedUpsert). A gate statement (requires/unique/idempotency) short-circuits with a
// `{committed:false, shortCircuit}` result the struct chain does not model — FAIL CLOSED (escalate),
// never a silently-wrong always-commit chain.
// ═══════════════════════════════════════════════════════════════════════════════════════════

/** Map a tx statement's leading SQL verb to its catalog component name (diagnostics + handler split). */
function txVerbComponent(sql: string): string {
  if (/^\s*INSERT\b/i.test(sql)) return 'Insert';
  if (/^\s*UPDATE\b/i.test(sql)) return 'Update';
  if (/^\s*DELETE\b/i.test(sql)) return 'Delete';
  return 'Write';
}

/** A non-RETURNING write's produced row is the summary `{changes, lastInsertRowid}` — a SINGLE obj (the
 * tx chain models every statement's produced value as one row, not a list). */
const TX_SUMMARY_ROW_TYPE = { obj: { changes: 'int', lastInsertRowid: 'int' } };

/**
 * Lower a {@link TransactionPlan} into a bc {@link ComponentGraphIR} that bakes the whole
 * RETURNING-chained transaction as ONE native componentRef chain (E5/#120). See the block comment
 * above. Fails closed (naming the shape) on any statement it cannot bake natively — a gate statement,
 * a batch op (no `writeMeta`), a non-ref param, an unresolvable column, or a multi-producer fan-in.
 */
export function lowerTransactionForNativeChain(
  plan: TransactionPlan,
  resolveColumnType: ColumnTypeResolver,
  dialect: DialectName,
  name: string,
): ComponentGraphIR {
  const reasons: string[] = [];
  // Gate / non-body statements are not modelled by the native struct chain (they carry short-circuit
  // control flow, not a produced row) — fail closed rather than bake a chain that always commits.
  const nonBody = plan.statements.filter((s) => s.role !== 'body' || s.gate !== undefined);
  if (nonBody.length > 0) {
    throw new TypedNativeCoverageError(name, [
      `transaction carries ${nonBody.length} gate/non-body statement(s) (${nonBody
        .map((s) => `${s.id}:${s.role}${s.gate !== undefined ? `/${s.gate}` : ''}`)
        .join(', ')}); the native RETURNING-chain covers gate-free pure-body chains only. A gate statement ` +
        `short-circuits with a {committed:false, shortCircuit} result the struct chain does not model — ` +
        `run this Command on the interpreter tx path (executeTransaction) or add native gate coverage.`,
    ]);
  }

  // Each producing statement's binds-name → its node id, for cross-statement RETURNING ref rewiring.
  const producerByName = new Map<string, string>();
  for (const s of plan.statements) if (s.binds !== undefined) producerByName.set(s.binds, s.id);

  const inputScalars = new Map<string, BcScalar>();
  const outTypeById = new Map<string, unknown>();

  const body = plan.statements.map((s) => {
    const meta = s.op.writeMeta;
    if (meta === undefined || meta.batch === true) {
      reasons.push(
        `statement '${s.id}' op is a batch/marker write — the native tx chain covers single-statement ` +
          `Insert/Update/Delete only (a batch op binds a {__batchRows} marker, not columns).`,
      );
      return { id: s.id, component: 'Write', ports: {}, outType: TX_SUMMARY_ROW_TYPE };
    }
    // Bake dialect-NEUTRAL `?` (the `?`→`$N` renumber is the runtime's single point). A mysql RETURNING
    // statement additionally gets the driver's pk-hint (from the plan op's `pk` descriptor) — the SAME
    // `mysqlPkHint` SSoT mode-2's `runtime.ts:390` uses — so the driver emulation re-selects the written
    // row by the real PK. No-op for pg/sqlite, non-RETURNING, or a statement without a declared pk.
    const ports: Record<string, unknown> = {
      // Pass the whole op so `mysqlPkHint` reads its `writeMeta.onConflict` (a tx upsert re-selects by
      // the conflict key). `mysqlPkHint` touches only `sql`; the deferred `params` ride through unused.
      sql: dialect === 'mysql' ? mysqlPkHint(s.op).sql : s.op.sql,
    };
    const producers = new Set<string>();
    s.op.params.forEach((p, i) => {
      const ref = refPathOf(p);
      if (ref === undefined) {
        reasons.push(`statement '${s.id}' param ${i} (${JSON.stringify(p)}) is not a {ref:[…]} — the native tx chain binds only input refs or prior-statement RETURNING refs.`);
        return;
      }
      if (ref.length === 1) {
        // An INPUT head: type it from the column this `?` binds (writeMeta.bindColumns, §4.1 schema SoT).
        const col = meta.bindColumns[i];
        if (col == null) {
          reasons.push(`statement '${s.id}' param ${i} refs input head '${ref[0]}' but binds no column (bindColumns[${i}] is null) — cannot type the native port.`);
          return;
        }
        const scalar = sqlTypeToBcScalar(resolveColumnType(meta.table, col));
        const prior = inputScalars.get(ref[0]);
        if (prior !== undefined && prior !== scalar) {
          reasons.push(`input head '${ref[0]}' resolves to conflicting scalar types ('${prior}' vs '${scalar}') across statements.`);
          return;
        }
        inputScalars.set(ref[0], scalar);
        ports[`p${i}`] = { ref: [ref[0]] };
      } else if (ref.length === 2) {
        // A prior-statement RETURNING ref: {ref:[writeName, field]} → {ref:[producerNodeId, field]}.
        const producerId = producerByName.get(ref[0]);
        if (producerId === undefined || producerId === s.id) {
          reasons.push(`statement '${s.id}' param ${i} refs '${ref[0]}.${ref[1]}' which is not an earlier statement's RETURNING bind — cannot wire natively.`);
          return;
        }
        producers.add(producerId);
        ports[`p${i}`] = { ref: [producerId, ref[1]] };
      } else {
        reasons.push(`statement '${s.id}' param ${i} ref path ${JSON.stringify(ref)} has ${ref.length} segments — the native tx chain covers 1-segment (input) or 2-segment (prior RETURNING) refs only.`);
      }
    });
    if (producers.size > 1) {
      reasons.push(`statement '${s.id}' references ${producers.size} distinct producer statements (${[...producers].join(', ')}) — the native tx chain covers a linear chain (one parent per node); a fan-in needs a richer wire.`);
    }
    // Every produced value is a SINGLE obj: the RETURNING row (typed per column), or the summary row.
    const outType =
      meta.returning.length > 0
        ? { obj: Object.fromEntries(meta.returning.map((col) => [col, sqlTypeToBcScalar(resolveColumnType(meta.table, col))])) }
        : TX_SUMMARY_ROW_TYPE;
    outTypeById.set(s.id, outType);
    const parent = producers.size === 1 ? [...producers][0] : undefined;
    return { id: s.id, component: txVerbComponent(s.op.sql), ports, outType, ...(parent !== undefined ? { parent } : {}) };
  });

  if (reasons.length > 0) throw new TypedNativeCoverageError(name, reasons);

  const inputPorts: Record<string, PortSchema> = {};
  for (const [head, scalar] of inputScalars) inputPorts[head] = { required: true, type: scalar };

  // Output Φ: an object keyed by each statement's binds-name (its node id if unnamed) → its produced
  // single-row struct. The runner returns EVERY statement's RETURNING row so the seam/consumer can read
  // the whole transaction's result (the chained ids included).
  const outputObj: Record<string, unknown> = {};
  const outputTypeObj: Record<string, unknown> = {};
  for (const s of plan.statements) {
    const key = s.binds ?? s.id;
    outputObj[key] = { ref: [s.id] };
    outputTypeObj[key] = outTypeById.get(s.id);
  }

  const component = {
    name,
    inputPorts,
    body,
    output: { obj: outputObj },
    outputType: { obj: outputTypeObj },
    // Sequential stages: a tx runs its statements IN ORDER (the RETURNING chain + deterministic order).
    plan: { concurrency: 1, groups: plan.statements.map((_, i) => [i]) },
  } as unknown as Component;
  return { irVersion: 1, exprVersion: 2, components: [component] };
}

/**
 * Resolve the READ codegen emitter for a target language, or throw. No fallback: a language
 * without a registered endpoint is a bc capability gap that fails LOUDLY (never a silent
 * substitution of the literal/interpreter/boxed emitter — an unspec'd fallback is invalid).
 */
export function typedEmitterFor(language: string, registered: readonly string[]): string {
  if (!(CODEGEN_LANGUAGES as readonly string[]).includes(language)) {
    throw new Error(
      `litedbmodel codegen: language '${language}' is not a supported target (supported: ${CODEGEN_LANGUAGES.join(', ')})`,
    );
  }
  const emitter = CODEGEN_EMITTER[language as CodegenLanguage];
  if (emitter === undefined) {
    throw new Error(
      `litedbmodel codegen: no READ codegen endpoint for '${language}'. litedbmodel codegen is STATIC ` +
        `de-interpreted codegen (spec §9) with NO literal/interpreter/boxed fallback (an unspec'd fallback ` +
        `is invalid). bc registers a READ endpoint only for ${Object.keys(CODEGEN_EMITTER).join('/')} — a ` +
        `'${language}' endpoint is a bc capability gap. ESCALATE to bc (bc#22 pattern); never substitute ` +
        `the literal emitter.`,
    );
  }
  if (!registered.includes(emitter)) {
    throw new Error(
      `litedbmodel codegen: bc's shared generator does not register the '${emitter}' emitter for '${language}' ` +
        `(bc registered: ${[...registered].sort().join(', ')}). ESCALATE to bc (bc#22 pattern), never fork locally.`,
    );
  }
  return emitter;
}

/**
 * Shape-INDEPENDENT emitter resolution — identical to {@link typedEmitterFor} now that there is
 * exactly one READ endpoint per language (kept as a distinct export for callers that historically
 * used the shape-independent form; both resolve the SAME table).
 */
export function codegenEmitterFor(language: string, registered: readonly string[]): string {
  return typedEmitterFor(language, registered);
}

/**
 * Generate the mode-3 READ codegen artifact for ONE §8 READ bundle in ONE target language. Lowers
 * the bundle's real Select-node read graph into the SQL-baking IR
 * ({@link lowerReadGraphForNativeSql}) — for go/rust's typed-NATIVE endpoint AND for TS's
 * `typescript-typed` endpoint alike (every language consumes the SAME baked-SQL ports). The lowering
 * derives each node's ports from its compiled
 * `statementsById` fragments, so a WHERE COLUMN-NAME MARKER head (`whereGe($.created_at, $.since)`
 * — `$.created_at` names the column, is never bound) is EXCLUDED from the emitted ports/input
 * struct, exactly as go/rust already handled it (#12 regression: TS previously fed the raw IR and
 * emitted a stray `created_at` binding → `unknown binding: created_at` at execution).
 *
 * A WRITE bundle is codegen'd through the SAME lowering and the SAME generic exec seam as a read —
 * read and write are ONE flow (both make SQL and execute it; both hand back rows), so they are NOT
 * split. `compileBundle` carries the write's component graph alongside its compiled `statement`, so
 * `bundle.readGraph` is present for a write too and the lowering bakes its `INSERT`/`UPDATE`/`DELETE`
 * exactly as it bakes a `SELECT`. A bundle carrying NEITHER a graph nor a statement is still a hard
 * error (nothing to generate).
 */
/**
 * Lower a §8 bundle to its final PORTABLE IR DOC — the EXACT unbranded `ComponentGraphIR` that
 * {@link generateCodegenArtifact} feeds to `loadCompiledIR` + `generateModule` (after
 * {@link lowerReadGraphForNativeSql}, or the tx-chain lowering). It is
 * language-INDEPENDENT (one doc → every language's module) and JSON-serializable — so a build can write it
 * out and drive bc's codegen CLI (`bc generate --in <doc.json> --lang <emitter>`) over the SAME lowering,
 * with NO duplication of the lowering. This doc is a BUILD-TIME codegen input ONLY (never read at runtime;
 * the runtime is the generated native module with baked SQL) — like graphddb's `operations.json`.
 */
export function lowerBundleToPortableIrDoc(bundle: SqlBundle, resolveColumnType: ColumnTypeResolver): ComponentGraphIR {
  // A COMPOSITE / nested Command carries a `transaction` plan but NO single-statement `readGraph` — a
  // multi-statement RETURNING chain re-expressed as ONE native componentRef chain (E5/#120).
  if (bundle.readGraph === undefined && bundle.transaction !== undefined) {
    return lowerTransactionForNativeChain(bundle.transaction, resolveColumnType, bundle.dialect, bundle.name);
  }
  if (bundle.readGraph === undefined) {
    throw new Error(
      `litedbmodel codegen: bundle '${bundle.name}' carries no component graph to generate from. Reads AND ` +
        `writes both compile one (compileBundle keeps the write's graph alongside its statement) — a bundle ` +
        `with neither was produced by some other path, and there is nothing to lower. No-assume, no-fallback.`,
    );
  }
  // THE SINGLE LOWERING (#116): bake the read/write's rendered per-dialect SQL into native-literal ports
  // (every read shape + writes). The native module carries the PRIMARY query ONLY — a relation is NOT
  // baked into it (#131): a RelationDecl belongsTo/hasMany is v1 LAZY-BATCH loading, a RUNTIME concern
  // the litedbmodel relation loader resolves over the single query primitive (dedupe → ONE batched child
  // query → group → distribute; `relation.rs`), never an executor/native-de-box primitive. The relation
  // ops ride `bundle.relations` (the runtime metadata the loader stitches), not the generated module.
  return lowerReadGraphForNativeSql(bundle.readGraph, resolveColumnType);
}

export function generateCodegenArtifact(
  bundle: SqlBundle,
  language: string,
  registeredLanguages: readonly string[],
  resolveColumnType: ColumnTypeResolver,
  runtimeImport?: string,
): CodegenArtifact {
  const emitter = typedEmitterFor(language, registeredLanguages);
  const ir = lowerBundleToPortableIrDoc(bundle, resolveColumnType);
  // bc 0.8.0 (scp-only-authoring, SA3/SA7): `generateModule` fail-closes on un-tokened IR
  // (`NON_COMPILED_IR`). This DERIVED `ir` carries no in-process provenance token, so re-adopt it at the
  // generation boundary via `loadCompiledIR` (recomputes the canonical fingerprint + mints the token — bc's
  // sanctioned "codegen fixture / derived IR" case). The bc CLI does the SAME `loadCompiledIR` over the
  // serialized doc, so the CLI path is equivalent by construction.
  const compiled = loadCompiledIR(ir);
  const module = generateModule(compiled, runtimeImport === undefined ? { language: emitter } : { language: emitter, runtimeImport });
  return { language: language as CodegenLanguage, module, companion: companionOf(bundle), bundle };
}

/**
 * The TS codegen EXECUTION path used to PROVE behavior-identity: a codegen runtime reads the SQL
 * catalog companion (the STATIC makeSQL bundle) and evaluates skip + value-specs → assemble →
 * render → execute — which is EXACTLY {@link executeBundle} over the SAME bundle.
 */
export function codegenExecuteBundleForTest(artifact: CodegenArtifact, input: Scope, db: SqliteDb): Value {
  return executeBundle(artifact.bundle, input, { db });
}

// ═══════════════════════════════════════════════════════════════════════════════════════════
// RUST COMPANION EMISSION (epic #123 / #124) — the boundary-injected `node_*` handlers + wire adapter.
//
// bc's `rust-typed-native` emitter generates the RUNTIME-FREE native module (ports structs + the INLINE
// de-box runner + the module-local `HandlerNR<comp>` / `WireValue`/`WireRow`/`WireList` traits). bc does
// NOT generate the handler impls (C4: handlers/wire adapters are boundary-INJECTED). litedbmodel — the
// bc-consumer — supplies them, and THIS is where litedbmodel GENERATES that companion (not hand-written):
// per-component `impl HandlerNR<comp>` whose `node_*` methods delegate UNIFORMLY to the SINGLE
// op-agnostic executor `exec(…, ExecMode::Rows|Summary)` — batch/skip nodes first marshal their params
// via `build_batch_params`/`build_skip_params` then run the SAME `exec` (no dedicated executor) — plus
// the one-line `wire_impls!` macro that bridges the module-local
// wire traits to the runtime's `Wire` classification (the orphan rule forbids the impls living in the
// runtime crate — the traits are local to the generated module). Derived from the SAME lowered IR
// {@link generateCodegenArtifact} feeds bc, so ports/param facts are single-sourced.
//
// RELATIONS are compiled into the companion as native literals (#141). The runtime receives only the
// baked SQL/key shape/limit values needed by its op-independent batch primitive; it performs no name
// lookup, JSON parsing, or childRelations traversal. Nested levels are direct calls emitted below.
// ═══════════════════════════════════════════════════════════════════════════════════════════

/** Camelize a node id to bc's ports-struct segment (`n0`→`N0`, `rel_posts`→`RelPosts`,
 * `tx_body_0`→`TxBody0`) — mirrors bc's `PortsNR<comp><nodeCamel>` identifier derivation. */
function nodeCamel(id: string): string {
  return id
    .split('_')
    .map((s) => (s.length === 0 ? '' : s.charAt(0).toUpperCase() + s.slice(1)))
    .join('');
}

/** A node's produced value is the non-returning WRITE summary row `{changes, lastInsertRowid}` (plain
 * `{arr:{obj}}` or tx `{obj}`) ⇒ the handler runs the write via the summary exec, not the rows exec. */
function isSummaryOut(outType: unknown): boolean {
  const o = outType as { arr?: { obj?: unknown }; obj?: unknown } | null;
  const obj = (o?.arr?.obj ?? o?.obj) as Record<string, unknown> | undefined;
  if (obj === undefined || typeof obj !== 'object') return false;
  const keys = Object.keys(obj).sort();
  return keys.length === 2 && keys[0] === 'changes' && keys[1] === 'lastInsertRowid';
}

/** The ordered param-port keys (`p0`,`p1`,…) present on a node's ports object. */
function paramKeys(ports: Record<string, unknown>): string[] {
  return Object.keys(ports)
    .filter((k) => /^p\d+$/.test(k))
    .sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
}

/** One param port → its runtime bind expression: an IN-list / array-bound head lowers via `wp_array`
 * (always a `Value::Arr`; the Postgres-native-array vs MySQL/SQLite-`json_each(?)`-JSON DIALECT decision
 * is resolved by the Driver's param-binder — the array-bind SSoT — so the companion never branches on
 * dialect); every scalar (input ref, element-field ref, literal, chained tx ref) via the type-agnostic
 * `wp`. The array-ness is read off the component input port schema (the SSoT bc used to bake the field's
 * Rust type) — no re-derivation. */
function paramBindExpr(ports: Record<string, unknown>, key: string, inputPorts: Record<string, PortSchema>): string {
  const ref = refPathOf(ports[key]);
  const isArray = ref !== undefined && ref.length === 1 && (inputPorts[ref[0]] as { type?: unknown } | undefined)?.type === 'array';
  return isArray ? `litedbmodel_runtime::wp_array(&ports.f_${key})` : `litedbmodel_runtime::wp(&ports.f_${key})`;
}

/** Emit ONE read/write/tx node's `node_*` handler body (a plain rows/summary read/write, a skip read, a
 * batch write, or an inline per-element map — all delegate to the runtime executors). Batched relations
 * and tx statements are emitted by their own paths. */
function emitReadWriteNode(
  n: Record<string, unknown>,
  comp: string,
  bundle: SqlBundle,
  inputPorts: Record<string, PortSchema>,
): string {
  const id = n.id as string;
  const method = `node_${id}`;
  const camel = nodeCamel(id);
  const isMap = 'map' in n;
  const ports = (isMap ? (n.map as { ports: Record<string, unknown> }).ports : (n.ports as Record<string, unknown>)) ?? {};
  const portsTy = `PortsNR${comp}${camel}`;
  const sig = `    fn ${method}(&self, ports: &${portsTy}, _bound: Option<String>) -> Result<Wire, BehaviorError> {`;
  // SKIP read: head + presence-gated WHERE fragments + tail (the runtime assembles present fragments).
  if ('sql_head' in ports) {
    const headParams = Object.keys(ports)
      .filter((k) => /^h\d+$/.test(k))
      .sort()
      .map((k) => paramBindExpr(ports, k, inputPorts));
    const tailParams = Object.keys(ports)
      .filter((k) => /^t\d+$/.test(k))
      .sort()
      .map((k) => paramBindExpr(ports, k, inputPorts));
    const fragKeys = Object.keys(ports)
      .filter((k) => /^w\d+$/.test(k))
      .sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
    const frags = fragKeys.map((wk) => {
      const pk = Object.keys(ports)
        .filter((k) => new RegExp(`^${wk}p\\d+$`).test(k))
        .sort();
      // A fragment is skip-optional iff any of its param heads is an OPTIONAL input port (bc bakes an
      // `Option<T>` field); it is PRESENT iff that Option is `Some`. A required fragment is always present.
      const optKey = pk.find((k) => {
        const r = refPathOf(ports[k]);
        return r !== undefined && r.length === 1 && (inputPorts[r[0]] as { required?: boolean } | undefined)?.required === false;
      });
      if (optKey !== undefined) {
        return `litedbmodel_runtime::SkipFrag { sql: ports.f_${wk}.clone(), present: ports.f_${optKey}.is_some(), params: ports.f_${optKey}.iter().map(|v| litedbmodel_runtime::wp(v)).collect() }`;
      }
      const params = pk.map((k) => paramBindExpr(ports, k, inputPorts)).join(', ');
      return `litedbmodel_runtime::SkipFrag { sql: ports.f_${wk}.clone(), present: true, params: vec![${params}] }`;
    });
    return [
      sig,
      `        let frags = vec![${frags.join(', ')}];`,
      // Assemble the present skip fragments (WHERE/AND connectors + params) via the marshaling helper,
      // then run through the SINGLE `exec` — no skip-specific executor (the executor surface is just exec).
      `        let (sql, params) = litedbmodel_runtime::build_skip_params(&ports.f_sql_head, &[${headParams.join(', ')}], &frags, &ports.f_sql_tail, &[${tailParams.join(', ')}]);`,
      `        let ctx = self.src.ctx().map_err(cvt)?;`,
      `        litedbmodel_runtime::exec(&ctx, &sql, &params, litedbmodel_runtime::ExecMode::Rows).map_err(cvt)`,
      `    }`,
    ].join('\n');
  }
  // Native relation batch: SQL + typed key-column arrays are baked BC ports. The ordinary handler
  // consumes every port, marshals only values in the shared runtime, then calls the same generic exec.
  if ('relation_shape' in ports) {
    const valueKeys = Object.keys(ports)
      .filter((k) => /^v\d+$/.test(k))
      .sort((a, b) => Number(a.slice(1)) - Number(b.slice(1)));
    const columns = valueKeys.map((k) => `ports.f_${k}.iter().map(|v| litedbmodel_runtime::wp(v)).collect::<Vec<Value>>()`);
    const shape = ports.relation_shape === 'per_column' ? 'PerColumn' : 'SingleJson';
    return [
      sig,
      `        let columns: Vec<Vec<Value>> = vec![${columns.join(', ')}];`,
      `        if columns.first().is_none_or(Vec::is_empty) { return Ok(Wire::from_rows(Vec::new())); }`,
      `        let (sql, params) = litedbmodel_runtime::build_relation_params(&ports.f_sql, &columns, litedbmodel_runtime::ArrayParamShape::${shape});`,
      `        let ctx = self.src.ctx().map_err(cvt)?;`,
      `        litedbmodel_runtime::exec(&ctx, &sql, &params, litedbmodel_runtime::ExecMode::Rows).map_err(cvt)`,
      `    }`,
    ].join('\n');
  }
  // BATCH write: parallel column arrays zipped into ONE json_each statement.
  if ('v0' in ports) {
    const marker = batchRowsMarkerOf(bundle.readGraph?.statementsById[id] ?? []);
    if (marker === undefined) throw new Error(`litedbmodel companion: batch node '${id}' has no batch-rows marker to derive its columns`);
    const cells = marker.columns.map((_, i) => `ports.f_v${i}.iter().map(|v| litedbmodel_runtime::wp(v)).collect::<Vec<Value>>()`);
    const cols = marker.columns.map((c) => JSON.stringify(c)).join(', ');
    const returning = !isSummaryOut(n.outType);
    // The param-shape DESCRIPTOR is baked from the marker type resolved at SQL generation (`__batchArray`
    // per-column → PerColumn / `__batchRows` zipped → SingleJson). The companion is a THIN uniform
    // delegate: marshal the params through the SHARED `build_batch_params` SSoT (the SAME fn mode-2's
    // render calls — no per-op zip), then run the SINGLE `exec` — no batch-specific executor.
    const shape = marker.perColumn ? 'PerColumn' : 'SingleJson';
    const mode = returning ? 'Rows' : 'Summary';
    return [
      sig,
      `        let cells: Vec<Vec<Value>> = vec![${cells.join(', ')}];`,
      `        let params = litedbmodel_runtime::build_batch_params(&[${cols}], &cells, litedbmodel_runtime::ArrayParamShape::${shape}, ports.f_sql.matches('?').count());`,
      `        let ctx = self.src.ctx().map_err(cvt)?;`,
      `        litedbmodel_runtime::exec(&ctx, &ports.f_sql, &params, litedbmodel_runtime::ExecMode::${mode}).map_err(cvt)`,
      `    }`,
    ].join('\n');
  }
  // PLAIN read / RETURNING-write / non-returning-write / inline per-element map.
  const params = paramKeys(ports).map((k) => paramBindExpr(ports, k, inputPorts)).join(', ');
  const mode = isSummaryOut(n.outType) ? 'litedbmodel_runtime::ExecMode::Summary' : 'litedbmodel_runtime::ExecMode::Rows';
  return [
    sig,
    `        let ctx = self.src.ctx().map_err(cvt)?;`,
    `        litedbmodel_runtime::exec(&ctx, &ports.f_sql, &[${params}], ${mode}).map_err(cvt)`,
    `    }`,
  ].join('\n');
}

/**
 * Generate the litedbmodel RUST COMPANION source for one bundle's generated module (referenced as
 * `super::<moduleName>`). Returns a self-contained module file: the `wire_impls!` bridge + the
 * `impl HandlerNR<comp>` (node_* → runtime executors) + the litedbmodel-consumer entry point
 * (`handler(driver)` for reads/writes; `run(driver, in_) -> committed:bool` for transactions, which
 * wraps the chain in the runtime transaction envelope).
 */
/**
 * #135 find-hardLimit auto-wiring — emit the GUARDED `run` entry for a read whose ReadGraph carries a
 * `findGuard`. It runs the bc runner, then enforces the cap via the SHARED
 * `litedbmodel_runtime::check_find_hard_limit` (the SAME core the mode-2 `assert_find_guard` calls) with
 * the cap/model baked from the meta — the `LimitExceededError` (`context: find`) is raised OUTSIDE the
 * runner so it is byte-equal to mode-2 (NOT swallowed into the runner's `OP_FAILED`).
 *
 * The runner's typed return (`Vec<Row>`) is read from the bc-generated module's PUBLIC runner signature
 * (`pub fn run_native_raw_struct_<Comp>… -> Result<<Ret>, BehaviorError>`) — that signature is bc's
 * output CONTRACT, so this reads bc output, never modifies bc, and needs no bc-internal type name. Empty
 * for a tx bundle or a read with no `findGuard` (⇒ no entry emitted, no behavior change).
 */
function emitGuardedFindEntry(bundle: SqlBundle, comp: string, ir: ReturnType<typeof lowerBundleToPortableIrDoc>): string[] {
  const isTx = bundle.transaction !== undefined && bundle.readGraph === undefined;
  const findGuard = isTx ? undefined : bundle.readGraph?.findGuard;
  if (findGuard === undefined) return [];
  // Read the runner's typed return from the bc-generated module's public signature (its output contract).
  const code = generateModule(loadCompiledIR(ir), { language: 'rust-typed-native' }).code;
  const match = code.match(new RegExp(`pub fn run_native_raw_struct_${comp}<[^>]*>\\s*\\([\\s\\S]*?\\)\\s*->\\s*Result<([\\s\\S]*?),\\s*BehaviorError>`));
  if (match === null) {
    throw new Error(`litedbmodel companion (#135 find-guard): cannot read the return type of run_native_raw_struct_${comp} from the bc-generated module signature`);
  }
  const ret = match[1].trim();
  if (!ret.startsWith('Vec<')) {
    throw new Error(`litedbmodel companion (#135 find-guard): a find guard expects a row-LIST return (Vec<…>), but run_native_raw_struct_${comp} returns '${ret}'`);
  }
  return [
    ``,
    `/// The GUARDED find entry (#135): run the native read, then enforce the find hard-limit via the`,
    `/// SAME shared \`check_find_hard_limit\` the mode-2 read-graph guard calls (cap/model baked from the`,
    `/// ReadGraph findGuard meta; the \`LIMIT hardLimit + 1\` is already in the baked SQL). A cap-exceeding`,
    `/// read throws \`LimitExceededError\` (\`context: find\`), byte-equal to mode-2 — surfaced OUTSIDE the`,
    `/// runner (the runner's BehaviorError maps to RuntimeError::Sql). The litedbmodel-consumer calls`,
    `/// THIS for a capped find, not the bare runner.`,
    `pub fn run(driver: &dyn Driver, in_: InNR${comp}) -> Result<${ret}, litedbmodel_runtime::RuntimeError> {`,
    `    let rows = run_native_raw_struct_${comp}(&handler(driver), in_)`,
    `        .map_err(|e| litedbmodel_runtime::RuntimeError::Sql(litedbmodel_runtime::SqlFailure { kind: e.code, policy: "fail".to_string(), sqlite_code: None, message: e.message }))?;`,
    `    litedbmodel_runtime::check_find_hard_limit(${findGuard.hardLimit}i64, rows.len() as i64, Some(${JSON.stringify(findGuard.model)}))?;`,
    `    Ok(rows)`,
    `}`,
  ];
}

/** Standard generated consumer entry for an unguarded read/write. */
function emitNativeRunEntry(bundle: SqlBundle, comp: string, ir: ReturnType<typeof lowerBundleToPortableIrDoc>): string[] {
  if (bundle.readGraph?.findGuard !== undefined || (bundle.transaction !== undefined && bundle.readGraph === undefined)) return [];
  const code = generateModule(loadCompiledIR(ir), { language: 'rust-typed-native' }).code;
  const match = code.match(new RegExp(`pub fn run_native_raw_struct_${comp}<[^>]*>\\s*\\([\\s\\S]*?\\)\\s*->\\s*Result<([\\s\\S]*?),\\s*BehaviorError>`));
  if (match === null) throw new Error(`litedbmodel generated run: cannot read ${comp} return type`);
  const ret = match[1].trim();
  return [
    ``,
    `pub fn run(driver: &dyn Driver, in_: InNR${comp}) -> Result<${ret}, litedbmodel_runtime::RuntimeError> {`,
    `    run_native_raw_struct_${comp}(&handler(driver), in_)`,
    `        .map_err(|e| litedbmodel_runtime::RuntimeError::Sql(litedbmodel_runtime::SqlFailure { kind: e.code, policy: "fail".to_string(), sqlite_code: None, message: e.message }))`,
    `}`,
  ];
}

// ═══════════════════════════════════════════════════════════════════════════════════════════
// TYPED-CHILD RELATIONS (#140) — the batched CHILD read de-boxed to a TYPED struct via bc.
//
// #131 keeps a relation OUT of the primary module (it is v1 lazy-batch, a runtime concern). #140 makes
// the batched CHILD ROWS TYPED: the relation loader is STILL the ONE SQL authority (`fetch_child_rows`
// dedupes/casts/renders/binds/execs — shared with mode-2, op.sql byte-unchanged), and the driver rows it
// fetches are de-boxed to TYPED child structs by a bc-generated CHILD MODULE — the SAME bc typed-native
// de-box a primary read uses — so the loader's group/distribute run over typed structs, NOT `Value::Obj`.
//
// The child read is DECLARED-VIA-BC (`compileEager` over a bare `Select({table: targetTable, select})`
// with the relation's `static columns` — NO hand-written IR) and generated through the SAME
// `generateCodegenArtifact` a primary read uses. The child module's baked SQL is inert (the loader runs
// op.sql, the SSoT); only its typed struct + bc de-box are consumed. The litedbmodel CHILD COMPANION
// bridges the loader-fetched `Wire` into that de-box (a stash handler → `run_native_raw_struct_<Comp>`).
// ═══════════════════════════════════════════════════════════════════════════════════════════

/** The deterministic file/mod + component names for ONE relation node at `path` (e.g. `['posts']` →
 * `generated_<op>_rel_posts`; `['posts','comments']` → `generated_<op>_rel_posts_comments`). The primary
 * companion's hydrator and the child-artifact generator both derive names HERE, so they never drift. */
function relChildNames(_primaryModuleName: string, path: readonly string[]): { module: string; companion: string; component: string } {
  const joined = path.join('_');
  return {
    module: `rel_${joined}`,
    companion: ``,
    component: `Rel${path.map(nodeCamel).join('')}`,
  };
}

/** The relation's `static columns` for its target table — each PROJECTED column typed from the SAME
 * `resolveColumnType` the primary read uses (so the child struct's types match the driver rows exactly).
 * A relation's `select` is always plain columns (`compileRelationOp` rejects `*`/computed). */
function relChildColumns(op: RelationOp, resolveColumnType: ColumnTypeResolver): ModelColumns {
  const table = op.targetTable;
  const select = op.select;
  if (table === undefined || select === undefined) {
    throw new Error(`litedbmodel codegen (#140): relation '${op.name}' carries no targetTable/select — cannot type its child de-box struct`);
  }
  return { [table]: Object.fromEntries(select.map((c) => [c, resolveColumnType(table, c)])) };
}

/** Declare the actual batched relation query as a BC component. `sql` comes exclusively from the
 * compiled RelationDecl; the generated node consumes typed key-array ports and executes normally. */
function compileRelationChildBundle(op: RelationOp, component: string, resolveColumnType: ColumnTypeResolver): SqlBundle {
  const columns = relChildColumns(op, resolveColumnType);
  const base = { table: op.targetTable!, select: [...op.select!], sql: op.sql, keyShape: op.keyShape, targetKeys: targetKeyCols(op) };
  const arity = parentKeyCols(op).length;
  if (arity > 2) throw new Error(`litedbmodel codegen: relation '${op.name}' key arity ${arity} is not native-covered`);
  const contract = arity === 1
    ? compileEager(component, ($, L) => (L as unknown as { RelationBatch(p: unknown): unknown }).RelationBatch({ ...base, 'key.0': ($ as Record<string, unknown>).k0 }), { columns })
    : compileEager(component, ($, L) => (L as unknown as { RelationBatch(p: unknown): unknown }).RelationBatch({ ...base, 'key.0': ($ as Record<string, unknown>).k0, 'key.1': ($ as Record<string, unknown>).k1 }), { columns });
  return compileBundle(contract, component, [], op.dialect as DialectName, undefined, resolveColumnType);
}

/** A relation op's ordered TARGET / PARENT key columns (single-key → 1 elem; composite → the tuple). */
function targetKeyCols(op: RelationOp): string[] {
  return op.targetKeys !== undefined ? [...op.targetKeys] : [op.targetKey!];
}
function parentKeyCols(op: RelationOp): string[] {
  return op.parentKeys !== undefined ? [...op.parentKeys] : [op.parentKey!];
}

/** The generated, recursively hydrated child type for one relation node. */
function hydratedRelationType(op: RelationOp, primaryModuleName: string, path: string[]): string {
  const row = `self::${relChildNames(primaryModuleName, path).module}::T0`;
  const children = op.childRelations ?? [];
  if (children.length === 0) return row;
  return `(${[row, ...children.map((child) => `Vec<${hydratedRelationType(child, primaryModuleName, [...path, child.name])}>`)].join(', ')})`;
}

/** The `child_key_of` closure body — the TYPED child's target-key tuple by FIELD ACCESS (no `obj_get`):
 * `|c| vec![litedbmodel_runtime::wp(&c.<tk0>), …]`. */
function childKeyOfClosure(cols: readonly string[]): string {
  const parts = cols.map((c) => `litedbmodel_runtime::wp(&c.${c})`).join(', ');
  return `|c| vec![${parts}]`;
}

/** The parent `key_of` closure for a NESTED level (the just-hydrated typed child becomes the parent):
 * single → `|c| c.<k>`; composite → `|c| (c.<k0>, c.<k1>)`. */
function nestedKeyOfClosure(cols: readonly string[]): string {
  if (cols.length === 1) return `|c| c.${cols[0]}`;
  return `|c| (${cols.map((c) => `c.${c}`).join(', ')})`;
}

/** Emit ONE top-level relation's TYPED hydrator into the primary companion: it drives the SHARED
 * per-level `hydrate_relation_typed` (the child rows de-box to typed structs; the loader's group/distribute
 * run over them), then UNROLLS each `childRelations` level (Rust cannot recurse generically over the
 * heterogeneous child types) — each level a batched read (N+1-free). Returns `Vec<(P, Vec<child>)>`. The
 * parent `key_of` is supplied by the caller (keeps the hydrator generic over the primary read struct). */
function emitRelationHydrator(op: RelationOp, primaryModuleName: string): string {
  // Recursively emit one batched level + its nested `childRelations` levels. Each level receives a
  // compile-time Rust descriptor literal; `parentsExpr` produces the typed parent Vec;
  // `keyOf` reads the parent key; `levelVar` binds this level's `Vec<(parent, Vec<childStruct>)>` result.
  const emitLevel = (
    op: RelationOp,
    path: string[],
    parentsExpr: string,
    keyOf: string,
    levelVar: string,
    indent: string,
  ): string[] => {
    const names = relChildNames(primaryModuleName, path);
    const children = op.childRelations ?? [];
    const rawVar = children.length === 0 ? levelVar : `${levelVar}_raw`;
    const parentCols = parentKeyCols(op);
    const keyInputs = parentCols.map((col, i) => `k${i}: ${parentsExpr}.iter().map(|parent| parent.${col}.clone()).collect()`);
    const lines = [
      `${indent}let child_rows = self::${names.module}::run_native_raw_struct_${names.component}(`,
      `${indent}    &self::${names.module}::handler(driver),`,
      `${indent}    self::${names.module}::InNR${names.component} { ${keyInputs.join(', ')} },`,
      `${indent}).map_err(|e| litedbmodel_runtime::RuntimeError::Sql(litedbmodel_runtime::SqlFailure { kind: e.code, policy: "fail".to_string(), sqlite_code: None, message: e.message }))?;`,
      `${indent}litedbmodel_runtime::check_relation_hard_limit(${op.hardLimit === undefined ? 'None' : `Some(${op.hardLimit}i64)`}, child_rows.len(), ${op.targetTable === undefined ? 'None' : `Some(${JSON.stringify(op.targetTable)})`}, ${JSON.stringify(op.name)})?;`,
      `${indent}let ${rawVar} = litedbmodel_runtime::hydrate_children(`,
      `${indent}    ${parentsExpr}, ${keyOf}, child_rows,`,
      `${indent}    ${childKeyOfClosure(targetKeyCols(op))},`,
      `${indent});`,
    ];
    children.forEach((child, i) => {
      const childPath = [...path, child.name];
      const childLevelVar = `${levelVar}_${i}`;
      // The just-hydrated typed children of THIS level become the parents of the next batched level
      // (batched once per level — never N+1). Each nested level is fully typed: its own bc child module +
      // de-box + typed key accessors read off the parent child struct.
      lines.push(
        ...emitLevel(
          child,
          childPath,
          `${rawVar}.iter().flat_map(|(_, cs)| cs.iter().cloned()).collect::<Vec<_>>()`,
          nestedKeyOfClosure(parentKeyCols(child)),
          childLevelVar,
          indent,
        ),
      );
    });
    if (children.length > 0) {
      children.forEach((_child, i) => lines.push(`${indent}let mut ${levelVar}_${i}_iter = ${levelVar}_${i}.into_iter();`));
      const nestedValues = children.map((_child, i) => `${levelVar}_${i}_iter.next().expect("generated relation hydration cardinality drift").1`);
      lines.push(
        `${indent}let ${levelVar} = ${rawVar}.into_iter().map(|(parent, children)| {`,
        `${indent}    let children = children.into_iter().map(|child| (child, ${nestedValues.join(', ')})).collect();`,
        `${indent}    (parent, children)`,
        `${indent}}).collect::<Vec<_>>();`,
      );
    }
    return lines;
  };
  return [
    ``,
    `/// TYPED hydrator for the '${op.name}' relation (#140): batch-load the child rows and de-box them to`,
    `/// TYPED structs via the SHARED \`hydrate_relation_typed\` (loader = the ONE SQL/dedupe/group/distribute`,
    `/// authority; children are typed, NOT \`Value::Obj\`). SQL/key metadata is baked into this function;`,
    `/// \`key_of\` is the caller's parent-key accessor. Deeper relations are direct-call expanded here.`,
    `pub fn hydrate_${op.name}(`,
    `    parents: Vec<T0>,`,
    `    driver: &dyn litedbmodel_runtime::Driver,`,
    `) -> Result<Vec<(T0, Vec<${hydratedRelationType(op, primaryModuleName, [op.name])}>)>, litedbmodel_runtime::RuntimeError> {`,
    ...emitLevel(op, [op.name], 'parents', nestedKeyOfClosure(parentKeyCols(op)).replace('|c|', '|parent|').replaceAll('c.', 'parent.'), 'level', '    '),
    `    Ok(level)`,
    `}`,
  ].join('\n');
}

function generateRustStaticAdapter(bundle: SqlBundle, moduleName: string, resolveColumnType: ColumnTypeResolver, inline = false): string {
  const ir = lowerBundleToPortableIrDoc(bundle, resolveColumnType);
  const c = ir.components[0] as unknown as {
    name: string;
    body: Record<string, unknown>[];
    inputPorts?: Record<string, PortSchema>;
  };
  const comp = c.name;
  const inputPorts = c.inputPorts ?? {};
  const isTx = bundle.transaction !== undefined && bundle.readGraph === undefined;

  const head = [
    ...(inline ? [] : [`#![allow(dead_code, unused_imports, non_snake_case, clippy::all)]`]),
    `// litedbmodel static runtime adapter for \`${moduleName}\` (co-located with the bc core).`,
    `// bc emits the runtime-free native module (ports + de-box runner + wire traits); litedbmodel emits`,
    `// THIS companion — the boundary-injected node_* handlers + wire adapter (bc C4). Every node_*`,
    `// delegates to litedbmodel_runtime's op-agnostic Driver-backed executors (the exec SSoT); the wire`,
    `// classification is single-sourced in the runtime and bridged here by the wire_impls! macro (the`,
    `// orphan rule forbids the module-local wire trait impls living in the runtime crate).`,
    ...(inline ? [] : [`use super::${moduleName}::*;`]),
    `use litedbmodel_runtime::{Driver, SqlFailure, Value, Wire};`,
    `// The dialect is a CONNECTION property (\`self.driver.dialect()\`), not baked here — the generated`,
    `// SQL is dialect-neutral in its placeholders (\`?\`); the runtime renumbers \`?\`→\`\$N\` per connection.`,
    ``,
    `litedbmodel_runtime::wire_impls!();`,
    ``,
    `/// Map a runtime SQL failure to the module-local BehaviorError (byte-equal codes: the bc runner`,
    `/// re-wraps a node failure as OP_FAILED regardless, so only the message/detail cross this seam).`,
    `fn cvt(e: SqlFailure) -> BehaviorError { BehaviorError::new(e.kind, e.message) }`,
    ``,
  ];

  if (isTx) {
    const methods = c.body
      .filter((n) => !('cond' in n))
      .map((n) => {
        const id = n.id as string;
        const camel = nodeCamel(id);
        const ports = (n.ports as Record<string, unknown>) ?? {};
        const params = paramKeys(ports)
          .map((k) => `litedbmodel_runtime::wp(&ports.f_${k})`)
          .join(', ');
        // A tx-chain statement produces a SINGLE row (the RETURNING row / summary obj) — the tx runner
        // de-boxes each via `as_row`, not `as_list` — so the `*Single` wire-shape mode.
        const mode = isSummaryOut(n.outType) ? 'litedbmodel_runtime::ExecMode::SummarySingle' : 'litedbmodel_runtime::ExecMode::RowSingle';
        return [
          `    fn node_${id}(&self, ports: &PortsNR${comp}${camel}, _bound: Option<String>) -> Result<Wire, BehaviorError> {`,
          `        litedbmodel_runtime::exec(self.ctx, &ports.f_sql, &[${params}], ${mode}).map_err(cvt)`,
          `    }`,
        ].join('\n');
      });
    return [
      ...head,
      `struct TxRt<'a, 'b, 'c> { ctx: &'a litedbmodel_runtime::ExecutionContext<'b, 'c> }`,
      `impl<'a, 'b, 'c> HandlerNR${comp} for TxRt<'a, 'b, 'c> {`,
      `    type Wire = Wire;`,
      ...methods,
      `}`,
      ``,
      `/// The litedbmodel-consumer entry: run the RETURNING-chained transaction (BEGIN…COMMIT / ROLLBACK`,
      `/// via the runtime tx envelope; every statement runs on the ONE pinned tx-scoped ctx). Returns`,
      `/// \`true\` when the chain committed, \`false\` on rollback.`,
      `pub fn run(driver: &dyn Driver, in_: InNR${comp}) -> Result<bool, BehaviorError> {`,
      `    litedbmodel_runtime::run_transaction(driver, |ctx| run_native_raw_struct_${comp}(&TxRt { ctx }, in_)).map_err(cvt)`,
      `}`,
      ``,
      `/// The OPTIONS-aware / ROUTED / RETRYING tx entry (#135 routing+isolation + #136 retry): open the`,
      `/// tx from a ConnSource (single driver, or the WRITER pool of a named connection) and apply the`,
      `/// TransactionOptions (isolation prelude, rollback_only, and the SHARED #81 retry loop). \`make_in\``,
      `/// REBUILDS the input per attempt so a retryable failure (deadlock / serialization / connection) can`,
      `/// re-run the whole tx on a FRESH connection — bc-independent (NO Clone on InNR${comp}). The bc`,
      `/// runner's BehaviorError maps to a SqlFailure whose message the SHARED is_retryable_tx_error SSoT`,
      `/// classifies. Returns \`true\` iff it committed; a non-retryable / retry-exhausted error re-raises.`,
      `pub fn run_on(src: litedbmodel_runtime::ConnSource, connection: Option<&str>, dialect: litedbmodel_runtime::TxDialect, options: &litedbmodel_runtime::TransactionOptions, make_in: impl Fn() -> InNR${comp}) -> Result<bool, litedbmodel_runtime::SqlFailure> {`,
      `    litedbmodel_runtime::run_transaction_on(src, connection, dialect, options, move |ctx| {`,
      `        run_native_raw_struct_${comp}(&TxRt { ctx }, make_in()).map_err(|e| litedbmodel_runtime::SqlFailure { kind: e.code, policy: "fail".to_string(), sqlite_code: None, message: e.message })`,
      `    })`,
      `}`,
      ``,
    ].join('\n');
  }

  // Every read/write/inline-map node delegates to the SINGLE op-agnostic executor via emitReadWriteNode
  // (an inline `.map` runs its child per parent element; a plain read/write runs once). Relations are NOT
  // handler nodes (#131) — they ride `bundle.relations` for the runtime loader, emitted below.
  const methods = c.body.filter((n) => !('cond' in n)).map((n) => emitReadWriteNode(n, comp, bundle, inputPorts));
  // #140 typed-child hydrators: one per top-level relation. Each drives the SHARED `hydrate_relation_typed`
  // (the child rows de-box to TYPED structs via the bc child module — NO `Value::Obj` grouped/retained) and
  // batches every `childRelations` level. The child bc modules + de-box companions are emitted separately
  // (`generateRelationChildArtifacts`) and referenced here by the SAME `relChildNames` derivation.
  const relationHydrators = Object.values(bundle.relations).map((op) => emitRelationHydrator(op, moduleName));
  // #135 find-hardLimit auto-wiring: when the ReadGraph carries a `findGuard` (the compile baked
  // `LIMIT hardLimit + 1` + the cap/model meta), emit a GUARDED `run` entry that runs the bc runner
  // then enforces the cap via the SHARED `check_find_hard_limit` — cap/model from the meta, the
  // LimitExceededError surfaced OUTSIDE the runner (not potted into OP_FAILED) so it is byte-equal to
  // mode-2. The runner's typed return (`Vec<Row>`) is read from the bc-generated module's PUBLIC runner
  // signature (its output CONTRACT) — reading bc output, not modifying bc.
  const guardedFindEntry = emitGuardedFindEntry(bundle, comp, ir);
  const nativeRunEntry = emitNativeRunEntry(bundle, comp, ir);
  return [
    ...head,
    `// The handler holds a ConnSource (#135): a single Driver (routing=None, byte-identical single-pool)`,
    `// OR a RoutingConfig (read→reader / write→writer, named-DB). node_* resolves the ctx per statement`,
    `// via \`self.src.ctx()\` — reader/writer routing is applied ONCE in the central seam, never per op.`,
    `pub struct Rt<'a> { src: litedbmodel_runtime::ConnSource<'a> }`,
    `impl<'a> HandlerNR${comp} for Rt<'a> {`,
    `    type Wire = Wire;`,
    ...methods,
    `}`,
    ``,
    `/// The litedbmodel-consumer entry: build the runtime-backed handler for \`${comp}\` over a SINGLE`,
    `/// driver (byte-identical single-pool path). The consumer calls`,
    `/// \`run_native_raw_struct_${comp}(&handler(driver), in_)\` — supplying NO node_* itself.`,
    `pub fn handler(driver: &dyn Driver) -> Rt<'_> { Rt { src: litedbmodel_runtime::ConnSource::Driver(driver) } }`,
    ``,
    `/// The ROUTED consumer entry (#135): the handler routes each read to the reader pool and each write`,
    `/// to the writer pool (named-DB via the registry) through the SAME central \`connection_for\` seam.`,
    `pub fn handler_routed(routing: &litedbmodel_runtime::RoutingConfig) -> Rt<'_> { Rt { src: litedbmodel_runtime::ConnSource::Routing(routing) } }`,
    ...guardedFindEntry,
    ...nativeRunEntry,
    ...relationHydrators,
    ``,
  ].join('\n');
}

/** Emit the sole consumer-visible Rust artifact: a runtime-free bc core followed by litedbmodel's
 * co-located static adapter. Relation child BC cores are nested modules in this same source file. */
export function generateRustExecutable(
  bundle: SqlBundle,
  moduleName: string,
  resolveColumnType: ColumnTypeResolver,
  registeredLanguages: readonly string[],
): string {
  const core = generateCodegenArtifact(bundle, 'rust', registeredLanguages, resolveColumnType).module.code;
  const adapter = generateRustStaticAdapter(bundle, moduleName, resolveColumnType, true);
  const childModules: string[] = [];
  const walk = (op: RelationOp, path: string[]): void => {
    const names = relChildNames(moduleName, path);
    const childBundle = compileRelationChildBundle(op, names.component, resolveColumnType);
    const childCore = generateCodegenArtifact(childBundle, 'rust', registeredLanguages, resolveColumnType).module.code;
    const childAdapter = generateRustStaticAdapter(childBundle, names.module, resolveColumnType, true);
    childModules.push(`pub mod ${names.module} {\n${childCore}\n${childAdapter}\n}`);
    for (const child of op.childRelations ?? []) walk(child, [...path, child.name]);
  };
  for (const op of Object.values(bundle.relations)) walk(op, [op.name]);
  return [core, adapter, ...childModules].join('\n\n');
}
