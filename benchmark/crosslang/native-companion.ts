// ════════════════════════════════════════════════════════════════════════════
// NATIVE codegen companion (epic #44 perf integration) — pre-decode the codegen
// EXECUTION data OUT of the JSON artifact at GENERATION time.
// ════════════════════════════════════════════════════════════════════════════
//
// OWNER ORDERS (absolute):
//   - The codegen path carries NO IR data (not in files, not in binaries, never
//     read at exec time) and NO dynamic-JSON walking at execution time
//     (no serde_json in the Rust codegen binary, no encoding/json touching the
//     Go codegen cell's execution data).
//   - Therefore the codegen cells consume a GENERATED NATIVE COMPANION: each
//     case's statement plan / transaction plan / relation batch op / bench input
//     is pre-decoded HERE (generation time) into a CLOSED-SET neutral model and
//     emitted as native Rust/Go source (const/struct data). The decode is
//     FAIL-CLOSED: any shape outside the closed set that actually occurs in the
//     bench corpus THROWS at generation — never a silent degrade.
//
// The closed set was enumerated from the REAL generated/bundles.json corpus:
//   param specs:  {ref:[..path]} · {__jsonArray:{ref:[..]},dialect} · string · int
//                 · {arr:[..scalar literals]} (PG UNNEST per-column batch arrays)
//   skip exprs:   {not:[{ne:[{refOpt:[head]},null]}]}   (SKIP-if-null only)
//   read graphs:  single component, single componentRef body node (the primary),
//                 output {ref:[primary]}, statements only for the primary
//   relations:    single-key hasMany/belongsTo/hasOne (no composite, no cross-DB)
//   tx plans:     ordered statements {id, role, gate?, binds?, op:{sql,params}}
//   inputs:       null/bool/int/str + int[]/str[] scalars (batch input is empty —
//                 the createMany rows are baked into the tx plan param)

// ── Neutral (language-independent) closed-set model ───────────────────────────

export type NScalar =
  | { t: 'null' }
  | { t: 'bool'; v: boolean }
  | { t: 'int'; v: number }
  | { t: 'str'; v: string };

export type NSpec =
  | { k: 'ref'; path: string[] }
  | { k: 'jsonArr'; path: string[]; dialect: string }
  | { k: 'str'; v: string }
  | { k: 'int'; v: number }
  /** bc literal-array expression `{arr:[..scalars]}` (the PG UNNEST per-column batch param). */
  | { k: 'arrLit'; v: NScalar[] };

export interface NStmt {
  sql: string;
  whereFragment: boolean;
  /** SKIP-if-null head (the only skip shape in the corpus), or undefined. */
  skipIfNull?: string;
  params: NSpec[];
}

export interface NReadPlan {
  dialect: string;
  stmts: NStmt[];
}

export interface NRelation {
  name: string;
  kind: 'hasMany' | 'belongsTo' | 'hasOne';
  parentKey: string;
  targetKey: string;
  dialect: string;
  sql: string;
}

export type NGate = 'existsElseRollback' | 'insertedElseRollback' | 'insertedElseNoop';

export interface NTxStmt {
  id: string;
  gate?: NGate;
  binds?: string;
  sql: string;
  params: NSpec[];
  /** hasReturn(sql) — SELECT head or a RETURNING token (baked; the sql is static). */
  isReturn: boolean;
}

export interface NTxPlan {
  entityFrom?: string;
  isBatch: boolean;
  statements: NTxStmt[];
}

export type NVal =
  | { t: 'null' }
  | { t: 'bool'; v: boolean }
  | { t: 'int'; v: number }
  | { t: 'str'; v: string }
  | { t: 'intArr'; v: number[] }
  | { t: 'strArr'; v: string[] };

export interface NCase {
  caseId: string;
  kind: 'read' | 'relation' | 'batch' | 'tx';
  /** The generated module entry (COMPONENT_NAMES[0]): the read entry or the write bundle name. */
  entry: string;
  read?: NReadPlan;
  relation?: NRelation;
  tx?: NTxPlan;
  /** WRITE bundles: the base statement sql (the `__sql` surrogate module input). */
  writeSql?: string;
  input: [string, NVal][];
}

// ── Fail-closed decode (generation time) ──────────────────────────────────────

function fail(caseId: string, what: string, got: unknown): never {
  throw new Error(
    `native-companion FAIL-CLOSED [${caseId}]: ${what} is outside the closed native set: ${JSON.stringify(got)}`,
  );
}

function decodeSpec(caseId: string, spec: unknown): NSpec {
  if (typeof spec === 'string') return { k: 'str', v: spec };
  if (typeof spec === 'number') {
    if (!Number.isInteger(spec)) fail(caseId, 'non-integer numeric param spec', spec);
    return { k: 'int', v: spec };
  }
  if (spec !== null && typeof spec === 'object' && !Array.isArray(spec)) {
    const o = spec as Record<string, unknown>;
    const keys = Object.keys(o);
    if (keys.length === 1 && keys[0] === 'ref') {
      const path = o.ref;
      if (Array.isArray(path) && path.length >= 1 && path.every((p) => typeof p === 'string')) {
        return { k: 'ref', path: path as string[] };
      }
      fail(caseId, 'ref param spec path', spec);
    }
    if (keys.length === 1 && keys[0] === 'arr') {
      const arr = o.arr;
      if (Array.isArray(arr)) {
        const v: NScalar[] = arr.map((e) => {
          if (e === null) return { t: 'null' } as NScalar;
          if (typeof e === 'boolean') return { t: 'bool', v: e } as NScalar;
          if (typeof e === 'number' && Number.isInteger(e)) return { t: 'int', v: e } as NScalar;
          if (typeof e === 'string') return { t: 'str', v: e } as NScalar;
          fail(caseId, 'literal array element', e);
        });
        return { k: 'arrLit', v };
      }
      fail(caseId, 'literal array param spec', spec);
    }
    if (keys.length === 2 && keys.includes('__jsonArray') && keys.includes('dialect')) {
      const inner = o.__jsonArray as Record<string, unknown>;
      const dialect = o.dialect;
      if (
        inner !== null && typeof inner === 'object' && !Array.isArray(inner) &&
        Object.keys(inner).length === 1 && Array.isArray(inner.ref) &&
        (inner.ref as unknown[]).every((p) => typeof p === 'string') &&
        typeof dialect === 'string'
      ) {
        return { k: 'jsonArr', path: inner.ref as string[], dialect };
      }
      fail(caseId, '__jsonArray param spec', spec);
    }
  }
  fail(caseId, 'param spec', spec);
}

/** The ONLY skip shape in the corpus: `{not:[{ne:[{refOpt:[head]},null]}]}` → SKIP-if-null(head). */
function decodeSkip(caseId: string, skip: unknown): string {
  const o = skip as Record<string, unknown>;
  const not = o !== null && typeof o === 'object' ? (o.not as unknown[]) : undefined;
  if (Array.isArray(not) && not.length === 1 && Object.keys(o).length === 1) {
    const ne = (not[0] as Record<string, unknown>)?.ne as unknown[];
    if (Array.isArray(ne) && ne.length === 2 && ne[1] === null) {
      const refOpt = (ne[0] as Record<string, unknown>)?.refOpt as unknown[];
      if (Array.isArray(refOpt) && refOpt.length === 1 && typeof refOpt[0] === 'string') {
        return refOpt[0];
      }
    }
  }
  fail(caseId, 'skip expression', skip);
}

/** Mirror of the runtime `hasReturn` (rust `is_return_stmt`): SELECT head token or a RETURNING token. */
function isReturnStmt(sql: string): boolean {
  const tokens = (s: string): string[] => s.toLowerCase().split(/[^a-z0-9_]+/);
  if (tokens(sql.slice(0, 8)).includes('select')) return true;
  return tokens(sql).includes('returning');
}

function countPlaceholders(sql: string): number {
  return sql.split('?').length - 1;
}

function decodeStmt(caseId: string, stmt: Record<string, unknown>): NStmt {
  const allowed = new Set(['sql', 'params', 'skip', 'whereFragment']);
  for (const k of Object.keys(stmt)) if (!allowed.has(k)) fail(caseId, `statement key '${k}'`, stmt);
  const sql = stmt.sql;
  if (typeof sql !== 'string') fail(caseId, 'statement sql', stmt);
  const params = ((stmt.params as unknown[]) ?? []).map((s) => decodeSpec(caseId, s));
  if (countPlaceholders(sql) !== params.length) {
    fail(caseId, `statement placeholder/param arity (${countPlaceholders(sql)} '?' vs ${params.length})`, sql);
  }
  const whereFragment = stmt.whereFragment === true;
  const out: NStmt = { sql, whereFragment, params };
  if (stmt.skip !== undefined && stmt.skip !== null) out.skipIfNull = decodeSkip(caseId, stmt.skip);
  return out;
}

function decodeReadPlan(caseId: string, bundle: any): NReadPlan {
  const rg = bundle.readGraph;
  const ir = rg.ir;
  if (!Array.isArray(ir?.components) || ir.components.length !== 1) {
    fail(caseId, 'read graph component count', ir?.components?.length);
  }
  const comp = ir.components[0];
  const body: any[] = comp.body ?? [];
  if (body.length !== 1) fail(caseId, 'read graph body node count', body.length);
  const node = body[0];
  if (node.component === undefined || 'map' in node || 'cond' in node) {
    fail(caseId, 'read graph node shape (map/cond/non-componentRef)', node);
  }
  const primaryId = node.id;
  const outRef = comp.output?.ref;
  if (!Array.isArray(outRef) || outRef.length !== 1 || outRef[0] !== primaryId) {
    fail(caseId, 'read graph output shape', comp.output);
  }
  const byId = rg.statementsById ?? {};
  const ids = Object.keys(byId);
  if (ids.length !== 1 || ids[0] !== primaryId) fail(caseId, 'statementsById keys', ids);
  // Every declared input port must be REQUIRED (a non-required port would need present-as-null
  // normalization that the native runtime intentionally does not carry — absent==null holds only
  // for the SKIP-if-null head, which is covered natively).
  for (const [port, schema] of Object.entries((comp.inputPorts ?? {}) as Record<string, any>)) {
    if (schema?.required !== true) fail(caseId, `non-required input port '${port}'`, schema);
  }
  const stmts = (byId[primaryId] as Record<string, unknown>[]).map((s) => decodeStmt(caseId, s));
  return { dialect: bundle.dialect, stmts };
}

function decodeRelation(caseId: string, op: any): NRelation {
  for (const k of ['parentKeys', 'targetKeys', 'connection']) {
    if (op[k] !== undefined) fail(caseId, `relation '${k}' (composite/cross-DB)`, op);
  }
  const { name, kind, parentKey, targetKey, dialect, sql } = op;
  if (
    typeof name !== 'string' || typeof parentKey !== 'string' || typeof targetKey !== 'string' ||
    typeof dialect !== 'string' || typeof sql !== 'string'
  ) fail(caseId, 'relation op fields', op);
  if (kind !== 'hasMany' && kind !== 'belongsTo' && kind !== 'hasOne') fail(caseId, 'relation kind', kind);
  if (countPlaceholders(sql) !== 1) fail(caseId, 'relation sql placeholder count', sql);
  return { name, kind, parentKey, targetKey, dialect, sql };
}

const GATES: readonly NGate[] = ['existsElseRollback', 'insertedElseRollback', 'insertedElseNoop'];

function decodeTxPlan(caseId: string, plan: any): NTxPlan {
  const statements: NTxStmt[] = (plan.statements as any[]).map((s) => {
    const op = s.op;
    if (op === undefined || typeof op.sql !== 'string') fail(caseId, 'tx statement op', s);
    if (op.skip !== undefined) fail(caseId, 'tx statement op.skip', s);
    const params = ((op.params as unknown[]) ?? []).map((p) => decodeSpec(caseId, p));
    if (countPlaceholders(op.sql) !== params.length) {
      fail(caseId, `tx placeholder/param arity (${countPlaceholders(op.sql)} '?' vs ${params.length})`, op.sql);
    }
    if (typeof s.id !== 'string' || typeof s.role !== 'string') fail(caseId, 'tx statement id/role', s);
    const out: NTxStmt = { id: s.id, sql: op.sql, params, isReturn: isReturnStmt(op.sql) };
    if (s.gate !== undefined && s.gate !== null) {
      if (!GATES.includes(s.gate)) fail(caseId, 'tx gate rule', s.gate);
      out.gate = s.gate;
    }
    if (s.binds !== undefined && s.binds !== null) {
      if (typeof s.binds !== 'string') fail(caseId, 'tx binds', s.binds);
      out.binds = s.binds;
    }
    return out;
  });
  const entityFrom = plan.entityFrom ?? undefined;
  if (entityFrom !== undefined && typeof entityFrom !== 'string') fail(caseId, 'tx entityFrom', plan.entityFrom);
  // Batch mode derivation — byte-mirror of the runtime: gate-free, binds-free, all-body, no entity.
  const src: any[] = plan.statements as any[];
  const isBatch = entityFrom === undefined &&
    src.every((s) => (s.gate ?? null) === null && (s.binds ?? null) === null && s.role === 'body');
  return { entityFrom, isBatch, statements };
}

function decodeInput(caseId: string, kind: string, input: any): [string, NVal][] {
  if (kind === 'batch') return []; // createMany rows are baked into the tx plan param
  const out: [string, NVal][] = [];
  for (const [k, v] of Object.entries(input ?? {})) {
    if (v === null) out.push([k, { t: 'null' }]);
    else if (typeof v === 'boolean') out.push([k, { t: 'bool', v }]);
    else if (typeof v === 'number' && Number.isInteger(v)) out.push([k, { t: 'int', v }]);
    else if (typeof v === 'string') out.push([k, { t: 'str', v }]);
    else if (Array.isArray(v) && v.every((e) => typeof e === 'number' && Number.isInteger(e))) {
      out.push([k, { t: 'intArr', v: v as number[] }]);
    } else if (Array.isArray(v) && v.every((e) => typeof e === 'string')) {
      out.push([k, { t: 'strArr', v: v as string[] }]);
    } else fail(caseId, `input value '${k}'`, v);
  }
  return out;
}

/** Decode ONE bench case artifact (case/kind/entry/bundle/input) into the closed native model. */
export function decodeNativeCase(c: {
  case: string; kind: string; entry?: string; withRelation?: string; bundle: any; input: any;
}): NCase {
  const kind = c.kind as NCase['kind'];
  if (!['read', 'relation', 'batch', 'tx'].includes(kind)) fail(c.case, 'case kind', c.kind);
  const out: NCase = {
    caseId: c.case,
    kind,
    entry: kind === 'read' || kind === 'relation' ? c.entry! : (c.bundle.name as string),
    input: decodeInput(c.case, kind, c.input),
  };
  if (typeof out.entry !== 'string' || out.entry.length === 0) fail(c.case, 'entry', c.entry);
  if (kind === 'read' || kind === 'relation') {
    out.read = decodeReadPlan(c.case, c.bundle);
    if (kind === 'relation') {
      const op = c.bundle.relations?.[c.withRelation!];
      if (op === undefined) fail(c.case, `relation '${c.withRelation}'`, Object.keys(c.bundle.relations ?? {}));
      out.relation = decodeRelation(c.case, op);
    }
  } else {
    out.tx = decodeTxPlan(c.case, c.bundle.transaction);
    const writeSql = c.bundle.statement?.sql;
    if (typeof writeSql !== 'string') fail(c.case, 'write bundle statement.sql', c.bundle.statement);
    out.writeSql = writeSql;
  }
  return out;
}

// ── Source emit helpers ────────────────────────────────────────────────────────

/** A string literal valid in BOTH Rust and Go source (JSON escaping; ASCII-printable only). */
function lit(s: string): string {
  // \n / \r / \t are fine — JSON.stringify escapes them into forms valid in Rust AND Go.
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\uffff]/.test(s)) {
    throw new Error(`native-companion FAIL-CLOSED: non-ASCII-printable char in string literal: ${JSON.stringify(s)}`);
  }
  return JSON.stringify(s);
}

const DIALECT_RS: Record<string, string> = { sqlite: 'Dialect::Sqlite', postgres: 'Dialect::Postgres', mysql: 'Dialect::Mysql' };
const DIALECT_GO: Record<string, string> = { sqlite: 'Sqlite', postgres: 'Postgres', mysql: 'Mysql' };
const GATE_RS: Record<NGate, string> = {
  existsElseRollback: 'Gate::ExistsElseRollback',
  insertedElseRollback: 'Gate::InsertedElseRollback',
  insertedElseNoop: 'Gate::InsertedElseNoop',
};
const GATE_GO: Record<NGate, string> = {
  existsElseRollback: 'GateExistsElseRollback',
  insertedElseRollback: 'GateInsertedElseRollback',
  insertedElseNoop: 'GateInsertedElseNoop',
};

function dialectRs(d: string): string {
  const out = DIALECT_RS[d];
  if (out === undefined) throw new Error(`native-companion FAIL-CLOSED: unknown dialect '${d}'`);
  return out;
}
function dialectGo(d: string): string {
  const out = DIALECT_GO[d];
  if (out === undefined) throw new Error(`native-companion FAIL-CLOSED: unknown dialect '${d}'`);
  return out;
}

// ── Rust companion emitter ─────────────────────────────────────────────────────

function scalarRs(e: NScalar): string {
  switch (e.t) {
    case 'null': return 'Lit::Null';
    case 'bool': return `Lit::Bool(${e.v})`;
    case 'int': return `Lit::Int(${e.v})`;
    case 'str': return `Lit::Str(${lit(e.v)})`;
  }
}

function specRs(s: NSpec): string {
  switch (s.k) {
    case 'ref': return `Spec::Ref(&[${s.path.map(lit).join(', ')}])`;
    case 'jsonArr': return `Spec::JsonArray { head: &[${s.path.map(lit).join(', ')}], dialect: ${dialectRs(s.dialect)} }`;
    case 'str': return `Spec::Str(${lit(s.v)})`;
    case 'int': return `Spec::Int(${s.v})`;
    case 'arrLit': return `Spec::ArrLit(&[${s.v.map(scalarRs).join(', ')}])`;
  }
}

function nvalRs(v: NVal): string {
  switch (v.t) {
    case 'null': return 'InVal::Null';
    case 'bool': return `InVal::Bool(${v.v})`;
    case 'int': return `InVal::Int(${v.v})`;
    case 'str': return `InVal::Str(${lit(v.v)})`;
    case 'intArr': return `InVal::IntArr(&[${v.v.join(', ')}])`;
    case 'strArr': return `InVal::StrArr(&[${v.v.map(lit).join(', ')}])`;
  }
}

function stmtRs(s: NStmt): string {
  const skip = s.skipIfNull === undefined ? 'None' : `Some(Skip::IfNull(${lit(s.skipIfNull)}))`;
  return `Stmt { sql: ${lit(s.sql)}, where_fragment: ${s.whereFragment}, skip: ${skip}, params: &[${s.params.map(specRs).join(', ')}] }`;
}

function caseRs(c: NCase, dialect: string): string {
  const kind = { read: 'CaseKind::Read', relation: 'CaseKind::Relation', batch: 'CaseKind::Batch', tx: 'CaseKind::Tx' }[c.kind];
  const read = c.read === undefined
    ? 'None'
    : `Some(&ReadPlan { dialect: ${dialectRs(c.read.dialect)}, stmts: &[\n        ${c.read.stmts.map(stmtRs).join(',\n        ')},\n    ] })`;
  const rel = c.relation === undefined
    ? 'None'
    : `Some(&Relation { name: ${lit(c.relation.name)}, kind: RelKind::${c.relation.kind === 'hasMany' ? 'HasMany' : c.relation.kind === 'belongsTo' ? 'BelongsTo' : 'HasOne'}, parent_key: ${lit(c.relation.parentKey)}, target_key: ${lit(c.relation.targetKey)}, dialect: ${dialectRs(c.relation.dialect)}, sql: ${lit(c.relation.sql)} })`;
  const tx = c.tx === undefined
    ? 'None'
    : `Some(&TxPlan { entity_from: ${c.tx.entityFrom === undefined ? 'None' : `Some(${lit(c.tx.entityFrom)})`}, is_batch: ${c.tx.isBatch}, statements: &[\n        ${c.tx.statements
        .map((s) => `TxStmt { id: ${lit(s.id)}, gate: ${s.gate === undefined ? 'None' : `Some(${GATE_RS[s.gate]})`}, binds: ${s.binds === undefined ? 'None' : `Some(${lit(s.binds)})`}, sql: ${lit(s.sql)}, params: &[${s.params.map(specRs).join(', ')}], is_return: ${s.isReturn} }`)
        .join(',\n        ')},\n    ] })`;
  const writeSql = c.writeSql === undefined ? 'None' : `Some(${lit(c.writeSql)})`;
  const input = `&[${c.input.map(([k, v]) => `(${lit(k)}, ${nvalRs(v)})`).join(', ')}]`;
  return `CasePlan {
    case_id: ${lit(c.caseId)},
    kind: ${kind},
    entry: ${lit(c.entry)},
    dialect: ${dialectRs(dialect)},
    read: ${read},
    relation: ${rel},
    tx: ${tx},
    write_sql: ${writeSql},
    input: ${input},
}`;
}

/**
 * The self-contained Rust companion module: closed-set plan types + the pre-decoded static data
 * for every dialect × case, PLUS the schema/seed DDL (so the codegen binary never opens the JSON
 * artifact). Contains NO IR data and NO fingerprints — statement/transaction/relation text and
 * value-spec bindings only.
 */
export function rustCompanionSource(
  byDialect: Record<string, NCase[]>,
  schema: readonly string[],
  seed: readonly string[],
): string {
  const dialects = Object.keys(byDialect);
  const caseConsts: string[] = [];
  const lookupArms: string[] = [];
  for (const d of dialects) {
    for (const c of byDialect[d]) {
      const constName = `CASE_${d.toUpperCase()}_${c.caseId.replace(/[^A-Za-z0-9]/g, '_').toUpperCase()}`;
      caseConsts.push(`pub static ${constName}: CasePlan = ${caseRs(c, d)};`);
      lookupArms.push(`        (${lit(d)}, ${lit(c.caseId)}) => Some(&${constName}),`);
    }
  }
  return `// GENERATED by benchmark/crosslang/generate.ts (native-companion.ts) — DO NOT EDIT.
//
// The NATIVE pre-decoded codegen execution data (owner order: the codegen binary carries NO IR
// data and parses NO JSON at execution time). Every statement plan / transaction plan / relation
// batch op / bench input below was decoded FROM the generated bundles at GENERATION time through
// a CLOSED-SET fail-closed decoder (native-companion.ts) — an out-of-set shape THROWS there.
// This file is pure data + closed enums: no serde, no JSON, no IR.
#![allow(dead_code)]

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Dialect { Sqlite, Postgres, Mysql }

impl Dialect {
    pub fn name(self) -> &'static str {
        match self { Dialect::Sqlite => "sqlite", Dialect::Postgres => "postgres", Dialect::Mysql => "mysql" }
    }
}

/// A scalar literal (a literal-array element).
pub enum Lit { Null, Bool(bool), Int(i64), Str(&'static str) }

/// A deferred value-spec (closed set — enumerated from the real bench corpus).
pub enum Spec {
    /// Scope ref by path (bc \`{ref:[..]}\`).
    Ref(&'static [&'static str]),
    /// IN-list marker (bc \`{__jsonArray:{ref:[..]},dialect}\`): postgres binds the array as-is;
    /// mysql/sqlite JSON-encode it to ONE string param (mysql bool → 1/0).
    JsonArray { head: &'static [&'static str], dialect: Dialect },
    Str(&'static str),
    Int(i64),
    /// bc literal-array expression \`{arr:[..]}\` (the PG UNNEST per-column batch param).
    ArrLit(&'static [Lit]),
}

/// The closed skip set: drop the statement when the head is null/absent.
pub enum Skip { IfNull(&'static str) }

pub struct Stmt {
    pub sql: &'static str,
    pub where_fragment: bool,
    pub skip: Option<Skip>,
    pub params: &'static [Spec],
}

pub struct ReadPlan {
    pub dialect: Dialect,
    pub stmts: &'static [Stmt],
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RelKind { HasMany, BelongsTo, HasOne }

pub struct Relation {
    pub name: &'static str,
    pub kind: RelKind,
    pub parent_key: &'static str,
    pub target_key: &'static str,
    pub dialect: Dialect,
    pub sql: &'static str,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Gate { ExistsElseRollback, InsertedElseRollback, InsertedElseNoop }

pub struct TxStmt {
    pub id: &'static str,
    pub gate: Option<Gate>,
    pub binds: Option<&'static str>,
    pub sql: &'static str,
    pub params: &'static [Spec],
    pub is_return: bool,
}

pub struct TxPlan {
    pub entity_from: Option<&'static str>,
    pub is_batch: bool,
    pub statements: &'static [TxStmt],
}

pub enum InVal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(&'static str),
    IntArr(&'static [i64]),
    StrArr(&'static [&'static str]),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CaseKind { Read, Relation, Batch, Tx }

pub struct CasePlan {
    pub case_id: &'static str,
    pub kind: CaseKind,
    /// The generated module entry (COMPONENT_NAMES[0]).
    pub entry: &'static str,
    pub dialect: Dialect,
    pub read: Option<&'static ReadPlan>,
    pub relation: Option<&'static Relation>,
    pub tx: Option<&'static TxPlan>,
    /// WRITE bundles: the base statement sql (the \`__sql\` surrogate module input).
    pub write_sql: Option<&'static str>,
    pub input: &'static [(&'static str, InVal)],
}

${caseConsts.join('\n\n')}

pub static CASE_IDS: &[&str] = &[${byDialect[dialects[0]].map((c) => lit(c.caseId)).join(', ')}];
pub static DIALECTS: &[&str] = &[${dialects.map(lit).join(', ')}];

pub fn case_plan(dialect: &str, case_id: &str) -> Option<&'static CasePlan> {
    match (dialect, case_id) {
${lookupArms.join('\n')}
        _ => None,
    }
}

// ── schema + seed (so the codegen binary never opens the JSON artifact) ────────
pub static SCHEMA: &[&str] = &[
${schema.map((s) => `    ${lit(s)},`).join('\n')}
];

pub static SEED: &[&str] = &[
${seed.map((s) => `    ${lit(s)},`).join('\n')}
];
`;
}

// ── Go companion emitter ───────────────────────────────────────────────────────

function scalarGo(e: NScalar): string {
  switch (e.t) {
    case 'null': return 'nil';
    case 'bool': return `${e.v}`;
    case 'int': return `int64(${e.v})`;
    case 'str': return lit(e.v);
  }
}

function specGo(s: NSpec): string {
  switch (s.k) {
    case 'ref': return `{Kind: SpecRef, Path: []string{${s.path.map(lit).join(', ')}}}`;
    case 'jsonArr': return `{Kind: SpecJSONArray, Path: []string{${s.path.map(lit).join(', ')}}, ArrDialect: ${dialectGo(s.dialect)}}`;
    case 'str': return `{Kind: SpecStr, Str: ${lit(s.v)}}`;
    case 'int': return `{Kind: SpecInt, Int: ${s.v}}`;
    case 'arrLit': return `{Kind: SpecArrLit, Arr: []any{${s.v.map(scalarGo).join(', ')}}}`;
  }
}

function nvalGo(v: NVal): string {
  switch (v.t) {
    case 'null': return 'nil';
    case 'bool': return `${v.v}`;
    case 'int': return `int64(${v.v})`;
    case 'str': return lit(v.v);
    case 'intArr': return `[]int64{${v.v.join(', ')}}`;
    case 'strArr': return `[]string{${v.v.map(lit).join(', ')}}`;
  }
}

function stmtGo(s: NStmt): string {
  const skip = s.skipIfNull === undefined ? '' : `, HasSkip: true, SkipIfNullHead: ${lit(s.skipIfNull)}`;
  return `{SQL: ${lit(s.sql)}, WhereFragment: ${s.whereFragment}${skip}, Params: []Spec{${s.params.map(specGo).join(', ')}}}`;
}

function caseGo(c: NCase, dialect: string): string {
  const read = c.read === undefined
    ? 'nil'
    : `&ReadPlan{Dialect: ${dialectGo(c.read.dialect)}, Stmts: []Stmt{\n\t\t\t${c.read.stmts.map(stmtGo).join(',\n\t\t\t')},\n\t\t}}`;
  const rel = c.relation === undefined
    ? 'nil'
    : `&Relation{Name: ${lit(c.relation.name)}, Kind: ${lit(c.relation.kind)}, ParentKey: ${lit(c.relation.parentKey)}, TargetKey: ${lit(c.relation.targetKey)}, Dialect: ${dialectGo(c.relation.dialect)}, SQL: ${lit(c.relation.sql)}}`;
  const tx = c.tx === undefined
    ? 'nil'
    : `&TxPlan{EntityFrom: ${lit(c.tx.entityFrom ?? '')}, IsBatch: ${c.tx.isBatch}, Statements: []TxStmt{\n\t\t\t${c.tx.statements
        .map((s) => `{ID: ${lit(s.id)}, Gate: ${s.gate === undefined ? 'GateNone' : GATE_GO[s.gate]}, Binds: ${lit(s.binds ?? '')}, SQL: ${lit(s.sql)}, Params: []Spec{${s.params.map(specGo).join(', ')}}, IsReturn: ${s.isReturn}}`)
        .join(',\n\t\t\t')},\n\t\t}}`;
  const input = `[]KV{${c.input.map(([k, v]) => `{K: ${lit(k)}, V: ${nvalGo(v)}}`).join(', ')}}`;
  return `{
\t\tCase: ${lit(c.caseId)}, Kind: ${lit(c.kind)}, Entry: ${lit(c.entry)}, Dialect: ${dialectGo(dialect)},
\t\tRead: ${read},
\t\tRel: ${rel},
\t\tTx: ${tx},
\t\tWriteSQL: ${lit(c.writeSql ?? '')},
\t\tInput: ${input},
\t}`;
}

/**
 * The self-contained Go companion package (\`go/lm_bench/cgplans\`): closed-set plan types + the
 * pre-decoded data for every dialect × case. Dependency-free (imports NOTHING — in particular no
 * encoding/json and no litedbmodel_runtime), no IR data, no fingerprints.
 */
export function goCompanionSource(byDialect: Record<string, NCase[]>): string {
  const dialects = Object.keys(byDialect);
  const entries: string[] = [];
  for (const d of dialects) {
    const cases = byDialect[d]
      .map((c) => `\t\t${lit(c.caseId)}: ${caseGo(c, d)},`)
      .join('\n');
    entries.push(`\t${lit(d)}: {\n${cases}\n\t},`);
  }
  return `// Code generated by benchmark/crosslang/generate.ts (native-companion.ts). DO NOT EDIT.
//
// The NATIVE pre-decoded codegen execution data (owner order: the codegen cell touches NO IR data
// and NO encoding/json at execution time). Decoded from the generated bundles at GENERATION time
// through a CLOSED-SET fail-closed decoder (native-companion.ts) — an out-of-set shape THROWS
// there. Pure data + closed enums: imports nothing.
package cgplans

type Dialect int

const (
\tSqlite Dialect = iota
\tPostgres
\tMysql
)

func (d Dialect) Name() string {
\tswitch d {
\tcase Postgres:
\t\treturn "postgres"
\tcase Mysql:
\t\treturn "mysql"
\t}
\treturn "sqlite"
}

type SpecKind int

const (
\tSpecRef SpecKind = iota
\tSpecJSONArray
\tSpecStr
\tSpecInt
\tSpecArrLit
)

// Spec is a deferred value-spec (closed set enumerated from the real bench corpus).
type Spec struct {
\tKind       SpecKind
\tPath       []string
\tStr        string
\tInt        int64
\tArrDialect Dialect
\t// Arr holds a literal-array param's NATIVE elements (int64 / string / bool / nil).
\tArr []any
}

type Stmt struct {
\tSQL            string
\tWhereFragment  bool
\tHasSkip        bool
\tSkipIfNullHead string
\tParams         []Spec
}

type ReadPlan struct {
\tDialect Dialect
\tStmts   []Stmt
}

type Relation struct {
\tName      string
\tKind      string // hasMany | belongsTo | hasOne
\tParentKey string
\tTargetKey string
\tDialect   Dialect
\tSQL       string
}

type Gate int

const (
\tGateNone Gate = iota
\tGateExistsElseRollback
\tGateInsertedElseRollback
\tGateInsertedElseNoop
)

type TxStmt struct {
\tID       string
\tGate     Gate
\tBinds    string // "" = none
\tSQL      string
\tParams   []Spec
\tIsReturn bool
}

type TxPlan struct {
\tEntityFrom string // "" = none
\tIsBatch    bool
\tStatements []TxStmt
}

// KV is one bench-input binding. V is a NATIVE Go value (int64 / string / bool / nil /
// []int64 / []string) — decoded at generation time, never JSON.
type KV struct {
\tK string
\tV any
}

type CasePlan struct {
\tCase     string
\tKind     string // read | relation | batch | tx
\tEntry    string // generated module entry (ComponentNames[0])
\tDialect  Dialect
\tRead     *ReadPlan
\tRel      *Relation
\tTx       *TxPlan
\tWriteSQL string // write bundles: the __sql surrogate module input
\tInput    []KV
}

// Plans maps dialect -> case id -> the pre-decoded native plan.
var Plans = map[string]map[string]*CasePlan{
${entries.join('\n')}
}

// CaseIDs lists the bench case ids in contract order.
var CaseIDs = []string{${byDialect[dialects[0]].map((c) => lit(c.caseId)).join(', ')}}
`;
}
