// ════════════════════════════════════════════════════════════════════════════
// Cross-language benchmark CASE/DIALECT CONSTANTS — the axis SSoT
// ════════════════════════════════════════════════════════════════════════════
//
// SINGLE source of truth for the axes every language runs: the 19 ORM-comparison
// ops (== the v1 SQL golden == v2 SCP parity == benchmark.ts litedbmodel
// column) × the three real dialects. Each language executes the SAME ops driver-
// included on all three real DBs (SQLite in-proc, MySQL :3307, PostgreSQL :5433).
//
// There is NO wire protocol. Each language is a STANDALONE process
// (adapters/<lang>) that runs all 19 ops × 3 dialects, self-measures, and writes a
// flat CSV to benchmark/crosslang/.results/<lang>.csv. The collector (collect.ts)
// globs those CSVs and renders CROSS-LANG.md.
//
// This file is pure constants — the op/dialect axes only. It carries NO SQL and NO
// artifact: the per-op, per-dialect SQL is emitted by BC codegen as native literals
// (see benchmark/crosslang/REBUILD.md, epic #107). It must never import a baked-SQL
// plan artifact.

// ── Dialects (the three real targets) ─────────────────────────────────────────
export const ORM_DIALECTS = ['sqlite', 'mysql', 'postgres'] as const;
export type OrmDialect = (typeof ORM_DIALECTS)[number];

// ── The 19 ORM ops (== benchmark.ts testCategories) ─
// Order + labels mirror benchmark/benchmark.ts exactly. `write` marks the ops whose
// logical op mutates (they run inside a transaction: BEGIN … COMMIT).
export interface OrmOpMeta {
  readonly id: string; // stable slug (protocol id)
  readonly label: string; // the human label (== benchmark.ts / golden op key)
  readonly write: boolean;
}

export const ORM_OPS: readonly OrmOpMeta[] = [
  { id: 'findAll', label: 'Find all (limit 100)', write: false },
  { id: 'filterPaginateSort', label: 'Filter, paginate & sort', write: false },
  { id: 'nestedFindAll', label: 'Nested find all (include posts)', write: false },
  { id: 'findFirst', label: 'Find first', write: false },
  { id: 'nestedFindFirst', label: 'Nested find first (include posts)', write: false },
  { id: 'findUnique', label: 'Find unique (by email)', write: false },
  { id: 'nestedFindUnique', label: 'Nested find unique (include posts)', write: false },
  { id: 'create', label: 'Create', write: true },
  { id: 'nestedCreate', label: 'Nested create (with post)', write: true },
  { id: 'update', label: 'Update', write: true },
  { id: 'nestedUpdate', label: 'Nested update (update user + post)', write: true },
  { id: 'upsert', label: 'Upsert', write: true },
  { id: 'nestedUpsert', label: 'Nested upsert (user + post)', write: true },
  { id: 'delete', label: 'Delete', write: true },
  { id: 'createMany', label: 'Create Many (10 records)', write: true },
  { id: 'upsertMany', label: 'Upsert Many (10 records)', write: true },
  { id: 'updateMany', label: 'Update Many (10 different values)', write: true },
  { id: 'nestedRelations', label: 'Nested relations (100->1000->10000)', write: false },
  { id: 'compositeRelations', label: 'Nested relations (composite key, 5 tenants)', write: false },
] as const;

export const ORM_OP_IDS: readonly string[] = ORM_OPS.map((o) => o.id);
export const ORM_WRITE_OP_IDS: ReadonlySet<string> = new Set(ORM_OPS.filter((o) => o.write).map((o) => o.id));
export const ORM_OP_LABEL: Record<string, string> = Object.fromEntries(ORM_OPS.map((o) => [o.id, o.label]));

// ── The op axis — the 19 ORM-comparison ops (no subset) ──────────────────────
export const CROSSLANG_CASE_IDS = ORM_OP_IDS;
export type CrosslangCaseId = string;
export const CROSSLANG_CASE_LABELS: Record<string, string> = ORM_OP_LABEL;

// Ops whose logical op is a WRITE (the report tags these `W`, reads `R`).
export const CROSSLANG_WRITE_CASES: ReadonlySet<string> = ORM_WRITE_OP_IDS;

// ── The dialect axis (the three real targets) ────────────────────────────────
export const CROSSLANG_DIALECTS = ORM_DIALECTS;
export type CrosslangDialect = OrmDialect;
