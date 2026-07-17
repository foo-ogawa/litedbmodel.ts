// ════════════════════════════════════════════════════════════════════════════
// Cross-language benchmark CASE/DIALECT CONSTANTS
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

import { ORM_OP_IDS, ORM_OP_LABEL, ORM_WRITE_OP_IDS, ORM_DIALECTS, type OrmDialect } from './orm-plan.js';

// ── The op axis — the 19 ORM-comparison ops (no subset) ──────────────────────
export const CROSSLANG_CASE_IDS = ORM_OP_IDS;
export type CrosslangCaseId = string;
export const CROSSLANG_CASE_LABELS: Record<string, string> = ORM_OP_LABEL;

// Ops whose logical op is a WRITE (the report tags these `W`, reads `R`).
export const CROSSLANG_WRITE_CASES: ReadonlySet<string> = ORM_WRITE_OP_IDS;

// ── The dialect axis (the three real targets) ────────────────────────────────
export const CROSSLANG_DIALECTS = ORM_DIALECTS;
export type CrosslangDialect = OrmDialect;
