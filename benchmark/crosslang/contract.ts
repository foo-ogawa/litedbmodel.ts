// ════════════════════════════════════════════════════════════════════════════
// Cross-language benchmark adapter CONTRACT (litedbmodel epic #63)
// ════════════════════════════════════════════════════════════════════════════
//
// SINGLE source of truth for the subprocess protocol every language adapter
// (TS / Python / PHP / Rust / Go) implements. The cross-language harness
// (harness.ts) spawns ONE production cell per language, speaks this line-delimited
// JSON (NDJSON) protocol over stdin/stdout, and aggregates the returned metrics.
//
// ONE production path (epic #63): each language's THIN GENERIC RUNTIME executes the
// SAME 19 ORM-comparison ops (== the #64 v1 SQL golden == #65 v2 SCP parity ==
// benchmark.ts litedbmodel column), driver-included, DB-backed on all three real
// dialects. There is NO impl axis (the old sql/codegen/ir/dynamic/prepared surfaces)
// and NO I/O-excluded micro/mock axis — both are gone (V8-JIT/timing-confounded and
// off the production path).
//
// Wire format: the harness writes ONE request object per line to the child's stdin
// and reads ONE response object per line from its stdout. stderr is diagnostics. The
// FIRST stdout line a fresh child writes MUST be `ready`. A child exits 0 on `shutdown`.

import { ORM_OP_IDS, ORM_OP_LABEL, ORM_WRITE_OP_IDS, ORM_DIALECTS, type OrmDialect } from './orm-plan.js';

// ── The op axis — the 19 ORM-comparison ops (no subset) ──────────────────────
export const CROSSLANG_CASE_IDS = ORM_OP_IDS;
export type CrosslangCaseId = string;
export const CROSSLANG_CASE_LABELS: Record<string, string> = ORM_OP_LABEL;

// Ops whose logical op is a WRITE (the harness asserts queries/op parity for these,
// rows/op parity for reads).
export const CROSSLANG_WRITE_CASES: ReadonlySet<string> = ORM_WRITE_OP_IDS;

// ── The dialect axis (the three real targets) ────────────────────────────────
export const CROSSLANG_DIALECTS = ORM_DIALECTS;
export type CrosslangDialect = OrmDialect;

// ── Protocol messages (harness → child) ──────────────────────────────────────
// Every case-scoped request carries the target `dialect`: the child runs the op's plan
// (from the shared orm-plan.json artifact) against the matching REAL database.
export type Request =
  | { kind: 'run'; case: CrosslangCaseId; dialect: CrosslangDialect; warmup: number; iterations: number }
  | { kind: 'throughput'; case: CrosslangCaseId; dialect: CrosslangDialect; iterations: number; concurrency: number }
  | { kind: 'cost'; case: CrosslangCaseId; dialect: CrosslangDialect }
  | { kind: 'rss' }
  | { kind: 'shutdown' };

// ── Protocol messages (child → harness) ──────────────────────────────────────
// A `skipped` response is an HONEST per-cell "did not run" (e.g. a language with no live
// PG driver, or an op a language genuinely cannot run) — rendered as an explicit note,
// never silently dropped.
export type Response =
  | { kind: 'ready'; language: string; impl: string; readyAtEpochMs: number }
  | { kind: 'run'; case: CrosslangCaseId; dialect: CrosslangDialect; samplesMs: number[] }
  | { kind: 'throughput'; case: CrosslangCaseId; dialect: CrosslangDialect; elapsedMs: number; completed: number }
  | { kind: 'cost'; case: CrosslangCaseId; dialect: CrosslangDialect; queries: number; rows: number }
  | { kind: 'rss'; rssBytes: number }
  | { kind: 'skipped'; case: string; dialect: CrosslangDialect; reason: string }
  | { kind: 'error'; message: string; stack?: string };

// ── Serialization helpers (shared by harness AND the TS adapter) ─────────────
export function encodeMessage(msg: Request | Response): string {
  return JSON.stringify(msg) + '\n';
}

export function decodeMessages<T>(buffer: string): { messages: T[]; rest: string } {
  const messages: T[] = [];
  let rest = buffer;
  let idx = rest.indexOf('\n');
  while (idx !== -1) {
    const line = rest.slice(0, idx);
    rest = rest.slice(idx + 1);
    const trimmed = line.trim();
    if (trimmed.length > 0) messages.push(JSON.parse(trimmed) as T);
    idx = rest.indexOf('\n');
  }
  return { messages, rest };
}
