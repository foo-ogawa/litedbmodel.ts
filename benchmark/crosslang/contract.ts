// ════════════════════════════════════════════════════════════════════════════
// Cross-language benchmark adapter CONTRACT (litedbmodel epic #44)
// ════════════════════════════════════════════════════════════════════════════
//
// SINGLE source of truth for the subprocess protocol every language adapter
// (TS / Python / PHP / Rust / Go) implements. The cross-language harness
// (harness.ts) spawns ONE subprocess per (language × impl) cell, speaks this
// line-delimited JSON (NDJSON) protocol over stdin/stdout, and aggregates the
// returned metrics. Same shape as graphddb#307's contract; generalized to the
// litedbmodel impl axis (sql / codegen / ir / dynamic / prepared).
//
// Wire format: the harness writes ONE request object per line to the child's
// stdin and reads ONE response object per line from its stdout. stderr is
// diagnostics. The FIRST stdout line a fresh child writes MUST be `ready`
// (the cold-start boundary the harness times). A child exits 0 on `shutdown`.

// ── The impl axis (litedbmodel exec surfaces — issue #44) ────────────────────
//   sql       — hand-optimized raw SQL via better-sqlite3 direct (baseline 1.0×)
//   codegen   — codegen-static: IR baked as a native literal + fingerprint-verified
//               at module load, then executed via the static makeSQL catalog. All langs.
//   ir        — dynamic-JSON: load the makeSQL bundle JSON + run via the shared runtime
//               (bc run_behavior + makeSQL handler). The non-TS reality. All langs.
//   dynamic   — TS only: DBModel.find/create == executeBehavior (recompile per call).
//   prepared  — TS only: compileBundle once → executeBundle many.
//   v1        — TS only: shipped litedbmodel@1.2.10 eager path (DBConditions direct) — regression gate.
export type Impl = 'sql' | 'codegen' | 'ir' | 'dynamic' | 'prepared' | 'v1';

// ── Case ids the cross-lang matrix runs ─────────────────────────────────────
// 8 representative litedbmodel access patterns across read / relation / write /
// transaction axes. Each id maps 1:1 onto a method every adapter implements.
export const CROSSLANG_CASE_IDS = [
  'find', //           SELECT: eq + SKIP-optional present + range, ORDER BY
  'complexWhere', //   SELECT: eq + range + LIKE + IN (multiple predicate kinds)
  'inList', //         SELECT: IN-list (single-JSON param)
  'belongsTo', //      relation: posts → author (parent + one batched query)
  'hasMany', //        relation: posts → comments (batch by parent key, N+1 avoided)
  'hasManyLimit', //   relation: posts → recent comments (per-parent LIMIT)
  'batchInsert', //    createMany: one logical op → grouped INSERT in one tx
  'writeTxGate', //    write-tx: gate-first create (requires + unique + body + derive), one tx
] as const;

export type CrosslangCaseId = (typeof CROSSLANG_CASE_IDS)[number];

export const CROSSLANG_CASE_LABELS: Record<CrosslangCaseId, string> = {
  find: 'find (eq+SKIP+range)',
  complexWhere: 'complex WHERE',
  inList: 'IN-list',
  belongsTo: 'relation belongsTo',
  hasMany: 'relation hasMany',
  hasManyLimit: 'relation hasMany-limit',
  batchInsert: 'batch insert',
  writeTxGate: 'write-tx gate-first',
};

// Cases whose logical op is a WRITE (the harness asserts queries/op parity for these,
// rows/op parity for reads).
export const CROSSLANG_WRITE_CASES = new Set<CrosslangCaseId>(['batchInsert', 'writeTxGate']);

// ── Micro-bench case ids (I/O-EXCLUDED — the load-bearing signal) ────────────
// A subset exercising distinct client-side code paths: a point/list read hydrate,
// a relation build+hydrate, and a marshaling-only write. The SQL driver is mocked
// (fixed rows, no round-trip) so the timed op is ONLY the client-side path
// (compile/render/param-eval/bind/`?`→`$N`/hydration).
export const CROSSLANG_MICRO_CASE_IDS = [
  'find', //         render WHERE + bind + hydrate a bounded row set
  'complexWhere', // render a multi-predicate WHERE + bind + hydrate
  'hasMany', //      build the relation plan + hydrate parent + child rows
  'writeTxGate', //  derive gate-first plan + render statements (write path)
] as const;

export type CrosslangMicroCaseId = (typeof CROSSLANG_MICRO_CASE_IDS)[number];

// ── Protocol messages (harness → child) ──────────────────────────────────────
export type Request =
  | { kind: 'run'; case: CrosslangCaseId; warmup: number; iterations: number }
  | { kind: 'throughput'; case: CrosslangCaseId; iterations: number; concurrency: number }
  | { kind: 'micro'; case: CrosslangMicroCaseId; warmup: number; iterations: number }
  | { kind: 'rss' }
  | { kind: 'cost'; case: CrosslangCaseId }
  | { kind: 'shutdown' };

// ── Protocol messages (child → harness) ──────────────────────────────────────
export type Response =
  | { kind: 'ready'; language: string; impl: Impl; readyAtEpochMs: number }
  | { kind: 'run'; case: CrosslangCaseId; samplesMs: number[] }
  | { kind: 'throughput'; case: CrosslangCaseId; elapsedMs: number; completed: number }
  | { kind: 'micro'; case: CrosslangMicroCaseId; samplesMs: number[] }
  | { kind: 'rss'; rssBytes: number }
  | { kind: 'cost'; case: CrosslangCaseId; queries: number; rows: number }
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
