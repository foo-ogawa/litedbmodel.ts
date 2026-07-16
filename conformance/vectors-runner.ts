/**
 * TS vector runner (WS7a, #30) — the reference leg of the cross-language conformance LOCK.
 *
 * Mirrors graphddb's `conformance/vectors-runner.ts`: it loads the FROZEN vector corpus
 * (`conformance/vectors/*.json`) and runs each vector through the litedbmodel SCP runtime it
 * CONSUMES — the BUILT published artifact `dist/scp/index.mjs` (`renderReadPrimary` /
 * `executeBundle` / `executeTransactionBundle` / `dialectFor`), NOT the raw source — so it
 * proves the exact package a consumer ships on reproduces the corpus byte-for-byte. It emits a
 * MACHINE-READABLE JSON summary as its LAST stdout line so the cross-language orchestrator
 * (`vectors-run.ts`) can tally every language identically:
 *
 *   {"lang":"ts","suites":{"render":{"pass":N,"fail":N}, ...},
 *    "total_pass":N,"total_fail":N,"version_mismatch":false}
 *
 * Exit: 0 all pass, 1 any fail, 2 corpus-version mismatch (pre-flight fail-closed).
 *
 * Prerequisite: `npm run build:scp` (the .mjs consumer artifact). Run via:
 *   npx tsx conformance/vectors-runner.ts
 *
 * WS7b-e each add a sibling runner (python/conformance vectors_runner.py, go/…, rust/…, php/…)
 * that consumes THIS SAME corpus through its own published runtime and emits the same summary.
 */
import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import Database from 'better-sqlite3';
import {
  renderReadPrimary,
  dialectFor,
  executeBundle,
  executeTransactionBundle,
  readBundle,
  LimitExceededError,
} from '../dist/scp/index.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
const VECTORS_DIR = process.env.LITEDBMODEL_VECTORS ?? join(HERE, 'vectors');

/** The corpus schema version this runner supports (pin — bumped on additive refreeze). */
const SUPPORTED_CORPUS_VERSION = 3;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Json = any;

/** bigint-safe decode: `{ $bigint }` → bigint, structural otherwise. */
function decodeValue(v: Json): unknown {
  if (v === null || typeof v !== 'object') return v;
  if (Array.isArray(v)) return v.map(decodeValue);
  const keys = Object.keys(v);
  if (keys.length === 1 && keys[0] === '$bigint') return BigInt(v.$bigint);
  const out: Record<string, unknown> = {};
  for (const [k, val] of Object.entries(v)) out[k] = decodeValue(val);
  return out;
}

/** bigint-safe encode (mirror of the generator's, so comparisons are canonical). */
function encodeValue(v: unknown): Json {
  if (typeof v === 'bigint') return { $bigint: v.toString() };
  if (v === null || typeof v !== 'object') return v;
  if (Array.isArray(v)) return v.map(encodeValue);
  const out: Record<string, Json> = {};
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) out[k] = encodeValue(val);
  return out;
}

function eq(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function seedDb(schema: string[]): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  for (const stmt of schema) db.exec(stmt);
  return db;
}

interface Tally {
  pass: number;
  fail: number;
}

function line(ok: boolean, name: string, detail?: string): void {
  // Human progress → stderr so stdout carries only the JSON summary line.
  if (ok) console.error(`  ✓ ${name}`);
  else {
    console.error(`  ✗ ${name}`);
    if (detail) console.error(`      ${detail}`);
  }
}

/** Run ONE vector through the consumed runtime; return ok + detail. */
function runVector(v: Json): { ok: boolean; detail?: string } {
  try {
    if (v.kind === 'render') {
      const r = renderReadPrimary(v.readGraph, decodeValue(v.input) as never);
      const sqlOk = r.sql === v.expectedSql;
      const paramsOk = eq(r.params.map(encodeValue), v.expectedParams);
      if (sqlOk && paramsOk) return { ok: true };
      const parts: string[] = [];
      if (!sqlOk) parts.push(`sql ${JSON.stringify(r.sql)} != ${JSON.stringify(v.expectedSql)}`);
      if (!paramsOk) parts.push(`params mismatch`);
      return { ok: false, detail: parts.join('; ') };
    }
    if (v.kind === 'write-render') {
      const sqlOk = v.statement.sql === v.expectedSql;
      const paramsOk = eq(v.statement.params.map(encodeValue), v.expectedParams);
      return { ok: sqlOk && paramsOk, detail: sqlOk && paramsOk ? undefined : `write-render mismatch` };
    }
    if (v.kind === 'exec') {
      const db = seedDb(v.schema);
      const raw = v.withRelation !== undefined
        ? readBundle(v.bundle, decodeValue(v.input) as never, { db, with: { [v.withRelation]: true } })
        : executeBundle(v.bundle, decodeValue(v.input) as never, { db });
      const result = encodeValue(raw);
      db.close();
      const ok = eq(result, v.expectedResult);
      return { ok, detail: ok ? undefined : `result mismatch` };
    }
    if (v.kind === 'expect-error') {
      // Phase E-2 hard-limit guard: the cap is baked into the bundle (findGuard / relation hardLimit),
      // so run it over-cap and assert the SAME LimitExceededError fields. No config surface needed.
      const db = seedDb(v.schema);
      let thrown: unknown;
      try {
        if (v.withRelation !== undefined) readBundle(v.bundle, decodeValue(v.input) as never, { db, with: { [v.withRelation]: true } });
        else executeBundle(v.bundle, decodeValue(v.input) as never, { db });
      } catch (e) { thrown = e; }
      db.close();
      if (!(thrown instanceof LimitExceededError)) {
        return { ok: false, detail: `expected LimitExceededError, got ${thrown === undefined ? 'no throw' : thrown instanceof Error ? thrown.name : String(thrown)}` };
      }
      const got = { name: thrown.name, limit: thrown.limit, count: thrown.count, context: thrown.context, ...(thrown.model !== undefined ? { model: thrown.model } : {}), ...(thrown.relation !== undefined ? { relation: thrown.relation } : {}) };
      const ok = eq(got, v.expectedError);
      return { ok, detail: ok ? undefined : `error ${JSON.stringify(got)} != ${JSON.stringify(v.expectedError)}` };
    }
    if (v.kind === 'tx') {
      const db = seedDb(v.schema);
      const result = encodeValue(executeTransactionBundle(v.bundle, decodeValue(v.input) as never, { db }));
      const stateOk = (v.expectedDbState ?? []).every((s: Json) => eq(encodeValue(db.prepare(s.query).all()), s.rows));
      db.close();
      const ok = eq(result, v.expectedResult) && stateOk;
      return { ok, detail: ok ? undefined : `result/db-state mismatch` };
    }
    if (v.kind === 'dialect') {
      const got = dialectFor(v.dialect).orderByNulls(v.args.expr, v.args.dir, v.args.nulls);
      const ok = got === v.expected;
      return { ok, detail: ok ? undefined : `${JSON.stringify(got)} != ${JSON.stringify(v.expected)}` };
    }
    return { ok: false, detail: `unknown vector kind: ${v.kind}` };
  } catch (e) {
    return { ok: false, detail: `threw: ${e instanceof Error ? e.message : String(e)}` };
  }
}

function main(): void {
  console.error('litedbmodel SCP conformance vectors — TS runner (consumed dist/scp artifact)');
  const files = readdirSync(VECTORS_DIR).filter((f) => f.endsWith('.json')).sort();

  // Pre-flight version sweep (fail-closed): reject the whole run on any suite-version mismatch.
  const suites = files.map((f) => JSON.parse(readFileSync(join(VECTORS_DIR, f), 'utf8')) as Json);
  const mismatched = suites.filter((s) => s.corpusVersion !== SUPPORTED_CORPUS_VERSION);
  if (mismatched.length > 0) {
    for (const s of mismatched) {
      console.error(`FAIL-CLOSED: suite '${s.suite}' corpusVersion ${s.corpusVersion} != supported ${SUPPORTED_CORPUS_VERSION}.`);
    }
    console.log(JSON.stringify({ lang: 'ts', suites: {}, total_pass: 0, total_fail: 0, version_mismatch: true }));
    process.exit(2);
  }

  const tallies: Record<string, Tally> = {};
  for (const suite of suites) {
    const t: Tally = { pass: 0, fail: 0 };
    console.error(`\n${suite.suite}.json — ${suite.vectors.length} vectors`);
    for (const v of suite.vectors) {
      const r = runVector(v);
      line(r.ok, v.name, r.detail);
      r.ok ? t.pass++ : t.fail++;
    }
    tallies[suite.suite] = t;
  }

  const total_pass = Object.values(tallies).reduce((n, s) => n + s.pass, 0);
  const total_fail = Object.values(tallies).reduce((n, s) => n + s.fail, 0);
  console.error(`\n${total_pass} passed, ${total_fail} failed / ${total_pass + total_fail} vectors across ${suites.length} suites`);
  console.log(JSON.stringify({ lang: 'ts', suites: tallies, total_pass, total_fail, version_mismatch: false }));
  process.exit(total_fail > 0 ? 1 : 0);
}

main();
