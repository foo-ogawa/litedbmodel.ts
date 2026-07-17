/**
 * E1 (#116, epic #115) PROOF-OF-APPROACH — the SQL-PORT lowering: bake the per-op SQL as a NATIVE
 * LITERAL in the generated module instead of shipping it in the runtime-read JSON companion.
 *
 * ## The empirical question this suite answers
 *
 * bc's typed-native emitter is SQL-AGNOSTIC: it bakes any port its `portIsStatic` predicate covers
 * (str/bool/null/ref/concat/number-literal/static-string-array) as a native literal — exactly how
 * graphddb gets `f_table:"UserPermissions"`. The open question was whether that extends to a FULL SQL
 * string port + the query's params as individual typed ports. It does (bc 0.8.0, `rust-typed-native`):
 *
 *   pub struct PortsNRFindUniqueN0 {
 *       pub f_sql: String, // "sql"
 *       pub f_p0: String,  // "p0"
 *       pub f_p1: i64,     // "p1"
 *   }
 *   let ports_n0 = PortsNRFindUniqueN0 {
 *       f_sql: "SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT ?".to_string(),
 *       f_p0: in_.email.clone(),
 *       f_p1: 1i64,
 *   };
 *
 * No `GeneratorFailure`/`UNSUPPORTED_NODE_STRAIGHTLINE` — the SQL literal, the scalar input ref, and
 * the bare LIMIT literal are all covered port shapes. So the module carries its own query and the JSON
 * companion is not needed on the read path.
 *
 * ## What this suite proves in-process
 *
 *  1. the emitted rust module CONTAINS the SQL as a native string literal and reads NO companion;
 *  2. the ONLY dependency it declares is `std` (no bc-runtime import, no JSON crate, no boxed Value);
 *  3. the lowering FAILS CLOSED (naming the shape) on every param/statement shape it cannot bake —
 *     never a silent mis-lowering.
 *
 * The out-of-process legs (rustc runtime-free compile + real sqlite execution byte-equal to the
 * mode-2 `executeBundle` oracle) run from `rust/e1_native_proof` against the artifacts this suite
 * emits into `/tmp/e1proof` — see that crate's `src/main.rs` for the exec seam.
 */
import { describe, it, expect } from 'vitest';
import { writeFileSync, rmSync, existsSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileBundle,
  executeBundle,
  whereEq,
  whereIn,
  inColumn,
  schemaColumnTypeResolver,
  generateCodegenArtifact,
  TypedNativeCoverageError,
} from '../../src/scp/index';
import { registeredLanguages } from 'behavior-contracts';
import Database from 'better-sqlite3';
import { ddl, seedStatements } from '../../benchmark/crosslang/orm-domain';

const REGISTERED = registeredLanguages();
const L = components();

const USER_COLUMNS = {
  benchmark_users: { id: 'INTEGER', email: 'TEXT', name: 'TEXT', created_at: 'TEXT', updated_at: 'TEXT' },
} as const;

/** The proof op: findUnique-shape — `SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT ?`. */
class OrmReads extends SemanticBehavior {
  static columns = USER_COLUMNS;
  FindUnique($: { email: unknown }) {
    return L.Select({
      table: 'benchmark_users',
      select: ['id', 'email', 'name'],
      where: [whereEq(($ as never)['email'], $.email)],
      limit: 1,
    });
  }
  /** An IN-list read — the array-bound head E1 does NOT yet bake (declared E2+ follow-on). */
  ByIds($: { ids: unknown }) {
    return L.Select({
      table: 'benchmark_users',
      select: ['id', 'email', 'name'],
      where: [whereIn(inColumn($ as never, 'id'), $.ids)],
    });
  }
}

const RESOLVE = schemaColumnTypeResolver(ddl('sqlite'));
const CONTRACT = publishBehaviors(OrmReads);
const EXPECTED_SQL = 'SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT ?';

function findUniqueBundle() {
  return compileBundle(CONTRACT, 'FindUnique', [], 'sqlite', undefined, RESOLVE);
}

describe('E1 — bc bakes a full-SQL static port + typed param ports (the empirical answer)', () => {
  it('emits f_sql as a native string literal + f_p0/f_p1 as typed ports — no GeneratorFailure', () => {
    const art = generateCodegenArtifact(findUniqueBundle(), 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true });
    const code = art.module.code;

    // (a) the full SQL string port is baked as a native literal.
    expect(code).toContain(`f_sql: "${EXPECTED_SQL}".to_string()`);
    // (b) the params are individual typed ports — a scalar input ref and a bare number literal.
    expect(code).toContain('f_p0: in_.email.clone()');
    expect(code).toContain('f_p1: 1i64');
    // the concrete ports struct carries native types only — no boxed Value field.
    expect(code).toContain('pub f_sql: String');
    expect(code).toContain('pub f_p0: String');
    expect(code).toContain('pub f_p1: i64');
  });

  it('the baked SQL is byte-identical to what the mode-2 runtime renders for the same bundle', () => {
    // The lowering renders through the SAME composeMakeSQL + renderPlaceholders assembly the runtime
    // uses, so this is identity by construction — asserted here so a divergence cannot pass silently.
    const bundle = findUniqueBundle();
    const stmts = bundle.readGraph!.statementsById.n0;
    const assembled = `${stmts[0].sql} WHERE ${stmts[1].sql}${stmts[2].sql}`;
    expect(assembled).toBe(EXPECTED_SQL);
  });

  it('the module declares NO runtime dependency — std only, no bc runtime / JSON / boxed Value', () => {
    const art = generateCodegenArtifact(findUniqueBundle(), 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true });
    const code = stripRustComments(art.module.code);
    const uses = [...code.matchAll(/^\s*(?:use|extern crate)\s+([^;]+);/gm)].map((m) => m[1].trim());
    expect(uses).toEqual(['std::cell::RefCell']);
    for (const marker of ['serde_json', 'behavior_contracts', 'run_behavior', 'RawValue', 'Box<dyn', 'dyn Any', 'Value::']) {
      expect(code).not.toContain(marker);
    }
  });

  it('reads NO companion: the module needs no catalog to know its own query', () => {
    const art = generateCodegenArtifact(findUniqueBundle(), 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true });
    // The SQL the companion carries is now IN the module — the companion is redundant for this read.
    const companionSql = art.companion.readGraph!.statementsById.n0.map((s) => s.sql).join('');
    expect(companionSql.length).toBeGreaterThan(0); // still emitted (staged for removal, see report)
    expect(art.module.code).toContain(EXPECTED_SQL); // …but the module no longer needs it
  });
});

describe('E1 — the lowering fails CLOSED on every shape it cannot bake (no silent mis-lowering)', () => {
  it('an IN-list / array-bound head is rejected, naming the shape', () => {
    const bundle = compileBundle(CONTRACT, 'ByIds', [], 'sqlite', undefined, RESOLVE);
    expect(() => generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true })).toThrow(
      TypedNativeCoverageError,
    );
    expect(() => generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true })).toThrow(
      /IN-list \/ array param/,
    );
  });

  it('the pre-E1 lowering still covers those shapes (E1 is opt-in; no coverage was silently narrowed)', () => {
    const bundle = compileBundle(CONTRACT, 'ByIds', [], 'sqlite', undefined, RESOLVE);
    // Same bundle, default (pre-E1) lowering → still generates, exactly as before this change.
    expect(() => generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE)).not.toThrow();
  });
});

/** Strip rust line/block comments, preserving string literals (so header prose never false-positives). */
function stripRustComments(src: string): string {
  let out = '';
  let i = 0;
  const n = src.length;
  while (i < n) {
    const c = src[i];
    if (c === '"') {
      out += c;
      i++;
      while (i < n && src[i] !== '"') {
        if (src[i] === '\\') {
          out += src[i];
          i++;
        }
        if (i < n) {
          out += src[i];
          i++;
        }
      }
      if (i < n) {
        out += src[i];
        i++;
      }
      continue;
    }
    if (c === '/' && src[i + 1] === '/') {
      while (i < n && src[i] !== '\n') i++;
      continue;
    }
    if (c === '/' && src[i + 1] === '*') {
      i += 2;
      while (i < n && !(src[i] === '*' && src[i + 1] === '/')) i++;
      i += 2;
      continue;
    }
    out += c;
    i++;
  }
  return out;
}

// ── emit the artifacts the out-of-process rust proof leg consumes ────────────────────────────
const PROOF_DIR = '/tmp/e1proof';
const DB_PATH = join(PROOF_DIR, 'proof.db');

/** The inputs the rust leg replays; each is checked against the mode-2 `executeBundle` oracle. */
const PROOF_INPUTS = ['user500@example.com', 'user1@example.com', 'user42@example.com', 'nobody@example.com'];

describe('E1 — emit module + seeded DB + mode-2 oracles for the rust execution leg', () => {
  it('writes /tmp/e1proof/{generated_findunique.rs,proof.db,oracles.json}', () => {
    mkdirSync(PROOF_DIR, { recursive: true });
    const bundle = findUniqueBundle();
    const art = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true });
    writeFileSync(join(PROOF_DIR, 'generated_findunique.rs'), art.module.code);

    // ONE seeded sqlite DB FILE shared by BOTH legs — the oracle and the rust run read byte-identical
    // data, so an equality pass cannot come from two independently-seeded DBs happening to agree.
    if (existsSync(DB_PATH)) rmSync(DB_PATH);
    const db = new Database(DB_PATH);
    db.pragma('foreign_keys = ON');
    for (const s of ddl('sqlite')) db.exec(s);
    for (const s of seedStatements('sqlite')) db.prepare(s.sql).run(...(s.params as never[]));

    const oracles: Record<string, unknown> = {};
    for (const email of PROOF_INPUTS) oracles[email] = executeBundle(bundle, { email } as never, { db: db as never });
    db.close();

    writeFileSync(join(PROOF_DIR, 'oracles.json'), JSON.stringify(oracles, null, 2));
    expect(JSON.stringify(oracles['user500@example.com'])).toBe('[{"id":500,"email":"user500@example.com","name":"User 500"}]');
    expect(JSON.stringify(oracles['nobody@example.com'])).toBe('[]');
  });
});
