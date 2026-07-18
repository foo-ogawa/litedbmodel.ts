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
  coalesce,
  opt,
  ne,
  when,
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
  /** An IN-list read — the array-bound head bakes as a native `Vec<i64>` port (bc#110). */
  ByIds($: { ids: unknown }) {
    return L.Select({
      table: 'benchmark_users',
      select: ['id', 'email', 'name'],
      where: [whereIn(inColumn($ as never, 'id'), $.ids)],
      order: 'id ASC',
    });
  }
  /** An OPTIONAL-limit read — its `LIMIT ?` param is `coalesce(opt($.limit), 20)` (the #122 shape). */
  Recent($: { limit: unknown }) {
    return L.Select({
      table: 'benchmark_users',
      select: ['id', 'email', 'name'],
      order: 'id ASC',
      limit: coalesce(opt($.limit), 20),
    });
  }
  /** A SKIP-guarded read — the `name = ?` fragment DROPS when `name` is absent (the skip shape). */
  ByName($: { id: unknown; name: unknown }) {
    return L.Select({
      table: 'benchmark_users',
      select: ['id', 'email', 'name'],
      where: [whereEq(($ as never)['id'], $.id), when(ne(opt($.name), null), () => whereEq(($ as never)['name'], $.name))],
      order: 'id ASC',
    });
  }
}

/** Write ops — read and write go through the SAME lowering + SAME generic exec seam (owner: one flow). */
class OrmWrites extends SemanticBehavior {
  static columns = USER_COLUMNS;
  /** INSERT … RETURNING — a row-returning write. */
  CreateUser($: { email: unknown; name: unknown }) {
    return L.Insert({ table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name, returning: 'id, email, name' });
  }
  /** UPDATE … WHERE … RETURNING — SET value + WHERE-bound head, both typed from the authored ports. */
  RenameUser($: { id: unknown; name: unknown }) {
    return L.Update({ table: 'benchmark_users', 'set.name': $.name, where: [whereEq(($ as never)['id'], $.id)], returning: 'id, email, name' });
  }
  /** DELETE … WHERE — a NON-returning write: the summary row [{changes, lastInsertRowid}]. */
  DeleteUser($: { id: unknown }) {
    return L.Delete({ table: 'benchmark_users', where: [whereEq(($ as never)['id'], $.id)] });
  }
}

/** A SKIP op on benchmark_posts — the `published = ?` fragment DROPS when `published` is absent. */
const POST_COLUMNS = {
  benchmark_posts: { id: 'INTEGER', title: 'TEXT', content: 'TEXT', published: 'INTEGER', author_id: 'INTEGER', created_at: 'TEXT' },
} as const;
class PostReads extends SemanticBehavior {
  static columns = POST_COLUMNS;
  ByAuthorMaybePublished($: { author_id: unknown; published: unknown }) {
    return L.Select({
      table: 'benchmark_posts',
      select: ['id', 'title', 'author_id', 'published'],
      where: [whereEq(($ as never)['author_id'], $.author_id), when(ne(opt($.published), null), () => whereEq(($ as never)['published'], $.published))],
      order: 'id ASC',
    });
  }
}

/** A single-key relation (E4/#119): posts + each post's author (belongsTo), on the SQL-baking lowering. */
const REL_COLUMNS = {
  benchmark_posts: { id: 'INTEGER', title: 'TEXT', content: 'TEXT', published: 'INTEGER', author_id: 'INTEGER', created_at: 'TEXT' },
  benchmark_users: { id: 'INTEGER', email: 'TEXT', name: 'TEXT', created_at: 'TEXT', updated_at: 'TEXT' },
} as const;
class FeedReads extends SemanticBehavior {
  static columns = REL_COLUMNS;
  PostsWithAuthor($: { author_id: unknown }) {
    const posts = L.Select({ table: 'benchmark_posts', select: ['id', 'title', 'author_id'], where: [whereEq(($ as never)['author_id'], $.author_id)], order: 'id ASC' });
    const authors = (posts as unknown as { map(fn: (p: never) => unknown): unknown }).map(($p: never) =>
      L.Select({ table: 'benchmark_users', select: ['id', 'name'], where: [whereEq(($p as never)['id'], ($p as never)['author_id'])] }),
    );
    return { posts, authors };
  }
}

const RESOLVE = schemaColumnTypeResolver(ddl('sqlite'));
const CONTRACT = publishBehaviors(OrmReads);
const WRITE_CONTRACT = publishBehaviors(OrmWrites);
const POST_CONTRACT = publishBehaviors(PostReads);
const POST_RESOLVE = schemaColumnTypeResolver(ddl('sqlite'));
const FEED_CONTRACT = publishBehaviors(FeedReads);
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

describe('E2 — IN-list: the array-bound head bakes as a native Vec<ElemT> port (bc#110)', () => {
  it('bakes the json_each SQL + a native Vec<i64> array port fed from the input struct', () => {
    const bundle = compileBundle(CONTRACT, 'ByIds', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true }).module.code;
    expect(code).toContain('f_sql: "SELECT id, email, name FROM benchmark_users WHERE id IN (SELECT value FROM json_each(?)) ORDER BY id ASC".to_string()');
    expect(code).toContain('pub f_p0: Vec<i64>');
    expect(code).toContain('f_p0: in_.ids.clone()');
    expect(code).toContain('pub ids: Vec<i64>');
    // The `@in` sentinel is a WHERE COLUMN-NAME MARKER, never a bound value — it must NOT reach the
    // input struct (it has no native port type and would emit an unresolvable field).
    expect(code).not.toContain('@in');
    // zero-boxing: the array rides natively, no serde_json on the hot path.
    expect(stripRustComments(code)).not.toContain('serde_json');
  });
});

describe('#122 — a coalesce(opt(limit), N) LIMIT default bakes NATIVE, preserving the default (bc 0.8.5 #139)', () => {
  it('bakes an OPTIONAL input port + `in_.limit.unwrap_or(20i64)` — the default is not dropped', () => {
    const bundle = compileBundle(CONTRACT, 'Recent', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true }).module.code;
    expect(code).toContain('f_sql: "SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT ?".to_string()');
    // the default is BAKED — absent limit resolves to 20, not dropped and not a silent 0.
    expect(code).toContain('in_.limit.unwrap_or(20i64)');
    // the head is an OPTIONAL input port (Option<i64>), the native representation of "absent".
    expect(code).toContain('pub limit: Option<i64>');
    expect(stripRustComments(code)).not.toContain('serde_json');
  });
});

describe('skip — a SKIP-optional WHERE fragment bakes fragmented; the seam drops it via the Option presence', () => {
  it('bakes head + tail + per-fragment ports; the skip head is an Option<i64> presence signal', () => {
    const bundle = compileBundle(POST_CONTRACT, 'ByAuthorMaybePublished', [], 'sqlite', undefined, POST_RESOLVE);
    const code = generateCodegenArtifact(bundle, 'rust', REGISTERED, POST_RESOLVE, undefined, { nativeSql: true }).module.code;
    expect(code).toContain('f_sql_head: "SELECT id, title, author_id, published FROM benchmark_posts".to_string()');
    expect(code).toContain('f_sql_tail: " ORDER BY id ASC".to_string()');
    expect(code).toContain('f_w0: "author_id = ?".to_string()');
    expect(code).toContain('f_w0p0: in_.author_id'); // required fragment head (i64)
    expect(code).toContain('f_w1: "published = ?".to_string()');
    expect(code).toContain('f_w1p0: in_.published'); // skip fragment head (Option<i64> — presence)
    expect(code).toContain('pub published: Option<i64>');
    expect(stripRustComments(code)).not.toContain('serde_json');
  });
});

describe('E4 (#119) — a single-key map relation bakes; the child binds the parent element field natively', () => {
  it('bakes parent + per-element child SQL; the element-field ref is a native i64 (from the parent outType)', () => {
    const bundle = compileBundle(FEED_CONTRACT, 'PostsWithAuthor', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true }).module.code;
    // parent (n0) SQL baked
    expect(code).toContain('f_sql: "SELECT id, title, author_id FROM benchmark_posts WHERE author_id = ? ORDER BY id ASC".to_string()');
    // child (n1) SQL baked; its param binds the mapped parent element's author_id — NOT an input head
    expect(code).toContain('f_sql: "SELECT id, name FROM benchmark_users WHERE id = ?".to_string()');
    expect(code).toContain('f_p0: oel_n1.author_id'); // native element-field access, typed i64 from the parent
    // the ONLY component input is author_id; the element var never leaks into InNR
    expect(code).toContain('pub author_id: i64');
    expect(code).not.toContain('$e0');
    expect(stripRustComments(code)).not.toContain('serde_json');
  });
});

describe('the lowering fails CLOSED on every shape it cannot bake (no silent mis-lowering)', () => {
  it('the pre-baking lowering still covers these shapes (nativeSql is opt-in; no coverage narrowed)', () => {
    for (const entry of ['Recent', 'ByName']) {
      const bundle = compileBundle(CONTRACT, entry, [], 'sqlite', undefined, RESOLVE);
      // Same bundle, default (companion) lowering → still generates, exactly as before this change.
      expect(() => generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE)).not.toThrow();
    }
  });

  it('a bundle with neither a graph nor a statement is refused (nothing to generate)', () => {
    expect(() => generateCodegenArtifact({ dialect: 'sqlite', name: 'Create', optionalHeads: [], relations: {} } as never, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true })).toThrow(
      /carries no component graph/,
    );
  });
});

describe('E3 — writes bake through the SAME lowering as reads (owner: read/write are one flow)', () => {
  it('Insert bakes RETURNING SQL + value heads typed from values.* ports', () => {
    const b = compileBundle(WRITE_CONTRACT, 'CreateUser', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(b, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true }).module.code;
    expect(code).toContain('f_sql: "INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id, email, name".to_string()');
    expect(code).toContain('f_p0: in_.email.clone()');
    expect(code).toContain('f_p1: in_.name.clone()');
  });

  it('Update bakes SET+WHERE SQL; the WHERE-bound head is typed from the authored where port (no FROM regex)', () => {
    const b = compileBundle(WRITE_CONTRACT, 'RenameUser', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(b, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true }).module.code;
    expect(code).toContain('f_sql: "UPDATE benchmark_users SET name = ? WHERE id = ? RETURNING id, email, name".to_string()');
    expect(code).toContain('f_p0: in_.name.clone()');
    expect(code).toContain('f_p1: in_.id'); // id typed i64 (native, no clone)
  });

  it('Delete (no RETURNING) bakes the summary-row outType [{changes, lastInsertRowid}]', () => {
    const b = compileBundle(WRITE_CONTRACT, 'DeleteUser', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(b, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true }).module.code;
    expect(code).toContain('f_sql: "DELETE FROM benchmark_users WHERE id = ?".to_string()');
    expect(code).toContain('pub changes: i64');
    expect(code).toContain('pub lastInsertRowid: i64');
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
/** IN-list inputs — incl. the EMPTY list (the #46 case that must yield zero rows, not an error). */
const BYIDS_INPUTS: number[][] = [[1, 2, 3], [42], [], [7, 7, 9], [999999]];

describe('E1/E2 — emit modules + seeded DB + mode-2 oracles for the rust execution leg', () => {
  it('writes /tmp/e1proof/{generated_*.rs,proof.db,oracles.json}', () => {
    mkdirSync(PROOF_DIR, { recursive: true });
    const bundle = findUniqueBundle();
    const art = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true });
    writeFileSync(join(PROOF_DIR, 'generated_findunique.rs'), art.module.code);

    const byIdsBundle = compileBundle(CONTRACT, 'ByIds', [], 'sqlite', undefined, RESOLVE);
    const byIdsArt = generateCodegenArtifact(byIdsBundle, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true });
    writeFileSync(join(PROOF_DIR, 'generated_byids.rs'), byIdsArt.module.code);

    // #122: the optional-LIMIT read — its baked `.unwrap_or(20)` must make an ABSENT limit fall back
    // to 20 and a PRESENT limit take effect, both byte-equal to the oracle.
    const recentBundle = compileBundle(CONTRACT, 'Recent', [], 'sqlite', undefined, RESOLVE);
    const recentArt = generateCodegenArtifact(recentBundle, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true });
    writeFileSync(join(PROOF_DIR, 'generated_recent.rs'), recentArt.module.code);

    // skip: the `published = ?` fragment drops when published is absent — present vs absent must
    // both match the mode-2 oracle (the seam assembles the present fragments over baked literals).
    const byMaybeBundle = compileBundle(POST_CONTRACT, 'ByAuthorMaybePublished', [], 'sqlite', undefined, POST_RESOLVE);
    const byMaybeArt = generateCodegenArtifact(byMaybeBundle, 'rust', REGISTERED, POST_RESOLVE, undefined, { nativeSql: true });
    writeFileSync(join(PROOF_DIR, 'generated_bymaybe.rs'), byMaybeArt.module.code);

    // E4 map relation: posts + per-post author. Output {authors, posts}.
    const feedBundle = compileBundle(FEED_CONTRACT, 'PostsWithAuthor', [], 'sqlite', undefined, RESOLVE);
    const feedArt = generateCodegenArtifact(feedBundle, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true });
    writeFileSync(join(PROOF_DIR, 'generated_feed.rs'), feedArt.module.code);

    // ONE seeded sqlite DB FILE shared by BOTH legs — the oracle and the rust run read byte-identical
    // data, so an equality pass cannot come from two independently-seeded DBs happening to agree.
    if (existsSync(DB_PATH)) rmSync(DB_PATH);
    const db = new Database(DB_PATH);
    db.pragma('foreign_keys = ON');
    for (const s of ddl('sqlite')) db.exec(s);
    for (const s of seedStatements('sqlite')) db.prepare(s.sql).run(...(s.params as never[]));

    const oracles: Record<string, unknown> = {};
    for (const email of PROOF_INPUTS) oracles[email] = executeBundle(bundle, { email } as never, { db: db as never });
    const byIdsOracles: Record<string, unknown> = {};
    for (const ids of BYIDS_INPUTS) byIdsOracles[ids.join(',')] = executeBundle(byIdsBundle, { ids } as never, { db: db as never });
    // #122: absent limit (key `""`) → default 20; present limit (`3`) → 3 rows.
    const recentOracles: Record<string, unknown> = {};
    recentOracles[''] = executeBundle(recentBundle, {} as never, { db: db as never });
    recentOracles['3'] = executeBundle(recentBundle, { limit: 3 } as never, { db: db as never });
    // skip: keyed `<author_id>|<published or ''>`. author_id=7 has posts; published present (1) vs absent.
    const byMaybeOracles: Record<string, unknown> = {};
    byMaybeOracles['7|'] = executeBundle(byMaybeBundle, { author_id: 7 } as never, { db: db as never });
    byMaybeOracles['7|1'] = executeBundle(byMaybeBundle, { author_id: 7, published: 1 } as never, { db: db as never });
    byMaybeOracles['7|0'] = executeBundle(byMaybeBundle, { author_id: 7, published: 0 } as never, { db: db as never });
    // E4 map: author 7 (has 2 posts), author 1, author 999 (no posts → empty relation).
    const feedOracles: Record<string, unknown> = {};
    for (const aid of [7, 1, 999]) feedOracles[String(aid)] = executeBundle(feedBundle, { author_id: aid } as never, { db: db as never });
    db.close();

    writeFileSync(join(PROOF_DIR, 'oracles.json'), JSON.stringify(oracles, null, 2));
    writeFileSync(join(PROOF_DIR, 'oracles_byids.json'), JSON.stringify(byIdsOracles, null, 2));
    writeFileSync(join(PROOF_DIR, 'oracles_recent.json'), JSON.stringify(recentOracles, null, 2));
    writeFileSync(join(PROOF_DIR, 'oracles_bymaybe.json'), JSON.stringify(byMaybeOracles, null, 2));
    writeFileSync(join(PROOF_DIR, 'oracles_feed.json'), JSON.stringify(feedOracles, null, 2));
    // the skip actually drops: absent published returns >= as many rows as present-filtered.
    expect((byMaybeOracles['7|'] as unknown[]).length).toBeGreaterThanOrEqual((byMaybeOracles['7|1'] as unknown[]).length);
    // the relation resolves: author 7 has 2 posts, each with its author list; author 999 has none.
    expect(((feedOracles['7'] as { posts: unknown[] }).posts).length).toBe(2);
    expect(((feedOracles['999'] as { posts: unknown[] }).posts).length).toBe(0);
    // the default actually takes effect: absent limit returns 20 rows (the seed has 111).
    expect((recentOracles[''] as unknown[]).length).toBe(20);
    expect((recentOracles['3'] as unknown[]).length).toBe(3);
    expect(JSON.stringify(oracles['user500@example.com'])).toBe('[{"id":500,"email":"user500@example.com","name":"User 500"}]');
    expect(JSON.stringify(oracles['nobody@example.com'])).toBe('[]');
    // the empty IN-list must be zero rows (not an error) — the #46 case.
    expect(JSON.stringify(byIdsOracles[''])).toBe('[]');
  });
});

// ── writes: a write MUTATES state, so each op runs against a FRESH copy of a clean seed, and the
//    oracle captures BOTH the returned rows/summary AND the resulting table state ──────────────
const WRITE_SEED = join(PROOF_DIR, 'write_seed.db');

/** A fresh seeded DB (SMALL — 3 rows — so the full-state comparison is legible). */
function freshWriteDb(path?: string): InstanceType<typeof Database> {
  if (path !== undefined && existsSync(path)) rmSync(path);
  const db = new Database(path ?? ':memory:');
  db.pragma('foreign_keys = ON');
  for (const s of ddl('sqlite')) db.exec(s);
  for (let id = 1; id <= 3; id++) db.prepare('INSERT INTO benchmark_users (id, email, name) VALUES (?, ?, ?)').run(id, `user${id}@example.com`, `User ${id}`);
  return db;
}
function tableState(db: InstanceType<typeof Database>): unknown {
  return db.prepare('SELECT id, email, name FROM benchmark_users ORDER BY id').all();
}

interface WriteCase { op: string; entry: keyof OrmWrites & string; args: string[]; input: Record<string, unknown>; }
const WRITE_CASES: WriteCase[] = [
  { op: 'createuser', entry: 'CreateUser', args: ['zed@example.com', 'Zed'], input: { email: 'zed@example.com', name: 'Zed' } },
  { op: 'renameuser', entry: 'RenameUser', args: ['2', 'Renamed Two'], input: { id: 2, name: 'Renamed Two' } },
  { op: 'deleteuser', entry: 'DeleteUser', args: ['1'], input: { id: 1 } },
];

describe('E3 — emit write modules + a clean seed DB + {result, state} oracles', () => {
  it('writes /tmp/e1proof/{generated_<write>.rs, write_seed.db, oracles_write.json}', () => {
    mkdirSync(PROOF_DIR, { recursive: true });
    // A clean 3-row seed the harness copies before each write run (a write mutates its copy).
    freshWriteDb(WRITE_SEED).close();

    const oracles: Record<string, unknown> = {};
    for (const wc of WRITE_CASES) {
      const bundle = compileBundle(WRITE_CONTRACT, wc.entry, [], 'sqlite', undefined, RESOLVE);
      const art = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE, undefined, { nativeSql: true });
      writeFileSync(join(PROOF_DIR, `generated_${wc.op}.rs`), art.module.code);
      // The mode-2 oracle runs the write on a FRESH connection over a file-seeded DB, exactly as the
      // rust leg does (rust opens the copied seed file fresh). This matters for a non-RETURNING
      // write's `lastInsertRowid`: on a connection that just ran the seed INSERTs it would report the
      // seed's last id; on a fresh connection (the real per-op case) it reports 0. Seeding on one
      // connection and operating on another makes both legs agree AND reflects real deployment.
      const oraclePath = join(PROOF_DIR, `oracle_${wc.op}.db`);
      freshWriteDb(oraclePath).close();
      const db = new Database(oraclePath);
      db.pragma('foreign_keys = ON');
      const result = executeBundle(bundle, wc.input as never, { db: db as never });
      const state = tableState(db);
      db.close();
      rmSync(oraclePath);
      oracles[wc.op] = { result, state };
    }
    writeFileSync(join(PROOF_DIR, 'oracles_write.json'), JSON.stringify(oracles, null, 2));

    // sanity: the INSERT returns the new row; the DELETE removes id=1.
    expect((oracles.createuser as { result: { id: number }[] }).result[0].email).toBe('zed@example.com');
    expect((oracles.deleteuser as { state: { id: number }[] }).state.some((r) => r.id === 1)).toBe(false);
  });
});
