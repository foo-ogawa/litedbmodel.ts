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
  generateRustCompanion,
  TypedNativeCoverageError,
  deriveTransactionPlan,
  compileWriteNode,
  mysqlPkHint,
  executeTransactionBundle,
} from '../../src/scp/index';
import type { TransactionPlan } from '../../src/scp/makesql/tx';
import type { SqlBundle } from '../../src/scp/runtime';
import { runRelationOp, distributeToParent, type RelationDecl } from '../../src/scp/relation';
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
    return L.Insert({ table: 'benchmark_users', pk: 'id', autoInc: 'id', 'values.email': $.email, 'values.name': $.name, returning: 'id, email, name' });
  }
  /** UPDATE … WHERE … RETURNING — SET value + WHERE-bound head, both typed from the authored ports. */
  RenameUser($: { id: unknown; name: unknown }) {
    return L.Update({ table: 'benchmark_users', pk: 'id', autoInc: 'id', 'set.name': $.name, where: [whereEq(($ as never)['id'], $.id)], returning: 'id, email, name' });
  }
  /** DELETE … WHERE — a NON-returning write: the summary row [{changes, lastInsertRowid}]. */
  DeleteUser($: { id: unknown }) {
    return L.Delete({ table: 'benchmark_users', pk: 'id', autoInc: 'id', where: [whereEq(($ as never)['id'], $.id)] });
  }
  /** E2 (#117) UPSERT — INSERT … ON CONFLICT … DO UPDATE … RETURNING (insert-path AND conflict-path). */
  UpsertUser($: { email: unknown; name: unknown }) {
    return L.Insert({ table: 'benchmark_users', pk: 'id', autoInc: 'id', 'values.email': $.email, 'values.name': $.name, onConflict: 'email', onConflictAction: 'update', returning: 'id, email, name' });
  }
  /** E3 (#118) createMany — ONE json_each batch INSERT for N records (parallel column arrays). */
  CreateMany($: { emails: unknown; names: unknown }) {
    return L.Insert({ table: 'benchmark_users', pk: 'id', autoInc: 'id', batch: 'true', 'values.email': $.emails, 'values.name': $.names, returning: 'id, email, name' });
  }
  /** E2 (#117) upsertMany — batch + onConflict: ONE json_each INSERT … ON CONFLICT … DO UPDATE. */
  UpsertMany($: { emails: unknown; names: unknown }) {
    return L.Insert({ table: 'benchmark_users', pk: 'id', autoInc: 'id', batch: 'true', 'values.email': $.emails, 'values.name': $.names, onConflict: 'email', onConflictAction: 'update', returning: 'id, email, name' });
  }
  /** E3 (#118) updateMany — ONE json_each batch UPDATE keyed by id, setting name for N rows. */
  UpdateMany($: { ids: unknown; names: unknown }) {
    return L.Update({ table: 'benchmark_users', pk: 'id', autoInc: 'id', batch: 'true', 'key.id': $.ids, 'set.name': $.names, returning: 'id, email, name' });
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

/** A COMPOSITE-key relation (E4): tenant_users + each user's posts joined on BOTH (tenant_id, user_id). */
const TENANT_COLUMNS = {
  benchmark_tenant_users: { tenant_id: 'INTEGER', user_id: 'INTEGER', name: 'TEXT' },
  benchmark_tenant_posts: { tenant_id: 'INTEGER', post_id: 'INTEGER', user_id: 'INTEGER', title: 'TEXT' },
} as const;
class TenantFeedReads extends SemanticBehavior {
  static columns = TENANT_COLUMNS;
  UsersWithPosts($: { tenant_id: unknown }) {
    const users = L.Select({ table: 'benchmark_tenant_users', select: ['tenant_id', 'user_id', 'name'], where: [whereEq(($ as never)['tenant_id'], $.tenant_id)], order: 'user_id ASC' });
    const posts = (users as unknown as { map(fn: (u: never) => unknown): unknown }).map(($u: never) =>
      L.Select({ table: 'benchmark_tenant_posts', select: ['tenant_id', 'post_id', 'title'], where: [whereEq(($u as never)['tenant_id'], ($u as never)['tenant_id']), whereEq(($u as never)['user_id'], ($u as never)['user_id'])], order: 'post_id ASC' }),
    );
    return { users, posts };
  }
}

const RESOLVE = schemaColumnTypeResolver(ddl('sqlite'));
const CONTRACT = publishBehaviors(OrmReads);
const WRITE_CONTRACT = publishBehaviors(OrmWrites);
const POST_CONTRACT = publishBehaviors(PostReads);
const POST_RESOLVE = schemaColumnTypeResolver(ddl('sqlite'));
const FEED_CONTRACT = publishBehaviors(FeedReads);
const TENANT_CONTRACT = publishBehaviors(TenantFeedReads);

/** The NATIVE BATCHED relation (E4/#119, the real deliverable): a parent read + a hasMany
 * RelationDecl batch — ONE child query for all parents, NOT the per-element `.map` (N+1). */
class TenantUsersRead extends SemanticBehavior {
  static columns = TENANT_COLUMNS;
  ByTenant($: { tenant_id: unknown }) {
    return L.Select({ table: 'benchmark_tenant_users', select: ['tenant_id', 'user_id', 'name'], where: [whereEq(($ as never)['tenant_id'], $.tenant_id)], order: 'user_id ASC' });
  }
}
const TENANT_USERS_CONTRACT = publishBehaviors(TenantUsersRead);
/** composite-key hasMany: users → posts joined on BOTH (tenant_id, user_id). */
const POSTS_COMPOSITE_REL: RelationDecl = {
  name: 'posts', kind: 'hasMany', targetTable: 'benchmark_tenant_posts',
  select: ['tenant_id', 'post_id', 'user_id', 'title'],
  parentKeys: ['tenant_id', 'user_id'], targetKeys: ['tenant_id', 'user_id'],
  order: 'post_id ASC', dialect: 'sqlite',
} as unknown as RelationDecl;

/** SINGLE-key batched relation (nestedRelations): posts → comments by post_id (one key). */
const SINGLE_REL_COLUMNS = {
  benchmark_posts: { id: 'INTEGER', title: 'TEXT', content: 'TEXT', published: 'INTEGER', author_id: 'INTEGER', created_at: 'TEXT' },
  benchmark_comments: { id: 'INTEGER', body: 'TEXT', post_id: 'INTEGER', created_at: 'TEXT' },
} as const;
class PostsRead extends SemanticBehavior {
  static columns = SINGLE_REL_COLUMNS;
  ByAuthor($: { author_id: unknown }) {
    return L.Select({ table: 'benchmark_posts', select: ['id', 'title', 'author_id'], where: [whereEq(($ as never)['author_id'], $.author_id)], order: 'id ASC' });
  }
}
const POSTS_SINGLE_CONTRACT = publishBehaviors(PostsRead);
const COMMENTS_SINGLE_REL: RelationDecl = {
  name: 'comments', kind: 'hasMany', targetTable: 'benchmark_comments',
  select: ['id', 'body', 'post_id'], parentKey: 'id', targetKey: 'post_id', order: 'id ASC', dialect: 'sqlite',
} as unknown as RelationDecl;

const EXPECTED_SQL = 'SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT ?';

function findUniqueBundle() {
  return compileBundle(CONTRACT, 'FindUnique', [], 'sqlite', undefined, RESOLVE);
}

describe('E1 — bc bakes a full-SQL static port + typed param ports (the empirical answer)', () => {
  it('emits f_sql as a native string literal + f_p0/f_p1 as typed ports — no GeneratorFailure', () => {
    const art = generateCodegenArtifact(findUniqueBundle(), 'rust', REGISTERED, RESOLVE);
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
    const art = generateCodegenArtifact(findUniqueBundle(), 'rust', REGISTERED, RESOLVE);
    const code = stripRustComments(art.module.code);
    const uses = [...code.matchAll(/^\s*(?:use|extern crate)\s+([^;]+);/gm)].map((m) => m[1].trim());
    expect(uses).toEqual(['std::cell::RefCell']);
    for (const marker of ['serde_json', 'behavior_contracts', 'run_behavior', 'RawValue', 'Box<dyn', 'dyn Any', 'Value::']) {
      expect(code).not.toContain(marker);
    }
  });

  it('the companion is RETIRED for reads: the module carries its own query, the companion carries no readGraph', () => {
    const art = generateCodegenArtifact(findUniqueBundle(), 'rust', REGISTERED, RESOLVE);
    // The read SQL is IN the module — the companion no longer ships the read graph (retired).
    expect((art.companion as { readGraph?: unknown }).readGraph).toBeUndefined();
    expect(art.module.code).toContain(EXPECTED_SQL);
  });
});

describe('E2 — IN-list: the array-bound head bakes as a native Vec<ElemT> port (bc#110)', () => {
  it('bakes the json_each SQL + a native Vec<i64> array port fed from the input struct', () => {
    const bundle = compileBundle(CONTRACT, 'ByIds', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE).module.code;
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
    const code = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE).module.code;
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
    const code = generateCodegenArtifact(bundle, 'rust', REGISTERED, POST_RESOLVE).module.code;
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
    const code = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE).module.code;
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

  it('COMPOSITE-key: the child binds TWO parent element fields (tenant_id AND user_id) — no code change needed', () => {
    // The measured answer to "is composite-key a bc gap?": NO. bc bakes N scalar element-field ports
    // (the SAME primitive as single-key, repeated), so the child SQL is a two-column tuple join and
    // both parent keys bake as native i64 element accesses. The single lowering already produces this
    // shape — composite `.map` needed ZERO codegen change.
    const bundle = compileBundle(TENANT_CONTRACT, 'UsersWithPosts', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE).module.code;
    expect(code).toContain('f_sql: "SELECT tenant_id, post_id, title FROM benchmark_tenant_posts WHERE tenant_id = ? AND user_id = ? ORDER BY post_id ASC".to_string()');
    expect(code).toContain('f_p0: oel_n1.tenant_id'); // first parent key — native i64
    expect(code).toContain('f_p1: oel_n1.user_id'); // second parent key — native i64
    expect(code).toContain('pub f_p0: i64');
    expect(code).toContain('pub f_p1: i64');
    expect(stripRustComments(code)).not.toContain('serde_json');
  });
});

describe('E4 (#119) — NATIVE BATCHED relation: ONE child query for all parents (not N+1)', () => {
  it('bakes the BATCHED child SQL (json_each tuple membership), NOT a per-row `= ?`; keys as native ports', () => {
    const bundle = compileBundle(TENANT_USERS_CONTRACT, 'ByTenant', [POSTS_COMPOSITE_REL], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE).module.code;
    // the child SQL is the BATCHED form (one query, json_each over the deduped key tuples) — the
    // batched relation the runtime issues, NOT the per-parent `WHERE tenant_id=? AND user_id=?`.
    expect(code).toContain('WHERE EXISTS (SELECT 1 FROM json_each(?) je WHERE json_extract(je.value, \'$[0]\') = benchmark_tenant_posts.tenant_id AND json_extract(je.value, \'$[1]\') = benchmark_tenant_posts.user_id)');
    expect(code).not.toContain('WHERE tenant_id = ? AND user_id = ?'); // NOT the per-row form
    // the batched map: the handler gets ALL parents at once (a Batch of items)
    expect(code).toContain('items: Vec<PortsNRByTenantRelPosts>');
    // both parent keys bake as native i64 element ports (the seam collects distinct + binds ONE json)
    expect(code).toContain('f_k0: oel_rel_posts.tenant_id');
    expect(code).toContain('f_k1: oel_rel_posts.user_id');
    expect(stripRustComments(code)).not.toContain('serde_json');
  });

  it('SINGLE-key (nestedRelations): bakes the batched `post_id IN (json_each(?))`, one key port', () => {
    const bundle = compileBundle(POSTS_SINGLE_CONTRACT, 'ByAuthor', [COMMENTS_SINGLE_REL], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE).module.code;
    expect(code).toContain('f_sql: "SELECT id, body, post_id FROM benchmark_comments WHERE post_id IN (SELECT value FROM json_each(?)) ORDER BY id ASC".to_string()');
    expect(code).toContain('f_k0: oel_rel_comments.id');
    expect(code).toContain('items: Vec<PortsNRByAuthorRelComments>');
  });
});

describe('the SINGLE lowering — the only path, no opt-in flag', () => {
  it('coalesce (Recent) and skip (ByName) both bake on the single lowering (rust + go + ts)', () => {
    for (const entry of ['Recent', 'ByName']) {
      const bundle = compileBundle(CONTRACT, entry, [], 'sqlite', undefined, RESOLVE);
      for (const lang of ['rust', 'go', 'typescript'] as const) {
        expect(() => generateCodegenArtifact(bundle, lang, REGISTERED, RESOLVE)).not.toThrow();
      }
    }
  });

  it('a bundle with neither a graph nor a statement is refused (nothing to generate)', () => {
    expect(() => generateCodegenArtifact({ dialect: 'sqlite', name: 'Create', optionalHeads: [], relations: {} } as never, 'rust', REGISTERED, RESOLVE)).toThrow(
      /carries no component graph/,
    );
  });
});

describe('E3 — writes bake through the SAME lowering as reads (owner: read/write are one flow)', () => {
  it('Insert bakes RETURNING SQL + value heads typed from values.* ports', () => {
    const b = compileBundle(WRITE_CONTRACT, 'CreateUser', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(b, 'rust', REGISTERED, RESOLVE).module.code;
    expect(code).toContain('f_sql: "INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id, email, name".to_string()');
    expect(code).toContain('f_p0: in_.email.clone()');
    expect(code).toContain('f_p1: in_.name.clone()');
  });

  it('Update bakes SET+WHERE SQL; the WHERE-bound head is typed from the authored where port (no FROM regex)', () => {
    const b = compileBundle(WRITE_CONTRACT, 'RenameUser', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(b, 'rust', REGISTERED, RESOLVE).module.code;
    expect(code).toContain('f_sql: "UPDATE benchmark_users SET name = ? WHERE id = ? RETURNING id, email, name".to_string()');
    expect(code).toContain('f_p0: in_.name.clone()');
    expect(code).toContain('f_p1: in_.id'); // id typed i64 (native, no clone)
  });

  it('Delete (no RETURNING) bakes the summary-row outType [{changes, lastInsertRowid}]', () => {
    const b = compileBundle(WRITE_CONTRACT, 'DeleteUser', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(b, 'rust', REGISTERED, RESOLVE).module.code;
    expect(code).toContain('f_sql: "DELETE FROM benchmark_users WHERE id = ?".to_string()');
    expect(code).toContain('pub changes: i64');
    expect(code).toContain('pub lastInsertRowid: i64');
  });

  it('E2 (#117) upsert bakes INSERT … ON CONFLICT … DO UPDATE … RETURNING through the SAME write path', () => {
    const b = compileBundle(WRITE_CONTRACT, 'UpsertUser', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(b, 'rust', REGISTERED, RESOLVE).module.code;
    // the FULL upsert is baked (single statement), NOT a plain INSERT — insert-path + conflict-path.
    expect(code).toContain('f_sql: "INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id, email, name".to_string()');
    expect(code).toContain('f_p0: in_.email.clone()');
    expect(code).toContain('f_p1: in_.name.clone()');
  });

  it('E3 (#118) createMany bakes ONE json_each batch INSERT + parallel column ARRAY ports (not N stmts)', () => {
    const b = compileBundle(WRITE_CONTRACT, 'CreateMany', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(b, 'rust', REGISTERED, RESOLVE).module.code;
    // ONE json_each statement (records ride the JSON param), NOT a per-row VALUES(?,?) list.
    expect(code).toContain('f_sql: "INSERT INTO benchmark_users (email, name) SELECT json_extract(value, \'$.email\'), json_extract(value, \'$.name\') FROM json_each(?) RETURNING id, email, name".to_string()');
    expect(code).not.toContain('VALUES (?, ?)'); // NOT the multi-VALUES per-row form
    // records as PARALLEL scalar array ports (bc has no Vec<struct>); the seam zips into the JSON.
    expect(code).toContain('pub f_v0: Vec<String>');
    expect(code).toContain('pub f_v1: Vec<String>');
    expect(code).toContain('f_v0: in_.emails.clone()');
    expect(code).toContain('f_v1: in_.names.clone()');
    expect(stripRustComments(code)).not.toContain('serde_json');
  });

  it('E2 (#117) upsertMany bakes the batch json_each with ON CONFLICT (batch + upsert combined)', () => {
    const b = compileBundle(WRITE_CONTRACT, 'UpsertMany', [], 'sqlite', undefined, RESOLVE);
    const code = generateCodegenArtifact(b, 'rust', REGISTERED, RESOLVE).module.code;
    expect(code).toContain('FROM json_each(?) WHERE true ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name');
    expect(code).toContain('pub f_v0: Vec<String>');
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
    const art = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE);
    writeFileSync(join(PROOF_DIR, 'generated_findunique.rs'), art.module.code);
    writeFileSync(join(PROOF_DIR, 'companion_findunique.rs'), generateRustCompanion(bundle, 'generated_findunique', RESOLVE));

    const byIdsBundle = compileBundle(CONTRACT, 'ByIds', [], 'sqlite', undefined, RESOLVE);
    const byIdsArt = generateCodegenArtifact(byIdsBundle, 'rust', REGISTERED, RESOLVE);
    writeFileSync(join(PROOF_DIR, 'generated_byids.rs'), byIdsArt.module.code);
    writeFileSync(join(PROOF_DIR, 'companion_byids.rs'), generateRustCompanion(byIdsBundle, 'generated_byids', RESOLVE));

    // #122: the optional-LIMIT read — its baked `.unwrap_or(20)` must make an ABSENT limit fall back
    // to 20 and a PRESENT limit take effect, both byte-equal to the oracle.
    const recentBundle = compileBundle(CONTRACT, 'Recent', [], 'sqlite', undefined, RESOLVE);
    const recentArt = generateCodegenArtifact(recentBundle, 'rust', REGISTERED, RESOLVE);
    writeFileSync(join(PROOF_DIR, 'generated_recent.rs'), recentArt.module.code);
    writeFileSync(join(PROOF_DIR, 'companion_recent.rs'), generateRustCompanion(recentBundle, 'generated_recent', RESOLVE));

    // skip: the `published = ?` fragment drops when published is absent — present vs absent must
    // both match the mode-2 oracle (the seam assembles the present fragments over baked literals).
    const byMaybeBundle = compileBundle(POST_CONTRACT, 'ByAuthorMaybePublished', [], 'sqlite', undefined, POST_RESOLVE);
    const byMaybeArt = generateCodegenArtifact(byMaybeBundle, 'rust', REGISTERED, POST_RESOLVE);
    writeFileSync(join(PROOF_DIR, 'generated_bymaybe.rs'), byMaybeArt.module.code);
    writeFileSync(join(PROOF_DIR, 'companion_bymaybe.rs'), generateRustCompanion(byMaybeBundle, 'generated_bymaybe', POST_RESOLVE));

    // E4 map relation: posts + per-post author. Output {authors, posts}.
    const feedBundle = compileBundle(FEED_CONTRACT, 'PostsWithAuthor', [], 'sqlite', undefined, RESOLVE);
    const feedArt = generateCodegenArtifact(feedBundle, 'rust', REGISTERED, RESOLVE);
    writeFileSync(join(PROOF_DIR, 'generated_feed.rs'), feedArt.module.code);
    writeFileSync(join(PROOF_DIR, 'companion_feed.rs'), generateRustCompanion(feedBundle, 'generated_feed', RESOLVE));

    // E4 COMPOSITE-key map: users + per-user posts joined on BOTH (tenant_id, user_id).
    const tenantBundle = compileBundle(TENANT_CONTRACT, 'UsersWithPosts', [], 'sqlite', undefined, RESOLVE);
    const tenantArt = generateCodegenArtifact(tenantBundle, 'rust', REGISTERED, RESOLVE);
    writeFileSync(join(PROOF_DIR, 'generated_tenantfeed.rs'), tenantArt.module.code);
    writeFileSync(join(PROOF_DIR, 'companion_tenantfeed.rs'), generateRustCompanion(tenantBundle, 'generated_tenantfeed', RESOLVE));

    // E4 NATIVE BATCHED relation: users + a hasMany composite RelationDecl (ONE child query).
    const relBundle = compileBundle(TENANT_USERS_CONTRACT, 'ByTenant', [POSTS_COMPOSITE_REL], 'sqlite', undefined, RESOLVE);
    const relArt = generateCodegenArtifact(relBundle, 'rust', REGISTERED, RESOLVE);
    writeFileSync(join(PROOF_DIR, 'generated_relbatch.rs'), relArt.module.code);
    writeFileSync(join(PROOF_DIR, 'companion_relbatch.rs'), generateRustCompanion(relBundle, 'generated_relbatch', RESOLVE));

    // E4 NATIVE BATCHED single-key relation (nestedRelations): posts + comments by post_id.
    const relSingleBundle = compileBundle(POSTS_SINGLE_CONTRACT, 'ByAuthor', [COMMENTS_SINGLE_REL], 'sqlite', undefined, RESOLVE);
    const relSingleArt = generateCodegenArtifact(relSingleBundle, 'rust', REGISTERED, RESOLVE);
    writeFileSync(join(PROOF_DIR, 'generated_relsingle.rs'), relSingleArt.module.code);
    writeFileSync(join(PROOF_DIR, 'companion_relsingle.rs'), generateRustCompanion(relSingleBundle, 'generated_relsingle', RESOLVE));

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
    // composite map: tenant 1 (4 users × 2 posts each) and tenant 999 (empty).
    const tenantOracles: Record<string, unknown> = {};
    for (const tid of [1, 999]) tenantOracles[String(tid)] = executeBundle(tenantBundle, { tenant_id: tid } as never, { db: db as never });
    // BATCHED relation oracle: the mode-2 runtime batched path (runRelationOp = ONE query +
    // distributeToParent groups per parent). The native module must byte-match this {rows, posts}.
    const relOracles: Record<string, unknown> = {};
    const relOp = relBundle.relations.posts;
    for (const tid of [1, 2, 999]) {
      const users = executeBundle(relBundle, { tenant_id: tid } as never, { db: db as never }) as Record<string, unknown>[];
      const { batch } = runRelationOp(relOp as never, users as never, db as never);
      const posts = users.map((u) => distributeToParent(relOp as never, u as never, batch as never));
      relOracles[String(tid)] = { rows: users, posts };
    }
    // single-key batched relation oracle (posts + comments).
    const relSingleOracles: Record<string, unknown> = {};
    const relSingleOp = relSingleBundle.relations.comments;
    for (const aid of [1, 7, 999]) {
      const posts = executeBundle(relSingleBundle, { author_id: aid } as never, { db: db as never }) as Record<string, unknown>[];
      const { batch } = runRelationOp(relSingleOp as never, posts as never, db as never);
      const comments = posts.map((p) => distributeToParent(relSingleOp as never, p as never, batch as never));
      relSingleOracles[String(aid)] = { rows: posts, comments };
    }
    db.close();

    writeFileSync(join(PROOF_DIR, 'oracles.json'), JSON.stringify(oracles, null, 2));
    writeFileSync(join(PROOF_DIR, 'oracles_byids.json'), JSON.stringify(byIdsOracles, null, 2));
    writeFileSync(join(PROOF_DIR, 'oracles_recent.json'), JSON.stringify(recentOracles, null, 2));
    writeFileSync(join(PROOF_DIR, 'oracles_bymaybe.json'), JSON.stringify(byMaybeOracles, null, 2));
    writeFileSync(join(PROOF_DIR, 'oracles_feed.json'), JSON.stringify(feedOracles, null, 2));
    writeFileSync(join(PROOF_DIR, 'oracles_tenantfeed.json'), JSON.stringify(tenantOracles, null, 2));
    writeFileSync(join(PROOF_DIR, 'oracles_relbatch.json'), JSON.stringify(relOracles, null, 2));
    writeFileSync(join(PROOF_DIR, 'oracles_relsingle.json'), JSON.stringify(relSingleOracles, null, 2));

    // Read cases for the LIVE native-vs-mode-2 harness (SSoT for read inputs): each carries the native
    // CLI op+args AND the typed mode-2 input scope, so the harness runs BOTH paths on the SAME live DB
    // and deep-equals (no sqlite oracle). relbatch/relsingle are omitted — their native {rows,posts}
    // batched-map de-box shape has no distinct interpreter entry (see report / HANDOFF).
    const readCases = [
      { key: 'findunique', op: 'findunique', args: ['user500@example.com'], input: { email: 'user500@example.com' } },
      { key: 'findunique-miss', op: 'findunique', args: ['nobody@example.com'], input: { email: 'nobody@example.com' } },
      { key: 'byids', op: 'byids', args: ['1,2,3'], input: { ids: [1, 2, 3] } },
      { key: 'byids-empty', op: 'byids', args: [''], input: { ids: [] } },
      { key: 'recent-default', op: 'recent', args: [''], input: {} },
      { key: 'recent-3', op: 'recent', args: ['3'], input: { limit: 3 } },
      { key: 'bymaybe-absent', op: 'bymaybe', args: ['7', ''], input: { author_id: 7 } },
      { key: 'bymaybe-present', op: 'bymaybe', args: ['7', '1'], input: { author_id: 7, published: 1 } },
      { key: 'feed', op: 'feed', args: ['7'], input: { author_id: 7 } },
      { key: 'tenantfeed', op: 'tenantfeed', args: ['1'], input: { tenant_id: 1 } },
    ];
    writeFileSync(join(PROOF_DIR, 'cases_read.json'), JSON.stringify(readCases, null, 2));

    // Batched-relation read cases: native emits {rows,posts}; the mode-2 leg runs the hydrated
    // read_bundle_pooled path; the harness normalizes BOTH to {parents, per-parent children} and
    // deep-equals (the stitch is the shared op-independent SSoT; the DISTINCT check is parent+child
    // de-box). `rel` is the relation field name the hydrated envelope carries per parent.
    const relCases = [
      { key: 'relbatch', op: 'relbatch', args: ['1'], input: { tenant_id: 1 }, rel: 'posts' },
      { key: 'relbatch-2', op: 'relbatch', args: ['2'], input: { tenant_id: 2 }, rel: 'posts' },
      { key: 'relbatch-empty', op: 'relbatch', args: ['999'], input: { tenant_id: 999 }, rel: 'posts' },
      { key: 'relsingle', op: 'relsingle', args: ['7'], input: { author_id: 7 }, rel: 'comments' },
      { key: 'relsingle-1', op: 'relsingle', args: ['1'], input: { author_id: 1 }, rel: 'comments' },
      { key: 'relsingle-empty', op: 'relsingle', args: ['999'], input: { author_id: 999 }, rel: 'comments' },
    ];
    writeFileSync(join(PROOF_DIR, 'cases_rel.json'), JSON.stringify(relCases, null, 2));
    expect(((tenantOracles['1'] as { users: unknown[] }).users).length).toBe(4);
    expect(((tenantOracles['999'] as { users: unknown[] }).users).length).toBe(0);
    // the batched relation stitched: tenant 1 → 4 users, each with their own 2 posts.
    expect(((relOracles['1'] as { posts: unknown[][] }).posts).length).toBe(4);
    expect(((relOracles['1'] as { posts: unknown[][] }).posts).every((p) => p.length === 2)).toBe(true);
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

/** 10 records for the createMany batch (no comma in values — the harness passes them CSV). */
const BATCH_EMAILS = Array.from({ length: 10 }, (_, i) => `batch${i}@example.com`);
const BATCH_NAMES = Array.from({ length: 10 }, (_, i) => `Batch ${i}`);

// `key` names the oracle case (distinct per input); `op` names the rust dispatch + generated module
// (shared across a module's cases — upsert reuses ONE module for both its insert-path + conflict-path).
interface WriteCase { key: string; op: string; entry: keyof OrmWrites & string; args: string[]; input: Record<string, unknown>; }
const WRITE_CASES: WriteCase[] = [
  { key: 'createuser', op: 'createuser', entry: 'CreateUser', args: ['zed@example.com', 'Zed'], input: { email: 'zed@example.com', name: 'Zed' } },
  { key: 'renameuser', op: 'renameuser', entry: 'RenameUser', args: ['2', 'Renamed Two'], input: { id: 2, name: 'Renamed Two' } },
  { key: 'deleteuser', op: 'deleteuser', entry: 'DeleteUser', args: ['1'], input: { id: 1 } },
  // E2 upsert — the SAME module, two paths: INSERT (new email → id 4) and CONFLICT (user1 exists → updates name).
  { key: 'upsert_insert', op: 'upsert', entry: 'UpsertUser', args: ['zed@example.com', 'Zed'], input: { email: 'zed@example.com', name: 'Zed' } },
  { key: 'upsert_conflict', op: 'upsert', entry: 'UpsertUser', args: ['user1@example.com', 'Renamed One'], input: { email: 'user1@example.com', name: 'Renamed One' } },
  // E3 createMany — 10 NEW records in ONE json_each statement (no conflict with the 3-row seed).
  {
    key: 'createmany', op: 'createmany', entry: 'CreateMany',
    args: [BATCH_EMAILS.join(','), BATCH_NAMES.join(',')],
    input: { emails: BATCH_EMAILS, names: BATCH_NAMES },
  },
  // E2 upsertMany — batch + onConflict: user1/user2 EXIST → updated; newX NEW → inserted. ONE statement.
  {
    key: 'upsertmany', op: 'upsertmany', entry: 'UpsertMany',
    args: ['user1@example.com,user2@example.com,newX@example.com', 'Up One,Up Two,New X'],
    input: { emails: ['user1@example.com', 'user2@example.com', 'newX@example.com'], names: ['Up One', 'Up Two', 'New X'] },
  },
  // E3 updateMany — set names for ids 1,2,3 in ONE json_each batch UPDATE.
  {
    key: 'updatemany', op: 'updatemany', entry: 'UpdateMany',
    args: ['1,2,3', 'Upd One,Upd Two,Upd Three'],
    input: { ids: [1, 2, 3], names: ['Upd One', 'Upd Two', 'Upd Three'] },
  },
];

describe('E3 — emit write modules + a clean seed DB + {result, state} oracles', () => {
  it('writes /tmp/e1proof/{generated_<write>.rs, write_seed.db, oracles_write.json}', () => {
    mkdirSync(PROOF_DIR, { recursive: true });
    // A clean 3-row seed the harness copies before each write run (a write mutates its copy).
    freshWriteDb(WRITE_SEED).close();

    const oracles: Record<string, { result: unknown; state: unknown; op: string; args: string[] }> = {};
    const emittedModules = new Set<string>();
    for (const wc of WRITE_CASES) {
      const bundle = compileBundle(WRITE_CONTRACT, wc.entry, [], 'sqlite', undefined, RESOLVE);
      if (!emittedModules.has(wc.op)) {
        const art = generateCodegenArtifact(bundle, 'rust', REGISTERED, RESOLVE);
        writeFileSync(join(PROOF_DIR, `generated_${wc.op}.rs`), art.module.code);
        writeFileSync(join(PROOF_DIR, `companion_${wc.op}.rs`), generateRustCompanion(bundle, `generated_${wc.op}`, RESOLVE));
        emittedModules.add(wc.op);
      }
      // The mode-2 oracle runs the write on a FRESH connection over a file-seeded DB, exactly as the
      // rust leg does (rust opens the copied seed file fresh). This matters for a non-RETURNING
      // write's `lastInsertRowid`: on a connection that just ran the seed INSERTs it would report the
      // seed's last id; on a fresh connection (the real per-op case) it reports 0. Seeding on one
      // connection and operating on another makes both legs agree AND reflects real deployment.
      const oraclePath = join(PROOF_DIR, `oracle_${wc.key}.db`);
      freshWriteDb(oraclePath).close();
      const db = new Database(oraclePath);
      db.pragma('foreign_keys = ON');
      const result = executeBundle(bundle, wc.input as never, { db: db as never });
      const state = tableState(db);
      db.close();
      rmSync(oraclePath);
      // Carry the rust op + args (native CLI dispatch) AND the typed input Scope (the mode-2 interpreter
      // oracle's input for `execute_bundle`) so the livedb harness can run BOTH paths on the same DB.
      oracles[wc.key] = { result, state, op: wc.op, args: wc.args, input: wc.input };
    }
    writeFileSync(join(PROOF_DIR, 'oracles_write.json'), JSON.stringify(oracles, null, 2));

    // sanity: INSERT returns the new row; DELETE removes id=1; upsert-conflict UPDATES user1's name.
    expect((oracles.createuser.result as { id: number }[])[0].email).toBe('zed@example.com');
    expect((oracles.deleteuser.state as { id: number }[]).some((r) => r.id === 1)).toBe(false);
    expect((oracles.upsert_conflict.result as { id: number; name: string }[])[0]).toMatchObject({ id: 1, name: 'Renamed One' });
    expect((oracles.upsert_conflict.state as { id: number }[]).length).toBe(3); // conflict updated, no new row
    expect((oracles.upsert_insert.state as { id: number }[]).length).toBe(4); // insert added a row
  });
});

// ═══════════════════════════════════════════════════════════════════════════════════════════
// E5 (#120) — multi-statement RETURNING-chained TRANSACTIONS bake as ONE native componentRef chain.
//
// A composite/nested Command derives (tx.ts `deriveTransactionPlan`) to a `TransactionPlan` — the SoT
// for the statement ORDER + the RETURNING→next wiring. `lowerTransactionForNativeChain` RE-EXPRESSES
// that plan as one bc componentRef chain: each statement a node with its baked `f_sql` + typed ports;
// a later statement's `$.ref.<name>.<field>` param bakes as a NATIVE read of the producer node's
// RETURNING row (`cell_<producer>.<field>`). The generated runner chains the statements; the ONE
// generic exec seam wraps it in the BEGIN…COMMIT / ROLLBACK envelope.
//
// The 4 ops (delete / nestedCreate / nestedUpdate / nestedUpsert) + a ROLLBACK control are built here
// from the SAME `deriveTransactionPlan` + `compileWriteNode` the runtime composite path uses, emitted
// as native modules, and their execution is proven byte-equal to the mode-2 `executeTransactionBundle`
// oracle (result + resulting DB state) by the out-of-process rust legs.
// ═══════════════════════════════════════════════════════════════════════════════════════════

/** Compile a structured write node → its `TxOp` (the shared write compiler, with native `writeMeta`),
 * for a given dialect. On mysql the RETURNING pk-hint is baked (the SAME `mysqlPkHint` SSoT the
 * production `compileSaveBundle` uses) so the driver emulation re-selects the written row by its real key. */
const txNode = (id: string, component: string, ports: Record<string, unknown>, dialect: 'sqlite' | 'postgres' | 'mysql' = 'sqlite') => {
  const op = compileWriteNode({ id, component, ports } as never, dialect);
  return dialect === 'mysql' ? mysqlPkHint(op) : op;
};

/** Build a tx `SqlBundle` (transaction plan, no single-statement readGraph) — the composite shape. */
function txBundleOf(name: string, plan: TransactionPlan, dialect: 'sqlite' | 'postgres' | 'mysql' = 'sqlite'): SqlBundle {
  const first = plan.statements[0].op;
  return { dialect, name, statement: { sql: first.sql, params: first.params }, optionalHeads: [], relations: {}, transaction: plan } as unknown as SqlBundle;
}

/** The 5 E5 transaction plans (4 ops + a rollback control), each a gate-free RETURNING-chained body,
 * compiled for `dialect` (the SAME per-dialect SQL the production write path emits — the upsert renders
 * `ON DUPLICATE KEY UPDATE` on mysql vs `ON CONFLICT` on pg/sqlite, and mysql RETURNING statements
 * carry the pk-hint). Defaults to sqlite for the in-proc E5 unit assertions. */
function txPlans(dialect: 'sqlite' | 'postgres' | 'mysql' = 'sqlite'): Record<string, { plan: TransactionPlan; module: string }> {
  const N = (id: string, component: string, ports: Record<string, unknown>) => txNode(id, component, ports, dialect);
  return {
    // delete: INSERT user RETURNING id → DELETE that user by the RETURNING id (chained).
    txdelete: {
      module: 'TxDelete',
      plan: deriveTransactionPlan('create', [
        { op: N('u', 'Insert', { table: 'benchmark_users', 'values.email': { ref: ['email'] }, 'values.name': { ref: ['name'] }, returning: 'id' }), label: 'Insert user', name: 'user', effects: {} },
        { op: N('d', 'Delete', { table: 'benchmark_users', where: { arr: [{ eq: [{ ref: ['id'] }, { ref: ['user', 'id'] }] }] } }), label: 'Delete user', name: 'deleted', effects: {} },
      ], { effects: {} }, dialect),
    },
    // nestedCreate: INSERT user RETURNING id → INSERT post whose author_id IS the RETURNING id.
    txnestedcreate: {
      module: 'TxNestedCreate',
      plan: deriveTransactionPlan('create', [
        { op: N('u', 'Insert', { table: 'benchmark_users', 'values.email': { ref: ['email'] }, 'values.name': { ref: ['name'] }, returning: 'id' }), label: 'Insert user', name: 'user', effects: {} },
        { op: N('p', 'Insert', { table: 'benchmark_posts', 'values.author_id': { ref: ['user', 'id'] }, 'values.title': { ref: ['title'] }, returning: 'id, author_id, title' }), label: 'Insert post', name: 'post', effects: {} },
      ], { effects: {} }, dialect),
    },
    // nestedUpdate: UPDATE user + UPDATE post (two writes, one atomic tx; keyed by the same input).
    txnestedupdate: {
      module: 'TxNestedUpdate',
      plan: deriveTransactionPlan('update', [
        { op: N('u', 'Update', { table: 'benchmark_users', 'set.name': { ref: ['name'] }, where: { arr: [{ eq: [{ ref: ['id'] }, { ref: ['user_id'] }] }] }, returning: 'id, name' }), label: 'Update user', name: 'user', effects: {} },
        { op: N('p', 'Update', { table: 'benchmark_posts', 'set.title': { ref: ['title'] }, where: { arr: [{ eq: [{ ref: ['author_id'] }, { ref: ['user_id'] }] }] }, returning: 'id, title' }), label: 'Update post', name: 'post', effects: {} },
      ], { effects: {} }, dialect),
    },
    // nestedUpsert: upsert user ON CONFLICT(email) RETURNING id → INSERT post using the RETURNING id.
    // The RETURNING id is the SAME whether the upsert took the INSERT path (new email) or the UPDATE
    // path (existing email) — so the chained post.author_id is correct on both. The pk descriptor lets
    // the mysql RETURNING emulation re-select the upserted row by its conflict key (email).
    txnestedupsert: {
      module: 'TxNestedUpsert',
      plan: deriveTransactionPlan('create', [
        { op: N('u', 'Insert', { table: 'benchmark_users', pk: 'id', autoInc: 'id', 'values.email': { ref: ['email'] }, 'values.name': { ref: ['name'] }, onConflict: 'email', onConflictAction: 'update', returning: 'id' }), label: 'Upsert user', name: 'user', effects: {} },
        { op: N('p', 'Insert', { table: 'benchmark_posts', 'values.author_id': { ref: ['user', 'id'] }, 'values.title': { ref: ['title'] }, returning: 'id, author_id, title' }), label: 'Insert post', name: 'post', effects: {} },
      ], { effects: {} }, dialect),
    },
    // ROLLBACK control: INSERT fresh user RETURNING id → INSERT a user whose email COLLIDES with a
    // seeded row (UNIQUE(email)). Statement 2 fails → the whole tx ROLLS BACK → statement 1's insert is
    // undone → the users table is left UNCHANGED. Proves the seam's BEGIN…ROLLBACK atomicity envelope.
    txrollback: {
      module: 'TxRollback',
      plan: deriveTransactionPlan('create', [
        { op: N('u', 'Insert', { table: 'benchmark_users', 'values.email': { ref: ['email'] }, 'values.name': { ref: ['name'] }, returning: 'id' }), label: 'Insert user', name: 'user', effects: {} },
        { op: N('d', 'Insert', { table: 'benchmark_users', 'values.email': { ref: ['dup_email'] }, 'values.name': { ref: ['name'] }, returning: 'id' }), label: 'Insert dup', name: 'dup', effects: {} },
      ], { effects: {} }, dialect),
    },
  };
}

describe('E5 (#120) — the tx-chain lowering bakes each statement + the RETURNING→next wire natively', () => {
  it('nestedCreate: both statements baked as native SQL literals; post.author_id reads the user RETURNING id', () => {
    const { txnestedcreate } = txPlans();
    const code = generateCodegenArtifact(txBundleOf(txnestedcreate.module, txnestedcreate.plan), 'rust', REGISTERED, RESOLVE).module.code;
    // Statement 1 (INSERT user RETURNING id) baked as a native literal.
    expect(code).toContain('f_sql: "INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id".to_string()');
    // Statement 2 (INSERT post) baked as a native literal.
    expect(code).toContain('f_sql: "INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?) RETURNING id, author_id, title".to_string()');
    // THE WIRE: statement 2's author_id port is a NATIVE read of statement 1's RETURNING row — not an
    // input, not a boxed lookup. `cell_tx_body_0.borrow().id` is the chained id (i64, from the outType).
    expect(code).toContain('f_p0: cell_tx_body_0.borrow().id');
    // statement 1's input ports are native scalars from the schema.
    expect(code).toContain('f_p0: in_.email.clone()');
    expect(stripRustComments(code)).not.toContain('serde_json');
  });

  it('delete: the DELETE binds the RETURNING id natively; its non-returning row is the summary', () => {
    const { txdelete } = txPlans();
    const code = generateCodegenArtifact(txBundleOf(txdelete.module, txdelete.plan), 'rust', REGISTERED, RESOLVE).module.code;
    expect(code).toContain('f_sql: "INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id".to_string()');
    expect(code).toContain('f_sql: "DELETE FROM benchmark_users WHERE id = ?".to_string()');
    expect(code).toContain('f_p0: cell_tx_body_0.borrow().id'); // DELETE targets the inserted id
    expect(code).toContain('pub changes: i64'); // the non-returning DELETE's produced row is the summary
  });

  it('nestedUpsert: the ON CONFLICT upsert RETURNING id chains into the post insert', () => {
    const { txnestedupsert } = txPlans();
    const code = generateCodegenArtifact(txBundleOf(txnestedupsert.module, txnestedupsert.plan), 'rust', REGISTERED, RESOLVE).module.code;
    expect(code).toContain('ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id');
    expect(code).toContain('f_p0: cell_tx_body_0.borrow().id');
  });

  it('every generated tx module is runtime-free (std only, no bc runtime / JSON / boxed Value)', () => {
    for (const { module, plan } of Object.values(txPlans())) {
      const code = stripRustComments(generateCodegenArtifact(txBundleOf(module, plan), 'rust', REGISTERED, RESOLVE).module.code);
      const uses = [...code.matchAll(/^\s*(?:use|extern crate)\s+([^;]+);/gm)].map((m) => m[1].trim());
      expect(uses).toEqual(['std::cell::RefCell']);
      for (const marker of ['serde_json', 'behavior_contracts', 'run_behavior', 'RawValue', 'Box<dyn', 'dyn Any', 'Value::']) {
        expect(code).not.toContain(marker);
      }
    }
  });

  it('NEGATIVE control — the RETURNING wire is load-bearing: mis-wiring it bakes a DIFFERENT port AND diverges at runtime', () => {
    // A mutant nestedCreate whose post.author_id binds an INPUT head (`{ref:['author_id']}`) instead of
    // the producer's RETURNING id (`{ref:['user','id']}`). If the wire were ignored, this would bake +
    // execute identically to the correct op — proving the green is NOT a tautology, it must NOT.
    const mutant = deriveTransactionPlan('create', [
      { op: txNode('u', 'Insert', { table: 'benchmark_users', 'values.email': { ref: ['email'] }, 'values.name': { ref: ['name'] }, returning: 'id' }), label: 'Insert user', name: 'user', effects: {} },
      { op: txNode('p', 'Insert', { table: 'benchmark_posts', 'values.author_id': { ref: ['author_id'] }, 'values.title': { ref: ['title'] }, returning: 'id, author_id, title' }), label: 'Insert post', name: 'post', effects: {} },
    ], { effects: {} });
    const mutantCode = generateCodegenArtifact(txBundleOf('TxMutant', mutant), 'rust', REGISTERED, RESOLVE).module.code;
    // STATIC: the mis-wired port bakes as an INPUT read, NOT the producer cell — the two wirings bake
    // demonstrably different native code (so a codegen regression that dropped the wire changes the bytes).
    expect(mutantCode).toContain('f_p0: in_.author_id');
    expect(mutantCode).not.toContain('f_p0: cell_tx_body_0.borrow().id');

    // DYNAMIC: run the mode-2 oracle for BOTH the correct op and the mutant against the SAME fresh seed.
    // The correct op's post.author_id is the NEW user's id (4); the mutant's is the raw input (999). They
    // DIFFER — so the byte-equal leg would FAIL a mis-wired native module (the comparison discriminates).
    const { txnestedcreate } = txPlans();
    const runState = (plan: TransactionPlan, input: Record<string, unknown>) => {
      const p = join(PROOF_DIR, 'tx_neg.db');
      freshTxDb(p).close();
      const db = new Database(p);
      db.pragma('foreign_keys = ON');
      executeTransactionBundle(txBundleOf('N', plan), input as never, { db: db as never });
      const posts = db.prepare('SELECT author_id FROM benchmark_posts ORDER BY id').all() as { author_id: number }[];
      db.close();
      rmSync(p);
      return posts[posts.length - 1].author_id;
    };
    const correct = runState(txnestedcreate.plan, { email: 'zed@example.com', name: 'Zed', title: 'T' });
    const broken = runState(mutant, { email: 'zed@example.com', name: 'Zed', title: 'T', author_id: 999 });
    expect(correct).toBe(4); // the chained RETURNING id
    expect(broken).toBe(999); // the mis-wired input value
    expect(correct).not.toBe(broken); // the wire genuinely determines the row — green is not a tautology
  });

  it('a GATE statement fails closed (the native chain covers gate-free RETURNING chains only)', () => {
    // A composite carrying a requires-gate short-circuits with {committed:false, shortCircuit} — a shape
    // the native struct chain does not model. The lowering must REFUSE it (never bake an always-commit
    // chain), naming the gap.
    const gated = deriveTransactionPlan('create', [
      { op: txNode('u', 'Insert', { table: 'benchmark_users', 'values.email': { ref: ['email'] }, 'values.name': { ref: ['name'] }, returning: 'id' }), label: 'Insert user', name: 'user', effects: { requires: [{ kind: 'requires' as const, table: 'benchmark_users', keys: { id: '$.input.gate_id' } }] } },
      { op: txNode('p', 'Insert', { table: 'benchmark_posts', 'values.author_id': { ref: ['user', 'id'] }, 'values.title': { ref: ['title'] }, returning: 'id' }), label: 'Insert post', name: 'post', effects: {} },
    ], { effects: {} });
    expect(() => generateCodegenArtifact(txBundleOf('TxGated', gated), 'rust', REGISTERED, RESOLVE)).toThrow(/gate\/non-body statement/);
  });
});

// ── the E5 exec proof: emit the tx modules + a users+posts seed + the executeTransactionBundle oracle ──
/** A fresh users+posts seed (SMALL — legible full-state comparison). */
function freshTxDb(path?: string): InstanceType<typeof Database> {
  if (path !== undefined && existsSync(path)) rmSync(path);
  const db = new Database(path ?? ':memory:');
  db.pragma('foreign_keys = ON');
  for (const s of ddl('sqlite')) db.exec(s);
  for (let id = 1; id <= 3; id++) db.prepare('INSERT INTO benchmark_users (id, email, name) VALUES (?, ?, ?)').run(id, `user${id}@example.com`, `User ${id}`);
  // A post owned by user 2 (nestedUpdate retitles it; the others leave it untouched).
  db.prepare('INSERT INTO benchmark_posts (id, title, author_id) VALUES (?, ?, ?)').run(1, 'Post B', 2);
  return db;
}
function txState(db: InstanceType<typeof Database>): unknown {
  return {
    users: db.prepare('SELECT id, email, name FROM benchmark_users ORDER BY id').all(),
    posts: db.prepare('SELECT id, title, author_id FROM benchmark_posts ORDER BY id').all(),
  };
}
const TX_SEED = join(PROOF_DIR, 'tx_seed.db');

interface TxCase { key: string; module: string; op: string; args: string[]; input: Record<string, unknown> }
const TX_CASES: TxCase[] = [
  { key: 'txdelete', module: 'txdelete', op: 'txdelete', args: ['zed@example.com', 'Zed'], input: { email: 'zed@example.com', name: 'Zed' } },
  { key: 'txnestedcreate', module: 'txnestedcreate', op: 'txnestedcreate', args: ['zed@example.com', 'Zed', 'Hello World'], input: { email: 'zed@example.com', name: 'Zed', title: 'Hello World' } },
  { key: 'txnestedupdate', module: 'txnestedupdate', op: 'txnestedupdate', args: ['2', 'User Two Renamed', 'Post B Renamed'], input: { user_id: 2, name: 'User Two Renamed', title: 'Post B Renamed' } },
  // upsert INSERT path (fresh email → new user id 4 → post author_id 4).
  { key: 'txnestedupsert_insert', module: 'txnestedupsert', op: 'txnestedupsert', args: ['zed@example.com', 'Zed', 'Zed Post'], input: { email: 'zed@example.com', name: 'Zed', title: 'Zed Post' } },
  // upsert CONFLICT path (existing user1 email → UPDATE returns id 1 → post author_id 1).
  { key: 'txnestedupsert_conflict', module: 'txnestedupsert', op: 'txnestedupsert', args: ['user1@example.com', 'User One Up', 'One Post'], input: { email: 'user1@example.com', name: 'User One Up', title: 'One Post' } },
  // ROLLBACK control: stmt 2 collides on user1's email → the tx rolls back → state == seed, committed false.
  { key: 'txrollback', module: 'txrollback', op: 'txrollback', args: ['fresh@example.com', 'user1@example.com', 'Fresh'], input: { email: 'fresh@example.com', dup_email: 'user1@example.com', name: 'Fresh' } },
];

describe('E5 (#120) — emit tx modules + a users+posts seed + {result, state} executeTransactionBundle oracles', () => {
  it('writes /tmp/e1proof/{generated_tx*.rs, tx_seed.db, oracles_tx.json}', () => {
    mkdirSync(PROOF_DIR, { recursive: true });
    freshTxDb(TX_SEED).close();

    const plans = txPlans();
    const emitted = new Set<string>();
    const oracles: Record<string, { result: unknown; state: unknown; op: string; args: string[] }> = {};

    for (const tc of TX_CASES) {
      const { module, plan } = plans[tc.module];
      if (!emitted.has(tc.op)) {
        const art = generateCodegenArtifact(txBundleOf(module, plan), 'rust', REGISTERED, RESOLVE);
        writeFileSync(join(PROOF_DIR, `generated_${tc.op}.rs`), art.module.code);
        writeFileSync(join(PROOF_DIR, `companion_${tc.op}.rs`), generateRustCompanion(txBundleOf(module, plan), `generated_${tc.op}`, RESOLVE));
        emitted.add(tc.op);
      }
      // The mode-2 oracle runs the tx on a FRESH file-seeded DB (the rust leg opens the copied seed
      // fresh) — same as the write oracle. executeTransactionBundle THROWS on a statement failure (it
      // ROLLBACKs then rethrows); the rollback control catches that → {committed:false} + the (undone) state.
      const oraclePath = join(PROOF_DIR, `tx_oracle_${tc.key}.db`);
      freshTxDb(oraclePath).close();
      const db = new Database(oraclePath);
      db.pragma('foreign_keys = ON');
      let committed: boolean;
      try {
        const r = executeTransactionBundle(txBundleOf(module, plan), tc.input as never, { db: db as never }) as { committed: boolean };
        committed = r.committed;
      } catch {
        committed = false; // a statement failed → executeTransaction rolled back and rethrew
      }
      const state = txState(db);
      db.close();
      rmSync(oraclePath);
      oracles[tc.key] = { result: { committed }, state, op: tc.op, args: tc.args, input: tc.input };
    }
    writeFileSync(join(PROOF_DIR, 'oracles_tx.json'), JSON.stringify(oracles, null, 2));

    // Sanity — the oracle state proves the CHAIN is real (not trivially both-empty):
    // nestedCreate: a new user (id 4) + a post whose author_id IS that chained id 4.
    const nc = oracles.txnestedcreate.state as { users: { id: number }[]; posts: { author_id: number }[] };
    expect(nc.users.map((u) => u.id)).toEqual([1, 2, 3, 4]);
    expect(nc.posts.find((p) => p.author_id === 4)).toBeDefined();
    expect(oracles.txnestedcreate.result).toEqual({ committed: true });
    // delete: user inserted (id 4) then deleted by that id → users back to the seed (chain targeted id 4).
    expect((oracles.txdelete.state as { users: unknown[] }).users.length).toBe(3);
    // upsert conflict: user1 updated (no new row), post author_id 1 (the UPDATE-path RETURNING id).
    const uc = oracles.txnestedupsert_conflict.state as { users: { id: number; name: string }[]; posts: { author_id: number }[] };
    expect(uc.users.length).toBe(3);
    expect(uc.users.find((u) => u.id === 1)!.name).toBe('User One Up');
    expect(uc.posts.some((p) => p.author_id === 1)).toBe(true);
    // ROLLBACK: committed false AND the users table is UNCHANGED (statement 1's insert was undone).
    expect(oracles.txrollback.result).toEqual({ committed: false });
    expect((oracles.txrollback.state as { users: unknown[] }).users.length).toBe(3);
  });
});

// ── LIVE-DB dialect emission (epic #123/#124 commit 2/3) — emit pg + mysql-dialect modules + companions
//    so the rust cell can run the SAME 20 ops against docker Postgres (:5433) / MySQL (:3307) through
//    litedbmodel_runtime's PostgresDriver/MysqlDriver and compare byte-for-byte to the SAME
//    dialect-independent mode-2 oracle. Reuses the SAME bundle constructors (contracts / WRITE_CONTRACT /
//    txPlans) with the dialect param — one source, per-dialect baked SQL ($N / json_each-vs-ANY / RETURNING).
describe('LIVE — emit pg + mysql dialect modules + companions for the docker byte-equal legs', () => {
  const READ_SPECS: Array<[string, ReturnType<typeof publishBehaviors>, string, RelationDecl[], typeof RESOLVE]> = [
    ['findunique', CONTRACT, 'FindUnique', [], RESOLVE],
    ['byids', CONTRACT, 'ByIds', [], RESOLVE],
    ['recent', CONTRACT, 'Recent', [], RESOLVE],
    ['bymaybe', POST_CONTRACT, 'ByAuthorMaybePublished', [], POST_RESOLVE],
    ['feed', FEED_CONTRACT, 'PostsWithAuthor', [], RESOLVE],
    ['tenantfeed', TENANT_CONTRACT, 'UsersWithPosts', [], RESOLVE],
    ['relbatch', TENANT_USERS_CONTRACT, 'ByTenant', [POSTS_COMPOSITE_REL], RESOLVE],
    ['relsingle', POSTS_SINGLE_CONTRACT, 'ByAuthor', [COMMENTS_SINGLE_REL], RESOLVE],
  ];
  const WRITE_SPECS: Array<[string, string]> = [
    ['createuser', 'CreateUser'], ['renameuser', 'RenameUser'], ['deleteuser', 'DeleteUser'],
    ['upsert', 'UpsertUser'], ['createmany', 'CreateMany'], ['upsertmany', 'UpsertMany'], ['updatemany', 'UpdateMany'],
  ];

  for (const dialect of ['postgres', 'mysql'] as const) {
    it(`writes /tmp/e1proof/${dialect}/{generated_*.rs, companion_*.rs} for all 20 ops`, () => {
      const dir = join(PROOF_DIR, dialect);
      mkdirSync(dir, { recursive: true });
      const emit = (op: string, bundle: SqlBundle, resolve: typeof RESOLVE) => {
        writeFileSync(join(dir, `generated_${op}.rs`), generateCodegenArtifact(bundle, 'rust', REGISTERED, resolve).module.code);
        writeFileSync(join(dir, `companion_${op}.rs`), generateRustCompanion(bundle, `generated_${op}`, resolve));
        // The SAME per-dialect bundle the mode-2 INTERPRETER oracle (`mode2` subcommand) runs — so the
        // livedb leg compares native (codegen) vs mode-2 (interpreter) on the SAME live DB, not vs the
        // sqlite oracle. JSON-serialized (readGraph IR + transaction plan) for `Node::parse` rust-side.
        writeFileSync(join(dir, `bundle_${op}.json`), JSON.stringify(bundle));
      };
      for (const [op, contract, entry, rels, resolve] of READ_SPECS) {
        // The batched-relation SQL is compiled per the RelationDecl's own dialect (json_each vs
        // `= ANY`/UNNEST) — override the fixture's 'sqlite' to THIS dialect so pg/mysql get their form.
        const dialectRels = rels.map((r) => ({ ...r, dialect })) as RelationDecl[];
        emit(op, compileBundle(contract, entry, dialectRels, dialect, undefined, resolve), resolve);
      }
      for (const [op, entry] of WRITE_SPECS) {
        emit(op, compileBundle(WRITE_CONTRACT, entry, [], dialect, undefined, RESOLVE), RESOLVE);
      }
      const plans = txPlans(dialect);
      for (const op of Object.keys(plans)) {
        emit(op, txBundleOf(plans[op].module, plans[op].plan, dialect), RESOLVE);
      }
    });
  }
});
