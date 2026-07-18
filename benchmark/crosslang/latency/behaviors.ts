// ════════════════════════════════════════════════════════════════════════════
// Latency-bench SHARED behaviors — the 4 representative ops, declared ONCE.
// ════════════════════════════════════════════════════════════════════════════
//
// The payoff bench (ts-IR-interpreter vs rust-native vs go-native) runs the SAME 4 ops, over the SAME
// seed sqlite, in every cell. They are declared here on the litedbmodel public authoring API and
// compiled to SqlBundles; from ONE bundle per op:
//   • the go cell   ← `generateCodegenArtifact(bundle,'go')`   (go-typed-native module)
//   • the rust cell ← the SAME bundle's rust-typed-native module (already proven in rust/e1_native_proof)
//   • the ts-IR cell← the SAME bundle run through the litedbmodel INTERPRETER (executeBundle/readBundle)
// so the three cells do byte-identical logical work — only the execution surface differs.
//
// The 4 ops cover the shape spectrum the coordinator asked for:
//   findUnique — a point read (single componentRef)
//   relComments — a batched relation (parent posts + ONE batched IN child query, N+1 avoided)
//   createUser — a single write (INSERT … RETURNING)
//   createMany — a batch write (ONE json_each INSERT for 10 records)
// They are byte-identical to the ops proven native in rust/e1_native_proof (gen.test.ts asserts the
// baked SQL matches), so the rust cell reuses that crate's generated modules unchanged.

// Import the litedbmodel VALUE API from the BUILT, self-contained bundle (`dist/scp/index.cjs`, which
// inlines the ESM-only behavior-contracts) so the ts-IR cell runs as a plain standalone `tsx` process
// (bare `from 'behavior-contracts'` only resolves under a bundler/vitest, not raw node/tsx). Types come
// from source (erased at runtime). Both gen.test.ts (vitest) and ts-ir.ts (tsx) import THIS module, so
// every cell shares ONE litedbmodel instance (no dual-instance IR-brand mismatch).
import { SemanticBehavior, components, publishBehaviors, compileBundle, whereEq, schemaColumnTypeResolver } from '../../../dist/scp/index.cjs';
import type { ColumnTypeResolver } from '../../../src/scp/coltype';
import type { RelationDecl } from '../../../src/scp/relation';
import type { SqlBundle } from '../../../src/scp/runtime';
import { ddl } from '../orm-domain';

const L = components();
export const RESOLVE: ColumnTypeResolver = schemaColumnTypeResolver(ddl('sqlite'));

const USER_COLUMNS = {
  benchmark_users: { id: 'INTEGER', email: 'TEXT', name: 'TEXT', created_at: 'TEXT', updated_at: 'TEXT' },
} as const;
const POST_COLUMNS = {
  benchmark_posts: { id: 'INTEGER', title: 'TEXT', content: 'TEXT', published: 'INTEGER', author_id: 'INTEGER', created_at: 'TEXT' },
  benchmark_comments: { id: 'INTEGER', body: 'TEXT', post_id: 'INTEGER', created_at: 'TEXT' },
} as const;

/** Point read: SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1. */
class Reads extends SemanticBehavior {
  static columns = USER_COLUMNS;
  FindUnique($: { email: unknown }) {
    return L.Select({ table: 'benchmark_users', select: ['id', 'email', 'name'], where: [whereEq(($ as never)['email'], $.email)], limit: 1 });
  }
}
/** Batched relation: parent posts by author + a hasMany comments batch (ONE IN child query). */
class PostReads extends SemanticBehavior {
  static columns = POST_COLUMNS;
  ByAuthor($: { author_id: unknown }) {
    return L.Select({ table: 'benchmark_posts', select: ['id', 'title', 'author_id'], where: [whereEq(($ as never)['author_id'], $.author_id)], order: 'id ASC' });
  }
}
/** Single + batch writes into benchmark_users (RETURNING id, email, name). */
class Writes extends SemanticBehavior {
  static columns = USER_COLUMNS;
  CreateUser($: { email: unknown; name: unknown }) {
    return L.Insert({ table: 'benchmark_users', 'values.email': $.email, 'values.name': $.name, returning: 'id, email, name' });
  }
  CreateMany($: { emails: unknown; names: unknown }) {
    return L.Insert({ table: 'benchmark_users', batch: 'true', 'values.email': $.emails, 'values.name': $.names, returning: 'id, email, name' });
  }
}

export const COMMENTS_REL: RelationDecl = {
  name: 'comments', kind: 'hasMany', targetTable: 'benchmark_comments',
  select: ['id', 'body', 'post_id'], parentKey: 'id', targetKey: 'post_id', order: 'id ASC', dialect: 'sqlite',
} as unknown as RelationDecl;

const READS = publishBehaviors(Reads);
const POST_READS = publishBehaviors(PostReads);
const WRITES = publishBehaviors(Writes);

/** The op axis: id → its compiled sqlite bundle (the ONE bundle every cell consumes). */
export interface BenchOp {
  readonly id: string;
  readonly kind: 'read' | 'write';
  readonly bundle: SqlBundle;
}

export function benchOps(): BenchOp[] {
  return [
    { id: 'findunique', kind: 'read', bundle: compileBundle(READS, 'FindUnique', [], 'sqlite', undefined, RESOLVE) },
    { id: 'relsingle', kind: 'read', bundle: compileBundle(POST_READS, 'ByAuthor', [COMMENTS_REL], 'sqlite', undefined, RESOLVE) },
    { id: 'createuser', kind: 'write', bundle: compileBundle(WRITES, 'CreateUser', [], 'sqlite', undefined, RESOLVE) },
    { id: 'createmany', kind: 'write', bundle: compileBundle(WRITES, 'CreateMany', [], 'sqlite', undefined, RESOLVE) },
  ];
}
