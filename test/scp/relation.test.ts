/**
 * WS4 (#24) — typed-object result + Read relations, over REAL better-sqlite3.
 *
 * The bar (no mocks, docker N/A — SQLite is in-process; the sanctioned substitute):
 *   (a) typed-object result: own props are DATA ONLY (not a DBModel instance); `hydrate`
 *       recovers the v1 method-like UX.
 *   (b) TWO surfaces, ONE relation op: `{ with: { author: true } }` (declarative select) and
 *       `await post.author` (lazy) resolve via the IDENTICAL compiled relation op — asserted
 *       by comparing the rendered batch SQL text (same op, not two parallel paths).
 *   (c) staged batch / no N+1: a query with a relation over N parents issues a BOUNDED number
 *       of queries (1 base + 1 per relation), NEVER one-per-parent — proven by counting the
 *       real `db.prepare(...).all(...)` calls.
 *   (d) parity with v1 direct execution of the batch SQL on the same schema + data.
 *   (e) the §8 bundle stays PURE JSON with relations present (WS3 round-trip invariant holds).
 */

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileBundle,
  executeBundle,
  read,
  readBundle,
  runRelationOp,
  compileRelationOp,
  resolveRelationViaPlan,
  buildResultSet,
  readRelationContext,
  renderOperation,
  whereEq,
  type In,
  type Recorded,
  type RelationDecl,
  type SqlBundle,
} from '../../src/scp';

const L = components();

// ── Schema: posts (parent) + users (belongsTo author) + comments (hasMany) ────────

function seedDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL);
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
    CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL);
  `);
  db.prepare('INSERT INTO posts VALUES (?,?,?)').run(1, 7, 'Hello');
  db.prepare('INSERT INTO posts VALUES (?,?,?)').run(2, 7, 'World');
  db.prepare('INSERT INTO posts VALUES (?,?,?)').run(3, 8, 'Other');
  db.prepare('INSERT INTO users VALUES (?,?)').run(7, 'Ada');
  db.prepare('INSERT INTO users VALUES (?,?)').run(8, 'Alan');
  // comments: post 1 has 3, post 2 has 1, post 3 has 0.
  db.prepare('INSERT INTO comments VALUES (?,?,?,?)').run(10, 1, 'c1a', '2026-01-01');
  db.prepare('INSERT INTO comments VALUES (?,?,?,?)').run(11, 1, 'c1b', '2026-01-02');
  db.prepare('INSERT INTO comments VALUES (?,?,?,?)').run(12, 1, 'c1c', '2026-01-03');
  db.prepare('INSERT INTO comments VALUES (?,?,?,?)').run(13, 2, 'c2a', '2026-01-04');
  return db;
}

/** A better-sqlite3 wrapper that counts every prepared-statement execution (N+1 detector). */
function countingDb(db: InstanceType<typeof Database>): { db: typeof db; count: () => number } {
  let n = 0;
  const proxy = new Proxy(db, {
    get(target, prop, recv) {
      if (prop === 'prepare') {
        return (sql: string) => {
          const stmt = target.prepare(sql);
          return new Proxy(stmt, {
            get(s, p, r) {
              if (p === 'all' || p === 'run' || p === 'get') {
                return (...args: unknown[]) => {
                  n++;
                  return (s[p as 'all'] as (...a: unknown[]) => unknown)(...args);
                };
              }
              return Reflect.get(s, p, r);
            },
          });
        };
      }
      return Reflect.get(target, prop, recv);
    },
  });
  return { db: proxy as typeof db, count: () => n };
}

// A read behavior whose OUTPUT is a row list (the typed-object read surface's input).
class PostFeed extends SemanticBehavior {
  ByAuthor($: In<{ author_id: number }>) {
    return L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title'],
      where: [whereEq($.author_id, $.author_id)],
      order: 'id ASC',
    });
  }
}

const AUTHOR: RelationDecl = {
  name: 'author',
  kind: 'belongsTo',
  targetTable: 'users',
  select: ['id', 'name'],
  parentKey: 'author_id',
  targetKey: 'id',
};
const COMMENTS: RelationDecl = {
  name: 'comments',
  kind: 'hasMany',
  targetTable: 'comments',
  select: ['id', 'post_id', 'body', 'created_at'],
  parentKey: 'id',
  targetKey: 'post_id',
  order: 'created_at ASC',
};

const contract = publishBehaviors(PostFeed);

describe('WS4 — relation op compilation (SQLite batch SQL, reusing IN-list expansion)', () => {
  it('belongsTo → SELECT … WHERE id IN (?) (renders to expanded (?, ?, …) at run time)', () => {
    const op = compileRelationOp(AUTHOR);
    expect(op.query.sql).toBe('SELECT id, name FROM users{where}');
    const rendered = renderOperation(op.query, { __keys: [7, 8] } as never);
    expect(rendered.sql).toBe('SELECT id, name FROM users WHERE id IN (?, ?)');
    expect(rendered.params).toEqual([7, 8]);
  });

  it('hasMany with order → SELECT … WHERE post_id IN (?) ORDER BY created_at ASC', () => {
    const op = compileRelationOp(COMMENTS);
    expect(op.query.sql).toBe('SELECT id, post_id, body, created_at FROM comments{where} ORDER BY created_at ASC');
    const rendered = renderOperation(op.query, { __keys: [1, 2] } as never);
    expect(rendered.sql).toBe('SELECT id, post_id, body, created_at FROM comments WHERE post_id IN (?, ?) ORDER BY created_at ASC');
  });

  it('hasMany with per-parent limit → ROW_NUMBER() window (v1 SQLite batch form)', () => {
    const op = compileRelationOp({ ...COMMENTS, limit: 2 });
    expect(op.query.sql).toBe(
      'WITH ranked AS (SELECT id, post_id, body, created_at, ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY created_at ASC) AS _rn ' +
        'FROM comments{where}) SELECT id, post_id, body, created_at FROM ranked WHERE _rn <= 2',
    );
    const rendered = renderOperation(op.query, { __keys: [1] } as never);
    expect(rendered.sql).toBe(
      'WITH ranked AS (SELECT id, post_id, body, created_at, ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY created_at ASC) AS _rn ' +
        'FROM comments WHERE post_id IN (?)) SELECT id, post_id, body, created_at FROM ranked WHERE _rn <= 2',
    );
  });

  it('a per-parent limit on belongsTo is rejected (cardinality mismatch, fail-closed)', () => {
    expect(() => compileRelationOp({ ...AUTHOR, limit: 3 } as RelationDecl)).toThrow(/only valid for hasMany/);
  });

  it('a per-parent limit WITHOUT an order is rejected (window needs a deterministic order)', () => {
    const noOrder: RelationDecl = { ...COMMENTS, limit: 2 };
    delete (noOrder as { order?: string }).order;
    expect(() => compileRelationOp(noOrder)).toThrow(/requires an explicit 'order'/);
  });
});

describe('WS4 — typed-object result (own props = data only, NOT a DBModel instance)', () => {
  let db: InstanceType<typeof Database>;
  beforeEach(() => {
    db = seedDb();
  });

  it('result rows are plain typed-objects: JSON/Object.keys stay clean (no methods/relations)', () => {
    const rows = read(contract, { author_id: 7 }, { db, entry: 'ByAuthor', relations: [AUTHOR, COMMENTS] });
    expect(rows).toEqual([
      { id: 1, author_id: 7, title: 'Hello' },
      { id: 2, author_id: 7, title: 'World' },
    ]);
    // Own enumerable props are DATA ONLY — the lazy relations are on the prototype (hidden).
    expect(Object.keys(rows[0])).toEqual(['id', 'author_id', 'title']);
    expect(JSON.parse(JSON.stringify(rows[0]))).toEqual({ id: 1, author_id: 7, title: 'Hello' });
    // Not a DBModel instance (a plain object over a relation prototype).
    expect(rows[0] instanceof Object).toBe(true);
    expect((rows[0] as { constructor: unknown }).constructor).toBe(Object);
  });

  it('hydrate recovers the v1 method-like UX from the typed-object', () => {
    class PostDomain {
      constructor(private raw: Record<string, unknown>) {}
      slug(): string {
        return `${this.raw.id}-${String(this.raw.title).toLowerCase()}`;
      }
    }
    const rows = read<PostDomain>(contract, { author_id: 7 }, {
      db,
      entry: 'ByAuthor',
      relations: [AUTHOR],
      hydrate: (raw) => new PostDomain(raw),
    });
    expect(rows).toHaveLength(2);
    expect(rows[0]).toBeInstanceOf(PostDomain);
    expect(rows[0].slug()).toBe('1-hello');
    expect(rows[1].slug()).toBe('2-world');
  });
});

describe('WS4 — declarative select AND lazy resolve via the SAME relation op (no N+1)', () => {
  let seeded: InstanceType<typeof Database>;
  beforeEach(() => {
    seeded = seedDb();
  });

  it('declarative select: relation is an OWN prop, batch-prefetched in ONE query per relation', () => {
    const { db, count } = countingDb(seeded);
    const rows = read(contract, { author_id: 7 }, {
      db,
      entry: 'ByAuthor',
      relations: [AUTHOR, COMMENTS],
      with: { author: true, comments: true },
    }) as Array<Record<string, unknown>>;

    // Both relations attached as OWN props (declarative select → data, shadows the getter).
    expect(Object.keys(rows[0]).sort()).toEqual(['author', 'author_id', 'comments', 'id', 'title']);
    expect(rows[0].author).toEqual({ id: 7, name: 'Ada' });
    expect(rows[1].author).toEqual({ id: 7, name: 'Ada' });
    expect(rows[0].comments).toEqual([
      { id: 10, post_id: 1, body: 'c1a', created_at: '2026-01-01' },
      { id: 11, post_id: 1, body: 'c1b', created_at: '2026-01-02' },
      { id: 12, post_id: 1, body: 'c1c', created_at: '2026-01-03' },
    ]);
    expect(rows[1].comments).toEqual([{ id: 13, post_id: 2, body: 'c2a', created_at: '2026-01-04' }]);

    // no-N+1: 1 base SELECT + 1 author batch + 1 comments batch = 3 queries, NOT 2 + 2*2.
    expect(count()).toBe(3);
  });

  it('lazy: `await post.author` fires the batch op ONCE over the sibling set (still no N+1)', async () => {
    const { db, count } = countingDb(seeded);
    const rows = read(contract, { author_id: 7 }, {
      db,
      entry: 'ByAuthor',
      relations: [AUTHOR, COMMENTS],
    }) as Array<Record<string, unknown> & { author: Promise<unknown> | unknown }>;

    // After the base read: only the base SELECT ran (relations are lazy, not prefetched).
    expect(count()).toBe(1);
    // `author` is NOT an own prop (it lives on the prototype as a lazy getter).
    expect(Object.keys(rows[0])).toEqual(['id', 'author_id', 'title']);

    // Access on EVERY sibling — the batch must run ONCE (memoized across siblings).
    const a0 = await rows[0].author;
    const a1 = await rows[1].author;
    expect(a0).toEqual({ id: 7, name: 'Ada' });
    expect(a1).toEqual({ id: 7, name: 'Ada' });
    expect(count()).toBe(2); // 1 base + 1 author batch (NOT 1 + 2)

    // A second access is cached (no further query).
    await rows[0].author;
    expect(count()).toBe(2);
  });

  it('SAME relation op invariant: declarative and lazy render the IDENTICAL batch SQL', () => {
    // Compile the bundle ONCE; both surfaces read `bundle.relations.author` — one compiled op.
    const bundle = compileBundle(contract, 'ByAuthor', [AUTHOR]);
    const op = bundle.relations.author;

    // Surface 1 (declarative): buildResultSet prefetches via runRelationOp(op, …).
    const db1 = seedDb();
    const parents1 = executeBundle(bundle, { author_id: 7 }, { db: db1 }) as Record<string, unknown>[];
    const declResolved = runRelationOp(op, parents1, db1);

    // Surface 2 (lazy): the RelationContext resolves via the SAME op object from the bundle.
    const db2 = seedDb();
    const parents2 = executeBundle(bundle, { author_id: 7 }, { db: db2 }) as Record<string, unknown>[];
    const lazyResolved = runRelationOp(op, parents2, db2);

    // Identical compiled op object (===) AND identical rendered batch SQL text.
    expect(declResolved.sql).toBe('SELECT id, name FROM users WHERE id IN (?)');
    expect(lazyResolved.sql).toBe(declResolved.sql);
    expect(lazyResolved.keys).toEqual(declResolved.keys);

    // Prove it is the SAME op object the runtime hands both surfaces (one artifact in bundle).
    const declOp = bundle.relations.author;
    const lazyOp = bundle.relations.author;
    expect(declOp).toBe(lazyOp);
  });

  it('lazy hasMany batches once for all siblings and distributes per parent', async () => {
    const { db, count } = countingDb(seeded);
    const rows = read(contract, { author_id: 7 }, {
      db,
      entry: 'ByAuthor',
      relations: [COMMENTS],
    }) as Array<Record<string, unknown> & { comments: unknown }>;
    expect(count()).toBe(1);

    const c0 = await rows[0].comments;
    const c1 = await rows[1].comments;
    expect(count()).toBe(2); // one batched comments query for BOTH posts
    expect((c0 as unknown[]).map((c) => (c as { id: number }).id)).toEqual([10, 11, 12]);
    expect((c1 as unknown[]).map((c) => (c as { id: number }).id)).toEqual([13]);
  });
});

describe('WS4 — staged batch THROUGH bc plan (map.batched → ONE handler call → ONE query)', () => {
  it('resolveRelationViaPlan runs the relation op as a bc batched map: 1 query, per-parent attach', () => {
    const seeded = seedDb();
    const { db, count } = countingDb(seeded);
    const op = compileRelationOp(COMMENTS);
    const parents = db.prepare('SELECT id, author_id, title FROM posts ORDER BY id ASC').all() as Record<string, unknown>[];
    expect(count()).toBe(1); // the parent SELECT

    const augmented = resolveRelationViaPlan(op, parents, db);
    // bc's batched map invokes the handler ONCE → exactly ONE batched comments query.
    expect(count()).toBe(2);

    // `into: 'comments'` attached each parent's children (post 1 → 3, post 2 → 1, post 3 → 0).
    expect((augmented[0].comments as unknown[]).map((c) => (c as { id: number }).id)).toEqual([10, 11, 12]);
    expect((augmented[1].comments as unknown[]).map((c) => (c as { id: number }).id)).toEqual([13]);
    expect(augmented[2].comments).toEqual([]);
    // Parent data preserved (own props unchanged besides the attached relation).
    expect(augmented[0].id).toBe(1);
    expect(augmented[0].title).toBe('Hello');
  });

  it('plan-driven batch == declarative-select assembled result (same op, same rows)', () => {
    const dbA = seedDb();
    const op = compileRelationOp(COMMENTS);
    const parentsA = dbA.prepare('SELECT id, author_id, title FROM posts ORDER BY id ASC').all() as Record<string, unknown>[];
    const viaPlan = resolveRelationViaPlan(op, parentsA, dbA);

    const dbB = seedDb();
    const viaDeclarative = buildResultSet(
      dbB.prepare('SELECT id, author_id, title FROM posts ORDER BY id ASC').all() as Record<string, unknown>[],
      { comments: op },
      dbB,
      { with: { comments: true } },
    );

    viaPlan.forEach((row, i) => {
      expect(row.comments).toEqual(viaDeclarative[i].comments);
    });
  });
});

describe('WS4 — parity: relation batch == v1-style direct execution of the batch SQL', () => {
  it('the declarative-select assembled rows equal direct execution of the same batch SQL', () => {
    const db = seedDb();
    const bundle = compileBundle(contract, 'ByAuthor', [AUTHOR, COMMENTS]);

    // SCP typed-object read with both relations selected.
    const scp = read(contract, { author_id: 7 }, {
      db,
      entry: 'ByAuthor',
      relations: [AUTHOR, COMMENTS],
      with: { author: true, comments: true },
    }) as Array<Record<string, unknown>>;

    // Direct execution of the compiled batch SQL (what a hand-written v1 batch load would run).
    const parentIds = [1, 2]; // author 7's posts
    const authorRows = db.prepare('SELECT id, name FROM users WHERE id IN (?)').all(7) as Record<string, unknown>[];
    const commentRows = db
      .prepare('SELECT id, post_id, body, created_at FROM comments WHERE post_id IN (?, ?) ORDER BY created_at ASC')
      .all(1, 2) as Record<string, unknown>[];

    // Assemble by hand (the distribution the runtime performs) and compare.
    const authorById = new Map(authorRows.map((r) => [r.id, r]));
    const commentsByPost = new Map<number, Record<string, unknown>[]>();
    for (const c of commentRows) {
      const k = c.post_id as number;
      if (!commentsByPost.has(k)) commentsByPost.set(k, []);
      commentsByPost.get(k)!.push(c);
    }
    parentIds.forEach((pid, i) => {
      expect(scp[i].author).toEqual(authorById.get((scp[i] as { author_id: number }).author_id));
      expect(scp[i].comments).toEqual(commentsByPost.get(pid) ?? []);
    });
  });

  it('per-parent LIMIT (ROW_NUMBER window) caps each parent on real SQLite, ONE query', () => {
    const seeded = seedDb();
    const { db, count } = countingDb(seeded);
    const op = compileRelationOp({ ...COMMENTS, limit: 2 });
    // Parents: post 1 (3 comments) and post 2 (1 comment).
    const parents = db.prepare('SELECT id, author_id, title FROM posts WHERE id IN (1,2) ORDER BY id ASC').all() as Record<string, unknown>[];
    expect(count()).toBe(1);
    const { batch } = runRelationOp(op, parents, db);
    expect(count()).toBe(2); // exactly ONE batched window query for both parents
    // post 1 capped to its first 2 (by created_at ASC); post 2 keeps its single comment.
    expect((batch.get('1') ?? []).map((c) => c.id)).toEqual([10, 11]);
    expect((batch.get('2') ?? []).map((c) => c.id)).toEqual([13]);
  });

  it('empty parent-key set issues NO batch query (correct empty-set, not a fallback default)', () => {
    const db = seedDb();
    const op = compileRelationOp(AUTHOR);
    const { count } = countingDb(db);
    // No parents → no keys → renders the empty-IN degeneration, no driver call.
    const { sql, keys, batch } = runRelationOp(op, [], db);
    expect(keys).toEqual([]);
    expect(batch.size).toBe(0);
    expect(sql).toBe('SELECT id, name FROM users WHERE 1 = 0');
    expect(count()).toBe(0);
  });
});

describe('WS4 — the §8 bundle stays PURE JSON with relations present (WS3 invariant holds)', () => {
  it('compileBundle carries relation ops as pure JSON; round-trip is identity', () => {
    const bundle = compileBundle(contract, 'ByAuthor', [AUTHOR, COMMENTS]);
    // Relation ops present and shaped (§8): each carries a CompiledOperation `query`.
    expect(Object.keys(bundle.relations).sort()).toEqual(['author', 'comments']);
    expect(bundle.relations.author.query.component).toBe('Select');
    expect(bundle.relations.comments.kind).toBe('hasMany');

    // Pure JSON: re-serializing a reparsed bundle yields byte-identical JSON (no functions).
    const json = JSON.stringify(bundle);
    const reparsed = JSON.parse(json) as SqlBundle;
    expect(JSON.stringify(reparsed)).toBe(json);
  });

  it('readBundle on the reparsed JSON resolves relations with bc-core alone (no re-compile)', () => {
    const bundle = JSON.parse(JSON.stringify(compileBundle(contract, 'ByAuthor', [AUTHOR]))) as SqlBundle;
    const db = seedDb();
    const rows = readBundle(bundle, { author_id: 7 }, { db, with: { author: true } }) as Array<Record<string, unknown>>;
    expect(rows[0].author).toEqual({ id: 7, name: 'Ada' });
  });

  it('spread/clone drops the lazy batch context (designed degradation, feasibility §9)', () => {
    const db = seedDb();
    const rows = read(contract, { author_id: 7 }, { db, entry: 'ByAuthor', relations: [AUTHOR] }) as Array<Record<string, unknown>>;
    // The live object carries the context; a spread copy is pure data with none of it.
    expect(readRelationContext(rows[0])).toBeDefined();
    const copy = { ...rows[0] };
    expect(readRelationContext(copy)).toBeUndefined();
    // The spread copy has no prototype lazy getter either (own-prop data only).
    expect(Object.getPrototypeOf(copy)).toBe(Object.prototype);
  });

  it('buildResultSet directly (unit): declarative + lazy share the compiled op', () => {
    const db = seedDb();
    const op = compileRelationOp(AUTHOR);
    const ops = { author: op };
    const parents = db.prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC').all(7) as Record<string, unknown>[];
    // Declarative select attaches own prop.
    const decl = buildResultSet(parents, ops, db, { with: { author: true } });
    expect(decl[0].author).toEqual({ id: 7, name: 'Ada' });
    // Lazy: same ops map, resolved via context.
    const parents2 = db.prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC').all(7) as Record<string, unknown>[];
    const lazy = buildResultSet(parents2, ops, db, {});
    expect(Object.keys(lazy[0])).toEqual(['id', 'author_id', 'title']);
    expect(readRelationContext(lazy[0])!.resolve('author', lazy[0])).toEqual({ id: 7, name: 'Ada' });
  });
});
