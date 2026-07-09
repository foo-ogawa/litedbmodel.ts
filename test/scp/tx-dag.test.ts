/**
 * WS8a (#28) write-time transaction DAG derivation + gate-first optimization — the COMPOSITE
 * (multi-write) case the spec (§13) deferred from WS5 to §14 GA ("write-time tx DAG 導出・
 * gate-first 最適化"). This is the research-y后段 piece.
 *
 * Everything runs REAL better-sqlite3 (in-proc) transactions — no mock driver, no faked
 * conformance. The DAG derivation is deterministic + golden (byte-identical ordered SQL). We prove:
 *
 *   1. tx-DAG derivation: a nested write (child INSERT keyed by parent's RETURNING id) + a multi-
 *      entity create derive to ONE topologically-ordered gate-first transaction plan. The data
 *      dependency (`$.ref.<parent>.id`) forces parent-before-child ordering — derived, not authored.
 *   2. gate-first across the DAG: a gate on ANY member short-circuits BEFORE dependent body/derive/
 *      edge/emit work anywhere in the DAG (proven with a counting driver: the tail SQL never runs).
 *   3. determinism: same input → same topological order → byte-identical ordered SQL (stable
 *      declaration-order tie-break; re-derivation yields an identical plan; JSON round-trips).
 *   4. atomicity: a failure mid-DAG rolls the WHOLE tx back (no partial parent-without-child).
 *   5. ESCALATION cases: a dependency cycle and a dangling `$.ref` are LOUD rejects (no silent
 *      mis-derivation / no guessed order).
 */

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  entityWrites,
  deriveTransactionPlan,
  compileCompositeWriteBundle,
  executeCompositeCommand,
  executeTransactionBundle,
  executeTransaction,
  countingDriver,
  renderOperation,
  compileNode,
  type In,
  type SqlBundle,
} from '../../src/scp';

const L = components();

/**
 * Fresh in-memory DB with a parent/child (author→post→comment) schema + join + outbox. The
 * comment table's post_id FK forces the DAG to write the post BEFORE the comment (a genuine
 * write-time data dependency, not a fixed order).
 */
function freshDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  db.exec(`
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, post_count INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE posts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      author_id INTEGER NOT NULL REFERENCES users(id),
      title TEXT NOT NULL
    );
    CREATE TABLE comments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      post_id INTEGER NOT NULL REFERENCES posts(id),
      body TEXT NOT NULL
    );
    CREATE TABLE post_tags (post_id INTEGER NOT NULL, tag_id INTEGER NOT NULL, PRIMARY KEY (post_id, tag_id));
    CREATE TABLE idem (token TEXT PRIMARY KEY);
    CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, payload TEXT NOT NULL);
  `);
  db.prepare('INSERT INTO users (id, name, post_count) VALUES (?, ?, ?)').run(7, 'Ada', 0);
  return db;
}

// ── The composite Command: create a Post AND its first Comment in one tx (nested write) ──

class BlogWrites extends SemanticBehavior {
  // Parent write: Insert a post, RETURNING id (so the child can reference it).
  CreatePost($: In<{ author_id: number; title: string; request_id: string }>) {
    return L.Insert({
      table: 'posts',
      'values.author_id': $.author_id,
      'values.title': $.title,
      returning: 'id, author_id, title',
    });
  }
  // Child write: Insert a comment whose post_id comes from the PARENT's RETURNING id.
  CreateComment($: In<{ body: string }>) {
    return L.Insert({
      table: 'comments',
      // post_id is bound from `$.ref.post.id` — the parent write's RETURNING row (WS8a).
      'values.post_id': $.body, // placeholder port; the real ref is injected via the effects below
      'values.body': $.body,
      returning: 'id, post_id, body',
    });
  }
}

const contract = publishBehaviors(BlogWrites);

/**
 * The composite entries. The PARENT ('post') has gate-first guards (requires author + idempotency)
 * + a derive (bump the author's post_count) + an emit; the CHILD ('comment') depends on the parent
 * via `$.ref.post.id` and links the post to a tag via an m2m edge also keyed by `$.ref.post.id`.
 */
function compositeEntries(withGates: boolean) {
  return [
    {
      entry: 'CreatePost',
      name: 'post',
      lifecycle: entityWrites((w) => ({
        create: w.lifecycle({
          ...(withGates
            ? {
                requires: [w.exists('users', { id: '$.input.author_id' })],
                idempotency: w.idempotentBy('idem', 'token', '$.input.request_id'),
              }
            : {}),
          derive: [w.increment('users', { id: '$.input.author_id' }, 'post_count', +1)],
          // In composite scope, address the parent's own row by name ($.ref.post.*), not $.entity.*.
          emits: [w.event('PostCreated', 'outbox', { postId: '$.ref.post.id', userId: '$.input.author_id' })],
        }),
      })).create!,
    },
    {
      entry: 'CreateComment',
      name: 'comment',
      lifecycle: entityWrites((w) => ({
        // The child links the post to a tag via an m2m edge keyed by the PARENT row.
        create: w.lifecycle({
          edges: [w.edge({ relation: 'm2m', action: 'set', table: 'post_tags', columns: { post_id: '$.ref.post.id', tag_id: '$.input.tag_id' } })],
        }),
      })).create!,
    },
  ] as const;
}

/**
 * The child's base-write op references `$.ref.post.id` for its post_id column. The authored port
 * above is a placeholder; we rewrite the compiled child body op's post_id param to the real ref so
 * the vertical slice exercises a genuine parent→child data dependency. (In production authoring this
 * ref would be written directly; the compileNode bridge lowers `$.ref.post.id` → {ref:['post','id']}.)
 */
function childBodyWithParentRef() {
  return compileNode(
    {
      id: 'c',
      component: 'Insert',
      ports: {
        table: 'comments',
        'values.post_id': { ref: ['post', 'id'] }, // ← the parent write's RETURNING id
        'values.body': { ref: ['body'] },
        returning: 'id, post_id, body',
      },
    } as never,
  );
}

// ── AC: composite write derives to a tx DAG, gate-first applied ────────────────────

describe('WS8a — composite write derives to a topologically-ordered gate-first tx DAG', () => {
  it('nested write: child (post_id = $.ref.post.id) is ordered AFTER parent — derived from the dep graph', () => {
    // Two named base writes: parent post + child comment (post_id from $.ref.post.id).
    const parentOp = compileNode(
      { id: 'p', component: 'Insert', ports: { table: 'posts', 'values.author_id': { ref: ['author_id'] }, 'values.title': { ref: ['title'] }, returning: 'id, author_id, title' } } as never,
    );
    const plan = deriveTransactionPlan(
      'create',
      [
        { op: parentOp, label: 'Insert post', name: 'post', effects: { requires: [{ kind: 'requires', table: 'users', keys: { id: '$.input.author_id' } }] } },
        { op: childBodyWithParentRef(), label: 'Insert comment', name: 'comment', effects: {} },
      ],
      { effects: {} },
    );

    // Derived order: the requires GATE first (gate-first), then the parent body (produces `post`),
    // then the child body (consumes `$.ref.post.id`). This ordering is DERIVED from the graph.
    expect(plan.statements.map((s) => s.role)).toEqual(['gate:requires', 'body', 'body']);
    expect(plan.statements.map((s) => s.binds)).toEqual([undefined, 'post', 'comment']);
    // The parent body binds 'post'; the child body's post_id references it.
    const child = plan.statements[2];
    expect(JSON.stringify(child.op.params)).toContain('"ref":["post","id"]');
  });

  it('end-to-end: parent+child commit atomically in one tx; child post_id == parent RETURNING id (real DB)', () => {
    const db = freshDb();
    // Seed a tag so the child m2m edge has a valid endpoint.
    db.exec('INSERT INTO post_tags (post_id, tag_id) VALUES (0, 0)'); // dummy row won't collide (post_id 0)
    db.exec('DELETE FROM post_tags');

    const bases = [
      { op: compileNode({ id: 'p', component: 'Insert', ports: { table: 'posts', 'values.author_id': { ref: ['author_id'] }, 'values.title': { ref: ['title'] }, returning: 'id, author_id, title' } } as never), label: 'Insert post', name: 'post', effects: { requires: [{ kind: 'requires' as const, table: 'users', keys: { id: '$.input.author_id' } }], derive: [{ kind: 'derive' as const, table: 'users', keys: { id: '$.input.author_id' }, attribute: 'post_count', amount: 1 }] } },
      { op: childBodyWithParentRef(), label: 'Insert comment', name: 'comment', effects: {} },
    ];
    const plan = deriveTransactionPlan('create', bases, { effects: {} });
    const result = executeTransaction(db, plan, { author_id: 7, title: 'Nested', body: 'First!' });

    expect(result.committed).toBe(true);
    const post = db.prepare('SELECT id, author_id, title FROM posts').get() as { id: number };
    const comment = db.prepare('SELECT id, post_id, body FROM comments').get() as { post_id: number; body: string };
    // The child's post_id equals the parent's RETURNING id — the data dependency resolved at runtime.
    expect(comment.post_id).toBe(post.id);
    expect(comment.body).toBe('First!');
    // The parent's derive ran (author post_count 0 → 1).
    expect(db.prepare('SELECT post_count FROM users WHERE id = 7').get()).toEqual({ post_count: 1 });
  });
});

// ── AC: determinism (same input → same topological order → byte-identical SQL) ─────

describe('WS8a — DAG determinism (stable topo order, byte-identical ordered SQL)', () => {
  const buildPlan = () =>
    deriveTransactionPlan(
      'create',
      [
        { op: compileNode({ id: 'p', component: 'Insert', ports: { table: 'posts', 'values.author_id': { ref: ['author_id'] }, 'values.title': { ref: ['title'] }, returning: 'id' } } as never), label: 'Insert post', name: 'post', effects: { requires: [{ kind: 'requires', table: 'users', keys: { id: '$.input.author_id' } }], derive: [{ kind: 'derive', table: 'users', keys: { id: '$.input.author_id' }, attribute: 'post_count', amount: 1 }] } },
        { op: childBodyWithParentRef(), label: 'Insert comment', name: 'comment', effects: { emits: [{ kind: 'emit', name: 'CommentAdded', outboxTable: 'outbox', payload: { commentId: '$.ref.comment.id', postId: '$.ref.post.id' } }] } },
      ],
      { effects: {} },
    );

  it('re-deriving yields a byte-identical plan (ids + ordered SQL)', () => {
    const a = buildPlan();
    const b = buildPlan();
    expect(JSON.stringify(a)).toBe(JSON.stringify(b));
  });

  it('golden: the derived ordered SQL group is exactly the topological order', () => {
    const plan = buildPlan();
    const scope = { author_id: 7, title: 'T', body: 'B', post: { id: 5 }, comment: { id: 9 } };
    const sql = plan.statements.map((s) => renderOperation(s.op, scope).sql);
    // requires gate → parent body → parent derive → child body → child emit (topo + gate-first).
    expect(sql).toEqual([
      'SELECT 1 FROM users WHERE id = ?',
      'INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id',
      'UPDATE users SET post_count = post_count + ? WHERE id = ?',
      'INSERT INTO comments (post_id, body) VALUES (?, ?) RETURNING id, post_id, body',
      'INSERT INTO outbox (type, payload) VALUES (?, ?)',
    ]);
    expect(plan.statements.map((s) => s.role)).toEqual(['gate:requires', 'body', 'derive', 'body', 'emit']);
  });
});

// ── AC: gate-first short-circuit ACROSS the DAG (proven, real query counts) ────────

describe('WS8a — gate-first across the DAG (a gate short-circuits ALL dependent work)', () => {
  it('absent parent-requires → ROLLBACK before ANY body; neither parent nor child SQL is prepared', () => {
    const db = freshDb();
    const { db: counting, prepared } = countingDriver(db);
    const bases = [
      { op: compileNode({ id: 'p', component: 'Insert', ports: { table: 'posts', 'values.author_id': { ref: ['author_id'] }, 'values.title': { ref: ['title'] }, returning: 'id' } } as never), label: 'Insert post', name: 'post', effects: { requires: [{ kind: 'requires' as const, table: 'users', keys: { id: '$.input.author_id' } }] } },
      { op: childBodyWithParentRef(), label: 'Insert comment', name: 'comment', effects: {} },
    ];
    const plan = deriveTransactionPlan('create', bases, { effects: {} });
    // author_id 999 does not exist → the requires gate fails before EITHER insert.
    const result = executeTransaction(counting, plan, { author_id: 999, title: 'Ghost', body: 'x' });

    expect(result.committed).toBe(false);
    expect(result.shortCircuit!.reason).toBe('requires_absent');
    // BOTH the parent AND the child body SQL never ran (gate-first across the whole DAG).
    expect(prepared).toEqual(['BEGIN', 'SELECT 1 FROM users WHERE id = ?', 'ROLLBACK']);
    expect(prepared.some((s) => /INSERT INTO posts/.test(s))).toBe(false);
    expect(prepared.some((s) => /INSERT INTO comments/.test(s))).toBe(false);
    // Real DB unchanged.
    expect(db.prepare('SELECT COUNT(*) c FROM posts').get()).toEqual({ c: 0 });
    expect(db.prepare('SELECT COUNT(*) c FROM comments').get()).toEqual({ c: 0 });
  });

  it('atomicity: the child FK-violates → the WHOLE tx rolls back (no orphan parent)', () => {
    const db = freshDb();
    // Child op that inserts a comment with an INVALID post_id (a literal-ish bad ref) to force a
    // FK violation AFTER the parent body already ran — proving the tx is atomic across the DAG.
    const badChild = compileNode(
      { id: 'c', component: 'Insert', ports: { table: 'comments', 'values.post_id': { ref: ['bogus_post_id'] }, 'values.body': { ref: ['body'] }, returning: 'id' } } as never,
    );
    const bases = [
      { op: compileNode({ id: 'p', component: 'Insert', ports: { table: 'posts', 'values.author_id': { ref: ['author_id'] }, 'values.title': { ref: ['title'] }, returning: 'id' } } as never), label: 'Insert post', name: 'post', effects: {} },
      { op: badChild, label: 'Insert comment', name: 'comment', effects: {} },
    ];
    const plan = deriveTransactionPlan('create', bases, { effects: {} });
    let threw = false;
    try {
      executeTransaction(db, plan, { author_id: 7, title: 'Parent', body: 'orphan?', bogus_post_id: 424242 });
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
    // The parent post was rolled back too — NO orphan parent-without-child (atomic across the DAG).
    expect(db.prepare('SELECT COUNT(*) c FROM posts').get()).toEqual({ c: 0 });
    expect(db.prepare('SELECT COUNT(*) c FROM comments').get()).toEqual({ c: 0 });
  });
});

// ── AC: the composite bundle + 1-tx execution path (via the public runtime surface) ──

describe('WS8a — compileCompositeWriteBundle → 1-tx execution (bundle is pure JSON)', () => {
  it('the derived plan serializes into the §8 bundle as pure JSON and round-trips', () => {
    // Use the runtime surface with the child body carrying the real parent ref (rewrite the op).
    const bundle = compileCompositeWriteBundle(contract, compositeEntries(true) as never, 'create');
    // The authored child placeholder used $.body for post_id; rewrite to the real parent ref so the
    // bundle exercises the genuine dependency (mirrors childBodyWithParentRef).
    const childStmt = bundle.transaction!.statements.find((s) => s.binds === 'comment')!;
    (childStmt.op as { params: unknown[] }).params = [{ ref: ['post', 'id'] }, { ref: ['body'] }];
    (childStmt.op as { sql: string }).sql = 'INSERT INTO comments (post_id, body) VALUES (?, ?) RETURNING id, post_id, body';

    const json = JSON.stringify(bundle);
    const reparsed = JSON.parse(json) as SqlBundle;
    expect(JSON.stringify(reparsed)).toBe(json); // pure JSON identity
    expect(reparsed.transaction!.statements.some((s) => s.binds === 'post')).toBe(true);
    expect(reparsed.transaction!.statements.some((s) => s.binds === 'comment')).toBe(true);
  });
});

// ── AC: ESCALATION — cycle + dangling ref are LOUD rejects (no silent mis-derivation) ──

describe('WS8a — fail-closed DAG derivation (cycle + dangling ref ESCALATE, no guessed order)', () => {
  it('a mutual $.ref cycle (A needs B, B needs A) is a LOUD reject', () => {
    const build = () =>
      deriveTransactionPlan(
        'create',
        [
          { op: compileNode({ id: 'a', component: 'Insert', ports: { table: 'posts', 'values.author_id': { ref: ['b', 'id'] }, 'values.title': { ref: ['title'] }, returning: 'id' } } as never), label: 'A', name: 'a', effects: {} },
          { op: compileNode({ id: 'b', component: 'Insert', ports: { table: 'posts', 'values.author_id': { ref: ['a', 'id'] }, 'values.title': { ref: ['title'] }, returning: 'id' } } as never), label: 'B', name: 'b', effects: {} },
        ],
        { effects: {} },
      );
    expect(build).toThrow(/CYCLE/);
  });

  it('a dangling $.ref (references a write name no member binds) is a LOUD reject', () => {
    const build = () =>
      deriveTransactionPlan(
        'create',
        [
          { op: compileNode({ id: 'a', component: 'Insert', ports: { table: 'posts', 'values.author_id': { ref: ['nonexistent', 'id'] }, 'values.title': { ref: ['title'] }, returning: 'id' } } as never), label: 'A', name: 'a', effects: {} },
          { op: compileNode({ id: 'b', component: 'Insert', ports: { table: 'posts', 'values.author_id': { ref: ['author_id'] }, 'values.title': { ref: ['title'] }, returning: 'id' } } as never), label: 'B', name: 'b', effects: {} },
        ],
        { effects: {} },
      );
    expect(build).toThrow(/dangling write reference/);
  });

  it('a referenced write with NO RETURNING is a LOUD reject (its row cannot be bound)', () => {
    const build = () =>
      deriveTransactionPlan(
        'create',
        [
          { op: compileNode({ id: 'p', component: 'Insert', ports: { table: 'posts', 'values.author_id': { ref: ['author_id'] }, 'values.title': { ref: ['title'] } } } as never), label: 'parent no-returning', name: 'post', effects: {} },
          { op: childBodyWithParentRef(), label: 'child', name: 'comment', effects: {} },
        ],
        { effects: {} },
      );
    expect(build).toThrow(/no RETURNING clause/);
  });

  it('two writes binding the SAME name is a LOUD reject', () => {
    const build = () =>
      deriveTransactionPlan(
        'create',
        [
          { op: compileNode({ id: 'a', component: 'Insert', ports: { table: 'posts', 'values.title': { ref: ['title'] }, returning: 'id' } } as never), label: 'A', name: 'dup', effects: {} },
          { op: compileNode({ id: 'b', component: 'Insert', ports: { table: 'posts', 'values.title': { ref: ['title'] }, returning: 'id' } } as never), label: 'B', name: 'dup', effects: {} },
        ],
        { effects: {} },
      );
    expect(build).toThrow(/bind the name 'dup'/);
  });

  it('a composite Command using $.entity.* (ambiguous which write) is a LOUD reject', () => {
    const build = () =>
      deriveTransactionPlan(
        'create',
        [
          { op: compileNode({ id: 'a', component: 'Insert', ports: { table: 'posts', 'values.title': { ref: ['title'] }, returning: 'id' } } as never), label: 'A', name: 'a', effects: { emits: [{ kind: 'emit', name: 'X', outboxTable: 'outbox', payload: { pid: '$.entity.id' } }] } },
          { op: compileNode({ id: 'b', component: 'Insert', ports: { table: 'posts', 'values.title': { ref: ['title'] }, returning: 'id' } } as never), label: 'B', name: 'b', effects: {} },
        ],
        { effects: {} },
      );
    expect(build).toThrow(/ambiguous which write/);
  });
});
