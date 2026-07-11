/**
 * WS5 (#25) write-time relations — the REAL 1-transaction vertical slice:
 *
 *   entityWrites declaration → gate-first transaction plan → 1 real SQLite tx
 *   (BEGIN; requires; idempotency; unique; INSERT; derive; edges; emits; COMMIT)
 *
 * Every test runs REAL better-sqlite3 (create tables → seed → run the Command through the SCP
 * write path → assert). No mock driver / no mocked transaction.
 *
 * ## docker N/A justification (#25 AC — same as WS3)
 *
 * litedbmodel's SQLite backend is better-sqlite3 — an IN-PROCESS, synchronous engine with no
 * server; there is nothing to containerize. This suite is the sanctioned substitute for a docker
 * integration test: it exercises the FULL write pipeline (BEGIN/COMMIT/ROLLBACK, gate-first
 * short-circuit, RETURNING → `$.entity`, cascade counter, outbox, idempotency, unique) against a
 * real SQLite database, in-process. Mocking the driver / transaction would defeat the purpose
 * (a failed audit) — these tests bind and execute real SQL and observe real DB state.
 *
 * The three #25 ACs are each proven here:
 *   (1) create/update/remove derive to ONE tx with write-time relations → the golden ordered-SQL
 *       group + real persistence of every side effect in the same tx.
 *   (2) gate-first short-circuits unnecessary SQL early → proven with a COUNTING driver (the tail
 *       SQL is never prepared) AND with DB state (no body/derive/outbox write happened).
 *   (3) golden (same input → same ordered SQL group + same result) → byte-identical rendered SQL
 *       list, and result parity vs an EQUIVALENT imperative execution of that same group.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  entityWrites,
  edgeWrites,
  deriveTransactionPlan,
  compileWriteBundle,
  executeCommand,
  executeTransactionBundle,
  executeTransaction,
  countingDriver,
  renderTxStatement,
  compileWriteNode,
  whereEq,
  SqlFailure,
  type In,
  type SqlBundle,
  type TransactionPlan,
  type EntityWritesDefinition,
} from '../../src/scp';

const L = components();

/** Fresh in-memory DB with the α write schema + seed rows for each test. */
function freshDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  db.exec(`
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, post_count INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE posts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      author_id INTEGER NOT NULL REFERENCES users(id),
      title TEXT NOT NULL,
      created_at TEXT
    );
    CREATE TABLE idem (token TEXT PRIMARY KEY);
    CREATE TABLE uniq (name TEXT NOT NULL, s0 INTEGER NOT NULL, f0 TEXT NOT NULL, PRIMARY KEY (name, s0, f0));
    CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, payload TEXT NOT NULL);
    CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT);
    CREATE TABLE post_tags (post_id INTEGER NOT NULL, tag_id INTEGER NOT NULL, PRIMARY KEY (post_id, tag_id));
  `);
  db.prepare('INSERT INTO users (id, name, post_count) VALUES (?, ?, ?)').run(7, 'Ada', 2);
  db.prepare('INSERT INTO users (id, name, post_count) VALUES (?, ?, ?)').run(8, 'Alan', 0);
  db.prepare('INSERT INTO tags (id, label) VALUES (?, ?)').run(100, 'sql');
  return db;
}

// ── The authored Command + its write-time-relations save contract (spec §2.2 / §2.4) ──

class PostCommands extends SemanticBehavior {
  // spec §2.4: `CreatePost` returns Insert(Post, {onWrite: Post.writes.create, returning}).
  Create($: In<{ author_id: number; title: string; request_id: string }>) {
    return L.Insert({
      table: 'posts',
      'values.author_id': $.author_id,
      'values.title': $.title,
      returning: 'id, author_id, title',
    });
  }

  Remove($: In<{ id: number; author_id: number }>) {
    return L.Delete({ table: 'posts', where: [whereEq($.id, $.id)], returning: 'id' });
  }
}

/**
 * The `create` save contract (spec §2.2 example): referential integrity (author exists) +
 * idempotency (request token) + uniqueness (title per author) as gate-first guards, then the
 * cascade counter (`users.post_count += 1`) and the outbox event, all in one tx.
 */
const postWrites: EntityWritesDefinition = entityWrites<PostCommands>((w) => ({
  create: w.lifecycle({
    requires: [w.exists('users', { id: '$.input.author_id' })],
    idempotency: w.idempotentBy('idem', 'token', '$.input.request_id'),
    unique: [
      w.unique({ name: 'title_per_author', guardTable: 'uniq', scope: ['$.input.author_id'], fields: ['$.input.title'] }),
    ],
    derive: [w.increment('users', { id: '$.input.author_id' }, 'post_count', +1)],
    emits: [w.event('PostCreated', 'outbox', { postId: '$.entity.id', userId: '$.input.author_id' })],
  }),
  remove: w.lifecycle({
    derive: [w.increment('users', { id: '$.input.author_id' }, 'post_count', -1)],
  }),
}));

const contract = publishBehaviors(PostCommands);

// The exact ordered SQL group (rendered) for a CREATE — the golden bar (spec §6 example).
const GOLDEN_CREATE_SQL: readonly string[] = [
  'SELECT 1 FROM users WHERE id = ?',
  'INSERT INTO idem (token) VALUES (?) ON CONFLICT DO NOTHING',
  'INSERT INTO uniq (name, s0, f0) VALUES (?, ?, ?) ON CONFLICT DO NOTHING',
  'INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, author_id, title',
  'UPDATE users SET post_count = post_count + ? WHERE id = ?',
  'INSERT INTO outbox (type, payload) VALUES (?, ?)',
];

// ── AC1 + AC3: 1-tx derivation + golden ordered SQL group + real side effects ─────

describe('WS5 — create derives to ONE gate-first transaction (golden SQL + real persistence)', () => {
  let db: InstanceType<typeof Database>;
  beforeEach(() => {
    db = freshDb();
  });

  it('the derived plan is the §6 ordered group (requires→idempotency→unique→body→derive→emit)', () => {
    const bundle = compileWriteBundle(contract, 'Create', postWrites, 'create');
    const plan = bundle.transaction!;
    expect(plan.statements.map((s) => s.role)).toEqual([
      'gate:requires',
      'gate:idempotency',
      'gate:unique',
      'body',
      'derive',
      'emit',
    ]);
    // Gate-first rules attach ONLY to the gate statements.
    expect(plan.statements.map((s) => s.gate)).toEqual([
      'existsElseRollback',
      'insertedElseNoop',
      'insertedElseRollback',
      undefined,
      undefined,
      undefined,
    ]);
    // The body write is the `$.entity` source (its RETURNING row feeds derive/emit).
    expect(plan.entityFrom).toBe(plan.statements[3].id);
  });

  it('golden: same input → byte-identical ordered SQL text group', () => {
    const bundle = compileWriteBundle(contract, 'Create', postWrites, 'create');
    // Render every statement against a bound scope (entity row present for the derive/emit stage).
    const scope = {
      author_id: 7,
      title: 'Hello',
      request_id: 'r-123',
      __entity: { id: 3, author_id: 7, title: 'Hello' },
    };
    const rendered = bundle.transaction!.statements.map((s) => renderTxStatement(s.op, scope).sql);
    expect(rendered).toEqual(GOLDEN_CREATE_SQL);

    // Deterministic derivation: recompiling yields a byte-identical plan (statement ids + SQL).
    const again = compileWriteBundle(contract, 'Create', postWrites, 'create');
    expect(JSON.stringify(again.transaction)).toBe(JSON.stringify(bundle.transaction));
  });

  it('executeCommand commits ALL side effects atomically in one tx (real DB state)', () => {
    const result = executeCommand(contract, postWrites, 'create', { author_id: 7, title: 'Hello', request_id: 'r-1' }, { db, entry: 'Create' });

    expect(result.committed).toBe(true);
    expect(result.shortCircuit).toBeUndefined();
    // The body RETURNING row is exposed as `$.entity`.
    expect(result.entity).toEqual({ id: 1, author_id: 7, title: 'Hello' });
    // Every ordered statement ran.
    expect(result.executed).toHaveLength(6);

    // Body persisted.
    expect(db.prepare('SELECT id, author_id, title FROM posts').all()).toEqual([{ id: 1, author_id: 7, title: 'Hello' }]);
    // derive: users.post_count 2 → 3 (the increment amount is from the declaration, not code).
    expect(db.prepare('SELECT post_count FROM users WHERE id = 7').get()).toEqual({ post_count: 3 });
    // idempotency token stored.
    expect(db.prepare('SELECT token FROM idem').all()).toEqual([{ token: 'r-1' }]);
    // unique guard row stored.
    expect(db.prepare('SELECT name, s0, f0 FROM uniq').all()).toEqual([{ name: 'title_per_author', s0: 7, f0: 'Hello' }]);
    // emit: the outbox row carries the JSON payload built from `$.entity.id` + `$.input.author_id`.
    const outbox = db.prepare('SELECT type, payload FROM outbox').get() as { type: string; payload: string };
    expect(outbox.type).toBe('PostCreated');
    expect(JSON.parse(outbox.payload)).toEqual({ postId: 1, userId: 7 });
  });
});

// ── AC2: gate-first short-circuit is a REAL early termination (query count + DB state) ──

describe('WS5 — gate-first short-circuit (proven: tail SQL never runs, no side effects)', () => {
  let db: InstanceType<typeof Database>;
  beforeEach(() => {
    db = freshDb();
  });

  it('absent `requires` → ROLLBACK before the body; tail SQL is NEVER prepared', () => {
    const { db: counting, prepared } = countingDriver(db);
    // author_id 999 does not exist → the `requires` existence gate fails first.
    const result = executeCommand(
      contract,
      postWrites,
      'create',
      { author_id: 999, title: 'Ghost', request_id: 'r-x' },
      { db: counting, entry: 'Create' },
    );

    expect(result.committed).toBe(false);
    expect(result.shortCircuit).toEqual({ statementId: expect.any(String), reason: 'requires_absent' });
    // Only BEGIN + the requires probe + ROLLBACK were prepared — the body/derive/emit SQL and the
    // idempotency/unique gate SQL were NEVER prepared (real query-count drop, spec §6 Gate First).
    expect(prepared).toEqual(['BEGIN', 'SELECT 1 FROM users WHERE id = ?', 'ROLLBACK']);
    expect(prepared.some((s) => /INSERT INTO posts/.test(s))).toBe(false);
    expect(prepared.some((s) => /UPDATE users/.test(s))).toBe(false);
    expect(prepared.some((s) => /INSERT INTO outbox/.test(s))).toBe(false);

    // DB state is UNCHANGED (the tx rolled back): no post, no counter change, no token, no outbox.
    expect(db.prepare('SELECT COUNT(*) c FROM posts').get()).toEqual({ c: 0 });
    expect(db.prepare('SELECT post_count FROM users WHERE id = 7').get()).toEqual({ post_count: 2 });
    expect(db.prepare('SELECT COUNT(*) c FROM idem').get()).toEqual({ c: 0 });
    expect(db.prepare('SELECT COUNT(*) c FROM outbox').get()).toEqual({ c: 0 });
    // The executed list stops at the failing gate (only the requires probe ran).
    expect(result.executed).toHaveLength(1);
  });

  it('the requires gate short-circuits BEFORE the idempotency/unique gates (order matters)', () => {
    const { db: counting, prepared } = countingDriver(db);
    executeCommand(contract, postWrites, 'create', { author_id: 999, title: 'X', request_id: 'r-y' }, { db: counting, entry: 'Create' });
    // Neither the idempotency INSERT nor the unique INSERT was prepared — the requires gate is FIRST.
    expect(prepared.some((s) => /INSERT INTO idem/.test(s))).toBe(false);
    expect(prepared.some((s) => /INSERT INTO uniq/.test(s))).toBe(false);
  });
});

// ── AC1(d): idempotency — a duplicate request_id performs NO double write ──────────

describe('WS5 — idempotency guard (duplicate request_id → short-circuit, no double write)', () => {
  it('second create with the same request_id is a no-op (idempotent) and rolls back', () => {
    const db = freshDb();
    const input = { author_id: 7, title: 'Once', request_id: 'dup-1' };

    const first = executeCommand(contract, postWrites, 'create', input, { db, entry: 'Create' });
    expect(first.committed).toBe(true);
    expect(db.prepare('SELECT post_count FROM users WHERE id = 7').get()).toEqual({ post_count: 3 });
    expect(db.prepare('SELECT COUNT(*) c FROM posts').get()).toEqual({ c: 1 });

    // Re-run with the SAME request_id (different title, so unique would NOT collide) → the
    // idempotency gate detects the duplicate token and short-circuits: NO second post, NO second
    // counter bump, NO second outbox row.
    const { db: counting, prepared } = countingDriver(db);
    const second = executeCommand(
      contract,
      postWrites,
      'create',
      { author_id: 7, title: 'Twice', request_id: 'dup-1' },
      { db: counting, entry: 'Create' },
    );
    expect(second.committed).toBe(false);
    expect(second.shortCircuit!.reason).toBe('idempotent_duplicate');
    // The body/derive/emit + the unique gate never ran after the idempotency gate hit.
    expect(prepared.some((s) => /INSERT INTO posts/.test(s))).toBe(false);
    expect(prepared.some((s) => /INSERT INTO uniq/.test(s))).toBe(false);

    // DB unchanged by the duplicate: still exactly ONE post, counter still 3, one outbox row.
    expect(db.prepare('SELECT COUNT(*) c FROM posts').get()).toEqual({ c: 1 });
    expect(db.prepare('SELECT post_count FROM users WHERE id = 7').get()).toEqual({ post_count: 3 });
    expect(db.prepare('SELECT COUNT(*) c FROM outbox').get()).toEqual({ c: 1 });
  });
});

// ── AC1(e): unique-guard collision handling ───────────────────────────────────────

describe('WS5 — unique guard (title-per-author collision → ROLLBACK, no partial write)', () => {
  it('a colliding (author, title) rolls the whole tx back after the body would have run', () => {
    const db = freshDb();
    // First create claims the unique (7, 'Dup').
    executeCommand(contract, postWrites, 'create', { author_id: 7, title: 'Dup', request_id: 'u-1' }, { db, entry: 'Create' });
    expect(db.prepare('SELECT COUNT(*) c FROM posts').get()).toEqual({ c: 1 });
    expect(db.prepare('SELECT post_count FROM users WHERE id = 7').get()).toEqual({ post_count: 3 });

    // Second create, SAME (author, title) but a fresh request_id (so idempotency passes) → the
    // unique gate collides and rolls back: no second post, counter unchanged.
    const { db: counting, prepared } = countingDriver(db);
    const second = executeCommand(
      contract,
      postWrites,
      'create',
      { author_id: 7, title: 'Dup', request_id: 'u-2' },
      { db: counting, entry: 'Create' },
    );
    expect(second.committed).toBe(false);
    expect(second.shortCircuit!.reason).toBe('unique_collision');
    // The body INSERT (and derive/emit) never ran — the unique gate is BEFORE the body.
    expect(prepared.some((s) => /INSERT INTO posts/.test(s))).toBe(false);
    expect(prepared.some((s) => /UPDATE users/.test(s))).toBe(false);

    // The idempotency token was inserted then rolled back (no residue), and no second post exists.
    expect(db.prepare('SELECT COUNT(*) c FROM posts').get()).toEqual({ c: 1 });
    expect(db.prepare('SELECT post_count FROM users WHERE id = 7').get()).toEqual({ post_count: 3 });
    expect(db.prepare('SELECT COUNT(*) c FROM idem').get()).toEqual({ c: 1 }); // only the first token
  });
});

// ── AC3: result parity vs an EQUIVALENT imperative execution of the same SQL group ──

describe('WS5 — parity: the SCP tx == equivalent imperative execution of the same ordered group', () => {
  it('same input → same ordered SQL group + same final DB state as hand-run statements', () => {
    const input = { author_id: 8, title: 'Parity', request_id: 'p-1' };

    // (a) SCP path on DB #1.
    const dbScp = freshDb();
    const scpResult = executeCommand(contract, postWrites, 'create', input, { db: dbScp, entry: 'Create' });
    expect(scpResult.committed).toBe(true);

    // (b) Equivalent IMPERATIVE execution on DB #2: hand-run the SAME ordered statements (the
    // golden group) in one BEGIN/COMMIT, threading the RETURNING row into the derive/emit binds.
    const dbImp = freshDb();
    dbImp.prepare('BEGIN').run();
    expect(dbImp.prepare('SELECT 1 FROM users WHERE id = ?').all(8)).toHaveLength(1); // requires passes
    dbImp.prepare('INSERT INTO idem (token) VALUES (?) ON CONFLICT DO NOTHING').run('p-1');
    dbImp.prepare('INSERT INTO uniq (name, s0, f0) VALUES (?, ?, ?) ON CONFLICT DO NOTHING').run('title_per_author', 8, 'Parity');
    const entity = dbImp
      .prepare('INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, author_id, title')
      .get(8, 'Parity') as { id: number };
    dbImp.prepare('UPDATE users SET post_count = post_count + ? WHERE id = ?').run(1, 8);
    dbImp.prepare('INSERT INTO outbox (type, payload) VALUES (?, ?)').run('PostCreated', JSON.stringify({ postId: entity.id, userId: 8 }));
    dbImp.prepare('COMMIT').run();

    // The two databases end in an IDENTICAL state across every table the tx touched.
    for (const q of [
      'SELECT id, author_id, title FROM posts ORDER BY id',
      'SELECT id, name, post_count FROM users ORDER BY id',
      'SELECT token FROM idem ORDER BY token',
      'SELECT name, s0, f0 FROM uniq ORDER BY name, s0, f0',
      'SELECT type, payload FROM outbox ORDER BY id',
    ]) {
      expect(dbScp.prepare(q).all()).toEqual(dbImp.prepare(q).all());
    }
  });
});

// ── remove lifecycle: 1-tx derive (counter −1) with a WHERE-bearing base write ─────

describe('WS5 — remove lifecycle (base Delete + cascade counter −1, one tx)', () => {
  it('remove derives Delete + derive(−1) in one tx; amount sign comes from the declaration', () => {
    const db = freshDb();
    // Seed a post to remove (author 7 starts at post_count 2).
    db.prepare('INSERT INTO posts (id, author_id, title) VALUES (?, ?, ?)').run(50, 7, 'Gone');

    const result = executeCommand(contract, postWrites, 'remove', { id: 50, author_id: 7 }, { db, entry: 'Remove' });
    expect(result.committed).toBe(true);
    // Post gone.
    expect(db.prepare('SELECT COUNT(*) c FROM posts WHERE id = 50').get()).toEqual({ c: 0 });
    // Counter decremented 2 → 1 (the −1 amount is declared, not a code default).
    expect(db.prepare('SELECT post_count FROM users WHERE id = 7').get()).toEqual({ post_count: 1 });
    // Ordered group: body Delete, then derive.
    const bundle = compileWriteBundle(contract, 'Remove', postWrites, 'remove');
    expect(bundle.transaction!.statements.map((s) => s.role)).toEqual(['body', 'derive']);
    // The rendered body Delete is byte-identical to v1's DELETE shape (WHERE id = ?).
    expect(renderTxStatement(bundle.transaction!.statements[0].op, { id: 50 }).sql).toBe('DELETE FROM posts WHERE id = ? RETURNING id');
    expect(renderTxStatement(bundle.transaction!.statements[1].op, { author_id: 7 }).sql).toBe('UPDATE users SET post_count = post_count + ? WHERE id = ?');
  });
});

// ── Bundle round-trip: the transaction plan is pure JSON (WS7 self-sufficiency) ────

describe('WS5 — §8 bundle carries the tx plan as pure JSON (round-trips, executes bc-core alone)', () => {
  it('serialize → JSON.parse → executeTransactionBundle == direct executeCommand', () => {
    const input = { author_id: 7, title: 'Roundtrip', request_id: 'rt-1' };

    // The published artifact: pure JSON, no TS state.
    const json = JSON.stringify(compileWriteBundle(contract, 'Create', postWrites, 'create'));
    const reparsed = JSON.parse(json) as SqlBundle;
    expect(reparsed.transaction).toBeDefined();

    const dbBundle = freshDb();
    const fromBundle = executeTransactionBundle(reparsed, input, { db: dbBundle });
    const dbDirect = freshDb();
    const direct = executeCommand(contract, postWrites, 'create', input, { db: dbDirect, entry: 'Create' });

    expect(fromBundle.entity).toEqual(direct.entity);
    expect(fromBundle.committed).toBe(true);
    // The reparsed-JSON path persisted everything (executed via the serialized plan alone).
    expect(dbBundle.prepare('SELECT post_count FROM users WHERE id = 7').get()).toEqual({ post_count: 3 });
    const ob = dbBundle.prepare('SELECT type, payload FROM outbox').get() as { payload: string };
    expect(JSON.parse(ob.payload)).toEqual({ postId: 1, userId: 7 });
  });

  it('the serialized bundle carries NO function/TS state (pure JSON identity round-trips)', () => {
    const bundle = compileWriteBundle(contract, 'Create', postWrites, 'create');
    const reparsed = JSON.parse(JSON.stringify(bundle)) as SqlBundle;
    expect(JSON.stringify(reparsed)).toBe(JSON.stringify(bundle));
  });
});

// ── m2m edge write: intermediate-table INSERT in the same tx ──────────────────────

describe('WS5 — edges (many-to-many intermediate-table link in the same tx)', () => {
  it('an m2m `set` edge INSERTs the join row bound from `$.entity` + `$.input`', () => {
    const db = freshDb();

    class TagPost extends SemanticBehavior {
      Create($: In<{ author_id: number; title: string; tag_id: number; request_id: string }>) {
        return L.Insert({
          table: 'posts',
          'values.author_id': $.author_id,
          'values.title': $.title,
          returning: 'id, author_id, title',
        });
      }
    }
    const c = publishBehaviors(TagPost);
    const writes = entityWrites((w) => ({
      create: w.lifecycle({
        requires: [w.exists('users', { id: '$.input.author_id' })],
        // link the new post to the tag: post_tags(post_id = $.entity.id, tag_id = $.input.tag_id).
        edges: [
          w.edge({ relation: 'm2m', action: 'set', table: 'post_tags', columns: { post_id: '$.entity.id', tag_id: '$.input.tag_id' } }),
        ],
      }),
    }));

    const plan = compileWriteBundle(c, 'Create', writes, 'create').transaction!;
    expect(plan.statements.map((s) => s.role)).toEqual(['gate:requires', 'body', 'edge']);

    const result = executeCommand(c, writes, 'create', { author_id: 7, title: 'Tagged', tag_id: 100, request_id: 'e-1' }, { db, entry: 'Create' });
    expect(result.committed).toBe(true);
    // The join row links the just-created post to the tag (post_id from `$.entity.id`).
    expect(db.prepare('SELECT post_id, tag_id FROM post_tags').all()).toEqual([{ post_id: result.entity!.id, tag_id: 100 }]);
  });
});

// ── Fail-closed guards (declaration + derivation) ─────────────────────────────────

describe('WS5 — fail-closed derivation guards (no bodging, no silent defaults)', () => {
  it('rejects a `$.entity.*` reference when the body write has no RETURNING', () => {
    class NoReturn extends SemanticBehavior {
      Create($: In<{ author_id: number; title: string }>) {
        return L.Insert({ table: 'posts', 'values.author_id': $.author_id, 'values.title': $.title });
      }
    }
    const c = publishBehaviors(NoReturn);
    const writes = entityWrites((w) => ({
      create: w.lifecycle({ emits: [w.event('X', 'outbox', { postId: '$.entity.id' })] }),
    }));
    expect(() => compileWriteBundle(c, 'Create', writes, 'create')).toThrow(/no RETURNING/);
  });

  it('rejects a malformed path root at declaration time (fail-closed)', () => {
    expect(() =>
      entityWrites((w) => ({ create: w.lifecycle({ requires: [w.exists('users', { id: 'author_id' })] }) })),
    ).toThrow(/not a valid path-rooted value/);
  });

  it('WS8a: two independent named base writes derive to ONE tx DAG (restriction lifted, not half-built)', () => {
    // Two INDEPENDENT write nodes in one Command → WS8a derives them into one ordered tx plan
    // (the WS5 "deferred to WS8" loud-reject is now genuinely implemented, not rejected).
    const plan = deriveTransactionPlan('create', [
      { op: compileWriteNode({ id: 'a', component: 'Insert', ports: { table: 'posts', 'values.title': { ref: ['title'] } } } as never), label: 'a', name: 'a', effects: {} },
      { op: compileWriteNode({ id: 'b', component: 'Insert', ports: { table: 'tags', 'values.label': { ref: ['label'] } } } as never), label: 'b', name: 'b', effects: {} },
    ], { effects: {} });
    // Both bodies present, ordered deterministically by declaration seq (a before b), no gates.
    expect(plan.statements.map((s) => s.role)).toEqual(['body', 'body']);
    expect(plan.statements.map((s) => s.binds)).toEqual(['a', 'b']);
  });

  it('edgeWrites rejects a non-edge effect (edge entity carries only edge writes)', () => {
    expect(() =>
      edgeWrites((w) => ({ create: w.lifecycle({ derive: [w.increment('users', { id: '$.input.x' }, 'n', 1)] }) })),
    ).toThrow(/ONLY .edges. effects/);
  });
});

// ── Driver failure → mapped SqlFailure with ROLLBACK ──────────────────────────────

describe('WS5 — driver failure maps to SqlFailure and rolls the tx back', () => {
  it('a FK violation on the body write throws a constraint SqlFailure and leaves no residue', () => {
    const db = freshDb();
    // A create whose author passes the requires gate would need the row; here we bypass requires
    // by using a save contract with NO gates, so the FK violation surfaces at the body INSERT.
    const noGate = entityWrites((w) => ({ create: w.lifecycle({}) }));
    // author_id 404 violates the posts.author_id FK (no such user).
    let thrown: unknown;
    try {
      executeCommand(contract, noGate, 'create', { author_id: 404, title: 'Bad', request_id: 'f-1' }, { db, entry: 'Create' });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(SqlFailure);
    expect((thrown as SqlFailure).kind).toBe('foreign_key_violation');
    // The tx rolled back — no post row.
    expect(db.prepare('SELECT COUNT(*) c FROM posts').get()).toEqual({ c: 0 });
  });
});

// ── executeTransaction directly (the lowest-level real-tx entrypoint) ─────────────

describe('WS5 — executeTransaction runs a hand-derived plan against real SQLite', () => {
  it('a minimal derived plan (requires gate + body) commits and returns the entity', () => {
    const db = freshDb();
    const plan: TransactionPlan = deriveTransactionPlan(
      'create',
      [
        {
          op: compileWriteNode({
            id: 'ins',
            component: 'Insert',
            ports: { table: 'posts', 'values.author_id': { ref: ['author_id'] }, 'values.title': { ref: ['title'] }, returning: 'id' },
          } as never),
          label: 'Insert posts',
        },
      ],
      { effects: { requires: [{ kind: 'requires', table: 'users', keys: { id: '$.input.author_id' } }] } },
    );
    const result = executeTransaction(db, plan, { author_id: 7, title: 'Direct' });
    expect(result.committed).toBe(true);
    expect(result.entity).toEqual({ id: 1 });
    expect(db.prepare('SELECT COUNT(*) c FROM posts').get()).toEqual({ c: 1 });
  });
});

// ---------------------------------------------------------------------------
// M4 (re-audit) — an UNKNOWN / forward-incompatible gate rule is FAIL-CLOSED (aligned across
// all 5 runtimes: TS + Python + Rust + Go + PHP). A corrupt gate string must NOT silently
// continue (fail-open would skip the gate and let the write COMMIT). Here on the TS runtime:
// `executeTransaction` aborts (throws) and does NOT commit. The other 4 languages assert the
// same invariant in their own runtime tests (python/tests/test_runtime.py,
// rust .../runtime.rs #[test], go write_test.go, php WriteRuntimeTest).
// ---------------------------------------------------------------------------
describe('M4 — unknown gate rule fails CLOSED (never a silent commit)', () => {
  it('executeTransaction ABORTS (throws) and does NOT commit on an unknown gate rule', () => {
    const db = freshDb();
    // A hand-built plan with a body write GATED by a bogus/forward-incompatible gate string.
    const bodyOp = compileWriteNode({
      id: 'ins',
      component: 'Insert',
      ports: { table: 'posts', 'values.author_id': { ref: ['author_id'] }, 'values.title': { ref: ['title'] }, returning: 'id' },
    } as never);
    const plan: TransactionPlan = {
      phase: 'create',
      entityFrom: null,
      onIdempotentHit: 'rollback',
      statements: [
        // Reuse a real existence probe as the gate statement, but tag it with an UNKNOWN gate rule.
        {
          id: 'g',
          role: 'gate:requires',
          op: { sql: 'SELECT 1 FROM users WHERE id = ?', params: [{ ref: ['author_id'] }] },
          gate: 'someFutureGateRuleThatDoesNotExist' as never,
          label: 'bogus gate',
        },
        { id: 'b', role: 'body', op: bodyOp, label: 'Insert posts' },
      ],
    };
    expect(() => executeTransaction(db, plan, { author_id: 7, title: 'X' })).toThrow(/unknown gate rule/);
    // FAIL-CLOSED: the write must NOT have committed (the tx rolled back).
    expect(db.prepare('SELECT COUNT(*) c FROM posts').get()).toEqual({ c: 0 });
  });
});
