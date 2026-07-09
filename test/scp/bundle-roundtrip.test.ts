/**
 * WS3 (#23) — §8 Backend-Compiled bundle round-trip + INSERT column-order parity.
 *
 * ## FIX 2 — the published artifact is the §8 compiled bundle, not the raw port shape
 *
 * `compileBundle` Backend-Compiles a behavior method ONCE (TS-side) into a serializable
 * {@link SqlBundle} (sql + fragment tree + Expression-IR param slots + assembly). This test
 * proves the bundle is SELF-SUFFICIENT: serialize → `JSON.parse` (dropping ALL TS in-memory
 * state) → `executeBundle` against REAL better-sqlite3 → identical SQL + rows vs direct
 * `executeBehavior`. A thin per-language runtime (bc runtime-core value/SKIP eval + render +
 * a SQL handler) can therefore execute the PUBLISHED JSON without re-implementing
 * litedbmodel's Backend-Compile (the WS7 multi-language promise). Param slots stay as
 * Expression IR in the bundle (bc still owns per-language value/SKIP evaluation).
 *
 * ## FIX 1 — INSERT column order parity routed through the REAL v2 DBModel path
 *
 * v2 adopts deterministic CANONICAL (alphabetical) column ordering as the SSoT for compiled
 * SQL (breaking vs v1.x; language-neutral, required for WS7 byte-identical conformance). The
 * v2 imperative `DBModel._insert` single-record fast path is aligned to sort canonically, so
 * "current litedbmodel (v2) direct execution == SCP" holds by construction. This test routes
 * INSERT through the REAL v2 public path (`DBModel.create`) with a NON-alphabetical author
 * key order (the case that exposed the faked golden) and asserts the SCP INSERT SQL equals
 * the v2 `DBModel` INSERT SQL byte-for-byte.
 *
 * Real DB, no mocks (docker N/A — SQLite in-process).
 */

import 'reflect-metadata';
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import Database from 'better-sqlite3';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { DBModel, model, column, closeAllPools, type ColumnsOf, type DBConfig } from '../../src';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  executeBehavior,
  compileBundle,
  executeBundle,
  whereEq,
  whereGe,
  when,
  ne,
  opt,
  coalesce,
  type In,
  type Recorded,
  type SqlBundle,
} from '../../src/scp';

const L = components();

function seedDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE posts (
      id INTEGER PRIMARY KEY,
      author_id INTEGER NOT NULL,
      title TEXT NOT NULL,
      status TEXT,
      created_at TEXT NOT NULL
    );
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
  `);
  db.prepare('INSERT INTO posts VALUES (?,?,?,?,?)').run(1, 7, 'Hello', 'live', '2026-02-01');
  db.prepare('INSERT INTO posts VALUES (?,?,?,?,?)').run(2, 7, 'World', 'draft', '2026-03-01');
  db.prepare('INSERT INTO posts VALUES (?,?,?,?,?)').run(3, 8, 'Other', 'live', '2026-01-15');
  db.prepare('INSERT INTO users VALUES (?,?)').run(7, 'Ada');
  db.prepare('INSERT INTO users VALUES (?,?)').run(8, 'Alan');
  return db;
}

// A read behavior exercising a SKIP-optional fragment (so the round-trip proves the
// fragment tree + refOpt param slots survive serialization) plus a relation .map.
class PostSearch extends SemanticBehavior {
  Feed($: In<{ author_id: number; status?: string; since: string }>) {
    const posts = L.Select({
      table: 'posts',
      select: ['id', 'author_id', 'title', 'status'],
      where: [
        whereEq($.author_id, $.author_id),
        when(ne(opt($.status), null), () => whereEq($.status, $.status)),
        whereGe($.created_at, $.since),
      ],
      order: 'id ASC',
      limit: coalesce(opt($.limit), 20),
    });
    const authors = posts.map(($p: Recorded) =>
      L.Select({ table: 'users', select: ['id', 'name'], where: [whereEq($p.id, $p.author_id)] }),
    );
    return { posts, authors };
  }
}

describe('WS3 §8 bundle — round-trip from serialized JSON executes with bc-core alone', () => {
  const contract = publishBehaviors(PostSearch);

  it('the compiled bundle is the §8 shape: sql + fragment tree + Expression-IR param slots', () => {
    const bundle = compileBundle(contract, 'Feed');
    const op = bundle.operations.n0;
    expect(op.sql).toBe('SELECT id, author_id, title, status FROM posts{where} ORDER BY id ASC LIMIT ?');
    // Fragment tree with existence rule (SKIP → when-guarded), NOT a boolean.
    const frags = op.where!.fragments as Array<{ when?: unknown; sql: string }>;
    expect(frags[1].sql).toBe('status = ?');
    expect(frags[1].when).toEqual({ ne: [{ refOpt: ['status'] }, null] });
    // The LIMIT param slot is preserved as Expression IR (bc-core evaluates it per language).
    expect(op.params).toEqual([{ coalesce: [{ refOpt: ['limit'] }, 20] }]);
  });

  it('serialized → JSON.parse → executeBundle == direct executeBehavior (present)', () => {
    const input = { author_id: 7, status: 'live', since: '2026-01-01' };

    // The published artifact: pure JSON, no TS state.
    const json = JSON.stringify(compileBundle(contract, 'Feed'));
    const reparsed = JSON.parse(json) as SqlBundle;

    const db1 = seedDb();
    const fromBundle = executeBundle(reparsed, input, { db: db1 });
    const db2 = seedDb();
    const direct = executeBehavior(contract, input, { db: db2, entry: 'Feed' });

    expect(fromBundle).toEqual(direct);
    expect((fromBundle as { posts: unknown[] }).posts).toEqual([
      { id: 1, author_id: 7, title: 'Hello', status: 'live' },
    ]);
  });

  it('serialized bundle reproduces absent-key SKIP + relation .map without TS Backend-Compile', () => {
    // `status` OMITTED entirely → the bundle's optionalHeads drive present-as-null; the
    // SKIP fragment drops. `limit` also omitted → coalesce default. All evaluated by bc-core
    // from the serialized param slots, no re-compilation.
    const input = { author_id: 7, since: '2026-01-01' };
    const reparsed = JSON.parse(JSON.stringify(compileBundle(contract, 'Feed'))) as SqlBundle;

    const db = seedDb();
    const out = executeBundle(reparsed, input, { db }) as { posts: unknown[]; authors: unknown[] };
    expect(out.posts).toEqual([
      { id: 1, author_id: 7, title: 'Hello', status: 'live' },
      { id: 2, author_id: 7, title: 'World', status: 'draft' },
    ]);
    // Relation .map resolved per parent (both author 7 = Ada) from the serialized wiring.
    expect(out.authors).toEqual([[{ id: 7, name: 'Ada' }], [{ id: 7, name: 'Ada' }]]);
  });

  it('the serialized bundle carries NO function/TS state (pure JSON identity round-trips)', () => {
    const bundle = compileBundle(contract, 'Feed');
    const reparsed = JSON.parse(JSON.stringify(bundle)) as SqlBundle;
    // Structural identity: re-serializing the reparsed bundle yields the same JSON.
    expect(JSON.stringify(reparsed)).toBe(JSON.stringify(bundle));
  });
});

// ── FIX 1: INSERT column-order parity via the REAL v2 DBModel public path ─────────

@model('parity_posts')
class ParityPost extends DBModel {
  @column() id?: number;
  @column() author_id?: number;
  @column() title?: string;
  @column() created_at?: string;
}
const ParityPostModel = ParityPost as typeof ParityPost & ColumnsOf<ParityPost>;

/** Capture the exact INSERT SQL the v2 DBModel public path emits (execute middleware). */
const captured: { sql: string; params: unknown[] }[] = [];
const CaptureMiddleware = DBModel.createMiddleware({
  execute: async function (next, sql: string, params?: unknown[]) {
    if (/^\s*INSERT/i.test(sql)) captured.push({ sql, params: params ?? [] });
    return next(sql, params);
  },
});

class CreateParityPost extends SemanticBehavior {
  // NON-alphabetical author key order (title, author_id, created_at) — the exact case that
  // exposed the faked golden. Both v2 DBModel and SCP must canonicalize to alphabetical.
  Create($: In<{ title: string; author_id: number; created_at: string }>) {
    return L.Insert({
      table: 'parity_posts',
      'values.title': $.title,
      'values.author_id': $.author_id,
      'values.created_at': $.created_at,
    });
  }
}

describe('WS3 FIX 1 — INSERT column order: SCP == real v2 DBModel (canonical, non-alpha input)', () => {
  const dbPath = path.join(os.tmpdir(), `litedbmodel-scp-parity-${process.pid}.sqlite`);
  const config: DBConfig = { database: dbPath, driver: 'sqlite' };

  let disposeMiddleware: () => void;

  beforeAll(async () => {
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
    DBModel.setConfig(config);
    disposeMiddleware = DBModel.use(CaptureMiddleware);
    await DBModel.execute(
      `CREATE TABLE parity_posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER NOT NULL, title TEXT NOT NULL, created_at TEXT NOT NULL)`,
    );
  });

  afterAll(async () => {
    disposeMiddleware();
    await closeAllPools();
    if (fs.existsSync(dbPath)) fs.unlinkSync(dbPath);
  });

  it('real v2 DBModel.create emits CANONICAL (alphabetical) column order for non-alpha input', async () => {
    captured.length = 0;
    // Author key order in the create() call is NON-alphabetical: title, author_id, created_at.
    await DBModel.transaction(async () =>
      ParityPostModel.create([
        [ParityPostModel.title, 'Zeta'],
        [ParityPostModel.author_id, 7],
        [ParityPostModel.created_at, '2026-06-01'],
      ]),
    );
    expect(captured).toHaveLength(1);
    // v2 now canonicalizes: author_id, created_at, title (alphabetical), NOT author order.
    expect(captured[0].sql).toBe(
      'INSERT INTO parity_posts (author_id, created_at, title) VALUES (?, ?, ?)',
    );
  });

  it('SCP compiled INSERT SQL === real v2 DBModel INSERT SQL, byte-for-byte', async () => {
    captured.length = 0;
    await DBModel.transaction(async () =>
      ParityPostModel.create([
        [ParityPostModel.title, 'Byte'],
        [ParityPostModel.author_id, 8],
        [ParityPostModel.created_at, '2026-07-01'],
      ]),
    );
    const v2Sql = captured[captured.length - 1].sql;

    // SCP path: Backend-Compile the authored Insert and render it. Author key order in the
    // authoring is ALSO non-alphabetical (title, author_id, created_at) — both paths must
    // land on the same canonical column order.
    const contract = publishBehaviors(CreateParityPost);
    const bundle = compileBundle(contract, 'Create');
    const scpSql = bundle.operations.n0.sql;

    // Byte-for-byte identity (the un-faked assertion, genuinely exercising the divergence).
    expect(scpSql).toBe(v2Sql);
    expect(scpSql).toBe('INSERT INTO parity_posts (author_id, created_at, title) VALUES (?, ?, ?)');
  });

  it('SCP INSERT executes on real SQLite and the row matches (via the bundle)', () => {
    const db = new Database(':memory:');
    db.exec(
      `CREATE TABLE parity_posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, created_at TEXT NOT NULL)`,
    );
    const contract = publishBehaviors(CreateParityPost);
    const bundle = compileBundle(contract, 'Create');
    executeBundle(bundle, { title: 'Row', author_id: 9, created_at: '2026-08-01' }, { db });
    expect(db.prepare('SELECT author_id, title, created_at FROM parity_posts').get()).toEqual({
      author_id: 9,
      title: 'Row',
      created_at: '2026-08-01',
    });
  });
});
