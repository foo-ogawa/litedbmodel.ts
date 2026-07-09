/**
 * #38 — single-field `emits` payload derives + executes end-to-end (real better-sqlite3).
 *
 * The WS5 write path emits each `emits` payload as a bc `{obj:{…}}` param (see write-plan
 * compileEmit). A SINGLE-field payload — `w.event('PostCreated', 'outbox', { postId: … })` —
 * was pre-#38 rejected at DERIVATION time by the portability guard (the lone field name read as
 * an unknown opcode). WS8a sidestepped this with two-field payloads. This test proves the
 * single-field payload now (a) derives, and (b) executes against a REAL in-memory SQLite DB with
 * the correct JSON persisted to the outbox — the exact shape #38 must unblock.
 */

import { beforeEach, describe, expect, it } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  entityWrites,
  compileWriteBundle,
  executeCommand,
  type In,
  type EntityWritesDefinition,
} from '../../src/scp';

const L = components();

class PostCommands extends SemanticBehavior {
  Create($: In<{ author_id: number; title: string; request_id: string }>) {
    return L.Insert({
      table: 'posts',
      'values.author_id': $.author_id,
      'values.title': $.title,
      returning: 'id, author_id, title',
    });
  }
}

// The save contract with a SINGLE-FIELD emit payload — the #38 target shape.
const postWrites: EntityWritesDefinition = entityWrites<PostCommands>((w) => ({
  create: w.lifecycle({
    emits: [w.event('PostCreated', 'outbox', { postId: '$.entity.id' })],
  }),
}));

const contract = publishBehaviors(PostCommands);

function freshDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.pragma('foreign_keys = ON');
  db.exec(`
    CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE posts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      author_id INTEGER NOT NULL REFERENCES users(id),
      title TEXT NOT NULL
    );
    CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, payload TEXT NOT NULL);
  `);
  db.prepare('INSERT INTO users (id, name) VALUES (?, ?)').run(7, 'Ada');
  return db;
}

describe('#38 — single-field emit payload derives + executes end-to-end', () => {
  let db: InstanceType<typeof Database>;
  beforeEach(() => {
    db = freshDb();
  });

  it('a single-field {obj:{postId}} emit DERIVES without a portability error', () => {
    // Pre-#38 this threw PortabilityError('postId' is not a portable operator) at derive time.
    const bundle = compileWriteBundle(contract, 'Create', postWrites, 'create');
    const plan = bundle.transaction!;
    expect(plan.statements.map((s) => s.role)).toEqual(['body', 'emit']);
    const emit = plan.statements.find((s) => s.role === 'emit')!;
    // The emit param is a single-field obj field map (postId only).
    const payloadParam = emit.op.params[1] as { obj: Record<string, unknown> };
    expect(Object.keys(payloadParam.obj)).toEqual(['postId']);
  });

  it('EXECUTES against real SQLite: outbox row carries the single-field JSON payload', () => {
    const result = executeCommand(
      contract,
      postWrites,
      'create',
      { author_id: 7, title: 'Hello', request_id: 'r-1' },
      { db, entry: 'Create' },
    );

    expect(result.committed).toBe(true);
    expect(result.entity).toEqual({ id: 1, author_id: 7, title: 'Hello' });

    // Body persisted.
    expect(db.prepare('SELECT id, author_id, title FROM posts').all()).toEqual([
      { id: 1, author_id: 7, title: 'Hello' },
    ]);
    // emit: the outbox row carries the JSON payload built from the single `$.entity.id` field.
    const outbox = db.prepare('SELECT type, payload FROM outbox').get() as { type: string; payload: string };
    expect(outbox.type).toBe('PostCreated');
    expect(JSON.parse(outbox.payload)).toEqual({ postId: 1 });
  });
});
