/**
 * WS2 (#22) authoring parse — the litedbmodel authoring surface (SemanticBehavior
 * declaration + eager public-API path) lowers through ONE bc compile path to one internal
 * Component-graph IR, effect is derived from the graph, and the lower path auto-applies the
 * portability guard fail-closed (spec §2.4 / §7 / §9 / #22 AC).
 *
 * #141: the leaf vocabulary is the op-independent transport ({@link emitRead}/{@link emitWrite} →
 * ONE `executeSQL` node with assembled `sql`), NOT the retired per-op `Select`/`Insert` catalog. So
 * a read/write behavior lowers to an `executeSQL` node whose `write` intent DERIVES the CQRS effect,
 * and whose assembled `sql` is the v1-shaped statement — asserted here instead of the old catalog
 * `table`/`select` port internals.
 */

import { describe, it, expect } from 'vitest';
import Database from 'better-sqlite3';
import {
  PortabilityError,
} from 'behavior-contracts';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileEager,
  emitRead,
  emitWrite,
  executeBehavior,
  assertComponentGraphPortable,
  eq,
  ge,
  type In,
  type Recorded,
  type ComponentGraphIR,
} from '../../src/scp';

const L = components();

/** The sole `executeSQL` transport node of a component (the assembled read/write statement). */
function execNode(contract: { methods: Record<string, { component: { body: readonly unknown[] } }> }, method: string) {
  return contract.methods[method].component.body.find(
    (n) => typeof n === 'object' && n !== null && 'component' in n && (n as { component: string }).component === 'executeSQL',
  ) as { ports: { sql: string; write: boolean } };
}

// ── A read behavior (Select → executeSQL transport) ───────────────────────────────────
class ReadBehaviors extends SemanticBehavior {
  static columns = {
    posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT', created_at: 'TEXT' },
  };
  PostSearch($: In<{ id: number; since: string }>) {
    return emitRead(L, 'Select', {
      table: 'posts',
      select: ['id', 'author_id', 'title', 'created_at'],
      where: [eq($.id, $.id), ge($.created_at, $.since)],
      order: 'created_at DESC',
    }, 'sqlite');
  }
}

// ── A write behavior (Insert → executeSQL transport, write intent) ─────────────────────
class WriteBehaviors extends SemanticBehavior {
  CreatePost($: In<{ authorId: number; title: string }>) {
    return emitWrite(L, 'Insert', {
      table: 'posts',
      'values.author_id': $.authorId,
      'values.title': $.title,
      returning: 'id, title',
    }, 'sqlite');
  }
}

describe('WS2 authoring — read behavior lowers to one executeSQL transport node', () => {
  const contract = publishBehaviors(ReadBehaviors);

  it('publishes exactly the public methods as root components (name = method name)', () => {
    expect(contract.components.map((c) => c.name)).toEqual(['PostSearch']);
    expect(Object.keys(contract.methods)).toEqual(['PostSearch']);
  });

  it('assembles the v1-shaped SELECT into the executeSQL node `sql` (head + WHERE + tail)', () => {
    const node = execNode(contract, 'PostSearch');
    expect(node.ports.sql).toBe(
      'SELECT id, author_id, title, created_at FROM posts WHERE id = ? AND created_at >= ? ORDER BY created_at DESC',
    );
    expect(node.ports.write).toBe(false);
  });
});

describe('WS2 authoring — effect derivation (graph-derived from the executeSQL write intent, never authored)', () => {
  it('a read behavior (write:false) derives Query', () => {
    expect(publishBehaviors(ReadBehaviors).methods.PostSearch.effect).toBe('query');
  });

  it('a write behavior (write:true) derives Command', () => {
    expect(publishBehaviors(WriteBehaviors).methods.CreatePost.effect).toBe('command');
  });

  it('the write behavior assembles the INSERT … RETURNING statement (write intent)', () => {
    const node = execNode(publishBehaviors(WriteBehaviors), 'CreatePost');
    expect(node.ports.write).toBe(true);
    expect(node.ports.sql).toMatch(/^INSERT INTO posts/);
    expect(node.ports.sql).toMatch(/RETURNING id, title$/);
  });
});

describe('WS2 single compile path — eager public API ≡ SemanticBehavior declaration', () => {
  class EagerEquivDecl extends SemanticBehavior {
    static columns = { posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT', created_at: 'TEXT' } };
    FindPost($: In<{ id: number; since: string }>) {
      return emitRead(L, 'Select', {
        table: 'posts',
        select: ['id', 'author_id', 'title'],
        where: [eq($.id, $.id), ge($.created_at, $.since)],
        order: 'created_at DESC',
      }, 'sqlite');
    }
  }

  it('produces byte-identical internal Component-graph IR for an equivalent query', () => {
    const decl = publishBehaviors(EagerEquivDecl);
    // Eager path: the SAME authoring body, funneled through compileEager (spec §9).
    const eager = compileEager('FindPost', ($: Recorded, _l) =>
      emitRead(L, 'Select', {
        table: 'posts',
        select: ['id', 'author_id', 'title'],
        where: [eq($.id, $.id), ge($.created_at, $.since)],
        order: 'created_at DESC',
      }, 'sqlite'),
      { columns: { posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT', created_at: 'TEXT' } } },
    );
    expect(JSON.stringify(eager.components)).toBe(JSON.stringify(decl.components));
    expect(eager.methods.FindPost.effect).toBe('query');
    expect(decl.methods.FindPost.effect).toBe('query');
  });

  it('the eager path runs the SAME native control-syntax scan (fail-closed)', () => {
    // A native `?:` inside the eager body is rejected by bc's source scan, exactly as in a declaration.
    expect(() =>
      compileEager('Bad', ($: Recorded, _l) =>
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
        emitRead(L, 'Select', { table: 'posts', select: ($ as { flag?: unknown }).flag ? ['id'] : ['title'] }, 'sqlite'),
        { columns: { posts: { id: 'INTEGER', title: 'TEXT' } } },
      ),
    ).toThrow(/native control syntax/i);
  });
});

describe('WS2 guard auto-wiring — non-portable opcode is rejected fail-closed (#22 AC)', () => {
  it('assertComponentGraphPortable rejects an opcode outside bc closed set', () => {
    const ir: ComponentGraphIR = {
      irVersion: 1,
      exprVersion: 2,
      components: [
        {
          name: 'Bogus',
          inputPorts: { x: { type: 'unknown', required: true } },
          body: [{ id: 'n0', component: 'executeSQL', ports: { sql: 'SELECT 1', params: { arr: [{ sqlRaw: [{ ref: ['x'] }] }] }, write: false, returning: false, bigint: false } }],
          output: { ref: ['n0'] },
        },
      ],
    };
    expect(() => assertComponentGraphPortable(ir)).toThrow(PortabilityError);
    expect(() => assertComponentGraphPortable(ir)).toThrow(/sqlRaw/);
  });

  it('the lower path runs on a portable behavior without throwing', () => {
    expect(() => publishBehaviors(ReadBehaviors)).not.toThrow();
    expect(() => publishBehaviors(WriteBehaviors)).not.toThrow();
  });
});

describe('WS2 emitted IR is executable end-to-end (seam to WS3 — the leaf transport)', () => {
  it('the lowered read behavior executes through executeBehavior against real better-sqlite3', () => {
    const db = new Database(':memory:');
    db.exec('CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER, title TEXT, created_at TEXT)');
    db.prepare('INSERT INTO posts VALUES (?,?,?,?)').run(1, 7, 'Hello', '2026-01-02');
    const out = executeBehavior(publishBehaviors(ReadBehaviors), { id: 1, since: '2026-01-01' } as never, {
      db: db as never, entry: 'PostSearch', dialect: 'sqlite',
    });
    db.close();
    expect(out).toEqual([{ id: 1, author_id: 7, title: 'Hello', created_at: '2026-01-02' }]);
  });
});
