/**
 * Single-statement WRITE path on the op-independent `executeSQL` leaf (#141/#143). A standalone
 * Insert/Update/Delete authored via `emitWrite` compiles to ONE `executeSQL` write leaf (the base
 * write's tuned SQL via the SAME `compileWriteNode` the write-tx spine uses) and executes per-input
 * via `executeBehavior` (bc `bindBehaviors` → the leaf transport). Asserts the RETURNING rows AND the
 * persisted DB state (the v2 write SSoT: canonical alphabetical INSERT column order, matching
 * DBModel._insert). The retired `compileStaticBundle`/`executeStaticWrite` static-bundle path is gone.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  executeBehavior,
  emitWrite,
  whereEq,
  type In,
} from '../../src/scp';

const L = components();

function freshDb(): InstanceType<typeof Database> {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE posts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      author_id INTEGER NOT NULL,
      title TEXT NOT NULL
    );
  `);
  db.prepare('INSERT INTO posts (author_id, title) VALUES (?, ?)').run(7, 'Existing');
  return db;
}

class PostCommands extends SemanticBehavior {
  static columns = { posts: { id: 'INTEGER', author_id: 'INTEGER', title: 'TEXT' } };
  Create($: In<{ author_id: number; title: string }>) {
    return emitWrite(L, 'Insert', {
      table: 'posts',
      'values.author_id': $.author_id,
      'values.title': $.title,
      returning: 'id, author_id, title',
    }, 'sqlite');
  }
  Rename($: In<{ id: number; title: string }>) {
    return emitWrite(L, 'Update', { table: 'posts', 'set.title': $.title, where: [whereEq($.id, $.id)], returning: 'id, title' }, 'sqlite');
  }
  Remove($: In<{ id: number }>) {
    return emitWrite(L, 'Delete', { table: 'posts', where: [whereEq($.id, $.id)], returning: 'id' }, 'sqlite');
  }
}

const contract = publishBehaviors(PostCommands);

describe('single-statement writes on the executeSQL leaf (real SQLite)', () => {
  let db: InstanceType<typeof Database>;
  beforeEach(() => {
    db = freshDb();
  });

  it('a write compiles to ONE executeSQL write leaf (write:true) with deferred value-specs', () => {
    const body = contract.methods.Create.component.body;
    const writeLeaves = body.filter(
      (n) =>
        !('cond' in n) &&
        !('map' in n) &&
        (n as { component?: string }).component === 'executeSQL' &&
        (n as { ports?: { write?: unknown } }).ports?.write === true,
    );
    expect(writeLeaves).toHaveLength(1);
    // Deferred value-specs (bc Expression IR refs), not concrete values, ride the `params` port.
    const params = (writeLeaves[0] as { ports: { params?: { arr?: unknown[] } } }).ports.params?.arr ?? [];
    expect(params.length).toBeGreaterThan(0);
  });

  it('Insert RETURNING yields the new row and persists it (canonical column order)', () => {
    const rows = executeBehavior(contract, { author_id: 8, title: 'Fresh' }, { db, entry: 'Create' });
    expect(rows).toEqual([{ id: 2, author_id: 8, title: 'Fresh' }]);
    expect(db.prepare('SELECT author_id, title FROM posts WHERE id = 2').get()).toEqual({ author_id: 8, title: 'Fresh' });
  });

  it('Update RETURNING mutates the row', () => {
    const rows = executeBehavior(contract, { id: 1, title: 'Renamed' }, { db, entry: 'Rename' });
    expect(rows).toEqual([{ id: 1, title: 'Renamed' }]);
    expect(db.prepare('SELECT title FROM posts WHERE id = 1').get()).toEqual({ title: 'Renamed' });
  });

  it('Delete RETURNING removes the row', () => {
    const rows = executeBehavior(contract, { id: 1 }, { db, entry: 'Remove' });
    expect(rows).toEqual([{ id: 1 }]);
    expect(db.prepare('SELECT COUNT(*) c FROM posts WHERE id = 1').get()).toEqual({ c: 0 });
  });
});
