/**
 * Static-symbolic makeSQL bundle — single-statement WRITE path (epic #43/#45 Phase B). A
 * standalone Insert/Update/Delete authored query compiles (symbolically) to ONE makeSQL
 * statement via the SAME `compileWriteNode` the write-tx spine uses, and executes per-input
 * with deferred value-specs. Asserts the RETURNING rows AND the persisted DB state (the v2
 * write SSoT: canonical alphabetical INSERT column order, matching DBModel._insert).
 */

import { describe, it, expect, beforeEach } from 'vitest';
import Database from 'better-sqlite3';
import {
  SemanticBehavior,
  components,
  publishBehaviors,
  compileStaticBundle,
  executeStaticWrite,
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
  Create($: In<{ author_id: number; title: string }>) {
    return L.Insert({
      table: 'posts',
      'values.author_id': $.author_id,
      'values.title': $.title,
      returning: 'id, author_id, title',
    });
  }
  Rename($: In<{ id: number; title: string }>) {
    return L.Update({ table: 'posts', 'set.title': $.title, where: [whereEq($.id, $.id)], returning: 'id, title' });
  }
  Remove($: In<{ id: number }>) {
    return L.Delete({ table: 'posts', where: [whereEq($.id, $.id)], returning: 'id' });
  }
}

const contract = publishBehaviors(PostCommands);

describe('static-symbolic makeSQL bundle — single-statement writes (real SQLite)', () => {
  let db: InstanceType<typeof Database>;
  beforeEach(() => {
    db = freshDb();
  });

  it('compile is symbolic and yields ONE makeSQL statement per write', () => {
    const bundle = compileStaticBundle(contract, 'sqlite', 'Create');
    expect(bundle.statements).toHaveLength(1);
    // Deferred value-specs (bc Expression IR refs), not concrete values.
    expect(bundle.statements[0].params.length).toBeGreaterThan(0);
  });

  it('Insert RETURNING yields the new row and persists it (canonical column order)', () => {
    const bundle = compileStaticBundle(contract, 'sqlite', 'Create');
    const rows = executeStaticWrite(bundle, { author_id: 8, title: 'Fresh' }, db);
    expect(rows).toEqual([{ id: 2, author_id: 8, title: 'Fresh' }]);
    expect(db.prepare('SELECT author_id, title FROM posts WHERE id = 2').get()).toEqual({ author_id: 8, title: 'Fresh' });
  });

  it('Update RETURNING mutates the row', () => {
    const bundle = compileStaticBundle(contract, 'sqlite', 'Rename');
    const rows = executeStaticWrite(bundle, { id: 1, title: 'Renamed' }, db);
    expect(rows).toEqual([{ id: 1, title: 'Renamed' }]);
    expect(db.prepare('SELECT title FROM posts WHERE id = 1').get()).toEqual({ title: 'Renamed' });
  });

  it('Delete RETURNING removes the row', () => {
    const bundle = compileStaticBundle(contract, 'sqlite', 'Remove');
    const rows = executeStaticWrite(bundle, { id: 1 }, db);
    expect(rows).toEqual([{ id: 1 }]);
    expect(db.prepare('SELECT COUNT(*) c FROM posts WHERE id = 1').get()).toEqual({ c: 0 });
  });
});
