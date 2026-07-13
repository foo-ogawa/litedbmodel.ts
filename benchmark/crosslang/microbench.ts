// ════════════════════════════════════════════════════════════════════════════
// I/O-excluded micro-bench mock driver (epic #44) — the LOAD-BEARING signal.
// ════════════════════════════════════════════════════════════════════════════
//
// The micro-bench swaps the real better-sqlite3 driver for a mock that returns
// FIXED rows with NO DB round-trip, so the timed op is ONLY the client-side path
// (compile/render/param-eval/bind/`?`→`$N`/hydration). The mock returns identical
// fixtures for every impl (sql/ir/codegen/dynamic/prepared) and every language, so
// unmarshal/hydrate cost is directly comparable. This is where the exec-surface
// differences (JSON-interpret vs baked-IR vs recompile-per-call) become visible.

// A fixed row set sized to the shared domain (author_id=1 → 5 posts, 25 comments).
const POSTS = Array.from({ length: 5 }, (_, i) => ({
  id: i + 1, author_id: 1, title: `post-${i + 1}`, status: 'live', views: (i + 1) * 10, created_at: '2026-02-01',
}));
const COMMENTS = Array.from({ length: 25 }, (_, i) => ({ id: i + 1, post_id: (i % 5) + 1, body: `comment-${i + 1}` }));
const USERS = [{ id: 1, name: 'user-1' }];

// Pick the fixed result rows for a rendered SQL statement by its shape (table +
// SELECT/INSERT/UPDATE). Deterministic — same statement ⇒ same fixture.
function fixtureFor(sql: string): Record<string, unknown>[] {
  const s = sql.toLowerCase();
  if (/^\s*select/.test(s)) {
    if (s.includes('from comments')) return COMMENTS;
    if (s.includes('from users')) return USERS;
    if (s.includes('from posts')) return POSTS;
    if (s.includes('from ')) return POSTS; // subquery/window forms → posts-shaped
    return [{ '1': 1 }]; // gate `SELECT 1`
  }
  if (/returning/i.test(sql)) return [{ id: 41, author_id: 1, title: 'txn-post' }];
  return [];
}

export interface MockDb {
  prepare(sql: string): { all(...p: unknown[]): unknown[]; get(...p: unknown[]): unknown; run(...p: unknown[]): { changes: number; lastInsertRowid: number } };
  transaction<T extends (...args: any[]) => any>(fn: T): T;
}

// A mock db honoring the runtime's `prepare().{all,get,run}` + `transaction`
// surface with NO real SQLite. `all`/`get` return the fixed fixture; `run` reports
// 1 change (so gate `insertedElseRollback` passes and RETURNING rows exist).
// `transaction(fn)` runs `fn` inline (no BEGIN/COMMIT — pure client-side timing).
export function mockDb(): MockDb {
  return {
    prepare(sql: string) {
      const rows = fixtureFor(sql);
      return {
        all: () => rows,
        get: () => rows[0],
        run: () => ({ changes: 1, lastInsertRowid: 41 }),
      };
    },
    transaction<T extends (...args: any[]) => any>(fn: T): T {
      return ((...args: any[]) => fn(...args)) as T;
    },
  };
}
