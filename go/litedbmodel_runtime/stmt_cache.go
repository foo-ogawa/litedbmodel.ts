// litedbmodel v2 SCP — prepared-statement caching (Go perf, #perf-go-stmt-cache).
//
// Go's database/sql PREPAREs a fresh server-side statement on EVERY db.Query / db.Exec /
// tx.Exec call and discards it immediately after — an extra network round-trip per statement on
// Postgres/MySQL, and a re-parse on SQLite. A real Go program prepares each unique SQL ONCE and
// reuses the *sql.Stmt handle (prepare-once, execute-many). This file adds that cache WITHOUT
// changing the SQL executed or the rows returned: the SAME statement text runs, only the prepared
// handle is reused, so conformance / livedb stay BYTE-IDENTICAL.
//
// Scoping (correctness — a prepared *sql.Stmt is bound to the handle it was prepared on):
//
//   - non-tx primary path (a *sql.DB): the cache lives for the *sql.DB's lifetime, keyed by SQL
//     text. A *sql.DB-level *sql.Stmt is goroutine-safe and database/sql re-prepares it per pooled
//     connection under the hood + caches per-conn — so the concurrent read fan-out (independent
//     sibling relations on distinct pooled conns, #40) shares ONE logical cache entry safely.
//
//   - tx-owned path (a *sql.Conn held for the whole transaction, Phase D #94): the cache lives for
//     the TX's lifetime (one owned connection), keyed by SQL text, prepared via conn.PrepareContext
//     so the handle is bound to THAT pinned connection. It is CLOSED when the tx connection is
//     released (the stmts must not outlive the conn) — see [txStmtCache.closeAll] wired into the tx
//     teardown. Concurrent transactions each own a DISTINCT *sql.Conn ⇒ a DISTINCT cache ⇒ no
//     cross-tx statement sharing (the Phase A-F concurrent-tx isolation is untouched).
//
//   - per-statement routing pool path (acquire → run → release a fresh *sql.Conn per statement,
//     Phase C #89): a stmt prepared there could not outlive the immediately-released connection, so
//     that path is left prepare-per-call (unchanged). It is NOT the hot path (the read fan-out and
//     the tx body are).

package litedbmodel_runtime

import (
	"context"
	"database/sql"
	"strings"
	"sync"
)

// cacheableStmt reports whether a rendered statement may be prepared+cached. A statement carrying
// RETURNING is EXCLUDED: the live MySQL path emulates RETURNING at the DRIVER-CONNECTION layer
// (scpMysqlConn.QueryContext strips RETURNING, runs the write, re-selects — see livedb.go), and a
// prepared *sql.Stmt executes via the driver's STATEMENT path (StmtQueryContext), which BYPASSES that
// conn-level interception. Caching a RETURNING statement would thus send raw `… RETURNING …` to MySQL
// (Error). RETURNING writes also run at most once per tx (negligible cache benefit). Excluding them
// keeps the cache dialect-agnostic AND byte-identical (pg/sqlite RETURNING still runs the exact SQL,
// just un-prepared). The hot path — read SELECTs + non-RETURNING writes — is still cached.
func cacheableStmt(query string) bool {
	// A cheap case-insensitive substring scan for the RETURNING keyword (bounded by a leading space to
	// avoid matching an identifier that merely ends in "returning"). The rendered SQL always spaces it.
	if containsFold(query, " returning ") || hasSuffixFold(query, " returning") {
		return false
	}
	// Transaction-control verbs (BEGIN/COMMIT/ROLLBACK/SET/SAVEPOINT/RELEASE) are the runtime's OWN tx
	// envelope, issued through the seam so middleware observes them (Phase D). They run at most ONCE per
	// tx (no reuse benefit) and some drivers treat a PREPAREd tx-control statement specially, so they
	// are executed directly (un-prepared) — byte-identical to the pre-cache path. Detected by the
	// leading verb of the trimmed statement.
	verb := leadingVerb(query)
	switch verb {
	case "begin", "commit", "rollback", "set", "savepoint", "release", "start":
		return false
	}
	return true
}

// leadingVerb returns the lowercased first whitespace-delimited token of a SQL statement (its verb).
func leadingVerb(query string) string {
	s := strings.TrimSpace(query)
	if i := strings.IndexAny(s, " \t\r\n"); i >= 0 {
		s = s[:i]
	}
	return strings.ToLower(s)
}

// containsFold is a case-insensitive strings.Contains (ASCII — the SQL keyword is ASCII).
func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), sub)
}

// hasSuffixFold is a case-insensitive strings.HasSuffix (ASCII).
func hasSuffixFold(s, suffix string) bool {
	if len(s) < len(suffix) {
		return false
	}
	return strings.EqualFold(s[len(s)-len(suffix):], suffix)
}

// dbStmtCache is the process-lifetime prepared-statement cache for ONE *sql.DB (the non-tx primary
// path). It is goroutine-safe (the read fan-out prepares/looks-up concurrently). Entries are never
// evicted: the bundle's SQL set is bounded (a fixed set of rendered statements per model/op), so the
// cache size is bounded by the app's distinct SQL — exactly the prepare-once, reuse-forever contract.
type dbStmtCache struct {
	db    *sql.DB
	mu    sync.RWMutex
	stmts map[string]*sql.Stmt
}

// dbCaches maps a *sql.DB to its statement cache so a fresh dbConnection{db} built per ConnectionFor
// call (the non-tx path builds one each statement) still shares ONE cache per underlying *sql.DB.
var (
	dbCachesMu sync.Mutex
	dbCaches   = map[*sql.DB]*dbStmtCache{}
)

// cacheForDB returns the shared [dbStmtCache] for db, creating it on first use. A nil db (or a
// non-*sql.DB SQLDB) has no cache — the caller falls back to the prepare-per-call path.
func cacheForDB(db SQLDB) *dbStmtCache {
	sqlDB, ok := db.(*sql.DB)
	if !ok || sqlDB == nil {
		return nil
	}
	dbCachesMu.Lock()
	defer dbCachesMu.Unlock()
	c := dbCaches[sqlDB]
	if c == nil {
		c = &dbStmtCache{db: sqlDB, stmts: map[string]*sql.Stmt{}}
		dbCaches[sqlDB] = c
	}
	return c
}

// CloseDBStmtCache drops (and closes every prepared *sql.Stmt in) the statement cache for db. Call it
// when a *sql.DB is being Close()d so the cache does not retain a dead handle / leak the map entry
// (database/sql closes the underlying stmts on db.Close anyway, but this releases the Go-side cache).
// A no-op for a db that has no cache. Safe to call more than once.
func CloseDBStmtCache(db *sql.DB) {
	if db == nil {
		return
	}
	dbCachesMu.Lock()
	c := dbCaches[db]
	delete(dbCaches, db)
	dbCachesMu.Unlock()
	if c == nil {
		return
	}
	c.mu.Lock()
	for k, st := range c.stmts {
		_ = st.Close()
		delete(c.stmts, k)
	}
	c.mu.Unlock()
}

// prepare returns the cached *sql.Stmt for query, preparing + caching it on first use. Concurrent
// callers that race to prepare the same NEW query both prepare, but only one entry is kept (the loser
// closes its extra handle) — so the map holds exactly one live stmt per SQL.
func (c *dbStmtCache) prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	c.mu.RLock()
	st := c.stmts[query]
	c.mu.RUnlock()
	if st != nil {
		return st, nil
	}
	st, err := c.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if existing := c.stmts[query]; existing != nil {
		// A concurrent caller won the race — keep theirs, drop ours.
		c.mu.Unlock()
		_ = st.Close()
		return existing, nil
	}
	c.stmts[query] = st
	c.mu.Unlock()
	return st, nil
}

// cacheWrapDB wraps the non-tx primary SQLDB (a *sql.DB) in a [cachedSQLDB] backed by the *sql.DB's
// shared statement cache. For a non-*sql.DB SQLDB (should not occur on the primary path) it returns
// the db unchanged, so queryRows / execWrite fall back to the uncached path (byte-identical).
func cacheWrapDB(db SQLDB) SQLDB {
	c := cacheForDB(db)
	if c == nil {
		return db
	}
	return dbCachedSQLDB{db: db.(*sql.DB), cache: c}
}

// dbCachedSQLDB is the non-tx primary connection as a [cachedSQLDB]: queryCached / execCached prepare
// (once) + reuse a *sql.DB-level *sql.Stmt for the SQL. The plain Query / Exec (the [SQLDB] surface)
// forward to the raw *sql.DB for any caller that does not take the cached branch.
type dbCachedSQLDB struct {
	db    *sql.DB
	cache *dbStmtCache
}

func (d dbCachedSQLDB) Query(query string, args ...any) (*sql.Rows, error) {
	return d.db.Query(query, args...)
}

func (d dbCachedSQLDB) Exec(query string, args ...any) (sql.Result, error) {
	return d.db.Exec(query, args...)
}

func (d dbCachedSQLDB) queryCached(query string, args []any) (*sql.Rows, error) {
	if !cacheableStmt(query) {
		rows, err := d.db.Query(query, args...) // RETURNING etc. — run un-prepared (byte-identical)
		if err != nil {
			return nil, mapSqliteError(err)
		}
		return rows, nil
	}
	st, err := d.cache.prepare(context.Background(), query)
	if err != nil {
		return nil, mapSqliteError(err)
	}
	rows, err := st.Query(args...)
	if err != nil {
		return nil, mapSqliteError(err)
	}
	return rows, nil
}

func (d dbCachedSQLDB) execCached(query string, args []any) (sql.Result, error) {
	if !cacheableStmt(query) {
		res, err := d.db.Exec(query, args...) // RETURNING etc. — run un-prepared (byte-identical)
		if err != nil {
			return nil, mapSqliteError(err)
		}
		return res, nil
	}
	st, err := d.cache.prepare(context.Background(), query)
	if err != nil {
		return nil, mapSqliteError(err)
	}
	res, err := st.Exec(args...)
	if err != nil {
		return nil, mapSqliteError(err)
	}
	return res, nil
}

// txStmtCache is the per-transaction prepared-statement cache with PREPARE-ON-REPEAT: ONE owned
// *sql.Conn, statements prepared on it (conn.PrepareContext) so every handle is bound to the pinned
// connection. It is NOT goroutine-safe by design — a transaction runs its statements sequentially on
// its single owned connection (the Phase A-F ownership model), so no lock is needed; a concurrent tx
// owns a DIFFERENT *sql.Conn + a DIFFERENT cache.
//
// Why prepare-on-repeat: a tx is short-lived (its owned conn returns to the pool at COMMIT/ROLLBACK),
// so a prepared stmt CANNOT be reused across transactions — only WITHIN the tx body. A statement used
// ONCE in the tx would pay a NET EXTRA round-trip if prepared (PrepareContext + Exec vs a pipelined
// ExecContext). So a statement runs DIRECTLY (uncached) on first sight and is only PREPARED+cached when
// it REPEATS within the same tx — zero regression for one-shot writes, the reuse win for an intra-tx
// loop of identical statements. Every cached stmt is CLOSED via [closeAll] at tx teardown (the handles
// must not outlive the connection they were prepared on).
type txStmtCache struct {
	conn  *sql.Conn
	seen  map[string]bool
	stmts map[string]*sql.Stmt
}

func newTxStmtCache(conn *sql.Conn) *txStmtCache {
	return &txStmtCache{conn: conn, seen: map[string]bool{}, stmts: map[string]*sql.Stmt{}}
}

// lookup returns the tx-cached *sql.Stmt for query if one is already prepared (a prior repeat within
// this tx), else (nil, false) — the caller then runs the statement DIRECTLY and records the sighting
// via [markSeen], so the NEXT occurrence prepares. This is the prepare-on-repeat gate.
func (c *txStmtCache) lookup(query string) (*sql.Stmt, bool) {
	st, ok := c.stmts[query]
	return st, ok
}

// prepareRepeat records a sighting of query and, if it has been seen BEFORE in this tx, prepares +
// caches it (returning the stmt); on first sight it records the sighting and returns (nil, nil) so the
// caller runs it directly. A prepare error is returned (the caller falls back to a direct exec).
func (c *txStmtCache) prepareRepeat(ctx context.Context, query string) (*sql.Stmt, error) {
	if !c.seen[query] {
		c.seen[query] = true
		return nil, nil // first sight — caller runs it directly
	}
	st, err := c.conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	c.stmts[query] = st
	return st, nil
}

// closeAll closes every cached prepared statement (called at tx teardown, BEFORE the owned *sql.Conn
// is released to the pool — the stmts are bound to that conn and must not leak). Errors are ignored
// (a poisoned connection's stmt-close can fail; the connection is being torn down regardless).
func (c *txStmtCache) closeAll() {
	if c == nil {
		return
	}
	for k, st := range c.stmts {
		_ = st.Close()
		delete(c.stmts, k)
	}
}
