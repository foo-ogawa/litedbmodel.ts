// litedbmodel v2 SCP — the **connection routing + config contract** (Phase C / #89, go port).
//
// The Go mirror of the TS contract-defining artifact `src/scp/connection-routing.ts` (#87) and the
// rust port `rust/litedbmodel_runtime/src/connection_routing.rs` (#88). It builds ON the Phase A
// [ExecutionContext] seam (exec_context.go) + the Phase A/B owned-connection tx runtime; it does NOT
// re-implement the seam — it supplies the pieces [ExecutionContext.ConnectionFor] uses to complete
// its resolution steps 2-4 (step 1, the tx-pin, stays in exec_context.go — only the ctx holds the
// pinned *sql.Tx).
//
// # The connectionFor(intent) resolution order (design §3, v1 DBModel.ts:313 parity)
//
// A statement's connection is resolved in THIS priority (first match wins):
//  1. active tx connection — inside a transaction, always the tx-owned connection (Phase A, resolved
//     in exec_context.go BEFORE this module is consulted).
//  2. writer scope / writer-sticky — inside [WithWriter], or within writerStickyDuration after a
//     transaction (read-your-writes), a READ goes to the WRITER pool (Phase C — here).
//  3. read=reader / write=writer — otherwise a read goes to the reader pool, a write to the writer
//     pool (reader/writer separation; single-pool config ⇒ reader === writer, Phase C).
//  4. named-DB routing — the target pool is selected by intent.DB (the connection NAME the
//     bundle/model metadata carries — decorator-free; decorator wiring is Phase F) against the
//     [ConnectionRegistry]; absent ⇒ the DEFAULT connection. Named-DB selection happens FIRST (it
//     picks WHICH connection's reader/writer split steps 2-3 apply to).
//
// # Backward-compat (the hard constraint)
//
// Single DB, reader === writer (one pool), empty config, unnamed connection ⇒ BYTE-IDENTICAL to the
// Phase A/B single-pool behavior. A ctx with NO [RoutingConfig] (the Phase A [ContextForDB] path)
// never touches this module — [ExecutionContext.ConnectionFor] returns the primary-db connection
// exactly as before. A [ConnectionRegistry] built from ONE pool routes every intent to that ONE
// pool, and the writer-sticky clock only ever diverts to a pool that is the SAME object.
//
// # The go pool model (honest deviation from TS)
//
// TS's AsyncConnectionPool wraps a pg.Pool / mysql2 pool. Go's *sql.DB IS ALREADY a goroutine-safe
// connection pool; the sizing knobs (SetMaxOpenConns / SetMaxIdleConns / SetConnMax{Lifetime,IdleTime})
// are set on the *sql.DB AFTER sql.Open — so the [PoolFactory] builds a CONFIGURED *sql.DB and applies
// those at construction (NOT on a pre-built shared *sql.DB — that was the #87 blocker). One acquired
// "connection" is a *sql.Conn (db.Conn(ctx)), which pins ONE physical connection for the session
// statements (search_path / statement_timeout) to apply + reset on release — the *sql.DB pool cap
// (maxPool) is the sole bound on how many *sql.Conn are live at once.

package litedbmodel_runtime

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// ── The runtime config (C3) — mirrors v1 DBConfig/DBConfigOptions ──────────────

// ConnectionConfig is the per-connection database config (C3) — the knobs a pool is built with.
// Mirrors the TS ConnectionConfig (connection-routing.ts): connection target + pool sizing +
// per-statement/keepalive/session knobs. Every field has a documented default via
// [ResolveConnectionConfig]; the go field names + defaults mirror the TS contract. This is a DATA
// contract — it describes how to BUILD a pool; the actual sql.Open / SetMaxOpenConns construction
// lives in a [PoolFactory].
//
// Go has no optional-field type, so sizing/keepalive/queryTimeout use a pointer for "unset vs. 0"
// (a *int of nil ⇒ apply the default; &0 ⇒ an explicit 0). SearchPath/Charset use "" for unset
// (an empty session knob emits nothing anyway, so "" and unset are indistinguishable — matching the
// TS `undefined` behavior).
type ConnectionConfig struct {
	// Driver dialect for this connection. Default "postgres".
	Driver string
	// Host / Port / Database / User / Password — connection target (server-based dialects; Database is
	// the file path for sqlite). "" / 0 ⇒ omitted (the driver DSN default).
	Host     string
	Port     int
	Database string
	User     string
	Password string
	// QueryTimeout is the per-statement timeout in MILLISECONDS, applied as a session statement_timeout
	// (PG) / max_execution_time (MySQL) so a runaway query is aborted by the SERVER. nil / 0 ⇒ no
	// statement timeout (the engine default). Default 0.
	QueryTimeout *int
	// KeepAlive enables TCP-style keepalive on pooled connections (mapped to SetConnMaxIdleTime /
	// SetConnMaxLifetime in the factory — go's database/sql has no raw TCP-keepalive knob, so the
	// keepalive intent is expressed as a max idle/lifetime bound; a documented per-driver deviation).
	// nil / false ⇒ disabled. Default false.
	KeepAlive *bool
	// KeepAliveInitialDelayMillis is the ms before the first keepalive probe (maps to the max idle time
	// bound). nil ⇒ 10000 (when KeepAlive). Default 10000.
	KeepAliveInitialDelayMillis *int
	// MinPool is the minimum pooled connections kept warm (SetMaxIdleConns). nil / 0 ⇒ 0. Default 0.
	MinPool *int
	// MaxPool is the maximum pooled connections (SetMaxOpenConns — the SOLE cap source). nil ⇒ 10.
	// Default 10.
	MaxPool *int
	// SearchPath is the PG search_path set on each pooled connection at checkout (schema routing).
	// "" ⇒ unset (no session statement).
	SearchPath string
	// Charset is the MySQL connection charset / PG client_encoding set on each pooled connection.
	// "" ⇒ unset.
	Charset string
}

// ResolvedConnectionConfig is the defaults-applied config the pool builder consumes — no unset holes
// on the knobs (mirrors the TS ResolvedConnectionConfig). SearchPath/Charset keep "" for unset (an
// empty session knob emits nothing).
type ResolvedConnectionConfig struct {
	Driver                      string
	Host                        string
	Port                        int
	Database                    string
	User                        string
	Password                    string
	QueryTimeout                int
	KeepAlive                   bool
	KeepAliveInitialDelayMillis int
	MinPool                     int
	MaxPool                     int
	SearchPath                  string
	Charset                     string
}

// ResolveConnectionConfig applies the C3 defaults (QueryTimeout=0, KeepAlive=false, MinPool=0,
// MaxPool=10, KeepAliveInitialDelayMillis=10000, Driver="postgres") — mirrors the TS
// resolveConnectionConfig.
func ResolveConnectionConfig(config ConnectionConfig) ResolvedConnectionConfig {
	driver := config.Driver
	if driver == "" {
		driver = "postgres"
	}
	intOr := func(p *int, def int) int {
		if p == nil {
			return def
		}
		return *p
	}
	boolOr := func(p *bool, def bool) bool {
		if p == nil {
			return def
		}
		return *p
	}
	return ResolvedConnectionConfig{
		Driver:                      driver,
		Host:                        config.Host,
		Port:                        config.Port,
		Database:                    config.Database,
		User:                        config.User,
		Password:                    config.Password,
		QueryTimeout:                intOr(config.QueryTimeout, 0),
		KeepAlive:                   boolOr(config.KeepAlive, false),
		KeepAliveInitialDelayMillis: intOr(config.KeepAliveInitialDelayMillis, 10000),
		MinPool:                     intOr(config.MinPool, 0),
		MaxPool:                     intOr(config.MaxPool, 10),
		SearchPath:                  config.SearchPath,
		Charset:                     config.Charset,
	}
}

// SessionStatements is the per-dialect session statements a connection runs at checkout to honor a
// [ResolvedConnectionConfig] (in order). Pure (no connection contact) so it is testable in isolation
// — mirrors the TS sessionStatements:
//
//   - statement timeout (QueryTimeout > 0): PG `SET statement_timeout = <ms>`; MySQL
//     `SET SESSION max_execution_time = <ms>` (both server-side, ms).
//   - SearchPath: PG `SET search_path TO <path>`; MySQL has no schema search path ⇒ ignored.
//   - Charset: MySQL `SET NAMES <charset>`; PG `SET client_encoding TO <charset>`.
//
// A knob with no value emits nothing (⇒ empty for an all-default config ⇒ the session is untouched,
// backward-compatible). sqlite has no server session ⇒ empty.
func SessionStatements(config ResolvedConnectionConfig) []string {
	out := []string{}
	dialect := config.Driver
	if dialect == "sqlite" {
		return out
	}
	if config.QueryTimeout > 0 {
		if dialect == "postgres" {
			out = append(out, fmt.Sprintf("SET statement_timeout = %d", config.QueryTimeout))
		} else {
			out = append(out, fmt.Sprintf("SET SESSION max_execution_time = %d", config.QueryTimeout))
		}
	}
	if config.SearchPath != "" && dialect == "postgres" {
		out = append(out, fmt.Sprintf("SET search_path TO %s", config.SearchPath))
	}
	if config.Charset != "" {
		if dialect == "mysql" {
			out = append(out, fmt.Sprintf("SET NAMES %s", config.Charset))
		} else {
			out = append(out, fmt.Sprintf("SET client_encoding TO %s", config.Charset))
		}
	}
	return out
}

// SessionResetStatements is the RESET statements that undo [SessionStatements] on release (per
// dialect), so a session knob set for THIS configured connection does NOT leak to the next caller
// that draws the SAME underlying pooled *sql.Conn — go's database/sql does NOT auto-reset session
// state when a *sql.Conn is closed back to the pool. RESET / SET … DEFAULT restores the server
// default. Only the knobs config actually set are reset (an all-default config ⇒ nothing to reset).
// Mirrors the TS sessionResetStatements.
func SessionResetStatements(config ResolvedConnectionConfig) []string {
	out := []string{}
	dialect := config.Driver
	if dialect == "sqlite" {
		return out
	}
	if config.QueryTimeout > 0 {
		if dialect == "postgres" {
			out = append(out, "RESET statement_timeout")
		} else {
			out = append(out, "SET SESSION max_execution_time = DEFAULT")
		}
	}
	if config.SearchPath != "" && dialect == "postgres" {
		out = append(out, "RESET search_path")
	}
	if config.Charset != "" {
		if dialect == "mysql" {
			out = append(out, "SET NAMES DEFAULT")
		} else {
			out = append(out, "RESET client_encoding")
		}
	}
	return out
}

// ── The owned-connection pool seam (C1 substrate) ──────────────────────────────

// PooledConn is ONE acquired, owned connection — a [Connection] (Execute/Run through the SAME
// queryRows / execWrite the seam uses) plus the release hook the pool needs. Outside a tx the routing
// path acquires one per statement (acquire → run → release), mirroring the TS per-statement owned
// wrapper. It wraps a *sql.Conn (a pinned physical connection) so session statements apply + reset on
// THIS connection.
type PooledConn interface {
	Connection
}

// Pool hands out an OWNED connection per Acquire and takes it back on Release — the go analogue of
// the TS AsyncConnectionPool. This is the substrate for per-statement / per-tx connection ownership:
// the routing path Acquires one owned connection, runs a statement on it, then Releases it. Concurrent
// statements Acquire DISTINCT connections (bounded by the *sql.DB MaxOpenConns cap) ⇒ isolation.
type Pool interface {
	// Acquire checks out one owned connection for the caller's exclusive use.
	Acquire() (PooledConn, error)
	// Release returns a connection to the pool. destroy ⇒ drop it (a poisoned/aborted connection).
	Release(conn PooledConn, destroy bool) error
}

// sqlConnPooled adapts a pinned *sql.Conn to [PooledConn]: Execute/Run route through the SAME
// queryRows / execWrite as the primary db path (byte-identical), scoped to this ONE physical
// connection.
type sqlConnPooled struct {
	conn *sql.Conn
}

func (c *sqlConnPooled) Execute(query string, args []any) ([]bc.Value, error) {
	return queryRows(sqlConnDB{c.conn}, query, args)
}

func (c *sqlConnPooled) Run(query string, args []any) (RunInfo, error) {
	changes, lastInsert, err := execWrite(sqlConnDB{c.conn}, query, args)
	if err != nil {
		return RunInfo{}, err
	}
	return RunInfo{Changes: changes, LastInsertRowid: lastInsert}, nil
}

// sqlConnDB adapts a *sql.Conn to the minimal [SQLDB] surface (Query/Exec) so queryRows / execWrite
// run on the pinned connection. It uses the non-Context Query/Exec forms (via context.Background)
// to stay byte-identical to the primary-db path (which likewise uses *sql.DB.Query/Exec).
type sqlConnDB struct{ conn *sql.Conn }

func (c sqlConnDB) Query(query string, args ...any) (*sql.Rows, error) {
	return c.conn.QueryContext(context.Background(), query, args...)
}

func (c sqlConnDB) Exec(query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(context.Background(), query, args...)
}

// SQLDBPool is a [Pool] over a *sql.DB (go's built-in pool). Acquire pins a fresh *sql.Conn
// (db.Conn), Release closes it back to the pool. The *sql.DB's SetMaxOpenConns is the SOLE cap on how
// many *sql.Conn are live at once. This is the concrete pool a [PoolFactory] returns after applying
// the sizing knobs on the *sql.DB at construction.
type SQLDBPool struct {
	db *sql.DB
}

// NewSQLDBPool wraps a (constructed, sized) *sql.DB as a [Pool].
func NewSQLDBPool(db *sql.DB) *SQLDBPool { return &SQLDBPool{db: db} }

// Acquire pins one physical connection (db.Conn) for exclusive use — bounded by the *sql.DB's
// SetMaxOpenConns cap (Acquire BLOCKS once the cap is reached until a Release frees one, exactly the
// "maxPool is the sole cap" contract).
func (p *SQLDBPool) Acquire() (PooledConn, error) {
	conn, err := p.db.Conn(context.Background())
	if err != nil {
		return nil, mapSqliteError(err)
	}
	return &sqlConnPooled{conn: conn}, nil
}

// Release closes the pinned *sql.Conn back to the *sql.DB pool. destroy is honored via
// Conn.Close (go returns a healthy conn to the pool on Close and drops a broken one automatically);
// a poisoned connection is dropped by the driver on the next use, so Close is the correct release for
// both cases.
func (p *SQLDBPool) Release(conn PooledConn, destroy bool) error {
	sc, ok := conn.(*sqlConnPooled)
	if !ok {
		return nil
	}
	_ = destroy // go's *sql.Conn.Close returns a clean conn to the pool + drops a broken one; no separate destroy path.
	return sc.conn.Close()
}

// ── The configured pool wrapper (C3) — session statements + reset-on-release ───

// configuredPool wraps a [Pool] so every Acquire first runs the [SessionStatements] for config
// (statement timeout / search_path / charset) and every Release runs [SessionResetStatements] to
// restore the server defaults (so a pooled *sql.Conn never leaks THIS config's session state to the
// next caller). A config with NO session knobs (all defaults) ⇒ ZERO extra statements ⇒ a transparent
// passthrough (backward-compat). Mirrors the TS configuredPool.
//
// If a session statement itself fails, the connection is Released as DESTROYED (a mis-configured
// connection never re-enters use). On Release, a DESTROYED connection skips the reset (it may be
// aborted — e.g. a fired statement timeout — and a reset would itself fail); a CLEAN connection is
// reset to defaults, and if the reset fails the connection is dropped.
type configuredPool struct {
	inner   Pool
	session []string
	reset   []string
}

// ConfiguredPool wraps pool so acquired connections apply config's session knobs on checkout and
// reset them on release. Returns pool unchanged when config has no session knobs (transparent
// passthrough — byte-identical to the raw pool). Mirrors the TS configuredPool.
func ConfiguredPool(pool Pool, config ResolvedConnectionConfig) Pool {
	session := SessionStatements(config)
	if len(session) == 0 {
		return pool // no knobs set ⇒ transparent passthrough (byte-identical)
	}
	return &configuredPool{inner: pool, session: session, reset: SessionResetStatements(config)}
}

func (p *configuredPool) Acquire() (PooledConn, error) {
	conn, err := p.inner.Acquire()
	if err != nil {
		return nil, err
	}
	for _, stmt := range p.session {
		if _, rerr := conn.Run(stmt, nil); rerr != nil {
			_ = p.inner.Release(conn, true) // a failed session setup poisons the connection — drop it
			return nil, rerr
		}
	}
	return conn, nil
}

func (p *configuredPool) Release(conn PooledConn, destroy bool) error {
	// Run the reset (best-effort) BEFORE returning the connection to the pool — for BOTH a clean and a
	// destroyed release. This is the go-specific reality: go's database/sql has no public "close and
	// DISCARD this pooled connection" API — *sql.Conn.Close() returns the physical connection to the
	// pool even when we ask to destroy it. A statement that fired a server statement_timeout does NOT
	// break the PG/MySQL connection (only the statement is canceled) — so the connection carries THIS
	// config's session knobs (statement_timeout / search_path / charset) back into the pool unless we
	// reset them. Running the reset here (even on destroy) is what actually prevents the session leak
	// (proven by TestPhaseCConfigSearchPathResetOnReleaseLivePG + the queryTimeout mutation, where the
	// unconfigured follow-up query must NOT inherit the timeout). If the reset itself fails (a genuinely
	// aborted/broken connection) we still Release with destroy so the underlying pool drops it if it can.
	resetErr := error(nil)
	for _, stmt := range p.reset {
		if _, rerr := conn.Run(stmt, nil); rerr != nil {
			resetErr = rerr
			break
		}
	}
	if resetErr != nil || destroy {
		return p.inner.Release(conn, true)
	}
	return p.inner.Release(conn, false)
}

// ── Reader/writer pool pair (C1) ───────────────────────────────────────────────

// ReaderWriterPools is a reader/writer pool PAIR for ONE named connection (C1). Reader serves
// read-intent statements; Writer serves write-intent statements, [WithWriter] reads, and
// writer-sticky reads. When a connection has no separate replica, Reader == Writer is the SAME pool
// (the single-pool backward-compat case). Mirrors the TS ReaderWriterPools.
type ReaderWriterPools struct {
	Reader Pool
	Writer Pool
}

// SinglePoolPair builds a [ReaderWriterPools] where Reader == Writer (single-pool, backward-compat).
func SinglePoolPair(pool Pool) ReaderWriterPools {
	return ReaderWriterPools{Reader: pool, Writer: pool}
}

// ReaderWriterPair builds a [ReaderWriterPools] from a distinct reader + writer pool (separation).
func ReaderWriterPair(reader, writer Pool) ReaderWriterPools {
	return ReaderWriterPools{Reader: reader, Writer: writer}
}

// ── The connection registry (C2) — name → reader/writer pools ──────────────────

// DefaultConnection is the reserved name of the DEFAULT (unnamed) connection. An intent.DB of ""
// uses this. Mirrors the TS DEFAULT_CONNECTION.
const DefaultConnection = "default"

// ConnectionRegistry is the multi-DB connection registry (C2): a map from a connection NAME →
// its [ReaderWriterPools]. [ExecutionContext.ConnectionFor] selects the pair by intent.DB (the
// connection name the bundle/model metadata carries — decorator-free; decorator wiring is Phase F),
// falling back to [DefaultConnection] when unnamed. Selecting a name that was never registered is a
// LOUD error (a real wiring bug — never a silent default fallback, which would run a query on the
// wrong DB). Mirrors the TS ConnectionRegistry.
//
// A single-DB deployment registers exactly one connection under [DefaultConnection] with
// Reader == Writer ⇒ every intent routes to that one pool ⇒ byte-identical to Phase A/B.
type ConnectionRegistry struct {
	connections map[string]ReaderWriterPools
}

// NewConnectionRegistry builds a registry from a name→pools map (copied). Loud on an empty map is
// deferred to [ConnectionRegistryBuilder.Build]; a directly-built registry may be single-entry.
func NewConnectionRegistry(connections map[string]ReaderWriterPools) *ConnectionRegistry {
	cp := make(map[string]ReaderWriterPools, len(connections))
	for k, v := range connections {
		cp[k] = v
	}
	return &ConnectionRegistry{connections: cp}
}

// SingleDefaultRegistry builds a registry from ONE pool as the default connection (Reader == Writer).
// The backward-compat path: a ctx built from a single pool wraps it here so its ConnectionFor routes
// every intent to that one pool. Mirrors the TS ConnectionRegistry.singleDefault.
func SingleDefaultRegistry(pool Pool) *ConnectionRegistry {
	return NewConnectionRegistry(map[string]ReaderWriterPools{DefaultConnection: SinglePoolPair(pool)})
}

// PairFor returns the reader/writer pair for name (or [DefaultConnection] when ""). Loud on a
// missing name (never a silent default fallback). Mirrors the TS ConnectionRegistry.pairFor.
func (r *ConnectionRegistry) PairFor(name string) (ReaderWriterPools, error) {
	key := name
	if key == "" {
		key = DefaultConnection
	}
	pair, ok := r.connections[key]
	if !ok {
		known := make([]string, 0, len(r.connections))
		for k := range r.connections {
			known = append(known, "'"+k+"'")
		}
		knownStr := "<none>"
		if len(known) > 0 {
			knownStr = strings.Join(known, ", ")
		}
		return ReaderWriterPools{}, &SqlFailure{
			Kind:   KindDriverError,
			Policy: "fail",
			Msg: fmt.Sprintf("scp connection routing: no connection registered under name '%s' "+
				"(known: %s). Register it via SetConfig/ConnectionRegistry, or drop the connection tag "+
				"on the bundle/model.", key, knownStr),
		}
	}
	return pair, nil
}

// Names returns the registered connection names (for diagnostics / CloseAllPools).
func (r *ConnectionRegistry) Names() []string {
	out := make([]string, 0, len(r.connections))
	for k := range r.connections {
		out = append(out, k)
	}
	return out
}

// ConnectionRegistryBuilder is an incremental [ConnectionRegistry] builder (name → pools). Mirrors
// the TS ConnectionRegistryBuilder.
type ConnectionRegistryBuilder struct {
	connections map[string]ReaderWriterPools
}

// NewConnectionRegistryBuilder starts an empty builder.
func NewConnectionRegistryBuilder() *ConnectionRegistryBuilder {
	return &ConnectionRegistryBuilder{connections: map[string]ReaderWriterPools{}}
}

// RegistryFromDefault starts a builder from a default connection's pools (chainable .Add for more).
// Mirrors the TS ConnectionRegistry.fromDefault.
func RegistryFromDefault(pools ReaderWriterPools) *ConnectionRegistryBuilder {
	return NewConnectionRegistryBuilder().Add(DefaultConnection, pools)
}

// Add registers name → its reader/writer pools (chainable). Re-adding a name overwrites it.
func (b *ConnectionRegistryBuilder) Add(name string, pools ReaderWriterPools) *ConnectionRegistryBuilder {
	b.connections[name] = pools
	return b
}

// Build finalizes into an immutable [ConnectionRegistry]. Loud if empty (must carry at least the
// default connection). Mirrors the TS ConnectionRegistryBuilder.build.
func (b *ConnectionRegistryBuilder) Build() (*ConnectionRegistry, error) {
	if len(b.connections) == 0 {
		return nil, &SqlFailure{Kind: KindDriverError, Policy: "fail",
			Msg: "scp connection routing: ConnectionRegistry must have at least the default connection"}
	}
	return NewConnectionRegistry(b.connections), nil
}

// ── Writer-sticky + withWriter (C1) ────────────────────────────────────────────

// WriterStickyClock is a writer-sticky CLOCK (C1, read-your-writes; v1 _shouldUseWriterSticky :344 +
// _lastTransactionTime). After a transaction (or a bare write) COMMITs, reads within
// WriterStickyDuration route to the WRITER pool so a just-committed row is visible despite
// reader-replica lag. The ctx owns ONE clock; the tx runtime .Mark()s it on every successful
// write/commit; the router reads .IsSticky(). Mirrors the TS WriterStickyClock.
//
// UseWriterAfterTransaction=false disables it entirely (.IsSticky() always false). A single-pool
// deployment (Reader == Writer) is unaffected by stickiness — the diverted pool is the same object.
//
// The Now func is injectable (tests advance it deterministically); nil ⇒ a real monotonic clock.
// Access is mutex-guarded so a concurrent tx (which .Mark()s) and a concurrent read (which
// .IsSticky()) do not race.
type WriterStickyClock struct {
	mu               sync.Mutex
	lastWriteAtMs    int64
	enabled          bool
	stickyDurationMs int64
	now              func() int64
}

// StickyOptions configures a [WriterStickyClock] (mirrors the TS WriterStickyClock ctor opts +
// buildRoutingConfig stickyOpts). Pointers express "unset ⇒ default": UseWriterAfterTransaction nil
// ⇒ true, WriterStickyDuration nil ⇒ 5000ms. Now nil ⇒ a real monotonic clock (ms).
type StickyOptions struct {
	UseWriterAfterTransaction *bool
	WriterStickyDuration      *int
	Now                       func() int64
}

// NewWriterStickyClock builds a clock from [StickyOptions] (defaults: enabled=true, duration=5000ms,
// real monotonic now).
func NewWriterStickyClock(opts StickyOptions) *WriterStickyClock {
	enabled := true
	if opts.UseWriterAfterTransaction != nil {
		enabled = *opts.UseWriterAfterTransaction
	}
	duration := int64(5000)
	if opts.WriterStickyDuration != nil {
		duration = int64(*opts.WriterStickyDuration)
	}
	now := opts.Now
	if now == nil {
		start := time.Now()
		now = func() int64 { return int64(time.Since(start) / time.Millisecond) }
	}
	return &WriterStickyClock{enabled: enabled, stickyDurationMs: duration, now: now}
}

// Mark records that a write/commit just happened (the tx runtime calls this on success).
func (c *WriterStickyClock) Mark() {
	if !c.enabled {
		return
	}
	c.mu.Lock()
	c.lastWriteAtMs = c.now()
	c.mu.Unlock()
}

// IsSticky reports whether a read is currently sticky-to-writer (within WriterStickyDuration of the
// last write).
func (c *WriterStickyClock) IsSticky() bool {
	if !c.enabled {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastWriteAtMs == 0 {
		return false
	}
	return c.now()-c.lastWriteAtMs < c.stickyDurationMs
}

// Reset resets the clock (e.g. between tests / on CloseAllPools).
func (c *WriterStickyClock) Reset() {
	c.mu.Lock()
	c.lastWriteAtMs = 0
	c.mu.Unlock()
}

// ── withWriter scope (C1) — go: explicit ctx marker ────────────────────────────
//
// The TS WithWriter uses an AsyncLocalStorage marker so ANY read issued inside the async scope routes
// to the writer AND any write is rejected (read-only). Go threads the ExecutionContext explicitly
// (no task-local — §3 table), so [ExecutionContext.WithWriter] derives a ctx whose reads route to
// the writer and whose writes are rejected ([WriteInReadOnly] via the Phase B read-only marker). This
// is the go-idiomatic analogue of the TS withWriter scope: the scope is the derived ctx, not an
// ambient async marker.

// ── The routing config a PooledContext carries (C1+C2+C3) ──────────────────────

// RoutingConfig is the routing configuration a routed [ExecutionContext] carries to complete its
// ConnectionFor resolution (steps 2-4): the multi-DB [ConnectionRegistry] + the [WriterStickyClock].
// Absent ⇒ the ctx falls back to its single primary-db connection (the byte-identical Phase A/B
// path). Mirrors the TS RoutingConfig.
type RoutingConfig struct {
	Registry *ConnectionRegistry
	Sticky   *WriterStickyClock
}

// resolvePool resolves WHICH pool serves a statement given its intent + the routing config — the
// completion of ConnectionFor's steps 2-4 (step 1, the tx-pin, is handled by the ctx BEFORE calling
// this, since only the ctx holds the pin). The order (mirrors the TS resolvePool):
//
//  1. named-DB (intent.DB) selects the [ReaderWriterPools] pair (loud on unknown name).
//  2. within that pair: a WRITE ⇒ the writer pool.
//  3. a READ in a writer scope (writerScope) OR within writer-sticky ⇒ the writer pool (read-your-writes).
//  4. otherwise a READ ⇒ the reader pool.
//
// Single-pool (Reader == Writer) ⇒ every branch returns the same pool (backward-compat).
func resolvePool(intent StatementIntent, writerScope bool, routing RoutingConfig) (Pool, error) {
	pair, err := routing.Registry.PairFor(intent.DB)
	if err != nil {
		return nil, err
	}
	if intent.Write {
		return pair.Writer, nil // writes always to the writer
	}
	if writerScope || (routing.Sticky != nil && routing.Sticky.IsSticky()) {
		return pair.Writer, nil // read-your-writes
	}
	return pair.Reader, nil // plain read → reader
}

// ── setConfig / closeAllPools (C3 public surface) ──────────────────────────────

// PoolCloser closes a pool's underlying connections (a *sql.DB Close). Mirrors the TS PoolCloser.
type PoolCloser func() error

// BuiltPool is a [PoolFactory]'s output: the [Pool] seam adapter + a [PoolCloser].
type BuiltPool struct {
	Pool  Pool
	Close PoolCloser
}

// PoolFactory BUILDS a pool from a [ResolvedConnectionConfig] + a role, returning the [Pool] seam
// adapter plus a [PoolCloser]. This is where the CONSTRUCTION knobs — pool sizing (MinPool/MaxPool)
// + KeepAlive/KeepAliveInitialDelayMillis — reach the real *sql.DB (sql.Open then
// SetMaxOpenConns/SetMaxIdleConns/SetConnMax{Lifetime,IdleTime}), because in go those are set on the
// *sql.DB AFTER sql.Open — NOT on a pre-built shared *sql.DB (the #87 blocker).
//
// [BuildRoutingConfig] OWNS the call to this factory with the RESOLVED config, so the configured
// sizing/keepAlive is the SOLE source of the pool's cap. role lets a factory build a distinct replica
// pool for the reader vs. the writer while sharing the sizing config; a factory that returns the SAME
// pool for both roles collapses to single-pool (Reader == Writer). Mirrors the TS PoolFactory shape
// (config, role) → { pool, close }.
type PoolFactory func(config ResolvedConnectionConfig, role string) (BuiltPool, error)

// ConnectionSetup is one connection's inputs to [BuildRoutingConfig]: its Name (default when ""), its
// [ConnectionConfig], and a [PoolFactory] that BuildRoutingConfig CALLS with the resolved config to
// construct the pool(s) — so sizing/keepAlive are applied at construction and the config is the sole
// cap source. SeparateWriter=true asks the factory for a DISTINCT writer pool (replica split);
// otherwise the factory's reader pool is reused as the writer (single-pool, Reader == Writer).
// Mirrors the TS ConnectionSetup.
type ConnectionSetup struct {
	Name           string
	Config         ConnectionConfig
	PoolFactory    PoolFactory
	SeparateWriter bool
}

// BuiltRouting is the C3 SetConfig result: the [RoutingConfig] a routed [ExecutionContext] runs on,
// plus a Close() that shuts every constructed pool ([CloseAllPools]). Mirrors the TS
// buildRoutingConfig return.
type BuiltRouting struct {
	Routing RoutingConfig
	Close   PoolCloser
}

// BuildRoutingConfig is the C3 SetConfig: build the [RoutingConfig] a routed [ExecutionContext] runs
// on, plus a Close() that shuts every constructed pool. Build it from one or more [ConnectionSetup]s
// (the one named "default", or the first unnamed, is the default connection). Mirrors the TS
// buildRoutingConfig.
//
// For each setup: resolve the config, CALL its [PoolFactory] to construct the pool(s) — so
// MinPool/MaxPool/KeepAlive/KeepAliveInitialDelayMillis are applied at construction (the config is
// the SOLE source of the cap) — then wrap each pool with [ConfiguredPool] so the SESSION knobs
// (queryTimeout/searchPath/charset) apply on checkout + reset on release.
func BuildRoutingConfig(setups []ConnectionSetup, sticky StickyOptions) (BuiltRouting, error) {
	if len(setups) == 0 {
		return BuiltRouting{}, &SqlFailure{Kind: KindDriverError, Policy: "fail",
			Msg: "scp setConfig: at least one connection setup is required"}
	}
	builder := NewConnectionRegistryBuilder()
	var closers []PoolCloser
	for _, s := range setups {
		resolved := ResolveConnectionConfig(s.Config)
		// CONSTRUCT the reader pool from the resolved config (sizing/keepAlive land at construction).
		readerBuilt, err := s.PoolFactory(resolved, "reader")
		if err != nil {
			closeAll(closers)
			return BuiltRouting{}, err
		}
		closers = append(closers, readerBuilt.Close)
		reader := ConfiguredPool(readerBuilt.Pool, resolved)
		var pair ReaderWriterPools
		if s.SeparateWriter {
			writerBuilt, werr := s.PoolFactory(resolved, "writer")
			if werr != nil {
				closeAll(closers)
				return BuiltRouting{}, werr
			}
			closers = append(closers, writerBuilt.Close)
			pair = ReaderWriterPair(reader, ConfiguredPool(writerBuilt.Pool, resolved))
		} else {
			pair = SinglePoolPair(reader) // reader == writer (one constructed pool)
		}
		name := s.Name
		if name == "" {
			name = DefaultConnection
		}
		builder.Add(name, pair)
	}
	registry, err := builder.Build()
	if err != nil {
		closeAll(closers)
		return BuiltRouting{}, err
	}
	routing := RoutingConfig{Registry: registry, Sticky: NewWriterStickyClock(sticky)}
	captured := closers
	return BuiltRouting{Routing: routing, Close: func() error { return closeRouting(captured) }}, nil
}

// closeAll best-effort closes every closer (used on a mid-build failure), tolerating failures.
func closeAll(closers []PoolCloser) {
	for _, c := range closers {
		if c != nil {
			_ = c()
		}
	}
}

// closeRouting closes every DISTINCT pool closer (deduped by identity is not needed — a single-pool
// pair pushes ONE closer; a replica split pushes two distinct ones), tolerating individual failures
// and returning the FIRST error (if any). Mirrors the TS closeRouting.
func closeRouting(closers []PoolCloser) error {
	var first error
	for _, c := range closers {
		if c == nil {
			continue
		}
		if err := c(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
