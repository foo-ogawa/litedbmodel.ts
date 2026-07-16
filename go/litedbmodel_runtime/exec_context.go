// litedbmodel v2 SCP — the **ExecutionContext + central Execute/Run seam** (Phase A / #77, go).
//
// The Go port of the TS contract-defining artifact `src/scp/exec-context.ts` (#75), mirroring the
// rust port `rust/litedbmodel_runtime/src/exec_context.rs` (#76). It replaces the raw `db SQLDB`
// threaded through `ExecuteBundle` / `ExecuteReadGraph` / `ExecuteTransactionBundle` / the relation
// walker with an [ExecutionContext] that carries:
//
//  1. a **connection provider** — [ExecutionContext.ConnectionFor](intent) resolves WHICH connection
//     a statement runs on (the tx-owned connection, else the primary db; Phase A wires only the
//     tx-owned + single-DB cases, reader/writer/named-DB are B/C/D on this seam);
//  2. a **middleware chain** — [ExecutionContext.Middleware], wrapping every SQL (empty in Phase A =
//     passthrough; the registration API is Phase D — this is only the hook point);
//  3. a **pinned tx connection** — a tx-scoped ctx pins ONE owned connection so every statement in a
//     transaction body runs on it (per-execution connection ownership, §3).
//
// # The central seam (§2) — ALL SQL funnels through here
//
//	Execute(ctx, sql, params, intent) -> []bc.Value  // SELECT / RETURNING reads
//	Run(ctx, sql, params, intent)     -> RunInfo      // INSERT/UPDATE/DELETE, BEGIN/COMMIT/ROLLBACK
//
// Both do the SAME three things, in order:
//
//	① run the middleware chain (empty ⇒ passthrough, behavior unchanged);
//	② resolve the connection via ctx.ConnectionFor(intent);
//	③ execute on that connection (the ONLY driver contact point).
//
// Every direct db.Query / db.Exec in the read / tx / relation path is replaced by a call through
// this seam. A grep for `.Query(` / `.Exec(` outside the connection adapters (queryRows / execWrite,
// which ARE the ONE driver contact) comes up empty in the runtime SQL path — that is the AC.
//
// # ONE interface, not sync/async-bifurcated (contract flag)
//
// The TS reference bifurcates into ExecutionContext/execute (better-sqlite3, sync) and
// AsyncExecutionContext/executeAsync (pg/mysql2, async) because that split is TS-runtime specific.
// Per the #77 contract flags the Go port **collapses to ONE interface**: Go's database/sql surface
// is a single blocking API over a goroutine-safe connection pool (a *sql.DB) — there is exactly ONE
// [ExecutionContext] / one [Execute] / one [Run]. There is likewise **no executeSafeIntegers** —
// that is a better-sqlite3 #59 BIGINT toggle; Go / PG / MySQL return BIGINT natively (int64 / string
// via scanValue), so the read seam is a plain execute.
//
// # Per-execution connection ownership (§3) — the concurrent-tx fix
//
// A transaction checks out ONE OWNED connection via db.Conn() → a *sql.Conn (Go's connection-owning
// primitive that keeps tx-control OBSERVABLE: it binds exactly one pooled connection for the tx's whole
// lifetime, the analogue of v1 `PoolTransaction`), pins it into a tx-scoped [ExecutionContext] threaded
// on a context.Context, then issues its OWN tx-control (SET/BEGIN/COMMIT/ROLLBACK) as REAL SQL THROUGH
// the seam ([Run] on the pinned conn) so a middleware OBSERVES it — full TS parity (Phase D / #94). The
// body's every statement resolves that *sql.Conn via ConnectionFor; a poisoned conn (a failed
// ROLLBACK/COMMIT) is DESTROYED on release (see [releaseTxConn]). Concurrent transactions each own a
// DISTINCT *sql.Conn over a DISTINCT pooled connection ⇒ isolated. There is NO driver-global tx slot,
// and no *sql.Tx (whose BEGIN/Commit/Rollback are opaque method calls the seam can't observe); this
// seam makes ownership central + explicit + middleware-visible, and forbids any shared-tx state.

package litedbmodel_runtime

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// ── Statement intent & the driver contact (§5) ────────────────────────────────

// StatementIntent is what a statement needs from the connection provider (§3): whether it writes (so
// it must go to a writer / the tx-owned connection, never a read replica) and an optional named DB
// (multi-DB routing, Phase B). Phase A resolves only Write (tx-owned vs. primary) and ignores DB
// (single DB); the field is in the contract now so B/C/D extend the resolver — not the seam.
type StatementIntent struct {
	// Write is true when the statement writes (INSERT/UPDATE/DELETE or BEGIN/COMMIT/ROLLBACK) ⇒ the
	// writer / tx connection.
	Write bool
	// DB is the named-DB routing key (multi-DB, Phase B). "" ⇒ the primary DB.
	DB string
}

// ReadIntent is a read intent (Write=false, primary DB).
func ReadIntent() StatementIntent { return StatementIntent{Write: false} }

// WriteIntent is a write intent (Write=true, primary DB).
func WriteIntent() StatementIntent { return StatementIntent{Write: true} }

// RunInfo is a non-returning write summary (INSERT/UPDATE/DELETE affected + last insert id), the
// analogue of better-sqlite3's `{changes, lastInsertRowid}`.
type RunInfo struct {
	Changes         int64
	LastInsertRowid int64
}

// ── The ONE driver contact point (§5) — a Connection ──────────────────────────

// Connection is the ONE driver contact point (§5): a resolved connection a statement runs on.
// Outside a tx this wraps the primary SQLDB (a *sql.DB — a pooled connection per call); inside a tx
// it wraps the tx-owned *sql.Conn (via connSQLDB). The seam is the ONLY caller; the runtime SQL path
// never touches a db.Query / db.Exec directly.
type Connection interface {
	// Execute runs a SELECT / RETURNING statement; returns the raw rows.
	Execute(sql string, args []any) ([]bc.Value, error)
	// Run runs a non-returning write / DDL / tx-control statement; returns the affected summary.
	Run(sql string, args []any) (RunInfo, error)
}

// dbConnection adapts a raw SQLDB (the primary db — a *sql.DB, the NON-tx path; the tx path pins a
// *sql.Conn via connSQLDB instead) to the [Connection] seam. Execute/Run are the SAME queryRows /
// execWrite the runtime used directly before the seam — so a ctx built via [ContextForDB] is
// byte-identical to the old raw-db path (the
// backward-compat wrapper, §6). It is the ONE place a db.Query / db.Exec is issued.
type dbConnection struct {
	db SQLDB
}

func (c dbConnection) Execute(sql string, args []any) ([]bc.Value, error) {
	return queryRows(c.db, sql, args)
}

func (c dbConnection) Run(sql string, args []any) (RunInfo, error) {
	changes, lastInsert, err := execWrite(c.db, sql, args)
	if err != nil {
		return RunInfo{}, err
	}
	return RunInfo{Changes: changes, LastInsertRowid: lastInsert}, nil
}

// ── Middleware chain (§4) — the hook point (empty in Phase A) ──────────────────

// SeamNext is the terminal of a middleware chain: resolve the connection + execute (the seam's ②③).
type SeamNext[T any] func(sql string, args []any) (T, error)

// ReadMiddleware / WriteMiddleware wrap a statement, delegating to `next` (Phase D supplies the
// registration API). Kept homogeneous per result type (rows / a run summary) so the fold is
// monomorphic — ONE registration shape per seam.
type ReadMiddleware func(sql string, args []any, next SeamNext[[]bc.Value]) ([]bc.Value, error)
type WriteMiddleware func(sql string, args []any, next SeamNext[RunInfo]) (RunInfo, error)

// MiddlewareChain is the ordered middleware chain a ctx carries (§4). wrapRead/wrapWrite fold the
// middlewares around `next` (the connection-resolve + execute terminal). An EMPTY chain is a pure
// passthrough — wrap returns next(sql, args) verbatim, so Phase A behavior is byte-identical.
//
// A chain resolves its middlewares from a [middlewareSource] at EACH wrap (mirror the TS
// MiddlewareStackSource) rather than capturing a fixed slice at construction — so a middleware
// REGISTERED after the ctx was built (the normal register-then-query order), and a per-execution-scope
// registry (concurrent isolation, Phase D), are both honored. A nil source ⇒ a fixed empty chain (the
// Phase A backward-compat passthrough: no registration surface ⇒ nothing to resolve ⇒ byte-identical).
type MiddlewareChain struct {
	// source resolves the CURRENT SQL-level middlewares (read + write), outermost-first, at wrap time.
	// nil ⇒ a guaranteed-empty passthrough chain (Phase A). Set by [ContextForDB] et al. to read the
	// per-scope registry carried on the ctx's Go context.Context (Phase D).
	source middlewareSource
}

// middlewareSource resolves the live SQL-level middlewares (read + write slices, index 0 = outermost)
// at each wrap. The Phase D registry-backed source reads the current execution scope's stack.
type middlewareSource func() (read []ReadMiddleware, write []WriteMiddleware)

// NewMiddlewareChain returns a new empty chain (Phase A default — a nil-source pure passthrough).
func NewMiddlewareChain() *MiddlewareChain { return &MiddlewareChain{} }

// newSourcedChain returns a chain whose middlewares are resolved from `source` at each wrap (Phase D).
func newSourcedChain(source middlewareSource) *MiddlewareChain {
	return &MiddlewareChain{source: source}
}

// resolve returns the current read + write middleware slices (index 0 = outermost), or (nil, nil) for
// a nil-source Phase A chain.
func (m *MiddlewareChain) resolve() (read []ReadMiddleware, write []WriteMiddleware) {
	if m.source == nil {
		return nil, nil
	}
	return m.source()
}

// IsEmpty reports whether the chain is empty RIGHT NOW (⇒ this wrap is a guaranteed passthrough).
func (m *MiddlewareChain) IsEmpty() bool {
	read, write := m.resolve()
	return len(read) == 0 && len(write) == 0
}

// wrapRead folds the READ chain around `next`, then invokes it. Empty ⇒ next(sql, args) verbatim.
func (m *MiddlewareChain) wrapRead(sql string, args []any, next SeamNext[[]bc.Value]) ([]bc.Value, error) {
	read, _ := m.resolve()
	if len(read) == 0 {
		return next(sql, args) // fast path: no middleware ⇒ byte-identical passthrough
	}
	fn := next
	for i := len(read) - 1; i >= 0; i-- {
		mw := read[i]
		inner := fn
		fn = func(s string, p []any) ([]bc.Value, error) { return mw(s, p, inner) }
	}
	return fn(sql, args)
}

// wrapWrite folds the WRITE chain around `next`, then invokes it. Empty ⇒ next(sql, args) verbatim.
func (m *MiddlewareChain) wrapWrite(sql string, args []any, next SeamNext[RunInfo]) (RunInfo, error) {
	_, write := m.resolve()
	if len(write) == 0 {
		return next(sql, args) // fast path: no middleware ⇒ byte-identical passthrough
	}
	fn := next
	for i := len(write) - 1; i >= 0; i-- {
		mw := write[i]
		inner := fn
		fn = func(s string, p []any) (RunInfo, error) { return mw(s, p, inner) }
	}
	return fn(sql, args)
}

// ── The ExecutionContext (§2 / §5) — ONE interface ────────────────────────────

// ExecutionContext is threaded (via context.Context, the Go-idiomatic decision — §3 table) through
// ExecuteBundle / ExecuteReadGraph / ExecuteTransactionBundle / the relation walker in place of a
// raw `db SQLDB`. It carries the connection provider (the primary db + an optional pinned tx
// connection), the middleware chain, and derives a tx-scoped ctx via [ExecutionContext.WithTxConnection].
//
// It also carries a context.Context (Go-idiomatic ctx propagation): the SAME Context flows into the
// derived tx-scoped ctx so a future cancellation / deadline (Phase C) threads through the seam
// without a signature change.
type ExecutionContext struct {
	// ctx is the Go context.Context this execution rides (cancellation / deadline / values). Phase A
	// carries it verbatim; the seam does not yet consult it (database/sql's non-Context Query/Exec
	// stay byte-identical to the pre-seam path), but it is threaded so B/C/D add ctx-aware execution
	// on this SAME field without re-plumbing every caller.
	ctx context.Context //nolint:containedctx // the SCP ExecutionContext deliberately carries the Go ctx (§3 propagation)
	// db is the primary connection provider (the non-tx path) — a *sql.DB (each Query/Exec = one
	// pooled conn) or an in-proc SQLite handle.
	db SQLDB
	// middleware is the chain wrapping every SQL (§4). Empty in Phase A.
	middleware *MiddlewareChain
	// pinned is the tx-owned connection (present ⇒ this is a tx-scoped ctx; every statement resolves
	// it). nil outside a transaction.
	pinned Connection
	// readOnly is the READ-ONLY marker (Phase B / #83 write=tx guard — mirror v1 `withWriter` / the
	// TS `withReadOnly` ALS marker / rust `read_only`): a write in a read-only-scoped ctx is REJECTED
	// ([WriteInReadOnly]). The explicit-ctx analogue of the TS async-local read-only marker; derived
	// via [ExecutionContext.WithReadOnly].
	readOnly bool
	// routing is the Phase C (#89) connection-routing config (the multi-DB [ConnectionRegistry] + the
	// [WriterStickyClock]). nil ⇒ the Phase A/B single-primary-db path (ConnectionFor returns the
	// primary-db connection, byte-identical). Non-nil ⇒ ConnectionFor completes its resolution steps
	// 2-4 (reader/writer split, writer-sticky, named-DB) via [resolvePool]. Set by [ContextForRouting].
	routing *RoutingConfig
	// writerScope is the Phase C (#89) [WithWriter] marker (the explicit-ctx analogue of the TS
	// withWriter async-local): a READ in a writer scope routes to the WRITER pool (read-your-writes),
	// and — because WithWriter also sets readOnly — any write is rejected. Derived via
	// [ExecutionContext.WithWriter].
	writerScope bool
}

// sharedEmptyChain is the Phase A middleware chain — a nil-source empty passthrough so a ctx can be
// built without any registration surface. It is used by [ContextForDBWith] when the caller passes a
// nil chain AND a background Go context (no scope to resolve).
var sharedEmptyChain = NewMiddlewareChain()

// registryChainFor builds the Phase D middleware chain for a ctx riding `goCtx`: it resolves the
// current execution scope's registry (carried on `goCtx`, else the process-global default) at EACH
// wrap. When no middleware is registered the resolved slices are empty ⇒ the seam is a byte-identical
// passthrough (the conformance / livedb / bench runners register none). This is the Go analogue of the
// TS `new MiddlewareChain(activeSqlMiddlewares)` — the chain reads the LIVE ambient/scope stack, not a
// snapshot, so a register-then-query order and per-scope isolation both resolve correctly.
func registryChainFor(goCtx context.Context) *MiddlewareChain {
	return newSourcedChain(func() ([]ReadMiddleware, []WriteMiddleware) {
		return currentRegistry(goCtx).sqlMiddlewares()
	})
}

// ContextForDB is the **backward-compat wrapper (§6)**: wrap a raw SQLDB in a thin [ExecutionContext]
// (primary = db, a background Go context, no pinned tx connection, a single DB). Its middleware chain
// resolves the ambient/scope registry (Phase D) at wrap time — so a middleware registered via
// [RegisterMiddleware] under a [WithMiddlewareScope] intercepts the SQL, and, when NO middleware is
// registered (the conformance / livedb / bench runners), the chain is empty and the seam is a pure
// **byte-identical** passthrough to queryRows / execWrite. Go analogue of the TS contextForDriver.
func ContextForDB(db SQLDB) *ExecutionContext {
	goCtx := context.Background()
	return &ExecutionContext{ctx: goCtx, db: db, middleware: registryChainFor(goCtx)}
}

// ContextForDBCtx wraps a raw SQLDB riding a caller-supplied context.Context (Phase D: a scoped Go
// context carrying a per-request middleware registry via [WithMiddlewareScope]). The middleware chain
// resolves THAT context's scope registry at wrap time, so a middleware registered inside the scope is
// seen and concurrent scopes stay isolated. Primary = db, no pinned tx connection, a single DB.
func ContextForDBCtx(goCtx context.Context, db SQLDB) *ExecutionContext {
	if goCtx == nil {
		goCtx = context.Background()
	}
	return &ExecutionContext{ctx: goCtx, db: db, middleware: registryChainFor(goCtx)}
}

// ContextForDBWith wraps a raw SQLDB with a caller-supplied context.Context + an explicit
// [MiddlewareChain]. When `middleware` is nil the chain is sourced from the passed context's scope
// registry (Phase D) — so a scoped Go context resolves its registered middlewares; a background
// context with no registrations stays a byte-identical passthrough. Prefer [ContextForDB] /
// [ContextForDBCtx] unless you hold a hand-built chain.
func ContextForDBWith(ctx context.Context, db SQLDB, middleware *MiddlewareChain) *ExecutionContext {
	if ctx == nil {
		ctx = context.Background()
	}
	if middleware == nil {
		middleware = registryChainFor(ctx)
	}
	return &ExecutionContext{ctx: ctx, db: db, middleware: middleware}
}

// ContextForRouting builds a Phase C (#89) routed [ExecutionContext] over a [RoutingConfig] (the
// multi-DB [ConnectionRegistry] + [WriterStickyClock]). Reads/writes resolve their pool via
// [resolvePool] (reader/writer split, writer-sticky, named-DB); no primary db is threaded (the
// routing registry owns the pools). Middleware defaults to the shared empty chain (Phase A
// passthrough) when nil; a background Go context is used. A ctx built from a SINGLE-default registry
// with Reader == Writer + a disabled sticky clock resolves byte-identically to the Phase A/B
// single-pool path. This is the go analogue of the TS `new PooledAsyncContext(routing)`.
func ContextForRouting(routing RoutingConfig, middleware *MiddlewareChain) *ExecutionContext {
	goCtx := context.Background()
	if middleware == nil {
		middleware = registryChainFor(goCtx)
	}
	return &ExecutionContext{ctx: goCtx, middleware: middleware, routing: &routing}
}

// ContextForRoutingCtx is [ContextForRouting] riding a caller-supplied (scoped) context.Context, so
// the routed async path resolves the scope's middleware registry at wrap time (Phase D).
func ContextForRoutingCtx(goCtx context.Context, routing RoutingConfig) *ExecutionContext {
	if goCtx == nil {
		goCtx = context.Background()
	}
	return &ExecutionContext{ctx: goCtx, middleware: registryChainFor(goCtx), routing: &routing}
}

// Routing returns the Phase C routing config this ctx carries (nil for the Phase A/B primary-db path).
func (c *ExecutionContext) Routing() *RoutingConfig { return c.routing }

// Context returns the Go context.Context this ExecutionContext rides.
func (c *ExecutionContext) Context() context.Context { return c.ctx }

// Middleware returns the middleware chain this ctx carries (§4).
func (c *ExecutionContext) Middleware() *MiddlewareChain { return c.middleware }

// DB returns the primary db (for the pooled read fan-out, which needs the goroutine-safe *sql.DB the
// concurrent sibling relations share — the tx path never fans out, so this is the non-tx provider).
func (c *ExecutionContext) DB() SQLDB { return c.db }

// InTransaction reports whether this is a tx-scoped ctx (a pinned connection is present). This is
// the explicit-ctx analogue of the TS async-local "inside a transaction" marker — the write=tx guard
// reads it, and the public [Transaction] boundary reads it for NESTED-tx join detection.
func (c *ExecutionContext) InTransaction() bool { return c.pinned != nil }

// ReadOnly reports whether this is a READ-ONLY-scoped ctx (Phase B / #83 write=tx guard). A write
// here is REJECTED ([WriteInReadOnly]) — the explicit-ctx analogue of the TS `withReadOnly` / v1
// `withWriter` read-only marker. Derived via [ExecutionContext.WithReadOnly].
func (c *ExecutionContext) ReadOnly() bool { return c.readOnly }

// WithReadOnly derives a READ-ONLY-scoped ctx (mirror v1 `withWriter` / the TS `withReadOnly` / rust
// `with_read_only`): reads are allowed, but ANY write funneled through the GUARDED write seam
// ([RunGuarded] / [ExecuteTransactionBundleCtx]) is rejected with [WriteInReadOnly]. Used for a
// writer-pinned read scope that must never accidentally mutate. Shares the primary db + middleware +
// Go context + pinned tx connection.
func (c *ExecutionContext) WithReadOnly() *ExecutionContext {
	return &ExecutionContext{ctx: c.ctx, db: c.db, middleware: c.middleware, pinned: c.pinned, readOnly: true, routing: c.routing, writerScope: c.writerScope}
}

// WithWriter derives a WRITER-scoped ctx (Phase C / #89 — the go analogue of the TS `withWriter`):
// every READ this ctx issues routes to the WRITER pool (read-your-writes without replication lag),
// and — because it ALSO sets the read-only marker — ANY write funneled through the GUARDED write seam
// ([RunGuarded] / [ExecuteTransactionBundleCtx]) is rejected with [WriteInReadOnly] (v1 parity —
// withWriter reads never mutate). Inside a transaction the tx-owned connection already wins in
// ConnectionFor, so WithWriter there is a no-op on routing (matches v1 :2941). Shares the primary db +
// middleware + Go context + pinned tx connection + routing.
func (c *ExecutionContext) WithWriter() *ExecutionContext {
	return &ExecutionContext{ctx: c.ctx, db: c.db, middleware: c.middleware, pinned: c.pinned, readOnly: true, routing: c.routing, writerScope: true}
}

// InWriterScope reports whether this is a [WithWriter]-scoped ctx (reads route to the writer).
func (c *ExecutionContext) InWriterScope() bool { return c.writerScope }

// ConnectionFor resolves WHICH connection a statement runs on (§3). Resolution order:
//  1. the tx-owned (pinned) connection wins (Phase A — only the ctx holds the pin);
//  2. else, if this ctx carries a Phase C [RoutingConfig], [resolvePool] picks the reader/writer pool
//     of the named connection (steps 2-4) and the returned connection acquires/runs/releases one
//     owned pooled connection per statement (the per-statement ownership the read fan-out uses);
//  3. else (Phase A/B single-primary-db path), the primary db (byte-identical — routing nil).
//
// A routing-resolution error (an unknown named DB — a loud wiring bug) is deferred to the acquired
// connection's Execute/Run: Go's Connection has no error return on ConnectionFor, so the error is
// carried by a [failingConnection] whose Execute/Run surface it — the seam propagates it uniformly
// (mirrors the TS synchronous throw inside connectionFor being surfaced through the seam).
func (c *ExecutionContext) ConnectionFor(intent StatementIntent) Connection {
	if c.pinned != nil {
		return c.pinned
	}
	if c.routing != nil {
		pool, err := resolvePool(intent, c.writerScope, *c.routing)
		if err != nil {
			return failingConnection{err: err}
		}
		return pooledStatementConnection{pool: pool}
	}
	return dbConnection{db: c.db}
}

// WithTxConnection derives a tx-scoped ctx pinning `conn` (every statement resolves it while this ctx
// is used). The derived ctx shares the primary db + middleware chain + Go context + routing;
// ConnectionFor returns the pinned tx connection instead of the db. This is the Go analogue of the TS
// withConnection(conn, true) / rust with_tx_connection.
func (c *ExecutionContext) WithTxConnection(conn Connection) *ExecutionContext {
	// A tx-scoped ctx INHERITS the read-only + writer markers + routing: a Transaction() opened inside
	// a read-only scope is still read-only (v1 parity — withWriter reads never mutate).
	return &ExecutionContext{ctx: c.ctx, db: c.db, middleware: c.middleware, pinned: conn, readOnly: c.readOnly, routing: c.routing, writerScope: c.writerScope}
}

// pooledStatementConnection is the ConnectionFor result for the Phase C routing path: it acquires ONE
// owned pooled connection, runs the statement, and releases it (acquire → run → release per statement)
// — the go analogue of the TS PooledAsyncContext.connectionFor inline wrapper. This is the read
// fan-out ownership model: each concurrent sibling statement acquires its own connection (bounded by
// the pool's MaxOpenConns cap). A poisoned connection (a statement error) is released as DESTROYED so
// it never re-enters the pool with a fired statement_timeout / aborted session.
type pooledStatementConnection struct {
	pool Pool
}

func (c pooledStatementConnection) Execute(sql string, args []any) ([]bc.Value, error) {
	conn, err := c.pool.Acquire()
	if err != nil {
		return nil, err
	}
	rows, execErr := conn.Execute(sql, args)
	_ = c.pool.Release(conn, execErr != nil)
	return rows, execErr
}

func (c pooledStatementConnection) Run(sql string, args []any) (RunInfo, error) {
	conn, err := c.pool.Acquire()
	if err != nil {
		return RunInfo{}, err
	}
	info, runErr := conn.Run(sql, args)
	_ = c.pool.Release(conn, runErr != nil)
	return info, runErr
}

// failingConnection carries a ConnectionFor resolution error (an unknown named DB) to the seam: its
// Execute/Run return the error, so a loud routing failure is surfaced through the SAME central seam
// as any driver error (mirrors the TS synchronous throw in connectionFor).
type failingConnection struct {
	err error
}

func (c failingConnection) Execute(string, []any) ([]bc.Value, error) { return nil, c.err }
func (c failingConnection) Run(string, []any) (RunInfo, error)        { return RunInfo{}, c.err }

// ── The central seam (§2) — the ONLY place SQL meets a connection ─────────────

// Execute is the central READ seam: ① middleware chain, ② resolve the connection, ③ execute. Every
// read (primary read node, relation batch, tx-body SELECT/RETURNING) funnels through here.
func Execute(ctx *ExecutionContext, sql string, args []any, intent StatementIntent) ([]bc.Value, error) {
	conn := ctx.ConnectionFor(intent)
	return ctx.middleware.wrapRead(sql, args, func(s string, p []any) ([]bc.Value, error) {
		return conn.Execute(s, p)
	})
}

// Run is the central WRITE seam: ① middleware chain, ② resolve the connection, ③ run. Every write and
// every tx-control statement (BEGIN/COMMIT/ROLLBACK on the non-tx db path) funnels through here.
func Run(ctx *ExecutionContext, sql string, args []any, intent StatementIntent) (RunInfo, error) {
	conn := ctx.ConnectionFor(intent)
	return ctx.middleware.wrapWrite(sql, args, func(s string, p []any) (RunInfo, error) {
		return conn.Run(s, p)
	})
}

// RunGuarded is the GUARDED write seam (Phase B / #83): enforce the write=tx guard
// ([CheckWriteAllowed]) for a DATA-mutating statement, then delegate to [Run]. A write issued
// OUTSIDE a transaction is rejected with [WriteOutsideTransaction]; a write in a
// [ExecutionContext.WithReadOnly] scope is rejected with [WriteInReadOnly] (read-only checked FIRST,
// v1 order). Tx-control statements (BEGIN/COMMIT/ROLLBACK/SET) are NOT guarded — the tx runtime
// issues them to OPEN the very scope the guard checks. The guard reads the CALLER's ctx markers
// (InTransaction / ReadOnly) — the explicit-ctx analogue of the TS async-local markers.
func RunGuarded(ctx *ExecutionContext, sql string, args []any, operation string, model string) (RunInfo, error) {
	if err := CheckWriteAllowed(operation, model, ctx.InTransaction(), ctx.ReadOnly()); err != nil {
		return RunInfo{}, err
	}
	return Run(ctx, sql, args, WriteIntent())
}

// ── Per-execution transaction ownership (§3) — the concurrent-tx fix ──────────

// txConnection is the OWNED transaction connection — the Go analogue of v1 `PoolTransaction`. It wraps
// ONE *sql.Conn (Go's connection-owning primitive that keeps tx-control as OBSERVABLE SQL: db.Conn()
// checks out exactly one pooled connection held for the whole transaction; the runtime issues its OWN
// BEGIN/COMMIT/ROLLBACK/SET on it as REAL SQL strings THROUGH the seam — middleware-visible — unlike a
// *sql.Tx whose BEGIN/Commit/Rollback are opaque method calls). Because the *sql.Conn satisfies SQLDB
// (via connSQLDB) every statement in the tx body — data writes AND tx-control — runs on it through the
// SAME queryRows / execWrite as the non-tx path, but pinned so ConnectionFor always returns THIS
// connection. Concurrent transactions each own a DISTINCT *sql.Conn over a DISTINCT pooled connection,
// so their writes never cross-talk (the isolation the shared-slot model would violate).
type txConnection struct {
	sqldb connSQLDB
}

func (c txConnection) Execute(sql string, args []any) ([]bc.Value, error) {
	return queryRows(c.sqldb, sql, args)
}

func (c txConnection) Run(sql string, args []any) (RunInfo, error) {
	changes, lastInsert, err := execWrite(c.sqldb, sql, args)
	if err != nil {
		return RunInfo{}, err
	}
	return RunInfo{Changes: changes, LastInsertRowid: lastInsert}, nil
}

// TxDecision is the body's decision about how to end the transaction — so a body can legitimately
// ROLLBACK and STILL return a value (the gate short-circuit: a failed gate rolls back but is NOT an
// error, it returns committed:false). An error from the body always rolls back + re-raises regardless.
type TxDecision struct {
	// Rollback true ⇒ ROLLBACK the owned connection (a legitimate non-error outcome, e.g. a gate
	// short-circuit); false ⇒ COMMIT.
	Rollback bool
}

// Commit is the COMMIT decision (the tx's owned connection commits, then the body's value returns).
func Commit() TxDecision { return TxDecision{Rollback: false} }

// Rollback is the (non-error) ROLLBACK decision (the tx's owned connection rolls back, then the
// body's value returns — a legitimate gate short-circuit).
func Rollback() TxDecision { return TxDecision{Rollback: true} }

// WithTransactionDecided runs `body` inside a transaction with **per-execution connection ownership**
// (§3, the concurrent-tx fix). This is the general form: `body` decides COMMIT vs ROLLBACK (see
// [TxDecision]); a non-nil error from `body` always rolls back and re-raises.
//
//  1. check out ONE OWNED connection via db.Conn() → a *sql.Conn (the tx's exclusive pooled connection,
//     NO BEGIN yet), the Go analogue of v1 PoolTransaction;
//  2. pin it into a tx-scoped [ExecutionContext] so EVERY statement `body` issues resolves THAT
//     connection via ConnectionFor — never a fresh pooled one;
//  3. issue the isolation-aware BEGIN THROUGH the seam ([Run] on the pinned conn) so it is
//     middleware-visible; run body(txCtx) → COMMIT / ROLLBACK THROUGH the seam per the returned
//     decision; on any error ROLLBACK (best-effort) and re-raise;
//  4. release the owned connection back to the pool (or DESTROY it via a bad-conn Raw when a failed
//     ROLLBACK/COMMIT poisoned it — see [releaseTxConn]).
//
// Concurrent calls each own a DISTINCT *sql.Conn, so their writes never cross-talk — the isolation a
// shared-slot model would violate. This mirrors the TS withTransactionAsync (#75) / rust
// with_transaction_decided (#76). See [WithTransactionDecidedIsolated] for the seam-visible tx-control.
func WithTransactionDecided[R any](ctx *ExecutionContext, db TxDB, body func(txCtx *ExecutionContext) (R, TxDecision, error)) (R, error) {
	return WithTransactionDecidedIsolated(ctx, db, IsolationNone, "", body)
}

// WithTransactionDecidedIsolated is the isolation-aware form of [WithTransactionDecided] (Phase B /
// #83), restructured for Phase D (#94) so the runtime's OWN tx-control is MIDDLEWARE-VISIBLE (full TS
// parity — owner decision option A). It:
//
//  1. checks out ONE OWNED *sql.Conn (db.Conn — Go's connection-owning primitive; NO BEGIN yet), pins
//     it into a tx-scoped ctx so every statement resolves THIS connection;
//  2. issues the isolation-aware BEGIN ([BeginStatements]: PG `BEGIN ISOLATION LEVEL …`; MySQL a
//     preceding `SET TRANSACTION ISOLATION LEVEL …` then `BEGIN`) THROUGH the seam ([Run] on the
//     pinned conn) — so a registered SQL-level middleware OBSERVES the BEGIN (+ SET);
//  3. runs body(txCtx);
//  4. issues COMMIT (success) / ROLLBACK (gate short-circuit or body error) THROUGH the seam ([Run] on
//     the pinned conn) — middleware OBSERVES the COMMIT / ROLLBACK too;
//  5. releases the OWNED connection back to the pool (or DESTROYS it, via a bad-conn Raw, when it is
//     poisoned — a failed ROLLBACK leaves it in an unknown state, so it must not re-enter the pool).
//
// `dialectName` selects the tx-control SQL + gates the SQLite hard-error (SQLite has no per-tx level —
// mirror [isolationPrelude]). [IsolationNone] ⇒ a bare `BEGIN`, byte-identical to the Phase A tx SQL.
// This is the ONE mechanism the public [Transaction] boundary and the write-tx plan executor both
// drive; the retry loop + nested-join live in [TransactionDecided], while THIS runs exactly ONE attempt
// on a freshly-acquired owned connection — so a retry re-checks-out a fresh conn AND re-issues BEGIN
// through the seam per attempt (a retry after a connection error thus RECONNECTS + re-BEGINs).
//
// tx-control is issued through [Run] (NOT the guarded write seam) so it is EXEMPT from the write=tx
// guard — BEGIN/COMMIT/ROLLBACK/SET are the runtime's own envelope, not user writes.
func WithTransactionDecidedIsolated[R any](ctx *ExecutionContext, db TxDB, isolation IsolationLevel, dialectName string, body func(txCtx *ExecutionContext) (R, TxDecision, error)) (R, error) {
	var zero R
	// Resolve the isolation-aware BEGIN SQL first (fail-closed: SQLite + a level is a hard error) BEFORE
	// acquiring a connection — an unsupported isolation must not check out a conn it can't honor. For
	// IsolationNone this is a bare []string{"BEGIN"} (byte-identical to the Phase A tx envelope).
	begins, err := BeginStatements(dialectName, isolation)
	if err != nil {
		return zero, err
	}

	goCtx := ctx.ctx
	if goCtx == nil {
		goCtx = context.Background()
	}
	// Check out the tx's OWNED connection (NO BEGIN yet — Go's db.Conn hands out one pooled connection
	// held for the whole transaction, the *sql.Conn analogue of v1 PoolTransaction ownership).
	conn, err := db.Conn(goCtx)
	if err != nil {
		return zero, mapSqliteError(err)
	}
	// Pin the OWNED *sql.Conn into a tx-scoped ctx: every statement (data + tx-control) the body/runtime
	// issues resolves THIS connection through the SAME seam — so tx-control is middleware-visible.
	txCtx := ctx.WithTxConnection(txConnection{sqldb: connSQLDB{conn: conn, ctx: goCtx}})
	// poisoned ⇒ the connection is in an unknown state (a failed ROLLBACK); DESTROY it on release so a
	// fired statement_timeout / aborted session never re-enters the pool.
	poisoned := false
	defer releaseTxConn(conn, &poisoned)

	// (2) BEGIN (+ isolation SET) THROUGH the seam on the pinned conn — middleware observes it.
	for _, begin := range begins {
		if _, e := Run(txCtx, begin, nil, WriteIntent()); e != nil {
			return zero, mapSqliteError(e)
		}
	}

	// (3) the transaction body.
	result, decision, bodyErr := body(txCtx)
	if bodyErr != nil {
		// A body error rolls back (best-effort) and re-raises; the original failure is surfaced. A failed
		// ROLLBACK poisons the connection (unknown state) ⇒ destroy on release.
		if _, e := Run(txCtx, "ROLLBACK", nil, WriteIntent()); e != nil {
			poisoned = true
		}
		return zero, bodyErr
	}
	if decision.Rollback {
		// (4a) a legitimate non-error rollback (e.g. a gate short-circuit): ROLLBACK through the seam,
		// return the value. A rollback failure IS surfaced (the connection would be in an unknown state).
		if _, e := Run(txCtx, "ROLLBACK", nil, WriteIntent()); e != nil {
			poisoned = true
			return zero, mapSqliteError(e)
		}
		return result, nil
	}
	// (4b) COMMIT through the seam — middleware observes it. A failed COMMIT rolls back + poisons.
	if _, e := Run(txCtx, "COMMIT", nil, WriteIntent()); e != nil {
		if _, re := Run(txCtx, "ROLLBACK", nil, WriteIntent()); re != nil {
			poisoned = true
		}
		return zero, mapSqliteError(e)
	}
	// Phase C (#89): a SUCCESSFUL commit ARMS the writer-sticky clock (read-your-writes) — subsequent
	// reads within writerStickyDuration route to the WRITER. No-op when the ctx carries no routing /
	// no sticky clock (the Phase A/B path) or when sticky is disabled (.Mark checks .enabled).
	if ctx.routing != nil && ctx.routing.Sticky != nil {
		ctx.routing.Sticky.Mark()
	}
	return result, nil
}

// releaseTxConn returns the tx's OWNED *sql.Conn to the pool, or DESTROYS it when poisoned (a failed
// ROLLBACK/COMMIT left it in an unknown state). A bad-conn Raw makes database/sql discard the
// underlying driver connection instead of pooling it (the Go analogue of the TS `pool.release(conn,
// destroy)` — a poisoned connection must not re-enter the pool). Close after a destroy is a safe no-op.
func releaseTxConn(conn *sql.Conn, poisoned *bool) {
	if *poisoned {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	_ = conn.Close()
}

// WithTransaction is the simple form of [WithTransactionDecided]: a nil error ⇒ COMMIT + return the
// value; a non-nil error ⇒ ROLLBACK + re-raise. For a body that never legitimately rolls back with a
// value.
func WithTransaction[R any](ctx *ExecutionContext, db TxDB, body func(txCtx *ExecutionContext) (R, error)) (R, error) {
	return WithTransactionDecided(ctx, db, func(txCtx *ExecutionContext) (R, TxDecision, error) {
		r, err := body(txCtx)
		return r, Commit(), err
	})
}

// ── The PUBLIC user-controlled transaction boundary (Phase B-core / #86, go port) ──

// Transaction is **the public user-controlled transaction boundary** (#86, go port of the TS
// `transaction` / rust `transaction`) — the REAL transaction feature v2 was missing.
// `Transaction(ctx, db, dialect, options, body)` opens ONE boundary the caller wraps around MULTIPLE
// arbitrary operations so they commit or roll back TOGETHER:
//
//	Transaction(ctx, db, "postgres", opts, func(txCtx *ExecutionContext) error {
//	    _, err := ExecuteTransactionBundleCtx(aBundle, aInput, txCtx, true) // ← every op inside JOINS
//	    if err != nil { return err }                                        //    this ONE boundary:
//	    _, err = ExecuteTransactionBundleCtx(bBundle, bInput, txCtx, true)  //    one conn, one BEGIN…COMMIT
//	    return err
//	})
//
// # What it does (v1 `DBModel.transaction` :2787 parity, on the SCP seam)
//
// It checks out ONE owned *sql.Conn (db.Conn), issues the isolation-aware BEGIN as REAL SQL THROUGH
// the seam ([BeginStatements]: PG `BEGIN ISOLATION LEVEL …`; MySQL a preceding `SET TRANSACTION
// ISOLATION LEVEL …` then `BEGIN`), pins that connection into a tx-scoped [ExecutionContext], runs
// `body(txCtx)`, then COMMITs (or ROLLBACKs on a body error / options.RollbackOnly) — also THROUGH the
// seam, so a registered middleware OBSERVES the runtime tx-control — with the #81 retry loop (deadlock
// / serialization / connection error) wrapped around the WHOLE boundary, a FRESH owned *sql.Conn per
// attempt (re-issuing BEGIN through the seam each time). It drives [WithTransactionDecidedIsolated] per
// attempt (the ONE mechanism).
//
// # The ambient-tx JOIN — how operations participate (the core #86 fix; go = explicit ctx)
//
// `body` receives the tx-scoped ctx EXPLICITLY (the go-idiomatic decision — no task-local; the TS
// pins it in an async-local, go/rust thread it by argument). Every operation `body` issues receives
// THAT ctx: a write via [ExecuteTransactionBundleCtx], a read via the read seam. Because that ctx's
// InTransaction() is already true (a connection is pinned), the write's own tx-bracketing DETECTS
// the ambient tx and JOINS it — running its statements on the pinned owned *sql.Conn WITHOUT opening
// its own BEGIN/COMMIT (see [TransactionDecided]'s nested-join). So N operations inside one
// Transaction() produce exactly ONE BEGIN + ONE COMMIT on ONE connection. Outside a Transaction()
// the ctx's InTransaction() is false, so a bare guarded write is rejected with
// [WriteOutsideTransaction].
//
// NESTED Transaction() joins the outer (one physical BEGIN/COMMIT; an inner error rolls back the
// WHOLE tx). Isolation/retry/RollbackOnly options on a nested call are IGNORED (the outer owns them).
func Transaction[R any](ctx *ExecutionContext, db TxDB, dialectName string, options TransactionOptions, body func(txCtx *ExecutionContext) (R, error)) (R, error) {
	return TransactionDecided(ctx, db, dialectName, options, func(txCtx *ExecutionContext) (R, TxDecision, error) {
		r, err := body(txCtx)
		return r, Commit(), err
	})
}

// TransactionDecided is the [TxDecision]-returning form of [Transaction] (Phase B / #83): `body`
// decides COMMIT vs ROLLBACK (a gate short-circuit returns a [Rollback] decision with committed:false
// — a legitimate non-error outcome), an error always rolls back + retries/re-raises. This is what the
// write-tx plan executor ([ExecuteTransactionBundleCtx]) drives, so a gate-first plan run inside a
// user Transaction() short-circuits correctly while still JOINING.
//
// Handles the three Phase B concerns the plain [WithTransactionDecided] does not:
//  1. nested-join — if ctx.InTransaction() is already true (an outer Transaction() pinned a
//     connection), run `body` on the OUTER ctx with NO new BEGIN/COMMIT/acquire (the inner body is
//     part of the outer physical tx; an inner error propagates and rolls back the WHOLE tx);
//  2. RollbackOnly — a successful Commit(r) is rewritten to Rollback(r) (the body result is returned
//     but NO change commits — dry-run/preview);
//  3. retry — a retryable failure (deadlock / serialization / connection error, via
//     [IsRetryableTxError]) re-runs the WHOLE boundary on a FRESH owned *sql.Conn (re-issuing BEGIN
//     through the seam), up to RetryLimit, with exponential backoff RetryDurationMs · 2^(k-1). A
//     non-retryable error re-raises immediately.
func TransactionDecided[R any](ctx *ExecutionContext, db TxDB, dialectName string, options TransactionOptions, body func(txCtx *ExecutionContext) (R, TxDecision, error)) (R, error) {
	var zero R
	// NESTED-TX JOIN (mirror v1 depth+1): already inside a tx on this ctx ⇒ join the outer. No new
	// connection, no BEGIN/COMMIT — the inner body is part of the outer physical transaction.
	// Isolation/retry/RollbackOnly on a nested call are ignored: the outer owns the envelope. A gate
	// Rollback here still returns its value (the caller reports committed:false) WITHOUT rolling back
	// the outer — the outer decides its own COMMIT/ROLLBACK.
	if ctx.InTransaction() {
		r, _, err := body(ctx)
		if err != nil {
			return zero, err
		}
		return r, nil
	}

	// A zero RetryLimit (e.g. a hand-built options value) floors to 1 — always at least one attempt.
	retryLimit := 1
	if options.RetryOnError {
		retryLimit = options.RetryLimit
		if retryLimit < 1 {
			retryLimit = 1
		}
	}
	rollbackOnly := options.RollbackOnly

	attempt := 0
	for {
		attempt++
		// ONE attempt on a FRESH owned *sql.Conn (a retry after a connection error thus RECONNECTS).
		result, err := WithTransactionDecidedIsolated(ctx, db, options.Isolation, dialectName, func(txCtx *ExecutionContext) (R, TxDecision, error) {
			r, decision, bErr := body(txCtx)
			if bErr != nil {
				return zero, Commit(), bErr
			}
			// RollbackOnly (dry-run): a SUCCESSFUL commit becomes a ROLLBACK, still returning the body
			// result — no committed change. An explicit gate Rollback stays a rollback.
			if rollbackOnly {
				return r, Rollback(), nil
			}
			return r, decision, nil
		})
		if err == nil {
			return result, nil
		}
		if attempt < retryLimit && IsRetryableTxError(err) {
			// Exponential backoff before RETRYing the whole transaction on a fresh connection.
			backoff := time.Duration(options.RetryDurationMs) * time.Millisecond * time.Duration(int64(1)<<(attempt-1))
			if backoff > 0 {
				time.Sleep(backoff)
			}
			continue
		}
		return zero, err
	}
}
