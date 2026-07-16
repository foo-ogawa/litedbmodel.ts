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
// A transaction acquires ONE connection via *sql.Tx (db.Begin() — Go's connection-owning primitive:
// a *sql.Tx binds exactly one pooled connection for its whole lifetime, the analogue of v1
// `PoolTransaction`), pins it into a tx-scoped [ExecutionContext] threaded on a context.Context,
// runs its body (every statement resolves that *sql.Tx via ConnectionFor), COMMITs/ROLLBACKs on the
// SAME *sql.Tx, and releases it. Concurrent transactions each own a DISTINCT *sql.Tx over a DISTINCT
// pooled connection ⇒ isolated. There is NO driver-global tx slot (Go never had one — the write path
// already threaded a *sql.Tx; this seam makes that ownership central + explicit, and forbids any
// shared-tx state re-appearing).

package litedbmodel_runtime

import (
	"context"
	"database/sql"

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
// it wraps the tx-owned *sql.Tx. The seam is the ONLY caller; the runtime SQL path never touches a
// db.Query / db.Exec directly.
type Connection interface {
	// Execute runs a SELECT / RETURNING statement; returns the raw rows.
	Execute(sql string, args []any) ([]bc.Value, error)
	// Run runs a non-returning write / DDL / tx-control statement; returns the affected summary.
	Run(sql string, args []any) (RunInfo, error)
}

// dbConnection adapts a raw SQLDB (the primary db, a *sql.DB — or a *sql.Tx for the tx path) to the
// [Connection] seam. Execute/Run are the SAME queryRows / execWrite the runtime used directly before
// the seam — so a ctx built via [ContextForDB] is byte-identical to the old raw-db path (the
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
// passthrough — wrap returns next(sql, args) verbatim, so Phase A behavior is byte-identical. Phase A
// always constructs an empty chain; the registration API + native middleware entries are Phase D
// (this is only the hook point).
type MiddlewareChain struct {
	read  []ReadMiddleware
	write []WriteMiddleware
}

// NewMiddlewareChain returns a new empty chain (Phase A default — pure passthrough).
func NewMiddlewareChain() *MiddlewareChain { return &MiddlewareChain{} }

// IsEmpty reports whether the chain is empty (⇒ wrap is a guaranteed passthrough).
func (m *MiddlewareChain) IsEmpty() bool { return len(m.read) == 0 && len(m.write) == 0 }

// wrapRead folds the READ chain around `next`, then invokes it. Empty ⇒ next(sql, args) verbatim.
func (m *MiddlewareChain) wrapRead(sql string, args []any, next SeamNext[[]bc.Value]) ([]bc.Value, error) {
	fn := next
	for i := len(m.read) - 1; i >= 0; i-- {
		mw := m.read[i]
		inner := fn
		fn = func(s string, p []any) ([]bc.Value, error) { return mw(s, p, inner) }
	}
	return fn(sql, args)
}

// wrapWrite folds the WRITE chain around `next`, then invokes it. Empty ⇒ next(sql, args) verbatim.
func (m *MiddlewareChain) wrapWrite(sql string, args []any, next SeamNext[RunInfo]) (RunInfo, error) {
	fn := next
	for i := len(m.write) - 1; i >= 0; i-- {
		mw := m.write[i]
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
}

// sharedEmptyChain is the Phase A middleware chain — an empty passthrough so [ContextForDB] can build
// a ctx without the caller owning a chain. Phase D swaps this for a per-ctx registered chain.
var sharedEmptyChain = NewMiddlewareChain()

// ContextForDB is the **backward-compat wrapper (§6)**: wrap a raw SQLDB in a thin [ExecutionContext]
// (primary = db, the shared EMPTY middleware chain, no pinned tx connection, a single DB, a
// background Go context). Existing callers (conformance / livedb / bench / unit that pass a raw db)
// keep working **byte-identically** — the seam is a pure passthrough to queryRows / execWrite. This
// is the Go analogue of the TS contextForDriver / rust for_driver (§6).
func ContextForDB(db SQLDB) *ExecutionContext {
	return &ExecutionContext{ctx: context.Background(), db: db, middleware: sharedEmptyChain}
}

// ContextForDBWith wraps a raw SQLDB with a caller-supplied context.Context + [MiddlewareChain]
// (Phase A: pass an empty chain; Phase D: a registered chain). Primary = db, no pinned tx
// connection, a single DB. Prefer [ContextForDB] when there is neither a middleware chain nor a
// non-background Go context.
func ContextForDBWith(ctx context.Context, db SQLDB, middleware *MiddlewareChain) *ExecutionContext {
	if middleware == nil {
		middleware = sharedEmptyChain
	}
	return &ExecutionContext{ctx: ctx, db: db, middleware: middleware}
}

// Context returns the Go context.Context this ExecutionContext rides.
func (c *ExecutionContext) Context() context.Context { return c.ctx }

// Middleware returns the middleware chain this ctx carries (§4).
func (c *ExecutionContext) Middleware() *MiddlewareChain { return c.middleware }

// DB returns the primary db (for the pooled read fan-out, which needs the goroutine-safe *sql.DB the
// concurrent sibling relations share — the tx path never fans out, so this is the non-tx provider).
func (c *ExecutionContext) DB() SQLDB { return c.db }

// InTransaction reports whether this is a tx-scoped ctx (a pinned connection is present).
func (c *ExecutionContext) InTransaction() bool { return c.pinned != nil }

// ConnectionFor resolves WHICH connection a statement runs on (§3). Phase A resolution: the tx-owned
// (pinned) connection wins; else the primary db. Reader/writer split (§3-2/3) + named-DB routing
// (§3-4) extend HERE in B/C/D — the seam does not change.
func (c *ExecutionContext) ConnectionFor(intent StatementIntent) Connection {
	if c.pinned != nil {
		return c.pinned
	}
	return dbConnection{db: c.db}
}

// WithTxConnection derives a tx-scoped ctx pinning `conn` (every statement resolves it while this ctx
// is used). The derived ctx shares the primary db + middleware chain + Go context; ConnectionFor
// returns the pinned tx connection instead of the db. This is the Go analogue of the TS
// withConnection(conn, true) / rust with_tx_connection.
func (c *ExecutionContext) WithTxConnection(conn Connection) *ExecutionContext {
	return &ExecutionContext{ctx: c.ctx, db: c.db, middleware: c.middleware, pinned: conn}
}

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

// ── Per-execution transaction ownership (§3) — the concurrent-tx fix ──────────

// txConnection is the OWNED transaction connection — the Go analogue of v1 `PoolTransaction`. It
// wraps ONE *sql.Tx (Go's connection-owning primitive: db.Begin() binds exactly one pooled connection
// to the *sql.Tx for the whole transaction). Because a *sql.Tx satisfies SQLDB (Query/Exec), every
// statement in the tx body runs on it through the SAME queryRows / execWrite as the non-tx path — but
// pinned so ConnectionFor always returns THIS connection. Concurrent transactions each hold a DISTINCT
// *sql.Tx over a DISTINCT pooled connection, so their writes never cross-talk.
type txConnection struct {
	tx *sql.Tx
}

func (c txConnection) Execute(sql string, args []any) ([]bc.Value, error) {
	return queryRows(c.tx, sql, args)
}

func (c txConnection) Run(sql string, args []any) (RunInfo, error) {
	changes, lastInsert, err := execWrite(c.tx, sql, args)
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
//  1. acquire ONE connection via db.Begin() → a *sql.Tx (the tx's exclusive connection; BEGIN issued
//     by database/sql on it), the Go analogue of v1 PoolTransaction;
//  2. pin it into a tx-scoped [ExecutionContext] so EVERY statement `body` issues resolves THAT
//     connection via ConnectionFor — never a fresh pooled one;
//  3. run body(txCtx) → COMMIT / ROLLBACK on the OWNED *sql.Tx per the returned decision; on any
//     error ROLLBACK (best-effort) and re-raise;
//  4. the owned connection is released back to the pool when *sql.Tx is committed / rolled back.
//
// Concurrent calls each acquire a DISTINCT *sql.Tx, so their writes never cross-talk — the isolation a
// shared-slot model would violate. This mirrors the TS withTransactionAsync (#75) / rust
// with_transaction_decided (#76).
func WithTransactionDecided[R any](ctx *ExecutionContext, db TxDB, body func(txCtx *ExecutionContext) (R, TxDecision, error)) (R, error) {
	var zero R
	// Acquire the tx's OWNED connection (BEGIN issued by database/sql inside Begin).
	tx, err := db.Begin()
	if err != nil {
		return zero, mapSqliteError(err)
	}
	// Pin the *sql.Tx into a tx-scoped ctx: every statement the body issues resolves THIS connection.
	txCtx := ctx.WithTxConnection(txConnection{tx: tx})

	result, decision, bodyErr := body(txCtx)
	if bodyErr != nil {
		// A body error rolls back (best-effort) and re-raises; the original failure is surfaced.
		_ = tx.Rollback()
		return zero, bodyErr
	}
	if decision.Rollback {
		// A legitimate non-error rollback (e.g. a gate short-circuit): roll back, return the value. A
		// rollback failure here IS surfaced (the connection would be in an unknown state).
		if rbErr := tx.Rollback(); rbErr != nil {
			return zero, mapSqliteError(rbErr)
		}
		return result, nil
	}
	if cErr := tx.Commit(); cErr != nil {
		_ = tx.Rollback()
		return zero, mapSqliteError(cErr)
	}
	return result, nil
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
