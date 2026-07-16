//! litedbmodel v2 SCP — the **ExecutionContext + central execute/run seam** (Phase A / #76, rust).
//!
//! The rust port of the TS contract-defining artifact `src/scp/exec-context.ts` (#75). It replaces
//! the raw `&dyn Driver` threaded through [`execute_bundle`](crate::runtime::execute_bundle) /
//! [`execute_read_graph`](crate::static_bundle::execute_read_graph) /
//! [`execute_transaction_bundle`](crate::runtime::execute_transaction_bundle) / the relation walker
//! with an [`ExecutionContext`] that carries:
//!
//!   1. a **connection provider** — [`ExecutionContext::connection_for`]`(intent)` resolves WHICH
//!      connection a statement runs on (the tx-owned connection, else the primary driver; Phase A
//!      wires only the tx-owned + single-DB cases, reader/writer/named-DB are B/C/D on this seam);
//!   2. a **middleware chain** — [`ExecutionContext::middleware`], wrapping every SQL (empty in
//!      Phase A = passthrough; the registration API is Phase D — this is only the hook point);
//!   3. a **pinned tx connection** — a tx-scoped ctx pins ONE owned connection so every statement in
//!      a transaction body runs on it (per-execution connection ownership, §3).
//!
//! ## The central seam (§2) — ALL SQL funnels through here
//!
//! ```text
//!   execute(ctx, sql, params) -> Rows      // SELECT / RETURNING reads
//!   run(ctx, sql, params)     -> RunInfo   // INSERT/UPDATE/DELETE, BEGIN/COMMIT/ROLLBACK
//! ```
//!
//! Both do the SAME three things, in order:
//!   ① run the middleware chain (empty ⇒ passthrough, behavior unchanged);
//!   ② resolve the connection via `ctx.connection_for(intent)`;
//!   ③ execute on that connection (the ONLY driver contact point).
//!
//! Every direct `driver.prepare(sql).all()/run()` in the read / tx / relation path is replaced by a
//! call through this seam. A `grep` for `.prepare(` outside the connection adapters
//! ([`DriverConnection`] and the tx handles) comes up empty in the runtime SQL path — that is the AC.
//!
//! ## ONE interface, not sync/async-bifurcated (contract flag)
//!
//! The TS reference bifurcates into `ExecutionContext`/`execute` (better-sqlite3, sync) and
//! `AsyncExecutionContext`/`executeAsync` (pg/mysql2, async) because that split is TS-runtime
//! specific. Per the #76 contract flags the rust port **collapses to ONE interface**: the rust
//! [`Driver`] seam is already a synchronous facade over async pools (each live driver `block_on`s the
//! pooled future internally), so there is exactly ONE [`ExecutionContext`] / one [`execute`] / one
//! [`run`]. There is likewise **no `executeSafeIntegers`** — that is a better-sqlite3 #59 BIGINT
//! toggle; rust/PG/MySQL return BIGINT natively (i64 / string), so the read seam is a plain execute.
//!
//! ## Per-execution connection ownership (§3) — the concurrent-tx fix
//!
//! A transaction acquires ONE connection via [`Driver::begin_tx`] (a [`TxConnection`] owned handle —
//! the rust analogue of v1 `litedbmodel.rs` `PoolTransaction`), pins it into a tx-scoped
//! [`ExecutionContext`], runs its body (every statement resolves that connection via
//! `connection_for`), COMMITs/ROLLBACKs on the SAME owned connection, and releases it (dropped/back to
//! the pool). Concurrent transactions each own a DISTINCT pooled connection ⇒ isolated. There is NO
//! driver-global single-slot writer (the removed `writer: Mutex<Option<...>>` on the PG/MySQL drivers
//! was exactly the shared-slot model that corrupts concurrent transactions).

use behavior_contracts::Value;

use crate::driver::{Driver, RunInfo};
use crate::errors::SqlFailure;

// ── Statement intent & the driver contact (§5) ────────────────────────────────

/// What a statement needs from the connection provider (§3): whether it writes (so it must go to a
/// writer / the tx-owned connection, never a read replica) and an optional named DB (multi-DB
/// routing, Phase B). Phase A resolves only `write` (tx-owned vs. primary) and ignores `db` (single
/// DB); the field is in the contract now so B/C/D extend the resolver — not the seam.
#[derive(Debug, Clone, Default)]
pub struct StatementIntent {
    /// The statement writes (INSERT/UPDATE/DELETE or BEGIN/COMMIT/ROLLBACK) ⇒ writer / tx connection.
    pub write: bool,
    /// Named-DB routing key (multi-DB, Phase B). `None` ⇒ the primary DB.
    pub db: Option<String>,
}

impl StatementIntent {
    /// A read intent (`write = false`, primary DB).
    pub fn read() -> Self {
        StatementIntent {
            write: false,
            db: None,
        }
    }
    /// A write intent (`write = true`, primary DB).
    pub fn write() -> Self {
        StatementIntent {
            write: true,
            db: None,
        }
    }
}

// ── The ONE driver contact point (§5) — a Connection ──────────────────────────

/// The ONE driver contact point (§5): a resolved connection a statement runs on. Outside a tx this
/// is the primary [`Driver`] (each call `prepare`s — for a live pooled driver, one pooled connection
/// per call); inside a tx it is the tx-owned [`TxConnection`] handle. The seam is the ONLY caller;
/// the runtime SQL path never touches a `driver.prepare(...)` directly.
pub trait Connection {
    /// Run a SELECT / RETURNING statement; return the raw rows.
    fn execute(&self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure>;
    /// Run a non-returning write / DDL / tx-control statement; return the affected summary.
    fn run(&self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure>;
}

/// Adapt a raw [`Driver`] to the [`Connection`] seam (the ONE driver contact for the non-tx path).
/// `execute`/`run` are the SAME `driver.prepare(sql).all()/run()` the runtime used directly before
/// the seam — so a ctx built via [`ExecutionContext::for_driver`] is byte-identical to the old raw
/// driver path (the backward-compat wrapper, §6).
pub struct DriverConnection<'a> {
    driver: &'a dyn Driver,
}

impl<'a> DriverConnection<'a> {
    pub fn new(driver: &'a dyn Driver) -> Self {
        DriverConnection { driver }
    }
}

impl Connection for DriverConnection<'_> {
    fn execute(&self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        self.driver.prepare(sql).all(params)
    }
    fn run(&self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        self.driver.prepare(sql).run(params)
    }
}

// ── Per-execution transaction ownership (§3) — the PoolTransaction analogue ────

/// An OWNED transaction connection — the rust analogue of v1 `litedbmodel.rs` `PoolTransaction`
/// (`handler.rs`). Acquired by [`Driver::begin_tx`], it holds ONE connection for the transaction's
/// whole duration: every statement in the tx body runs on it (`execute`/`run`), and the tx ends by
/// consuming the handle via [`TxConnection::commit`] / [`TxConnection::rollback`] (which run the
/// COMMIT/ROLLBACK on the SAME owned connection, then release it — dropped, or back to the pool).
///
/// Concurrent transactions each hold a DISTINCT handle over a DISTINCT pooled connection, so their
/// writes never cross-talk — the isolation the removed driver-global `writer` slot violated.
pub trait TxConnection {
    /// Run a SELECT / RETURNING statement on the tx's owned connection.
    fn execute(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure>;
    /// Run a non-returning write / DDL statement on the tx's owned connection.
    fn run(&mut self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure>;
    /// COMMIT on the owned connection, then release it. Consumes the handle.
    fn commit(self: Box<Self>) -> Result<(), SqlFailure>;
    /// ROLLBACK on the owned connection, then release it (best-effort). Consumes the handle.
    fn rollback(self: Box<Self>) -> Result<(), SqlFailure>;
}

/// The shared, interior-mutable slot holding a tx's OWNED connection for the duration of a tx-scoped
/// ctx. The `Option` lets [`with_transaction`] `take()` the handle back out (to consume it via
/// commit/rollback) after the tx body's borrow ends, without moving the cell itself. It is always
/// `Some` while the tx body runs (a statement resolving `None` here is a runtime invariant bug).
pub type TxSlot<'t> = std::cell::RefCell<Option<Box<dyn TxConnection + 't>>>;

/// A [`Connection`] view over the shared tx slot. The seam resolves this (via `connection_for`) for
/// every statement inside a tx, so all of them run on the SAME owned connection. Interior mutability
/// (`RefCell`) is needed because `Connection::execute`/`run` take `&self` (the seam is shared over the
/// ctx) while [`TxConnection`] takes `&mut self` (an owned connection is used exclusively — a tx body
/// is not concurrent with itself).
pub struct TxConnectionRef<'s, 't> {
    slot: &'s TxSlot<'t>,
}

impl<'s, 't> TxConnectionRef<'s, 't> {
    pub fn new(slot: &'s TxSlot<'t>) -> Self {
        TxConnectionRef { slot }
    }

    fn missing() -> SqlFailure {
        SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: "scp runtime: tx-scoped statement resolved an empty tx connection slot \
                      (the owned connection was already consumed)"
                .into(),
        }
    }
}

impl Connection for TxConnectionRef<'_, '_> {
    fn execute(&self, sql: &str, params: &[Value]) -> Result<Vec<Value>, SqlFailure> {
        match self.slot.borrow_mut().as_mut() {
            Some(tx) => tx.execute(sql, params),
            None => Err(Self::missing()),
        }
    }
    fn run(&self, sql: &str, params: &[Value]) -> Result<RunInfo, SqlFailure> {
        match self.slot.borrow_mut().as_mut() {
            Some(tx) => tx.run(sql, params),
            None => Err(Self::missing()),
        }
    }
}

// ── Middleware chain (§4) — the hook point (empty in Phase A) ──────────────────

/// The terminal of a middleware chain: resolve the connection + execute (the seam's ②③).
pub type SeamNext<'n, T> = dyn Fn(&str, &[Value]) -> Result<T, SqlFailure> + 'n;

/// One middleware: wrap a statement, delegating to `next` (Phase D supplies the registration API).
/// Generic over the seam result `T` (rows or a run summary) via the two concrete aliases below so ONE
/// registration shape serves both the read and write seams.
pub trait Middleware<T>: Send + Sync {
    fn wrap(&self, sql: &str, params: &[Value], next: &SeamNext<T>) -> Result<T, SqlFailure>;
}

/// The ordered middleware chain a ctx carries (§4). `wrap` folds the middlewares around `next` (the
/// connection-resolve + execute terminal). An EMPTY chain is a pure passthrough — `wrap` returns
/// `next(sql, params)` verbatim, so Phase A behavior is byte-identical. Phase A always constructs an
/// empty chain; the registration API + native middleware entries are Phase D (this is only the hook
/// point). Kept homogeneous per result type (`read`/`write`) so the fold is monomorphic and cheap.
#[derive(Default)]
pub struct MiddlewareChain {
    read: Vec<Box<dyn Middleware<Vec<Value>>>>,
    write: Vec<Box<dyn Middleware<RunInfo>>>,
}

impl MiddlewareChain {
    /// A new empty chain (Phase A default — pure passthrough).
    pub fn new() -> Self {
        MiddlewareChain::default()
    }

    /// Is the chain empty (⇒ `wrap` is a guaranteed passthrough)?
    pub fn is_empty(&self) -> bool {
        self.read.is_empty() && self.write.is_empty()
    }

    /// Fold the READ chain around `next`, then invoke it. Empty ⇒ `next(sql, params)` verbatim.
    fn wrap_read(
        &self,
        sql: &str,
        params: &[Value],
        next: &SeamNext<Vec<Value>>,
    ) -> Result<Vec<Value>, SqlFailure> {
        wrap_chain(&self.read, sql, params, next)
    }

    /// Fold the WRITE chain around `next`, then invoke it. Empty ⇒ `next(sql, params)` verbatim.
    fn wrap_write(
        &self,
        sql: &str,
        params: &[Value],
        next: &SeamNext<RunInfo>,
    ) -> Result<RunInfo, SqlFailure> {
        wrap_chain(&self.write, sql, params, next)
    }
}

/// Fold a homogeneous middleware stack around `next`. Empty ⇒ pure passthrough (`next` verbatim).
fn wrap_chain<T>(
    stack: &[Box<dyn Middleware<T>>],
    sql: &str,
    params: &[Value],
    next: &SeamNext<T>,
) -> Result<T, SqlFailure> {
    if stack.is_empty() {
        return next(sql, params);
    }
    // Fold outermost-first: mw[0] wraps mw[1] wraps ... wraps next.
    fn go<'a, T>(
        stack: &'a [Box<dyn Middleware<T>>],
        sql: &str,
        params: &[Value],
        next: &SeamNext<'a, T>,
    ) -> Result<T, SqlFailure> {
        match stack.split_first() {
            None => next(sql, params),
            Some((head, tail)) => {
                let inner = move |s: &str, p: &[Value]| go(tail, s, p, next);
                head.wrap(sql, params, &inner)
            }
        }
    }
    go(stack, sql, params, next)
}

// ── The ExecutionContext (§2 / §5) — ONE interface ────────────────────────────

/// The execution context threaded (explicitly, by argument — the approved rust-idiomatic decision,
/// not a task-local) through `execute_bundle` / `execute_read_graph` / `execute_transaction_bundle` /
/// the relation walker in place of a raw `&dyn Driver`. It carries the connection provider (the
/// primary driver + an optional pinned tx connection), the middleware chain, and derives a tx-scoped
/// ctx via [`ExecutionContext::with_tx_connection`].
///
/// Lifetimes: `'a` borrows the primary driver + the middleware chain; `'t` borrows the pinned tx
/// connection (only present in a tx-scoped ctx). The base ctx has `'t == 'a` and no pin.
pub struct ExecutionContext<'a, 't> {
    /// The primary driver — the non-tx connection provider (each `prepare` = one pooled conn for a
    /// live driver; the SAME in-proc connection for SQLite).
    driver: &'a dyn Driver,
    /// The middleware chain wrapping every SQL (§4). Empty in Phase A.
    middleware: &'a MiddlewareChain,
    /// The pinned tx connection slot (present ⇒ this is a tx-scoped ctx; every statement resolves it).
    /// The slot's tx handle borrows the primary driver (`'a`); `'t` is the (shorter) borrow of the
    /// slot itself — keeping them distinct avoids the invariant-lifetime borrow conflict when
    /// [`with_transaction`] takes the handle back out.
    pinned: Option<&'t TxSlot<'a>>,
    /// The READ-ONLY marker (Phase B / #82 write=tx guard — mirror v1 `with_writer` / the TS
    /// `withReadOnly` ALS marker): a write in a read-only-scoped ctx is REJECTED
    /// ([`crate::tx_options::write_in_read_only`]). The explicit-ctx analogue of the TS async-local
    /// read-only marker; derived via [`ExecutionContext::with_read_only`].
    read_only: bool,
}

/// The shared empty (Phase A) middleware chain — a `'static` passthrough so [`for_driver`] can build
/// a ctx without the caller owning a chain. Phase D swaps this for a per-ctx registered chain.
fn empty_chain() -> &'static MiddlewareChain {
    static EMPTY: std::sync::OnceLock<MiddlewareChain> = std::sync::OnceLock::new();
    EMPTY.get_or_init(MiddlewareChain::new)
}

/// **Backward-compat wrapper (§6).** Wrap a raw [`Driver`] in a thin [`ExecutionContext`]: the
/// primary = the driver, the shared EMPTY middleware chain, no pinned tx connection, a single DB.
/// Existing callers (conformance / livedb / bench / unit that pass a raw driver) keep working
/// **byte-identically** — the seam is a pure passthrough to `driver.prepare(...).all()/run()`. This is
/// the rust analogue of the TS `contextForDriver` (§6).
pub fn for_driver(driver: &dyn Driver) -> ExecutionContext<'_, '_> {
    ExecutionContext::new(driver, empty_chain())
}

impl<'a> ExecutionContext<'a, 'a> {
    /// Wrap a raw [`Driver`] with a caller-supplied [`MiddlewareChain`] (Phase A: pass an empty one;
    /// Phase D: a registered chain). The primary = the driver, no pinned tx connection, a single DB.
    /// Prefer [`for_driver`] when there is no middleware.
    pub fn new(driver: &'a dyn Driver, middleware: &'a MiddlewareChain) -> Self {
        ExecutionContext {
            driver,
            middleware,
            pinned: None,
            read_only: false,
        }
    }
}

impl<'a, 't> ExecutionContext<'a, 't> {
    /// The middleware chain this ctx carries (§4).
    pub fn middleware(&self) -> &MiddlewareChain {
        self.middleware
    }

    /// The primary driver (for the pooled read fan-out, which needs a `Sync` driver reference the
    /// executor threads share — the tx path never fans out, so this is the non-tx provider).
    pub fn driver(&self) -> &'a dyn Driver {
        self.driver
    }

    /// Is this a tx-scoped ctx (a pinned connection is present)? This is the explicit-ctx analogue of
    /// the TS async-local "inside a transaction" marker — the write=tx guard reads it, and the public
    /// [`transaction`] boundary reads it for NESTED-tx join detection.
    pub fn in_transaction(&self) -> bool {
        self.pinned.is_some()
    }

    /// Is this a READ-ONLY-scoped ctx (Phase B / #82 write=tx guard)? A write here is REJECTED
    /// ([`crate::tx_options::write_in_read_only`]) — the explicit-ctx analogue of the TS `withReadOnly`
    /// / v1 `with_writer` read-only marker. Derived via [`ExecutionContext::with_read_only`].
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// Derive a READ-ONLY-scoped ctx (mirror v1 `with_writer` / the TS `withReadOnly`): reads are
    /// allowed, but ANY write funneled through the GUARDED write seam ([`run_guarded`]) throws
    /// [`crate::tx_options::write_in_read_only`]. Used for a writer-pinned read scope that must never
    /// accidentally mutate.
    pub fn with_read_only(&self) -> ExecutionContext<'a, 't> {
        ExecutionContext {
            driver: self.driver,
            middleware: self.middleware,
            pinned: self.pinned,
            read_only: true,
        }
    }

    /// Resolve WHICH connection a statement runs on (§3). Phase A resolution: the tx-owned (pinned)
    /// connection wins; else the primary driver. Reader/writer split (§3-2/3) + named-DB routing
    /// (§3-4) extend HERE in B/C/D — the seam does not change.
    fn connection_for<'s>(&'s self, _intent: &StatementIntent) -> Box<dyn Connection + 's> {
        match self.pinned {
            Some(slot) => Box::new(TxConnectionRef::new(slot)),
            None => Box::new(DriverConnection::new(self.driver)),
        }
    }

    /// Derive a tx-scoped ctx pinning `slot` (every statement resolves it while this ctx is used). The
    /// derived ctx shares the primary driver + middleware chain; `connection_for` returns the pinned
    /// tx connection instead of the driver. `'x` is the borrow of the slot (shorter than `'a`, the
    /// driver borrow the tx handle inside the slot holds).
    pub fn with_tx_connection<'x>(&self, slot: &'x TxSlot<'a>) -> ExecutionContext<'a, 'x> {
        ExecutionContext {
            driver: self.driver,
            middleware: self.middleware,
            pinned: Some(slot),
            // A tx-scoped ctx INHERITS the read-only marker: a `transaction()` opened inside a
            // read-only scope is still read-only (v1 parity — `with_writer` reads never mutate).
            read_only: self.read_only,
        }
    }
}

// ── The central seam (§2) — the ONLY place SQL meets a connection ─────────────

/// Central READ seam: ① middleware chain, ② resolve the connection, ③ execute. Every read (primary
/// read node, relation batch, tx-body SELECT/RETURNING) funnels through here.
pub fn execute(
    ctx: &ExecutionContext,
    sql: &str,
    params: &[Value],
    intent: &StatementIntent,
) -> Result<Vec<Value>, SqlFailure> {
    let conn = ctx.connection_for(intent);
    ctx.middleware
        .wrap_read(sql, params, &move |s, p| conn.execute(s, p))
}

/// Central WRITE seam: ① middleware chain, ② resolve the connection, ③ run. Every write and every
/// tx-control statement (BEGIN/COMMIT/ROLLBACK on the non-tx driver path) funnels through here.
pub fn run(
    ctx: &ExecutionContext,
    sql: &str,
    params: &[Value],
    intent: &StatementIntent,
) -> Result<RunInfo, SqlFailure> {
    let conn = ctx.connection_for(intent);
    ctx.middleware
        .wrap_write(sql, params, &move |s, p| conn.run(s, p))
}

/// GUARDED write seam (Phase B / #82): enforce the write=tx guard
/// ([`crate::tx_options::check_write_allowed`]) for a DATA-mutating statement, then delegate to
/// [`run`]. A write issued OUTSIDE a transaction throws [`crate::tx_options::write_outside_transaction`];
/// a write in a [`ExecutionContext::with_read_only`] scope throws
/// [`crate::tx_options::write_in_read_only`] (read-only checked FIRST, v1 order). Tx-control statements
/// (BEGIN/COMMIT/ROLLBACK/SET) are NOT guarded — the tx runtime issues them to OPEN the very scope the
/// guard checks. This is the seam a bare model-level write (create/update/delete/upsert/batch) goes
/// through. The guard reads the CALLER's ctx markers (`in_transaction` / `read_only`) — the
/// explicit-ctx analogue of the TS async-local markers.
pub fn run_guarded(
    ctx: &ExecutionContext,
    sql: &str,
    params: &[Value],
    operation: &str,
    model: Option<&str>,
) -> Result<RunInfo, SqlFailure> {
    crate::tx_options::check_write_allowed(
        operation,
        model,
        ctx.in_transaction(),
        ctx.read_only(),
    )?;
    run(ctx, sql, params, &StatementIntent::write())
}

// ── The per-execution-ownership transaction (§3) — the concurrent-tx fix ──────

/// The body's decision about how to end the transaction — so a body can legitimately ROLLBACK and
/// STILL return a value (the gate short-circuit: a failed gate rolls back but is NOT an error, it
/// returns `committed:false`). An `Err` from the body always rolls back + re-raises regardless.
pub enum TxDecision<R> {
    /// COMMIT the owned connection, then return `R`.
    Commit(R),
    /// ROLLBACK the owned connection, then return `R` (a legitimate non-error outcome).
    Rollback(R),
}

/// Run `body` inside a transaction with **per-execution connection ownership** (§3, the concurrent-tx
/// fix). This is the general form: `body` decides COMMIT vs ROLLBACK (see [`TxDecision`]); an `Err`
/// always rolls back and re-raises.
///
///   1. acquire ONE connection via [`Driver::begin_tx`] (the tx's exclusive connection; BEGIN issued
///      on it), the rust analogue of v1 `PoolTransaction`;
///   2. pin it into a tx-scoped [`ExecutionContext`] so EVERY statement `body` issues resolves THAT
///      connection via `connection_for` — never a fresh pooled one;
///   3. run `body(&tx_ctx)` → COMMIT / ROLLBACK on the OWNED connection per the returned decision; on
///      any `Err` ROLLBACK (best-effort) and re-raise;
///   4. the owned connection is released (dropped / back to the pool) when the [`TxConnection`] is
///      consumed by commit/rollback.
///
/// Concurrent calls each acquire a DISTINCT connection, so their writes never cross-talk — the
/// isolation the shared-slot model (a driver-global `Mutex<Option<writer>>`) violated. This mirrors
/// the TS `withTransactionAsync` reference (#75).
pub fn with_transaction_decided<'a, R>(
    ctx: &ExecutionContext<'a, '_>,
    body: impl FnOnce(&ExecutionContext) -> Result<TxDecision<R>, SqlFailure>,
) -> Result<R, SqlFailure> {
    with_transaction_decided_isolated(ctx, &[], &[], body)
}

/// The isolation-aware form of [`with_transaction_decided`] (Phase B / #82): `before_begin` /
/// `after_begin` are the isolation-prelude statements the driver runs around its `BEGIN`
/// ([`crate::tx_options::isolation_prelude`] — MySQL SET pre-BEGIN, PG SET post-BEGIN). Both empty ⇒
/// identical to [`with_transaction_decided`] (a bare `BEGIN`). This is the ONE mechanism the public
/// [`transaction`] boundary and the write-tx plan executor both drive; the retry loop + nested-join
/// live in [`transaction`], while THIS runs exactly ONE attempt on a freshly-acquired owned
/// connection.
pub fn with_transaction_decided_isolated<'a, R>(
    ctx: &ExecutionContext<'a, '_>,
    before_begin: &[String],
    after_begin: &[String],
    body: impl FnOnce(&ExecutionContext) -> Result<TxDecision<R>, SqlFailure>,
) -> Result<R, SqlFailure> {
    // Acquire the tx's OWNED connection (isolation prelude + BEGIN issued on it inside
    // begin_tx_isolated) and hold it in the slot. The handle borrows the primary driver (`'a`); the
    // slot holds it for the tx body's duration.
    let tx: Box<dyn TxConnection + 'a> =
        ctx.driver().begin_tx_isolated(before_begin, after_begin)?;
    let slot: TxSlot<'a> = std::cell::RefCell::new(Some(tx));
    // Scope the tx-ctx borrow of `slot` so it ends before we take the handle back for commit/rollback.
    let result = {
        let tx_ctx = ctx.with_tx_connection(&slot);
        body(&tx_ctx)
    };

    // Take the owned handle back out and consume it exactly once, per the body's decision. `take()`
    // leaves the slot empty (drop is a no-op).
    let tx = slot.borrow_mut().take();
    match (result, tx) {
        (Ok(TxDecision::Commit(r)), Some(tx)) => {
            tx.commit()?;
            Ok(r)
        }
        (Ok(TxDecision::Rollback(r)), Some(tx)) => {
            // A legitimate non-error rollback (e.g. a gate short-circuit): roll back, return the
            // value. A rollback failure here IS surfaced (the connection would be poisoned).
            tx.rollback()?;
            Ok(r)
        }
        (Ok(TxDecision::Commit(r)) | Ok(TxDecision::Rollback(r)), None) => Ok(r), // no handle (shouldn't happen)
        (Err(e), Some(tx)) => {
            let _ = tx.rollback(); // best-effort; the original failure is surfaced
            Err(e)
        }
        (Err(e), None) => Err(e),
    }
}

/// The simple form of [`with_transaction_decided`]: `Ok(r)` ⇒ COMMIT + return `r`; `Err` ⇒ ROLLBACK +
/// re-raise. For a body that never legitimately rolls back with a value.
pub fn with_transaction<'a, R>(
    ctx: &ExecutionContext<'a, '_>,
    body: impl FnOnce(&ExecutionContext) -> Result<R, SqlFailure>,
) -> Result<R, SqlFailure> {
    with_transaction_decided(ctx, |tx_ctx| body(tx_ctx).map(TxDecision::Commit))
}

// ── The PUBLIC user-controlled transaction boundary (Phase B-core / #86, rust port) ──

/// **The public user-controlled transaction boundary** (#86, rust port of the TS `transaction`) —
/// the REAL transaction feature v2 was missing. `transaction(ctx, dialect, options, body)` opens ONE
/// boundary the caller wraps around MULTIPLE arbitrary operations so they commit or roll back
/// TOGETHER:
///
/// ```text
///   transaction(&ctx, Dialect::Postgres, &opts, |tx_ctx| {
///       execute_transaction_bundle_ctx(a_bundle, a_input, tx_ctx)?;  // ← every op inside JOINS
///       execute_transaction_bundle_ctx(b_bundle, b_input, tx_ctx)?;  //    this ONE boundary:
///       Ok(())                                                        //    one conn, one BEGIN…COMMIT
///   })
/// ```
///
/// ## What it does (v1 `litedbmodel.rs` `transaction_with_options` parity, on the SCP seam)
///
/// It acquires ONE owned connection, issues the isolation-aware `BEGIN`
/// ([`crate::tx_options::isolation_prelude`] — PG SET post-BEGIN, MySQL SET pre-BEGIN), pins that
/// connection into a tx-scoped [`ExecutionContext`], runs `body(tx_ctx)`, then `COMMIT` (or
/// `ROLLBACK` on a body error / `options.rollback_only`), with the #81 retry loop (deadlock /
/// serialization / connection error) wrapped around the WHOLE boundary — a FRESH owned connection per
/// attempt. It drives [`with_transaction_decided_isolated`] per attempt (the ONE mechanism).
///
/// ## The ambient-tx JOIN — how operations participate (the core #86 fix; rust = explicit ctx)
///
/// `body` receives the tx-scoped ctx EXPLICITLY (the approved rust-idiomatic decision — no
/// task-local; the TS pins it in an async-local, rust threads it by argument). Every operation `body`
/// issues receives THAT ctx: a write via [`crate::runtime::execute_transaction_bundle_ctx`], a read
/// via the read seam. Because that ctx's `in_transaction()` is already true (a connection is pinned),
/// the write's own tx-bracketing DETECTS the ambient tx and JOINS it — running its statements on the
/// pinned owned connection WITHOUT opening its own `BEGIN/COMMIT` (see
/// [`transaction_decided`]'s nested-join). So N operations inside one `transaction()` produce exactly
/// ONE BEGIN + ONE COMMIT on ONE connection. Outside a `transaction()` the ctx's `in_transaction()`
/// is false, so a bare write's guard fires ([`crate::tx_options::write_outside_transaction`]).
///
/// NESTED `transaction()` joins the outer (one physical BEGIN/COMMIT; an inner error rolls back the
/// WHOLE tx). Isolation/retry/rollback_only options on a nested call are IGNORED (the outer owns
/// them) — mirror v1 `TransactionExecutor::transaction` (`handler.rs:801`, `depth+1`, no new BEGIN).
pub fn transaction<'a, R>(
    ctx: &ExecutionContext<'a, '_>,
    dialect: crate::tx_options::Dialect,
    options: &crate::tx_options::TransactionOptions,
    body: impl Fn(&ExecutionContext) -> Result<R, SqlFailure>,
) -> Result<R, SqlFailure> {
    transaction_decided(ctx, dialect, options, |tx_ctx| {
        body(tx_ctx).map(TxDecision::Commit)
    })
}

/// The [`TxDecision`]-returning form of [`transaction`] (Phase B / #82): `body` decides COMMIT vs
/// ROLLBACK (a gate short-circuit returns [`TxDecision::Rollback`] with `committed:false` — a
/// legitimate non-error outcome), an `Err` always rolls back + retries/re-raises. This is what the
/// write-tx plan executor ([`crate::runtime::execute_transaction_bundle_ctx`]) drives, so a
/// gate-first plan run inside a user `transaction()` short-circuits correctly while still JOINING.
///
/// Handles the three Phase B concerns the plain [`with_transaction_decided`] does not:
///   1. **nested-join** — if `ctx.in_transaction()` is already true (an outer `transaction()` pinned a
///      connection), run `body` on the OUTER ctx with NO new BEGIN/COMMIT/acquire (the inner body is
///      part of the outer physical tx; an inner error propagates and rolls back the WHOLE tx);
///   2. **rollback_only** — a successful `Commit(r)` is rewritten to `Rollback(r)` (the body result is
///      returned but NO change commits — dry-run/preview);
///   3. **retry** — a retryable failure (deadlock / serialization / connection error, via
///      [`crate::tx_options::is_retryable_tx_error`]) re-runs the WHOLE boundary on a FRESH owned
///      connection, up to `retry_limit`, with exponential backoff `retry_duration_ms · 2^(k-1)`. A
///      non-retryable error re-raises immediately.
pub fn transaction_decided<'a, R>(
    ctx: &ExecutionContext<'a, '_>,
    dialect: crate::tx_options::Dialect,
    options: &crate::tx_options::TransactionOptions,
    body: impl Fn(&ExecutionContext) -> Result<TxDecision<R>, SqlFailure>,
) -> Result<R, SqlFailure> {
    // NESTED-TX JOIN (mirror v1 `TransactionExecutor::transaction` depth+1): already inside a tx on
    // this ctx ⇒ join the outer. No new connection, no BEGIN/COMMIT — the inner body is part of the
    // outer physical transaction. Isolation/retry/rollback_only on a nested call are ignored: the
    // outer owns the envelope. A gate `Rollback` here still returns its value (the caller reports
    // `committed:false`) WITHOUT rolling back the outer — the outer decides its own COMMIT/ROLLBACK.
    if ctx.in_transaction() {
        return match body(ctx)? {
            TxDecision::Commit(r) | TxDecision::Rollback(r) => Ok(r),
        };
    }

    // The isolation prelude (PG SET post-BEGIN / MySQL SET pre-BEGIN / SQLite = hard-error on a level).
    let (before_begin, after_begin) =
        crate::tx_options::isolation_prelude(dialect, options.isolation)?;
    let rollback_only = options.rollback_only;
    let retry_limit = if options.retry_on_error {
        options.retry_limit.max(1)
    } else {
        1
    };

    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        // ONE attempt on a FRESH owned connection (a retry after a connection error thus RECONNECTS).
        let outcome =
            with_transaction_decided_isolated(ctx, &before_begin, &after_begin, |tx_ctx| {
                let decided = body(tx_ctx)?;
                // rollback_only (dry-run): a SUCCESSFUL commit becomes a ROLLBACK, still returning the
                // body result — no committed change. An explicit gate Rollback stays a rollback.
                Ok(if rollback_only {
                    match decided {
                        TxDecision::Commit(r) | TxDecision::Rollback(r) => TxDecision::Rollback(r),
                    }
                } else {
                    decided
                })
            });

        match outcome {
            Ok(r) => return Ok(r),
            Err(e) => {
                if attempt < retry_limit && crate::tx_options::is_retryable_tx_error(&e) {
                    // Exponential backoff before RETRYing the whole transaction on a fresh connection.
                    let backoff = options
                        .retry_duration_ms
                        .saturating_mul(1u64 << (attempt - 1));
                    if backoff > 0 {
                        std::thread::sleep(std::time::Duration::from_millis(backoff));
                    }
                    continue;
                }
                return Err(e);
            }
        }
    }
}

// ── Unit tests (no DB) — the seam + middleware passthrough + tx ownership ───────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{forwarding_tx, PreparedStatement, RunInfo as DrvRunInfo};
    use std::cell::RefCell;
    use std::rc::Rc;

    /// A fake driver recording every SQL it prepares (through the seam) into a shared log — proves
    /// the seam funnels SQL to the driver, and lets a tx assert BEGIN/…/COMMIT ordering.
    struct RecordDriver {
        log: Rc<RefCell<Vec<String>>>,
        fail_on: Option<String>, // a SQL that, when prepared+run/all, errors (to drive rollback)
    }

    struct RecStmt {
        log: Rc<RefCell<Vec<String>>>,
        sql: String,
        fail_on: Option<String>,
    }

    impl PreparedStatement for RecStmt {
        fn all(&mut self, _p: &[Value]) -> Result<Vec<Value>, SqlFailure> {
            self.log.borrow_mut().push(self.sql.clone());
            if self.fail_on.as_deref() == Some(self.sql.as_str()) {
                return Err(fail("boom"));
            }
            Ok(vec![Value::Obj(vec![(
                "sql".into(),
                Value::Str(self.sql.clone()),
            )])])
        }
        fn run(&mut self, _p: &[Value]) -> Result<DrvRunInfo, SqlFailure> {
            self.log.borrow_mut().push(self.sql.clone());
            if self.fail_on.as_deref() == Some(self.sql.as_str()) {
                return Err(fail("boom"));
            }
            Ok(DrvRunInfo {
                changes: 1,
                last_insert_rowid: 0,
            })
        }
    }

    impl Driver for RecordDriver {
        fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
            Box::new(RecStmt {
                log: self.log.clone(),
                sql: sql.to_string(),
                fail_on: self.fail_on.clone(),
            })
        }
        fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
            forwarding_tx(self)
        }
    }

    fn fail(msg: &str) -> SqlFailure {
        SqlFailure {
            kind: "driver_error".into(),
            policy: "fail".into(),
            sqlite_code: None,
            message: msg.into(),
        }
    }

    fn driver(fail_on: Option<&str>) -> (RecordDriver, Rc<RefCell<Vec<String>>>) {
        let log = Rc::new(RefCell::new(Vec::new()));
        (
            RecordDriver {
                log: log.clone(),
                fail_on: fail_on.map(str::to_string),
            },
            log,
        )
    }

    #[test]
    fn seam_execute_funnels_to_driver() {
        let (d, log) = driver(None);
        let ctx = for_driver(&d);
        let rows = execute(&ctx, "SELECT 1", &[], &StatementIntent::read()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(*log.borrow(), vec!["SELECT 1".to_string()]);
    }

    #[test]
    fn seam_run_funnels_to_driver() {
        let (d, log) = driver(None);
        let ctx = for_driver(&d);
        let info = run(
            &ctx,
            "INSERT INTO t VALUES (1)",
            &[],
            &StatementIntent::write(),
        )
        .unwrap();
        assert_eq!(info.changes, 1);
        assert_eq!(*log.borrow(), vec!["INSERT INTO t VALUES (1)".to_string()]);
    }

    #[test]
    fn empty_middleware_is_passthrough() {
        // An empty chain returns the terminal verbatim — the byte-identical Phase A behavior.
        let chain = MiddlewareChain::new();
        assert!(chain.is_empty());
        let r = chain
            .wrap_read("SELECT 1", &[], &|_s, _p| Ok(vec![Value::Int(42)]))
            .unwrap();
        assert!(matches!(r.as_slice(), [Value::Int(42)]));
    }

    #[test]
    fn middleware_wraps_and_delegates() {
        // A middleware that appends a marker row around `next` — proves the fold + delegation.
        struct Mw;
        impl Middleware<Vec<Value>> for Mw {
            fn wrap(
                &self,
                sql: &str,
                params: &[Value],
                next: &SeamNext<Vec<Value>>,
            ) -> Result<Vec<Value>, SqlFailure> {
                let mut rows = next(sql, params)?;
                rows.push(Value::Str("mw".into()));
                Ok(rows)
            }
        }
        let mut chain = MiddlewareChain::new();
        chain.read.push(Box::new(Mw));
        assert!(!chain.is_empty());
        let r = chain
            .wrap_read("SELECT 1", &[], &|_s, _p| Ok(vec![Value::Int(1)]))
            .unwrap();
        assert!(
            matches!(r.as_slice(), [Value::Int(1), Value::Str(s)] if s == "mw"),
            "middleware should append its marker row after next()"
        );
    }

    #[test]
    fn with_transaction_commits_on_ok() {
        // BEGIN → body statement → COMMIT, all on the same forwarding tx connection.
        let (d, log) = driver(None);
        let ctx = for_driver(&d);
        let out = with_transaction(&ctx, |tx| {
            run(
                tx,
                "INSERT INTO t VALUES (1)",
                &[],
                &StatementIntent::write(),
            )?;
            Ok(7)
        })
        .unwrap();
        assert_eq!(out, 7);
        assert_eq!(
            *log.borrow(),
            vec![
                "BEGIN".to_string(),
                "INSERT INTO t VALUES (1)".to_string(),
                "COMMIT".to_string(),
            ]
        );
    }

    #[test]
    fn with_transaction_rolls_back_on_err() {
        // A body error rolls back: BEGIN → (failing stmt) → ROLLBACK, and the error propagates.
        let (d, log) = driver(Some("BAD"));
        let ctx = for_driver(&d);
        let res: Result<(), SqlFailure> = with_transaction(&ctx, |tx| {
            run(
                tx,
                "INSERT INTO t VALUES (1)",
                &[],
                &StatementIntent::write(),
            )?;
            run(tx, "BAD", &[], &StatementIntent::write())?; // errors
            Ok(())
        });
        assert!(res.is_err());
        assert_eq!(
            *log.borrow(),
            vec![
                "BEGIN".to_string(),
                "INSERT INTO t VALUES (1)".to_string(),
                "BAD".to_string(),
                "ROLLBACK".to_string(),
            ]
        );
    }

    #[test]
    fn with_transaction_decided_rollback_returns_value() {
        // A gate-style non-error rollback: the body returns TxDecision::Rollback(value) → ROLLBACK is
        // issued but the value is returned (no error) — the tx short-circuit semantics.
        let (d, log) = driver(None);
        let ctx = for_driver(&d);
        let out = with_transaction_decided(&ctx, |tx| {
            run(
                tx,
                "INSERT INTO t VALUES (1)",
                &[],
                &StatementIntent::write(),
            )?;
            Ok(TxDecision::Rollback("gated".to_string()))
        })
        .unwrap();
        assert_eq!(out, "gated");
        assert_eq!(
            *log.borrow(),
            vec![
                "BEGIN".to_string(),
                "INSERT INTO t VALUES (1)".to_string(),
                "ROLLBACK".to_string(),
            ]
        );
    }

    #[test]
    fn tx_scoped_ctx_reports_in_transaction() {
        let (d, _log) = driver(None);
        let ctx = for_driver(&d);
        assert!(!ctx.in_transaction());
        with_transaction(&ctx, |tx| {
            assert!(tx.in_transaction());
            Ok(())
        })
        .unwrap();
    }

    // ── Phase B (#82) — the write=tx guard + the public transaction() boundary ─────

    use crate::tx_options::{Dialect, IsolationLevel, TransactionOptions};

    fn opts() -> TransactionOptions {
        TransactionOptions::default()
    }

    #[test]
    fn guard_rejects_write_outside_transaction() {
        let (d, log) = driver(None);
        let ctx = for_driver(&d);
        // A bare write on a base ctx (no active tx) is REJECTED before any SQL.
        let e =
            run_guarded(&ctx, "INSERT INTO t VALUES (1)", &[], "INSERT", Some("t")).unwrap_err();
        assert_eq!(e.kind, "write_outside_transaction");
        assert!(
            log.borrow().is_empty(),
            "no SQL issued before the guard fires"
        );
    }

    #[test]
    fn guard_rejects_write_in_read_only_first() {
        let (d, log) = driver(None);
        let base = for_driver(&d);
        // A read-only-scoped tx ctx: read-only is checked FIRST, even though a tx is active.
        with_transaction(&base, |tx| {
            let ro = tx.with_read_only();
            let e = run_guarded(&ro, "UPDATE t SET x=1", &[], "UPDATE", Some("t")).unwrap_err();
            assert_eq!(e.kind, "write_in_read_only_context");
            Ok(())
        })
        .unwrap();
        // Only BEGIN + COMMIT ran (the guarded write never issued SQL).
        assert_eq!(
            *log.borrow(),
            vec!["BEGIN".to_string(), "COMMIT".to_string()]
        );
    }

    #[test]
    fn guard_allows_write_inside_transaction() {
        let (d, log) = driver(None);
        let base = for_driver(&d);
        with_transaction(&base, |tx| {
            // Inside a tx ⇒ the guard passes and the write runs on the owned connection.
            run_guarded(tx, "INSERT INTO t VALUES (1)", &[], "INSERT", Some("t"))?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            *log.borrow(),
            vec![
                "BEGIN".to_string(),
                "INSERT INTO t VALUES (1)".to_string(),
                "COMMIT".to_string(),
            ]
        );
    }

    #[test]
    fn transaction_boundary_one_begin_one_commit_for_n_ops() {
        // Two ops inside ONE transaction() boundary ⇒ exactly ONE BEGIN + ONE COMMIT (the ambient
        // JOIN — the second op does NOT open its own BEGIN/COMMIT).
        let (d, log) = driver(None);
        let ctx = for_driver(&d);
        transaction(&ctx, Dialect::Postgres, &opts(), |tx| {
            run(
                tx,
                "INSERT INTO a VALUES (1)",
                &[],
                &StatementIntent::write(),
            )?;
            // A "nested" transaction_decided (as a joined write would do) must NOT open a new BEGIN.
            transaction(tx, Dialect::Postgres, &opts(), |inner| {
                run(
                    inner,
                    "INSERT INTO b VALUES (2)",
                    &[],
                    &StatementIntent::write(),
                )?;
                Ok(())
            })?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            *log.borrow(),
            vec![
                "BEGIN".to_string(),
                "INSERT INTO a VALUES (1)".to_string(),
                "INSERT INTO b VALUES (2)".to_string(),
                "COMMIT".to_string(),
            ],
            "one BEGIN + one COMMIT; the nested op JOINED (no inner BEGIN/COMMIT)"
        );
    }

    #[test]
    fn transaction_body_error_rolls_back_the_whole_tx() {
        // A failing op inside the boundary ⇒ ONE BEGIN + ONE ROLLBACK, zero COMMIT; the earlier op's
        // statement is part of the same physical tx (rolled back with it).
        let (d, log) = driver(Some("BAD"));
        let ctx = for_driver(&d);
        let res: Result<(), SqlFailure> = transaction(&ctx, Dialect::Postgres, &opts(), |tx| {
            run(
                tx,
                "INSERT INTO a VALUES (1)",
                &[],
                &StatementIntent::write(),
            )?;
            run(tx, "BAD", &[], &StatementIntent::write())?; // errors
            Ok(())
        });
        assert!(res.is_err());
        assert_eq!(
            *log.borrow(),
            vec![
                "BEGIN".to_string(),
                "INSERT INTO a VALUES (1)".to_string(),
                "BAD".to_string(),
                "ROLLBACK".to_string(),
            ]
        );
    }

    #[test]
    fn rollback_only_returns_value_but_does_not_commit() {
        // rollback_only: a successful body ROLLBACKs (dry-run) but still returns its value.
        let (d, log) = driver(None);
        let ctx = for_driver(&d);
        let o = TransactionOptions {
            rollback_only: true,
            ..opts()
        };
        let out = transaction(&ctx, Dialect::Postgres, &o, |tx| {
            run(
                tx,
                "INSERT INTO a VALUES (1)",
                &[],
                &StatementIntent::write(),
            )?;
            Ok(42)
        })
        .unwrap();
        assert_eq!(out, 42);
        assert_eq!(
            *log.borrow(),
            vec![
                "BEGIN".to_string(),
                "INSERT INTO a VALUES (1)".to_string(),
                "ROLLBACK".to_string(),
            ],
            "rollback_only ⇒ ROLLBACK, never COMMIT"
        );
    }

    #[test]
    fn isolation_prelude_pg_sets_after_begin() {
        // PG isolation ⇒ SET TRANSACTION ISOLATION LEVEL runs AS THE FIRST in-tx statement (post-BEGIN,
        // via the default begin_tx_isolated forwarding).
        let (d, log) = driver(None);
        let ctx = for_driver(&d);
        let o = TransactionOptions {
            isolation: Some(IsolationLevel::Serializable),
            ..opts()
        };
        transaction(&ctx, Dialect::Postgres, &o, |tx| {
            run(
                tx,
                "INSERT INTO a VALUES (1)",
                &[],
                &StatementIntent::write(),
            )?;
            Ok(())
        })
        .unwrap();
        assert_eq!(
            *log.borrow(),
            vec![
                "BEGIN".to_string(),
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE".to_string(),
                "INSERT INTO a VALUES (1)".to_string(),
                "COMMIT".to_string(),
            ]
        );
    }

    /// A driver that fails the body statement on the FIRST `attempts` attempts (with a retryable
    /// message), then succeeds — proving the whole-tx retry loop re-runs on a fresh connection.
    struct FlakyDriver {
        log: Rc<RefCell<Vec<String>>>,
        remaining_fails: RefCell<u32>,
    }
    struct FlakyStmt {
        log: Rc<RefCell<Vec<String>>>,
        sql: String,
        should_fail: bool,
    }
    impl PreparedStatement for FlakyStmt {
        fn all(&mut self, _p: &[Value]) -> Result<Vec<Value>, SqlFailure> {
            self.log.borrow_mut().push(self.sql.clone());
            Ok(Vec::new())
        }
        fn run(&mut self, _p: &[Value]) -> Result<DrvRunInfo, SqlFailure> {
            self.log.borrow_mut().push(self.sql.clone());
            if self.should_fail && self.sql == "WORK" {
                return Err(SqlFailure {
                    kind: "driver_error".into(),
                    policy: "fail".into(),
                    sqlite_code: None,
                    message: "could not serialize access due to concurrent update (SQLSTATE 40001)"
                        .into(),
                });
            }
            Ok(DrvRunInfo {
                changes: 1,
                last_insert_rowid: 0,
            })
        }
    }
    impl Driver for FlakyDriver {
        fn prepare(&self, sql: &str) -> Box<dyn PreparedStatement + '_> {
            // Decide fail-or-not per WORK statement, decrementing the remaining-fails budget.
            let should_fail = if sql == "WORK" {
                let mut r = self.remaining_fails.borrow_mut();
                if *r > 0 {
                    *r -= 1;
                    true
                } else {
                    false
                }
            } else {
                false
            };
            Box::new(FlakyStmt {
                log: self.log.clone(),
                sql: sql.to_string(),
                should_fail,
            })
        }
        fn begin_tx(&self) -> Result<Box<dyn TxConnection + '_>, SqlFailure> {
            forwarding_tx(self)
        }
    }

    #[test]
    fn retry_reruns_whole_tx_on_retryable_error() {
        let log = Rc::new(RefCell::new(Vec::new()));
        let d = FlakyDriver {
            log: log.clone(),
            remaining_fails: RefCell::new(1), // fail once, succeed on attempt 2
        };
        let ctx = for_driver(&d);
        let o = TransactionOptions {
            retry_on_error: true,
            retry_limit: 3,
            retry_duration_ms: 0, // no sleep in the test
            ..opts()
        };
        let out = transaction(&ctx, Dialect::Postgres, &o, |tx| {
            run(tx, "WORK", &[], &StatementIntent::write())?;
            Ok(7)
        })
        .unwrap();
        assert_eq!(out, 7);
        // Attempt 1: BEGIN, WORK(fails), ROLLBACK. Attempt 2: BEGIN, WORK(ok), COMMIT.
        assert_eq!(
            *log.borrow(),
            vec![
                "BEGIN".to_string(),
                "WORK".to_string(),
                "ROLLBACK".to_string(),
                "BEGIN".to_string(),
                "WORK".to_string(),
                "COMMIT".to_string(),
            ]
        );
    }

    #[test]
    fn non_retryable_error_does_not_retry() {
        let (d, log) = driver(Some("WORK"));
        let ctx = for_driver(&d);
        let o = TransactionOptions {
            retry_on_error: true,
            retry_limit: 3,
            retry_duration_ms: 0,
            ..opts()
        };
        // The default RecordDriver fails "WORK" with message "boom" (NOT retryable) ⇒ exactly one
        // attempt, then the error surfaces.
        let res: Result<(), SqlFailure> = transaction(&ctx, Dialect::Postgres, &o, |tx| {
            run(tx, "WORK", &[], &StatementIntent::write())?;
            Ok(())
        });
        assert!(res.is_err());
        assert_eq!(
            *log.borrow(),
            vec![
                "BEGIN".to_string(),
                "WORK".to_string(),
                "ROLLBACK".to_string(),
            ],
            "a non-retryable error is NOT retried (single attempt)"
        );
    }
}
