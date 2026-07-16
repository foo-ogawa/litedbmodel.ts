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

    /// Is this a tx-scoped ctx (a pinned connection is present)?
    pub fn in_transaction(&self) -> bool {
        self.pinned.is_some()
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
    // Acquire the tx's OWNED connection (BEGIN issued on it inside begin_tx) and hold it in the slot.
    // The handle borrows the primary driver (`'a`); the slot holds it for the tx body's duration.
    let tx: Box<dyn TxConnection + 'a> = ctx.driver().begin_tx()?;
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
}
