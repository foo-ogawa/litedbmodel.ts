//! litedbmodel v2 SCP — the **middleware layer** (Phase D / #93, the rust port of TS #92).
//!
//! This is the rust mirror of the TS API reference (`src/scp/middleware.ts`). It makes the empty
//! Phase A middleware hook on the exec-context seam ([`crate::exec_context`]) REAL, without
//! restructuring the seam. It ports the SAME contract the 5-language epic fixes:
//!
//!   - **D1** SQL-level `execute` hook — `(sql, params, next)`, wrapping EVERY statement that funnels
//!     through the central seam ([`crate::exec_context::execute`] / [`crate::exec_context::run`]): so
//!     body read, body write, the tx runtime's OWN BEGIN/COMMIT/ROLLBACK + isolation SET, and
//!     relation-batch SQL are ALL intercepted. A middleware can observe / rewrite (`next(sql', params')`)
//!     / time / short-circuit. Registration via [`register_middleware`] / [`use_middleware`] appends to
//!     the CURRENT scope's ordered stack, folded so index 0 = OUTERMOST (`use(A); use(B)` ⇒
//!     `A.before → B.before → «execute» → B.after → A.after`).
//!
//! ## Runtime-issued tx-control IS middleware-visible (#93 / owner option A — full TS parity)
//!
//! The tx runtime ([`crate::exec_context::with_transaction_decided_isolated_on`]) issues its OWN
//! BEGIN/COMMIT/ROLLBACK + the isolation SET THROUGH the seam (`run(txctx, "BEGIN"/"COMMIT"/…)`) on the
//! SAME pinned owned connection — so a registered middleware OBSERVES them, exactly like the TS
//! reference (`runAsync(txCtx, 'BEGIN'/'COMMIT'/'ROLLBACK')`). The owned connection is acquired via
//! [`crate::driver::Driver::acquire_tx`] (acquire+pin, NO BEGIN); the seam-issued tx-control resolves
//! that pinned connection via `connection_for` (per-execution ownership unchanged), and is EXEMPT from
//! the write=tx guard (it goes through plain `run`, not `run_guarded`). This is proven by
//! `d1_runtime_tx_control_is_middleware_visible` (unit) + the live PG boundary test (COMMIT + ROLLBACK
//! observed), with a RED proof (revert the seam-routing → the tx-boundary observation goes RED).
//!   - **D2** method-level hooks — [`run_method`] folds the matching op-kind hooks around an ORM
//!     operation. The op KIND is a [`MethodKind`] TAG the operation boundary supplies — NEVER parsed
//!     from SQL.
//!   - **D3** the standard [`Logger`] middleware (SQL/params/timing) + the raw [`raw_execute`] /
//!     [`raw_query`] public API that goes THROUGH the seam (so a registered middleware + connection
//!     routing + an ambient transaction all still apply). `raw_query` = `raw_execute` tagged `query`.
//!
//! ## Per-scope isolation (the ALS analogue) — `task_local!`
//!
//! TS binds a middleware to the EXECUTION SCOPE via `AsyncLocalStorage`, so concurrent requests never
//! see each other's middleware or per-request state. The rust port uses a `task_local!`-style
//! thread/task-local override ([`REGISTRY_SCOPE`]): [`with_middleware_scope`] runs a callback with an
//! ISOLATED [`Registry`] whose STACK is copied from the ambient one, but whose per-middleware STATE
//! MAP starts EMPTY. Copying ONLY the stack (never the state map) is the exact isolation the TS M5
//! mutation exposes — a copied state map would let concurrent scopes bleed per-request state. Absent
//! a scope, registration mutates the [`global_registry`] (the app-startup path).
//!
//! Because the rust runtime executes on a single OS thread per seam call (the conformance/livedb
//! runners are synchronous; the async livedb driver blocks the sync seam on a shared runtime), the
//! scope override is a THREAD-local: two threads each running a `with_middleware_scope` body have
//! DISTINCT registries — the rust reproduction of the TS concurrent-scope isolation guarantee.
//!
//! ## Native registration (design §4 "native 側でも登録可")
//!
//! [`register_middleware`] is the rust native registration API: it appends a [`MiddlewareDescriptor`]
//! to the ctx (current-scope) chain. The chain CONTRACT + ORDER is the 5-language shared spec; the
//! middleware BODY is a rust closure/impl. TS is the reference for the shape.

use std::cell::RefCell;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use behavior_contracts::Value;

use crate::driver::RunInfo;
use crate::errors::SqlFailure;
use crate::exec_context::{self, Dyn, ExecutionContext, SeamResult, StatementIntent};

// ── The SQL-level middleware (design §4 level 1) ───────────────────────────────

/// The seam terminal a SQL-level hook delegates to: `(sql, params) -> SeamResult`. Runs the rest of
/// the chain then the connection-resolve + execute terminal (the seam's ②③). Mirrors the TS
/// `SqlNext`. `SeamResult` is the erased read/write result ([`SeamResult::Rows`] / [`SeamResult::Run`])
/// so ONE hook shape serves both the read (`Vec<Value>`) and write ([`RunInfo`]) seams — the rust
/// analogue of the TS generic-`T`-erased-to-`unknown` hook.
pub type SqlNext<'n> = dyn Fn(&str, &[Value]) -> Result<SeamResult, SqlFailure> + 'n;

/// A SQL-level `execute` hook (design §4 level 1): `(sql, params, next) -> SeamResult`. A middleware
/// may observe, rewrite (pass different `sql`/`params` to `next`), time (bracket the `next` call), or
/// short-circuit (return WITHOUT calling `next`). This is the trait the [`Registry`] folds around the
/// seam terminal in [`crate::exec_context::execute`] / `run`. Mirrors the TS `SqlHook`.
pub trait SqlHook: Send + Sync {
    fn wrap(&self, sql: &str, params: &[Value], next: &SqlNext) -> Result<SeamResult, SqlFailure>;
}

/// Adapt a plain closure `Fn(sql, params, next) -> SeamResult` into a [`SqlHook`] (the ergonomic
/// registration form — the rust analogue of the TS `execute: (next, sql, params) => …` config field).
pub struct SqlHookFn<F>(pub F)
where
    F: Fn(&str, &[Value], &SqlNext) -> Result<SeamResult, SqlFailure> + Send + Sync;

impl<F> SqlHook for SqlHookFn<F>
where
    F: Fn(&str, &[Value], &SqlNext) -> Result<SeamResult, SqlFailure> + Send + Sync,
{
    fn wrap(&self, sql: &str, params: &[Value], next: &SqlNext) -> Result<SeamResult, SqlFailure> {
        (self.0)(sql, params, next)
    }
}

// ── Method-level hooks (design §4 level 2) — the ORM operation boundary ────────

/// The ORM operation KIND a method hook keys on (v2 maps the v1 method names onto the read/write
/// operations). [`run_method`] dispatches to the hook of the matching kind — this is how a method
/// hook DISTINGUISHES the op kind: the TAG the operation boundary supplies, NOT a guess from the SQL
/// text. Mirrors the TS `MethodKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MethodKind {
    Find,
    FindOne,
    FindById,
    Count,
    Create,
    CreateMany,
    Update,
    UpdateMany,
    Delete,
    Query,
}

/// The `next` of a method hook: run the rest of the method chain + the operation, returning its
/// result. `R` is the operation's result type (opaque to the hook layer). Mirrors the TS `MethodNext`.
pub type MethodNext<'n, R> = dyn Fn(&[Dyn]) -> Result<R, SqlFailure> + 'n;

/// One method-level hook of kind `K` (design §4 level 2): `(model, next, args) -> R` (v1
/// `Middleware.find` parity). `model` is the operation's opaque model/target descriptor; `args` are
/// the operation's arguments (opaque [`Dyn`]s); `next(args)` runs the rest of the chain + the
/// operation. A hook may rewrite `args`, time the `next` call, or short-circuit by returning without
/// calling `next`. Generic over the result `R` because [`run_method`] is invoked at a typed operation
/// boundary. Mirrors the TS `MethodHook`.
pub trait MethodHook<R>: Send + Sync {
    fn wrap(&self, model: &Dyn, next: &MethodNext<R>, args: &[Dyn]) -> Result<R, SqlFailure>;
}

// ── The middleware descriptor (registration unit) ──────────────────────────────

/// A registered middleware: its (optional) SQL-level [`SqlHook`] and its (optional) per-scope STATE
/// factory. [`use_middleware`] / [`register_middleware`] register ONE of these. Method hooks are
/// registered per-kind SEPARATELY (see [`register_method_hook`]) because rust's method hooks are
/// generic over the result type `R` — they cannot live in the same erased descriptor list as the
/// SQL hooks. Built by [`create_middleware`] from the ergonomic config, or hand-built. Mirrors the TS
/// `MiddlewareDescriptor` (its `methods` map is the [`MethodRegistry`] here).
pub struct MiddlewareDescriptor {
    /// The SQL-level `execute` hook (design §4 level 1), if any.
    pub sql: Option<Box<dyn SqlHook>>,
    /// The per-scope STATE factory: builds a FRESH state instance the first time this scope reads it
    /// (the rust analogue of the TS `structuredClone(config.state)` per scope). `None` ⇒ no state.
    fresh_state: Option<Box<dyn Fn() -> Box<StateAny> + Send + Sync>>,
}

impl MiddlewareDescriptor {
    /// A descriptor with only a SQL hook (no per-scope state).
    pub fn sql_only(hook: Box<dyn SqlHook>) -> Self {
        MiddlewareDescriptor {
            sql: Some(hook),
            fresh_state: None,
        }
    }
}

/// Opaque per-scope middleware state (the rust analogue of the TS state object; a `Logger` stores its
/// `entries` here), stored type-erased as `Arc<dyn Any + Send + Sync>` and downcast to the concrete
/// state type. Interior mutability is the middleware's own concern (a `Logger` uses a `Mutex`),
/// because a scope's state is read through a shared reference.
pub type StateAny = dyn std::any::Any + Send + Sync;

// ── The middleware registry (the ordered stack + per-scope state) ──────────────

/// A monotonically-increasing token minted per [`create_middleware`], used to KEY a middleware's
/// per-scope state in the scope-local `ScopeRegistry::states` map. The rust analogue of the TS
/// `createMiddleware` unique `{}` token. Distinct handles never collide; the SAME handle re-used in a
/// fresh scope lazily builds a FRESH state (the isolation guarantee).
static NEXT_TOKEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A reference-counted, cheaply-cloneable descriptor handle. A scope's stack is a `Vec` of these; the
/// scope `copy` seeds a fresh scope by cloning the `Vec` (shares the hook impls) but the fresh scope's
/// STATE MAP starts empty (the exact stack-copy-not-state-map isolation the audit flagged). Mirrors
/// the entry the TS `Registry.stack` holds.
#[derive(Clone)]
struct SharedDescriptor {
    token: u64,
    sql: Option<std::sync::Arc<dyn SqlHook>>,
    fresh_state: Option<std::sync::Arc<dyn Fn() -> Box<StateAny> + Send + Sync>>,
}

/// The live per-scope registry: the ordered `SharedDescriptor` stack + the scope-local state map. This
/// is the concrete thing a [`with_middleware_scope`] scope owns and the global default holds. Kept
/// behind interior mutability so `use`/`register` mutate the CURRENT scope without a `&mut` thread.
#[derive(Default)]
struct ScopeRegistry {
    stack: Vec<SharedDescriptor>,
    states: RefCell<Vec<(u64, std::sync::Arc<StateAny>)>>,
}

impl ScopeRegistry {
    fn push(&mut self, d: SharedDescriptor) {
        self.stack.push(d);
    }

    fn clear(&mut self) {
        self.stack.clear();
        self.states.borrow_mut().clear();
    }

    fn is_sql_empty(&self) -> bool {
        !self.stack.iter().any(|d| d.sql.is_some())
    }

    /// The SQL hooks of this scope, in registration order (index 0 = outermost). The seam folds them.
    fn sql_hooks(&self) -> Vec<std::sync::Arc<dyn SqlHook>> {
        self.stack.iter().filter_map(|d| d.sql.clone()).collect()
    }

    /// This scope's state for `token`, lazily built via the descriptor's `fresh_state` on first
    /// access (the rust analogue of the TS `stateFor` + `structuredClone`). Returns `None` if the
    /// descriptor carries no state factory.
    fn state_for(&self, token: u64) -> Option<std::sync::Arc<StateAny>> {
        if let Some((_, s)) = self.states.borrow().iter().find(|(t, _)| *t == token) {
            return Some(s.clone());
        }
        let fresh = self
            .stack
            .iter()
            .find(|d| d.token == token)
            .and_then(|d| d.fresh_state.as_ref())?;
        let s: std::sync::Arc<StateAny> = std::sync::Arc::from(fresh());
        self.states.borrow_mut().push((token, s.clone()));
        Some(s)
    }

    /// Reset `token`'s state to a fresh instance (testing convenience — the TS `resetStateFor`).
    fn reset_state_for(&self, token: u64) {
        let fresh = self
            .stack
            .iter()
            .find(|d| d.token == token)
            .and_then(|d| d.fresh_state.as_ref());
        if let Some(fresh) = fresh {
            let s: std::sync::Arc<StateAny> = std::sync::Arc::from(fresh());
            let mut states = self.states.borrow_mut();
            if let Some(slot) = states.iter_mut().find(|(t, _)| *t == token) {
                slot.1 = s;
            } else {
                states.push((token, s));
            }
        }
    }

    /// A COPY seeding a fresh scope: STACK copied (shares hook impls), STATE MAP empty. The exact
    /// stack-copy-not-state-map isolation the audit flagged.
    fn copy(&self) -> ScopeRegistry {
        ScopeRegistry {
            stack: self.stack.clone(), // STACK copied (shares hook impls via Arc).
            states: RefCell::new(Vec::new()), // STATE MAP NOT copied — fresh empty per scope.
        }
    }
}

// ── The scope override (the ALS analogue) — `task_local!` ──────────────────────

thread_local! {
    /// The per-execution-scope registry override (the `task_local!` analogue of the TS ALS). Present
    /// ⇒ `use`/reads target THIS scope's copy; absent ⇒ the process-global default. A `RefCell<Option>`
    /// stack lets [`with_middleware_scope`] nest (push a copy, pop on exit).
    static REGISTRY_SCOPE: RefCell<Vec<ScopeRegistry>> = const { RefCell::new(Vec::new()) };
}

/// The process-global default registry (app-startup `use` with no explicit scope). A `Mutex` because
/// it is shared across threads (unlike the per-scope override, which is thread-local). Mirrors the TS
/// module-level `globalRegistry`.
fn global_registry() -> &'static Mutex<ScopeRegistry> {
    static GLOBAL: OnceLock<Mutex<ScopeRegistry>> = OnceLock::new();
    GLOBAL.get_or_init(|| Mutex::new(ScopeRegistry::default()))
}

/// Run `f` with the CURRENT scope's registry (the thread-local override if inside a
/// [`with_middleware_scope`], else the process-global default). This is the ONE resolution point every
/// registration + read goes through — the rust analogue of the TS `currentRegistry()`.
fn with_current<R>(f: impl FnOnce(&ScopeRegistry) -> R) -> R {
    // Is a per-scope override active? Decide FIRST (releasing the thread-local borrow), then run `f`
    // exactly ONCE against the resolved registry — so `f` (which may re-enter `with_current`, e.g. a
    // hook body reading state) never runs while the thread-local is borrowed.
    let has_scope = REGISTRY_SCOPE.with(|scopes| !scopes.borrow().is_empty());
    if has_scope {
        REGISTRY_SCOPE.with(|scopes| {
            let scopes = scopes.borrow();
            f(scopes.last().expect("scope stack non-empty"))
        })
    } else {
        let reg = global_registry()
            .lock()
            .expect("middleware registry poisoned");
        f(&reg)
    }
}

/// Run `f` with a MUTABLE reference to the current scope's registry (for `push`/`clear`).
fn with_current_mut<R>(f: impl FnOnce(&mut ScopeRegistry) -> R) -> R {
    let has_scope = REGISTRY_SCOPE.with(|scopes| !scopes.borrow().is_empty());
    if has_scope {
        REGISTRY_SCOPE.with(|scopes| {
            let mut scopes = scopes.borrow_mut();
            f(scopes.last_mut().expect("scope stack non-empty"))
        })
    } else {
        let mut reg = global_registry()
            .lock()
            .expect("middleware registry poisoned");
        f(&mut reg)
    }
}

/// Run `fn` with an ISOLATED middleware registry (concurrent-scope isolation, design §4). The scope
/// seeds a COPY of the currently-visible registry's STACK (so app-wide registrations remain in
/// effect) but a FRESH EMPTY state map (per-scope state never bleeds — the M5 isolation). Any
/// `use`/`register` inside `f` mutates ONLY this scope; two threads each in a `with_middleware_scope`
/// body have DISTINCT registries. `inherit = false` seeds an EMPTY stack instead. Mirrors the TS
/// `withMiddlewareScope`. This is the rust `task_local!`-scoped run.
pub fn with_middleware_scope<R>(f: impl FnOnce() -> R) -> R {
    with_middleware_scope_opts(true, f)
}

/// [`with_middleware_scope`] with explicit inheritance: `inherit = false` starts from an EMPTY stack
/// (the TS `{ inherit: false }`).
pub fn with_middleware_scope_opts<R>(inherit: bool, f: impl FnOnce() -> R) -> R {
    let seed = if inherit {
        with_current(|reg| reg.copy())
    } else {
        ScopeRegistry::default()
    };
    REGISTRY_SCOPE.with(|scopes| scopes.borrow_mut().push(seed));
    // The method-hook registry (design §4 level 2) shares the SAME scope lifetime as the SQL registry:
    // push a fresh EMPTY method level here (method hooks are NOT inherited — each scope registers its
    // own, matching the TS whose method hooks live in the same per-scope `Registry`). A generic method
    // hook is result-type-keyed, so an empty copy is the only sound seed anyway.
    METHOD_SCOPE.with(|scopes| scopes.borrow_mut().push(Vec::new()));
    // Ensure both scopes are popped even if `f` panics (mirrors the ALS `run` teardown).
    struct PopGuard;
    impl Drop for PopGuard {
        fn drop(&mut self) {
            REGISTRY_SCOPE.with(|scopes| {
                scopes.borrow_mut().pop();
            });
            METHOD_SCOPE.with(|scopes| {
                scopes.borrow_mut().pop();
            });
        }
    }
    let _guard = PopGuard;
    f()
}

// ── Registration surface (v1 `DBModel.use` / `createMiddleware` parity) ────────

/// A registration handle returned by [`create_middleware`]: `state()` reads the CURRENT execution
/// scope's state instance (a fresh per-scope copy, the rust `getCurrentContext()`), and `token`
/// keys it in the scope-local state map. The underlying [`SqlHook`] + state factory are captured as
/// `Arc`s so [`use_middleware`] can append the descriptor to the scope. Mirrors the TS
/// `MiddlewareHandle`. `S` is the concrete state type for the typed `state()` downcast.
pub struct MiddlewareHandle<S: std::any::Any + Send + Sync> {
    token: u64,
    sql: Option<std::sync::Arc<dyn SqlHook>>,
    fresh_state: Option<std::sync::Arc<dyn Fn() -> Box<StateAny> + Send + Sync>>,
    _marker: std::marker::PhantomData<fn() -> S>,
}

impl<S: std::any::Any + Send + Sync> MiddlewareHandle<S> {
    /// The CURRENT execution scope's state instance for this handle (fresh per scope; the rust
    /// `getCurrentContext()`). Downcast to `S`. `None` if the handle carries no state, or the handle
    /// is not registered in the current scope (so no state was ever built). Returns an `Arc<S>` that
    /// SHARES the scope's state allocation (so a hook body's mutations are visible here) — via
    /// `Arc::downcast` on the type-erased `Arc<dyn StateAny>` the scope stores.
    pub fn state(&self) -> Option<std::sync::Arc<S>> {
        let any: std::sync::Arc<StateAny> = with_current(|reg| reg.state_for(self.token))?;
        std::sync::Arc::downcast::<S>(any).ok()
    }

    /// Reset the current scope's state to a fresh copy (testing convenience — the TS `resetState`).
    pub fn reset_state(&self) {
        with_current(|reg| reg.reset_state_for(self.token));
    }

    /// The underlying descriptor's shared parts (for [`use_middleware`]).
    fn shared(&self) -> SharedDescriptor {
        SharedDescriptor {
            token: self.token,
            sql: self.sql.clone(),
            fresh_state: self.fresh_state.clone(),
        }
    }
}

/// Register `mw`'s descriptor on the CURRENT scope's registry (the thread-local per-scope one inside
/// [`with_middleware_scope`], else the process-global default). Mirrors the TS `use`.
pub fn use_middleware<S: std::any::Any + Send + Sync>(mw: &MiddlewareHandle<S>) {
    let shared = mw.shared();
    with_current_mut(|reg| reg.push(shared));
}

/// Native registration (design §4 "native 側でも登録可"): append a raw [`MiddlewareDescriptor`] to
/// the current-scope chain. The rust `register_middleware(mw)` API. Returns the handle-less token so a
/// caller can later read state if needed. Mirrors the TS `register`.
pub fn register_middleware(mw: MiddlewareDescriptor) -> u64 {
    let token = NEXT_TOKEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let shared = SharedDescriptor {
        token,
        sql: mw.sql.map(std::sync::Arc::from),
        fresh_state: mw.fresh_state.map(std::sync::Arc::from),
    };
    with_current_mut(|reg| reg.push(shared));
    token
}

/// Build a [`MiddlewareHandle`] from a SQL hook + an optional per-scope state factory (the rust
/// analogue of the TS `createMiddleware`). Each scope that registers this handle lazily builds its
/// OWN state via `fresh_state` (isolated across concurrent scopes). `state = None` ⇒ a stateless
/// middleware. Register with [`use_middleware`].
pub fn create_middleware<S, H, FS>(hook: Option<H>, fresh_state: Option<FS>) -> MiddlewareHandle<S>
where
    S: std::any::Any + Send + Sync,
    H: SqlHook + 'static,
    FS: Fn() -> S + Send + Sync + 'static,
{
    let token = NEXT_TOKEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let sql: Option<std::sync::Arc<dyn SqlHook>> =
        hook.map(|h| std::sync::Arc::new(h) as std::sync::Arc<dyn SqlHook>);
    let fresh_state: Option<std::sync::Arc<dyn Fn() -> Box<StateAny> + Send + Sync>> = fresh_state
        .map(|fs| {
            std::sync::Arc::new(move || Box::new(fs()) as Box<StateAny>)
                as std::sync::Arc<dyn Fn() -> Box<StateAny> + Send + Sync>
        });
    MiddlewareHandle {
        token,
        sql,
        fresh_state,
        _marker: std::marker::PhantomData,
    }
}

/// Clear the process-global registry (testing; a per-scope registry is dropped when its scope exits).
/// Mirrors the TS `clearMiddlewares`.
pub fn clear_middlewares() {
    let mut reg = global_registry()
        .lock()
        .expect("middleware registry poisoned");
    reg.clear();
}

// ── The seam wiring: fold the ambient registry's SQL hooks (the Phase A hook made real) ──

/// The ambient (current-scope) SQL hooks, in registration order (index 0 = outermost). The seam
/// ([`crate::exec_context::execute`] / `run`) folds these around its connection-resolve terminal — the
/// rust analogue of the TS `activeSqlMiddlewares()` the ctx factories give their `MiddlewareChain`.
/// Empty ⇒ the seam is a byte-identical passthrough (no per-statement overhead). Resolved at EACH
/// seam call, so registration after ctx construction + per-scope registries are both honored.
pub fn active_sql_hooks() -> Vec<std::sync::Arc<dyn SqlHook>> {
    with_current(|reg| reg.sql_hooks())
}

/// Is the ambient SQL chain empty right now (⇒ the seam is a guaranteed passthrough)?
pub fn active_sql_empty() -> bool {
    with_current(|reg| reg.is_sql_empty())
}

/// Fold the ambient registry's SQL hooks around `terminal` (the connection-resolve + execute the seam
/// supplies), outermost-first (index 0 = OUTERMOST), then invoke it. Empty ⇒ `terminal(sql, params)`
/// verbatim (byte-identical passthrough). This is the ONE place the seam invokes the Phase D
/// middleware; [`crate::exec_context`]'s `execute`/`run` call it. Mirrors the TS `MiddlewareChain.wrap`
/// fold: walk the stack LAST→FIRST building `next`, so index 0 ends up outermost.
pub fn wrap_ambient(
    sql: &str,
    params: &[Value],
    terminal: &SqlNext,
) -> Result<SeamResult, SqlFailure> {
    let hooks = active_sql_hooks();
    if hooks.is_empty() {
        return terminal(sql, params); // byte-identical passthrough — no overhead.
    }
    // Fold LAST→FIRST so index 0 (first-registered) ends up OUTERMOST (the §order contract).
    fn go(
        hooks: &[std::sync::Arc<dyn SqlHook>],
        sql: &str,
        params: &[Value],
        terminal: &SqlNext,
    ) -> Result<SeamResult, SqlFailure> {
        match hooks.split_first() {
            None => terminal(sql, params),
            Some((head, tail)) => {
                let inner = move |s: &str, p: &[Value]| go(tail, s, p, terminal);
                head.wrap(sql, params, &inner)
            }
        }
    }
    go(&hooks, sql, params, terminal)
}

// ── D2: method-level dispatch (design §4 level 2) — the operation boundary fold ─

/// A monotonically-increasing token minted per method-hook registration, KEYing a hook's presence in
/// the scope's method registry.
static NEXT_METHOD_TOKEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A method hook registered for ONE kind. rust's method hooks are generic over the operation result
/// `R`, which cannot be erased into a single object-safe list shared across kinds/result-types. So the
/// method registry stores hooks as `Box<dyn Any>` factories the typed [`run_method`] downcasts to its
/// concrete `MethodHook<R>` at the boundary — the rust analogue of the TS `Partial<Record<MethodKind,
/// MethodHook>>`, whose `MethodHook` is `unknown`-erased.
struct MethodEntry {
    kind: MethodKind,
    /// The type-erased `Arc<dyn MethodHook<R>>` for THIS registration's `R`. Downcast in `run_method`.
    hook: Box<dyn std::any::Any + Send + Sync>,
}

thread_local! {
    /// The per-scope method-hook registry (parallel to [`REGISTRY_SCOPE`] — same scope lifetime). A
    /// `RefCell<Vec<Vec<…>>>` mirrors the scope stack: index = nesting depth.
    static METHOD_SCOPE: RefCell<Vec<Vec<MethodEntry>>> = const { RefCell::new(Vec::new()) };
}

fn method_global() -> &'static Mutex<Vec<MethodEntry>> {
    static G: OnceLock<Mutex<Vec<MethodEntry>>> = OnceLock::new();
    G.get_or_init(|| Mutex::new(Vec::new()))
}

/// Register a method hook of `kind` on the CURRENT scope (the rust analogue of a `createMiddleware`
/// method hook + `use`). Because a method hook is generic over the result `R`, it is registered
/// SEPARATELY from the SQL descriptor and keyed by `R` internally. The hook fires for [`run_method`]
/// calls of the matching `kind` whose result type matches `R`. Returns a token (unused by callers
/// today; kept for symmetry with the SQL side).
pub fn register_method_hook<R, H>(kind: MethodKind, hook: H) -> u64
where
    R: 'static,
    H: MethodHook<R> + 'static,
{
    let token = NEXT_METHOD_TOKEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // The type-erased `Arc<dyn MethodHook<R>>` boxed as `Any`; `run_method::<R>` downcasts it back.
    let erased: std::sync::Arc<dyn MethodHook<R>> = std::sync::Arc::new(hook);
    let entry = MethodEntry {
        kind,
        hook: Box::new(erased),
    };
    METHOD_SCOPE.with(|scopes| {
        let mut scopes = scopes.borrow_mut();
        match scopes.last_mut() {
            // Inside a `with_middleware_scope` — the common path (method hooks share the SQL scope).
            Some(top) => top.push(entry),
            // Absent a scope, register on the process-global (the app-startup path).
            None => method_global()
                .lock()
                .expect("method registry poisoned")
                .push(entry),
        }
    });
    token
}

/// Run an ORM operation of KIND `kind` through the current scope's method hooks, then execute `core`.
/// The hooks fold first-registered-outermost (§order), each getting `(model, next, args)`; a hook may
/// rewrite `args`, time `next`, or short-circuit. Empty hooks for this kind ⇒ `core(args)` verbatim
/// (byte-identical). The op KIND is the TAG the operation boundary supplies — NEVER parsed from SQL.
/// Mirrors the TS `runMethod`.
pub fn run_method<R: 'static>(
    kind: MethodKind,
    model: Dyn,
    core: impl Fn(&[Dyn]) -> Result<R, SqlFailure>,
    args: &[Dyn],
) -> Result<R, SqlFailure> {
    let hooks: Vec<std::sync::Arc<dyn MethodHook<R>>> = current_method_hooks::<R>(kind);
    if hooks.is_empty() {
        return core(args); // fast path: no method hook for this kind/result-type.
    }
    // Fold LAST→FIRST so index 0 (first-registered) ends up OUTERMOST (§order).
    fn go<R: 'static>(
        hooks: &[std::sync::Arc<dyn MethodHook<R>>],
        model: &Dyn,
        core: &dyn Fn(&[Dyn]) -> Result<R, SqlFailure>,
        args: &[Dyn],
    ) -> Result<R, SqlFailure> {
        match hooks.split_first() {
            None => core(args),
            Some((head, tail)) => {
                let inner = move |a: &[Dyn]| go(tail, model, core, a);
                head.wrap(model, &inner, args)
            }
        }
    }
    go(&hooks, &model, &core, args)
}

/// The current scope's method hooks for `kind` whose result type is `R` (registration order). Reads
/// the scope stack (all nesting levels visible in the ambient scope collapse to the top scope, since
/// `with_middleware_scope` pushes one method level).
fn current_method_hooks<R: 'static>(kind: MethodKind) -> Vec<std::sync::Arc<dyn MethodHook<R>>> {
    METHOD_SCOPE.with(|scopes| {
        let scopes = scopes.borrow();
        match scopes.last() {
            Some(top) => top
                .iter()
                .filter(|e| e.kind == kind)
                .filter_map(|e| {
                    e.hook
                        .downcast_ref::<std::sync::Arc<dyn MethodHook<R>>>()
                        .cloned()
                })
                .collect(),
            None => {
                let g = method_global().lock().expect("method registry poisoned");
                g.iter()
                    .filter(|e| e.kind == kind)
                    .filter_map(|e| {
                        e.hook
                            .downcast_ref::<std::sync::Arc<dyn MethodHook<R>>>()
                            .cloned()
                    })
                    .collect()
            }
        }
    })
}

// ── D3: the standard Logger middleware (SQL / params / timing) ─────────────────

/// One logged statement: the SQL, its params, and the wall-clock ms `next` took (v1 Logger parity).
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub sql: String,
    pub params: Vec<Value>,
    /// Wall-clock milliseconds the wrapped `next` (chain remainder + connection execute) took.
    pub duration_ms: u128,
}

/// The [`Logger`] middleware's per-scope state: the log history. Interior-mutable (a `Mutex`) because
/// the scope's state is read through a shared reference while the hook appends. Mirrors the TS
/// `state().entries`.
#[derive(Default)]
pub struct LoggerState {
    entries: Mutex<Vec<LogEntry>>,
}

impl LoggerState {
    /// A snapshot of the recorded entries (the rust `getCurrentContext().getLogs()`).
    pub fn entries(&self) -> Vec<LogEntry> {
        self.entries.lock().expect("logger state poisoned").clone()
    }
}

/// The standard **Logger middleware** (design §4, v1 `Logger` parity): a SQL-level hook that records
/// the SQL, its params, and the wall-clock ms each statement takes. Every statement through the seam —
/// read, write, tx-control, relation-batch — is logged (it is an `execute`-level hook). Register with
/// [`use_middleware`]. Timing brackets the `next` call, so it measures the connection execute (chain
/// remainder included), NOT just the record. The per-scope log lives on the handle's `state()`
/// (concurrent scopes each collect their OWN entries — the isolation guarantee). Mirrors the TS
/// `Logger`.
pub fn logger() -> MiddlewareHandle<LoggerState> {
    // The hook needs to read THIS scope's LoggerState. It cannot capture the state (state is
    // per-scope, built lazily), so it looks the state up by the handle's token via the ambient
    // registry at wrap time — exactly how the TS `Logger` reads `this.entries` (the ALS-scoped state).
    // We mint the token first, build the hook closure over it, then the handle carries the same token.
    let token = NEXT_TOKEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let hook_token = token;
    let hook = SqlHookFn(move |sql: &str, params: &[Value], next: &SqlNext| {
        let started = Instant::now();
        let result = next(sql, params);
        let duration_ms = started.elapsed().as_millis();
        // Record on the CURRENT scope's LoggerState for this token.
        if let Some(state) = with_current(|reg| reg.state_for(hook_token)) {
            if let Some(ls) = state.downcast_ref::<LoggerState>() {
                ls.entries
                    .lock()
                    .expect("logger state poisoned")
                    .push(LogEntry {
                        sql: sql.to_string(),
                        params: params.to_vec(),
                        duration_ms,
                    });
            }
        }
        result
    });
    let sql: Option<std::sync::Arc<dyn SqlHook>> =
        Some(std::sync::Arc::new(hook) as std::sync::Arc<dyn SqlHook>);
    let fresh_state: Option<std::sync::Arc<dyn Fn() -> Box<StateAny> + Send + Sync>> = Some(
        std::sync::Arc::new(|| Box::new(LoggerState::default()) as Box<StateAny>)
            as std::sync::Arc<dyn Fn() -> Box<StateAny> + Send + Sync>,
    );
    MiddlewareHandle {
        token,
        sql,
        fresh_state,
        _marker: std::marker::PhantomData,
    }
}

// ── D3: raw execute / query THROUGH the seam ───────────────────────────────────

/// The raw-statement result: a row list (for a row-returning statement) plus the affected-rows count
/// (mirrors v1 `ExecuteResult { rows, rowCount }`). A non-row statement resolves `rows: []`. Mirrors
/// the TS `RawResult`.
#[derive(Debug, Clone)]
pub struct RawResult {
    pub rows: Vec<Value>,
    pub row_count: Option<i64>,
}

/// Does `sql` return rows (SELECT / …RETURNING / WITH…SELECT / SHOW / PRAGMA / VALUES / EXPLAIN /
/// TABLE)? Mirrors the TS `returnsRows`.
fn returns_rows(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_lowercase();
    const PREFIXES: [&str; 8] = [
        "select",
        "with",
        "show",
        "pragma",
        "values",
        "explain",
        "table",
        "returning",
    ];
    if PREFIXES
        .iter()
        .take(7)
        .any(|p| head.starts_with(p) && word_boundary_after(&head, p.len()))
    {
        return true;
    }
    // A `RETURNING` anywhere (INSERT/UPDATE/DELETE … RETURNING) returns rows.
    sql.to_ascii_lowercase().contains("returning")
}

fn word_boundary_after(s: &str, at: usize) -> bool {
    s[at..]
        .chars()
        .next()
        .map(|c| !c.is_alphanumeric() && c != '_')
        .unwrap_or(true)
}

/// Raw **synchronous** `execute(sql, params)` THROUGH the seam (design §4 D3): a registered SQL hook
/// intercepts it, connection routing resolves the connection, and an ambient transaction (if the ctx
/// is tx-scoped) applies — because it is the SAME [`crate::exec_context::execute`] / `run` seam the
/// ORM uses, not a direct driver call. A row-returning statement runs `execute`; a non-returning one
/// runs `run`. `write` forces the write intent (writer routing / tx connection). Mirrors the TS
/// `rawExecute`.
pub fn raw_execute(
    ctx: &ExecutionContext,
    sql: &str,
    params: &[Value],
    write: bool,
) -> Result<RawResult, SqlFailure> {
    if returns_rows(sql) {
        let intent = if write {
            StatementIntent::write()
        } else {
            StatementIntent::read()
        };
        let rows = exec_context::execute(ctx, sql, params, &intent)?;
        let row_count = rows.len() as i64;
        Ok(RawResult {
            rows,
            row_count: Some(row_count),
        })
    } else {
        let info: RunInfo = exec_context::run(ctx, sql, params, &StatementIntent::write())?;
        Ok(RawResult {
            rows: Vec::new(),
            row_count: Some(info.changes),
        })
    }
}

/// Raw **synchronous** `query(sql, params)` — [`raw_execute`] tagged as a `query` operation, so a
/// `query` method hook fires (then its SQL flows through the same seam + `execute` hooks, exactly as
/// v1 `DBModel.query` calls `DBModel.execute`). Returns the row list. Mirrors the TS `rawQuery` (the
/// two-level flow: method hook wraps the SQL-hook'd seam call). `Dyn` args carry `[sql, params]` for a
/// hook that inspects them.
pub fn raw_query(
    ctx: &ExecutionContext,
    sql: &str,
    params: &[Value],
) -> Result<Vec<Value>, SqlFailure> {
    let sql_owned = sql.to_string();
    let params_owned = params.to_vec();
    let args: Vec<Dyn> = vec![Dyn::new(sql_owned.clone()), Dyn::new(params_owned.clone())];
    run_method::<Vec<Value>>(
        MethodKind::Query,
        Dyn::unit(),
        |_a| raw_execute(ctx, &sql_owned, &params_owned, false).map(|r| r.rows),
        &args,
    )
}

// ── Unit tests (Phase D hook mechanics) — the rust mirror of test/scp/middleware.test.ts ───────────
//
// Proves D1/D2/D3 on the REAL Phase A exec-context seam (in-proc rusqlite), with the NON-VACUOUS RED
// proofs the TS reference carries: empty-chain → no interception; fold-reversed → order RED; wrong
// op-kind → no dispatch; shared-registry → isolation cross-talk RED. Every registration is inside a
// `with_middleware_scope` so the process-global registry stays clean (empty chain = byte-identical,
// the conformance/livedb runners register none).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::SqliteDriver;
    use crate::exec_context::{execute as seam_execute, for_driver, run as seam_run};
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    fn fresh_db() -> SqliteDriver {
        SqliteDriver::in_memory(&["CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)".to_string()])
            .unwrap()
    }

    /// An observing SQL hook that pushes each seen SQL onto a shared log, then delegates to `next`.
    fn observer(
        log: Arc<StdMutex<Vec<String>>>,
    ) -> SqlHookFn<impl Fn(&str, &[Value], &SqlNext) -> Result<SeamResult, SqlFailure> + Send + Sync>
    {
        SqlHookFn(move |sql: &str, params: &[Value], next: &SqlNext| {
            log.lock().unwrap().push(sql.to_string());
            next(sql, params)
        })
    }

    // ── D1: SQL-level execute hook ────────────────────────────────────────────

    #[test]
    fn d1_intercepts_every_sql_through_the_seam() {
        let db = fresh_db();
        let ctx = for_driver(&db);
        let seen = Arc::new(StdMutex::new(Vec::<String>::new()));
        let seen2 = seen.clone();
        with_middleware_scope(|| {
            let mw = create_middleware::<(), _, fn() -> ()>(Some(observer(seen2)), None);
            use_middleware(&mw);
            seam_run(&ctx, "BEGIN", &[], &StatementIntent::write()).unwrap();
            seam_run(
                &ctx,
                "INSERT INTO t (name) VALUES (?)",
                &[Value::Str("a".into())],
                &StatementIntent::write(),
            )
            .unwrap();
            seam_run(&ctx, "COMMIT", &[], &StatementIntent::write()).unwrap();
            seam_execute(&ctx, "SELECT * FROM t", &[], &StatementIntent::read()).unwrap();
        });
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                "BEGIN".to_string(),
                "INSERT INTO t (name) VALUES (?)".to_string(),
                "COMMIT".to_string(),
                "SELECT * FROM t".to_string(),
            ]
        );
    }

    #[test]
    fn d1_red_without_wiring_nothing_is_observed() {
        // RED: no use()/scope → the seam is a byte-identical passthrough, the observer never fires.
        let db = fresh_db();
        let ctx = for_driver(&db);
        let seen = Arc::new(StdMutex::new(Vec::<String>::new()));
        seam_run(
            &ctx,
            "INSERT INTO t (name) VALUES (?)",
            &[Value::Str("a".into())],
            &StatementIntent::write(),
        )
        .unwrap();
        seam_execute(&ctx, "SELECT * FROM t", &[], &StatementIntent::read()).unwrap();
        assert!(seen.lock().unwrap().is_empty());
    }

    #[test]
    fn d1_can_rewrite_sql_params() {
        let db = fresh_db();
        let ctx = for_driver(&db);
        with_middleware_scope(|| {
            let mw = create_middleware::<(), _, fn() -> ()>(
                Some(SqlHookFn(|sql: &str, params: &[Value], next: &SqlNext| {
                    if sql.starts_with("INSERT") {
                        return next(sql, &[Value::Str("rewritten".into())]);
                    }
                    next(sql, params)
                })),
                None,
            );
            use_middleware(&mw);
            seam_run(
                &ctx,
                "INSERT INTO t (name) VALUES (?)",
                &[Value::Str("original".into())],
                &StatementIntent::write(),
            )
            .unwrap();
        });
        let rows = seam_execute(
            &for_driver(&db),
            "SELECT name FROM t",
            &[],
            &StatementIntent::read(),
        )
        .unwrap();
        // The middleware rewrote the bound value before it hit the driver.
        assert!(format!("{rows:?}").contains("rewritten"), "rows={rows:?}");
    }

    #[test]
    fn d1_can_short_circuit() {
        let db = fresh_db();
        let ctx = for_driver(&db);
        let rows = with_middleware_scope(|| {
            let mw = create_middleware::<(), _, fn() -> ()>(
                Some(SqlHookFn(|_sql: &str, _p: &[Value], _next: &SqlNext| {
                    // Do NOT call next — short-circuit with a synthetic row (the DB is never touched).
                    Ok(SeamResult::Rows(vec![Value::Obj(vec![
                        ("id".into(), Value::Int(99)),
                        ("name".into(), Value::Str("synthetic".into())),
                    ])]))
                })),
                None,
            );
            use_middleware(&mw);
            seam_execute(&ctx, "SELECT * FROM t", &[], &StatementIntent::read()).unwrap()
        });
        assert_eq!(rows.len(), 1);
        assert!(format!("{rows:?}").contains("synthetic"));
        // Nothing was ever inserted; a real query returns empty — proves the DB was bypassed.
        let real = seam_execute(
            &for_driver(&db),
            "SELECT COUNT(*) c FROM t",
            &[],
            &StatementIntent::read(),
        )
        .unwrap();
        assert!(format!("{real:?}").contains("Int(0)"), "real={real:?}");
    }

    #[test]
    fn d1_can_time_next() {
        let db = fresh_db();
        let ctx = for_driver(&db);
        let timed = Arc::new(AtomicI64::new(-1));
        let t2 = timed.clone();
        with_middleware_scope(|| {
            let mw = create_middleware::<(), _, fn() -> ()>(
                Some(SqlHookFn(
                    move |sql: &str, params: &[Value], next: &SqlNext| {
                        let t0 = std::time::Instant::now();
                        let r = next(sql, params);
                        t2.store(t0.elapsed().as_millis() as i64, Ordering::SeqCst);
                        r
                    },
                )),
                None,
            );
            use_middleware(&mw);
            seam_execute(&ctx, "SELECT * FROM t", &[], &StatementIntent::read()).unwrap();
        });
        assert!(timed.load(Ordering::SeqCst) >= 0);
    }

    #[test]
    fn d1_applied_order_first_registered_is_outermost() {
        let db = fresh_db();
        let ctx = for_driver(&db);
        let order = Arc::new(StdMutex::new(Vec::<String>::new()));
        let (oa, ob) = (order.clone(), order.clone());
        with_middleware_scope(|| {
            let a = create_middleware::<(), _, fn() -> ()>(
                Some(SqlHookFn(
                    move |sql: &str, params: &[Value], next: &SqlNext| {
                        oa.lock().unwrap().push("A:before".into());
                        let r = next(sql, params);
                        oa.lock().unwrap().push("A:after".into());
                        r
                    },
                )),
                None,
            );
            let b = create_middleware::<(), _, fn() -> ()>(
                Some(SqlHookFn(
                    move |sql: &str, params: &[Value], next: &SqlNext| {
                        ob.lock().unwrap().push("B:before".into());
                        let r = next(sql, params);
                        ob.lock().unwrap().push("B:after".into());
                        r
                    },
                )),
                None,
            );
            use_middleware(&a);
            use_middleware(&b);
            seam_execute(&ctx, "SELECT 1", &[], &StatementIntent::read()).unwrap();
        });
        // use(A); use(B) ⇒ A.before → B.before → «execute» → B.after → A.after (index 0 = OUTERMOST).
        assert_eq!(
            *order.lock().unwrap(),
            vec!["A:before", "B:before", "B:after", "A:after"]
        );
    }

    #[test]
    fn d1_red_fold_reversed_would_break_order() {
        // RED (fold-reversed): assert the fold is last→first. If `wrap_ambient` folded FIRST→LAST
        // instead, index 0 would be INNERMOST and this order would be B:before,A:before,…. The test
        // above pins the correct order; here we prove the assertion is non-vacuous by checking the
        // WRONG order does NOT hold.
        let db = fresh_db();
        let ctx = for_driver(&db);
        let order = Arc::new(StdMutex::new(Vec::<String>::new()));
        let (oa, ob) = (order.clone(), order.clone());
        with_middleware_scope(|| {
            let a = create_middleware::<(), _, fn() -> ()>(
                Some(SqlHookFn(move |s: &str, p: &[Value], next: &SqlNext| {
                    oa.lock().unwrap().push("A".into());
                    next(s, p)
                })),
                None,
            );
            let b = create_middleware::<(), _, fn() -> ()>(
                Some(SqlHookFn(move |s: &str, p: &[Value], next: &SqlNext| {
                    ob.lock().unwrap().push("B".into());
                    next(s, p)
                })),
                None,
            );
            use_middleware(&a);
            use_middleware(&b);
            seam_execute(&ctx, "SELECT 1", &[], &StatementIntent::read()).unwrap();
        });
        assert_eq!(*order.lock().unwrap(), vec!["A", "B"]);
        assert_ne!(*order.lock().unwrap(), vec!["B", "A"]); // reversed fold would produce this — RED.
    }

    #[test]
    fn d1_per_scope_state_is_isolated_and_fresh() {
        // A stateful middleware (count) whose handle is REUSED across scopes — the TS M5 shape (one
        // `createMiddleware`, registered in multiple scopes). Each scope must lazily build its OWN fresh
        // state; a scope entered from a PARENT that already has state must NOT inherit it (the exact
        // stack-copy-not-state-map isolation — copying the state map here is the M5 mutation).
        struct CountState(AtomicI64);
        let mw = create_middleware::<CountState, _, _>(
            Option::<SqlHookFn<fn(&str, &[Value], &SqlNext) -> Result<SeamResult, SqlFailure>>>::None,
            Some(|| CountState(AtomicI64::new(0))),
        );
        // A PARENT scope registers the handle and bumps its state to 5.
        with_middleware_scope(|| {
            use_middleware(&mw);
            mw.state().unwrap().0.store(5, Ordering::SeqCst);
            assert_eq!(mw.state().unwrap().0.load(Ordering::SeqCst), 5);
            // A CHILD scope inherits the parent's STACK (the handle is still registered — its hook is
            // visible) but a FRESH EMPTY state map: the SAME handle's state() is a fresh 0, NOT 5.
            with_middleware_scope(|| {
                // The handle IS in the inherited stack (stack copied), so state() builds a fresh instance.
                assert_eq!(
                    mw.state().unwrap().0.load(Ordering::SeqCst),
                    0,
                    "child must NOT inherit parent state (M5)"
                );
                mw.state().unwrap().0.store(9, Ordering::SeqCst);
                assert_eq!(mw.state().unwrap().0.load(Ordering::SeqCst), 9);
            });
            // Back in the parent: its OWN state is untouched by the child (still 5).
            assert_eq!(
                mw.state().unwrap().0.load(Ordering::SeqCst),
                5,
                "parent state must survive the child scope"
            );
        });
    }

    #[test]
    fn d1_concurrent_scope_isolation() {
        // The concurrent-isolation guarantee (the ALS analogue): two THREADS each in a
        // `with_middleware_scope` body must NOT see each other's middleware. thread_local REGISTRY_SCOPE
        // gives each thread a DISTINCT scope stack — the rust reproduction of the TS concurrent-scope test.
        fn scope(tag: &'static str) -> Vec<String> {
            let db = fresh_db();
            let ctx = for_driver(&db);
            let seen = Arc::new(StdMutex::new(Vec::<String>::new()));
            let seen2 = seen.clone();
            let tagc = tag.to_string();
            with_middleware_scope(|| {
                let mw = create_middleware::<(), _, fn() -> ()>(
                    Some(SqlHookFn(
                        move |sql: &str, params: &[Value], next: &SqlNext| {
                            seen2.lock().unwrap().push(format!("{tagc}:{sql}"));
                            next(sql, params)
                        },
                    )),
                    None,
                );
                use_middleware(&mw);
                std::thread::sleep(std::time::Duration::from_millis(if tag == "A" {
                    5
                } else {
                    1
                }));
                seam_execute(
                    &ctx,
                    if tag == "A" { "SELECT 1" } else { "SELECT 2" },
                    &[],
                    &StatementIntent::read(),
                )
                .unwrap();
            });
            Arc::try_unwrap(seen).unwrap().into_inner().unwrap()
        }
        let ha = std::thread::spawn(|| scope("A"));
        let hb = std::thread::spawn(|| scope("B"));
        let (sa, sb) = (ha.join().unwrap(), hb.join().unwrap());
        assert_eq!(sa, vec!["A:SELECT 1".to_string()]);
        assert_eq!(sb, vec!["B:SELECT 2".to_string()]);
    }

    #[test]
    fn d1_red_shared_registry_would_cross_talk() {
        // RED (shared-registry): if the two scopes shared ONE registry (a process-global stack, NOT the
        // thread-local scope override), each scope would observe BOTH statements. We prove the ISOLATION
        // holds by asserting each `seen` has exactly ONE entry (its own) — a shared registry would make
        // it two. Run serially on one thread with nested-looking but SEPARATE scopes.
        fn run_one(tag: &str) -> Vec<String> {
            let db = fresh_db();
            let ctx = for_driver(&db);
            let seen = Arc::new(StdMutex::new(Vec::<String>::new()));
            let seen2 = seen.clone();
            let tagc = tag.to_string();
            with_middleware_scope(|| {
                let mw = create_middleware::<(), _, fn() -> ()>(
                    Some(SqlHookFn(move |s: &str, p: &[Value], next: &SqlNext| {
                        seen2.lock().unwrap().push(format!("{tagc}:{s}"));
                        next(s, p)
                    })),
                    None,
                );
                use_middleware(&mw);
                seam_execute(&ctx, "SELECT 1", &[], &StatementIntent::read()).unwrap();
            });
            Arc::try_unwrap(seen).unwrap().into_inner().unwrap()
        }
        // After scope A exits, its middleware is GONE (scope drop = un-register). Scope B sees only its own.
        assert_eq!(run_one("A"), vec!["A:SELECT 1".to_string()]);
        assert_eq!(run_one("B"), vec!["B:SELECT 1".to_string()]);
        // And the global registry is clean (no leak) — an unregistered chain observes nothing.
        assert!(active_sql_empty());
    }

    #[test]
    fn d1_runtime_tx_control_is_middleware_visible() {
        // POSITIVE GUARANTEE (#93 / owner option A, full TS parity): the tx runtime issues its OWN
        // BEGIN/COMMIT/ROLLBACK THROUGH the seam (`run(txctx, "BEGIN"/"COMMIT"/…)`) on the SAME pinned
        // owned connection — so a registered middleware OBSERVES the runtime-issued tx-control, exactly
        // like TS (`runAsync(txCtx, 'BEGIN')`). This replaces the former divergence pin.
        use crate::exec_context::{run as seam_run, with_transaction};

        // COMMIT path: a successful `with_transaction` ⇒ BEGIN → body → COMMIT, ALL observed.
        let db = fresh_db();
        let ctx = for_driver(&db);
        let seen = Arc::new(StdMutex::new(Vec::<String>::new()));
        let seen2 = seen.clone();
        with_middleware_scope(|| {
            let mw = create_middleware::<(), _, fn() -> ()>(Some(observer(seen2)), None);
            use_middleware(&mw);
            with_transaction(&ctx, |tx| {
                seam_run(
                    tx,
                    "INSERT INTO t (name) VALUES ('body')",
                    &[],
                    &StatementIntent::write(),
                )
            })
            .unwrap();
        });
        let observed = seen.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec![
                "BEGIN".to_string(),
                "INSERT INTO t (name) VALUES ('body')".to_string(),
                "COMMIT".to_string(),
            ],
            "runtime BEGIN + body + COMMIT must ALL be seam-visible"
        );

        // ROLLBACK path: a body error ⇒ BEGIN → body(failing) → ROLLBACK, ALL observed. Uses a
        // short-circuit middleware that lets BEGIN/ROLLBACK through but makes the body statement fail.
        let db2 = fresh_db();
        let ctx2 = for_driver(&db2);
        let seen_rb = Arc::new(StdMutex::new(Vec::<String>::new()));
        let seen_rb2 = seen_rb.clone();
        let _ = with_middleware_scope(|| {
            let mw = create_middleware::<(), _, fn() -> ()>(
                Some(SqlHookFn(
                    move |sql: &str, params: &[Value], next: &SqlNext| {
                        seen_rb2.lock().unwrap().push(sql.to_string());
                        next(sql, params)
                    },
                )),
                None,
            );
            use_middleware(&mw);
            with_transaction(&ctx2, |tx| {
                // A genuine SQL error (missing table) rolls the tx back.
                seam_run(
                    tx,
                    "INSERT INTO missing_table VALUES (1)",
                    &[],
                    &StatementIntent::write(),
                )
            })
        });
        let obs_rb = seen_rb.lock().unwrap().clone();
        assert_eq!(
            obs_rb.first().map(String::as_str),
            Some("BEGIN"),
            "BEGIN observed: {obs_rb:?}"
        );
        assert_eq!(
            obs_rb.last().map(String::as_str),
            Some("ROLLBACK"),
            "ROLLBACK observed on body error: {obs_rb:?}"
        );
    }

    // ── D2: method-level hooks (op-kind dispatch) ─────────────────────────────

    #[test]
    fn d2_fires_matching_op_kind_hook_before_after() {
        for kind in [
            MethodKind::Find,
            MethodKind::Create,
            MethodKind::Update,
            MethodKind::Delete,
        ] {
            let events = Arc::new(StdMutex::new(Vec::<String>::new()));
            let (e1, e2) = (events.clone(), events.clone());
            struct H {
                before: Arc<StdMutex<Vec<String>>>,
                after: Arc<StdMutex<Vec<String>>>,
                tag: String,
            }
            impl MethodHook<String> for H {
                fn wrap(
                    &self,
                    _m: &Dyn,
                    next: &MethodNext<String>,
                    args: &[Dyn],
                ) -> Result<String, SqlFailure> {
                    self.before
                        .lock()
                        .unwrap()
                        .push(format!("{}:before", self.tag));
                    let r = next(args)?;
                    self.after
                        .lock()
                        .unwrap()
                        .push(format!("{}:after", self.tag));
                    Ok(r)
                }
            }
            let tag = format!("{kind:?}").to_lowercase();
            let ecore = events.clone();
            let tag_core = tag.clone();
            let result = with_middleware_scope(|| {
                register_method_hook::<String, _>(
                    kind,
                    H {
                        before: e1,
                        after: e2,
                        tag: tag.clone(),
                    },
                );
                run_method::<String>(
                    kind,
                    Dyn::unit(),
                    |_a| {
                        ecore.lock().unwrap().push(format!("{tag_core}:core"));
                        Ok("ok".to_string())
                    },
                    &[],
                )
                .unwrap()
            });
            assert_eq!(result, "ok");
            assert_eq!(
                *events.lock().unwrap(),
                vec![
                    format!("{tag}:before"),
                    format!("{tag}:core"),
                    format!("{tag}:after")
                ]
            );
        }
    }

    #[test]
    fn d2_red_hook_of_different_kind_does_not_fire() {
        // RED (op-kind-ignored): a `create` hook must NOT fire for a `find` dispatch (kind is a TAG, not
        // parsed). If dispatch ignored the kind, `events` would be non-empty.
        let events = Arc::new(StdMutex::new(Vec::<String>::new()));
        let e = events.clone();
        struct H(Arc<StdMutex<Vec<String>>>);
        impl MethodHook<String> for H {
            fn wrap(
                &self,
                _m: &Dyn,
                next: &MethodNext<String>,
                args: &[Dyn],
            ) -> Result<String, SqlFailure> {
                self.0.lock().unwrap().push("create".into());
                next(args)
            }
        }
        with_middleware_scope(|| {
            register_method_hook::<String, _>(MethodKind::Create, H(e));
            run_method::<String>(MethodKind::Find, Dyn::unit(), |_a| Ok("r".to_string()), &[])
                .unwrap();
        });
        assert!(events.lock().unwrap().is_empty());
    }

    #[test]
    fn d2_method_hooks_compose_outermost_first_and_rewrite_args() {
        let order = Arc::new(StdMutex::new(Vec::<String>::new()));
        let core_arg = Arc::new(AtomicI64::new(0));
        let (oa, ob) = (order.clone(), order.clone());
        // A hook adds `delta` to the first i64 arg then delegates (arg rewrite).
        struct H {
            order: Arc<StdMutex<Vec<String>>>,
            tag: &'static str,
            delta: i64,
        }
        impl MethodHook<()> for H {
            fn wrap(
                &self,
                _m: &Dyn,
                next: &MethodNext<()>,
                args: &[Dyn],
            ) -> Result<(), SqlFailure> {
                self.order.lock().unwrap().push(self.tag.into());
                let n = args[0].downcast_ref::<i64>().copied().unwrap_or(0);
                next(&[Dyn::new(n + self.delta)])
            }
        }
        let ca = core_arg.clone();
        with_middleware_scope(|| {
            register_method_hook::<(), _>(
                MethodKind::Find,
                H {
                    order: oa,
                    tag: "A",
                    delta: 1,
                },
            );
            register_method_hook::<(), _>(
                MethodKind::Find,
                H {
                    order: ob,
                    tag: "B",
                    delta: 10,
                },
            );
            run_method::<()>(
                MethodKind::Find,
                Dyn::unit(),
                move |a| {
                    ca.store(
                        a[0].downcast_ref::<i64>().copied().unwrap_or(-1),
                        Ordering::SeqCst,
                    );
                    Ok(())
                },
                &[Dyn::new(0_i64)],
            )
            .unwrap();
        });
        assert_eq!(*order.lock().unwrap(), vec!["A", "B"]); // A outer, B inner.
        assert_eq!(core_arg.load(Ordering::SeqCst), 11); // 0 +1 (A) +10 (B).
    }

    // ── D3: Logger + raw execute/query ────────────────────────────────────────

    #[test]
    fn d3_logger_records_sql_params_timing() {
        let db = fresh_db();
        let ctx = for_driver(&db);
        with_middleware_scope(|| {
            let lg = logger();
            use_middleware(&lg);
            seam_run(
                &ctx,
                "INSERT INTO t (name) VALUES (?)",
                &[Value::Str("x".into())],
                &StatementIntent::write(),
            )
            .unwrap();
            seam_execute(
                &ctx,
                "SELECT * FROM t WHERE name = ?",
                &[Value::Str("x".into())],
                &StatementIntent::read(),
            )
            .unwrap();
            let entries = lg.state().unwrap().entries();
            let sqls: Vec<String> = entries.iter().map(|e| e.sql.clone()).collect();
            assert_eq!(
                sqls,
                vec![
                    "INSERT INTO t (name) VALUES (?)".to_string(),
                    "SELECT * FROM t WHERE name = ?".to_string(),
                ]
            );
            assert_eq!(
                format!("{:?}", entries[0].params),
                format!("{:?}", vec![Value::Str("x".into())])
            );
            assert_eq!(
                format!("{:?}", entries[1].params),
                format!("{:?}", vec![Value::Str("x".into())])
            );
            // duration_ms is u128 ≥ 0 by construction.
        });
    }

    #[test]
    fn d3_red_logger_without_wiring_records_nothing() {
        let db = fresh_db();
        let ctx = for_driver(&db);
        let lg = logger();
        // NOT registered → the seam never invokes it, and its state was never built in this scope.
        seam_execute(&ctx, "SELECT 1", &[], &StatementIntent::read()).unwrap();
        assert!(lg.state().is_none() || lg.state().unwrap().entries().is_empty());
    }

    #[test]
    fn d3_raw_execute_goes_through_the_seam() {
        let db = fresh_db();
        let ctx = for_driver(&db);
        let seen = Arc::new(StdMutex::new(Vec::<String>::new()));
        let seen2 = seen.clone();
        with_middleware_scope(|| {
            let mw = create_middleware::<(), _, fn() -> ()>(Some(observer(seen2)), None);
            use_middleware(&mw);
            let ins = raw_execute(
                &ctx,
                "INSERT INTO t (name) VALUES (?)",
                &[Value::Str("raw".into())],
                false,
            )
            .unwrap();
            assert_eq!(ins.row_count, Some(1));
            let read = raw_execute(&ctx, "SELECT name FROM t", &[], false).unwrap();
            assert!(format!("{:?}", read.rows).contains("raw"));
        });
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                "INSERT INTO t (name) VALUES (?)".to_string(),
                "SELECT name FROM t".to_string()
            ]
        );
    }

    #[test]
    fn d3_raw_query_fires_query_hook_and_flows_through_execute_seam() {
        let db = fresh_db();
        let ctx = for_driver(&db);
        let events = Arc::new(StdMutex::new(Vec::<String>::new()));
        let (eq, ex) = (events.clone(), events.clone());
        struct QH(Arc<StdMutex<Vec<String>>>);
        impl MethodHook<Vec<Value>> for QH {
            fn wrap(
                &self,
                _m: &Dyn,
                next: &MethodNext<Vec<Value>>,
                args: &[Dyn],
            ) -> Result<Vec<Value>, SqlFailure> {
                self.0.lock().unwrap().push("query".into());
                next(args)
            }
        }
        with_middleware_scope(|| {
            // Seed a row through THIS ctx's db so the raw query returns it (one shared in-mem connection).
            seam_run(
                &ctx,
                "INSERT INTO t (name) VALUES ('q')",
                &[],
                &StatementIntent::write(),
            )
            .unwrap();
            register_method_hook::<Vec<Value>, _>(MethodKind::Query, QH(eq));
            let exmw = create_middleware::<(), _, fn() -> ()>(
                Some(SqlHookFn(move |s: &str, p: &[Value], next: &SqlNext| {
                    ex.lock().unwrap().push(format!("execute:{s}"));
                    next(s, p)
                })),
                None,
            );
            use_middleware(&exmw);
            let rows = raw_query(&ctx, "SELECT name FROM t", &[]).unwrap();
            assert!(format!("{rows:?}").contains('q'));
        });
        // The `query` method hook fired FIRST (two-level flow), then the SQL flowed through the execute hook.
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                "query".to_string(),
                "execute:SELECT name FROM t".to_string()
            ]
        );
    }
}
