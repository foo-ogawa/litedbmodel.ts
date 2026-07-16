// litedbmodel v2 SCP — the **middleware layer** (Phase D / #94, go).
//
// The Go port of the TS API-reference `src/scp/middleware.ts` (#92), mirroring the rust port (#93).
// It builds ON the Phase A [ExecutionContext] seam (the [MiddlewareChain] hook Phase A reserved) — it
// does NOT restructure the seam; it makes the reserved hook real. Two hook levels (design §4):
//
//  1. **SQL-level execute hook** — `(sql, args, next)`. Wraps EVERY statement that funnels through the
//     central seam ([Execute] / [Run]), so read, write, tx-control (BEGIN/COMMIT/ROLLBACK), and
//     relation-batch SQL are ALL intercepted. A middleware can observe / rewrite (call `next` with a
//     different sql/args) / time / short-circuit (return without calling `next`). This is the seam's
//     [MiddlewareChain] folded around the connection-resolve terminal.
//  2. **method-level hook** — at the ORM operation boundary, keyed by the operation KIND (a TAG the
//     operation boundary supplies — find/findOne/findById/count/create/createMany/update/updateMany/
//     delete/query — NEVER parsed from the SQL text). [RunMethod] folds the matching op-kind hooks
//     around the operation; a hook runs before/after (around) the whole operation and can rewrite its
//     args / short-circuit its result.
//
// # Registration + APPLIED ORDER (the 5-language contract — v1 DBModel.use parity)
//
// [RegisterMiddleware] appends to an ordered stack (of the CURRENT execution scope) and returns an
// un-register fn. The stack is folded so the FIRST-registered middleware is the OUTERMOST wrapper:
// given register(A); register(B), a statement runs A.before → B.before → «execute» → B.after →
// A.after. This holds identically for the SQL-level chain ([MiddlewareChain.wrapRead]/wrapWrite) and
// the method-level chain ([RunMethod]) — the fold walks the stack from LAST to FIRST building `next`,
// so index 0 ends up outermost. This ORDER is the normative contract.
//
// # Per-execution-scope isolation (NOT a serializing process global) — design §4
//
// A middleware binds to the EXECUTION SCOPE, so concurrent requests/goroutines never see each other's
// middleware or per-request state. The TS reference uses AsyncLocalStorage; the go port carries the
// scope on a context.Context value ([WithMiddlewareScope]). TWO coupled parts, both scope-local:
//
//   - the REGISTRY (the ordered stack) — [WithMiddlewareScope] seeds a COPY of the visible stack, so
//     app-wide registrations remain in effect but a `register` inside the scope mutates ONLY this
//     scope's copy;
//   - the per-middleware STATE — a scope starts with an EMPTY state map. On scope entry ONLY the stack
//     is copied, NOT the state map, so each scope lazily builds its OWN fresh state instances. This is
//     the concurrency-isolation guarantee: two goroutines in distinct scopes never see each other's
//     per-middleware state (copying the state map would bleed it — the TS M5 failure).
//
// Absent an explicit scope, [RegisterMiddleware] mutates a process-global default registry (the
// app-startup registration path). A registered middleware is resolved at EACH seam wrap (via the
// ctx's Go context, [registryChainFor]), so registration after ctx construction and per-scope
// registries are both honored, and an EMPTY registry ⇒ a byte-identical passthrough.

package litedbmodel_runtime

import (
	"context"
	"regexp"
	"sync"
	"time"

	bc "github.com/foo-ogawa/behavior-contracts/go"
)

// ── Hook types (design §4) ─────────────────────────────────────────────────────

// MethodKind is the ORM operation KIND a method hook keys on (a TAG the operation boundary supplies,
// NEVER a guess from the SQL text). [RunMethod] dispatches to the hook of the matching kind.
type MethodKind string

const (
	MethodFind       MethodKind = "find"
	MethodFindOne    MethodKind = "findOne"
	MethodFindByID   MethodKind = "findById"
	MethodCount      MethodKind = "count"
	MethodCreate     MethodKind = "create"
	MethodCreateMany MethodKind = "createMany"
	MethodUpdate     MethodKind = "update"
	MethodUpdateMany MethodKind = "updateMany"
	MethodDelete     MethodKind = "delete"
	MethodQuery      MethodKind = "query"
)

// MethodNext runs the rest of the method chain + the operation, resolving its result. A hook may
// rewrite `args` before calling it, or skip it entirely (short-circuit).
type MethodNext func(args ...any) (any, error)

// MethodHook is one method-level hook: `(model, next, args…)`. `model` is the operation's model/target
// descriptor (opaque — the runtime supplies it); `args` are the operation's arguments. `next(args…)`
// runs the rest of the chain + the operation.
type MethodHook func(model any, next MethodNext, args ...any) (any, error)

// ── The middleware descriptor (registration unit) ──────────────────────────────

// MiddlewareDescriptor is a registered middleware: its (optional) SQL-level read/write hooks, its
// per-kind method hooks, and a per-scope STATE factory. [RegisterMiddleware] registers ONE of these.
// Build one directly, or via the ergonomic [MiddlewareConfig] + [NewMiddleware].
//
// The read + write SQL hooks are the go homogeneous-per-result-type split of the TS single generic
// `execute` hook (the seam folds rows and run-summaries in separate monomorphic chains): a middleware
// that observes/logs both supplies both (the SAME body over the two result types) — see [NewMiddleware],
// which derives both from ONE ergonomic execute closure.
//
// The SQL hooks are STATE-AWARE (design §4 per-scope isolation): a hook needing per-scope state carries
// a `token` + `newState`, and the registry binds the resolved scope-local state into the hook body at
// fold time (via [Registry.sqlMiddlewares], which knows the wrap's Go context). A [ReadMiddleware] /
// [WriteMiddleware] built directly (no state) uses SQLRead/SQLWrite; a [NewMiddleware] handle populates
// the state-aware SQLReadState/SQLWriteState + token/newState.
type MiddlewareDescriptor struct {
	// SQLRead is a stateless SQL-level hook over the READ seam ([]bc.Value results). nil ⇒ not wrapped.
	SQLRead ReadMiddleware
	// SQLWrite is a stateless SQL-level hook over the WRITE seam (RunInfo results). nil ⇒ not wrapped.
	SQLWrite WriteMiddleware
	// SQLReadState / SQLWriteState are the state-aware SQL hooks (the resolved per-scope state is passed
	// in as the first arg). Populated by [NewMiddleware]; the registry binds `state` per wrap.
	SQLReadState  func(state any, sql string, args []any, next SeamNext[[]bc.Value]) ([]bc.Value, error)
	SQLWriteState func(state any, sql string, args []any, next SeamNext[RunInfo]) (RunInfo, error)
	// Methods are the method-level hooks keyed by op kind (design §4 level 2). nil/absent ⇒ not wrapped.
	Methods map[MethodKind]MethodHook
	// token + newState back the per-scope state a state-aware hook (or [MiddlewareHandle.State]) reads.
	// nil token ⇒ no state.
	token    *stateToken
	newState func() any
}

// ── The middleware registry (the ordered stack + per-scope state) ──────────────

// Registry is the ordered middleware stack + the per-SCOPE state map. [RegisterMiddleware] appends
// (first-registered = outermost). A Registry is EITHER the process-global default (app-startup
// registration) OR a per-execution-scope copy pushed by [WithMiddlewareScope]. The two share this
// type; only their lifetime differs. The stack + the state map are BOTH scope-local — but a scope
// copy seeds ONLY the stack, so its state map starts EMPTY (the concurrency-isolation guarantee).
//
// Registry is goroutine-safe (a mutex guards the stack + state map): the process-global default may be
// touched from any goroutine, and a scope registry — though normally used by one goroutine — is
// guarded uniformly so a rogue share does not race.
type Registry struct {
	mu    sync.Mutex
	stack []*MiddlewareDescriptor
	// states holds THIS scope's per-middleware state, keyed by the state-owning token a [NewMiddleware]
	// handle carries. Because a scope Registry is a COPY that seeds ONLY the stack (states starts nil),
	// each scope lazily builds its OWN state instances — isolated across concurrent scopes with no
	// bleed. Copying the states map here would reproduce the TS M5 cross-talk failure — it is NOT copied.
	states map[*stateToken]any
}

// stateToken is the per-[NewMiddleware]-handle identity the scope-local state map keys on (a fresh
// pointer per handle, like the TS `{}` token). Two handles never collide; the same handle resolves the
// same scope-local state instance.
type stateToken struct{}

// newRegistry returns an empty registry (the process-global default's initial state).
func newRegistry() *Registry { return &Registry{} }

// register appends `mw` (⇒ outermost) and returns an idempotent un-register fn.
func (r *Registry) register(mw *MiddlewareDescriptor) func() {
	r.mu.Lock()
	r.stack = append(r.stack, mw)
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		for i, m := range r.stack {
			if m == mw {
				r.stack = append(r.stack[:i:i], r.stack[i+1:]...)
				return
			}
		}
	}
}

// clear drops every registration (testing — the process-global reset).
func (r *Registry) clear() {
	r.mu.Lock()
	r.stack = nil
	r.states = nil
	r.mu.Unlock()
}

// snapshot returns a stable copy of the current stack (so a fold is not racing a concurrent register).
func (r *Registry) snapshot() []*MiddlewareDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.stack) == 0 {
		return nil
	}
	out := make([]*MiddlewareDescriptor, len(r.stack))
	copy(out, r.stack)
	return out
}

// sqlMiddlewares returns the registered SQL-level read + write hooks, registration order (index 0 =
// first = outermost), for the [MiddlewareChain] fold. A state-aware hook is bound to THIS scope's
// resolved state (fetched from the registry keyed by the descriptor's token) so a hook body reads its
// OWN per-scope state — the concurrency-isolation guarantee. Empty ⇒ the seam is a byte-identical
// passthrough.
func (r *Registry) sqlMiddlewares() (read []ReadMiddleware, write []WriteMiddleware) {
	for _, mw := range r.snapshot() {
		mw := mw
		if mw.SQLRead != nil {
			read = append(read, mw.SQLRead)
		} else if mw.SQLReadState != nil {
			read = append(read, func(sql string, args []any, next SeamNext[[]bc.Value]) ([]bc.Value, error) {
				return mw.SQLReadState(r.resolveState(mw), sql, args, next)
			})
		}
		if mw.SQLWrite != nil {
			write = append(write, mw.SQLWrite)
		} else if mw.SQLWriteState != nil {
			write = append(write, func(sql string, args []any, next SeamNext[RunInfo]) (RunInfo, error) {
				return mw.SQLWriteState(r.resolveState(mw), sql, args, next)
			})
		}
	}
	return read, write
}

// resolveState returns THIS scope's state for a descriptor (nil for a stateless one).
func (r *Registry) resolveState(mw *MiddlewareDescriptor) any {
	if mw.token == nil || mw.newState == nil {
		return nil
	}
	return r.stateFor(mw.token, mw.newState)
}

// methodHooks returns the method hooks for `kind` (registration order, index 0 = outermost), for the
// [RunMethod] fold.
func (r *Registry) methodHooks(kind MethodKind) []MethodHook {
	var out []MethodHook
	for _, mw := range r.snapshot() {
		if mw.Methods == nil {
			continue
		}
		if h, ok := mw.Methods[kind]; ok && h != nil {
			out = append(out, h)
		}
	}
	return out
}

// stateFor returns THIS scope's state for `token`, lazily created via `fresh` on first access. The
// state map is scope-local (a scope copy starts empty), so each scope builds its OWN instance.
func (r *Registry) stateFor(token *stateToken, fresh func() any) any {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.states == nil {
		r.states = map[*stateToken]any{}
	}
	if s, ok := r.states[token]; ok {
		return s
	}
	s := fresh()
	r.states[token] = s
	return s
}

// copyStackOnly returns a scope-child registry that seeds ONLY the stack — the state map starts EMPTY
// so concurrent scopes never bleed per-middleware state (the TS M5 isolation guarantee). The seeded
// stack is a COPY (a register inside the scope does not touch the parent's stack).
func (r *Registry) copyStackOnly() *Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
	child := &Registry{}
	if len(r.stack) > 0 {
		child.stack = make([]*MiddlewareDescriptor, len(r.stack))
		copy(child.stack, r.stack)
	}
	// child.states deliberately left nil — NOT copied.
	return child
}

// ── Scope: the per-execution registry on a context.Context (isolation, design §4) ──

// globalRegistry is the process-global default registry (app-startup [RegisterMiddleware] with no
// explicit scope). Concurrent scopes each carry their OWN registry on a context.Context.
var globalRegistry = newRegistry()

// registryKeyType is the private context key the scope registry rides under (unexported type ⇒ no
// collision with any other package's context values).
type registryKeyType struct{}

var registryKey = registryKeyType{}

// currentRegistry resolves the registry the given Go context sees: the per-scope override on the
// context (present ⇒ inside a [WithMiddlewareScope]), else the process-global default. A nil context
// resolves the global default. This is the Go analogue of the TS `currentRegistry()` reading the ALS
// override — resolved at EACH seam wrap so registration after ctx construction and per-scope
// registries both take effect.
func currentRegistry(goCtx context.Context) *Registry {
	if goCtx != nil {
		if r, ok := goCtx.Value(registryKey).(*Registry); ok && r != nil {
			return r
		}
	}
	return globalRegistry
}

// WithMiddlewareScope runs `fn` with an ISOLATED middleware registry scoped to a child context.Context
// (concurrent-request/goroutine isolation, design §4). The scope seeds a COPY of the currently-visible
// registry's STACK (so app-wide registrations remain in effect) but an EMPTY state map (so per-request
// state never bleeds across concurrent scopes — the TS M5 guarantee). Any [RegisterMiddleware] /
// per-request state inside `fn` mutates ONLY this scope — two concurrent WithMiddlewareScope bodies
// never see each other's middleware. `fn` receives the SCOPED context; thread it into your
// [ContextForDBCtx] / [ContextForRoutingCtx] so the seam resolves THIS scope's registry.
//
// This is the go analogue of the TS `withMiddlewareScope` (ALS): rust = task_local, py = contextvars,
// php = an explicit registry arg. `parent` may be a request context (deadline/cancellation flow
// through); nil ⇒ a background context.
func WithMiddlewareScope[R any](parent context.Context, fn func(scopeCtx context.Context) (R, error)) (R, error) {
	if parent == nil {
		parent = context.Background()
	}
	seed := currentRegistry(parent).copyStackOnly()
	scopeCtx := context.WithValue(parent, registryKey, seed)
	return fn(scopeCtx)
}

// WithEmptyMiddlewareScope is [WithMiddlewareScope] starting from an EMPTY registry (no inherited
// stack) — the go analogue of the TS `withMiddlewareScope(fn, { inherit: false })`.
func WithEmptyMiddlewareScope[R any](parent context.Context, fn func(scopeCtx context.Context) (R, error)) (R, error) {
	if parent == nil {
		parent = context.Background()
	}
	scopeCtx := context.WithValue(parent, registryKey, newRegistry())
	return fn(scopeCtx)
}

// ── Registration surface (v1 DBModel.use parity) ──────────────────────────────

// RegisterMiddleware registers `mw` on the registry the given Go context sees (the per-scope one
// inside a [WithMiddlewareScope], else the process-global default). Returns an un-register fn (v1
// DBModel.use). This is the **native registration** surface (design §4 "native 側でも登録可"): a go
// caller builds its own descriptor (or a [NewMiddleware] handle) and appends it to the ctx chain.
func RegisterMiddleware(goCtx context.Context, mw *MiddlewareDescriptor) func() {
	return currentRegistry(goCtx).register(mw)
}

// ClearMiddlewares clears the process-global registry (testing; a per-scope registry is dropped when
// its scope context is discarded).
func ClearMiddlewares() { globalRegistry.clear() }

// ── The ergonomic middleware handle (createMiddleware parity) ──────────────────

// ExecuteFn is the ergonomic SQL-level hook body [MiddlewareConfig] carries: `(state, next, sql, args)`
// (the v1 argument order — the TS `execute(next, sql, params)` with `this` = state as the leading
// arg). It is generic over the seam result type via the [NewMiddleware] adaptation, so ONE body serves
// both the read (rows) and write (run-summary) seams: it receives its per-scope `state` (nil if the
// config declares none) and an `execNext` it calls with the (possibly rewritten) sql/args, and returns
// whatever `execNext` returns (observe/rewrite/time) or a synthetic result (short-circuit).
type ExecuteFn func(state any, next ExecNext, sql string, args []any) (any, error)

// ExecNext is the terminal the ergonomic [ExecuteFn] delegates to (the rest of the chain + the
// connection execute). Its result is `[]bc.Value` on the read seam / [RunInfo] on the write seam
// (boxed as `any`); a middleware body passes it through (observe) or returns a different value
// (short-circuit).
type ExecNext func(sql string, args []any) (any, error)

// MethodFn is the ergonomic method-hook body [MiddlewareConfig] carries — identical to [MethodHook].
type MethodFn = MethodHook

// MiddlewareConfig is the ergonomic hook config [NewMiddleware] consumes (the go analogue of the TS
// createMiddleware config): an optional per-scope initial-state factory, the SQL-level Execute hook,
// and the per-kind method hooks. A hook reads its per-scope state via the [MiddlewareHandle.State].
type MiddlewareConfig struct {
	// NewState builds a FRESH per-scope state instance (bound lazily on first access WITHIN a scope, so
	// each execution scope gets its own — the isolation the empty-state-map copy guarantees). nil ⇒ the
	// handle carries no state.
	NewState func() any
	// Execute is the SQL-level hook (v1 `(next, sql, args)` order), adapted to both the read + write
	// seams. nil ⇒ no SQL-level wrapping.
	Execute ExecuteFn
	// Methods are the per-kind method hooks. nil/absent ⇒ no method wrapping.
	Methods map[MethodKind]MethodFn
}

// MiddlewareHandle is the registration handle [NewMiddleware] returns: its [MiddlewareHandle.Descriptor]
// registers it, and [MiddlewareHandle.State] reads the CURRENT execution scope's state instance (a
// fresh per-scope value, the v1 getCurrentContext). One handle → one state token.
type MiddlewareHandle struct {
	// descriptor is the underlying registration unit ([RegisterMiddleware] it).
	descriptor *MiddlewareDescriptor
	token      *stateToken
	newState   func() any
}

// Descriptor returns the underlying [MiddlewareDescriptor] to register (via [RegisterMiddleware]).
func (h *MiddlewareHandle) Descriptor() *MiddlewareDescriptor { return h.descriptor }

// State reads the CURRENT execution scope's state instance for this handle (the go context resolves
// the scope registry; a fresh instance is lazily built per scope via the config's NewState). Returns
// nil for a handle with no NewState. This is the v1 getCurrentContext — concurrent scopes each read
// their OWN state.
func (h *MiddlewareHandle) State(goCtx context.Context) any {
	if h.newState == nil {
		return nil
	}
	return currentRegistry(goCtx).stateFor(h.token, h.newState)
}

// ResetState resets this handle's state in the current scope to a fresh instance (testing convenience).
func (h *MiddlewareHandle) ResetState(goCtx context.Context) {
	if h.newState == nil {
		return
	}
	r := currentRegistry(goCtx)
	r.mu.Lock()
	if r.states == nil {
		r.states = map[*stateToken]any{}
	}
	r.states[h.token] = h.newState()
	r.mu.Unlock()
}

// NewMiddleware builds a [MiddlewareHandle] from the ergonomic [MiddlewareConfig] (the go analogue of
// the TS createMiddleware). The single Execute closure is adapted to BOTH the read + write SQL seams
// (so ONE body observes/logs/times reads AND writes); each method hook passes through. Per-scope state
// is keyed on a fresh token — read it in a hook body via `handle.State(goCtx)`.
func NewMiddleware(cfg MiddlewareConfig) *MiddlewareHandle {
	token := &stateToken{}
	desc := &MiddlewareDescriptor{token: token, newState: cfg.NewState}

	if cfg.Execute != nil {
		// Adapt the generic `(state, next, sql, args) any` body to the READ seam: box the rows result as
		// `any` for the body, unbox the body's return to []bc.Value. The registry binds `state` per wrap
		// (this scope's instance). A short-circuit returns synthetic rows as a []bc.Value.
		desc.SQLReadState = func(state any, sql string, args []any, next SeamNext[[]bc.Value]) ([]bc.Value, error) {
			execNext := func(s string, p []any) (any, error) { return next(s, p) }
			out, err := cfg.Execute(state, execNext, sql, args)
			if err != nil {
				return nil, err
			}
			return coerceRows(out), nil
		}
		// Adapt to the WRITE seam: the body's `any` result unboxes to a RunInfo.
		desc.SQLWriteState = func(state any, sql string, args []any, next SeamNext[RunInfo]) (RunInfo, error) {
			execNext := func(s string, p []any) (any, error) { return next(s, p) }
			out, err := cfg.Execute(state, execNext, sql, args)
			if err != nil {
				return RunInfo{}, err
			}
			return coerceRunInfo(out), nil
		}
	}

	if len(cfg.Methods) > 0 {
		desc.Methods = make(map[MethodKind]MethodHook, len(cfg.Methods))
		for kind, fn := range cfg.Methods {
			desc.Methods[kind] = fn
		}
	}

	return &MiddlewareHandle{descriptor: desc, token: token, newState: cfg.NewState}
}

// coerceRows unboxes a read-seam hook's `any` result to []bc.Value: a []bc.Value passes through; nil ⇒
// an empty row list. A middleware short-circuit therefore returns synthetic rows as a []bc.Value.
func coerceRows(v any) []bc.Value {
	if v == nil {
		return []bc.Value{}
	}
	if rows, ok := v.([]bc.Value); ok {
		return rows
	}
	return []bc.Value{}
}

// coerceRunInfo unboxes a write-seam hook's `any` result to RunInfo: a RunInfo passes through; anything
// else ⇒ a zero RunInfo (a middleware short-circuit returns a RunInfo directly).
func coerceRunInfo(v any) RunInfo {
	if info, ok := v.(RunInfo); ok {
		return info
	}
	return RunInfo{}
}

// ── Method-level dispatch (design §4 level 2) — the operation-boundary fold ─────

// RunMethod runs an ORM operation of KIND `kind` through the current scope's method hooks, then
// executes `core`. The hooks fold first-registered-outermost (§order), each getting `(model, next,
// args…)`; a hook may rewrite `args`, time `next`, or short-circuit. NO hook for this kind ⇒
// `core(args…)` verbatim (byte-identical — an unhooked operation runs untouched).
//
// `kind` is the operation-boundary TAG — how a method hook distinguishes read vs. write vs.
// find/create/… — NEVER parsed from the SQL text. `goCtx` resolves the scope registry.
func RunMethod(goCtx context.Context, kind MethodKind, model any, core func(args ...any) (any, error), args ...any) (any, error) {
	hooks := currentRegistry(goCtx).methodHooks(kind)
	if len(hooks) == 0 {
		return core(args...) // fast path: no method hook for this kind ⇒ untouched
	}
	next := MethodNext(func(a ...any) (any, error) { return core(a...) })
	for i := len(hooks) - 1; i >= 0; i-- {
		hook := hooks[i]
		inner := next
		next = func(a ...any) (any, error) { return hook(model, inner, a...) }
	}
	return next(args...)
}

// ── D3: the standard Logger middleware (SQL / params / timing) ─────────────────

// LogEntry is one logged statement: the SQL, its params, and the wall-clock duration `next` took
// (v1 Logger parity).
type LogEntry struct {
	SQL      string
	Params   []any
	Duration time.Duration
}

// LoggerState is the Logger's per-scope log history (the handle's State) — concurrent scopes each
// collect their OWN entries (isolated). Guarded so a middleware firing across goroutines within one
// scope does not race the slice append.
type LoggerState struct {
	mu      sync.Mutex
	entries []LogEntry
}

// Entries returns a copy of the recorded entries (in statement order).
func (s *LoggerState) Entries() []LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]LogEntry, len(s.entries))
	copy(out, s.entries)
	return out
}

func (s *LoggerState) add(e LogEntry) {
	s.mu.Lock()
	s.entries = append(s.entries, e)
	s.mu.Unlock()
}

// LoggerOptions configures [Logger]: a sink called with each entry, and an injectable clock (tests).
type LoggerOptions struct {
	// Sink is called with each [LogEntry] as its statement completes (after `next`). nil ⇒ record only.
	Sink func(LogEntry)
	// Now is an injectable clock (tests). nil ⇒ time.Now.
	Now func() time.Time
}

// Logger is the standard **Logger middleware** (design §4, v1 StatisticsMiddleware/Logger parity): a
// SQL-level hook that records the SQL, its params, and the wall-clock duration each statement takes.
// EVERY statement through the seam — read, write, tx-control, relation-batch — is logged (it is an
// execute-level hook). Register it via [RegisterMiddleware] with `Logger(opts).Descriptor()`; read the
// per-scope history via `Logger(opts)` handle's `.State(goCtx).(*LoggerState).Entries()`. Timing
// brackets the `next` call, so it measures the connection execute (chain remainder included).
//
// The log history lives on the handle's per-scope State ([LoggerState]), so concurrent scopes each
// collect their OWN entries (isolated) — the go analogue of the TS scope-local entries list.
func Logger(opts LoggerOptions) *MiddlewareHandle {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return NewMiddleware(MiddlewareConfig{
		NewState: func() any { return &LoggerState{} },
		Execute: func(state any, next ExecNext, sql string, args []any) (any, error) {
			// `state` is THIS scope's *LoggerState (the registry bound it per wrap) — so concurrent scopes
			// each record into their OWN history. Timing brackets `next` (the connection execute).
			started := now()
			out, err := next(sql, args)
			entry := LogEntry{SQL: sql, Params: append([]any(nil), args...), Duration: now().Sub(started)}
			if ls, ok := state.(*LoggerState); ok && ls != nil {
				ls.add(entry)
			}
			if opts.Sink != nil {
				opts.Sink(entry)
			}
			return out, err
		},
	})
}

// ── D3: raw Execute / Query THROUGH the seam ───────────────────────────────────
//
// A public raw statement API that goes through the exec-context seam, so a registered SQL-level
// middleware sees it AND connection routing / an ambient transaction still apply (design §4 D3). It is
// a thin front over the seam's [Execute] / [Run] — the SAME central point every runtime-generated
// statement uses. RawQuery is RawExecute tagged as a `query` operation kind (so a `query` method hook
// fires), matching v1 DBModel.query → execute.

// rawLeadingRe matches a row-returning leading keyword (SELECT / WITH / SHOW / PRAGMA / VALUES /
// EXPLAIN / TABLE) — byte-true to the TS returnsRows leading-keyword test.
var rawLeadingRe = regexp.MustCompile(`(?i)^\s*(select|with|show|pragma|values|explain|table)\b`)

// returnsRows reports whether `sql` returns rows (a SELECT-family leading keyword, or a RETURNING
// clause). Mirrors the TS middleware.ts returnsRows so RawExecute picks Execute vs. Run identically.
func returnsRows(sql string) bool {
	return rawLeadingRe.MatchString(sql) || returningRe.MatchString(sql)
}

// RawResult is the raw-statement result: a row list (for a row-returning statement) plus the
// affected-rows count (mirrors v1 ExecuteResult {rows, rowCount}). A non-row statement returns rows: [].
type RawResult struct {
	Rows     []bc.Value
	RowCount int64
}

// RawExecuteOptions configures [RawExecute]: force the write intent (writer routing / tx connection)
// for a row-returning write (an INSERT … RETURNING routed to the writer).
type RawExecuteOptions struct {
	Write bool
}

// RawExecute is the raw `execute(sql, args)` THROUGH the seam (design §4 D3): a registered SQL-level
// middleware intercepts it, connection routing resolves the connection, and an ambient transaction (if
// `ctx` is tx-scoped) applies — because it is the SAME [Execute] / [Run] seam the runtime uses, not a
// direct db call. A row-returning statement (SELECT / … RETURNING / SHOW / PRAGMA / VALUES / WITH …
// SELECT) runs [Execute]; a non-returning one runs [Run]. `opts.Write` forces the write intent for a
// row-returning write.
func RawExecute(ctx *ExecutionContext, sql string, args []any, opts RawExecuteOptions) (RawResult, error) {
	if returnsRows(sql) {
		intent := ReadIntent()
		if opts.Write {
			intent = WriteIntent()
		}
		rows, err := Execute(ctx, sql, args, intent)
		if err != nil {
			return RawResult{}, err
		}
		return RawResult{Rows: rows, RowCount: int64(len(rows))}, nil
	}
	info, err := Run(ctx, sql, args, WriteIntent())
	if err != nil {
		return RawResult{}, err
	}
	return RawResult{Rows: []bc.Value{}, RowCount: info.Changes}, nil
}

// RawQuery is the raw `query(sql, args)` — [RawExecute] tagged as a `query` operation, so a `query`
// method hook fires (then its SQL flows through the same seam + SQL-level hooks, exactly as v1
// DBModel.query calls DBModel.execute). Returns the row list. Two-level: the `query` method hook wraps
// the RawExecute, whose SQL then hits the execute hooks.
func RawQuery(ctx *ExecutionContext, sql string, args []any) ([]bc.Value, error) {
	out, err := RunMethod(ctx.ctx, MethodQuery, nil, func(_ ...any) (any, error) {
		res, e := RawExecute(ctx, sql, args, RawExecuteOptions{})
		if e != nil {
			return nil, e
		}
		return res.Rows, nil
	}, sql, args)
	if err != nil {
		return nil, err
	}
	if rows, ok := out.([]bc.Value); ok {
		return rows, nil
	}
	return []bc.Value{}, nil
}
