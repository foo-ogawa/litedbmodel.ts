<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — the **middleware layer** (Phase D / #96, PHP).
 *
 * The PHP port of the Phase D **API REFERENCE** `src/scp/middleware.ts` (#92), mirroring the rust
 * (#93) / go (#94) / python (#95) ports. It builds ON the Phase A {@see ExecutionContext} seam (the
 * empty {@see MiddlewareChain} hook Phase A reserved) — it does NOT restructure the seam; it makes the
 * reserved hook real. It mirrors the TS contract exactly: the registration surface, the SQL-level
 * `execute(next, sql, params)` chain contract + APPLIED ORDER, the method-level hook shape + op-kind
 * dispatch, the standard {@see Logger()} middleware, and the raw {@see rawExecute()} / {@see rawQuery()}
 * API that goes THROUGH the exec-context seam (so middleware + connection routing + transaction all
 * still apply).
 *
 * ## The two hook levels (design §4)
 *
 *   1. **SQL-level `execute` hook** — `(sql, params, next) => mixed`. Wraps EVERY statement that
 *      funnels through the central seam ({@see execute()} / {@see run()} / {@see runControl()}), so
 *      read, write, tx-control (BEGIN/COMMIT/ROLLBACK/isolation-SET), and relation-batch SQL are ALL
 *      intercepted. A middleware can observe / rewrite (`next(sql', params')`) / time / short-circuit
 *      (return without calling `next`). This is the seam's {@see MiddlewareChain} folded around the
 *      connection-resolve terminal.
 *
 *      **Runtime tx-control IS middleware-visible (owner option A, Phase D #96 — 5-language parity):**
 *      the transaction() combinator ({@see withTransactionDecided()}) issues its own bracketing
 *      `BEGIN`/`COMMIT`/`ROLLBACK` (+ isolation `SET`) THROUGH the seam ({@see runControl()}) on the
 *      pinned owned connection — so a registered middleware observes them, exactly as the TS reference
 *      routes BEGIN/COMMIT through `runAsync`. The physical tx-control statement stays `exec()` on the
 *      SAME owned connection (a prepared tx-control statement is unreliable on live PG/MySQL), so the
 *      audited Phase A/B ownership + atomicity + isolation-SET ordering are unchanged. Tx-control is
 *      exempt from the write=tx GUARD (it OPENS the very scope the guard checks). Body writes, reads,
 *      and relation batches inside the transaction ALL funnel through the seam and ARE intercepted too.
 *   2. **method-level hook** — at the ORM operation boundary, keyed by the operation KIND
 *      (find/findOne/findById/count/create/createMany/update/updateMany/delete/query). {@see runMethod()}
 *      folds the matching method hooks around the operation. The op kind is a TAG the operation
 *      boundary supplies — it is NEVER parsed from the SQL text.
 *
 * ## Registration + APPLIED ORDER (the 5-language contract — v1 `DBModel.use` parity)
 *
 * {@see registerMiddleware()} / {@see use_()} append to an ordered stack and return an un-register
 * closure. The stack is folded so the FIRST-registered middleware is the OUTERMOST wrapper: given
 * `use(A); use(B)`, a statement runs `A.before → B.before → «execute» → B.after → A.after`. This holds
 * identically for the SQL-level chain ({@see MiddlewareChain::wrap()}) and the method-level chain
 * ({@see runMethod()}) — the fold walks the stack from LAST to FIRST building `next`, so index 0 ends
 * up outermost. This ORDER is the normative contract the ports share.
 *
 * ## Per-scope isolation (PHP-specific — HONEST note)
 *
 * The TS reference uses `AsyncLocalStorage` so N concurrent HTTP requests each mutate their OWN
 * registry + per-request state and never serialize on one global slot. **PHP is 1-request-1-process:
 * there is no in-process concurrency to isolate**, so the ALS cross-talk mechanism has nothing to
 * guard against — the "two concurrent scopes don't see each other's middleware" test that TS runs is
 * genuinely N/A here (it is NOT faked). What IS applicable and IS real:
 *
 *   - **explicit-registry scope** — {@see Registry} is a first-class value: you can build one, pass it
 *     as the EXPLICIT `$registry` arg to the registration/dispatch calls, and thread it through the
 *     seam. This is the §3-table "php = an explicit registry arg" mechanism — the honest
 *     single-threaded analogue of the ALS scope.
 *   - **scoped registration** — {@see withMiddlewareScope()} pins an ISOLATED registry (a COPY of the
 *     currently-visible one, or empty) as the AMBIENT registry for the duration of a callback + restores
 *     the prior on exit (the same process-scoped-holder pattern as {@see TxAmbient}). Registrations +
 *     per-scope state inside the callback mutate ONLY that scope; the prior registry is untouched. This
 *     is what the unit tests use so the process-global default stays clean (an unregistered chain is
 *     byte-identical — the conformance/livedb runners register none).
 *
 * ## Fold direction + empty-chain passthrough (the reproduced invariants)
 *
 *   - **fold last→first, index 0 OUTERMOST** — {@see MiddlewareChain::wrap()} (SQL) and {@see runMethod()}
 *     (method) both build `next` walking the stack from LAST to FIRST, so the first-registered wrapper
 *     is outermost.
 *   - **empty / unregistered chain ⇒ byte-identical passthrough** — an empty SQL stack makes `wrap`
 *     return `$next($sql, $params)` verbatim; an empty method-hook list makes `runMethod` call
 *     `$core(...$args)` verbatim.
 *
 * NB native registration (design §4 "native 側でも登録可"): {@see registerMiddleware()} appends a
 * PHP-closure {@see MiddlewareDescriptor} to the ctx chain; the CHAIN CONTRACT + ORDER above is the
 * shared 5-language contract, the middleware BODY is the language's closure. TS is the reference shape.
 */

// ── Method-level hook kinds (design §4 level 2) — the ORM operation boundary ────

/**
 * The ORM operation KIND a method hook keys on (v2 maps the v1 method names onto the read/write
 * operations). A read operation is tagged `find`/`findOne`/`findById`/`count`/`query`; a write is
 * tagged `create`/`createMany`/`update`/`updateMany`/`delete`. {@see runMethod()} dispatches to the
 * hook of the matching kind — this is how a method hook DISTINGUISHES the op kind (the tag the
 * operation boundary supplies, NOT a guess from the SQL text). Mirrors the TS `MethodKind` union.
 */
final class MethodKind
{
    public const FIND = 'find';
    public const FIND_ONE = 'findOne';
    public const FIND_BY_ID = 'findById';
    public const COUNT = 'count';
    public const CREATE = 'create';
    public const CREATE_MANY = 'createMany';
    public const UPDATE = 'update';
    public const UPDATE_MANY = 'updateMany';
    public const DELETE = 'delete';
    public const QUERY = 'query';

    /** The 10 op-kind keys (the config keys {@see createMiddleware()} reads), in a fixed order. */
    public const ALL = [
        self::FIND, self::FIND_ONE, self::FIND_BY_ID, self::COUNT, self::CREATE,
        self::CREATE_MANY, self::UPDATE, self::UPDATE_MANY, self::DELETE, self::QUERY,
    ];
}

// ── The middleware descriptor (the registration unit) ──────────────────────────

/**
 * A registered middleware: its (optional) SQL-level hook, its per-kind method hooks, and a per-scope
 * STATE factory + token. {@see registerMiddleware()} registers ONE of these. Built by
 * {@see createMiddleware()} from the ergonomic (v1-shaped) config; a hand-built descriptor is also
 * accepted. Mirrors the TS `MiddlewareDescriptor`.
 */
final class MiddlewareDescriptor
{
    /**
     * @param (callable(string, list<mixed>, callable):mixed)|null $sql the SQL-level `execute` hook
     *        `(sql, params, next)` (design §4 level 1), if any.
     * @param array<string, callable(mixed, callable, mixed...):mixed> $methods the method-level hooks
     *        keyed by {@see MethodKind} (design §4 level 2).
     * @param object|null $stateToken the identity key for this middleware's per-scope state instance
     *        in a {@see Registry} (a distinct `new \stdClass` per descriptor); null ⇒ no state.
     * @param (callable():object)|null $freshState builds a FRESH state object (a deep copy of the
     *        config's initial state) on first access in a scope; null ⇒ no state.
     */
    public function __construct(
        public $sql = null,
        public array $methods = [],
        public readonly ?object $stateToken = null,
        public $freshState = null,
    ) {
    }
}

/**
 * The registration handle returned by {@see createMiddleware()}: {@see descriptor()} is the underlying
 * {@see MiddlewareDescriptor} (for {@see registerMiddleware()} / {@see use_()}); {@see state()} reads
 * the CURRENT scope's state instance (fresh per scope, v1 `getCurrentContext()`); {@see resetState()}
 * resets it. Mirrors the TS `MiddlewareHandle`.
 */
final class MiddlewareHandle
{
    /**
     * @param (callable(?Registry):object)|null $stateReader reads the state for a registry (null ⇒ default scope).
     * @param (callable(?Registry):void)|null $stateReset resets the state for a registry.
     */
    public function __construct(
        private readonly MiddlewareDescriptor $descriptor,
        private $stateReader = null,
        private $stateReset = null,
    ) {
    }

    /** The underlying descriptor (register via {@see registerMiddleware()} / {@see use_()}). */
    public function descriptor(): MiddlewareDescriptor
    {
        return $this->descriptor;
    }

    /**
     * The CURRENT scope's state instance for this middleware (fresh per scope, v1 `getCurrentContext()`).
     * Pass an explicit `$registry` to read THAT registry's copy (the explicit-registry-scope form).
     */
    public function state(?Registry $registry = null): object
    {
        if ($this->stateReader === null) {
            return new \stdClass();
        }
        return ($this->stateReader)($registry);
    }

    /** Reset this middleware's state in the current (or explicit `$registry`) scope to a fresh copy. */
    public function resetState(?Registry $registry = null): void
    {
        if ($this->stateReset !== null) {
            ($this->stateReset)($registry);
        }
    }
}

// ── The middleware registry (the ordered stack + per-scope state) ──────────────

/**
 * The ordered middleware stack (Phase D). {@see use()} appends (first-registered = outermost, §order),
 * returning an un-register closure. {@see sqlHooks()} / {@see methodHooks()} return the folded-order
 * slices the seam + {@see runMethod()} consume. A {@see Registry} is EITHER the process-global default
 * (app-startup registration) OR a per-scope copy pushed by {@see withMiddlewareScope()} OR an explicit
 * registry a caller builds + threads (the §3-table PHP mechanism) — the two share this class; only
 * their lifetime differs. Mirrors the TS `Registry`.
 */
final class Registry
{
    /** @var list<MiddlewareDescriptor> */
    private array $stack = [];

    /**
     * Per-scope STATE instances, keyed by a descriptor's state TOKEN. Because a scope's registry is a
     * COPY ({@see copy()}) that starts with an EMPTY state map, each scope lazily builds its OWN fresh
     * state instance — isolated across scopes (an explicit registry has its own map too).
     *
     * @var \SplObjectStorage<object, object>
     */
    private \SplObjectStorage $states;

    public function __construct()
    {
        $this->states = new \SplObjectStorage();
    }

    /** This scope's state for `$token`, lazily created via `$fresh` on first access (v1 getCurrentContext). */
    public function stateFor(object $token, callable $fresh): object
    {
        if (!$this->states->contains($token)) {
            $this->states->attach($token, $fresh());
        }
        return $this->states[$token];
    }

    /** Reset `$token`'s state in this scope to a fresh instance (testing convenience). */
    public function resetStateFor(object $token, callable $fresh): void
    {
        $this->states[$token] = $fresh();
    }

    /** Register `$mw` (appended ⇒ outermost). Returns an idempotent un-register closure. */
    public function use(MiddlewareDescriptor $mw): callable
    {
        $this->stack[] = $mw;
        return function () use ($mw): void {
            $i = array_search($mw, $this->stack, true);
            if ($i !== false) {
                array_splice($this->stack, (int) $i, 1);
            }
        };
    }

    /** Remove `$mw` (v1 `removeMiddleware`). Returns whether it was present. */
    public function remove(MiddlewareDescriptor $mw): bool
    {
        $i = array_search($mw, $this->stack, true);
        if ($i === false) {
            return false;
        }
        array_splice($this->stack, (int) $i, 1);
        return true;
    }

    /** Drop every registration (v1 `clearMiddlewares` — testing). */
    public function clear(): void
    {
        $this->stack = [];
    }

    /**
     * The registered descriptors, registration order (index 0 = first = outermost).
     *
     * @return list<MiddlewareDescriptor>
     */
    public function all(): array
    {
        return $this->stack;
    }

    /**
     * The SQL-level hooks (registration order), for the {@see MiddlewareChain} fold.
     *
     * @return list<callable(string, list<mixed>, callable):mixed>
     */
    public function sqlHooks(): array
    {
        $out = [];
        foreach ($this->stack as $mw) {
            if ($mw->sql !== null) {
                $out[] = $mw->sql;
            }
        }
        return $out;
    }

    /**
     * The method hooks for `$kind` (registration order), for the {@see runMethod()} fold.
     *
     * @return list<callable(mixed, callable, mixed...):mixed>
     */
    public function methodHooks(string $kind): array
    {
        $out = [];
        foreach ($this->stack as $mw) {
            if (isset($mw->methods[$kind])) {
                $out[] = $mw->methods[$kind];
            }
        }
        return $out;
    }

    /** A shallow COPY (a scope registry seeded from the current set; the state map is NOT copied). */
    public function copy(): Registry
    {
        $r = new Registry();
        $r->stack = $this->stack;
        return $r;
    }
}

/**
 * The process-global default registry + the per-scope AMBIENT override holder — the PHP analogue of
 * the TS `globalRegistry` + `AsyncLocalStorage<Registry>`. PHP is 1-request-1-process, so a plain
 * process-scoped holder ({@see TxAmbient}-style) is the honest single-threaded equivalent of the ALS:
 * only ONE execution scope is live at a time, and there is no concurrent scope to leak into.
 */
final class MiddlewareRegistryHolder
{
    private static ?Registry $global = null;
    /** The per-scope override (present ⇒ registration/reads target THIS scope's copy). */
    private static ?Registry $scoped = null;

    /** The process-global default registry (app-startup `registerMiddleware` with no explicit scope). */
    public static function global(): Registry
    {
        return self::$global ??= new Registry();
    }

    /** The registry the current execution scope resolves to: the scoped override, else the global. */
    public static function current(): Registry
    {
        return self::$scoped ?? self::global();
    }

    /** Pin `$registry` as the scoped ambient override; return the PRIOR override (for symmetric restore). */
    public static function setScope(?Registry $registry): ?Registry
    {
        $prev = self::$scoped;
        self::$scoped = $registry;
        return $prev;
    }

    /** Restore a previously-{@see setScope()}'d override (the `finally`-side unwind). */
    public static function restoreScope(?Registry $prev): void
    {
        self::$scoped = $prev;
    }
}

/**
 * The registry the current execution scope resolves to: an active {@see withMiddlewareScope()} scope's
 * registry, else the process-global default. The PHP analogue of the TS `currentRegistry()`.
 */
function currentRegistry(): Registry
{
    return MiddlewareRegistryHolder::current();
}

/**
 * Run `$fn` with an ISOLATED middleware registry pinned as the ambient scope (the PHP single-threaded
 * analogue of the TS `withMiddlewareScope` ALS run). The scope seeds a COPY of the currently-visible
 * registry (so app-wide registrations remain in effect), or an EMPTY one (`$inherit = false`); any
 * {@see registerMiddleware()} / per-scope state inside `$fn` mutates ONLY this scope, and the prior
 * ambient registry is restored on exit. Returns `$fn`'s value.
 *
 * NB PHP is 1-request-1-process, so this is NOT concurrency isolation (there are no concurrent scopes
 * to keep apart — see the class doc). It is the "keep the global registry clean for THIS unit of work"
 * mechanism the tests use, and the honest single-threaded equivalent of the ALS scope.
 *
 * @param callable():mixed $fn
 */
function withMiddlewareScope(callable $fn, bool $inherit = true): mixed
{
    $seed = $inherit ? currentRegistry()->copy() : new Registry();
    $prev = MiddlewareRegistryHolder::setScope($seed);
    try {
        return $fn();
    } finally {
        MiddlewareRegistryHolder::restoreScope($prev);
    }
}

/**
 * The LIVE SQL-level middleware stack of the current execution scope — the source
 * {@see Context::ambientChain()} gives its {@see MiddlewareChain}. Resolved at EACH `wrap`, so
 * registration after ctx construction, and a per-scope registry, are both honored. Empty ⇒ the seam
 * is a byte-identical passthrough. The PHP analogue of the TS `activeSqlMiddlewares()`.
 *
 * @return list<callable(string, list<mixed>, callable):mixed>
 */
function activeSqlMiddlewares(): array
{
    return currentRegistry()->sqlHooks();
}

// ── Registration surface (v1 `DBModel.use` / native `registerMiddleware` parity) ─

/**
 * **Native `registerMiddleware`** (design §4 "native 側でも登録可"): register a {@see MiddlewareHandle}
 * (or a raw {@see MiddlewareDescriptor}) on the CURRENT scope's registry (the ambient per-scope one
 * inside {@see withMiddlewareScope()}, else the process-global default), OR on an EXPLICIT `$registry`.
 * Returns an un-register closure (v1 `DBModel.use` :414). This is the PHP runtime's own registration
 * API appending to its ctx chain — the shared CHAIN CONTRACT + ORDER, the language's closure body.
 */
function registerMiddleware(MiddlewareHandle|MiddlewareDescriptor $mw, ?Registry $registry = null): callable
{
    $descriptor = $mw instanceof MiddlewareHandle ? $mw->descriptor() : $mw;
    return ($registry ?? currentRegistry())->use($descriptor);
}

/**
 * The v1-`DBModel.use`-named alias of {@see registerMiddleware()} (`use` is a PHP keyword, hence the
 * trailing underscore). Register a middleware; returns an un-register closure.
 */
function use_(MiddlewareHandle|MiddlewareDescriptor $mw, ?Registry $registry = null): callable
{
    return registerMiddleware($mw, $registry);
}

/**
 * Build a {@see MiddlewareHandle} from a v1-shaped config (v1 `createMiddleware` parity). Each hook
 * body runs with `$this` bound (via {@see \Closure::bindTo()}) to the CURRENT scope's state object (a
 * fresh deep copy of `$config['state']` per scope, v1 `structuredClone` — v1 `getCurrentContext()`).
 * The `execute` hook is adapted from the v1 `(next, sql, params)` order to the seam's `(sql, params,
 * next)` order, so a v1 hook body ports unchanged. Method hooks pass through in the v1
 * `(model, next, ...args)` shape.
 *
 * @param array{
 *   state?: object|array<string,mixed>,
 *   execute?: callable,
 *   find?: callable, findOne?: callable, findById?: callable, count?: callable,
 *   create?: callable, createMany?: callable, update?: callable, updateMany?: callable,
 *   delete?: callable, query?: callable,
 * } $config
 */
function createMiddleware(array $config): MiddlewareHandle
{
    // A distinct token per descriptor keys its per-scope state in a Registry. The fresh-state factory
    // deep-copies the config's initial state (v1 structuredClone) — an object is cloned recursively,
    // an array is wrapped in a fresh stdClass so hook bodies read `$this->prop`.
    $token = new \stdClass();
    $initial = $config['state'] ?? null;
    $freshState = static function () use ($initial): object {
        if ($initial === null) {
            return new \stdClass();
        }
        if (is_object($initial)) {
            return deepCloneState($initial);
        }
        // An array initial state ⇒ a fresh stdClass with the keys as properties (deep-copied).
        $obj = new \stdClass();
        foreach ($initial as $k => $v) {
            $obj->{$k} = is_object($v) || is_array($v) ? deepCloneState((object) $v) : $v;
        }
        return $obj;
    };

    // The state reader for a given (or current) scope's registry — lazily builds the fresh instance.
    $stateReader = static fn (?Registry $registry): object =>
        ($registry ?? currentRegistry())->stateFor($token, $freshState);
    $stateReset = static function (?Registry $registry) use ($token, $freshState): void {
        ($registry ?? currentRegistry())->resetStateFor($token, $freshState);
    };

    // SQL-level hook: adapt v1 (next, sql, params) → seam (sql, params, next); bind `$this` to state.
    $sql = null;
    if (isset($config['execute'])) {
        $userExecute = $config['execute'];
        $sql = static function (string $s, array $p, callable $next) use ($userExecute, $stateReader) {
            // v1 `next(sql, params?)` — a rewrite may omit params (⇒ []). Bind $this to the state.
            $seamNext = static fn (string $ns, ?array $np = null): mixed => $next($ns, $np ?? []);
            $bound = \Closure::fromCallable($userExecute)->bindTo($stateReader(null), null);
            $callable = $bound ?? $userExecute; // static closures can't bind — call as-is
            return $callable($seamNext, $s, $p);
        };
    }

    $methods = [];
    foreach (MethodKind::ALL as $kind) {
        if (isset($config[$kind])) {
            $userHook = $config[$kind];
            $methods[$kind] = static function (mixed $model, callable $next, mixed ...$args) use ($userHook, $stateReader): mixed {
                $bound = \Closure::fromCallable($userHook)->bindTo($stateReader(null), null);
                $callable = $bound ?? $userHook;
                return $callable($model, $next, ...$args);
            };
        }
    }

    $descriptor = new MiddlewareDescriptor($sql, $methods, $token, $freshState);
    return new MiddlewareHandle($descriptor, $stateReader, $stateReset);
}

/** Recursively deep-copy a state object (the v1 `structuredClone` of the initial state). */
function deepCloneState(object $obj): object
{
    $copy = new \stdClass();
    foreach (get_object_vars($obj) as $k => $v) {
        if (is_object($v)) {
            $copy->{$k} = deepCloneState($v);
        } elseif (is_array($v)) {
            $copy->{$k} = array_map(
                static fn ($x) => is_object($x) ? deepCloneState($x) : $x,
                $v,
            );
        } else {
            $copy->{$k} = $v;
        }
    }
    return $copy;
}

// ── Method-level dispatch (design §4 level 2) — the operation boundary fold ─────

/**
 * Run an ORM operation of KIND `$kind` through the current scope's method hooks, then execute `$core`.
 * The hooks fold first-registered-outermost (§order, walking LAST→FIRST), each getting `(model, next,
 * ...args)`; a hook may rewrite `$args`, time `$next`, or short-circuit. Empty hooks for this kind ⇒
 * `$core(...$args)` verbatim (byte-identical — no method registered = the operation runs untouched).
 *
 * The op kind is a TAG supplied by the caller at the operation boundary — it is NEVER parsed from any
 * SQL text. Mirrors the TS `runMethod`. Pass an explicit `$registry` for the explicit-registry-scope.
 *
 * @param callable(mixed...):mixed $core the actual operation, taking the (possibly hook-rewritten) args.
 * @param list<mixed> $args
 */
function runMethod(string $kind, mixed $model, callable $core, array $args, ?Registry $registry = null): mixed
{
    $hooks = ($registry ?? currentRegistry())->methodHooks($kind);
    if (count($hooks) === 0) {
        return $core(...$args); // fast path: no method hook for this kind
    }
    $next = static fn (mixed ...$a): mixed => $core(...$a);
    for ($i = count($hooks) - 1; $i >= 0; $i--) {
        $hook = $hooks[$i];
        $inner = $next;
        $next = static fn (mixed ...$a): mixed => $hook($model, $inner, ...$a);
    }
    return $next(...$args);
}

// ── D3: the standard Logger middleware (SQL / params / timing) ─────────────────

/** One logged statement: the SQL, its params, and the wall-clock ms `next` took (v1 Logger parity). */
final class LogEntry
{
    /**
     * @param list<mixed> $params
     * @param float $durationMs wall-clock milliseconds the wrapped `next` (chain remainder + execute) took.
     */
    public function __construct(
        public readonly string $sql,
        public readonly array $params,
        public readonly float $durationMs,
    ) {
    }
}

/**
 * The standard **Logger middleware** (design §4, v1 `StatisticsMiddleware`/`Logger` parity): a
 * SQL-level hook that records the SQL, its params, and the wall-clock ms each statement takes. Every
 * statement through the seam — read, write, tx-control, relation-batch — is logged (it is an
 * `execute`-level hook). Register it with {@see registerMiddleware()}. Timing brackets the `$next`
 * call, so it measures the connection execute (chain remainder included), NOT just the record call.
 *
 * The per-scope log history lives on the handle's `state()->entries` (v1
 * `getCurrentContext()->getLogs()`) — a scope-local list. `$sink` is called with each entry as its
 * statement completes; `$now` is an injectable clock (tests). Mirrors the TS `Logger`.
 *
 * @param (callable(LogEntry):void)|null $sink called with each {@see LogEntry} after `next`.
 * @param (callable():float)|null $now injectable millisecond clock (default: `microtime(true)*1000`).
 */
function Logger(?callable $sink = null, ?callable $now = null): MiddlewareHandle
{
    $clock = $now ?? static fn (): float => microtime(true) * 1000.0;
    $state = new \stdClass();
    $state->entries = [];
    return createMiddleware([
        'state' => $state,
        // `$this` is the per-scope state (isolated log history). Time `$next`, record, re-raise on error.
        'execute' => function (callable $next, string $sql, array $params) use ($clock, $sink): mixed {
            $started = $clock();
            $record = function () use ($clock, $started, $sql, $params, $sink): void {
                $entry = new LogEntry($sql, $params, $clock() - $started);
                $this->entries[] = $entry;
                if ($sink !== null) {
                    $sink($entry);
                }
            };
            try {
                $result = $next($sql, $params);
            } catch (\Throwable $e) {
                $record();
                throw $e;
            }
            $record();
            return $result;
        },
    ]);
}

// ── D3: raw execute / query THROUGH the seam ───────────────────────────────────

/**
 * The raw-statement result: a row list (for a row-returning statement) plus the affected-rows count
 * (mirrors v1 `ExecuteResult { rows, rowCount }` / the TS `RawResult`). A non-row statement resolves
 * `rows: []`.
 */
final class RawResult
{
    /**
     * @param list<\stdClass> $rows
     */
    public function __construct(
        public readonly array $rows,
        public readonly ?int $rowCount,
    ) {
    }
}

/** Does `$sql` return rows (SELECT / …RETURNING / WITH…SELECT / SHOW / PRAGMA / VALUES / EXPLAIN / TABLE)? */
function returnsRows(string $sql): bool
{
    return preg_match('/^\s*(select|with|show|pragma|values|explain|table)\b/i', $sql) === 1
        || preg_match('/\breturning\b/i', $sql) === 1;
}

/**
 * Raw `execute(sql, params)` THROUGH the seam (design §4 D3): a registered SQL-level middleware
 * intercepts it, connection routing resolves the connection, and an ambient transaction (if the ctx is
 * tx-scoped) applies — because it is the SAME {@see execute()} / {@see run()} seam the ORM uses, not a
 * direct PDO call. A row-returning statement runs `execute`; a non-returning one runs `run`. `$write`
 * forces the write intent (writer routing / tx connection) for a row-returning write. Mirrors the TS
 * `rawExecute`.
 *
 * @param list<mixed> $params
 */
function rawExecute(ExecutionContext $ctx, string $sql, array $params = [], bool $write = false): RawResult
{
    if (returnsRows($sql)) {
        $rows = execute($ctx, $sql, $params, new StatementIntent($write));
        return new RawResult($rows, count($rows));
    }
    $info = run($ctx, $sql, $params);
    return new RawResult([], $info->changes);
}

/**
 * Raw `query(sql, params)` — {@see rawExecute()} tagged as a `query` operation, so a `query` method
 * hook fires (then its SQL flows through the same seam + `execute` hooks, exactly as v1
 * `DBModel::query` calls `DBModel::execute`). Returns the row list. Mirrors the TS `rawQuery`.
 *
 * @param list<mixed> $params
 * @return list<\stdClass>
 */
function rawQuery(ExecutionContext $ctx, string $sql, array $params = [], ?Registry $registry = null): array
{
    /** @var list<\stdClass> */
    return runMethod(
        MethodKind::QUERY,
        null,
        static fn (): array => rawExecute($ctx, $sql, $params)->rows,
        [$sql, $params],
        $registry,
    );
}

// ── Reset / testing helpers ────────────────────────────────────────────────────

/** Clear the process-global registry (testing; a per-scope registry is dropped when its scope exits). */
function clearMiddlewares(): void
{
    MiddlewareRegistryHolder::global()->clear();
}
