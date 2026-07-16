<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — the **ExecutionContext + central execute/run seam** (Phase A / #79, PHP).
 *
 * The PHP port of the TS contract-defining artifact `src/scp/exec-context.ts` (#75), mirroring the
 * rust port `rust/litedbmodel_runtime/src/exec_context.rs` (#76), the go port
 * `go/litedbmodel_runtime/exec_context.go` (#77) and the python port
 * `python/litedbmodel_runtime/exec_context.py` (#78). It replaces the raw `\PDO $db` threaded
 * through `Runtime::executeBundle` / `StaticBundle::executeReadGraph` / the relation walker /
 * `WriteRuntime::executeTransaction` with an {@see ExecutionContext} that carries:
 *
 *   1. a **connection provider** — {@see ExecutionContext::connectionFor()} `(intent)` resolves WHICH
 *      connection a statement runs on (the tx-owned connection, else the primary PDO; Phase A wires
 *      only the tx-owned + single-DB cases, reader/writer/named-DB are B/C/D on this seam);
 *   2. a **middleware chain** — {@see ExecutionContext::$middleware}, wrapping every SQL (empty in
 *      Phase A = passthrough; the registration API is Phase D — this is only the hook point);
 *   3. a **pinned tx connection** — a tx-scoped ctx pins ONE owned connection so every statement in a
 *      transaction body runs on it (per-execution connection ownership, §3).
 *
 * ## The central seam (§2) — ALL SQL funnels through here
 *
 * ```
 *   execute(ctx, sql, params) -> Rows      // SELECT / RETURNING reads
 *   run(ctx, sql, params)     -> RunInfo   // INSERT/UPDATE/DELETE, BEGIN/COMMIT/ROLLBACK
 * ```
 *
 * Both do the SAME three things, in order:
 *   ① run the middleware chain (empty ⇒ passthrough, behavior unchanged);
 *   ② resolve the connection via `ctx->connectionFor(intent)`;
 *   ③ execute on that connection (the ONLY driver contact point).
 *
 * Every direct `$db->prepare(sql)->execute()` / `$db->exec()` in the read / tx / relation path is
 * replaced by a call through this seam. A `grep` for `->prepare(` / `->exec(` outside the connection
 * adapters (this module's {@see PdoConnection} + {@see PdoTxConnection}, and the LiveDb PDO
 * subclasses, which ARE the ONE driver contact) comes up empty in the runtime SQL path — that is the AC.
 *
 * ## ONE interface, not sync/async-bifurcated (contract flag)
 *
 * The TS reference bifurcates into `ExecutionContext`/`execute` (better-sqlite3, sync) and
 * `AsyncExecutionContext`/`executeAsync` (pg/mysql2, async) because that split is TS-runtime specific.
 * Per the #79 contract flags the PHP port **collapses to ONE interface**: PDO is a single blocking
 * API — there is exactly ONE {@see ExecutionContext} / one {@see execute()} / one {@see run()}. There
 * is likewise **no executeSafeIntegers** — that is a better-sqlite3 #59 BIGINT toggle; PDO returns
 * BIGINT as a native string (PG/MySQL) or a plain value (SQLite), so the read seam is a plain execute.
 *
 * ## Per-execution connection ownership (§3) — the concurrent-tx fix
 *
 * A transaction acquires ONE connection via {@see PdoDriver::beginTx()} (a {@see PdoTxConnection}
 * owned handle — the PHP analogue of v1 `PoolTransaction` / go's `*sql.Tx`), pins it into a tx-scoped
 * {@see ExecutionContext} **propagated as an EXPLICIT argument** (§3 table: PHP has no
 * AsyncLocalStorage / contextvars), runs its body (every statement resolves that connection via
 * `connectionFor`), COMMITs/ROLLBACKs on the SAME owned connection, and releases it EXACTLY ONCE.
 *
 * PHP is 1-request-1-process, so there is exactly ONE `\PDO` (no pool) and it can hold exactly ONE
 * active transaction; the tx therefore OWNS that connection for its span (the single-connection
 * ownership model — the PHP analogue of Python's SQLite `_SqliteTxConnection`). There is NO
 * driver-global / static shared-tx slot: `beginTx()` returns a fresh owned handle each call, and the
 * seam combinator is the SOLE releaser. See the class docs on why in-process concurrent-tx isolation
 * is N/A for PHP (the multi-statement atomicity + ownership contract IS fully exercised).
 */

// ── The ONE driver contact point (§5) — a Connection ──────────────────────────

/**
 * The ONE driver contact point (§5): a resolved connection a statement runs on. Outside a tx this is
 * the primary {@see PdoDriver} (each `prepare` = the one in-proc/live PDO); inside a tx it is the
 * tx-owned {@see PdoTxConnection} handle. The seam is the ONLY caller; the runtime SQL path never
 * touches a `$pdo->prepare(...)` directly.
 */
interface Connection
{
    /**
     * Run a SELECT / RETURNING statement; return the raw rows (FETCH_OBJ, byte-identical to the
     * pre-seam path).
     *
     * @param list<mixed> $params
     * @return list<\stdClass>
     */
    public function execute(string $sql, array $params): array;

    /**
     * Run a non-returning write / DDL / tx-control statement; return the affected summary.
     *
     * @param list<mixed> $params
     */
    public function run(string $sql, array $params): RunInfo;
}

/** The summary of a non-returning write: affected-row count + last insert id (mirrors better-sqlite3). */
final class RunInfo
{
    public function __construct(
        public readonly int $changes,
        public readonly int|string $lastInsertRowid,
    ) {
    }
}

/**
 * Adapt a raw `\PDO` to the {@see Connection} seam (the ONE driver contact for the non-tx path).
 * `execute`/`run` issue the SAME `$pdo->prepare($sql)->execute()` the runtime used directly before the
 * seam — so a ctx built via {@see Context::forPdo()} is byte-identical to the old raw-`\PDO` path (the
 * backward-compat wrapper, §6). It is the ONE place a `$pdo->prepare()` is issued on the non-tx path.
 */
final class PdoConnection implements Connection
{
    public function __construct(private readonly \PDO $pdo)
    {
    }

    /** The raw PDO (LiveDb PDO subclasses adapt placeholder/RETURNING at prepare/execute time). */
    public function pdo(): \PDO
    {
        return $this->pdo;
    }

    public function execute(string $sql, array $params): array
    {
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute(array_values($params));
        $rows = $stmt->fetchAll(\PDO::FETCH_OBJ);
        return is_array($rows) ? array_values($rows) : [];
    }

    public function run(string $sql, array $params): RunInfo
    {
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute(array_values($params));
        // NB: lastInsertRowid is NOT eagerly resolved. The PHP runtime never consumes it (only
        // `changes` is read), and PDO::lastInsertId() on Postgres calls lastval() — which RAISES
        // `lastval is not yet defined` for a write that touched no sequence AND, worse, that raise
        // ABORTS the whole PG transaction (25P02 on the next statement). The pre-seam write path read
        // only rowCount(), so leaving lastInsertRowid at 0 is byte-identical and tx-safe.
        return new RunInfo($stmt->rowCount(), 0);
    }
}

/**
 * A {@see Connection} view over a tx's OWNED {@see PdoTxConnection} handle. The seam resolves this
 * (via `connectionFor`) for every statement inside a tx, so all of them run on the SAME owned
 * connection. It funnels `execute`/`run` to the handle's PDO — the SAME single `\PDO` the tx issued
 * its `BEGIN` on — so a statement mis-routed to a DIFFERENT (autocommit) connection would escape the
 * transaction (the mutation the atomicity "teeth" test exploits).
 */
final class TxConnectionAdapter implements Connection
{
    public function __construct(private readonly PdoTxConnection $tx)
    {
    }

    public function execute(string $sql, array $params): array
    {
        return $this->tx->all($sql, $params);
    }

    public function run(string $sql, array $params): RunInfo
    {
        return $this->tx->run($sql, $params);
    }
}

// ── The SQL driver seam (§5) — prepare + beginTx over a \PDO ───────────────────

/**
 * The minimal SQL-driver seam the runtime needs: a {@see Connection} for the non-tx path plus a
 * {@see PdoDriver::beginTx()} that acquires an OWNED tx handle. This is the PHP analogue of the
 * Python `Driver` (prepare + begin_tx) / go's `*sql.DB` (Query/Exec + Begin). A single `\PDO`
 * backs it (PHP has no pool); the tx therefore owns THAT `\PDO` for its span.
 */
final class PdoDriver
{
    private readonly PdoConnection $conn;

    public function __construct(private readonly \PDO $pdo)
    {
        $this->conn = new PdoConnection($pdo);
    }

    /** The raw PDO (so callers that must reach the driver — LiveDb resets — still can). */
    public function pdo(): \PDO
    {
        return $this->pdo;
    }

    /** The non-tx {@see Connection} adapter (the ONE driver contact for reads / non-tx writes). */
    public function connection(): PdoConnection
    {
        return $this->conn;
    }

    /**
     * Acquire an OWNED {@see PdoTxConnection} for a transaction (per-execution connection ownership,
     * §3). `BEGIN` is issued on the acquired connection here. The seam combinator
     * ({@see withTransactionDecided()}) pins the returned handle so every statement in the tx body
     * runs on it, then ends the tx (COMMIT/ROLLBACK) + releases EXACTLY ONCE.
     *
     * A fresh handle every call (no static/driver-global slot) — the removal the design mandates.
     * PHP's single `\PDO` can hold ONE active transaction, so the handle owns THAT connection; a
     * nested `beginTx` while a tx is live would be an SQL error at `BEGIN`, matching the pre-seam
     * `$db->exec('BEGIN')` behavior byte-for-byte.
     *
     * `$before` / `$after` carry the per-transaction isolation prelude (Phase B / #85 —
     * {@see \LiteDbModel\Runtime\isolationPrelude()}): MySQL's `SET TRANSACTION ISOLATION LEVEL` runs
     * pre-BEGIN (it scopes the NEXT tx), PG's runs post-BEGIN (valid as the first in-tx statement).
     * Both are issued on the SAME acquired connection, atomically bracketing the `BEGIN`. Empty ⇒ a
     * bare `BEGIN` (byte-identical to the Phase A path).
     *
     * @param list<string> $before statements run BEFORE `BEGIN` (MySQL isolation SET)
     * @param list<string> $after  statements run AFTER `BEGIN` (PG isolation SET)
     */
    public function beginTx(array $before = [], array $after = []): PdoTxConnection
    {
        foreach ($before as $sql) {
            $this->pdo->exec($sql);
        }
        $this->pdo->exec('BEGIN');
        foreach ($after as $sql) {
            $this->pdo->exec($sql);
        }
        return new PdoTxConnection($this->pdo);
    }
}

/**
 * The OWNED tx handle over a `\PDO` (Phase A / #79) — the PHP analogue of v1 `PoolTransaction` /
 * go's `*sql.Tx` / python's `_SqliteTxConnection`. It holds ONE connection (PHP's single `\PDO`) for
 * the transaction's whole duration: every statement in the tx body runs on it (`all` / `run`), the
 * tx ends by running {@see commit()} / {@see rollback()} on the SAME owned connection, and the
 * combinator then {@see release()}s it EXACTLY ONCE.
 *
 * **Release ownership**: `commit`/`rollback` ONLY run the SQL — they do NOT release. The seam
 * combinator ({@see withTransactionDecided()}) is the SOLE owner of {@see release()}, calling it in a
 * `finally` so the owned connection is reset on EVERY path (success, body error, AND a
 * commit/rollback that itself throws — the leak the #78 self-release model missed). {@see release()}
 * is idempotent (a second call is a no-op).
 */
final class PdoTxConnection
{
    private bool $released = false;
    /** How many times {@see release()} actually ran its body (must be EXACTLY 1 over a tx lifetime). */
    private int $releaseCount = 0;
    /** The `destroy` flag of the effective release (null until released) — true ⇒ the poisoned path. */
    private ?bool $releasedDestroy = null;

    public function __construct(private readonly \PDO $pdo)
    {
    }

    /** The number of effective releases (the leak-guard assertion: EXACTLY 1 on every tx path). */
    public function releaseCount(): int
    {
        return $this->releaseCount;
    }

    /** The `destroy` flag of the effective release (true ⇒ poisoned/destroyed; null ⇒ not released). */
    public function releasedDestroy(): ?bool
    {
        return $this->releasedDestroy;
    }

    /**
     * @param list<mixed> $params
     * @return list<\stdClass>
     */
    public function all(string $sql, array $params): array
    {
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute(array_values($params));
        $rows = $stmt->fetchAll(\PDO::FETCH_OBJ);
        return is_array($rows) ? array_values($rows) : [];
    }

    /**
     * @param list<mixed> $params
     */
    public function run(string $sql, array $params): RunInfo
    {
        $stmt = $this->pdo->prepare($sql);
        $stmt->execute(array_values($params));
        // lastInsertRowid left at 0 — see PdoConnection::run (never consumed; lastval() poisons a PG tx).
        return new RunInfo($stmt->rowCount(), 0);
    }

    /**
     * COMMIT the owned connection. A failure propagates so the combinator releases with `destroy=true`
     * (a commit that raised leaves the connection in an unknown state — it must not be reused as-is).
     */
    public function commit(): void
    {
        $this->pdo->exec('COMMIT');
    }

    /** ROLLBACK the owned connection (best-effort); a failure propagates ⇒ the combinator destroys it. */
    public function rollback(): void
    {
        $this->pdo->exec('ROLLBACK');
    }

    /**
     * Release the owned connection EXACTLY ONCE (idempotent). PHP's single `\PDO` is never dropped
     * mid-process (there is no pool to return to), so release RESETS the transactional state: if the
     * connection is `destroy`ed (a COMMIT/ROLLBACK that itself raised left an open tx) a best-effort
     * `ROLLBACK` clears it so the connection is not left poisoned with a dangling transaction — the
     * leak-guard the #78 audit fixed. Called by the combinator's `finally`; never inside
     * `commit`/`rollback`.
     */
    public function release(bool $destroy): void
    {
        if ($this->released) {
            return;
        }
        $this->released = true;
        $this->releaseCount++;
        $this->releasedDestroy = $destroy;
        if ($destroy) {
            // The connection may still be inside an open transaction (a raising commit/rollback).
            // Clear it best-effort so the single \PDO is not left poisoned for the rest of the process.
            try {
                $this->pdo->exec('ROLLBACK');
            } catch (\Throwable) {
                // Nothing more to do — the connection had no open tx (already committed/rolled back)
                // or is genuinely unusable; either way this is the single release point.
            }
        }
    }
}

// ── Middleware chain (§4) — the hook point (empty in Phase A) ──────────────────

/**
 * The ordered middleware chain a ctx carries (§4). {@see wrap()} folds the middlewares around
 * `$next` (the connection-resolve + execute terminal). An EMPTY chain is a pure passthrough — `wrap`
 * returns `$next($sql, $params)` verbatim, so Phase A behavior is byte-identical. ONE shape serves
 * both the read (`Rows`) and write (`RunInfo`) seams. Phase A always constructs an empty chain; the
 * registration API + native middleware entries are Phase D (this is only the hook point).
 */
final class MiddlewareChain
{
    /** @var list<callable(string, list<mixed>, callable):mixed> */
    private array $stack;

    /**
     * @param list<callable(string, list<mixed>, callable):mixed> $stack
     */
    public function __construct(array $stack = [])
    {
        $this->stack = array_values($stack);
    }

    /** Is the chain empty (⇒ `wrap` is a guaranteed passthrough)? */
    public function isEmpty(): bool
    {
        return count($this->stack) === 0;
    }

    /**
     * Fold the chain around `$next`, then invoke it. Empty ⇒ `$next($sql, $params)` verbatim.
     *
     * @param list<mixed> $params
     * @param callable(string, list<mixed>):mixed $next
     */
    public function wrap(string $sql, array $params, callable $next): mixed
    {
        if (count($this->stack) === 0) {
            return $next($sql, $params);
        }
        $fn = $next;
        for ($i = count($this->stack) - 1; $i >= 0; $i--) {
            $mw = $this->stack[$i];
            $inner = $fn;
            $fn = static fn (string $s, array $p): mixed => $mw($s, $p, $inner);
        }
        return $fn($sql, $params);
    }
}

// ── The ExecutionContext (§2 / §5) — ONE interface ────────────────────────────

/**
 * The execution context threaded through `Runtime::executeBundle` / `StaticBundle::executeReadGraph` /
 * the relation walker / `WriteRuntime::executeTransaction` in place of a raw `\PDO`. It carries the
 * connection provider (the primary driver + an optional pinned tx connection), the middleware chain,
 * and derives a tx-scoped ctx via {@see withConnection()}.
 *
 * ctx propagation (§3) is via an **explicit argument** — PHP has no AsyncLocalStorage / contextvars,
 * so the ctx is threaded down every call rather than stored ambiently. A transaction pins its owned
 * connection into a derived ctx and passes THAT ctx to the body.
 *
 * NOT `final`: `connectionFor` is the sanctioned extension point — B/C/D subclass it to add
 * reader/writer split (§3-2/3) + named-DB routing (§3-4) without touching the seam, and the #79
 * atomicity mutation proof subclasses it to mis-route a tx write (proving the ownership assertion has
 * teeth). Phase A's own resolution is the single-DB / tx-owned case below.
 */
class ExecutionContext
{
    /**
     * @param Connection|null $pinned the pinned tx connection (present ⇒ tx-scoped ctx; every statement resolves it).
     * @param bool $readOnly the READ-ONLY marker (Phase B / #85 write=tx guard — mirror v1 `withWriter` /
     *        the TS `withReadOnly` ALS marker / rust/go/py `read_only`): a write in a read-only-scoped ctx
     *        is REJECTED ({@see WriteInReadOnlyContextError}). Derived via {@see withReadOnly()}.
     */
    public function __construct(
        private readonly PdoDriver $driver,
        public readonly MiddlewareChain $middleware,
        private readonly ?Connection $pinned = null,
        private readonly bool $readOnly = false,
    ) {
    }

    /** The primary driver (the non-tx provider; the tx path never fans out on PHP's single \PDO). */
    public function driver(): PdoDriver
    {
        return $this->driver;
    }

    /** Is this a tx-scoped ctx (a pinned connection is present)? */
    public function inTransaction(): bool
    {
        return $this->pinned !== null;
    }

    /**
     * Is this a READ-ONLY-scoped ctx (Phase B / #85 write=tx guard)? A write here is REJECTED
     * ({@see WriteInReadOnlyContextError}). Derived via {@see withReadOnly()}.
     */
    public function readOnly(): bool
    {
        return $this->readOnly;
    }

    /**
     * Derive a READ-ONLY-scoped ctx (mirror v1 `withWriter` / the TS `withReadOnly` / rust/go/py
     * `with_read_only`): reads are allowed, but ANY write funneled through the GUARDED write seam
     * ({@see runGuarded()} / a guarded `executeTransactionBundle`) is rejected with
     * {@see WriteInReadOnlyContextError}. A tx-scoped ctx INHERITS its pinned connection + driver +
     * middleware; a transaction() opened inside a read-only scope stays read-only (v1 parity).
     */
    public function withReadOnly(): ExecutionContext
    {
        return new ExecutionContext($this->driver, $this->middleware, $this->pinned, true);
    }

    /**
     * Resolve WHICH connection a statement runs on (§3). Phase A resolution: the tx-owned (pinned)
     * connection wins; else the primary driver's connection. Reader/writer split (§3-2/3) + named-DB
     * routing (§3-4) extend HERE in B/C/D — the seam does not change.
     */
    public function connectionFor(StatementIntent $intent): Connection
    {
        return $this->pinned ?? $this->driver->connection();
    }

    /**
     * Derive a tx-scoped ctx pinning `$conn` (every statement resolves it while `$tx` is true). The
     * derived ctx shares the primary driver + middleware chain. The PHP analogue of the TS
     * `withConnection(conn, tx)` / go `WithTxConnection` / rust `with_tx_connection` / python
     * `with_connection`.
     */
    public function withConnection(Connection $conn, bool $tx): ExecutionContext
    {
        // A tx-scoped ctx INHERITS the read-only marker: a transaction() opened inside a read-only
        // scope is still read-only (v1 parity — withWriter reads never mutate).
        return new ExecutionContext($this->driver, $this->middleware, $tx ? $conn : null, $this->readOnly);
    }
}

/**
 * What a statement needs from the connection provider (§3): whether it writes (so it must go to a
 * writer / the tx-owned connection, never a read replica) and an optional named DB (multi-DB routing,
 * Phase B). Phase A resolves only `write` (tx-owned vs. primary) and ignores `db` (single DB); the
 * field is in the contract now so B/C/D extend the resolver — not the seam.
 */
final class StatementIntent
{
    public function __construct(
        public readonly bool $write = false,
        public readonly ?string $db = null,
    ) {
    }

    /** A read intent (write=false, primary DB). */
    public static function read(): StatementIntent
    {
        return new StatementIntent(false);
    }

    /** A write intent (write=true, primary DB). */
    public static function write(): StatementIntent
    {
        return new StatementIntent(true);
    }
}

// ── The central seam (§2) — the ONLY place SQL meets a connection ─────────────

/**
 * Central READ seam: ① middleware chain, ② resolve the connection, ③ execute. Every read (primary
 * read node, relation batch, tx-body SELECT/RETURNING) funnels through here.
 *
 * @param list<mixed> $params
 * @return list<\stdClass>
 */
function execute(ExecutionContext $ctx, string $sql, array $params, ?StatementIntent $intent = null): array
{
    $intent ??= StatementIntent::read();
    $conn = $ctx->connectionFor($intent);
    /** @var list<\stdClass> */
    return $ctx->middleware->wrap($sql, $params, static fn (string $s, array $p): array => $conn->execute($s, $p));
}

/**
 * Central WRITE seam: ① middleware chain, ② resolve the connection, ③ run. Every write and every
 * tx-control statement (BEGIN/COMMIT/ROLLBACK on the non-tx driver path) funnels through here.
 *
 * @param list<mixed> $params
 */
function run(ExecutionContext $ctx, string $sql, array $params, ?StatementIntent $intent = null): RunInfo
{
    $intent ??= StatementIntent::write();
    $conn = $ctx->connectionFor($intent);
    /** @var RunInfo */
    return $ctx->middleware->wrap($sql, $params, static fn (string $s, array $p): RunInfo => $conn->run($s, $p));
}

// ── Backward-compat wrappers (§6) ──────────────────────────────────────────────

/**
 * Backward-compat factory (§6) + the raw-`\PDO`-or-ctx coercion the public entry points accept, so
 * every existing caller (conformance / livedb / bench / unit) that threads a raw `\PDO` keeps working
 * byte-identically while the ctx-threaded internals funnel every SQL through the seam.
 */
final class Context
{
    /** The shared Phase A empty (passthrough) middleware chain. Phase D swaps in a per-ctx chain. */
    private static ?MiddlewareChain $emptyChain = null;

    private static function empty(): MiddlewareChain
    {
        return self::$emptyChain ??= new MiddlewareChain();
    }

    /**
     * **Backward-compat wrapper (§6).** Wrap a raw `\PDO` in a thin {@see ExecutionContext}: reader =
     * writer = the same PDO, an EMPTY middleware chain, a single DB, no pinned tx connection. Existing
     * callers keep working **byte-identically** — the seam is a pure passthrough to
     * `$pdo->prepare(...)->execute()`. The PHP analogue of the TS `contextForDriver` / rust
     * `for_driver` / go `ContextForDB` / python `context_for_driver`.
     */
    public static function forPdo(\PDO $pdo): ExecutionContext
    {
        return new ExecutionContext(new PdoDriver($pdo), self::empty(), null);
    }

    /**
     * Accept EITHER a raw `\PDO` (wrap it via {@see forPdo()} — the byte-identical backward-compat
     * path) OR an already-built {@see ExecutionContext} (pass through). The public runtime entry
     * points (`executeBundle` / `executeTransactionBundle` / `runRelationOp` / `readBundle`) take this
     * union so every existing raw-`\PDO` caller keeps working while the internals thread a ctx.
     */
    public static function of(\PDO|ExecutionContext $pdoOrCtx): ExecutionContext
    {
        return $pdoOrCtx instanceof ExecutionContext ? $pdoOrCtx : self::forPdo($pdoOrCtx);
    }
}

// ── The per-execution-ownership transaction (§3) — the concurrent-tx fix ──────

/**
 * The body's decision about how to end the transaction — so a body can legitimately ROLLBACK and
 * STILL return a value (the gate short-circuit: a failed gate rolls back but is NOT an error, it
 * returns `committed:false`). A thrown exception from the body always rolls back + re-raises.
 */
final class TxDecision
{
    public function __construct(
        public readonly bool $rollback,
        public readonly mixed $value,
    ) {
    }
}

/** The COMMIT decision (the tx's owned connection commits, then `$value` returns). */
function commit(mixed $value): TxDecision
{
    return new TxDecision(false, $value);
}

/**
 * The (non-error) ROLLBACK decision (the tx's owned connection rolls back, then `$value` returns — a
 * legitimate gate short-circuit).
 */
function rollbackWith(mixed $value): TxDecision
{
    return new TxDecision(true, $value);
}

/**
 * Run `$body` inside a transaction with **per-execution connection ownership** (§3, the concurrent-tx
 * fix). The general form: `$body` decides COMMIT vs ROLLBACK (see {@see TxDecision}); a thrown
 * exception from `$body` always rolls back and re-raises.
 *
 *   1. acquire ONE connection via {@see PdoDriver::beginTx()} — a {@see PdoTxConnection} (the tx's
 *      exclusive connection; BEGIN issued on it), the PHP analogue of v1 `PoolTransaction`;
 *   2. pin it into a tx-scoped {@see ExecutionContext} passed EXPLICITLY to `$body` so EVERY statement
 *      `$body` issues resolves THAT connection via the seam — never a fresh one;
 *   3. run `$body($txCtx)` → COMMIT / ROLLBACK on the OWNED connection per the returned decision; on
 *      any thrown exception ROLLBACK (best-effort) and re-raise;
 *   4. **release the owned connection EXACTLY ONCE in a `finally`** (the SOLE releaser — the
 *      {@see PdoTxConnection} never self-releases). It is reset on the clean paths, and cleared
 *      (best-effort ROLLBACK) when poisoned — a body error, OR a COMMIT/ROLLBACK that itself threw
 *      (rare but real: a deferred-constraint violation at COMMIT). Without this `finally` a throwing
 *      COMMIT would leave the single `\PDO` stuck inside an open transaction — the #78 audit defect.
 *
 * `$before` / `$after` carry the per-transaction isolation prelude (Phase B / #85 —
 * {@see \LiteDbModel\Runtime\isolationPrelude()}): MySQL's `SET` runs pre-BEGIN, PG's post-BEGIN.
 * Empty ⇒ a bare `BEGIN` (byte-identical to the Phase A path).
 *
 * The tx-scoped ctx is ALSO pinned as the AMBIENT context ({@see TxAmbient}) for the duration of
 * `$body` — so an operation issued INSIDE the body (a nested `executeTransactionBundle` whose caller
 * threads only the raw `\PDO`) still detects the tx via {@see currentContext()} and JOINs it. This
 * is the PHP analogue of the TS `txContext.run` async-local / python's `run_with_pinned_context`
 * (PHP is 1-req-1-process, so a process-scoped holder is the honest single-threaded equivalent).
 *
 * @param callable(ExecutionContext):TxDecision $body
 * @param list<string> $before isolation statements run BEFORE `BEGIN` (MySQL SET)
 * @param list<string> $after  isolation statements run AFTER `BEGIN` (PG SET)
 */
function withTransactionDecided(ExecutionContext $ctx, callable $body, array $before = [], array $after = []): mixed
{
    $tx = $ctx->driver()->beginTx($before, $after);
    $txCtx = $ctx->withConnection(new TxConnectionAdapter($tx), true);

    // `destroy` starts true: the connection is only proven clean once a COMMIT/ROLLBACK completes
    // without throwing. ANY failure below (body error, or a commit/rollback that itself throws)
    // leaves it true ⇒ the finally clears the poisoned connection instead of leaving an open tx.
    $destroy = true;
    // Pin the tx-scoped ctx as the AMBIENT context for the body's duration (restored on exit) so an
    // operation issued inside `$body` that only has the raw \PDO still JOINs this tx via currentContext().
    $prevAmbient = TxAmbient::set($txCtx);
    try {
        try {
            $decision = $body($txCtx);
        } catch (\Throwable $e) {
            // A body error rolls back (BEST-EFFORT) then re-raises the ORIGINAL failure. A rollback
            // that itself throws must NOT mask the body error — swallow it but keep destroy=true so the
            // finally clears the connection. A clean rollback ⇒ the connection is proven clean.
            try {
                $tx->rollback();
                $destroy = false;
            } catch (\Throwable) {
                // poisoned; destroy stays true. The original body error surfaces via the re-throw.
            }
            throw $e;
        }
        if ($decision->rollback) {
            // A legitimate non-error rollback (e.g. a gate short-circuit): roll back, return the value.
            $tx->rollback();
            $destroy = false;
            return $decision->value;
        }
        $tx->commit();
        $destroy = false;
        return $decision->value;
    } finally {
        // Restore the prior ambient BEFORE releasing (symmetric unwinding), then the SINGLE release
        // point — runs on every path (success, body error, throwing commit/rollback).
        TxAmbient::restore($prevAmbient);
        $tx->release($destroy);
    }
}

// ── ctx propagation (§3) — the PHP-idiomatic ambient holder ────────────────────

/**
 * The AMBIENT (process-scoped) tx {@see ExecutionContext} holder — the PHP analogue of the TS
 * `AsyncLocalStorage` / python's `contextvars.ContextVar` (§3 table). PHP is 1-request-1-process
 * (no threads, no async continuation), so a plain process-scoped slot is the honest, race-free
 * equivalent: only ONE transaction can be live on the single `\PDO` at a time, and there is no
 * concurrent execution scope to leak into. {@see withTransactionDecided()} pins the tx-scoped ctx
 * here for the body's duration + restores the prior value on exit; the write guard + the ambient-tx
 * JOIN read it via {@see currentContext()}. Outside a tx the slot is `null`.
 */
final class TxAmbient
{
    private static ?ExecutionContext $current = null;

    /** Pin `$ctx` as the ambient tx ctx; return the PRIOR value (for symmetric restore). */
    public static function set(?ExecutionContext $ctx): ?ExecutionContext
    {
        $prev = self::$current;
        self::$current = $ctx;
        return $prev;
    }

    /** Restore a previously-{@see set()} ambient value (the `finally`-side unwind). */
    public static function restore(?ExecutionContext $prev): void
    {
        self::$current = $prev;
    }

    /** The ambient tx ctx of THIS process, or `null` outside a pinned tx scope. */
    public static function current(): ?ExecutionContext
    {
        return self::$current;
    }
}

/**
 * The ambient (process-propagated) {@see ExecutionContext} of THIS execution scope, or `null` outside
 * a pinned tx scope. The seam consults it so a callee that only has the raw `\PDO` still resolves the
 * tx-owned connection when it runs inside a {@see withTransactionDecided()} body. The PHP analogue of
 * python's `current_context()`.
 */
function currentContext(): ?ExecutionContext
{
    return TxAmbient::current();
}

/**
 * Run `$fn` with `$ctx` pinned as the ambient tx ctx for THIS scope (restored on exit). Every implicit
 * {@see currentContext()} inside `$fn` returns `$ctx`. The PHP analogue of python's
 * `run_with_pinned_context` — used by the read-only guard test to pin a derived read-only ctx around
 * a nested write.
 *
 * @param callable():mixed $fn
 */
function runWithPinnedContext(ExecutionContext $ctx, callable $fn): mixed
{
    $prev = TxAmbient::set($ctx);
    try {
        return $fn();
    } finally {
        TxAmbient::restore($prev);
    }
}

// ── The write=tx GUARD seam (Phase B / #85) ────────────────────────────────────

/** Is THIS execution scope inside an active transaction? Reads the AMBIENT-propagated ctx. */
function ambientInTransaction(): bool
{
    $ambient = currentContext();
    return $ambient !== null && $ambient->inTransaction();
}

/** Is THIS execution scope a READ-ONLY context? Reads the AMBIENT-propagated ctx's read-only marker. */
function ambientReadOnly(): bool
{
    $ambient = currentContext();
    return $ambient !== null && $ambient->readOnly();
}

/**
 * Enforce the write=tx guard against the AMBIENT ({@see TxAmbient})-propagated tx/read-only markers
 * (mirror v1 `_checkWriteAllowed` / the TS `checkWriteAllowed`). A write in a read-only scope →
 * {@see WriteInReadOnlyContextError}; a write with NO active transaction →
 * {@see WriteOutsideTransactionError}. Read-only is checked FIRST (v1 order). The PHP port reads the
 * guard state from the ambient holder (not an explicit ctx arg) so a bare model-level write — which
 * only has the raw `\PDO` — still sees the caller's {@see transaction()} scope.
 */
function checkWriteAllowedAmbient(string $operation, ?string $model = null): void
{
    checkWriteAllowed($operation, $model, ambientInTransaction(), ambientReadOnly());
}

/**
 * GUARDED write seam (mirror the TS `runGuarded` / go `RunGuarded` / py `run_guarded`): enforce the
 * write=tx guard ({@see checkWriteAllowedAmbient()}) for a DATA-mutating statement, then delegate to
 * {@see run()}. A write issued OUTSIDE a {@see transaction()} throws {@see WriteOutsideTransactionError};
 * a write in a read-only scope throws {@see WriteInReadOnlyContextError}. Tx-control statements
 * (BEGIN/COMMIT/ROLLBACK/SET) are NOT guarded — the tx runtime issues them to OPEN the very scope the
 * guard checks.
 *
 * @param list<mixed> $params
 */
function runGuarded(ExecutionContext $ctx, string $sql, array $params, string $operation, ?string $model = null): RunInfo
{
    checkWriteAllowedAmbient($operation, $model);
    return run($ctx, $sql, $params, StatementIntent::write());
}

// ── The PUBLIC user-controlled transaction boundary (Phase B-core / #86, PHP) ──

/**
 * **The public user-controlled transaction boundary** (#85, PHP port of the TS `transaction` / rust
 * `transaction` / go `Transaction` / python `transaction`) — the REAL transaction feature v2 was
 * missing. `transaction($ctx, $fn, $options?, $dialectName?)` opens ONE boundary the caller wraps
 * around MULTIPLE arbitrary operations so they commit or roll back TOGETHER:
 *
 * ```php
 * transaction($ctx, function () {
 *     Runtime::executeTransactionBundle($aBundle, $aInput, $aPdo);  // ← every op inside JOINS this
 *     Runtime::executeTransactionBundle($bBundle, $bInput, $bPdo);  //    ONE boundary: one conn,
 * }, new TransactionOptions(isolation: IsolationLevel::Serializable));  //  one BEGIN…COMMIT, all-or-nothing
 * ```
 *
 * ## What it does (v1 `DBModel.transaction` :2787 parity, on the SCP seam)
 *
 * It acquires ONE owned connection ({@see PdoDriver::beginTx()} with the isolation prelude), pins it
 * into a tx-scoped {@see ExecutionContext} propagated via the AMBIENT holder ({@see TxAmbient}), runs
 * `$fn`, then COMMITs (or ROLLBACKs on a body error / `$options->rollbackOnly`), with the #81 retry
 * loop (deadlock / serialization / connection error) wrapped around the WHOLE boundary — a FRESH
 * owned connection per attempt.
 *
 * ## The ambient-tx JOIN — how operations participate (the core #86 fix; PHP = ambient holder)
 *
 * `$fn` takes NO connection argument. Instead the pinned tx ctx lives in the ambient holder
 * ({@see currentContext()}). Every operation `$fn` issues — a live-DB write via
 * `executeTransactionBundle`, a read via `executeBundle` — detects that ambient pinned ctx and runs
 * its statements on THAT connection **without opening its own BEGIN/COMMIT** (the nested-join). So N
 * operations inside one `transaction($fn)` produce exactly ONE BEGIN + ONE COMMIT on ONE connection.
 * Outside a `transaction($fn)` the ambient pin is absent, so a bare guarded write's guard fires
 * ({@see WriteOutsideTransactionError}).
 *
 * NESTED `transaction()` joins the outer (one physical BEGIN/COMMIT; an inner error rolls back the
 * WHOLE tx). Isolation/retry/rollbackOnly options on a nested call are IGNORED (the outer owns them).
 * Mirrors v1 `DBModel.transaction` :2794-2797.
 *
 * @param callable():mixed $fn
 */
function transaction(
    ExecutionContext $ctx,
    callable $fn,
    ?TransactionOptions $options = null,
    string $dialectName = 'postgres',
): mixed {
    $opts = $options ?? new TransactionOptions();

    // NESTED-TX JOIN (mirror v1 :2794): already inside a tx on this ambient scope ⇒ join the outer.
    // No new connection, no BEGIN/COMMIT — the inner body is part of the outer physical transaction.
    // Isolation/retry/rollbackOnly on a nested call are ignored: the outer owns the envelope.
    if (ambientInTransaction()) {
        return $fn();
    }

    // Validate + build the isolation prelude BEFORE acquiring a connection (fail-closed: SQLite + a
    // level is a hard error; an unsupported isolation must not open a tx it can't honor).
    [$before, $after] = isolationPrelude($dialectName, $opts->isolation);

    $retryLimit = 1;
    if ($opts->retryOnError) {
        $retryLimit = $opts->retryLimit >= 1 ? $opts->retryLimit : 1;
    }

    $attempt = 0;
    while (true) {
        $attempt++;
        // ONE attempt on a FRESH owned connection. `$fn` reads the pinned tx ctx from the ambient
        // holder (TxAmbient::set inside withTransactionDecided), so every op JOINs this one connection.
        $body = static function (ExecutionContext $txCtx) use ($fn, $opts): TxDecision {
            $value = $fn();
            // rollbackOnly (dry-run): ROLLBACK but still return the body value — no committed change.
            return $opts->rollbackOnly ? rollbackWith($value) : commit($value);
        };

        try {
            return withTransactionDecided($ctx, $body, $before, $after);
        } catch (WriteOutsideTransactionError | WriteInReadOnlyContextError $e) {
            // A guard rejection is a programming error, never retryable — re-throw immediately.
            throw $e;
        } catch (\Throwable $error) {
            // PARITY (go `mapSqliteError` → `SqlFailure.Unwrap()` classified by `IsRetryableTxError` /
            // rust / py): map a RAW \PDOException into the `SqlFailure` envelope so the retry classifier
            // reads the TYPED SQLSTATE/errno THROUGH `$wrapped`/getPrevious() — the SAME envelope
            // go/rust/py classify through. A live PG 40001 / MySQL 1213 (raised at COMMIT as a raw
            // \PDOException) thus flows through `SqlFailure::fromPdo` here, making the wrapped chain
            // genuinely load-bearing on the live retry path (neuter it → this classification goes RED).
            // An already-mapped `SqlFailure` (e.g. from a nested executeTransactionBundle) is left as-is.
            $failure = $error;
            if (!($failure instanceof SqlFailure) && $error instanceof \PDOException) {
                $failure = SqlFailure::fromPdo($error);
            }
            if ($attempt < $retryLimit && $opts->retryOnError && isRetryableTxError($failure)) {
                // Exponential backoff before RETRYing the whole transaction on a fresh connection.
                $backoffMs = $opts->retryDurationMs * (2 ** ($attempt - 1));
                if ($backoffMs > 0) {
                    usleep($backoffMs * 1000);
                }
                continue;
            }
            throw $failure;
        }
    }
}

/**
 * The simple form of {@see withTransactionDecided()}: `$body` returns a value ⇒ COMMIT + return it; a
 * thrown exception ⇒ ROLLBACK + re-raise. For a body that never legitimately rolls back with a value.
 *
 * @param callable(ExecutionContext):mixed $body
 */
function withTransaction(ExecutionContext $ctx, callable $body): mixed
{
    return withTransactionDecided($ctx, static fn (ExecutionContext $txCtx): TxDecision => commit($body($txCtx)));
}
