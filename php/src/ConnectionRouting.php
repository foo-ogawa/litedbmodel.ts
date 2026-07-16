<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — the **connection routing + config contract** (Phase C / #91, PHP).
 *
 * The PHP port of the Phase C **API REFERENCE** `src/scp/connection-routing.ts` (#87), mirroring the
 * rust port (#88), the go port (#89) and the python port (#90). It builds ON the Phase A
 * {@see ExecutionContext} seam and the Phase A/B owned-connection transaction runtime; it does NOT
 * re-implement the seam — it supplies the pieces the routing-aware {@see RoutingExecutionContext}
 * uses to complete `connectionFor(intent)`'s resolution (steps 2-4). The field names + defaults, the
 * connection-registry shape, `withWriter` semantics, and the `connectionFor(intent)` resolution ORDER
 * mirror the TS contract EXACTLY.
 *
 * ## The `connectionFor(intent)` resolution order (design §3, v1 `DBModel.ts:313` parity)
 *
 * A statement's connection is resolved in THIS priority (first match wins):
 *   1. **active tx connection** — inside a transaction, always the tx-owned connection (Phase A/B,
 *      resolved by the base {@see ExecutionContext} BEFORE routing, so a named-DB tx runs entirely on
 *      ONE pinned writer conn — Phase B is NOT broken).
 *   2. **writer scope / writer-sticky** — inside {@see withWriter}, or within `writerStickyDuration`
 *      after a transaction (read-your-writes), a READ goes to the WRITER pool (Phase C — here).
 *   3. **read=reader / write=writer** — otherwise a read goes to the reader pool, a write to the
 *      writer pool (reader/writer separation; single-pool config ⇒ reader === writer).
 *   4. **named-DB routing** — the target pool is selected by `intent.db` (the connection NAME the
 *      bundle/model metadata carries — decorator-free; Phase F wires decorators) against the
 *      {@see ConnectionRegistry}; absent ⇒ the DEFAULT connection. Named-DB selection happens FIRST
 *      (it picks WHICH connection's reader/writer split steps 2-3 then apply to).
 *
 * ## Backward-compat (the hard constraint)
 *
 * Single DB, `reader === writer` (one pool), empty config, unnamed connection ⇒ BYTE-IDENTICAL to the
 * Phase A/B single-PDO behavior. The existing {@see Context::forPdo()} / {@see Context::of()} path is
 * UNCHANGED — routing is an OPT-IN extension via {@see setConfig()} / a {@see RoutingConfig}. A
 * registry built from ONE pool routes every intent to that one pool, and the writer-sticky clock only
 * ever diverts to a pool that is the SAME object — so nothing observable changes.
 *
 * ## PHP "pool" honesty — what pool sizing / keepAlive map to (READ THIS)
 *
 * The TS reference targets pg/mysql2 pools with an in-process min/max connection pool. **PHP is
 * 1-request-1-process**: there is no long-lived, shared, in-process min/max pool. `\PDO` offers
 * `PDO::ATTR_PERSISTENT` (a per-worker connection cached across requests by the SAPI), but NOT an
 * in-process concurrent pool the way pg/mysql2 do. So:
 *
 *   - **queryTimeout** — REAL. Applied as a session `statement_timeout` (PG) / `max_execution_time`
 *     (MySQL) so a runaway query is aborted by the SERVER. Fully honored ({@see ConfiguredPdoPool}).
 *   - **searchPath / charset** — REAL. Applied as session statements on checkout WITH reset on release
 *     (no session leak if a persistent PDO is reused within/across requests). Fully honored.
 *   - **connection routing / reader-writer selection / named-DB / withWriter / writer-sticky /
 *     closeAllPools** — REAL. Independent of pool sizing.
 *   - **keepAlive** — REAL where meaningful: {@see PDO::ATTR_PERSISTENT} (a keep-open persistent
 *     connection) is the PHP analogue of a warm pooled connection; the {@see PdoPoolFactory} maps
 *     `keepAlive` → `ATTR_PERSISTENT`. There is no per-probe TCP keepalive delay in `\PDO`, so
 *     `keepAliveInitialDelayMillis` has **no meaning in PHP and is IGNORED** (documented N/A).
 *   - **minPool / maxPool** — **N/A in PHP** (documented, not faked). PHP has no in-process
 *     concurrent connection pool to cap: one process holds ONE active `\PDO`, so there is no
 *     concurrency-cap to enforce (unlike pg/mysql2, where N async siblings hold N pooled connections).
 *     The fields are ACCEPTED (so the {@see ConnectionConfig} shape matches the other ports for
 *     cross-lang parity) and their resolved defaults are carried, but they DO NOT construct or bound
 *     anything — and this file does not pretend they do. A `maxPool` concurrency-cap test (like the TS
 *     one) is therefore NOT ported: PHP's execution model cannot support it, and faking it would be a
 *     lie. {@see PdoPoolFactory} does NOT read `minPool`/`maxPool` — they are inert by construction,
 *     not by a silently-dropped knob.
 */

// ── The runtime config (C3) — mirrors the TS ConnectionConfig / v1 DBConfig ────────

/**
 * Per-connection database config (C3) — the knobs a pool is built with. Mirrors the TS
 * {@see ConnectionConfig} field names + defaults EXACTLY (the cross-lang DATA contract). Every field
 * is optional with a documented default. See the file docblock for which knobs are REAL in PHP and
 * which are documented-N/A (`minPool`/`maxPool` = N/A; `keepAliveInitialDelayMillis` = N/A).
 */
final class ConnectionConfig
{
    /**
     * @param 'postgres'|'mysql'|'sqlite' $driver Driver dialect for this connection. @default 'postgres'
     * @param ?string $host DB host (server-based dialects).
     * @param ?int $port DB port.
     * @param ?string $database DB name (or file path for sqlite).
     * @param ?string $user Username.
     * @param ?string $password Password.
     * @param int $queryTimeout Per-statement timeout in MILLISECONDS (server-side statement_timeout /
     *        max_execution_time). 0 ⇒ no statement timeout. @default 0
     * @param bool $keepAlive Keep the connection warm (PHP: {@see PDO::ATTR_PERSISTENT}). @default false
     * @param int $keepAliveInitialDelayMillis N/A in PHP (no per-probe TCP keepalive delay in \PDO).
     *        Accepted for cross-lang parity; IGNORED. @default 10000
     * @param int $minPool N/A in PHP (no in-process connection pool). Accepted for parity; INERT. @default 0
     * @param int $maxPool N/A in PHP (no in-process connection pool). Accepted for parity; INERT. @default 10
     * @param ?string $searchPath PG `search_path` set as a session statement on checkout (schema routing).
     * @param ?string $charset MySQL connection charset / PG client_encoding set on each session.
     */
    public function __construct(
        public readonly string $driver = 'postgres',
        public readonly ?string $host = null,
        public readonly ?int $port = null,
        public readonly ?string $database = null,
        public readonly ?string $user = null,
        public readonly ?string $password = null,
        public readonly int $queryTimeout = 0,
        public readonly bool $keepAlive = false,
        public readonly int $keepAliveInitialDelayMillis = 10000,
        public readonly int $minPool = 0,
        public readonly int $maxPool = 10,
        public readonly ?string $searchPath = null,
        public readonly ?string $charset = null,
    ) {
    }
}

/**
 * The resolved (defaults-applied) config the pool builder consumes — the PHP analogue of the TS
 * {@see ResolvedConnectionConfig}. In PHP {@see ConnectionConfig} already carries non-null defaults on
 * every knob, so {@see resolveConnectionConfig()} is (nearly) the identity — it exists so the ports'
 * public surface matches (a `resolve` step + a resolved type) and to make the "defaults applied"
 * boundary explicit.
 */
final class ResolvedConnectionConfig
{
    /**
     * @param 'postgres'|'mysql'|'sqlite' $driver
     */
    public function __construct(
        public readonly string $driver,
        public readonly ?string $host,
        public readonly ?int $port,
        public readonly ?string $database,
        public readonly ?string $user,
        public readonly ?string $password,
        public readonly int $queryTimeout,
        public readonly bool $keepAlive,
        public readonly int $keepAliveInitialDelayMillis,
        public readonly int $minPool,
        public readonly int $maxPool,
        public readonly ?string $searchPath,
        public readonly ?string $charset,
    ) {
    }
}

/** Apply the C3 defaults (queryTimeout=0, keepAlive=false, minPool=0, maxPool=10). */
function resolveConnectionConfig(?ConnectionConfig $config = null): ResolvedConnectionConfig
{
    $c = $config ?? new ConnectionConfig();
    return new ResolvedConnectionConfig(
        driver: $c->driver,
        host: $c->host,
        port: $c->port,
        database: $c->database,
        user: $c->user,
        password: $c->password,
        queryTimeout: $c->queryTimeout,
        keepAlive: $c->keepAlive,
        keepAliveInitialDelayMillis: $c->keepAliveInitialDelayMillis,
        minPool: $c->minPool,
        maxPool: $c->maxPool,
        searchPath: $c->searchPath,
        charset: $c->charset,
    );
}

/**
 * The SESSION statements a connection must run at checkout to honor a {@see ResolvedConnectionConfig}
 * (issued once per acquired connection, in order). This is the per-dialect mapping, pure (no
 * connection contact) so it is testable in isolation — mirrors the TS `sessionStatements`:
 *
 *   - **statement timeout** (`queryTimeout` > 0): PG `SET statement_timeout = <ms>`; MySQL
 *     `SET SESSION max_execution_time = <ms>` (both server-side, ms).
 *   - **searchPath**: PG `SET search_path TO <path>`; MySQL has no schema search path ⇒ ignored.
 *   - **charset**: MySQL `SET NAMES <charset>`; PG `SET client_encoding TO <charset>`.
 *
 * A key with no value emits nothing (⇒ empty array for an all-default config ⇒ the session is
 * untouched, backward-compatible). sqlite has no server session ⇒ empty.
 *
 * @return list<string>
 */
function sessionStatements(ResolvedConnectionConfig $config): array
{
    $out = [];
    $dialect = $config->driver;
    if ($dialect === 'sqlite') {
        return $out;
    }
    if ($config->queryTimeout > 0) {
        $out[] = $dialect === 'postgres'
            ? "SET statement_timeout = {$config->queryTimeout}"
            : "SET SESSION max_execution_time = {$config->queryTimeout}";
    }
    if ($config->searchPath !== null && $dialect === 'postgres') {
        $out[] = "SET search_path TO {$config->searchPath}";
    }
    if ($config->charset !== null) {
        $out[] = $dialect === 'mysql'
            ? "SET NAMES {$config->charset}"
            : "SET client_encoding TO {$config->charset}";
    }
    return $out;
}

/**
 * The RESET statements that undo {@see sessionStatements()} on release (per dialect), so a session
 * knob (`statement_timeout` / `search_path` / `client_encoding` / `max_execution_time` / charset) set
 * for THIS configured connection does NOT leak to the next caller that draws the SAME underlying
 * connection. In PHP the SAME persistent `\PDO` is reused within (and, with ATTR_PERSISTENT, across)
 * requests, so `RESET` / `SET … DEFAULT` restores the server default and prevents a session leak. Only
 * the knobs `config` actually set are reset (an all-default config ⇒ nothing to reset). Mirrors the TS
 * `sessionResetStatements`.
 *
 * @return list<string>
 */
function sessionResetStatements(ResolvedConnectionConfig $config): array
{
    $out = [];
    $dialect = $config->driver;
    if ($dialect === 'sqlite') {
        return $out;
    }
    if ($config->queryTimeout > 0) {
        $out[] = $dialect === 'postgres' ? 'RESET statement_timeout' : 'SET SESSION max_execution_time = DEFAULT';
    }
    if ($config->searchPath !== null && $dialect === 'postgres') {
        $out[] = 'RESET search_path';
    }
    if ($config->charset !== null) {
        $out[] = $dialect === 'mysql' ? 'SET NAMES DEFAULT' : 'RESET client_encoding';
    }
    return $out;
}

// ── The PHP "pool" (a \PDO holder) — the AsyncConnectionPool analogue ──────────────

/**
 * The PHP analogue of the TS {@see AsyncConnectionPool}: a provider that hands out a {@see Connection}
 * for a statement + applies/undoes the per-connection {@see ResolvedConnectionConfig} session knobs.
 *
 * PHP is 1-request-1-process with a single `\PDO` per connection target — there is NO in-process
 * concurrent connection pool (see the file docblock). So a "pool" here is a holder over ONE `\PDO`:
 * `acquire()` returns the {@see Connection} view (applying the session statements when the connection
 * is realized within this request), and `release()` runs the RESET statements so a reused (persistent)
 * `\PDO` does not leak this config's session state. The routing ctx calls `acquire()` per statement; a
 * poisoned (timed-out) connection is dropped (`release(destroy: true)`), matching the TS
 * `configuredPool` release semantics.
 */
interface PdoPool
{
    /**
     * Check out a {@see Connection} for a statement. Applies the config's {@see sessionStatements()}
     * on the `\PDO` (so the statement runs under this connection's configured session).
     */
    public function acquire(): Connection;

    /**
     * Return a connection. `$destroy` ⇒ the connection is poisoned (e.g. a fired statement timeout
     * aborted it) and the reset is SKIPPED; otherwise the config's {@see sessionResetStatements()} run
     * so the connection's session state does not leak to the next caller (persistent-PDO reuse).
     */
    public function release(Connection $conn, bool $destroy = false): void;

    /** Close the underlying connection(s) — the {@see PoolCloser} target. */
    public function close(): void;

    /** The connection's driver dialect (so the tx runtime picks the isolation prelude for the right engine). */
    public function driver(): string;

    /**
     * The backing {@see PdoDriver} (the ONE `\PDO` this pool holds). A transaction acquires its OWNED
     * connection by calling {@see PdoDriver::beginTx()} on THIS — so a named-DB tx runs entirely on the
     * target connection's writer `\PDO` (the tx-pin STILL wins over routing; Phase B is not broken).
     */
    public function backingDriver(): PdoDriver;
}

/**
 * A {@see PdoPool} over a single {@see PdoDriver} (one `\PDO`). This is where C3's session knobs
 * (`queryTimeout`/`searchPath`/`charset`) become REAL per-server effects: on `acquire()` the session
 * statements run on the `\PDO`; on a clean `release()` the reset statements restore the defaults so a
 * reused persistent `\PDO` never leaks this config's state. An all-default config ⇒ ZERO extra
 * statements ⇒ a transparent passthrough (byte-identical to the Phase A/B path).
 *
 * If a session statement itself fails (e.g. an invalid search_path), the checkout throws and the
 * connection is left unconfigured (the config never partially applied). On release, a poisoned
 * connection (`$destroy = true`) SKIPS the reset (a reset on an aborted-by-timeout connection would
 * itself fail); a clean connection is reset. This mirrors the TS {@see configuredPool} exactly.
 */
final class ConfiguredPdoPool implements PdoPool
{
    /** @var list<string> the session statements to apply on checkout (empty ⇒ passthrough). */
    private readonly array $session;
    /** @var list<string> the reset statements to run on a clean release. */
    private readonly array $reset;

    public function __construct(
        private readonly PdoDriver $backing,
        private readonly ResolvedConnectionConfig $config,
    ) {
        $this->session = sessionStatements($config);
        $this->reset = sessionResetStatements($config);
    }

    public function acquire(): Connection
    {
        $pdo = $this->backing->pdo();
        foreach ($this->session as $stmt) {
            // A failed session setup leaves the connection unconfigured; the throw propagates so a
            // mis-configured connection is never used with a half-applied config.
            $pdo->exec($stmt);
        }
        return $this->backing->connection();
    }

    public function release(Connection $conn, bool $destroy = false): void
    {
        if ($destroy) {
            // A poisoned/aborted connection: skip the reset (it would itself fail on an aborted conn).
            return;
        }
        $pdo = $this->backing->pdo();
        foreach ($this->reset as $stmt) {
            $pdo->exec($stmt);
        }
    }

    public function close(): void
    {
        // \PDO has no explicit close; dropping the last reference closes it — the PoolCloser drops the
        // driver ref (setConfig's close()). Nothing to do here.
    }

    public function driver(): string
    {
        return $this->config->driver;
    }

    public function backingDriver(): PdoDriver
    {
        return $this->backing;
    }
}

/**
 * A trivial {@see PdoPool} over a {@see PdoDriver} with NO session config (the backward-compat /
 * unconfigured path): `acquire()` is a plain connection handout, `release()` is a no-op. Used for a
 * pool wrapped around a raw `\PDO` with an all-default config, and by
 * {@see ConnectionRegistry::singleDefault()} when built from a bare {@see PdoDriver}.
 */
final class PlainPdoPool implements PdoPool
{
    public function __construct(
        private readonly PdoDriver $backing,
        private readonly string $driver = 'postgres',
    ) {
    }

    public function acquire(): Connection
    {
        return $this->backing->connection();
    }

    public function release(Connection $conn, bool $destroy = false): void
    {
        // no-op: no session config to reset.
    }

    public function close(): void
    {
    }

    public function driver(): string
    {
        return $this->driver;
    }

    public function backingDriver(): PdoDriver
    {
        return $this->backing;
    }
}

// ── Reader/writer pool pair (C1) ───────────────────────────────────────────────────

/**
 * A reader/writer pool PAIR for ONE named connection (C1). `reader` serves read-intent statements;
 * `writer` serves write-intent statements, `withWriter` reads, and writer-sticky reads. When a
 * connection has no separate replica, `reader === writer` is the SAME {@see PdoPool} object — routing
 * then always lands on that one pool (the single-pool backward-compat case). Mirrors the TS
 * {@see ReaderWriterPools}.
 */
final class ReaderWriterPools
{
    public function __construct(
        public readonly PdoPool $reader,
        public readonly PdoPool $writer,
    ) {
    }

    /** Build a pair where reader === writer (single-pool, backward-compat). */
    public static function single(PdoPool $pool): ReaderWriterPools
    {
        return new ReaderWriterPools($pool, $pool);
    }
}

// ── The connection registry (C2) — name → reader/writer pools ──────────────────────

/** The reserved name of the DEFAULT (unnamed) connection. An `intent.db` of `null` uses this. */
const DEFAULT_CONNECTION = 'default';

/**
 * The multi-DB connection registry (C2): a map from a connection NAME → its {@see ReaderWriterPools}.
 * The routing ctx selects the pair by `intent.db` (the connection name the bundle/model metadata
 * carries — decorator-free; Phase F wires decorators), falling back to {@see DEFAULT_CONNECTION} when
 * unnamed. Selecting a name that was never registered is a LOUD error (a real wiring bug — never a
 * silent default fallback, which would run a query on the wrong DB; mirrors the TS
 * {@see ConnectionRegistry} + the V0 cross-DB relation registry's loud-fail policy).
 *
 * A single-DB deployment registers exactly one connection under {@see DEFAULT_CONNECTION} with
 * `reader === writer` ⇒ every intent routes to that one pool ⇒ byte-identical to Phase A/B.
 */
final class ConnectionRegistry
{
    /** @var array<string, ReaderWriterPools> name → reader/writer pools. */
    private readonly array $connections;

    /**
     * @param array<string, ReaderWriterPools> $connections
     */
    public function __construct(array $connections)
    {
        $this->connections = $connections;
    }

    /**
     * Build a registry from ONE pool as the default connection (reader === writer). The backward-compat
     * path: a {@see RoutingExecutionContext} built from a single pool wraps it here so its
     * `connectionFor` routes every intent to that one pool.
     */
    public static function singleDefault(PdoPool $pool): ConnectionRegistry
    {
        return new ConnectionRegistry([DEFAULT_CONNECTION => ReaderWriterPools::single($pool)]);
    }

    /** Fluent builder: start from a default connection's pools, then `->add(name, pools)` more. */
    public static function fromDefault(ReaderWriterPools $pools): ConnectionRegistryBuilder
    {
        return (new ConnectionRegistryBuilder())->add(DEFAULT_CONNECTION, $pools);
    }

    /** The reader/writer pair for `$name` (or {@see DEFAULT_CONNECTION} when `null`). Loud on a missing name. */
    public function pairFor(?string $name): ReaderWriterPools
    {
        $key = $name ?? DEFAULT_CONNECTION;
        $pair = $this->connections[$key] ?? null;
        if ($pair === null) {
            $known = implode(', ', array_map(static fn (string $k): string => "'{$k}'", array_keys($this->connections)));
            throw new \RuntimeException(
                "scp connection routing: no connection registered under name '{$key}' "
                . '(known: ' . ($known !== '' ? $known : '<none>') . '). Register it via '
                . 'setConfig/ConnectionRegistry, or drop the connection tag on the bundle/model.'
            );
        }
        return $pair;
    }

    /** The registered connection names (for diagnostics / closeAllPools). @return list<string> */
    public function names(): array
    {
        return array_values(array_keys($this->connections));
    }

    /** Every DISTINCT pool object across all connections (a shared reader===writer counts once). @return list<PdoPool> */
    public function distinctPools(): array
    {
        $seen = [];
        foreach ($this->connections as $pair) {
            $seen[spl_object_id($pair->reader)] = $pair->reader;
            $seen[spl_object_id($pair->writer)] = $pair->writer;
        }
        return array_values($seen);
    }
}

/** Incremental {@see ConnectionRegistry} builder (name → pools). Mirrors the TS ConnectionRegistryBuilder. */
final class ConnectionRegistryBuilder
{
    /** @var array<string, ReaderWriterPools> */
    private array $connections = [];

    /** Register `$name` → its reader/writer pools (chainable). Re-adding a name overwrites it. */
    public function add(string $name, ReaderWriterPools $pools): ConnectionRegistryBuilder
    {
        $this->connections[$name] = $pools;
        return $this;
    }

    /** Finalize into an immutable {@see ConnectionRegistry}. */
    public function build(): ConnectionRegistry
    {
        if (count($this->connections) === 0) {
            throw new \RuntimeException(
                'scp connection routing: ConnectionRegistry must have at least the default connection'
            );
        }
        return new ConnectionRegistry($this->connections);
    }
}

// ── Writer-sticky + withWriter (C1) ────────────────────────────────────────────────

/**
 * The ambient "route reads to the writer" marker (mirror v1 `withWriter` writer context / the TS
 * `writerScopeStore` AsyncLocalStorage). PHP is 1-request-1-process (no threads / async continuation),
 * so a process-scoped counter is the honest, race-free equivalent — only ONE execution scope is live.
 * {@see withWriter()} increments it for the scope's duration and restores it in a `finally`.
 */
final class WriterScope
{
    private static int $depth = 0;

    /** Enter a writer scope (nestable). */
    public static function enter(): void
    {
        self::$depth++;
    }

    /** Leave a writer scope. */
    public static function leave(): void
    {
        if (self::$depth > 0) {
            self::$depth--;
        }
    }

    /** Is the current scope inside a {@see withWriter()} scope? */
    public static function active(): bool
    {
        return self::$depth > 0;
    }
}

/** True if the current execution scope is inside a {@see withWriter()} scope. */
function inWriterScope(): bool
{
    return WriterScope::active();
}

/**
 * Run `$fn` with reads pinned to the WRITER pool (mirror v1 `DBModel.withWriter` / the TS
 * `withWriter`): every read `$fn` issues resolves the writer pool (read-your-writes without
 * replication lag), and — because this ALSO enters a read-only scope (via the ambient read-only ctx) —
 * ANY write funneled through the guarded write seam inside `$fn` throws
 * {@see WriteInReadOnlyContextError}. Nested `withWriter` is idempotent (already in a writer scope ⇒
 * just run `$fn`). Inside a transaction the tx-owned connection already wins in `connectionFor`, so a
 * `withWriter` there is a no-op on routing (matches v1).
 *
 * The write-reject half rides the AMBIENT read-only ctx ({@see TxAmbient}): {@see withWriter()} pins a
 * read-only-scoped ctx for `$fn`'s duration so {@see checkWriteAllowedAmbient()} rejects a write. If
 * an ambient ctx is passed explicitly (or already present), the read-only marker is derived from it;
 * otherwise the writer scope still diverts reads (the routing half), but there is no ambient ctx to
 * mark read-only against. Pass `$ctx` to guarantee the write-reject half fires.
 *
 * @param callable():mixed $fn
 */
function withWriter(callable $fn, ?ExecutionContext $ctx = null): mixed
{
    if (inWriterScope()) {
        return $fn();
    }
    WriterScope::enter();
    // Pin a read-only-scoped ambient ctx so the write-reject half fires (v1's single writerContext is
    // BOTH writer-routing AND read-only). Prefer an explicit ctx; else derive from the ambient one.
    $base = $ctx ?? currentContext();
    $prev = null;
    $pinned = false;
    if ($base !== null) {
        $prev = TxAmbient::set($base->withReadOnly());
        $pinned = true;
    }
    try {
        return $fn();
    } finally {
        if ($pinned) {
            TxAmbient::restore($prev);
        }
        WriterScope::leave();
    }
}

/**
 * A writer-sticky CLOCK (C1, read-your-writes; v1 `_shouldUseWriterSticky` :344 + `_lastTransactionTime`
 * / the TS {@see WriterStickyClock}). After a transaction (or a bare write) COMMITs, reads within
 * `stickyDurationMs` route to the WRITER pool so a just-committed row is visible despite reader-replica
 * lag. The ctx owns ONE clock instance; the tx runtime `mark()`s it on every successful write/commit;
 * `connectionFor` reads `isSticky()`.
 *
 * `useWriterAfterTransaction = false` disables it entirely (`isSticky()` always false). A single-pool
 * deployment (reader === writer) is unaffected by stickiness — the diverted pool is the same object.
 * The clock is INJECTABLE (`$now`) so tests advance it deterministically (mirrors the TS injectable
 * `now`); it defaults to a millisecond wall clock.
 */
final class WriterStickyClock
{
    private float $lastWriteAt = 0.0;
    private readonly bool $enabled;
    private readonly int $stickyDurationMs;
    /** @var callable():float the injectable clock (ms); defaults to a wall clock. */
    private $now;

    /**
     * @param callable():float|null $now injectable millisecond clock (tests advance it).
     */
    public function __construct(
        bool $useWriterAfterTransaction = true,
        int $writerStickyDuration = 5000,
        ?callable $now = null,
    ) {
        $this->enabled = $useWriterAfterTransaction;
        $this->stickyDurationMs = $writerStickyDuration;
        $this->now = $now ?? static fn (): float => microtime(true) * 1000.0;
    }

    /** Record that a write/commit just happened (the tx runtime calls this on success). */
    public function mark(): void
    {
        if ($this->enabled) {
            $this->lastWriteAt = ($this->now)();
        }
    }

    /** Is a read currently sticky-to-writer (within `writerStickyDuration` of the last write)? */
    public function isSticky(): bool
    {
        if (!$this->enabled || $this->lastWriteAt === 0.0) {
            return false;
        }
        return (($this->now)() - $this->lastWriteAt) < $this->stickyDurationMs;
    }

    /** Reset the clock (e.g. between tests / on closeAllPools). */
    public function reset(): void
    {
        $this->lastWriteAt = 0.0;
    }
}

// ── The routing config a RoutingExecutionContext carries (C1+C2+C3) ────────────────

/**
 * The routing configuration a {@see RoutingExecutionContext} carries to complete its
 * `connectionFor(intent)` resolution (steps 2-4): the multi-DB {@see ConnectionRegistry} + the
 * {@see WriterStickyClock}. Mirrors the TS {@see RoutingConfig}.
 */
final class RoutingConfig
{
    public function __construct(
        public readonly ConnectionRegistry $registry,
        public readonly WriterStickyClock $sticky,
    ) {
    }
}

/**
 * Resolve WHICH {@see PdoPool} serves a statement given its {@see StatementIntent} and the routing
 * config — the completion of `connectionFor`'s steps 2-4 (step 1, the tx-pin, is handled by the base
 * {@see ExecutionContext} BEFORE calling this). The order (mirrors the TS `resolvePool`):
 *
 *   1. **named-DB** (`intent.db`) selects the {@see ReaderWriterPools} pair (loud on unknown name).
 *   2. within that pair: a WRITE ⇒ the writer pool.
 *   3. a READ in a {@see withWriter()} scope OR within writer-sticky ⇒ the writer pool (read-your-writes).
 *   4. otherwise a READ ⇒ the reader pool.
 *
 * Single-pool (reader === writer) ⇒ every branch returns the same pool (backward-compat).
 */
function resolvePool(StatementIntent $intent, RoutingConfig $routing): PdoPool
{
    $pair = $routing->registry->pairFor($intent->db);
    if ($intent->write) {
        return $pair->writer; // writes always to the writer
    }
    if (inWriterScope() || $routing->sticky->isSticky()) {
        return $pair->writer; // read-your-writes
    }
    return $pair->reader; // plain read → reader
}

// ── The routing-aware ExecutionContext (C1+C2+C3) ──────────────────────────────────

/**
 * A {@see ExecutionContext} that completes `connectionFor(intent)`'s steps 2-4 (reader/writer split,
 * writer-sticky/withWriter, named-DB routing) from a {@see RoutingConfig}. The tx-pin (step 1) is
 * inherited from the base {@see ExecutionContext}: a tx-scoped ctx's pinned connection STILL wins, so a
 * named-DB transaction runs entirely on ONE pinned writer conn (Phase B is NOT broken).
 *
 * Because the base ctx resolves `$pinned` first, this subclass only reaches routing for a NON-tx
 * statement. It acquires the resolved pool's connection per statement (applying the session config) and
 * releases it — the PHP analogue of the TS `PooledAsyncContext.connectionFor` acquire/run/release, with
 * PHP's release running SYNCHRONOUSLY after the statement via a {@see RoutedConnection} wrapper.
 *
 * `withConnection` (the tx pin) preserves the routing so a read issued AFTER the tx (writer-sticky)
 * still routes; but inside the tx the pinned connection wins, so routing is inert there.
 */
final class RoutingExecutionContext extends ExecutionContext
{
    public function __construct(
        PdoDriver $driver,
        MiddlewareChain $middleware,
        private readonly RoutingConfig $routing,
        ?Connection $pinned = null,
        bool $readOnly = false,
    ) {
        parent::__construct($driver, $middleware, $pinned, $readOnly);
    }

    /** The routing config (so the tx runtime can reach the registry/sticky clock). */
    public function routing(): RoutingConfig
    {
        return $this->routing;
    }

    /**
     * Resolve WHICH connection a statement runs on (§3). STEP 1: the tx-owned (pinned) connection wins
     * (the base ctx's `$pinned` — a named-DB tx runs entirely on it). STEPS 2-4: {@see resolvePool()}
     * selects the pool by intent; the returned {@see RoutedConnection} acquires the pool's connection
     * (applying session config), runs the statement, and releases it (session reset) — one acquire per
     * statement, mirroring the TS per-statement owned-connection wrapper.
     */
    public function connectionFor(StatementIntent $intent): Connection
    {
        // STEP 1 (§3): the tx-owned (pinned) connection wins. It may be pinned on THIS ctx (a tx-scoped
        // derivation) OR carried in the AMBIENT holder ({@see TxAmbient}) — the PHP analogue of the TS
        // ALS store: a statement issued via the OUTER routing ctx while a routedTransaction() body runs
        // still resolves the tx-owned connection (so a named-DB tx runs entirely on ONE pinned conn —
        // routing is inert inside the tx; Phase B ownership is NOT broken).
        $pinned = $this->pinnedConnection();
        if ($pinned !== null) {
            return $pinned;
        }
        $ambient = currentContext();
        if ($ambient !== null && $ambient !== $this && $ambient->inTransaction()) {
            return $ambient->connectionFor($intent); // resolve the ambient tx's pinned connection
        }
        // STEPS 2-4 (§3): named-DB → reader/writer split → writer-sticky/withWriter. `resolvePool`
        // returns WHICH pool serves this intent; the returned wrapper acquires/runs/releases one owned
        // connection per statement (per-statement acquire, mirroring the TS wrapper).
        $pool = resolvePool($intent, $this->routing);
        return new RoutedConnection($pool);
    }

    /**
     * Derive a tx-scoped ctx pinning `$conn` — preserving the routing config + read-only marker so a
     * writer-sticky read AFTER the tx still routes. Overrides the base so the derived ctx is a
     * {@see RoutingExecutionContext} (not a plain one).
     */
    public function withConnection(Connection $conn, bool $tx): ExecutionContext
    {
        return new RoutingExecutionContext(
            $this->driver(),
            $this->middleware,
            $this->routing,
            $tx ? $conn : null,
            $this->readOnly(),
        );
    }

    /** Derive a read-only-scoped routing ctx (write-reject). Overrides the base to keep routing. */
    public function withReadOnly(): ExecutionContext
    {
        return new RoutingExecutionContext(
            $this->driver(),
            $this->middleware,
            $this->routing,
            $this->pinnedConnection(),
            true,
        );
    }
}

/**
 * A {@see Connection} view over a resolved {@see PdoPool}: each `execute`/`run` `acquire()`s a
 * connection from the pool (applying the config's session statements), runs the statement, and
 * `release()`s it (running the reset statements so a persistent-PDO reuse does not leak session state).
 * On a statement error the connection is released DESTROYED (poisoned — e.g. a fired statement timeout
 * aborted it), skipping the reset. This is the PHP analogue of the TS
 * `PooledAsyncContext.connectionFor` per-statement acquire/run/release wrapper.
 */
final class RoutedConnection implements Connection
{
    public function __construct(private readonly PdoPool $pool)
    {
    }

    public function execute(string $sql, array $params): array
    {
        $conn = $this->pool->acquire();
        try {
            $rows = $conn->execute($sql, $params);
        } catch (\Throwable $e) {
            $this->pool->release($conn, true); // poisoned (e.g. aborted-by-timeout) — skip reset, drop it
            throw $e;
        }
        $this->pool->release($conn, false);
        return $rows;
    }

    public function run(string $sql, array $params): RunInfo
    {
        $conn = $this->pool->acquire();
        try {
            $info = $conn->run($sql, $params);
        } catch (\Throwable $e) {
            $this->pool->release($conn, true);
            throw $e;
        }
        $this->pool->release($conn, false);
        return $info;
    }

    public function control(string $sql): void
    {
        // Phase D #96: a runtime tx-control statement. In a routed deployment tx-control is issued on
        // the PINNED tx connection (a {@see TxConnectionAdapter}, which wins in `connectionFor`), so
        // this non-tx routed path is not the tx-control terminal in practice; it is implemented
        // faithfully (acquire → control → release) to satisfy the {@see Connection} contract.
        $conn = $this->pool->acquire();
        try {
            $conn->control($sql);
        } catch (\Throwable $e) {
            $this->pool->release($conn, true);
            throw $e;
        }
        $this->pool->release($conn, false);
    }
}

// ── setConfig / closeAllPools (C3 public surface) ──────────────────────────────────

/**
 * A driver's pool factory: BUILD a {@see PdoPool} from a {@see ResolvedConnectionConfig}, returning the
 * pool + a closer. The PHP analogue of the TS {@see PoolFactory} shape `(config, role) -> { pool, close }`.
 *
 * In PHP a factory constructs a `\PDO` from the config's connection params and applies the CONSTRUCTION
 * knobs that ARE meaningful here — `keepAlive` → {@see PDO::ATTR_PERSISTENT} (a warm persistent
 * connection). Pool sizing (`minPool`/`maxPool`) and `keepAliveInitialDelayMillis` are N/A in PHP (no
 * in-process pool / no per-probe TCP keepalive delay in \PDO — see the file docblock) and are NOT read
 * by {@see PdoPoolFactory} — they are inert by construction, not by a silently-dropped knob.
 *
 * `$role` ('reader'|'writer') lets a factory build a distinct replica connection for the reader vs. the
 * writer (a real reader/writer split would target different hosts via role-varied config); a factory
 * that returns the SAME pool for both roles collapses to single-pool (reader === writer).
 */
interface PoolFactory
{
    /**
     * @return array{pool: PdoPool, close: PoolCloser}
     */
    public function build(ResolvedConnectionConfig $config, string $role): array;
}

/** A pool CLOSER — closes a pool's underlying connection(s) (PHP: drop the last \PDO ref via the holder). */
final class PoolCloser
{
    /** @var callable():void */
    private $close;

    /** @param callable():void $close */
    public function __construct(callable $close)
    {
        $this->close = $close;
    }

    public function __invoke(): void
    {
        ($this->close)();
    }
}

/**
 * The reference {@see PoolFactory} for `\PDO` (PG / MySQL / SQLite). Constructs a `\PDO` from the
 * {@see ResolvedConnectionConfig} connection params, applying `keepAlive` → {@see PDO::ATTR_PERSISTENT}
 * AT CONSTRUCTION (the PHP analogue of a warm pooled connection). Wraps it in a {@see PdoDriver} and a
 * {@see ConfiguredPdoPool} so the SESSION knobs (queryTimeout/searchPath/charset) apply on checkout.
 * For PG it returns a {@see PgLivePdo} (`$N`→`?` placeholder rewrite); for MySQL a {@see MysqlLivePdo}
 * (RETURNING emulation) — so the routed pools carry the SAME dialect adaptation the LiveDb path does.
 *
 * Pool sizing (`minPool`/`maxPool`) + `keepAliveInitialDelayMillis` are N/A in PHP and are NOT read
 * here (documented; see the file docblock) — no in-process pool to cap, no per-probe keepalive delay.
 *
 * A caller may inject a pre-built `\PDO` (e.g. a shared test connection over the docker port) via
 * {@see PdoPoolFactory::forExisting()} so the reader/writer pair can wrap the SAME live connection —
 * routing is then observable independently of a real replica split (as the TS recording pools do).
 */
final class PdoPoolFactory implements PoolFactory
{
    /** @var (callable(ResolvedConnectionConfig, string): \PDO)|null an override that supplies the \PDO. */
    private $connect;

    /**
     * @param (callable(ResolvedConnectionConfig, string): \PDO)|null $connect override the \PDO construction
     *        (e.g. wrap a shared/existing connection). Default: construct from the config.
     */
    public function __construct(?callable $connect = null)
    {
        $this->connect = $connect;
    }

    /** A factory that hands out an EXISTING `\PDO` for BOTH roles (single-pool over a shared connection). */
    public static function forExisting(\PDO $pdo): PdoPoolFactory
    {
        return new PdoPoolFactory(static fn (ResolvedConnectionConfig $c, string $role): \PDO => $pdo);
    }

    public function build(ResolvedConnectionConfig $config, string $role): array
    {
        $pdo = $this->connect !== null ? ($this->connect)($config, $role) : self::connectFromConfig($config);
        $driver = new PdoDriver($pdo);
        $pool = new ConfiguredPdoPool($driver, $config);
        // \PDO has no explicit close; dropping the last ref closes it. The closer holds the driver ref
        // (keeping the \PDO alive) and drops it on close() → GC closes the connection.
        $held = $driver;
        $close = new PoolCloser(static function () use (&$held): void {
            $held = null; // drop the last ref → the \PDO is closed by GC.
        });
        return ['pool' => $pool, 'close' => $close];
    }

    /**
     * Construct a `\PDO` from the config's connection params, applying `keepAlive` →
     * {@see PDO::ATTR_PERSISTENT} at construction. PG → {@see PgLivePdo}, MySQL → {@see MysqlLivePdo}
     * (the dialect-adapting subclasses), SQLite → a plain `\PDO`.
     */
    private static function connectFromConfig(ResolvedConnectionConfig $config): \PDO
    {
        $opts = [\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION];
        // keepAlive → a persistent connection (the PHP analogue of a warm pooled connection).
        if ($config->keepAlive) {
            $opts[\PDO::ATTR_PERSISTENT] = true;
        }
        switch ($config->driver) {
            case 'postgres':
                $dsn = "pgsql:host={$config->host};port={$config->port};dbname={$config->database}";
                return new PgLivePdo($dsn, (string) $config->user, (string) $config->password, $opts);
            case 'mysql':
                $dsn = "mysql:host={$config->host};port={$config->port};dbname={$config->database}";
                $opts[\PDO::ATTR_EMULATE_PREPARES] = false;
                $pdo = new MysqlLivePdo($dsn, (string) $config->user, (string) $config->password, $opts);
                $pdo->setAttribute(\PDO::ATTR_STATEMENT_CLASS, [MysqlReturningStatement::class, [$pdo]]);
                return $pdo;
            case 'sqlite':
                return new \PDO("sqlite:{$config->database}", null, null, $opts);
            default:
                throw new \RuntimeException("scp connection routing: unknown driver '{$config->driver}'");
        }
    }
}

/**
 * One connection's inputs to {@see setConfig()}: its NAME (default when absent), its
 * {@see ConnectionConfig}, and a {@see PoolFactory} that {@see setConfig()} CALLS with the resolved
 * config to construct the pool(s). Mirrors the TS {@see ConnectionSetup}.
 *
 * `$separateWriter = true` asks the factory for a DISTINCT writer pool (reader/writer replica split);
 * otherwise the factory's reader pool is reused as the writer (single-pool, reader === writer).
 */
final class ConnectionSetup
{
    public function __construct(
        public readonly PoolFactory $poolFactory,
        public readonly ?ConnectionConfig $config = null,
        public readonly ?string $name = null,
        public readonly bool $separateWriter = false,
    ) {
    }
}

/**
 * The C3 `setConfig` result: the {@see RoutingConfig} a {@see RoutingExecutionContext} runs on, plus a
 * `close()` that shuts every constructed pool (closeAllPools). Mirrors the TS `buildRoutingConfig`
 * result `{ routing, close }`.
 */
final class RoutingSetup
{
    public function __construct(
        public readonly RoutingConfig $routing,
        public readonly PoolCloser $close,
    ) {
    }

    /** Close every constructed pool (the closeAllPools surface). */
    public function closeAllPools(): void
    {
        ($this->close)();
    }
}

/**
 * The C3 `setConfig` — {@see buildRoutingConfig()}'s PHP name. Build a {@see RoutingConfig} from one or
 * more {@see ConnectionSetup}s (the one named `default`, or the first unnamed, is the default
 * connection). For each setup: resolve the config, CALL its {@see PoolFactory} to construct the
 * pool(s) — so `keepAlive` is applied at construction — then the pools carry the SESSION knobs
 * (queryTimeout/searchPath/charset) via {@see ConfiguredPdoPool} on checkout. Mirrors the TS
 * `buildRoutingConfig`.
 *
 * (PHP honesty: pool SIZING `minPool`/`maxPool` is N/A here — see the file docblock — so unlike the TS
 * reference there is no "sizing is the sole cap" step. `keepAlive` is the meaningful construction knob.)
 *
 * @param list<ConnectionSetup> $setups
 */
function setConfig(
    array $setups,
    bool $useWriterAfterTransaction = true,
    int $writerStickyDuration = 5000,
    ?callable $now = null,
): RoutingSetup {
    if (count($setups) === 0) {
        throw new \RuntimeException('scp setConfig: at least one connection setup is required');
    }
    $builder = new ConnectionRegistryBuilder();
    /** @var list<PoolCloser> $closers */
    $closers = [];
    foreach ($setups as $s) {
        $resolved = resolveConnectionConfig($s->config);
        // CONSTRUCT the reader pool from the resolved config (keepAlive lands at construction).
        $readerBuilt = $s->poolFactory->build($resolved, 'reader');
        $closers[] = $readerBuilt['close'];
        $reader = $readerBuilt['pool'];
        if ($s->separateWriter) {
            $writerBuilt = $s->poolFactory->build($resolved, 'writer');
            $closers[] = $writerBuilt['close'];
            $pair = new ReaderWriterPools($reader, $writerBuilt['pool']);
        } else {
            $pair = ReaderWriterPools::single($reader); // reader === writer (one constructed pool)
        }
        $builder->add($s->name ?? DEFAULT_CONNECTION, $pair);
    }
    $routing = new RoutingConfig(
        $builder->build(),
        new WriterStickyClock($useWriterAfterTransaction, $writerStickyDuration, $now),
    );
    $close = new PoolCloser(static function () use ($closers): void {
        // Close every DISTINCT closer (deduped by identity), tolerating individual failures.
        $seen = [];
        foreach ($closers as $c) {
            $id = spl_object_id($c);
            if (isset($seen[$id])) {
                continue;
            }
            $seen[$id] = true;
            try {
                ($c)();
            } catch (\Throwable) {
                // best-effort close.
            }
        }
    });
    return new RoutingSetup($routing, $close);
}

/**
 * Alias for {@see setConfig()} under the TS reference's name, for callers mirroring the TS surface
 * (`buildRoutingConfig(setups, stickyOpts)`). Same behavior.
 *
 * @param list<ConnectionSetup> $setups
 */
function buildRoutingConfig(
    array $setups,
    bool $useWriterAfterTransaction = true,
    int $writerStickyDuration = 5000,
    ?callable $now = null,
): RoutingSetup {
    return setConfig($setups, $useWriterAfterTransaction, $writerStickyDuration, $now);
}

/**
 * Build a {@see RoutingExecutionContext} from a {@see RoutingConfig} (the routing-aware ctx a routed
 * deployment threads in place of {@see Context::forPdo()}). The default connection's WRITER pool's
 * backing driver is used for the base ctx's `driver()` (the non-routed tx accessor); routing resolves
 * the actual per-statement pool from the registry. Convenience over the ctor.
 */
function routingContext(RoutingConfig $routing, ?MiddlewareChain $middleware = null): RoutingExecutionContext
{
    // The base ctx's driver() is the DEFAULT connection's writer backing driver — the tx path's
    // non-routed accessor. Named-DB tx routing overrides it per {@see routedTransaction()}.
    $defaultWriter = $routing->registry->pairFor(null)->writer;
    // Phase D (#96): default to the AMBIENT-sourced chain (resolves the current scope's registry at
    // wrap time) so a routed deployment gets middleware too; empty registry ⇒ byte-identical.
    return new RoutingExecutionContext($defaultWriter->backingDriver(), $middleware ?? Context::ambientChain(), $routing);
}

/**
 * The C1/C2 routing-aware {@see transaction()} boundary. It runs the SAME Phase B public
 * {@see transaction()} (retry / isolation / nested-join / write=tx guard — UNCHANGED) but:
 *
 *   - **C2 named-DB tx pin**: it acquires the tx's OWNED connection from the WRITER pool of the target
 *     connection (`$connection` name, or the default) — so a named-DB transaction runs ENTIRELY on that
 *     ONE pinned writer `\PDO`. The active-tx pin STILL wins over routing (the base ctx resolves the
 *     pinned connection FIRST in `connectionFor`), so every statement in the body — read or write, any
 *     `intent.db` — runs on that ONE connection (Phase B ownership is NOT broken).
 *   - **C1 writer-sticky mark**: on a SUCCESSFUL commit it `mark()`s the {@see WriterStickyClock}, so
 *     reads issued AFTER the tx (within `writerStickyDuration`) route to the writer pool
 *     (read-your-writes). A `rollbackOnly` (dry-run) tx committed NOTHING ⇒ it does NOT arm stickiness.
 *
 * It delegates to the base {@see transaction()} on a ctx whose base `driver()` is the target
 * connection's writer backing driver (so `withTransactionDecided` BEGINs on the right `\PDO`).
 *
 * @param callable():mixed $fn
 */
function routedTransaction(
    RoutingExecutionContext $ctx,
    callable $fn,
    ?TransactionOptions $options = null,
    string $dialectName = 'postgres',
    ?string $connection = null,
): mixed {
    // NESTED-TX JOIN: already inside a tx ⇒ the base transaction() joins the outer; sticky was armed by
    // the outer's commit. Delegate straight through (no re-pin, no double-mark).
    if (ambientInTransaction()) {
        return transaction($ctx, $fn, $options, $dialectName);
    }

    $opts = $options ?? new TransactionOptions();
    // C2: the tx BEGINs on the target connection's WRITER pool's backing driver (named-DB pin). Every
    // in-body statement then resolves the pinned connection (base ctx STEP 1), so routing is inert
    // inside the tx — the whole named-DB tx runs on ONE writer \PDO (Phase B ownership preserved).
    $writerPool = $ctx->routing()->registry->pairFor($connection)->writer;
    $txCtx = new RoutingExecutionContext(
        $writerPool->backingDriver(),
        $ctx->middleware,
        $ctx->routing(),
    );

    $result = transaction($txCtx, $fn, $opts, $dialectName);
    // C1 writer-sticky: a COMMITTED tx arms the sticky clock (a rollbackOnly dry-run committed nothing).
    if (!$opts->rollbackOnly) {
        $ctx->routing()->sticky->mark();
    }
    return $result;
}
