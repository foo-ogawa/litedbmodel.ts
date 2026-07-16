<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — the **tx-completeness contract** (Phase B / #85, PHP port of
 * `src/scp/tx-options.ts`): the {@see IsolationLevel} enum + per-dialect isolation-level SQL, the
 * {@see TransactionOptions} shape + defaults, the retryable-error classifier
 * ({@see isRetryableTxError()}), and the write=tx guard errors
 * ({@see WriteOutsideTransactionError} / {@see WriteInReadOnlyContextError}).
 *
 * This is the PHP mirror of the Phase B **API REFERENCE** (`tx-options.ts`, #81), matching the rust
 * `tx_options.rs` (#82), the go `tx_options.go` (#83) + the python `tx_options.py` (#84): the option
 * field names + defaults, guard-error semantics, isolation-level→SQL mapping per dialect,
 * retryable-error classification, and retry-loop policy all match that contract EXACTLY. It is
 * dialect-neutral and driver-agnostic on purpose: the public {@see transaction()} boundary
 * (ExecutionContext.php) consumes it; nothing here touches a connection. It layers the
 * options/guards/retry/isolation on top of the Phase A {@see ExecutionContext} + owned-connection
 * ownership ({@see withTransactionDecided()}); it does NOT re-implement connection ownership. It
 * mirrors v1 `DBModel.ts` (`transaction` :2787, `checkWriteAllowed` :886, `isRetryableError` :2865)
 * but on the SCP seam.
 *
 * ## The typed-code retryable classifier is LOAD-BEARING (go #83 audit lesson)
 *
 * The retryable classification extracts the driver error CODE **TYPED** — a {@see \PDOException}
 * carries the standard `errorInfo` triple `[SQLSTATE, driverCode, driverMessage]`. For PG the
 * SQLSTATE is `errorInfo[0]` (`40001` serialization_failure / `40P01` deadlock_detected); for MySQL
 * the numeric errno is `errorInfo[1]` (`1213` ER_LOCK_DEADLOCK / `1205` ER_LOCK_WAIT_TIMEOUT). This
 * is the PRIMARY, driver-version-independent classifier — it does NOT string-match. The go port's
 * first attempt shipped the typed block as DEAD CODE (the live driver error was flattened to a
 * plain-string failure before it reached the classifier, so only the string-substring fallback ever
 * fired — esp. at COMMIT, where a PG 40001 write-skew or a MySQL 1213 deadlock actually surfaces).
 *
 * For PARITY with rust/go/py the PHP tx runtime maps a raw {@see \PDOException} into the
 * {@see SqlFailure} envelope (retaining the concrete error on `$wrapped` + `getPrevious()` — the go
 * `SqlFailure.Unwrap()` analogue) BEFORE the retry classifier sees it. So the LIVE retry path
 * classifies a PG 40001 / MySQL 1213 THROUGH the envelope, exactly like go/rust/py:
 * {@see isRetryableTxError()} TRAVERSES the wrapped chain ({@see retryableByTypedCode()}) to reach the
 * concrete {@see \PDOException}'s `errorInfo`. The message-substring match is a belt-and-suspenders
 * FALLBACK for a type-erased error; it is EXERCISE-DISABLABLE ({@see disableRetryStringFallback()})
 * so the live regression test can prove the typed path alone still classifies live 40001 / 1213
 * (guarding against silent rot back to dead code — the exact defect go #83's audit caught).
 */

// ── Isolation level (the portable enum + per-dialect SQL) ─────────────────────

/**
 * The three portable SQL isolation levels the tx API exposes (matching the v2 public surface). READ
 * UNCOMMITTED is deliberately NOT offered — neither PG (which silently upgrades it to READ COMMITTED)
 * nor a correctness-minded default wants it, and omitting it keeps the ports' enum identical. The
 * value maps to the canonical SQL phrase via {@see phrase()}.
 */
enum IsolationLevel: string
{
    case ReadCommitted = 'read committed';
    case RepeatableRead = 'repeatable read';
    case Serializable = 'serializable';

    /** The canonical SQL phrase for this level (e.g. `SERIALIZABLE`). Mirrors `isolationPhrase`. */
    public function phrase(): string
    {
        return match ($this) {
            IsolationLevel::ReadCommitted => 'READ COMMITTED',
            IsolationLevel::RepeatableRead => 'REPEATABLE READ',
            IsolationLevel::Serializable => 'SERIALIZABLE',
        };
    }
}

/**
 * The tx-start statements for `$dialectName` at `$isolation` (in issue order). Per-dialect because
 * the three engines express per-transaction isolation DIFFERENTLY (mirror `beginStatements`):
 *
 *   - **postgres**: `BEGIN ISOLATION LEVEL <phrase>` — one statement, the level rides the BEGIN.
 *   - **mysql**: `SET TRANSACTION ISOLATION LEVEL <phrase>` MUST precede `BEGIN` (it scopes the very
 *     NEXT transaction only), so this returns TWO statements: the SET, then a bare `BEGIN`.
 *   - **sqlite**: SQLite has NO per-transaction isolation-level knob — its isolation is a
 *     process/PRAGMA property (`journal_mode=WAL` for snapshot reads), NOT a `BEGIN` clause. So an
 *     isolation request on SQLite is a HARD ERROR here (we do NOT silently drop it — that would fake
 *     honoring the level). Absent `$isolation` SQLite emits a bare `BEGIN`.
 *
 * With no `$isolation`, every dialect emits a single bare `BEGIN` (the Phase A behavior, byte-identical).
 *
 * NB: the actual `BEGIN` is issued by {@see PdoDriver::beginTx()} (which owns the connection). The
 * runtime uses {@see isolationPrelude()} (the driver-facing split) to bridge — MySQL's SET runs
 * pre-BEGIN, PG's SET runs post-BEGIN as the first in-tx statement. This SQL-text form is retained
 * for TS parity / the conformance-facing surface.
 *
 * @return list<string>
 */
function beginStatements(string $dialectName, ?IsolationLevel $isolation = null): array
{
    if ($isolation === null) {
        return ['BEGIN'];
    }
    $phrase = $isolation->phrase();
    return match ($dialectName) {
        'postgres' => ["BEGIN ISOLATION LEVEL {$phrase}"],
        // The SET scopes ONLY the next tx; it must be issued before BEGIN, on the SAME connection.
        'mysql' => ["SET TRANSACTION ISOLATION LEVEL {$phrase}", 'BEGIN'],
        'sqlite' => throw new \RuntimeException(
            "scp tx: SQLite does not support a per-transaction isolation level ('{$phrase}'). "
            . 'SQLite isolation is a connection/PRAGMA property (e.g. journal_mode=WAL for snapshot '
            . 'reads), not a BEGIN clause — set it on the connection, and omit '
            . 'TransactionOptions.isolation for SQLite.'
        ),
        default => throw new \RuntimeException("scp tx: unknown dialect '{$dialectName}'"),
    };
}

/**
 * Split the isolation prelude into `[before-BEGIN, after-BEGIN]` statement lists the driver runs
 * around its own `BEGIN` (Phase B / #85). This is the DRIVER-facing form of {@see beginStatements()}:
 * because {@see PdoDriver::beginTx()} issues the plain `BEGIN` itself, the isolation SET is delivered
 * as prelude statements it runs BEFORE (MySQL — the SET scopes the next tx) or AFTER (Postgres — the
 * SET is valid as the first in-tx statement) that `BEGIN`.
 *
 *   - **postgres**: `[[], ["SET TRANSACTION ISOLATION LEVEL <phrase>"]]` — runs post-BEGIN.
 *   - **mysql**: `[["SET TRANSACTION ISOLATION LEVEL <phrase>"], []]` — runs pre-BEGIN.
 *   - **sqlite** with isolation: a HARD ERROR (no per-tx level); no isolation ⇒ both empty.
 *
 * No isolation ⇒ `[[], []]` for every dialect (the Phase A bare `BEGIN`, byte-identical). Mirrors
 * rust `isolation_prelude` / go `isolationPrelude` / python `isolation_prelude`.
 *
 * @return array{0:list<string>, 1:list<string>}
 */
function isolationPrelude(string $dialectName, ?IsolationLevel $isolation = null): array
{
    if ($isolation === null) {
        return [[], []];
    }
    $set = "SET TRANSACTION ISOLATION LEVEL {$isolation->phrase()}";
    return match ($dialectName) {
        'postgres' => [[], [$set]],
        'mysql' => [[$set], []],
        'sqlite' => throw new \RuntimeException(
            "scp tx: SQLite does not support a per-transaction isolation level ('{$isolation->phrase()}'). "
            . 'SQLite isolation is a connection/PRAGMA property (e.g. journal_mode=WAL for snapshot '
            . 'reads), not a BEGIN clause — set it on the connection, and omit '
            . 'TransactionOptions.isolation for SQLite.'
        ),
        default => throw new \RuntimeException("scp tx: unknown dialect '{$dialectName}'"),
    };
}

// ── TransactionOptions (the Phase B public option shape) ──────────────────────

/**
 * Options for a transaction (the Phase B contract; mirrors v1 `TransactionOptions` plus the new
 * `isolation`, and the TS/rust/go/py `TransactionOptions`). Every field is optional with a stable
 * default; the field names + defaults match the other ports EXACTLY (retryOnError=true, retryLimit=3,
 * retryDurationMs=200, rollbackOnly=false, isolation=null). NB this DIFFERS from the v1 defaults
 * (retry OFF, 100 ms) — the v2 Phase B contract (`tx-options.ts`) is the SSoT the 5 ports mirror,
 * and it defaults retry ON with a 200 ms base.
 */
final class TransactionOptions
{
    public function __construct(
        /**
         * Per-transaction isolation level. Issued via {@see isolationPrelude()} on the tx-owned
         * connection (PG: post-BEGIN SET; MySQL: a preceding SET). `null` ⇒ the engine default. SQLite
         * has no per-tx level ⇒ passing this on SQLite is an error.
         */
        public readonly ?IsolationLevel $isolation = null,
        /** Retry the whole tx on a retryable error (deadlock / serialization / connection). Default true. */
        public readonly bool $retryOnError = true,
        /** Max attempts before giving up (the FIRST try counts as attempt 1). Default 3. */
        public readonly int $retryLimit = 3,
        /** Backoff base in ms; attempt k waits `retryDurationMs * 2^(k-1)` (exponential). Default 200. */
        public readonly int $retryDurationMs = 200,
        /**
         * ROLLBACK instead of COMMIT at the end of a SUCCESSFUL body (dry-run / preview): the body runs
         * and its result is returned, but NO change is committed. A body error still ROLLBACKs +
         * re-raises as usual. Default false.
         */
        public readonly bool $rollbackOnly = false,
    ) {
    }
}

// ── Retryable-error classification (per dialect) ──────────────────────────────

/**
 * The exercise-only toggle behind {@see isRetryableTxError()}'s string FALLBACK. When true, the
 * classifier SKIPS the message-substring fallback so the PRIMARY typed-code path (`errorInfo`
 * SQLSTATE/errno traversed through the wrapped {@see \PDOException}) must stand on its own. It exists
 * ONLY for the live regression test that proves the typed extraction is genuinely load-bearing
 * (guarding against the typed block silently rotting back to dead code — the defect go #83's audit
 * caught); production never sets it. Process-level (1-req-1-process); the regression test flips it
 * serially and restores it in a `finally`.
 */
final class RetryClassifierFlags
{
    /** @internal exercise-only — see {@see disableRetryStringFallback()}. */
    public static bool $disableStringFallback = false;

    /**
     * @internal exercise-only. When true, {@see iterErrorChain()} yields ONLY the top-of-chain error
     *           (no traversal into `$wrapped` / `getPrevious()`), so the typed classifier can NOT reach
     *           a concrete {@see \PDOException} that was mapped into a {@see SqlFailure} at COMMIT time.
     *           This is the FAITHFUL NEUTER used by the live retry RED proof: with the string fallback
     *           ALSO off, a mapped live 40001 / 1213 can no longer be classified retryable — so the
     *           loser can't retry and gives up (RED). It proves the wrapped-chain traversal (the go
     *           `SqlFailure.Unwrap()` analogue) is genuinely load-bearing on the live retry path, not
     *           dead code (the exact defect go #83's audit caught). Production never sets it.
     */
    public static bool $neuterWrappedChain = false;
}

/**
 * @internal Set/clear the string-fallback disable flag (exercise-only). Returns the prior value so the
 *           caller can restore it. Used ONLY by the live typed-retry regression proof.
 */
function disableRetryStringFallback(bool $disable): bool
{
    $prior = RetryClassifierFlags::$disableStringFallback;
    RetryClassifierFlags::$disableStringFallback = $disable;
    return $prior;
}

/**
 * Yield `$error` then every wrapped/caused error reachable via {@see SqlFailure::$wrapped} /
 * `getPrevious()` — so the typed classifier reaches a concrete {@see \PDOException} even when it was
 * mapped into a {@see SqlFailure} at COMMIT time. Bounded + cycle-guarded.
 *
 * @return list<\Throwable>
 */
function iterErrorChain(\Throwable $error): array
{
    $out = [];
    $seen = [];
    $cur = $error;
    while ($cur !== null && !in_array(spl_object_id($cur), $seen, true)) {
        $seen[] = spl_object_id($cur);
        $out[] = $cur;
        // EXERCISE-ONLY neuter (the live retry RED proof): stop at the top of the chain so the typed
        // classifier can't reach a mapped-away concrete \PDOException (proving the traversal is load-bearing).
        if (RetryClassifierFlags::$neuterWrappedChain) {
            break;
        }
        // `$wrapped` is the explicit retained driver error (go Unwrap() analogue); getPrevious() is set
        // by `new SqlFailure(.., $wrapped)` (which threads it as $previous).
        $next = $cur instanceof SqlFailure ? $cur->wrapped : null;
        $next ??= $cur->getPrevious();
        $cur = $next;
    }
    return $out;
}

/**
 * The PG SQLSTATE + MySQL errno for `$err` if it is a {@see \PDOException}, from the standard
 * `errorInfo` triple `[SQLSTATE, driverCode, driverMessage]`. TYPED extraction — NOT a string match.
 *
 *   - PG: `errorInfo[0]` is the 5-char SQLSTATE (e.g. `40001`); PDO_pgsql sets it on EVERY server
 *     error (including one raised at COMMIT, e.g. a SERIALIZABLE write-skew 40001).
 *   - MySQL: `errorInfo[1]` is the numeric errno (e.g. 1213 ER_LOCK_DEADLOCK); `errorInfo[0]` is a
 *     generic HY000/40001 SQLSTATE, so the errno is the reliable code.
 *
 * @return array{sqlstate:?string, errno:?int}
 */
function pdoErrorCodes(\PDOException $err): array
{
    $info = $err->errorInfo;
    $sqlstate = is_array($info) && isset($info[0]) && is_string($info[0]) && $info[0] !== '' ? $info[0] : null;
    $errno = is_array($info) && isset($info[1]) && is_int($info[1]) ? $info[1] : null;
    return ['sqlstate' => $sqlstate, 'errno' => $errno];
}

/**
 * Does any error in `$error`'s wrapped chain carry a retryable PG SQLSTATE (40001/40P01) or MySQL
 * errno (1213/1205)? This is the PRIMARY, driver-version-independent classifier — it does NOT
 * string-match. It is the load-bearing live path: a PG 40001 raised at COMMIT is mapped into a
 * {@see SqlFailure} whose `$wrapped` / `getPrevious()` re-exposes the concrete {@see \PDOException},
 * and this reaches its `errorInfo`. Mirrors go `retryableByTypedCode` (`errors.As` on the concrete
 * driver error via `SqlFailure.Unwrap()`) / python `retryable_by_typed_code`.
 */
function retryableByTypedCode(\Throwable $error): bool
{
    foreach (iterErrorChain($error) as $e) {
        if (!($e instanceof \PDOException)) {
            continue;
        }
        $codes = pdoErrorCodes($e);
        $state = $codes['sqlstate'];
        if ($state === '40001' || $state === '40P01') { // serialization_failure / deadlock_detected
            return true;
        }
        $errno = $codes['errno'];
        if ($errno === 1213 || $errno === 1205) { // ER_LOCK_DEADLOCK / ER_LOCK_WAIT_TIMEOUT
            return true;
        }
    }
    return false;
}

/**
 * Is `$error` a broken/stale connection (retryable via reconnect)? A message/-code heuristic matching
 * `src/connection-errors.ts` (`isConnectionError`) plus the PDO_pgsql / PDO_mysql connection-closed
 * phrasings. A dropped/reset/refused connection reconnects on the next attempt (a fresh connection).
 * Mirrors rust/go/py `is_connection_error`.
 */
function isConnectionError(\Throwable $error): bool
{
    $m = $error->getMessage();
    return str_contains($m, 'Connection terminated')
        || str_contains($m, 'Client has encountered a connection error')
        || str_contains($m, 'ECONNRESET')
        || str_contains($m, 'ECONNREFUSED')
        || str_contains($m, 'Connection lost')
        || str_contains($m, 'This socket has been ended by the other party')
        || str_contains($m, 'EPIPE')
        || str_contains($m, 'PROTOCOL_CONNECTION_LOST')
        // PDO_pgsql / PDO_mysql connection-closed phrasings.
        || str_contains($m, 'server closed the connection')
        || str_contains($m, 'connection closed')
        || str_contains($m, 'Lost connection to')
        || str_contains($m, 'MySQL server has gone away')
        || str_contains($m, 'gone away');
}

/**
 * Is `$error` a RETRYABLE transaction failure — a deadlock, a serialization failure, or a broken
 * connection — for which re-running the whole transaction can succeed? Classification is by the
 * driver's stable error CODE first (PG SQLSTATE / MySQL errno via {@see \PDOException::$errorInfo},
 * traversed through the wrapped chain), with the v1 message substrings as a FALLBACK (mirrors TS
 * `isRetryableTxError` / rust / go / py). A data conflict (unique/FK/check) is NOT retryable —
 * re-running would fail identically.
 *
 * Codes (per dialect):
 *   - **postgres** SQLSTATE: `40001` serialization_failure, `40P01` deadlock_detected.
 *   - **mysql** errno: `1213` ER_LOCK_DEADLOCK, `1205` ER_LOCK_WAIT_TIMEOUT.
 *   - **connection errors** (either dialect): via {@see isConnectionError()}.
 *
 * The typed-code extraction ({@see retryableByTypedCode()}, traversing the wrapped chain to the
 * concrete {@see \PDOException}) is the PRIMARY, load-bearing mechanism — NOT dead code behind the
 * string match. The string fallback is EXERCISE-DISABLABLE ({@see disableRetryStringFallback()}) so
 * the live regression proves the typed path alone catches PG 40001 + MySQL 1213 (the go #83 audit
 * lesson).
 */
function isRetryableTxError(\Throwable $error): bool
{
    if (isConnectionError($error)) {
        return true;
    }
    // PRIMARY: stable CODE via the concrete driver error (reachable through the wrapped chain).
    if (retryableByTypedCode($error)) {
        return true;
    }
    // FALLBACK: the v1 message substrings (driver-version-independent phrasing) — belt-and-suspenders
    // for a type-erased error. Disablable in tests to prove the typed path is load-bearing.
    if (RetryClassifierFlags::$disableStringFallback) {
        return false;
    }
    $m = $error->getMessage();
    return str_contains($m, '40001')
        || str_contains($m, '40P01')
        || str_contains($m, '1213')
        || str_contains($m, '1205')
        || str_contains($m, 'The transaction might succeed if retried')
        || str_contains($m, 'try restarting transaction')
        || str_contains($m, 'could not serialize access due to concurrent update')
        || str_contains($m, 'could not serialize access')
        || str_contains($m, 'Deadlock found')
        || str_contains($m, 'deadlock detected')
        || str_contains($m, 'Lock wait timeout exceeded');
}

// ── write=tx guards (mirror v1 `checkWriteAllowed`, DBModel.ts:886) ────────────

/**
 * The write=tx guard error: a write issued OUTSIDE a {@see transaction()} boundary. Mirrors v1
 * `WriteOutsideTransactionError` (the TS / rust / go / py analogue). Carries a stable `kind` so the
 * runtime surfaces it uniformly.
 */
final class WriteOutsideTransactionError extends \RuntimeException
{
    public const KIND = 'write_outside_transaction';
    public readonly string $kind;

    public function __construct(public readonly string $operation, public readonly ?string $model = null)
    {
        $this->kind = self::KIND;
        parent::__construct(sprintf('Write operation "%s" on %s requires a transaction', $operation, $model ?? ''));
    }
}

/**
 * The write=tx guard error: a write issued in a READ-ONLY scope. Mirrors v1
 * `WriteInReadOnlyContextError` (the TS / rust / go / py analogue).
 */
final class WriteInReadOnlyContextError extends \RuntimeException
{
    public const KIND = 'write_in_read_only_context';
    public readonly string $kind;

    public function __construct(public readonly string $operation, public readonly ?string $model = null)
    {
        $this->kind = self::KIND;
        parent::__construct(sprintf('Write operation "%s" on %s is not allowed in read-only context', $operation, $model ?? ''));
    }
}

/**
 * Enforce the write=tx guard (mirror v1 `DBModel._checkWriteAllowed`, DBModel.ts:886-896, + the TS
 * `checkWriteAllowed` / rust / go / py): a write in a read-only scope → {@see WriteInReadOnlyContextError};
 * a write with NO active transaction → {@see WriteOutsideTransactionError}. Called at every write
 * ENTRY (create/update/delete/upsert/batch) BEFORE any SQL. The order matches v1: read-only is
 * checked FIRST (the more specific rejection).
 *
 * `$inTransaction` / `$readOnly` come from the AMBIENT ({@see TxAmbient})-propagated
 * {@see ExecutionContext}: a write inside {@see transaction()} runs with a tx-scoped ctx
 * (`inTransaction=true`), a bare write with no ambient tx ctx (`false`).
 */
function checkWriteAllowed(string $operation, ?string $model, bool $inTransaction, bool $readOnly): void
{
    if ($readOnly) {
        throw new WriteInReadOnlyContextError($operation, $model);
    }
    if (!$inTransaction) {
        throw new WriteOutsideTransactionError($operation, $model);
    }
}
