<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — Error Mapping (PHP port of src/scp/errors.ts, WS7d #33; spec §11 item 5).
 *
 * Maps a PDO driver error to an SCP {@link SqlFailure}: a structured Failure with a stable
 * `kind` and the bc Execution-Plan Policy Kind the runtime honors (fail / retry / continue).
 * The SQL handler returns `{error}` to bc's `runBehavior`; a `fail`-policy node re-throws through
 * `runPlan` (`OP_FAILED`), so the driver surfaces this structured failure at the boundary.
 *
 * The mapping is closed and explicit (no silent catch-all that hides a driver error). The TS
 * reference keys off better-sqlite3's `SQLITE_*` string `code`; PDO SQLite exposes the SQLite
 * extended result code via `errorInfo[1]` (int) and an SQLSTATE class in `getCode()`. This port
 * derives the SAME `SQLITE_*` family from the PDO error so the message embeds it (surviving bc's
 * `OP_FAILED` wrap) and re-mapping at the boundary reproduces the same kind/policy/code.
 */
final class SqlFailure extends \RuntimeException
{
    public const KIND_CONSTRAINT = 'constraint_violation';
    public const KIND_FOREIGN_KEY = 'foreign_key_violation';
    public const KIND_RETRYABLE = 'retryable';
    public const KIND_DRIVER = 'driver_error';

    /**
     * @param \Throwable|null $wrapped the ORIGINAL concrete driver error (a live {@see \PDOException})
     *        when this failure maps a live-DB error (Phase B / #85). {@see fromPdo()} flattens the
     *        driver TEXT into the message, but a text string is OPAQUE to a TYPED classifier — so
     *        {@see \LiteDbModel\Runtime\isRetryableTxError()}'s SQLSTATE/errno extraction (the robust,
     *        driver-version-independent classifier) would be DEAD CODE unless the concrete error stays
     *        reachable. `$wrapped` (the go `SqlFailure.Unwrap()` analogue) re-exposes it so the
     *        classifier reads `errorInfo` (PG SQLSTATE / MySQL errno) even at COMMIT time (where a PG
     *        40001 write-skew / MySQL 1213 deadlock surfaces). It is ALSO threaded as the exception
     *        `$previous` so `getPrevious()` reaches the same concrete error. `null` for a synthetic
     *        failure or an in-proc SQLite error (which carries its own `sqliteCode`). This attribute
     *        never touches the byte-identical conformance surface (the corpus compares the encoded
     *        result, never the error object).
     */
    public function __construct(
        public readonly string $kind,
        public readonly string $policy,
        public readonly ?string $sqliteCode,
        string $message,
        public readonly ?\Throwable $wrapped = null,
    ) {
        parent::__construct($message, 0, $wrapped);
    }

    /**
     * Map a caught PDO error to a SqlFailure. The `SQLITE_CONSTRAINT_*` family maps to
     * constraint / FK kinds (policy `fail` — a data conflict is not retryable); `SQLITE_BUSY` /
     * `SQLITE_LOCKED` map to `retryable` (policy `retry`); anything else maps to `driver_error`
     * (policy `fail`), preserving the original message (errors.ts `mapSqliteError`).
     *
     * A NON-SQLite (live PG/MySQL) driver error retains the concrete {@see \PDOException} as
     * `$wrapped` so the TYPED retryable classifier can reach its `errorInfo` SQLSTATE/errno through the
     * mapped failure (Phase B / #85 typed-retryable path). This is the branch a PG 40001 raised at
     * COMMIT lands in.
     */
    public static function fromPdo(\PDOException $e): self
    {
        $code = self::sqliteCodeOf($e);
        $message = $e->getMessage();
        if ($code === null) {
            return new self(self::KIND_DRIVER, 'fail', null, "non-SQLite driver error: {$message}", $e);
        }
        return self::fromCode($code, $message, $e);
    }

    /**
     * Build a SqlFailure from a resolved `SQLITE_*` code + message. Used both by {@link fromPdo}
     * and by the boundary re-map (errors.ts `mapSqliteError` code path) — the message embeds the
     * `[SQLITE_*]` tag so it survives being wrapped by bc's `runPlan` `OP_FAILED`.
     */
    public static function fromCode(string $code, string $message, ?\Throwable $wrapped = null): self
    {
        $tagged = "[{$code}] {$message}";
        if ($code === 'SQLITE_CONSTRAINT_FOREIGNKEY') {
            return new self(self::KIND_FOREIGN_KEY, 'fail', $code, $tagged, $wrapped);
        }
        if (str_starts_with($code, 'SQLITE_CONSTRAINT')) {
            return new self(self::KIND_CONSTRAINT, 'fail', $code, $tagged, $wrapped);
        }
        if ($code === 'SQLITE_BUSY' || $code === 'SQLITE_LOCKED') {
            return new self(self::KIND_RETRYABLE, 'retry', $code, $tagged, $wrapped);
        }
        return new self(self::KIND_DRIVER, 'fail', $code, $tagged, $wrapped);
    }

    /**
     * Derive the `SQLITE_*` code family from a PDO error. PDO's `errorInfo[1]` is the SQLite
     * extended result code (int); the message also carries the human family. We prefer the
     * message text (it names the constraint family precisely) and fall back to the extended code.
     * Returns null when the error is not recognizably a SQLite driver error.
     */
    private static function sqliteCodeOf(\PDOException $e): ?string
    {
        $info = $e->errorInfo;
        $extended = is_array($info) && isset($info[1]) && is_int($info[1]) ? $info[1] : null;
        $msg = $e->getMessage();

        // SQLite constraint messages are stable text; map the family from the message.
        if (stripos($msg, 'FOREIGN KEY constraint failed') !== false) {
            return 'SQLITE_CONSTRAINT_FOREIGNKEY';
        }
        if (
            stripos($msg, 'UNIQUE constraint failed') !== false
            || stripos($msg, 'PRIMARY KEY') !== false
            || stripos($msg, 'CHECK constraint failed') !== false
            || stripos($msg, 'NOT NULL constraint failed') !== false
            || stripos($msg, 'constraint failed') !== false
        ) {
            return 'SQLITE_CONSTRAINT';
        }
        if (stripos($msg, 'database is locked') !== false) {
            return 'SQLITE_BUSY';
        }
        // Extended-result-code fallback: 19 = SQLITE_CONSTRAINT, 5 = SQLITE_BUSY, 6 = SQLITE_LOCKED.
        if ($extended === 19) {
            return 'SQLITE_CONSTRAINT';
        }
        if ($extended === 5) {
            return 'SQLITE_BUSY';
        }
        if ($extended === 6) {
            return 'SQLITE_LOCKED';
        }
        // An SQLSTATE HY000 (general SQLite error) with no recognizable family → generic driver.
        if ($e->getCode() !== '' && $e->getCode() !== 0) {
            return 'SQLITE_ERROR';
        }
        return null;
    }
}
