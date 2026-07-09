<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — LIVE PostgreSQL / MySQL PDO drivers (WS7g, #36).
 *
 * The PHP leg of the coordinated cross-language live-DB validation pass. The {@see Runtime} /
 * {@see WriteRuntime} handler seam already takes ANY `\PDO`; this file supplies two live `\PDO`
 * SUBCLASSES that adapt the two dialect divergences a raw PDO can't absorb, so the runtime stays
 * UNCHANGED:
 *
 *   - Postgres ({@see PgLivePdo}): the `postgres`-tagged bundle renders `$N` placeholders (the
 *     Render final-pass), but PDO_pgsql binds `?` positionally (it does NOT translate `$N`). This
 *     subclass rewrites `$N`→`?` in `prepare()`/`exec()` before handing SQL to the real driver.
 *     RETURNING is native on PG, so a RETURNING row comes back through the normal fetch.
 *
 *   - MySQL ({@see MysqlLivePdo} + {@see MysqlReturningStatement}): the `mysql`-tagged bundle
 *     renders `?` (native to PDO_mysql — no rewrite), but MySQL 8.0 has NO `RETURNING`. The
 *     statement subclass emulates it at the seam (strip RETURNING → run the INSERT → re-select the
 *     AUTO_INCREMENT PK's columns) — the dialect-behavior-by-convention the WS6 TS ScpDialect uses.
 *
 * Both run with PDO autocommit ON so the WriteRuntime's explicit `BEGIN`/`COMMIT`/`ROLLBACK`
 * envelope drives a REAL transaction on the live DB (the gate-first write-tx).
 */
final class LiveDb
{
    /** Connect to a live Postgres, returning a placeholder-adapting `\PDO`. */
    public static function postgres(string $host, int $port, string $user, string $password, string $dbname): \PDO
    {
        $dsn = "pgsql:host={$host};port={$port};dbname={$dbname}";
        return new PgLivePdo($dsn, $user, $password, [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
        ]);
    }

    /** Connect to a live MySQL, returning a RETURNING-emulating `\PDO`. */
    public static function mysql(string $host, int $port, string $user, string $password, string $dbname): \PDO
    {
        // 127.0.0.1 (not "localhost") forces a TCP connection to the published container port.
        $dsn = "mysql:host={$host};port={$port};dbname={$dbname}";
        // Native prepares (emulate OFF) so an integer param binds over the binary protocol as an
        // integer — MySQL rejects a QUOTED `LIMIT '20'`, which emulated prepares would produce.
        $pdo = new MysqlLivePdo($dsn, $user, $password, [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_EMULATE_PREPARES => false,
        ]);
        $pdo->setAttribute(\PDO::ATTR_STATEMENT_CLASS, [MysqlReturningStatement::class, [$pdo]]);
        return $pdo;
    }
}

/**
 * A `\PDO` that rewrites Postgres `$N` placeholders to `?` before the real driver sees them.
 * Render numbers `$1..$N` left-to-right, so a plain positional `?` swap preserves bind order.
 */
final class PgLivePdo extends \PDO
{
    private static function rewrite(string $sql): string
    {
        // Every `$<digits>` on the compiled surface is a bound param position (the render pipeline
        // never emits a `$N` inside a string literal), so a global replace is safe.
        return preg_replace('/\$\d+/', '?', $sql) ?? $sql;
    }

    #[\ReturnTypeWillChange]
    public function prepare(string $query, array $options = []): \PDOStatement|false
    {
        return parent::prepare(self::rewrite($query), $options);
    }

    #[\ReturnTypeWillChange]
    public function exec(string $statement): int|false
    {
        return parent::exec(self::rewrite($statement));
    }
}

/**
 * A `\PDO` for MySQL that emulates `INSERT … RETURNING`. With native prepares (emulate OFF) a
 * server-side prepare of a RETURNING statement fails at `prepare()` time — BEFORE any statement
 * override could run. So `prepare()` itself intercepts: it parses + STRIPS the RETURNING clause,
 * stashes the `{table, cols}` request in {@see $pendingReturning}, and server-prepares the plain
 * INSERT. The {@see MysqlReturningStatement} its statements are, reads that pending slot on
 * `execute()` to run the INSERT then re-select the inserted PK's columns. Statements in the
 * gate-first tx run sequentially, so a single pending slot is race-free.
 */
final class MysqlLivePdo extends \PDO
{
    /** @var array{table:string, cols:string}|null the RETURNING request for the NEXT execute(). */
    public ?array $pendingReturning = null;

    #[\ReturnTypeWillChange]
    public function prepare(string $query, array $options = []): \PDOStatement|false
    {
        if (preg_match('/^\s*INSERT\s+(?:IGNORE\s+)?INTO\s+([A-Za-z_][A-Za-z0-9_]*)\b.*\bRETURNING\s+(.+?)\s*$/is', $query, $m) === 1) {
            $insertSql = preg_replace('/\s+RETURNING\s+.+$/is', '', $query) ?? $query;
            $this->pendingReturning = ['table' => $m[1], 'cols' => $m[2]];
            return parent::prepare($insertSql, $options);
        }
        $this->pendingReturning = null;
        return parent::prepare($query, $options);
    }
}

/**
 * A `\PDOStatement` that, when its owning {@see MysqlLivePdo} flagged a pending RETURNING request
 * at prepare time, executes the (RETURNING-stripped) INSERT, captures `lastInsertId()`, and
 * re-selects the requested columns by the AUTO_INCREMENT PK — the MySQL RETURNING emulation. A
 * plain statement behaves normally.
 */
final class MysqlReturningStatement extends \PDOStatement
{
    private MysqlLivePdo $pdo;
    /** @var list<\stdClass>|null cached re-selected RETURNING rows (null = passthrough). */
    private ?array $returningRows = null;
    /** @var array{table:string, cols:string}|null captured at construction from the PDO's slot. */
    private ?array $returning;

    protected function __construct(MysqlLivePdo $pdo)
    {
        $this->pdo = $pdo;
        // The PDO set its pending slot in prepare() immediately before constructing this statement.
        $this->returning = $pdo->pendingReturning;
        $pdo->pendingReturning = null;
    }

    #[\ReturnTypeWillChange]
    public function execute(?array $params = null): bool
    {
        $bound = $params === null ? null : array_values($params);
        $ok = parent::execute($bound); // the RETURNING-stripped INSERT
        if ($this->returning !== null) {
            $lastId = (int) $this->pdo->lastInsertId();
            $sel = $this->pdo->prepare("SELECT {$this->returning['cols']} FROM {$this->returning['table']} WHERE id = ?");
            $sel->execute([$lastId]);
            $rows = $sel->fetchAll(\PDO::FETCH_OBJ);
            $this->returningRows = is_array($rows) ? array_values($rows) : [];
        } else {
            $this->returningRows = null;
        }
        return $ok;
    }

    #[\ReturnTypeWillChange]
    public function fetchAll(int $mode = \PDO::FETCH_DEFAULT, ...$args): array
    {
        if ($this->returningRows !== null) {
            return $this->returningRows;
        }
        return parent::fetchAll($mode, ...$args);
    }
}
