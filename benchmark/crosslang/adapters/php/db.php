<?php

declare(strict_types=1);

/**
 * Raw PDO driver seam for the php SDK baseline (hand-SQL) — the fair 1.0x denominator, + the shared
 * `dialect_of` / target parsing the ir cell reuses. The php twin of adapters/py/db.py.
 *
 * Dialect is detected from the ONE target string verify-cells passes (a sqlite file path, a libpq
 * `key=val` conninfo for postgres, or a `mysql://` URL for mysql) — the same shape the rust/go/py
 * cells parse. The live pg/mysql PDOs are opened through litedbmodel's own {@see LiteDbModel\Runtime\LiveDb}
 * factory (the ONLY sanctioned way to open the dockerized dialect connections — pg `$N`→`?` rewrite is
 * a no-op on the SDK's `?`-only SQL, and the mysql RETURNING emulation never fires since the SDK does an
 * explicit re-select instead of RETURNING); sqlite is a plain `new PDO`.
 */

require_once __DIR__ . '/runtime_bootstrap.php';

use LiteDbModel\Runtime\LiveDb;

function dialect_of(string $target): string
{
    if (str_starts_with($target, 'mysql://')) {
        return 'mysql';
    }
    if (str_contains($target, 'dbname=') || (str_contains($target, 'host=') && str_contains($target, 'port='))) {
        return 'postgres';
    }
    return 'sqlite';
}

/**
 * mysql://user:pass@host:port/db → connect kwargs.
 *
 * @return array{host:string, port:int, user:string, password:string, database:string}
 */
function parse_mysql_url(string $url): array
{
    $rest = substr($url, strlen('mysql://'));
    [$cred, $hostpart] = explode('@', $rest, 2);
    [$user, $password] = explode(':', $cred, 2);
    [$hostport, $db] = explode('/', $hostpart, 2);
    [$host, $port] = explode(':', $hostport, 2);
    return ['host' => $host, 'port' => (int) $port, 'user' => $user, 'password' => $password, 'database' => $db];
}

/**
 * libpq `key=val` conninfo → connect kwargs (host/port/user/password/dbname).
 *
 * @return array<string,string>
 */
function parse_pg_conninfo(string $conninfo): array
{
    $kv = [];
    foreach (preg_split('/\s+/', trim($conninfo)) as $tok) {
        if ($tok === '') {
            continue;
        }
        [$k, $v] = explode('=', $tok, 2);
        $kv[$k] = $v;
    }
    return $kv;
}

/** Open the dialect's PDO from a verify-cells target string (sqlite path / pg conninfo / mysql URL). */
function open_pdo(string $target): \PDO
{
    $dialect = dialect_of($target);
    if ($dialect === 'postgres') {
        $kv = parse_pg_conninfo($target);
        return LiveDb::postgres($kv['host'] ?? 'localhost', (int) ($kv['port'] ?? '5432'), $kv['user'], $kv['password'], $kv['dbname']);
    }
    if ($dialect === 'mysql') {
        $c = parse_mysql_url($target);
        // PDO_mysql maps the host "localhost" to a UNIX socket; the dockerized MySQL is a TCP port, so
        // force TCP with 127.0.0.1 (the exact convention LiveDb.php documents + livedb_runner defaults to).
        $host = $c['host'] === 'localhost' ? '127.0.0.1' : $c['host'];
        return LiveDb::mysql($host, $c['port'], $c['user'], $c['password'], $c['database']);
    }
    $pdo = new \PDO("sqlite:{$target}");
    $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    $pdo->exec('PRAGMA foreign_keys = ON');
    return $pdo;
}

/** A single-connection raw PDO over the dialect (the sdk cell's hand-SQL driver). */
final class RawDb
{
    public string $dialect;
    public \PDO $pdo;

    public function __construct(string $target)
    {
        $this->dialect = dialect_of($target);
        $this->pdo = open_pdo($target);
    }

    /**
     * @param list<mixed> $params
     * @return list<array<string,mixed>>
     */
    public function query(string $sql, array $params = []): array
    {
        $st = $this->pdo->prepare($sql);
        $st->execute(array_values($params));
        return $st->fetchAll(\PDO::FETCH_ASSOC);
    }

    /**
     * Run a write, return the affected-row count. Deliberately does NOT fetch a last-insert-id: on pg
     * `lastInsertId()` runs `lastval()`, which ERRORS when no sequence was touched this session and —
     * inside a BEGIN — aborts the whole transaction. The one place that needs the auto-increment id
     * (a mysql insert) reads `$this->pdo->lastInsertId()` itself, where that call is safe & meaningful.
     * (The py twin returns psycopg's `cursor.lastrowid`, which is simply `None` on pg — same net effect.)
     *
     * @param list<mixed> $params
     */
    public function execute(string $sql, array $params = []): int
    {
        $st = $this->pdo->prepare($sql);
        $st->execute(array_values($params));
        return $st->rowCount();
    }

    public function begin(): void
    {
        $this->pdo->exec('BEGIN');
    }

    public function commit(): void
    {
        $this->pdo->exec('COMMIT');
    }

    public function rollback(): void
    {
        $this->pdo->exec('ROLLBACK');
    }
}
