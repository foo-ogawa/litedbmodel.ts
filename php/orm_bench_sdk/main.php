<?php

declare(strict_types=1);

/**
 * Raw-driver SDK-baseline ORM-bench cell (php leg) — the apples-to-apples twin of the native-codegen
 * cell {@see \LiteDbModel\Bench\OrmBench}.
 *
 * Runs the SAME 19 ORM ops over the SAME canonical fixture and the SAME in-memory sqlite storage the
 * native cell uses (`new PDO('sqlite::memory:')`), but every op is HAND-WRITTEN SQL issued straight at
 * PDO. The vendored `litedbmodel_runtime` and the bc-generated `behaviors_generated.php` are NOT loaded
 * and NOT in the path — this file is a self-contained raw-PDO cell (no composer autoload).
 *
 * Fairness (a strawman SDK invalidates the comparison):
 *   - SAME storage: in-memory sqlite (no file → no fsync/WAL the native in-memory cell never pays).
 *   - Prepared-statement REUSE: each op's SQL is prepared once and the PDOStatement cached by SQL text
 *     ($stmts), re-executed with fresh params across iterations — matching the native runtime's
 *     prepared-statement cache, not a re-parse-per-call strawman.
 *   - N+1-FREE relations: parent read → pluck keys → ONE batched child read (WHERE fk IN (…)) → group
 *     in memory, the SAME query counts the native cell proves (nestedFindAll=2, nestedRelations=3,
 *     compositeRelations=3, batch write=1, RETURNING-chained tx = BEGIN + body + COMMIT).
 *   - SAME seed + inputs as the native twin: the small canonical nested fixture (mirrored from OrmBench
 *     — the fixture each isolated cell carries), re-seeded before each op, and the SAME per-op inputs
 *     (findUnique=user1, update id=1, …).
 *
 * Usage:
 *   php orm_bench_sdk/main.php <dialect> <spec> [reps] [warmup]   # print the CSV (cell,dialect,op,iter,us)
 *   php orm_bench_sdk/main.php safety <dialect> <spec>            # assert + print the safety counts
 */

// ── the canonical fixture from the ONE seed SSoT (benchmark/crosslang/.setup/sqlite.json, emitted from
//    orm-domain.ts) — the SAME fixture the native twin loads. Shared TEST DATA, not covered code. ─────
require_once __DIR__ . '/../lm_bench_setup.php';
/**
 * The cached setup doc: `schema` (drop+create, applied once) + `delete`+`insert` (the canonical
 * 110-user fixture, per op). Cached so the JSON is read once.
 *
 * @return array{schema:list<string>,delete:list<string>,insert:list<string>}
 */
function benchSetup(): array
{
    static $doc = null;
    if ($doc === null) {
        $doc = lm_bench_load_setup('sqlite');
    }
    return $doc;
}

const OPS = [
    'findAll', 'filterPaginateSort', 'findFirst', 'findUnique',
    'nestedFindAll', 'nestedFindFirst', 'nestedFindUnique', 'nestedRelations', 'compositeRelations',
    'create', 'update', 'upsert', 'createMany', 'upsertMany', 'updateMany',
    'nestedCreate', 'nestedUpsert', 'nestedUpdate', 'delete',
];

const RELATION_QUERY_COUNTS = [
    'nestedFindAll' => 2, 'nestedFindFirst' => 2, 'nestedFindUnique' => 2,
    'nestedRelations' => 3, 'compositeRelations' => 3,
];
const BATCH_QUERY_COUNTS = ['createMany' => 1, 'upsertMany' => 1, 'updateMany' => 1];
// tx: BEGIN + body + COMMIT. nestedUpsert re-SELECTs the id (upsert has no portable RETURNING) → 5.
const TX_STMT_COUNTS = ['nestedCreate' => 4, 'nestedUpsert' => 5, 'nestedUpdate' => 4, 'delete' => 4];

/**
 * The ONE exec seam. All DB access rides these methods, so the prepared-statement cache and the
 * statement counter (safety proof) each live in one place.
 */
final class Db
{
    /** @var array<string,\PDOStatement> per-SQL prepared-statement cache (reused across iterations) */
    private array $stmts = [];
    public int $count = 0;

    public function __construct(public \PDO $pdo)
    {
    }

    private function prep(string $sql): \PDOStatement
    {
        return $this->stmts[$sql] ??= $this->pdo->prepare($sql);
    }

    /** @param list<mixed> $params @return list<array<int,mixed>> */
    public function query(string $sql, array $params = []): array
    {
        $this->count++;
        $stmt = $this->prep($sql);
        $stmt->execute($params);
        return $stmt->fetchAll(\PDO::FETCH_NUM);
    }

    /** @param list<mixed> $params */
    public function exec(string $sql, array $params = []): void
    {
        $this->count++;
        $this->prep($sql)->execute($params);
    }

    /** param-free control statement (BEGIN / COMMIT). */
    public function execRaw(string $sql): void
    {
        $this->count++;
        $this->pdo->exec($sql);
    }

    /** @param list<mixed> $params */
    public function insertReturningId(string $sql, array $params): int
    {
        $this->count++;
        $this->prep($sql)->execute($params);
        return (int) $this->pdo->lastInsertId();
    }
}

function openDb(string $spec): Db
{
    unset($spec); // sqlite pilot: an IN-MEMORY DB — SAME storage as the native cell.
    $pdo = new \PDO('sqlite::memory:');
    $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    $pdo->setAttribute(\PDO::ATTR_STRINGIFY_FETCHES, false);
    foreach (benchSetup()['schema'] as $stmt) {
        $pdo->exec($stmt);
    }
    return new Db($pdo);
}

function seed(Db $db): void
{
    foreach (array_merge(benchSetup()['delete'], benchSetup()['insert']) as $stmt) {
        $db->pdo->exec($stmt); // runs on the PDO directly (off-seam) → never counted
    }
}

/** @return array{0:list<string>,1:list<string>} */
function batchRows(int $it, bool $stable): array
{
    $emails = [];
    $names = [];
    for ($i = 0; $i < 10; $i++) {
        $emails[] = $stable ? "many{$i}@bench.com" : "many{$it}_{$i}@bench.com";
        $names[] = "Many {$i}";
    }
    return [$emails, $names];
}

function placeholders(int $n): string
{
    return implode(',', array_fill(0, $n, '?'));
}

/** row-tuple IN body sqlite accepts: (VALUES (?,?),(?,?),…). */
function tupleIn(int $rows, int $cols): string
{
    $one = '(' . placeholders($cols) . ')';
    return '(VALUES ' . implode(',', array_fill(0, $rows, $one)) . ')';
}

/** @param list<array<int,mixed>> $rows @return list<int> */
function pluck(array $rows, int $col): array
{
    return array_map(static fn (array $r): int => (int) $r[$col], $rows);
}

/** in-memory stitch by the parent-key column (mirrors the runtime distribute). @param list<array<int,mixed>> $rows */
function groupBy(array $rows, int $keyCol): void
{
    $m = [];
    foreach ($rows as $idx => $r) {
        $m[$r[$keyCol]][] = $idx;
    }
    unset($m);
}

/** @param list<array<int,mixed>> $users */
function nestedPostsFor(Db $db, array $users): void
{
    $ids = pluck($users, 0);
    if ($ids === []) {
        return;
    }
    $sql = 'SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN (' . placeholders(count($ids)) . ') ORDER BY id ASC';
    groupBy($db->query($sql, $ids), 2);
}

/** @param list<array<int,mixed>> $users @return list<int> */
function nestedPostsCollectIds(Db $db, array $users): array
{
    $ids = pluck($users, 0);
    if ($ids === []) {
        return [];
    }
    $sql = 'SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN (' . placeholders(count($ids)) . ') ORDER BY id ASC';
    $posts = $db->query($sql, $ids);
    groupBy($posts, 2);
    return pluck($posts, 0);
}

/** @param list<int> $postIds */
function batchedComments(Db $db, array $postIds): void
{
    if ($postIds === []) {
        return;
    }
    $sql = 'SELECT id, body, post_id FROM benchmark_comments WHERE post_id IN (' . placeholders(count($postIds)) . ') ORDER BY id ASC';
    groupBy($db->query($sql, $postIds), 2);
}

function compositeRelations(Db $db): void
{
    $tusers = $db->query('SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = ? ORDER BY user_id ASC', [1]);
    if ($tusers === []) {
        return;
    }
    $psql = 'SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE (tenant_id, user_id) IN ' . tupleIn(count($tusers), 2);
    $pparams = [];
    foreach ($tusers as $r) {
        $pparams[] = (int) $r[0];
        $pparams[] = (int) $r[1];
    }
    $tposts = $db->query($psql, $pparams);
    if ($tposts === []) {
        return;
    }
    $csql = 'SELECT tenant_id, comment_id, post_id, body FROM benchmark_tenant_comments WHERE (tenant_id, post_id) IN ' . tupleIn(count($tposts), 2);
    $cparams = [];
    foreach ($tposts as $r) {
        $cparams[] = (int) $r[0];
        $cparams[] = (int) $r[1];
    }
    $db->query($csql, $cparams);
}

function updateMany(Db $db): void
{
    [, $names] = batchRows(0, false);
    $whens = '';
    $params = [];
    for ($k = 0; $k < 10; $k++) {
        $whens .= ' WHEN ? THEN ?';
        $params[] = $k + 1;
        $params[] = $names[$k];
    }
    for ($k = 0; $k < 10; $k++) {
        $params[] = $k + 1;
    }
    $sql = 'UPDATE benchmark_users SET name = CASE id' . $whens . ' END WHERE id IN (' . placeholders(10) . ')';
    $db->exec($sql, $params);
}

/** @param list<string> $emails @param list<string> $names */
function batchInsert(Db $db, array $emails, array $names, string $conflict): void
{
    $tuples = implode(',', array_fill(0, 10, '(?, ?)'));
    $params = [];
    for ($k = 0; $k < 10; $k++) {
        $params[] = $emails[$k];
        $params[] = $names[$k];
    }
    $db->exec('INSERT INTO benchmark_users (email, name) VALUES ' . $tuples . $conflict, $params);
}

/**
 * The 19 ops (native-cell order). Fixed inputs mirror the php native cell; mutating ops vary their
 * UNIQUE column by $it. Read LIMIT/ORDER shapes match the ops SSoT (== the native generated SQL).
 */
function runOp(Db $db, string $op, int $it): void
{
    switch ($op) {
        case 'findAll':
            $db->query('SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100');
            break;
        case 'filterPaginateSort':
            $db->query('SELECT id, title, content, published, author_id, created_at FROM benchmark_posts '
                . 'WHERE published = ? ORDER BY created_at DESC LIMIT 20 OFFSET 10', [1]);
            break;
        case 'findFirst':
            $db->query('SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1', ['User%']);
            break;
        case 'findUnique':
            $db->query('SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1', ['user1@example.com']);
            break;
        case 'nestedFindAll':
            nestedPostsFor($db, $db->query('SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100'));
            break;
        case 'nestedFindFirst':
            nestedPostsFor($db, $db->query('SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1', ['User%']));
            break;
        case 'nestedFindUnique':
            nestedPostsFor($db, $db->query('SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1', ['user1@example.com']));
            break;
        case 'nestedRelations':
            $users = $db->query('SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100');
            batchedComments($db, nestedPostsCollectIds($db, $users));
            break;
        case 'compositeRelations':
            compositeRelations($db);
            break;
        case 'create':
            $db->exec('INSERT INTO benchmark_users (email, name) VALUES (?, ?)', ["new{$it}@bench.com", 'New']);
            break;
        case 'update':
            $db->exec('UPDATE benchmark_users SET name = ? WHERE id = ?', ['Updated 1', 1]);
            break;
        case 'upsert':
            $db->exec('INSERT INTO benchmark_users (email, name) VALUES (?, ?) '
                . 'ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name',
                ['user1@example.com', 'Upserted One']);
            break;
        case 'createMany':
            [$emails, $names] = batchRows($it, false);
            batchInsert($db, $emails, $names, '');
            break;
        case 'upsertMany':
            $emails = ['user1@example.com', 'user2@example.com'];
            for ($k = 0; $k < 8; $k++) {
                $emails[] = "many{$k}@bench.com";
            }
            [, $names] = batchRows($it, true);
            batchInsert($db, $emails, $names, ' ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name');
            break;
        case 'updateMany':
            updateMany($db);
            break;
        case 'nestedCreate':
            $db->execRaw('BEGIN');
            $uid = $db->insertReturningId('INSERT INTO benchmark_users (email, name) VALUES (?, ?)', ["nc{$it}@bench.com", 'NC']);
            $db->exec('INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)', [$uid, 'NC Post']);
            $db->execRaw('COMMIT');
            break;
        case 'nestedUpsert':
            $db->execRaw('BEGIN');
            $db->exec('INSERT INTO benchmark_users (email, name) VALUES (?, ?) '
                . 'ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name',
                ['user1@example.com', 'NUp']);
            $rows = $db->query('SELECT id FROM benchmark_users WHERE email = ?', ['user1@example.com']);
            $db->exec('INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)', [(int) $rows[0][0], 'NUp Post']);
            $db->execRaw('COMMIT');
            break;
        case 'nestedUpdate':
            $db->execRaw('BEGIN');
            $db->exec('UPDATE benchmark_users SET name = ? WHERE id = ?', ['NU', 1]);
            $db->exec('UPDATE benchmark_posts SET title = ? WHERE author_id = ?', ['NU Post', 1]);
            $db->execRaw('COMMIT');
            break;
        case 'delete':
            $db->execRaw('BEGIN');
            $uid = $db->insertReturningId('INSERT INTO benchmark_users (email, name) VALUES (?, ?)', ["del{$it}@bench.com", 'Del']);
            $db->exec('DELETE FROM benchmark_users WHERE id = ?', [$uid]);
            $db->execRaw('COMMIT');
            break;
        default:
            throw new \RuntimeException("unknown op {$op}");
    }
}

function measure(string $dialect, string $spec, int $reps, int $warmup): void
{
    $db = openDb($spec);
    echo "cell,dialect,op,iter,us\n";
    foreach (OPS as $op) {
        seed($db); // re-seed before each op (matches the native cell)
        for ($it = 0; $it < $warmup; $it++) {
            runOp($db, $op, $it);
        }
        for ($it = 0; $it < $reps; $it++) {
            $g = $it + $warmup;
            $t = hrtime(true);
            runOp($db, $op, $g);
            $us = intdiv(hrtime(true) - $t, 1000);
            echo "sdk,{$dialect},{$op},{$it},{$us}\n";
        }
    }
}

function safety(string $dialect, string $spec): void
{
    unset($dialect);
    $db = openDb($spec);
    $expected = RELATION_QUERY_COUNTS + BATCH_QUERY_COUNTS + TX_STMT_COUNTS;
    foreach ($expected as $op => $want) {
        seed($db);
        $db->count = 0;
        runOp($db, $op, 0);
        $got = $db->count;
        if ($got !== $want) {
            throw new \RuntimeException("{$op} statement-count regression: got {$got}, expect {$want}");
        }
        $kind = isset(TX_STMT_COUNTS[$op]) ? 'statements (BEGIN + body + COMMIT)' : 'queries';
        echo "{$op} {$kind}={$got} (expect {$want})\n";
    }
}

// ── argv dispatch ────────────────────────────────────────────────────────────────────────────────
$args = $_SERVER['argv'];
array_shift($args); // drop the script name
if (($args[0] ?? null) === 'safety') {
    safety($args[1] ?? 'sqlite', $args[2] ?? 'sqlite');
    return;
}
$dialect = $args[0] ?? 'sqlite';
$spec = $args[1] ?? 'sqlite';
$reps = isset($args[2]) ? (int) $args[2] : 300;
$warmup = isset($args[3]) ? (int) $args[3] : 30;
measure($dialect, $spec, $reps, $warmup);
