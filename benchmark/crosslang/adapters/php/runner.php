<?php

declare(strict_types=1);

/**
 * litedbmodel cross-language adapter RUNNER — PHP (epic #44).
 *
 * Speaks the line-delimited JSON contract (../../contract.ts) over stdin/stdout for the
 * three PHP cells: sql / codegen / ir.
 *
 *   sql     — hand-optimized raw SQL via PDO sqlite (baseline 1.0x)
 *   codegen — the makeSQL bundle resident + integrity-verified ONCE at load, executed via
 *             the LiteDbModel\Runtime thin runtime
 *   ir      — the bundle loaded FROM the generated JSON on disk, executed via the SAME runtime
 *
 * Consumes generated/bundles.json (the language-neutral §8 artifact) unchanged.
 */

use LiteDbModel\Runtime\Runtime;
use LiteDbModel\Runtime\Relation;

$HERE = __DIR__;
$REPO = dirname($HERE, 4);
require $REPO . '/php/vendor/autoload.php';

$BUNDLES_PATH = dirname($HERE, 2) . '/generated/bundles.json';

$impl = 'sql';
foreach ($argv as $a) {
    if (str_starts_with($a, '--impl=')) {
        $impl = substr($a, 7);
    }
}

$rawArtifact = file_get_contents($BUNDLES_PATH);
$artifact = json_decode($rawArtifact); // stdClass
$CASES = [];
foreach ($artifact->cases as $c) {
    $CASES[$c->case] = $c;
}
$SCHEMA = $artifact->schema;
$SEED = $artifact->seed;

/** codegen: verify each baked bundle's integrity once (fail-closed) + keep resident.
 *  ir: reparse the JSON from disk. Same case map; the difference is the cold-start check + source. */
function loadBundles(string $impl, string $raw, object $artifact, array $cases): array
{
    if ($impl === 'codegen') {
        foreach ($artifact->cases as $c) {
            // integrity hash of the resident bundle (the baked-artifact fail-closed check)
            $c->_integrity = 'fp:' . substr(hash('sha256', json_encode($c->bundle)), 0, 16);
        }
        return $cases;
    }
    if ($impl === 'ir') {
        $reparsed = json_decode($raw);
        $m = [];
        foreach ($reparsed->cases as $c) {
            $m[$c->case] = $c;
        }
        return $m;
    }
    return $cases;
}

$BUNDLES = loadBundles($impl, $rawArtifact, $artifact, $CASES);

// ── seeded PDO ────────────────────────────────────────────────────────────────
function seedDb(array $schema, array $seed): PDO
{
    $db = new PDO('sqlite::memory:');
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $db->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, false);
    $db->exec('PRAGMA foreign_keys = ON');
    foreach ($schema as $s) {
        $db->exec((string) $s);
    }
    foreach ($seed as $s) {
        $db->exec((string) $s);
    }
    return $db;
}

// ── sql baseline (hand-optimized raw SQL via PDO) ─────────────────────────────
function sqlOp(string $caseId, PDO $db, array $cases): callable
{
    switch ($caseId) {
        case 'find':
            return function () use ($db) {
                $s = $db->prepare('SELECT id, author_id, title, status, views, created_at FROM posts WHERE author_id = ? AND status = ? AND created_at >= ? ORDER BY id ASC');
                $s->execute([1, 'live', '2026-02-01']);
                $s->fetchAll(PDO::FETCH_ASSOC);
            };
        case 'complexWhere':
            return function () use ($db) {
                $s = $db->prepare('SELECT id, author_id, title, status, views FROM posts WHERE author_id = ? AND created_at >= ? AND title LIKE ? AND id IN (?, ?, ?, ?, ?) ORDER BY id ASC');
                $s->execute([1, '2026-02-01', 'post-%', 1, 2, 3, 4, 5]);
                $s->fetchAll(PDO::FETCH_ASSOC);
            };
        case 'inList':
            return function () use ($db) {
                $ids = range(1, 10);
                $ph = implode(', ', array_fill(0, count($ids), '?'));
                $s = $db->prepare("SELECT id, title FROM posts WHERE id IN ($ph) ORDER BY id ASC");
                $s->execute($ids);
                $s->fetchAll(PDO::FETCH_ASSOC);
            };
        case 'belongsTo':
            return function () use ($db) {
                $s = $db->prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC');
                $s->execute([1]);
                $posts = $s->fetchAll(PDO::FETCH_ASSOC);
                $aids = array_values(array_unique(array_map(fn ($r) => $r['author_id'], $posts)));
                $ph = implode(', ', array_fill(0, count($aids), '?'));
                $s2 = $db->prepare("SELECT id, name FROM users WHERE id IN ($ph)");
                $s2->execute($aids);
                $s2->fetchAll(PDO::FETCH_ASSOC);
            };
        case 'hasMany':
            return function () use ($db) {
                $s = $db->prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC');
                $s->execute([1]);
                $posts = $s->fetchAll(PDO::FETCH_ASSOC);
                $ids = array_map(fn ($r) => $r['id'], $posts);
                $ph = implode(', ', array_fill(0, count($ids), '?'));
                $s2 = $db->prepare("SELECT id, post_id, body FROM comments WHERE post_id IN ($ph)");
                $s2->execute($ids);
                $s2->fetchAll(PDO::FETCH_ASSOC);
            };
        case 'hasManyLimit':
            return function () use ($db) {
                $s = $db->prepare('SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC');
                $s->execute([1]);
                $posts = $s->fetchAll(PDO::FETCH_ASSOC);
                $ids = array_map(fn ($r) => $r['id'], $posts);
                $ph = implode(', ', array_fill(0, count($ids), '?'));
                $s2 = $db->prepare("SELECT id, post_id, body FROM (SELECT id, post_id, body, ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY id DESC) rn FROM comments WHERE post_id IN ($ph)) WHERE rn <= 3");
                $s2->execute($ids);
                $s2->fetchAll(PDO::FETCH_ASSOC);
            };
        case 'batchInsert':
            $rows = $cases['batchInsert']->input->rows;
            return function () use ($db, $rows) {
                $cols = ['author_id', 'title', 'status', 'views', 'created_at'];
                $vals = implode(',', array_fill(0, count($rows), '(' . implode(',', array_fill(0, count($cols), '?')) . ')'));
                $flat = [];
                foreach ($rows as $r) {
                    foreach ($cols as $c) {
                        $flat[] = $r->$c;
                    }
                }
                $s = $db->prepare('INSERT INTO posts (' . implode(',', $cols) . ") VALUES $vals");
                $s->execute($flat);
            };
        case 'writeTxGate':
            $inp = $cases['writeTxGate']->input;
            return function () use ($db, $inp) {
                $db->beginTransaction();
                $g = $db->prepare('SELECT 1 FROM users WHERE id = ?');
                $g->execute([$inp->author_id]);
                if ($g->fetchAll(PDO::FETCH_ASSOC) === []) {
                    $db->rollBack();
                    throw new RuntimeException('requires_absent');
                }
                $u = $db->prepare('INSERT INTO uniq (name, s0, f0) VALUES (?, ?, ?) ON CONFLICT DO NOTHING');
                $u->execute(['title_per_author', (string) $inp->author_id, $inp->title]);
                $b = $db->prepare('INSERT INTO posts (author_id, title, created_at) VALUES (?, ?, ?) RETURNING id, author_id, title');
                $b->execute([$inp->author_id, $inp->title, $inp->created_at]);
                $b->fetchAll(PDO::FETCH_ASSOC);
                $d = $db->prepare('UPDATE users SET post_count = post_count + ? WHERE id = ?');
                $d->execute([1, $inp->author_id]);
                $db->commit();
            };
        default:
            throw new RuntimeException("unknown case $caseId");
    }
}

// ── litedbmodel runtime (codegen / ir) op ─────────────────────────────────────
function lmOp(string $caseId, PDO $db, array $bundles): callable
{
    $c = $bundles[$caseId];
    $bundle = $c->bundle;
    $kind = $c->kind;
    $inp = (array) $c->input;
    if ($kind === 'batch' || $kind === 'tx') {
        $scope = $kind === 'tx' ? $inp : [];
        return fn () => Runtime::executeTransactionBundle($bundle, $scope, $db);
    }
    if ($kind === 'relation') {
        $withName = $c->withRelation;
        return fn () => Relation::readBundle($bundle, $inp, $db, [$withName]);
    }
    return fn () => Runtime::executeBundle($bundle, $inp, $db);
}

function makeOp(string $impl, string $caseId, PDO $db, array $cases, array $bundles): callable
{
    if ($impl === 'sql') {
        return sqlOp($caseId, $db, $cases);
    }
    return lmOp($caseId, $db, $bundles);
}

// ── fairness cost probe: DML statements + rows read (tx-control excluded) ──────
// PDO's ATTR_STATEMENT_CLASS lets us install a custom PDOStatement subclass that
// counts execute()/fetchAll() through a shared counter — the sanctioned PDO hook
// (prepare()'s return type can't be overridden). tx-control statements (BEGIN /
// COMMIT / PRAGMA) are recognized and NOT counted.
final class CostCounter
{
    public int $queries = 0;
    public int $rows = 0;
}

final class CountingStatement extends PDOStatement
{
    private CostCounter $counter;
    private bool $isDml = true;

    protected function __construct(CostCounter $counter)
    {
        $this->counter = $counter;
        $this->isDml = !preg_match('/^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE|PRAGMA)\b/i', $this->queryString);
    }

    public function execute(?array $params = null): bool
    {
        if ($this->isDml) {
            $this->counter->queries++;
        }
        return parent::execute($params);
    }

    public function fetchAll(int $mode = PDO::FETCH_DEFAULT, ...$args): array
    {
        $r = parent::fetchAll($mode, ...$args);
        if ($this->isDml) {
            $this->counter->rows += count($r);
        }
        return $r;
    }
}

function cost(string $impl, string $caseId, array $schema, array $seed, array $cases, array $bundles): array
{
    $counter = new CostCounter();
    $db = new PDO('sqlite::memory:');
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $db->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, false);
    $db->exec('PRAGMA foreign_keys = ON');
    foreach ($schema as $s) {
        $db->exec((string) $s);
    }
    foreach ($seed as $s) {
        $db->exec((string) $s);
    }
    // Only NOW install the counting statement class, so seed inserts aren't counted.
    $db->setAttribute(PDO::ATTR_STATEMENT_CLASS, [CountingStatement::class, [$counter]]);
    $op = makeOp($impl, $caseId, $db, $cases, $bundles);
    $op();
    return [$counter->queries, $counter->rows];
}

// ── micro-bench: mock statement (fixed rows, no round-trip) ────────────────────
// A real PDO prepares the SQL (cheap parse) but a MockStatement subclass — installed
// via ATTR_STATEMENT_CLASS — SHORT-CIRCUITS execute()/fetchAll() to return fixed
// fixtures with NO DB round-trip, so the timed op is ONLY the client-side path
// (render/bind/`?`→`$N`/hydration). Transaction control (begin/commit) is a no-op.
final class MockStatement extends PDOStatement
{
    protected function __construct()
    {
    }

    public function execute(?array $params = null): bool
    {
        return true;
    }

    public function fetchAll(int $mode = PDO::FETCH_DEFAULT, ...$args): array
    {
        $rows = mockFixture($this->queryString);
        // The runtime fetches FETCH_OBJ (stdClass); the raw-SQL baseline fetches FETCH_ASSOC
        // (arrays). Honor the requested mode so both paths get their expected row shape.
        if ($mode === PDO::FETCH_ASSOC) {
            return array_map(fn ($r) => (array) $r, $rows);
        }
        return $rows;
    }

    public function fetch(int $mode = PDO::FETCH_DEFAULT, $cursorOrientation = PDO::FETCH_ORI_NEXT, int $cursorOffset = 0): mixed
    {
        $rows = mockFixture($this->queryString);
        $r = $rows[0] ?? false;
        if ($r !== false && $mode === PDO::FETCH_ASSOC) {
            return (array) $r;
        }
        return $r;
    }

    public function rowCount(): int
    {
        return count(mockFixture($this->queryString));
    }
}

final class MockPDO extends PDO
{
    public function __construct(array $schema = [])
    {
        parent::__construct('sqlite::memory:');
        $this->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        // The schema must EXIST so sqlite can PREPARE (compile) each statement; execute()
        // and fetchAll() are then short-circuited to fixtures (no real round-trip). Tables
        // stay EMPTY — the fixtures come from MockStatement, not the DB.
        foreach ($schema as $s) {
            parent::exec((string) $s);
        }
        $this->setAttribute(PDO::ATTR_STATEMENT_CLASS, [MockStatement::class]);
    }

    public function beginTransaction(): bool
    {
        return true;
    }

    public function commit(): bool
    {
        return true;
    }

    public function rollBack(): bool
    {
        return true;
    }
}

function mockFixture(string $sql): array
{
    static $posts = null, $comments = null, $users = null;
    if ($posts === null) {
        // FETCH_OBJ shape — the runtime read handler fetches PDO::FETCH_OBJ (stdClass rows).
        $posts = [];
        for ($i = 1; $i <= 5; $i++) {
            $posts[] = (object) ['id' => $i, 'author_id' => 1, 'title' => "post-$i", 'status' => 'live', 'views' => $i * 10, 'created_at' => '2026-02-01'];
        }
        $comments = [];
        for ($i = 1; $i <= 25; $i++) {
            $comments[] = (object) ['id' => $i, 'post_id' => (($i - 1) % 5) + 1, 'body' => "comment-$i"];
        }
        $users = [(object) ['id' => 1, 'name' => 'user-1']];
    }
    $s = strtolower($sql);
    if (str_starts_with(ltrim($s), 'select')) {
        if (str_contains($s, 'from comments')) {
            return $comments;
        }
        if (str_contains($s, 'from users')) {
            return $users;
        }
        if (str_contains($s, 'from posts')) {
            return $posts;
        }
        if (str_contains($s, 'from ')) {
            return $posts;
        }
        return [(object) ['1' => 1]];
    }
    if (str_contains($s, 'returning')) {
        return [(object) ['id' => 41, 'author_id' => 1, 'title' => 'txn-post']];
    }
    return [];
}

// ── timing ─────────────────────────────────────────────────────────────────────
function collect(callable $op, int $warmup, int $iterations): array
{
    for ($i = 0; $i < $warmup; $i++) {
        $op();
    }
    $samples = [];
    for ($i = 0; $i < $iterations; $i++) {
        $t0 = hrtime(true);
        $op();
        $samples[] = (hrtime(true) - $t0) / 1e6; // ns → ms
    }
    return $samples;
}

function writeMsg(array $obj): void
{
    fwrite(STDOUT, json_encode($obj) . "\n");
}

function handle(array $req, string $impl, array $schema, array $seed, array $cases, array $bundles): void
{
    $kind = $req['kind'];
    if ($kind === 'run') {
        $db = seedDb($schema, $seed);
        $op = makeOp($impl, $req['case'], $db, $cases, $bundles);
        $samples = collect($op, $req['warmup'], $req['iterations']);
        writeMsg(['kind' => 'run', 'case' => $req['case'], 'samplesMs' => $samples]);
    } elseif ($kind === 'throughput') {
        $db = seedDb($schema, $seed);
        $op = makeOp($impl, $req['case'], $db, $cases, $bundles);
        $t0 = hrtime(true);
        $completed = 0;
        for ($i = 0; $i < $req['iterations']; $i++) {
            $op();
            $completed++;
        }
        $elapsed = (hrtime(true) - $t0) / 1e6;
        writeMsg(['kind' => 'throughput', 'case' => $req['case'], 'elapsedMs' => $elapsed, 'completed' => $completed]);
    } elseif ($kind === 'micro') {
        $db = new MockPDO($schema);
        $op = makeOp($impl, $req['case'], $db, $cases, $bundles);
        $samples = collect($op, $req['warmup'], $req['iterations']);
        writeMsg(['kind' => 'micro', 'case' => $req['case'], 'samplesMs' => $samples]);
    } elseif ($kind === 'rss') {
        writeMsg(['kind' => 'rss', 'rssBytes' => memory_get_usage(true)]);
    } elseif ($kind === 'cost') {
        [$q, $r] = cost($impl, $req['case'], $schema, $seed, $cases, $bundles);
        writeMsg(['kind' => 'cost', 'case' => $req['case'], 'queries' => $q, 'rows' => $r]);
    } elseif ($kind === 'shutdown') {
        exit(0);
    }
}

writeMsg(['kind' => 'ready', 'language' => 'php', 'impl' => $impl, 'readyAtEpochMs' => microtime(true) * 1000.0]);

$in = fopen('php://stdin', 'r');
while (($line = fgets($in)) !== false) {
    $line = trim($line);
    if ($line === '') {
        continue;
    }
    $req = json_decode($line, true);
    if ($req === null) {
        writeMsg(['kind' => 'error', 'message' => 'bad request line']);
        continue;
    }
    try {
        handle($req, $impl, $SCHEMA, $SEED, $CASES, $BUNDLES);
    } catch (\Throwable $e) {
        writeMsg(['kind' => 'error', 'message' => $e->getMessage(), 'stack' => $e->getTraceAsString()]);
    }
}
