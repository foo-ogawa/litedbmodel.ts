<?php

declare(strict_types=1);

/**
 * The php SDK BASELINE — raw PDO + hand-SQL for benchmark_* (the fair 1.0x denominator).
 *
 * The php twin of adapters/py/sdk.py + the go SDK cell (adapters/go/sdk.go): reads rows DIRECTLY by
 * column name (NOT via the litedbmodel runtime — that is the ir cell). All placeholders are PDO
 * positional `?`; the child IN-clause is an `IN (?, …)` list on ALL dialects (PDO_pgsql binds an
 * `IN (?,…)` list fine, so no pg array-bind needed). `filterPaginateSort` binds `published` = true on
 * postgres (a boolean column — pg is strict) and 1 on sqlite/mysql. mysql has NO RETURNING, so upsert +
 * tx-chain inserts do an explicit re-select (mirror the seam behavior, not the code). v1-faithful
 * returning (upsert → [{id}], no-returning writes → null, tx → {committed,state}).
 */

require_once __DIR__ . '/canon.php';
require_once __DIR__ . '/db.php';

/** @return list<string> */
function bench_batch_emails(): array
{
    return array_map(static fn ($i) => "many{$i}@bench.com", range(0, 9));
}

/** @return list<string> */
function bench_batch_names(): array
{
    return array_map(static fn ($i) => "Many {$i}", range(0, 9));
}

/** @return list<string> */
function bench_upsert_emails(): array
{
    return array_merge(['user1@example.com', 'user2@example.com'], array_slice(bench_batch_emails(), 0, 8));
}

// ── reads ──
function sdk_find_all(RawDb $db): string
{
    return canon_rows($db->query('SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100'), BENCH_FIELDS['findAll']);
}

function sdk_find_first(RawDb $db): string
{
    return canon_rows($db->query('SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1', ['User%']), BENCH_FIELDS['findFirst']);
}

function sdk_find_unique(RawDb $db): string
{
    return canon_rows($db->query('SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1', ['user500@example.com']), BENCH_FIELDS['findUnique']);
}

function sdk_filter_paginate_sort(RawDb $db): string
{
    // `published` is BOOLEAN on pg (rejects int=bool), int on sqlite/mysql — bind per dialect.
    $published = $db->dialect === 'postgres' ? true : 1;
    $rows = $db->query('SELECT id, title, content, published, author_id, created_at FROM benchmark_posts WHERE published = ? ORDER BY created_at DESC LIMIT 20 OFFSET 10', [$published]);
    return canon_rows($rows, BENCH_FIELDS['filterPaginateSort']);
}

// ── single writes (v1: no-returning → null; upsert → [{id}]) ──
function sdk_create(RawDb $db): string
{
    $db->execute('INSERT INTO benchmark_users (email, name) VALUES (?, ?)', ['new@bench.com', 'New']);
    return 'null';
}

function sdk_update(RawDb $db): string
{
    $db->execute('UPDATE benchmark_users SET name = ? WHERE id = ?', ['Updated 100', 100]);
    return 'null';
}

function sdk_upsert(RawDb $db): string
{
    if ($db->dialect === 'mysql') {
        $db->execute('INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name)', ['user1@example.com', 'Upserted One']);
        $rows = $db->query('SELECT id FROM benchmark_users WHERE email = ? ORDER BY id', ['user1@example.com']);
    } else {
        $rows = $db->query('INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id', ['user1@example.com', 'Upserted One']);
    }
    return canon_rows($rows, BENCH_FIELDS['upsert']);
}

// ── batch writes (v1: no-returning → null) ──
/**
 * @param list<string> $emails
 * @param list<string> $names
 */
function sdk_insert_many_values(RawDb $db, array $emails, array $names, string $tail): string
{
    $tuples = implode(', ', array_fill(0, count($emails), '(?, ?)'));
    $params = [];
    foreach ($emails as $i => $e) {
        $params[] = $e;
        $params[] = $names[$i];
    }
    $db->execute("INSERT INTO benchmark_users (email, name) VALUES {$tuples}{$tail}", $params);
    return 'null';
}

function sdk_create_many(RawDb $db): string
{
    return sdk_insert_many_values($db, bench_batch_emails(), bench_batch_names(), '');
}

function sdk_upsert_many(RawDb $db): string
{
    $tail = $db->dialect === 'mysql'
        ? ' ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name)'
        : ' ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name';
    return sdk_insert_many_values($db, bench_upsert_emails(), bench_batch_names(), $tail);
}

function sdk_update_many(RawDb $db): string
{
    // hand-OPTIMIZED single CASE update (not a per-row loop); NO returning (v1) → null.
    $names = bench_batch_names();
    $cases = [];
    for ($i = 0; $i < 10; $i++) {
        $cases[] = 'WHEN ' . ($i + 1) . ' THEN ?';
    }
    $db->execute('UPDATE benchmark_users SET name = CASE id ' . implode(' ', $cases) . ' END WHERE id IN (1,2,3,4,5,6,7,8,9,10)', $names);
    return 'null';
}

// ── read + relation (parent + ONE batched IN child, N+1 avoided) ──
/**
 * @param list<mixed> $keys
 * @return array{0:string,1:list<mixed>}
 */
function sdk_in_clause(array $keys): array
{
    $marks = implode(', ', array_fill(0, count($keys), '?'));
    return ["IN ({$marks})", array_values($keys)];
}

/**
 * @param list<mixed> $parentParams
 * @param list<string> $parentFields
 * @param list<string> $childFields
 */
function sdk_rel_single(RawDb $db, string $parentSql, array $parentParams, string $parentKey, array $parentFields, string $childSqlTmpl, string $childKey, array $childFields, string $rel): string
{
    $parents = $db->query($parentSql, $parentParams);
    $keys = array_map(static fn ($r) => $r[$parentKey], $parents);
    [$inClause, $childParams] = sdk_in_clause($keys);
    $children = $db->query(str_replace('{IN}', $inClause, $childSqlTmpl), $childParams);
    $groups = [];
    foreach ($children as $c) {
        $groups[$c[$childKey]][] = $c;
    }
    $ps = [];
    $cs = [];
    foreach ($parents as $r) {
        $ps[] = canon_row($r, $parentFields);
        $kids = $groups[$r[$parentKey]] ?? [];
        $cs[] = '[' . implode(',', array_map(static fn ($c) => canon_row($c, $childFields), $kids)) . ']';
    }
    return rel_json($rel, $ps, $cs);
}

function sdk_nested_find_all(RawDb $db): string
{
    $m = BENCH_REL_FIELDS['nestedFindAll'];
    return sdk_rel_single($db, 'SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100', [], 'id', $m['parent'],
        'SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC', 'author_id', $m['child'], $m['rel']);
}

function sdk_nested_find_first(RawDb $db): string
{
    $m = BENCH_REL_FIELDS['nestedFindFirst'];
    return sdk_rel_single($db, 'SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1', ['User%'], 'id', $m['parent'],
        'SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC', 'author_id', $m['child'], $m['rel']);
}

function sdk_nested_find_unique(RawDb $db): string
{
    $m = BENCH_REL_FIELDS['nestedFindUnique'];
    return sdk_rel_single($db, 'SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1', ['user1@example.com'], 'id', $m['parent'],
        'SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC', 'author_id', $m['child'], $m['rel']);
}

// FULL 3-level SDK (#119): THREE batched queries (parents; level-2 by parent key; level-3 by ALL
// level-2 ids) + client stitch into {rows,posts,comments} (comments flattened per parent, in post order).
// Single-key stitch (nestedRelations); the composite-key variant is bespoke below.
/**
 * @param list<array<string,mixed>> $parents
 */
function sdk_stitch3(RawDb $db, string $op, array $parents, string $parentKey, string $postsSqlTmpl, string $postsKey, string $postsId, string $commentsSqlTmpl, string $commentsKey): string
{
    $m = BENCH_REL3_FIELDS[$op];
    $pkeys = array_map(static fn ($r) => $r[$parentKey], $parents);
    [$pIn, $pParams] = sdk_in_clause($pkeys);
    $posts = $pkeys ? $db->query(str_replace('{IN}', $pIn, $postsSqlTmpl), $pParams) : [];
    $pids = array_map(static fn ($r) => $r[$postsId], $posts);
    [$cIn, $cParams] = sdk_in_clause($pids);
    $comments = $pids ? $db->query(str_replace('{IN}', $cIn, $commentsSqlTmpl), $cParams) : [];
    $postsByParent = [];
    foreach ($posts as $p) {
        $postsByParent[$p[$postsKey]][] = $p;
    }
    $commentsByPost = [];
    foreach ($comments as $c) {
        $commentsByPost[$c[$commentsKey]][] = $c;
    }
    $rowsS = [];
    $postsS = [];
    $commentsS = [];
    foreach ($parents as $r) {
        $ps = $postsByParent[$r[$parentKey]] ?? [];
        $rowsS[] = canon_row($r, $m['parent']);
        $postsS[] = canon_rows($ps, $m['posts']);
        $flat = [];
        foreach ($ps as $p) {
            foreach ($commentsByPost[$p[$postsId]] ?? [] as $c) {
                $flat[] = $c;
            }
        }
        $commentsS[] = canon_rows($flat, $m['comments']);
    }
    return rel3_json($rowsS, $postsS, $commentsS);
}

function sdk_nested_relations(RawDb $db): string
{
    $users = $db->query('SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100');
    return sdk_stitch3($db, 'nestedRelations', $users, 'id',
        'SELECT id, title, author_id FROM benchmark_posts WHERE author_id {IN} ORDER BY id ASC', 'author_id', 'id',
        'SELECT id, body, post_id FROM benchmark_comments WHERE post_id {IN} ORDER BY id ASC', 'post_id');
}

function sdk_composite_relations(RawDb $db): string
{
    // tenant_id fixed = 1: level-2 posts + level-3 comments both filtered by tenant, stitched by sub-key.
    $m = BENCH_REL3_FIELDS['compositeRelations'];
    $tusers = $db->query('SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = ? ORDER BY user_id ASC', [1]);
    $tposts = $db->query('SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts WHERE tenant_id = ? ORDER BY post_id ASC', [1]);
    $tcomments = $db->query('SELECT tenant_id, comment_id, post_id, body FROM benchmark_tenant_comments WHERE tenant_id = ? ORDER BY comment_id ASC', [1]);
    $postsByUser = [];
    foreach ($tposts as $p) {
        $postsByUser[$p['user_id']][] = $p;
    }
    $commentsByPost = [];
    foreach ($tcomments as $c) {
        $commentsByPost[$c['post_id']][] = $c;
    }
    $rowsS = [];
    $postsS = [];
    $commentsS = [];
    foreach ($tusers as $u) {
        $ps = $postsByUser[$u['user_id']] ?? [];
        $rowsS[] = canon_row($u, $m['parent']);
        $postsS[] = canon_rows($ps, $m['posts']);
        $flat = [];
        foreach ($ps as $p) {
            foreach ($commentsByPost[$p['post_id']] ?? [] as $c) {
                $flat[] = $c;
            }
        }
        $commentsS[] = canon_rows($flat, $m['comments']);
    }
    return rel3_json($rowsS, $postsS, $commentsS);
}

// ── transactions (BEGIN … COMMIT/ROLLBACK, then {committed,state}) ──
function sdk_insert_user_id(RawDb $db, string $email, string $name): int
{
    if ($db->dialect === 'mysql') {
        $db->execute('INSERT INTO benchmark_users (email, name) VALUES (?, ?)', [$email, $name]);
        return (int) $db->pdo->lastInsertId();
    }
    $rows = $db->query('INSERT INTO benchmark_users (email, name) VALUES (?, ?) RETURNING id', [$email, $name]);
    return (int) $rows[0]['id'];
}

function sdk_upsert_user_id(RawDb $db, string $email, string $name): int
{
    if ($db->dialect === 'mysql') {
        $db->execute('INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE email = VALUES(email), name = VALUES(name)', [$email, $name]);
        $rows = $db->query('SELECT id FROM benchmark_users WHERE email = ? ORDER BY id', [$email]);
        return (int) $rows[0]['id'];
    }
    $rows = $db->query('INSERT INTO benchmark_users (email, name) VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name RETURNING id', [$email, $name]);
    return (int) $rows[0]['id'];
}

/** @return array{0:list<array<string,mixed>>,1:list<array<string,mixed>>} */
function sdk_state_snapshot(RawDb $db): array
{
    $users = $db->query('SELECT id, email, name FROM benchmark_users ORDER BY id');
    $posts = $db->query('SELECT id, title, author_id FROM benchmark_posts ORDER BY id');
    return [$users, $posts];
}

function sdk_run_tx(RawDb $db, callable $body): string
{
    $committed = false;
    $db->begin();
    try {
        $body();
        $db->commit();
        $committed = true;
    } catch (\Throwable) {
        $db->rollback();
        $committed = false;
    }
    [$users, $posts] = sdk_state_snapshot($db);
    return tx_json($committed, $users, $posts);
}

function sdk_delete(RawDb $db): string
{
    return sdk_run_tx($db, function () use ($db): void {
        $uid = sdk_insert_user_id($db, 'del0@bench.com', 'Del');
        $db->execute('DELETE FROM benchmark_users WHERE id = ?', [$uid]);
    });
}

function sdk_nested_create(RawDb $db): string
{
    return sdk_run_tx($db, function () use ($db): void {
        $uid = sdk_insert_user_id($db, 'nc@bench.com', 'NC');
        $db->execute('INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)', [$uid, 'NC Post']);
    });
}

function sdk_nested_update(RawDb $db): string
{
    return sdk_run_tx($db, function () use ($db): void {
        $db->execute('UPDATE benchmark_users SET name = ? WHERE id = ?', ['NU', 7]);
        $db->execute('UPDATE benchmark_posts SET title = ? WHERE author_id = ?', ['NU Post', 7]);
    });
}

function sdk_nested_upsert(RawDb $db): string
{
    return sdk_run_tx($db, function () use ($db): void {
        $uid = sdk_upsert_user_id($db, 'user1@example.com', 'NUp');
        $db->execute('INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)', [$uid, 'NUp Post']);
    });
}

/** @return array<string,callable(RawDb):string> */
function sdk_dispatch(): array
{
    return [
        'findAll' => 'sdk_find_all', 'filterPaginateSort' => 'sdk_filter_paginate_sort', 'findFirst' => 'sdk_find_first', 'findUnique' => 'sdk_find_unique',
        'create' => 'sdk_create', 'update' => 'sdk_update', 'upsert' => 'sdk_upsert',
        'createMany' => 'sdk_create_many', 'upsertMany' => 'sdk_upsert_many', 'updateMany' => 'sdk_update_many',
        'nestedFindAll' => 'sdk_nested_find_all', 'nestedFindFirst' => 'sdk_nested_find_first', 'nestedFindUnique' => 'sdk_nested_find_unique',
        'nestedRelations' => 'sdk_nested_relations', 'compositeRelations' => 'sdk_composite_relations',
        'delete' => 'sdk_delete', 'nestedCreate' => 'sdk_nested_create', 'nestedUpdate' => 'sdk_nested_update', 'nestedUpsert' => 'sdk_nested_upsert',
    ];
}

function sdk_cell(string $op, RawDb $db): string
{
    $fn = sdk_dispatch()[$op] ?? null;
    if ($fn === null) {
        throw new \InvalidArgumentException("sdk: unknown op {$op}");
    }
    return $fn($db);
}
