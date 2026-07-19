<?php

declare(strict_types=1);

/**
 * Canonical result serialization — byte-matching benchmark/crosslang/oracle.ts canonVal/canonRow.
 *
 * The PHP twin of adapters/py/canon.py. Shared by the sdk (hand-SQL) + ir (interpreter) php cells.
 * Hand-rolled to the SAME rules the rust/go seams use (int bare, string JSON-quoted, bool→0/1, null)
 * so every cell's stdout equals the ONE dialect-independent sqlite oracle string. The per-op projected
 * field order + the read/rel/tx result shapes mirror oracle.ts (FIELDS / REL_FIELDS / stateSnapshot).
 *
 * Field access is polymorphic: the ir cell hands back PDO rows as `stdClass` (the litedbmodel runtime's
 * value model), the sdk cell hands back raw PDO assoc-array rows + the tx `entity` is an assoc array.
 * The projected INTEGER fields are cast to int here so a pg/mysql PDO string column (`"7"`) canonicalizes
 * to the bare digits the sqlite oracle emits (`7`); `published` maps pg `t`/`true`/1 → 1, else 0.
 */

/** The projected field order per op (== oracle.ts FIELDS == the native row struct field order). */
const BENCH_FIELDS = [
    'findAll' => ['id', 'email', 'name'],
    'findFirst' => ['id', 'email', 'name'],
    'findUnique' => ['id', 'email', 'name'],
    'filterPaginateSort' => ['id', 'title', 'content', 'published', 'author_id', 'created_at'],
    'upsert' => ['id'],
];

/** read+rel 2-LEVEL: parent fields + child (relation) fields + the relation key name (== oracle.ts REL_FIELDS). */
const BENCH_REL_FIELDS = [
    'nestedFindAll' => ['parent' => ['id', 'email', 'name'], 'child' => ['id', 'title', 'author_id'], 'rel' => 'posts'],
    'nestedFindFirst' => ['parent' => ['id', 'email', 'name'], 'child' => ['id', 'title', 'author_id'], 'rel' => 'posts'],
    'nestedFindUnique' => ['parent' => ['id', 'email', 'name'], 'child' => ['id', 'title', 'author_id'], 'rel' => 'posts'],
];

/** read+rel FULL 3-LEVEL chain (#119): parent + level-2 (posts) + level-3 (comments) field orders (== oracle.ts REL3_FIELDS). */
const BENCH_REL3_FIELDS = [
    'nestedRelations' => ['parent' => ['id', 'email', 'name'], 'posts' => ['id', 'title', 'author_id'], 'comments' => ['id', 'body', 'post_id']],
    'compositeRelations' => ['parent' => ['tenant_id', 'user_id', 'name'], 'posts' => ['tenant_id', 'post_id', 'user_id', 'title'], 'comments' => ['tenant_id', 'comment_id', 'post_id', 'body']],
];

/** The projected fields whose oracle token is bare digits (PDO may hand these back as strings). */
const BENCH_INT_FIELDS = ['id', 'author_id', 'published', 'tenant_id', 'user_id', 'post_id', 'comment_id'];

/** Read a field from a row that is either a stdClass (ir/runtime) or an assoc array (sdk/entity). */
function bench_field(mixed $row, string $f): mixed
{
    if (is_array($row)) {
        return $row[$f] ?? null;
    }
    if ($row instanceof \stdClass) {
        return $row->{$f} ?? null;
    }
    return null;
}

/** One value → the oracle's canonical token (mirror oracle.ts canonVal), field-type aware. */
function canon_val(mixed $v, ?string $field = null): string
{
    if ($v === null) {
        return 'null';
    }
    if ($field !== null && in_array($field, BENCH_INT_FIELDS, true)) {
        if ($field === 'published') {
            // pg boolean PDO → 't'/'f'; mysql tinyint → '1'/'0'; sqlite → int 1/0; runtime → bool.
            $truthy = $v === true || $v === 1 || $v === '1' || $v === 't' || $v === 'true' || $v === 'TRUE';
            return $truthy ? '1' : '0';
        }
        return (string) (int) $v;
    }
    if (is_bool($v)) {
        return $v ? '1' : '0';
    }
    if (is_int($v)) {
        return (string) $v;
    }
    if (is_float($v)) {
        // Integral floats print bare (the bench has no fractional projected value); mirrors JS Number.
        return ($v === floor($v) && is_finite($v)) ? (string) (int) $v : (string) $v;
    }
    return json_encode((string) $v, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
}

/** @param list<string> $fields */
function canon_row(mixed $row, array $fields): string
{
    $parts = [];
    foreach ($fields as $f) {
        $parts[] = json_encode($f) . ':' . canon_val(bench_field($row, $f), $f);
    }
    return '{' . implode(',', $parts) . '}';
}

/**
 * @param list<mixed> $rows
 * @param list<string> $fields
 */
function canon_rows(array $rows, array $fields): string
{
    $out = [];
    foreach ($rows as $r) {
        $out[] = canon_row($r, $fields);
    }
    return '[' . implode(',', $out) . ']';
}

/**
 * {"rows":[parent…],"<rel>":[[child…]…]} — the native T2 {rows, <rel>} shape (oracle.ts).
 *
 * @param list<string> $parents already-canonicalized parent rows
 * @param list<string> $childLists already-canonicalized child row-lists
 */
function rel_json(string $rel, array $parents, array $childLists): string
{
    return '{"rows":[' . implode(',', $parents) . '],' . json_encode($rel) . ':[' . implode(',', $childLists) . ']}';
}

/**
 * {"rows":[parent…],"posts":[[post…]…],"comments":[[comment…]…]} — the native 3-level shape (oracle.ts).
 *
 * @param list<string> $parents already-canonicalized parent rows
 * @param list<string> $postsLists already-canonicalized per-parent level-2 (posts) row-lists
 * @param list<string> $commentsLists already-canonicalized per-parent flattened level-3 (comments) row-lists
 */
function rel3_json(array $parents, array $postsLists, array $commentsLists): string
{
    return '{"rows":[' . implode(',', $parents) . '],"posts":[' . implode(',', $postsLists) . '],"comments":[' . implode(',', $commentsLists) . ']}';
}

/**
 * {"committed":<b>,"state":{"users":[…],"posts":[…]}} — the write/tx affected-tables snapshot.
 *
 * @param list<mixed> $users
 * @param list<mixed> $posts
 */
function tx_json(bool $committed, array $users, array $posts): string
{
    $state = '{"users":' . canon_rows($users, ['id', 'email', 'name']) . ',"posts":' . canon_rows($posts, ['id', 'title', 'author_id']) . '}';
    return '{"committed":' . ($committed ? 'true' : 'false') . ',"state":' . $state . '}';
}
