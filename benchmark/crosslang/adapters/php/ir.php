<?php

declare(strict_types=1);

/**
 * The php IR cell — litedbmodel's SHIPPED PHP runtime INTERPRETER executing the 19 bench ops.
 *
 * The php twin of adapters/py/ir.py. This is the honest "ir(interpreter)" tier (NOT native codegen —
 * py/php native codegen is a known bc capability gap). It consumes the JSON-serialized §8 bundle
 * (adapters/php/bundles.json, built by gen-bundles.ts) and runs each op through the SAME public
 * litedbmodel runtime entry points the conformance corpus drives:
 *   • read            → Runtime::executeBundle
 *   • read + relation → Relation::readBundle  (batch-load + hydrate the `with` relation)
 *   • ALL writes      → Runtime::executeTransactionBundleInternal  (guard opt-out — this cell IS the
 *                        per-command auto-tx boundary, mirroring the py cell's guard=False)
 * Results are canonicalized to the SAME dialect-independent oracle string every other cell matches
 * (canon.php): reads → row list, read+rel → {rows, <rel>}, no-returning write → null, upsert → [{id}],
 * tx → {committed, state}.
 */

require_once __DIR__ . '/canon.php';
require_once __DIR__ . '/db.php';

use LiteDbModel\Runtime\Relation;
use LiteDbModel\Runtime\Runtime;

const IR_READ_OPS = ['findAll', 'filterPaginateSort', 'findFirst', 'findUnique'];
// ALL writes ride executeTransactionBundleInternal (litedbmodel's real php write path). v1 returning:
// no-returning writes → null; upsert returns its PK (the tx `entity`); tx ops → {committed, state}.
const IR_NULL_WRITE_OPS = ['create', 'update', 'createMany', 'upsertMany', 'updateMany'];
const IR_TX_OPS = ['delete', 'nestedCreate', 'nestedUpdate', 'nestedUpsert'];

/** @return array{0:list<array<string,mixed>>,1:list<array<string,mixed>>} */
function ir_state_snapshot(\PDO $db): array
{
    $users = $db->query('SELECT id, email, name FROM benchmark_users ORDER BY id')->fetchAll(\PDO::FETCH_ASSOC);
    $posts = $db->query('SELECT id, title, author_id FROM benchmark_posts ORDER BY id')->fetchAll(\PDO::FETCH_ASSOC);
    return [$users, $posts];
}

function ir_cell(string $op, string $target): string
{
    $bundles = json_decode((string) file_get_contents(__DIR__ . '/bundles.json'), false);
    $dialect = dialect_of($target);
    $entry = $bundles->{$dialect}->{$op};
    $bundle = $entry->bundle;
    // The bundle stays a stdClass tree (the runtime's value model); the input becomes an assoc array.
    $input = json_decode(json_encode($entry->input), true) ?? [];

    $db = open_pdo($target);
    try {
        if (in_array($op, IR_READ_OPS, true)) {
            return canon_rows(Runtime::executeBundle($bundle, $input, $db), BENCH_FIELDS[$op]);
        }
        if (array_key_exists($op, BENCH_REL_FIELDS)) {
            $m = BENCH_REL_FIELDS[$op];
            $rows = Relation::readBundle($bundle, $input, $db, [$m['rel']]);
            $parents = [];
            $children = [];
            foreach ($rows as $r) {
                $parents[] = canon_row($r, $m['parent']);
                $kids = $r->{$m['rel']} ?? [];
                $children[] = '[' . implode(',', array_map(static fn ($c) => canon_row($c, $m['child']), $kids)) . ']';
            }
            return rel_json($m['rel'], $parents, $children);
        }
        if (array_key_exists($op, BENCH_REL3_FIELDS)) {
            // FULL 3-level chain (#119): parent read → level-2 (posts) batch → level-3 (comments) batch,
            // via the runtime's Relation::runRelationOp / distributeToParent (mirror oracle.ts + py ir.py).
            // The level-2 relation carries its level-3 childRelations; comments flattened per parent in post order.
            $m = BENCH_REL3_FIELDS[$op];
            $postsRel = $bundle->relations->{$entry->withRel};
            $commentsRel = $postsRel->childRelations[0];
            $parents = Runtime::executeBundle($bundle, $input, $db);
            $pBatch = Relation::runRelationOp($postsRel, $parents, $db);
            $perParentPosts = array_map(static fn ($p) => Relation::distributeToParent($postsRel, $p, $pBatch), $parents);
            $allPosts = $perParentPosts ? array_merge(...$perParentPosts) : [];
            $cBatch = Relation::runRelationOp($commentsRel, $allPosts, $db);
            $rowsS = [];
            $postsS = [];
            $commentsS = [];
            foreach ($parents as $i => $r) {
                $ps = $perParentPosts[$i];
                $rowsS[] = canon_row($r, $m['parent']);
                $postsS[] = canon_rows($ps, $m['posts']);
                $flat = [];
                foreach ($ps as $p) {
                    foreach (Relation::distributeToParent($commentsRel, $p, $cBatch) as $c) {
                        $flat[] = $c;
                    }
                }
                $commentsS[] = canon_rows($flat, $m['comments']);
            }
            return rel3_json($rowsS, $postsS, $commentsS);
        }
        if ($op === 'upsert') {
            $res = Runtime::executeTransactionBundleInternal($bundle, $input, $db);
            return canon_rows([$res['entity']], BENCH_FIELDS['upsert']); // v1 upsert returns the PK
        }
        if (in_array($op, IR_NULL_WRITE_OPS, true)) {
            Runtime::executeTransactionBundleInternal($bundle, $input, $db); // mutate; v1 no-returning → null
            return 'null';
        }
        if (in_array($op, IR_TX_OPS, true)) {
            $res = Runtime::executeTransactionBundleInternal($bundle, $input, $db);
            [$users, $posts] = ir_state_snapshot($db);
            return tx_json((bool) $res['committed'], $users, $posts);
        }
        throw new \InvalidArgumentException("ir: unknown op {$op}");
    } finally {
        $db = null;
    }
}
