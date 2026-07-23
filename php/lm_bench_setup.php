<?php

declare(strict_types=1);

/**
 * Load the ONE cross-lang ORM-bench seed SSoT — benchmark/crosslang/.setup/<dialect>.json, emitted from
 * orm-domain.ts by emit-setup.ts — for BOTH php bench cells (orm_bench + orm_bench_sdk). No php cell
 * hand-writes a schema or seed: each applies `schema` once at open and `delete`+`insert` (the canonical
 * 110-user fixture, literal SQL) per op. This is the single php-side reader of the JSON artifact.
 *
 * @return array{dialect:string,users:int,schema:list<string>,delete:list<string>,insert:list<string>}
 */
function lm_bench_load_setup(string $dialect): array
{
    $path = __DIR__ . '/../benchmark/crosslang/.setup/' . $dialect . '.json';
    $raw = @file_get_contents($path);
    if ($raw === false) {
        throw new \RuntimeException("read seed SSoT $path");
    }
    $doc = json_decode($raw, true);
    if (!is_array($doc)) {
        throw new \RuntimeException("parse seed SSoT $path");
    }
    return $doc;
}
