<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — the SHARED cross-language runaway-prevention error (PHP port of the
 * TS reference `LimitExceededError` in src/scp/errors.ts; Phase E-2, epic #74; #103).
 *
 * Thrown by the PHP runtime post-fetch guard when a top-level read (`context: 'find'`) or a
 * hasMany relation batch (`context: 'relation'`) returns MORE rows than the hard limit BAKED
 * onto the artifact (`readGraph.findGuard` / `RelationOp.hardLimit`), so an accidental missing-
 * WHERE / N+1 pattern fails LOUD instead of loading an unbounded result. There is NO config
 * surface — the cap is read from the JSON artifact ONLY (mirror of the TS reference and the
 * rust/go/py ports).
 *
 * NOT a {@see SqlFailure}: a runaway guard is a litedbmodel-level policy error, not a mapped
 * driver failure, and it carries no `SQLITE_*` code — so `Runtime::reErrorToSqlFailure`
 * (which re-maps ONLY messages carrying a `SQLITE_*` tag) propagates it unchanged.
 *
 * ## Byte-identical error contract (mirror of errors.ts)
 *   - fields: `limit` (the cap), `count` (rows fetched), `context` (`'find'` | `'relation'`),
 *     `model` (the read/parent model — for `relation`, the relation TARGET TABLE), `relation`
 *     (the relation name; `'relation'` context only);
 *   - message: `Query limit exceeded: <where> returned <count-phrase> records, but limit is
 *     <limit>. This usually indicates a missing WHERE clause or an N+1 query pattern. Set a
 *     higher limit or use pagination.` — `find` → `<where>` = `find() on <model>`, count-phrase
 *     `more than <limit>` (the `LIMIT hardLimit + 1` N+1 fetch only KNOWS the total exceeds the
 *     cap); `relation` → `<where>` = `relation '<relation>' on <model>`, count-phrase the EXACT
 *     `<count>` (the batch is fetched in full, no N+1).
 */
final class LimitExceededError extends \RuntimeException
{
    /** The error name mirrored across the ports (matches the TS `Error.name`). */
    public const NAME = 'LimitExceededError';

    /**
     * @param int         $limit    the configured hard-limit cap.
     * @param int         $count    rows fetched. `find`: the `LIMIT hardLimit + 1` fetch size
     *                              (the true total is only known to EXCEED `limit`). `relation`:
     *                              the EXACT batch-total row count.
     * @param string      $context  `'find'` | `'relation'`.
     * @param string|null $model    the read model (`find`) / relation TARGET TABLE (`relation`).
     * @param string|null $relation the relation name (`'relation'` context only).
     */
    public function __construct(
        public readonly int $limit,
        public readonly int $count,
        public readonly string $context,
        public readonly ?string $model = null,
        public readonly ?string $relation = null,
    ) {
        $where = $context === 'find'
            ? 'find() on ' . ($model ?? 'unknown')
            : "relation '" . ($relation ?? 'unknown') . "' on " . ($model ?? 'unknown');
        $countPhrase = $context === 'find' ? "more than {$limit}" : (string) $count;
        parent::__construct(
            "Query limit exceeded: {$where} returned {$countPhrase} records, "
            . "but limit is {$limit}. This usually indicates a missing WHERE clause or "
            . 'an N+1 query pattern. Set a higher limit or use pagination.'
        );
    }

    /**
     * The SHARED post-fetch runaway check (SSoT) — the ONE `count > limit ⇒ throw` primitive the
     * relation-context guard ({@see Relation::runRelationOp}) calls, so no path re-implements the
     * comparison or the error assembly (PHP port of the python `LimitExceededError.check` / rust
     * `LimitExceededError::check` / go `CheckLimit` SSoT). Returns (no-op) when within the cap;
     * throws otherwise. `find`-context reads run their own node-matching guard inline
     * ({@see StaticBundle}) — the same asymmetry as the python port.
     */
    public static function check(int $limit, int $count, string $context, ?string $model = null, ?string $relation = null): void
    {
        if ($count > $limit) {
            throw new self($limit, $count, $context, $model, $relation);
        }
    }
}
