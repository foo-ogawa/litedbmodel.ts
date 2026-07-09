<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — dialect strategy table (PHP port of src/scp/dialect.ts, WS7d #33).
 *
 * The SSoT for every SQL-dialect difference the render pipeline needs, ported BYTE-FOR-BYTE
 * from the TS reference `src/scp/dialect.ts` (WS6). The dialect axis is compiled ONCE, TS-side;
 * the PHP runtime only needs the render-time divergences a §8 bundle exercises:
 *   - `finalizePlaceholders`: SQLite/MySQL identity; Postgres `?`→`$1,$2,…` in ONE
 *     left-to-right pass over the fully-assembled SQL text (spec §8).
 *   - `orderByNulls`: PG/SQLite native `NULLS FIRST/LAST`; MySQL `<expr> IS NULL <dir>` emulation.
 *
 * The INSERT-conflict / guard-INSERT strategy methods are compiled into the bundle's SQL text
 * TS-side (a §8 bundle's `operations[*].sql` already carries the dialect-correct INSERT text),
 * so the PHP render path never re-derives them — it only applies `finalizePlaceholders` +
 * `orderByNulls`. Those two are therefore the whole PHP dialect surface.
 */
final class Dialect
{
    /**
     * @param 'sqlite'|'postgres'|'mysql' $name
     */
    private function __construct(public readonly string $name)
    {
    }

    /** Resolve a dialect name to its strategy (fail-closed — no silent default, mirrors dialectFor). */
    public static function forName(string $name): self
    {
        if ($name !== 'sqlite' && $name !== 'postgres' && $name !== 'mysql') {
            throw new \RuntimeException(
                "scp dialect: unknown dialect '{$name}' (known: sqlite, postgres, mysql)"
            );
        }
        return new self($name);
    }

    /**
     * Convert the fully-assembled, param-flattened SQL text's `?` placeholders to this dialect's
     * final placeholder style (render.ts `finalize` / dialect.ts `finalizePlaceholders`).
     * SQLite/MySQL: identity. Postgres: a single left-to-right pass replacing the Nth `?` with
     * `$N` (spec §8 final one-pass — no number-reassignment problem).
     */
    public function finalizePlaceholders(string $sql): string
    {
        if ($this->name !== 'postgres') {
            return $sql;
        }
        return self::toDollarPlaceholders($sql);
    }

    /** Postgres `?`→`$1,$2,…` one-pass (dialect.ts `toDollarPlaceholders`). */
    public static function toDollarPlaceholders(string $sql): string
    {
        $n = 0;
        $out = '';
        $len = strlen($sql);
        for ($i = 0; $i < $len; $i++) {
            $ch = $sql[$i];
            if ($ch === '?') {
                $n += 1;
                $out .= '$' . $n;
            } else {
                $out .= $ch;
            }
        }
        return $out;
    }

    /**
     * Deterministic NULL ordering for an `ORDER BY <expr> <dir>` term (dialect.ts `orderByNulls`).
     * Postgres/SQLite native `NULLS FIRST/LAST`; MySQL emulates with a leading `<expr> IS NULL` key.
     *
     * @param 'ASC'|'DESC' $dir
     * @param 'FIRST'|'LAST' $nulls
     */
    public function orderByNulls(string $expr, string $dir, string $nulls): string
    {
        if ($this->name === 'mysql') {
            // MySQL: NULL sorts LOWEST by default. `expr IS NULL` is 1 for null, 0 otherwise.
            //   NULLS FIRST → flag DESC (1 before 0); NULLS LAST → flag ASC (0 before 1).
            $flagDir = $nulls === 'FIRST' ? 'DESC' : 'ASC';
            return "{$expr} IS NULL {$flagDir}, {$expr} {$dir}";
        }
        // sqlite / postgres: native.
        return "{$expr} {$dir} NULLS {$nulls}";
    }
}
