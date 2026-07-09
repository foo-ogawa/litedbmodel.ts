<?php

declare(strict_types=1);

namespace LiteDbModel\Runtime;

/**
 * litedbmodel v2 SCP — PHP runtime (WS7e scaffold, #30).
 *
 * Interprets the language-neutral §8 published bundle (SqlBundle: sql + fragment tree +
 * closed-set Expression-IR param slots + transaction plan, dialect-tagged) and executes it
 * against a PDO SQL driver, semantics-identical to the TS reference (src/scp). The generic
 * Expression-IR evaluation is delegated to the VENDORED behavior-contracts PHP port (the bc PHP
 * port is not published to Packagist — it is vendored under src/BehaviorContracts/ behind a
 * drift gate, mirroring graphddb; see README).
 *
 * WS7A_SCAFFOLD: the runtime surface is declared here; the bodies are WS7e. They throw so a
 * premature call fails loudly instead of returning a fake result.
 */
final class Runtime
{
    /** Version mirrored from package.json by scripts/sync-versions.mjs (SSoT). */
    public const VERSION = '1.2.10';

    /**
     * Render a §8 CompiledOperation against a scope for a dialect → ['sql' => ..., 'params' => ...]. WS7e.
     *
     * @param array<string,mixed> $operation
     * @param array<string,mixed> $scope
     * @return array{sql: string, params: list<mixed>}
     */
    public static function renderOperation(array $operation, array $scope, string $dialect): array
    {
        throw new \RuntimeException('litedbmodel/runtime: renderOperation is WS7e (WS7a scaffold only)');
    }

    /**
     * Execute a §8 read/exec SqlBundle end-to-end. WS7e.
     *
     * @param array<string,mixed> $bundle
     * @param array<string,mixed> $input
     */
    public static function executeBundle(array $bundle, array $input, \PDO $db): mixed
    {
        throw new \RuntimeException('litedbmodel/runtime: executeBundle is WS7e (WS7a scaffold only)');
    }

    /**
     * Execute a §8 write-tx SqlBundle as one gate-first transaction. WS7e.
     *
     * @param array<string,mixed> $bundle
     * @param array<string,mixed> $input
     */
    public static function executeTransactionBundle(array $bundle, array $input, \PDO $db): mixed
    {
        throw new \RuntimeException('litedbmodel/runtime: executeTransactionBundle is WS7e (WS7a scaffold only)');
    }

    /** The dialect NULLS-ordering primitive. WS7e. */
    public static function orderByNulls(string $expr, string $dir, string $nulls, string $dialect): string
    {
        throw new \RuntimeException('litedbmodel/runtime: orderByNulls is WS7e (WS7a scaffold only)');
    }
}
