<?php

/**
 * !!! VENDORED — DO NOT EDIT !!!
 *
 * Mechanically vendored from behavior-contracts/php/src/PlanFailure.php by
 * scripts/vendor-behavior-contracts-php.mjs (litedbmodel#33). The SSoT is the
 * behavior-contracts repo; edit there and re-run the vendoring script. A CI drift
 * gate (npm run vendor:bc-php:check) fails if this copy diverges.
 */

/**
 * PlanFailure.php — execution-plan.md 実行の Failure（PHP port）。
 *
 * TS `PlanFailure` / Python `PlanFailure` と同一の `code` セットを持つ:
 *   OP_FAILED / UNKNOWN_POLICY / INVALID_PLAN
 */

declare(strict_types=1);

namespace LiteDbModel\Runtime\BehaviorContracts;

final class PlanFailure extends \RuntimeException
{
    /**
     * PlanFailure コード。`\Exception::$code`（親の int プロパティ）と衝突するため
     * 専用プロパティに保持する（TS/Python の `.code` に相当）。
     *
     * @var string
     */
    public string $failureCode;

    public function __construct(string $code, string $message)
    {
        parent::__construct($message);
        $this->failureCode = $code;
    }

    /** @return never */
    public static function raise(string $code, string $message): never
    {
        throw new self($code, $message);
    }
}
