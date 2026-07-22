<?php

/**
 * !!! VENDORED — DO NOT EDIT !!!
 *
 * Mechanically vendored from behavior-contracts/php/src/BehaviorFailure.php by
 * scripts/vendor-behavior-contracts-php.mjs (litedbmodel#33). The SSoT is the
 * behavior-contracts repo; edit there and re-run the vendoring script. A CI drift
 * gate (npm run vendor:bc-php:check) fails if this copy diverges.
 */

/**
 * BehaviorFailure.php — runBehavior（component-graph IR 実行）の Failure（PHP port）。
 *
 * TS `BehaviorFailure` / Python `BehaviorFailure` と同一の `code` セットを持つ:
 *   UNKNOWN_COMPONENT / UNKNOWN_NODE_KIND / MAP_OVER_NOT_ARRAY / UNKNOWN_ENTRY
 */

declare(strict_types=1);

namespace LiteDbModel\Runtime\BehaviorContracts;

final class BehaviorFailure extends \RuntimeException
{
    /**
     * BehaviorFailure コード。`\Exception::$code`（親の int プロパティ）と衝突するため
     * 専用プロパティに保持する（TS/Python の `.code` に相当）。
     *
     * @var string
     */
    public string $failureCode;

    /**
     * 構造化された回復可能ペイロード（scp-error.md「The Error Value」）。leaf 由来を verbatim に
     * 運ぶ（該当が無ければ null）。
     *
     * @var array<string, mixed>|null
     */
    public ?array $detail;

    /**
     * @param array<string, mixed>|null $detail
     */
    public function __construct(string $code, string $message, ?array $detail = null)
    {
        parent::__construct($message);
        $this->failureCode = $code;
        $this->detail = $detail;
    }

    /**
     * @param array<string, mixed>|null $detail
     * @return never
     */
    public static function raise(string $code, string $message, ?array $detail = null): never
    {
        throw new self($code, $message, $detail);
    }
}
