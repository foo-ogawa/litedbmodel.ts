<?php

/**
 * !!! VENDORED — DO NOT EDIT !!!
 *
 * Mechanically vendored from behavior-contracts/php/src/FingerprintFailure.php by
 * scripts/vendor-behavior-contracts-php.mjs (litedbmodel#33). The SSoT is the
 * behavior-contracts repo; edit there and re-run the vendoring script. A CI drift
 * gate (npm run vendor:bc-php:check) fails if this copy diverges.
 */

/**
 * FingerprintFailure.php — fingerprint 入力の不変条件違反（PHP port, bc#13 SP2）。
 *
 * TS `FingerprintFailure` / Python `FingerprintFailure` と同一の code セットを持つ:
 *   UNSAFE_NUMBER / INVALID_VALUE / FORBIDDEN_KEY（canonical 直列化の fail-closed）
 */

declare(strict_types=1);

namespace LiteDbModel\Runtime\BehaviorContracts;

final class FingerprintFailure extends \RuntimeException
{
    /**
     * Failure コード。`\Exception::$code`（親の int プロパティ）と衝突するため
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
