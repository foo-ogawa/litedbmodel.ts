<?php

/**
 * !!! VENDORED — DO NOT EDIT !!!
 *
 * Mechanically vendored from behavior-contracts/php/src/ExprFailure.php by
 * scripts/vendor-behavior-contracts-php.mjs (litedbmodel#33). The SSoT is the
 * behavior-contracts repo; edit there and re-run the vendoring script. A CI drift
 * gate (npm run vendor:bc-php:check) fails if this copy diverges.
 */

/**
 * ExprFailure.php — expression 評価の Failure（PHP port）。
 *
 * TS `ExprFailure` / Python `ExprFailure` と同一の `code` セットを持つ。code は
 * expression-ir.md / expr-eval.ts のいずれか:
 *   INT_OVERFLOW / NAN_OR_INF / MOD_ZERO / PRECISION_LOSS / TYPE_MISMATCH /
 *   NULL_REF / MISSING_PROP / UNKNOWN_BINDING / UNKNOWN_OP / INVALID_NODE /
 *   INVALID_LITERAL / FORBIDDEN_KEY
 */

declare(strict_types=1);

namespace LiteDbModel\Runtime\BehaviorContracts;

final class ExprFailure extends \RuntimeException
{
    /**
     * ExprFailure コード。`\Exception::$code`（親の int プロパティ）と衝突するため
     * 専用プロパティに保持する（TS/Python の `.code` に相当）。
     *
     * @var string
     */
    public string $failureCode;

    /**
     * 構造化された回復可能ペイロード（scp-error.md「The Error Value」）。宣言型と実際の値の
     * **両方**を持つ地点（outType 適合検査）で載る。該当が無ければ null。
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
     * ExprFailure を送出する（TS/Python の `fail()` 相当。never を返す）。
     *
     * @param array<string, mixed>|null $detail
     * @return never
     */
    public static function raise(string $code, string $message, ?array $detail = null): never
    {
        throw new self($code, $message, $detail);
    }
}
