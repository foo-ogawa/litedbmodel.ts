<?php

/**
 * !!! VENDORED — DO NOT EDIT !!!
 *
 * Mechanically vendored from behavior-contracts/php/src/ExprEval.php by
 * scripts/vendor-behavior-contracts-php.mjs (litedbmodel#33). The SSoT is the
 * behavior-contracts repo; edit there and re-run the vendoring script. A CI drift
 * gate (npm run vendor:bc-php:check) fails if this copy diverges.
 */

/**
 * ExprEval.php — expression-ir.md の参照評価器（規範実装、PHP port）。
 *
 * TS 参照実装 `ts/src/expr-eval.ts` / Python port `expr_eval.py` の意味論を
 * **完全一致** で移植したもの。
 *
 * 値モデル（../expression-ir.md §5）:
 *   int = PHP `int`（i64 checked。PHP_INT は 64bit 環境で正確に i64）
 *   / float = PHP `float`（NaN/±Inf は Failure）
 *   / string / bool / null / list(array) / object(stdClass)
 *
 * §8 の cross-language 罠（本 port の要点）:
 *   - int 演算は checked i64。PHP は int 演算が i64 を溢れると **暗黙に float へ昇格** する
 *     ので、演算後に float 化していれば INT_OVERFLOW（PHP_INT_MAX == i64 max を利用）。
 *   - 評価結果が NaN / ±Inf になった時点で Failure。
 *   - mod は truncated division（符号は被除数）。PHP の `%`（int）と `fmod`（float）は
 *     どちらも truncated なので符号は被除数に一致する（Python と異なり補正不要）。
 *   - 文字列比較は **code point 順**。PHP の `<`/`>` は numeric string を数値比較する型
 *     ジャグリングがあるため使わず、code point を明示比較する（mbstring）。
 *   - and / or / coalesce / cond は短絡評価。
 *   - 未知オペレータ / __proto__ own key は fail-closed。
 *
 * 値の受け渡し規約: JSON は `json_decode($s, false)` で復元すること。これにより
 * JSON object は stdClass、JSON array は PHP list になり、両者が判別可能になる
 * （assoc-array 復元だと `{}` と `[]` が区別できず obj/arr が発散する）。
 */

declare(strict_types=1);

namespace LiteDbModel\Runtime\BehaviorContracts;

// FORBIDDEN_OBJECT_KEY は Constants.php（composer `files` autoload で常時ロード）に定義。
// ExprEval / Codec の双方が参照するため、load 順に依存しない位置へ移した（SSoT は 1 か所）。

final class ExprEval
{
    // ── i64 / f64 規範 ──────────────────────────────────────────────────────
    // PHP_INT_MIN / PHP_INT_MAX は 64bit 環境で i64 の下限/上限と一致する。
    private const WIDEN_EXACT = 9007199254740992;   // 2^53（div widening の正確域）
    private const SAFE_INT = 9007199254740991;      // 2^53 - 1（JS MAX_SAFE_INTEGER）

    /**
     * 式ノードを評価する。
     *
     * @param mixed $node  IR ノード（json_decode(.., false) 由来を推奨）。
     * @param array<string,mixed> $scope 束縛（省略時は空）。
     * @return mixed 評価結果（値モデル）。
     * @throws ExprFailure 評価失敗（コードは ExprFailure の定数）。
     */
    public static function evaluate(mixed $node, array $scope = []): mixed
    {
        // ── リテラル ──
        if ($node === null) {
            return null;
        }
        if (is_bool($node) || is_string($node)) {
            return $node;
        }
        if (self::isInt($node)) {
            // 分類規則（§2.3）: 生の整数 number は安全整数域内に限る。
            if ($node < -self::SAFE_INT || $node > self::SAFE_INT) {
                ExprFailure::raise(
                    'INVALID_LITERAL',
                    "integral number literal {$node} exceeds safe range; use {int:\"...\"}"
                );
            }
            return $node;
        }
        if (is_float($node)) {
            return self::checkFinite($node);
        }
        if (is_array($node)) {
            // json_decode(.., false) 由来なら list は PHP array。bare array は式ではない。
            ExprFailure::raise('INVALID_NODE', 'bare array is not an expression (use {arr:[...]})');
        }
        if (!($node instanceof \stdClass)) {
            ExprFailure::raise('INVALID_NODE', 'invalid node');
        }

        // ── オペレータノード = 単一キー ──
        $props = get_object_vars($node);
        $keys = array_keys($props);
        if (count($keys) !== 1) {
            ExprFailure::raise(
                'INVALID_NODE',
                'operator node must have exactly one key, got [' . implode(', ', $keys) . ']'
            );
        }
        $op = (string) $keys[0];
        $arg = $props[$op];

        switch ($op) {
            // ── リテラルラッパ ──
            case 'int':
                if (!is_string($arg)) {
                    ExprFailure::raise('INVALID_NODE', '{int: ...} expects a string');
                }
                return self::parseI64Literal($arg);
            case 'float':
                // bool は number ではない（typeof number は bool を含まない）。
                if (is_bool($arg) || !(is_int($arg) || is_float($arg))) {
                    ExprFailure::raise('INVALID_NODE', '{float: ...} expects a number');
                }
                return self::checkFinite((float) $arg);

            // ── 参照 ──
            case 'ref':
            case 'refOpt':
                return self::evalRef($op, $arg, $scope);

            // ── 構築 ──
            case 'obj':
                if (!($arg instanceof \stdClass)) {
                    ExprFailure::raise('INVALID_NODE', '{obj: ...} expects an object');
                }
                $out = new \stdClass();
                foreach (get_object_vars($arg) as $k => $v) {
                    if ($k === FORBIDDEN_OBJECT_KEY) {
                        ExprFailure::raise(
                            'FORBIDDEN_KEY',
                            'obj key "' . FORBIDDEN_OBJECT_KEY . '" is forbidden (fail-closed)'
                        );
                    }
                    $out->{$k} = self::evaluate($v, $scope);
                }
                return $out;
            case 'arr':
                return array_map(
                    static fn ($e) => self::evaluate($e, $scope),
                    self::argArray($op, $arg)
                );

            // ── 数値（モノモーフィック・checked）──
            case 'add':
            case 'sub':
            case 'mul':
                [$a, $b] = self::evalBinary($op, $arg, $scope);
                if (self::isInt($a) && self::isInt($b)) {
                    // PHP は int 演算が i64 を溢れると float へ昇格する。float 化 = overflow。
                    $r = $op === 'add' ? $a + $b : ($op === 'sub' ? $a - $b : $a * $b);
                    if (!is_int($r)) {
                        ExprFailure::raise('INT_OVERFLOW', "i64 overflow in {$op}");
                    }
                    return $r;
                }
                if (is_float($a) && is_float($b)) {
                    $r = $op === 'add' ? $a + $b : ($op === 'sub' ? $a - $b : $a * $b);
                    return self::checkFinite($r);
                }
                ExprFailure::raise(
                    'TYPE_MISMATCH',
                    "{$op}: int×int か float×float（got " . self::typeName($a) . '×' . self::typeName($b) . '）'
                );
                // no break (raise never returns)
            case 'neg':
                $a = self::evaluate(self::argUnary($op, $arg), $scope);
                if (self::isInt($a)) {
                    $r = -$a;
                    if (!is_int($r)) {
                        // -(i64 min) は i64 max+1 で float 化 = overflow。
                        ExprFailure::raise('INT_OVERFLOW', 'i64 overflow in neg');
                    }
                    return $r;
                }
                if (is_float($a)) {
                    return self::checkFinite(-$a);
                }
                ExprFailure::raise('TYPE_MISMATCH', 'neg: numeric expected, got ' . self::typeName($a));
                // no break
            case 'div':
                // 常に float 除算。int は正確に widening、0 除算は NaN/Inf 規則で Failure。
                [$a, $b] = self::evalBinary($op, $arg, $scope);
                $fa = self::widenToFloat($a);
                $fb = self::widenToFloat($b);
                if ($fb === 0.0) {
                    ExprFailure::raise('NAN_OR_INF', 'division produced a non-finite float (div by zero)');
                }
                return self::checkFinite($fa / $fb);
            case 'mod':
                // truncated division の剰余。符号は被除数（PHP の % / fmod と一致）。
                [$a, $b] = self::evalBinary($op, $arg, $scope);
                if (self::isInt($a) && self::isInt($b)) {
                    if ($b === 0) {
                        ExprFailure::raise('MOD_ZERO', 'int mod by zero');
                    }
                    // PHP の % は truncated（符号は被除数）。-7 % 2 == -1。
                    return $a % $b;
                }
                if (is_float($a) && is_float($b)) {
                    return self::checkFinite(fmod($a, $b));
                }
                ExprFailure::raise(
                    'TYPE_MISMATCH',
                    "mod: int×int か float×float（got " . self::typeName($a) . '×' . self::typeName($b) . '）'
                );
                // no break

            // ── 文字列 ──
            case 'concat':
                $rawParts = self::argArray($op, $arg);
                // n-ary（min 2 args）。arity<2 は不正 IR（expression-ir.md §2.1/§3/§6）。
                if (count($rawParts) < 2) {
                    ExprFailure::raise('INVALID_NODE', 'concat expects >= 2 args, got ' . count($rawParts));
                }
                $parts = [];
                foreach ($rawParts as $e) {
                    $p = self::evaluate($e, $scope);
                    if (!is_string($p)) {
                        ExprFailure::raise('TYPE_MISMATCH', 'concat: string のみ（got ' . self::typeName($p) . '）');
                    }
                    $parts[] = $p;
                }
                return implode('', $parts);

            // ── 比較 ──
            case 'eq':
            case 'ne':
                [$a, $b] = self::evalBinary($op, $arg, $scope);
                $equal = self::valueEquals($a, $b);
                return $op === 'eq' ? $equal : !$equal;
            case 'lt':
            case 'le':
            case 'gt':
            case 'ge':
                [$a, $b] = self::evalBinary($op, $arg, $scope);
                if (self::isInt($a) && self::isInt($b)) {
                    $c = $a < $b ? -1 : ($a > $b ? 1 : 0);
                } elseif (is_float($a) && is_float($b)) {
                    $c = $a < $b ? -1 : ($a > $b ? 1 : 0);
                } elseif (is_string($a) && is_string($b)) {
                    $c = self::cmpCodePoints($a, $b);
                } else {
                    ExprFailure::raise(
                        'TYPE_MISMATCH',
                        "{$op}: 同一型の int/float/string のみ（got " . self::typeName($a) . '×' . self::typeName($b) . '）'
                    );
                }
                return match ($op) {
                    'lt' => $c < 0,
                    'le' => $c <= 0,
                    'gt' => $c > 0,
                    default => $c >= 0, // ge
                };

            // ── bool / null（短絡評価）──
            case 'and':
            case 'or':
                [$ea, $eb] = self::rawBinary($op, $arg);
                $a = self::requireBool(self::evaluate($ea, $scope), $op);
                if ($op === 'and' && !$a) {
                    return false; // 短絡: 右辺は評価されない
                }
                if ($op === 'or' && $a) {
                    return true;
                }
                return self::requireBool(self::evaluate($eb, $scope), $op);
            case 'not':
                return !self::requireBool(self::evaluate(self::argUnary($op, $arg), $scope), 'not');
            case 'coalesce':
                [$ea, $eb] = self::rawBinary($op, $arg);
                $a = self::evaluate($ea, $scope);
                return $a === null ? self::evaluate($eb, $scope) : $a; // 右辺は遅延評価
            case 'cond':
                if (!is_array($arg) || count($arg) !== 3) {
                    ExprFailure::raise('INVALID_NODE', 'cond expects [c, t, e]');
                }
                $c = self::requireBool(self::evaluate($arg[0], $scope), 'cond');
                return self::evaluate($c ? $arg[1] : $arg[2], $scope); // 採用側のみ評価

            // ── その他 ──
            case 'len':
                $a = self::evaluate(self::argUnary($op, $arg), $scope);
                if (!self::isList($a)) {
                    ExprFailure::raise(
                        'TYPE_MISMATCH',
                        'len: 配列のみ（string length は v1 に存在しない。got ' . self::typeName($a) . '）'
                    );
                }
                return count($a);

            default:
                ExprFailure::raise('UNKNOWN_OP', "unknown operator: {$op}（fail-closed）");
        }

        // unreachable（全 case が return / raise）。
        ExprFailure::raise('INVALID_NODE', 'unreachable');
    }

    // ── 参照 ────────────────────────────────────────────────────────────────
    private static function evalRef(string $op, mixed $arg, array $scope): mixed
    {
        $path = self::argArray($op, $arg);
        if (count($path) === 0) {
            ExprFailure::raise('INVALID_NODE', "{$op} expects a non-empty string path");
        }
        foreach ($path as $p) {
            if (!is_string($p)) {
                ExprFailure::raise('INVALID_NODE', "{$op} expects a non-empty string path");
            }
        }
        $head = $path[0];
        $rest = array_slice($path, 1);
        if (!array_key_exists($head, $scope)) {
            ExprFailure::raise('UNKNOWN_BINDING', "unknown binding: {$head}");
        }
        $cur = $scope[$head];
        foreach ($rest as $seg) {
            if ($cur === null) {
                if ($op === 'refOpt') {
                    return null; // ?. の null 伝播
                }
                ExprFailure::raise('NULL_REF', "null intermediate at .{$seg}（?. を使う）");
            }
            if (!($cur instanceof \stdClass)) {
                ExprFailure::raise('TYPE_MISMATCH', "cannot access .{$seg} on " . self::typeName($cur));
            }
            if (!array_key_exists($seg, get_object_vars($cur))) {
                ExprFailure::raise('MISSING_PROP', "missing property .{$seg}");
            }
            $cur = $cur->{$seg};
        }
        return $cur;
    }

    // ── i64 / f64 チェック ────────────────────────────────────────────────────
    private static function checkFinite(float $v): float
    {
        if (is_nan($v) || is_infinite($v)) {
            ExprFailure::raise('NAN_OR_INF', 'non-finite float');
        }
        return $v;
    }

    /**
     * {int:"..."} の文字列を i64 int として復元する。範囲外は INT_OVERFLOW、
     * 形式違反は INVALID_LITERAL。PHP には bigint が無いため文字列で境界比較する。
     */
    private static function parseI64Literal(string $s): int
    {
        // 形式: 省略可能な先頭 '-' + 10 進数字（先頭ゼロ許容。JS BigInt("007")==7 相当）。
        if (!preg_match('/^-?\d+$/', $s)) {
            ExprFailure::raise('INVALID_LITERAL', "invalid int literal: {$s}");
        }
        $neg = ($s[0] === '-');
        $digits = ltrim($neg ? substr($s, 1) : $s, '0');
        if ($digits === '') {
            return 0; // "0" / "-0" / "0000"
        }
        // i64 境界を 10 進文字列比較で判定する。
        $maxAbs = $neg ? '9223372036854775808' : '9223372036854775807';
        if (self::decStrGreater($digits, $maxAbs)) {
            ExprFailure::raise('INT_OVERFLOW', "i64 overflow: {$s}");
        }
        $v = (int) ($neg ? '-' . $digits : $digits);
        return $v;
    }

    /** 正の 10 進数字列 $a が $b より大きいか（先頭ゼロなし前提）。 */
    private static function decStrGreater(string $a, string $b): bool
    {
        if (strlen($a) !== strlen($b)) {
            return strlen($a) > strlen($b);
        }
        return strcmp($a, $b) > 0;
    }

    private static function widenToFloat(mixed $v): float
    {
        if (is_float($v)) {
            return $v;
        }
        if (self::isInt($v)) {
            if ($v > self::WIDEN_EXACT || $v < -self::WIDEN_EXACT) {
                ExprFailure::raise('PRECISION_LOSS', "int {$v} exceeds exact float range (±2^53)");
            }
            return (float) $v;
        }
        ExprFailure::raise('TYPE_MISMATCH', 'numeric operand expected, got ' . self::typeName($v));
    }

    // ── 文字列比較（code point 順）────────────────────────────────────────────
    /**
     * code point 順の比較（-1/0/1）。UTF-8 文字列を code point 列に分解して比較する
     * （PHP の `<`/`>` は numeric string を数値化する型ジャグリングがあるため使わない）。
     */
    public static function cmpCodePoints(string $a, string $b): int
    {
        $ca = self::codePoints($a);
        $cb = self::codePoints($b);
        $na = count($ca);
        $nb = count($cb);
        $n = min($na, $nb);
        for ($i = 0; $i < $n; $i++) {
            if ($ca[$i] !== $cb[$i]) {
                return $ca[$i] < $cb[$i] ? -1 : 1;
            }
        }
        if ($na === $nb) {
            return 0;
        }
        return $na < $nb ? -1 : 1;
    }

    /** @return int[] UTF-8 文字列の code point 列。 */
    private static function codePoints(string $s): array
    {
        if ($s === '') {
            return [];
        }
        $chars = mb_str_split($s, 1, 'UTF-8');
        $out = [];
        foreach ($chars as $ch) {
            $out[] = mb_ord($ch, 'UTF-8');
        }
        return $out;
    }

    // ── 型ユーティリティ ──────────────────────────────────────────────────────
    private static function isInt(mixed $v): bool
    {
        // PHP は bool が int のサブクラスではないので is_int(true)===false（罠なし）。
        return is_int($v);
    }

    private static function isList(mixed $v): bool
    {
        return is_array($v);
    }

    private static function typeName(mixed $v): string
    {
        if ($v === null) {
            return 'null';
        }
        if (is_bool($v)) {
            return 'boolean';
        }
        if (is_int($v)) {
            return 'int';
        }
        if (is_float($v)) {
            return 'float';
        }
        if (is_string($v)) {
            return 'string';
        }
        if (is_array($v)) {
            return 'arr';
        }
        if ($v instanceof \stdClass) {
            return 'object';
        }
        return get_debug_type($v);
    }

    private static function requireBool(mixed $v, string $ctx): bool
    {
        if (!is_bool($v)) {
            ExprFailure::raise(
                'TYPE_MISMATCH',
                "{$ctx}: bool expected, got " . self::typeName($v) . '（truthiness は存在しない）'
            );
        }
        return $v;
    }

    // ── 等価（eq/ne 用。null 判定を許可、それ以外は同一スカラ型のみ）──────────────
    private static function valueEquals(mixed $a, mixed $b): bool
    {
        if ($a === null || $b === null) {
            return $a === null && $b === null;
        }
        $ta = self::typeName($a);
        $tb = self::typeName($b);
        if ($ta !== $tb) {
            ExprFailure::raise('TYPE_MISMATCH', "eq/ne: 同一型のみ（got {$ta}×{$tb}）");
        }
        if ($ta === 'arr' || $ta === 'object') {
            ExprFailure::raise('TYPE_MISMATCH', 'eq/ne: obj/arr の等価は v1 で未定義');
        }
        return $a === $b;
    }

    // ── 引数ヘルパ ────────────────────────────────────────────────────────────
    /** @return array<int,mixed> */
    private static function argArray(string $op, mixed $arg): array
    {
        if (!is_array($arg)) {
            ExprFailure::raise('INVALID_NODE', "{$op} expects an args array");
        }
        return $arg;
    }

    private static function argUnary(string $op, mixed $arg): mixed
    {
        $a = self::argArray($op, $arg);
        if (count($a) !== 1) {
            ExprFailure::raise('INVALID_NODE', "{$op} expects 1 arg");
        }
        return $a[0];
    }

    /** @return array{0:mixed,1:mixed} */
    private static function rawBinary(string $op, mixed $arg): array
    {
        $a = self::argArray($op, $arg);
        if (count($a) !== 2) {
            ExprFailure::raise('INVALID_NODE', "{$op} expects 2 args");
        }
        return [$a[0], $a[1]];
    }

    /** @return array{0:mixed,1:mixed} */
    private static function evalBinary(string $op, mixed $arg, array $scope): array
    {
        [$ea, $eb] = self::rawBinary($op, $arg);
        return [self::evaluate($ea, $scope), self::evaluate($eb, $scope)]; // 左から右
    }
}
