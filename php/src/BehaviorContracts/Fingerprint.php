<?php

/**
 * !!! VENDORED — DO NOT EDIT !!!
 *
 * Mechanically vendored from behavior-contracts/php/src/Fingerprint.php by
 * scripts/vendor-behavior-contracts-php.mjs (litedbmodel#33). The SSoT is the
 * behavior-contracts repo; edit there and re-run the vendoring script. A CI drift
 * gate (npm run vendor:bc-php:check) fails if this copy diverges.
 */

/**
 * Fingerprint.php — 可搬 IR ドキュメントの決定的 fingerprint（PHP port, bc#13 SP2）。
 *
 * TS 参照実装 `ts/src/generator/fingerprint.ts` の完全一致 port。生成モジュール
 * （共通 Generator の php emitter 出力）が require 時に埋め込みリテラルから
 * fingerprint を再計算し、生成時に焼き込まれた定数と比較するために使う
 * （skew は LOUD reject — graphddb #208 の prepared-artifact 規律）。
 *
 * クロス言語一致の規律（TS / Python / Rust / Go と完全一致）:
 *   1. JSON ドキュメント（stdClass / list array / スカラ — json_decode(.., false) 形）を
 *      Value ドメインへ持ち上げる。**整数値の number は int**、非整数のみ float。
 *      JSON のデータモデル（RFC 8259）は 1 と 1.0 を区別しないため、integral float は
 *      int に正準化される。-0.0 は float のまま（TS 側と一致）。
 *   2. canonical JSON（全階層 code-point キーソート・canonical-serialization.md §3。
 *      float は CPython repr 形式 = pyFloatRepr）で直列化。
 *   3. UTF-8 bytes への FNV-1a 64。表現は `"fnv1a64:" + 16桁小文字hex`。
 *
 * 制約（fail-closed）: |n| >= 2^53 の整数値 number は拒否する（可搬 IR の大整数は
 * Expression IR の {int:"..."} を使う — expression-ir.md §2.3 と同じ規律。TS は
 * JSON.parse で精度を失うため、ここで許すと言語間で fingerprint が発散する）。
 *
 * 注: PHP port には canonical 直列化 COMMON（CanonicalValue / CanonicalJSON suite）は
 * 未実装（WS6 スコープ外）。本クラスの canonical JSON / pyFloatRepr / FNV-1a 64 は
 * fingerprint 用途に閉じた内部実装であり、他言語の CanonicalJSON と同じ規範
 * （全階層キーソート・pyFloatRepr・ensure_ascii=false）に従う。
 */

declare(strict_types=1);

namespace LiteDbModel\Runtime\BehaviorContracts;

final class Fingerprint
{
    private const FLOAT64_EXACT = 9007199254740992; // 2^53
    private const FORBIDDEN_KEY = '__proto__';

    /**
     * componentGraph — 可搬 IR ドキュメントの決定的 fingerprint（`"fnv1a64:<16hex>"`）。
     *
     * 同一構造の IR（キー順・整数/整数値float の表記差を除く）は言語を跨いで同一の
     * fingerprint になる。生成モジュールの require 時自己検査・consumer 側の
     * 「live IR ↔ 生成コード」照合（#208 の prepared-loader と同型）に使う。
     *
     * @param mixed $doc 可搬 IR ドキュメント（stdClass / list array / スカラ）。
     * @throws FingerprintFailure UNSAFE_NUMBER / INVALID_VALUE / FORBIDDEN_KEY（fail-closed）。
     */
    public static function componentGraph(mixed $doc): string
    {
        $canonical = self::canonicalJson(self::toValueDomain($doc, '$'));
        return 'fnv1a64:' . self::fnv1a64($canonical);
    }

    // ── Value ドメインへの持ち上げ（数値正準化） ─────────────────────────────────

    private static function toValueDomain(mixed $doc, string $path): mixed
    {
        if ($doc === null || is_bool($doc) || is_string($doc)) {
            return $doc;
        }
        if (is_int($doc)) {
            if ($doc <= -self::FLOAT64_EXACT || $doc >= self::FLOAT64_EXACT) {
                self::unsafeNumber((string) $doc, $path);
            }
            return $doc;
        }
        if (is_float($doc)) {
            // 整数値 float は int へ正準化（-0.0 は float のまま — TS と一致）。
            if (is_finite($doc) && floor($doc) === $doc && !($doc === 0.0 && self::isNegativeZero($doc))) {
                if (abs($doc) >= (float) self::FLOAT64_EXACT) {
                    self::unsafeNumber(self::pyFloatRepr($doc), $path);
                }
                return (int) $doc;
            }
            return $doc;
        }
        if (is_array($doc)) {
            if (!array_is_list($doc)) {
                FingerprintFailure::raise(
                    'INVALID_VALUE',
                    "non-list PHP array at {$path} cannot appear in a portable IR document " .
                        '(objects must be stdClass — json_decode(.., false) form) (fail-closed)'
                );
            }
            $out = [];
            foreach ($doc as $i => $v) {
                $out[] = self::toValueDomain($v, "{$path}[{$i}]");
            }
            return $out;
        }
        if ($doc instanceof \stdClass) {
            $out = new \stdClass();
            foreach (get_object_vars($doc) as $k => $v) {
                $out->{(string) $k} = self::toValueDomain($v, "{$path}." . (string) $k);
            }
            return $out;
        }
        FingerprintFailure::raise(
            'INVALID_VALUE',
            'value of type ' . get_debug_type($doc) . " at {$path} cannot appear in a portable IR document (fail-closed)"
        );
    }

    /** @return never */
    private static function unsafeNumber(string $text, string $path): never
    {
        FingerprintFailure::raise(
            'UNSAFE_NUMBER',
            "integral number {$text} at {$path} exceeds float64-exact range; " .
                'portable IR must use {int:"..."} (fail-closed)'
        );
    }

    private static function isNegativeZero(float $f): bool
    {
        return $f === 0.0 && fdiv(1.0, $f) === -INF;
    }

    // ── canonical JSON（全階層 code-point キーソート — canonical-serialization.md §3）──

    private static function canonicalJson(mixed $v): string
    {
        if ($v === null) {
            return 'null';
        }
        if (is_bool($v)) {
            return $v ? 'true' : 'false';
        }
        if (is_string($v)) {
            return self::jsonString($v);
        }
        if (is_int($v)) {
            return (string) $v;
        }
        if (is_float($v)) {
            return self::pyFloatRepr($v);
        }
        if (is_array($v)) {
            $parts = [];
            foreach ($v as $e) {
                $parts[] = self::canonicalJson($e);
            }
            return '[' . implode(',', $parts) . ']';
        }
        if ($v instanceof \stdClass) {
            $props = get_object_vars($v);
            $keys = array_map(static fn ($k): string => (string) $k, array_keys($props));
            // code-point 順 = UTF-8 バイト順（strcmp）。
            usort($keys, static fn (string $a, string $b): int => strcmp($a, $b));
            $parts = [];
            foreach ($keys as $k) {
                if ($k === self::FORBIDDEN_KEY) {
                    FingerprintFailure::raise(
                        'FORBIDDEN_KEY',
                        'object key "' . self::FORBIDDEN_KEY . '" is forbidden (fail-closed)'
                    );
                }
                $parts[] = self::jsonString($k) . ':' . self::canonicalJson($props[$k]);
            }
            return '{' . implode(',', $parts) . '}';
        }
        FingerprintFailure::raise('INVALID_VALUE', 'cannot canonically serialize value of type ' . get_debug_type($v));
    }

    /**
     * jsonString — ensure_ascii=false 意味論の JSON 文字列エンコード
     * （非 ASCII は UTF-8 素通し。json.dumps(ensure_ascii=False) / JSON.stringify と一致）。
     */
    private static function jsonString(string $s): string
    {
        $out = '"';
        $len = strlen($s);
        for ($i = 0; $i < $len; $i++) {
            $c = $s[$i];
            $b = ord($c);
            if ($c === '"') {
                $out .= '\\"';
            } elseif ($c === '\\') {
                $out .= '\\\\';
            } elseif ($c === "\n") {
                $out .= '\\n';
            } elseif ($c === "\r") {
                $out .= '\\r';
            } elseif ($c === "\t") {
                $out .= '\\t';
            } elseif ($b === 0x08) {
                $out .= '\\b';
            } elseif ($b === 0x0C) {
                $out .= '\\f';
            } elseif ($b < 0x20) {
                $out .= sprintf('\\u%04x', $b);
            } else {
                $out .= $c; // UTF-8 継続バイト含め素通し
            }
        }
        return $out . '"';
    }

    // ── pyFloatRepr（CPython repr(float) / json.dumps(float) と完全一致）──────────

    /**
     * pyFloatRepr — 有限 float を CPython repr(float) と同一表記で描画する
     * （Go pyfloat.go / Rust pyfloat 実装の port）。NaN/±Inf は fail-closed。
     */
    public static function pyFloatRepr(float $f): string
    {
        if (is_nan($f) || is_infinite($f)) {
            FingerprintFailure::raise('INVALID_VALUE', "non-finite float cannot be serialized: {$f} (fail-closed)");
        }
        if ($f === 0.0) {
            return self::isNegativeZero($f) ? '-0.0' : '0.0';
        }

        [$neg, $digits, $decpt] = self::shortestDigits($f);
        $n = strlen($digits);

        // format_float_short 'r': scientific iff decpt <= -4 or decpt > 16.
        if ($decpt <= -4 || $decpt > 16) {
            $mant = $digits[0];
            if ($n > 1) {
                $mant .= '.' . substr($digits, 1);
            }
            $e = $decpt - 1;
            $esign = $e < 0 ? '-' : '+';
            $eabs = (string) abs($e);
            if (strlen($eabs) < 2) {
                $eabs = '0' . $eabs;
            }
            $out = $mant . 'e' . $esign . $eabs;
        } elseif ($decpt <= 0) {
            $out = '0.' . str_repeat('0', -$decpt) . $digits;
        } elseif ($decpt >= $n) {
            $out = $digits . str_repeat('0', $decpt - $n) . '.0';
        } else {
            $out = substr($digits, 0, $decpt) . '.' . substr($digits, $decpt);
        }

        return $neg ? '-' . $out : $out;
    }

    /**
     * shortestDigits — 最短 round-trip 10進数字列と 10進小数点位置を返す。
     *
     * @return array{0: bool, 1: string, 2: int} [neg, digits, decpt]
     */
    private static function shortestDigits(float $f): array
    {
        $chosen = '';
        for ($p = 0; $p <= 17; $p++) {
            $s = sprintf('%.' . $p . 'e', $f);
            if ((float) $s === $f) {
                $chosen = $s;
                break;
            }
        }
        if ($chosen === '') {
            $chosen = sprintf('%.17e', $f);
        }

        $s = $chosen;
        $neg = false;
        if ($s[0] === '-') {
            $neg = true;
            $s = substr($s, 1);
        }
        $mantissa = $s;
        $expPart = '0';
        $ePos = stripos($s, 'e');
        if ($ePos !== false) {
            $mantissa = substr($s, 0, $ePos);
            $expPart = substr($s, $ePos + 1);
        }
        $exp = (int) $expPart;

        $intPart = $mantissa;
        $fracPart = '';
        $dotPos = strpos($mantissa, '.');
        if ($dotPos !== false) {
            $intPart = substr($mantissa, 0, $dotPos);
            $fracPart = substr($mantissa, $dotPos + 1);
        }
        $digits = $intPart . $fracPart;
        $decpt = strlen($intPart) + $exp;

        $trimmedLeft = ltrim($digits, '0');
        if ($trimmedLeft === '') {
            return [$neg, '0', 1];
        }
        $decpt -= strlen($digits) - strlen($trimmedLeft);
        $digits = $trimmedLeft;

        $digits = rtrim($digits, '0');
        if ($digits === '') {
            $digits = '0';
        }
        return [$neg, $digits, $decpt];
    }

    // ── FNV-1a 64（純 PHP 64bit — 乗算は 16bit limb 分割で 2^64 mod を正確に計算）──

    private static function fnv1a64(string $data): string
    {
        $h = -3750763034362895579; // 0xcbf29ce484222325 の signed 64bit 表現
        $len = strlen($data);
        for ($i = 0; $i < $len; $i++) {
            $h ^= ord($data[$i]);
            $h = self::mul64($h, 0x100000001b3);
        }
        return sprintf('%016x', $h);
    }

    /**
     * mul64 — 64bit × 64bit の下位 64bit（mod 2^64）。PHP の int 乗算は 64bit を
     * 超えると float に落ちるため、16bit limb に分割して正確に計算する。
     */
    private static function mul64(int $a, int $b): int
    {
        $a0 = $a & 0xFFFF;
        $a1 = ($a >> 16) & 0xFFFF;
        $a2 = ($a >> 32) & 0xFFFF;
        $a3 = ($a >> 48) & 0xFFFF;
        $b0 = $b & 0xFFFF;
        $b1 = ($b >> 16) & 0xFFFF;
        $b2 = ($b >> 32) & 0xFFFF;
        $b3 = ($b >> 48) & 0xFFFF;

        $c0 = $a0 * $b0;
        $c1 = $a1 * $b0 + $a0 * $b1;
        $c2 = $a2 * $b0 + $a1 * $b1 + $a0 * $b2;
        $c3 = $a3 * $b0 + $a2 * $b1 + $a1 * $b2 + $a0 * $b3;

        $r0 = $c0 & 0xFFFF;
        $carry = ($c0 >> 16) & 0xFFFFFFFFFFFF;
        $t1 = $c1 + $carry;
        $r1 = $t1 & 0xFFFF;
        $carry = ($t1 >> 16) & 0xFFFFFFFFFFFF;
        $t2 = $c2 + $carry;
        $r2 = $t2 & 0xFFFF;
        $carry = ($t2 >> 16) & 0xFFFFFFFFFFFF;
        $t3 = $c3 + $carry;
        $r3 = $t3 & 0xFFFF;

        return ($r3 << 48) | ($r2 << 32) | ($r1 << 16) | $r0;
    }
}
