<?php

/**
 * !!! VENDORED — DO NOT EDIT !!!
 *
 * Mechanically vendored from behavior-contracts/php/src/Constants.php by
 * scripts/vendor-behavior-contracts-php.mjs (litedbmodel#33). The SSoT is the
 * behavior-contracts repo; edit there and re-run the vendoring script. A CI drift
 * gate (npm run vendor:bc-php:check) fails if this copy diverges.
 */

/**
 * Constants.php — 名前空間レベルの共有定数（load 順に依存しない SSoT）。
 *
 * FORBIDDEN_OBJECT_KEY は ExprEval / Codec の双方が object 構築経路で参照する。file-scope の
 * const は PSR-4 クラスオートロードでは定義されない（クラス参照でしか当該ファイルが読まれない）
 * ため、どちらか一方だけを先に触る経路（例: codegen 直線モジュールが ExprEval を経由せず Codec を
 * 呼ぶ）で "Undefined constant" になり得る。この定数を composer の `files` autoload に載せて
 * **常に**定義されるようにする（値の SSoT は 1 か所のまま）。
 */

declare(strict_types=1);

namespace LiteDbModel\Runtime\BehaviorContracts;

/**
 * 禁止キー（expression-ir.md §2.3/§8）。IR/JSON 由来の任意文字列キーから object を
 * 構築するすべての経路で own key "__proto__" を fail-closed で拒否する。JS では
 * prototype セッタを踏んでキーが消える一方、PHP/Python/Rust/Go は保持するため、
 * 同一 IR が言語間で発散する（prototype pollution 対策）。全言語で FORBIDDEN_KEY。
 */
const FORBIDDEN_OBJECT_KEY = '__proto__';
