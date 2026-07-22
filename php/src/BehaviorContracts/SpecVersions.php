<?php

/**
 * !!! VENDORED — DO NOT EDIT !!!
 *
 * Mechanically vendored from behavior-contracts/php/src/SpecVersions.php by
 * scripts/vendor-behavior-contracts-php.mjs (litedbmodel#33). The SSoT is the
 * behavior-contracts repo; edit there and re-run the vendoring script. A CI drift
 * gate (npm run vendor:bc-php:check) fails if this copy diverges.
 */

/**
 * SpecVersions.php — この PHP port が対応する仕様バージョン（PHP port）。
 *
 * IR/vector の spec version（ライブラリ semver とは別管理）。TS の
 * `ts/package.json` "specVersions"、Python の `SPEC_VERSIONS` / `ENVELOPE_SPEC_VERSION`、
 * Rust の `SpecVersions` consts、Go の `SpecVersions` map と **完全一致** させること
 * （scripts/check-spec-versions.mjs がクロス言語で照合する）。
 */

declare(strict_types=1);

namespace LiteDbModel\Runtime\BehaviorContracts;

final class SpecVersions
{
    public const EXPRESSION = 2;
    public const TEMPLATE = 1;
    public const PLAN = 1;
    public const CANONICAL = 2;
    // behavior (component-graph IR / runBehavior)。v4（#128/A6・SCP-only）: provenance token +
    // 全ノード型確定を serialized-IR 不変条件へ格上げ（released 3=fanout を retire）。v1–v3 意味論は
    // 不変だが、旧 runtime（behavior≤3）は新 IR を loud reject する（機構 = 生成モジュールの
    // baked-spec-skew ゲート[EXPECTED_SPEC_VERSIONS.behavior vs runtime] + conformance pre-flight
    // version fail-closed。validateEnvelope は envelope の specVersion「major.minor」文字列を見るので
    // あって behaviorVersion 整数ではない）。
    // v5 carries two changes: (a) a map node's Element Error Policy Kind (error|skip) + the structured detail a Failure carries (scp-error.md) — elementPolicy:skip changes which elements are present; (b) an OMITTED input key for a port DECLARED optional (inputPorts[name].required === false, i.e. {opt:T}) binds to null — the runner reads the component's own inputPorts declaration when building the entry scope. A required / undeclared name is unaffected (still UNKNOWN_BINDING). An old runtime (behavior<=4) loud-rejects a v5 IR via the baked-spec-skew gate + conformance pre-flight version fail-closed.
    public const BEHAVIOR = 5;
    // guard（v3）: map の elementPolicy 閉集合（error|skip）と、per-element Failure の無い文脈（batched map）での skip 宣言の reject を vector-pin。（v2 = assertCompiled + UNTYPED_NODE + operator 型シグネチャ SSoT の accept/reject。）
    public const GUARD = 3;
    // c2（c2-catalog-swap — catalog-swap 実行 + IR 構造一致の 5 言語 pin, bc#28）。
    public const C2 = 1;
    // provenance（v1・#128/A6）: シリアライズ境界の canonical fingerprint 照合（改竄/stale の
    // 5 言語 loud reject）+ 非空虚性（CONFORMANCE_MUTATE）。
    public const PROVENANCE = 1;

    /** validateEnvelope に渡す graphddb 形式の既定 supported 版（"<major>.<minor>"）。 */
    public const ENVELOPE = '1.1';
}
