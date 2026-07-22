<?php

/**
 * !!! VENDORED — DO NOT EDIT !!!
 *
 * Mechanically vendored from behavior-contracts/php/src/Plan.php by
 * scripts/vendor-behavior-contracts-php.mjs (litedbmodel#33). The SSoT is the
 * behavior-contracts repo; edit there and re-run the vendoring script. A CI drift
 * gate (npm run vendor:bc-php:check) fails if this copy diverges.
 */

/**
 * Plan.php — execution-plan.md の参照実装（doc-grade, COMMON, PHP port）。
 *
 * TS 参照実装 `ts/src/plan.ts` / Python port `plan.py` の意味論を **完全一致** で移植。
 *
 * runPlan(plan, ops, exec): stage groups を逐次実行し、各 stage 内を index 昇順で回し、
 * 未生成 Port の Skip 伝播と Error Policy Kind（fail/retry/continue）を解釈して
 * 決定的な result tree（Φ 合流）を返す。ノード実行は consumer コールバック `exec` に委譲。
 *
 * 決定性の要（execution-plan.md §4）:
 *   - stage 内は index 昇順で評価（concurrency 値は結果に影響しない）。
 *   - stage 間は逐次（後 stage が前 stage の result を読む）。
 *
 * 並列実行（bc#23、execution-plan.md §4.1）: PHP port は **documented sequential
 * fallback** を採る（標準 PHP runtime にスレッドが無く、callable seam も同期のため
 * stage 内重畳を実装しない）。§4.1 の規範は「並列実行しても観測結果が逐次と byte 一致
 * すること」であり、逐次実行はその自明な conforming 実装である（concurrency は plan
 * 構造の一部だが結果を変えない — §3）。並列化する他言語（TS async / Python threads /
 * Go goroutines / Rust run_plan_parallel）と同じ vectors で結果一致が担保される。
 *
 * 値モデルは ExprEval / Codec と同じ: object は stdClass、list は PHP array。
 *
 * @phpstan-type OpSpec array{id:string, parent:int|null, bindField?:string, relationKind?:string, policy?:string}
 * @phpstan-type ExecOutcome array{ok:mixed}|array{error:string}
 * @phpstan-type OpState array{status:string, value?:mixed, error?:string}
 * @phpstan-type RunResult array{states:array<int,array<string,mixed>>, executed:string[], skipped:string[]}
 */

declare(strict_types=1);

namespace LiteDbModel\Runtime\BehaviorContracts;

final class Plan
{
    /**
     * 単一値 relation の未生成 = null / connection の未生成 = {items:[],cursor:null}（§2 末尾）。
     */
    public static function unproducedValue(?string $kind): mixed
    {
        if ($kind === 'connection') {
            $o = new \stdClass();
            $o->items = [];
            $o->cursor = null;
            return $o;
        }
        return null;
    }

    /**
     * runPlan — execution-plan.md の骨格。
     *
     * @param array{groups:array<int,int[]>,concurrency:int}|null $plan groups/concurrency。null なら 1 op = 1 stage の逐次 fallback（§4）。
     * @param array<int,array<string,mixed>> $ops operation 定義（index 順。ops[0] は root）。
     * @param callable(array<string,mixed>,mixed):array<string,mixed> $exec ノード実行（consumer 委譲）。exec(op, boundValue) -> {ok}|{error}。
     * @return array{states:array<int,?array<string,mixed>>, executed:string[], skipped:string[]}
     */
    public static function runPlan(?array $plan, array $ops, callable $exec): array
    {
        $n = count($ops);
        /** @var array<int,?array<string,mixed>> $states */
        $states = array_fill(0, $n, null);
        /** @var int[] $executedIdx */
        $executedIdx = [];
        /** @var int[] $skippedIdx */
        $skippedIdx = [];

        // plan 不在 → 1 op = 1 stage の逐次 fallback（§4 / §6 罠）。
        if ($plan !== null) {
            $stages = $plan['groups'];
        } else {
            $stages = [];
            for ($i = 0; $i < $n; $i++) {
                $stages[] = [$i];
            }
        }

        self::validateCoverage($stages, $n);

        // 逐次 stage × stage 内 index 昇順（concurrency 値は結果不変 — §3）。
        foreach ($stages as $stage) {
            $ordered = $stage;
            sort($ordered);
            foreach ($ordered as $i) {
                $states[$i] = self::runOp($i, $ops[$i], $states, $exec, $executedIdx, $skippedIdx);
            }
        }

        sort($executedIdx);
        sort($skippedIdx);

        return [
            'states' => $states,
            'executed' => array_map(static fn (int $i): string => (string) $ops[$i]['id'], $executedIdx),
            'skipped' => array_map(static fn (int $i): string => (string) $ops[$i]['id'], $skippedIdx),
        ];
    }

    /**
     * @param array<string,mixed> $op
     * @param array<int,?array<string,mixed>> $states
     * @param callable(array<string,mixed>,mixed):array<string,mixed> $exec
     * @param int[] $executedIdx
     * @param int[] $skippedIdx
     * @return array<string,mixed> OpState
     */
    private static function runOp(
        int $i,
        array $op,
        array $states,
        callable $exec,
        array &$executedIdx,
        array &$skippedIdx
    ): array {
        $policy = $op['policy'] ?? 'fail';
        if ($policy !== 'fail' && $policy !== 'retry' && $policy !== 'continue') {
            PlanFailure::raise('UNKNOWN_POLICY', "unknown policy kind: {$policy}"); // fail-closed
        }

        // ── §2 Skip 伝播: 依存 Port が Success でなければ Skip（下流へ連鎖）──
        $boundValue = null;
        $hasBound = false;
        $parent = $op['parent'] ?? null;
        if ($parent !== null) {
            $parentState = $states[$parent] ?? null;
            // 規則 2: 親が Skip/Failed（未生成）なら子は Skip。
            if ($parentState === null || ($parentState['status'] ?? null) !== 'ok') {
                $skippedIdx[] = $i;
                return ['status' => 'skipped'];
            }
            // null-binding skip（データ駆動）: 親 result[bindField] が null/欠落なら Skip。
            $bindField = $op['bindField'] ?? null;
            if ($bindField !== null) {
                $pv = $parentState['value'];
                $bound = null;
                $found = false;
                if ($pv instanceof \stdClass) {
                    $props = get_object_vars($pv);
                    if (array_key_exists($bindField, $props)) {
                        $bound = $props[$bindField];
                        $found = true;
                    }
                }
                if (!$found || $bound === null) {
                    $skippedIdx[] = $i;
                    return ['status' => 'skipped'];
                }
                $boundValue = $bound;
                $hasBound = true;
            }
        }

        // ── ノード実行（consumer 委譲）──
        $outcome = $exec($op, $hasBound ? $boundValue : null);
        $executedIdx[] = $i;

        if (array_key_exists('ok', $outcome)) {
            return ['status' => 'ok', 'value' => $outcome['ok']];
        }

        // ── §3 失敗の Policy Kind 解釈 ──
        // leaf 由来の detail は Policy Kind 解釈を跨いでそのまま運ぶ（scp-error.md: runtime は
        // 運搬のみ・合成も推論もしない）。
        $leafDetail = $outcome['detail'] ?? null;
        if ($policy === 'fail') {
            PlanFailure::raise('OP_FAILED', "operation '{$op['id']}' failed under 'fail' policy: {$outcome['error']}", $leafDetail);
        }
        if ($policy === 'retry') {
            // 持続失敗は fail と同じ最終伝播へ収束させる（§3.2）。
            PlanFailure::raise('OP_FAILED', "operation '{$op['id']}' failed under 'retry' policy (exhausted): {$outcome['error']}", $leafDetail);
        }
        // policy === 'continue': 失敗 op は Port 未生成 → §2 の Skip 伝播（下流を Skip）。
        $skippedIdx[] = $i;
        array_pop($executedIdx); // 実行はしたが Port を生成しなかった → 未生成として扱う。
        return ['status' => 'skipped'];
    }

    /**
     * finalTree — states を index→port の result tree（Φ 合流の観測形）へ整形する。
     * ok → 値 / skipped → 未生成表現（single=null / connection=空 connection）。
     * conformance vector はこの tree を期待値と byte 比較する（canonicalJson 経由）。
     *
     * @param array<int,?array<string,mixed>> $states runPlan が返す states。
     * @param array<int,array<string,mixed>> $ops operation 定義（index 順）。
     * @return array<string,mixed> id → 値/未生成表現。
     */
    public static function finalTree(array $states, array $ops): array
    {
        $tree = [];
        foreach ($states as $i => $s) {
            if ($s === null) {
                continue;
            }
            $id = (string) $ops[$i]['id'];
            if (($s['status'] ?? null) === 'ok') {
                $tree[$id] = $s['value'];
            } elseif (($s['status'] ?? null) === 'skipped') {
                $tree[$id] = self::unproducedValue($ops[$i]['relationKind'] ?? null);
            }
            // failed は runPlan が投げているのでここに到達しない。
        }
        return $tree;
    }

    /**
     * 各 index がちょうど 1 stage に属し、全 op が被覆されることを検査（§1）。
     *
     * @param array<int,int[]> $stages
     */
    private static function validateCoverage(array $stages, int $n): void
    {
        $seen = [];
        foreach ($stages as $stage) {
            foreach ($stage as $i) {
                if ($i < 0 || $i >= $n) {
                    PlanFailure::raise('INVALID_PLAN', "stage index {$i} out of range [0,{$n})");
                }
                if (isset($seen[$i])) {
                    PlanFailure::raise('INVALID_PLAN', "operation {$i} appears in more than one stage");
                }
                $seen[$i] = true;
            }
        }
        if (count($seen) !== $n) {
            $covered = count($seen);
            PlanFailure::raise('INVALID_PLAN', "plan does not cover all {$n} operations (covered {$covered})");
        }
    }
}
